/* Copyright (c) 2018, 2019, Alibaba and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "sql/recycle_bin/recycle_table.h"
#include "sql/recycle_bin/recycle.h"
#include "sql/recycle_bin/recycle_parse.h"

#include <set>
#include "m_string.h"
#include "mysql/components/services/log_builtins.h"
#include "sql/auth/auth_acls.h"
#include "sql/dd/cache/dictionary_client.h"
#include "sql/dd/collection.h"
#include "sql/dd/dd_schema.h"
#include "sql/dd/dd_table.h"
#include "sql/dd/impl/types/check_constraint_impl.h"
#include "sql/dd/impl/types/column_impl.h"
#include "sql/dd/impl/types/foreign_key_impl.h"
#include "sql/dd/impl/types/index_impl.h"
#include "sql/dd/impl/types/partition_impl.h"
#include "sql/dd/impl/types/table_impl.h"
#include "sql/dd/impl/types/trigger_impl.h"
#include "sql/dd/impl/utils.h"  // dd::my_time_t_to_ull_datetime()
#include "sql/dd/object_id.h"
#include "sql/dd/properties.h"
#include "sql/dd/types/check_constraint.h"
#include "sql/dd/types/column.h"
#include "sql/dd/types/foreign_key.h"
#include "sql/dd/types/index.h"
#include "sql/dd/types/partition.h"
#include "sql/dd/types/table.h"
#include "sql/dd/types/trigger.h"
#include "sql/handler.h"
#include "sql/mdl.h"
#include "sql/mysqld.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/sql_table.h"
#include "sql/strfunc.h"
#include "sql/table.h"
#include "sql/thd_raii.h"

namespace im {
namespace recycle_bin {

LEX_CSTRING RECYCLE_BIN_SCHEMA = {C_STRING_WITH_LEN("__recycle_bin__")};

LEX_CSTRING ORIGIN_SCHEMA = {C_STRING_WITH_LEN("__origin_schema__")};

LEX_CSTRING ORIGIN_TABLE = {C_STRING_WITH_LEN("__origin_table__")};

/* Seconds before really purging the recycled table. */
ulonglong recycle_bin_retention = 7 * 24 * 60 * 60;

/**
  Log warning message when recycle table

  @param[in]      db          Dropping table db
  @param[in]      table_name  Dropping table name
  @param[in]      reason      Why failed
*/
static void log_recycle_warning(const char *db, const char *table_name,
                                const char *reason) {
  std::stringstream ss;
  ss << "Fail to recycle table " << db << "." << table_name << " since "
     << reason;
  LogErr(WARNING_LEVEL, ER_RECYCLE_BIN, ss.str().c_str());
}

/* Error handling constructor */
Recycle_error_handler::Recycle_error_handler(THD *thd, Table_ref *table_list)
    : m_thd(thd), m_table_list(table_list), m_error(false) {
  thd->push_internal_handler(this);
}

/* Ignore those error when recycle */
bool Recycle_error_handler::handle_condition(
    THD *, uint, const char *, Sql_condition::enum_severity_level *level,
    const char *message) {
  /* Log warning */
  log_recycle_warning(m_table_list->db, m_table_list->table_name, message);
  if (*level == Sql_condition::SL_ERROR) m_error = true;
  return true;
}

/* Pop current error handling */
Recycle_error_handler::~Recycle_error_handler() {
  m_thd->pop_internal_handler();
}

/* Save mdl savepoint and override error handling */
Recycle_context_wrapper::Recycle_context_wrapper(THD *thd,
                                                 Table_ref *table_list)
    : m_thd(thd), m_error_handler(thd, table_list) {
  m_mdl_savepoint = thd->mdl_context.mdl_savepoint();
}

/* Restore the MDL state if some error happened. */
Recycle_context_wrapper::~Recycle_context_wrapper() {
  if (m_error_handler.is_error()) {
    m_thd->mdl_context.rollback_to_savepoint(m_mdl_savepoint);
  }
}

/* Constructor */
Timestamp_timezone_guard::Timestamp_timezone_guard(THD *thd) : m_thd(thd) {
  m_tz = m_thd->variables.time_zone;
  m_thd->variables.time_zone = my_tz_OFFSET0;
}

Timestamp_timezone_guard::~Timestamp_timezone_guard() {
  m_thd->variables.time_zone = m_tz;
}

/**
  Build table_list object to check access conveniently.

  @param[in]      thd       thread context
  @param[in]      db        db string
  @param[in]      db_len    db string length
  @param[in]      table     table name string
  @param[in]      table_len table name string length

  @retval         table_list      single table list object
*/
Table_ref *build_table_list(THD *thd, const char *db, size_t db_len,
                            const char *table, size_t table_len) {
  Table_ref *table_list = new (thd->mem_root) Table_ref;
  table_list->db = thd->mem_strdup(db);
  table_list->db_length = db_len;
  table_list->table_name = thd->mem_strdup(table);
  table_list->table_name_length = table_len;
  table_list->next_local = table_list->next_global = nullptr;

  return table_list;
}
/**
  Whether the table was recycled into recycle-bin from normal table.

  Attention:
    Here didn't hold the table level MDL lock, so it's loose checking
*/
static bool is_recycled_table(THD *thd, const char *table,
                              bool need_check_access) {
  const dd::Schema *sch = nullptr;
  const char *schema = nullptr;
  DBUG_ENTER("is_recycled_table");
  dd::Schema_MDL_locker mdl_handler(thd);
  dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());

  schema = RECYCLE_BIN_SCHEMA.str;
  if (mdl_handler.ensure_locked(schema) ||
      thd->dd_client()->acquire(schema, &sch)) {
    LogErr(WARNING_LEVEL, ER_RECYCLE_BIN, "acquire recycle schema");
    DBUG_RETURN(true);
  }
  if (sch == nullptr) {
    my_error(ER_PREPARE_RECYCLE_TABLE_ERROR, MYF(0),
             "recycle schema didn't exist.");
    DBUG_RETURN(true);
  }

  const dd::Table *table_def = nullptr;

  if (thd->dd_client()->acquire(schema, table, &table_def)) DBUG_RETURN(true);

  if (table_def == nullptr) {
    std::stringstream ss;
    ss << table << " didn't exist";
    my_error(ER_PREPARE_RECYCLE_TABLE_ERROR, MYF(0), ss.str().c_str());
    DBUG_RETURN(true);
  }
  if (!table_def->options().exists(ORIGIN_SCHEMA.str) ||
      !table_def->options().exists(ORIGIN_TABLE.str)) {
    std::stringstream ss;
    ss << table << " isn't recycled table";
    my_error(ER_PREPARE_RECYCLE_TABLE_ERROR, MYF(0), ss.str().c_str());
    DBUG_RETURN(true);
  }

  if (need_check_access) {
    LEX_STRING origin_db;
    LEX_STRING origin_table;
    table_def->options().get(ORIGIN_SCHEMA.str, &origin_db, thd->mem_root);
    table_def->options().get(ORIGIN_TABLE.str, &origin_table, thd->mem_root);
    Table_ref *table_list =
        build_table_list(thd, origin_db.str, origin_db.length, origin_table.str,
                         origin_table.length);
    if (check_table_access(thd, DROP_ACL, table_list, false, 1, false))
      DBUG_RETURN(true);
  }

  DBUG_RETURN(false);
}

/**
  Retrieve all recycle schema tables.

  @param[in]        thd         thread context
  @param[in]        mem_root    Memory pool
  @param[in/out]    container   show table result container

  @retval           true        Error
  @retval           false       success
*/
bool get_recycle_tables(THD *thd, MEM_ROOT *mem_root,
                        std::vector<Recycle_show_result *> *container) {
  const dd::Schema *sch = nullptr;
  const char *schema = nullptr;
  std::vector<const dd::Table *> tables;
  DBUG_ENTER("get_recycle_tables");
  assert(container);

  /* Should set timezone offset = 0 or m_last_altered will be confused */
  Timestamp_timezone_guard timezone_guard(thd);

  dd::Schema_MDL_locker mdl_handler(thd);
  dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());

  schema = RECYCLE_BIN_SCHEMA.str;
  if (mdl_handler.ensure_locked(schema) ||
      thd->dd_client()->acquire(schema, &sch) || sch == nullptr ||
      thd->dd_client()->fetch_schema_components(sch, &tables)) {
    LogErr(WARNING_LEVEL, ER_RECYCLE_BIN, "fetch all recycle tables");
    DBUG_RETURN(true);
  }
  for (const dd::Table *table : tables) {
    if (table->options().exists(ORIGIN_SCHEMA.str) &&
        table->options().exists(ORIGIN_TABLE.str)) {
      Recycle_show_result *show_table = new (mem_root) Recycle_show_result();

      table->options().get(ORIGIN_SCHEMA.str, &(show_table->origin_schema),
                           mem_root);
      table->options().get(ORIGIN_TABLE.str, &(show_table->origin_table),
                           mem_root);

      lex_string_strmake(mem_root, &(show_table->schema), sch->name().c_str(),
                         sch->name().length());
      lex_string_strmake(mem_root, &(show_table->table), table->name().c_str(),
                         table->name().length());
      /* Recycled time come from last alter (recycle operation) */
      show_table->recycled_time = table->get_last_altered();

      container->push_back(show_table);
    }
  }
  DBUG_RETURN(false);
}

/**
   Lock the recycle schema

   @param[in]     thd       thread context

   @retval        true      Failure
   @retval        false     Success
*/
static bool lock_recycle_schema(THD *thd) {
  DBUG_ENTER("lock_recycle_schema");
  if (!thd->mdl_context.owns_equal_or_stronger_lock(MDL_key::SCHEMA,
                                                    RECYCLE_BIN_SCHEMA.str, "",
                                                    MDL_INTENTION_EXCLUSIVE)) {
    MDL_request mdl_request;
    MDL_REQUEST_INIT(&mdl_request, MDL_key::SCHEMA, RECYCLE_BIN_SCHEMA.str, "",
                     MDL_INTENTION_EXCLUSIVE, MDL_TRANSACTION);
    if (thd->mdl_context.acquire_lock(&mdl_request,
                                      thd->variables.lock_wait_timeout))
      DBUG_RETURN(true);
  }
  DBUG_RETURN(false);
}
/**
  Check the state whether it's suitable to recycle
  the table which is planned to drop.

  Log some warnings and Continue to drop it if check state fail.

  @param[in]      thd         thread context


  @retval         false       OK
  @retval         true        Not suitable
*/
static bool check_state(THD *thd, Table_ref *table) {
  DBUG_ENTER("check_state");

  /* The recycle table switch */
  if (!thd->variables.recycle_bin) DBUG_RETURN(true);

  /* Error already */
  if (thd->is_error()) DBUG_RETURN(true);

  /* Recycle state within current thread context */
  if (!thd->recycle_state->is_set()) DBUG_RETURN(true);

  /* Exclusive lock on target table */
  assert(thd->mdl_context.owns_equal_or_stronger_lock(
      MDL_key::TABLE, table->db, table->table_name, MDL_EXCLUSIVE));

  /* If dropping table is in recycle schema, deny to recycle it */
  if (my_strcasecmp(system_charset_info, table->db, RECYCLE_BIN_SCHEMA.str) ==
      0) {
    log_recycle_warning(table->db, table->table_name, "schema is recycle bin.");
    DBUG_RETURN(true);
  }
  /* We cann't remove my own table/table_def from table cache if within lock
   * mode */
  if (thd->locked_tables_mode) {
    log_recycle_warning(table->db, table->table_name, "within lock mode.");
    DBUG_RETURN(true);
  }
  /* Deny to recycle the table which restored from recycle */
  dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
  const dd::Table *table_def = nullptr;
  if (thd->dd_client()->acquire(table->db, table->table_name, &table_def))
    DBUG_RETURN(true);

  /* Maybe SUPER_ACL user operated recycle schema directly */
  if (table_def == nullptr || table_def->options().exists(ORIGIN_SCHEMA.str) ||
      table_def->options().exists(ORIGIN_TABLE.str)) {
    log_recycle_warning(table->db, table->table_name,
                        "table doesn't exist or has been recycled.");
    DBUG_RETURN(true);
  }

  /**
    Deny to recycle if table has fts index, since innodb didn't allow
    to evict the dict_table_t which has fts aux ndex.

    TODO: support fts table.
  */
  for (const dd::Index *idx : table_def->indexes()) {
    if (idx->type() == dd::Index::IT_FULLTEXT) {
      log_recycle_warning(table->db, table->table_name,
                          "FTS table doesn't support to be recycled.");
      DBUG_RETURN(true);
    }
  }

  /* Support general tablespace and encrypted table */
  DBUG_RETURN(false);
}

#ifndef DBUG_OFF

static ulonglong string_to_number(const char *str) {
  ulonglong num = 0;
  size_t length = strlen(str);
  for (size_t i = 0; i < length; i++) {
    num += str[i];
  }
  return num;
}
#endif

/**
  Prepare to recycle table;

  1) Lock recycle schema
  2) Generate unique table name and lock it.

  Release the MDL lock and log the error instead of report to client,
  and continue to drop it.

  @param[in]      thd         thread context
  @param[in]      table_list  dropping table
  @param[out]     new_table   renamed new table name

  @retval         false       success
  @retval         true        failure
*/
static bool prepare_recycle_table(THD *thd, Table_ref *table_list,
                                  const char **new_table) {
  char buff[128];
  DBUG_ENTER("prepare_recycle_table");
  /**
    1) Save the MDL savepoint.
    2) Overide the error handling.
  */
  Recycle_context_wrapper recycle_context(thd, table_list);

  /* Lock recycle schema */
  if (lock_recycle_schema(thd)) DBUG_RETURN(true);

  /* Check recycle schema exists */
  {
    dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
    const dd::Schema *recycle_schema = nullptr;
    if (thd->dd_client()->acquire(RECYCLE_BIN_SCHEMA.str, &recycle_schema))
      DBUG_RETURN(true);

    if (recycle_schema == nullptr) {
      my_error(ER_PREPARE_RECYCLE_TABLE_ERROR, MYF(0),
               "recycle schema didn't exist.");
      DBUG_RETURN(true);
    }
  }

  dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
  const dd::Table *table_def = nullptr;
  if (thd->dd_client()->acquire(table_list->db, table_list->table_name,
                                &table_def))
    DBUG_RETURN(true);

  handlerton *hton;
  if (dd::table_storage_engine(thd, table_def, &hton)) {
    assert(false);
    DBUG_RETURN(true);
  }
  if (!(hton->flags & HTON_SUPPORTS_RECYCLE_BIN)) {
    my_error(ER_PREPARE_RECYCLE_TABLE_ERROR, MYF(0),
             "Table engine didn't support recycle bin.");
    DBUG_RETURN(true);
  }
/* Generate unique table name [ Engine_name + SE_private_id ] */
#ifndef DBUG_OFF
  dd::Object_id unique_id = string_to_number(table_list->table_name);
#else
  dd::Object_id unique_id;
  if (table_def->partitions().size() > 0) {
    const dd::Partition *partition_def = table_def->partitions().front();
    if (partition_def->sub_partitions().size() > 0)
      unique_id = partition_def->sub_partitions().front()->se_private_id();
    else
      unique_id = partition_def->se_private_id();
  } else {
    unique_id = table_def->se_private_id();
  }
#endif
  const char *engine_name = table_def->engine().c_str();
  char *prefix = thd->strmake(engine_name, strlen(engine_name));
  my_casedn_str(system_charset_info, prefix);
  snprintf(buff, sizeof(buff), "__%s_%llu", prefix, unique_id);
  *new_table = thd->strmake(buff, strlen(buff));

  /* Hold the new table mdl lock */
  {
    MDL_request mdl_request;
    MDL_REQUEST_INIT(&mdl_request, MDL_key::TABLE, RECYCLE_BIN_SCHEMA.str,
                     *new_table, MDL_EXCLUSIVE, MDL_TRANSACTION);
    if (thd->mdl_context.acquire_lock(&mdl_request,
                                      thd->variables.lock_wait_timeout))
      DBUG_RETURN(true);
  }

  /* check the new table exist */
  const dd::Table *new_table_def = nullptr;
  if (thd->dd_client()->acquire(RECYCLE_BIN_SCHEMA.str, *new_table,
                                &new_table_def))
    DBUG_RETURN(true);

  if (new_table_def != nullptr) {
    my_error(ER_PREPARE_RECYCLE_TABLE_ERROR, MYF(0),
             "table already exists in recycle_bin");
    DBUG_RETURN(true);
  }

  /**
    Check the storage engine by open_table,
    Report error in advance like ibd file missing.
  */
  {
    Open_table_context ot_ctx(thd, MYSQL_OPEN_REOPEN);
    bool save_prelocking_placeholder = table_list->prelocking_placeholder;
    assert(table_list->table == nullptr);
    /* As a substatement open table. */
    table_list->prelocking_placeholder = true;
    if (open_table(thd, table_list, &ot_ctx)) {
      table_list->prelocking_placeholder = save_prelocking_placeholder;
      DBUG_RETURN(true);
    }
    close_thread_table(thd, &thd->open_tables);
    table_list->prelocking_placeholder = save_prelocking_placeholder;
    table_list->table = NULL;
  }

  /**
    Some error didn't report as return value, so check is_error() here.
    Like open_table failed if field collation is invalid when fill_columns, see
    test case
    t/invalid_collation.test;
  */
  if (recycle_context.is_error()) DBUG_RETURN(true);

  DBUG_RETURN(false);
}

/**
  Collect all the foreigh key related to dropping table.

  @param[in]      table_def       Dropping table definition
  @param[in]      hton            Dropping table engine
  @param[in/out]  fk_invalidator  FKs
*/
static void collect_recycle_table_fks(
    dd::Table *table_def, handlerton *hton,
    Foreign_key_parents_invalidator *fk_invalidator) {
  /* referenced tables */
  for (dd::Foreign_key *fk : *table_def->foreign_keys()) {
    char buff_db[NAME_LEN + 1];
    char buff_table[NAME_LEN + 1];

    my_stpncpy(buff_db, fk->referenced_table_schema_name().c_str(), NAME_LEN);
    my_stpncpy(buff_table, fk->referenced_table_name().c_str(), NAME_LEN);

    if (lower_case_table_names == 2) {
      my_casedn_str(system_charset_info, buff_db);
      my_casedn_str(system_charset_info, buff_table);
    }
    fk_invalidator->add(buff_db, buff_table, hton);
  }

  /* children tables */
  for (const dd::Foreign_key_parent *fk : table_def->foreign_key_parents()) {
    char buff_db[NAME_LEN + 1];
    char buff_table[NAME_LEN + 1];
    my_stpncpy(buff_db, fk->child_schema_name().c_str(), NAME_LEN);
    my_stpncpy(buff_table, fk->child_table_name().c_str(), NAME_LEN);

    /*
      In lower-case-table-names == 2 mode we store origin versions of table
      and db names in the data-dictionary. Hence they need to be lowercased
      to produce correct MDL key for them and for other uses.
    */
    if (lower_case_table_names == 2) {
      my_casedn_str(system_charset_info, buff_db);
      my_casedn_str(system_charset_info, buff_table);
    }
    fk_invalidator->add(buff_db, buff_table, hton);
  }
}

/* Init SE attributes. */
static void init_se_attributes(HA_CREATE_INFO *create_info) {
  create_info->data_file_name = nullptr;
  create_info->tablespace = nullptr;
}
/* Copy SE attributes. */
void move_se_attributes(HA_CREATE_INFO *create_info,
                        HA_CREATE_INFO *original_create_info) {
  create_info->data_file_name = original_create_info->data_file_name;
  create_info->tablespace = original_create_info->tablespace;
}

/**
  Rename the table into recycle_bin schema, and drop related object,
  Only keep the fundamental table elements, drop FK , triggers.

  Logic of dealing with table elements.

  1) View:
     Didn't update view metadata, treat as invalid.
  2) drop FK
  3) drop trigger
  4) left table stats & index stats
  5) histograms
     Rename column statistics

  @param[in]      thd             thread context
  @param[in]      post_ddl_htons  atomic hton container
  @param[in]      table_list      dropping table
  @param[in]      fk_invalidator  Reference table container
  @param[in]      only self       Whether only collect myself when add FK
                                  container
  @param[out]     ha_create_info  the original create info from SE before
                                  rename.

  @retval         ok              Success
  @retval         drop_continue   Should continue to drop table
  @retval         error           Report client error

*/
Recycle_result recycle_base_table(
    THD *thd, std::set<handlerton *> *post_ddl_htons,
    Foreign_key_parents_invalidator *fk_invalidator, bool only_self,
    Table_ref *table_list, HA_CREATE_INFO *original_create_info) {
  const char *old_db = nullptr;
  const char *old_table = nullptr;
  const char *new_db = nullptr;
  const char *new_table = nullptr;
  DBUG_ENTER("recycle_base_table");

  old_db = table_list->db;
  old_table = table_list->table_name;
  new_db = RECYCLE_BIN_SCHEMA.str;

  /* Check state */
  if (check_state(thd, table_list)) DBUG_RETURN(Recycle_result::CONTINUE);

  /* Prepare the target table and lock it */
  if (prepare_recycle_table(thd, table_list, &new_table))
    DBUG_RETURN(Recycle_result::CONTINUE);

  assert(new_table != nullptr);

  /* Remove all the table/table_definition cache */
  tdc_remove_table(thd, TDC_RT_REMOVE_ALL, old_db, old_table, false);

  dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
  const dd::Schema *recycle_schema = nullptr;
  if (thd->dd_client()->acquire(new_db, &recycle_schema))
    DBUG_RETURN(Recycle_result::ERROR);

  /* Prepare_recycle_table has already check recycle schema */
  assert(recycle_schema);

  const dd::Table *from_table_def = nullptr;
  dd::Table *to_table_def = nullptr;
  if (thd->dd_client()->acquire(old_db, old_table, &from_table_def) ||
      thd->dd_client()->acquire_for_modification(old_db, old_table,
                                                 &to_table_def))
    DBUG_RETURN(Recycle_result::ERROR);

  assert(from_table_def != nullptr && to_table_def != nullptr);

  handlerton *hton;
  if (dd::table_storage_engine(thd, from_table_def, &hton)) {
    assert(false);
    DBUG_RETURN(Recycle_result::ERROR);
  }

  /* Collect all the tables related to dropping table since of FK */
  if (!only_self)
    collect_recycle_table_fks(const_cast<dd::Table *>(from_table_def), hton,
                              fk_invalidator);

  fk_invalidator->add(new_db, new_table, hton);

  assert(!to_table_def->options().exists(ORIGIN_SCHEMA.str));
  assert(!to_table_def->options().exists(ORIGIN_TABLE.str));

  /* Rename table */
  to_table_def->set_schema_id(recycle_schema->id());
  to_table_def->set_name(new_table);
  to_table_def->options().set(ORIGIN_SCHEMA.str, old_db);
  to_table_def->options().set(ORIGIN_TABLE.str, old_table);
  to_table_def->set_last_altered(
      dd::my_time_t_to_ull_datetime(thd->query_start_in_secs()));

  /* Update the constraints */
  if (dd::rename_check_constraints(old_table, to_table_def))
    DBUG_RETURN(Recycle_result::ERROR);

  /* Remove all the foreign keys */
  to_table_def->drop_all_foreign_keys();

  /* Romove all the triggers */
  to_table_def->drop_all_triggers();

  // Check if we hit FN_REFLEN bytes along with file extension.
  char from[FN_REFLEN + 1];
  char to[FN_REFLEN + 1];
  bool was_truncated;
  build_table_filename(from, sizeof(from) - 1, old_db, old_table, "", false);
  build_table_filename(to, sizeof(to) - 1, new_db, new_table, "", false,
                       &was_truncated);

  // Get the handler for the table, and issue an error if we cannot load it.
  handler *file =
      (hton == NULL ? 0
                    : get_new_handler((TABLE_SHARE *)0,
                                      from_table_def->partition_type() !=
                                          dd::Table::PT_NONE,
                                      thd->mem_root, hton));
  if (!file) {
    my_error(ER_STORAGE_ENGINE_NOT_LOADED, MYF(0), old_db, old_table);
    DBUG_RETURN(Recycle_result::ERROR);
  }

  /*
    If lower_case_table_names == 2 (case-preserving but case-insensitive
    file system) and the storage is not HA_FILE_BASED, we need to provide
    a lowercase file name.
  */
  char lc_from[FN_REFLEN + 1];
  char lc_to[FN_REFLEN + 1];
  char *from_base = from;
  char *to_base = to;
  if (lower_case_table_names == 2 &&
      !(file->ha_table_flags() & HA_FILE_BASED)) {
    char tmp_name[NAME_LEN + 1];
    my_stpcpy(tmp_name, old_table);
    my_casedn_str(files_charset_info, tmp_name);
    build_table_filename(lc_from, sizeof(lc_from) - 1, old_db, tmp_name, "",
                         false);
    from_base = lc_from;

    my_stpcpy(tmp_name, new_table);
    my_casedn_str(files_charset_info, tmp_name);
    build_table_filename(lc_to, sizeof(lc_to) - 1, new_db, tmp_name, "", false);
    to_base = lc_to;
  }

  /* Backup the old table HA_CREATE_INFO attributes which are stored in SE. */
  if (original_create_info) {
    file->get_create_info(from_base, from_table_def, original_create_info);
  }

  int error =
      file->ha_rename_table(from_base, to_base, from_table_def, to_table_def);
  /**
     Whether successful or not, here should add the ddl_htons.
  */
  if (hton->post_ddl) post_ddl_htons->insert(hton);

  if (error != 0) {
    if (error == HA_ERR_WRONG_COMMAND)
      my_error(ER_NOT_SUPPORTED_YET, MYF(0), "RECYCLE TABLE");
    else {
      char errbuf[MYSYS_STRERROR_SIZE];
      my_error(ER_ERROR_ON_RENAME, MYF(0), from, to, error,
               my_strerror(errbuf, sizeof(errbuf), error));
    }
    destroy(file);
    goto err_after_post_ddl;
  }
  destroy(file);

  DBUG_EXECUTE_IF("simulate_crashed_table_error",
                  my_error(HA_ERR_CRASHED, MYF(ME_ERRORLOG), old_table);
                  goto err_after_post_ddl;);

  if (thd->dd_client()->update(to_table_def)) {
    goto err_after_post_ddl;
  }

  /* Rename the histograms */
  if (rename_histograms(thd, old_db, old_table, new_db, new_table)) {
    goto err_after_post_ddl;
  }

  /* Remove all the table/table_definition cache */
  tdc_remove_table(thd, TDC_RT_REMOVE_ALL, new_db, new_table, false);
  DBUG_RETURN(Recycle_result::OK);

err_after_post_ddl:
  tdc_remove_table(thd, TDC_RT_REMOVE_ALL, new_db, new_table, false);
  DBUG_RETURN(Recycle_result::ERROR);
}
/**
  drop the table in recycle_bin

  @param[in]      thd       thread context
  @param[in]      table     Target table name

  @retval         false     success
  @retval         true      failure
*/
bool drop_base_recycle_table(THD *thd, const char *table) {
  LEX *lex;
  DBUG_ENTER("drop_base_recycle_table");
  lex = thd->lex;
  lex->sql_command = SQLCOM_DROP_TABLE;
  lex->drop_temporary = false;
  lex->drop_if_exists = false;

  Table_ref *table_list =
      build_table_list(thd, RECYCLE_BIN_SCHEMA.str, RECYCLE_BIN_SCHEMA.length,
                       table, strlen(table));
  table_list->open_type = OT_BASE_ONLY;

  MDL_REQUEST_INIT(&table_list->mdl_request, MDL_key::TABLE, table_list->db,
                   table_list->table_name, MDL_EXCLUSIVE, MDL_TRANSACTION);
  /* Didn't check any access here */
  bool res =
      mysql_rm_table(thd, table_list, lex->drop_if_exists, lex->drop_temporary);
  DBUG_RETURN(res);
}

/**
  Purge the table in recycle_bin

  @param[in]      thd       thread context
  @param[in]      table     Target table name

  @retval         false     success
  @retval         true      failure
*/
bool recycle_purge_table(THD *thd, const char *table) {
  DBUG_ENTER("recycle_purge_table");
  Recycle_lex recycle_lex(thd);
  Disable_binlog_guard binlog_guard(thd);
  Disable_autocommit_guard autocommit_guard(thd);
  /**
    Purge proc will be blocked if readonly, but recycle scheduler will pass.

    It's not absolutely safe when set read only, since here didn't hold global
    read lock, But tolerate it.
  */
  if (check_readonly(thd, true)) DBUG_RETURN(true);

  /**
    Attention
      Ingore the global read lock, we are allowed to
      purge table under read only mode.
  */
  if (lock_recycle_schema(thd)) DBUG_RETURN(true);

  {
    MDL_request mdl_request;
    MDL_REQUEST_INIT(&mdl_request, MDL_key::TABLE, RECYCLE_BIN_SCHEMA.str,
                     table, MDL_EXCLUSIVE, MDL_TRANSACTION);
    if (thd->mdl_context.acquire_lock(&mdl_request,
                                      thd->variables.lock_wait_timeout))
      DBUG_RETURN(true);
  }

  if (is_recycled_table(thd, table, true)) DBUG_RETURN(true);

  DBUG_RETURN(drop_base_recycle_table(thd, table));
}

class Sql_command_backup {
 public:
  explicit Sql_command_backup(THD *thd, enum enum_sql_command command)
      : m_thd(thd) {
    m_sql_command = m_thd->lex->sql_command;
    m_thd->lex->sql_command = command;
    m_is_recycle_command = m_thd->is_recycle_command;
    m_thd->is_recycle_command = true;
  }

  ~Sql_command_backup() {
    m_thd->lex->sql_command = m_sql_command;
    m_thd->is_recycle_command = m_is_recycle_command;
  }

 private:
  THD *m_thd;
  enum enum_sql_command m_sql_command;
  bool m_is_recycle_command;
};

/**
  Reset the dd::Table entity id value, and the reference entity id value.
*/
template <typename Item, typename Item_impl>
static void reset_primary_key_id(const dd::Collection<Item *> &items) {
  for (const Item *item : items) {
    Item_impl *impl = dynamic_cast<Item_impl *>(const_cast<Item *>(item));
    if (impl) impl->set_id(dd::INVALID_OBJECT_ID);
  }
}
/**
  Recycle the table when truncate table.

  @param[in]      thd                 current thd
  @param[in]      path                table path
  @param[in]      table               table list
  @param[in]      create_info         temporary create info
  @param[in]      update_create_info  Whether update create info
  @param[in]      is_temp_table       Whether it's temporary table
  @param[in]      table_def           dd Table object

  @retval         ok              Success
  @retval         drop_continue   Should continue to truncate table
  @retval         error           Report client error
*/
Recycle_result recycle_truncate_table(THD *thd, const char *path,
                                      Table_ref *table_list,
                                      HA_CREATE_INFO *create_info,
                                      bool update_create_info,
                                      bool is_temp_table,
                                      dd::Table *table_def) {
  int error = 0;
  std::set<handlerton *> dummy_ddl_htons;
  Foreign_key_parents_invalidator fk_invalidator;
  Recycle_result res;
  HA_CREATE_INFO original_create_info;
  DBUG_ENTER("recycle_truncate_table");
  init_se_attributes(&original_create_info);

  res = recycle_base_table(thd, &dummy_ddl_htons, &fk_invalidator, true,
                           table_list, &original_create_info);

  if (res == Recycle_result::OK) {
    fk_invalidator.force_invalidate(thd);

    /* 1. Reset all the primary key of DD table. */

    /* 1.1 Reset tablespace id (Unnecessary right now.)*/

    /* 1.2 Reset dd::Table id */
    dd::Table_impl *table_impl = dynamic_cast<dd::Table_impl *>(table_def);
    if (table_impl) table_impl->set_id(dd::INVALID_OBJECT_ID);

    /* 1.3 Reset dd::Column id */
    reset_primary_key_id<dd::Column, dd::Column_impl>(
        static_cast<const dd::Table *>(table_def)->columns());

    /* 1.4 Reset dd::Index id */
    reset_primary_key_id<dd::Index, dd::Index_impl>(
        static_cast<const dd::Table *>(table_def)->indexes());

    /* 1.5 Reset dd::foreign key id */
    reset_primary_key_id<dd::Foreign_key, dd::Foreign_key_impl>(
        static_cast<const dd::Table *>(table_def)->foreign_keys());

    /* 1.6 Reset dd::triggers */
    reset_primary_key_id<dd::Trigger, dd::Trigger_impl>(
        static_cast<const dd::Table *>(table_def)->triggers());

    /* 1.7 Reset dd::check_constraints */
    reset_primary_key_id<dd::Check_constraint, dd::Check_constraint_impl>(
        static_cast<const dd::Table *>(table_def)->check_constraints());

    /* 1.8 Reset dd::partition */
    for (const dd::Partition *item :
         static_cast<const dd::Table *>(table_def)->partitions()) {
      dd::Partition_impl *impl =
          dynamic_cast<dd::Partition_impl *>(const_cast<dd::Partition *>(item));
      if (impl) {
        impl->set_id(dd::INVALID_OBJECT_ID);
      }
      /* Reset sub partition */
      for (const dd::Partition *sub_item : item->subpartitions()) {
        dd::Partition_impl *sub_impl = dynamic_cast<dd::Partition_impl *>(
            const_cast<dd::Partition *>(sub_item));
        if (sub_impl) {
          sub_impl->set_id(dd::INVALID_OBJECT_ID);
        }
      }
    }
    /* 2. modify the sql command temporary */
    Sql_command_backup backup(thd, SQLCOM_CREATE_TABLE);

    /* 3. create new table */
    if ((error =
             ha_create_table(thd, path, table_list->db, table_list->table_name,
                             create_info, update_create_info, is_temp_table,
                             table_def, true, &original_create_info)))
      res = Recycle_result::ERROR;
  } else {
    goto end;
  }

end:
  DBUG_RETURN(res);
}

} /* namespace recycle_bin */

} /* namespace im */
