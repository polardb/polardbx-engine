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

#include "lex_string.h"
#include "m_string.h"
#include "mysql/components/services/log_builtins.h"
#include "mysql/components/services/log_shared.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/table.h"
#include "sql/transaction.h"

#include "sql/common/table.h"
#include "sql/outline/outline.h"
#include "sql/outline/outline_digest.h"
#include "sql/outline/outline_table.h"
#include "sql/outline/outline_table_common.h"

namespace im {

/* outline table schema name */
LEX_CSTRING OUTLINE_SCHEMA_NAME = {C_STRING_WITH_LEN("mysql")};

/* outline table name */
LEX_CSTRING OUTLINE_TABLE_NAME = {C_STRING_WITH_LEN("outline")};

/* outline table alias */
const char *OUTLINE_TABLE_ALIAS = "outline";

/* Table "mysql.outline" definition */
static const TABLE_FIELD_TYPE
    mysql_outline_table_fields[MYSQL_OUTLINE_FIELD_COUNT] = {
        {{C_STRING_WITH_LEN("Id")}, {C_STRING_WITH_LEN("bigint")}, {NULL, 0}},
        {{C_STRING_WITH_LEN("Schema_name")},
         {C_STRING_WITH_LEN("varchar(64)")},
         {NULL, 0}},
        {{C_STRING_WITH_LEN("Digest")},
         {C_STRING_WITH_LEN("varchar(64)")},
         {NULL, 0}},
        {{C_STRING_WITH_LEN("Digest_text")},
         {C_STRING_WITH_LEN("longtext")},
         {NULL, 0}},
        {{C_STRING_WITH_LEN("Type")},
         {C_STRING_WITH_LEN(
             "enum('IGNORE INDEX','USE INDEX','FORCE INDEX','OPTIMIZER')")},
         {C_STRING_WITH_LEN("utf8mb3")}},
        {{C_STRING_WITH_LEN("Scope")},
         {C_STRING_WITH_LEN(
             "enum('','FOR JOIN','FOR ORDER BY','FOR GROUP BY')")},
         {C_STRING_WITH_LEN("utf8mb3")}},
        {{C_STRING_WITH_LEN("State")},
         {C_STRING_WITH_LEN("enum('N','Y')")},
         {C_STRING_WITH_LEN("utf8mb3")}},
        {{C_STRING_WITH_LEN("Position")},
         {C_STRING_WITH_LEN("bigint")},
         {NULL, 0}},
        {{C_STRING_WITH_LEN("Hint")}, {C_STRING_WITH_LEN("text")}, {NULL, 0}}};

static const TABLE_FIELD_DEF outline_table_def = {MYSQL_OUTLINE_FIELD_COUNT,
                                                  mysql_outline_table_fields};

const char *outline_type_str[] = {"IGNORE INDEX", "USE INDEX", "FORCE INDEX",
                                  "OPTIMIZER"};

const char outline_state_str[] = {'N', 'Y'};

#define HINT_EQUAL(t1, t2) (static_cast<size_t>(t1) == static_cast<size_t>(t2))

/* Make sure it is consistent with index_hint_type_str */
static_assert(HINT_EQUAL(INDEX_HINT_IGNORE, Outline_type::INDEX_IGNORE));
static_assert(HINT_EQUAL(INDEX_HINT_USE, Outline_type::INDEX_USE));
static_assert(HINT_EQUAL(INDEX_HINT_FORCE, Outline_type::INDEX_FORCE));

/* Outline scopes */
const Outline_scope outline_scopes[] = {
    {{C_STRING_WITH_LEN("")}, INDEX_HINT_MASK_ALL},
    {{C_STRING_WITH_LEN("FOR JOIN")}, INDEX_HINT_MASK_JOIN},
    {{C_STRING_WITH_LEN("FOR ORDER BY")}, INDEX_HINT_MASK_ORDER},
    {{C_STRING_WITH_LEN("FOR GROUP BY")}, INDEX_HINT_MASK_GROUP}};

const Outline_scope OUTLINE_NULL_SCOPE = {{nullptr, 0}, 0};

/* Convert string to outline type */
Outline_type to_outline_type(const char *str) {
  for (size_t i = 0; i < static_cast<size_t>(Outline_type::UNKNOWN); i++) {
    if (my_strcasecmp(system_charset_info, outline_type_str[i], str) == 0) {
      return static_cast<Outline_type>(i);
    }
  }
  return Outline_type::UNKNOWN;
}
/* Conver string to outline scope */
Outline_scope to_outline_scope(const char *str) {
  for (size_t i = 0; i < sizeof(outline_scopes) / sizeof(Outline_scope); i++) {
    if (my_strcasecmp(system_charset_info, outline_scopes[i].str.str, str) ==
        0) {
      return outline_scopes[i];
    }
  }
  return OUTLINE_NULL_SCOPE;
}
/**
  Reconstruct and Report error by adding handler error.

  @param[in]      errcode     handler error.
*/
void Outline_reader::print_ha_error(int errcode) {
  ha_error(ER_OUTLINE_TABLE_OP_FAILED, errcode);
}
/**
  Log the conf table error.

  @param[in]    err     Conf error type
*/
void Outline_reader::log_error(Conf_error err) {
  log_conf_error(ER_OUTLINE, err);
}
/* Create new outline record */
Conf_record *Outline_reader::new_record() {
  return new (m_mem_root) Outline_record();
}
/**
  Push invalid outline record warning

  @param[in]      record        Outline record
  @param[in]      when          Operation
*/
void Outline_reader::row_warning(Conf_record *record, const char *when,
                                 const char *msg) {
  /* Report row warning if invalid rule */
  push_warning_printf(m_thd, Sql_condition::SL_WARNING, ER_OUTLINE_INVALID,
                      ER_THD(m_thd, ER_OUTLINE_INVALID), record->get_id(), when,
                      msg);
}

/**
  Save the row value into outline_record structure.

  @param[out]   record      Outline record
*/
void Outline_reader::read_attributes(Conf_record *r) {
  DBUG_ENTER("Outline_reader::read_attributes");
  Outline_record *record = dynamic_cast<Outline_record *>(r);
  assert(record);
  record->reset();
  record->id = m_table->field[MYSQL_OUTLINE_FIELD_ID]->val_int();
  record->schema = to_lex_cstring(
      get_field(m_mem_root, m_table->field[MYSQL_OUTLINE_FIELD_SCHEMA]));
  record->digest = to_lex_cstring(
      get_field(m_mem_root, m_table->field[MYSQL_OUTLINE_FIELD_DIGEST]));
  record->digest_text = to_lex_cstring(
      get_field(m_mem_root, m_table->field[MYSQL_OUTLINE_FIELD_DIGEST_TEXT]));
  record->type =
      to_outline_type(m_table->field[MYSQL_OUTLINE_FIELD_TYPE]->val_int());

  size_t scope_pos = m_table->field[MYSQL_OUTLINE_FIELD_SCOPE]->val_int();
  record->scope = outline_scopes[scope_pos - 1];

  record->state =
      to_outline_state(m_table->field[MYSQL_OUTLINE_FIELD_STATE]->val_int());

  record->pos = m_table->field[MYSQL_OUTLINE_FIELD_POSITION]->val_int();
  record->hint = to_lex_cstring(
      get_field(m_mem_root, m_table->field[MYSQL_OUTLINE_FIELD_HINT]));
  DBUG_VOID_RETURN;
}

/**
  Validate the optimizer outline, try parse it,
  remove it from container and report warning if invalid.

  @parma[in]      records       Outline records

  @retval         num           invalid optimizer outline count
*/
size_t Outline_reader::validate_optimizer_outline(Conf_records *records) {
  size_t num = 0;
  for (Conf_records::const_iterator it = records->cbegin();
       it != records->cend();) {
    Outline_record *r = dynamic_cast<Outline_record *>(*it);
    if (r->type == Outline_type::OPTIMIZER) {
      LEX_CSTRING str =
          to_lex_cstring(m_thd->strmake(r->hint.str, r->hint.length));
      if (!parse_optimizer_hint(m_thd, str)) {
        row_warning(r, "read outline", "parse optimizer hint error");
        records->erase(it);
        num++;
      } else
        it++;
    } else
      it++;
  }

  return num;
}

/**
  Reconstruct and Report error by adding handler error.

  @param[in]      errcode     handler error.
*/
void Outline_writer::print_ha_error(int errcode) {
  ha_error(ER_OUTLINE_TABLE_OP_FAILED, errcode);
}

/**
  Store the outline attributes into table->field

  @param[in]      record        the table row object
*/
void Outline_writer::store_attributes(const Conf_record *r) {
  DBUG_ENTER("Outline_writer::store_attributes");
  const Outline_record *record = dynamic_cast<const Outline_record *>(r);

  if (m_op_type == Conf_table_op::OP_INSERT) {
    restore_record(m_table, s->default_values);

    /* Outline schema */
    if (!blank(record->schema)) {
      m_table->field[MYSQL_OUTLINE_FIELD_SCHEMA]->store(
          record->schema.str, record->schema.length, system_charset_info);
      m_table->field[MYSQL_OUTLINE_FIELD_SCHEMA]->set_notnull();
    } else
      m_table->field[MYSQL_OUTLINE_FIELD_SCHEMA]->set_null();

    /* Outline digest */
    m_table->field[MYSQL_OUTLINE_FIELD_DIGEST]->store(
        record->digest.str, record->digest.length, system_charset_info);
    m_table->field[MYSQL_OUTLINE_FIELD_DIGEST]->set_notnull();

    /* Outline digest text */
    if (!blank(record->digest_text)) {
      m_table->field[MYSQL_OUTLINE_FIELD_DIGEST_TEXT]->store(
          record->digest_text.str, record->digest_text.length,
          system_charset_info);
      m_table->field[MYSQL_OUTLINE_FIELD_DIGEST_TEXT]->set_notnull();
    } else
      m_table->field[MYSQL_OUTLINE_FIELD_DIGEST_TEXT]->set_null();

    /* Outline type */
    longlong type_pos = static_cast<longlong>(record->type);
    m_table->field[MYSQL_OUTLINE_FIELD_TYPE]->store(
        outline_type_str[type_pos], strlen(outline_type_str[type_pos]),
        system_charset_info);

    /* Outline scope */
    assert(record->scope.str.str);
    m_table->field[MYSQL_OUTLINE_FIELD_SCOPE]->store(
        record->scope.str.str, strlen(record->scope.str.str),
        system_charset_info);

    /* Outline state */
    longlong state_pos = static_cast<longlong>(record->state);
    m_table->field[MYSQL_OUTLINE_FIELD_STATE]->store(
        &outline_state_str[state_pos], 1, system_charset_info);

    /* position */
    m_table->field[MYSQL_OUTLINE_FIELD_POSITION]->store((longlong)record->pos,
                                                        true);
    m_table->field[MYSQL_OUTLINE_FIELD_POSITION]->set_notnull();

    /* hint */
    assert(!blank(record->hint));
    m_table->field[MYSQL_OUTLINE_FIELD_HINT]->store(
        record->hint.str, record->hint.length, system_charset_info);
    m_table->field[MYSQL_OUTLINE_FIELD_HINT]->set_notnull();
  }
  DBUG_VOID_RETURN;
}

/**
  Store the record outline id into table->field[ID].

  @param[in]      record        the table row
*/
void Outline_writer::store_id(const Conf_record *r) {
  const Outline_record *record = dynamic_cast<const Outline_record *>(r);
  m_table->field[MYSQL_OUTLINE_FIELD_ID]->store(record->id, true);
}

/**
  Retrieve some from table->fields into record.

  @param[out]     record        the table row object
*/
void Outline_writer::retrieve_attr(Conf_record *) {}

/**
  Push row not found warning to client.

  @param[in]      record        table table row object
*/
void Outline_writer::row_not_found_warning(Conf_record *r) {
  Outline_record *record = dynamic_cast<Outline_record *>(r);
  /* Not found rule, push warning here */
  push_warning_printf(m_thd, Sql_condition::SL_WARNING, ER_OUTLINE_NOT_FOUND,
                      ER_THD(m_thd, ER_OUTLINE_NOT_FOUND), record->id, "table");
}
/**
  Open the outline table, report error if failed.*

  Attention:
  It didn't open attached transaction, so it must commit
  current transaction context when close outline table.

  Make sure it launched within main thread booting
  or statement that cause implicit commit.

  Report client error if failed.

  @param[in]      thd           Thread context
  @param[in]      table_list    Outline table
  @param[in]      write         read or write

  @retval         Conf_error
*/
Conf_error open_outline_table(THD *thd, TABLE_LIST_PTR &table_list,
                              bool write) {
  DBUG_ENTER("open_outline_table");

  if (open_conf_table(thd, table_list, OUTLINE_SCHEMA_NAME, OUTLINE_TABLE_NAME,
                      OUTLINE_TABLE_ALIAS, &outline_table_def, write))

    DBUG_RETURN(Conf_error::CONF_ER_TABLE_OP_ERROR);

  DBUG_RETURN(Conf_error::CONF_OK);
}

/**
  Commit the outline transaction.
  Call reload_outlines() if commit failed.

  @param[in]      thd           Thread context
  @param[in]      rollback      Rollback request
*/
bool outline_end_trans(THD *thd, bool rollback) {
  bool result;
  if ((result = conf_end_trans(thd, rollback))) reload_outlines(thd);
  return result;
}

static void outline_log_error(Conf_error err) {
  log_conf_error(ER_OUTLINE, err);
}

/**
  Init the statement outlines when mysqld reboot

  It should log error message if failed, reported client error
  will be ingored.

  @param[in]      bootstrap     Whether initialize or restart.
*/
void statement_outline_init(bool bootstrap) {
  Conf_error error;
  DBUG_ENTER("statement_outline_init");

  if (bootstrap) DBUG_VOID_RETURN;

  /* When reboot, we need to create new THD to read rules */
  THD *orig_thd = current_thd;

  THD *thd = new THD;
  thd->thread_stack = pointer_cast<char *>(&thd);
  thd->store_globals();
  lex_start(thd);

  error = reload_outlines(thd);

  lex_end(thd->lex);
  delete thd;
  if (orig_thd) orig_thd->store_globals();
  /* Log error message if any */
  outline_log_error(error);
  DBUG_VOID_RETURN;
}

/**
  Flush all outlines and load outlines from table.

  Report client error if failed, whether log error
  message decided by caller.

  @param[in]      thd           Thread context

  @retval         Conf_error
*/
Conf_error reload_outlines(THD *thd) {
  Conf_error error;
  TABLE_LIST_PTR table_list;
  Conf_records *records = nullptr;
  size_t num;
  DBUG_ENTER("reload_outlines");

  if ((error = open_outline_table(thd, table_list, false)) !=
      Conf_error::CONF_OK)
    DBUG_RETURN(error);

  assert(table_list->table);

  Outline_reader reader(thd, table_list->table, thd->mem_root);
  records = reader.read_all_rows(&error);

  if (error != Conf_error::CONF_OK) goto err_and_close;
  assert(records);

  num = reader.validate_optimizer_outline(records);

  if (num > 0) {
    outline_log_error(Conf_error::CONF_ER_RECORD);
  }

  refresh_outline_cache(records, true);

err_and_close:
  commit_and_close_conf_table(thd);
  destroy(records);
  DBUG_RETURN(error);
}

/**
  Add new outline into outline table and insert outline cache.
  Report client error if failed.

  @param[in]      thd         Thread context
  @param[in]      record      outline

  @retval         false       Success
  @retval         true        Failure
*/
bool add_outline(THD *thd, Conf_record *r) {
  Conf_error error;
  TABLE_LIST_PTR table_list;
  Conf_records records(key_memory_outline);
  DBUG_ENTER("add_outline");

  if ((error = open_outline_table(thd, table_list, true)) !=
      Conf_error::CONF_OK)
    DBUG_RETURN(true);

  assert(table_list->table);

  Outline_writer writer(thd, table_list->table, thd->mem_root,
                        Conf_table_op::OP_INSERT);

  if (writer.write_row(r)) goto err;

  /* Only write into table if not active rule */
  if (!(r->check_active())) goto end;

  records.push_back(r);
  System_outline::instance()->fill_records(&records, false);

end:
  outline_end_trans(thd, false);
  DBUG_RETURN(false);

err:
  outline_end_trans(thd, true);
  DBUG_RETURN(true);
}

/**
  Delete the outline.

  Only report warning message if row or cache not found
*/
bool del_outline(THD *thd, Conf_record *record) {
  Conf_error error;
  TABLE_LIST_PTR table_list;
  size_t num;
  DBUG_ENTER("del_outline");

  if ((error = open_outline_table(thd, table_list, true)) !=
      Conf_error::CONF_OK)
    DBUG_RETURN(true);

  Outline_writer writer(thd, table_list->table, thd->mem_root,
                        Conf_table_op::OP_DELETE);

  if (writer.delete_row_by_id(record)) goto err;

  num = System_outline::instance()->delete_outline(record->get_id());

  if (num < System_outline::instance()->get_partition()) {
    push_warning_printf(thd, Sql_condition::SL_WARNING, ER_OUTLINE_NOT_FOUND,
                        ER_THD(thd, ER_OUTLINE_NOT_FOUND), record->get_id(),
                        "cache");
  }

  outline_end_trans(thd, false);
  DBUG_RETURN(false);

err:
  outline_end_trans(thd, true);
  DBUG_RETURN(true);
}

} /* namespace im */
