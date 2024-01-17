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

#include "sql/ccl/ccl.h"
#include "sql/ccl/ccl_cache.h"
#include "sql/ccl/ccl_table.h"
#include "sql/ccl/ccl_table_common.h"

#include "sql/common/table.h"
#include "sql/common/table_common.h"

namespace im {

/* ccl table schema name */
LEX_CSTRING CCL_SCHEMA_NAME = {C_STRING_WITH_LEN("mysql")};

/* ccl table name */
LEX_CSTRING CCL_TABLE_NAME = {C_STRING_WITH_LEN("concurrency_control")};

const char *CCL_TABLE_ALIAS = "concurrency_control";

/* Whether the table is concurrency_control */
bool is_ccl_table(const char *table_name, size_t len) {
  if (len == CCL_TABLE_NAME.length &&
      my_strcasecmp(system_charset_info, CCL_TABLE_NAME.str, table_name))
    return true;
  return false;
}
/* Table "mysql.concurrency_control" definition */
static const TABLE_FIELD_TYPE mysql_ccl_table_fields[MYSQL_CCL_FIELD_COUNT] = {
    {{C_STRING_WITH_LEN("Id")}, {C_STRING_WITH_LEN("bigint")}, {NULL, 0}},
    {{C_STRING_WITH_LEN("Type")},
     {C_STRING_WITH_LEN("enum('SELECT','UPDATE','INSERT','DELETE')")},
     {C_STRING_WITH_LEN("utf8mb3")}},
    {{C_STRING_WITH_LEN("Schema_name")},
     {C_STRING_WITH_LEN("varchar(64)")},
     {NULL, 0}},
    {{C_STRING_WITH_LEN("Table_name")},
     {C_STRING_WITH_LEN("varchar(64)")},
     {NULL, 0}},
    {{C_STRING_WITH_LEN("Concurrency_count")},
     {C_STRING_WITH_LEN("bigint")},
     {NULL, 0}},
    {{C_STRING_WITH_LEN("Keywords")}, {C_STRING_WITH_LEN("text")}, {NULL, 0}},
    {{C_STRING_WITH_LEN("State")},
     {C_STRING_WITH_LEN("enum('N','Y')")},
     {C_STRING_WITH_LEN("utf8mb3")}},
    {{C_STRING_WITH_LEN("Ordered")},
     {C_STRING_WITH_LEN("enum('N','Y')")},
     {C_STRING_WITH_LEN("utf8mb3")}}};

static const TABLE_FIELD_DEF ccl_table_def = {MYSQL_CCL_FIELD_COUNT,
                                              mysql_ccl_table_fields};

const char *ccl_rule_type_str[] = {"SELECT", "UPDATE", "INSERT", "DELETE",
                                   "QUEUE"};
const char ccl_rule_state_str[] = {'N', 'Y'};
const char ccl_rule_ordered_str[] = {'N', 'Y'};

/**
  Open the ccl table, report error if failed.*

  Attention:
  It didn't open attached transaction, so it must commit
  current transaction context when close ccl table.

  Make sure it launched within main thread booting
  or statement that cause implicit commit.

  Report client error if failed.

  @param[in]      thd           Thread context
  @param[in]      table_list    CCL table
  @param[in]      write         read or write

  @retval         Conf_error
*/
Conf_error open_ccl_table(THD *thd, TABLE_LIST_PTR &table_list, bool write) {
  DBUG_ENTER("open_ccl_table");
  if (open_conf_table(thd, table_list, CCL_SCHEMA_NAME, CCL_TABLE_NAME,
                      CCL_TABLE_ALIAS, &ccl_table_def, write))

    DBUG_RETURN(Conf_error::CONF_ER_TABLE_OP_ERROR);

  DBUG_RETURN(Conf_error::CONF_OK);
}

/**
  Commit the ccl transaction.
  Call reload_ccl_rules() if commit failed.

  @param[in]      thd           Thread context
  @param[in]      rollback      Rollback request
*/
bool ccl_end_trans(THD *thd, bool rollback) {
  bool result;
  if ((result = conf_end_trans(thd, rollback))) reload_ccl_rules(thd);
  return result;
}
/**
  Push invalid ccl record warning

  @param[in]      record        Ccl record
  @param[in]      when          Operation
*/
void Ccl_reader::row_warning(Conf_record *record, const char *when,
                             const char *) {
  push_warning_printf(m_thd, Sql_condition::SL_WARNING, ER_CCL_INVALID_RULE,
                      ER_THD(m_thd, ER_CCL_INVALID_RULE), record->get_id(),
                      when);
}

/**
  Reconstruct and Report error by adding handler error.

  @param[in]      errcode     handler error.
*/
void Ccl_reader::print_ha_error(int errcode) {
  ha_error(ER_CCL_TABLE_OP_FAILED, errcode);
}
/**
  Log the conf table error.

  @param[in]    err     Conf error type
*/
void Ccl_reader::log_error(Conf_error err) { log_conf_error(ER_CCL, err); }

/* Create new ccl record */
Conf_record *Ccl_reader::new_record() { return new (m_mem_root) Ccl_record(); }

/**
  Save the row value into ccl_record structure.
*/
void Ccl_reader::read_attributes(Conf_record *r) {
  DBUG_ENTER("Ccl_reader::read_attributes");
  Ccl_record *record = dynamic_cast<Ccl_record *>(r);
  assert(record);
  record->reset();
  record->id = m_table->field[MYSQL_CCL_FIELD_ID]->val_int();
  record->type = to_ccl_type(m_table->field[MYSQL_CCL_FIELD_TYPE]->val_int());
  record->schema_name =
      get_field(m_mem_root, m_table->field[MYSQL_CCL_FIELD_SCHEMA_NAME]);
  record->table_name =
      get_field(m_mem_root, m_table->field[MYSQL_CCL_FIELD_TABLE_NAME]);
  record->concurrency_count =
      m_table->field[MYSQL_CCL_FIELD_CONCURRENCY_COUNT]->val_int();
  record->keywords =
      get_field(m_mem_root, m_table->field[MYSQL_CCL_FIELD_KEYWORDS]);
  record->state =
      to_ccl_state(m_table->field[MYSQL_CCL_FIELD_STATE]->val_int());
  record->ordered =
      to_ccl_ordered(m_table->field[MYSQL_CCL_FIELD_ORDERED]->val_int());

  DBUG_VOID_RETURN;
}
/**
  Reconstruct and Report error by adding handler error.

  @param[in]      errcode     handler error.
*/
void Ccl_writer::print_ha_error(int errcode) {
  ha_error(ER_CCL_TABLE_OP_FAILED, errcode);
}

/**
  Store the rule attributes into table->field

  @param[in]      record        the table row object
*/
void Ccl_writer::store_attributes(const Conf_record *r) {
  const Ccl_record *record = dynamic_cast<const Ccl_record *>(r);

  if (m_op_type == Conf_table_op::OP_INSERT) {
    restore_record(m_table, s->default_values);

    /* rule type */
    longlong rule_type_pos = static_cast<longlong>(record->type);
    m_table->field[MYSQL_CCL_FIELD_TYPE]->store(
        ccl_rule_type_str[rule_type_pos],
        strlen(ccl_rule_type_str[rule_type_pos]), system_charset_info);
    /* rule schema_name */
    if (!blank(record->schema_name)) {
      m_table->field[MYSQL_CCL_FIELD_SCHEMA_NAME]->store(
          record->schema_name, strlen(record->schema_name),
          system_charset_info);
      m_table->field[MYSQL_CCL_FIELD_SCHEMA_NAME]->set_notnull();
    } else
      m_table->field[MYSQL_CCL_FIELD_SCHEMA_NAME]->set_null();

    /* rule table_name*/
    if (!blank(record->table_name)) {
      m_table->field[MYSQL_CCL_FIELD_TABLE_NAME]->store(
          record->table_name, strlen(record->table_name), system_charset_info);
      m_table->field[MYSQL_CCL_FIELD_TABLE_NAME]->set_notnull();
    } else
      m_table->field[MYSQL_CCL_FIELD_TABLE_NAME]->set_null();

    /* rule concurrency count */
    m_table->field[MYSQL_CCL_FIELD_CONCURRENCY_COUNT]->store(
        (longlong)record->concurrency_count, true);
    m_table->field[MYSQL_CCL_FIELD_CONCURRENCY_COUNT]->set_notnull();

    /* rule keywords */
    if (!blank(record->keywords)) {
      m_table->field[MYSQL_CCL_FIELD_KEYWORDS]->store(
          record->keywords, strlen(record->keywords), system_charset_info);
      m_table->field[MYSQL_CCL_FIELD_KEYWORDS]->set_notnull();
    } else
      m_table->field[MYSQL_CCL_FIELD_KEYWORDS]->set_null();

    longlong rule_state_pos = static_cast<longlong>(record->state);
    m_table->field[MYSQL_CCL_FIELD_STATE]->store(
        &ccl_rule_state_str[rule_state_pos], 1, system_charset_info);

    m_table->field[MYSQL_CCL_FIELD_STATE]->set_notnull();

    longlong rule_ordered_pos = static_cast<longlong>(record->ordered);
    m_table->field[MYSQL_CCL_FIELD_ORDERED]->store(
        &ccl_rule_ordered_str[rule_ordered_pos], 1, system_charset_info);
    m_table->field[MYSQL_CCL_FIELD_ORDERED]->set_notnull();
  }
}
/**
  Store the record rule id into table->field[ID].

  @param[in]      record        the table row
*/
void Ccl_writer::store_id(const Conf_record *r) {
  const Ccl_record *record = dynamic_cast<const Ccl_record *>(r);
  m_table->field[MYSQL_CCL_FIELD_ID]->store(record->id, true);
}
/**
  Retrieve ccl type from table->fields into record.

  @param[out]     record        the table row object
*/
void Ccl_writer::retrieve_attr(Conf_record *r) {
  Ccl_record *record = dynamic_cast<Ccl_record *>(r);
  record->type = to_ccl_type(m_table->field[MYSQL_CCL_FIELD_TYPE]->val_int());
}
/**
  Push row not found warning to client.

  @param[in]      record        table table row object
*/
void Ccl_writer::row_not_found_warning(Conf_record *r) {
  Ccl_record *record = dynamic_cast<Ccl_record *>(r);
  /* Not found rule, push warning here */
  push_warning_printf(m_thd, Sql_condition::SL_WARNING, ER_CCL_RULE_NOT_FOUND,
                      ER_THD(m_thd, ER_CCL_RULE_NOT_FOUND), record->id,
                      "table");
}
/**
  Init the rules when mysqld reboot

  It should log error message if failed, reported client error
  will be ingored.

  @param[in]      bootstrap     Whether initialize or restart.
*/
void ccl_rules_init(bool bootstrap) {
  Conf_error error;
  DBUG_ENTER("Ccl_rules_init");

  if (bootstrap) DBUG_VOID_RETURN;

  /* When reboot, we need to create new THD to read rules */
  THD *orig_thd = current_thd;

  THD *thd = new THD;
  thd->thread_stack = pointer_cast<char *>(&thd);
  thd->store_globals();
  lex_start(thd);

  ccl_queue_buckets_init(ccl_queue_bucket_count, ccl_queue_bucket_size);

  error = reload_ccl_rules(thd);

  lex_end(thd->lex);
  delete thd;
  if (orig_thd) orig_thd->store_globals();
  /* Log error message if any */
  ccl_log_error(error);
  DBUG_VOID_RETURN;
}

/**
  Flush all rules and load rules from table.

  Report client error if failed, whether log error
  message decided by caller.

  Log warning message if the number of successful loading into cache
  is not equal to records in table.

  @param[in]      thd           Thread context

  @retval         Ccl_error
*/
Conf_error reload_ccl_rules(THD *thd) {
  Conf_error error;
  size_t num;
  TABLE_LIST_PTR table_list;
  Conf_records *records = nullptr;
  DBUG_ENTER("reload_ccl_rules");

  if ((error = open_ccl_table(thd, table_list, false)) != Conf_error::CONF_OK)
    DBUG_RETURN(error);

  assert(table_list->table);

  Ccl_reader reader(thd, table_list->table, thd->mem_root);
  records = reader.read_all_rows(&error);

  if (error != Conf_error::CONF_OK) goto err_and_close;

  assert(records);

  /** Here only push warning in refresh_ccl_cache. */
  num = refresh_ccl_cache(records, true);
  if (num != records->size()) {
    std::stringstream ss;
    ss << "it should load " << records->size() << " rules, actually " << num
       << " rules.";
    LogErr(WARNING_LEVEL, ER_CCL, ss.str().c_str());
  }
err_and_close:
  commit_and_close_conf_table(thd);
  destroy(records);
  DBUG_RETURN(error);
}

/**
  Add new rule into concurrency_table and insert ccl cache.
  Report client error if failed.

  @param[in]      thd         Thread context
  @param[in]      record      Rule

  @retval         false       Success
  @retval         true        Failure
*/
bool add_ccl_rule(THD *thd, Conf_record *r) {
  Conf_error error;
  size_t num;
  TABLE_LIST_PTR table_list;
  Conf_records records(key_memory_ccl);
  Ccl_record_group group;
  Ccl_record *record;

  DBUG_ENTER("add_ccl_rule");

  if ((error = open_ccl_table(thd, table_list, true)) != Conf_error::CONF_OK)
    DBUG_RETURN(true);

  Ccl_writer writer(thd, table_list->table, thd->mem_root,
                    Conf_table_op::OP_INSERT);

  if (writer.write_row(r)) goto err;

  /* Only write into table if not active rule */
  if (!(r->check_active())) goto end;

  /* Personalized process ccl rule cache  */
  record = dynamic_cast<Ccl_record *>(r);
  records.push_back(record);
  num = group.classify_rule(record->type, &records);

  if (num != 1) {
    my_error(ER_CCL_INVALID_RULE, MYF(0), 0, "add rule");
    goto err;
  }
  /* Here must report client error if some rule failed. */
  if ((num = System_ccl::instance()->refresh_rules(
           group, record->type, false, Ccl_error_level::CCL_CRITICAL)) !=
      group.size())
    goto err;

end:
  ccl_end_trans(thd, false);
  DBUG_RETURN(false);

err:
  ccl_end_trans(thd, true);
  DBUG_RETURN(true);
}

/**
  Delete the rule.
  Delete the row or clear the rule cache or both.

  Only report warning message if row or cache not found
*/
bool del_ccl_rule(THD *thd, Conf_record *record) {
  Conf_error error;
  TABLE_LIST_PTR table_list;
  size_t num;
  DBUG_ENTER("del_ccl_rule");

  if ((error = open_ccl_table(thd, table_list, true)) != Conf_error::CONF_OK)
    DBUG_RETURN(true);

  Ccl_writer writer(thd, table_list->table, thd->mem_root,
                    Conf_table_op::OP_DELETE);

  if (writer.delete_row_by_id(record)) goto err;

  num = System_ccl::instance()->delete_rule(record->get_id());
  if (num < 1) {
    push_warning_printf(thd, Sql_condition::SL_WARNING, ER_CCL_RULE_NOT_FOUND,
                        ER_THD(thd, ER_CCL_RULE_NOT_FOUND), record->get_id(),
                        "cache");
  }
  ccl_end_trans(thd, false);
  DBUG_RETURN(false);

err:
  ccl_end_trans(thd, true);
  DBUG_RETURN(true);
}
} /* namespace im */
