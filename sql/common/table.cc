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

#include "sql/common/table.h"

#include "lex_string.h"
#include "m_string.h"
#include "my_base.h"
#include "my_dbug.h"
#include "mysql/components/services/log_builtins.h"
#include "mysql/components/services/log_shared.h"
#include "sql/handler.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/sql_executor.h"
#include "sql/transaction.h"

namespace im {

/* Implement the report error interface */
void Conf_table_intact::report_error(uint ecode, const char *fmt, ...) {
  longlong log_ecode = 0;
  switch (ecode) {
    case 0:
      log_ecode = ER_SERVER_TABLE_CHECK_FAILED;
      break;
    case ER_CANNOT_LOAD_FROM_TABLE_V2:
      log_ecode = ER_SERVER_CANNOT_LOAD_FROM_TABLE_V2;
      break;
    case ER_COL_COUNT_DOESNT_MATCH_PLEASE_UPDATE_V2:
      log_ecode = ER_SERVER_COL_COUNT_DOESNT_MATCH_PLEASE_UPDATE_V2;
      break;
    case ER_COL_COUNT_DOESNT_MATCH_CORRUPTED_V2:
      log_ecode = ER_SERVER_COL_COUNT_DOESNT_MATCH_CORRUPTED_V2;
      break;
    default:
      assert(false);
      return;
  }
  va_list args;
  va_start(args, fmt);
  LogEvent()
      .type(LOG_TYPE_ERROR)
      .prio(ERROR_LEVEL)
      .errcode(log_ecode)
      .messagev(fmt, args);
  va_end(args);
}

/**
  Open a configure table, and check the definition.

  Report client error if failed.

  @param[in]      thd           Thread context
  @param[in]      table_list    CONF table
  @param[in]      schema        Schema name
  @param[in]      table         Table name
  @param[in]      alias         Table alias name
  @param[in]      write         Read or write

  @retval         false         Success
  @retval         true          Failure
*/
bool open_conf_table(THD *thd, TABLE_LIST_PTR &table_list,
                     const LEX_CSTRING &schema, const LEX_CSTRING &table,
                     const char *alias, const TABLE_FIELD_DEF *def,
                     bool write) {
  enum thr_lock_type lock_type = TL_READ;
  enum enum_mdl_type mdl_type = MDL_SHARED_READ_ONLY;
  Conf_table_intact table_intact(thd);
  Table_ref *tbl;
  DBUG_ENTER("open_conf_table");

  if (write) {
    lock_type = TL_WRITE;
    mdl_type = MDL_SHARED_NO_READ_WRITE;
  }
  tbl = new Table_ref(schema.str, schema.length, table.str, table.length, alias,
                      lock_type, mdl_type);
  table_list.reset(tbl);

  if (open_and_lock_tables(thd, table_list.get(), MYSQL_LOCK_IGNORE_TIMEOUT))
    DBUG_RETURN(true);

  assert(table_list->table);

  /* Check the conf table engine */
  if (!(table_list->table->file->ht->is_supported_system_table(
          table_list->db, table_list->table_name, true))) {
    my_error(ER_UNSUPPORTED_ENGINE, MYF(0),
             ha_resolve_storage_engine_name(table_list->table->file->ht),
             table_list->db, table_list->table_name);
    goto error_and_close;
  }

  /* Check the conf table definition */
  if (table_intact.check(thd, table_list->table, def)) {
    my_error(ER_CANNOT_LOAD_FROM_TABLE_V2, MYF(0), schema.str, table.str);
    goto error_and_close;
  }
  table_list->table->open_by_handler = 1;
  DBUG_RETURN(false);

error_and_close:
  commit_and_close_conf_table(thd);
  DBUG_RETURN(true);
}

/**
  Commit current transaction, close the opened tables
  release the MDL transactional locks.

  @param[in]      thd           Thread context
*/
void commit_and_close_conf_table(THD *thd) {
  trans_commit_stmt(thd);
  trans_commit_implicit(thd);
  close_mysql_tables(thd);
  thd->mdl_context.release_transactional_locks();
}

/**
  Commit the conf transaction.

  @param[in]      thd           Thread context
  @param[in]      rollback      Rollback request
*/
bool conf_end_trans(THD *thd, bool rollback) {
  bool result;

  if (rollback) {
    result = trans_rollback_stmt(thd);
    result |= trans_rollback_implicit(thd);
  } else {
    result = trans_commit_stmt(thd);
    result |= trans_commit_implicit(thd);
  }
  close_mysql_tables(thd);

  thd->mdl_context.release_transactional_locks();
  return result;
}

/**
  Reconstruct error by handler error.

  @param[in]    nr          Redefine error code
  @param[in]    ha_error    error number from SE.
*/
void ha_error(int nr, int ha_error) {
  char buffer[MYSYS_ERRMSG_SIZE];

  my_strerror(buffer, sizeof(buffer), ha_error);
  my_error(nr, MYF(0), ha_error, buffer);
}

/**
  Log the error string by conf error type.
*/
void log_conf_error(int nr, Conf_error err) {
  switch (err) {
    case Conf_error::CONF_ER_TABLE_OP_ERROR: {
      LogErr(ERROR_LEVEL, nr, "table operation failed");
      break;
    }
    case Conf_error::CONF_ER_RECORD: {
      LogErr(ERROR_LEVEL, nr, "some records are invalid");
      break;
    }
    case Conf_error::CONF_OK:
      break;
  }
}

/**
  Setup table reader context, report error if failed.

  @retval         true        Failure
  @retval         false       Success
*/
bool Conf_reader::setup_table() {
  DBUG_ENTER("Conf_reader::setup_table");
  m_read_record_info = init_table_iterator(m_thd, m_table, false, false);
  if (m_read_record_info == nullptr) DBUG_RETURN(true);

  m_table->use_all_columns();
  DBUG_RETURN(false);
}

/**
  Read all rows from table.

  @param[out]     error     Conf error

  @retval         records   all records object from table rows
*/
Conf_records *Conf_reader::read_all_rows(Conf_error *error) {
  int errcode;
  Conf_records *container;
  DBUG_ENTER("Conf_reader::read_all_rows");
  *error = Conf_error::CONF_OK;

  if (setup_table()) goto err;

  container = new (m_mem_root) Conf_records(PSI_NOT_INSTRUMENTED);
  while (!(errcode = m_read_record_info->Read())) {
    Conf_record *record = new_record();
    read_attributes(record);

    const char *msg = "unknown";
    if ((record->check_valid(&msg))) {
      /* Must be active record, or ignore it */
      if (record->check_active()) container->push_back(record);
    } else {
      /* Report warning and log error if invalid record */
      row_warning(record, "read all rows", msg);
      log_error(Conf_error::CONF_ER_RECORD);
    }
  }
  /* Release */
  m_read_record_info.reset(0);
  if (errcode > 0) {
    print_ha_error(errcode);
    goto err;
  }
  DBUG_RETURN(container);
err:
  *error = Conf_error::CONF_ER_TABLE_OP_ERROR;
  DBUG_RETURN(nullptr);
}

/**
  Setup table writer context.
*/
void Conf_writer::setup_table() {
  DBUG_ENTER("Conf_writer::setup_table");
  m_table->use_all_columns();
  if (has_autoinc() && m_op_type == Conf_table_op::OP_INSERT)
    m_table->next_number_field = m_table->found_next_number_field;

  DBUG_VOID_RETURN;
}

/**
  Write the record into table.

  @param[in]      record      row

  @retval         error number
*/
int Conf_writer::write_row(Conf_record *record) {
  int error;
  DBUG_ENTER("Conf_writer::write_row");
  setup_table();
  store_attributes(record);
  error = m_table->file->ha_write_row(m_table->record[0]);
  if (error) {
    if (!m_table->file->is_ignorable_error(error)) {
      print_ha_error(error);
      DBUG_RETURN(error);
    }
  }

  if (has_autoinc()) {
    /* set auto inc id */
    record->set_id(m_table->next_number_field->val_int());
    m_table->file->ha_release_auto_increment();
    m_table->next_number_field = nullptr;
  }
  DBUG_RETURN(0);
}

/**
  Delete the record from table.
  Only push warning if not found the record in table.

  @param[in/out]  record        the row

  @retval         false         Success
  @retval         true          Failure
*/
bool Conf_writer::delete_row_by_id(Conf_record *record) {
  int error;
  uchar user_key[MAX_KEY_LENGTH];
  DBUG_ENTER("Conf_writer::delete_row_by_id");
  setup_table();
  /* Save record id into table->field */
  store_id(record);
  /* Promise the id column has first key */
  key_copy(user_key, m_table->record[0], m_table->key_info,
           m_table->key_info->key_length);
  error = m_table->file->ha_index_read_idx_map(m_table->record[0], 0, user_key,
                                               HA_WHOLE_KEY, HA_READ_KEY_EXACT);

  if (error) {
    if (error != HA_ERR_KEY_NOT_FOUND && error != HA_ERR_END_OF_FILE) {
      print_ha_error(error);
      DBUG_RETURN(true);
    }
    row_not_found_warning(record);
    DBUG_RETURN(false);
  } else {  // exist row
    /* Save some attributes from index read */
    retrieve_attr(record);
    store_record(m_table, record[1]);
    error = m_table->file->ha_delete_row(m_table->record[1]);
    if (error) {
      print_ha_error(error);
      DBUG_RETURN(true);
    }
    DBUG_RETURN(false);
  }
}

} /* namespace im */
