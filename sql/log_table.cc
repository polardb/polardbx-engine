/* Copyright (c) 2000, 2019, Alibaba and/or its affiliates. All rights reserved.
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

#include "sql/dd/cache/dictionary_client.h"
#include "sql/dd/dd_schema.h"
#include "sql/dd/dd_table.h"
#include "sql/dd/types/abstract_table.h"  // dd::enum_table_type
#include "sql/dd/types/table.h"           // dd::Table
#include "sql/dd_table_share.h"           // open_table_def
#include "sql/handler.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/sql_table.h"
#include "sql/table.h"

#include "sql/log_table.h"

namespace im {

/**
  Last rotated file name.

  It will display './DATABASE/FILE.EXT' format name
  when show global variables, and it is not protected
  by any lock, we think it is safe even though there
  will may be concurrent update.
*/
char rotate_log_table_last_name[FN_REFLEN] = {0};

char *rotate_log_table_last_name_ptr = rotate_log_table_last_name;

void update_rotate_log_table_last_name_ptr() {
  rotate_log_table_last_name_ptr = rotate_log_table_last_name;
}

/**
  Validate and backup current context.

  @retval     true        Failure
  @retval     false       Success
*/
bool Log_table::validate_and_backup_context() {
  /**
    Report error if THD is in:
     - Locked tables
     - Active transaction
     - Stored function or trigger
  */

  /**
    Never reached active transaction, because FLUSH command
    will trigger implicit commit earlier than here.
  */
  if (m_thd->locked_tables_mode || m_thd->in_active_multi_stmt_transaction()) {
    my_error(ER_LOCK_OR_ACTIVE_TRANSACTION, MYF(0));
    return true;
  }

  /* Never reached here, it will report error early if FLUSH command */
  if (m_thd->in_sub_stmt) {
    my_error(ER_STMT_NOT_ALLOWED_IN_SF_OR_TRG, MYF(0), "FLUSH");
    return true;
  }
  /**
    Now only support Log tables:
      - Slow_log
      - General_log
  */
  if (m_log_table_type == QUERY_LOG_NONE) {
    my_error(ER_ROTATE_REQUIRE_LOG_TABLE, MYF(0));
    return true;
  }

  assert(!(m_thd->server_status & SERVER_STATUS_IN_TRANS));

  /* Backup and override current context. */
  m_ctx_state.m_thd_option_bits = m_thd->variables.option_bits;
  m_thd->reset_n_backup_open_tables_state(&m_ctx_state.m_open_tables_state,
                                          Open_tables_state::SYSTEM_TABLES);
  m_thd->variables.option_bits &= ~OPTION_BIN_LOG;
  m_backup = true;
  return false;
}

/* Destructor */
Log_table::~Log_table() {
  /* Restore current context. */
  if (m_backup) {
    m_thd->variables.option_bits = m_ctx_state.m_thd_option_bits;
    m_thd->restore_backup_open_tables_state(&m_ctx_state.m_open_tables_state);
  }
}

/**
  Lock the target log table, report error if failed.

  @retval     true        Failure
  @retval     false       Success
*/
bool Log_table::lock_log_table() {
  const char *table_name = nullptr;
  const char *schema_name = nullptr;
  uint table_length = 0;
  uint schema_length = 0;
  DBUG_ENTER("Log_table::lock_log_table");
  assert(m_log_table_type != QUERY_LOG_NONE);

  schema_name = MYSQL_SCHEMA_NAME.str;
  schema_length = MYSQL_SCHEMA_NAME.length;

  if (m_log_table_type == QUERY_LOG_SLOW) {
    table_name = SLOW_LOG_NAME.str;
    table_length = SLOW_LOG_NAME.length;
  } else if (m_log_table_type == QUERY_LOG_GENERAL) {
    table_name = GENERAL_LOG_NAME.str;
    table_length = GENERAL_LOG_NAME.length;
  } else {
    assert(0);
  }

  m_table_list = new (m_thd->mem_root)
      Table_ref(schema_name, schema_length, table_name, table_length,
                table_name, TL_WRITE, MDL_EXCLUSIVE);

  m_table_list->open_strategy = Table_ref::OPEN_IF_EXISTS;
  m_table_list->open_type = OT_BASE_ONLY;

  assert(m_table_list->next_global == NULL);

  /**
    Lock the table include:
     - Global read lock
     - Schema lock
     - Table lock
  */
  if (lock_table_names(m_thd, m_table_list, NULL,
                       m_thd->variables.lock_wait_timeout, 0))
    DBUG_RETURN(true);

  dd::cache::Dictionary_client::Auto_releaser releaser(m_thd->dd_client());
  const dd::Table *table_def = nullptr;

  if (m_thd->dd_client()->acquire(schema_name, table_name, &table_def)) {
    // Report error in DD subsystem.
    DBUG_RETURN(true);
  }

  // Report no such table error.
  if (table_def == nullptr ||
      table_def->hidden() == dd::Abstract_table::HT_HIDDEN_SE) {
    my_error(ER_NO_SUCH_TABLE, MYF(0), schema_name, table_name);
    DBUG_RETURN(true);
  }

  /* Table is already locked exclusively. */
  tdc_remove_table(m_thd, TDC_RT_REMOVE_ALL, m_table_list->db,
                   m_table_list->table_name, false);

  DBUG_RETURN(false);
}

/**
  Rotate the target log table data file.

  @retval     true        Failure
  @retval     false       Success
*/
bool Log_table::rotate_table() {
  int error = 1;
  DBUG_ENTER("Log_table::rotate_table");

  /* Lock the table exclusively */
  if (lock_log_table()) DBUG_RETURN(true);

  dd::cache::Dictionary_client::Auto_releaser releaser(m_thd->dd_client());
  const dd::Table *table_def = NULL;
  if (m_thd->dd_client()->acquire(m_table_list->db, m_table_list->table_name,
                                  &table_def)) {
    // Error is reported by the dictionary subsystem.
    DBUG_RETURN(true);
  }
  assert(table_def);

  TABLE table_arg;
  TABLE_SHARE share;
  HA_CREATE_INFO create_info;
  char path[FN_REFLEN + 1];
  char name_buff[FN_REFLEN + 1];
  const char *name;
  build_table_filename(path, sizeof(path) - 1, m_table_list->db,
                       m_table_list->table_name, "", 0);

  init_tmp_table_share(m_thd, &share, m_table_list->db, 0,
                       m_table_list->table_name, path, nullptr);

  if (open_table_def(m_thd, &share, *table_def)) goto err;

  destroy(&table_arg);
  if (open_table_from_share(m_thd, &share, "", 0, (uint)READ_ALL, 0, &table_arg,
                            false, nullptr))
    goto err;

  name = get_canonical_filename(table_arg.file, share.path.str, name_buff);

  error = table_arg.file->ha_rotate_table(name, table_def);
  if (error) {
    table_arg.file->print_error(error, MYF(0));
  }

  (void)closefrm(&table_arg, 0);

err:
  free_table_share(&share);
  DBUG_RETURN(error != 0);
}

/**
  Rotate log table.

  @param[in]      thd               current thread
  @param[in]      log_table_type    Slow_log or general_log

  @retval     true        Failure
  @retval     false       Success
*/
bool rotate_log_table(THD *thd, enum_log_table_type log_table_type) {
  DBUG_ENTER("rotate_log_table");
  Log_table log_table(thd, log_table_type);

  if (log_table.validate_and_backup_context()) DBUG_RETURN(true);

  DBUG_RETURN(log_table.rotate_table());
}

} /* namespace im */

/**
  Handler interface implementation

  Putting here is in order to decrease possibility of conflict.
*/
int handler::ha_rotate_table(const char *name, const dd::Table *table_def) {
  assert(table_share->tmp_table != NO_TMP_TABLE || m_lock_type == F_WRLCK);
  mark_trx_read_write();

  return (rotate_table(name, table_def));
}

/**
  Rotate the log table.

  @param[in]      thd       current thread
  @param[in]      log_type  log table type

  @retval         true      failure
  @retval         false     success
*/
bool Query_logger::rotate_log_table(THD *thd, enum_log_table_type log_type) {
  bool res = false;
  /* Now only support slow log rotate */
  assert(log_type == QUERY_LOG_SLOW);

  if (thd->variables.rotate_log_table && is_log_table_enabled(log_type)) {
    res = im::rotate_log_table(thd, log_type);
  }

  return res;
}
