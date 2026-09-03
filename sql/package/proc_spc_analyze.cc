/* Copyright (c) 2026, Alibaba and/or its affiliates. All rights reserved.

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

#include "sql/package/proc_spc_analyze.h"

#include "sql/auth/auth_acls.h"
#include "sql/handler.h"
#include "sql/protocol.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/table.h"
#include "thr_lock.h"

namespace im {

/* Schema: dbms_spc */
LEX_CSTRING SPC_PROC_SCHEMA = {C_STRING_WITH_LEN("dbms_spc")};

/* Singleton instance */
Proc *Proc_spc_analyze_table::instance() {
  static Proc *proc = new Proc_spc_analyze_table(key_memory_package);
  return proc;
}

Sql_cmd *Proc_spc_analyze_table::invoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_proc_spc_analyze_table(thd, list, this);
}

bool Sql_cmd_proc_spc_analyze_table::pc_execute(THD *thd) {
  DBUG_TRACE;

  /* Check parameters - expect exactly 1 parameter (table name) */
  if (m_list->size() != 1) {
    my_error(ER_WRONG_PARAMETERS_TO_PROCEDURE, MYF(0),
             "dbms_spc.analyze_table, Expected 1 parameter (table_name)");
    return true;
  }

  /* Get the table name parameter */
  Item *table_item = m_list->front();
  if (table_item->type() != Item::STRING_ITEM) {
    my_error(ER_WRONG_PARAMETERS_TO_PROCEDURE, MYF(0),
             "dbms_spc.analyze_table, table_name must be a string");
    return true;
  }

  String table_name_buf;
  String *table_name_str = table_item->val_str(&table_name_buf);

  if (table_name_str == nullptr || table_name_str->length() == 0) {
    my_error(ER_WRONG_PARAMETERS_TO_PROCEDURE, MYF(0),
             "dbms_spc.analyze_table, table_name cannot be empty");
    return true;
  }

  /* Parse the table name - support both "table" and "db.table" formats */
  const char *db_name = nullptr;
  const char *table_name = nullptr;
  const char *full_name = table_name_str->c_ptr();

  /* Find if there's a dot separator */
  const char *dot_pos = strchr(full_name, '.');
  if (dot_pos != nullptr) {
    /* db.table format */
    size_t db_len = dot_pos - full_name;

    /* Allocate memory for db_name */
    char *db_name_buf = static_cast<char *>(thd->alloc(db_len + 1));
    if (db_name_buf == nullptr) {
      my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR), db_len + 1);
      return true;
    }
    memcpy(db_name_buf, full_name, db_len);
    db_name_buf[db_len] = '\0';
    db_name = db_name_buf;

    table_name = dot_pos + 1;
  } else {
    /* table only format - use current database */
    db_name = thd->db().str;
    table_name = full_name;

    if (db_name == nullptr || strlen(db_name) == 0) {
      my_error(ER_NO_DB_ERROR, MYF(0));
      return true;
    }
  }

  /* Open table through SQL layer to verify existence and get handler */
  Table_ref table_ref(db_name, table_name, TL_READ, MDL_SHARED_READ);

  if (open_and_lock_tables(thd, &table_ref, 0)) {
    /* Error already set by open_and_lock_tables */
    return true;
  }

  TABLE *table = table_ref.table;
  if (table == nullptr) {
    close_thread_tables(thd);
    my_error(ER_NO_SUCH_TABLE, MYF(0), db_name, table_name);
    return true;
  }

  /* Call analyze_spc() to get formatted statistics */
  std::string status_buf;
  int ret = table->file->analyze_spc(&status_buf);

  close_thread_tables(thd);

  if (ret == HA_ADMIN_NOT_IMPLEMENTED) {
    push_warning_printf(thd, Sql_condition::SL_WARNING, ER_NOT_SUPPORTED_YET,
                        "dbms_spc.analyze_table is not supported by this "
                        "storage engine");
    return false;
  }

  if (ret != HA_ADMIN_OK) {
    push_warning_printf(thd, Sql_condition::SL_WARNING, ER_INTERNAL_ERROR,
                        "SPC analyze failed with error: %d", ret);
    return false;
  }

  /* Copy status string to MEM_ROOT so std::string can be released */
  m_status_len = status_buf.length();
  m_status = static_cast<char *>(thd->alloc(m_status_len + 1));
  if (m_status != nullptr) {
    memcpy(m_status, status_buf.c_str(), m_status_len + 1);
  } else {
    m_status_len = 0;
  }

  return false; /* Success */
}

void Sql_cmd_proc_spc_analyze_table::send_result(THD *thd, bool error) {
  DBUG_TRACE;

  if (error) {
    /* Error already sent */
    return;
  }

  Protocol *protocol = thd->get_protocol();

  /* Send result set metadata */
  if (m_proc->send_result_metadata(thd)) return;

  /* Send result row with formatted status string */
  protocol->start_row();
  protocol->store_string(m_status ? m_status : "",
                         m_status_len, system_charset_info);
  protocol->end_row();

  /* Send OK packet with EOF */
  my_eof(thd);
}

}  // namespace im
