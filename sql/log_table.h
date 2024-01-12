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

#ifndef SQL_LOG_TABLE_H_INCLUDED
#define SQL_LOG_TABLE_H_INCLUDED

#include "sql/log.h"
#include "sql/mdl.h"
#include "sql/sql_class.h"

class THD;

/**
  Log table management

  @Note

  Some features on Log table (slow_log/general_log)
   - Rotate table data file when flush log table

  Some variables:
   - rotate_log_table         Determined whether enable it when
                              flush slow logs
   - rotate_log_table_last_name
                              Last rotated file name
*/
namespace im {

/**
  Last rotated file name.

  It will display './DATABASE/FILE.EXT' format name
  when show global variables, and it is not protected
  by any lock, we think it is safe even though there
  will may be concurrent update.
*/
extern char rotate_log_table_last_name[];

extern char *rotate_log_table_last_name_ptr;

extern void update_rotate_log_table_last_name_ptr();

/**
  Struture used to save current runtime context
*/
struct Statement_context {
  Statement_context() {}
  ~Statement_context() {}

  /* Option_bits */
  ulonglong m_thd_option_bits;

  /* Opened tables and MDL savepoint */
  Open_tables_backup m_open_tables_state;
};

/* Log table management */
class Log_table {
 public:
  Log_table(THD *thd, enum_log_table_type log_table_type)
      : m_thd(thd),
        m_log_table_type(log_table_type),
        m_ctx_state(),
        m_table_list(nullptr),
        m_backup(false) {}

  ~Log_table();

  /**
    Validate and backup current context.

    @retval     true        Failure
    @retval     false       Success
  */
  bool validate_and_backup_context();

  /**
    Rotate the target log table data file.

    @retval     true        Failure
    @retval     false       Success
  */
  bool rotate_table();

 private:
  /**
    Lock the target log table, report error if failed.

    @retval     true        Failure
    @retval     false       Success
  */
  bool lock_log_table();

 private:
  /* Runtime connection */
  THD *m_thd;
  /* Target log table type */
  enum_log_table_type m_log_table_type;
  /* Temporary saved context */
  Statement_context m_ctx_state;
  /* Temporary created table_list object */
  Table_ref *m_table_list;
  /* Whether already backup context */
  bool m_backup;
};

/**
  Rotate log table.

  @param[in]      thd               current thread
  @param[in]      log_table_type    Slow_log or general_log

  @retval     true        Failure
  @retval     false       Success
*/
extern bool rotate_log_table(THD *thd, enum_log_table_type log_table_type);

} /* namespace im */
#endif
