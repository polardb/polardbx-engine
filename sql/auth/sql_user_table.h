/* Copyright (c) 2000, 2017, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   51 Franklin Street, Suite 500, Boston, MA 02110-1335 USA */
#ifndef SQL_USER_TABLE_INCLUDED
#define SQL_USER_TABLE_INCLUDED

#include <sys/types.h>

#include "derror.h"                     /* ER_DEFAULT */
#include "log.h"                        /* error_log_printf */
#include "my_compiler.h"
#include "my_inttypes.h"
#include "mysqld_error.h"
#include "sql_connect.h"
#include "table.h"


class THD;

extern const TABLE_FIELD_DEF mysql_db_table_def;
extern const TABLE_FIELD_DEF mysql_user_table_def;
extern const TABLE_FIELD_DEF mysql_proxies_priv_table_def;
extern const TABLE_FIELD_DEF mysql_procs_priv_table_def;
extern const TABLE_FIELD_DEF mysql_columns_priv_table_def;
extern const TABLE_FIELD_DEF mysql_tables_priv_table_def;
extern const TABLE_FIELD_DEF mysql_role_edges_table_def;
extern const TABLE_FIELD_DEF mysql_default_roles_table_def;

/**
  Class to validate the flawlessness of ACL table
  before performing ACL operations.
*/
class Acl_table_intact : public Table_check_intact
{
public:
  Acl_table_intact(THD *c_thd) : thd(c_thd) {}

protected:
  void report_error(uint code, const char *fmt, ...)
    MY_ATTRIBUTE((format(printf, 3, 4)))
  {
    va_list args;
    va_start(args, fmt);

    if (code == 0)
      error_log_printf(WARNING_LEVEL, fmt, args);
    else if (code == ER_CANNOT_LOAD_FROM_TABLE_V2)
    {
      char *db_name, *table_name;
      db_name= va_arg(args, char *);
      table_name= va_arg(args, char *);
      my_error(code, MYF(ME_ERRORLOG), db_name, table_name);
    }
    else
      my_printv_error(code, ER_THD(thd, code), MYF(ME_ERRORLOG), args);

    va_end(args);
  }

private:
  THD *thd;
};


/**  Enum for ACL tables */
typedef enum ACL_TABLES
{
  TABLE_USER= 0,
  TABLE_DB,
  TABLE_TABLES_PRIV,
  TABLE_COLUMNS_PRIV,
  TABLE_PROCS_PRIV,
  TABLE_PROXIES_PRIV,
  TABLE_ROLE_EDGES,
  TABLE_DEFAULT_ROLES,
  TABLE_DYNAMIC_PRIV,
  LAST_ENTRY  /* Must always be at the end */
} ACL_TABLES;

int handle_grant_table(THD *thd, TABLE_LIST *tables, ACL_TABLES table_no, bool drop,
                       LEX_USER *user_from, LEX_USER *user_to);

#endif /* SQL_USER_TABLE_INCLUDED */
