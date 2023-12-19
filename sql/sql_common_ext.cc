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
#include "sql/sql_common_ext.h"
#include "sql/auth/auth_acls.h"
#include "sql/auth/auth_common.h"  // check_readonly
#include "sql/handler.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/transaction.h"  // trans_commit_implicit

/** instance lock mode names */
const char *lock_instance_mode_names[] = {
    "LOCK_NON",   "LOCK_TABLE_CREATION", "LOCK_WRITE_GROWTH",
    "LOCK_WRITE", "LOCK_READ",           NullS};

/* mysql instance lock mode */
ulong lock_instance_mode = 0;

ulong kill_idle_transaction_timeout = 0;

/* Report the readonly error or instance locked error */
void err_readonly_or_instance_lock(THD *thd) {
  if (lock_instance_mode > LOCK_INSTANCE_NON)
    my_error(ER_OPTION_PREVENTS_STATEMENT, MYF(0),
             lock_instance_mode_names[lock_instance_mode]);
  else
    err_readonly(thd);
}

/**
  Check current instance lock status and sql command
  before check readonly;

  @retval LOCK_CHECK_DENY       Disallowed this command
  @retval LOCK_CHECK_CONTINUE   Continue to check
*/
enum enum_lock_instance_check check_instance_lock_before_readonly(
    enum_sql_command sql_cmd) {
  if (lock_instance_mode == LOCK_INSTANCE_TABLE_CREATION) {
    switch (sql_cmd) {
      case SQLCOM_CREATE_TABLE:
        return LOCK_CHECK_DENY;
      default:
        break;
    }
  }
  return LOCK_CHECK_CONTINUE;
}

/**
  Check current instance lock status and sql command
  after check readonly;

  @retval LOCK_CHECK_ALLOWED    Allowed this command
  @retval LOCK_CHECK_DENY       Disallowed this command
  @retval LOCK_CHECK_CONTINUE   Continue to check
*/
enum enum_lock_instance_check check_instance_lock_after_readonly(
    enum_sql_command sql_cmd) {
  if (lock_instance_mode == LOCK_INSTANCE_WRITE_GROWTH) {
    switch (sql_cmd) {
      case SQLCOM_DROP_TABLE:
      case SQLCOM_DROP_INDEX:
      case SQLCOM_TRUNCATE:
      case SQLCOM_DROP_DB:
      case SQLCOM_OPTIMIZE:
      case SQLCOM_COMMIT:
      case SQLCOM_ROLLBACK:
        return LOCK_CHECK_ALLOWED;
      default:
        break;
    }
  } else if (lock_instance_mode == LOCK_INSTANCE_READ) {
    switch (sql_cmd) {
      case SQLCOM_SELECT:
        return LOCK_CHECK_DENY;
      default:
        break;
    }
  }
  return LOCK_CHECK_CONTINUE;
}

/**
  Judge whether thd is slave thread or has SUPER ACL.

  @retval   true    slave thread or super connection
*/
bool thd_is_slave_or_super(THD *thd) {
  Security_context *sctx = thd->security_context();
  bool is_super =
      sctx->check_access(SUPER_ACL) ||
      sctx->has_global_grant(STRING_WITH_LEN("CONNECTION_ADMIN")).first;
  if (thd->slave_thread || is_super) return true;

  return false;
}
