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

#ifndef SQL_SQL_COMMON_EXT_H_INCLUDED
#define SQL_SQL_COMMON_EXT_H_INCLUDED

#include "my_inttypes.h"
#include "my_sqlcommand.h"

class THD;

/** instance lock modes */
enum enum_lock_instance_modes {
  LOCK_INSTANCE_NON = 0,
  LOCK_INSTANCE_TABLE_CREATION,  // deny table creation
  LOCK_INSTANCE_WRITE_GROWTH,    // read only but allow to shrink space
  LOCK_INSTANCE_WRITE,           // read only
  LOCK_INSTANCE_READ             // read only and block all command
};

/* Check the command whether allowed or not
   within different lock mode */
enum enum_lock_instance_check {
  LOCK_CHECK_ALLOWED,
  LOCK_CHECK_DENY,
  LOCK_CHECK_CONTINUE
};

extern const char *lock_instance_mode_names[];

/* mysql instance lock mode */
extern ulong lock_instance_mode;

/** Kill the transaction that is idle when timeout,
    in order to release the holded resources */
extern ulong kill_idle_transaction_timeout;

/* Report the readonly error or instance locked error */
extern void err_readonly_or_instance_lock(THD *thd);

/**
  Check current instance lock status and sql command
  before check readonly;

  @retval LOCK_CHECK_DENY       Disallowed this command
  @retval LOCK_CHECK_CONTINUE   Continue to check
*/
extern enum enum_lock_instance_check check_instance_lock_before_readonly(
    enum_sql_command sql_cmd);

/**
  Check current instance lock status and sql command
  after check readonly;

  @retval LOCK_CHECK_ALLOWED    Allowed this command
  @retval LOCK_CHECK_DENY       Disallowed this command
  @retval LOCK_CHECK_CONTINUE   Continue to check
*/
extern enum enum_lock_instance_check check_instance_lock_after_readonly(
    enum_sql_command sql_cmd);

/**
  Judge whether thd is slave thread or has SUPER ACL.

  @retval   true    slave thread or super connection
*/
bool thd_is_slave_or_super(THD *thd);

#endif