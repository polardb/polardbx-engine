/* Copyright (c) 2024, Alibaba and/or its affiliates. All rights reserved.

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

#include "sql/package/proc_ftrunc_lock.h"

#include "my_dbug.h"
#include "my_inttypes.h"
#include "mysqld_error.h"
#include "sql/auth/sql_security_ctx.h"  // Security_context
#include "sql/mdl.h"  // enum_mdl_type, MDL_EXCLUSIVE, MDL_SHARED
#include "sql/sql_ftrunc_lock.h"

namespace im {

namespace ftrunc_lock {

/* Uniform schema name for lock */
LEX_CSTRING FTRUNC_LOCK_PROC_SCHEMA = {STRING_WITH_LEN("dbms_lock")};

/* Singleton instance for acquire_s_ftrunc */
Proc *Ftrunc_lock_proc_acquire_shared::instance() {
  static Proc *proc = new Ftrunc_lock_proc_acquire_shared(key_memory_package);
  return proc;
}

/* Singleton instance for acquire_x_ftrunc */
Proc *Ftrunc_lock_proc_acquire_exclusive::instance() {
  static Proc *proc =
      new Ftrunc_lock_proc_acquire_exclusive(key_memory_package);
  return proc;
}

/* Singleton instance for release_ftrunc */
Proc *Ftrunc_lock_proc_release::instance() {
  static Proc *proc = new Ftrunc_lock_proc_release(key_memory_package);
  return proc;
}

/**
  Invoke the sql_cmd object for acquire_s_ftrunc() proc.
*/
Sql_cmd *Ftrunc_lock_proc_acquire_shared::invoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

/**
  Invoke the sql_cmd object for acquire_x_ftrunc() proc.
*/
Sql_cmd *Ftrunc_lock_proc_acquire_exclusive::invoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

/**
  Invoke the sql_cmd object for release_ftrunc() proc.
*/
Sql_cmd *Ftrunc_lock_proc_release::invoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

/**
  Check access, require BACKUP_ADMIN privilege.

  @param[in]    thd           Thread context

  @retval       true          Failure (no privilege)
  @retval       false         Success (has privilege)
*/
bool Sql_cmd_ftrunc_lock_proc_base::check_access(THD *thd) {
  Security_context *sctx = thd->security_context();
  if (!sctx->has_global_grant(STRING_WITH_LEN("BACKUP_ADMIN")).first) {
    my_error(ER_SPECIFIC_ACCESS_DENIED_ERROR, MYF(0), "BACKUP_ADMIN");
    return true;
  }
  return false;
}

/**
  Acquire FTRUNC_LOCK MDL lock with shared mode (S lock).

  @param[in]    THD           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Sql_cmd_ftrunc_lock_proc_acquire_shared::pc_execute(THD *thd) {
  DBUG_ENTER("Sql_cmd_ftrunc_lock_proc_acquire_shared::pc_execute");
  /* Acquire FTRUNC_LOCK with shared mode, use lock_wait_timeout */
  DBUG_RETURN(acquire_ftrunc_lock(thd, thd->variables.lock_wait_timeout,
                                   MDL_SHARED));
}

/**
  Acquire FTRUNC_LOCK MDL lock with exclusive mode (X lock).

  @param[in]    THD           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Sql_cmd_ftrunc_lock_proc_acquire_exclusive::pc_execute(THD *thd) {
  DBUG_ENTER("Sql_cmd_ftrunc_lock_proc_acquire_exclusive::pc_execute");
  /* Acquire FTRUNC_LOCK with exclusive mode, use lock_wait_timeout */
  DBUG_RETURN(acquire_ftrunc_lock(thd, thd->variables.lock_wait_timeout,
                                  MDL_EXCLUSIVE));
}

/**
  Release FTRUNC_LOCK MDL lock.

  @param[in]    THD           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Sql_cmd_ftrunc_lock_proc_release::pc_execute(THD *thd) {
  DBUG_ENTER("Sql_cmd_ftrunc_lock_proc_release::pc_execute");
  /* Release FTRUNC_LOCK */
  release_ftrunc_lock(thd);

  DBUG_RETURN(false);
}

/* Override the default send result. */
void Sql_cmd_ftrunc_lock_proc_base::send_result(THD *thd, bool error) {
  DBUG_ENTER("Sql_cmd_ftrunc_lock_proc_base::send_result");
  if (error) {
    assert(thd->is_error());
    DBUG_VOID_RETURN;
  }
  my_ok(thd);
  DBUG_VOID_RETURN;
}

} /* namespace ftrunc_lock */

} /* namespace im */
