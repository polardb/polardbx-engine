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

#include "sql/package/proc_checkpoint.h"
#include "mutex_lock.h"
#include "sql/derror.h"
#include "sql/mysqld_thd_manager.h"

extern void advance_checkpoint_now();

namespace im {

const LEX_CSTRING PROC_CHECKPOINT_SCHEMA = {
    C_STRING_WITH_LEN("dbms_checkpoint")};

Proc *Advance_checkpoint_proc::instance() {
  static Proc *proc = new Advance_checkpoint_proc(key_memory_package);

  return proc;
}

Sql_cmd *Advance_checkpoint_proc::invoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_checkpoint_advance::pc_execute(THD *) {
  bool error = false;
  DBUG_ENTER("Advance_checkpoint_proc::pc_execute");
  
  advance_checkpoint_now();

  DBUG_RETURN(error);
}

} /* namespace im */