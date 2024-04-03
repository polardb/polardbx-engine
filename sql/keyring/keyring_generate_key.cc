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

#include "keyring_generate_key.h"
#include "mysql/service_mysql_keyring.h"
#include "sql/sql_backup_lock.h"

namespace im {

Proc *Proc_generate_key::instance() {
  static Proc_generate_key *proc = new Proc_generate_key(key_memory_package);
  return proc;
}

Sql_cmd *Proc_generate_key::evoke_cmd(THD *thd,
                                      mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Cmd_generate_key(thd, list, this);
}

bool Cmd_generate_key::pc_execute(THD *) {
  DBUG_ENTER("Cmd_generate_key::pc_execute");

  /* Block if FTWRL or backuping */
  MDL_request_list mdl_requests;
  MDL_request global_request;
  MDL_request backup_lock_request;

  if (m_thd->global_read_lock.can_acquire_protection()) DBUG_RETURN(true);

  MDL_REQUEST_INIT(&global_request, MDL_key::GLOBAL, "", "",
                   MDL_INTENTION_EXCLUSIVE, MDL_STATEMENT);
  MDL_REQUEST_INIT(&backup_lock_request, MDL_key::BACKUP_LOCK, "", "",
                   MDL_INTENTION_EXCLUSIVE, MDL_STATEMENT);
  mdl_requests.push_front(&global_request);
  mdl_requests.push_front(&backup_lock_request);

  ulong timeout = m_thd->variables.lock_wait_timeout;
  if (m_thd->mdl_context.acquire_locks(&mdl_requests, timeout))
    DBUG_RETURN(true);

  /**
    This API requires KMS/Agent to generate a new master key.
    The generated key information is stroed in KMS and Agent.
    No need to pass any parameters in this calling.
  */
  int ret = my_key_generate(NULL, NULL, NULL, 0);
  if (ret) {
    my_error(ER_KEYRING_UDF_KEYRING_SERVICE_ERROR, MYF(0),
             "keyring_key_generate");
  }

  DBUG_RETURN(ret);
}

}  // namespace im
