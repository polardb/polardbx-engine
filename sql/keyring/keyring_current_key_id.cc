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

#include "keyring_current_key_id.h"

#include "mysql/service_mysql_keyring.h"

namespace im {

Proc *Proc_current_key_id::instance() {
  static Proc_current_key_id *proc =
      new Proc_current_key_id(key_memory_package);
  return proc;
}

Sql_cmd *Proc_current_key_id::evoke_cmd(THD *thd,
                                        mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Cmd_current_key_id(thd, list, this);
}

void Cmd_current_key_id::send_result(THD *thd, bool error) {
  Protocol *protocol;
  int ret;
  size_t id_len;
  char *key_id = NULL, *key_type = NULL;

  DBUG_ENTER("Cmd_current_key_id::send_result");

  if (error) {
    assert(thd->is_error());
    goto ERROR;
  }

  /**
    The 1th parameter key_id is set to NULL,
    means that we want to fetch the latest master key id, and
    the returned value is stored in key_id & id_len.
  */
  ret = my_key_fetch(NULL, &key_type, NULL, reinterpret_cast<void **>(&key_id),
                     &id_len);
  if (ret) {
    my_error(ER_KEYRING_UDF_KEYRING_SERVICE_ERROR, MYF(0), "keyring_key_fetch");
    goto ERROR;
  }

  if (m_proc->send_result_metadata(thd)) goto ERROR;

  protocol = thd->get_protocol();
  protocol->start_row();
  protocol->store(key_id, system_charset_info);
  if (protocol->end_row()) goto ERROR;

  my_eof(thd);

ERROR:
  /**
    Callers is responsible for releasing resources.
  */
  if (key_type) my_free(key_type);
  if (key_id) my_free(key_id);

  DBUG_VOID_RETURN;
}

}  // namespace im
