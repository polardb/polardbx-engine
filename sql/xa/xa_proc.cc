/* Copyright (c) 2018, 2023, Alibaba and/or its affiliates. All rights reserved.

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

#include "storage/innobase/include/lizard0xa0iface.h"
#include "sql/protocol.h"
#include "sql/xa/xa_proc.h"

namespace im {
/* All concurrency control system memory usage */
PSI_memory_key key_memory_xa_proc;

/* The uniform schema name for xa */
const LEX_CSTRING XA_PROC_SCHEMA = {C_STRING_WITH_LEN("dbms_xa")};

/* Singleton instance for find_by_gtrid */
Proc *Xa_proc_find_by_gtrid::instance() {
  static Proc *proc = new Xa_proc_find_by_gtrid(key_memory_xa_proc);
  return proc;
}

Sql_cmd *Xa_proc_find_by_gtrid::evoke_cmd(THD *thd,
                                          mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

/**
  Parse the GTRID from the parameter list

  @param[in]  list    parameter list
  @param[out] gtrid   GTRID
  @param[out] length  length of gtrid

  @retval     true if parsing error.
*/
bool get_gtrid(const mem_root_deque<Item *> *list, char *gtrid, unsigned &length) {
  char buff[128];

  String str(buff, sizeof(buff), system_charset_info);
  String *res;

  /* gtrid */
  res = (*list)[0]->val_str(&str);
  length = res->length();
  if (length > MAXGTRIDSIZE) {
    return true;
  }
  memcpy(gtrid, res->ptr(), length);

  return false;
}

bool Sql_cmd_xa_proc_find_by_gtrid::pc_execute(THD *) {
  DBUG_ENTER("Sql_cmd_xa_proc_find_by_gtrid::pc_execute");
  DBUG_RETURN(false);
}

void Sql_cmd_xa_proc_find_by_gtrid::send_result(THD *thd, bool error) {
  DBUG_ENTER("Sql_cmd_xa_proc_find_by_gtrid::send_result");

  Protocol *protocol;
  XID xid;
  lizard::xa::Transaction_info info;
  bool found;
  char gtrid[MAXGTRIDSIZE];
  unsigned gtrid_length;

  protocol = thd->get_protocol();

  if (error) {
    assert(thd->is_error());
    DBUG_VOID_RETURN;
  }

  if (get_gtrid(m_list, gtrid, gtrid_length)) {
    my_error(ER_XA_PROC_WRONG_GTRID, MYF(0), MAXGTRIDSIZE);
    DBUG_VOID_RETURN;
  }

  if (m_proc->send_result_metadata(thd)) DBUG_VOID_RETURN;

  found = lizard::xa::get_transaction_info_by_gtrid(gtrid, gtrid_length, &info);

  if (found) {
    protocol->start_row();
    protocol->store((ulonglong)info.gcn);

    const char *state = lizard::xa::transaction_state_to_str(info.state);
    protocol->store_string(state, strlen(state), system_charset_info);

    if (protocol->end_row()) DBUG_VOID_RETURN;
  }

  my_eof(thd);
  DBUG_VOID_RETURN;
}

}
