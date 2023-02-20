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
#include "sql/xa/xa_proc.h"

namespace im {
/* All concurrency control system memory usage */
PSI_memory_key key_memory_xa_proc;

/* The uniform schema name for xa */
const LEX_STRING XA_PROC_SCHEMA = {C_STRING_WITH_LEN("dbms_xa")};

/* Singleton instance for find_by_xid */
Proc *Xa_proc_find_by_xid::instance() {
  static Proc *proc = new Xa_proc_find_by_xid(key_memory_xa_proc);
  return proc;
}

Sql_cmd *Xa_proc_find_by_xid::evoke_cmd(THD *thd, List<Item> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

/**
  Parse the XID from the parameter list

  @param[in]  list  parameter list
  @param[out] xid   XID

  @retval     true if parsing error.
*/
bool get_xid(const List<Item> *list, XID *xid) {
  char buff[256];
  char gtrid[MAXGTRIDSIZE];
  char bqual[MAXBQUALSIZE];
  size_t gtrid_length;
  size_t bqual_length;
  size_t formatID;


  String str(buff, sizeof(buff), system_charset_info);
  String *res;

  /* gtrid */
  res = (*list)[0]->val_str(&str);
  gtrid_length = res->length();
  if (gtrid_length > MAXGTRIDSIZE) {
    return true;
  }
  memcpy(gtrid, res->ptr(), gtrid_length);

  /* bqual */
  res = (*list)[1]->val_str(&str);
  bqual_length = res->length();
  if (bqual_length > MAXBQUALSIZE) {
    return true;
  }
  memcpy(bqual, res->ptr(), bqual_length);

  /* formatID */
  formatID = (*list)[2]->val_int();

  /** Set XID. */
  xid->set(formatID, gtrid, gtrid_length, bqual, bqual_length);

  return false;
}

bool Sql_cmd_xa_proc_find_by_xid::pc_execute(THD *) {
  DBUG_ENTER("Sql_cmd_xa_proc_find_by_xid::pc_execute");
  DBUG_RETURN(false);
}

void Sql_cmd_xa_proc_find_by_xid::send_result(THD *thd, bool error) {
  DBUG_ENTER("Sql_cmd_xa_proc_find_by_xid::send_result");

  Protocol *protocol;
  XID xid;
  lizard::xa::Transaction_info info;
  bool found;

  protocol = thd->get_protocol();

  if (error) {
    DBUG_ASSERT(thd->is_error());
    DBUG_VOID_RETURN;
  }

  if (get_xid(m_list, &xid)) {
    my_error(ER_XA_PROC_WRONG_XID, MYF(0), MAXGTRIDSIZE, MAXBQUALSIZE);
    DBUG_VOID_RETURN;
  }

  if (m_proc->send_result_metadata(thd)) DBUG_VOID_RETURN;

  found = lizard::xa::get_transaction_info_by_xid(&xid, &info);

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
