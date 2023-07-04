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

#include "sql/mysqld.h"  // server_uuid_ptr...
#include "sql/protocol.h"
#include "sql/sql_parse.h"  // sql_command_flags...
#include "sql/xa/sql_xa_prepare.h"

#include "sql/xa/lizard_xa_proc.h"
#include "sql/xa/lizard_xa_trx.h"
#include "sql/lizard_binlog.h"

namespace im {
/* All concurrency control system memory usage */
PSI_memory_key key_memory_xa_proc;

/* The uniform schema name for xa */
const LEX_CSTRING XA_PROC_SCHEMA = {C_STRING_WITH_LEN("dbms_xa")};

const LEX_CSTRING transaction_state_str[] = {{STRING_WITH_LEN("COMMIT")},
                                             {STRING_WITH_LEN("ROLLBACK")},
                                             {STRING_WITH_LEN("UNKNOWN")}};

const LEX_CSTRING transaction_csr_str[] = {{C_STRING_WITH_LEN("AUTOMATIC_GCN")},
                                           {C_STRING_WITH_LEN("ASSIGNED_GCN")},
                                           {C_STRING_WITH_LEN("NONE")}};

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

/**
  Parse the XID from the parameter list

  @param[in]  list  parameter list
  @param[out] xid   XID

  @retval     true if parsing error.
*/
bool get_xid(const mem_root_deque<Item *> *list, XID *xid) {
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

bool Sql_cmd_xa_proc_find_by_gtrid::pc_execute(THD *) {
  DBUG_ENTER("Sql_cmd_xa_proc_find_by_gtrid::pc_execute");
  DBUG_RETURN(false);
}

static const LEX_CSTRING get_csr_str(const enum my_csr_t my_csr) {
  if (my_csr == MYSQL_CSR_NONE) {
    return transaction_csr_str[2];
  } else {
    return transaction_csr_str[my_csr];
  }
}

void Sql_cmd_xa_proc_find_by_gtrid::send_result(THD *thd, bool error) {
  DBUG_ENTER("Sql_cmd_xa_proc_find_by_gtrid::send_result");
  handlerton *ttse = innodb_hton;
  assert(ttse);

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

  found = ttse->ext.search_trx_by_gtrid(gtrid, gtrid_length, &info);
  if (found) {
    protocol->start_row();
    protocol->store((ulonglong)info.gcn.get_gcn());

    protocol->store_string(transaction_state_str[info.state].str,
                           transaction_state_str[info.state].length,
                           system_charset_info);

    const LEX_CSTRING csr_str = get_csr_str(info.gcn.get_csr());
    protocol->store_string(csr_str.str, csr_str.length, system_charset_info);

    if (protocol->end_row()) DBUG_VOID_RETURN;
  }

  my_eof(thd);
  DBUG_VOID_RETURN;
}

Proc *Xa_proc_prepare_with_trx_slot::instance() {
  static Proc *proc = new Xa_proc_prepare_with_trx_slot(key_memory_xa_proc);
  return proc;
}

Sql_cmd *Xa_proc_prepare_with_trx_slot::evoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_xa_proc_prepare_with_trx_slot::pc_execute(THD *thd) {
  DBUG_ENTER("Sql_cmd_xa_proc_prepare_with_trx_slot");

  XID xid;
  XID_STATE *xid_state = thd->get_transaction()->xid_state();

  /** 1. parsed XID from parameters list. */
  if (get_xid(m_list, &xid)) {
    my_error(ER_XA_PROC_WRONG_XID, MYF(0), MAXGTRIDSIZE, MAXBQUALSIZE);
    DBUG_RETURN(true);
  }

  /** 2. Check whether it is an xa transaction that has completed "XA END" */
  if (!xid_state->has_state(XID_STATE::XA_IDLE)) {
    my_error(ER_XAER_RMFAIL, MYF(0), xid_state->state_name());
    DBUG_RETURN(true);
  } else if (!xid_state->has_same_xid(&xid)) {
    my_error(ER_XAER_NOTA, MYF(0));
    DBUG_RETURN(true);
  }

  /** 3. Assign transaction slot. */
  if (lizard::xa::apply_trx_for_xa(thd, &xid, &m_slot_ptr)) {
    DBUG_RETURN(true);
  }

  /** 4. Do xa prepare. */
  Sql_cmd_xa_prepare *executor = new (thd->mem_root) Sql_cmd_xa_prepare(&xid);
  executor->set_delay_ok();
  if (executor->execute(thd)) {
    DBUG_RETURN(true);
  }

  DBUG_RETURN(false);
}

void Sql_cmd_xa_proc_prepare_with_trx_slot::send_result(THD *thd, bool error) {
  DBUG_ENTER("Sql_cmd_xa_proc_prepare_with_trx_slot::send_result");
  Protocol *protocol;

  if (error) {
    assert(thd->is_error());
    DBUG_VOID_RETURN;
  }

  protocol = thd->get_protocol();

  if (m_proc->send_result_metadata(thd)) DBUG_VOID_RETURN;

  protocol->start_row();
  assert(strlen(server_uuid_ptr) <= 256);
  protocol->store_string(server_uuid_ptr, strlen(server_uuid_ptr),
                         system_charset_info);
  protocol->store((ulonglong)m_slot_ptr);
  if (protocol->end_row()) DBUG_VOID_RETURN;

  my_eof(thd);
  DBUG_VOID_RETURN;
}

bool Sql_cmd_xa_proc_send_heartbeat::pc_execute(THD *) {
  DBUG_ENTER("Sql_cmd_xa_proc_send_heartbeat::pc_execute");
  lizard::xa::hb_freezer_heartbeat();

  DBUG_RETURN(false);
}

Proc *Xa_proc_send_heartbeat::instance() {
  static Proc *proc = new Xa_proc_send_heartbeat(key_memory_xa_proc);
  return proc;
}

Sql_cmd *Xa_proc_send_heartbeat::evoke_cmd(THD *thd,
                                           mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

/**
  Parse the GCN from the parameter list

  @param[in]  list    parameter list
  @param[out] gcn     GCN

  @retval     true if parsing error.
*/
bool parse_gcn_from_parameter_list(const mem_root_deque<Item *> *list,
                                   my_gcn_t *gcn) {
  /* GCN */
  *gcn = (*list)[0]->val_uint();

  return false;
}

bool Sql_cmd_xa_proc_advance_gcn_no_flush::pc_execute(THD *) {
  DBUG_ENTER("Sql_cmd_xa_proc_advance_gcn_no_flush::pc_execute");
  handlerton *ttse = innodb_hton;
  my_gcn_t gcn;
  if (parse_gcn_from_parameter_list(m_list, &gcn)) {
    /** Not possible. */
    DBUG_ABORT();
    DBUG_RETURN(true);
  }

  ttse->ext.set_gcn_if_bigger(gcn);
  DBUG_RETURN(false);
}

Proc *Xa_proc_advance_gcn_no_flush::instance() {
  static Proc *proc = new Xa_proc_advance_gcn_no_flush(key_memory_xa_proc);
  return proc;
}

Sql_cmd *Xa_proc_advance_gcn_no_flush::evoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}
}
