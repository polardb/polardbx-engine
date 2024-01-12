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
#include "sql/xa/transaction_cache.h"  // xa::Transaction_cache::find...

#include "sql/lizard_binlog.h"
#include "sql/xa/lizard_xa_proc.h"
#include "sql/xa/lizard_xa_trx.h"

namespace im {
enum XA_status {
  /** Another seesion has attached XA. */
  ATTACHED,
  /** Detached XA, the real state of the XA can't be ACTIVE. */
  DETACHED,
  /** The XA has been erased from transaction cache, and also has been
  committed. */
  COMMIT,
  /** The XA has been erased from transaction cache, and also has been
  rollbacked. */
  ROLLBACK,
  /** Can't find such a XA in transaction cache and in transaction slots, it
  might never exist or has been forgotten. */
  NOTSTART_OR_FORGET,
  /** Found the XA in transaction slots, but the real state (commit/rollback)
  of the transaction can't be confirmed (using the old TXN format.). */
  NOT_SUPPORT,
};

/* All concurrency control system memory usage */
PSI_memory_key key_memory_xa_proc;

/* The uniform schema name for xa */
const LEX_CSTRING XA_PROC_SCHEMA = {C_STRING_WITH_LEN("dbms_xa")};

const LEX_CSTRING transaction_csr_str[] = {{C_STRING_WITH_LEN("AUTOMATIC_GCN")},
                                           {C_STRING_WITH_LEN("ASSIGNED_GCN")},
                                           {C_STRING_WITH_LEN("NONE")}};

const LEX_CSTRING xa_status_str[] = {{C_STRING_WITH_LEN("ATTACHED")},
                                     {C_STRING_WITH_LEN("DETACHED")},
                                     {C_STRING_WITH_LEN("COMMIT")},
                                     {C_STRING_WITH_LEN("ROLLBACK")},
                                     {C_STRING_WITH_LEN("NOTSTART_OR_FORGET")},
                                     {C_STRING_WITH_LEN("NOT_SUPPORT")}};

/* Singleton instance for find_by_xid */
Proc *Xa_proc_find_by_xid::instance() {
  static Proc *proc = new Xa_proc_find_by_xid(key_memory_xa_proc);
  return proc;
}

Sql_cmd *Xa_proc_find_by_xid::evoke_cmd(THD *thd,
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
bool get_gtrid(const mem_root_deque<Item *> *list, char *gtrid,
               unsigned &length) {
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

bool Sql_cmd_xa_proc_find_by_xid::pc_execute(THD *) {
  DBUG_ENTER("Sql_cmd_xa_proc_find_by_xid::pc_execute");
  DBUG_RETURN(false);
}

static const LEX_CSTRING get_csr_str(const enum my_csr_t my_csr) {
  if (my_csr == MYSQL_CSR_NONE) {
    return transaction_csr_str[2];
  } else {
    return transaction_csr_str[my_csr];
  }
}

void Sql_cmd_xa_proc_find_by_xid::send_result(THD *thd, bool error) {
  DBUG_ENTER("Sql_cmd_xa_proc_find_by_xid::send_result");
  handlerton *ttse = innodb_hton;
  assert(ttse);

  Protocol *protocol;
  XID xid;
  XID_STATE *xs;
  XA_status xa_status;
  lizard::xa::Transaction_info info;
  bool found;
  my_gcn_t gcn;
  enum my_csr_t csr;

  protocol = thd->get_protocol();

  if (error) {
    assert(thd->is_error());
    DBUG_VOID_RETURN;
  }

  if (get_xid(m_list, &xid)) {
    my_error(ER_XA_PROC_WRONG_XID, MYF(0), MAXGTRIDSIZE, MAXBQUALSIZE);
    DBUG_VOID_RETURN;
  }

  std::shared_ptr<Transaction_ctx> transaction =
      xa::Transaction_cache::find(&xid);
  if (transaction) {
    /** Case 1: Found in transaction cache. */
    xs = transaction->xid_state();

    /** There is no lock held to access this state, so the judgment of this
    state is not always accurate. It's OK for the procedure, but can't be
    used to do other thing. */
    xa_status =
        xs->is_real_attached() ? XA_status::ATTACHED : XA_status::DETACHED;

    gcn = MYSQL_GCN_NULL;

    csr = MYSQL_CSR_NONE;
  } else {
    found = ttse->ext.search_trx_by_xid(&xid, &info);
    if (found) {
      /** Case 2: Found in innodb engine. */
      switch (info.state) {
        /** Case 2.1: Normal cases. COMMIT / ROLLBACK */
        case lizard::xa::TRANS_STATE_COMMITTED:
          xa_status = XA_status::COMMIT;
          break;
        case lizard::xa::TRANS_STATE_ROLLBACK:
          xa_status = XA_status::ROLLBACK;
          break;
        /** Case 2.2: Being rollbacked in backgroud. Attached by background. */
        case lizard::xa::TRANS_STATE_ROLLBACKING_BACKGROUND:
          xa_status = XA_status::ATTACHED;
          break;
        /** Case 2.3: Sepecial case. Found old TXN format. */
        default:
          assert(info.state == lizard::xa::TRANS_STATE_UNKNOWN);
          xa_status = XA_status::NOT_SUPPORT;
          break;
      };

      gcn = info.gcn.get_gcn();

      csr = info.gcn.get_csr();
    } else {
      /** Case 3: Not ever start or already forget. */
      xa_status = XA_status::NOTSTART_OR_FORGET;

      gcn = MYSQL_GCN_NULL;

      csr = MYSQL_CSR_NONE;
    }
  }

  if (m_proc->send_result_metadata(thd)) DBUG_VOID_RETURN;

  protocol->start_row();

  const LEX_CSTRING xa_status_msg = xa_status_str[xa_status];
  protocol->store_string(xa_status_msg.str, xa_status_msg.length,
                         system_charset_info);

  protocol->store((ulonglong)gcn);

  const LEX_CSTRING csr_str = get_csr_str(csr);
  protocol->store_string(csr_str.str, csr_str.length, system_charset_info);

  if (protocol->end_row()) DBUG_VOID_RETURN;

  my_eof(thd);
  DBUG_VOID_RETURN;
}

Proc *Xa_proc_prepare_with_trx_slot::instance() {
  static Proc *proc = new Xa_proc_prepare_with_trx_slot(key_memory_xa_proc);
  return proc;
}

class Nested_xa_prepare_lex {
 public:
  Nested_xa_prepare_lex(THD *thd, XID *xid)
      : m_thd(thd), m_saved_lex(thd->lex) {
    thd->lex = new (thd->mem_root) st_lex_local;
    lex_start(thd);

    thd->lex->sql_command = SQLCOM_XA_PREPARE;
    thd->lex->m_sql_cmd = new (thd->mem_root) Sql_cmd_xa_prepare(xid);
  }

  ~Nested_xa_prepare_lex() {
    free_lex(m_thd->lex);
    m_thd->lex = m_saved_lex;
  }

 private:
  void free_lex(LEX *lex) {
    lex_end(lex);
    destroy(lex->result);
    lex->destroy();
    delete (st_lex_local *)lex;
  }

  THD *m_thd;
  LEX *m_saved_lex;
};

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

  /** 4. Do xa prepare. Will generate a new nested LEX to complete xa_prepare.
  Because Sql_cmd_xa_prepare::execute will depend on the state on LEX in
  some places. */
  Nested_xa_prepare_lex nested_xa_prepare_lex(thd, &xid);
  (dynamic_cast<Sql_cmd_xa_prepare *>(thd->lex->m_sql_cmd))->set_delay_ok();
  if (thd->lex->m_sql_cmd->execute(thd)) {
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
}  // namespace im
