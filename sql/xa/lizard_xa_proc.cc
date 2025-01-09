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

#include <algorithm>

#include "sql/mysqld.h"  // server_uuid_ptr...
#include "sql/protocol.h"
#include "sql/sql_parse.h"  // sql_command_flags...
#include "sql/xa/sql_xa_prepare.h"
#include "sql/xa/sql_xa_commit.h"
#include "sql/xa/transaction_cache.h"  // xa::Transaction_cache::find...

#include "sql/lizard_binlog.h"
#include "sql/xa/lizard_xa_proc.h"
#include "sql/xa/lizard_xa_trx.h"
#include "sql/lizard/lizard_hb_freezer.h"
#include "sql/raii/sentry.h"

using namespace lizard;

namespace im {

/* All concurrency control system memory usage */
PSI_memory_key key_memory_xa_proc;

/* The uniform schema name for xa */
const LEX_CSTRING XA_PROC_SCHEMA = {C_STRING_WITH_LEN("dbms_xa")};

const LEX_CSTRING transaction_csr_str[] = {{C_STRING_WITH_LEN("AUTOMATIC_GCN")},
                                           {C_STRING_WITH_LEN("ASSIGNED_GCN")}};

const LEX_CSTRING xa_status_str[] = {{C_STRING_WITH_LEN("ATTACHED")},
                                     {C_STRING_WITH_LEN("DETACHED_PREPARE")},
                                     {C_STRING_WITH_LEN("COMMIT")},
                                     {C_STRING_WITH_LEN("ROLLBACK")},
                                     {C_STRING_WITH_LEN("NOTSTART_OR_FORGET")},
                                     {C_STRING_WITH_LEN("NOT_SUPPORT")}};

static inline bool trx_slot_check_retention() {
  handlerton *ttse = innodb_hton;
  return ttse->ext.trx_slot_check_retention();
}

/**
  Parse the XID from the parameter list

  @param[in]  list  parameter list
  @param[out] xid   XID

  @retval     true if parsing error.
*/
static bool get_xid(const mem_root_deque<Item *> *list, const size_t item_idx,
                    XID *xid) {
  char buff[256];
  char gtrid[MAXGTRIDSIZE];
  char bqual[MAXBQUALSIZE];
  size_t gtrid_length;
  size_t bqual_length;
  size_t formatID;

  String str(buff, sizeof(buff), system_charset_info);
  String *res;

  /* gtrid */
  res = (*list)[item_idx]->val_str(&str);
  gtrid_length = res->length();
  if (gtrid_length > MAXGTRIDSIZE) {
    return true;
  }
  memcpy(gtrid, res->ptr(), gtrid_length);

  /* bqual */
  res = (*list)[item_idx + 1]->val_str(&str);
  bqual_length = res->length();
  if (bqual_length > MAXBQUALSIZE) {
    return true;
  }
  memcpy(bqual, res->ptr(), bqual_length);

  /* formatID */
  formatID = (*list)[item_idx + 2]->val_int();

  /** Set XID. */
  xid->set(formatID, gtrid, gtrid_length, bqual, bqual_length);

  return false;
}

static bool get_slot_ptr(const mem_root_deque<Item *> *list,
                         const size_t item_idx, slot_ptr_t *slot_ptr) {
  /* Slot Address */
  assert(list->size() > item_idx);
  *slot_ptr = (*list)[item_idx]->val_int();
  return false;
}

/**************************************/
/* find_by_xid Related */
/**************************************/
Proc *Xa_proc_find_by_xid::instance() {
  static Proc *proc = new Xa_proc_find_by_xid(key_memory_xa_proc);
  return proc;
}

Sql_cmd *Xa_proc_find_by_xid::invoke_cmd(THD *thd,
                                         mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this, false);
}

bool Sql_cmd_xa_proc_find_by_xid::pc_execute(THD *) {
  DBUG_ENTER("Sql_cmd_xa_proc_find_by_xid::pc_execute");
  DBUG_RETURN(false);
}

static const LEX_CSTRING get_csr_str(const csr_t my_csr) {
  return transaction_csr_str[my_csr];
}

void Sql_cmd_xa_proc_find_by_xid::send_result(THD *thd, bool error) {
  DBUG_ENTER("Sql_cmd_xa_proc_find_by_xid::send_result");
  LEX_CSTRING csr_str;

  Protocol *protocol;
  XID xid;
  auto thd_xs = thd->get_transaction()->xid_state();
  MyXAInfo info(XA_status::NOTSTART_OR_FORGET);
  size_t uuid_len = 0;
  slot_ptr_t slot_ptr_hint = 0;

  protocol = thd->get_protocol();

  if (error) {
    assert(thd->is_error());
    DBUG_VOID_RETURN;
  }

  if (get_xid(m_list, Xa_proc_find_by_xid_with_hint::XA_PARAM_GTRID, &xid)) {
    my_error(ER_XA_PROC_WRONG_XID, MYF(0), MAXGTRIDSIZE, MAXBQUALSIZE);
    DBUG_VOID_RETURN;
  }

  if (m_has_tslot_hint) {
    get_slot_ptr(m_list, Xa_proc_find_by_xid_with_hint::XA_PARAM_COLUMN_UBA,
                 &slot_ptr_hint);
  }

  /** Cannot be doing XA transaction. */
  if (!thd_xs->has_state(XID_STATE::XA_NOTR) ||
      thd->in_active_multi_stmt_transaction()) {
    DBUG_PRINT("xa", ("Failed to look up tran because it is NOT in XA_NOTR"));
    my_error(ER_XAER_RMFAIL, MYF(0), thd_xs->state_name());
    DBUG_VOID_RETURN;
  }

  lizard::xa::search_trx_info(&xid, &info, slot_ptr_hint);

  if (m_proc->send_result_metadata(thd)) DBUG_VOID_RETURN;

  protocol->start_row();

  const LEX_CSTRING xa_status_msg = xa_status_str[info.status];
  protocol->store_string(xa_status_msg.str, xa_status_msg.length,
                         system_charset_info);

  switch (info.status) {
    case ATTACHED:
    case NOTSTART_OR_FORGET:
    case NOT_SUPPORT:
      /** GCN */
      protocol->store_null();
      /** CSR */
      protocol->store_null();
      /** tid */
      protocol->store_null();
      /** transaction ptr */
      protocol->store_null();
      /** n_globals */
      protocol->store_null();
      /** n_locals */
      protocol->store_null();
      /** master tid */
      protocol->store_null();
      /** master transaction ptr */
      protocol->store_null();
      /** Server UUID */
      protocol->store_null();
      break;
    case DETACHED_PREPARE:
    case COMMIT:
    case ROLLBACK:
      /** 1. Return GCN info */
      if (info.is_proposal) {
        ut_a(info.status == DETACHED_PREPARE);
        protocol->store((ulonglong)info.gcn.gcn);
        csr_str = get_csr_str(info.gcn.csr);
        protocol->store_string(csr_str.str, csr_str.length, system_charset_info);
      } else if (info.gcn.is_null()) {
        /** Prepare without ac_prepare. */
        ut_a(info.status == DETACHED_PREPARE);
        protocol->store_null();
        protocol->store_null();
      } else {
        assert(info.status == COMMIT || info.status == ROLLBACK);
        protocol->store((ulonglong)info.gcn.gcn);
        csr_str = get_csr_str(info.gcn.csr);
        protocol->store_string(csr_str.str, csr_str.length, system_charset_info);
      }

      /** 2. Return physical ID of myself. */
      if (!info.slot.is_null()) {
        assert(info.slot.is_valid());
        protocol->store((ulonglong)info.slot.tid);
        protocol->store((ulonglong)info.slot.slot_ptr);
      } else {
        protocol->store_null();
        protocol->store_null();
      }

      /** 3. Return branch info. */
      if (!info.branch.is_null()) {
        protocol->store((ulonglong)info.branch.n_global);
        protocol->store((ulonglong)info.branch.n_local);
      } else {
        protocol->store_null();
        protocol->store_null();
      }

      /** 4. Return master physical ID */
      if (!info.maddr.is_null()) {
        assert(info.slot.is_valid());
        protocol->store((ulonglong)info.maddr.tid);
        protocol->store((ulonglong)info.maddr.slot_ptr);
      } else {
        protocol->store_null();
        protocol->store_null();
      }

      /** 5. Return Server UUID */
      uuid_len = std::min(strlen(server_uuid_ptr), MAX_SERVER_UUID_LENGTH);
      protocol->store_string(server_uuid_ptr, uuid_len, system_charset_info);

      break;
    default:
      assert(0);
      break;
  }

  if (protocol->end_row()) DBUG_VOID_RETURN;

  my_eof(thd);
  DBUG_VOID_RETURN;
}


/**************************************/
/* find_by_xid_with_hint Related */
/**************************************/
Proc *Xa_proc_find_by_xid_with_hint::instance() {
  static Proc *proc = new Xa_proc_find_by_xid_with_hint(key_memory_xa_proc);
  return proc;
}

Sql_cmd *Xa_proc_find_by_xid_with_hint::invoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this, true);
}

Proc *Xa_proc_prepare_with_trx_slot::instance() {
  static Proc *proc = new Xa_proc_prepare_with_trx_slot(key_memory_xa_proc);
  return proc;
}

/**************************************/
/* prepare_with_trx_slot Related */
/**************************************/
class Nested_xa_prepare_lex {
 public:
  Nested_xa_prepare_lex(THD *thd, XID *xid)
      : m_thd(thd), m_saved_lex(thd->lex) {
    thd->lex = new (thd->mem_root) st_lex_local;
    lex_start(thd);

    thd->lex->sql_command = SQLCOM_XA_PREPARE;
    thd->lex->m_sql_cmd = new (thd->mem_root) Sql_cmd_xa_prepare(xid);
  }

  Sql_cmd_xa_prepare *get_cmd_executor() {
    return dynamic_cast<Sql_cmd_xa_prepare *>(m_thd->lex->m_sql_cmd);
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

Sql_cmd *Xa_proc_prepare_with_trx_slot::invoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_xa_proc_prepare_with_trx_slot::pc_execute(THD *thd) {
  DBUG_ENTER("Sql_cmd_xa_proc_prepare_with_trx_slot");

  XID xid;
  XID_STATE *xid_state = thd->get_transaction()->xid_state();

  /** 0. Check retention for TXN (contains coordinator logs). */
  if (!trx_slot_check_retention()) {
    my_error(ER_XA_PROC_RETENTION_NOT_SATISFIED, MYF(0));
    DBUG_RETURN(true);
  }

  /** 1. parsed XID from parameters list. */
  if (get_xid(m_list, Xa_proc_prepare_with_trx_slot::XA_PARAM_GTRID, &xid)) {
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
  if (lizard::xa::apply_trx_for_xa(thd, &xid, &m_slot_ptr, nullptr)) {
    DBUG_RETURN(true);
  }

  /** 4. Do xa prepare. Will generate a new nested LEX to complete xa_prepare.
  Because Sql_cmd_xa_prepare::execute will depend on the state on LEX in
  some places. */
  Nested_xa_prepare_lex nested_xa_prepare_lex(thd, &xid);
  Sql_cmd_xa_prepare *cmd_executor = nested_xa_prepare_lex.get_cmd_executor();
  cmd_executor->set_delay_ok();
  if (cmd_executor->execute(thd)) {
    DBUG_RETURN(true);
  }

  DBUG_RETURN(false);
}

void Sql_cmd_xa_proc_prepare_with_trx_slot::send_result(THD *thd, bool error) {
  DBUG_ENTER("Sql_cmd_xa_proc_prepare_with_trx_slot::send_result");
  Protocol *protocol;
  size_t uuid_len = 0;

  if (error) {
    assert(thd->is_error());
    DBUG_VOID_RETURN;
  }

  protocol = thd->get_protocol();

  if (m_proc->send_result_metadata(thd)) DBUG_VOID_RETURN;

  protocol->start_row();
  /** TODO: check uuid len <22-07-24, zanye.zjy> */
  uuid_len = std::min(strlen(server_uuid_ptr), MAX_SERVER_UUID_LENGTH);
  protocol->store_string(server_uuid_ptr, uuid_len, system_charset_info);
  protocol->store((ulonglong)m_slot_ptr);
  if (protocol->end_row()) DBUG_VOID_RETURN;

  my_eof(thd);
  DBUG_VOID_RETURN;
}

bool Sql_cmd_xa_proc_send_heartbeat::pc_execute(THD *) {
  DBUG_ENTER("Sql_cmd_xa_proc_send_heartbeat::pc_execute");
  lizard::hb_freezer_heartbeat();

  DBUG_RETURN(false);
}

Proc *Xa_proc_send_heartbeat::instance() {
  static Proc *proc = new Xa_proc_send_heartbeat(key_memory_xa_proc);
  return proc;
}

Sql_cmd *Xa_proc_send_heartbeat::invoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

/**
  Parse the GCN from the parameter list

  @param[in]  list    parameter list
  @param[out] gcn     GCN

  @retval     true if parsing error.
*/
bool parse_gcn_from_parameter_list(const mem_root_deque<Item *> *list,
                                   gcn_t *gcn) {
  /* GCN */
  *gcn = (*list)[0]->val_uint();

  return false;
}

bool Sql_cmd_xa_proc_advance_gcn_no_flush::pc_execute(THD *) {
  DBUG_ENTER("Sql_cmd_xa_proc_advance_gcn_no_flush::pc_execute");
  handlerton *ttse = innodb_hton;
  gcn_t gcn;
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

Sql_cmd *Xa_proc_advance_gcn_no_flush::invoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

/**************************************************************************/
/* Xa_proc_ac_prepare */
/* ************************************************************************/
Proc *Xa_proc_ac_prepare::instance() {
  static Proc *proc = new Xa_proc_ac_prepare(key_memory_xa_proc);
  return proc;
}

Sql_cmd *Xa_proc_ac_prepare::invoke_cmd(THD *thd,
                                        mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_xa_proc_ac_prepare::pc_execute(THD *thd) {
  DBUG_ENTER("Sql_cmd_xa_proc_ac_prepare::pc_execute");
  branch_num_t n_global;
  branch_num_t n_local;
  gcn_t pre_commit_gcn;
  XID xid;
  XID_STATE *xid_state =nullptr;
  AC_prepare_policy *policy = nullptr;

  xid_state = thd->get_transaction()->xid_state();
  /** 0. Check retention for TXN (contains coordinator logs). */
  if (!trx_slot_check_retention()) {
    my_error(ER_XA_PROC_RETENTION_NOT_SATISFIED, MYF(0));
    DBUG_RETURN(true);
  }

  /** 1. parsed XID, n_branch, n_local_branch, pre commit gcn from parameters
  list. */
  if (get_xid(m_list, Xa_proc_ac_prepare::XA_PARAM_GTRID, &xid)) {
    my_error(ER_XA_PROC_WRONG_XID, MYF(0), MAXGTRIDSIZE, MAXBQUALSIZE);
    DBUG_RETURN(true);
  }

  n_global = (*m_list)[Xa_proc_ac_prepare::XA_PARAM_N_BRANCH]->val_int();
  n_local = (*m_list)[Xa_proc_ac_prepare::XA_PARAM_N_LOCAL_BRANCH]->val_int();

  if (n_global > MAX_BRANCH || n_local > MAX_BRANCH) {
    my_error(ER_XA_PROC_AC_TOO_MANY_PARTICIPANTS, MYF(0), n_global, n_local);
    DBUG_RETURN(true);
  }

  if (n_global == 0 || n_local == 0 || n_local > n_global) {
    my_error(ER_XA_PROC_INVALID_XA_BRANCH_PARAM, MYF(0), n_global, n_local);
    DBUG_RETURN(true);
  }

  pre_commit_gcn =
      (*m_list)[Xa_proc_ac_prepare::XA_PARAM_PRE_COMMIT_GCN]->val_int();

  if (pre_commit_gcn == GCN_NULL || pre_commit_gcn < GCN_INITIAL) {
    my_error(ER_XA_PROC_AC_INVALID_GCN, MYF(0), pre_commit_gcn);
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
  if (lizard::xa::apply_trx_for_xa(thd, &xid, &m_slot_ptr, &m_trx_id)) {
    DBUG_RETURN(true);
  }

  /** 4. Take AC_prepare_policy from pocket, and init it. */
  policy =
      thd->cpolicy_ctx.activate_ac_prepare(pre_commit_gcn, {n_global, n_local});

  /** 5. Do xa prepare. Will generate a new nested LEX to complete xa_prepare.
  Because Sql_cmd_xa_prepare::execute will depend on the state on LEX in
  some places. */
  Nested_xa_prepare_lex nested_xa_prepare_lex(thd, &xid);
  Sql_cmd_xa_prepare *cmd_executor = nested_xa_prepare_lex.get_cmd_executor();
  cmd_executor->set_delay_ok();
  if (cmd_executor->execute(thd)) {
    DBUG_RETURN(true);
  }

  m_pmmt = policy->clone_pmmt();

  DBUG_RETURN(false);
}

void Sql_cmd_xa_proc_ac_prepare::send_result(THD *thd, bool error) {
  DBUG_ENTER("Sql_cmd_xa_proc_ac_prepare::send_result");
  Protocol *protocol = nullptr;
  size_t uuid_len = 0;

  if (error) {
    assert(thd->is_error());
    DBUG_VOID_RETURN;
  }

  protocol = thd->get_protocol();

  if (m_proc->send_result_metadata(thd)) DBUG_VOID_RETURN;

  protocol->start_row();
  /** TODO: check uuid len <22-07-24, zanye.zjy> */
  uuid_len = std::min(strlen(server_uuid_ptr), MAX_SERVER_UUID_LENGTH);
  protocol->store_string(server_uuid_ptr, uuid_len, system_charset_info);
  protocol->store((ulonglong)m_trx_id);
  protocol->store((ulonglong)m_slot_ptr);
  protocol->store((ulonglong)m_pmmt.gcn);
  auto csr_str = get_csr_str(m_pmmt.csr);
  protocol->store_string(csr_str.str, csr_str.length, system_charset_info);
  if (protocol->end_row()) DBUG_VOID_RETURN;

  my_eof(thd);
  DBUG_VOID_RETURN;
}

/**
  dbms_xa.ac_commit(...)

  Do actually XA COMMIT when using async commit.
*/
/* Singleton instance */
Proc *Xa_proc_ac_commit::instance() {
  static Proc *proc = new Xa_proc_ac_commit(key_memory_xa_proc);
  return proc;
}

Sql_cmd *Xa_proc_ac_commit::invoke_cmd(THD *thd,
                                       mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

class Nested_xa_commit_lex {
 public:
  Nested_xa_commit_lex(THD *thd, XID *xid)
      : m_thd(thd), m_saved_lex(thd->lex) {
    thd->lex = new (thd->mem_root) st_lex_local;
    lex_start(thd);

    thd->lex->sql_command = SQLCOM_XA_COMMIT;
    thd->lex->m_sql_cmd = new (thd->mem_root) Sql_cmd_xa_commit(xid, XA_NONE);
  }

  Sql_cmd_xa_commit *get_cmd_executor() {
    return dynamic_cast<Sql_cmd_xa_commit *>(m_thd->lex->m_sql_cmd);
  }

  ~Nested_xa_commit_lex() {
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

bool Sql_cmd_xa_proc_ac_commit::get_master_parms(char server_uuid[],
                                                 xa_addr_t *addr) {
  char buff[MAX_SERVER_UUID_LENGTH];
  String str(buff, sizeof(buff), system_charset_info);
  String *res;
  size_t length;

  *addr = {};

  /* gtrid */
  res = (*m_list)[Xa_proc_ac_commit::XA_PARAM_UUID]->val_str(&str);
  length = res->length();
  if (length > MAX_SERVER_UUID_LENGTH) {
    return true;
  }

  if (length == 0) {
    return false;
  }

  if (strcmp(res->c_ptr(), server_uuid_ptr)) {
    my_error(ER_XA_PROC_MISMATCH_SERVER_UUID, MYF(0), res->c_ptr(), server_uuid_ptr);
    return true;
  }

  memcpy(server_uuid, res->c_ptr(), length);

  *addr = {
      (trx_id_t)(*m_list)[Xa_proc_ac_commit::XA_PARAM_MASTER_TRX_ID]->val_int(),
      (slot_ptr_t)(*m_list)[Xa_proc_ac_commit::XA_PARAM_MASTER_UBA]->val_int()};

  if (!addr->is_valid()) {
    my_error(ER_XA_PROC_INVALID_MASTER_INFO, MYF(0), server_uuid, addr->tid,
             addr->slot_ptr);
    return true;
  }

  return false;
}

bool Sql_cmd_xa_proc_ac_commit::pc_execute(THD *thd) {
  DBUG_ENTER("Sql_cmd_xa_proc_ac_commit::pc_execute");

  gcn_t hlc_gcn;
  XID xid;
  char server_uuid[MAX_SERVER_UUID_LENGTH + 1] = "";
  xa_addr_t addr;

  /** 0. Check retention for TXN (contains coordinator logs). */
  if (!trx_slot_check_retention()) {
    my_error(ER_XA_PROC_RETENTION_NOT_SATISFIED, MYF(0));
    DBUG_RETURN(true);
  }

  /** 1. parsed XID, master branch info, commit_gcn from parameters list. */
  if (get_xid(m_list, Xa_proc_ac_commit::XA_PARAM_GTRID, &xid)) {
    my_error(ER_XA_PROC_WRONG_XID, MYF(0), MAXGTRIDSIZE, MAXBQUALSIZE);
    DBUG_RETURN(true);
  }

  hlc_gcn = (*m_list)[Xa_proc_ac_commit::XA_PARAM_COMMIT_GCN]->val_int();
  if (hlc_gcn == GCN_NULL || hlc_gcn < GCN_INITIAL) {
    my_error(ER_XA_PROC_AC_INVALID_GCN, MYF(0), hlc_gcn);
    DBUG_RETURN(true);
  }

  if (get_master_parms(server_uuid, &addr)) {
    DBUG_RETURN(true);
  }

  /** 2. Take AC_commit_policy from pocket, and init it. */
  thd->cpolicy_ctx.activate_ac_commit(hlc_gcn, addr);

  /** 3. Do XA COMMIT. Will generate a new nested LEX to complete xa_commit.
  Because Sql_cmd_xa_commit::execute will depend on the state on LEX in
  some places. */
  Nested_xa_commit_lex nested_xa_commit_lex(thd, &xid);
  Sql_cmd_xa_commit *cmd_executor = nested_xa_commit_lex.get_cmd_executor();
  cmd_executor->set_delay_ok();
  if (cmd_executor->execute(thd)) {
    DBUG_RETURN(true);
  }

  DBUG_RETURN(false);
}

}  // namespace im
