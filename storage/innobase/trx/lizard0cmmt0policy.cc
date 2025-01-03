/*****************************************************************************

Copyright (c) 2013, 2024, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file trx/lizard0cmmt0policy.cc
  Lizard XA policy structure.

 Created 2024-10-31 by Jiyang.zhang
 *******************************************************/
#include "sql/tc_log.h"
#include "sql/binlog.h"

#include "handler/lizard0ha_innodb.h"

#include "trx0trx.h"

#include "sql/xa/lizard_cmmt_policy.h"
#include "sql/gcn_log_event.h"

#include "lizard0gcs.h"
#include "lizard0undo.h"
#include "lizard0undo0retent.h"
#include "sql/mysqld.h"

using namespace binary_log;

namespace lizard {

/*-----------------------------------------------------------------------------*/
/** Single_shard_policy */
/*-----------------------------------------------------------------------------*/
void Single_shard_policy::decide(THD *thd) {
  if (m_decided) return;

  /** Although the external gcn is not needed for single shard policy,
   * we will set it if it is specified by users. */
  if (thd->variables.innodb_commit_gcn != GCN_NULL) {
    m_commit_gcn = {thd->variables.innodb_commit_gcn, CSR_ASSIGNED};
    gcs_set_gcn_if_bigger(m_commit_gcn.gcn);
  } else {
    m_commit_gcn = {gcs_load_gcn(), CSR_AUTOMATIC};
  }
  m_decided = true;
}

void Single_shard_policy::copy_to_trx(trx_t *trx) const {
  trx->txn_desc.copy_xa_when_commit(m_commit_gcn, XA_ADDR_NULL);
}

void Single_shard_policy::build_gcn_event(Gcn_event *event) const {
  event->build_gcn(m_commit_gcn, false);
}

void Single_shard_policy::reset() {
  m_commit_gcn.reset();
  m_decided = false;
}

[[nodiscard]] Commit_policy *Single_shard_policy::clone() const {
  return new Single_shard_policy(*this);
}

[[nodiscard]] Commit_policy *Single_shard_policy::clone(
    MEM_ROOT *mem_root) const {
  return new (mem_root) Single_shard_policy(*this);
}

/*-----------------------------------------------------------------------------*/
/** XA_commit_policy */
/*-----------------------------------------------------------------------------*/

void XA_commit_policy::init(gcn_t commit_gcn) {
  ut_ad(m_commit_gcn.is_null());

  /** If commit_gcn has been specified, use it directly */
  if (commit_gcn != GCN_NULL) {
    m_commit_gcn = {commit_gcn, CSR_ASSIGNED};
  }
}
void XA_commit_policy::decide(THD *thd) {
  if (m_decided) return;

  if (!m_commit_gcn.is_null()) {
    gcs_set_gcn_if_bigger(m_commit_gcn.gcn);
  } else {
    /** If commit_gcn has not been specified, load it from gcs */
    m_commit_gcn = {gcs_load_gcn(), CSR_AUTOMATIC};
  }
  m_decided = true;
}
void XA_commit_policy::copy_to_trx(trx_t *trx) const {
  ut_ad(trx);
  ut_ad(!m_commit_gcn.is_null());
  trx->txn_desc.copy_xa_when_commit(m_commit_gcn, XA_ADDR_NULL);
}

void XA_commit_policy::build_gcn_event(Gcn_event *event) const {
  event->build_gcn(m_commit_gcn, false);
}

void XA_commit_policy::reset() {
  m_commit_gcn.reset();
  m_decided = false;
}

[[nodiscard]] Commit_policy *XA_commit_policy::clone() const {
  return new XA_commit_policy(*this);
}

[[nodiscard]] Commit_policy *XA_commit_policy::clone(MEM_ROOT *mem_root) const {
  return new (mem_root) XA_commit_policy(*this);
}

/*-----------------------------------------------------------------------------*/
/** AC_prepare_policy */
/*-----------------------------------------------------------------------------*/
void AC_prepare_policy::init(gcn_t pre_commit_gcn, const xa_branch_t &branch) {
  ut_ad(m_pre_commit_gcn == GCN_NULL);
  m_pre_commit_gcn = pre_commit_gcn;
  m_branch = branch;
}

void AC_prepare_policy::decide(THD *thd) {
  if (m_decided) return;

  gcn_t sys_gcn;

  ut_a(m_proposal_gcn.is_null());

  sys_gcn = gcs_load_gcn();
  if (sys_gcn > m_pre_commit_gcn) {
    m_proposal_gcn = {sys_gcn, CSR_AUTOMATIC};
  } else {
    m_proposal_gcn = {m_pre_commit_gcn, CSR_ASSIGNED};
  }

  if (m_proposal_gcn.csr == CSR_ASSIGNED) {
    gcs_set_gcn_if_bigger(m_proposal_gcn.gcn);
  }
  m_decided = true;
}

void AC_prepare_policy::copy_to_trx(trx_t *trx) const {
  ut_a(!m_proposal_gcn.is_null());
  ut_a(!m_branch.is_null());

  ut_a(trx->txn_desc.pmmt.is_null());

  trx->txn_desc.copy_xa_when_prepare(m_proposal_gcn, m_branch);
}

void AC_prepare_policy::build_gcn_event(Gcn_event *event) const {
  event->build_gcn(m_proposal_gcn, true);
  event->build_branch(m_branch);
}

void AC_prepare_policy::reset() {
  m_pre_commit_gcn = GCN_NULL;
  m_branch = XA_BRANCH_NULL;
  m_proposal_gcn.reset();
  m_decided = false;
}

[[nodiscard]] Commit_policy *AC_prepare_policy::clone() const {
  return new AC_prepare_policy(*this);
}

[[nodiscard]] Commit_policy *AC_prepare_policy::clone(
    MEM_ROOT *mem_root) const {
  return new (mem_root) AC_prepare_policy(*this);
}

/*-----------------------------------------------------------------------------*/
/** AC_commit_policy */
/*-----------------------------------------------------------------------------*/
void AC_commit_policy::init(gcn_t hlc_gcn, const xa_addr_t &master_addr) {
  ut_ad(m_hlc_gcn == GCN_NULL);
  m_hlc_gcn = hlc_gcn;
  m_master_addr = master_addr;
}

void AC_commit_policy::decide(THD *thd) {
  if (m_decided) return;

  /* trx_t *trx; */
  proposal_mark_t pmmt;
  const trx_undo_t *txn_undo = nullptr;
  bool external_automatic;
  csr_t csr;
  trx_t *trx;

  trx = innobase_get_trx_by_thd(thd);

  /** For read-only transactions, xa prepare would commit them instead of
  leaving them commited by xa commit. As a result, trx would be nullptr. */
  if (trx == nullptr) {
    m_decided = true;
    return;
  }

  ut_a(m_hlc_gcn != GCN_NULL);
  ut_a(m_commit_gcn.is_null());

  /**
    Async Commit:
    1. Decide commit GCN by external GCN and proposal GCN.
    2. Decide master address.
  */

  if ((txn_undo = trx_undo_get_txn(trx))) {
    pmmt = txn_undo->pmmt;
  }

  if (pmmt.is_null()) {
    /**
      If no TXN, can not do Async Commit. Like:
      xa start '';
      ...update...
      xa end '';
      xa prepare '';
      call ac_commit(...);
    */
    /** Pretend to normal XA COMMIT rather than AC COMMIT. */
    csr = CSR_ASSIGNED;
  } else {
    if (m_hlc_gcn < pmmt.gcn) {
      char err_msg[128];
      snprintf(
          err_msg, sizeof(err_msg),
          "Transaction (%s), external commit gcn (%lu) < proposal gcn (%lu) "
          "when commit.",
          trx->xid->key(), m_hlc_gcn, pmmt.gcn);
      lizard_warn(ER_LIZARD) << err_msg;
    }

    external_automatic = (m_hlc_gcn > pmmt.gcn);
    csr = external_automatic ? CSR_AUTOMATIC : pmmt.csr;
  }

  m_commit_gcn = {m_hlc_gcn, csr};
  m_master_addr.decide_if_ac_commit(trx);
  m_decided = true;

  gcs_set_gcn_if_bigger(m_hlc_gcn);
}

void AC_commit_policy::copy_to_trx(trx_t *trx) const {
  ut_ad(trx);

  trx->txn_desc.copy_xa_when_commit(m_commit_gcn, m_master_addr);
}

void AC_commit_policy::build_gcn_event(Gcn_event *event) const {
  event->build_gcn(m_commit_gcn, false);
}

void AC_commit_policy::reset() {
  m_hlc_gcn = GCN_NULL;
  m_master_addr = XA_ADDR_NULL;
  m_commit_gcn.reset();
  m_decided = false;
}

[[nodiscard]] Commit_policy *AC_commit_policy::clone() const {
  return new AC_commit_policy(*this);
}

[[nodiscard]] Commit_policy *AC_commit_policy::clone(MEM_ROOT *mem_root) const {
  return new (mem_root) AC_commit_policy(*this);
}

/*-----------------------------------------------------------------------------*/
/** Binlog_ac_prepare_policy */
/*-----------------------------------------------------------------------------*/
Binlog_ac_prepare_policy::Binlog_ac_prepare_policy()
    : m_proposal_gcn(), m_branch() {}

void Binlog_ac_prepare_policy::init(const gcn_tuple_t &tuple,
                                    const xa_branch_t &branch) {
  ut_ad(!tuple.is_null());
  m_proposal_gcn = tuple;
  m_branch = branch;
  /** Might will not run interface ::decide. */
  gcs_set_gcn_if_bigger(m_proposal_gcn.gcn);
}

void Binlog_ac_prepare_policy::decide(THD *thd) {
  if (m_decided) return;
  /** Already decided. Rplica from binlog. Do nothing. */
  m_decided = true;
  gcs_set_gcn_if_bigger(m_proposal_gcn.gcn);
}

void Binlog_ac_prepare_policy::copy_to_trx(trx_t *trx) const {
  trx->txn_desc.copy_xa_when_prepare(m_proposal_gcn, m_branch);
}

void Binlog_ac_prepare_policy::build_gcn_event(Gcn_event *event) const {
  event->build_gcn(m_proposal_gcn, true);
  event->build_branch(m_branch);
}

void Binlog_ac_prepare_policy::reset() {
  m_proposal_gcn.reset();
  m_branch.reset();
  m_decided = false;
}

[[nodiscard]] Commit_policy *Binlog_ac_prepare_policy::clone() const {
  return new Binlog_ac_prepare_policy(*this);
}

[[nodiscard]] Commit_policy *Binlog_ac_prepare_policy::clone(
    MEM_ROOT *mem_root) const {
  return new (mem_root) Binlog_ac_prepare_policy(*this);
}

/*-----------------------------------------------------------------------------*/
/** Binlog_commit_policy */
/*-----------------------------------------------------------------------------*/
Binlog_commit_policy::Binlog_commit_policy() : m_commit_gcn() {}

void Binlog_commit_policy::init(const gcn_tuple_t &tuple) {
  m_commit_gcn = tuple;
  gcs_set_gcn_if_bigger(m_commit_gcn.gcn);
}

void Binlog_commit_policy::decide(THD *thd) {
  if (m_decided) return;
  /** Already decided. Rplica from binlog. Do nothing. */
  m_decided = true;
}

void Binlog_commit_policy::copy_to_trx(trx_t *trx) const {
  ut_ad(trx);
  trx->txn_desc.copy_xa_when_commit(m_commit_gcn, XA_ADDR_NULL);
}

void Binlog_commit_policy::build_gcn_event(Gcn_event *event) const {
  event->build_gcn(m_commit_gcn, false);
}

void Binlog_commit_policy::reset() {
  m_commit_gcn.reset();
  m_decided = false;
}

[[nodiscard]] Commit_policy *Binlog_commit_policy::clone() const {
  return new Binlog_commit_policy(*this);
}

[[nodiscard]] Commit_policy *Binlog_commit_policy::clone(
    MEM_ROOT *mem_root) const {
  return new (mem_root) Binlog_commit_policy(*this);
}

/*-----------------------------------------------------------------------------*/
/** CMMIT_policy_container */
/*-----------------------------------------------------------------------------*/

/** Activate ac prepare policy
 *
 * @param[in]		proposal gcn
 * @param[in]		branch info
 *
 * @retval		ac prepare policy handler.*/
AC_prepare_policy *Commit_policy_ctx::activate_ac_prepare(
    const gcn_t &pre_commit_gcn, const xa_branch_t &branch) {
  ut_ad(is_implicit_policy());

  m_ac_prepare.reset();
  m_ac_prepare.init(pre_commit_gcn, branch);

  m_current = &m_ac_prepare;
  m_type = AC_PREPARE;

  return &m_ac_prepare;
}

/**
 * When AC_commit was called, we need to activate AC_commit_policy.
 *
 * @param[in] commit_gcn the real commit gcn specified by user.
 * @param[in] master_addr the master transaction slot physical address
 */
void Commit_policy_ctx::activate_ac_commit(const gcn_t &commit_gcn,
                                               const xa_addr_t &master_addr) {
  ut_ad(is_implicit_policy());

  m_ac_commit.reset();
  m_ac_commit.init(commit_gcn, master_addr);

  m_current = &m_ac_commit;
  m_type = AC_COMMIT;
}

/**
 * When Xa Commit(not one phase) or XA Rollback is excuted, we should check if
 * we should active XA_commit_policy. There are three cases:
 * 1. For AC_commit which also calls Xa Commit, we should do nothing cause that
 * AC_commit_policy has been activated.
 *
 * 2. For normal XA commit or XA Rollback exectued in replica, we should do
 * nothing cause that Binlog_commit_policy has been activated.
 *
 * 3. Otherwise, we should active XA_commit_policy.
 *
 * @param[in] commit_gcn: commit gcn
 */
void Commit_policy_ctx::activate_xa_commit(const gcn_t &commit_gcn) {
  if (is_explicit_policy()) {
    /** When execute async commit or replay binlog of XA Commit, AC_commit
     * policy or Binlog_commit policy will be activated before executing
     * sql_cmd_xa_commit. So we just skip here cause that XA_commit_policy is
     * not needed. Notice that this does not violate the rules of state
     * transition
     */
    ut_ad(m_type == AC_COMMIT || m_type == BINLOG_COMMIT);
    return;
  }
  ut_ad(is_implicit_policy());

  m_xa_commit.reset();
  m_xa_commit.init(commit_gcn);

  m_current = &m_xa_commit;
  m_type = XA_COMMIT;
}

/**
 * When dealing with binlog(i.e. recovery or replay the binlog), we should
 * activate Binlog commit policy. There are two cases:
 * 1. If we are handling the AC_prepare, we have to set xa branch info to trxs,
 * Binlog_ac_prepare_policy need to be activated as a result.
 *
 * 2. Otherwise, no matter what command we execute(i.e XA Prepare/XA Commit or
 * normal commit), We just want to set gcn specified by Gcn_log_event to trxs.
 * Binlog_commit_policy is activated as a result.
 *
 * @param[in] gcn_event GCN_log_event
 */
void Commit_policy_ctx::activate_binlog(const Gcn_log_event *gcn_event) {
  ut_ad(is_implicit_policy());

  if (gcn_event->is_pmmt_gcn()) {
    m_binlog_ac_prepare.reset();
    m_binlog_ac_prepare.init(gcn_event->clone_pmmt(),
                             gcn_event->clone_branch());

    m_type = BINLOG_AC_PREPARE;
    m_current = &m_binlog_ac_prepare;
  } else {
    m_binlog_commit.reset();
    m_binlog_commit.init(gcn_event->clone_cmmt());

    m_type = BINLOG_COMMIT;
    m_current = &m_binlog_commit;
  }
}

void Commit_policy_ctx::activate_single_shard() {
  m_single_shard.reset();
  m_current = &m_single_shard;
  m_type = SINGLE_SHARD;
}

/*-----------------------------------------------------------------------------*/
/** Policy interface */
/*-----------------------------------------------------------------------------*/

/**
 * Decide the real GCN based on current commit policy.
 *
 * @param thd
 */
void commit_policy_decide(THD *thd) {
  thd->cpolicy_ctx.current_policy()->decide(thd);
}

/**
 * Copy the decided gcn to trx based on current commit policy.
 *
 * @param[in] thd
 * @param[in/out] trx
 */
void commit_policy_copy(THD *thd, trx_t *trx) {
  if (!thd->cpolicy_ctx.current_policy()->has_decided()) {
    commit_policy_decide(thd);
  }
  ut_ad(thd->cpolicy_ctx.current_policy()->has_decided());

  thd->cpolicy_ctx.current_policy()->copy_to_trx(trx);
}
}  // namespace lizard
