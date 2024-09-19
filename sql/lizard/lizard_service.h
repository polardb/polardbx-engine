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

#ifndef LIZARD_LIZARD_SERVICE_TYPES_INCLUDED
#define LIZARD_LIZARD_SERVICE_TYPES_INCLUDED

#include "storage/innobase/include/lizard0gcs0service.h"
#include "storage/innobase/include/lizard0trx0service.h"
#include "storage/innobase/include/lizard0txn0service.h"

#include <string>

#include "my_dbug.h"

struct trx_undo_t;
struct txn_slot_t;

/*-----------------------------------------------------------------------------*/
/** MyGCN structure that represent user behavior */
/*-----------------------------------------------------------------------------*/
struct MyGCN {
 private:
  /** GCN tuple */
  gcn_tuple_t m_gtuple;
  /** proposal gcn or commit gcn. */
  bool is_proposal;
  /** Whether gcn is decided */
  bool has_decided;
  /** Whether SYS_GCN is pushed up by me */
  bool has_pushed_up;

 public:
  MyGCN()
      : m_gtuple(),
        is_proposal(false),
        has_decided(false),
        has_pushed_up(false) {}

  bool decided() const { return has_decided; }

  bool pushed_up() const { return has_pushed_up; }

  /**
    Assign external GCN for AC PREPARE.
    @param[in]    gcn           pre gcn
  */
  void assign_from_ac_prepare(gcn_t gcn);

  /**
    Assign external GCN for AC COMMIT.
    @param[in]    gcn           commit gcn
  */
  void assign_from_ac_commit(gcn_t gcn);

  /**
    Decide from external assigned GCN.
    @param[in]    gcn           assigned gcn
  */
  void assign_from_var(gcn_t gcn);

  /**
    Decide from Gcn_log_event. Might be proposal or non-proposal.
  */
  void assign_from_binlog(const gcn_tuple_t &gcn_tuple, bool proposal);

  /**
    Decide by loading local SYS_GCN.
  */
  void decide_if_null();

  /**
    Decide proposal_GCN.
    @param[in]    proposal
  */
  void decide_if_ac_prepare(const gcn_tuple_t &proposal);

  /**
    Decide commit by hlc. Only allowed to negotiate csr
    @param[in]    csr
    @param[in]    external_automatic
  */
  void decide_if_ac_commit(const csr_t csr, bool external_automatic);

  /**
    Some components, such as CDC, rely on the order of GCN and Binlog to meet
    certain conditions. For example, if the Binlog has the form:
    P1...C1...P2...C2, then it is assumed that the GCN of C1 must be less than
    C2. In regular TSO transactions, this is satisfied. This is because the
    timing is as follows:
    1.  XA PREPARE (P1)
    2.  GET GCN1
    3.1 XA COMMIT with GCN1 (C1)
    3.2 XA PREPARE (P2)
    4.  GET GCN2
    5.  XA COMMIT (C2)
    Step-3.1 and Step-3.2 can happen in any order, but Step-4 definitely occurs
    after Step-2.

    However, for Async Commit, the above conditions may not be met:
    1.  ac_prepare (P1 with PRE_GCN1 = 85)
    2.  decide PROPOSAL_GCN1 = max(PRE_GCN1 = 85, SYS_GCN = 90)
    3.1 ac_commit (C1 with GCN1 = 100), and SYS_GCN is not yet pushed up
    3.2 ac_prepare (P2 with PRE_GCN2 = 87)
    3.3 decide PROPOSAL_GCN2 = max(PRE_GCN2 = 87, SYS_GCN = 90)
    3.4 push up SYS_GCN = 100 because C1 (GCN1 = 100)
    4.  ac_commit (C2 with GCN2 = 87)
    Notes, 3.1-3.4 belong to the same BGC (Binlog Group Commit).
    The above situation obviously violates the preset assumptions because:
    GCN1 > GCN2

    Delaying the increase of SYS_GCN does not violate any distributed
    consistency guarantee, but in order to maintain the original assumption, the
    increase of SYS_GCN will be advanced before the decision of PROPOSAL_GCN to
    ensure that the following conditions are met:
    GCN1 <= PROPOSAL_GCN2 <= GCN2
  */
  void push_up_sys_gcn();

  bool is_pmmt_gcn() const { return !is_null() && is_proposal; }
  bool is_cmmt_gcn() const { return !is_null() && !is_proposal; }
  bool is_automatic() const { return csr() == csr_t::CSR_AUTOMATIC; }
  bool is_assigned() const { return csr() == csr_t::CSR_ASSIGNED; }
  gcn_t gcn() const { return m_gtuple.gcn; }
  csr_t csr() const { return m_gtuple.csr; }

  bool is_null() const {
#ifndef NDEBUG
    if (m_gtuple.is_null()) {
      assert(!is_proposal);
      assert(!has_pushed_up);
      assert(!has_decided);
    }
#endif
    return m_gtuple.is_null();
  }

  void reset() {
    m_gtuple.reset();
    is_proposal = false;
    has_decided = false;
    has_pushed_up = false;
  }

  std::string print() const {
    char buf[64];
    const char *csr_msg = nullptr;
    switch (csr()) {
      case csr_t::CSR_ASSIGNED:
        csr_msg = "csr_t::CSR_ASSIGNED";
        break;
      case csr_t::CSR_AUTOMATIC:
        csr_msg = "csr_t::CSR_AUTOMATIC";
        break;
    }

    snprintf(buf, sizeof(buf), "Proposal = %s, GCN_SRC = %s, gcn_val = %lu",
             is_proposal ? "true" : "false", csr_msg, gcn());
    return buf;
  }

  /** Clone proposal gcn
   *
   * @retval	propose gcn */
  gcn_tuple_t clone_pmmt() const {
    assert(is_pmmt_gcn());
    return tuple();
  }

  /** Clone commit gcn
   *
   * @retval	commit gcn */
  gcn_tuple_t clone_cmmt() const {
    assert(is_cmmt_gcn());
    return tuple();
  }

 private:
  gcn_tuple_t tuple() const { return m_gtuple; }
  /**
    Copy from pmmt which must be already pushed up.
    @param[in]    gcn_tuple     {gcn, csr}
  */
  void copy_pmmt(const gcn_tuple_t &gcn_tuple);

  /**
    Copy from cmmt which must be already pushed up.
    @param[in]    gcn_tuple     {gcn, csr}
  */
  void copy_cmmt(const gcn_tuple_t &gcn_tuple);

 public:
  friend struct commit_mark_t;
  friend struct proposal_mark_t;
};
/*-----------------------------------------------------------------------------*/
/** GCN vision represent user query readview. */
struct MyVisionGCN {
 public:
  MyVisionGCN() { reset(); }

  MyVisionGCN(csr_t _csr, gcn_t _gcn, scn_t _scn) {
    assert(_csr == csr_t::CSR_ASSIGNED ? _scn == SCN_NULL : _scn != SCN_NULL);

    csr = _csr;
    gcn = _gcn;
    current_scn = _scn;
  }

  void reset() {
    gcn = GCN_NULL;
    csr = CSR_AUTOMATIC;
    current_scn = SCN_NULL;
  }

  bool is_null() { return gcn == GCN_NULL; }

  gcn_t gcn;
  csr_t csr;
  scn_t current_scn;
};

enum XA_status {
  /** Another seesion has attached XA. */
  ATTACHED,
  /** Detached XA, the real state of the XA is PREPARE_IN_TC. */
  DETACHED_PREPARE,
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

struct MyXAInfo {
 public:
  MyXAInfo(XA_status s) : status(s), gcn(), slot(), branch(), maddr() {}

  XA_status status;

  /** Proposal GCN when prepare, or commit GCN after commit / rollback */
  MyGCN gcn;

  /* XA branch ID */
  xa_addr_t slot;

  /* The count branch info of the global transaction. */
  xa_branch_t branch;

  /* XA master branch ID */
  xa_addr_t maddr;

 public:
  bool is_null() {
    return gcn.is_null() && slot.is_null() && branch.is_null() &&
           maddr.is_null();
  }
  /** Init xa attributes from txn undo when active or prepare.
   *
   * @param[in]		trx id
   * @param[in]		txn undo if allocate */
  void init_by_txn_undo(const trx_id_t tid, const trx_undo_t *txn_undo);

  /** Init xa attributes from txn slot after transaction finished.
   *
   * @param[in]		txn slot */
  void init_by_txn_slot(const txn_slot_t *txn_slot);
};

const MyXAInfo MY_XA_INFO_ATTACH(XA_status::ATTACHED);

const MyXAInfo MY_XA_INFO_FORGET(XA_status::NOTSTART_OR_FORGET);

const MyXAInfo MY_XA_INFO_NOT_SUPPORT(XA_status::NOT_SUPPORT);

namespace lizard {
namespace xa {

enum Transaction_state {
  TRANS_STATE_COMMITTED = 0,
  TRANS_STATE_ROLLBACK = 1,
  TRANS_STATE_ROLLBACKING_BACKGROUND = 2,
  TRANS_STATE_UNKNOWN = 3,
};

struct Transaction_info {
  Transaction_state state;
  MyGCN gcn;
};

}  // namespace xa
}  // namespace lizard

namespace lizard {

/** Rollback segment statistics */
struct rseg_stat_t {
 public:
  explicit rseg_stat_t()
      : rseg_pages(0),
        history_length(0),
        history_pages(0),
        secondary_length(0),
        secondary_pages(0) {}

 public:
  uint64_t rseg_pages;
  uint64_t history_length;
  uint64_t history_pages;
  uint64_t secondary_length;
  uint64_t secondary_pages;
};


/** Undo tablespace status include purge/erase state */
class trunc_status_t {
 public:
  trunc_status_t()
      : undo_name(),
        file_pages(0),
        rseg_stat(),
        oldest_history_utc(0),
        oldest_secondary_utc(0),
        oldest_history_scn(0),
        oldest_secondary_scn(0),
        oldest_history_gcn(0),
        oldest_secondary_gcn(0) {}

  void aggregate(const rseg_stat_t &value) {
    rseg_stat.rseg_pages += value.rseg_pages;
    rseg_stat.history_length += value.history_length;
    rseg_stat.history_pages += value.history_pages;
    rseg_stat.secondary_length += value.secondary_length;
    rseg_stat.secondary_pages += value.secondary_pages;
  }

  std::string undo_name;
  uint64_t file_pages;

  rseg_stat_t rseg_stat;

  utc_t oldest_history_utc;
  utc_t oldest_secondary_utc;

  scn_t oldest_history_scn;
  scn_t oldest_secondary_scn;

  gcn_t oldest_history_gcn;
  gcn_t oldest_secondary_gcn;
};

class purge_status_t {
 public:
  purge_status_t()
      : history_length(0),
        current_scn(0),
        current_gcn(0),
        purged_scn(0),
        purged_gcn(0),
        erased_scn(0),
        erased_gcn(0) {}

  uint64_t history_length;

  scn_t current_scn;
  gcn_t current_gcn;

  scn_t purged_scn;
  gcn_t purged_gcn;

  scn_t erased_scn;
  gcn_t erased_gcn;
};

}  // namespace lizard


#endif
