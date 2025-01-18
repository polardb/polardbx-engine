/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file trx/lizard0txn.cc
  Lizard transaction management.

 Created 2020-03-27 by Jianwei.zhao
 *******************************************************/

#include "trx0rseg.h"

#include "lizard0txn.h"
#include "lizard0scn.h"
#include "lizard0txn0space.h"
#include "lizard0undo.h"
#include "lizard0xa.h"

/** assemble undo ptr */
void txn_desc_t::assemble(const commit_mark_t &mark,
                          const slot_addr_t &slot_addr, bool is_slave_arg) {
  bool state = (mark.scn != SCN_NULL);
  if (state) {
    assert_commit_mark_allocated(mark);
  } else {
    assert_commit_mark_initial(mark);
  }
  cmmt = mark;
  undo_addr_t undo_addr(slot_addr, state, mark.csr, is_slave_arg);
  this->undo_ptr = undo_addr.encode();
}

/** assemble undo ptr */
void txn_desc_t::assemble_undo_ptr(const slot_addr_t &slot_addr) {
  /** Only used to generate raw undo ptr for now */
  assert_commit_mark_initial(cmmt);
  assert(cmmt.csr == CSR_AUTOMATIC);

  undo_addr_t undo_addr(slot_addr);
  this->undo_ptr = undo_addr.encode();
}

void txn_desc_t::resurrect_xa(const proposal_mark_t &txn_pmmt,
                              const xa_branch_t &txn_branch,
                              const xa_addr_t &txn_maddr) {
  pmmt = txn_pmmt;
  branch = txn_branch;
  maddr = txn_maddr;
}

void txn_desc_t::copy_xa_when_prepare(const gcn_tuple_t &xa_gcn,
                                      const xa_branch_t &xa_branch) {
  pmmt.csr = xa_gcn.csr;
  pmmt.gcn = xa_gcn.gcn;

  ut_ad(!xa_branch.is_null());
  branch = xa_branch;
}

void txn_desc_t::copy_xa_when_commit(const gcn_tuple_t &xa_gcn,
                                     const xa_addr_t &xa_maddr) {
  cmmt.csr = xa_gcn.csr;
  cmmt.gcn = xa_gcn.gcn;
  maddr = xa_maddr;
}

namespace lizard {

slot_addr_t txn_sys_t::SLOT_ADDR_NO_REDO = {
    SLOT_SPACE_ID_FAKE, SLOT_PAGE_NO_FAKE, SLOT_OFFSET_NO_REDO};

slot_addr_t txn_sys_t::SLOT_ADDR_NULL = {0, 0, 0};

/**
 * prepare_in_tc is treated as first phase commit within 2PC, so we should
 * mark our transaction system like trx_commit_mark, but only proposal xa
 * transaction will really do something.
 *
 * @param[in]		trx	transaction context
 * @param[in/out]	undo	undo memory structure
 * @param[in]		hdr	undo log hdr
 * @param[in]		mtr
 *
 * @retval		proposal commit mark.
 * */
proposal_mark_t trx_prepare_mark(trx_t *trx, trx_undo_t *undo,
                                 trx_ulogf_t *log_hdr, mtr_t *mtr) {
  xa_branch_t branch;
  proposal_mark_t pmmt;
  ut_ad(undo->pmmt.is_null());

  /** Only write proposal info in TXN. */
  if (!(undo->flag & TRX_UNDO_FLAG_TXN)) {
    return pmmt;
  }

  /** Write proposal mark. */
  pmmt = trx->txn_desc.pmmt;
  branch = trx->txn_desc.branch;
  if (pmmt.is_null()) {
    return pmmt;
  }

  /** Can't be NULL. See Sql_cmd_xa_proc_ac_prepare. */
  ut_a(!branch.is_null());

  /** Generate proposal gcn */
  pmmt = gcs->new_prepare(trx, mtr);

  ut_ad(undo->tags_allocated());
  /** 1. Set async commit flag. */
  undo->allocate_ac_prepare();
  mlog_write_ulint(log_hdr + TXN_UNDO_LOG_EXT_STORAGE, undo->xes_storage,
                   MLOG_1BYTE, mtr);

  /** 2. Write proposal mark. */
  undo->pmmt = pmmt;
  mlog_write_ull(log_hdr + TXN_UNDO_LOG_XES_AC_PROPOSAL_GCN, pmmt.gcn, mtr);

  if (pmmt.csr == CSR_ASSIGNED) {
    undo->set_ac_csr_assigned_on_tags();
  } else {
    ut_a(!undo->ac_csr_assigned_on_tags());
  }
  mlog_write_ulint(log_hdr + TXN_UNDO_LOG_XES_TAGS, undo->tags, MLOG_2BYTES,
                   mtr);

  undo->branch = branch;
  txn_undo_hdr_write_xa_branch(log_hdr, branch, mtr);

  return pmmt;
}

/**
  Assign a new commit scn for the transaction when commit

  @param[in]      trx       current transaction
  @param[in/out]  cmmt_ptr   Commit scn which was generated only once
  @param[in]      undo      txn undo log
  @param[in]      undo page txn undo log header page
  @param[in]      offset    txn undo log header offset
  @param[in]      mtr       mini transaction
  @param[out]     serialised

  @retval         scn       commit scn struture
*/
commit_mark_t trx_commit_mark(trx_t *trx, commit_mark_t *cmmt_ptr,
                              trx_undo_t *undo, page_t *undo_hdr_page,
                              ulint hdr_offset, bool *serialised, mtr_t *mtr) {
  trx_usegf_t *seg_hdr;
  trx_ulogf_t *undo_hdr;
  commit_mark_t cmmt;

  ut_ad(gcs);
  ut_ad(trx && undo && undo_hdr_page && mtr);

  ut_ad((trx->rsegs.m_txn.rseg != nullptr &&
         mutex_own(&trx->rsegs.m_txn.rseg->mutex)) ||
        trx->rsegs.m_noredo.update_undo == undo);
  /** Attention: Some transaction commit directly from ACTIVE */

  /** TODO:
      If it didn't have prepare state, then only flush redo log once when
      commit, It maybe cause vision problem, other session has see the data,
      but scn redo log is lost.
  */
  ut_ad(trx_state_eq(trx, TRX_STATE_PREPARED) ||
        trx_state_eq(trx, TRX_STATE_ACTIVE));

  trx_undo_page_validation(undo_hdr_page);

  /** Here we didn't hold trx_sys mutex */
  ut_ad(!trx_sys_mutex_own());

  ut_ad(!cmmt_ptr || commit_mark_state(*cmmt_ptr) == SCN_STATE_ALLOCATED);

  /** Here must hold the X lock on the page */
  ut_ad(mtr_memo_contains_page(mtr, undo_hdr_page, MTR_MEMO_PAGE_X_FIX));

  seg_hdr = undo_hdr_page + TRX_UNDO_SEG_HDR;
  ulint state = mach_read_from_2(seg_hdr + TRX_UNDO_STATE);

  /** TXN undo log must be finished */
  ut_a(state == TRX_UNDO_CACHED || state == TRX_UNDO_TO_PURGE);

  /** Commit must be the last log hdr */
  ut_ad(hdr_offset == mach_read_from_2(seg_hdr + TRX_UNDO_LAST_LOG));

  undo_hdr = undo_hdr_page + hdr_offset;
  ut_ad(!trx_undo_hdr_cmmt_committed(undo_hdr, mtr));

  assert_lizard_min_safe_scn_valid();

  /* Step 1: modify trx->scn */
  if (cmmt_ptr == nullptr) {
    ut_ad(*serialised == false);
    *serialised = true;
    cmmt = gcs->new_commit(trx, mtr);
  } else {
    assert_trx_commit_mark_allocated(trx);
    cmmt = *cmmt_ptr;
    ut_ad(trx->txn_desc.cmmt.scn == cmmt.scn);
    ut_ad(trx->txn_desc.cmmt.gcn == cmmt.gcn);
  }
  ut_ad(commit_mark_state(cmmt) == SCN_STATE_ALLOCATED);

  /* Step 2: modify undo header. */
  trx_undo_hdr_write_cmmt(undo_hdr, cmmt, mtr);
  ut_ad(trx_undo_hdr_cmmt_committed(undo_hdr, mtr));

  /* Step 3: modify undo->scn */
  assert_undo_commit_mark_initial(undo);
  undo->cmmt = cmmt;

  assert_lizard_min_safe_scn_valid();
  return cmmt;
}

/** Load min active trx id which is cached within trx struct. */
trx_id_t trx_load_min_active_tid(const trx_t *trx) {
  return trx->min_active_tid.load();
}

/** Commit txn memory structure after txn slot mini-transaction commit.
 *
 * @param[in/out]		trx
 * @param[in]			serialised */
void txn_commit_in_memory(trx_t *trx, bool serialised) {
  if (serialised) {
    trx_mutex_enter(trx);
    /** Commit undo ptr */
    undo_ptr_set_commit(&trx->txn_desc.undo_ptr, trx->txn_desc.cmmt.csr,
                        !trx->txn_desc.maddr.is_null());
    trx_mutex_exit(trx);
    ut_ad(trx->txn_desc.is_whole_committed());

    /** Update tcn cache. */
    lizard::trx_cache_tcn(trx, serialised);
  } else {
    ut_ad(!trx->txn_desc.alloced());
  }
}

/** Get active transaction according to txn rec.
 *
 * @param[in/out]	txn rec
 * @param[in]		increment ref count
 * @param[in]		optional trx which is used to get local min active tid
 *
 * @retval	txn rw object.
 * */
txn_rw_t txn_rw_is_active(txn_rec_t *txn_rec, bool do_ref_count,
                          const trx_t *optional_trx) {
  txn_rw_t txn_rw;
  buf_block_t *block = nullptr;
  bool active = false;
  trx_id_t rec_tid = txn_rec->trx_id;

  ut_ad(rec_tid > 0);

  /** if record tid is less than min active id*/
  if (optional_trx && rec_tid < trx_load_min_active_tid(optional_trx)) {
    txn_rw.reset();
    return txn_rw;
  }

  /** lookup txn slot. */
  std::tie(active, block) = txn_slot_is_active(txn_rec, do_ref_count);
  if (!active) {
    ut_ad(txn_rec->is_committed());
    txn_rw.reset();
    return txn_rw;
  }

  ut_ad(txn_rec->is_active());
  ut_ad(block != nullptr || !do_ref_count);

  /** Maybe commit walk here. */
  trx_t *trx = trx_rw_is_active(txn_rec->trx_id, do_ref_count);
  txn_rw = {trx, block, txn_rec->undo_ptr};

  /** Find active trx. */
  if (!trx) {
    /** Unfix block if has. */
    txn_rw.release_slot();
    txn_rw.reset();
  } else {
    ut_ad(txn_rw.is_active());
    ut_ad(txn_rw.was_slot_fixed() || !do_ref_count);
  }

  return txn_rw;
}

/** Get active transaction according to txn rw.
 *
 * @param[in]		txn rw
 * @param[in]		increment ref count
 *
 * @retval	txn rw object.
 * */
txn_rw_t txn_rw_is_active(const txn_rw_t &txn_rw, bool do_ref_count) {
  ut_ad(txn_rw.is_active());

  txn_rec_t txn_rec = {txn_rw.trx->id, SCN_NULL, txn_rw.undo_ptr, GCN_NULL};
  return txn_rw_is_active(&txn_rec, do_ref_count, nullptr);
}

/** Get active transaction according to trx id and slot.
 *
 * @param[in]		txn identity
 * @param[in]		increment ref count
 *
 * @retval	txn rw object.
 * */
txn_rw_t txn_rw_is_active(const txn_id_t &txn_id, bool do_ref_count) {
  txn_rw_t rw;
  if (!undo_ptr_is_active(txn_id.undo_ptr)) {
    return rw;
  }

  txn_rec_t txn_rec = {txn_id.trx_id, SCN_NULL, txn_id.undo_ptr, GCN_NULL};
  return txn_rw_is_active(&txn_rec, do_ref_count, nullptr);
}

/**
 * Judge transaction have committed through txn slot.
 *
 * @param[in]	txn rw object
 *
 * @retval	true	Committed
 * @retval	false	Active
 * */
bool txn_rw_is_committed_in_memory(const txn_rw_t &txn_rw) {
  ut_ad(txn_rw.is_active());
  ut_ad(txn_rw.was_slot_fixed());

  txn_rec_t txn_rec = {txn_rw.trx->id, SCN_NULL, txn_rw.undo_ptr, GCN_NULL};
  ut_ad(txn_rec.is_active());

  return !txn_slot_is_active(&txn_rec, false).first;
}

/**
   Resurrect txn undo log segment,
   Maybe the trx didn't have m_redo update/insert undo log.

   There are three different state:

   1) TXN_UNDO N INSERT_UNDO Y  UPDATE_UNO N
      : The transaction has committed, but rseg->slot of insert undo
        didn't set FIL_NULL, since cleanup insert undo is in other mini
        transaction;
        But here it will not commit again, just cleanup.

   2) TXN_UNDO Y UPDATE_UNDO N INSERT_UNDO N
      : The transaction only allocate txn undo log header, then instance
        crashed;

   3) TXN_UNDO Y UPDATE_UNDO/INSERT_UNDO (one Y or two Y)

   We didn't allowed only have UPDATE UNDO but didn't have txn undo;
   Since the txn undo allocation is prior to undate undo;

*/
void trx_resurrect_txn(trx_t *trx, trx_undo_t *undo, trx_rseg_t *rseg) {
  undo_addr_t undo_addr;

  ut_ad(trx->rsegs.m_txn.rseg == nullptr);
  ut_ad(undo->empty);

  /** Already has update/insert undo */
  if (trx->rsegs.m_redo.rseg != nullptr) {
    ut_ad(undo->trx_id == trx->id);
    ut_ad(trx->is_recovered);
    if (trx->rsegs.m_redo.update_undo != nullptr &&
        trx->state == TRX_STATE_COMMITTED_IN_MEMORY) {
      assert_trx_commit_mark_allocated(trx);
      assert_trx_undo_ptr_initial(trx);
      lizard_ut_ad(trx->txn_desc.cmmt == undo->cmmt);
      ut_ad(trx->rsegs.m_redo.update_undo->slot_addr == undo->slot_addr);
    } else {
      assert_trx_commit_mark_initial(trx);
    }

    if (trx->rsegs.m_redo.update_undo != nullptr) {
      ut_ad(trx->rsegs.m_redo.update_undo->slot_addr == undo->slot_addr);
    } else if (trx->rsegs.m_redo.insert_undo != nullptr) {
      ut_ad(trx->rsegs.m_redo.insert_undo->slot_addr == undo->slot_addr);
    }
  } else {
    /** It must be the case: MySQL crashed as soon as the txn undo is created.
    Only temporary table will not create txn */
    *trx->xid = undo->xid;
    trx->id = undo->trx_id;
    trx_sys_rw_trx_add(trx);
    trx->is_recovered = true;
    trx->ddl_operation = undo->dict_operation;
  }
  ut_ad(trx->rsegs.m_txn.txn_undo == nullptr);
  rseg->trx_ref_count++;
  trx->rsegs.m_txn.rseg = rseg;
  trx->rsegs.m_txn.txn_undo = undo;
  trx->rsegs.m_txn.xid_for_hash.null();

  assert_commit_mark_allocated(undo->prev_image);

  /**
     Currently it's impossible only have txn undo for normal transaction.
     But if crashed just after allocated txn undo, here maybe possible.
  */
  if (trx->rsegs.m_redo.rseg == nullptr) {
    lizard_ut_ad(undo->state == TRX_UNDO_ACTIVE || undo->is_prepared());
    ut_ad(trx_state_eq(trx, TRX_STATE_NOT_STARTED));

    if (undo->state == TRX_UNDO_ACTIVE) {
      trx->state.store(TRX_STATE_ACTIVE, std::memory_order_relaxed);
    } else {
      /* Can't be TRX_UNDO_CACHED because the undo is in txn_undo_list. */
      ut_a(undo->is_prepared());
      ++trx_sys->n_prepared_trx;
      trx->state.store(TRX_STATE_PREPARED, std::memory_order_relaxed);
    }

    /* A running transaction always has the number field inited to
    TRX_ID_MAX */

    // trx->no = TRX_ID_MAX;

    assert_undo_commit_mark_initial(undo);
    assert_trx_commit_mark_initial(trx);
    assert_txn_desc_initial(trx);

  } else {
    /** trx state has been initialized */
    ut_ad(!trx_state_eq(trx, TRX_STATE_NOT_STARTED));
    if (trx->rsegs.m_redo.insert_undo != nullptr &&
        trx->rsegs.m_redo.update_undo == nullptr) {
      /** SCN info wasn't written in insert undo. */
      if (trx->state == TRX_STATE_COMMITTED_IN_MEMORY) {
        /** Since the insert undo didn't have valid scn number */
        assert_undo_commit_mark_allocated(undo);
        trx->txn_desc.cmmt = undo->cmmt;
      }
    } else if (trx->rsegs.m_redo.update_undo != nullptr) {
      /** Update undo scn must be equal with txn undo scn */
      ut_ad(trx->rsegs.m_redo.update_undo->cmmt == undo->cmmt);
    }
  }

  trx_mutex_enter(trx);
  /** Resurrect trx->txn_desc.undo_ptr */
  trx->txn_desc.assemble_undo_ptr(undo->slot_addr);
  trx_mutex_exit(trx);

  /** Resurrect XA info. */
  trx->txn_desc.resurrect_xa(undo->pmmt, undo->branch, undo->maddr);

  /* trx_start_low() is not called with resurrect, so need to initialize
  start time here.*/
  if (trx->state.load(std::memory_order_relaxed) == TRX_STATE_ACTIVE ||
      trx->state.load(std::memory_order_relaxed) == TRX_STATE_PREPARED) {
    trx->start_time.store(std::chrono::system_clock::from_time_t(time(nullptr)),
                          std::memory_order_relaxed);
  }

  ut_ad(trx->txn_desc.cmmt == undo->cmmt);
}

/** Prepares a transaction for given rollback segment.
 @return lsn_t: lsn assigned for commit of scheduled rollback segment */
lsn_t txn_prepare_low(
    trx_t *trx,               /*!< in/out: transaction */
    txn_undo_ptr_t *undo_ptr, /*!< in/out: pointer to rollback
                              segment scheduled for prepare. */
    mtr_t *mtr) {
  ut_ad(mtr);

  // trx_rseg_t *rseg = undo_ptr->rseg;

  /* Change the undo log segment states from TRX_UNDO_ACTIVE to
  TRX_UNDO_PREPARED: these modifications to the file data
  structure define the transaction as prepared in the file-based
  world, at the serialization point of lsn. */

  // rseg->latch();

  ut_ad(undo_ptr->txn_undo);
  /* It is not necessary to obtain trx->undo_mutex here
  because only a single OS thread is allowed to do the
  transaction prepare for this transaction. */
  trx_undo_set_state_at_prepare(trx, undo_ptr->txn_undo, false, mtr);

  // rseg->unlatch();

  /*--------------*/
  /* This mtr commit makes the transaction prepared in
  file-based world. */

  // mtr_commit(&mtr);
  /*--------------*/

  /*
  if (!noredo_logging) {
    const lsn_t lsn = mtr.commit_lsn();
    ut_ad(lsn > 0);
    return lsn;
  }
  */

  return 0;
}

/**
  Round-bin get the rollback segment from transaction tablespace

  @retval     rollback segment
*/
static trx_rseg_t *get_next_txn_rseg() {
  static std::atomic<ulint> rseg_counter = 0;

  undo::Tablespace *undo_space;
  trx_rseg_t *rseg = nullptr;

  ulong n_rollback_segments = srv_rollback_segments;
  /** Lizard : didn't support variable of rollback segment count */
  ut_a(FSP_MAX_ROLLBACK_SEGMENTS == srv_rollback_segments);

  ulint current = rseg_counter;
  rseg_counter.fetch_add(1);

  /** Notes: didn't need undo::spaces->s_lock() */
  ut_ad(txn_spaces.size() == FSP_IMPLICIT_TXN_TABLESPACES);

  ulint target_undo_tablespaces = FSP_IMPLICIT_TXN_TABLESPACES;
  while (rseg == nullptr) {
    ulint window = current % (target_undo_tablespaces * n_rollback_segments);
    ulint space_slot = window % target_undo_tablespaces;
    ulint rseg_slot = window / target_undo_tablespaces;
    current++;

    undo_space = txn_spaces.at(space_slot);
    ut_ad(undo_space->is_active());

    rseg = undo_space->get_active(rseg_slot);
  }

  ut_ad(rseg);
  ut_ad(rseg->trx_ref_count > 0);
  return rseg;
}


/**
  Always assign transaction rollback segment for trx
  @param[in]      trx
*/
void trx_assign_txn_rseg(trx_t *trx) {
  const XID *xid_in_thd;
  XID &xid = trx->rsegs.m_txn.xid_for_hash;
  ut_a(xid.is_null());

  ut_ad(trx->rsegs.m_txn.rseg == nullptr);

  /** 1. Get XID if it is in an external XA. */
  xid_in_thd = get_external_xid_from_thd(trx->mysql_thd);
  if (xid_in_thd) {
    xid = *xid_in_thd;
  } else {
    ut_ad(xid.is_null());
  }

  /** 2. Assign rollback segment. By XID if need. */
  if (srv_read_only_mode) {
    trx->rsegs.m_txn.rseg = nullptr;
  } else if (xid.is_null()) {
    trx->rsegs.m_txn.rseg = get_next_txn_rseg();
  } else {
    trx->rsegs.m_txn.rseg = txn_rseg_assign_by_xid(&xid);
  }
}

/**
  Whether the txn rollback segment has been assigned
  @param[in]      trx
*/
bool trx_is_txn_rseg_assigned(const trx_t *trx) {
  return trx->rsegs.m_txn.rseg != nullptr;
}

/**
  Whether the txn undo log has modified.
*/
bool trx_is_txn_rseg_updated(const trx_t *trx) {
  return trx->rsegs.m_txn.txn_undo != nullptr;
}

/**
  Map(Hash) XID to {txn_space_slot, rseg_slot}.

  NOTES: If truncate TXN, this mapping relationship will be destroyed.
         Fortunately, truncate of TXN tablespace is not supported for
         now.

  @retval     {txn_space_slot, rseg_slot}
*/
static std::pair<ulint, ulint> txn_rseg_map_slot_by_xid(const XID *xid) {
  ut_ad(undo::spaces->own_latch());

  size_t current = hash_xid(xid);

  size_t n_rollback_segments = srv_rollback_segments;
  /** Lizard : didn't support variable of rollback segment count */
  ut_a(FSP_MAX_ROLLBACK_SEGMENTS == srv_rollback_segments);

  size_t target_undo_tablespaces = FSP_IMPLICIT_TXN_TABLESPACES;
  /** Notes: didn't need undo::spaces->s_lock() */
  ut_ad(txn_spaces.size() == FSP_IMPLICIT_TXN_TABLESPACES);

  size_t window = current % (target_undo_tablespaces * n_rollback_segments);
  size_t space_slot = window % target_undo_tablespaces;
  size_t rseg_slot = window / target_undo_tablespaces;

  return std::make_pair(space_slot, rseg_slot);
}

/**
  Get a TXN rseg by XID.

  @retval     rollback segment
*/
trx_rseg_t *txn_rseg_assign_by_xid(const XID *xid) {
  ulint space_slot;
  ulint rseg_slot;

  /* The number of undo tablespaces cannot be changed while
  we have this s_lock. */
  undo::spaces->s_lock();

  std::tie(space_slot, rseg_slot) = txn_rseg_map_slot_by_xid(xid);

  undo::Tablespace *undo_space;
  trx_rseg_t *rseg = nullptr;

  undo_space = txn_spaces.at(space_slot);
  ut_ad(undo_space->is_active());
  ut_ad(undo_space->is_txn());

  /** NOTES: Truncate of txn is not supported, so always active for now.
   */
  rseg = undo_space->get_active(rseg_slot);
  ut_a(rseg);

  undo::spaces->s_unlock();

  ut_ad(rseg->trx_ref_count > 0);

  return (rseg);
}

struct Find_txn_slot_by_xid {
 public:
  Find_txn_slot_by_xid(const XID *xid, txn_slot_t *txn_slot,
                       std::unordered_set<page_no_t> *pages)
      : m_xid(xid), m_txn_slot(txn_slot), m_searched_pages(pages) {}

  /**
    Check whether the page has been searched.
    @param[in]  page_no    page no to search

    @retval     true       page has been searched
  */
  bool operator()(const page_no_t page_no) {
    return (m_searched_pages->find(page_no) != m_searched_pages->end());
  }

  /**
    Iterate over the txn slots on the page (rseg, page_no) until finding the txn
    that matches the exact xid.
    @param[in]  page_no    page no to search
    @param[in]  undo_page  undo page
    @param[in]  mtr        mini-transaction handle

    @retval     true       found
  */
  bool operator()(const page_no_t page_no, const page_t *undo_page,
                  mtr_t *mtr) {
    bool found = false;

    /** Skip page if it has already been searched. */
    if (m_searched_pages->find(page_no) != m_searched_pages->end()) {
      return false;
    }

    found = txn_undo_log_iterate_by_offset(
        undo_page, mtr,
        [&](const page_t *undo_page, const trx_ulogf_t *log_hdr,
            mtr_t *mtr) -> bool {
          return txn_undo_hdr_read_by_xid(m_xid, undo_page, log_hdr, mtr,
                                          m_txn_slot);
        });

    m_searched_pages->insert(page_no);
    return found;
  }

 private:
  const XID *m_xid;
  txn_slot_t *m_txn_slot;
  std::unordered_set<page_no_t> *m_searched_pages;
};

/**
  Because the retained txn could be in the history list, the free list, the
  cached list, and the txn list, we need to search all of them.

  @param[in]  rseg       trx_rseg_t
  @param[out] func       search function
*/
template <typename Functor>
static bool txn_rseg_iterate_lists(trx_rseg_t *rseg, Functor &func) {
  trx_rsegf_t *rseg_header;
  page_t *undo_page;
  fil_addr_t node_addr;

  mtr_t mtr;

  mtr.start();

  mutex_enter(&(rseg->mutex));

  /** 1. Iterate over the history list. */
  rseg_header =
      trx_rsegf_get(rseg->space_id, rseg->page_no, rseg->page_size, &mtr);

  node_addr = flst_get_first(rseg_header + TRX_RSEG_HISTORY, &mtr);

  mtr.commit();

  while (node_addr.page != FIL_NULL) {
    mtr.start();

    undo_page = trx_undo_page_get_s_latched_with_hint(
        page_id_t(rseg->space_id, node_addr.page), rseg->page_size,
        Cache_hint::KEEP_OLD, &mtr);

    if (func(node_addr.page, undo_page, &mtr)) {
      mtr.commit();
      goto func_exit;
    }

    node_addr = flst_get_next_addr(undo_page + node_addr.boffset, &mtr);
    mtr.commit();
  }

  /** 2. If not found, iterate over the free list. */
  mtr.start();
  rseg_header =
      trx_rsegf_get(rseg->space_id, rseg->page_no, rseg->page_size, &mtr);
  node_addr = flst_get_first(rseg_header + TXN_RSEG_FREE_LIST, &mtr);
  mtr.commit();

  while (node_addr.page != FIL_NULL) {
    mtr.start();

    undo_page = trx_undo_page_get_s_latched_with_hint(
        page_id_t(rseg->space_id, node_addr.page), rseg->page_size,
        Cache_hint::KEEP_OLD, &mtr);

    if (func(node_addr.page, undo_page, &mtr)) {
      mtr.commit();
      goto func_exit;
    }

    node_addr = flst_get_next_addr(undo_page + node_addr.boffset, &mtr);
    mtr.commit();
  }

  /** 3. If not found, iterate over the cached list. */
  for (trx_undo_t *undo = UT_LIST_GET_FIRST(rseg->txn_undo_cached);
       undo != nullptr; undo = UT_LIST_GET_NEXT(undo_list, undo)) {
    if (func(undo->hdr_page_no)) {
      /** Skip reading page if it has already been searched. */
      continue;
    }

    mtr.start();

    undo_page = trx_undo_page_get_s_latched_with_hint(
        page_id_t(rseg->space_id, undo->hdr_page_no), rseg->page_size,
        Cache_hint::KEEP_OLD, &mtr);

    if (func(undo->hdr_page_no, undo_page, &mtr)) {
      mtr.commit();
      goto func_exit;
    }

    mtr.commit();
  }

  /** 4. If not found, iterate over the txn list. */
  for (trx_undo_t *undo = UT_LIST_GET_FIRST(rseg->txn_undo_list);
       undo != nullptr; undo = UT_LIST_GET_NEXT(undo_list, undo)) {
    if (func(undo->hdr_page_no)) {
      /** Skip reading page if it has already been searched. */
      continue;
    }

    mtr.start();

    undo_page = trx_undo_page_get_s_latched_with_hint(
        page_id_t(rseg->space_id, undo->hdr_page_no), rseg->page_size,
        Cache_hint::KEEP_OLD, &mtr);

    if (func(undo->hdr_page_no, undo_page, &mtr)) {
      mtr.commit();
      goto func_exit;
    }

    mtr.commit();
  }

  mutex_exit(&(rseg->mutex));
  return false;

func_exit:
  mutex_exit(&(rseg->mutex));
  return true;
}

/**
  Find transaction slot in the finalized state by XID.

  @param[in]  rseg         The rollseg where the transaction is being looked up.
  @params[in] xid          xid
  @param[out] txn_slot     Corresponding txn undo header

  @retval     true if the corresponding transaction is found, false otherwise.
*/
bool txn_rseg_find_txn_slot_by_xid(trx_rseg_t *rseg, const XID *xid,
                                   txn_slot_t *txn_slot) {
  std::unordered_set<page_no_t> undo_pages;
  Find_txn_slot_by_xid finder(xid, txn_slot, &undo_pages);

  return txn_rseg_iterate_lists<Find_txn_slot_by_xid>(rseg, finder);
}

/**
  If during an external XA, check whether the mapping relationship between xid
  and rollback segment is as expected.

  @param[in]        trx         current transaction

  @return           true        if success
*/
bool txn_rseg_check_xid_mapping(const XID *xid, const trx_rseg_t *expect_rseg) {
  ulint space_slot;
  ulint rseg_slot;
  bool match;

  ut_ad(expect_rseg != nullptr);

  /* The number of undo tablespaces cannot be changed while
  we have this s_lock. */
  undo::spaces->s_lock();

  std::tie(space_slot, rseg_slot) = txn_rseg_map_slot_by_xid(xid);

  undo::Tablespace *undo_space;

  undo_space = txn_spaces.at(space_slot);
  ut_ad(undo_space->is_active());
  ut_ad(undo_space->is_txn());

  match = undo_space->compare_rseg(rseg_slot, expect_rseg);

  undo::spaces->s_unlock();

  ut_ad(expect_rseg->trx_ref_count > 0);

  return match;
}

}  // namespace lizard
