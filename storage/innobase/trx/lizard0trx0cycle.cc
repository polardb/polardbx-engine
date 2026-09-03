/*****************************************************************************

Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file trx/lizard0trx0cycle.cc
  Transactional slot cycle management.

 Created 2025-07-03 by Jianwei.zhao
 *******************************************************/

#include "sync0types.h"
#include "trx0undo.h"
#include "trx0rseg.h"

#include "lizard0gcs.h"
#include "lizard0srv0cycle.h"
#include "lizard0trx0cycle.h"
#include "lizard0txn0slot0undo.h"
#include "lizard0txn0space.h"
#include "lizard0undo.h"
#include "lizard0undo0retent.h"

#ifdef UNIV_PFS_MUTEX
/* cycle sys mutex PFS key */
mysql_pfs_key_t cycle_pq_mutex_key;
#endif

#ifdef UNIV_PFS_RWLOCK
/* cycle sys mutex PFS key */
mysql_pfs_key_t cycle_rw_latch_key;
#endif

namespace lizard {

/** Whether to print cycle process info. */
bool srv_cycle_print_process_debug = false;

/** */
ulint srv_cycle_rseg_batch_size = 64;

/** Global data structure of cycle system. */
trx_cycle_t *cycle_sys = nullptr;

/** Constructor. */
trx_cycle_t::trx_cycle_t()
    : m_cvision(),
      m_last_scn(0),
      m_last_us(0),
      m_last_gcn(0),
      m_rw_latch(nullptr),
      m_last_scn_booted(false),
      state(cycle_state_t::CYCLE_STATE_INIT) {
  m_rw_latch = static_cast<rw_lock_t *>(
      ut::zalloc_withkey(UT_NEW_THIS_FILE_PSI_KEY, sizeof(*m_rw_latch)));

  rw_lock_create(cycle_rw_latch_key, m_rw_latch, LATCH_ID_CYCLE_RW);

  mutex_create(LATCH_ID_CYCLE_PQ, &m_pq_mutex);
}

/** Destructor. */
trx_cycle_t::~trx_cycle_t() {
  rw_lock_free(m_rw_latch);
  ut::free(m_rw_latch);
  m_rw_latch = nullptr;

  mutex_free(&m_pq_mutex);
}

/** Boot trx cycle system. */
void trx_cycle_t::boot() {
  ut_ad(m_last_scn_booted == false);

  /** Start next round. */
  commit_order_t limit = iterate();

  for (size_t i = 0; i < FSP_IMPLICIT_TXN_TABLESPACES; i++) {
    undo::Tablespace *undo_space = txn_spaces.at(i);
    ut_ad(undo_space->is_txn());
    ut_ad(fsp_is_txn_tablespace_by_id(undo_space->id()));

    commit_order_t ommt = boot_space(undo_space);
    limit = ut_min(limit, ommt);
  }

  /* Save min */
  store_last(limit);
  m_last_scn_booted = true;
  return;
}

/** Boot a space rsegs.
 *
 * @param[in]		txn slot tablespace
 *
 * @retval	minimum last order or null
 * */
commit_order_t trx_cycle_t::boot_space(undo::Tablespace *space) {
  ut_ad(space->is_txn());

  commit_order_t ommt;
  for (auto rseg : *space->rsegs()) {
    /** Acquire last scn from history list. */
    rseg->latch();
    if (rseg->last_page_no != FIL_NULL) {
      ut_ad(!rseg->last_ommt.is_null());
      ommt = ut_min(ommt, rseg->last_ommt);
    }
    rseg->unlatch();
  }
  return ommt;
}

/** Iterate next recycle round through clone oldest vision, return
 * min active scn as until scn if history list is null.
 *
 * @retval	last commit order
 * */
commit_order_t trx_cycle_t::iterate() {
  gcs_clone_oldest_vision(&m_cvision);

  return m_cvision.limit();
}

/** txn slot header can be recycled .
 * @param[in]		ommt      commit order
 * @retval	    true if blocking cycle */
bool trx_cycle_t::advice(const commit_order_t &ommt) {
  ut_ad(!ommt.is_null());
  ut_ad(ommt.scn <= gcs_load_scn());

  /** Confirm oldest vision. */
  if (ommt.scn > m_cvision.snapshot_scn()) {
    return true;
  }

  /** Confirm undo retention include space and time. */
  if (!Undo_manager::instance().can_purge(ommt)) {
    return true;
  }

  return false;
}

/** try to recycle txn slot rsegment
 * @param[in]		txn slot roll segment
 * @param[in]		batch size every rseg.
 * @param[in/out]	how many logs was recycled
 *
 * @retval minimum last order or null
 * */
commit_order_t trx_cycle_t::recycle_rseg(trx_rseg_t *rseg, ulint batch_size,
                                         ulint *n_logs) {
  trx_rsegf_t *rseg_hdr = nullptr;
  page_t *undo_page = nullptr;
  trx_ulogf_t *log_hdr = nullptr;
  trx_usegf_t *seg_hdr = nullptr;
  trx_upagef_t *page_hdr = nullptr;
  fil_addr_t hdr_addr;
  fil_addr_t prev_addr;
  commit_order_t ommt;
  bool last_log;
  ulint n_log_hdr = 0;
  commit_order_t last_ommt;

  ut_ad(rseg->is_txn);

  mtr_t mtr;
  mtr_start(&mtr);

  rseg->latch();

  /** Quick judge that we can skip page read.*/
  if (rseg->last_page_no == FIL_NULL || advice(rseg->last_ommt)) {
    last_ommt = rseg->last_ommt;
    rseg->unlatch(false);
    mtr_commit(&mtr);
    return last_ommt;
  }

  rseg_hdr =
      trx_rsegf_get(rseg->space_id, rseg->page_no, rseg->page_size, &mtr);
  hdr_addr = trx_purge_get_log_from_hist(
      flst_get_last(rseg_hdr + TRX_RSEG_HISTORY, &mtr));

  /** We have hold latch and last page no is not null. */
  ut_ad(hdr_addr.page != FIL_NULL);

loop:
  if (hdr_addr.page == FIL_NULL) {
    rseg->last_page_no = FIL_NULL;
    rseg->last_ommt.set_null();
    last_ommt = rseg->last_ommt;
    rseg->unlatch(false);
    mtr_commit(&mtr);
    return last_ommt;
  }

  undo_page = trx_undo_page_get(page_id_t(rseg->space_id, hdr_addr.page),
                                rseg->page_size, &mtr);
  ut_a(trx_useg_verify(undo_page, rseg->page_size, &mtr));

  log_hdr = undo_page + hdr_addr.boffset;
  ommt = lizard::trx_undo_hdr_read_cmmt(log_hdr, &mtr);

  /** If blocked by advice. */
  if (advice(ommt) || ++n_log_hdr > batch_size) {
    rseg->last_page_no = hdr_addr.page;
    rseg->last_offset = hdr_addr.boffset;
    rseg->last_ommt = ommt;
    last_ommt = rseg->last_ommt;
    rseg->unlatch(false);
    mtr_commit(&mtr);
    return last_ommt;
  }

  prev_addr = trx_purge_get_log_from_hist(
      flst_get_prev_addr(log_hdr + TRX_UNDO_HISTORY_NODE, &mtr));

  seg_hdr = undo_page + TRX_UNDO_SEG_HDR;
  page_hdr = undo_page + TRX_UNDO_PAGE_HDR;
  ut_a(mach_read_from_2(page_hdr + TRX_UNDO_PAGE_TYPE) == TRX_UNDO_TXN);

  /** Last txn log header. */
  last_log = mach_read_from_2(seg_hdr + TRX_UNDO_STATE) == TRX_UNDO_TO_PURGE &&
             mach_read_from_2(log_hdr + TRX_UNDO_NEXT_LOG) == 0;

  if (last_log) {
    rseg->unlatch(false);
    mtr_commit(&mtr);

    txn_slot_recycle_segment(rseg, hdr_addr);
  } else {
    /* Remove the log hdr from the rseg history. */
    txn_slot_history_remove_log_hdr(rseg, rseg_hdr, log_hdr, &mtr);

    rseg->unlatch(false);
    mtr_commit(&mtr);
  }
  /** increment logs. */
  *n_logs += 1;

  mtr_start(&mtr);

  rseg->latch();

  rseg_hdr =
      trx_rsegf_get(rseg->space_id, rseg->page_no, rseg->page_size, &mtr);

  /** although list has been null, but user transaction continue to commit and
   * didn't modify last_xxxx. */
  if (prev_addr.page == FIL_NULL) {
    hdr_addr = trx_purge_get_log_from_hist(
        flst_get_last(rseg_hdr + TRX_RSEG_HISTORY, &mtr));
  } else {
    hdr_addr = prev_addr;
  }
  goto loop;
}

/** Less and equal than until scn can be truncated. */
ulint trx_cycle_t::recycle_history(ulint batch_size) {
  ulint n_logs = 0;
  ut_ad(txn_spaces.size() == FSP_IMPLICIT_TXN_TABLESPACES);

  /** 1. Clone next round. */
  commit_order_t limit = iterate();

  /** txn space is fixed, so ignore ddl mutex. */
  for (uint i = 0; i < FSP_IMPLICIT_TXN_TABLESPACES; i++) {
    undo::Tablespace *undo_space = txn_spaces.at(i);
    ut_ad(undo_space->is_txn());
    ut_ad(fsp_is_txn_tablespace_by_id(undo_space->id()));

    commit_order_t ommt = recycle_space(undo_space, batch_size, &n_logs);
    limit = ut_min(limit, ommt);
  }

  /* Save min last for purge system. */
  store_last(limit);

  if (srv_cycle_print_process_debug) {
    lizard_info(ER_LIZARD) << "Recycle transaction slots " << n_logs
                           << " until scn " << limit.scn;
  }
  return n_logs;
}

/** Recycle all rsegs within space.
 *
 * @param[in]		txn slot tablespace
 * @param[in]		batch size every rseg.
 * @param[in/out]	how many txn slot recycled.
 *
 * @retval	minimum last order or null
 * */
commit_order_t trx_cycle_t::recycle_space(undo::Tablespace *space,
                                          ulint batch_size, ulint *n_logs) {
  commit_order_t min;
  for (auto rseg : *space->rsegs()) {
    ut_ad(rseg->is_txn);
    commit_order_t last = recycle_rseg(rseg, batch_size, n_logs);
    min = ut_min(min, last);
  }
  return min;
}

/** Allocate memory structure. */
void trx_cycle_sys_mem_create() {
  cycle_sys = static_cast<trx_cycle_t *>(
      ut::zalloc_withkey(UT_NEW_THIS_FILE_PSI_KEY, sizeof(*cycle_sys)));
  new (cycle_sys) trx_cycle_t();
  return;
}

/** Initialize cycle system. */
void trx_cycle_sys_initialize(size_t n_threads) {
  ut_a(n_threads == 1);
  cycle_sys->boot();
  return;
}

/** Close cycle system. */
void trx_cycle_sys_close() {
  call_destructor(cycle_sys);
  ut::free(cycle_sys);
  cycle_sys = nullptr;
}

/*-----------------------------------------------------------------------------*/
/**
  Add the txn undo log header into history for cycle.

  @param[in]      trx       transaction
  @param[in/out]  undo_ptr    txn undo log structure
  @param[in]      undo_page   txn undo log header page, x-latched
  @param[in]      mtr */
void trx_cycle_add_txn_slot_to_history(trx_t *trx, txn_slot_ptr_t *undo_ptr,
                                       page_t *undo_page, mtr_t *mtr) {
  trx_undo_t *undo = nullptr;
  trx_rseg_t *rseg = nullptr;
  trx_rsegf_t *rseg_hdr = nullptr;
  trx_ulogf_t *undo_hdr = nullptr;
  static ulint round = 0;

  undo = undo_ptr->txn_undo;
  rseg = undo->rseg;
  ut_ad(rseg == undo_ptr->rseg);
  ut_ad(rseg->is_txn);
  ut_ad(trx->txn_desc.is_cmmt_alloced());

  rseg_hdr = trx_rsegf_get(undo->rseg->space_id, undo->rseg->page_no,
                              undo->rseg->page_size, mtr);
  undo_hdr = undo_page + undo->hdr_offset;

  ut_ad(trx_undo_hdr_read_slot(undo_hdr, mtr).is_validate());

  if (undo->state != TRX_UNDO_CACHED) {
    ulint hist_size;
#ifdef UNIV_DEBUG
    /** Lizard: txn undo only has log header */
    trx_usegf_t *seg_header = undo_page + TRX_UNDO_SEG_HDR;
    ut_ad(undo->size == flst_get_len(seg_header + TRX_UNDO_PAGE_LIST));
#endif /* UNIV_DEBUG */

    /* The undo log segment will not be reused */
    if (UNIV_UNLIKELY(undo->id >= TRX_RSEG_N_SLOTS)) {
      ib::fatal(UT_LOCATION_HERE, ER_IB_MSG_1165) << "undo->id is " << undo->id;
    }

    trx_rsegf_set_nth_undo(rseg_hdr, undo->id, FIL_NULL, mtr);

    MONITOR_DEC(MONITOR_NUM_UNDO_SLOT_USED);

    hist_size =
        mtr_read_ulint(rseg_hdr + TRX_RSEG_HISTORY_SIZE, MLOG_4BYTES, mtr);

    ut_a(undo->size == 1);
    mlog_write_ulint(rseg_hdr + TRX_RSEG_HISTORY_SIZE, hist_size + undo->size,
                     MLOG_4BYTES, mtr);
  }

  /** Here is my interpretation about the format of undo header page:
  1. A undo header page can hold multiple undo headers, whose format can
     be known in 'undo log header' in trx0undo.h
  2. Only current transaction who uses the undo page can use **undo
     log page hader**, the undo records from the transaction can be
     placed in multiple pages. But the other pages are normal undo pages,
     which only belong to the transaction.
  3. A 'undo log header' represents a undo_t, only belongs to a trx.
  4. The undo records of the other 'undo log header' can only be placed in
     the undo page.
  5. When added in history list, the TRX_UNDO_HISTORY_NODE are used to
     form a linked history list.
  */
  /* Add the log as the first in the history list */
  flst_add_first(rseg_hdr + TRX_RSEG_HISTORY, undo_hdr + TRX_UNDO_HISTORY_NODE,
                 mtr);

  ulint history_len = trx_sys->txn_rseg_history_len.fetch_add(1);
  /** Every 512 times try to wakeup cycle thread */
  if (round++ % TXN_RSEGS == 0 &&
      history_len >= TXN_RSEGS * srv_cycle_rseg_batch_size) {
    srv_wake_cycle_thread_if_not_active();
  }

  /* Update maximum transaction scn for this rollback segment. */
  mlog_write_ull(rseg_hdr + TRX_RSEG_MAX_TRX_SCN, trx->txn_desc.cmmt.scn, mtr);

  /* lizard: TRX_UNDO_TRX_NO is reserved */
  // mlog_write_ull(undo_header + TRX_UNDO_TRX_NO, trx->no, mtr);

  /* Write information about delete markings to the undo log header */
  if (!undo->del_marks) {
    mlog_write_ulint(undo_hdr + TRX_UNDO_DEL_MARKS, false, MLOG_2BYTES, mtr);
  } else {
    /** Txn undo log didn't have any delete marked record to purge forever  */
    ut_a(0);
  }
  /* Lizard: txn undo didn't need gtid information */

  if (rseg->last_page_no == FIL_NULL) {
    rseg->last_page_no = undo->hdr_page_no;
    rseg->last_offset = undo->hdr_offset;
    rseg->last_del_marks = undo->del_marks;
    rseg->last_ommt = trx->txn_desc.cmmt;
  }
}

}  // namespace lizard
