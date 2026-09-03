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

/** @file trx/lizard0trx0purge.cc
 Lizard transaction purge system implementation.

 Created 2020-03-27 by zanye.zjy
 *******************************************************/

#include "komodo0service.h"

#include "lizard0cmmt0types.h"
#include "lizard0gcs.h"
#include "lizard0row.h"
#include "lizard0row0gpp.h"
#include "lizard0trx0erase.h"
#include "lizard0trx0purge.h"
#include "lizard0txn0slot.h"
#include "lizard0txn0space.h"
#include "lizard0undo.h"

#include "fut0lst.h"
#include "mtr0log.h"
#include "que0que.h"
#include "row0purge.h"
#include "trx0purge.h"
#include "trx0rseg.h"

/** A sentinel undo record used as a return value when we have a whole
undo log which can be skipped by purge */
extern trx_undo_rec_t trx_purge_ignore_rec;

/** lizard: a sentinel undo record to identify the BLOCKED record */
extern trx_undo_rec_t trx_purge_blocked_rec;

namespace lizard {

/** Sentinel value */
const purge_rsegs_t purge_rsegs_iterator_t::NullElement(SCN_NULL);

/** Constructor */
purge_rsegs_iterator_t::purge_rsegs_iterator_t(trx_purge_t *purge_sys)
    : m_purge_sys(purge_sys),
      m_undo_rsegs(NullElement),
      m_iter(m_undo_rsegs.end()) {}

const page_size_t purge_rsegs_iterator_t::set_next() {
  mutex_enter(&m_purge_sys->pq_mutex);
  ut_ad(trx_purge_scn_validation());

  if (m_iter != m_undo_rsegs.end()) {
    m_purge_sys->iter.ommt = (*m_iter)->last_ommt;
  } else if (!m_purge_sys->purge_heap->empty()) {
    /** We can't just pop the top element of the heap. In the past,
    It must be the smallest trx_no in the top of the heap, so we just
    wait until the purge sys get a big enough view.

    However, it's possible the top element is not the one with smallest
    scn. In order to avoid pop the element, we added the following codes.
    We might relax the limit in the future. */
    scn_t safe_scn = m_purge_sys->pvision.safe_scn();

    if (purge_sys->purge_heap->top().get_scn() > safe_scn) {
      mutex_exit(&m_purge_sys->pq_mutex);
      m_purge_sys->rseg = nullptr;
      return (univ_page_size);
    }

    m_undo_rsegs = NullElement;

    while (!m_purge_sys->purge_heap->empty()) {
      if (m_undo_rsegs.get_scn() == SCN_NULL) {
        m_undo_rsegs = purge_sys->purge_heap->top();
      } else if (purge_sys->purge_heap->top().get_scn() ==
                 m_undo_rsegs.get_scn()) {
        /** Assume that there are temp rseg and durable rseg in a trx,
        when the trx was commited, only temp rseg was added in the purge
        heap for the reason: the last_page_no of durable rseg is not
        equal FIL_NULL. And then the rseg was poped and pushed again,
        the following branch can be achieved */
        m_undo_rsegs.insert(purge_sys->purge_heap->top());
      } else {
        break;
      }
      m_purge_sys->purge_heap->pop();
    }

    m_iter = m_undo_rsegs.begin();
  } else {
    /* Queue is empty, reset iterator. */
    m_undo_rsegs = NullElement;
    m_iter = m_undo_rsegs.end();

    mutex_exit(&m_purge_sys->pq_mutex);

    m_purge_sys->rseg = nullptr;

    /* return a dummy object, not going to be used by the caller */
    return (univ_page_size);
  }

  m_purge_sys->rseg = *m_iter++;

  mutex_exit(&m_purge_sys->pq_mutex);

  ut_a(m_purge_sys->rseg != nullptr);

  m_purge_sys->rseg->latch();

  ut_a(m_purge_sys->rseg->last_page_no != FIL_NULL);
  ut_a(!m_purge_sys->rseg->is_txn);
  ut_ad(m_purge_sys->rseg->last_ommt.scn == m_undo_rsegs.get_scn());

  /* The space_id must be a tablespace that contains rollback segments.
  That includes the system, temporary and all undo tablespaces. */
  ut_a(fsp_is_system_or_temp_tablespace(m_purge_sys->rseg->space_id) ||
       fsp_is_undo_tablespace(m_purge_sys->rseg->space_id));

  const page_size_t page_size(m_purge_sys->rseg->page_size);

  /** Lizard: We don't hold pq_mutex when we commit a trx. The possible case:
  TRX_A: scn = 5, scn allocated, rseg not pushed in purge_heap
  TRX_B: scn = 6, scn allocated, rseg pushed in purge_heap

  Then, purge_sys purge undo of TRX_B, purge_sys->iter.scn = 6. And rseg of
  TRX_A finally pushed in purge_heap, the following assert can't be achieved.

  In other words, the rollback segments are not added to the heap in order,
  which may result in the above situations. We haven't found a possible
  hazard, so we comment the assertion out */
  /* ut_a(purge_sys->iter.scn <= purge_sys->rseg->last_scn); */

  m_purge_sys->iter.ommt = m_purge_sys->rseg->last_ommt;
  m_purge_sys->hdr_offset = m_purge_sys->rseg->last_offset;
  m_purge_sys->hdr_page_no = m_purge_sys->rseg->last_page_no;

  m_purge_sys->rseg->unlatch();

  return (page_size);
}

/**
  Initialize / reload purged_scn from purge_sys->purge_heap

  @retval              a valid scn if found, or PURGED_SCN_INVALID if in
                       "srv_force_recovery >= SRV_FORCE_NO_UNDO_LOG_SCAN"
*/
scn_t trx_purge_boot_purged_scn() {
  scn_t min_history_scn;
  /** If undo log scan is forbidden, purge_sys->purged_scn can't get a valid
  value */
  if (srv_force_recovery >= SRV_FORCE_NO_UNDO_LOG_SCAN) {
    return SCN_NULL;
  }

  ut_ad(purge_sys);

  if (purge_sys->purge_heap->empty()) {
    min_history_scn = gcs_load_scn();
  } else {
    min_history_scn = purge_sys->purge_heap->top().get_scn();
    ut_ad(min_history_scn < gcs_load_scn());
  }

  return min_history_scn;
}

/** Add into sp list, only allow for update undo.
 *
 * @param[in] rseg            Rollback segment
 * @param[in] rseg header
 * @param[in] undo log header
 * @param[in] undo log type */
void trx_purge_add_sp_list(trx_rseg_t *rseg, trx_rsegf_t *rseg_hdr,
                           trx_ulogf_t *log_hdr, ulint type, mtr_t *mtr) {
  ut_ad(mutex_own(&rseg->mutex));

  if (type == TRX_UNDO_UPDATE) {
    flst_add_first(rseg_hdr + TRX_RSEG_SEMI_PURGE_LIST,
                   log_hdr + TRX_UNDO_HISTORY_NODE, mtr);
  }
}

/**
 * Migrate the undo log segment from the history list to semi-purge list.
 *
 * @param[in] rseg            Rollback segment
 * @param[in] hdr_addr        File address of log_hdr
 */
void trx_purge_migrate_last_log(trx_rseg_t *rseg, fil_addr_t hdr_addr) {
  mtr_t mtr;
  page_t *undo_page;
  trx_rsegf_t *rseg_hdr;
  trx_ulogf_t *log_hdr;
  trx_usegf_t *seg_hdr;
  ulint seg_size;
  ulint hist_size;
  ulint sp_size;
  ulint type;
  bool del_mark = false;
  commit_mark_t cmmt;

  mtr_start(&mtr);
  mutex_enter(&rseg->mutex);

  rseg_hdr =
      trx_rsegf_get(rseg->space_id, rseg->page_no, rseg->page_size, &mtr);
  undo_page = trx_undo_page_get(page_id_t(rseg->space_id, hdr_addr.page),
                                rseg->page_size, &mtr);
  seg_hdr = undo_page + TRX_UNDO_SEG_HDR;
  log_hdr = undo_page + hdr_addr.boffset;

  type = mach_read_from_2(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE);
  del_mark = mtr_read_ulint(log_hdr + TRX_UNDO_DEL_MARKS, MLOG_2BYTES, &mtr);
  cmmt = trx_undo_hdr_read_cmmt(log_hdr, &mtr);

  ut_ad(type == TRX_UNDO_UPDATE);
  ut_ad(lizard::trx_useg_is_2pp(undo_page, rseg->page_size, &mtr));
  ut_ad(mach_read_from_2(log_hdr + TRX_UNDO_NEXT_LOG) == 0);

  seg_size = flst_get_len(seg_hdr + TRX_UNDO_PAGE_LIST);
  hist_size =
      mtr_read_ulint(rseg_hdr + TRX_RSEG_HISTORY_SIZE, MLOG_4BYTES, &mtr);
  sp_size = mtr_read_ulint(rseg_hdr + TRX_RSEG_SEMI_PURGE_LIST_SIZE,
                           MLOG_4BYTES, &mtr);
  ut_ad(hist_size >= seg_size);
  ut_ad(rseg->get_curr_size() >= seg_size);
  ut_ad(rseg->get_curr_size() > (hist_size + sp_size));

  trx_purge_remove_log_hdr(rseg, rseg_hdr, log_hdr, &mtr);
  mlog_write_ulint(rseg_hdr + TRX_RSEG_HISTORY_SIZE, hist_size - seg_size,
                   MLOG_4BYTES, &mtr);

  trx_purge_add_sp_list(rseg, rseg_hdr, log_hdr, type, &mtr);
  mlog_write_ulint(rseg_hdr + TRX_RSEG_SEMI_PURGE_LIST_SIZE, sp_size + seg_size,
                   MLOG_4BYTES, &mtr);

  /** Add into erase heap if necessary. */
  trx_add_rsegs_for_erase(rseg, hdr_addr, del_mark, cmmt);

  mutex_exit(&rseg->mutex);
  mtr_commit(&mtr);
}

/**
  Get last (oldest) log header from history list.
  @params[in]   rseg            update undo rollback segment
  @params[out]  log header address
  @params[out]	rollback segment statistics

  @retval	commit mark of log header
*/
commit_order_t trx_purge_get_last_log(trx_rseg_t *rseg, fil_addr_t &addr,
                                      komodo::rseg_stat_t *stat) {
  mtr_t mtr;
  trx_rsegf_t *rseg_hdr;
  trx_ulogf_t *log_hdr;
  page_t *undo_page;
  commit_order_t ommt;

  mtr_start(&mtr);
  rseg->latch();

  rseg_hdr =
      trx_rsegf_get(rseg->space_id, rseg->page_no, rseg->page_size, &mtr);

  /** Collect rollback segment statistics */
  if (stat) {
    stat->rseg_pages = rseg->get_curr_size();
    stat->history_pages =
        mtr_read_ulint(rseg_hdr + TRX_RSEG_HISTORY_SIZE, MLOG_4BYTES, &mtr);
    stat->history_length = flst_get_len(rseg_hdr + TRX_RSEG_HISTORY);
  }

  addr = trx_purge_get_log_from_hist(
      flst_get_last(rseg_hdr + TRX_RSEG_HISTORY, &mtr));

  if (addr.page == FIL_NULL) {
    rseg->unlatch(false);
    mtr_commit(&mtr);
    return ommt;
  }

  undo_page = trx_undo_page_get_s_latched_with_hint(
      page_id_t(rseg->space_id, addr.page), rseg->page_size,
      Cache_hint::KEEP_OLD, &mtr);

  log_hdr = undo_page + addr.boffset;
  ommt = trx_undo_hdr_read_cmmt(log_hdr, &mtr);

  rseg->unlatch(false);
  mtr_commit(&mtr);
  return ommt;
}

#ifdef UNIV_DEBUG
/**
  Validate all transactions whose SCN > purged_scn is always unpurged.

  @return         true      sucessful validation
*/
bool trx_purge_scn_validation() {
  bool ret = false;
  scn_t top_scn;

  /** If undo log scan is forbidden, purge_sys->purged_scn can't get a valid
  value */
  if (srv_force_recovery >= SRV_FORCE_NO_UNDO_LOG_SCAN) {
    return true;
  }

  /* purge sys not init yet */
  if (!purge_sys) return true;

  ut_ad(mutex_own(&purge_sys->pq_mutex));

  ut_a(gcs_clean_purged_scn() != SCN_NULL);

  if (!purge_sys->purge_heap->empty()) {
    top_scn = purge_sys->purge_heap->top().get_scn();
    ret = (gcs_clean_purged_scn() <= top_scn);
  } else {
    // purge validation always uses accurate min safe scn
    ret = (gcs_clean_purged_scn() <= gcs_traverse_min_safe_scn());
  }
  ut_ad(ret);

  return ret;
}
#endif

/** Check if an update undo log has been marked as purged.
@param[in]  rseg txn rseg
@param[in]  page_size
@return     true   if purged */
bool trx_purge_txn_slot_has_purged(const trx_rseg_t *rseg, page_no_t undo_page,
                                   ulint offset, const page_size_t &page_size) {
  page_t *page;
  trx_ulogf_t *log_hdr;
  trx_id_t trx_id;
  trx_id_t txn_trx_id = TRX_ID_MAX;
  ulint txn_state = txn_slot_t::PURGED;
  trx_ulogf_t *txn_hdr;
  ut_ad(!rseg->is_txn);

  mtr_t mtr;
  mtr_start(&mtr);

  /* Get current undo log header */
  page = trx_undo_page_get_s_latched(page_id_t(rseg->space_id, undo_page),
                                     page_size, &mtr);

  log_hdr = page + offset;

#ifdef UNIV_DEBUG
  ulint type = mach_read_from_2(page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE);
  ulint flag = mach_read_from_1(log_hdr + TRX_UNDO_FLAGS);
  ut_ad(type == TRX_UNDO_UPDATE);
  ut_ad(!(flag & TRX_UNDO_FLAG_TXN));
#endif

  trx_id = mach_read_from_8(log_hdr + TRX_UNDO_TRX_ID);
  /* Get addr of the corresponding txn undo log header */
  slot_addr_t slot_addr = trx_undo_hdr_read_slot(log_hdr, &mtr);
  if (slot_addr.is_no_redo()) goto no_txn;
  ut_a(!slot_addr.is_null());

  /** The insert/update undo should be released first, otherwise
  it will be deadlocked */
  mtr_commit(&mtr);

  ut_ad(fsp_is_txn_tablespace_by_id(slot_addr.space_id));

  mtr_start(&mtr);

  /* Get the txn undo log header */
  txn_hdr = trx_undo_page_get_s_latched(
                page_id_t(slot_addr.space_id, slot_addr.page_no),
                univ_page_size, &mtr) +
            slot_addr.offset;

  txn_trx_id = mach_read_from_8(txn_hdr + TRX_UNDO_TRX_ID);
  txn_state = txn_slot_hdr_read_state(txn_hdr);

no_txn:
  mtr_commit(&mtr);

  /* No txn, so it is a tempory rseg, no need to check. */
  if (slot_addr.is_no_redo()) return true;

  /* State of the txn undo log should be PURGED if not reused yet. */
  return (txn_trx_id != trx_id || txn_state == txn_slot_t::PURGED);
}

void trx_purge_start_history() {
  que_thr_t *thr = nullptr;
  for (thr = UT_LIST_GET_FIRST(purge_sys->query->thrs); thr != nullptr;
       thr = UT_LIST_GET_NEXT(thrs, thr)) {
    purge_node_t *node = static_cast<purge_node_t *>(thr->child);
    node->start_history_purge();
  }
}

/**
 Optimistically repositions the `pcur` in the purge node to the clustered
 index record. This method uses extra GPP information from the secondary index
 record to attempt an optimistic repositioning without a top-down B-tree search.
 If repositioning fails, it defaults to `row_purge_reposition_pcur()`, which
 conducts a top-down B-tree search to reposition the `pcur`.

 * @param[in] latch_mode       Latching mode, should be BTR_SEARCH_LEAF
 * @param[in,out] node   Purge node
 * @param[in] sec_cursor Cursor for the secondary index
 * @param[in] mtr        Mini-transaction
 * @return True if the cluster index record was successfully positioned
 */
bool row_purge_optimistic_reposition_pcur(ulint latch_mode, purge_node_t *node,
                                          btr_cur_t *sec_cursor, mtr_t *mtr) {
  if (!sec_cursor) {
    return false;
  }

  ut_ad(latch_mode == BTR_SEARCH_LEAF);
  ut_ad(!node->found_clust);

  ut_ad(!sec_cursor->index->is_clustered());

  ut_ad(page_is_leaf(btr_cur_get_page(sec_cursor)));

  /** Try to guess the clustered index record optimistically. */
  node->found_clust = row_purge_optimistic_guess_clust(
      node->table->first_index(), sec_cursor->index, node->ref,
      btr_cur_get_reg(sec_cursor), &node->pcur, latch_mode, mtr);

  return (node->found_clust);
}

}  // namespace lizard
