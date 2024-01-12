/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
lzeusited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the zeusplied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file trx/lizard0purge.cc
 Lizard transaction purge system implementation.

 Created 2020-03-27 by zanye.zjy
 *******************************************************/

#include "lizard0purge.h"
#include "lizard0dbg.h"
#include "lizard0gcs.h"
#include "lizard0scn.h"

#include "mtr0log.h"
#include "trx0purge.h"
#include "trx0rseg.h"

#ifdef UNIV_PFS_MUTEX
/* lizard purge blocked stat mutex PFS key */
mysql_pfs_key_t purge_blocked_stat_mutex_key;
#endif

namespace lizard {

/** Sentinel value */
const TxnUndoRsegs TxnUndoRsegsIterator::NullElement(SCN_NULL);

/** Constructor */
TxnUndoRsegsIterator::TxnUndoRsegsIterator(trx_purge_t *purge_sys)
    : m_purge_sys(purge_sys),
      m_txn_undo_rsegs(NullElement),
      m_iter(m_txn_undo_rsegs.end()) {}

const page_size_t TxnUndoRsegsIterator::set_next(bool *keep_top) {
  ut_ad(keep_top != NULL);
  mutex_enter(&m_purge_sys->pq_mutex);
  *keep_top = false;

  lizard_purged_scn_validation();

  if (m_iter != m_txn_undo_rsegs.end()) {
    m_purge_sys->iter.scn = (*m_iter)->last_scn;
  } else if (!m_purge_sys->purge_heap->empty()) {
    /** We can't just pop the top element of the heap. In the past,
    It must be the smallest trx_no in the top of the heap, so we just
    wait until the purge sys get a big enough view.

    However, it's possible the top element is not the one with smallest
    scn. In order to avoid pop the element, we added the following codes.
    We might relax the limit in the future. */

    if (purge_sys->purge_heap->top().get_scn() >
        m_purge_sys->vision.snapshot_scn()) {
      *keep_top = true;
      ut_ad(purge_sys == m_purge_sys);
      mutex_exit(&m_purge_sys->pq_mutex);
      return (univ_page_size);
    }

    m_txn_undo_rsegs = NullElement;

    while (!m_purge_sys->purge_heap->empty()) {
      if (m_txn_undo_rsegs.get_scn() == SCN_NULL) {
        m_txn_undo_rsegs = purge_sys->purge_heap->top();
      } else if (purge_sys->purge_heap->top().get_scn() ==
                 m_txn_undo_rsegs.get_scn()) {
        /** Assume that there are temp rseg and durable rseg in a trx,
        when the trx was commited, only temp rseg was added in the purge
        heap for the reason: the last_page_no of durable rseg is not
        equal FIL_NULL. And then the rseg was poped and pushed again,
        the following branch can be achieved */
        m_txn_undo_rsegs.insert(purge_sys->purge_heap->top());
      } else {
        break;
      }
      m_purge_sys->purge_heap->pop();
    }

    /* In order for 'AS OF' to correctly determine whether the undo log
     * is still available, we should ensure that the txn rseg of a transaction
     * beging purged before the other rsegs. */
    m_iter = m_txn_undo_rsegs.arrange_txn_first();
  } else {
    /* Queue is empty, reset iterator. */
    m_txn_undo_rsegs = NullElement;
    m_iter = m_txn_undo_rsegs.end();

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
  ut_ad(m_purge_sys->rseg->last_scn == m_txn_undo_rsegs.get_scn());

  /* The space_id must be a tablespace that contains rollback segments.
  That includes the system, temporary and all undo tablespaces. */
  ut_a(fsp_is_system_or_temp_tablespace(m_purge_sys->rseg->space_id) ||
       fsp_is_undo_tablespace(m_purge_sys->rseg->space_id));

  const page_size_t page_size(m_purge_sys->rseg->page_size);

  /** ZEUS: We don't hold pq_mutex when we commit a trx. The possible case:
  TRX_A: scn = 5, scn allocated, rseg not pushed in purge_heap
  TRX_B: scn = 6, scn allocated, rseg pushed in purge_heap

  Then, purge_sys purge undo of TRX_B, purge_sys->iter.scn = 6. And rseg of
  TRX_A finally pushed in purge_heap, the following assert can't be achieved.

  In other words, the rollback segments are not added to the heap in order,
  which may result in the above situations. We haven't found a possible
  hazard, so we comment the assertion out */
  /* ut_a(purge_sys->iter.scn <= purge_sys->rseg->last_scn); */

  m_purge_sys->iter.scn = m_purge_sys->rseg->last_scn;
  m_purge_sys->hdr_offset = m_purge_sys->rseg->last_offset;
  m_purge_sys->hdr_page_no = m_purge_sys->rseg->last_page_no;

  m_purge_sys->rseg->unlatch();

  return (page_size);
}

template <typename XCN, unsigned long long POS>
XCN Purged_cnum<XCN, POS>::read() {
  XCN num;
  ut_ad(sizeof(XCN) == 8);
  gcs_sysf_t *hdr;
  mtr_t mtr;

  mtr_start(&mtr);
  hdr = gcs_sysf_get(&mtr);

  num = mach_read_from_8(hdr + POS);

  /** If lizard version is low, purge_gcn has not been saved. */
  if (num == 0 && POS == GCS_DATA_PURGE_GCN) {
    num = GCN_INITIAL;
  } else {
    ut_a(num >= GCN_INITIAL);
  }

  mtr_commit(&mtr);

  return num;
}

template <typename XCN, unsigned long long POS>
void Purged_cnum<XCN, POS>::write(XCN num) {
  ut_ad(m_inited == true);
  gcs_sysf_t *hdr;
  mtr_t mtr;

  mtr_start(&mtr);
  hdr = gcs_sysf_get(&mtr);
  mlog_write_ull(hdr + POS, num, &mtr);
  mtr_commit(&mtr);
}

template <typename XCN, unsigned long long POS>
void Purged_cnum<XCN, POS>::init() {
  ut_ad(m_inited == false);

  m_purged_xcn = read();
  m_inited = true;
}

template <typename XCN, unsigned long long POS>
XCN Purged_cnum<XCN, POS>::get() {
  ut_ad(m_inited == true);
  return m_purged_xcn.load();
}

/**
  Flush the bigger commit number to lizard tbs,
  Only one single thread to persist.
*/
template <typename XCN, unsigned long long POS>
void Purged_cnum<XCN, POS>::flush(XCN num) {
  ut_ad(m_inited == true);
  if (num > m_purged_xcn) {
    m_purged_xcn.store(num);
    write(m_purged_xcn);
  }
}
template void Purged_cnum<gcn_t, GCS_DATA_PURGE_GCN>::init();
template void Purged_cnum<gcn_t, GCS_DATA_PURGE_GCN>::flush(gcn_t num);
template gcn_t Purged_cnum<gcn_t, GCS_DATA_PURGE_GCN>::read();
template gcn_t Purged_cnum<gcn_t, GCS_DATA_PURGE_GCN>::get();
template void Purged_cnum<gcn_t, GCS_DATA_PURGE_GCN>::write(gcn_t num);

/**
  Initialize / reload purged_scn from purge_sys->purge_heap

  @retval              a valid scn if found, or PURGED_SCN_INVALID if in
                       "srv_force_recovery >= SRV_FORCE_NO_UNDO_LOG_SCAN"
*/
scn_t trx_purge_reload_purged_scn() {
  scn_t min_history_scn;
  /** If undo log scan is forbidden, purge_sys->purged_scn can't get a valid
  value */
  if (srv_force_recovery >= SRV_FORCE_NO_UNDO_LOG_SCAN) {
    return PURGED_SCN_INVALID;
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

/**
  Set purged_scn in purge sys

  @param[in]    txn_scn     purged scn
*/
void trx_purge_set_purged_scn(scn_t txn_scn) {
  /* It's safe because there is purge coordinator thread and server
  starting thread updating it. */
  purge_sys->purged_scn.store(txn_scn);
}

/**
  precheck if txn of the row is purged, without really reading txn

  @param[in]    txn_rec     the current row to be checked

  @retval       bool        true if the corresponding txn has been purged
*/
bool precheck_if_txn_is_purged(txn_rec_t *txn_rec) {
  if (!undo_ptr_is_active(txn_rec->undo_ptr)) {
    /** scn must allocated */
    lizard_ut_ad(txn_rec->scn > 0 && txn_rec->scn < SCN_MAX);

    return (txn_rec->scn <= purge_sys->purged_scn);
  }
  return false;
}

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
/**
  Validate all transactions whose SCN > purged_scn is always unpurged.

  @return         true      sucessful validation
*/
bool purged_scn_validation() {
  bool ret = false;
  scn_t top_scn;

  /** If undo log scan is forbidden, purge_sys->purged_scn can't get a valid
  value */
  if (srv_force_recovery >= SRV_FORCE_NO_UNDO_LOG_SCAN) {
    return true;
  }

  /* purge sys not init yet */
  if (!purge_sys) return true;

  ut_a(mutex_own(&purge_sys->pq_mutex));

  ut_a(purge_sys->purged_scn.load() != PURGED_SCN_INVALID);

  if (!purge_sys->purge_heap->empty()) {
    top_scn = purge_sys->purge_heap->top().get_scn();
    ret = (purge_sys->purged_scn <= top_scn);
  } else {
    ret = (purge_sys->purged_scn.load() <= gcs_load_min_safe_scn());
  }
  ut_ad(ret);

  return ret;
}
#endif /* UNIV_DEBUG || defined LIZARD_DEBUG */

void Purge_blocked_stat::get(String *blocked_cause, ulint *utc) {
  mutex_enter(&m_mutex);
  *utc = m_utc;
  switch (m_blocked_cause) {
    case purge_blocked_cause_t::BLOCKED_BY_VISION:
      blocked_cause->set_ascii(
          C_STRING_WITH_LEN("The purge sys is blocked by an active vision."));
      break;
    case purge_blocked_cause_t::RETENTION_BY_SPACE:
      snprintf(detailed_cause, sizeof(detailed_cause),
               "Undo retention is triggered by space. The current size of undo "
               "is %lu MB, which is less than the configured "
               "innodb_undo_space_reserved_size of %lu MB.",
               m_undo_used_size, m_retention_reserved_size);
      blocked_cause->copy(detailed_cause, strlen(detailed_cause),
                          blocked_cause->charset());
      break;
    case purge_blocked_cause_t::RETENTION_BY_TIME:
      snprintf(detailed_cause, sizeof(detailed_cause),
               "Undo retention is triggered by time. The undo has been "
               "retained for %lu seconds, which is less than the configured "
               "innodb_undo_retention of %lu seconds.",
               m_undo_retained_time, m_retention_time);
      blocked_cause->copy(detailed_cause, strlen(detailed_cause),
                          blocked_cause->charset());
      break;
    case purge_blocked_cause_t::BLOCKED_BY_HB:
      blocked_cause->set_ascii(
          C_STRING_WITH_LEN("The purge sys is blocked because no heartbeat has "
                            "been received for a long time."));
      break;
    case purge_blocked_cause_t::NO_UNDO_LEFT:
      blocked_cause->set_ascii(
          C_STRING_WITH_LEN("No undo logs left in the history list."));
      break;
    default:
      blocked_cause->set_ascii(
          C_STRING_WITH_LEN("The purge sys is not blocked."));
      break;
  }
  mutex_exit(&m_mutex);
}

void Purge_blocked_stat::set(purge_blocked_cause_t cause, ulint utc) {
  mutex_enter(&m_mutex);
  m_blocked_cause = cause;
  m_utc = utc;
  m_undo_used_size = 0;
  m_retention_reserved_size = 0;
  m_undo_retained_time = 0;
  m_retention_time = 0;
  mutex_exit(&m_mutex);
}

void Purge_blocked_stat::retained_by_space(purge_blocked_cause_t cause,
                                           ulint utc, ulint used_size,
                                           ulint undo_retention_reserved_size) {
  mutex_enter(&m_mutex);
  m_blocked_cause = cause;
  m_utc = utc;
  m_undo_used_size = used_size;
  m_retention_reserved_size = undo_retention_reserved_size;
  m_undo_retained_time = 0;
  m_retention_time = 0;
  mutex_exit(&m_mutex);
}

void Purge_blocked_stat::retained_by_time(purge_blocked_cause_t cause,
                                          ulint utc, ulint retained_time,
                                          ulint undo_retention_time) {
  mutex_enter(&m_mutex);
  m_blocked_cause = cause;
  m_utc = utc;
  m_undo_retained_time = retained_time;
  m_retention_time = undo_retention_time;
  m_undo_used_size = 0;
  m_retention_reserved_size = 0;
  mutex_exit(&m_mutex);
}

}  // namespace lizard
