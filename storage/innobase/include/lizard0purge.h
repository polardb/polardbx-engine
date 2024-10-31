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

/** @file include/lizard0purge.h
 Lizard transaction purge system implementation.

 Created 2020-03-27 by zanye.zjy
 *******************************************************/

#ifndef lizard0purge_h
#define lizard0purge_h

#include "lizard0gcs.h"
#include "lizard0scn.h"
#include "lizard0purge0types.h"
#include "page0size.h"

/**	Two Phase Purge (2PP)
 *
 * Followed 2PC naming tradition, we introduce a new purge stratedy,
 *
 * Two phase purge:
 *
 * 	First stage is original purge;
 *
 * 	Second stage is erase that is named by LIZARD system.
 */
struct trx_purge_t;
struct mtr_t;

/** purged_scn is not valid */
constexpr scn_t PURGED_SCN_INVALID = SCN_NULL;

namespace lizard {

/**
  Here's an explanation of the changes associated with Lizard transaction system
  and purge sys. In the past, when committing, innodb holds rseg::mutex,
  trx_sys::mutex to generate new trx_id as a commited number called trx_no for a
  trx, and then holds trx_sys::mutex, rseg::mutex, and purge_sys::pq_mutex to
  add resgs to purge_sys::purge_queue. So, we get the following conclusions:
  c-a. The history list in rollback segments is ordered.
  c-b. The purge_queue is ordered.

  Now, we only hold rseg::mutex to generate a new scn number, and then hold
  rseg::mutex and purge_sys::pq_mutex to add resgs to purge_sys::purge_heap.
  So, the above c-b is not statistified. There is a possible
  problem:
  p-a: purge_sys->iter.scn might advance purge_sys->vision.scn. The later
       rseg might be pushed to purge_sys::purge_heap first. In function
       **trx_purge_rseg_get_next_history_log** set purge_sys->iter.scn as
       rseg->last_scn + 1, and push the rseg in purge_heap. And then set_next
       might choose the pointed records, whose scn is possible larger than
       purge_sys->iter.scn, to purge.

  purge sys should never purge those records whose scn less than
  purge_sys->vision.scn.
*/

/** Choose the rollback segment with the smallest scn. */
struct TxnUndoRsegsIterator {
  /** Constructor */
  TxnUndoRsegsIterator(trx_purge_t *purge_sys);

  /**
    Sets the next rseg to purge in m_purge_sys.

    @param[out]		go_next   false if the top rseg's last_lsn
    @retval                 page size of the table for which the log is.

    NOTE: if rseg is NULL when this function returns this means that
    there are no rollback segments to purge and then the returned page
    size object should not be used.
  */
  const page_size_t set_next(bool *go_next);

 private:
  // Disable copying
  TxnUndoRsegsIterator(const TxnUndoRsegsIterator &);
  TxnUndoRsegsIterator &operator=(const TxnUndoRsegsIterator &);

  /** The purge system pointer */
  trx_purge_t *m_purge_sys;

  /** The current element to process */
  TxnUndoRsegs m_txn_undo_rsegs;

  /** Track the current element in m_txn_undo_rseg */
  typename Rsegs_array<2>::iterator m_iter;

  /** Sentinel value */
  static const TxnUndoRsegs NullElement;
};

struct min_safe_scn {
 public:
  min_safe_scn() : m_scn(0) {}
  min_safe_scn(scn_t scn) : m_scn(scn) {}

  scn_t get_min() const { return m_scn; }

  void push(scn_t scn) {
    ut_ad(scn >= m_scn);
    m_scn = scn;
  }

 private:
  scn_t m_scn;
};

/** Collect rsegs into the purge heap for the first time */
bool trx_purge_collect_rsegs(TxnUndoRsegs *elem,
                                 trx_undo_ptr_t *redo_rseg_undo_ptr,
                                 trx_undo_ptr_t *temp_rseg_undo_ptr,
                                 txn_undo_ptr_t *txn_rseg_undo_ptr);

/** Add the rseg into the purge queue heap */
void trx_purge_add_rsegs(commit_mark_t &scn, TxnUndoRsegs *elem);

/**
  Initialize / reload purged_scn from purge_sys->purge_heap

  @retval              a valid scn if found, or PURGED_SCN_INVALID if in
                       "srv_force_recovery >= SRV_FORCE_NO_UNDO_LOG_SCAN"
*/
scn_t trx_purge_reload_purged_scn();

/**
  Set purged_scn in purge sys

  @param[in]    txn_scn     purged scn
*/
void trx_purge_set_purged_scn(scn_t txn_scn);

/**
  precheck if txn of the row is purged, without really reading txn

  @param[in]    txn_rec     the current row to be checked

  @retval       bool        true if the corresponding txn has been purged
*/
bool txn_rec_is_purged_by_precheck(const txn_rec_t *txn_rec);

void trx_purge_add_sp_list(trx_rseg_t *rseg, trx_rsegf_t *rseg_hdr,
                           trx_ulogf_t *log_hdr, ulint type, mtr_t *mtr);

/**
 * Migrate the undo log segment from the history list to semi-purge list.
 *
 * @param[in] rseg            Rollback segment
 * @param[in] hdr_addr        File address of log_hdr
 */
void trx_purge_migrate_last_log(trx_rseg_t *rseg, fil_addr_t hdr_addr);

extern void trx_purge_start_history();

/**
 Optimistically repositions the `pcur` in the purge node to the clustered
 index record. This method uses extra GPP information from the secondary index
 record to attempt an optimistic repositioning without a top-down B-tree search.
 If repositioning fails, it defaults to `row_purge_reposition_pcur()`, which
 conducts a top-down B-tree search to reposition the `pcur`.

 * @param[in] mode       Search mode, should be BTR_SEARCH_LEAF
 * @param[in,out] node   Purge node
 * @param[in] sec_cursor Cursor for the secondary index
 * @param[in] mtr        Mini-transaction
 * @return True if the cluster index record was successfully positioned
 */
bool row_purge_optimistic_reposition_pcur(ulint mode, purge_node_t *node,
                                          btr_cur_t *sec_cursor, mtr_t *mtr);

/**
  Validate all transactions whose SCN > purged_scn is always unpurged.

  @return         true      sucessful validation
*/
extern bool purged_scn_validation();
}  // namespace lizard

#if defined UNIV_DEBUG || defined LIZARD_DEBUG

#define lizard_purged_scn_validation()     \
  do {                                     \
    ut_a(lizard::purged_scn_validation()); \
  } while (0)

#else

#define lizard_purged_scn_validation()

#endif /* UNIV_DEBUG || defined LIZARD_DEBUG */

#endif
