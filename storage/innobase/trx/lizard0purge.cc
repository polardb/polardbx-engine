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
#include "lizard0scn.h"

#include "mtr0log.h"
#include "trx0purge.h"

namespace lizard {

/** Sentinel value */
const TxnUndoRsegs TxnUndoRsegsIterator::NullElement(SCN_NULL);

/** Constructor */
TxnUndoRsegsIterator::TxnUndoRsegsIterator(trx_purge_t *purge_sys)
    : m_purge_sys(purge_sys),
      m_txn_undo_rsegs(NullElement),
      m_iter(m_txn_undo_rsegs.end()) {}

const page_size_t TxnUndoRsegsIterator::set_next(bool *go_next) {
  ut_ad(go_next != NULL);
  mutex_enter(&m_purge_sys->pq_mutex);
  *go_next = true;

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
      *go_next = false;
      ut_ad(purge_sys == m_purge_sys);
      m_purge_sys->iter.scn = purge_sys->purge_heap->top().get_scn();
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

    m_iter = m_txn_undo_rsegs.begin();
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

}  // namespace lizard

