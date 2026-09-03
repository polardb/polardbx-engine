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

/** @file trx/lizard0trx.cc
  Lizard transaction structure.

 Created 2020-08-27 by Jianwei.zhao
 *******************************************************/

#include "lizard0trx.h"
#include "lizard0cleanout.h"
#include "lizard0gcs0cache.h"
#include "lizard0row.h"
#include "lizard0trx0cycle.h"
#include "lizard0trx0purge.h"
#include "lizard0txn.h"
#include "lizard0undo.h"

#include "trx0trx.h"

#ifndef UNIV_HOTBACKUP
#include <sql_class.h>
#endif

namespace lizard {
/** Search from cache and overwrite the txn rec
 *
 * @param[in/out]   txn rec
 * @param[in/out]   txn lookup status
 *
 * @retval          cache hit or not */
bool trx_ccache_find(const trx_t *trx, txn_rec_t *txn_rec,
                     txn_status_t *txn_status) {
  if (trx) {
    return trx->ccache->find(txn_rec, txn_status);
  }
  return false;
}
/** Cache Commit Number if txn_rec was looked up.
 *
 * @param[in]	txn rec info that looked up
 * @param[in]	txn status */
void trx_ccache_insert(trx_t *trx, const txn_rec_t &txn_rec,
                       const txn_status_t &status) {
  if (trx) {
    trx->ccache->insert(txn_rec, status);
  }
}
/** Cache Commit Number after trx commit.
 *
 * @param[in]	Committed trx
 * @param[in]	Allocated commit number  */
void trx_ccache_insert(const trx_t *trx, bool serialised) {
  if (trx) {
    trx->ccache->insert(trx, serialised);
  }
}

/** Allocate cleanout from trx context. */
void trx_cleanout_alloc(trx_t *trx) {
  ut_ad(trx->cleanout == nullptr);

  trx->cleanout = ut::new_withkey<Commit_cleanout>(
      ut::make_psi_memory_key(mem_key_row_cleanout));
  ut_ad(trx->cleanout != nullptr);
}

/** Free cleanout object. */
void trx_cleanout_free(trx_t *trx) {
  if (trx->cleanout) {
    ut::delete_(trx->cleanout);
    trx->cleanout = nullptr;
  }
}

/** Get the number of tcn entries for thread local. */
ulint thread_calc_ccache_entries() {
  if (srv_tcn_cache_level == NONE_LEVEL) return 0;

  return 128;
}

/** submit cycle/purge rseg when commit. */
void commit_rsegs_t::submit(scn_t scn) {
  ut_ad(scn != SCN_NULL && m_scn == SCN_NULL);
  m_scn = scn;

  /** Store update undo log */
  if (m_stored) {
    m_rsegs.set_scn(m_scn);

    mutex_enter(&purge_sys->pq_mutex);
    purge_sys->purge_heap->push(std::move(m_rsegs));
    ut_ad(trx_purge_scn_validation());
    mutex_exit(&purge_sys->pq_mutex);
  }
}

/** Rsegs except of insert undo will be put into rseg->history list when commit,
 * but txn slot undo and update undo was insert different heaps which are
 * processed by cycle and purge system.
 *
 * @param[in]	trx	commit transaction handler.
 * @param[in]	redo rseg
 * @param[in]	temp rseg
 * @param[in]	txn  rseg
 * */
void trx_commit_rsegs_collect(trx_t *trx, trx_undo_ptr_t *redo_ptr,
                              trx_undo_ptr_t *temp_ptr) {
  trx_rseg_t *redo_rseg = nullptr;
  trx_rseg_t *temp_rseg = nullptr;
  commit_rsegs_t *cmmt_rsegs = nullptr;

  cmmt_rsegs = &trx->commit_rsegs;
  ut_ad(cmmt_rsegs->is_null());

  if (redo_ptr != nullptr) {
    redo_rseg = redo_ptr->rseg;
    ut_ad(mutex_own(&redo_rseg->mutex));
  }

  if (temp_ptr != NULL) {
    temp_rseg = temp_ptr->rseg;
    ut_ad(mutex_own(&temp_rseg->mutex));
  }

  /** Update undo insert into purge structure. */
  if (redo_rseg != NULL && redo_rseg->last_page_no == FIL_NULL) {
    cmmt_rsegs->store(redo_rseg);
  }

  /** Update undo insert into purge structure. */
  if (temp_rseg != NULL && temp_rseg->last_page_no == FIL_NULL) {
    cmmt_rsegs->store(temp_rseg);
  }
}

/** Submit collected rsegs to cycle/purge system if necessary
 *
 * @param[in]	trx
 * @param[in]	commit mark
 * */
void trx_commit_rsegs_submit(trx_t *trx, const commit_mark_t &cmmt) {
  trx->commit_rsegs.submit(cmmt.scn);
}

/** Constructor. Acquire the mutex of every rseg modified by the
 * transaction in the fixed txn -> redo -> temp order, so the caller can
 * modify the undo of any of them afterwards without breaking the mutex
 * ordering. An rseg which is absent or not modified is skipped and is
 * reported as not locked.
 *
 * @param[in]	trx	the transaction which owns the rsegs */
locked_rsegs_t::locked_rsegs_t(trx_t *trx)
    : m_trx(trx),
      m_txn_rseg_locked(false),
      m_redo_rseg_locked(false),
      m_temp_rseg_locked(false) {
  ut_ad(m_trx);

  if (trx->rsegs.m_txn.rseg != NULL && trx_is_txn_rseg_updated(trx)) {
    mutex_enter(&trx->rsegs.m_txn.rseg->mutex);
    m_txn_rseg_locked = true;
  }

  if (trx->rsegs.m_redo.rseg != NULL && trx_is_redo_rseg_updated(trx)) {
    mutex_enter(&trx->rsegs.m_redo.rseg->mutex);
    m_redo_rseg_locked = true;
  }

  if (trx->rsegs.m_noredo.rseg != NULL && trx_is_temp_rseg_updated(trx)) {
    mutex_enter(&trx->rsegs.m_noredo.rseg->mutex);
    m_temp_rseg_locked = true;
  }
}

/** Destructor. It never releases the mutexes on behalf of the caller,
 * so assert that none of them is left held. */
locked_rsegs_t::~locked_rsegs_t() {
  ut_ad(!m_txn_rseg_locked);
  ut_ad(!m_redo_rseg_locked);
  ut_ad(!m_temp_rseg_locked);
}

/** Release all the acquired rseg mutexes, so the object is safe to be
 * destroyed. It's idempotent since every flag is cleared once its mutex
 * is released. */
void locked_rsegs_t::release_mutex() {
  if (m_txn_rseg_locked) {
    mutex_exit(&m_trx->rsegs.m_txn.rseg->mutex);
    m_txn_rseg_locked = false;
  }

  if (m_redo_rseg_locked) {
    mutex_exit(&m_trx->rsegs.m_redo.rseg->mutex);
    m_redo_rseg_locked = false;
  }

  if (m_temp_rseg_locked) {
    mutex_exit(&m_trx->rsegs.m_noredo.rseg->mutex);
    m_temp_rseg_locked = false;
  }
}

/** Assert the rseg assignment of a trx is consistent with its recovered state.
Specifically, when the trx has a txn rseg but no redo rseg, it must be a
recovered transaction. Returns true so that it can be wrapped by ut_ad().
@param[in]      trx   innodb transaction
@return         always true */
bool trx_assert_in_recovery(const trx_t *trx) {
  if (trx->rsegs.m_txn.rseg != nullptr && trx->rsegs.m_redo.rseg == nullptr) {
    ut_a(trx->is_recovered);
  }
  return true;
}

}  // namespace lizard
