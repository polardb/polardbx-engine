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

/** @file include/lizard0trx.h
  Lizard transaction structure.

 Created 2020-08-27 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0trx_h
#define lizard0trx_h

#include "trx0types.h"
#include "trx0trx.h"

#include "lizard0txn0slot0types.h"
#include "lizard0txn0slot0uba0types.h"

struct trx_undo_ptr_t;
struct txn_slot_ptr_t;

namespace lizard {

/**
  Read the scn
  @param[in]        ptr       buffer

  @return           scn_t  scn
*/
static inline scn_t trx_read_scn(const byte *ptr) {
  ut_ad(ptr);
  return mach_read_from_8(ptr);
}

/**
  Read the UBA
  @param[in]        ptr        buffer

  @return           undo_ptr_t undo_ptr
*/
static inline undo_ptr_t trx_read_undo_ptr(const byte *ptr) {
  ut_ad(ptr);
  return mach_read_from_8(ptr);
}
/**
  Read the gcn
  @param[in]        ptr       buffer

  @return           scn_t  scn
*/
static inline gcn_t trx_read_gcn(const byte *ptr) {
  ut_ad(ptr);
  return mach_read_from_8(ptr);
}

/**
  Write the scn into the buffer
  @param[in/out]    ptr     buffer
  @param[in]        scn     scn id
*/
static inline void trx_write_scn(byte *ptr, scn_t scn) {
  ut_ad(ptr);
  mach_write_to_8(ptr, scn);
}

/**
  Write the UBA into the buffer
  @param[in/out]    ptr       buffer
  @param[in]        undo_ptr  UBA
*/
static inline void trx_write_undo_ptr(byte *ptr, undo_ptr_t undo_ptr) {
  ut_ad(ptr);
  mach_write_to_8(ptr, undo_ptr);
}

/**
  Write the gcn into the buffer
  @param[in/out]    ptr     buffer
  @param[in]        scn     scn id
*/
static inline void trx_write_gcn(byte *ptr, gcn_t gcn) {
  ut_ad(ptr);
  mach_write_to_8(ptr, gcn);
}

/** Load min active trx id which is cached within trx struct. */
static inline trx_id_t trx_load_min_active_tid(const trx_t *trx) {
  return trx->min_active_tid.load(std::memory_order_relaxed);
}

/** Alloc cleanout cursors. */
extern void trx_cleanout_alloc(trx_t *trx);

/** Free cleanout cursors. */
extern void trx_cleanout_free(trx_t *trx);

/** Search from cache and overwrite the txn rec
 *
 * @param[in/out]   txn rec
 * @param[in/out]   txn lookup status
 *
 * @retval          cache hit or not */
extern bool trx_ccache_find(const trx_t *trx, txn_rec_t *txn_rec,
                            txn_status_t *txn_status);

/** Cache Commit Number if txn_rec was looked up.
 *
 * @param[in]	txn rec info that looked up
 * @param[in]	txn status */
extern void trx_ccache_insert(trx_t *trx, const txn_rec_t &txn_rec,
                              const txn_status_t &status);
/** Cache Commit Number after trx commit.
 *
 * @param[in]	Committed trx
 * @param[in]	Allocated commit number  */
extern void trx_ccache_insert(const trx_t *trx, bool serialised);

/** Get the number of tcn entries for thread local. */
extern ulint thread_calc_ccache_entries();

/** Rsegs except of insert undo will be put into rseg->history list when commit,
 * but txn slot undo and update undo was insert different heaps which are
 * processed by cycle and purge system.
 *
 * @param[in]	trx	commit transaction handler.
 * @param[in]	redo rseg
 * @param[in]	temp rseg */
extern void trx_commit_rsegs_collect(trx_t *trx, trx_undo_ptr_t *redo_ptr,
                                     trx_undo_ptr_t *temp_ptr);
/** Submit collected rsegs to cycle/purge system if necessary
 *
 * @param[in]	trx
 * @param[in]	commit mark */
extern void trx_commit_rsegs_submit(trx_t *trx, const commit_mark_t &cmmt);

/** Assert the rseg assignment of a trx is consistent with its recovered state.
Specifically, when the trx has a txn rseg but no redo rseg, it must be a
recovered transaction. Returns true so that it can be wrapped by ut_ad().
@param[in]      trx   innodb transaction
@return         always true */
bool trx_assert_in_recovery(const trx_t *trx);

}  // namespace lizard

#endif
