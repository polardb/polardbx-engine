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

/** @file trx/lizard0txn0mem.cc
  Lizard txn memory structure.

 Created 2025-07-03 by Jianwei.zhao
 *******************************************************/
#include "trx0trx.h"

#include "lizard0gcs.h"
#include "lizard0trx.h"
#include "lizard0txn0mem.h"
#include "lizard0txn0slot0uba0types.h"

namespace lizard {
/** Commit txn memory structure after txn slot mini-transaction commit.
 *
 * @param[in/out]		trx
 * @param[in]			serialised */
void txn_commit_in_memory(trx_t *trx, bool serialised) {
  if (serialised) {
    trx_mutex_enter(trx);
    /** Commit undo ptr */
    undo_addr_t::set_commit(&trx->txn_desc.undo_ptr, trx->txn_desc.cmmt.csr,
                        !trx->txn_desc.xa_maddr().is_null());
    trx_mutex_exit(trx);
    ut_ad(trx->txn_desc.is_whole_committed());

    /** Update tcn cache. */
    gcs_cache_insert(trx, serialised);

    /** Update local trx tcn cache. */
    trx_ccache_insert(trx, serialised);

  } else {
    ut_ad(!trx->txn_desc.is_slot_alloced());
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
  bool my_thr = false;

  ut_ad(rec_tid > 0);

  /** Target rec transaction is still myself. */
  if (optional_trx && optional_trx->id != 0 && optional_trx->id == rec_tid) {
    my_thr = true;
    goto search;
  }

  /** if record tid is less than min active id*/
  if (optional_trx && rec_tid < trx_load_min_active_tid(optional_trx)) {
    txn_rw.reset();
    return txn_rw;
  }

  if (rec_tid < gcs_peek_min_active_tid(txn_rec->undo_ptr)) {
    txn_rw.reset();
    return txn_rw;
  }

  /** lookup txn slot. */
  std::tie(active, block) = txn_rec_is_active(txn_rec, nullptr, do_ref_count);
  if (!active) {
    ut_ad(txn_rec->is_committed());
    txn_rw.reset();
    return txn_rw;
  }

  ut_ad(txn_rec->is_active());
  ut_ad(block != nullptr || !do_ref_count);

search:
  /** Maybe commit walk here. */
  trx_t *trx = trx_rw_is_active(txn_rec->trx_id, do_ref_count);
  txn_rw = {trx, block, txn_rec->undo_ptr, my_thr};

  /** Find active trx. */
  if (!trx) {
    /** Unfix block if has. */
    txn_rw.release_slot();
    txn_rw.reset();
  } else {
    ut_ad(txn_rw.is_active());
    ut_ad(txn_rw.was_slot_fixed() || !do_ref_count);
    ut_ad(!my_thr || trx == optional_trx);
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
  if (!undo_addr_t::decode_active(txn_id.undo_ptr)) {
    return rw;
  }

  txn_rec_t txn_rec = {txn_id.trx_id, SCN_NULL, txn_id.undo_ptr, GCN_NULL};
  return txn_rw_is_active(&txn_rec, do_ref_count, nullptr);
}

/** Get active transaction according to transaction id.
 *
 * @param[in]		txn identity
 * @param[in]		increment ref count
 *
 * @retval	txn rw object.
 * */
txn_rw_t txn_rw_is_active(const trx_id_t rec_tid, bool do_ref_count,
                          const trx_t *optional_trx) {
  txn_rw_t txn_rw;
  undo_ptr_t undo_ptr = UNDO_PTR_NULL;
  ut_a(rec_tid > 0);

  /** if record tid is less than min active id*/
  if (optional_trx && rec_tid < trx_load_min_active_tid(optional_trx)) {
    txn_rw.reset();
    return txn_rw;
  }

  /** Rec tid come from btree record, so UBA must be allocated. */
  trx_sys->latch_and_execute_with_active_trx(
      rec_tid,
      [&rec_tid, &undo_ptr](trx_t *in_trx) {
        if (in_trx != nullptr) {
          ut_ad(in_trx->id == rec_tid);
          trx_mutex_enter(in_trx);
          undo_ptr = in_trx->txn_desc.undo_ptr;
          trx_mutex_exit(in_trx);
        }
      },
      UT_LOCATION_HERE);

  /** Not found trx in trx_sys. */
  if (undo_ptr == UNDO_PTR_NULL) {
    txn_rw.reset();
    return txn_rw;
  }

  /** Txn committed in memory,but still in trx_sys. */
  if (!undo_addr_t::decode_active(undo_ptr)) {
    txn_rw.reset();
    return txn_rw;
  } else {
    txn_rec_t txn_rec = {rec_tid, SCN_NULL, undo_ptr, GCN_NULL};
    return txn_rw_is_active(&txn_rec, do_ref_count, optional_trx);
  }
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
  trx_t *trx = nullptr;
  ut_ad(txn_rw.is_active());
  ut_ad(txn_rw.was_slot_fixed());

  trx = txn_rw.trx;
  txn_rec_t txn_rec = {txn_rw.trx->id, SCN_NULL, txn_rw.undo_ptr, GCN_NULL};
  ut_ad(txn_rec.is_active());

  if (txn_rw.is_my_thr()) {
    // unnecessary mutex here.
    // trx_mutex_enter(trx);
    bool active = undo_addr_t::decode_active(trx->txn_desc.undo_ptr);
    // trx_mutex_exit(trx);
    /** Judge again myself, must be active. */
    ut_ad(active);
    return !active;
  }

  return !txn_rec_is_active(&txn_rec, txn_rw.block, false).first;
}

}  // namespace lizard
