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

/** @file include/lizard0txn0mem.h
  Lizard txn memory structure.

 Created 2025-07-03 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0txn0mem_h
#define lizard0txn0mem_h

#include "trx0types.h"
#include "buf0buf.h"

#include "lizard0txn0rec0types.h"
#include "lizard0txn0slot0uba0types.h"

/** Transaction object if active. */
struct txn_rw_t {
 public:
  // referenced trx object if active.
  trx_t *trx;
  // fixed txn slot buf block object if active.
  buf_block_t *block;

  // Txn slot address and transaction state.
  undo_ptr_t undo_ptr;

  /** If current thr is target trx. */
  bool my_thr;

 public:
  txn_rw_t()
      : trx(nullptr), block(nullptr), undo_ptr(UNDO_PTR_NULL), my_thr(false) {}

  txn_rw_t(trx_t *trx_arg, buf_block_t *block_arg, undo_ptr_t undo_ptr_arg,
           bool my_thr_arg)
      : trx(trx_arg),
        block(block_arg),
        undo_ptr(undo_ptr_arg),
        my_thr(my_thr_arg) {}

  void reset() {
    ut_a(block == nullptr);

    trx = nullptr;
    block = nullptr;
    undo_ptr = UNDO_PTR_NULL;
    my_thr = false;
  }

  bool alloced() const { return undo_ptr != UNDO_PTR_NULL; }

  bool is_active() const {
    return trx != nullptr && undo_ptr != UNDO_PTR_NULL &&
           undo_addr_t::decode_active(undo_ptr);
  }

  /** release the txn slot buf block fix count. */
  void release_slot() {
    if (block) {
      ut_ad(was_slot_fixed());
      buf_block_unfix(block);
      block = nullptr;
    }

    my_thr = false;
  }

  bool is_my_thr() const { return my_thr; }

  /** maybe myself or target trx slot. */
  bool was_slot_fixed() const {
    return my_thr || (block != nullptr && block->page.buf_fix_count.load() > 0);
  }
};

/** transaction identity include trx_id and txn slot address.*/
struct txn_id_t {
 public:
  trx_id_t trx_id;
  undo_ptr_t undo_ptr;

 public:
  txn_id_t() : trx_id(0), undo_ptr(UNDO_PTR_NULL) {}

  txn_id_t(trx_id_t trx_id_arg, undo_ptr_t undo_ptr_arg)
      : trx_id(trx_id_arg), undo_ptr(undo_ptr_arg) {}

  bool alloced() const { return undo_ptr != UNDO_PTR_NULL; }
};

namespace lizard {
/** Commit txn memory structure after txn slot mini-transaction commit.
 *
 * @param[in/out]		trx
 * @param[in]			serialised */
void txn_commit_in_memory(trx_t *trx, bool serialised);

/** Get active transaction according to txn rec.
 *
 * @param[in/out]	txn rec
 * @param[in]		increment ref count
 * @param[in]		optional trx which is used to get local min active tid
 *
 * @retval	txn rw object.
 * */
txn_rw_t txn_rw_is_active(txn_rec_t *txn_rec, bool do_ref_count,
                          const trx_t *optional_trx);

/** Get active transaction according to txn rw.
 *
 * @param[in]		txn rw
 * @param[in]		increment ref count
 *
 * @retval	txn rw object.
 * */
txn_rw_t txn_rw_is_active(const txn_rw_t &txn_rw, bool do_ref_count);

/** Get active transaction according to txn identity.
 *
 * @param[in]		txn identity
 * @param[in]		increment ref count
 *
 * @retval	txn rw object.
 * */
txn_rw_t txn_rw_is_active(const txn_id_t &txn_id, bool do_ref_count);

/** Get active transaction according to transaction id.
 *
 * @param[in]		txn identity
 * @param[in]		increment ref count
 *
 * @retval	txn rw object.
 * */
txn_rw_t txn_rw_is_active(const trx_id_t rec_tid, bool do_ref_count,
                          const trx_t *optional_trx);

/**
 * Judge transaction have committed through txn slot.
 *
 * @param[in]	txn rw object
 *
 * @retval	true	Committed
 * @retval	false	Active
 * */
bool txn_rw_is_committed_in_memory(const txn_rw_t &txn_rw);

}  // namespace lizard

#endif
