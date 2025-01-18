/*****************************************************************************
Copyright (c) 2013, 2024, Alibaba and/or its affiliates. All Rights Reserved.
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
/** @file lock/lizard0lock0lock.cc
 *
 * Lizard transaction lock system

 Created 2024-09-23 by Ting Yuan
 *******************************************************/

#include "row0row.h"
#define LOCK_MODULE_IMPLEMENTATION

#include "lizard0lock0lock.h"
#include "lizard0row.h"
#include "lizard0txn0rec.h"
#include "lizard0btr0pcur.h"

#include "lock0priv.h"

namespace lizard {

/** Read txn rec info from index record.
 *
 * @param[in]		rec
 * @param[in]		cluster index
 * @param[in]		offsets
 * @param[out]		txn rec.
 * */
static void lock_clust_rec_some_has_impl(const rec_t *rec,
                                         const dict_index_t *index,
                                         const ulint *offsets,
                                         txn_rec_t *txn_rec) {
  ut_ad(index->is_clustered());
  ut_ad(page_rec_is_user_rec(rec));
  ut_ad(!index->table->is_intrinsic());

  row_get_txn_rec(rec, index, offsets, txn_layout_t::TL_CLOVER, txn_rec);
}

/** Checks if some transaction has an implicit x-lock on a record in a panda
index.
@param[in]   rec       user record
@param[in]   index     panda index
@param[in]   offsets   rec_get_offsets(rec, index)
@return transaction which has the x-lock, or nullptr if there is none;
The caller must confirm all positive results by checking if the trx
is still active. */
static void lock_panda_rec_some_has_impl(const rec_t *rec,
                                             const dict_index_t *index,
                                             const ulint *offsets,
                                             txn_rec_t *txn_rec) {
  ut_ad(!index->is_clustered());
  ut_ad(index->is_panda());
  ut_ad(page_rec_is_user_rec(rec));

  row_get_txn_rec(rec, index, offsets, txn_layout_t::TL_BAMBOO, txn_rec);
}

static trx_id_t lock_panda_rec_some_has_impl(const rec_t *rec,
                                             const dict_index_t *index,
                                             const ulint *offsets) {
  ut_ad(!index->is_clustered());
  ut_ad(index->is_panda());
  ut_ad(page_rec_is_user_rec(rec));

  return row_get_rec_trx_id(rec, index, offsets);
}

/** Checks if some transaction has an implicit x-lock on a record.
 @param[in]   rec       user record
 @param[in]   index     index w/ transaction info
 @param[in]   offsets   rec_get_offsets(rec, index)
 @return transaction which has the x-lock, or nullptr if there is none;
 The caller must confirm all positive results by checking if the trx
 is still active. */
void lock_clust_or_panda_rec_some_has_impl(const rec_t *rec,
                                           const dict_index_t *index,
                                           const ulint *offsets,
                                           txn_rec_t *txn_rec) {
  ut_ad(!index->table->is_temporary());
  ut_ad(index->is_clustered() || index->is_panda());
  ut_ad(page_rec_is_user_rec(rec));

  if (index->is_clustered()) {
    lock_clust_rec_some_has_impl(rec, index, offsets, txn_rec);
  } else {
    ut_ad(index->is_panda());
    lock_panda_rec_some_has_impl(rec, index, offsets, txn_rec);
  }
}

trx_id_t lock_clust_or_panda_rec_some_has_impl(const rec_t *rec,
                                               const dict_index_t *index,
                                               const ulint *offsets) {
  ut_ad(!index->table->is_temporary());
  ut_ad(index->is_clustered() || index->is_panda());
  ut_ad(page_rec_is_user_rec(rec));
  trx_id_t trx_id;
  if (index->is_clustered()) {
    trx_id = lock_clust_rec_some_has_impl(rec, index, offsets);
  } else {
    ut_ad(index->is_panda());
    trx_id = lock_panda_rec_some_has_impl(rec, index, offsets);
  }
  return trx_id;
}

/**
 Checks whether a panda index record can be seen in a consistent read.
 @param[in]   rec       user record with transactional information
 @param[in]   index     panda index
 @param[in]   offsets   rec_get_offsets(rec, index)
 @param[in]   vision    consistent read view
 @return true if sees, or false if an earlier version of the index record needed
 */
static bool lock_panda_rec_cons_read_sees(const rec_t *rec,
                                          const dict_index_t *index,
                                          const ulint *offsets,
                                          btr_pcur_t *pcur,
                                          const lizard::Vision *vision) {
  ut_ad(!index->table->is_temporary());
  ut_ad(dict_index_is_panda(index));
  ut_ad(page_rec_is_user_rec(rec));
  ut_ad(rec_offs_validate(rec, index, offsets));
  ut_ad(lizard::pcur_position_validate(pcur, rec, index));

  if (recv_recovery_is_on()) {
    return (false);
  }

  const txn_layout_t layout = TL_BAMBOO;
  bool sees = false;

  txn_rec_t txn_rec(rec, index, offsets, layout);
  cleanout_ctx_t cctx(pcur);
  /** Try to see optimistically. */
  if (txn_rec_try_see(&txn_rec, layout, vision, cctx(MTR_LOG_NONE))) {
    return true;
  }

  txn_rec_execute_when_query(&txn_rec, layout, vision->visible_by(),
                             cctx(MTR_LOG_NO_REDO));

  sees = vision->modifications_visible(&txn_rec, index->table->name);

#ifdef UNIV_DEBUG
  trx_id_t max_trx_id = page_get_max_trx_id(page_align(rec));
  ut_ad(max_trx_id > 0);
  ut_ad(max_trx_id >= txn_rec.trx_id);
  if (vision->sees(max_trx_id)) {
    ut_ad(sees);
  }
#endif /* UNIV_DEBUG */

  return sees;
}

/**
 Checks whether an index record with transactional information can be seen in a
 consistent read.
 @param[in]   rec       user record with transactional information
 @param[in]   index     index with transactional information
 @param[in]   offsets   rec_get_offsets(rec, index)
 @param[in]   pcur      current pcursor that define position, used in cleanout
 @param[in]   vision    consistent read view
 @return true if sees, or false if an earlier version of the index record needed
 */

bool lock_clust_or_panda_rec_cons_read_sees(const rec_t *rec,
                                            dict_index_t *index,
                                            const ulint *offsets,
                                            btr_pcur_t *pcur,
                                            lizard::Vision *vision) {
  ut_ad(!index->table->is_temporary());

  if (index->is_clustered()) {
    return lock_clust_rec_cons_read_sees(rec, index, offsets, pcur, vision);
  } else {
    ut_ad(index->is_panda());
    return lock_panda_rec_cons_read_sees(rec, index, offsets, pcur, vision);
  }
}
}  // namespace lizard
