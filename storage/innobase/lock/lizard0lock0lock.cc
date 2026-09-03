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

#include "lizard0row0lotus.h"
#include "row0row.h"
#define LOCK_MODULE_IMPLEMENTATION

#include "lizard0lock0lock.h"
#include "lizard0row.h"
#include "lizard0txn0rec.h"
#include "lizard0btr0pcur.h"


/** check if some transaction has an implicit x-lock on a record in a secondary
 * index through traditional way, first optimistically judge by page max tid,
 * and then position cluster index record to find.*/
txn_rw_t lock_sec_rec_some_has_impl_slow(const Slice *slc, dict_index_t *index,
                                         const trx_t *optional_trx);
namespace lizard {

/** Read txn rec info from index record.
 *
 * @param[in]		rec
 * @param[in]		cluster index
 * @param[in]		offsets
 * @param[out]		txn rec.
 * */
static inline void lock_clust_rec_some_has_impl(const Slice *slc,
                                                const dict_index_t *index,
                                                txn_rec_t *txn_rec) {
  ut_ad(index->is_clustered());
  ut_ad(page_rec_is_user_rec(slc->rec()));
  ut_ad(!index->table->is_intrinsic());
  ut_ad(slc->contiguous());

  row_get_txn_rec(slc, index, txn_layout_t::TL_CLOVER, txn_rec);
}

/** Checks if some transaction has an implicit x-lock on a record in a panda
index.
@param[in]   rec       user record
@param[in]   index     panda index
@param[in]   offsets   rec_get_offsets(rec, index)
@return transaction which has the x-lock, or nullptr if there is none;
The caller must confirm all positive results by checking if the trx
is still active. */
static inline void lock_panda_rec_some_has_impl(const Slice *slc,
                                            const dict_index_t *index,
                                               txn_rec_t *txn_rec) {
  ut_ad(!index->is_clustered());
  ut_ad(index->is_panda());
  ut_ad(page_rec_is_user_rec(slc->rec()));
  ut_ad(slc->contiguous());

  row_get_txn_rec(slc, index, txn_layout_t::TL_BAMBOO, txn_rec);
}

/** Checks if some transaction has an implicit x-lock on a record on a secondary
 through trx id if exist.

 @param[in]   rec       user record
 @param[in]   index     index w/ transaction info
 @param[in]   offsets   rec_get_offsets(rec, index)
 @param[in]   trx       myself

 @retval      transaction if found trx id.*/
static std::pair<trx_id_t, txn_rw_t> lock_sec_rec_some_has_impl_fast(
    const Slice *slc,          /*!< in: rec on page */
    const dict_index_t *index, /*!<in: index */
    const trx_t *optional_trx) /*!<in: optional myself trx context. */
{
  txn_rw_t txn_rw;
  trx_id_t rec_tid = 0;

  ut_ad(!index->table->is_temporary());
  ut_ad(!index->is_clustered());
  ut_ad(page_rec_is_user_rec(slc->rec()));
  ut_ad(slc->is_positioned(index));

  if (index->is_hippo() && hippo_lock_enabled) {
    rec_tid = row_get_rec_trx_id(slc, index);

    /** Maybe invalid that happen when rollback. */
    if (rec_tid > 0) {
      txn_rw = lizard::txn_rw_is_active(rec_tid, true, optional_trx);
    }
  }

  return {rec_tid, txn_rw};
}

/** Checks if some transaction has an implicit x-lock on a record on a secondary
 @param[in]   rec       user record
 @param[in]   index     index w/ transaction info
 @param[in]   offsets   rec_get_offsets(rec, index)
 @param[in]   trx       myself

 @retval      transaction if found.*/
txn_rw_t lock_sec_rec_some_has_impl(const Slice *slc, dict_index_t *index,
                                    const trx_t *optional_trx) {
  trx_id_t rec_tid = 0;
  txn_rw_t txn_rw;

  /** Try to check fast by rec tid if exist on secondary index record. */
  std::tie(rec_tid, txn_rw) =
      lock_sec_rec_some_has_impl_fast(slc, index, optional_trx);

  if (rec_tid > 0) {
    return txn_rw;
  }

  /** Switch to slow way by page max tid and cluster index record trx id. */
  return lock_sec_rec_some_has_impl_slow(slc, index, optional_trx);
}
/** Checks if some transaction has an implicit x-lock on a record.
 @param[in]   rec       user record
 @param[in]   index     index w/ transaction info
 @param[in]   offsets   rec_get_offsets(rec, index)
 @param[in/out] clover/bamboo txn rec

 @return transaction which has the x-lock, or nullptr if there is none;
 The caller must confirm all positive results by checking if the trx
 is still active. */
void lock_clust_or_panda_rec_some_has_impl(const Slice *slc,
                                           const dict_index_t *index,
                                           txn_rec_t *txn_rec) {
  ut_ad(!index->table->is_temporary());
  ut_ad(index->is_clustered() || index->is_panda());
  ut_ad(page_rec_is_user_rec(slc->rec()));

  if (index->is_clustered()) {
    lock_clust_rec_some_has_impl(slc, index, txn_rec);
  } else {
    ut_ad(index->is_panda());
    lock_panda_rec_some_has_impl(slc, index, txn_rec);
  }
}

/**
 Checks whether a panda index record can be seen in a consistent read.
 @param[in]   rec       user record with transactional information
 @param[in]   index     panda index
 @param[in]   offsets   rec_get_offsets(rec, index)
 @param[in]   vision    consistent read view
 @param[in]   trx       current transaction context

 @return true if sees, or false if an earlier version of the index record needed
 */
static bool lock_panda_rec_cons_read_sees(const Slice *slc,
                                          const dict_index_t *index,
                                          btr_pcur_t *pcur,
                                          const lizard::Vision *vision,
                                          trx_t *trx) /*!< in: optional trx */
{
  const txn_layout_t layout = TL_BAMBOO;
  bool sees = false;
  ut_ad(!index->table->is_temporary());
  ut_ad(dict_index_is_panda(index));
  ut_ad(page_rec_is_user_rec(slc->rec()));
  ut_ad(slc->is_positioned(index));

  ut_ad(lizard::pcur_position_validate(pcur, slc->rec(), index));

  if (lizard::dict_index_inject_stress_test_for_panda(index)) {
    ut_a(lizard::row_panda_rec_neighbor_unique_check(slc->reg(), index));
  }

  if (recv_recovery_is_on()) {
    return (false);
  }

  if (lizard::dict_index_inject_stress_test_for_panda(index)) {
    ut_a(lizard::row_panda_rec_neighbor_unique_check(slc->reg(), index));
  }

  txn_rec_t txn_rec(slc, index, layout);
  cleanout_ctx_t cctx(trx, pcur);

  /** Try to see optimistically. */
  if (txn_rec_try_see(&txn_rec, layout, vision, cctx(MTR_LOG_NONE))) {
    return true;
  }

  txn_rec_execute_when_query(&txn_rec, layout, vision->visible_by(),
                             cctx(MTR_LOG_NO_REDO));

  sees = vision->modifications_visible(&txn_rec, index->table->name);

#ifdef UNIV_DEBUG
  trx_id_t max_trx_id = page_get_max_trx_id(page_align(slc->reg()));
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

bool lock_clust_or_panda_rec_cons_read_sees(const Slice *slc,
                                            dict_index_t *index,
                                            btr_pcur_t *pcur,
                                            lizard::Vision *vision,
                                            trx_t *trx) /*!< in: optional trx */
{
  ut_ad(!index->table->is_temporary());

  if (index->is_clustered()) {
    return lock_clust_rec_cons_read_sees(slc, index, pcur, vision, trx);
  } else {
    ut_ad(index->is_panda());
    return lock_panda_rec_cons_read_sees(slc, index, pcur, vision, trx);
  }
}

/** Checks that a non-clustered index record is seen in a consistent read
through tid on secondary index if exist.

 @param[in]   rec       user record with transactional information
 @param[in]   index     index with transactional information
 @param[in]   offsets   rec_get_offsets(rec, index)
 @param[in]   vision    consistent read view

 @retval      see or not sure if rec tid exist. */
static std::pair<trx_id_t, bool> lock_sec_rec_cons_read_sees_fast(
    const Slice *slc, const dict_index_t *index, const lizard::Vision *vision) {
  trx_id_t rec_tid = 0;
  bool see = false;

  ut_ad(!index->table->is_temporary());
  ut_ad(!lizard::dict_index_is_panda(index));
  ut_ad(page_rec_is_user_rec(slc->rec()));
  ut_ad(slc->is_positioned(index));

  if (index->is_hippo() && hippo_mvcc_enabled) {
    rec_tid = row_get_rec_trx_id(slc, index);

    /** Maybe invalid that happen when rollback. */
    if (rec_tid > 0) {
      see = vision->sees(rec_tid);
    }
  }
  return {rec_tid, see};
}

/** Checks that a non-clustered index record is seen in a consistent read.*/
bool lock_sec_rec_cons_read_sees(const Slice *slc, const dict_index_t *index,
                                 const lizard::Vision *vision) {
  trx_id_t rec_tid = 0;
  bool see = false;

  ut_ad(!index->table->is_temporary());
  ut_ad(!lizard::dict_index_is_panda(index));
  ut_ad(page_rec_is_user_rec(slc->rec()));
  ut_ad(slc->is_positioned(index));

  std::tie(rec_tid, see) =
      lock_sec_rec_cons_read_sees_fast(slc, index, vision);

  if (rec_tid > 0) return see;

  return lock_sec_rec_cons_read_sees_slow(slc->rec(), index, vision);
}

}  // namespace lizard
