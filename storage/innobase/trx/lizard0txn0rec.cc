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

/** @file trx/lizard0txn0rec.cc
  Lizard transactional record management.

 Created 2024-10-15 by Ting Yuan
 *******************************************************/

#include "lizard0txn0rec.h"
#include "lizard0btr0cur0bamboo.h"
#include "lizard0btr0cur0clover.h"
#include "lizard0cleanout.h"
#include "lizard0cleanout0safe.h"
#include "lizard0gcs.h"
#include "lizard0row.h"
#include "lizard0row0bamboo.h"
#include "lizard0row0clover.h"
#include "lizard0trx.h"
#include "lizard0txn0rec0types.h"
#include "lizard0txn0slot.h"
#include "lizard0undo.h"

#include "row0row.h"

/** Construct txn attributes from rec offsets. */
txn_rec_t::txn_rec_t(const Slice *slc, const dict_index_t *index,
                     const txn_layout_t &layout) {
  lizard::row_get_txn_rec(slc, index, layout, this);
}

void txn_rec_t::copy_from_txn_desc(const trx_t *trx) {
  trx_id = trx->id;
  scn = trx->txn_desc.cmmt.scn;
  undo_ptr = trx->txn_desc.undo_ptr;
  gcn = trx->txn_desc.cmmt.gcn;
  return;
}

bool txn_rec_t::is_validate(const dict_index_t *index) const {
  if (is_null()) return true;

  /** UBA is valid */
  undo_addr_t undo_addr(undo_ptr);
  ut_a(undo_addr.is_validate(index));

  /** Commit number is valid */
  ut_a(is_committed() || is_active());
  return true;
}

namespace lizard {
/**
  Decide the real trx state.
    1) Search tcn cache
    2) Lookup txn undo

  Return whether the trx corresponding to the record is active.

  @param[in/out]  txn record
  @param[in/out]  txn state
  @param[in]      cache hint

  @retval true    active
          false   committed
*/
static bool txn_rec_real_state_by_lookup_low(txn_rec_t *txn_rec,
                                             txn_status_t *txn_status,
                                             const Cache_hint hint) {
  bool active = false;
  bool cache_hit = false;
  txn_lookup_t txn_lookup;

  /** Unknown the real state */
  // ut_ad(txn_rec->need_lookup(ccr));
  ut_ad(txn_rec && txn_status);
  ut_ad(!lizard::txn_sys_t::instance()->is_special(txn_rec->undo_ptr));

  cache_hit = gcs_cache_find(txn_rec, txn_status);
  if (cache_hit) {
    ut_ad(txn_rec->is_whole_committed());
    return false;
  }

  /** Record is still active, lookup txn hdr to confirm it. */
  std::tie(active, *txn_status) =
      txn_slot_read_once(txn_rec, &txn_lookup, hint);

  ut_ad(!txn_lookup.was_slot_fixed());

  if (!active) {
    ut_ad(txn_rec->is_whole_committed());
    /** Cache txn info into tcn. */
    gcs_cache_insert(*txn_rec, *txn_status);
  }

  return active;
}

/**
 * Determine whether the record needs to be cleaned out.
 * If cleaning is necessary, look up the txn_rec associated with the record.
 *
 * @param[in/out]  txn_record  The transaction record.
 * @param[in]      cache_hint   A hint for cache usage.
 *
 * @retval true    The record needs to be cleaned out.
 * @retval false   No cleanout needed.
 */
static bool txn_rec_cleanout_state(txn_rec_t *txn_rec, Cache_hint hint) {
  txn_status_t txn_status = txn_status_t::ACTIVE;
  bool active = false;

  if (txn_rec->is_committed()) {
    return false;
  }

  active = txn_rec_real_state_by_lookup_low(txn_rec, &txn_status, hint);
  return !active;
}

/**
  Determine the cached or real trx state.
  Return whether the trx corresponding to the record is active.

  @param[in/out]  txn_rec   txn record
  @param[in]      hint      cache hint
  @param[in]      ccr       category of commit number combination.

  @retval true    active
          false   committed
*/
bool txn_rec_real_state(txn_rec_t *txn_rec, Cache_hint hint, ccr_t ccr) {
  txn_status_t txn_status = txn_status_t::ACTIVE;

  if (!txn_rec->need_lookup(ccr)) {
    ut_ad(txn_rec->is_committed());

    return false;
  }

  return txn_rec_real_state_by_lookup_low(txn_rec, &txn_status, hint);
}

/** Determine txn slot real transaction state, and fix related block if active.
 *
 * @param[in/out]	txn_rec		txn record
 * @param[in]		fixed txn slot buf block
 * @param[in]		fix or not if active
 *
 * @retval		state and fixed block if active and do_fix.
 * */
std::pair<bool, buf_block_t *> txn_rec_is_active(txn_rec_t *txn_rec,
                                                 buf_block_t *block,
                                                 bool do_ref_count) {
  bool active = false;
  bool cache_hit = false;
  txn_lookup_t txn_lookup(do_ref_count);

  if (!txn_rec->need_lookup(CCR_SCN)) {
    ut_ad(txn_rec->is_committed());

    return {false, nullptr};
  }

  cache_hit = gcs_cache_find(txn_rec, &txn_lookup.real_status);
  if (cache_hit) {
    ut_ad(txn_rec->is_whole_committed());
    return {false, nullptr};
  }

  std::tie(active, std::ignore) =
      txn_slot_read_again(txn_rec, &txn_lookup, block, Cache_hint::KEEP_OLD);

  return {active, txn_lookup.block};
}

/**
  Clean out the record during the query.
  Attempt to collect the cursor, and it will be cleaned out when the query
  finishes.

  @param[in/out]  txn_rec   txn record
  @param[in]      layout    rec layout
  @param[in/out]	cleanout collector
*/
static void txn_rec_cleanout_when_query(txn_rec_t *txn_rec,
                                        const txn_layout_t &layout,
                                        cleanout_ctx_t &cctx) {
  bool active = false;
  bool cache_hit = false;
  txn_lookup_t txn_lookup;
  txn_status_t txn_status;

  ut_ad(txn_rec->is_active());
  ut_ad(cctx.is_usable());
  ut_ad(txn_layout_is_arranged(layout));

  /** Search tcn cache */
  cache_hit = gcs_cache_find(txn_rec, &txn_status);
  if (cache_hit) {
    ut_ad(txn_rec->is_whole_committed());
    /** Collect record to cleanout later. */
    cctx.collect_txn(*txn_rec, layout);
    return;
  }

  ut_ad(cache_hit == false);

  std::tie(active, txn_status) =
      txn_slot_read_once(txn_rec, &txn_lookup, Cache_hint::KEEP_OLD);
  ut_ad(!txn_lookup.was_slot_fixed());

  if (!active) {
    ut_ad(txn_rec->is_whole_committed());
    /** Collect record to cleanout later.*/
    cctx.collect_txn(*txn_rec, layout);
    /** Cache txn info into tcn. */
    gcs_cache_insert(*txn_rec, txn_status);
  }
}

/**
  Fill the txn_rec and attempt to clean out the record during the query.
  If cleaning is needed, collect the cursor, and it will be cleaned
  out when the query finishes.
  If cleaning is not needed, lookup and fill the txn_rec if necessary.

  @param[in/out]  txn_rec	  txn record
  @param[in]      layout    rec layout
  @param[in]      ccr       category of commit number combination.
  @param[in]	  cctx      cleanout context
*/
void txn_rec_execute_when_query(txn_rec_t *txn_rec, const txn_layout_t &layout,
                                ccr_t ccr, cleanout_ctx_t &cctx) {
  if (txn_rec->is_active() && cctx.is_usable()) {
    txn_rec_cleanout_when_query(txn_rec, layout, cctx);
  } else {
    txn_rec_real_state(txn_rec, Cache_hint::KEEP_OLD, ccr);
  }
}

/**
  Clean out the record during modification.
  If cleaning is needed, attempt to look up the txn_rec and perform the
  cleanout.

  @param[in]      trx_id    trx_id of the transactions
                            who updates / deletes the record
  @param[in]      rec       record
  @param[in]      index     index
  @param[in]      offsets   rec_get_offsets(rec)
  @param[in]      layout    rec layout
  @param[in/out]  block     buffer block of the record
  @param[in/out]  mtr       mini-transaction
*/
void txn_rec_cleanout_when_modify(const trx_id_t trx_id, Slice *slc,
                                  const dict_index_t *index,
                                  const txn_layout_t &layout,
                                  const buf_block_t *block, mtr_t *mtr) {
  trx_id_t rec_id;
  bool cleanout = false;

  ut_ad(trx_id > 0);
  ut_ad(slc->is_positioned(index));
  ut_ad(slc->contiguous());

  rec_id = row_get_rec_trx_id(slc, index);

  if (trx_id == rec_id) {
    /* update a exist row which has been modified by
    the current active transaction */
    return;
  }

  /** scn must be consistent with the undo_ptr */
  ut_ad(row_txn_is_valid(slc, index, layout));

  ut_ad(index->is_clustered() || index->is_panda());
  ut_ad(!index->table->is_intrinsic());

  txn_rec_t rec_txn(slc, index, layout);

  /** lookup the scn by UBA address */
  cleanout = txn_rec_cleanout_state(&rec_txn, Cache_hint::KEEP_OLD);

  if (cleanout) {
    ut_ad(mtr_memo_contains_flagged(mtr, block, MTR_MEMO_PAGE_X_FIX));

    switch (layout) {
      case TL_CLOVER:
        row_upd_rec_clover_fields_in_cleanout(
            slc, const_cast<page_zip_des_t *>(buf_block_get_page_zip(block)),
            index, &rec_txn);
        /** Write redo log */
        if (opt_cleanout_write_redo)
          btr_cur_upd_clover_fields_clust_rec_log(slc->rec(), index, &rec_txn,
                                                  mtr);
        break;
      case TL_BAMBOO:
        row_upd_rec_bamboo_fields_in_cleanout(
            slc, const_cast<page_zip_des_t *>(buf_block_get_page_zip(block)),
            index, &rec_txn);
        /** Write redo log */
        if (opt_cleanout_write_redo)
          btr_cur_upd_bamboo_fields_sec_rec_log(slc->rec(), index, &rec_txn,
                                                mtr);
        break;
      default:
        ut_error;
    }
  }
}

/**
  Lookup the referenced transaction state.
  1) Lookup the TXN of **txn_rec**, so get the master_uba, master_trx_id
  2) Search the real trx state of the master transaction.

  @param[in/out]  txn record
  @param[out]     referenced (master) transaction txn record.

  @retval true    active
          false   committed
*/
bool txn_rec_get_master_by_lookup(txn_rec_t *txn_rec, txn_rec_t *ref_txn_rec) {
  bool active = false;
  txn_status_t ref_txn_status = txn_status_t::ACTIVE;
  txn_lookup_t txn_lookup;
  ut_ad(txn_rec->is_slave());
  ut_ad(txn_rec->is_committed());

  /** Try to read master address. */
  txn_slot_read_once(txn_rec, &txn_lookup, Cache_hint::KEEP_OLD);

  ut_ad(!txn_lookup.was_slot_fixed());
  /** Task myself txn as master if have lost transaction slot.
   * It will be safe for query since transaction group will be purged
   * simultaneously */
  if (txn_lookup.txn_missing()) {
    *ref_txn_rec = *txn_rec;
    ref_txn_rec->clear_slave();

    ut_ad(!ref_txn_rec->is_slave() && ref_txn_rec->slot() == txn_rec->slot());
    return ref_txn_rec->is_active();
  }

  const auto &master = txn_lookup.txn_slot.maddr;
  ut_a(!master.is_null());

  /** Pretend a un-cleanout record. */
  ref_txn_rec->trx_id = master.tid;
  ref_txn_rec->undo_ptr = master.slot_ptr;
  ref_txn_rec->gcn = GCN_NULL;
  ref_txn_rec->scn = SCN_NULL;

  ut_a(ref_txn_rec->is_active());

  active = txn_rec_real_state_by_lookup_low(
      ref_txn_rec, &ref_txn_status, Cache_hint::KEEP_OLD);
  switch (ref_txn_status) {
    case txn_status_t::ACTIVE:
      ut_ad(active && ref_txn_rec->is_active());
      ut_ad(!ref_txn_rec->is_slave());
      break;
    case txn_status_t::COMMITTED:
    case txn_status_t::PURGED:
    case txn_status_t::ERASED:
      ut_ad(!active && ref_txn_rec->is_committed());
      if (ref_txn_rec->is_slave()) {
        lizard_error(ER_LIZARD)
            << "There should be only one master branch in a XA GROUP.";
        /** Reset slave info to skip infinite recursion when decision
        visibility. */
        ref_txn_rec->clear_slave();
      }

      if (txn_rec->gcn != ref_txn_rec->gcn) {
        lizard_error(ER_LIZARD) << "Transactions in a group should have only "
                                   "one external commit number.";
      }

      break;
    case txn_status_t::REUSE:
    case txn_status_t::UNDO_INVALID:
      ut_ad(!active && ref_txn_rec->is_committed());
      ut_ad(!ref_txn_rec->is_slave());
      break;
  }

  ut_ad(!ref_txn_rec->is_slave());
  return active;
}

/**
  Clean out the record when hit tcn cache.
  Attempt to collect the cursor, and it will be cleaned out when the query
  finishes.

  @param[in/out]  txn_rec	  txn record
  @param[in]      layout    txn layout
  @param[in]      vision    query vision
  @param[in/out]  cleanout collector
*/
static void txn_rec_cleanout_when_hit(txn_rec_t *txn_rec,
                                      const txn_layout_t &layout,
                                      const Vision *vision,
                                      cleanout_ctx_t &cctx) {
  bool cache_hit = false;
  txn_status_t txn_status;

  ut_ad(txn_rec->is_active());
  ut_ad(cctx.is_usable());

  /** Search local tcn cache. */
  cache_hit = trx_ccache_find(cctx.trx(), txn_rec, &txn_status);
  if (cache_hit) {
    ut_ad(txn_rec->is_whole_committed());
    /** Collect record to cleanout later. */
    cctx.collect_txn(*txn_rec, layout);
    return;
  }

  /** Skip if rec tid is out of range of tcn cache. */
  if (!gcs_cache_guess(*txn_rec, vision->up_limit_tid())) {
    return;
  }

  /** Search global tcn cache */
  cache_hit = gcs_cache_find(txn_rec, &txn_status);
  if (cache_hit) {
    ut_ad(txn_rec->is_whole_committed());

    /** Collect record to cleanout later. */
    cctx.collect_txn(*txn_rec, layout);

    /** Cache local tcn */
    trx_ccache_insert(cctx.trx(), *txn_rec, txn_status);
    return;
  }
  return;
}

/** Optimistic vision see only through trx id, and try to cleanout if hit tcn
 *  cache.
 *
 *  @param[in/out]	txn rec
 *  @param[in]		layout
 *  @param[in]		vision
 *  @param[in/out]	cleanout context
 *
 *  @retval	true	see
 *  @retval	false	not sure
 */
bool txn_rec_try_see(txn_rec_t *txn_rec, const txn_layout_t &layout,
                     const Vision *vision, cleanout_ctx_t &cctx) {
  bool see = false;
  trx_id_t rec_tid;

  rec_tid = txn_rec->trx_id;
  ut_ad(rec_tid > 0 && rec_tid < TRX_ID_MAX);

  // See modification myself.
  if (rec_tid == vision->creator_trx_id()) {
    return true;
  }

  /** see txn_rec_execute_when_query */
  if (cctx.qc_level() == QC_ALL) {
    return false;
  }

  see = vision->sees(rec_tid);

  ut_ad(cctx.qc_level() == QC_NONE || cctx.qc_level() == QC_NO_SLOT);

  if (see && txn_rec->is_active() && cctx.is_usable() &&
      cctx.qc_level() == QC_NO_SLOT) {
    txn_rec_cleanout_when_hit(txn_rec, layout, vision, cctx);
  }

  return see;
}

}  // namespace lizard
