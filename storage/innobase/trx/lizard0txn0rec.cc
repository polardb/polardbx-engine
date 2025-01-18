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
#include "lizard0btr0cur.h"
#include "lizard0cleanout.h"
#include "lizard0cleanout0safe.h"
#include "lizard0erase.h"
#include "lizard0row.h"
#include "lizard0undo.h"
#include "lizard0txn0rec0types.h"
#include "lizard0row0clover.h"
#include "lizard0row0bamboo.h"
#include "lizard0btr0cur0clover.h"
#include "lizard0btr0cur0bamboo.h"

#include "row0row.h"

/** Construct txn attributes from rec offsets. */
txn_rec_t::txn_rec_t(const rec_t *rec, const dict_index_t *index,
                     const ulint *offsets, const txn_layout_t &layout) {
  lizard::row_get_txn_rec(rec, index, offsets, layout, this);
}

namespace lizard {

#if defined UNIV_DEBUG
/** Confirm txn rec validation
 * @param[in]	txn rec
 * @param[in]	dict index
 *
 * @retval	true	valid */
bool txn_rec_validate(const txn_rec_t *txn_rec, const dict_index_t *index) {
  if (txn_rec->is_null()) return true;

  /** UBA is valid */
  undo_addr_t undo_addr;
  undo_addr.decode(txn_rec->undo_ptr);
  ut_a(undo_addr_validate(&undo_addr, index));

  /** Commit number is valid */
  ut_a(txn_rec->is_committed() || txn_rec->is_active());

  return true;
}
#endif

/** Confirm value and print undo address if not true.*/
template <typename Type>
static void ut_print(const slot_addr_t &slot_addr, ut::Location loc,
                     Type value) {
  if (!value) {
    lizard_error(ER_LIZARD)
        << slot_addr.print() << " at file=" << basename(loc.filename)
        << ", line=" << loc.line;
    ut_a(0);
  }
}

bool Txn_slot_reuse_by_tid_checker::operator()(
    const trx_ulogf_t *log_hdr) const {
  trx_id_t real_trx_id;
  real_trx_id = mach_read_from_8(log_hdr + TRX_UNDO_TRX_ID);

  return real_trx_id != m_trx_id;
}

bool Txn_slot_reuse_by_xid_checker::operator()(
    const trx_ulogf_t *log_hdr) const {
  XID read_xid;

  auto flag = mach_read_ulint(log_hdr + TRX_UNDO_FLAGS, MLOG_1BYTE);
  if (!(flag & TRX_UNDO_FLAG_XID)) {
    return true;
  }

  trx_undo_read_xid(log_hdr, &read_xid);

  return (!read_xid.eq(m_xid));
}

/**
  Try to read the real scn of given records. Address directly to the
  corresponding txn undo header by UBA.

  @param[in]      slot_ptr        slot address
  @param[in]      hint            Cache hint
  @param[in]      ignore_missing  ignore missing if fail to get page
  @param[in]      txn_mtr         txn mtr
  @param[in]      reuse_checker   check if the TXN is reused.
  @param[in]      fatal_if_error  fatal if found invalid TXN.
  @param[out]     txn_lookup      txn lookup result, nullptr if don't care.

  @return         bool            whether corresponding trx is active.
*/
static bool _txn_slot_read_func(const slot_addr_t &slot_addr, Cache_hint hint,
                                bool guess, mtr_t *txn_mtr,
                                const Txn_slot_reuse_checker &reuse_checker,
                                txn_lookup_t *txn_lookup) {
  page_t *undo_page = nullptr;
  buf_block_t *undo_block = nullptr;
  ulint fil_type;
  ulint undo_page_start;
  trx_upagef_t *page_hdr;
  ulint undo_page_type;
  ulint real_trx_state;
  trx_usegf_t *seg_hdr;
  trx_ulogf_t *undo_hdr;
  txn_slot_t txn_slot;
  ulint hdr_flag;
  bool have_mtr = false;
  mtr_t temp_mtr;
  mtr_t *mtr;
  bool skip_fatal = guess;

  have_mtr = (txn_mtr != nullptr);

  mtr = have_mtr ? txn_mtr : &temp_mtr;

  ut_ad(mtr && txn_lookup);

  /** ----------------------------------------------------------*/
  /** Phase 1: Read the undo header page */
  ut_ad(slot_addr.offset <= UNIV_PAGE_SIZE_MAX);

  const page_id_t page_id(slot_addr.space_id, slot_addr.page_no);

  if (!have_mtr) mtr_start(mtr);

  /** Undo tablespace always univ_page_size */
  if (guess) {
    /** guess didn't support fix mode. */
    ut_ad(!txn_lookup->is_do_ref_count());
    undo_block = trx_undo_block_get_s_latched_with_hint_guess(
        page_id, univ_page_size, hint, mtr);
  } else {
    undo_block = trx_undo_block_get_s_latched_with_hint(page_id, univ_page_size,
                                                        hint, mtr);
  }
  if (undo_block) {
    undo_page = buf_block_get_frame(undo_block);
  }

  /** transaction tablespace didn't allowed to be truncated */
  if (!undo_page) {
    ut_print(slot_addr, UT_LOCATION_HERE, skip_fatal);
    goto undo_invalid;
  }

  /** ----------------------------------------------------------*/
  /** Phase 2: Judge the fil page */
  fil_type = fil_page_get_type(undo_page);

  /** The type of undo log segment must be FIL_PAGE_UNDO_LOG */
  if (fil_type != FIL_PAGE_UNDO_LOG) {
    ut_print(slot_addr, UT_LOCATION_HERE, skip_fatal);
    goto undo_invalid;
  }

  /** ----------------------------------------------------------*/
  /** Phase 3: judge whether it's undo log header or undo log data */
  page_hdr = undo_page + TRX_UNDO_PAGE_HDR;
  undo_page_start = mach_read_from_2(page_hdr + TRX_UNDO_PAGE_START);

  /** If the undo record start from undo segment header, it's normal
      undo log data page.
  */
  // ut_a(undo_page_start != (TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_HDR_SIZE));
  (void)undo_page_start;

  /** ----------------------------------------------------------*/
  /** Phase 4: judge whether it's txn undo */
  undo_page_type = mach_read_from_2(page_hdr + TRX_UNDO_PAGE_TYPE);

  if (undo_page_type != TRX_UNDO_TXN) {
    ut_print(slot_addr, UT_LOCATION_HERE, skip_fatal);
    goto undo_invalid;
  }

  /** ----------------------------------------------------------*/
  /** Phase 5: check the undo segment state */
  seg_hdr = undo_page + TRX_UNDO_SEG_HDR;
  real_trx_state = mach_read_from_2(seg_hdr + TRX_UNDO_STATE);

  /** real_trx_state should only be the following states */
  ut_print(slot_addr, UT_LOCATION_HERE,
           real_trx_state == TRX_UNDO_ACTIVE ||
               real_trx_state == TRX_UNDO_CACHED ||
               real_trx_state == TRX_UNDO_PREPARED_80028 ||
               real_trx_state == TRX_UNDO_PREPARED ||
               real_trx_state == TRX_UNDO_PREPARED_IN_TC ||
               real_trx_state == TRX_UNDO_TO_PURGE);

  /** ----------------------------------------------------------*/
  /** Phase 6: The offset (minus TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE)
  is a fixed multiple of TRX_UNDO_LOG_HDR_SIZE. */
  if (DBUG_EVALUATE_IF("force_do_slot_read", 1, 0)) {
    if (!(slot_addr.offset >= TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE)) {
      goto undo_invalid;
    }

    if (!((slot_addr.offset - (TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE)) %
              TRX_UNDO_LOG_GTID_HDR_SIZE ==
          0)) {
      goto undo_invalid;
    }
  } else {
    ut_print(slot_addr, UT_LOCATION_HERE,
             (slot_addr.offset >= TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE));
    ut_print(slot_addr, UT_LOCATION_HERE,
             ((slot_addr.offset - (TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE)) %
                  TRX_UNDO_LOG_GTID_HDR_SIZE ==
              0));
  }

  /**
    It is possible to access TXN beyond TRX_UNDO_PAGE_FREE, because when the
    TXN page is reused, TRX_UNDO_PAGE_FREE will be reset, but the actual TXN
    slot will not be reset.

    So TRX_UNDO_FLAGS (might use TXN_UNDO_LOG_EXT_MAGIC ?) is used to determine
    if the TXN slot is never used.
  */
  /*
  ulint free;
  free = mach_read_from_2(page_hdr + TRX_UNDO_PAGE_FREE);
  if (slot_addr.offset >= free) {
    ut_print(slot_addr, UT_LOCATION_HERE, skip_fatal);
    goto undo_invalid;
  }
  */

  /** ----------------------------------------------------------*/
  /** Phase 7: Check the flag in undo hdr, should be TRX_UNDO_FLAG_TXN,
  unless it's in cleanout_safe_mode. */
  undo_hdr = undo_page + slot_addr.offset;
  hdr_flag = mtr_read_ulint(undo_hdr + TRX_UNDO_FLAGS, MLOG_1BYTE, mtr);
  if (!(hdr_flag & TRX_UNDO_FLAG_TXN)) {
    ut_print(slot_addr, UT_LOCATION_HERE, skip_fatal);
    goto undo_invalid;
  }

  /** ----------------------------------------------------------*/
  /** Phase 8: check the txn extension fields in txn undo header */
  /** Already check page type and txn flags. */
  trx_undo_hdr_read_txn_slot(undo_page, undo_hdr, mtr, &txn_slot);
  ut_print(slot_addr, UT_LOCATION_HERE, (txn_slot.magic_n == TXN_MAGIC_N));

  /** NOTES: If the extent flag is used, there might be some records's flag
  that is equal to 0, and there also might be other records's flag that's not
  equal to 0 at the same time. */
  // if (txn_slot.ext_storage != 0) {
  //   /** The header might be raw */
  //   generic_stats.txn_undo_lost_ext_flag_wrong.inc();
  //   goto undo_invalid;
  // }

  /** ----------------------------------------------------------*/
  /** Phase 9: check the trx_id in txn undo header */
  if (reuse_checker(undo_hdr)) {
    generic_stats.txn_undo_lost_trx_id_mismatch.inc();
    goto undo_reuse;
  }

  /** Revision:
    We don't check txn_undo_lost_page_offset_overflow again, because
    it's a normal case: old UBAs point at the page that was reused,
    but the remain txn hdrs might still be valid. */

  /** ----------------------------------------------------------*/
  /** Phase 10: Here the txn header is the exactly header belongs to the
  record. Then, we get txn state in txn undo header to determine what's
  the real state of the transaction. */
  if (txn_slot.state == TXN_UNDO_LOG_ACTIVE) {
    lizard_ut_ad(mach_read_from_2(seg_hdr + TRX_UNDO_LAST_LOG) ==
                 slot_addr.offset);
    lizard_ut_ad(real_trx_state == TRX_UNDO_ACTIVE ||
                 real_trx_state == TRX_UNDO_PREPARED_80028 ||
                 real_trx_state == TRX_UNDO_PREPARED ||
                 real_trx_state == TRX_UNDO_PREPARED_IN_TC);
    goto still_active;
  } else if (txn_slot.state == TXN_UNDO_LOG_COMMITED) {
    goto already_commit;
  } else if (txn_slot.state == TXN_UNDO_LOG_PURGED) {
    goto undo_purged;
  } else {
    lizard_ut_ad(txn_slot.state == TXN_UNDO_LOG_ERASED);
    goto undo_erased;
  }

still_active:
  assert_commit_mark_initial(txn_slot.image);
  txn_lookup->init(txn_slot, txn_slot.image, txn_status_t::ACTIVE);
  txn_lookup->fix_slot_when_active(undo_block);

  if (!have_mtr) mtr_commit(mtr);
  return true;

already_commit:
  assert_commit_mark_allocated(txn_slot.image);
  txn_lookup->init(txn_slot, txn_slot.image, txn_status_t::COMMITTED);
  if (!have_mtr) mtr_commit(mtr);
  return false;

undo_purged:
  assert_commit_mark_allocated(txn_slot.image);
  txn_lookup->init(txn_slot, txn_slot.image, txn_status_t::PURGED);
  if (!have_mtr) mtr_commit(mtr);
  return false;

undo_erased:
  assert_commit_mark_allocated(txn_slot.image);
  txn_lookup->init(txn_slot, txn_slot.image, txn_status_t::ERASED);
  if (!have_mtr) mtr_commit(mtr);
  return false;

undo_reuse:
  assert_commit_mark_allocated(txn_slot.prev_image);
  txn_lookup->init(txn_slot, txn_slot.prev_image, txn_status_t::REUSE);
  if (!have_mtr) mtr_commit(mtr);
  return false;

undo_invalid:
  /** Can't never be lost if cleanout_safe_mode isn't taken into
  consideration */
  ut_a(opt_cleanout_safe_mode || guess);
  txn_lookup->init(txn_slot, CMMT_INVALID, txn_status_t::UNDO_INVALID);
  if (!have_mtr) mtr_commit(mtr);
  return false;
}

/**
  Try to lookup the real scn of given records.

  @param[in/out]  txn_rec       txn info of the records.
  @param[out]     txn_lookup    txn lookup result, nullptr if don't care
  @param[in]      hint          Cache hint
  @param[in]      txn_mtr       Non-nullptr if use external mtr, the caller is
                                responsible for committing mtr;
                                If passing nullptr, it will use a temporary mtr.

  @return         pair          first: whether corresponding trx is active.
                                second: txn slot real status.
*/
static std::pair<bool, txn_status_t> txn_slot_read_low(txn_rec_t *txn_rec,
                                                       txn_lookup_t *txn_lookup,
                                                       Cache_hint hint,
                                                       mtr_t *txn_mtr) {
  bool ret = false;
  undo_addr_t undo_addr;
  ut_ad(txn_lookup);
  ut_ad(!lizard::txn_sys_t::instance()->is_special(txn_rec->undo_ptr));

  /** In theory, lizard has to findout the real acutal scn (if have) by
  uba */
  generic_stats.txn_undo_lookup_by_uba.inc();

  if (opt_cleanout_safe_mode) {
    undo_addr.decode(txn_rec->undo_ptr);
    bool exist = txn_undo_logs->exist({undo_addr.space_id, undo_addr.page_no});
    if (!exist) {
      txn_slot_t txn_slot = {
          CMMT_INVALID,
          /** txn_slot.undo_ptr should be from txn undo header, and it
          must be active state when coming here */
          txn_rec->undo_ptr,
          txn_rec->trx_id,
          TXN_MAGIC_N,
          CMMT_INVALID,
          TXN_UNDO_LOG_PURGED,
          0,
          0,
          false,
          PMMT_INVALID,
          {0, 0},
          {0, 0},
      };
      txn_rec->scn = CMMT_INVALID.scn;
      txn_rec->gcn = CMMT_INVALID.gcn;
      undo_ptr_set_commit(&txn_rec->undo_ptr, CSR_AUTOMATIC, false);
      txn_lookup->init(txn_slot, CMMT_INVALID, txn_status_t::UNDO_INVALID);

      generic_stats.txn_undo_lost_page_miss_when_safe.inc();
      return std::make_pair(false, txn_lookup->real_status);
    }
  }

  Txn_slot_reuse_by_tid_checker tid_checker(txn_rec->trx_id);

  ret = _txn_slot_read_func(slot_addr_t(txn_rec->undo_ptr), hint, false,
                            txn_mtr, tid_checker, txn_lookup);
  const txn_slot_t &txn_slot = txn_lookup->txn_slot;

  switch (txn_lookup->real_status) {
    case txn_status_t::ACTIVE:
      break;
    case txn_status_t::COMMITTED:
      txn_rec->scn = txn_slot.image.scn;
      txn_rec->gcn = txn_slot.image.gcn;
      undo_ptr_set_commit(&txn_rec->undo_ptr, txn_slot.image.csr,
                          !txn_slot.maddr.is_null());
      break;
    case txn_status_t::PURGED:
      txn_rec->scn = txn_slot.image.scn;
      txn_rec->gcn = txn_slot.image.gcn;
      undo_ptr_set_commit(&txn_rec->undo_ptr, txn_slot.image.csr,
                          !txn_slot.maddr.is_null());
      break;
    case txn_status_t::ERASED:
      txn_rec->scn = txn_slot.image.scn;
      txn_rec->gcn = txn_slot.image.gcn;
      undo_ptr_set_commit(&txn_rec->undo_ptr, txn_slot.image.csr,
                          !txn_slot.maddr.is_null());
      break;
    case txn_status_t::REUSE:
      txn_rec->scn = txn_slot.prev_image.scn;
      txn_rec->gcn = txn_slot.prev_image.gcn;
      undo_ptr_set_commit(&txn_rec->undo_ptr, txn_slot.prev_image.csr, false);
      break;
    case txn_status_t::UNDO_INVALID:
    default:
      txn_rec->scn = CMMT_INVALID.scn;
      txn_rec->gcn = CMMT_INVALID.gcn;
      undo_ptr_set_commit(&txn_rec->undo_ptr, CSR_AUTOMATIC, false);
      break;
  }

  return std::make_pair(ret, txn_lookup->real_status);
}

/**
  Try to read TXN by only TXN slot address. The TXN slot might not be found.

  @param[in]      slot_ptr      TXN Slot address
  @param[in]      hint          Cache hint
  @param[in]      reuse_checker Check if the TXN slot is reused.
  @param[out]     txn_lookup    txn lookup result, nullptr if don't care

  @return   true if the expected TXN is found.
*/
bool txn_slot_read_guess(const slot_ptr_t slot_ptr, Cache_hint hint,
                         const Txn_slot_reuse_checker &reuse_checker,
                         txn_lookup_t *txn_lookup) {
  const slot_addr_t slot_addr(slot_ptr);

  generic_stats.txn_read_guess_request.inc();

  if (!slot_addr_disk_mapped(slot_addr) &&
      !DBUG_EVALUATE_IF("force_do_slot_read", 1, 0)) {
    return false;
  }

  bool guess = true;
  DBUG_EXECUTE_IF("expect_txn_read_guess_success", guess = false;);
  _txn_slot_read_func(slot_addr, hint, guess, nullptr, reuse_checker,
                      txn_lookup);
  ut_ad(!txn_lookup->was_slot_fixed());

  bool missing = txn_lookup->txn_missing();

  if (missing) {
    generic_stats.txn_read_guess_fail.inc();
  }

  return !missing;
}

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
                                             Cache_hint hint) {
  bool active = false;
  bool cache_hit = false;
  txn_lookup_t txn_lookup;

  /** Unknown the real state */
  // ut_ad(txn_rec->need_lookup(ccr));
  ut_ad(txn_rec && txn_status);
  ut_ad(!lizard::txn_sys_t::instance()->is_special(txn_rec->undo_ptr));

  cache_hit = trx_search_tcn(txn_rec, txn_status);
  if (cache_hit) {
    ut_ad(txn_rec->is_whole_committed());
    return false;
  }

  /** Record is still active, lookup txn hdr to confirm it. */
  std::tie(active, *txn_status) =
      txn_slot_read_low(txn_rec, &txn_lookup, hint, nullptr);

  ut_ad(!txn_lookup.was_slot_fixed());

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
 * @param[in]		fix or not if active
 *
 * @retval		state and fixed block if active and do_fix.
 * */
extern std::pair<bool, buf_block_t *> txn_slot_is_active(txn_rec_t *txn_rec,
                                                         bool do_ref_count) {
  bool active = false;
  bool cache_hit = false;
  txn_lookup_t txn_lookup(do_ref_count);

  if (!txn_rec->need_lookup(CCR_SCN)) {
    ut_ad(txn_rec->is_committed());

    return {false, nullptr};
  }

  cache_hit = trx_search_tcn(txn_rec, &txn_lookup.real_status);
  if (cache_hit) {
    ut_ad(txn_rec->is_whole_committed());
    return {false, nullptr};
  }

  std::tie(active, std::ignore) =
      txn_slot_read_low(txn_rec, &txn_lookup, Cache_hint::KEEP_OLD, nullptr);

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
  cache_hit = trx_search_tcn(txn_rec, &txn_status);
  if (cache_hit) {
    ut_ad(txn_rec->is_whole_committed());
    /** Collect record to cleanout later. */
    cctx.collect_txn(*txn_rec, layout);
    return;
  }

  ut_ad(cache_hit == false);

  std::tie(active, txn_status) =
      txn_slot_read_low(txn_rec, &txn_lookup, Cache_hint::KEEP_OLD, nullptr);
  ut_ad(!txn_lookup.was_slot_fixed());

  if (!active) {
    ut_ad(txn_rec->is_whole_committed());
    /** Collect record to cleanout later.*/
    cctx.collect_txn(*txn_rec, layout);
    /** Cache txn info into tcn. */
    trx_cache_tcn(*txn_rec, txn_status);
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
void txn_rec_cleanout_when_modify(const trx_id_t trx_id, rec_t *rec,
                                  const dict_index_t *index,
                                  const ulint *offsets,
                                  const txn_layout_t &layout,
                                  const buf_block_t *block, mtr_t *mtr) {
  trx_id_t rec_id;
  bool cleanout = false;

  ut_ad(trx_id > 0);

  rec_id = row_get_rec_trx_id(rec, index, offsets);

  if (trx_id == rec_id) {
    /* update a exist row which has been modified by
    the current active transaction */
    return;
  }

  /** scn must be consistent with the undo_ptr */
  assert_row_txn_is_valid(rec, index, offsets, layout);

  ut_ad(index->is_clustered() || index->is_panda());
  ut_ad(!index->table->is_intrinsic());

  txn_rec_t rec_txn(rec, index, offsets, layout);

  /** lookup the scn by UBA address */
  cleanout = txn_rec_cleanout_state(&rec_txn, Cache_hint::KEEP_OLD);

  if (cleanout) {
    ut_ad(mtr_memo_contains_flagged(mtr, block, MTR_MEMO_PAGE_X_FIX));

    switch (layout) {
      case TL_CLOVER:
        row_upd_rec_clover_fields_in_cleanout(
            const_cast<rec_t *>(rec),
            const_cast<page_zip_des_t *>(buf_block_get_page_zip(block)), index,
            offsets, &rec_txn);
        /** Write redo log */
        if (opt_cleanout_write_redo)
          btr_cur_upd_clover_fields_clust_rec_log(rec, index, &rec_txn, mtr);
        break;
      case TL_BAMBOO:
        row_upd_rec_bamboo_fields_in_cleanout(
            const_cast<rec_t *>(rec),
            const_cast<page_zip_des_t *>(buf_block_get_page_zip(block)), index,
            offsets, &rec_txn);
        /** Write redo log */
        if (opt_cleanout_write_redo)
          btr_cur_upd_bamboo_fields_sec_rec_log(rec, index, &rec_txn, mtr);
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
  txn_slot_read_low(txn_rec, &txn_lookup, Cache_hint::KEEP_OLD, nullptr);

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
  Check if the TXN is purged or erased. The latch of the TXN page will be held
  if precheck failed.

  @param[in/out]  txn_rec         txn_info of record
  @param[in]      flashback_area  true if it's a flashback area query
  @param[in]      txn_mtr         txn mtr

  @retval         true if txn has been purged (non flashback area) or
                  erased (flashback area)
*/
bool txn_rec_is_missing_history(txn_rec_t *txn_rec, bool flashback_area,
                                mtr_t *txn_mtr) {
  txn_lookup_t txn_lookup;

  DBUG_EXECUTE_IF("simulate_prev_image_purged_during_query", return true;);

  /** precheck, if the record has been cleanout, and the TXN has been purged,
  no need to hold TXN page latch and undo page latch */
  if (flashback_area) {
    if (txn_rec_is_erased_by_precheck(txn_rec)) {
      /** Must be cleanout, so no need to lookup again */
      ut_ad(txn_rec->is_committed());
      return true;
    }
  } else {
    if (txn_rec_is_purged_by_precheck(txn_rec)) {
      /** Must be cleanout, so no need to lookup again */
      ut_ad(txn_rec->is_committed());
      return true;
    }
  }

  /** precheck fail, then lookup by reading txn. */
  txn_slot_read_low(txn_rec, &txn_lookup, Cache_hint::KEEP_OLD, txn_mtr);
  ut_ad(!txn_lookup.was_slot_fixed());

  return txn_lookup.undo_missing(flashback_area);
}

/**
  Clean out the record when hit tcn cache.
  Attempt to collect the cursor, and it will be cleaned out when the query
  finishes.

  @param[in/out]  txn_rec	  txn record
  @param[in]      layout    txn layout
  @param[in/out]  cleanout collector
*/
static void txn_rec_cleanout_when_hit(txn_rec_t *txn_rec,
                                      const txn_layout_t &layout,
                                      cleanout_ctx_t &cctx) {
  bool cache_hit = false;
  txn_status_t txn_status;

  ut_ad(txn_rec->is_active());
  ut_ad(cctx.is_usable());

  /** Search tcn cache */
  cache_hit = trx_search_tcn(txn_rec, &txn_status);
  if (cache_hit) {
    ut_ad(txn_rec->is_whole_committed());
    /** Collect record to cleanout later. */
    cctx.collect_txn(*txn_rec, layout);
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
  trx_id_t trx_id;

  trx_id = txn_rec->trx_id;
  ut_ad(trx_id > 0 && trx_id < TRX_ID_MAX);

  see = vision->sees(trx_id);

  if (see && txn_rec->is_active() && cctx.is_usable()) {
    txn_rec_cleanout_when_hit(txn_rec, layout, cctx);
  }

  return see;
}

}  // namespace lizard
