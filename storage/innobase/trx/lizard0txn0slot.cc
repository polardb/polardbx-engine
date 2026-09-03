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

/** @file trx/lizard0txn0slot.cc
  Lizard transactional slot management.

 Created 2025-07-03 by Jianwei.zhao
 *******************************************************/

#include "trx0rseg.h"

#include "lizard0cleanout0safe.h"
#include "lizard0dbg.h"
#include "lizard0gcs.h"
#include "lizard0txn0slot.h"
#include "lizard0txn0slot0types.h"
#include "lizard0txn0slot0uba0types.h"
#include "lizard0txn0slot0undo.h"
#include "lizard0txn0space.h"
#include "lizard0undo.h"

extern void trx_undo_read_xid(const trx_ulogf_t *log_hdr, XID *xid);

/**
  Decode the slot_ptr into slot address
  @param[in]      slot ptr
*/
void slot_addr_t::decode(slot_ptr_t slot_ptr) {
  ulint rseg_id;

  offset = (ulint)slot_ptr & 0xFFFF;
  slot_ptr >>= UBA_WIDTH_OFFSET;

  page_no = (ulint)slot_ptr & 0xFFFFFFFF;
  slot_ptr >>= UBA_WIDTH_PAGE_NO;

  rseg_id = (ulint)slot_ptr & 0x7F;
  slot_ptr >>= UBA_WIDTH_SPACE_ID;

  /* Confirm the reserved bits. slot_ptr has already been right-shifted past
  offset/page_no/space_id here, so the unused bits now sit at the low end and
  must be matched with the position-relative mask. */
  ut_ad(((ulint)slot_ptr & (UBA_MASK_UNUSED >> UBA_POS_UNUSED)) == 0);

  if (rseg_id == SLOT_SPACE_NUM_FAKE) {
    space_id = SLOT_SPACE_ID_FAKE;
  } else if (rseg_id > FSP_IMPLICIT_TXN_TABLESPACES) {
    space_id = SPACE_UNKNOWN;
  } else {
    space_id = trx_rseg_id_to_space_id(rseg_id, false);
  }
}

bool slot_addr_t::is_null() const { return *this == null(); }
bool slot_addr_t::is_no_redo() const { return *this == no_redo(); }
bool slot_addr_t::is_redo() const {
  return lizard::fsp_is_txn_tablespace_by_id(space_id);
}

/**
  Check the slot address if is actually points at the real disk storage.
*/
bool slot_addr_t::is_disk_mapped() const {
  /* Space ID */
  if (!lizard::fsp_is_txn_tablespace_by_id(space_id)) {
    return false;
  }

  /* Offset */
  if (offset < TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE) {
    return false;
  }

  if ((offset - (TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE)) %
      lizard::txn_slot_hdr_fix_size()) {
    return false;
  }
  return true;
}

/** Check slot address validation. */
bool slot_addr_t::is_validate() const {
  /** no_redo insert/update undo */
  if (space_id == SLOT_SPACE_ID_FAKE) {
    return true;
  } else if (is_null()) {
    return true;
  } else {
    return is_disk_mapped();
  }
}

/**
  Encode Slot_addr into slot_ptr

  @return slot_ptr_t */
slot_ptr_t slot_addr_t::encode() const {
  ulint rseg_id;

  ut_ad(!is_null());
  ut_ad(is_validate());

  if (space_id == SLOT_SPACE_ID_FAKE) {
    rseg_id = SLOT_SPACE_NUM_FAKE;
  } else {
    rseg_id = undo::id2num(space_id);
  }

  return (slot_ptr_t)(rseg_id) << UBA_POS_SPACE_ID |
         (slot_ptr_t)(page_no) << UBA_POS_PAGE_NO | offset;
}

/*-----------------------------------------------------------------------------*/
/* undo_addr_t related */
/*-----------------------------------------------------------------------------*/
void undo_addr_t::decode(undo_ptr_t undo_ptr) {
  slot_addr_t::decode((slot_ptr_t)undo_ptr);

  is_slave =
      static_cast<bool>((undo_ptr & UBA_MASK_IS_SLAVE) >> UBA_POS_IS_SLAVE);

  csr = static_cast<csr_t>((undo_ptr & UBA_MASK_CSR) >> UBA_POS_CSR);

  state = static_cast<bool>((undo_ptr & UBA_MASK_STATE) >> UBA_POS_STATE);

  /**
    It should not be trx_sys tablespace for normal table except
    of temporary table/LOG_DDL/DYNAMIC_METADATA/DDL in-process table */

  /**
    Revision:
    We give a fixed UBA in undo log header if didn't allocate txn undo
    for temporary table.
  */
  if (space_id == SLOT_SPACE_ID_FAKE) {
    ut_ad(offset >= SLOT_OFFSET_LIMIT);
  }
}

undo_ptr_t undo_addr_t::encode() const {
  return (undo_ptr_t)(state) << UBA_POS_STATE |
         (undo_ptr_t)(csr) << UBA_POS_CSR |
         (undo_ptr_t)(is_slave) << UBA_POS_IS_SLAVE |
         (undo_ptr_t)slot_addr_t::encode();
}


/** Check the UBA validation */
bool undo_addr_t::is_validate(const dict_index_t *index) {
  bool internal_dm_table = false;
  if (index) {
    internal_dm_table =
        (my_strcasecmp(system_charset_info, index->table->name.m_name,
                       "mysql/innodb_dynamic_metadata") == 0
             ? true
             : false);
  }

  if ((index && index->table->is_temporary())) {
    ut_a(lizard::txn_sys_t::instance()->is_temporary(*this));
  } else if (internal_dm_table) {
    ut_a(lizard::txn_sys_t::instance()->is_dynamic_metadata(*this));
  }

  /** If not special, must be normal txn undo address. */
  if (!lizard::txn_sys_t::instance()->is_special(*this)) {
    ut_a(slot_addr_t::is_validate());
  }
  return true;
}

/*-----------------------------------------------------------------------------*/
/* txn_slot_t related */
/*-----------------------------------------------------------------------------*/
bool txn_slot_t::tags_allocated() const {
  return xes_storage_t(xes_storage).has_tags();
}

bool txn_slot_t::is_rollback() const {
  /** The TXN must be the new format. */
  ut_a(tags_allocated());

  switch (state) {
    case txn_slot_t::COMMITED:
    case txn_slot_t::PURGED:
      return xes_tags_t(tags).is_rollback();
    case txn_slot_t::ACTIVE:
      ut_a(!(xes_tags_t(tags).is_rollback()));
      return false;
    default:
      ut_error;
  }
}

bool txn_slot_t::ac_prepare_allocated() const {
  return xes_storage_t(xes_storage).has_ac_prepare();
}

bool txn_slot_t::ac_commit_allocated() const {
  return xes_storage_t(xes_storage).has_ac_commit();
}


bool txn_slot_t::is_validate() const {
  if (magic_n != txn_slot_t::MAGIC_N) {
    return false;
  }

  if (tags_allocated()) {
    if (state == txn_slot_t::ACTIVE) {
      if (is_rollback()) {
        return false;
      }
    }
  }

  if (ac_commit_allocated()) {
    if (maddr.is_null()) {
      return false;
    }
  }

  slot_addr_t slot_addr(slot_ptr);
  if (!slot_addr.is_validate()) {
    return false;
  }

  return true;
}

namespace lizard {

/** Confirm value and print undo address if not true.*/
template <typename Type>
static inline void ut_print(const slot_addr_t &slot_addr, ut::Location loc,
                            Type value) {
  if (!value) {
    lizard_error(ER_LIZARD)
        << slot_addr.print() << " at file=" << basename(loc.filename)
        << ", line=" << loc.line;
    ut_a(0);
  }
}

/** Check txn slot through transaction id. */
bool Txn_slot_reuse_by_tid_checker::operator()(
    const trx_ulogf_t *log_hdr) const {
  trx_id_t real_trx_id;
  real_trx_id = mach_read_from_8(log_hdr + TRX_UNDO_TRX_ID);

  return real_trx_id != m_trx_id;
}

/** Check txn slot through xid. */
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

/** Gets an undo log page block with cache hint and s-latches it.

  @param[in]      page_id         Page id
  @param[in]      page_size       Page size
  @param[in]      block           fixed txn slot buffer block
  @param[in]      hint            Cache hint
  @param[in,out]  mtr             Mini-transaction

  @return pointer to page s-latched */
static buf_block_t *txn_slot_block_get_s_latched(const page_id_t &page_id,
                                                 const page_size_t &page_size,
                                                 buf_block_t *block,
                                                 Cache_hint hint, mtr_t *mtr) {
  if (block == nullptr) {
    return trx_undo_block_get_s_latched_with_hint(page_id, univ_page_size, hint,
                                                  mtr);
  }
  ut_ad(block);
  ut_ad(mtr->is_active());

  buf_page_mutex_enter(block);
  ut_a(buf_block_get_state(block) == BUF_BLOCK_FILE_PAGE);

  buf_block_buf_fix_inc(block, UT_LOCATION_HERE);
  buf_page_mutex_exit(block);

  mtr_memo_type_t fix_type = MTR_MEMO_PAGE_S_FIX;
  rw_lock_s_lock_gen(&block->lock, 0, UT_LOCATION_HERE);
  mtr_memo_push(mtr, block, fix_type);

  ut_a(block->page.id == page_id);
  ut_ad(block->page.buf_fix_count.load() >= 2);

  auto buf_pool = buf_pool_from_block(block);
  Counter::inc(buf_pool->stat.m_n_page_gets, block->page.id.page_no());
  PPI_STATEMENT_CALL(inc_statement_logical_read)(1);

  return block;
}

/**
  Try to read the real scn of given records. Address directly to the
  corresponding txn undo header by UBA.

  @param[in]      slot_ptr        slot address
  @param[in]      fixed_block     already fixed block
  @param[in]      hint            Cache hint
  @param[in]      guess           ignore missing if fail to get page
  @param[in]      txn_mtr         txn mtr
  @param[in]      reuse_checker   check if the TXN is reused.
  @param[out]     txn_lookup      txn lookup result, nullptr if don't care.

  @return         bool            whether corresponding trx is active.
*/
static bool _txn_slot_read_func(const slot_addr_t &slot_addr,
                                buf_block_t *fixed_block, Cache_hint hint,
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
  const bool skip_fatal = guess;

  have_mtr = (txn_mtr != nullptr);

  mtr = have_mtr ? txn_mtr : &temp_mtr;

  ut_ad(mtr && txn_lookup);

  /** ----------------------------------------------------------*/
  /** Phase 1: Read the undo header page */
  ut_ad(slot_addr.offset <= UNIV_PAGE_SIZE_MAX);

  const page_id_t page_id(slot_addr.space_id, slot_addr.page_no);

  if (!have_mtr) mtr_start(mtr);

  /** Undo tablespace always univ_page_size */
  if (!guess) {
    ut_ad(!(txn_lookup->is_do_ref_count() && fixed_block));
    undo_block = txn_slot_block_get_s_latched(page_id, univ_page_size,
                                              fixed_block, hint, mtr);
  } else {
    /** guess didn't support fix mode. */
    ut_ad(!txn_lookup->is_do_ref_count());
    ut_ad(fixed_block == nullptr);
    undo_block = trx_undo_block_guess_s_latched_with_hint(
        page_id, univ_page_size, hint, mtr);
  }

  if (undo_block) {
    undo_page = buf_block_get_frame(undo_block);
  }

  /** transaction tablespace didn't allowed to be truncated */
  if (UNIV_UNLIKELY(!undo_page)) {
    ut_print(slot_addr, UT_LOCATION_HERE, skip_fatal);
    goto undo_invalid;
  }

  /** ----------------------------------------------------------*/
  /** Phase 2: Judge the fil page */
  fil_type = fil_page_get_type(undo_page);

  /** The type of undo log segment must be FIL_PAGE_UNDO_LOG */
  if (UNIV_UNLIKELY(fil_type != FIL_PAGE_UNDO_LOG)) {
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

  if (UNIV_UNLIKELY(undo_page_type != TRX_UNDO_TXN)) {
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

    So TRX_UNDO_FLAGS (might use TXN_SLOT_EXT_MAGIC ?) is used to determine
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
  if (UNIV_UNLIKELY(!(hdr_flag & TRX_UNDO_FLAG_TXN))) {
    ut_print(slot_addr, UT_LOCATION_HERE, skip_fatal);
    goto undo_invalid;
  }

  /** ----------------------------------------------------------*/
  /** Phase 8: check the txn extension fields in txn undo header */
  /** Already check page type and txn flags. */
  txn_slot_hdr_read_txn_slot(undo_page, undo_hdr, mtr, &txn_slot);
  ut_print(slot_addr, UT_LOCATION_HERE,
           (txn_slot.magic_n == txn_slot_t::MAGIC_N));

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
  if (txn_slot.state == txn_slot_t::ACTIVE) {
    ut_ad(mach_read_from_2(seg_hdr + TRX_UNDO_LAST_LOG) == slot_addr.offset);
    ut_ad(real_trx_state == TRX_UNDO_ACTIVE ||
          real_trx_state == TRX_UNDO_PREPARED_80028 ||
          real_trx_state == TRX_UNDO_PREPARED ||
          real_trx_state == TRX_UNDO_PREPARED_IN_TC);
    goto still_active;
  } else if (txn_slot.state == txn_slot_t::COMMITED) {
    goto already_commit;
  } else if (txn_slot.state == txn_slot_t::PURGED) {
    goto undo_purged;
  } else {
    lizard_ut_ad(txn_slot.state == txn_slot_t::ERASED);
    goto undo_erased;
  }

still_active:
  ut_ad(txn_slot.image.is_initial());
  txn_lookup->init(txn_slot, txn_slot.image, txn_status_t::ACTIVE);
  txn_lookup->fix_slot_when_active(undo_block);

  if (!have_mtr) mtr_commit(mtr);
  return true;

already_commit:
  ut_ad(txn_slot.image.is_alloced());
  txn_lookup->init(txn_slot, txn_slot.image, txn_status_t::COMMITTED);
  if (!have_mtr) mtr_commit(mtr);
  return false;

undo_purged:
  ut_ad(txn_slot.image.is_alloced());
  txn_lookup->init(txn_slot, txn_slot.image, txn_status_t::PURGED);
  if (!have_mtr) mtr_commit(mtr);
  return false;

undo_erased:
  ut_ad(txn_slot.image.is_alloced());
  txn_lookup->init(txn_slot, txn_slot.image, txn_status_t::ERASED);
  if (!have_mtr) mtr_commit(mtr);
  return false;

undo_reuse:
  ut_ad(txn_slot.prev_image.is_alloced());
  txn_lookup->init(txn_slot, txn_slot.prev_image, txn_status_t::REUSE);
  if (!have_mtr) mtr_commit(mtr);
  return false;

undo_invalid:
  /** Can't never be lost if cleanout_safe_mode isn't taken into
  consideration */
  ut_a(opt_cleanout_safe_mode || guess);
  txn_lookup->init(txn_slot, commit_mark_t::invalid(),
                   txn_status_t::UNDO_INVALID);
  if (!have_mtr) mtr_commit(mtr);
  return false;
}

/**
  Try to lookup the real scn of given records.

  @param[in/out]  txn_rec       txn info of the records.
  @param[out]     txn_lookup    txn lookup result, nullptr if don't care
  @param[in]      buf block	txn slot buffer block which was fixed
  @param[in]      hint          Cache hint
  @param[in]      txn_mtr       Non-nullptr if use external mtr, the caller is
                                responsible for committing mtr;
                                If passing nullptr, it will use a temporary mtr.

  @return         pair          first: whether corresponding trx is active.
                                second: txn slot real status.
*/
static std::pair<bool, txn_status_t> txn_slot_read_low(txn_rec_t *txn_rec,
                                                       txn_lookup_t *txn_lookup,
                                                       buf_block_t *block,
                                                       const Cache_hint hint,
                                                       mtr_t *txn_mtr) {
  bool ret = false;
  ut_ad(txn_lookup);
  ut_ad(!lizard::txn_sys_t::instance()->is_special(txn_rec->undo_ptr));

  /** In theory, lizard has to findout the real acutal scn (if have) by
  uba */
  generic_stats.txn_undo_lookup_by_uba.inc();

  if (opt_cleanout_safe_mode) {
    undo_addr_t undo_addr(txn_rec->undo_ptr);
    bool exist = txn_undo_logs->exist({undo_addr.space_id, undo_addr.page_no});
    if (!exist) {
      commit_mark_t cmmt = commit_mark_t::invalid();
      proposal_mark_t pmmt = proposal_mark_t::invalid();
      txn_slot_t txn_slot(cmmt,
                          /** txn_slot.undo_ptr should be from txn undo header,
                          and it must be active state when coming here */
                          txn_rec->undo_ptr, txn_rec->trx_id,
                          txn_slot_t::MAGIC_N, cmmt, txn_slot_t::PURGED, 0, 0,
                          false, pmmt, {0, 0}, {0, 0});

      txn_rec->scn = cmmt.scn;
      txn_rec->gcn = cmmt.gcn;
      undo_addr_t::set_commit(&txn_rec->undo_ptr, CSR_AUTOMATIC, false);
      txn_lookup->init(txn_slot, cmmt, txn_status_t::UNDO_INVALID);

      generic_stats.txn_undo_lost_page_miss_when_safe.inc();
      return std::make_pair(false, txn_lookup->real_status);
    }
  }

  Txn_slot_reuse_by_tid_checker tid_checker(txn_rec->trx_id);

  ret = _txn_slot_read_func(slot_addr_t(txn_rec->undo_ptr), block, hint, false,
                            txn_mtr, tid_checker, txn_lookup);
  const txn_slot_t &txn_slot = txn_lookup->txn_slot;

  switch (txn_lookup->real_status) {
    case txn_status_t::ACTIVE:
      break;
    case txn_status_t::COMMITTED:
      txn_rec->scn = txn_slot.image.scn;
      txn_rec->gcn = txn_slot.image.gcn;
      undo_addr_t::set_commit(&txn_rec->undo_ptr, txn_slot.image.csr,
                          !txn_slot.maddr.is_null());
      break;
    case txn_status_t::PURGED:
      txn_rec->scn = txn_slot.image.scn;
      txn_rec->gcn = txn_slot.image.gcn;
      undo_addr_t::set_commit(&txn_rec->undo_ptr, txn_slot.image.csr,
                          !txn_slot.maddr.is_null());
      break;
    case txn_status_t::ERASED:
      txn_rec->scn = txn_slot.image.scn;
      txn_rec->gcn = txn_slot.image.gcn;
      undo_addr_t::set_commit(&txn_rec->undo_ptr, txn_slot.image.csr,
                          !txn_slot.maddr.is_null());
      break;
    case txn_status_t::REUSE:
      txn_rec->scn = txn_slot.prev_image.scn;
      txn_rec->gcn = txn_slot.prev_image.gcn;
      undo_addr_t::set_commit(&txn_rec->undo_ptr, txn_slot.prev_image.csr, false);
      break;
    case txn_status_t::UNDO_INVALID:
    default:
      txn_rec->scn = commit_mark_t::invalid().scn;
      txn_rec->gcn = commit_mark_t::invalid().gcn;
      undo_addr_t::set_commit(&txn_rec->undo_ptr, CSR_AUTOMATIC, false);
      break;
  }

  return std::make_pair(ret, txn_lookup->real_status);
}




/**
  Try to lookup the real scn of given record only once,

  @param[in/out]  txn_rec       txn info of the records.
  @param[out]     txn_lookup    txn lookup result, nullptr if don't care
  @param[in]      hint          Cache hint

  @return         pair          first: whether corresponding trx is active.
                                second: txn slot real status.
*/
std::pair<bool, txn_status_t> txn_slot_read_once(txn_rec_t *txn_rec,
                                                 txn_lookup_t *txn_lookup,
                                                 const Cache_hint hint) {
  return txn_slot_read_low(txn_rec, txn_lookup, nullptr, hint, nullptr);
}
/**
  Try to lookup the real scn of given record again with fixed block,

  @param[in/out]  txn_rec       txn info of the records.
  @param[out]     txn_lookup    txn lookup result, nullptr if don't care
  @param[in]      hint          Cache hint

  @return         pair          first: whether corresponding trx is active.
                                second: txn slot real status.
*/
std::pair<bool, txn_status_t> txn_slot_read_again(txn_rec_t *txn_rec,
                                                  txn_lookup_t *txn_lookup,
                                                  buf_block_t *block,
                                                  const Cache_hint hint) {
  return txn_slot_read_low(txn_rec, txn_lookup, block, hint, nullptr);
}

/**
  Try to lookup the real scn of given record with related mtr,

  @param[in/out]  txn_rec       txn info of the records.
  @param[out]     txn_lookup    txn lookup result, nullptr if don't care
  @param[in]      hint          Cache hint

  @return         pair          first: whether corresponding trx is active.
                                second: txn slot real status.
*/
std::pair<bool, txn_status_t> txn_slot_read_couple(txn_rec_t *txn_rec,
                                                   txn_lookup_t *txn_lookup,
                                                   const Cache_hint hint,
                                                   mtr_t *mtr) {
  return txn_slot_read_low(txn_rec, txn_lookup, nullptr, hint, mtr);
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

  if (!slot_addr.is_disk_mapped() &&
      !DBUG_EVALUATE_IF("force_do_slot_read", 1, 0)) {
    return false;
  }

  bool guess = true;
  DBUG_EXECUTE_IF("expect_txn_read_guess_success", guess = false;);
  _txn_slot_read_func(slot_addr, nullptr, hint, guess, nullptr, reuse_checker,
                      txn_lookup);
  ut_ad(!txn_lookup->was_slot_fixed());

  bool missing = txn_lookup->txn_missing();

  if (missing) {
    generic_stats.txn_read_guess_fail.inc();
  }

  return !missing;
}

/**
  precheck if txn of the row is cycled, without really reading txn

  @param[in]    txn_rec     the current row to be checked

  @retval       bool        true if the corresponding txn has been cycled
*/
static bool txn_slot_is_cycled_by_precheck(const txn_rec_t *txn_rec) {
  if (txn_rec->is_committed()) {
    return (txn_rec->scn < gcs_clean_cycled_position().scn);
  }
  return false;
}

/**
  precheck if txn of the row is purged, without really reading txn

  @param[in]    txn_rec     the current row to be checked

  @retval       bool        true if the corresponding txn has been purged
*/
[[maybe_unused]] static bool txn_slot_is_purged_by_precheck(
    const txn_rec_t *txn_rec) {
  if (txn_rec->is_committed()) {
    return (txn_rec->scn <= gcs_clean_purged_scn());
  }
  return false;
}

/**
  precheck if txn of the row is erased, without really reading txn

  @param[in]    txn_rec     the current row to be checked

  @retval       bool        true if the corresponding txn has been purged
*/
static bool txn_slot_is_erased_by_precheck(const txn_rec_t *txn_rec) {
  if (txn_rec->is_committed()) {
    return (txn_rec->scn <= gcs_clean_erased_scn());
  }
  return false;
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
bool txn_slot_is_missing_history(txn_rec_t *txn_rec, bool flashback_area,
                                mtr_t *txn_mtr) {
  txn_lookup_t txn_lookup;

  DBUG_EXECUTE_IF("simulate_prev_image_purged_during_query", return true;);

  /** precheck, if the record has been cleanout, and the TXN has been purged,
  no need to hold TXN page latch and undo page latch */
  if (flashback_area) {
    if (txn_slot_is_erased_by_precheck(txn_rec)) {
      /** Must be cleanout, so no need to lookup again */
      ut_ad(txn_rec->is_committed());
      return true;
    }
  } else {
    /** Revision:
     * Use cycled position is more accurate and efficient than purged. */
    if (txn_slot_is_cycled_by_precheck(txn_rec)) {
      /** Must be cleanout, so no need to lookup again */
      ut_ad(txn_rec->is_committed());
      return true;
    }
  }

  /** precheck fail, then lookup by reading txn. */
  txn_slot_read_couple(txn_rec, &txn_lookup, Cache_hint::KEEP_OLD, txn_mtr);
  ut_ad(!txn_lookup.was_slot_fixed());

  return txn_lookup.undo_missing(flashback_area);
}

}  // namespace lizard
