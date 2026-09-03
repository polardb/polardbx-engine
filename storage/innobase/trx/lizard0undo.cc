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

/** @file include/lizard0undo.h
  Lizard transaction undo and purge types.

 Created 2020-04-02 by Jianwei.zhao
 *******************************************************/

#include "page0types.h"
#include "trx0trx.h"
#include "trx0undo.h"

#include "ha_innodb.h"

#include "lizard0cleanout.h"
#include "lizard0cleanout0safe.h"
#include "lizard0cmmt0types.h"
#include "lizard0gcs.h"
#include "lizard0mon.h"
#include "lizard0row.h"
#include "lizard0txn.h"
#include "lizard0txn0slot0types.h"
#include "lizard0undo.h"
#include "lizard0xa.h"

/**
  SCN generation strategy:

  1) Always assign txn undo log for every transaction.

  2) All the records include temproary table, the undo log slot in the row point
     to the same txn undo log header whatever the undo type.

  3) The insert undo log didn't write the scn into the undo log header, since it
     will purge directly after commit.

  4) The temproary txn undo log scn number will be delayed written, it will be
     ok since the vision of record didn't look up temporary txn undo log header.

  ...

*/

/**
  The thread of SCN generation:

  1) trx->state = TRX_PREPARED

  2) hold rseg mutex

  3) finish the txn undo log header
      -- hold txn undo log header page X latch

  4) generate SCN number and write into txn undo log header

  5) cleanup the txn undo log header
      -- hold rseg header page X latch

  6) add rseg into purge queue
      -- hold purge queue mutex
      -- release mutex

  7) release rseg mutex

  8) mtr commit
      -- release undo log header page X latch
      -- release rseg header page X latch

  9) commit in memory

  ...
*/

/**
  Attention:
  The transaction ordered by scn in history list only promise within a rollback
  segment.
*/

bool trx_undo_t::tags_allocated() const {
  return xes_storage_t(xes_storage).has_tags();
}
void trx_undo_t::allocate_tags() { xes_storage_t::allocate_tags(xes_storage); }
bool trx_undo_t::ac_prepare_allocated() const {
  return xes_storage_t(xes_storage).has_ac_prepare();
}
bool trx_undo_t::ac_commit_allocated() const {
  return xes_storage_t(xes_storage).has_ac_commit();
}
void trx_undo_t::allocate_ac_prepare() {
  xes_storage_t::allocate_ac_prepare(xes_storage);
}
void trx_undo_t::allocate_ac_commit() {
  xes_storage_t::allocate_ac_commit(xes_storage);
}
void trx_undo_t::set_rollback_on_tags() { xes_tags_t::mark_rollback(tags); }
void trx_undo_t::set_ac_csr_assigned_on_tags() {
  xes_tags_t::mark_assigned(tags);
}
bool trx_undo_t::ac_csr_assigned_on_tags() const {
  return xes_tags_t(tags).is_assigned();
}

/** Confirm the consistent of scn, undo type, undo state. */
bool trx_undo_t::is_cmmt_validate() const {
  if (type == TRX_UNDO_INSERT) {
    if (state == TRX_UNDO_CACHED || state == TRX_UNDO_TO_FREE) {
      ut_a(cmmt.is_initial());
    } else if (state == TRX_UNDO_ACTIVE || is_prepared()) {
      ut_a(cmmt.is_initial());
    } else {
      ut_a(0);
    }
  } else if (type == TRX_UNDO_UPDATE) {
    if (state == TRX_UNDO_CACHED || state == TRX_UNDO_TO_PURGE) {
      /** The update undo log has put into history,
          so commit scn must be valid */
      ut_a(cmmt.is_alloced());
    } else if (state == TRX_UNDO_ACTIVE || is_prepared()) {
      /** The transaction still be active or has been prepared, */
      ut_a(cmmt.is_initial());
    } else if (state == TRX_UNDO_TO_FREE) {
      /** It's impossible to be FREE for update undo log */
      ut_a(0);
    } else {
      ut_a(0);
    }
  } else if (type == TRX_UNDO_TXN) {
    if (state == TRX_UNDO_CACHED || state == TRX_UNDO_TO_PURGE) {
      /** The txn undo log has put into history,
          so commit scn must be valid */
      ut_a(cmmt.is_alloced());
    } else if (state == TRX_UNDO_ACTIVE || is_prepared()) {
      /** The transaction still be active or has been prepared, */
      ut_a(cmmt.is_initial());
    } else if (state == TRX_UNDO_TO_FREE) {
      /** It's impossible to be FREE for txn undo log */
      ut_a(0);
    } else {
      ut_a(0);
    }
  } else {
    ut_a(0);
  }
  return true;
}

/** Confirm the consistent of proposal mark. */
bool trx_undo_t::is_pmmt_validate() const {
  if (type == TRX_UNDO_INSERT || type == TRX_UNDO_UPDATE) {
    ut_a(pmmt.is_initial());
  } else if (type == TRX_UNDO_TXN) {
    if (state == TRX_UNDO_CACHED || state == TRX_UNDO_TO_PURGE ||
        state == TRX_UNDO_PREPARED_IN_TC) {
      if (!pmmt.is_null()) {
        ut_a(pmmt.is_alloced());
      } else {
        ut_a(pmmt.is_initial());
      }
    } else if (state == TRX_UNDO_ACTIVE) {
      /** Might "prepare->rollback background" */
      ut_a(pmmt.is_initial() || pmmt.is_alloced());
    } else if (TRX_UNDO_PREPARED_80028 || state == TRX_UNDO_PREPARED) {
      ut_a(pmmt.is_initial());
    } else if (state == TRX_UNDO_TO_FREE) {
      /** It's impossible to be FREE for txn undo log */
      ut_error;
    } else {
      ut_error;
    }
  } else {
    ut_error;
  }
  return true;
}

namespace lizard {

/* Lizard transaction undo header operation */
/*-----------------------------------------------------------------------------*/

/**
  Validate the page is undo page

  @param[in]      page      undo page
  @return         true      it's undo page
*/
bool trx_undo_page_validate(const page_t *page) {
  const trx_upagef_t *page_hdr = nullptr;
  page_type_t page_type;
  ulint undo_type;

  ut_a(page);

  /** Valiate fil_page type */
  page_type = fil_page_get_type(page);
  if (page_type != FIL_PAGE_UNDO_LOG) return false;

  /** Validate undo type */
  page_hdr = page + TRX_UNDO_PAGE_HDR;
  undo_type = mach_read_from_2(page_hdr + TRX_UNDO_PAGE_TYPE);

  if (undo_type != TRX_UNDO_TXN && undo_type != TRX_UNDO_INSERT &&
      undo_type != TRX_UNDO_UPDATE)
    return false;

  return true;
}

/**
  Initial the NULL value on SCN and UTC when create undo log header.
  include all kinds of undo log header type.
  The redo log logic is included in "MLOG_UNDO_HDR_CREATE";

  @param[in]      log_hdr       undo log header
  @param[in]      mtr           current mtr context
*/
void trx_undo_hdr_init_cmmt(trx_ulogf_t *log_hdr, mtr_t *mtr) {
  ut_a(mtr && log_hdr);

  /** Here must hold the SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr, MTR_MEMO_PAGE_SX_FIX | MTR_MEMO_PAGE_X_FIX));

  /** Validate the undo page */
  ut_ad(trx_undo_page_validate(page_align(log_hdr)));

  mach_write_to_8(log_hdr + TRX_UNDO_SCN, SCN_NULL);
  mach_write_to_8(log_hdr + TRX_UNDO_UTC, UTC_NULL);
  mach_write_to_8(log_hdr + TRX_UNDO_GCN, GCN_NULL);
}

/**
  Write the scn and utc when commit.
  Include the redo log

  @param[in]      log_hdr       undo log header
  @param[in]      commit_mark    commit scn number
  @param[in]      mtr           current mtr context
*/
void trx_undo_hdr_write_cmmt(trx_ulogf_t *log_hdr, commit_mark_t &cmmt,
                             mtr_t *mtr) {
  /** Here must hold the SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr, MTR_MEMO_PAGE_SX_FIX | MTR_MEMO_PAGE_X_FIX));

  /** Validate the undo page */
  ut_ad(trx_undo_page_validate(page_align(log_hdr)));

  /** utc didn't include csr. */
  ut_ad(UTC::interpet_csr(cmmt.us) == 0);

  UTC utc(cmmt.us, cmmt.csr);

  mlog_write_ull(log_hdr + TRX_UNDO_SCN, cmmt.scn, mtr);
  mlog_write_ull(log_hdr + TRX_UNDO_UTC, utc.encode(), mtr);
  mlog_write_ull(log_hdr + TRX_UNDO_GCN, cmmt.gcn, mtr);
}

/**
  Read the scn and utc.

  @param[in]      log_hdr       undo log header
  @param[in]      mtr           current mtr context
*/
commit_mark_t trx_undo_hdr_read_cmmt(const trx_ulogf_t *log_hdr, mtr_t *mtr) {
  /** Here must hold the S/SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr,
      MTR_MEMO_PAGE_S_FIX | MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));

  /** Validate the undo page */
  ut_ad(trx_undo_page_validate(page_align(log_hdr)));

  commit_mark_t cmmt;
  UTC utc;

  cmmt.scn = mach_read_from_8(log_hdr + TRX_UNDO_SCN);
  cmmt.gcn = mach_read_from_8(log_hdr + TRX_UNDO_GCN);

  utc.decode(mach_read_from_8(log_hdr + TRX_UNDO_UTC));
  std::tie(cmmt.us, cmmt.csr) = utc.get();

  return cmmt;
}

/**
  Read slot address.

  @param[in]      log_hdr       undo log header
  @param[in]      mtr           current mtr context
  @return         decoded slot_addr_t
*/
slot_addr_t trx_undo_hdr_read_slot(const trx_ulogf_t *log_hdr, mtr_t *mtr) {
  /** Here must hold the S/SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr,
      MTR_MEMO_PAGE_S_FIX | MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));

  /** Validate the undo page */
  ut_ad(trx_undo_page_validate(page_align(log_hdr)));

  return slot_addr_t(mach_read_from_8(log_hdr + TRX_UNDO_SLOT));
}

/**
  Write the slot  address into undo log header
  @param[in]      undo log header
  @param[in]      slot
  @param[in]      mtr
*/
void trx_undo_hdr_write_slot(trx_ulogf_t *log_hdr, const slot_addr_t &slot_addr,
                             mtr_t *mtr) {
  /** Here must hold the SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr, MTR_MEMO_PAGE_SX_FIX | MTR_MEMO_PAGE_X_FIX));

  ut_ad(slot_addr.is_validate());

  mlog_write_ull(log_hdr + TRX_UNDO_SLOT, slot_addr.encode(), mtr);
}

/**
  Write the slot address into undo log header
  @param[in]      undo log header
  @param[in]      trx
  @param[in]      mtr
*/
slot_addr_t trx_undo_hdr_write_slot(trx_ulogf_t *log_hdr, const trx_t *trx,
                                    mtr_t *mtr) {
  /** Here must hold the SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr, MTR_MEMO_PAGE_SX_FIX | MTR_MEMO_PAGE_X_FIX));

  if (trx_is_txn_rseg_updated(trx)) {
    ut_ad(trx->txn_desc.is_slot_alloced());
    ut_ad(undo_addr_t::decode_csr(trx->txn_desc.undo_ptr) == CSR_AUTOMATIC);

    trx_undo_t *txn_undo = trx->rsegs.m_txn.txn_undo;
    ut_ad(txn_undo);

    slot_addr_t slot_addr = txn_undo->slot_addr;

    mlog_write_ull(log_hdr + TRX_UNDO_SLOT, slot_addr.encode(), mtr);
    return slot_addr;
  } else {
    /**
      If it's temporary table, didn't have txn undo, but it will have
      update/insert undo log header.
    */
    slot_addr_t slot_addr = slot_addr_t::no_redo();

    mlog_write_ull(log_hdr + TRX_UNDO_SLOT, slot_addr.encode(), mtr);
    return slot_addr;
  }
}
/** Iterate all undo log header include insert/update/txn undo log according to
 * prev/next list.
 *
 * @param[in]		undo header page
 * @param[in]		page size
 * @param[in]		start log header or nullptr
 * @param[in]		function
 * @param[in]		reverse or not
 * */
template <typename Functor>
bool trx_undo_log_iterate_by_list(const page_t *undo_page,
                                  const page_size_t &page_size,
                                  const trx_ulogf_t *log_hdr, mtr_t *mtr,
                                  Functor F, bool reverse = false) {
  const trx_usegf_t *seg_hdr = nullptr;
  const trx_ulogf_t *start = nullptr;
  ulint last_log = 0;
  ulint next = 0;
  ut_ad(mtr->memo_contains_page_flagged(undo_page, MTR_MEMO_PAGE_S_FIX |
                                                       MTR_MEMO_PAGE_X_FIX |
                                                       MTR_MEMO_PAGE_SX_FIX));
  seg_hdr = undo_page + TRX_UNDO_SEG_HDR;
  last_log = mach_read_from_2(seg_hdr + TRX_UNDO_LAST_LOG);
  if (last_log == 0) {
    ut_ad(log_hdr == nullptr);
    return false;
  }

  if (reverse) {
    start = undo_page + last_log;
  } else {
    start = seg_hdr + TRX_UNDO_SEG_HDR_SIZE;
  }

  if (log_hdr != nullptr) {
    start = log_hdr;
  }

  while (start != nullptr) {
    if (F(start)) return true;

    if (reverse) {
      next = mach_read_from_2(start + TRX_UNDO_PREV_LOG);
    } else {
      next = mach_read_from_2(start + TRX_UNDO_NEXT_LOG);
    }
    start = next == 0 ? nullptr : undo_page + next;
  }
  return false;
}

/**
 * Init segment tailer list when reuse txn undo log segemnt.
 *
 * @param[in/out]	txn undo page
 * @param[in]		page size
 * @param[in/out]	mtr */
void trx_useg_reuse(page_t *undo_page, const page_size_t &page_size,
                    mtr_t *mtr) {
#if defined UNIV_DEBUG
  byte flag = 0;
  flag = trx_useg_read_flag(undo_page, page_size, mtr);
  /** When reuse txn segment, the tailer is all zero or only flag sp_list
   *  modify here if add new flag.
   * */
  ut_a(flag != 0xff);
  if (trx_useg_flag_is_set(flag, TRX_USEG_FLAG_EXIST_2PP)) {
    ulint counter = 0;
    trx_undo_log_iterate_by_list(
        undo_page, page_size, nullptr, mtr,
        [&counter, &mtr](const trx_ulogf_t *log_hdr) -> bool {
          if (trx_undo_log_is_2pp(log_hdr, mtr)) {
            counter++;
          }
          return false;
        });
    ut_a(counter > 0);
  }
#endif

  byte *ptr = undo_page + page_size.logical();
  mlog_write_ulint(ptr - (TRX_USEG_END + TRX_USEG_END_FLAG), 0, MLOG_1BYTE,
                   mtr);
  return;
}

/**********************************************************************************/
//	Two Phase Purge
/**********************************************************************************/
/**
  Reads the two-phase purge flag in the transaction undo log header
  @param[in]  undo_header     Pointer to the undo log header
  @param[in]  mtr             Mini-transaction
  @return     True if the 2PP flag is set, false otherwise
*/
bool trx_undo_log_is_2pp(const trx_ulogf_t *log_hdr, mtr_t *mtr) {
  auto flag = mtr_read_ulint(log_hdr + TRX_UNDO_FLAGS, MLOG_1BYTE, mtr);
  return (flag & TRX_UNDO_FLAG_2PP);
}

/** Read undo log segment tailer flag.
 *
 *  @param[in]		undo log header page.
 *  @param[in]		page size
 *  @param[in]		mini transaction
 *
 *  @retval	flag (1byte)
 * */
byte trx_useg_read_flag(const page_t *undo_page, const page_size_t &page_size,
                        mtr_t *mtr) {
  byte flag = 0;
  ut_ad(mtr->memo_contains_page_flagged(undo_page, MTR_MEMO_PAGE_S_FIX |
                                                       MTR_MEMO_PAGE_X_FIX |
                                                       MTR_MEMO_PAGE_SX_FIX));

  flag = mach_read_from_1(undo_page + page_size.logical() -
                          (TRX_USEG_END + TRX_USEG_END_FLAG));
  return flag;
}

/**
  Check if the useg flag has the specified bit.

  NOTES:
  The flag might be 0xff, which is from the old version of the mysqld that
  might erase the end of the undo page by 0xFF.

  @param[in]    flag            useg flag
  @param[in]    bit_mask        bits to check

*/
bool trx_useg_flag_is_set(byte flag, byte bit_mask) {
  if (flag == 0xff) {
    return false;
  }

  return flag & bit_mask;
}

/**
  Check if is two-phase purge flag in the undo log segment tailer.
  @param[in]    undo log header page.
  @param[in]    page size
  @param[in]    mini transaction
*/
bool trx_useg_is_2pp(const page_t *undo_page, const page_size_t &page_size,
                     mtr_t *mtr) {
  byte flag = 0;
  ulint type;
  const trx_upagef_t *page_hdr;

  page_hdr = undo_page + TRX_UNDO_PAGE_HDR;
  type = mach_read_from_2(page_hdr + TRX_UNDO_PAGE_TYPE);
  ut_a(type == TRX_UNDO_UPDATE || type == TRX_UNDO_TXN);

  flag = trx_useg_read_flag(undo_page, page_size, mtr);
  return (trx_useg_flag_is_set(flag, TRX_USEG_FLAG_EXIST_2PP));
}

/** Set 2PP flag on undo log segment.
 *
 *  @param[in/out]	undo log header page.
 *  @param[in]		page size
 *  @param[in]		mini transaction
 * */
static void trx_useg_set_2pp(page_t *undo_page, const page_size_t &page_size,
                             mtr_t *mtr) {
  byte flag = 0;
  byte *ptr = undo_page + page_size.logical();
  ut_ad(mtr->memo_contains_page_flagged(undo_page, MTR_MEMO_PAGE_X_FIX));

  ut_a(trx_useg_verify(undo_page, page_size, mtr));

  flag = trx_useg_read_flag(undo_page, page_size, mtr);
  ut_a(flag != 0xff);
  if (!trx_useg_flag_is_set(flag, TRX_USEG_FLAG_EXIST_2PP)) {
    flag |= TRX_USEG_FLAG_EXIST_2PP;
    mlog_write_ulint(ptr - (TRX_USEG_END + TRX_USEG_END_FLAG), flag, MLOG_1BYTE,
                     mtr);
  }
}

/** Set 2PP flag on undo log header and flag undo log sement if not.
 *
 * @param[in/out]	undo
 * @param[in]		mini transaction
 * */
static void trx_undo_write_2pp(trx_undo_t *undo, mtr_t *mtr) {
  page_t *undo_page = nullptr;
  trx_ulogf_t *undo_hdr = nullptr;
  ulint offset = 0;
  ut_ad(undo);
  ut_ad(!(undo->flag & TRX_UNDO_FLAG_2PP));
  ut_ad(undo->type == TRX_UNDO_UPDATE || undo->type == TRX_UNDO_TXN);

  offset = undo->hdr_offset;
  /** Must hold rseg mutex. */
  ut_ad(mutex_own(&undo->rseg->mutex));

  undo_page = trx_undo_page_get(page_id_t(undo->space, undo->hdr_page_no),
                                undo->page_size, mtr);
  undo_hdr = undo_page + offset;

  undo->flag |= TRX_UNDO_FLAG_2PP;

  mlog_write_ulint(undo_hdr + TRX_UNDO_FLAGS, undo->flag, MLOG_1BYTE, mtr);

  trx_useg_set_2pp(undo_page, undo->page_size, mtr);
}

/** When report update undo, set 2pp flag if need.
 *
 * @param[in]		    index	  clust index or panda idnex
 * @param[in]		    trx	    transaction context
 * @param[in/out]	  undo	  update undo
 * @param[in/out]	  mtr     mini transaction */
void trx_undo_set_2pp_at_report(const dict_index_t *index, trx_t *trx,
                                trx_undo_t *update_undo, bool is_2pp) {
  trx_undo_t *txn_undo = nullptr;
  trx_rseg_t *txn_rseg = nullptr;
  trx_rseg_t *redo_rseg = nullptr;
  mtr_t mtr;
  ut_ad(trx && update_undo);
  ut_ad(index && index->table);
  ut_ad(index->is_clustered() || index->is_panda());

  if (update_undo->is_2pp() || !is_2pp) {
    return;
  }

  ut_ad(!index->table->is_temporary());

  /** Txn rollback segment should have been allocated */
  ut_ad(trx_is_txn_rseg_assigned(trx));
  ut_ad(mutex_own(&(trx->undo_mutex)));

  txn_rseg = trx->rsegs.m_txn.rseg;
  redo_rseg = trx->rsegs.m_redo.rseg;
  ut_ad(redo_rseg == update_undo->rseg);

  mtr.start();
  /** Hold both rseg mutex. */
  txn_rseg->latch();
  redo_rseg->latch();

  txn_undo = trx->rsegs.m_txn.txn_undo;
  ut_ad(txn_undo != nullptr);

  trx_undo_write_2pp(txn_undo, &mtr);
  trx_undo_write_2pp(update_undo, &mtr);

  txn_rseg->unlatch(false);
  redo_rseg->unlatch();
  mtr.commit();

#ifdef UNIV_DEBUG
  /** Validate the txn size after the undo page latches have been released. */
  txn_rseg->latch();
  txn_rseg->unlatch();
#endif /* UNIV_DEBUG */
}

/**
 * Init segment tailer when allocate new undo log segemnt.
 *
 * @param[in/out]	txn undo page
 * @param[in]		undo type
 * @param[in]		page size
 * @param[in/out]	mtr */
void trx_useg_allocate(page_t *undo_page, const page_size_t &page_size,
                       mtr_t *mtr) {
  byte flag = 0;

  flag = trx_useg_read_flag(undo_page, page_size, mtr);
  /** When allocate new txn segment, the tailer is all zero. */
  ut_a(flag == 0);

  return;
}

bool trx_useg_verify(page_t *undo_page, const page_size_t &page_size,
                     mtr_t *mtr) {
  byte flag = 0;

  flag = trx_useg_read_flag(undo_page, page_size, mtr);

  ut_a(flag == 0x00 /** Not used */ ||
       flag == 0xff /** trx_undo_erase_page_end from old version mysqld */ ||
       (flag & (~TRX_USEG_END_FLAG_MASK)) == 0x00);

  return true;
}

}  // namespace lizard
