/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */


/** @file include/lizard0undo.h
  Lizard transaction undo and purge types.

 Created 2020-04-02 by Jianwei.zhao
 *******************************************************/

#include "trx0rec.h"
#include "trx0undo.h"
#include "trx0rseg.h"
#include "page0types.h"

#include "sql_plugin_var.h"
#include "sql_error.h"
#include "sql_class.h"

#include "lizard0scn.h"
#include "lizard0sys.h"
#include "lizard0txn.h"
#include "lizard0undo.h"
#include "lizard0undo0types.h"
#include "lizard0mon.h"
#include "lizard0cleanout.h"
#include "lizard0row.h"

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

#ifdef UNIV_PFS_MUTEX
/* Lizard undo retention start mutex PFS key */
mysql_pfs_key_t lizard_undo_retention_mutex_key;
#endif

namespace lizard {

/** The max percent of txn undo page that can be reused */
ulint txn_undo_page_reuse_max_percent = TXN_UNDO_PAGE_REUSE_MAX_PCT_DEF;

/**
  Encode UBA into undo_ptr that need to copy into record
  @param[in]      undo addr
  @param[out]     undo ptr
*/
void undo_encode_undo_addr(const undo_addr_t &undo_addr, undo_ptr_t *undo_ptr) {
  lizard_undo_addr_validation(&undo_addr, nullptr);
  ulint rseg_id = undo::id2num(undo_addr.space_id);
  /** Reserved UNDO_PTR didn't need encode, so assert here */
  /** 1. assert temporary table undo ptr */
  lizard_ut_ad(undo_addr.offset != UNDO_PTR_OFFSET_TEMP_TAB_REC);

  *undo_ptr = (undo_ptr_t)(undo_addr.state) << UBA_POS_STATE |
              (undo_ptr_t)rseg_id << UBA_POS_SPACE_ID |
              (undo_ptr_t)(undo_addr.page_no) << UBA_POS_PAGE_NO |
              undo_addr.offset;
}

/* Lizard transaction undo header operation */
/*-----------------------------------------------------------------------------*/

#if defined UNIV_DEBUG || defined LIZARD_DEBUG

/** Check the UBA validation */
bool undo_addr_validation(const undo_addr_t *undo_addr,
                          const dict_index_t *index) {
  bool internal_dm_table = false;
  if (index) {
    internal_dm_table =
        (my_strcasecmp(system_charset_info, index->table->name.m_name,
                       "mysql/innodb_dynamic_metadata") == 0
             ? true
             : false);
  }

  if ((index && index->table->is_temporary())) {
    ut_a(undo_addr->state == true);
    ut_a(undo_addr->space_id == 0);
    ut_a(undo_addr->page_no == 0);
    ut_a(undo_addr->offset == UNDO_PTR_OFFSET_TEMP_TAB_REC);
  } else if (internal_dm_table) {
    ut_a(undo_addr->state == true);
    ut_a(undo_addr->space_id == 0);
    ut_a(undo_addr->page_no == 0);
    ut_a(undo_addr->offset == UNDO_PTR_OFFSET_DYNAMIC_METADATA);
  } else {
    ut_a(fsp_is_txn_tablespace_by_id(undo_addr->space_id));
    ut_a(undo_addr->page_no > 0);
    /** TODO: offset must be align to TXN_UNDO_EXT */
    ut_a(undo_addr->offset >= (TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE));
  }

  return true;
}
/**
  Validate the page is undo page

  @param[in]      page      undo page
  @return         true      it's undo page
*/
bool trx_undo_page_validation(const page_t *page) {
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

/** Confirm the consistent of scn, undo type, undo state. */
bool undo_scn_validation(const trx_undo_t *undo) {
  commit_scn_t scn = undo->cmmt;
  ulint type = undo->type;
  ulint state = undo->state;

  if (type == TRX_UNDO_INSERT) {
    if (state == TRX_UNDO_CACHED || state == TRX_UNDO_TO_FREE) {
      ut_a(commit_scn_state(scn) == SCN_STATE_INITIAL);
    } else if (state == TRX_UNDO_ACTIVE || state == TRX_UNDO_PREPARED) {
      ut_a(commit_scn_state(scn) == SCN_STATE_INITIAL);
    } else {
      ut_a(0);
    }
  } else if (type == TRX_UNDO_UPDATE) {
    if (state == TRX_UNDO_CACHED || state == TRX_UNDO_TO_PURGE) {
      /** The update undo log has put into history,
          so commit scn must be valid */
      ut_a(commit_scn_state(scn) == SCN_STATE_ALLOCATED);
    } else if (state == TRX_UNDO_ACTIVE || state == TRX_UNDO_PREPARED) {
      /** The transaction still be active or has been prepared, */
      ut_a(commit_scn_state(scn) == SCN_STATE_INITIAL);
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
      ut_a(commit_scn_state(scn) == SCN_STATE_ALLOCATED);
    } else if (state == TRX_UNDO_ACTIVE || state == TRX_UNDO_PREPARED) {
      /** The transaction still be active or has been prepared, */
      ut_a(commit_scn_state(scn) == SCN_STATE_INITIAL);
    } else if (state == TRX_UNDO_TO_FREE) {
      /** It's impossible to be FREE for update undo log */
      ut_a(0);
    } else {
      ut_a(0);
    }
  } else {
    ut_a(0);
  }
  return true;
}

/** Comfirm the commit scn is uninited */
static bool trx_undo_hdr_scn_committed(trx_ulogf_t *log_hdr, mtr_t *mtr) {
  commit_scn_t scn = trx_undo_hdr_read_scn(log_hdr, mtr);
  if (commit_scn_state(scn) == SCN_STATE_ALLOCATED) return true;

  return false;
}

/** Confirm the UBA is valid in undo log header */
bool trx_undo_hdr_uba_validation(const trx_ulogf_t *log_hdr, mtr_t *mtr) {
  undo_addr_t undo_addr;
  undo_ptr_t undo_ptr = trx_undo_hdr_read_uba(log_hdr, mtr);
  undo_decode_undo_ptr(undo_ptr, &undo_addr);
  /**
    Maybe the UBA in undo log header is fixed and predefined UBA
    or a meaningful UBA.
  */
  if (undo_ptr == UNDO_PTR_UNDO_HDR ||
      (undo_addr.state == true &&
       (fsp_is_txn_tablespace_by_id(undo_addr.space_id))))
    return true;

  return false;
}

/** Check if an update undo log has been marked as purged.
@param[in]  rseg txn rseg
@param[in]  page_size
@return     true   if purged */
bool txn_undo_log_has_purged(const trx_rseg_t *rseg,
                             const page_size_t &page_size) {
  if (fsp_is_txn_tablespace_by_id(rseg->space_id)) {
    ut_ad(!rseg->last_del_marks);
    /* Txn rseg is considered to be purged */
    return true;
  }

  page_t *page;
  trx_ulogf_t *log_hdr;
  ulint type, flag;
  trx_id_t trx_id;

  undo_addr_t undo_addr;
  undo_ptr_t uba_ptr;
  trx_id_t txn_trx_id;
  ulint txn_state;
  trx_ulogf_t *txn_hdr;

  mtr_t mtr;
  mtr_start(&mtr);

  /* Get current undo log header */
  page = trx_undo_page_get_s_latched(
      page_id_t(rseg->space_id, rseg->last_page_no), page_size, &mtr);

  log_hdr = page + rseg->last_offset;
  type = mach_read_from_2(page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE);
  flag = mach_read_from_1(log_hdr + TRX_UNDO_FLAGS);
  trx_id = mach_read_from_8(log_hdr + TRX_UNDO_TRX_ID);
  ut_ad(type == TRX_UNDO_UPDATE);
  ut_ad(!(flag & TRX_UNDO_FLAG_TXN));

  /* Get addr of the corresponding txn undo log header */
  uba_ptr = trx_undo_hdr_read_uba(log_hdr, &mtr);
  if (uba_ptr == UNDO_PTR_UNDO_HDR) goto no_txn;

  /** The insert/update undo should be released first, otherwise
  it will be deadlocked */
  mtr_commit(&mtr);

  undo_decode_undo_ptr(uba_ptr, &undo_addr);
  ut_ad(undo_addr.state && fsp_is_txn_tablespace_by_id(undo_addr.space_id));

  mtr_start(&mtr);

  /* Get the txn undo log header */
  txn_hdr = trx_undo_page_get_s_latched(
                page_id_t(undo_addr.space_id, undo_addr.page_no),
                univ_page_size, &mtr) +
            undo_addr.offset;

  txn_trx_id = mach_read_from_8(txn_hdr + TRX_UNDO_TRX_ID);
  txn_state = mach_read_from_2(txn_hdr + TXN_UNDO_LOG_STATE);

no_txn:
  mtr_commit(&mtr);

  /* No txn, so it is a tempory rseg, no need to check. */
  if (uba_ptr == UNDO_PTR_UNDO_HDR) return true;

  /* State of the txn undo log should be PURGED if not reused yet. */
  return (txn_trx_id != trx_id || txn_state == TXN_UNDO_LOG_PURGED);
}

#endif

/**
  Get txn undo state at trx finish.

  @param[in]      free_limit       space left on txn undo page
  @return  TRX_UNDO_TO_PURGE or TRX_UNDO_CACHED
*/
ulint decide_txn_undo_state_at_finish(ulint free_limit) {
  // 275 undo record + 100 safty margin.
  // why 100 ? In trx_undo_header_create:
  // ut_a(free + TRX_UNDO_LOG_GTID_HDR_SIZE < UNIV_PAGE_SIZE - 100);
  static const ulint min_reserve = TXN_UNDO_LOG_EXT_HDR_SIZE + 100;

  ulint reuse_limit = txn_undo_page_reuse_max_percent * UNIV_PAGE_SIZE / 100;

  if (free_limit >= reuse_limit) {
    return TRX_UNDO_TO_PURGE;
  } else if (free_limit + min_reserve >= UNIV_PAGE_SIZE) {
    return TRX_UNDO_TO_PURGE;
  } else {
    return TRX_UNDO_CACHED;
  }
}

/**
  Initial the NULL value on SCN and UTC when create undo log header.
  include all kinds of undo log header type.
  The redo log logic is included in "MLOG_UNDO_HDR_CREATE";

  @param[in]      log_hdr       undo log header
  @param[in]      mtr           current mtr context
*/
void trx_undo_hdr_init_scn(trx_ulogf_t *log_hdr, mtr_t *mtr) {
  ut_a(mtr && log_hdr);

  /** Here must hold the SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr, MTR_MEMO_PAGE_SX_FIX | MTR_MEMO_PAGE_X_FIX));

  /** Validate the undo page */
  lizard_trx_undo_page_validation(page_align(log_hdr));

  mach_write_to_8(log_hdr + TRX_UNDO_SCN, SCN_NULL);
  mach_write_to_8(log_hdr + TRX_UNDO_UTC, UTC_NULL);
  mach_write_to_8(log_hdr + TRX_UNDO_UBA, UNDO_PTR_NULL);
  mach_write_to_8(log_hdr + TRX_UNDO_GCN, GCN_NULL);
}

/**
  Write the scn and utc when commit.
  Include the redo log

  @param[in]      log_hdr       undo log header
  @param[in]      commit_scn    commit scn number
  @param[in]      mtr           current mtr context
*/
void trx_undo_hdr_write_scn(trx_ulogf_t *log_hdr, commit_scn_t &cmmt,
                            mtr_t *mtr) {
  /** Here must hold the SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr, MTR_MEMO_PAGE_SX_FIX | MTR_MEMO_PAGE_X_FIX));

  /** Validate the undo page */
  lizard_trx_undo_page_validation(page_align(log_hdr));

  mlog_write_ull(log_hdr + TRX_UNDO_SCN, cmmt.scn, mtr);
  mlog_write_ull(log_hdr + TRX_UNDO_UTC, cmmt.utc, mtr);
  mlog_write_ull(log_hdr + TRX_UNDO_GCN, cmmt.gcn, mtr);
}

/**
  Read the scn and utc.

  @param[in]      log_hdr       undo log header
  @param[in]      mtr           current mtr context
*/
commit_scn_t trx_undo_hdr_read_scn(const trx_ulogf_t *log_hdr, mtr_t *mtr) {
  /** Here must hold the S/SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr,
      MTR_MEMO_PAGE_S_FIX | MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));

  /** Validate the undo page */
  lizard_trx_undo_page_validation(page_align(log_hdr));

  commit_scn_t cmmt;

  cmmt.scn = mach_read_from_8(log_hdr + TRX_UNDO_SCN);
  cmmt.utc = mach_read_from_8(log_hdr + TRX_UNDO_UTC);
  cmmt.gcn = mach_read_from_8(log_hdr + TRX_UNDO_GCN);

  return cmmt;
}

/**
  Read the scn, utc, gcn from prev image.

  @param[in]      log_hdr       undo log header
  @param[in]      mtr           current mtr context
*/
commit_scn_t txn_undo_hdr_read_prev_scn(const trx_ulogf_t *log_hdr,
                                        mtr_t *mtr) {
  /** Here must hold the S/SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr,
      MTR_MEMO_PAGE_S_FIX | MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));

  /** Validate the undo page */
  lizard_trx_undo_page_validation(page_align(log_hdr));

  commit_scn_t cmmt;

  cmmt.scn = mach_read_from_8(log_hdr + TXN_UNDO_PREV_SCN);
  cmmt.utc = mach_read_from_8(log_hdr + TXN_UNDO_PREV_UTC);
  cmmt.gcn = mach_read_from_8(log_hdr + TXN_UNDO_PREV_GCN);

  return cmmt;
}

/**
  Add the space for the txn especially.

  @param[in]      undo_page     undo log header page
  @param[in]      log_hdr       undo log hdr
  @param[in]      mtr
*/
void trx_undo_hdr_add_space_for_txn(page_t *undo_page, trx_ulogf_t *log_hdr,
                                    mtr_t *mtr) {

  trx_upagef_t *page_hdr;
  ulint free;
  ulint new_free;

  page_hdr = undo_page + TRX_UNDO_PAGE_HDR;

  free = mach_read_from_2(page_hdr + TRX_UNDO_PAGE_FREE);

  /* free is now the end offset of the old style undo log header */
  ut_a(free == ((ulint)(log_hdr - undo_page) + TRX_UNDO_LOG_XA_HDR_SIZE));

  new_free = free + (TXN_UNDO_LOG_EXT_HDR_SIZE - TRX_UNDO_LOG_XA_HDR_SIZE);

  /* Add space for TXN extension after the header, update the free offset
  fields on the undo log page and in the undo log header */

  mlog_write_ulint(page_hdr + TRX_UNDO_PAGE_START, new_free, MLOG_2BYTES, mtr);

  mlog_write_ulint(page_hdr + TRX_UNDO_PAGE_FREE, new_free, MLOG_2BYTES, mtr);

  mlog_write_ulint(log_hdr + TRX_UNDO_LOG_START, new_free, MLOG_2BYTES, mtr);
}
/**
  Init the txn extension information.

  @param[in]      undo          undo memory struct
  @param[in]      undo_page     undo log header page
  @param[in]      log_hdr       undo log hdr
  @param[in]      prev_image    prev scn/utc if the undo log header is reused
  @param[in]      mtr
*/
void trx_undo_hdr_init_for_txn(trx_undo_t *undo, page_t *undo_page,
                               trx_ulogf_t *log_hdr,
                               const commit_scn_t &prev_image, mtr_t *mtr) {
  ut_ad(mach_read_from_2(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE) ==
        TRX_UNDO_TXN);

  /* Write the magic number */
  mlog_write_ulint(log_hdr + TXN_UNDO_LOG_EXT_MAGIC, TXN_MAGIC_N, MLOG_4BYTES,
                   mtr);

  assert_commit_scn_allocated(prev_image);
  /* Write the prev scn */
  mlog_write_ull(log_hdr + TXN_UNDO_PREV_SCN, prev_image.scn, mtr);
  /* Write the prev utc */
  mlog_write_ull(log_hdr + TXN_UNDO_PREV_UTC, prev_image.utc, mtr);
  /* Write the prev gcn */
  mlog_write_ull(log_hdr + TXN_UNDO_PREV_GCN, prev_image.gcn, mtr);

  /* Write initial state */
  txn_undo_set_state_at_init(log_hdr, mtr);

  /* Write the txn undo extension flag */
  mlog_write_ulint(log_hdr + TXN_UNDO_LOG_EXT_FLAG, 0, MLOG_1BYTE, mtr);

  ut_a(undo->flag == 0);

  undo->flag |= TRX_UNDO_FLAG_TXN;

  /** Write the undo flag when create undo log header */
  mlog_write_ulint(log_hdr + TRX_UNDO_FLAGS, undo->flag, MLOG_1BYTE, mtr);

  /** Copy prev image into undo structure */
  undo->prev_image = prev_image;
  assert_commit_scn_allocated(undo->prev_image);
}

/**
  Read UBA.

  @param[in]      log_hdr       undo log header
  @param[in]      mtr           current mtr context
*/
undo_ptr_t trx_undo_hdr_read_uba(const trx_ulogf_t *log_hdr, mtr_t *mtr) {
  /** Here must hold the S/SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr,
      MTR_MEMO_PAGE_S_FIX | MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));

  /** Validate the undo page */
  lizard_trx_undo_page_validation(page_align(log_hdr));

  return mach_read_from_8(log_hdr + TRX_UNDO_UBA);
}

/**
  Write the UBA address into undo log header
  @param[in]      undo log header
  @param[in]      UBA
  @param[in]      mtr
*/
void trx_undo_hdr_write_uba(trx_ulogf_t *log_hdr, const undo_addr_t &undo_addr,
                            mtr_t *mtr) {
  /** Here must hold the SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr, MTR_MEMO_PAGE_SX_FIX | MTR_MEMO_PAGE_X_FIX));

  undo_ptr_t undo_ptr;
  undo_encode_undo_addr(undo_addr, &undo_ptr);

  mlog_write_ull(log_hdr + TRX_UNDO_UBA, undo_ptr, mtr);
}

/**
  Write the UBA address into undo log header
  @param[in]      undo log header
  @param[in]      trx
  @param[in]      mtr
*/
void trx_undo_hdr_write_uba(trx_ulogf_t *log_hdr, const trx_t *trx,
                            mtr_t *mtr) {
  /** Here must hold the SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr, MTR_MEMO_PAGE_SX_FIX | MTR_MEMO_PAGE_X_FIX));

  if (trx_is_txn_rseg_updated(trx)) {
    assert_trx_undo_ptr_allocated(trx);
    /** Modify the state as commit, then write into log header */
    mlog_write_ull(log_hdr + TRX_UNDO_UBA,
                   trx->txn_desc.undo_ptr | (undo_ptr_t)1 << UBA_POS_STATE,
                   mtr);
  } else {
    /**
      If it's temporary table, didn't have txn undo, but it will have
      update/insert undo log header.
    */
    mlog_write_ull(log_hdr + TRX_UNDO_UBA, UNDO_PTR_UNDO_HDR, mtr);
  }
}

/**
  Read the txn undo log header extension information.

  @param[in]      undo page
  @param[in]      undo log header
  @param[in]      mtr
  @param[out]     txn_undo_hdr
*/
void trx_undo_hdr_read_txn(const page_t *undo_page,
                           const trx_ulogf_t *undo_header, mtr_t *mtr,
                           txn_undo_hdr_t *txn_undo_hdr) {
  ulint type;
  type = mtr_read_ulint(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE,
                        MLOG_2BYTES, mtr);
  ut_a(type == TRX_UNDO_TXN);

  auto flag = mtr_read_ulint(undo_header + TRX_UNDO_FLAGS, MLOG_1BYTE, mtr);

  /** If in cleanout safe mode,  */
  ut_a((flag & TRX_UNDO_FLAG_TXN) != 0 || opt_cleanout_safe_mode);

  /** read commit image in txn undo header */
  txn_undo_hdr->image.scn = mach_read_from_8(undo_header + TRX_UNDO_SCN);

  txn_undo_hdr->image.utc = mach_read_from_8(undo_header + TRX_UNDO_UTC);

  txn_undo_hdr->image.gcn = mach_read_from_8(undo_header + TRX_UNDO_GCN);

  txn_undo_hdr->undo_ptr = mach_read_from_8(undo_header + TRX_UNDO_UBA);

  txn_undo_hdr->trx_id = mach_read_from_8(undo_header + TRX_UNDO_TRX_ID);

  txn_undo_hdr->magic_n =
      mtr_read_ulint(undo_header + TXN_UNDO_LOG_EXT_MAGIC, MLOG_4BYTES, mtr);

  txn_undo_hdr->prev_image.scn =
      mach_read_from_8(undo_header + TXN_UNDO_PREV_SCN);

  txn_undo_hdr->prev_image.utc =
      mach_read_from_8(undo_header + TXN_UNDO_PREV_UTC);

  txn_undo_hdr->prev_image.gcn =
      mach_read_from_8(undo_header + TXN_UNDO_PREV_GCN);

  txn_undo_hdr->state = mtr_read_ulint(undo_header + TXN_UNDO_LOG_STATE,
                                       MLOG_2BYTES, mtr);

  txn_undo_hdr->ext_flag =
      mtr_read_ulint(undo_header + TXN_UNDO_LOG_EXT_FLAG, MLOG_1BYTE, mtr);

  ut_ad(txn_undo_hdr->magic_n == TXN_MAGIC_N && txn_undo_hdr->ext_flag == 0);
}

/* Lizard transaction rollback segment operation */
/*-----------------------------------------------------------------------------*/

/**
  Round-bin get the rollback segment from transaction tablespace

  @retval     rollback segment
*/
static trx_rseg_t *get_next_txn_rseg() {
  static ulint rseg_counter = 0;

  undo::Tablespace *undo_space;
  trx_rseg_t *rseg = nullptr;

  ulong n_rollback_segments = srv_rollback_segments;
  /** Lizard : didn't support variable of rollback segment count */
  ut_a(FSP_MAX_ROLLBACK_SEGMENTS == srv_rollback_segments);

  ulint current = rseg_counter;
  os_atomic_increment_ulint(&rseg_counter, 1);

  /** Notes: didn't need undo::spaces->s_lock() */
  ut_ad(txn_spaces.size() == FSP_IMPLICIT_TXN_TABLESPACES);

  ulint target_undo_tablespaces = FSP_IMPLICIT_TXN_TABLESPACES;
  while (rseg == nullptr) {
    ulint window = current % (target_undo_tablespaces * n_rollback_segments);
    ulint space_slot = window % target_undo_tablespaces;
    ulint rseg_slot = window / target_undo_tablespaces;
    current++;

    undo_space = txn_spaces.at(space_slot);
    ut_ad(undo_space->is_active());

    rseg = undo_space->get_active(rseg_slot);
  }

  ut_ad(rseg);
  ut_ad(rseg->trx_ref_count > 0);
  return rseg;
}

/**
  Always assign transaction rollback segment for trx
  @param[in]      trx
*/
void trx_assign_txn_rseg(trx_t *trx) {
  ut_ad(trx->rsegs.m_txn.rseg == nullptr);

  trx->rsegs.m_txn.rseg = srv_read_only_mode ? nullptr : get_next_txn_rseg();
}

/**
  Whether the txn rollback segment has been assigned
  @param[in]      trx
*/
bool trx_is_txn_rseg_assigned(trx_t *trx) {
  return trx->rsegs.m_txn.rseg != nullptr;
}

/**
  Whether the txn undo log has modified.
*/
bool trx_is_txn_rseg_updated(const trx_t *trx) {
  return trx->rsegs.m_txn.txn_undo != nullptr;
}

/**
  Get undo log segment from free list
  @param[in]      trx       transaction
  @param[in]      rseg      rollback segment
  @param[in]      type      undo type
  @param[in]      trx_id    transaction id
  @param[in]      xid       xid
  @param[in/out]  undo      undo memory object

  @retval         DB_SUCCESS    SUCCESS
*/
static dberr_t txn_undo_get_free(trx_t *trx, trx_rseg_t *rseg, ulint type,
                                 trx_id_t trx_id, const XID *xid,
                                 trx_undo_t **undo) {
  page_t *undo_page = nullptr;
  trx_rsegf_t *rseg_header;
  trx_upagef_t *page_hdr;
  trx_usegf_t *seg_hdr;
  page_no_t page_no;
  ulint offset;
  ulint len;
  ulint size;
  flst_base_node_t *base;
  fil_addr_t node_addr;
  ulint seg_size;
  ulint free_size;
  undo_addr_t undo_addr;
  commit_scn_t prev_image = COMMIT_SCN_LOST;

  ulint slot_no = ULINT_UNDEFINED;
  dberr_t err = DB_SUCCESS;

  ut_ad(type == TRX_UNDO_TXN);
  ut_ad(trx_is_txn_rseg_assigned(trx));
  ut_ad(rseg == trx->rsegs.m_txn.rseg);

  ut_ad(mutex_own(&rseg->mutex));

  mtr_t mtr;
  mtr.start();

  /** Only transaction rollback segment have free list */
  ut_ad(fsp_is_txn_tablespace_by_id(rseg->space_id));

  /** Phase 1 : Find a free slot in rseg array */
  rseg_header =
      trx_rsegf_get(rseg->space_id, rseg->page_no, rseg->page_size, &mtr);

  slot_no = trx_rsegf_undo_find_free(rseg_header, &mtr);

  if (slot_no == ULINT_UNDEFINED) {
    ib::error(ER_IB_MSG_1212)
        << "Cannot find a free slot for an txn undo log."
           " You may have too many active transactions running concurrently."
           " Please add more rollback segments or undo tablespaces.";

    err = DB_TOO_MANY_CONCURRENT_TRXS;
    *undo = nullptr;
    goto func_exit;
  }

  /** Phase 2 : Find a undo log segment from free list */
  base = rseg_header + TXN_RSEG_FREE_LIST;
  len = flst_get_len(base);
  size =
      mtr_read_ulint(rseg_header + TXN_RSEG_FREE_LIST_SIZE, MLOG_4BYTES, &mtr);

  /* txn undo log segment only have one page */
  ut_a(len == size);
  if (len == 0) {
    *undo = nullptr;
    goto func_exit;
  }

  node_addr = flst_get_first(base, &mtr);

  /** The page node was used by free list */
  ut_ad(node_addr.boffset == (TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE));

  undo_page = trx_undo_page_get(page_id_t(rseg->space_id, node_addr.page),
                                rseg->page_size, &mtr);

  page_no = page_get_page_no(undo_page);
  page_hdr = undo_page + TRX_UNDO_PAGE_HDR;
  seg_hdr = undo_page + TRX_UNDO_SEG_HDR;

  seg_size = flst_get_len(seg_hdr + TRX_UNDO_PAGE_LIST);
  ut_a(seg_size == 1);

  /** Phase 3: Remove the node from free list. */
  flst_remove(rseg_header + TXN_RSEG_FREE_LIST,
              undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE, &mtr);
  free_size =
      mtr_read_ulint(rseg_header + TXN_RSEG_FREE_LIST_SIZE, MLOG_4BYTES, &mtr);

  mlog_write_ulint(rseg_header + TXN_RSEG_FREE_LIST_SIZE, free_size - seg_size,
                   MLOG_4BYTES, &mtr);

  os_atomic_decrement_ulint(&lizard_sys->txn_undo_log_free_list_len, 1);

  /** Phase 4 : Reinit the undo log segment header page */
  trx_undo_page_init(undo_page, type, &mtr);

  mlog_write_ulint(page_hdr + TRX_UNDO_PAGE_FREE,
                   TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE, MLOG_2BYTES, &mtr);

  mlog_write_ulint(seg_hdr + TRX_UNDO_LAST_LOG, 0, MLOG_2BYTES, &mtr);

  flst_init(seg_hdr + TRX_UNDO_PAGE_LIST, &mtr);

  flst_add_last(seg_hdr + TRX_UNDO_PAGE_LIST, page_hdr + TRX_UNDO_PAGE_NODE,
                &mtr);

  ut_ad(mach_read_from_2(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE) ==
        TRX_UNDO_TXN);

  rseg->curr_size++;

  offset = trx_undo_header_create(undo_page, trx_id, &prev_image, &mtr);

  ut_ad(offset == TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE);

  /** Lizard: add UBA into undo log header */
  undo_addr = {rseg->space_id, page_no, offset, SCN_NULL, true, GCN_NULL};

  /** Current undo log hdr is UBA */
  lizard::trx_undo_hdr_write_uba(undo_page + offset, undo_addr, &mtr);

  trx_undo_header_add_space_for_xid(undo_page, undo_page + offset, &mtr,
                                    trx_undo_t::Gtid_storage::NONE);

  trx_undo_hdr_add_space_for_txn(undo_page, undo_page + offset, &mtr);

  /** Phase 5 : Set the undo log slot */
  trx_rsegf_set_nth_undo(rseg_header, slot_no, page_no, &mtr);

  /** Phase 6 : Create a memory object for txn undo */
  *undo =
      trx_undo_mem_create(rseg, slot_no, type, trx_id, xid, page_no, offset);

  trx_undo_hdr_init_for_txn(*undo, undo_page, undo_page + offset, prev_image,
                            &mtr);

  ut_ad((*undo)->flag == TRX_UNDO_FLAG_TXN);

  assert_commit_scn_allocated((*undo)->prev_image);

  if (*undo == NULL) {
    err = DB_OUT_OF_MEMORY;
    goto func_exit;
  } else {
    lizard_stats.txn_undo_log_free_list_get.inc();
  }

func_exit:
  mtr.commit();
  return err;
}

/**
  Allocate a undo log segment for transaction from TXN space, it
  only save the scn and trx state currently, so ignore other attributes.

  Pls use trx_undo_assign_undo() for INSERT/UPDATE undo.

  @param[in]        trx
  @param[in/out]    undo_ptr
  @param[in]        TXN type

  @retval           DB_SUCCESS    if assign successful
  @retval           DB_TOO_MANY_CONCURRENT_TRXS,
                    DB_OUT_OF_FILE_SPACE
                    DB_READ_ONLY
                    DB_OUT_OF_MEMORY
*/
static dberr_t txn_undo_assign_undo(trx_t *trx, txn_undo_ptr_t *undo_ptr,
                                    ulint type) {
  mtr_t mtr;
  trx_rseg_t *rseg;
  trx_undo_t *undo;
  dberr_t err = DB_SUCCESS;

  ut_ad(trx && type == TRX_UNDO_TXN);
  ut_ad(trx_is_txn_rseg_assigned(trx));
  ut_ad(undo_ptr == &(trx->rsegs.m_txn));
  ut_ad(mutex_own(&(trx->undo_mutex)));

  rseg = undo_ptr->rseg;

  lizard_stats.txn_undo_log_request.inc();

  mtr_start(&mtr);

  mutex_enter(&rseg->mutex);

  DBUG_EXECUTE_IF("ib_create_table_fail_too_many_trx",
                  err = DB_TOO_MANY_CONCURRENT_TRXS;
                  goto func_exit;);
  undo =
#ifdef UNIV_DEBUG
      srv_inject_too_many_concurrent_trxs
          ? nullptr
          :
#endif
          trx_undo_reuse_cached(trx, rseg, type, trx->id, trx->xid,
                                trx_undo_t::Gtid_storage::NONE, &mtr);

  if (undo == nullptr) {
     err = txn_undo_get_free(trx, rseg, type, trx->id, trx->xid, &undo);

    if (err != DB_SUCCESS) {
      goto func_exit;
    }
  }

  if (undo == nullptr) {
    err = trx_undo_create(trx, rseg, type, trx->id, trx->xid,
                          trx_undo_t::Gtid_storage::NONE, &undo, &mtr);

    if (err != DB_SUCCESS) {
      goto func_exit;
    }
  }

  UT_LIST_ADD_FIRST(rseg->txn_undo_list, undo);
  ut_ad(undo_ptr->txn_undo == nullptr);
  undo_ptr->txn_undo = undo;

func_exit:
  mutex_exit(&(rseg->mutex));
  mtr_commit(&mtr);

  return (err);
}

/**
  Always assign a txn undo log for transaction.

  @param[in]        trx         current transaction

  @return           DB_SUCCESS  Success
*/
dberr_t trx_always_assign_txn_undo(trx_t *trx){
  dberr_t err = DB_SUCCESS;
  trx_undo_t *undo = nullptr;
  txn_undo_ptr_t *undo_ptr = nullptr;
  undo_addr_t undo_addr;

  ut_ad(trx);
  /** Txn rollback segment should have been allocated */
  ut_ad(trx_is_txn_rseg_assigned(trx));

  /** At least one of m_redo or m_noredo rollback segment has been allocated */
  ut_ad(trx_is_rseg_assigned(trx));

  ut_ad(mutex_own(&(trx->undo_mutex)));

  undo_ptr = &trx->rsegs.m_txn;
  ut_ad(undo_ptr);

  if (undo_ptr->txn_undo == nullptr) {
    /**
      Update undo will allocated until prepared state for GTID persist,
      But here we didn't allowed for txn undo.
    */
    ut_ad(!(trx_state_eq(trx, TRX_STATE_PREPARED)));
    assert_txn_desc_initial(trx);
    err = txn_undo_assign_undo(trx, undo_ptr, TRX_UNDO_TXN);
    undo = undo_ptr->txn_undo;

    if (undo == nullptr) {
      lizard_error(ER_LIZARD) << "Could not allocate transaction undo log";
      ut_ad(err != DB_SUCCESS);
    } else {
      /** Only allocate log header, */
      undo->empty = true;

      undo_addr.state = false;
      undo_addr.scn = SCN_NULL;
      undo_addr.space_id = undo->space;
      undo_addr.page_no = undo->hdr_page_no;
      undo_addr.offset = undo->hdr_offset;
      undo_addr.gcn = GCN_NULL;

      undo_encode_undo_addr(undo_addr, &trx->txn_desc.undo_ptr);

      assert_commit_scn_allocated(undo->prev_image);
      trx->prev_image = undo->prev_image;
    }
  } else {
    assert_trx_undo_ptr_allocated(trx);
    assert_commit_scn_allocated(trx->prev_image);
  }

  return err;
}
/*-----------------------------------------------------------------------------*/

/**
  Init the txn description as NULL initial value.
  @param[in]      trx       current transaction
*/
void trx_init_txn_desc(trx_t *trx) { trx->txn_desc = TXN_DESC_NULL; }

/**
  Assign a new commit scn for the transaction when commit

  @param[in]      trx       current transaction
  @param[in/out]  cmmt_ptr   Commit scn which was generated only once
  @param[in]      undo      txn undo log
  @param[in]      undo page txn undo log header page
  @param[in]      offset    txn undo log header offset
  @param[in]      mtr       mini transaction
  @param[out]     serialised

  @retval         scn       commit scn struture
*/
commit_scn_t trx_commit_scn(trx_t *trx, commit_scn_t *cmmt_ptr,
                            trx_undo_t *undo, page_t *undo_hdr_page,
                            ulint hdr_offset, bool *serialised, mtr_t *mtr) {
  trx_usegf_t *seg_hdr;
  trx_ulogf_t *undo_hdr;
  commit_scn_t cmmt = COMMIT_SCN_NULL;

  ut_ad(lizard_sys);
  ut_ad(trx && undo && undo_hdr_page && mtr);

  ut_ad((trx->rsegs.m_txn.rseg != nullptr &&
         mutex_own(&trx->rsegs.m_txn.rseg->mutex)) ||
        trx->rsegs.m_noredo.update_undo == undo);
  /** Attention: Some transaction commit directly from ACTIVE */

  /** TODO:
      If it didn't have prepare state, then only flush redo log once when
      commit, It maybe cause vision problem, other session has see the data,
      but scn redo log is lost.
  */
  ut_ad(trx_state_eq(trx, TRX_STATE_PREPARED) ||
        trx_state_eq(trx, TRX_STATE_ACTIVE));

  lizard_trx_undo_page_validation(undo_hdr_page);

  /** Here we didn't hold trx_sys mutex */
  ut_ad(!trx_sys_mutex_own());

  ut_ad(!cmmt_ptr || commit_scn_state(*cmmt_ptr) == SCN_STATE_ALLOCATED);

  /** Here must hold the X lock on the page */
  ut_ad(mtr_memo_contains_page(mtr, undo_hdr_page, MTR_MEMO_PAGE_X_FIX));

  seg_hdr = undo_hdr_page + TRX_UNDO_SEG_HDR;
  ulint state = mach_read_from_2(seg_hdr + TRX_UNDO_STATE);

  /** TXN undo log must be finished */
  ut_a(state == TRX_UNDO_CACHED || state == TRX_UNDO_TO_PURGE);

  /** Commit must be the last log hdr */
  ut_ad(hdr_offset == mach_read_from_2(seg_hdr + TRX_UNDO_LAST_LOG));

  undo_hdr = undo_hdr_page + hdr_offset;
  ut_ad(!trx_undo_hdr_scn_committed(undo_hdr, mtr));

  assert_lizard_min_safe_scn_valid();

  /* Step 1: modify trx->scn */
  if (cmmt_ptr == nullptr) {
    lizard_sys_scn_mutex_enter();

    DBUG_EXECUTE_IF("crash_before_gcn_commit",
                    ut_ad(trx->txn_desc.cmmt.gcn != GCN_NULL ? 0 : 1););

    /** Generate a new scn */
    std::pair<commit_scn_t, bool> cmmt_result =
        lizard_sys->scn.new_commit_scn(trx->txn_desc.cmmt.gcn);

    cmmt = cmmt_result.first;
    ut_a(!cmmt_result.second);

    assert_trx_scn_initial(trx);
    /** We don't want to call **ut_time_system_us** within the scope
    of the lizard_sys mutex protection. So we just only set
    trx->txn_desc.scn.first here */
    trx->txn_desc.cmmt.scn = cmmt.scn;

    /** If a read only transaction (for example: start transaction read only),
    temporary table can be also modified. It doesn't matter if purge_sys purges
    them */

    /** Revision:
        Temp undo still need to purge/truncate, so delay it by adding into
        serialisation list */

    /** add to lizard_sys->serialisation_list_scn */
    UT_LIST_ADD_LAST(lizard_sys->serialisation_list_scn, trx);

    ut_ad(*serialised == false);
    *serialised = true;

    trx->txn_desc.cmmt.gcn = cmmt.gcn;
    lizard_sys_scn_mutex_exit();

    trx->txn_desc.cmmt.utc = cmmt.utc = ut_time_system_us();

  } else {
    assert_trx_scn_allocated(trx);
    cmmt = *cmmt_ptr;
    ut_ad(trx->txn_desc.cmmt.scn == cmmt.scn);
  }
  ut_ad(commit_scn_state(cmmt) == SCN_STATE_ALLOCATED);

  /* Step 2: modify undo header. */
  trx_undo_hdr_write_scn(undo_hdr, cmmt, mtr);
  ut_ad(trx_undo_hdr_scn_committed(undo_hdr, mtr));

  /* Step 3: modify undo->scn */
  assert_undo_scn_initial(undo);
  undo->cmmt = cmmt;

  assert_lizard_min_safe_scn_valid();

  return cmmt;
}

/**
  Add the txn undo log header into history.

  @param[in]      trx       transaction
  @param[in/out]  undo_ptr    txn undo log structure
  @param[in]      undo_page   txn undo log header page, x-latched
  @param[in]      update_rseg_history_len
                              if true: update rseg history
                              len else skip updating it.
  @param[in]      n_added_logs
                              number of logs added
  @param[in]      mtr
*/
static void trx_purge_add_txn_undo_to_history(trx_t *trx,
                                              txn_undo_ptr_t *undo_ptr,
                                              page_t *undo_page,
                                              bool update_rseg_history_len,
                                              ulint n_added_logs, mtr_t *mtr) {
  trx_undo_t *undo;
  trx_rseg_t *rseg;
  trx_rsegf_t *rseg_header;
  trx_ulogf_t *undo_header;

  undo = undo_ptr->txn_undo;
  rseg = undo->rseg;
  ut_ad(rseg == undo_ptr->rseg);

  rseg_header = trx_rsegf_get(undo->rseg->space_id, undo->rseg->page_no,
                              undo->rseg->page_size, mtr);

  undo_header = undo_page + undo->hdr_offset;

  lizard_trx_undo_hdr_uba_validation(undo_header, mtr);

  if (undo->state != TRX_UNDO_CACHED) {
    ulint hist_size;
#ifdef UNIV_DEBUG
    trx_usegf_t *seg_header = undo_page + TRX_UNDO_SEG_HDR;
#endif /* UNIV_DEBUG */

    /* The undo log segment will not be reused */

    if (UNIV_UNLIKELY(undo->id >= TRX_RSEG_N_SLOTS)) {
      ib::fatal(ER_IB_MSG_1165) << "undo->id is " << undo->id;
    }

    trx_rsegf_set_nth_undo(rseg_header, undo->id, FIL_NULL, mtr);

    MONITOR_DEC(MONITOR_NUM_UNDO_SLOT_USED);

    hist_size =
        mtr_read_ulint(rseg_header + TRX_RSEG_HISTORY_SIZE, MLOG_4BYTES, mtr);

    ut_ad(undo->size == flst_get_len(seg_header + TRX_UNDO_PAGE_LIST));

    /** Lizard: txn undo only has log header */
    ut_a(undo->size == 1);

    mlog_write_ulint(rseg_header + TRX_RSEG_HISTORY_SIZE,
                     hist_size + undo->size, MLOG_4BYTES, mtr);
  }

  /** Here is my interpretation about the format of undo header page:
  1. A undo header page can hold multiple undo headers, whose format can
     be known in 'undo log header' in trx0undo.h
  2. Only current transaction who uses the undo page can use **undo
     log page hader**, the undo records from the transaction can be
     placed in multiple pages. But the other pages are normal undo pages,
     which only belong to the transaction.
  3. A 'undo log header' represents a undo_t, only belongs to a trx.
  4. The undo records of the other 'undo log header' can only be placed in
     the undo page.
  5. When added in history list, the TRX_UNDO_HISTORY_NODE are used to
     form a linked history list.
  */
  /* Add the log as the first in the history list */
  flst_add_first(rseg_header + TRX_RSEG_HISTORY,
                 undo_header + TRX_UNDO_HISTORY_NODE, mtr);

  if (update_rseg_history_len) {
    os_atomic_increment_ulint(&trx_sys->rseg_history_len, n_added_logs);
    srv_wake_purge_thread_if_not_active();
  }

  /* Update maximum transaction scn for this rollback segment. */
  assert_trx_scn_allocated(trx);
  mlog_write_ull(rseg_header + TRX_RSEG_MAX_TRX_SCN, trx->txn_desc.cmmt.scn,
                 mtr);

  /* lizard: TRX_UNDO_TRX_NO is reserved */
  //mlog_write_ull(undo_header + TRX_UNDO_TRX_NO, trx->no, mtr);

  /* Write information about delete markings to the undo log header */
  if (!undo->del_marks) {
    mlog_write_ulint(undo_header + TRX_UNDO_DEL_MARKS, FALSE, MLOG_2BYTES, mtr);
  } else {
    /** Txn undo log didn't have any delete marked record to purge forever  */
    ut_a(0);
  }

  /* Lizard: txn undo didn't need gtid information */

  /* Write GTID information if there. */
  // trx_undo_gtid_write(trx, undo_header, undo, mtr);

  if (rseg->last_page_no == FIL_NULL) {
    rseg->last_page_no = undo->hdr_page_no;
    rseg->last_offset = undo->hdr_offset;
    rseg->last_del_marks = undo->del_marks;

    /** trx->scn must be allocated  */
    assert_trx_scn_allocated(trx);

    rseg->last_scn = trx->txn_desc.cmmt.scn;
  }
}

/**
  Cleanup txn undo log segment when commit,

  It will :
    1) Add the UBA header into rseg->history
    2) Reinit the rseg->slot as FIL_NULL
    3) Destroy or reuse the undo mem object

  @param[in]      trx         trx owning the txn undo log
  @param[in/out]  undo_ptr    txn undo log structure
  @param[in]      undo_page   txn undo log header page, x-latched
  @param[in]      update_rseg_history_len
                              if true: update rseg history
                              len else skip updating it.
  @param[in]      n_added_logs
                              number of logs added
  @param[in]      mtr
*/

void trx_txn_undo_cleanup(trx_t *trx, txn_undo_ptr_t *undo_ptr,
                          page_t *undo_page, bool update_rseg_history_len,
                          ulint n_added_logs, mtr_t *mtr) {
  trx_rseg_t *rseg;
  trx_undo_t *undo;

  undo = undo_ptr->txn_undo;
  rseg = undo_ptr->rseg;

  ut_ad(mutex_own(&(rseg->mutex)));
  assert_undo_scn_allocated(undo);

  trx_purge_add_txn_undo_to_history(trx, undo_ptr, undo_page,
                                    update_rseg_history_len, n_added_logs, mtr);

  UT_LIST_REMOVE(rseg->txn_undo_list, undo);

  undo_ptr->txn_undo = NULL;

  if (undo->state == TRX_UNDO_CACHED) {
    UT_LIST_ADD_FIRST(rseg->txn_undo_cached, undo);

    MONITOR_INC(MONITOR_NUM_UNDO_SLOT_CACHED);
    LIZARD_MONITOR_INC_TXN_CACHED(1);
  } else {
    ut_ad(undo->state == TRX_UNDO_TO_PURGE);

    trx_undo_mem_free(undo);
  }
}

/**
   Resurrect txn undo log segment,
   Maybe the trx didn't have m_redo update/insert undo log.

   There are three different state:

   1) TXN_UNDO N INSERT_UNDO Y  UPDATE_UNO N
      : The transaction has committed, but rseg->slot of insert undo
        didn't set FIL_NULL, since cleanup insert undo is in other mini
        transaction;
        But here it will not commit again, just cleanup.

   2) TXN_UNDO Y UPDATE_UNDO N INSERT_UNDO N
      : The transaction only allocate txn undo log header, then instance
        crashed;

   3) TXN_UNDO Y UPDATE_UNDO/INSERT_UNDO (one Y or two Y)

   We didn't allowed only have UPDATE UNDO but didn't have txn undo;
   Since the txn undo allocation is prior to undate undo;

*/
void trx_resurrect_txn(trx_t *trx, trx_undo_t *undo, trx_rseg_t *rseg) {
  ut_ad(trx->rsegs.m_txn.rseg == nullptr);
  ut_ad(undo->empty);

  /** Already has update/insert undo */
  if (trx->rsegs.m_redo.rseg != nullptr) {
    ut_ad(undo->trx_id == trx->id);
    ut_ad(trx->is_recovered);
    if (trx->rsegs.m_redo.update_undo != nullptr &&
        trx->state == TRX_STATE_COMMITTED_IN_MEMORY) {
      assert_trx_scn_allocated(trx);
      assert_trx_undo_ptr_initial(trx);
      lizard_ut_ad(trx->txn_desc.cmmt == undo->cmmt);
    } else {
      assert_trx_scn_initial(trx);
    }
  } else {
    /** It must be the case: MySQL crashed as soon as the txn undo is created.
    Only temporary table will not create txn */
    *trx->xid = undo->xid;
    trx->id = undo->trx_id;
    trx->is_recovered = true;
    trx->ddl_operation = undo->dict_operation;
  }
  ut_ad(trx->rsegs.m_txn.txn_undo == nullptr);
  rseg->trx_ref_count++;
  trx->rsegs.m_txn.rseg = rseg;
  trx->rsegs.m_txn.txn_undo = undo;

  assert_commit_scn_allocated(undo->prev_image);
  trx->prev_image = undo->prev_image;

  /**
     Currently it's impossible only have txn undo for normal transaction.
     But if crashed just after allocated txn undo, here maybe possible.
  */
  if (trx->rsegs.m_redo.rseg == nullptr) {
    lizard_ut_ad(undo->state == TRX_UNDO_ACTIVE);
    trx->state = TRX_STATE_ACTIVE;

    /* A running transaction always has the number field inited to
    TRX_ID_MAX */

    //trx->no = TRX_ID_MAX;

    assert_undo_scn_initial(undo);
    assert_trx_scn_initial(trx);
    assert_txn_desc_initial(trx);

  } else {
    /** trx state has been initialized */
    ut_ad(!trx_state_eq(trx, TRX_STATE_NOT_STARTED));
    if (trx->rsegs.m_redo.insert_undo != nullptr &&
        trx->rsegs.m_redo.update_undo == nullptr) {
      /** SCN info wasn't written in insert undo. */
      if (trx->state == TRX_STATE_COMMITTED_IN_MEMORY) {
        /** Since the insert undo didn't have valid scn number */
        assert_undo_scn_allocated(undo);
        trx->txn_desc.cmmt = undo->cmmt;
      }
    } else if (trx->rsegs.m_redo.update_undo != nullptr) {
      /** Update undo scn must be equal with txn undo scn */
      ut_ad(trx->rsegs.m_redo.update_undo->cmmt == undo->cmmt);
    }
  }
  ut_ad(trx->txn_desc.cmmt == undo->cmmt);
}

/** Prepares a transaction for given rollback segment.
 @return lsn_t: lsn assigned for commit of scheduled rollback segment */
lsn_t txn_prepare_low(
    trx_t *trx,               /*!< in/out: transaction */
    txn_undo_ptr_t *undo_ptr, /*!< in/out: pointer to rollback
                              segment scheduled for prepare. */
    mtr_t *mtr) {
  ut_ad(mtr);

  //trx_rseg_t *rseg = undo_ptr->rseg;

  /* Change the undo log segment states from TRX_UNDO_ACTIVE to
  TRX_UNDO_PREPARED: these modifications to the file data
  structure define the transaction as prepared in the file-based
  world, at the serialization point of lsn. */

  //mutex_enter(&rseg->mutex);

  ut_ad(undo_ptr->txn_undo);
    /* It is not necessary to obtain trx->undo_mutex here
    because only a single OS thread is allowed to do the
    transaction prepare for this transaction. */
  trx_undo_set_state_at_prepare(trx, undo_ptr->txn_undo, false, mtr);

  //mutex_exit(&rseg->mutex);

  /*--------------*/
  /* This mtr commit makes the transaction prepared in
  file-based world. */

  // mtr_commit(&mtr);
  /*--------------*/

  /*
  if (!noredo_logging) {
    const lsn_t lsn = mtr.commit_lsn();
    ut_ad(lsn > 0);
    return lsn;
  }
  */

  return 0;
}

/**
  Validate txn undo free list node and rseg free list

  @param[in]    rseg_hdr      rollback segment header
  @parma[in]    undo_page     undo page
  @param[in]    mtr           mini transaction
*/
bool txn_undo_free_list_validate(trx_rsegf_t *rseg_hdr, page_t *undo_page,
                                 mtr_t *mtr) {
  trx_usegf_t *seg_hdr;
  ulint len;
  fil_addr_t addr;

  seg_hdr = undo_page + TRX_UNDO_SEG_HDR;

  /** Confirm the page list only include undo log header page */
  len = flst_get_len(seg_hdr + TRX_UNDO_PAGE_LIST);
  addr = flst_get_last(seg_hdr + TRX_UNDO_PAGE_LIST, mtr);

  if (addr.page != page_get_page_no(undo_page) ||
      addr.boffset != (TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE))
    return false;

  if (len != 1) return false;

  /** Confirm the free list reuse PAGE_NODE of undo log header */
  len = mtr_read_ulint(rseg_hdr + TXN_RSEG_FREE_LIST_SIZE, MLOG_4BYTES, mtr);
  if (len != flst_get_len(rseg_hdr + TXN_RSEG_FREE_LIST)) return false;

  addr = flst_get_first(rseg_hdr + TXN_RSEG_FREE_LIST, mtr);

  if (addr.boffset != (TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE)) return false;

  return true;
}

/**
  Put the txn undo log segment into free list after purge all.

  @param[in]        rseg        rollback segment
  @param[in]        hdr_addr    txn log hdr address
*/
void txn_purge_segment_to_free_list(trx_rseg_t *rseg, fil_addr_t hdr_addr) {
  mtr_t mtr;
  trx_rsegf_t *rseg_hdr;
  trx_ulogf_t *log_hdr;
  trx_usegf_t *seg_hdr;
  page_t *undo_page;
  ulint seg_size;
  ulint hist_size;
  ulint free_size;

  mtr_start(&mtr);

  mutex_enter(&rseg->mutex);
  rseg_hdr =
      trx_rsegf_get(rseg->space_id, rseg->page_no, rseg->page_size, &mtr);

  undo_page = trx_undo_page_get(page_id_t(rseg->space_id, hdr_addr.page),
                                rseg->page_size, &mtr);

  seg_hdr = undo_page + TRX_UNDO_SEG_HDR;
  log_hdr = undo_page + hdr_addr.boffset;

  /** The page list always has only its self page */
  seg_size = flst_get_len(seg_hdr + TRX_UNDO_PAGE_LIST);
  ut_a(seg_size == 1);

  /** Remove the undo log segment from history list */
  trx_purge_remove_log_hdr(rseg_hdr, log_hdr, &mtr);

  hist_size =
      mtr_read_ulint(rseg_hdr + TRX_RSEG_HISTORY_SIZE, MLOG_4BYTES, &mtr);

  ut_ad(hist_size >= seg_size);

  mlog_write_ulint(rseg_hdr + TRX_RSEG_HISTORY_SIZE, hist_size - seg_size,
                   MLOG_4BYTES, &mtr);

  ut_ad(rseg->curr_size >= seg_size);

  /** Curr_size didn't include the free list undo log segment */
  rseg->curr_size -= seg_size;

  /** Add the undo log segment from history list */
  free_size =
      mtr_read_ulint(rseg_hdr + TXN_RSEG_FREE_LIST_SIZE, MLOG_4BYTES, &mtr);

  mlog_write_ulint(rseg_hdr + TXN_RSEG_FREE_LIST_SIZE, free_size + seg_size,
                   MLOG_4BYTES, &mtr);

  flst_add_first(rseg_hdr + TXN_RSEG_FREE_LIST,
                undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE, &mtr);

  os_atomic_increment_ulint(&lizard_sys->txn_undo_log_free_list_len, 1);

  lizard_txn_undo_free_list_validate(rseg_hdr, undo_page, &mtr);

  mutex_exit(&(rseg->mutex));

  mtr_commit(&mtr);

  lizard_stats.txn_undo_log_free_list_put.inc();
}

/**
  Write the scn into the buffer
  @param[in/out]    ptr       buffer
  @param[in]        txn_desc  txn description
*/
void trx_write_scn(byte *ptr, const txn_desc_t *txn_desc) {
  ut_ad(ptr && txn_desc);
  assert_undo_ptr_allocated(txn_desc->undo_ptr);
  trx_write_scn(ptr, txn_desc->cmmt.scn);
}

/**
  Write the scn into the buffer
  @param[in/out]    ptr     buffer
  @param[in]        scn     scn id
*/
void trx_write_scn(byte *ptr, scn_id_t scn) {
  ut_ad(ptr);
  mach_write_to_8(ptr, scn);
}

/**
  Write the UBA into the buffer
  @param[in/out]    ptr       buffer
  @param[in]        txn_desc  txn description
*/
void trx_write_undo_ptr(byte *ptr, const txn_desc_t *txn_desc) {
  ut_ad(ptr && txn_desc);
  assert_undo_ptr_allocated(txn_desc->undo_ptr);
  trx_write_undo_ptr(ptr, txn_desc->undo_ptr);
}

/**
  Write the UBA into the buffer
  @param[in/out]    ptr       buffer
  @param[in]        undo_ptr  UBA
*/
void trx_write_undo_ptr(byte *ptr, undo_ptr_t undo_ptr) {
  ut_ad(ptr);
  mach_write_to_8(ptr, undo_ptr);
}

/**
  Write the gcn into the buffer
  @param[in/out]    ptr       buffer
  @param[in]        txn_desc  txn description
*/
void trx_write_gcn(byte *ptr, const txn_desc_t *txn_desc) {
  ut_ad(ptr && txn_desc);
  assert_undo_ptr_allocated(txn_desc->undo_ptr);
  trx_write_gcn(ptr, txn_desc->cmmt.gcn);
}

/**
  Write the gcn into the buffer
  @param[in/out]    ptr     buffer
  @param[in]        scn     scn id
*/
void trx_write_gcn(byte *ptr, gcn_t gcn) {
  ut_ad(ptr);
  mach_write_to_8(ptr, gcn);
}

/**
  Read the scn
  @param[in]        ptr       buffer

  @return           scn_id_t  scn
*/
scn_id_t trx_read_scn(const byte *ptr) {
  ut_ad(ptr);
  return mach_read_from_8(ptr);
}

/**
  Read the UBA
  @param[in]        ptr        buffer

  @return           undo_ptr_t undo_ptr
*/
undo_ptr_t trx_read_undo_ptr(const byte *ptr) {
  ut_ad(ptr);
  return mach_read_from_8(ptr);
}

/**
  Read the gcn
  @param[in]        ptr       buffer

  @return           scn_id_t  scn
*/
gcn_t trx_read_gcn(const byte *ptr) {
  ut_ad(ptr);
  return mach_read_from_8(ptr);
}

/**
  Decode the undo_ptr into UBA
  @param[in]      undo ptr
  @param[out]     undo addr
*/
void undo_decode_undo_ptr(const undo_ptr_t uba, undo_addr_t *undo_addr) {
  ulint rseg_id;
  undo_ptr_t undo_ptr = uba;
  ut_ad(undo_addr);

  undo_addr->offset = (ulint)undo_ptr & 0xFFFF;
  undo_ptr >>= UBA_WIDTH_OFSET;
  undo_addr->page_no = (ulint)undo_ptr & 0xFFFFFFFF;
  undo_ptr >>= UBA_WIDTH_PAGE_NO;
  rseg_id = (ulint)undo_ptr & 0x7F;

  /* Confirm the reserved bits */
  ut_ad(((ulint)undo_ptr & 0x7f80) == 0);

  undo_ptr >>= (UBA_WIDTH_SPACE_ID + UBA_WIDTH_UNUSED);
  undo_addr->state = (bool)undo_ptr;

  /**
    It should not be trx_sys tablespace for normal table except
    of temporary table/LOG_DDL/DYNAMIC_METADATA/DDL in-process table */

  /**
    Revision:
    We give a fixed UBA in undo log header if didn't allocate txn undo
    for temporary table.
  */
  if (rseg_id == 0) {
    lizard_ut_ad(undo_addr->offset == UNDO_PTR_OFFSET_TEMP_TAB_REC ||
                 undo_addr->offset == UNDO_PTR_OFFSET_DYNAMIC_METADATA ||
                 undo_addr->offset == UNDO_PTR_OFFSET_LOG_DDL ||
                 undo_addr->offset == UNDO_PTR_OFFSET_UNDO_HDR);
  }
  /** It's always redo txn undo log */
  undo_addr->space_id = trx_rseg_id_to_space_id(rseg_id, false);
}

/**
  Try to lookup the real scn of given records. Address directly to the
  corresponding txn undo header by UBA.

  @param[in/out]  txn_rec       txn info of the records.
  @param[out]     txn_lookup    txn lookup result, nullptr if don't care.

  @return         bool          whether corresponding trx is active.
*/
static bool txn_undo_hdr_lookup_func(txn_rec_t *txn_rec,
                                     txn_lookup_t *txn_lookup, mtr_t *txn_mtr) {
  undo_addr_t undo_addr;
  page_t *undo_page;
  ulint fil_type;
  ulint undo_page_start;
  trx_upagef_t *page_hdr;
  ulint undo_page_type;
  ulint real_trx_state;
  trx_id_t real_trx_id;
  trx_usegf_t *seg_hdr;
  trx_ulogf_t *undo_hdr;
  txn_undo_hdr_t txn_undo_hdr;
  ulint hdr_flag;
  bool have_mtr = false;
  mtr_t temp_mtr;
  mtr_t *mtr;

  have_mtr = (txn_mtr != nullptr);

  mtr = have_mtr ? txn_mtr : &temp_mtr;

  ut_ad(mtr);

  /** ----------------------------------------------------------*/
  /** Phase 1: Read the undo header page */
  undo_decode_undo_ptr(txn_rec->undo_ptr, &undo_addr);
  ut_ad(undo_addr.offset <= UNIV_PAGE_SIZE_MAX);

  const page_id_t page_id(undo_addr.space_id, undo_addr.page_no);

  if (!have_mtr) mtr_start(mtr);

  /** Undo tablespace always univ_page_size */
  undo_page = trx_undo_page_get_s_latched(page_id, univ_page_size, mtr);

  /** transaction tablespace didn't allowed to be truncated */
  ut_a(undo_page);

  /** ----------------------------------------------------------*/
  /** Phase 2: Judge the fil page */
  fil_type = fil_page_get_type(undo_page);
  /** The type of undo log segment must be FIL_PAGE_UNDO_LOG */
  ut_a(fil_type == FIL_PAGE_UNDO_LOG);

  /** ----------------------------------------------------------*/
  /** Phase 3: judge whether it's undo log header or undo log data */
  page_hdr = undo_page + TRX_UNDO_PAGE_HDR;
  undo_page_start = mach_read_from_2(page_hdr + TRX_UNDO_PAGE_START);

  /** If the undo record start from undo segment header, it's normal
      undo log data page.
  */
  ut_a(undo_page_start != (TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_HDR_SIZE));

  /** ----------------------------------------------------------*/
  /** Phase 4: judge whether it's txn undo */
  undo_page_type = mach_read_from_2(page_hdr + TRX_UNDO_PAGE_TYPE);

  ut_a(undo_page_type == TRX_UNDO_TXN);

  /** ----------------------------------------------------------*/
  /** Phase 5: check the undo segment state */
  seg_hdr = undo_page + TRX_UNDO_SEG_HDR;
  real_trx_state = mach_read_from_2(seg_hdr + TRX_UNDO_STATE);

  /** real_trx_state should only be the following states */
  ut_a(real_trx_state == TRX_UNDO_ACTIVE ||
       real_trx_state == TRX_UNDO_CACHED ||
       real_trx_state == TRX_UNDO_PREPARED ||
       real_trx_state == TRX_UNDO_TO_PURGE);

  /** ----------------------------------------------------------*/
  /** Phase 6: The offset (minus TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE)
  is a fixed multiple of TRX_UNDO_LOG_GTID_HDR_SIZE */
  lizard_ut_ad(undo_addr.offset >= TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE);
  lizard_ut_ad((undo_addr.offset
                  - (TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE))
               % TRX_UNDO_LOG_GTID_HDR_SIZE == 0);

  /** ----------------------------------------------------------*/
  /** Phase 7: Check the flag in undo hdr, should be TRX_UNDO_FLAG_TXN,
  unless it's in cleanout_safe_mode. */
  undo_hdr = undo_page + undo_addr.offset;
  hdr_flag = mtr_read_ulint(undo_hdr + TRX_UNDO_FLAGS, MLOG_1BYTE, mtr);
  if (!(hdr_flag & TRX_UNDO_FLAG_TXN)) {
    goto undo_corrupted;
  }

  /** ----------------------------------------------------------*/
  /** Phase 8: check the txn extension fields in txn undo header */
  trx_undo_hdr_read_txn(undo_page, undo_hdr, mtr, &txn_undo_hdr);
  if (txn_undo_hdr.magic_n != TXN_MAGIC_N) {
    /** The header might be raw */
    lizard_stats.txn_undo_lost_magic_number_wrong.inc();
    goto undo_corrupted;
  }

  /** NOTES: If the extent flag is used, there might be some records's flag
  that is equal to 0, and there also might be other records's flag that's not
  equal to 0 at the same time. */
  if (txn_undo_hdr.ext_flag != 0) {
    /** The header might be raw */
    lizard_stats.txn_undo_lost_ext_flag_wrong.inc();
    goto undo_corrupted;
  }

  /** ----------------------------------------------------------*/
  /** Phase 9: check the trx_id in txn undo header */
  real_trx_id = txn_undo_hdr.trx_id;
  if (real_trx_id != txn_rec->trx_id) {
    lizard_stats.txn_undo_lost_trx_id_mismatch.inc();
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
  if (txn_undo_hdr.state == TXN_UNDO_LOG_ACTIVE) {
    lizard_ut_ad(mach_read_from_2(seg_hdr + TRX_UNDO_LAST_LOG)
                   == undo_addr.offset);
    lizard_ut_ad(real_trx_state == TRX_UNDO_ACTIVE ||
                 real_trx_state == TRX_UNDO_PREPARED);
    goto still_active;
  } else if (txn_undo_hdr.state == TXN_UNDO_LOG_COMMITED) {
    goto already_commit;
  } else {
    lizard_ut_ad(txn_undo_hdr.state == TXN_UNDO_LOG_PURGED);
    goto undo_purged;
  }

still_active:
  assert_commit_scn_initial(txn_undo_hdr.image);
  txn_lookup_t_set(txn_lookup, txn_undo_hdr, txn_undo_hdr.image,
                   txn_state_t::TXN_STATE_ACTIVE);
  if (!have_mtr) mtr_commit(mtr);
  return true; 

already_commit:
  assert_commit_scn_allocated(txn_undo_hdr.image);
  txn_rec->scn = txn_undo_hdr.image.scn;
  txn_rec->gcn = txn_undo_hdr.image.gcn;
  lizard_undo_ptr_set_commit(&txn_rec->undo_ptr);
  txn_lookup_t_set(txn_lookup, txn_undo_hdr, txn_undo_hdr.image,
                   txn_state_t::TXN_STATE_COMMITTED);
  if (!have_mtr) mtr_commit(mtr);
  return false;

undo_purged:
  assert_commit_scn_allocated(txn_undo_hdr.image);
  txn_rec->scn = txn_undo_hdr.image.scn;
  txn_rec->gcn = txn_undo_hdr.image.gcn;
  lizard_undo_ptr_set_commit(&txn_rec->undo_ptr);
  txn_lookup_t_set(txn_lookup, txn_undo_hdr, txn_undo_hdr.image,
                   txn_state_t::TXN_STATE_PURGED);
  if (!have_mtr) mtr_commit(mtr);
  return false;

undo_reuse:
  assert_commit_scn_allocated(txn_undo_hdr.prev_image);
  txn_rec->scn = txn_undo_hdr.prev_image.scn;
  txn_rec->gcn = txn_undo_hdr.prev_image.gcn;
  lizard_undo_ptr_set_commit(&txn_rec->undo_ptr);
  txn_lookup_t_set(txn_lookup, txn_undo_hdr, txn_undo_hdr.prev_image,
                   txn_state_t::TXN_STATE_REUSE);
  if (!have_mtr) mtr_commit(mtr);
  return false;

undo_corrupted:
  /** Can't never be lost if cleanout_safe_mode isn't taken into
  consideration */
  ut_a(opt_cleanout_safe_mode);
  txn_rec->scn = SCN_UNDO_CORRUPTED;
  txn_rec->gcn = GCN_UNDO_CORRUPTED;
  lizard_undo_ptr_set_commit(&txn_rec->undo_ptr);
  txn_lookup_t_set(txn_lookup, txn_undo_hdr,
                   {SCN_UNDO_CORRUPTED, UTC_UNDO_CORRUPTED, GCN_UNDO_CORRUPTED},
                   txn_state_t::TXN_STATE_UNDO_CORRUPTED);
  if (!have_mtr) mtr_commit(mtr);
  return false;
}

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
/*
static bool txn_undo_hdr_lookup_strict(txn_rec_t *txn_rec) {
  return false;
}
*/
#endif /* UNIV_DEBUG || LIZARD_DEBUG */

/**
  Try to lookup the real scn of given records.

  @param[in/out]  txn_rec       txn info of the records.
  @param[out]     txn_lookup    txn lookup result, nullptr if don't care

  @return         bool          whether corresponding trx is active.
*/
bool txn_undo_hdr_lookup_low(txn_rec_t *txn_rec,
                             txn_lookup_t *txn_lookup,
                             mtr_t *txn_mtr) {
  bool ret;
  undo_addr_t undo_addr;
  bool exist;

  /** In theory, lizard has to findout the real acutal scn (if have) by
  uba */
  lizard_stats.txn_undo_lookup_by_uba.inc();

  if (opt_cleanout_safe_mode) {
    undo_decode_undo_ptr(txn_rec->undo_ptr, &undo_addr);
    exist = txn_undo_logs->exist({undo_addr.space_id, undo_addr.page_no});
    if (!exist) {
      txn_undo_hdr_t txn_undo_hdr = {
          {SCN_UNDO_CORRUPTED, UTC_UNDO_CORRUPTED, GCN_UNDO_CORRUPTED},
          /** txn_undo_hdr.undo_ptr should be from txn undo header, and it
          must be active state when coming here */
          txn_rec->undo_ptr,
          txn_rec->trx_id,
          TXN_MAGIC_N,
          {SCN_UNDO_CORRUPTED, UTC_UNDO_CORRUPTED, GCN_UNDO_CORRUPTED},
          TXN_UNDO_LOG_PURGED,
          0,
      };
      txn_rec->scn = SCN_UNDO_CORRUPTED;
      txn_rec->gcn = GCN_UNDO_CORRUPTED;
      lizard_undo_ptr_set_commit(&txn_rec->undo_ptr);
      lizard_stats.txn_undo_lost_page_miss_when_safe.inc();
      txn_lookup_t_set(txn_lookup, txn_undo_hdr,
                       {SCN_UNDO_CORRUPTED, UTC_UNDO_CORRUPTED, GCN_UNDO_CORRUPTED},
                       txn_state_t::TXN_STATE_UNDO_CORRUPTED);
      return false;
    }
  }

  ret = txn_undo_hdr_lookup_func(txn_rec, txn_lookup, txn_mtr);

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
  /*
  bool strict_ret;
  txn_rec_t txn_strict;
  memcpy(&txn_strict, txn_rec, sizeof(*txn_rec));

  strict_ret = txn_undo_hdr_lookup_strict(&txn_strict, expected_id);

  ut_a(ret == strict_ret);
  ut_a(txn_rec->scn == txn_strict.scn);
  ut_a(txn_rec->undo_ptr == txn_strict.undo_ptr);
  */

#endif /* UNIV_DEBUG || LIZARD_DEBUG */
  return ret;
}
/** Add the rseg into the purge queue heap */
void trx_add_rsegs_for_purge(commit_scn_t &cmmt, TxnUndoRsegs *elem) {
  ut_ad(cmmt.scn == elem->get_scn());
  mutex_enter(&purge_sys->pq_mutex);
  purge_sys->purge_heap->push(*elem);
  lizard_purged_scn_validation();
  mutex_exit(&purge_sys->pq_mutex);
}

/** Collect rsegs into the purge heap for the first time */
bool trx_collect_rsegs_for_purge(TxnUndoRsegs *elem,
                                 trx_undo_ptr_t *redo_rseg_undo_ptr,
                                 trx_undo_ptr_t *temp_rseg_undo_ptr,
                                 txn_undo_ptr_t *txn_rseg_undo_ptr) {
  bool has = false;
  trx_rseg_t *redo_rseg = nullptr;
  trx_rseg_t *temp_rseg = nullptr;
  trx_rseg_t *txn_rseg = nullptr;

  if (redo_rseg_undo_ptr != nullptr) {
    redo_rseg = redo_rseg_undo_ptr->rseg;
    ut_ad(mutex_own(&redo_rseg->mutex));
  }

  if (temp_rseg_undo_ptr != NULL) {
    temp_rseg = temp_rseg_undo_ptr->rseg;
    ut_ad(mutex_own(&temp_rseg->mutex));
  }

  if (txn_rseg_undo_ptr != nullptr) {
    txn_rseg = txn_rseg_undo_ptr->rseg;
    ut_ad(mutex_own(&txn_rseg->mutex));
  }

  if (redo_rseg != NULL && redo_rseg->last_page_no == FIL_NULL) {
    elem->push_back(redo_rseg);
    has = true;
  }

  if (temp_rseg != NULL && temp_rseg->last_page_no == FIL_NULL) {
    elem->push_back(temp_rseg);
    has = true;
  }

  if (txn_rseg != NULL && txn_rseg->last_page_no == FIL_NULL) {
    elem->push_back(txn_rseg);
    has = true;
  }
  return has;
}

/*
  static members.
*/
ulint Undo_retention::retention_time = 0;
ulint Undo_retention::space_limit = 100 * 1024;
ulint Undo_retention::space_reserve = 0;
char Undo_retention::status[128] = {0};
Undo_retention Undo_retention::inst;

int Undo_retention::check_limit(THD *thd, SYS_VAR *var, void *save,
                                struct st_mysql_value *value) {
  if (check_func_long(thd, var, save, value)) return 1;

  if (*(ulong *)save < space_reserve) {
    push_warning_printf(thd, Sql_condition::SL_WARNING, ER_WRONG_ARGUMENTS,
                        "InnoDB: innodb_undo_space_limit should more than"
                        " innodb_undo_space_reserve.");
    return 1;
  }

  return 0;
}

int Undo_retention::check_reserve(THD *thd, SYS_VAR *var, void *save,
                                  struct st_mysql_value *value) {
  if (check_func_long(thd, var, save, value)) return 1;

  if (*(ulong *)save > space_limit) {
    push_warning_printf(thd, Sql_condition::SL_WARNING, ER_WRONG_ARGUMENTS,
                        "InnoDB: innodb_undo_space_reserve should less than"
                        " innodb_undo_space_limit.");
    return 1;
  }

  return 0;
}

void Undo_retention::on_update(THD *, SYS_VAR *, void *var_ptr,
                               const void *save) {
  *static_cast<ulong *>(var_ptr) = *static_cast<const ulong *>(save);
  srv_purge_wakeup(); /* notify purge thread to try again */
}

void Undo_retention::on_update_and_start(THD *thd, SYS_VAR *var, void *var_ptr,
                                         const void *save) {
  ulong old_value = *static_cast<const ulong *>(var_ptr);
  ulong new_value = *static_cast<const ulong *>(save);
  on_update(thd, var, var_ptr, save);

  /* If open the undo retention, refresh stat data synchronously. */
  if (new_value > 0 && old_value == 0) {
    instance()->refresh_stat_data();
  }
}

/*
  Collect latest undo space sizes periodically.
*/
void Undo_retention::refresh_stat_data() {
  mutex_enter(&m_mutex);
  ulint used_size = 0;
  ulint file_size = 0;
  std::vector<space_id_t> undo_spaces;

  if (retention_time == 0) {
    m_stat_done = false;
    mutex_exit(&m_mutex);
    return;
  }

  /* Actual used size */
  undo::spaces->s_lock();
  for (auto undo_space : undo::spaces->m_spaces) {
    ulint size = 0;

    for (auto rseg : *undo_space->rsegs()) {
      size += rseg->curr_size;
    }

    used_size += size;
    undo_spaces.push_back(undo_space->id());
  }
  undo::spaces->s_unlock();

  /* Physical file size */
  for (auto id : undo_spaces) {
    auto size = fil_space_get_size(id);
    file_size += size;
  }

  m_total_used_size = used_size;

  m_stat_done = true;

  snprintf(status, sizeof(status),
           "space:{file:%lu, used:%lu, limit:%lu, reserved:%lu}, "
           "time:{top:%lu, now:%lu}", file_size, used_size,
           mb_to_pages(space_limit), mb_to_pages(space_reserve),
           m_last_top_utc, current_utc());
  mutex_exit(&m_mutex);
}

/*
  Decide whether to block purge or not based on the current
  undo tablespace size and retention configuration.

  @return     true     if blocking purge
*/
bool Undo_retention::purge_advise() {
  m_last_top_utc = (ulint)(purge_sys->top_undo_utc / 1000000);

  /* Retention turned off or stating not done, can not advise */
  if (retention_time == 0 || !m_stat_done) return false;

  /* During recovery, purge_sys::top_undo_utc may be not initialized,
  because txn_rseg of this transaction has been processed. */
  if (m_last_top_utc == 0) return false;

  ulint used_size = m_total_used_size.load();

  if (space_limit > 0) {
    /* Rule_1: reach space limit, do purge */
    if (used_size > mb_to_pages(space_limit)) return false;
  }

  /* Rule_2: retention time not satisfied, block purge */
  if ((m_last_top_utc + retention_time) > current_utc()) return true;

  if (space_reserve > 0) {
    /* Rule_3: below reserved size yet, can hold more history data */
    if (used_size < mb_to_pages(space_reserve)) return true;
  }

  /* Rule_4: time satisfied and exceeded the reserved, just do purge */
  return false;
}

/* Init undo_retention */
void undo_retention_init() {

  /* Init the lizard undo retention mutex. */
  Undo_retention::instance()->init_mutex();

  /* Force to refrese once at starting */
  Undo_retention::instance()->refresh_stat_data();
}

/**
  Decide the real trx state when read current record.
  1) Search tcn cache
  2) Lookup txn undo

  And try to collect cursor to cache txn and cleanout record.


  @param[in/out]	txn record

  @retval	true		active
                false		committed
*/
bool txn_rec_cleanout_state_by_misc(txn_rec_t *txn_rec, btr_pcur_t *pcur,
                                    const rec_t *rec, const dict_index_t *index,
                                    const ulint *offsets) {
  bool active = false;
  bool cache_hit = false;

  /** If record is not active, return false directly. */
  if (!lizard_undo_ptr_is_active(txn_rec->undo_ptr)) {
    lizard_ut_ad(txn_rec->scn > 0 && txn_rec->scn < SCN_MAX);
    lizard_ut_ad(txn_rec->gcn > 0 && txn_rec->gcn < GCN_MAX);
    return false;
  }

  /** Search tcn cache */
  cache_hit = trx_search_tcn(txn_rec, pcur, nullptr);
  if (cache_hit) {
    ut_ad(!lizard_undo_ptr_is_active(txn_rec->undo_ptr));
    lizard_ut_ad(txn_rec->scn > 0 && txn_rec->scn < SCN_MAX);
    lizard_ut_ad(txn_rec->gcn > 0 && txn_rec->gcn < GCN_MAX);

    /** Collect record to cleanout later. */
    row_cleanout_collect(txn_rec->trx_id, *txn_rec, rec, index, offsets, pcur);

    return false;
  }

  ut_ad(cache_hit == false);

  active = txn_undo_hdr_lookup_low(txn_rec, nullptr, nullptr);
  if (active) {
    return active;
  } else {
    ut_ad(!lizard_undo_ptr_is_active(txn_rec->undo_ptr));
    lizard_ut_ad(txn_rec->scn > 0 && txn_rec->scn < SCN_MAX);
    lizard_ut_ad(txn_rec->gcn > 0 && txn_rec->gcn < GCN_MAX);

    /** Collect record to cleanout later.*/
    row_cleanout_collect(txn_rec->trx_id, *txn_rec, rec, index, offsets, pcur);
    /** Cache txn info into tcn. */
    trx_cache_tcn(txn_rec->trx_id, *txn_rec, rec, index, offsets, pcur);

    return false;
  }
}

}  // namespace lizard
