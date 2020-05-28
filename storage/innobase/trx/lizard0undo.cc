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

#include "trx0rec.h"
#include "trx0undo.h"
#include "trx0rseg.h"
#include "page0types.h"

#include "lizard0scn.h"
#include "lizard0sys.h"
#include "lizard0txn.h"
#include "lizard0undo.h"
#include "lizard0undo0types.h"

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

namespace lizard {

/**
  Encode UBA into undo_ptr that need to copy into record
  @param[in]      undo addr
  @param[out]     undo ptr
*/
void undo_encode_undo_addr(const undo_addr_t &undo_addr, undo_ptr_t *undo_ptr) {
  lizard_undo_addr_validation(&undo_addr, nullptr);
  ulint rseg_id = undo::id2num(undo_addr.space_id);

  *undo_ptr = (undo_ptr_t)(undo_addr.state) << 55 | (undo_ptr_t)rseg_id << 48 |
              (undo_ptr_t)(undo_addr.page_no) << 16 | undo_addr.offset;
}

/* Lizard transaction undo header operation */
/*-----------------------------------------------------------------------------*/

#if defined UNIV_DEBUG || defined LIZARD_DEBUG

/** Check the UBA validation */
bool undo_addr_validation(const undo_addr_t *undo_addr,
                          const dict_index_t *index) {
  bool internal_table = false;
  if (index) {
    internal_table =
        (my_strcasecmp(system_charset_info, index->table->name.m_name,
                       "mysql/innodb_dynamic_metadata") == 0
             ? true
             : false);
  }

  if ((index && index->table->is_temporary()) || internal_table) {
    ut_a(undo_addr->space_id == 0);
    ut_a(undo_addr->page_no == 0);
    ut_a(undo_addr->offset == 0);
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
  commit_scn_t scn = undo->scn;
  ulint type = undo->type;
  ulint state = undo->state;

  if (type == TRX_UNDO_INSERT) {
    if (state == TRX_UNDO_CACHED || state == TRX_UNDO_TO_FREE) {
      ut_a(commit_scn_state(scn) == SCN_STATE_INITIAL);
    } else if (state == TRX_UNDO_ACTIVE || undo->is_prepared()) {
      ut_a(commit_scn_state(scn) == SCN_STATE_INITIAL);
    } else {
      ut_a(0);
    }
  } else if (type == TRX_UNDO_UPDATE) {
    if (state == TRX_UNDO_CACHED || state == TRX_UNDO_TO_PURGE) {
      /** The update undo log has put into history,
          so commit scn must be valid */
      ut_a(commit_scn_state(scn) == SCN_STATE_ALLOCATED);
    } else if (state == TRX_UNDO_ACTIVE || undo->is_prepared()) {
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
    } else if (state == TRX_UNDO_ACTIVE || undo->is_prepared()) {
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
#endif

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
}

/**
  Write the scn and utc when commit.
  Include the redo log

  @param[in]      log_hdr       undo log header
  @param[in]      commit_scn    commit scn number
  @param[in]      mtr           current mtr context
*/
void trx_undo_hdr_write_scn(trx_ulogf_t *log_hdr,
                            std::pair<scn_t, utc_t> &cmmt_scn, mtr_t *mtr) {
  /** Here must hold the SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr, MTR_MEMO_PAGE_SX_FIX | MTR_MEMO_PAGE_X_FIX));

  /** Validate the undo page */
  lizard_trx_undo_page_validation(page_align(log_hdr));

  mlog_write_ull(log_hdr + TRX_UNDO_SCN, cmmt_scn.first, mtr);
  mlog_write_ull(log_hdr + TRX_UNDO_UTC, cmmt_scn.second, mtr);
}

/**
  Read the scn and utc.

  @param[in]      log_hdr       undo log header
  @param[in]      mtr           current mtr context
*/
std::pair<scn_t, utc_t> trx_undo_hdr_read_scn(trx_ulogf_t *log_hdr,
                                              mtr_t *mtr) {
  /** Here must hold the S/SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr,
      MTR_MEMO_PAGE_S_FIX | MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));

  /** Validate the undo page */
  lizard_trx_undo_page_validation(page_align(log_hdr));

  scn_t scn = mach_read_from_8(log_hdr + TRX_UNDO_SCN);
  utc_t utc = mach_read_from_8(log_hdr + TRX_UNDO_UTC);

  return std::make_pair(scn, utc);
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

  @param[in]      undo_page     undo log header page
  @param[in]      log_hdr       undo log hdr
  @param[in]      mtr
*/
void trx_undo_hdr_init_for_txn(trx_undo_t *undo, page_t *undo_page,
                               trx_ulogf_t *log_hdr, mtr_t *mtr) {
  ut_ad(mach_read_from_2(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE) ==
        TRX_UNDO_TXN);

  /* Write the magic number */
  mlog_write_ulint(log_hdr + TXN_UNDO_LOG_EXT_MAGIC, TXN_MAGIC_N, MLOG_4BYTES,
                   mtr);

  /* Write the txn undo extension flag */
  mlog_write_ulint(log_hdr + TXN_UNDO_LOG_EXT_FLAG, 0, MLOG_1BYTE, mtr);

  ut_a(undo->flag == 0);

  undo->flag |= TRX_UNDO_FLAG_TXN;

  /** Write the undo flag when create undo log header */
  mlog_write_ulint(log_hdr + TRX_UNDO_FLAGS, undo->flag, MLOG_1BYTE, mtr);
}

/**
  Read the txn undo log header extension information.

  @param[in]      undo page
  @param[in]      undo log header
  @param[in]      mtr
  @param[out]     txn_undo_ext
*/
void trx_undo_hdr_read_txn(const page_t *undo_page,
                           const trx_ulogf_t *undo_header, mtr_t *mtr,
                           txn_undo_ext_t *txn_undo_ext) {
  ulint type;
  type = mtr_read_ulint(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE,
                        MLOG_2BYTES, mtr);
  ut_a(type == TRX_UNDO_TXN);

  auto flag = mtr_read_ulint(undo_header + TRX_UNDO_FLAGS, MLOG_1BYTE, mtr);

  ut_a((flag & TRX_UNDO_FLAG_TXN) != 0);

  txn_undo_ext->magic_n =
      mtr_read_ulint(undo_header + TXN_UNDO_LOG_EXT_MAGIC, MLOG_4BYTES, mtr);

  txn_undo_ext->flag =
      mtr_read_ulint(undo_header + TXN_UNDO_LOG_EXT_FLAG, MLOG_1BYTE, mtr);

  ut_a(txn_undo_ext->magic_n == TXN_MAGIC_N && txn_undo_ext->flag == 0);
}

/* Lizard transaction rollback segment operation */
/*-----------------------------------------------------------------------------*/

/**
  Round-bin get the rollback segment from transaction tablespace

  @retval     rollback segment
*/
static trx_rseg_t *get_next_txn_rseg() {
  static std::atomic<ulint> rseg_counter = 0;

  undo::Tablespace *undo_space;
  trx_rseg_t *rseg = nullptr;

  ulong n_rollback_segments = srv_rollback_segments;
  /** Lizard : didn't support variable of rollback segment count */
  ut_a(FSP_MAX_ROLLBACK_SEGMENTS == srv_rollback_segments);

  ulint current = rseg_counter;
  rseg_counter.fetch_add(1);

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
          trx_undo_reuse_cached(rseg, type, trx->id, trx->xid,
                                trx_undo_t::Gtid_storage::NONE, &mtr);

  /* TODO:
  if (undo == nullptr) {
     err = txn_undo_get_free(trx, rseg, type, trx->id, trx->xid, &undo);

    if (err != DB_SUCCESS) {
      goto func_exit;
    }
  }
  */

  if (undo == nullptr) {
    err = trx_undo_create(rseg, type, trx->id, trx->xid,
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
      ib::error(ER_LIZARD) << "Could not allocate transaction undo log";
      ut_ad(err != DB_SUCCESS);
    } else {
      /** Only allocate log header, */
      undo->empty = true;

      undo_addr.state = false;
      undo_addr.scn = SCN_NULL;
      undo_addr.space_id = undo->space;
      undo_addr.page_no = undo->hdr_page_no;
      undo_addr.offset = undo->hdr_offset;

      undo_encode_undo_addr(undo_addr, &trx->txn_desc.undo_ptr);
    }
  } else {
    assert_trx_undo_ptr_allocated(trx);
  }

  return err;
}
/*-----------------------------------------------------------------------------*/

/** Comfirm the commit scn is uninited */
static bool trx_undo_hdr_scn_committed(trx_ulogf_t *log_hdr, mtr_t *mtr) {
  commit_scn_t scn = trx_undo_hdr_read_scn(log_hdr, mtr);
  if (commit_scn_state(scn) == SCN_STATE_ALLOCATED) return true;

  return false;
}

/**
  Init the txn description as NULL initial value.
  @param[in]      trx       current transaction
*/
void trx_init_txn_desc(trx_t *trx) { trx->txn_desc = TXN_DESC_NULL; }

/**
  Assign a new commit scn for the transaction when commit

  @param[in]      trx       current transaction
  @param[in/out]  scn_ptr   Commit scn which was generated only once
  @param[in]      undo      txn undo log
  @param[in]      undo page txn undo log header page
  @param[in]      offset    txn undo log header offset
  @param[in]      mtr       mini transaction

  @retval         scn       commit scn struture
*/
commit_scn_t trx_commit_scn(trx_t *trx, commit_scn_t *scn_ptr, trx_undo_t *undo,
                            page_t *undo_hdr_page, ulint hdr_offset,
                            mtr_t *mtr) {
  trx_usegf_t *seg_hdr;
  trx_ulogf_t *undo_hdr;
  commit_scn_t scn = COMMIT_SCN_NULL;

  ut_ad(lizard_sys);
  ut_ad(trx && undo && undo_hdr_page && mtr);

  ut_ad(mutex_own(&trx->rsegs.m_txn.rseg->mutex));
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

  ut_ad(!scn_ptr || commit_scn_state(*scn_ptr) == SCN_STATE_ALLOCATED);

  /** Here must hold the X lock on the page */
  ut_ad(mtr_memo_contains_page(mtr, undo_hdr_page, MTR_MEMO_PAGE_X_FIX));

  seg_hdr = undo_hdr_page + TRX_UNDO_SEG_HDR;
  ulint state = mach_read_from_2(seg_hdr + TRX_UNDO_STATE);

  /** TXN undo log must be finished */
  ut_ad(state == TRX_UNDO_CACHED || state == TRX_UNDO_TO_PURGE);

  /** Commit must be the last log hdr */
  ut_ad(hdr_offset == mach_read_from_2(seg_hdr + TRX_UNDO_LAST_LOG));

  undo_hdr = undo_hdr_page + hdr_offset;
  ut_ad(!trx_undo_hdr_scn_committed(undo_hdr, mtr));

  /** SCN number has been allocated when cleanup redo update undo log */
  scn = (scn_ptr != NULL) ? *scn_ptr : lizard_sys->scn.new_commit_scn();

  ut_ad(commit_scn_state(scn) == SCN_STATE_ALLOCATED);

  /* Step 1: modify the update undo header. */
  trx_undo_hdr_write_scn(undo_hdr, scn, mtr);
  ut_ad(trx_undo_hdr_scn_committed(undo_hdr, mtr));

  /* Step 2: modify undo->scn */
  assert_undo_scn_initial(undo);
  undo->scn = scn;

  /* Step 3: modify trx->scn */
  if (!scn_ptr) {
    assert_trx_scn_initial(trx);
    trx_mutex_enter(trx);
    trx->txn_desc.scn = scn;
    trx_mutex_exit(trx);
  } else {
    assert_trx_scn_allocated(trx);
    ut_ad(trx->txn_desc.scn.first == scn.first);
  }

  return scn;
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

  if (undo->state != TRX_UNDO_CACHED) {
    ulint hist_size;
#ifdef UNIV_DEBUG
    trx_usegf_t *seg_header = undo_page + TRX_UNDO_SEG_HDR;
#endif /* UNIV_DEBUG */

    /* The undo log segment will not be reused */

    if (UNIV_UNLIKELY(undo->id >= TRX_RSEG_N_SLOTS)) {
      ib::fatal(UT_LOCATION_HERE, ER_IB_MSG_1165) << "undo->id is " << undo->id;
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
    trx_sys->rseg_history_len.fetch_add(n_added_logs);
    if (trx_sys->rseg_history_len.load() >
        srv_n_purge_threads * srv_purge_batch_size) {
      srv_wake_purge_thread_if_not_active();
    }
  }

  /* Update maximum transaction number for this rollback segment. */
  mlog_write_ull(rseg_header + TRX_RSEG_MAX_TRX_NO, trx->no, mtr);

  /* Write the trx number to the undo log header */
  mlog_write_ull(undo_header + TRX_UNDO_TRX_NO, trx->no, mtr);

  /* Write information about delete markings to the undo log header */
  if (!undo->del_marks) {
    mlog_write_ulint(undo_header + TRX_UNDO_DEL_MARKS, false, MLOG_2BYTES, mtr);
  }

  /* Lizard: txn undo didn't need gtid information */

  /* Write GTID information if there. */
  // trx_undo_gtid_write(trx, undo_header, undo, mtr);

  if (rseg->last_page_no == FIL_NULL) {
    rseg->last_page_no = undo->hdr_page_no;
    rseg->last_offset = undo->hdr_offset;
    rseg->last_trx_no = trx->no;
    rseg->last_del_marks = undo->del_marks;

    /** trx->scn must be allocated  */
    assert_trx_scn_allocated(trx);

    // TODO:
    // rseg->last_scn = trx->scn.first;
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
  undo_addr_t undo_addr;

  ut_ad(trx->rsegs.m_txn.rseg == nullptr);
  ut_ad(undo->empty);

  /** Already has update/insert undo */
  if (trx->rsegs.m_redo.rseg != nullptr) {
    ut_ad(undo->trx_id == trx->id);
    ut_ad(trx->is_recovered);
    assert_trx_scn_initial(trx);
  } else {
    /** Maybe only allocated txn undo when crashed. */
    *trx->xid = undo->xid;
    trx->id = undo->trx_id;
    trx_sys_rw_trx_add(trx);
    trx->is_recovered = true;
    trx->ddl_operation = undo->dict_operation;
  }
  ut_ad(trx->rsegs.m_txn.txn_undo == nullptr);
  rseg->trx_ref_count++;
  trx->rsegs.m_txn.rseg = rseg;
  trx->rsegs.m_txn.txn_undo = undo;

  undo_addr.state = false;
  undo_addr.space_id = undo->space;
  undo_addr.page_no = undo->hdr_page_no;
  undo_addr.offset = undo->hdr_offset;

  /**
     Currently it's impossible only have txn undo for normal transaction.
     But if crashed just after allocated txn undo, here maybe possible.
  */
  if (trx->rsegs.m_redo.rseg == nullptr) {
    lizard_ut_ad(undo->state == TRX_UNDO_ACTIVE);
    trx->state = TRX_STATE_ACTIVE;

    /* A running transaction always has the number field inited to
    TRX_ID_MAX */

    trx->no = TRX_ID_MAX;

    assert_undo_scn_initial(undo);
    assert_trx_scn_initial(trx);
    assert_txn_desc_initial(trx);

  } else {
    /** trx state has been initialized */
    ut_ad(!trx_state_eq(trx, TRX_STATE_NOT_STARTED));
    if (trx->rsegs.m_redo.insert_undo != nullptr &&
        trx->rsegs.m_redo.update_undo == nullptr) {
      if (trx->state == TRX_STATE_COMMITTED_IN_MEMORY) {
        /** Since the insert undo didn't have valid scn number */
        assert_undo_scn_allocated(undo);
        trx->txn_desc.scn = undo->scn;

        /** Commit state. */
        undo_addr.scn = undo->scn.first;
        undo_addr.state = true;
      }
    } else if (trx->rsegs.m_redo.update_undo != nullptr) {
      /** Update undo scn must be equal with txn undo scn */
      ut_ad(trx->rsegs.m_redo.update_undo->scn == undo->scn);
    }
  }

  /** Resurrect trx->txn_desc.undo_ptr */
  undo_encode_undo_addr(undo_addr, &trx->txn_desc.undo_ptr);

  /* trx_start_low() is not called with resurrect, so need to initialize
  start time here.*/
  if (trx->state.load(std::memory_order_relaxed) == TRX_STATE_ACTIVE ||
      trx->state.load(std::memory_order_relaxed) == TRX_STATE_PREPARED) {
    trx->start_time.store(std::chrono::system_clock::from_time_t(time(nullptr)),
                          std::memory_order_relaxed);
  }
}

/** Prepares a transaction for given rollback segment.
 @return lsn_t: lsn assigned for commit of scheduled rollback segment */
lsn_t txn_prepare_low(
    trx_t *trx,               /*!< in/out: transaction */
    txn_undo_ptr_t *undo_ptr, /*!< in/out: pointer to rollback
                              segment scheduled for prepare. */
    mtr_t *mtr) {
  ut_ad(mtr);

  trx_rseg_t *rseg = undo_ptr->rseg;

  /* Change the undo log segment states from TRX_UNDO_ACTIVE to
  TRX_UNDO_PREPARED: these modifications to the file data
  structure define the transaction as prepared in the file-based
  world, at the serialization point of lsn. */

  rseg->latch();

  ut_ad(undo_ptr->txn_undo);
    /* It is not necessary to obtain trx->undo_mutex here
    because only a single OS thread is allowed to do the
    transaction prepare for this transaction. */
  trx_undo_set_state_at_prepare(trx, undo_ptr->txn_undo, false, mtr);

  rseg->unlatch();

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
}  // namespace lizard
