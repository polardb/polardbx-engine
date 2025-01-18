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
#include "trx0rec.h"
#include "trx0rseg.h"
#include "trx0trx.h"
#include "trx0undo.h"
#include "fut0lst.h"

#include "sql_class.h"
#include "sql_error.h"
#include "sql_plugin_var.h"

#include "ha_innodb.h"
#include "clone0clone.h"

#include "lizard0cleanout.h"
#include "lizard0cleanout0safe.h"
#include "lizard0gcs.h"
#include "lizard0mon.h"
#include "lizard0mysql.h"
#include "lizard0row.h"
#include "lizard0scn.h"
#include "lizard0txn0space.h"
#include "lizard0undo.h"
#include "lizard0undo0types.h"
#include "lizard0xa.h"
#include "lizard0erase.h"
#include "lizard0mtr.h"
#include "lizard0txn.h"
#include "lizard0undo0retent.h"

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
  return xes_storage & XES_ALLOCATED_TAGS;
}
void trx_undo_t::allocate_tags() { xes_storage |= XES_ALLOCATED_TAGS; }
bool trx_undo_t::ac_prepare_allocated() const {
  return xes_storage & XES_ALLOCATED_AC_PREPARE;
}
bool trx_undo_t::ac_commit_allocated() const {
  return xes_storage & XES_ALLOCATED_AC_COMMIT;
}
void trx_undo_t::allocate_ac_prepare() {
  xes_storage |= XES_ALLOCATED_AC_PREPARE;
}
void trx_undo_t::allocate_ac_commit() {
  xes_storage |= XES_ALLOCATED_AC_COMMIT;
}
void trx_undo_t::set_rollback_on_tags() { tags |= XES_TAGS_ROLLBACK; }
void trx_undo_t::set_ac_csr_assigned_on_tags() { tags |= XES_TAGS_AC_ASSIGNED; }
bool trx_undo_t::ac_csr_assigned_on_tags() const {
  return tags & XES_TAGS_AC_ASSIGNED;
}

namespace lizard {

/** The max percent of txn undo page that can be reused */
ulint txn_undo_page_reuse_max_percent = TXN_UNDO_PAGE_REUSE_MAX_PCT_DEF;

/* Lizard transaction undo header operation */
/*-----------------------------------------------------------------------------*/

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
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

/** Confirm the consistent of scn, undo type, undo state. */
bool undo_commit_mark_validate(const trx_undo_t *undo) {
  commit_mark_t cmmt = undo->cmmt;
  ulint type = undo->type;
  ulint state = undo->state;

  if (type == TRX_UNDO_INSERT) {
    if (state == TRX_UNDO_CACHED || state == TRX_UNDO_TO_FREE) {
      ut_a(commit_mark_state(cmmt) == SCN_STATE_INITIAL);
    } else if (state == TRX_UNDO_ACTIVE || undo->is_prepared()) {
      ut_a(commit_mark_state(cmmt) == SCN_STATE_INITIAL);
    } else {
      ut_a(0);
    }
  } else if (type == TRX_UNDO_UPDATE) {
    if (state == TRX_UNDO_CACHED || state == TRX_UNDO_TO_PURGE) {
      /** The update undo log has put into history,
          so commit scn must be valid */
      ut_a(commit_mark_state(cmmt) == SCN_STATE_ALLOCATED);
    } else if (state == TRX_UNDO_ACTIVE || undo->is_prepared()) {
      /** The transaction still be active or has been prepared, */
      ut_a(commit_mark_state(cmmt) == SCN_STATE_INITIAL);
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
      ut_a(commit_mark_state(cmmt) == SCN_STATE_ALLOCATED);
    } else if (state == TRX_UNDO_ACTIVE || undo->is_prepared()) {
      /** The transaction still be active or has been prepared, */
      ut_a(commit_mark_state(cmmt) == SCN_STATE_INITIAL);
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

bool undo_proposal_mark_validate(const trx_undo_t *undo) {
  proposal_mark_t pmmt = undo->pmmt;
  ulint type = undo->type;
  ulint state = undo->state;

  if (type == TRX_UNDO_INSERT || type == TRX_UNDO_UPDATE) {
    ut_a(proposal_mark_state(pmmt) == PROPOSAL_STATE_NULL);
  } else if (type == TRX_UNDO_TXN) {
    if (state == TRX_UNDO_CACHED || state == TRX_UNDO_TO_PURGE ||
        state == TRX_UNDO_PREPARED_IN_TC) {
      if (!pmmt.is_null()) {
        ut_a(proposal_mark_state(pmmt) == PROPOSAL_STATE_ALLOCATED);
      } else {
        ut_a(proposal_mark_state(pmmt) == PROPOSAL_STATE_NULL);
      }
    } else if (state == TRX_UNDO_ACTIVE) {
      /** Might "prepare->rollback background" */
      ut_a(proposal_mark_state(pmmt) == PROPOSAL_STATE_ALLOCATED ||
           proposal_mark_state(pmmt) == PROPOSAL_STATE_NULL);
    } else if (TRX_UNDO_PREPARED_80028 || state == TRX_UNDO_PREPARED) {
      ut_a(proposal_mark_state(pmmt) == PROPOSAL_STATE_NULL);
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

bool txn_slot_validate(const txn_slot_t &txn_slot) {
  if (txn_slot.magic_n != TXN_MAGIC_N) {
    return false;
  }

  if (txn_slot.tags_allocated()) {
    if (txn_slot.state == TXN_UNDO_LOG_ACTIVE) {
      if (txn_slot.is_rollback()) {
        return false;
      }
    }
  }

  if (txn_slot.ac_commit_allocated()) {
    if (txn_slot.maddr.is_null()) {
      return false;
    }
  }

  slot_addr_t slot_addr(txn_slot.slot_ptr);
  if (!slot_addr_validate(slot_addr)) {
    return false;
  }

  return true;
}

bool trx_undo_hdr_slot_validate(const trx_ulogf_t *log_hdr, mtr_t *mtr) {
  slot_addr_t slot_addr;
  slot_addr = trx_undo_hdr_read_slot(log_hdr, mtr);
  return slot_addr_validate(slot_addr);
}

/** Confirm the SLOT is valid in undo log header */
bool trx_undo_hdr_txn_validate(const page_t *undo_page,
                               const trx_ulogf_t *log_hdr, mtr_t *mtr) {
  txn_slot_t txn_slot;
  trx_undo_hdr_read_txn_slot(undo_page, log_hdr, mtr, &txn_slot);
  return txn_slot_validate(txn_slot);
}

/** Check if an update undo log has been marked as purged.
@param[in]  rseg txn rseg
@param[in]  page_size
@return     true   if purged */
bool txn_undo_log_has_purged(const trx_rseg_t *rseg,
                             const page_size_t &page_size) {
  if (rseg->is_txn) {
    ut_ad(!rseg->last_del_marks);
    /* Txn rseg is considered to be purged */
    return true;
  }

  page_t *page;
  trx_ulogf_t *log_hdr;
  ulint type, flag;
  trx_id_t trx_id;
  slot_addr_t slot_addr;
  trx_id_t txn_trx_id = TRX_ID_MAX;
  ulint txn_state = TXN_UNDO_LOG_PURGED;
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
  slot_addr = trx_undo_hdr_read_slot(log_hdr, &mtr);
  if (slot_addr.is_no_redo()) goto no_txn;
  ut_a(!slot_addr.is_null());

  /** The insert/update undo should be released first, otherwise
  it will be deadlocked */
  mtr_commit(&mtr);

  ut_ad(fsp_is_txn_tablespace_by_id(slot_addr.space_id));

  mtr_start(&mtr);

  /* Get the txn undo log header */
  txn_hdr = trx_undo_page_get_s_latched(
                page_id_t(slot_addr.space_id, slot_addr.page_no),
                univ_page_size, &mtr) +
            slot_addr.offset;

  txn_trx_id = mach_read_from_8(txn_hdr + TRX_UNDO_TRX_ID);
  txn_state = mach_read_from_2(txn_hdr + TXN_UNDO_LOG_STATE);

no_txn:
  mtr_commit(&mtr);

  /* No txn, so it is a tempory rseg, no need to check. */
  if (slot_addr.is_no_redo()) return true;

  /* State of the txn undo log should be PURGED if not reused yet. */
  return (txn_trx_id != trx_id || txn_state == TXN_UNDO_LOG_PURGED);
}

#endif

/** Comfirm the commit mark is committed
 *
 * @param[in]	log hdr
 * @param[in]	mini transaction
 *
 * @retval	true	committed
 * @retval	false	not committed
 * */
bool trx_undo_hdr_cmmt_committed(trx_ulogf_t *log_hdr, mtr_t *mtr) {
  commit_mark_t cmmt = trx_undo_hdr_read_cmmt(log_hdr, mtr);
  if (commit_mark_state(cmmt) == SCN_STATE_ALLOCATED) return true;

  return false;
}

/**
  Get txn undo state at trx finish.

  @param[in]      free_limit       space left on txn undo page
  @return  TRX_UNDO_TO_PURGE or TRX_UNDO_CACHED
*/
ulint txn_undo_decide_state_at_finish(ulint free_limit) {
  // 275 undo record + 100 safty margin.
  // why 100 ? In trx_undo_header_create:
  // ut_a(free + TRX_UNDO_LOG_GTID_XA_HDR_SIZE < UNIV_PAGE_SIZE - 100);
  static const ulint min_reserve = TRX_UNDO_LOG_GTID_XA_HDR_SIZE + 100;

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
void trx_undo_hdr_init_cmmt(trx_ulogf_t *log_hdr, mtr_t *mtr) {
  ut_a(mtr && log_hdr);

  /** Here must hold the SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr, MTR_MEMO_PAGE_SX_FIX | MTR_MEMO_PAGE_X_FIX));

  /** Validate the undo page */
  trx_undo_page_validation(page_align(log_hdr));

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
  trx_undo_page_validation(page_align(log_hdr));

  /** utc didn't include csr. */
  ut_ad(UTC_GET_CSR(cmmt.us) == 0);

  mlog_write_ull(log_hdr + TRX_UNDO_SCN, cmmt.scn, mtr);
  mlog_write_ull(log_hdr + TRX_UNDO_UTC, encode_utc(cmmt.us, cmmt.csr), mtr);
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
  trx_undo_page_validation(page_align(log_hdr));

  commit_mark_t cmmt;

  cmmt.scn = mach_read_from_8(log_hdr + TRX_UNDO_SCN);
  cmmt.gcn = mach_read_from_8(log_hdr + TRX_UNDO_GCN);

  std::pair<utc_t, csr_t> utc =
      decode_utc(mach_read_from_8(log_hdr + TRX_UNDO_UTC));

  cmmt.us = utc.first;
  cmmt.csr = utc.second;

  return cmmt;
}

/**
  Read the scn, utc, gcn from prev image.

  @param[in]      log_hdr       undo log header
  @param[in]      mtr           current mtr context
*/
commit_mark_t txn_undo_hdr_read_prev_cmmt(const trx_ulogf_t *log_hdr,
                                          mtr_t *mtr) {
  /** Here must hold the S/SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr,
      MTR_MEMO_PAGE_S_FIX | MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));

  /** Validate the undo page */
  trx_undo_page_validation(page_align(log_hdr));

  commit_mark_t cmmt;

  cmmt.scn = mach_read_from_8(log_hdr + TXN_UNDO_PREV_SCN);
  cmmt.gcn = mach_read_from_8(log_hdr + TXN_UNDO_PREV_GCN);

  std::pair<utc_t, csr_t> utc =
      decode_utc(mach_read_from_8(log_hdr + TXN_UNDO_PREV_UTC));

  cmmt.us = utc.first;
  cmmt.csr = utc.second;

  return cmmt;
}

/**
 * Write xa branch info.
 *
 * @param[in]	log_hdr		undo log header
 * @param[in]	branch		xa branch info
 * @param[in]	mtr		current mtr context
 */
void txn_undo_hdr_write_xa_branch(trx_ulogf_t *log_hdr,
                                  const xa_branch_t &branch, mtr_t *mtr) {
  /** Here must hold the SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr, MTR_MEMO_PAGE_SX_FIX | MTR_MEMO_PAGE_X_FIX));

  /** Validate the undo page */
  trx_undo_page_validation(page_align(log_hdr));
  ut_ad(branch.n_local <= branch.n_global);

  mlog_write_ulint(log_hdr + TXN_UNDO_LOG_XES_AC_N_GLOBALS, branch.n_global,
                   MLOG_2BYTES, mtr);
  mlog_write_ulint(log_hdr + TXN_UNDO_LOG_XES_AC_N_LOCALS, branch.n_local,
                   MLOG_2BYTES, mtr);
}

static void txn_undo_hdr_write_xa_master(trx_ulogf_t *log_hdr,
                                         const xa_addr_t &maddr, mtr_t *mtr) {
  /** Here must hold the SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr, MTR_MEMO_PAGE_SX_FIX | MTR_MEMO_PAGE_X_FIX));

  /** Validate the undo page */
  trx_undo_page_validation(page_align(log_hdr));

  mlog_write_ull(log_hdr + TXN_UNDO_LOG_XES_AC_MASTER_TID, maddr.tid, mtr);

  ut_a(undo_ptr_is_slot(maddr.slot_ptr));

  mlog_write_ull(log_hdr + TXN_UNDO_LOG_XES_AC_MASTER_SLOT_PTR, maddr.slot_ptr,
                 mtr);
}

/** Read proposal mark information from txn undo log header.
 *
 * @param[in]		log header pointer
 * @param[in]		mini transaction
 *
 * @retval		proposal mark */
static proposal_mark_t txn_undo_hdr_read_pmmt(const trx_ulogf_t *log_hdr,
                                              mtr_t *mtr) {
  proposal_mark_t pmmt;
  /** Here must hold the S/SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr,
      MTR_MEMO_PAGE_S_FIX | MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));

  pmmt.gcn = mach_read_from_8(log_hdr + TXN_UNDO_LOG_XES_AC_PROPOSAL_GCN);
  pmmt.csr =
      undo_decode_xes_tags(
          mtr_read_ulint(log_hdr + TXN_UNDO_LOG_XES_TAGS, MLOG_2BYTES, mtr))
          .csr;
  return pmmt;
}

static xa_branch_t txn_undo_hdr_read_xa_branch(const trx_ulogf_t *log_hdr,
                                               mtr_t *mtr) {
  xa_branch_t branch;
  /** Here must hold the S/SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr,
      MTR_MEMO_PAGE_S_FIX | MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));

  branch.n_global =
      mtr_read_ulint(log_hdr + TXN_UNDO_LOG_XES_AC_N_GLOBALS, MLOG_2BYTES, mtr);
  branch.n_local =
      mtr_read_ulint(log_hdr + TXN_UNDO_LOG_XES_AC_N_LOCALS, MLOG_2BYTES, mtr);

  return branch;
}

static xa_addr_t txn_undo_hdr_read_xa_master(const trx_ulogf_t *log_hdr,
                                             mtr_t *mtr) {
  xa_addr_t addr;

  /** Here must hold the S/SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr,
      MTR_MEMO_PAGE_S_FIX | MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));

  addr.tid = mach_read_from_8(log_hdr + TXN_UNDO_LOG_XES_AC_MASTER_TID);
  addr.slot_ptr =
      mach_read_from_8(log_hdr + TXN_UNDO_LOG_XES_AC_MASTER_SLOT_PTR);

  return addr;
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
  Initialize the txn extension fields for the txn undo log header.

  @param[in]      undo_page         undo log header page
  @param[in]      log_hdr           undo log hdr
  @param[in]      prev_image        prev scn/utc if the undo log header is
  reused
  @param[in]      xes_storage   txn extension storage flag
  @param[in]      mtr               mini transaction
*/
void trx_undo_hdr_txn_ext_init(page_t *undo_page, trx_ulogf_t *log_hdr,
                               const commit_mark_t &prev_image,
                               uint8 xes_storage, mtr_t *mtr) {
  ut_ad(mach_read_from_2(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE) ==
        TRX_UNDO_TXN);

  /* Write the magic number */
  mlog_write_ulint(log_hdr + TXN_UNDO_LOG_EXT_MAGIC, TXN_MAGIC_N, MLOG_4BYTES,
                   mtr);

  assert_commit_mark_allocated(prev_image);
  /* Write the prev scn */
  mlog_write_ull(log_hdr + TXN_UNDO_PREV_SCN, prev_image.scn, mtr);
  /* Write the prev utc */
  ut_ad(UTC_GET_CSR(prev_image.us) == 0);

  mlog_write_ull(log_hdr + TXN_UNDO_PREV_UTC,
                 encode_utc(prev_image.us, prev_image.csr), mtr);
  /* Write the prev gcn */
  mlog_write_ull(log_hdr + TXN_UNDO_PREV_GCN, prev_image.gcn, mtr);

  /* Write initial state */
  txn_undo_set_state_at_init(log_hdr, mtr);

  if (DBUG_EVALUATE_IF("sim_old_txn_undo_hdr", true, false)) {
    /* TXN old format: Write the txn undo extension flag */
    mlog_write_ulint(log_hdr + TXN_UNDO_LOG_EXT_STORAGE,
                     XES_ALLOCATED_NONE, MLOG_1BYTE, mtr);
  } else {
    /* Write the txn undo extension flag */
    mlog_write_ulint(log_hdr + TXN_UNDO_LOG_EXT_STORAGE, xes_storage,
                     MLOG_1BYTE, mtr);

    /* Write the txn undo tags_1 */
    mlog_write_ulint(log_hdr + TXN_UNDO_LOG_XES_TAGS, 0, MLOG_2BYTES, mtr);
  }

  /** Write the undo flag when create undo log header */
  mlog_write_ulint(log_hdr + TRX_UNDO_FLAGS, TRX_UNDO_FLAG_TXN, MLOG_1BYTE,
                   mtr);
}

/**
  Read slot address.

  @param[in]      log_hdr       undo log header
  @param[in]      mtr           current mtr context
  @return         decoded slot_addr_t
*/
slot_addr_t trx_undo_hdr_read_slot(const trx_ulogf_t *log_hdr, mtr_t *mtr) {
  slot_ptr_t slot_ptr;
  slot_addr_t slot_addr;

  /** Here must hold the S/SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr,
      MTR_MEMO_PAGE_S_FIX | MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));

  /** Validate the undo page */
  trx_undo_page_validation(page_align(log_hdr));

  slot_ptr = mach_read_from_8(log_hdr + TRX_UNDO_SLOT);
  slot_addr.decode(slot_ptr);

  return slot_addr;
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

  ut_ad(slot_addr_validate(slot_addr));

  slot_ptr_t slot_ptr = slot_addr.encode();

  mlog_write_ull(log_hdr + TRX_UNDO_SLOT, slot_ptr, mtr);
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

  slot_ptr_t slot_ptr;
  if (trx_is_txn_rseg_updated(trx)) {
    assert_trx_undo_ptr_allocated(trx);
    ut_ad(undo_ptr_get_csr(trx->txn_desc.undo_ptr) == CSR_AUTOMATIC);

    trx_undo_t *txn_undo = trx->rsegs.m_txn.txn_undo;
    ut_ad(txn_undo);

    slot_ptr = txn_undo->slot_addr.encode();
    mlog_write_ull(log_hdr + TRX_UNDO_SLOT, slot_ptr, mtr);

    return txn_undo->slot_addr;
  } else {
    /**
      If it's temporary table, didn't have txn undo, but it will have
      update/insert undo log header.
    */
    slot_ptr = txn_sys_t::SLOT_ADDR_NO_REDO.encode();
    mlog_write_ull(log_hdr + TRX_UNDO_SLOT, slot_ptr, mtr);

    return txn_sys_t::SLOT_ADDR_NO_REDO;
  }
}


/**
  Read the txn undo log header extension information.

  @param[in]      undo page
  @param[in]      undo log header
  @param[in]      mtr
  @param[out]     txn_slot

  return true if it's a TXN slot. Otherwise return false.
*/
void trx_undo_hdr_read_txn_slot(const page_t *undo_page,
                                const trx_ulogf_t *undo_header, mtr_t *mtr,
                                txn_slot_t *txn_slot) {
  ulint type;
  type = mtr_read_ulint(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE,
                        MLOG_2BYTES, mtr);
  ut_a(type == TRX_UNDO_TXN);

  auto flag = mtr_read_ulint(undo_header + TRX_UNDO_FLAGS, MLOG_1BYTE, mtr);

  /** If in cleanout safe mode,  */
  ut_a((flag & TRX_UNDO_FLAG_TXN) != 0 || opt_cleanout_safe_mode);

  txn_slot->is_2pp = (flag & TRX_UNDO_FLAG_2PP);

  /** read commit image in txn undo header */
  txn_slot->image = trx_undo_hdr_read_cmmt(undo_header, mtr);

  slot_addr_t slot_addr = {page_get_space_id(undo_page),
                           page_get_page_no(undo_page),
                           ulint((byte *)undo_header - (byte *)undo_page)};
  txn_slot->slot_ptr = slot_addr.encode();
  /** Revision: slot_ptr was used by master uba. */
  // txn_slot->slot_ptr = mach_read_from_8(undo_header + TRX_UNDO_SLOT);

  txn_slot->trx_id = mach_read_from_8(undo_header + TRX_UNDO_TRX_ID);

  txn_slot->magic_n =
      mtr_read_ulint(undo_header + TXN_UNDO_LOG_EXT_MAGIC, MLOG_4BYTES, mtr);

  txn_slot->prev_image = txn_undo_hdr_read_prev_cmmt(undo_header, mtr);

  txn_slot->state =
      mtr_read_ulint(undo_header + TXN_UNDO_LOG_STATE, MLOG_2BYTES, mtr);

  txn_slot->xes_storage =
      mtr_read_ulint(undo_header + TXN_UNDO_LOG_EXT_STORAGE, MLOG_1BYTE, mtr);

  ut_ad(txn_slot->tags == 0);

  if (txn_slot->tags_allocated()) {
    txn_slot->tags =
        mtr_read_ulint(undo_header + TXN_UNDO_LOG_XES_TAGS, MLOG_2BYTES, mtr);

    if (txn_slot->state == TXN_UNDO_LOG_ACTIVE) {
      ut_ad(!txn_slot->is_rollback());
    }
  }

  if (txn_slot->ac_prepare_allocated()) {
    txn_slot->pmmt = txn_undo_hdr_read_pmmt(undo_header, mtr);
    txn_slot->branch = txn_undo_hdr_read_xa_branch(undo_header, mtr);
  }

  if (txn_slot->ac_commit_allocated()) {
    txn_slot->maddr = txn_undo_hdr_read_xa_master(undo_header, mtr);
    ut_ad(!txn_slot->maddr.is_null());
  }
  ut_ad(txn_slot->magic_n == TXN_MAGIC_N);
}

/**
 * Read the txn undo log hdr if xid matched.
 *
 * @param[in]		xid
 * @param[in]		undo page
 * @param[in]		txn undo log header
 * @param[in]		mini transaction
 * @param[out]		txn slot
 *
 * @retval	true	Found
 * @retval	false	Not found
 * */
bool txn_undo_hdr_read_by_xid(const XID *xid, const page_t *undo_page,
                              const trx_ulogf_t *log_hdr, mtr_t *mtr,
                              txn_slot_t *txn_slot) {
  bool found = false;
  XID read_xid;

  /** 1. Check if undo log has XID. */
  auto flag = mach_read_ulint(log_hdr + TRX_UNDO_FLAGS, MLOG_1BYTE);
  if (!(flag & TRX_UNDO_FLAG_XID)) {
    return found;
  }

  /** 2. Read and check XID. */
  trx_undo_read_xid(log_hdr, &read_xid);

  if (read_xid.eq(xid)) {
    trx_undo_hdr_read_txn_slot(undo_page, log_hdr, mtr, txn_slot);
    found = true;
  }
  return found;
}

/** Allocate txn undo and return transaction slot address.
 *
 * @param[in]   trx
 * @param[out]  Slot address
 * @param[out]  trx_id
 *
 * @retval  DB_SUCCESS
 * @retval  DB_ERROR
 **/
dberr_t trx_assign_txn_undo(trx_t *trx, slot_ptr_t *slot_ptr,
                            trx_id_t *trx_id) {
  dberr_t err = DB_SUCCESS;

  ut_ad(trx_is_registered_for_2pc(trx) && trx_is_started(trx) && trx->id != 0 &&
        !trx->read_only && !trx->internal);

  ut_ad(trx_is_txn_rseg_assigned(trx));

  auto undo_ptr = &trx->rsegs.m_txn;
  if (!undo_ptr->txn_undo) {
    // auto &gtid_persistor = clone_sys->get_gtid_persistor();
    // gtid_persistor.set_persist_gtid(trx, true);
    /** For External XA transaction, THD::se_persists_gtid must be always true.
    That's mean that the GTID is always persisted by SE for External XA
    transaction.

    Now this function is only used for External XA transaction, so the GTID must
    be always persisted by SE. If in the future, this function is not only used
    for External XA transaction, the gtid_persistor.set_persist_gtid might be
    called to set the SE_GTID_PERSIST flag. */
    /**
      Revision:
      For group update (hotspot), the follower might update nothing but only
      generate binlog. It means that the GTID might be also assigned but the
      SE hasn't persist the GTID. We fit it so that the GTID is also persisted
      by SE.
    */
    ut_ad(trx->mysql_thd &&
          ((trx->mysql_thd->get_transaction() &&
            !trx->mysql_thd->get_transaction()->xid_state()->has_state(
                XID_STATE::XA_NOTR)) ||
           trx->mysql_thd->gu_ctx.is_follower()));

    mutex_enter(&trx->undo_mutex);
    err = trx_always_assign_txn_undo(trx);
    mutex_exit(&trx->undo_mutex);
  }

  if (err == DB_SUCCESS && slot_ptr) {
    ut_ad(undo_ptr->txn_undo);
    *slot_ptr = undo_ptr->txn_undo->slot_addr.encode();
  }

  if (err == DB_SUCCESS && trx_id) {
    *trx_id = trx->id;
  }

  return err;
}

trx_undo_t *trx_undo_get_txn(const trx_t *trx) {
  if (trx && trx_is_txn_rseg_assigned(trx) && trx_is_txn_rseg_updated(trx))
    return trx->rsegs.m_txn.txn_undo;

  return nullptr;
}

/**
  Add space for txn extension and initialize the fields.
  @param[in]      rseg              rollback segment
  @param[in]      undo_page         undo log header page
  @param[in]      mtr               mini transaction
  @param[in]      offset            txn header byte offset on page
  @param[in]      xes_storage   txn extension storage flag
  @param[out]     slot_addr         slot address of created txn
  @param[out]     prev_image        prev scn/utc
*/
void trx_undo_header_add_space_for_txn(trx_rseg_t *rseg, page_t *undo_page,
                                       mtr_t *mtr, ulint offset,
                                       uint8 xes_storage,
                                       slot_addr_t *slot_addr,
                                       commit_mark_t *prev_image) {
  page_no_t page_no;

  ut_ad(mutex_own(&rseg->mutex));
  ut_ad(slot_addr);
  ut_ad(prev_image);

  /** Lizard: add slot addr into undo log header */
  page_no = page_get_page_no(undo_page);
  *slot_addr = {rseg->space_id, page_no, offset};
  ut_ad(slot_addr->is_redo());
  trx_undo_hdr_write_slot(undo_page + offset, *slot_addr, mtr);

  /** Add space for txn. */
  trx_undo_hdr_add_space_for_txn(undo_page, undo_page + offset, mtr);

  /** Init txn extension fields. */
  trx_undo_hdr_txn_ext_init(undo_page, undo_page + offset, *prev_image,
                            xes_storage, mtr);
}

/**
 * Init segment tailer list when reuse txn undo log segemnt.
 *
 * @param[in/out]	txn undo page
 * @param[in]		page size
 * @param[in/out]	mtr */
static void txn_useg_reuse(page_t *undo_page, const page_size_t &page_size,
                           mtr_t *mtr);

/**
  Initialize the page header and segment header for txn undo.
  Currently, this is only used by the 'txn_undo_get_free' and
  'txn_purge_segment_to_cached_list' functions for txn undo segment reuse.

  @param[in]      rseg      rollback segment
  @param[in]      rseg_header      rollback segment header
  @param[in]      undo_page      txn undo page
  @param[in]      slot_no       index for free slot in undo log seg
  @param[in]      mtr       mini transaction
  @param[in]      trx_id        transaction id
  @param[in]      xes_storage   txn extension storage flag
  @param[out]     slot_addr     slot address of created txn
  @param[out]     prev_image    prev scn/utc
*/
static ulint txn_undo_segment_reuse(trx_rseg_t *rseg, trx_rsegf_t *rseg_header,
                                    page_t *undo_page, ulint slot_no,
                                    mtr_t *mtr, trx_id_t trx_id,
                                    uint8 xes_storage,
                                    slot_addr_t *slot_addr,
                                    commit_mark_t *prev_image) {
  trx_upagef_t *page_hdr;
  trx_usegf_t *seg_hdr;
  page_no_t page_no;
  ulint offset = 0;

  ut_ad(slot_no != ULINT_UNDEFINED);

  seg_hdr = undo_page + TRX_UNDO_SEG_HDR;
  page_hdr = undo_page + TRX_UNDO_PAGE_HDR;
  page_no = page_get_page_no(undo_page);

  /** Init txn undo page header. */
  trx_undo_page_init(undo_page, TRX_UNDO_TXN, mtr);

  /** Init txn undo segment tailor. */
  txn_useg_reuse(undo_page, rseg->page_size, mtr);

  /** Init txn undo segment header. */
  mlog_write_ulint(page_hdr + TRX_UNDO_PAGE_FREE,
                   TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE, MLOG_2BYTES, mtr);
  mlog_write_ulint(seg_hdr + TRX_UNDO_LAST_LOG, 0, MLOG_2BYTES, mtr);
  flst_init(seg_hdr + TRX_UNDO_PAGE_LIST, mtr);
  flst_add_last(seg_hdr + TRX_UNDO_PAGE_LIST, page_hdr + TRX_UNDO_PAGE_NODE,
                mtr);
  ut_ad(mach_read_from_2(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE) ==
        TRX_UNDO_TXN);

  /** Set the undo log slot */
  trx_rsegf_set_nth_undo(rseg_header, slot_no, page_no, mtr);

  if (trx_id != 0) {
    /** Allocate a txn header for a foreground trx. */
    offset = trx_undo_header_create(undo_page, trx_id, prev_image, mtr);

    trx_undo_header_add_space_for_xid(undo_page, undo_page + offset, mtr,
                                      trx_undo_t::Gtid_storage::NONE);

    trx_undo_header_add_space_for_txn(rseg, undo_page, mtr, offset, xes_storage,
                                      slot_addr, prev_image);

    ut_ad(offset == TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE);
    ut_ad(slot_addr->is_redo());
    assert_commit_mark_allocated(*prev_image);
  } else {
    /** Reset TRX_UNDO_STATE to TRX_UNDO_CACHED for a background task. */
    mlog_write_ulint(seg_hdr + TRX_UNDO_STATE, TRX_UNDO_CACHED, MLOG_2BYTES,
                     mtr);
  }

  return offset;
}

/**
  Get newest log header in last (oldest) log segment from free list .
  @params[in]   rseg            update undo rollback segment
  @params[out]  log header address of last log
  @params[out]	rollback segment statistics

  @retval	commit mark of last log header
*/
static commit_mark_t txn_free_get_last_log(trx_rseg_t *rseg, fil_addr_t &addr,
                                           mtr_t *mtr, rseg_stat_t *stat) {
  trx_rsegf_t *rseg_hdr;
  page_t *undo_page;
  ulint offset;
  commit_mark_t cmmt;
  ut_ad(mutex_own(&rseg->mutex));
  ut_ad(rseg->is_txn);

  rseg_hdr = trx_rsegf_get(rseg->space_id, rseg->page_no, rseg->page_size, mtr);

  if (stat) {
    stat->rseg_pages = rseg->get_curr_size();
    stat->secondary_pages =
        mtr_read_ulint(rseg_hdr + TXN_RSEG_FREE_LIST_SIZE, MLOG_4BYTES, mtr);
    stat->secondary_length = flst_get_len(rseg_hdr + TXN_RSEG_FREE_LIST);
  }

  addr = flst_get_last(rseg_hdr + TXN_RSEG_FREE_LIST, mtr);
  if (fil_addr_is_null(addr)) {
    /** The free list is empty. */
    return cmmt;
  }
  undo_page = trx_undo_page_get(page_id_t(rseg->space_id, addr.page),
                                rseg->page_size, mtr);
  offset = mach_read_from_2(undo_page + TRX_UNDO_SEG_HDR + TRX_UNDO_LAST_LOG);
  addr.boffset = offset;

  if (offset != 0) {
    cmmt = trx_undo_hdr_read_cmmt(undo_page + offset, mtr);
  }
  return cmmt;
}

static void txn_free_remove_page(trx_rsegf_t *rseg_hdr, page_t *undo_page,
                                 mtr_t *mtr) {
  flst_remove(rseg_hdr + TXN_RSEG_FREE_LIST,
              undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE, mtr);
}

static page_t *txn_free_get_next_page(trx_rseg_t *rseg, mtr_t *mtr) {
  trx_rsegf_t *rseg_hdr;
  trx_usegf_t *seg_hdr;
  page_t *undo_page;
  page_t *prev_undo_page;
  ulint seg_size;
  ulint free_size;
  fil_addr_t hdr_addr;

  ut_ad(mutex_own(&rseg->mutex));

  rseg_hdr = trx_rsegf_get(rseg->space_id, rseg->page_no, rseg->page_size, mtr);

  hdr_addr = flst_get_last(rseg_hdr + TXN_RSEG_FREE_LIST, mtr);
  undo_page = trx_undo_page_get(page_id_t(rseg->space_id, hdr_addr.page),
                                rseg->page_size, mtr);

  auto prev_addr = flst_get_prev_addr(
      undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE, mtr);

  /** Phase 1: Remove from free list */
  seg_hdr = undo_page + TRX_UNDO_SEG_HDR;
  seg_size = flst_get_len(seg_hdr + TRX_UNDO_PAGE_LIST);
  /** The page list always has only its self page */
  ut_a(seg_size == 1);
  txn_free_remove_page(rseg_hdr, undo_page, mtr);

  free_size =
      mtr_read_ulint(rseg_hdr + TXN_RSEG_FREE_LIST_SIZE, MLOG_4BYTES, mtr);
  ut_ad(free_size >= seg_size);
  mlog_write_ulint(rseg_hdr + TXN_RSEG_FREE_LIST_SIZE, free_size - seg_size,
                   MLOG_4BYTES, mtr);

  /** Phase 2: Load next page in the free list after remove. */
  if (fil_addr_is_null(prev_addr)) {
    rseg->last_free_ommt.set_null();
  } else {
    prev_undo_page = trx_undo_page_get_s_latched(
        page_id_t(rseg->space_id, prev_addr.page), rseg->page_size, mtr);

    auto last_log_offset =
        mach_read_from_2(prev_undo_page + TRX_UNDO_SEG_HDR + TRX_UNDO_LAST_LOG);

    if (last_log_offset != 0) {
      rseg->last_free_ommt = trx_undo_hdr_read_cmmt(prev_undo_page + last_log_offset, mtr);
    }
  }

  return undo_page;
}

static page_t *txn_free_fetch_next_page(trx_rseg_t *rseg, mtr_t *mtr) {
  ut_ad(mutex_own(&rseg->mutex));
  /** Only transaction rollback segment have free list */
  ut_ad(rseg->is_txn);

  if (rseg->last_free_ommt.is_null()) {
    ut_d(fil_addr_t addr;);
    ut_ad(txn_free_get_last_log(rseg, addr, mtr, nullptr).is_null());

    return nullptr;
  }

  if (!txn_retention_satisfied(rseg->last_free_ommt.us)) {
    return nullptr;
  }

  return txn_free_get_next_page(rseg, mtr);
}

/**
  Add the node to the txn cached list.

  @param[in]    rseg       trx_rseg_t the txn belongs to
  @param[in]    undo_page  txn undo page
  @param[in]    slot_no    slot number
  @param[in]    mtr        mini transaction
*/
static void txn_add_node_to_cached_list(trx_rseg_t *rseg, page_t *undo_page,
                                        ulint slot_no, mtr_t *mtr) {
  trx_rsegf_t *rseg_hdr;
  page_no_t undo_page_no;
  ulint offset;

  ut_ad(mutex_own(&rseg->mutex));
  /** Only transaction rollback segment have free list */
  ut_ad(rseg->is_txn);
  ut_ad(slot_no != ULINT_UNDEFINED);

  /** Phase 1: Reinit the txn undo log segment header page for reuse. */
  rseg_hdr = trx_rsegf_get(rseg->space_id, rseg->page_no, rseg->page_size, mtr);

  offset =
      txn_undo_segment_reuse(rseg, rseg_hdr, undo_page, slot_no, mtr, 0,
                             XES_ALLOCATED_NONE, nullptr, nullptr);
  ut_a(offset == 0);

  /** Phase 2: Create a memory object for txn undo. */
  XID xid;
  xid.reset();
  undo_page_no = page_get_page_no(undo_page);
  trx_undo_t *undo =
      trx_undo_mem_create(rseg, slot_no, TRX_UNDO_TXN, 0, &xid, undo_page_no, 0,
                          txn_sys_t::SLOT_ADDR_NULL);
  undo->state = TRX_UNDO_CACHED;
  undo->empty = true;

  /** Phase 3: Add the txn undo to cached list. */
  UT_LIST_ADD_LAST(rseg->txn_undo_cached, undo);

  MONITOR_INC(MONITOR_NUM_UNDO_SLOT_CACHED);
  generic_stats.txn_undo_log_recycle.inc();
}

/* txn retention end */

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
  page_no_t page_no;
  ulint offset;
  fil_addr_t node_addr;
  commit_mark_t prev_image = CMMT_LOST;
  slot_addr_t slot_addr;
  uint8 xes_storage = XES_ALLOCATED_V1;

  ulint slot_no = ULINT_UNDEFINED;
  dberr_t err = DB_SUCCESS;

  ut_ad(type == TRX_UNDO_TXN);
  ut_ad(trx_is_txn_rseg_assigned(trx));
  ut_ad(rseg == trx->rsegs.m_txn.rseg);

  ut_ad(mutex_own(&rseg->mutex));

  mtr_t mtr;
  mtr.start();

  /** Only transaction rollback segment have free list */
  ut_ad(rseg->is_txn);

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

  /** Phase 2 : Remove the oldest undo log segment from free list */
  if ((undo_page = txn_free_fetch_next_page(rseg, &mtr)) == nullptr) {
    *undo = nullptr;
    goto func_exit;
  }

  /** Phase 3: Reinit the txn undo log segment header page and create a txn undo
   * header for use. */
  ut_ad(undo_page);
  page_no = page_get_page_no(undo_page);

  offset =
      txn_undo_segment_reuse(rseg, rseg_header, undo_page, slot_no, &mtr,
                             trx_id, xes_storage, &slot_addr, &prev_image);

  ut_ad(offset == TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE);
  ut_ad(slot_addr.is_redo());
  assert_commit_mark_allocated(prev_image);

  /** Phase 4: Create and init a memory object for txn undo. */
  *undo = trx_undo_mem_create(rseg, slot_no, type, trx_id, xid, page_no, offset,
                              slot_addr, &prev_image);

  (*undo)->xes_storage = xes_storage;
  ut_ad((*undo)->flag == TRX_UNDO_FLAG_TXN);

  assert_commit_mark_allocated((*undo)->prev_image);

  if (*undo == NULL) {
    err = DB_OUT_OF_MEMORY;
    goto func_exit;
  } else {
    generic_stats.txn_undo_log_free_list_get.inc();
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

  generic_stats.txn_undo_log_request.inc();

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
dberr_t trx_always_assign_txn_undo(trx_t *trx) {
  dberr_t err = DB_SUCCESS;
  trx_undo_t *undo = nullptr;
  txn_undo_ptr_t *undo_ptr = nullptr;

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
      ut_ad(undo->slot_addr.is_redo());
      /** Only allocate log header, */
      undo->empty = true;

      ut_ad(undo->slot_addr.equal_with(undo->space, undo->hdr_page_no,
                                       undo->hdr_offset));

      trx_mutex_enter(trx);
      trx->txn_desc.assemble_undo_ptr(undo->slot_addr);
      trx_mutex_exit(trx);

      assert_commit_mark_allocated(undo->prev_image);
    }
  } else {
    assert_trx_undo_ptr_allocated(trx);
  }

  return err;
}
/*-----------------------------------------------------------------------------*/
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

  trx_undo_hdr_slot_validation(undo_header, mtr);

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

  /* Update maximum transaction scn for this rollback segment. */
  assert_trx_commit_mark_allocated(trx);
  mlog_write_ull(rseg_header + TRX_RSEG_MAX_TRX_SCN, trx->txn_desc.cmmt.scn,
                 mtr);

  /* lizard: TRX_UNDO_TRX_NO is reserved */
  // mlog_write_ull(undo_header + TRX_UNDO_TRX_NO, trx->no, mtr);

  /* Write information about delete markings to the undo log header */
  if (!undo->del_marks) {
    mlog_write_ulint(undo_header + TRX_UNDO_DEL_MARKS, false, MLOG_2BYTES, mtr);
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
    assert_trx_commit_mark_allocated(trx);

    rseg->last_ommt = trx->txn_desc.cmmt;
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
  assert_undo_commit_mark_allocated(undo);

  trx_purge_add_txn_undo_to_history(trx, undo_ptr, undo_page,
                                    update_rseg_history_len, n_added_logs, mtr);

  UT_LIST_REMOVE(rseg->txn_undo_list, undo);

  undo_ptr->txn_undo = nullptr;

  if (undo->state == TRX_UNDO_CACHED) {
    UT_LIST_ADD_FIRST(rseg->txn_undo_cached, undo);

    MONITOR_INC(MONITOR_NUM_UNDO_SLOT_CACHED);
  } else {
    ut_ad(undo->state == TRX_UNDO_TO_PURGE);

    trx_undo_mem_free(undo);
  }
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

ulint srv_txn_cached_list_keep_size = 0;

/**
  Prefetch the oldest node from the txn free list to the txn cached list.

  @param[in]        rseg        rollback segment
  @param[in]        hdr_addr    txn log hdr address
*/
static void txn_try_prefetch_to_cached_list(trx_rseg_t *rseg, mtr_t *mtr) {
  trx_rsegf_t *rseg_hdr;
  page_t *undo_page = nullptr;
  ulint slot_no = ULINT_UNDEFINED;

  ut_ad(mutex_own(&rseg->mutex));
  /** Only transaction rollback segment have free list */
  ut_ad(rseg->is_txn);

  /** Phase 1 : Find a free slot in rseg array */
  rseg_hdr = trx_rsegf_get(rseg->space_id, rseg->page_no, rseg->page_size, mtr);
  slot_no = trx_rsegf_undo_find_free(rseg_hdr, mtr);
  if (slot_no == ULINT_UNDEFINED) {
    lizard_warn(ER_LIZARD)
        << "Can't find a free slot for txn undo log when recycle, put back "
           "free list instead, maybe decrease "
           "innodb_txn_cached_list_keep_size.";
    return;
  }

  /** Phase 2 : Remove the oldest node in the free list. */
  if ((undo_page = txn_free_fetch_next_page(rseg, mtr)) == nullptr) {
    /** Failed to get the oldest node that satisfied retetion time from the
     * free list. */
    return;
  }

  /** Phase 3 : Prefetch the node to the cached list. */
  ut_ad(undo_page != nullptr);
  txn_add_node_to_cached_list(rseg, undo_page, slot_no, mtr);
}

/**
  Move the txn undo log segment into free list, then try to prefetch an
  available undo log segment from the free list into the cached list.
  @param[in]        rseg        rollback segment
  @param[in]        hdr_addr    txn log hdr address
*/
void txn_recycle_segment(trx_rseg_t *rseg, fil_addr_t hdr_addr) {
  mtr_t mtr;

  mtr_start(&mtr);
  mutex_enter(&rseg->mutex);

  txn_purge_segment_to_free_list(rseg, hdr_addr, &mtr);

  if (srv_txn_cached_list_keep_size > 0 &&
      rseg->txn_undo_cached.get_length() < srv_txn_cached_list_keep_size) {
    txn_try_prefetch_to_cached_list(rseg, &mtr);
  }

  mutex_exit(&rseg->mutex);
  mtr_commit(&mtr);
}

/**
  Put the txn undo log segment into free list after purge all.

  @param[in]        rseg        rollback segment
  @param[in]        hdr_addr    txn log hdr address
*/
void txn_purge_segment_to_free_list(trx_rseg_t *rseg, fil_addr_t hdr_addr,
                                    mtr_t *mtr) {
  trx_rsegf_t *rseg_hdr;
  trx_ulogf_t *log_hdr;
  trx_usegf_t *seg_hdr;
  page_t *undo_page;
  ulint seg_size;
  ulint hist_size;
  ulint free_size;

  ut_ad(mutex_own(&rseg->mutex));

  rseg_hdr = trx_rsegf_get(rseg->space_id, rseg->page_no, rseg->page_size, mtr);

  undo_page = trx_undo_page_get(page_id_t(rseg->space_id, hdr_addr.page),
                                rseg->page_size, mtr);

  seg_hdr = undo_page + TRX_UNDO_SEG_HDR;
  log_hdr = undo_page + hdr_addr.boffset;

  ut_ad(mach_read_from_2(log_hdr + TRX_UNDO_NEXT_LOG) == 0);

  /** The page list always has only its self page */
  seg_size = flst_get_len(seg_hdr + TRX_UNDO_PAGE_LIST);
  ut_a(seg_size == 1);

  /** Remove the undo log segment from history list */
  trx_purge_remove_log_hdr(rseg_hdr, log_hdr, mtr);

  hist_size =
      mtr_read_ulint(rseg_hdr + TRX_RSEG_HISTORY_SIZE, MLOG_4BYTES, mtr);

  ut_ad(hist_size >= seg_size);

  mlog_write_ulint(rseg_hdr + TRX_RSEG_HISTORY_SIZE, hist_size - seg_size,
                   MLOG_4BYTES, mtr);

  ut_ad(rseg->get_curr_size() >= seg_size);

  /** Add the undo log segment from history list */
  free_size =
      mtr_read_ulint(rseg_hdr + TXN_RSEG_FREE_LIST_SIZE, MLOG_4BYTES, mtr);

  /** Independent statistic free_size is equal with free list length since of
   * one page segment */
  ut_ad(free_size == flst_get_len(rseg_hdr + TXN_RSEG_FREE_LIST));

  mlog_write_ulint(rseg_hdr + TXN_RSEG_FREE_LIST_SIZE, free_size + seg_size,
                   MLOG_4BYTES, mtr);

  flst_add_first(rseg_hdr + TXN_RSEG_FREE_LIST,
                 undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE, mtr);

  if (rseg->last_free_ommt.is_null()) {
    rseg->last_free_ommt = trx_undo_hdr_read_cmmt(log_hdr, mtr);
  }

  txn_undo_free_list_validation(rseg_hdr, undo_page, mtr);

  generic_stats.txn_undo_log_free_list_put.inc();
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
void trx_write_scn(byte *ptr, scn_t scn) {
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

  @return           scn_t  scn
*/
scn_t trx_read_scn(const byte *ptr) {
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

  @return           scn_t  scn
*/
gcn_t trx_read_gcn(const byte *ptr) {
  ut_ad(ptr);
  return mach_read_from_8(ptr);
}

void txn_undo_write_xid(const XID *xid, trx_undo_t *undo) {
  trx_usegf_t *seg_hdr;
  trx_ulogf_t *undo_header;
  page_t *undo_page;
  mtr_t mtr;
  ulint offset;

  mtr_start(&mtr);
  undo_page = trx_undo_page_get(page_id_t(undo->space, undo->hdr_page_no),
                                undo->page_size, &mtr);
  seg_hdr = undo_page + TRX_UNDO_SEG_HDR;

  offset = mach_read_from_2(seg_hdr + TRX_UNDO_LAST_LOG);
  undo_header = undo_page + offset;

  ut_ad(!xid->is_null());
  trx_undo_write_xid(undo_header, xid, &mtr);

  mtr_commit(&mtr);
}

void txn_undo_set_state_at_finish(trx_t *trx, trx_ulogf_t *log_hdr,
                                  bool is_rollback, mtr_t *mtr) {
  xa_addr_t maddr;
  auto txn_undo = trx->rsegs.m_txn.txn_undo;
  ut_ad(trx_is_txn_rseg_assigned(trx) && trx_is_txn_rseg_updated(trx));

  maddr = trx->txn_desc.maddr;

  /** 1. Set rollback tag if need */
  if (txn_undo->tags_allocated() && is_rollback) {
    txn_undo->set_rollback_on_tags();
    mlog_write_ulint(log_hdr + TXN_UNDO_LOG_XES_TAGS, txn_undo->tags,
                     MLOG_2BYTES, mtr);
  }

  if (!maddr.is_null()) {
    ut_ad(maddr.is_valid());
    ut_ad(maddr.tid != trx->id &&
          maddr.slot_ptr != undo_ptr_get_slot(trx->txn_desc.undo_ptr));
    txn_undo->allocate_ac_commit();
    mlog_write_ulint(log_hdr + TXN_UNDO_LOG_EXT_STORAGE, txn_undo->xes_storage,
                     MLOG_1BYTE, mtr);

    txn_undo->maddr = maddr;
    txn_undo_hdr_write_xa_master(log_hdr, maddr, mtr);
  }

  /** 3. Set COMMITED state */
  txn_undo_set_state(log_hdr, TXN_UNDO_LOG_COMMITED, mtr);
}

/**
  Set TXN_UNDO_LOG_STATE as TXN_UNDO_LOG_ERASED when erase. NOTES:
  1. Can not hold any other undo page latch because no rsegs mutex is held.
  2. Did not hold rseg mutext because only a TXN undo page is modified.

  @params[in]   txn_cursor        TXN cursor
  @params[in]   scn               the corresponding scn
  @params[in]   page_size         TXN undo page size.
*/
void txn_undo_set_state_at_erase(const txn_cursor_t &txn_cursor, scn_t scn,
                                 const page_size_t &page_size) {
  page_t *undo_page;
  trx_ulogf_t *log_hdr;
  trx_id_t trx_id;
  commit_mark_t cmmt;
  ulint txn_state;
  slot_addr_t txn_addr;

  mtr_t mtr;

  txn_addr = txn_cursor.txn_addr;

  ut_a(txn_cursor.trx_id != 0);

  mtr_start(&mtr);

  undo_page = trx_undo_page_get(page_id_t(txn_addr.space_id, txn_addr.page_no),
                                page_size, &mtr);

  log_hdr = undo_page + txn_addr.offset;

  trx_id = mach_read_from_8(log_hdr + TRX_UNDO_TRX_ID);

  if (trx_id != txn_cursor.trx_id) {
    /* Restore failed, the TXN has been reused. */
    mtr_commit(&mtr);
    return;
  }

  if (!trx_undo_log_is_2pp(log_hdr, &mtr)) {
    /* It's not 2PP log header */
    mtr_commit(&mtr);
    return;
  }

  cmmt = trx_undo_hdr_read_cmmt(log_hdr, &mtr);
  ut_a(cmmt.scn == scn);

  txn_state = mach_read_from_2(log_hdr + TXN_UNDO_LOG_STATE);
  ut_a(txn_state == TXN_UNDO_LOG_PURGED || txn_state == TXN_UNDO_LOG_ERASED);

  if (txn_state == TXN_UNDO_LOG_PURGED) {
    txn_undo_set_state(log_hdr, TXN_UNDO_LOG_ERASED, &mtr);
  }

  mtr_commit(&mtr);
}

void trx_undo_mem_init_for_txn(trx_rseg_t *rseg, trx_undo_t *undo,
                               page_t *undo_page,
                               const trx_ulogf_t *undo_header, ulint type,
                               uint32_t flag, ulint state, mtr_t *mtr) {
  assert_commit_mark_initial(undo->cmmt);
  assert_commit_mark_initial(undo->prev_image);
  ut_ad(undo->xes_storage == XES_ALLOCATED_NONE);
  ut_ad(undo->tags == 0);
  ut_ad(undo->pmmt.is_null());
  ut_ad(undo->branch.is_null());
  ut_ad(undo->maddr.is_null());

  if (type == TRX_UNDO_TXN) {
    ut_ad(flag & TRX_UNDO_FLAG_TXN);
    ut_ad(state != TRX_UNDO_TO_FREE);
    trx_undo_hdr_txn_validation(undo_page, undo_header, mtr);

    /* 1. Init SCN, GCN, UTC */
    undo->cmmt = trx_undo_hdr_read_cmmt(undo_header, mtr);
    undo_commit_mark_validation(undo);

    /* 2. Init prev image. */
    undo->prev_image = txn_undo_hdr_read_prev_cmmt(undo_header, mtr);
    assert_commit_mark_allocated(undo->prev_image);

    /** 3. Init xes_storage */
    undo->xes_storage =
        mtr_read_ulint(undo_header + TXN_UNDO_LOG_EXT_STORAGE, MLOG_1BYTE, mtr);

    /** 4. Init txn_tags_1 */
    if (undo->tags_allocated()) {
      undo->tags =
          mtr_read_ulint(undo_header + TXN_UNDO_LOG_XES_TAGS, MLOG_2BYTES, mtr);
    }

    /** 5. Init async commit related. */
    if (undo->ac_prepare_allocated()) {
      undo->pmmt = txn_undo_hdr_read_pmmt(undo_header, mtr);
      undo->branch = txn_undo_hdr_read_xa_branch(undo_header, mtr);

      undo_proposal_mark_validation(undo);
    }

    if (undo->ac_commit_allocated()) {
      undo->maddr = txn_undo_hdr_read_xa_master(undo_header, mtr);
    }

    /** 6. Init txn_undo_list or txn_undo_cached */
    if (state != TRX_UNDO_CACHED) {
      UT_LIST_ADD_LAST(rseg->txn_undo_list, undo);
    } else {
      UT_LIST_ADD_LAST(rseg->txn_undo_cached, undo);
      MONITOR_INC(MONITOR_NUM_UNDO_SLOT_CACHED);
    }
  } else {
    ut_ad(!(flag & TRX_UNDO_FLAG_TXN));
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
static byte trx_useg_read_flag(const page_t *undo_page,
                               const page_size_t &page_size, mtr_t *mtr) {
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
static inline bool trx_useg_flag_is_set(byte flag, byte bit_mask) {
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
static void trx_useg_set_2pp(page_t *undo_page,
                                   const page_size_t &page_size, mtr_t *mtr) {
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

/**
 * Init segment tailer list when reuse txn undo log segemnt.
 *
 * @param[in/out]	txn undo page
 * @param[in]		page size
 * @param[in/out]	mtr */
static void txn_useg_reuse(page_t *undo_page, const page_size_t &page_size,
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

bool trx_useg_verify(page_t *undo_page, const page_size_t &page_size,
                     mtr_t *mtr) {
  byte flag = 0;

  flag = trx_useg_read_flag(undo_page, page_size, mtr);

  ut_a(flag == 0x00 /** Not used */ ||
       flag == 0xff /** trx_undo_erase_page_end from old version mysqld */ ||
       (flag & (~TRX_USEG_END_FLAG_MASK)) == 0x00);

  return true;
}

/**********************************************************************************/
//	Purge/Erase Status
/**********************************************************************************/

/**
  Get last (oldest) log header from history list.
  @params[in]   rseg            update undo rollback segment
  @params[out]  log header address
  @params[out]	rollback segment statistics

  @retval	commit mark of log header
*/
static commit_mark_t trx_purge_get_last_log(trx_rseg_t *rseg, fil_addr_t &addr,
                                            rseg_stat_t *stat = nullptr) {
  mtr_t mtr;
  trx_rsegf_t *rseg_hdr;
  trx_ulogf_t *log_hdr;
  page_t *undo_page;
  commit_mark_t cmmt;

  mtr_start(&mtr);
  rseg->latch();

  rseg_hdr =
      trx_rsegf_get(rseg->space_id, rseg->page_no, rseg->page_size, &mtr);

  /** Collect rollback segment statistics */
  if (stat) {
    stat->rseg_pages = rseg->get_curr_size();
    stat->history_pages =
        mtr_read_ulint(rseg_hdr + TRX_RSEG_HISTORY_SIZE, MLOG_4BYTES, &mtr);
    stat->history_length = flst_get_len(rseg_hdr + TRX_RSEG_HISTORY);
  }

  addr = trx_purge_get_log_from_hist(
      flst_get_last(rseg_hdr + TRX_RSEG_HISTORY, &mtr));

  if (addr.page == FIL_NULL) {
    rseg->unlatch();
    mtr_commit(&mtr);
    return cmmt;
  }

  undo_page = trx_undo_page_get_s_latched_with_hint(
      page_id_t(rseg->space_id, addr.page), rseg->page_size,
      Cache_hint::KEEP_OLD, &mtr);

  log_hdr = undo_page + addr.boffset;
  cmmt = trx_undo_hdr_read_cmmt(log_hdr, &mtr);

  rseg->unlatch();
  mtr_commit(&mtr);
  return cmmt;
}
/**
  Get newest log header in last (oldest) log segment from free list .
  @params[in]   rseg            update undo rollback segment
  @params[out]  log header address of last log

  @retval	commit mark of last log header
*/
commit_mark_t txn_free_get_last_log(trx_rseg_t *rseg, fil_addr_t &addr,
                                    rseg_stat_t *stat) {
  mtr_t mtr;
  commit_mark_t cmmt;
  ut_ad(rseg->is_txn);

  mtr_start(&mtr);
  rseg->latch();

  cmmt = txn_free_get_last_log(rseg, addr, &mtr, stat);

  rseg->unlatch();
  mtr_commit(&mtr);
  return cmmt;
}

/** Calculate rsegment status of undo tablespace.
 *
 * @param[in/out]	status array.
 * */
void trx_trunc_status(std::vector<trunc_status_t> &array) {
  mutex_enter(&undo::ddl_mutex);
  undo::spaces->s_lock();

  for (auto undo_space : undo::spaces->m_spaces) {
    trunc_status_t status;
    /** 1. undo tablespace name and file size. */
    status.undo_name = undo_space->space_name();
    status.file_pages = fil_space_get_size(undo_space->id());

    undo_space->rsegs()->s_lock();

    commit_mark_t hist_cmmt, sec_cmmt, last;
    fil_addr_t addr;
    for (auto rseg : *undo_space->rsegs()) {
      rseg_stat_t stat;
      /** 2. Oldest log hdr in history list */
      last = trx_purge_get_last_log(rseg, addr, &stat);
      if (last.scn < hist_cmmt.scn) {
        hist_cmmt = last;
      }
      /** 3. Oldest log hdr in semi-purge or free list */
      if (undo_space->is_txn()) {
        last = txn_free_get_last_log(rseg, addr, &stat);
      } else {
        last = trx_erase_get_last_log(rseg, addr, &stat);
      }
      if (last.scn < sec_cmmt.scn) {
        sec_cmmt = last;
      }
      status.aggregate(stat);
    }
    undo_space->rsegs()->s_unlock();

    status.oldest_history_utc = hist_cmmt.us;
    status.oldest_secondary_utc = sec_cmmt.us;

    status.oldest_history_scn = hist_cmmt.scn;
    status.oldest_secondary_scn = sec_cmmt.scn;

    status.oldest_history_gcn = hist_cmmt.gcn;
    status.oldest_secondary_gcn = sec_cmmt.gcn;
    /***/
    array.push_back(status);
  }

  undo::spaces->s_unlock();
  mutex_exit(&undo::ddl_mutex);
}

/** Calculate purge/erase status of undo tablespace.
 *
 * @param[in/out]	status array.
 * */
void trx_purge_status(purge_status_t &status) {
  status.history_length = trx_sys->rseg_history_len.load();

  status.current_scn = gcs_load_scn();
  status.current_gcn = gcs_load_gcn();

  status.purged_scn = purge_sys->purged_scn.load();
  status.purged_gcn = purge_sys->purged_gcn.get();

  status.erased_scn = erase_sys->erased_scn.load();
  status.erased_gcn = erase_sys->erased_gcn.get();
}

}  // namespace lizard
