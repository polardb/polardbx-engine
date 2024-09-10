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

#include "lizard0cleanout.h"
#include "lizard0cleanout0safe.h"
#include "lizard0gcs.h"
#include "lizard0mon.h"
#include "lizard0mysql.h"
#include "lizard0row.h"
#include "lizard0scn.h"
#include "lizard0txn.h"
#include "lizard0undo.h"
#include "lizard0undo0types.h"
#include "lizard0xa.h"
#include "lizard0erase.h"
#include "lizard0mtr.h"

void trx_undo_read_xid(
    const trx_ulogf_t *log_hdr, /*!< in: undo log header */
    XID *xid); /*!< out: X/Open XA Transaction Identification */

#ifndef UNIV_HOTBACKUP
/** Write X/Open XA Transaction Identification (XID) to undo log header */
void trx_undo_write_xid(
    trx_ulogf_t *log_hdr, /*!< in: undo log header */
    const XID *xid,       /*!< in: X/Open XA Transaction Identification */
    mtr_t *mtr);          /*!< in: mtr */
#endif

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
mysql_pfs_key_t undo_retention_mutex_key;
#endif

/*-----------------------------------------------------------------------------*/
/* txn_slot_t related */
/*-----------------------------------------------------------------------------*/
bool txn_slot_t::tags_allocated() const {
  return xes_storage & XES_ALLOCATED_TAGS;
}

bool txn_slot_t::is_rollback() const {
  /** The TXN must be the new format. */
  ut_a(tags_allocated());

  switch (state) {
    case TXN_UNDO_LOG_COMMITED:
    case TXN_UNDO_LOG_PURGED:
      return lizard::undo_decode_xes_tags(tags).is_rollback;
    case TXN_UNDO_LOG_ACTIVE:
      ut_a(!(lizard::undo_decode_xes_tags(tags).is_rollback));
      return false;
    default:
      ut_error;
  }
}

bool txn_slot_t::ac_prepare_allocated() const {
  return xes_storage & XES_ALLOCATED_AC_PREPARE;
}
bool txn_slot_t::ac_commit_allocated() const {
  return xes_storage & XES_ALLOCATED_AC_COMMIT;
}

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

/** assemble undo ptr */
void txn_desc_t::assemble(const commit_mark_t &mark,
                          const slot_addr_t &slot_addr) {
  bool state = (mark.scn != SCN_NULL);
  if (state) {
    assert_commit_mark_allocated(mark);
  } else {
    assert_commit_mark_initial(mark);
  }
  cmmt = mark;
  undo_addr_t undo_addr(slot_addr, state, mark.csr);
  lizard::undo_encode_undo_addr(undo_addr, &this->undo_ptr);
}
/** assemble undo ptr */
void txn_desc_t::assemble_undo_ptr(const slot_addr_t &slot_addr) {
  bool state = (cmmt.scn != SCN_NULL);
  if (state) {
    assert_commit_mark_allocated(cmmt);
  } else {
    assert_commit_mark_initial(cmmt);
  }
  undo_addr_t undo_addr(slot_addr, state, cmmt.csr);
  lizard::undo_encode_undo_addr(undo_addr, &this->undo_ptr);
}

void txn_desc_t::resurrect_xa(const proposal_mark_t &txn_pmmt,
                              const xa_branch_t &txn_branch,
                              const xa_addr_t &txn_maddr) {
  pmmt = txn_pmmt;
  branch = txn_branch;
  maddr = txn_maddr;
}

void txn_desc_t::copy_xa_when_prepare(const MyGCN &xa_gcn,
                                      const xa_branch_t &xa_branch) {
  ut_ad(xa_gcn.is_pmmt_gcn());
  ut_ad(xa_gcn.decided());
  ut_ad(xa_gcn.pushed_up());
  pmmt.gcn = xa_gcn.gcn();
  pmmt.csr = xa_gcn.csr();

  ut_ad(!xa_branch.is_null());
  branch = xa_branch;
}

void txn_desc_t::copy_xa_when_commit(const MyGCN &xa_gcn,
                                     const xa_addr_t &xa_maddr) {
  ut_ad(xa_gcn.is_cmmt_gcn());
  ut_ad(xa_gcn.decided());
  ut_ad(xa_gcn.pushed_up());
  cmmt.gcn = xa_gcn.gcn();
  cmmt.csr = xa_gcn.csr();

  maddr = xa_maddr;
}

bool slot_addr_t::is_null() const {
  return *this == lizard::txn_sys_t::SLOT_ADDR_NULL;
}

bool slot_addr_t::is_no_redo() const {
  return *this == lizard::txn_sys_t::SLOT_ADDR_NO_REDO;
}

bool slot_addr_t::is_redo() const {
  return lizard::fsp_is_txn_tablespace_by_id(space_id);
}

namespace lizard {

/**
 * Init segment tailer list when reuse txn undo log segemnt.
 *
 * @param[in/out]	txn undo page
 * @param[in]		page size
 * @param[in/out]	mtr */
static void txn_useg_reuse(page_t *undo_page, const page_size_t &page_size,
                           mtr_t *mtr);

/** The max percent of txn undo page that can be reused */
ulint txn_undo_page_reuse_max_percent = TXN_UNDO_PAGE_REUSE_MAX_PCT_DEF;

slot_addr_t txn_sys_t::SLOT_ADDR_NO_REDO = {
    SLOT_SPACE_ID_FAKE, SLOT_PAGE_NO_FAKE, SLOT_OFFSET_NO_REDO};

slot_addr_t txn_sys_t::SLOT_ADDR_NULL = {0, 0, 0};

/** Retention time of txn undo data in seconds. */
ulong txn_retention_time = 0;

xes_tags_t undo_decode_xes_tags(ulint tags) {
  xes_tags_t xtt = {false, csr_t::CSR_AUTOMATIC};
  if (tags & XES_TAGS_ROLLBACK) {
    xtt.is_rollback = true;
  }
  if (tags & XES_TAGS_AC_ASSIGNED) {
    xtt.csr = csr_t::CSR_ASSIGNED;
  }
  return xtt;
}

/**
  Encode UBA into undo_ptr that need to copy into record
  @param[in]      undo addr
  @param[out]     undo ptr
*/
void undo_encode_undo_addr(const undo_addr_t &undo_addr, undo_ptr_t *undo_ptr) {
  ulint rseg_id = undo::id2num(undo_addr.space_id);

  *undo_ptr = (undo_ptr_t)(undo_addr.state) << UBA_POS_STATE |
              (undo_ptr_t)(undo_addr.csr) << UBA_POS_CSR |
              (undo_ptr_t)(undo_addr.is_slave) << UBA_POS_IS_SLAVE |
              (undo_ptr_t)rseg_id << UBA_POS_SPACE_ID |
              (undo_ptr_t)(undo_addr.page_no) << UBA_POS_PAGE_NO |
              undo_addr.offset;
}

/**
  Encode addr into slot_ptr that need to write undo header.
  @param[in]      slot addr
  @param[out]     slot ptr
*/
void undo_encode_slot_addr(const slot_addr_t &slot_addr, slot_ptr_t *slot_ptr) {
  ulint rseg_id = undo::id2num(slot_addr.space_id);
  /** Must be a valid txn undo slot address or no_redo special address. */
  lizard_ut_ad(slot_addr_validate(slot_addr));

  *slot_ptr = (slot_ptr_t)rseg_id << SLOT_POS_SPACE_ID |
              (slot_ptr_t)(slot_addr.page_no) << SLOT_POS_PAGE_NO |
              slot_addr.offset;
}
bool undo_slot_addr_equal(const slot_addr_t &slot_addr,
                          const undo_ptr_t undo_ptr) {
  undo_addr_t undo_addr;
  undo_decode_undo_ptr(undo_ptr, &undo_addr);
  if (undo_addr.offset == slot_addr.offset &&
      undo_addr.page_no == slot_addr.page_no &&
      undo_addr.space_id == slot_addr.space_id)
    return true;

  return false;
}

/* Lizard transaction undo header operation */
/*-----------------------------------------------------------------------------*/

#if defined UNIV_DEBUG || defined LIZARD_DEBUG

/** Check the UBA validation */
bool undo_addr_validate(const undo_addr_t *undo_addr,
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
    ut_a(txn_sys_t::instance()->is_temporary(*undo_addr));
  } else if (internal_dm_table) {
    ut_a(txn_sys_t::instance()->is_dynamic_metadata(*undo_addr));
  }

  /** If not special, must be normal txn undo address. */
  if (!txn_sys_t::instance()->is_special(*undo_addr)) {
    ut_a(fsp_is_txn_tablespace_by_id(undo_addr->space_id));
    ut_a(undo_addr->page_no > 0);
    /** TODO: offset must be align to TXN_UNDO_EXT */
    ut_a(undo_addr->offset >= (TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE));
  }
  return true;
}

bool slot_addr_validate(const slot_addr_t &slot_addr) {
  /** no_redo insert/update undo */
  if (slot_addr.is_no_redo() || slot_addr.is_null()) {
    return true;
  } else {
    ut_a(fsp_is_txn_tablespace_by_id(slot_addr.space_id));
    ut_a(slot_addr.page_no > 0);
    /** TODO: offset must be align to TXN_UNDO_EXT */
    ut_a(slot_addr.offset >= (TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE));
  }
  return true;
}

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

/** Comfirm the commit scn is uninited */
static bool trx_undo_hdr_scn_committed(trx_ulogf_t *log_hdr, mtr_t *mtr) {
  commit_mark_t scn = trx_undo_hdr_read_cmmt(log_hdr, mtr);
  if (commit_mark_state(scn) == SCN_STATE_ALLOCATED) return true;

  return false;
}

/** Confirm the SLOT is valid in undo log header */
bool trx_undo_hdr_slot_validate(const trx_ulogf_t *log_hdr, mtr_t *mtr) {
  slot_addr_t slot_addr;
  trx_undo_hdr_read_slot(log_hdr, &slot_addr, mtr);
  return slot_addr_validate(slot_addr);
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
  trx_undo_hdr_read_slot(log_hdr, &slot_addr, &mtr);
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

/**
  Get txn undo state at trx finish.

  @param[in]      free_limit       space left on txn undo page
  @return  TRX_UNDO_TO_PURGE or TRX_UNDO_CACHED
*/
ulint decide_txn_undo_state_at_finish(ulint free_limit) {
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

static void txn_undo_hdr_write_xa_branch(trx_ulogf_t *log_hdr,
                                         const xa_branch_t &branch,
                                         mtr_t *mtr) {
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
  @param[out]     slot addr	decode from slot ptr.
  @param[in]      mtr           current mtr context
*/
slot_ptr_t trx_undo_hdr_read_slot(const trx_ulogf_t *log_hdr,
                                  slot_addr_t *slot_addr, mtr_t *mtr) {
  slot_ptr_t slot_ptr;
  /** Here must hold the S/SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr,
      MTR_MEMO_PAGE_S_FIX | MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));

  /** Validate the undo page */
  trx_undo_page_validation(page_align(log_hdr));

  slot_ptr = mach_read_from_8(log_hdr + TRX_UNDO_SLOT);
  if (slot_addr) {
    undo_decode_slot_ptr(slot_ptr, slot_addr);
  }
  return slot_ptr;
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

  slot_ptr_t slot_ptr;
  undo_encode_slot_addr(slot_addr, &slot_ptr);

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

    undo_encode_slot_addr(txn_undo->slot_addr, &slot_ptr);
    mlog_write_ull(log_hdr + TRX_UNDO_SLOT, slot_ptr, mtr);

    return txn_undo->slot_addr;
  } else {
    /**
      If it's temporary table, didn't have txn undo, but it will have
      update/insert undo log header.
    */
    undo_encode_slot_addr(txn_sys_t::SLOT_ADDR_NO_REDO, &slot_ptr);
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
  undo_encode_slot_addr(slot_addr, &txn_slot->slot_ptr);
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
  Map(Hash) XID to {txn_space_slot, rseg_slot}.

  NOTES: If truncate TXN, this mapping relationship will be destroyed.
         Fortunately, truncate of TXN tablespace is not supported for
         now.

  @retval     {txn_space_slot, rseg_slot}
*/
static txn_space_rseg_slot_t get_txn_space_and_rseg_slot_by_xid(
    const XID *xid) {
  ut_ad(undo::spaces->own_latch());

  size_t current = xa::hash_xid(xid);

  size_t n_rollback_segments = srv_rollback_segments;
  /** Lizard : didn't support variable of rollback segment count */
  ut_a(FSP_MAX_ROLLBACK_SEGMENTS == srv_rollback_segments);

  size_t target_undo_tablespaces = FSP_IMPLICIT_TXN_TABLESPACES;
  /** Notes: didn't need undo::spaces->s_lock() */
  ut_ad(txn_spaces.size() == FSP_IMPLICIT_TXN_TABLESPACES);

  size_t window = current % (target_undo_tablespaces * n_rollback_segments);
  size_t space_slot = window % target_undo_tablespaces;
  size_t rseg_slot = window / target_undo_tablespaces;

  return {space_slot, rseg_slot};
}

/**
  Get a TXN rseg by XID.

  @retval     rollback segment
*/
trx_rseg_t *get_txn_rseg_by_xid(const XID *xid) {
  txn_space_rseg_slot_t txn_slot;

  /* The number of undo tablespaces cannot be changed while
  we have this s_lock. */
  undo::spaces->s_lock();

  txn_slot = get_txn_space_and_rseg_slot_by_xid(xid);

  undo::Tablespace *undo_space;
  trx_rseg_t *rseg = nullptr;

  undo_space = txn_spaces.at(txn_slot.space_slot);
  ut_ad(undo_space->is_active());
  ut_ad(undo_space->is_txn());

  /** NOTES: Truncate of txn is not supported, so always active for now. */
  rseg = undo_space->get_active(txn_slot.rseg_slot);
  ut_a(rseg);

  undo::spaces->s_unlock();

  ut_ad(rseg->trx_ref_count > 0);

  return (rseg);
}

/**
  If during an external XA, check whether the mapping relationship between xid
  and rollback segment is as expected.

  @param[in]        trx         current transaction

  @return           true        if success
*/
bool txn_check_xid_rseg_mapping(const XID *xid, const trx_rseg_t *expect_rseg) {
  txn_space_rseg_slot_t txn_slot;
  bool match;

  ut_ad(expect_rseg != nullptr);

  /* The number of undo tablespaces cannot be changed while
  we have this s_lock. */
  undo::spaces->s_lock();

  txn_slot = get_txn_space_and_rseg_slot_by_xid(xid);

  undo::Tablespace *undo_space;

  undo_space = txn_spaces.at(txn_slot.space_slot);
  ut_ad(undo_space->is_active());
  ut_ad(undo_space->is_txn());

  match = undo_space->compare_rseg(txn_slot.rseg_slot, expect_rseg);

  undo::spaces->s_unlock();

  ut_ad(expect_rseg->trx_ref_count > 0);

  return match;
}

/** Allocate txn undo and return transaction slot address.
 *
 * @param[in]	trx
 * @param[out]	Slot address
 *
 * @retval	DB_SUCCESS
 * @retval	DB_ERROR
 **/
dberr_t trx_assign_txn_undo(trx_t *trx, slot_ptr_t *slot_ptr,
                            trx_id_t *trx_id) {
  dberr_t err = DB_SUCCESS;

  ut_ad(trx_is_registered_for_2pc(trx) && trx_is_started(trx) && trx->id != 0 &&
        !trx->read_only && !trx->internal);

  ut_ad(trx_is_txn_rseg_assigned(trx));

  auto undo_ptr = &trx->rsegs.m_txn;
  if (!undo_ptr->txn_undo) {
    mutex_enter(&trx->undo_mutex);
    err = trx_always_assign_txn_undo(trx);
    mutex_exit(&trx->undo_mutex);
  }

  if (err == DB_SUCCESS && slot_ptr) {
    ut_ad(undo_ptr->txn_undo);
    undo_encode_slot_addr(undo_ptr->txn_undo->slot_addr, slot_ptr);
  }

  if (err == DB_SUCCESS && trx_id) {
    *trx_id = trx->id;
  }

  return err;
}

struct Find_transaction_info_by_xid {
  Find_transaction_info_by_xid(const XID *in_xid)
      : xid(in_xid), found(false), txn_slot(), searched_pages() {}

  /**
    Check whether the page has been searched.
    @param[in]  page_no    page no to search

    @retval     true       page has been searched
  */
  bool operator()(const page_no_t page_no) {
    return (searched_pages.find(page_no) != searched_pages.end());
  }

  /**
    Iterate over the txn slots on the page (rseg, page_no) until finding the txn
    that matches the exact xid.
    @param[in]  page_no    page no to search
    @param[in]  undo_page  undo page
    @param[in]  mtr        mini-transaction handle

    @retval     true       found
  */
  bool operator()(const page_no_t page_no, page_t *undo_page, mtr_t *mtr) {
    trx_ulogf_t *txn_header;
    uint32_t last_offset;
    XID read_xid;

    ut_ad(!found);

    /** Skip page if it has already been searched. */
    if (searched_pages.find(page_no) != searched_pages.end()) {
      return false;
    }

    last_offset =
        mach_read_from_2(undo_page + TRX_UNDO_SEG_HDR + TRX_UNDO_LAST_LOG);

    /** Iterate over the txn slots on the undo page. */
    for (uint32_t txn_offset = TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE;
         txn_offset <= last_offset; txn_offset += TXN_UNDO_LOG_EXT_HDR_SIZE) {
      /** 1. get the txn header. */
      txn_header = undo_page + txn_offset;

      /** 2. Check if undo log has XID. */
      auto flag = mach_read_ulint(txn_header + TRX_UNDO_FLAGS, MLOG_1BYTE);
      if (!(flag & TRX_UNDO_FLAG_XID)) {
        continue;
      }

      /** 3. Read and check XID. */
      trx_undo_read_xid(const_cast<trx_ulogf_t *>(txn_header), &read_xid);
      if (read_xid.eq(xid)) {
        trx_undo_hdr_read_txn_slot(undo_page, txn_header, mtr, &txn_slot);
        found = true;
        break;
      }
    }

    searched_pages.insert(page_no);
    return found;
  }

  const XID *xid;
  bool found;
  txn_slot_t txn_slot;
  std::unordered_set<page_no_t> searched_pages;
};

/**
  Because the retained txn could be in the history list, the free list, the
  cached list, and the txn list, we need to search all of them.

  @param[in]  rseg       trx_rseg_t
  @param[out] func       search function
*/
template <typename Functor>
static void txn_rseg_iterate_lists(trx_rseg_t *rseg, Functor &func) {
  trx_rsegf_t *rseg_header;
  page_t *undo_page;
  fil_addr_t node_addr;

  mtr_t mtr;

  mtr.start();

  mutex_enter(&(rseg->mutex));

  /** 1. Iterate over the history list. */
  rseg_header =
      trx_rsegf_get(rseg->space_id, rseg->page_no, rseg->page_size, &mtr);

  node_addr = flst_get_first(rseg_header + TRX_RSEG_HISTORY, &mtr);

  mtr.commit();

  while (node_addr.page != FIL_NULL) {
    mtr.start();

    undo_page = trx_undo_page_get_s_latched(
        page_id_t(rseg->space_id, node_addr.page), rseg->page_size, &mtr);

    if (func(node_addr.page, undo_page, &mtr)) {
      mtr.commit();
      goto func_exit;
    }

    node_addr = flst_get_next_addr(undo_page + node_addr.boffset, &mtr);
    mtr.commit();
  }

  /** 2. If not found, iterate over the free list. */
  mtr.start();
  rseg_header =
      trx_rsegf_get(rseg->space_id, rseg->page_no, rseg->page_size, &mtr);
  node_addr = flst_get_first(rseg_header + TXN_RSEG_FREE_LIST, &mtr);
  mtr.commit();

  while (node_addr.page != FIL_NULL) {
    mtr.start();

    undo_page = trx_undo_page_get_s_latched(
        page_id_t(rseg->space_id, node_addr.page), rseg->page_size, &mtr);

    if (func(node_addr.page, undo_page, &mtr)) {
      mtr.commit();
      goto func_exit;
    }

    node_addr = flst_get_next_addr(undo_page + node_addr.boffset, &mtr);
    mtr.commit();
  }

  /** 3. If not found, iterate over the cached list. */
  for (trx_undo_t *undo = UT_LIST_GET_FIRST(rseg->txn_undo_cached);
       undo != nullptr; undo = UT_LIST_GET_NEXT(undo_list, undo)) {
    if (func(undo->hdr_page_no)) {
      /** Skip reading page if it has already been searched. */
      continue;
    }

    mtr.start();

    undo_page = trx_undo_page_get_s_latched(
        page_id_t(rseg->space_id, undo->hdr_page_no), rseg->page_size, &mtr);

    if (func(undo->hdr_page_no, undo_page, &mtr)) {
      mtr.commit();
      goto func_exit;
    }

    mtr.commit();
  }

  /** 4. If not found, iterate over the txn list. */
  for (trx_undo_t *undo = UT_LIST_GET_FIRST(rseg->txn_undo_list);
       undo != nullptr; undo = UT_LIST_GET_NEXT(undo_list, undo)) {
    if (func(undo->hdr_page_no)) {
      /** Skip reading page if it has already been searched. */
      continue;
    }

    mtr.start();

    undo_page = trx_undo_page_get_s_latched(
        page_id_t(rseg->space_id, undo->hdr_page_no), rseg->page_size, &mtr);

    if (func(undo->hdr_page_no, undo_page, &mtr)) {
      mtr.commit();
      goto func_exit;
    }

    mtr.commit();
  }

func_exit:
  mutex_exit(&(rseg->mutex));
}

/**
  Find transactions in the finalized state by XID.

  @param[in]  rseg         The rollseg where the transaction is being looked up.
  @params[in] xid          xid
  @param[out] txn_slot     Corresponding txn undo header

  @retval     true if the corresponding transaction is found, false otherwise.
*/
bool txn_rseg_find_trx_info_by_xid(trx_rseg_t *rseg, const XID *xid,
                                   txn_slot_t *txn_slot) {
  Find_transaction_info_by_xid finder(xid);

  txn_rseg_iterate_lists<Find_transaction_info_by_xid>(rseg, finder);

  if (finder.found) {
    *txn_slot = finder.txn_slot;
    return true;
  }

  return false;
}

/**
  Always assign transaction rollback segment for trx
  @param[in]      trx
*/
void trx_assign_txn_rseg(trx_t *trx) {
  const XID *xid_in_thd;
  XID &xid = trx->rsegs.m_txn.xid_for_hash;
  ut_a(xid.is_null());

  ut_ad(trx->rsegs.m_txn.rseg == nullptr);

  /** 1. Get XID if it is in an external XA. */
  xid_in_thd = xa::get_external_xid_from_thd(trx->mysql_thd);
  if (xid_in_thd) {
    xid = *xid_in_thd;
  } else {
    ut_ad(xid.is_null());
  }

  /** 2. Assign rollback segment. By XID if need. */
  if (srv_read_only_mode) {
    trx->rsegs.m_txn.rseg = nullptr;
  } else if (xid.is_null()) {
    trx->rsegs.m_txn.rseg = get_next_txn_rseg();
  } else {
    trx->rsegs.m_txn.rseg = get_txn_rseg_by_xid(&xid);
  }
}

/**
  Whether the txn rollback segment has been assigned
  @param[in]      trx
*/
bool trx_is_txn_rseg_assigned(const trx_t *trx) {
  return trx->rsegs.m_txn.rseg != nullptr;
}

/**
  Whether the txn undo log has modified.
*/
bool trx_is_txn_rseg_updated(const trx_t *trx) {
  return trx->rsegs.m_txn.txn_undo != nullptr;
}

trx_undo_t *txn_undo_get(const trx_t *trx) {
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
commit_mark_t txn_free_get_last_log(trx_rseg_t *rseg, fil_addr_t &addr,
                                    mtr_t *mtr, rseg_stat_t *stat) {
  trx_rsegf_t *rseg_hdr;
  page_t *undo_page;
  ulint offset;
  commit_mark_t cmmt;
  ut_ad(mutex_own(&rseg->mutex));

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

/**
  Check if the txn retention time has been satisfied.
  If the retention time has been satisfied, the txn undo has been retained for
  the required period as defined by txn_retention_time.

  @param[in]  utc        utc on txn to be checked

  @retval     true if the txn retention satisfied
*/
bool txn_retention_satisfied(utc_t utc) {
  ut_ad(utc > 0);

  auto cur_utc = ut_time_system_us();
  std::chrono::microseconds elapsed_time(cur_utc - utc);
  std::chrono::microseconds retention_time =
      std::chrono::seconds(txn_retention_time);
  ut_a(elapsed_time.count() > 0);

  if (elapsed_time > retention_time) {
    return true;
  }

  return false;
}

static void txn_free_remove_page(trx_rsegf_t *rseg_hdr, page_t *undo_page,
                                 mtr_t *mtr) {
  flst_remove(rseg_hdr + TXN_RSEG_FREE_LIST,
              undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE, mtr);
  gcs->txn_undo_log_free_list_len.fetch_sub(1);
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
  LIZARD_MONITOR_INC_TXN_CACHED(1);
  lizard_stats.txn_undo_log_recycle.inc();
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

      trx->txn_desc.assemble_undo_ptr(undo->slot_addr);

      assert_commit_mark_allocated(undo->prev_image);
    }
  } else {
    assert_trx_undo_ptr_allocated(trx);
  }

  return err;
}
/*-----------------------------------------------------------------------------*/

/**
  Init the txn description as NULL initial value.
  @param[in]      trx       current transaction
*/
void trx_init_txn_desc(trx_t *trx) { trx->txn_desc.reset(); }

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
commit_mark_t trx_commit_mark(trx_t *trx, commit_mark_t *cmmt_ptr,
                              trx_undo_t *undo, page_t *undo_hdr_page,
                              ulint hdr_offset, bool *serialised, mtr_t *mtr) {
  trx_usegf_t *seg_hdr;
  trx_ulogf_t *undo_hdr;
  commit_mark_t cmmt;

  ut_ad(gcs);
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

  trx_undo_page_validation(undo_hdr_page);

  /** Here we didn't hold trx_sys mutex */
  ut_ad(!trx_sys_mutex_own());

  ut_ad(!cmmt_ptr || commit_mark_state(*cmmt_ptr) == SCN_STATE_ALLOCATED);

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
    ut_ad(*serialised == false);
    *serialised = true;
    cmmt = gcs->new_commit(trx, mtr);
  } else {
    assert_trx_commit_mark_allocated(trx);
    cmmt = *cmmt_ptr;
    ut_ad(trx->txn_desc.cmmt.scn == cmmt.scn);
    ut_ad(trx->txn_desc.cmmt.gcn == cmmt.gcn);
  }
  ut_ad(commit_mark_state(cmmt) == SCN_STATE_ALLOCATED);

  /* Step 2: modify undo header. */
  trx_undo_hdr_write_cmmt(undo_hdr, cmmt, mtr);
  ut_ad(trx_undo_hdr_scn_committed(undo_hdr, mtr));

  /* Step 3: modify undo->scn */
  assert_undo_commit_mark_initial(undo);
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
  undo_addr_t undo_addr;

  ut_ad(trx->rsegs.m_txn.rseg == nullptr);
  ut_ad(undo->empty);

  /** Already has update/insert undo */
  if (trx->rsegs.m_redo.rseg != nullptr) {
    ut_ad(undo->trx_id == trx->id);
    ut_ad(trx->is_recovered);
    if (trx->rsegs.m_redo.update_undo != nullptr &&
        trx->state == TRX_STATE_COMMITTED_IN_MEMORY) {
      assert_trx_commit_mark_allocated(trx);
      assert_trx_undo_ptr_initial(trx);
      lizard_ut_ad(trx->txn_desc.cmmt == undo->cmmt);
      ut_ad(trx->rsegs.m_redo.update_undo->slot_addr == undo->slot_addr);
    } else {
      assert_trx_commit_mark_initial(trx);
    }

    if (trx->rsegs.m_redo.update_undo != nullptr) {
      ut_ad(trx->rsegs.m_redo.update_undo->slot_addr == undo->slot_addr);
    } else if (trx->rsegs.m_redo.insert_undo != nullptr) {
      ut_ad(trx->rsegs.m_redo.insert_undo->slot_addr == undo->slot_addr);
    }
  } else {
    /** It must be the case: MySQL crashed as soon as the txn undo is created.
    Only temporary table will not create txn */
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
  trx->rsegs.m_txn.xid_for_hash.null();

  assert_commit_mark_allocated(undo->prev_image);

  /**
     Currently it's impossible only have txn undo for normal transaction.
     But if crashed just after allocated txn undo, here maybe possible.
  */
  if (trx->rsegs.m_redo.rseg == nullptr) {
    lizard_ut_ad(undo->state == TRX_UNDO_ACTIVE || undo->is_prepared());
    ut_ad(trx_state_eq(trx, TRX_STATE_NOT_STARTED));

    if (undo->state == TRX_UNDO_ACTIVE) {
      trx->state.store(TRX_STATE_ACTIVE, std::memory_order_relaxed);
    } else {
      /* Can't be TRX_UNDO_CACHED because the undo is in txn_undo_list. */
      ut_a(undo->is_prepared());
      ++trx_sys->n_prepared_trx;
      trx->state.store(TRX_STATE_PREPARED, std::memory_order_relaxed);
    }

    /* A running transaction always has the number field inited to
    TRX_ID_MAX */

    // trx->no = TRX_ID_MAX;

    assert_undo_commit_mark_initial(undo);
    assert_trx_commit_mark_initial(trx);
    assert_txn_desc_initial(trx);

  } else {
    /** trx state has been initialized */
    ut_ad(!trx_state_eq(trx, TRX_STATE_NOT_STARTED));
    if (trx->rsegs.m_redo.insert_undo != nullptr &&
        trx->rsegs.m_redo.update_undo == nullptr) {
      /** SCN info wasn't written in insert undo. */
      if (trx->state == TRX_STATE_COMMITTED_IN_MEMORY) {
        /** Since the insert undo didn't have valid scn number */
        assert_undo_commit_mark_allocated(undo);
        trx->txn_desc.cmmt = undo->cmmt;
      }
    } else if (trx->rsegs.m_redo.update_undo != nullptr) {
      /** Update undo scn must be equal with txn undo scn */
      ut_ad(trx->rsegs.m_redo.update_undo->cmmt == undo->cmmt);
    }
  }

  /** Resurrect trx->txn_desc.undo_ptr */
  trx->txn_desc.assemble_undo_ptr(undo->slot_addr);

  /** Resurrect XA info. */
  trx->txn_desc.resurrect_xa(undo->pmmt, undo->branch, undo->maddr);

  /* trx_start_low() is not called with resurrect, so need to initialize
  start time here.*/
  if (trx->state.load(std::memory_order_relaxed) == TRX_STATE_ACTIVE ||
      trx->state.load(std::memory_order_relaxed) == TRX_STATE_PREPARED) {
    trx->start_time.store(std::chrono::system_clock::from_time_t(time(nullptr)),
                          std::memory_order_relaxed);
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

  // trx_rseg_t *rseg = undo_ptr->rseg;

  /* Change the undo log segment states from TRX_UNDO_ACTIVE to
  TRX_UNDO_PREPARED: these modifications to the file data
  structure define the transaction as prepared in the file-based
  world, at the serialization point of lsn. */

  // rseg->latch();

  ut_ad(undo_ptr->txn_undo);
  /* It is not necessary to obtain trx->undo_mutex here
  because only a single OS thread is allowed to do the
  transaction prepare for this transaction. */
  trx_undo_set_state_at_prepare(trx, undo_ptr->txn_undo, false, mtr);

  // rseg->unlatch();

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

  gcs->txn_undo_log_free_list_len.fetch_add(1);

  if (rseg->last_free_ommt.is_null()) {
    rseg->last_free_ommt = trx_undo_hdr_read_cmmt(log_hdr, mtr);
  }

  txn_undo_free_list_validation(rseg_hdr, undo_page, mtr);

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
  undo_ptr >>= UBA_WIDTH_OFFSET;
  undo_addr->page_no = (ulint)undo_ptr & 0xFFFFFFFF;
  undo_ptr >>= UBA_WIDTH_PAGE_NO;
  rseg_id = (ulint)undo_ptr & 0x7F;
  undo_ptr >>= UBA_WIDTH_SPACE_ID;

  /* Confirm the reserved bits */
  ut_ad(((ulint)undo_ptr & 0x3f) == 0);
  undo_ptr >>= UBA_WIDTH_UNUSED;
  undo_addr->is_slave = static_cast<bool>(undo_ptr & 0x1);

  undo_ptr >>= UBA_WIDTH_IS_SLAVE;
  undo_addr->csr = static_cast<csr_t>(undo_ptr & 0x1);

  undo_ptr >>= UBA_WIDTH_CSR;
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
    lizard_ut_ad(undo_addr->offset >= SLOT_OFFSET_LIMIT);
  }
  /** It's always redo txn undo log */
  undo_addr->space_id = trx_rseg_id_to_space_id(rseg_id, false);
}

/**
  Decode the slot_ptr into slot address
  @param[in]      slot ptr
  @param[out]     slot addr
*/
void undo_decode_slot_ptr(slot_ptr_t ptr_arg, slot_addr_t *slot_addr) {
  ulint rseg_id;
  slot_ptr_t slot_ptr = ptr_arg;
  ut_ad(slot_addr);

  slot_addr->offset = (ulint)slot_ptr & 0xFFFF;
  slot_ptr >>= SLOT_WIDTH_OFFSET;
  slot_addr->page_no = (ulint)slot_ptr & 0xFFFFFFFF;
  slot_ptr >>= SLOT_WIDTH_PAGE_NO;
  rseg_id = (ulint)slot_ptr & 0x7F;
  slot_ptr >>= SLOT_WIDTH_SPACE_ID;

  /* Confirm the reserved bits */
  ut_ad(((ulint)slot_ptr & 0x3f) == 0);

  if (!slot_addr->is_null() && rseg_id == 0) {
    lizard_ut_ad(slot_addr->is_no_redo());
  }
  /** It's redo txn slot or no_redo special txn slot */
  slot_addr->space_id = trx_rseg_id_to_space_id(rseg_id, false);
}

/**
  Try to lookup the real scn of given records. Address directly to the
  corresponding txn undo header by UBA.

  @param[in/out]  txn_rec       txn info of the records.
  @param[out]     txn_lookup    txn lookup result, nullptr if don't care.
  @param[in]      txn_mtr       txn mtr
  @param[in]      mode          Fetch mode.
  @return         bool          whether corresponding trx is active.
*/
static bool txn_slot_lookup_func(txn_rec_t *txn_rec, txn_lookup_t *txn_lookup,
                                 mtr_t *txn_mtr, Page_fetch mode) {
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
  txn_slot_t txn_slot;
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
  undo_page = trx_undo_page_get_s_latched(page_id, univ_page_size, mtr, mode);

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
  // ut_a(undo_page_start != (TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_HDR_SIZE));
  (void)undo_page_start;

  /** ----------------------------------------------------------*/
  /** Phase 4: judge whether it's txn undo */
  undo_page_type = mach_read_from_2(page_hdr + TRX_UNDO_PAGE_TYPE);

  ut_a(undo_page_type == TRX_UNDO_TXN);

  /** ----------------------------------------------------------*/
  /** Phase 5: check the undo segment state */
  seg_hdr = undo_page + TRX_UNDO_SEG_HDR;
  real_trx_state = mach_read_from_2(seg_hdr + TRX_UNDO_STATE);

  /** real_trx_state should only be the following states */
  ut_a(real_trx_state == TRX_UNDO_ACTIVE || real_trx_state == TRX_UNDO_CACHED ||
       real_trx_state == TRX_UNDO_PREPARED_80028 ||
       real_trx_state == TRX_UNDO_PREPARED ||
       real_trx_state == TRX_UNDO_PREPARED_IN_TC ||
       real_trx_state == TRX_UNDO_TO_PURGE);

  /** ----------------------------------------------------------*/
  /** Phase 6: The offset (minus TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE)
  is a fixed multiple of TRX_UNDO_LOG_HDR_SIZE */
  lizard_ut_ad(undo_addr.offset >= TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE);
  lizard_ut_ad((undo_addr.offset - (TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE)) %
                   TRX_UNDO_LOG_GTID_HDR_SIZE ==
               0);

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
  trx_undo_hdr_read_txn_slot(undo_page, undo_hdr, mtr, &txn_slot);
  if (txn_slot.magic_n != TXN_MAGIC_N) {
    /** The header might be raw */
    lizard_stats.txn_undo_lost_magic_number_wrong.inc();
    goto undo_corrupted;
  }

  /** NOTES: If the extent flag is used, there might be some records's flag
  that is equal to 0, and there also might be other records's flag that's not
  equal to 0 at the same time. */
  // if (txn_slot.ext_storage != 0) {
  //   /** The header might be raw */
  //   lizard_stats.txn_undo_lost_ext_flag_wrong.inc();
  //   goto undo_corrupted;
  // }

  /** ----------------------------------------------------------*/
  /** Phase 9: check the trx_id in txn undo header */
  real_trx_id = txn_slot.trx_id;
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
  if (txn_slot.state == TXN_UNDO_LOG_ACTIVE) {
    lizard_ut_ad(mach_read_from_2(seg_hdr + TRX_UNDO_LAST_LOG) ==
                 undo_addr.offset);
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
  txn_lookup_t_set(txn_lookup, txn_slot, txn_slot.image, txn_status_t::ACTIVE);
  if (!have_mtr) mtr_commit(mtr);
  return true;

already_commit:
  assert_commit_mark_allocated(txn_slot.image);
  txn_rec->scn = txn_slot.image.scn;
  txn_rec->gcn = txn_slot.image.gcn;
  undo_ptr_set_commit(&txn_rec->undo_ptr, txn_slot.image.csr,
                      !txn_slot.maddr.is_null());
  txn_lookup_t_set(txn_lookup, txn_slot, txn_slot.image,
                   txn_status_t::COMMITTED);
  if (!have_mtr) mtr_commit(mtr);
  return false;

undo_purged:
  assert_commit_mark_allocated(txn_slot.image);
  txn_rec->scn = txn_slot.image.scn;
  txn_rec->gcn = txn_slot.image.gcn;
  undo_ptr_set_commit(&txn_rec->undo_ptr, txn_slot.image.csr,
                      !txn_slot.maddr.is_null());
  txn_lookup_t_set(txn_lookup, txn_slot, txn_slot.image, txn_status_t::PURGED);
  if (!have_mtr) mtr_commit(mtr);
  return false;

undo_erased:
  assert_commit_mark_allocated(txn_slot.image);
  txn_rec->scn = txn_slot.image.scn;
  txn_rec->gcn = txn_slot.image.gcn;
  undo_ptr_set_commit(&txn_rec->undo_ptr, txn_slot.image.csr,
                      !txn_slot.maddr.is_null());
  txn_lookup_t_set(txn_lookup, txn_slot, txn_slot.image, txn_status_t::ERASED);
  if (!have_mtr) mtr_commit(mtr);
  return false;

undo_reuse:
  assert_commit_mark_allocated(txn_slot.prev_image);
  txn_rec->scn = txn_slot.prev_image.scn;
  txn_rec->gcn = txn_slot.prev_image.gcn;
  undo_ptr_set_commit(&txn_rec->undo_ptr, txn_slot.prev_image.csr, false);
  txn_lookup_t_set(txn_lookup, txn_slot, txn_slot.prev_image,
                   txn_status_t::REUSE);
  if (!have_mtr) mtr_commit(mtr);
  return false;

undo_corrupted:
  /** Can't never be lost if cleanout_safe_mode isn't taken into
  consideration */
  ut_a(opt_cleanout_safe_mode);
  txn_rec->scn = CMMT_CORRUPTED.scn;
  txn_rec->gcn = CMMT_CORRUPTED.gcn;
  undo_ptr_set_commit(&txn_rec->undo_ptr, CSR_AUTOMATIC, false);
  txn_lookup_t_set(txn_lookup, txn_slot, CMMT_CORRUPTED,
                   txn_status_t::UNDO_CORRUPTED);
  if (!have_mtr) mtr_commit(mtr);
  return false;
}

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
/*
static bool txn_slot_lookup_strict(txn_rec_t *txn_rec) {
  return false;
}
*/
#endif /* UNIV_DEBUG || LIZARD_DEBUG */

/**
  Try to lookup the real scn of given records.

  @param[in/out]  txn_rec       txn info of the records.
  @param[out]     txn_lookup    txn lookup result, nullptr if don't care
  @param[in]      mode          Fetch mode.
  @return         pair          first: whether corresponding trx is active.
                                second: txn slot real status.
*/
std::pair<bool, txn_status_t> txn_slot_lookup_low(txn_rec_t *txn_rec,
                                                  txn_lookup_t *txn_lookup,
                                                  mtr_t *txn_mtr, Page_fetch mode) {
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
      txn_slot_t txn_slot = {
          CMMT_CORRUPTED,
          /** txn_slot.undo_ptr should be from txn undo header, and it
          must be active state when coming here */
          txn_rec->undo_ptr,
          txn_rec->trx_id,
          TXN_MAGIC_N,
          CMMT_CORRUPTED,
          TXN_UNDO_LOG_PURGED,
          0,
          0,
          false,
          PMMT_CORRUPTED,
          {0, 0},
          {0, 0},
      };
      txn_rec->scn = CMMT_CORRUPTED.scn;
      txn_rec->gcn = CMMT_CORRUPTED.gcn;
      undo_ptr_set_commit(&txn_rec->undo_ptr, CSR_AUTOMATIC, false);
      txn_lookup_t_set(txn_lookup, txn_slot, CMMT_CORRUPTED,
                       txn_status_t::UNDO_CORRUPTED);

      lizard_stats.txn_undo_lost_page_miss_when_safe.inc();
      return std::make_pair(false, txn_lookup->real_status);
    }
  }

  ret = txn_slot_lookup_func(txn_rec, txn_lookup, txn_mtr, mode);

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
  /*
  bool strict_ret;
  txn_rec_t txn_strict;
  memcpy(&txn_strict, txn_rec, sizeof(*txn_rec));

  strict_ret = txn_slot_lookup_strict(&txn_strict, expected_id);

  ut_a(ret == strict_ret);
  ut_a(txn_rec->scn == txn_strict.scn);
  ut_a(txn_rec->undo_ptr == txn_strict.undo_ptr);
  */

#endif /* UNIV_DEBUG || LIZARD_DEBUG */
  return std::make_pair(ret, txn_lookup->real_status);
}
/** Add the rseg into the purge queue heap */
void trx_add_rsegs_for_purge(commit_mark_t &cmmt, TxnUndoRsegs *elem) {
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
    elem->insert(redo_rseg);
    has = true;
  }

  if (temp_rseg != NULL && temp_rseg->last_page_no == FIL_NULL) {
    elem->insert(temp_rseg);
    has = true;
  }

  if (txn_rseg != NULL && txn_rseg->last_page_no == FIL_NULL) {
    elem->insert(txn_rseg);
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
      size += rseg->get_curr_size();
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
  m_total_file_size = file_size;

  m_stat_done = true;

  mutex_exit(&m_mutex);
}

/*
  Decide whether to block purge or not based on the current
  undo tablespace size and retention configuration.

  @return     true     if blocking purge
*/
bool Undo_retention::purge_advise(ulint us) {
  ut_a(us != 0);
  ulint utc = (ulint)(us / 1000000);

  /* Retention turned off or stating not done, can not advise */
  if (retention_time == 0 || !m_stat_done) return false;

  ulint used_size = m_total_used_size.load();

  if (space_limit > 0) {
    /* Rule_1: reach space limit, do purge */
    if (used_size > mb_to_pages(space_limit)) return false;
  }

  /* Rule_2: retention time not satisfied, block purge */
  auto cur_utc = current_utc();
  if ((utc + retention_time) > cur_utc) {
    return true;
  }

  /* Rule_3: below reserved size yet, can hold more history data */
  if (space_reserve > 0 && used_size < mb_to_pages(space_reserve)) {
    return true;
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
 * Judge if has gtid when recovery trx.
 *
 * @retval	true
 * @retval	false
 */
bool XA_specification_strategy::has_gtid() {
  auto binlog_xa_spec =
      dynamic_cast<binlog::Binlog_xa_specification *>(m_xa_spec);

  if (binlog_xa_spec && binlog_xa_spec->has_gtid()) {
    return true;
  }

  return false;
}
/**
 * Judge storage way for gtid according to gtid source.
 */
trx_undo_t::Gtid_storage XA_specification_strategy::decide_gtid_storage() {
  trx_undo_t::Gtid_storage storage = trx_undo_t::Gtid_storage::NONE;
  auto binlog_xa_spec =
      dynamic_cast<binlog::Binlog_xa_specification *>(m_xa_spec);
  ut_ad(has_gtid());
  ut_ad(binlog_xa_spec->is_legal_source());

  switch (binlog_xa_spec->source()) {
    case binlog::Binlog_xa_specification::Source::NONE:
      ut_a(0);
      break;
    case binlog::Binlog_xa_specification::Source::COMMIT:
      storage = trx_undo_t::Gtid_storage::COMMIT;
      break;
    case binlog::Binlog_xa_specification::Source::XA_COMMIT_ONE_PHASE:
    case binlog::Binlog_xa_specification::Source::XA_PREPARE:
    case binlog::Binlog_xa_specification::Source::XA_COMMIT:
    case binlog::Binlog_xa_specification::Source::XA_ROLLBACK:
      storage = trx_undo_t::Gtid_storage::PREPARE_AND_COMMIT;
      break;
  }
  return storage;
}

/**
 * Overwrite gtid storage type of trx_undo_t when recovery.
 */
void XA_specification_strategy::overwrite_gtid_storage(trx_t *trx) {
  trx_undo_t *undo{nullptr};
  ut_ad(trx == m_trx);
  ut_ad(has_gtid());

  if (trx->rsegs.m_redo.rseg != nullptr && trx_is_redo_rseg_updated(trx)) {
    undo = trx->rsegs.m_redo.update_undo;
    if (undo) {
      undo->m_gtid_storage = decide_gtid_storage();
    }
  }
}

/** Fill gtid info from xa spec. */
void XA_specification_strategy::get_gtid_info(Gtid_desc &gtid_desc) {
  auto binlog_xa_spec =
      dynamic_cast<binlog::Binlog_xa_specification *>(m_xa_spec);
  ut_ad(has_gtid());

  gtid_desc.m_version = GTID_VERSION;

  auto &gtid = binlog_xa_spec->m_gtid;
  auto &sid = binlog_xa_spec->m_sid;

  gtid_desc.m_info.fill(0);
  auto char_buf = reinterpret_cast<char *>(&gtid_desc.m_info[0]);
  auto len = gtid.to_string(sid, char_buf);
  ut_a((size_t)len <= GTID_INFO_SIZE);
  gtid_desc.m_is_set = true;
}

/**
 * Judge if has gcn when commit detached XA
 *
 * @retval  true
 * @retval  false
 */
bool XA_specification_strategy::has_commit_gcn() const {
  return m_xa_spec && m_xa_spec->gcn().is_cmmt_gcn();
}

bool XA_specification_strategy::has_proposal_gcn() const {
  return m_xa_spec && m_xa_spec->gcn().is_pmmt_gcn();
}

/**
 * Overwrite commit gcn in trx when commit detached XA
 */
void XA_specification_strategy::overwrite_xa_when_commit(trx_t *trx) const {
  ut_ad(has_commit_gcn());

  if (trx_is_started(trx) && trx->rsegs.m_txn.rseg != nullptr &&
      trx_is_txn_rseg_updated(trx)) {
    ut_ad(trx->txn_desc.cmmt.is_null());

    MyGCN xa_gcn = m_xa_spec->gcn();
    xa_addr_t xa_maddr = m_xa_spec->xa_maddr();

    decide_xa_when_commit(trx, &xa_gcn, &xa_maddr);

    trx->txn_desc.copy_xa_when_commit(xa_gcn, xa_maddr);
  }
}

void XA_specification_strategy::overwrite_xa_when_prepare(trx_t *trx) const {
  ut_ad(has_proposal_gcn());

  if (trx_is_started(trx) && trx->rsegs.m_txn.rseg != nullptr &&
      trx_is_txn_rseg_updated(trx)) {
    ut_ad(trx->txn_desc.pmmt.is_null());

    MyGCN xa_gcn = m_xa_spec->gcn();
    const xa_branch_t xa_branch = m_xa_spec->xa_branch();

    decide_xa_when_prepare(&xa_gcn);

    ut_ad(has_proposal_gcn());
    trx->txn_desc.copy_xa_when_prepare(xa_gcn, xa_branch);
  }
}

Guard_xa_specification::Guard_xa_specification(trx_t *trx,
                                               XA_specification *xa_spec,
                                               bool prepare)
    : m_trx(trx), m_xa_spec(xa_spec) {
  ut_ad(trx);

  trx->xa_spec = m_xa_spec;
  XA_specification_strategy xss(trx);

  if (xss.has_gtid()) {
    xss.overwrite_gtid_storage(trx);
  }

  if (prepare && xss.has_proposal_gcn()) {
    xss.overwrite_xa_when_prepare(trx);
  }

  if (!prepare && xss.has_commit_gcn()) {
    xss.overwrite_xa_when_commit(trx);
  }
}

Guard_xa_specification::~Guard_xa_specification() { m_trx->xa_spec = nullptr; }

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
  txn_lookup_t txn_lookup;
  txn_status_t txn_status;

  /** If record is not active, return false directly. */
  if (!undo_ptr_is_active(txn_rec->undo_ptr)) {
    lizard_ut_ad(txn_rec->scn > 0 && txn_rec->scn <= SCN_MAX);
    lizard_ut_ad(txn_rec->gcn > 0 && txn_rec->gcn <= GCN_MAX);
    return false;
  }

  /** Search tcn cache */
  cache_hit = trx_search_tcn(txn_rec, &txn_status);
  if (cache_hit) {
    ut_ad(!undo_ptr_is_active(txn_rec->undo_ptr));
    lizard_ut_ad(txn_rec->scn > 0 && txn_rec->scn <= SCN_MAX);
    lizard_ut_ad(txn_rec->gcn > 0 && txn_rec->gcn <= GCN_MAX);

    /** Collect record to cleanout later. */
    scan_cleanout_collect(txn_rec->trx_id, *txn_rec, rec, index, offsets, pcur);

    return false;
  }

  ut_ad(cache_hit == false);

  std::tie(active, txn_status) =
      txn_slot_lookup_low(txn_rec, &txn_lookup, nullptr);
  if (active) {
    return active;
  } else {
    ut_ad(!undo_ptr_is_active(txn_rec->undo_ptr));
    lizard_ut_ad(txn_rec->scn > 0 && txn_rec->scn <= SCN_MAX);
    lizard_ut_ad(txn_rec->gcn > 0 && txn_rec->gcn <= GCN_MAX);

    /** Collect record to cleanout later.*/
    scan_cleanout_collect(txn_rec->trx_id, *txn_rec, rec, index, offsets, pcur);

    /** Cache txn info into tcn. */
    trx_cache_tcn(*txn_rec, txn_status);

    return false;
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

  /** Must be non-active. */
  txn_slot_lookup_low(txn_rec, &txn_lookup, nullptr);

  ut_a(!txn_lookup.txn_slot.maddr.is_null());

  const auto &master = txn_lookup.txn_slot.maddr;

  /** Pretend a un-cleanout record. */
  ref_txn_rec->trx_id = master.tid;
  ref_txn_rec->undo_ptr = master.slot_ptr;
  ref_txn_rec->gcn = GCN_NULL;
  ref_txn_rec->scn = SCN_NULL;

  ut_a(undo_ptr_is_active(ref_txn_rec->undo_ptr));

  active = txn_rec_real_state_by_lookup(ref_txn_rec, &ref_txn_status, nullptr);
  switch (ref_txn_status) {
    case txn_status_t::ACTIVE:
      ut_ad(active);
      ut_ad(!undo_ptr_is_slave(ref_txn_rec->undo_ptr));
      break;
    case txn_status_t::COMMITTED:
    case txn_status_t::PURGED:
    case txn_status_t::ERASED:
      ut_ad(!active);

      if (undo_ptr_is_slave(ref_txn_rec->undo_ptr)) {
        lizard_error(ER_LIZARD)
            << "There should be only one master branch in a XA GROUP.";
        /** Reset slave info to skip infinite recursion when decision
        visibility. */
        undo_ptr_set_commit(&ref_txn_rec->undo_ptr, ref_txn_rec->csr(), false);
      }

      if (txn_rec->gcn != ref_txn_rec->gcn) {
        lizard_error(ER_LIZARD) << "Transactions in a group should have only "
                                   "one external commit number.";
      }

      break;
    case txn_status_t::REUSE:
    case txn_status_t::UNDO_CORRUPTED:
      ut_ad(!active);
      ut_ad(!undo_ptr_is_slave(ref_txn_rec->undo_ptr));
      break;
  }

  return active;
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

/**
 * prepare_in_tc is treated as first phase commit within 2PC, so we should
 * mark our transaction system like trx_commit_mark, but only proposal xa
 * transaction will really do something.
 * */
proposal_mark_t trx_prepare_mark(trx_t *trx, trx_undo_t *undo,
                                 trx_ulogf_t *log_hdr, mtr_t *mtr) {
  xa_branch_t branch;
  proposal_mark_t pmmt;
  ut_ad(undo->pmmt.is_null());

  /** Only write proposal info in TXN. */
  if (!(undo->flag & TRX_UNDO_FLAG_TXN)) {
    return pmmt;
  }

  /** Write proposal mark. */
  pmmt = trx->txn_desc.pmmt;
  branch = trx->txn_desc.branch;
  if (pmmt.is_null()) {
    return pmmt;
  }

  /** Can't be NULL. See Sql_cmd_xa_proc_ac_prepare. */
  ut_a(!branch.is_null());

  /** Generate proposal gcn */
  pmmt = gcs->new_prepare(trx, mtr);

  ut_ad(undo->tags_allocated());
  /** 1. Set async commit flag. */
  undo->allocate_ac_prepare();
  mlog_write_ulint(log_hdr + TXN_UNDO_LOG_EXT_STORAGE, undo->xes_storage,
                   MLOG_1BYTE, mtr);

  /** 2. Write proposal mark. */
  undo->pmmt = pmmt;
  mlog_write_ull(log_hdr + TXN_UNDO_LOG_XES_AC_PROPOSAL_GCN, pmmt.gcn, mtr);

  if (pmmt.csr == CSR_ASSIGNED) {
    undo->set_ac_csr_assigned_on_tags();
  } else {
    ut_a(!undo->ac_csr_assigned_on_tags());
  }
  mlog_write_ulint(log_hdr + TXN_UNDO_LOG_XES_TAGS, undo->tags, MLOG_2BYTES,
                   mtr);

  undo->branch = branch;
  txn_undo_hdr_write_xa_branch(log_hdr, branch, mtr);

  return pmmt;
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
      LIZARD_MONITOR_INC_TXN_CACHED(1);
    }
  } else {
    ut_ad(!(flag & TRX_UNDO_FLAG_TXN));
  }
}

/** Iterate all undo log header
 *
 * @param[in]		undo header page
 * @param[in]		page size
 * @param[in]		start log header or nullptr
 * @param[in]		function
 * @param[in]		reverse or not
 * */
template <typename Functor>
void trx_undo_log_iterate(const page_t *undo_page, const page_size_t &page_size,
                          const trx_ulogf_t *log_hdr, mtr_t *mtr, Functor F,
                          bool reverse = false) {
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
    return;
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
    F(start);
    if (reverse) {
      next = mach_read_from_2(start + TRX_UNDO_PREV_LOG);
    } else {
      next = mach_read_from_2(start + TRX_UNDO_NEXT_LOG);
    }
    start = next == 0 ? nullptr : undo_page + next;
  }
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
 * @param[in]		index	clust index
 * @param[in]		trx	transaction context
 * @param[in/out]	undo	update undo
 * @param[in/out]	mtr */
void trx_undo_set_2pp_at_report(const dict_index_t *index, trx_t *trx,
                                      trx_undo_t *update_undo,
                                      bool is_2pp) {
  trx_undo_t *txn_undo = nullptr;
  trx_rseg_t *txn_rseg = nullptr;
  trx_rseg_t *redo_rseg = nullptr;
  mtr_t mtr;
  ut_ad(trx && update_undo);
  ut_ad(index && index->is_clustered() && index->table);

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

  txn_rseg->unlatch();
  redo_rseg->unlatch();
  mtr.commit();
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
    trx_undo_log_iterate(undo_page, page_size, nullptr, mtr,
                         [&counter, &mtr](const trx_ulogf_t *log_hdr) -> void {
                           if (trx_undo_log_is_2pp(log_hdr, mtr)) {
                             counter++;
                           }
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

/**
  Check if the TXN is purged or erased. The latch of the TXN page will be held
  if precheck failed.

  @param[in/out]  txn_rec         txn_info of record
  @param[in]      flashback_area  true if it's a flashback area query
  @param[in]      txn_mtr         txn mtr

  @retval         true if txn has been purged (non flashback area) or
                  erased (flashback area)
*/
bool txn_undo_is_missing_history(txn_rec_t *txn_rec, bool flashback_area,
                                 mtr_t *txn_mtr, Page_fetch mode) {
  txn_lookup_t txn_lookup;

  DBUG_EXECUTE_IF("simulate_prev_image_purged_during_query",
                  return true;);

  /** precheck, if the record has been cleanout, and the TXN has been purged,
  no need to hold TXN page latch and undo page latch */
  if (flashback_area) {
    if (precheck_if_txn_is_erased(txn_rec)) {
      /** Must be cleanout, so no need to lookup again */
      ut_ad(!undo_ptr_is_active(txn_rec->undo_ptr));
      return true;
    }
  } else {
    if (precheck_if_txn_is_purged(txn_rec)) {
      /** Must be cleanout, so no need to lookup again */
      ut_ad(!undo_ptr_is_active(txn_rec->undo_ptr));
      return true;
    }
  }

  /** precheck fail, then lookup by reading txn. */
  txn_rec_lock_state_by_lookup(txn_rec, &txn_lookup, txn_mtr, mode);

  return !txn_lookup_rollptr_is_valid(&txn_lookup, flashback_area);
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

  undo_page = trx_undo_page_get_s_latched(page_id_t(rseg->space_id, addr.page),
                                          rseg->page_size, &mtr);

  log_hdr = undo_page + addr.boffset;
  cmmt = trx_undo_hdr_read_cmmt(log_hdr, &mtr);

  rseg->unlatch(false);
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

  mtr_start(&mtr);
  rseg->latch();

  cmmt = txn_free_get_last_log(rseg, addr, &mtr, stat);

  rseg->unlatch(false);
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
