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

/** @file trx/lizard0txn0slot0undo.cc
  Lizard transactional slot undo log

 Created 2025-07-14 by Jianwei.zhao
 *******************************************************/

#include "handler/ha_innodb.h"
#include "trx0rseg.h"
#include "trx0undo.h"

#include "lizard0cleanout0safe.h"
#include "lizard0gcs.h"
#include "lizard0trx0cycle.h"
#include "lizard0txn0slot.h"
#include "lizard0txn0slot0undo.h"
#include "lizard0undo.h"
#include "lizard0undo0retent.h"

void trx_undo_read_xid(const trx_ulogf_t *log_hdr, XID *xid);

void trx_undo_write_xid(trx_ulogf_t *log_hdr, const XID *xid, mtr_t *mtr);
trx_undo_t *trx_undo_reuse_cached(trx_t *trx, trx_rseg_t *rseg, ulint type,
                                  trx_id_t trx_id, const XID *xid,
                                  trx_undo_t::Gtid_storage gtid_storage,
                                  mtr_t *mtr);
dberr_t trx_undo_create(trx_t *trx, trx_rseg_t *rseg, ulint type,
                        trx_id_t trx_id, const XID *xid,
                        trx_undo_t::Gtid_storage gtid_storage,
                        trx_undo_t **undo, mtr_t *mtr);
void trx_resurrect_update_in_prepared_state(trx_t *trx, const trx_undo_t *undo);
/** Adds space for the XA XID after an undo log old-style header.
@param[in,out]	undo_page	undo log segment header page
@param[in,out]	log_hdr		undo log header
@param[in,out]	mtr		mini transaction
@param[in]	gtid_storage    GTID storage type */
void trx_undo_header_add_space_for_xid(page_t *undo_page, trx_ulogf_t *log_hdr,
                                       mtr_t *mtr,
                                       trx_undo_t::Gtid_storage gtid_storage);
void trx_undo_page_init(page_t *undo_page, ulint type, mtr_t *mtr);
ulint trx_undo_header_create(page_t *undo_page, trx_id_t trx_id,
                             commit_mark_t *prev_image, mtr_t *mtr);

namespace lizard {

/**
  Lizard transaction system undo format:

  At the end of undo log header history node:

  8 bytes     SCN number
  8 bytes     UTC time
  8 bytes     UBA address
  8 bytes     GCN

  Those three options will be included into all INSERT/UPDATE/TXN undo
  log header.


  Start from undo log old header, txn_undo will be different with trx_undo:

  1) txn undo : flag + reserved space

  2) trx undo : XA + GTID

  As the optional info, those will be controlled by TRX_UNDO_FLAGS.

     0x01 TRX_UNDO_FLAG_XID
     0x02 TRX_UNDO_FLAG_GTID
     0x80 TRX_UNDO_FLAG_TXN


  Attention:
    The UBA in undo log header only demonstrate the address, the state within
  UBA always is committed,  didn't use state to judge the transaction state.
*/

/** Those will only exist in txn undo log header*/
/*-------------------------------------------------------------*/
/* Random magic number */
#define TXN_SLOT_EXT_MAGIC (TRX_UNDO_LOG_XA_HDR_SIZE)
/* Previous scn of the trx who used the same TXN */
#define TXN_SLOT_PREV_SCN (TXN_SLOT_EXT_MAGIC + 4)
/* Previous utc of the trx who used the same TXN */
#define TXN_SLOT_PREV_UTC (TXN_SLOT_PREV_SCN + 8)
/* Previous gcn of the trx who used the same TXN */
#define TXN_SLOT_PREV_GCN (TXN_SLOT_PREV_UTC + 8)
/* Undo log state */
#define TXN_SLOT_STATE (TXN_SLOT_PREV_GCN + 8)
/*--------------------------------------------------------*/
/* Transaction Slot extention storage format (XES)
 *
 * Name               Bytes
 * -------------      -----
 * Storage Format     1 (Bits: [TAG | AC_PMMT | AC_CMMT.....])
 * Tag                2 (Bits: [ROLLBACK | CSR_ASSIGNED.....])
 * AC                 ...
 *   PROPOSAL_GCN     8
 *   BRANCH           2
 *   LOCAL BRANCH     2
 *   MASTER TRX ID    8
 * UNUSED             12
 * */
/*--------------------------------------------------------*/
/* Flag how to use reserved space */
#define TXN_SLOT_EXT_STORAGE (TXN_SLOT_STATE + 2)

/*--------------------------------------------------------*/
// TXN Extention Storage (Tags)
/*--------------------------------------------------------*/
/*--------------------------------------------------------*/
/* New tags. */
#define TXN_SLOT_XES_TAGS (TXN_SLOT_EXT_STORAGE + 1)
/*--------------------------------------------------------*/

/*--------------------------------------------------------*/
// TXN Extention Storage (Async Commit)
/*--------------------------------------------------------*/
/* Proposal GCN. */
#define TXN_SLOT_XES_AC_PROPOSAL_GCN (TXN_SLOT_XES_TAGS + 2)
/* The count of a global transaction branchs */
#define TXN_SLOT_XES_AC_N_GLOBALS (TXN_SLOT_XES_AC_PROPOSAL_GCN + 8)
/* The count of a global transaction branchs in a node. */
#define TXN_SLOT_XES_AC_N_LOCALS (TXN_SLOT_XES_AC_N_GLOBALS + 2)
/* Master branch trx_id.*/
#define TXN_SLOT_XES_AC_MASTER_TID (TXN_SLOT_XES_AC_N_LOCALS + 2)
/** Master branch UBA. Reused TRX_UNDO_SLOT, and it can only be represented when
master_trx_id is not empty.*/
#define TXN_SLOT_XES_AC_MASTER_SLOT_PTR TRX_UNDO_SLOT
/*--------------------------------------------------------*/

/* Unused space */
#define TXN_SLOT_XES_RESERVED (TXN_SLOT_XES_AC_MASTER_TID + 8)
/* Unused space size */
#define TXN_SLOT_XES_RESERVED_LEN 12
/* txn undo log header size */
#define TXN_SLOT_EXT_HDR_SIZE \
  (TXN_SLOT_XES_RESERVED + TXN_SLOT_XES_RESERVED_LEN)
/*-------------------------------------------------------------*/
static_assert(TXN_SLOT_EXT_HDR_SIZE == TRX_UNDO_LOG_GTID_HDR_SIZE,
              "txn and trx undo log header size must be equal!");

/** Pls reuse the reserved space */
static_assert(TXN_SLOT_EXT_HDR_SIZE == 275,
              "txn undo log header size cann't change!");

ulint srv_txn_cached_list_keep_size = 0;

/** The max percent of txn undo page that can be reused */
ulint txn_slot_page_reuse_max_percent = TXN_SLOT_PAGE_REUSE_MAX_PCT_DEF;

/** txn slot header fixed size */
ulint txn_slot_hdr_fix_size() { return TXN_SLOT_EXT_HDR_SIZE; }

/**
  Read the scn, utc, gcn from prev image.

  @param[in]      log_hdr       undo log header
  @param[in]      mtr           current mtr context */
static commit_mark_t txn_slot_hdr_read_prev_cmmt(const trx_ulogf_t *log_hdr,
                                                 mtr_t *mtr) {
  /** Here must hold the S/SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr,
      MTR_MEMO_PAGE_S_FIX | MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));

  /** Validate the undo page */
  ut_ad(trx_undo_page_validate(page_align(log_hdr)));

  commit_mark_t cmmt;
  UTC utc;

  cmmt.scn = mach_read_from_8(log_hdr + TXN_SLOT_PREV_SCN);
  cmmt.gcn = mach_read_from_8(log_hdr + TXN_SLOT_PREV_GCN);

  utc.decode(mach_read_from_8(log_hdr + TXN_SLOT_PREV_UTC));
  std::tie(cmmt.us, cmmt.csr) = utc.get();

  return cmmt;
}

/** Read proposal mark information from txn undo log header.
 *
 * @param[in]		log header pointer
 * @param[in]		mini transaction
 *
 * @retval		proposal mark */
proposal_mark_t txn_slot_hdr_read_pmmt(const trx_ulogf_t *log_hdr, mtr_t *mtr) {
  proposal_mark_t pmmt;
  /** Here must hold the S/SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr,
      MTR_MEMO_PAGE_S_FIX | MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));

  pmmt.gcn = mach_read_from_8(log_hdr + TXN_SLOT_XES_AC_PROPOSAL_GCN);
  pmmt.csr = xes_tags_t(mtr_read_ulint(log_hdr + TXN_SLOT_XES_TAGS,
                                       MLOG_2BYTES, mtr))
                 .csr();
  return pmmt;
}

/** Read state information from txn undo log header.
 *
 * @param[in]		log header pointer
 * @param[in]		mini transaction
 *
 * @retval		state */
ulint txn_slot_hdr_read_state(const trx_ulogf_t *log_hdr) {
  return mach_read_from_2(log_hdr + TXN_SLOT_STATE);
}

/** Read branch information from txn undo log header.
 *
 * @param[in]		log header pointer
 * @param[in]		mini transaction
 *
 * @retval		branch */
xa_branch_t txn_slot_hdr_read_xa_branch(const trx_ulogf_t *log_hdr,
                                        mtr_t *mtr) {
  xa_branch_t branch;
  /** Here must hold the S/SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr,
      MTR_MEMO_PAGE_S_FIX | MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));

  branch.n_global =
      mtr_read_ulint(log_hdr + TXN_SLOT_XES_AC_N_GLOBALS, MLOG_2BYTES, mtr);
  branch.n_local =
      mtr_read_ulint(log_hdr + TXN_SLOT_XES_AC_N_LOCALS, MLOG_2BYTES, mtr);

  return branch;
}

/** Read master information from txn undo log header.
 *
 * @param[in]		log header pointer
 * @param[in]		mini transaction
 *
 * @retval		master*/
xa_addr_t txn_slot_hdr_read_xa_master(const trx_ulogf_t *log_hdr, mtr_t *mtr) {
  xa_addr_t addr;
  /** Here must hold the S/SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr,
      MTR_MEMO_PAGE_S_FIX | MTR_MEMO_PAGE_X_FIX | MTR_MEMO_PAGE_SX_FIX));

  addr.tid = mach_read_from_8(log_hdr + TXN_SLOT_XES_AC_MASTER_TID);
  addr.slot_ptr =
      mach_read_from_8(log_hdr + TXN_SLOT_XES_AC_MASTER_SLOT_PTR);

  return addr;
}

/**
  Read the txn undo log header extension information.

  @param[in]      undo page
  @param[in]      undo log header
  @param[in]      mtr
  @param[out]     txn_slot */
void txn_slot_hdr_read_txn_slot(const page_t *undo_page,
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
      mtr_read_ulint(undo_header + TXN_SLOT_EXT_MAGIC, MLOG_4BYTES, mtr);

  txn_slot->prev_image = txn_slot_hdr_read_prev_cmmt(undo_header, mtr);

  txn_slot->state =
      mtr_read_ulint(undo_header + TXN_SLOT_STATE, MLOG_2BYTES, mtr);

  txn_slot->xes_storage =
      mtr_read_ulint(undo_header + TXN_SLOT_EXT_STORAGE, MLOG_1BYTE, mtr);

  ut_ad(txn_slot->tags == 0);

  if (txn_slot->tags_allocated()) {
    txn_slot->tags =
        mtr_read_ulint(undo_header + TXN_SLOT_XES_TAGS, MLOG_2BYTES, mtr);

    if (txn_slot->state == txn_slot_t::ACTIVE) {
      ut_ad(!txn_slot->is_rollback());
    }
  }

  if (txn_slot->ac_prepare_allocated()) {
    txn_slot->pmmt = txn_slot_hdr_read_pmmt(undo_header, mtr);
    txn_slot->branch = txn_slot_hdr_read_xa_branch(undo_header, mtr);
  }

  if (txn_slot->ac_commit_allocated()) {
    txn_slot->maddr = txn_slot_hdr_read_xa_master(undo_header, mtr);
    ut_ad(!txn_slot->maddr.is_null());
  }
  ut_ad(txn_slot->magic_n == txn_slot_t::MAGIC_N);
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
bool txn_slot_hdr_read_txn_slot_by_xid(const XID *xid, const page_t *undo_page,
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
    txn_slot_hdr_read_txn_slot(undo_page, log_hdr, mtr, txn_slot);
    found = true;
  }
  return found;
}

/** Set txn slot undo log state.
@param[in,out]  log_hdr undo log header
@param[in,out]  mtr     mini transaction
@param[in]      state	  state */
static void txn_slot_hdr_write_state(trx_ulogf_t *log_hdr, ulint state,
                                     mtr_t *mtr) {
  /* When creating a new page, hold SX latch, otherwise X latch */
  ut_ad(mtr_memo_contains_page(mtr, page_align(log_hdr),
                               MTR_MEMO_PAGE_SX_FIX | MTR_MEMO_PAGE_X_FIX));

#if defined UNIV_DEBUG
  auto page = page_align(log_hdr);
  auto type = mach_read_from_2(page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE);
  auto flag = mach_read_from_1(log_hdr + TRX_UNDO_FLAGS);
  ulint old_state = mach_read_from_2(log_hdr + TXN_SLOT_STATE);

  ut_a(type == TRX_UNDO_TXN);

  /* It must be TXN undo log, or be initializing */
  ut_a(state == txn_slot_t::ACTIVE || (flag & TRX_UNDO_FLAG_TXN) != 0);

  /** If adding new state, take care of the switch(state) like
  trx_search_history_by_xid. */
  ut_a(state == txn_slot_t::ACTIVE || state == txn_slot_t::COMMITED ||
       state == txn_slot_t::PURGED || state == txn_slot_t::ERASED);

  if (state == txn_slot_t::COMMITED)
    ut_a(old_state == txn_slot_t::ACTIVE);
  else if (state == txn_slot_t::PURGED)
    ut_a(old_state == txn_slot_t::COMMITED || /* Normal case */
         old_state == txn_slot_t::PURGED);    /* Crash recovery */
  else if (state == txn_slot_t::ERASED)
    ut_a(old_state == txn_slot_t::PURGED);
  else
    ut_a(state == txn_slot_t::ACTIVE);
#endif

  mlog_write_ulint(log_hdr + TXN_SLOT_STATE, state, MLOG_2BYTES, mtr);
}

/** Write xa branch info.
 *
 * @param[in]	log_hdr		undo log header
 * @param[in]	branch		xa branch info
 * @param[in]	mtr		current mtr context  */
void txn_slot_hdr_write_xa_branch(trx_ulogf_t *log_hdr,
                                  const xa_branch_t &branch, mtr_t *mtr) {
  /** Here must hold the SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr, MTR_MEMO_PAGE_SX_FIX | MTR_MEMO_PAGE_X_FIX));

  /** Validate the undo page */
  ut_ad(trx_undo_page_validate(page_align(log_hdr)));
  ut_ad(branch.n_local <= branch.n_global);

  mlog_write_ulint(log_hdr + TXN_SLOT_XES_AC_N_GLOBALS, branch.n_global,
                   MLOG_2BYTES, mtr);
  mlog_write_ulint(log_hdr + TXN_SLOT_XES_AC_N_LOCALS, branch.n_local,
                   MLOG_2BYTES, mtr);
}

/** Write xa master info.
 *
 * @param[in]	log_hdr		undo log header
 * @param[in]	master		xa master info
 * @param[in]	mtr		current mtr context  */
void txn_slot_hdr_write_xa_master(trx_ulogf_t *log_hdr, const xa_addr_t &maddr,
                                  mtr_t *mtr) {
  /** Here must hold the SX/X lock on the page */
  ut_ad(mtr_memo_contains_page_flagged(
      mtr, log_hdr, MTR_MEMO_PAGE_SX_FIX | MTR_MEMO_PAGE_X_FIX));

  /** Validate the undo page */
  ut_ad(trx_undo_page_validate(page_align(log_hdr)));

  mlog_write_ull(log_hdr + TXN_SLOT_XES_AC_MASTER_TID, maddr.tid, mtr);

  ut_a(undo_addr_t::decode_is_slot(maddr.slot_ptr));

  mlog_write_ull(log_hdr + TXN_SLOT_XES_AC_MASTER_SLOT_PTR, maddr.slot_ptr,
                 mtr);
}

/** Write xa pmmt info.
 *
 * @param[in]	log_hdr		undo log header
 * @param[in]	pmmt
 * @param[in]	mtr		current mtr context  */
void txn_slot_hdr_write_ac_proposal_gcn(trx_ulogf_t *log_hdr, gcn_t gcn,
                                        mtr_t *mtr) {
  mlog_write_ull(log_hdr + TXN_SLOT_XES_AC_PROPOSAL_GCN, gcn, mtr);
}

/** Write xes info.
 *
 * @param[in]	log_hdr		undo log header
 * @param[in]	xes
 * @param[in]	mtr		current mtr context  */
void txn_slot_hdr_write_ext_storage(trx_ulogf_t *log_hdr,
                                         uint8_t xes_storage, mtr_t *mtr) {
  mlog_write_ulint(log_hdr + TXN_SLOT_EXT_STORAGE, xes_storage, MLOG_1BYTE,
                   mtr);
}

/** Write tags info.
 *
 * @param[in]	log_hdr		undo log header
 * @param[in]	tags
 * @param[in]	mtr		current mtr context  */
void txn_slot_hdr_write_xes_tags(trx_ulogf_t *log_hdr, ulint tags, mtr_t *mtr) {
  mlog_write_ulint(log_hdr + TXN_SLOT_XES_TAGS, tags, MLOG_2BYTES, mtr);
}

/** Set txn undo log state when commiting.
@param[in,out]  log_hdr undo log header
@param[in,out]  mtr     mini transaction */
void txn_slot_hdr_write_state_at_finish(trx_t *trx, trx_ulogf_t *log_hdr,
                                       bool is_rollback, mtr_t *mtr) {
  xa_addr_t maddr;
  auto txn_undo = trx->rsegs.m_txn.txn_undo;
  ut_ad(trx_is_txn_rseg_assigned(trx) && trx_is_txn_rseg_updated(trx));

  maddr = trx->txn_desc.xa_maddr();

  /** 1. Set rollback tag if need */
  if (txn_undo->tags_allocated() && is_rollback) {
    txn_undo->set_rollback_on_tags();
    mlog_write_ulint(log_hdr + TXN_SLOT_XES_TAGS, txn_undo->tags,
                     MLOG_2BYTES, mtr);
  }

  if (!maddr.is_null()) {
    ut_ad(maddr.is_valid());
    ut_ad(maddr.tid != trx->id &&
          maddr.slot_ptr != undo_addr_t::decode_slot(trx->txn_desc.undo_ptr));

    txn_undo->allocate_ac_commit();
    mlog_write_ulint(log_hdr + TXN_SLOT_EXT_STORAGE, txn_undo->xes_storage,
                     MLOG_1BYTE, mtr);

    txn_undo->maddr = maddr;
    txn_slot_hdr_write_xa_master(log_hdr, maddr, mtr);
  }

  /** 3. Set COMMITED state */
  txn_slot_hdr_write_state(log_hdr, txn_slot_t::COMMITED, mtr);
}

/**
  Set TXN_SLOT_STATE as txn_slot_t::ERASED when erase. NOTES:
  1. Can not hold any other undo page latch because no rsegs mutex is held.
  2. Did not hold rseg mutext because only a TXN undo page is modified.

  @params[in]   txn_cursor        TXN cursor
  @params[in]   scn               the corresponding scn
  @params[in]   page_size         TXN undo page size.
*/
void txn_slot_hdr_write_state_at_erase(const txn_cursor_t &txn_cursor, scn_t scn,
                                      const page_size_t &page_size) {
  page_t *undo_page;
  trx_ulogf_t *log_hdr;
  trx_id_t trx_id;
  commit_mark_t cmmt;
  ulint txn_state;
  slot_addr_t txn_addr = txn_cursor.txn_addr;
  ut_a(txn_cursor.trx_id != 0);

  mtr_t mtr;

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

  txn_state = mach_read_from_2(log_hdr + TXN_SLOT_STATE);
  ut_a(txn_state == txn_slot_t::PURGED || txn_state == txn_slot_t::ERASED);

  if (txn_state == txn_slot_t::PURGED) {
    txn_slot_hdr_write_state(log_hdr, txn_slot_t::ERASED, &mtr);
  }

  mtr_commit(&mtr);
}


/**
  Get newest log header in last (oldest) log segment from free list .
  @params[in]   rseg            update undo rollback segment
  @params[out]  log header address of last log
  @params[out]	rollback segment statistics

  @retval	commit mark of last log header
*/
static commit_order_t txn_slot_free_get_last_log(trx_rseg_t *rseg,
                                                 fil_addr_t &addr, mtr_t *mtr,
                                                 komodo::rseg_stat_t *stat) {
  trx_rsegf_t *rseg_hdr;
  page_t *undo_page;
  ulint offset;
  commit_order_t ommt;
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
    return ommt;
  }
  undo_page = trx_undo_page_get(page_id_t(rseg->space_id, addr.page),
                                rseg->page_size, mtr);
  offset = mach_read_from_2(undo_page + TRX_UNDO_SEG_HDR + TRX_UNDO_LAST_LOG);
  addr.boffset = offset;

  if (offset != 0) {
    ommt = trx_undo_hdr_read_cmmt(undo_page + offset, mtr);
  }
  return ommt;
}

static void txn_slot_free_remove_page(trx_rsegf_t *rseg_hdr, page_t *undo_page,
                                      mtr_t *mtr) {
  flst_remove(rseg_hdr + TXN_RSEG_FREE_LIST,
              undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_NODE, mtr);
}

static page_t *txn_slot_free_get_next_page(trx_rseg_t *rseg, mtr_t *mtr) {
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
  txn_slot_free_remove_page(rseg_hdr, undo_page, mtr);

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

/**
  Get newest log header in last (oldest) log segment from free list .
  @params[in]   rseg            update undo rollback segment
  @params[out]  log header address of last log

  @retval	commit mark of last log header
*/
commit_order_t txn_slot_free_get_last_log(trx_rseg_t *rseg, fil_addr_t &addr,
                                          komodo::rseg_stat_t *stat) {
  mtr_t mtr;
  commit_order_t ommt;
  ut_ad(rseg->is_txn);

  mtr_start(&mtr);
  rseg->latch();

  ommt = txn_slot_free_get_last_log(rseg, addr, &mtr, stat);

  rseg->unlatch(false);
  mtr_commit(&mtr);
  return ommt;
}

static page_t *txn_slot_free_fetch_next_page(trx_rseg_t *rseg, mtr_t *mtr) {
  ut_ad(mutex_own(&rseg->mutex));
  /** Only transaction rollback segment have free list */
  ut_ad(rseg->is_txn);

  if (rseg->last_free_ommt.is_null()) {
    ut_d(fil_addr_t addr;);
    ut_ad(txn_slot_free_get_last_log(rseg, addr, mtr, nullptr).is_null());

    return nullptr;
  }

  if (!txn_retention_satisfied(rseg->last_free_ommt.us)) {
    return nullptr;
  }

  return txn_slot_free_get_next_page(rseg, mtr);
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
ulint txn_slot_segment_reuse(trx_rseg_t *rseg, trx_rsegf_t *rseg_header,
                             page_t *undo_page, ulint slot_no, mtr_t *mtr,
                             trx_id_t trx_id, uint8 xes_storage,
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
  trx_useg_reuse(undo_page, rseg->page_size, mtr);

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

  /** The rseg slot is occupied again when reusing the segment, keep the
  counter balanced with the MONITOR_DEC when the slot is freed at commit. */
  MONITOR_INC(MONITOR_NUM_UNDO_SLOT_USED);

  if (trx_id != 0) {
    /** Allocate a txn header for a foreground trx. */
    offset = trx_undo_header_create(undo_page, trx_id, prev_image, mtr);

    trx_undo_header_add_space_for_xid(undo_page, undo_page + offset, mtr,
                                      trx_undo_t::Gtid_storage::NONE);

    txn_slot_hdr_add_space_for_xes(rseg, undo_page, mtr, offset, xes_storage,
                                   slot_addr, prev_image);

    ut_ad(offset == TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE);
    ut_ad(slot_addr->is_redo());
    ut_ad(prev_image->is_alloced());
  } else {
    /** Reset TRX_UNDO_STATE to TRX_UNDO_CACHED for a background task. */
    mlog_write_ulint(seg_hdr + TRX_UNDO_STATE, TRX_UNDO_CACHED, MLOG_2BYTES,
                     mtr);
  }

  return offset;
}

/**
  Add the node to the txn cached list.

  @param[in]    rseg       trx_rseg_t the txn belongs to
  @param[in]    undo_page  txn undo page
  @param[in]    slot_no    slot number
  @param[in]    mtr        mini transaction
*/
static void txn_slot_add_node_to_cached_list(trx_rseg_t *rseg,
                                             page_t *undo_page, ulint slot_no,
                                             mtr_t *mtr) {
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
      txn_slot_segment_reuse(rseg, rseg_hdr, undo_page, slot_no, mtr, 0,
                                  xes_storage_t::v0(), nullptr, nullptr);
  ut_a(offset == 0);

  /** Phase 2: Create a memory object for txn undo. */
  XID xid;
  xid.reset();
  undo_page_no = page_get_page_no(undo_page);
  trx_undo_t *undo = trx_undo_mem_create(rseg, slot_no, TRX_UNDO_TXN, 0, &xid,
                                         undo_page_no, 0, slot_addr_t::null());
  undo->state = TRX_UNDO_CACHED;
  undo->empty = true;

  /** Phase 3: Add the txn undo to cached list. */
  UT_LIST_ADD_LAST(rseg->txn_undo_cached, undo);

  MONITOR_INC(MONITOR_NUM_UNDO_SLOT_CACHED);
  generic_stats.txn_undo_log_recycle.inc();
}

/**
  Prefetch the oldest node from the txn free list to the txn cached list.

  @param[in]        rseg        rollback segment
  @param[in]        hdr_addr    txn log hdr address
*/
static void txn_slot_try_prefetch_to_cached_list(trx_rseg_t *rseg, mtr_t *mtr) {
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
  if ((undo_page = txn_slot_free_fetch_next_page(rseg, mtr)) == nullptr) {
    /** Failed to get the oldest node that satisfied retetion time from the
     * free list. */
    return;
  }

  /** Phase 3 : Prefetch the node to the cached list. */
  ut_ad(undo_page != nullptr);
  txn_slot_add_node_to_cached_list(rseg, undo_page, slot_no, mtr);
}

/**
  Validate txn undo free list node and rseg free list

  @param[in]    rseg_hdr      rollback segment header
  @parma[in]    undo_page     undo page
  @param[in]    mtr           mini transaction
*/
bool txn_slot_free_list_validate(trx_rsegf_t *rseg_hdr, page_t *undo_page,
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

/** Remove an rseg header from the history list.
@param[in,out]  rseg_hdr        Rollback segment header
@param[in]      log_hdr         Undo log segment header
@param[in,out]  mtr             Mini-transaction. */
void txn_slot_history_remove_log_hdr(trx_rseg_t *rseg, trx_rsegf_t *rseg_hdr,
                                     trx_ulogf_t *log_hdr, mtr_t *mtr) {
  ut_ad(rseg->is_txn);
  flst_remove(rseg_hdr + TRX_RSEG_HISTORY, log_hdr + TRX_UNDO_HISTORY_NODE,
              mtr);

  txn_slot_hdr_write_state(log_hdr, txn_slot_t::PURGED, mtr);

  trx_sys->txn_rseg_history_len.fetch_sub(1);
}

/**
  Put the txn undo log segment into free list after purge all.

  @param[in]        rseg        rollback segment
  @param[in]        hdr_addr    txn log hdr address
*/
void txn_slot_move_segment_to_free_list(trx_rseg_t *rseg, fil_addr_t hdr_addr,
                                        mtr_t *mtr) {
  trx_rsegf_t *rseg_hdr;
  trx_ulogf_t *log_hdr;
  trx_usegf_t *seg_hdr;
  page_t *undo_page;
  ulint seg_size;
  ulint hist_size;
  ulint free_size;
  ut_ad(rseg->is_txn);

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
  txn_slot_history_remove_log_hdr(rseg, rseg_hdr, log_hdr, mtr);

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

  ut_ad(txn_slot_free_list_validate(rseg_hdr, undo_page, mtr));

  generic_stats.txn_undo_log_free_list_put.inc();
}

/**
  Move the txn undo log segment into free list, then try to prefetch an
  available undo log segment from the free list into the cached list.
  @param[in]        rseg        rollback segment
  @param[in]        hdr_addr    txn log hdr address
*/
void txn_slot_recycle_segment(trx_rseg_t *rseg, fil_addr_t hdr_addr) {
  mtr_t mtr;

  mtr_start(&mtr);
  mutex_enter(&rseg->mutex);

  txn_slot_move_segment_to_free_list(rseg, hdr_addr, &mtr);

  if (srv_txn_cached_list_keep_size > 0 &&
      rseg->txn_undo_cached.get_length() < srv_txn_cached_list_keep_size) {
    txn_slot_try_prefetch_to_cached_list(rseg, &mtr);
  }

  mutex_exit(&rseg->mutex);
  mtr_commit(&mtr);
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
static dberr_t txn_slot_get_free(trx_t *trx, trx_rseg_t *rseg, ulint type,
                                      trx_id_t trx_id, const XID *xid,
                                      trx_undo_t **undo) {
  page_t *undo_page = nullptr;
  trx_rsegf_t *rseg_header;
  page_no_t page_no;
  ulint offset;
  fil_addr_t node_addr;
  commit_mark_t prev_image = commit_mark_t::lost();
  slot_addr_t slot_addr(SLOT_PTR_NULL);
  uint8 xes_storage = xes_storage_t::v1();

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
  if ((undo_page = txn_slot_free_fetch_next_page(rseg, &mtr)) == nullptr) {
    *undo = nullptr;
    goto func_exit;
  }

  /** Phase 3: Reinit the txn undo log segment header page and create a txn undo
   * header for use. */
  ut_ad(undo_page);
  page_no = page_get_page_no(undo_page);

  offset = txn_slot_segment_reuse(rseg, rseg_header, undo_page, slot_no, &mtr,
                                  trx_id, xes_storage, &slot_addr, &prev_image);

  ut_ad(offset == TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE);
  ut_ad(slot_addr.is_redo());
  ut_ad(prev_image.is_alloced());

  /** Phase 4: Create and init a memory object for txn undo. */
  *undo = trx_undo_mem_create(rseg, slot_no, type, trx_id, xid, page_no, offset,
                              slot_addr, &prev_image);

  (*undo)->xes_storage = xes_storage;
  ut_ad((*undo)->flag == TRX_UNDO_FLAG_TXN);

  ut_ad((*undo)->prev_image.is_alloced());
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
dberr_t txn_slot_assign_undo(trx_t *trx, txn_slot_ptr_t *undo_ptr, ulint type) {
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
    err = txn_slot_get_free(trx, rseg, type, trx->id, trx->xid, &undo);

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
  Add the space for the txn especially.

  @param[in]      undo_page     undo log header page
  @param[in]      log_hdr       undo log hdr
  @param[in]      mtr
*/
static void txn_slot_hdr_add_space_for_xes(page_t *undo_page,
                                           trx_ulogf_t *log_hdr, mtr_t *mtr) {
  trx_upagef_t *page_hdr;
  ulint free;
  ulint new_free;

  page_hdr = undo_page + TRX_UNDO_PAGE_HDR;

  free = mach_read_from_2(page_hdr + TRX_UNDO_PAGE_FREE);

  /* free is now the end offset of the old style undo log header */
  ut_a(free == ((ulint)(log_hdr - undo_page) + TRX_UNDO_LOG_XA_HDR_SIZE));

  new_free = free + (TXN_SLOT_EXT_HDR_SIZE - TRX_UNDO_LOG_XA_HDR_SIZE);

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
static void txn_slot_hdr_init_xes(page_t *undo_page, trx_ulogf_t *log_hdr,
                                  const commit_mark_t &prev_image,
                                  uint8 xes_storage, mtr_t *mtr) {
  ut_ad(mach_read_from_2(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE) ==
        TRX_UNDO_TXN);

  /* Write the magic number */
  mlog_write_ulint(log_hdr + TXN_SLOT_EXT_MAGIC, txn_slot_t::MAGIC_N,
                   MLOG_4BYTES, mtr);

  ut_ad(prev_image.is_alloced());
  /* Write the prev scn */
  mlog_write_ull(log_hdr + TXN_SLOT_PREV_SCN, prev_image.scn, mtr);

  /* Write the prev utc */
  ut_ad(UTC::interpet_csr(prev_image.us) == 0);

  UTC utc(prev_image.us, prev_image.csr);
  mlog_write_ull(log_hdr + TXN_SLOT_PREV_UTC, utc.encode(), mtr);

  /* Write the prev gcn */
  mlog_write_ull(log_hdr + TXN_SLOT_PREV_GCN, prev_image.gcn, mtr);

  /* Write initial state */
  txn_slot_hdr_write_state(log_hdr, txn_slot_t::ACTIVE, mtr);

  if (DBUG_EVALUATE_IF("sim_old_txn_undo_hdr", true, false)) {
    /* TXN old format: Write the txn undo extension flag */
    mlog_write_ulint(log_hdr + TXN_SLOT_EXT_STORAGE, xes_storage_t::v0(),
                     MLOG_1BYTE, mtr);
  } else {
    /* Write the txn undo extension flag */
    mlog_write_ulint(log_hdr + TXN_SLOT_EXT_STORAGE, xes_storage, MLOG_1BYTE,
                     mtr);

    /* Write the txn undo tags_1 */
    mlog_write_ulint(log_hdr + TXN_SLOT_XES_TAGS, 0, MLOG_2BYTES, mtr);
  }

  /** Write the undo flag when create undo log header */
  mlog_write_ulint(log_hdr + TRX_UNDO_FLAGS, TRX_UNDO_FLAG_TXN, MLOG_1BYTE,
                   mtr);
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
void txn_slot_hdr_add_space_for_xes(trx_rseg_t *rseg, page_t *undo_page,
                                    mtr_t *mtr, ulint offset, uint8 xes_storage,
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
  txn_slot_hdr_add_space_for_xes(undo_page, undo_page + offset, mtr);

  /** Init txn extension fields. */
  txn_slot_hdr_init_xes(undo_page, undo_page + offset, *prev_image, xes_storage,
                        mtr);
}

/**
  Initializes the part of TXN for an undo log memory object if it's TXN undo
  log. The memory object is inserted in the appropriate list in the rseg.

  @params[in] rseg        rollback segment memory object
  @params[in] undo        undo log memory object
  @params[in] undo_page   undo log page
  @params[in] undo_header undo log header
  @params[in] type        undo type, TRX_UNDO_TXN or others
  @params[in] flag        undo log flag, read from TRX_UNDO_FLAGS
  @params[in] state       undo log state, TRX_UNDO_CACHED, or others
  @params[in] mtr         mini transaction for write
*/
void txn_slot_mem_init_for_xes(trx_rseg_t *rseg, trx_undo_t *undo,
                               page_t *undo_page, const trx_ulogf_t *log_hdr,
                               ulint type, uint32_t flag, ulint state,
                               mtr_t *mtr) {
  ut_ad(undo->cmmt.is_initial());
  ut_ad(undo->prev_image.is_initial());
  ut_ad(undo->xes_storage == xes_storage_t::v0());
  ut_ad(undo->tags == 0);
  ut_ad(undo->pmmt.is_null());
  ut_ad(undo->branch.is_null());
  ut_ad(undo->maddr.is_null());

  if (type == TRX_UNDO_TXN) {
    ut_ad(flag & TRX_UNDO_FLAG_TXN);
    ut_ad(state != TRX_UNDO_TO_FREE);

#if defined UNIV_DEBUG
    /** Confirm the SLOT is valid in undo log header */
    txn_slot_t txn_slot;
    txn_slot_hdr_read_txn_slot(undo_page, log_hdr, mtr, &txn_slot);
    ut_ad(txn_slot.is_validate());
#endif

    /* 1. Init SCN, GCN, UTC */
    undo->cmmt = trx_undo_hdr_read_cmmt(log_hdr, mtr);
    ut_ad(undo->is_cmmt_validate());

    /* 2. Init prev image. */
    undo->prev_image = txn_slot_hdr_read_prev_cmmt(log_hdr, mtr);
    ut_ad(undo->prev_image.is_alloced());

    /** 3. Init xes_storage */
    undo->xes_storage =
        mtr_read_ulint(log_hdr + TXN_SLOT_EXT_STORAGE, MLOG_1BYTE, mtr);

    /** 4. Init txn_tags_1 */
    if (undo->tags_allocated()) {
      undo->tags =
          mtr_read_ulint(log_hdr + TXN_SLOT_XES_TAGS, MLOG_2BYTES, mtr);
    }

    /** 5. Init async commit related. */
    if (undo->ac_prepare_allocated()) {
      undo->pmmt = txn_slot_hdr_read_pmmt(log_hdr, mtr);
      undo->branch = txn_slot_hdr_read_xa_branch(log_hdr, mtr);

      ut_ad(undo->is_pmmt_validate());
    }

    if (undo->ac_commit_allocated()) {
      undo->maddr = txn_slot_hdr_read_xa_master(log_hdr, mtr);
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

/**
  Get txn undo state at trx finish.

  @param[in]      free_limit       space left on txn undo page
  @return  TRX_UNDO_TO_PURGE or TRX_UNDO_CACHED
*/
ulint txn_slot_decide_state_at_finish(ulint free_limit) {
  // 275 undo record + 100 safty margin.
  // why 100 ? In trx_undo_header_create:
  // ut_a(free + TRX_UNDO_LOG_GTID_XA_HDR_SIZE < UNIV_PAGE_SIZE - 100);
  static const ulint min_reserve = TRX_UNDO_LOG_GTID_XA_HDR_SIZE + 100;

  ulint reuse_limit = txn_slot_page_reuse_max_percent * UNIV_PAGE_SIZE / 100;

  if (free_limit >= reuse_limit) {
    return TRX_UNDO_TO_PURGE;
  } else if (free_limit + min_reserve >= UNIV_PAGE_SIZE) {
    return TRX_UNDO_TO_PURGE;
  } else {
    return TRX_UNDO_CACHED;
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
  @param[in]      mtr */
void txn_slot_cleanup_undo(trx_t *trx, txn_slot_ptr_t *undo_ptr,
                           page_t *undo_page, mtr_t *mtr) {
  trx_rseg_t *rseg;
  trx_undo_t *undo;

  undo = undo_ptr->txn_undo;
  rseg = undo_ptr->rseg;

  ut_ad(mutex_own(&(rseg->mutex)));
  ut_ad(undo->cmmt.is_alloced());

  trx_cycle_add_txn_slot_to_history(trx, undo_ptr, undo_page, mtr);

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

}  // namespace lizard
