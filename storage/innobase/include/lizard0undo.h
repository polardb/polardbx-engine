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

#ifndef lizard0undo_h
#define lizard0undo_h

#include "clone0repl.h"
#include "fut0lst.h"
#include "trx0types.h"
#include "trx0undo.h"

#include "lizard0dbg.h"
#include "lizard0erase0types.h"
#include "lizard0scn.h"
#include "lizard0undo0rec0types.h"
#include "lizard0undo0types.h"
#include "lizard0ut.h"

#include "ut0dbg.h"

#include "sql/binlog/binlog_xa_specification.h"
#include "sql/lizard/lizard_service.h"

#include <atomic>
#include <queue>
#include <set>
#include <vector>


struct trx_rseg_t;
struct trx_undo_t;
struct txn_desc_t;

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
#define TXN_UNDO_LOG_EXT_MAGIC (TRX_UNDO_LOG_XA_HDR_SIZE)
/* Previous scn of the trx who used the same TXN */
#define TXN_UNDO_PREV_SCN (TXN_UNDO_LOG_EXT_MAGIC + 4)
/* Previous utc of the trx who used the same TXN */
#define TXN_UNDO_PREV_UTC (TXN_UNDO_PREV_SCN + 8)
/* Previous gcn of the trx who used the same TXN */
#define TXN_UNDO_PREV_GCN (TXN_UNDO_PREV_UTC + 8)
/* Undo log state */
#define TXN_UNDO_LOG_STATE (TXN_UNDO_PREV_GCN + 8)
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
#define TXN_UNDO_LOG_EXT_STORAGE (TXN_UNDO_LOG_STATE + 2)

/*--------------------------------------------------------*/
// TXN Extention Storage (Tags)
/*--------------------------------------------------------*/
/*--------------------------------------------------------*/
/* New tags. */
#define TXN_UNDO_LOG_XES_TAGS (TXN_UNDO_LOG_EXT_STORAGE + 1)
/*--------------------------------------------------------*/

/*--------------------------------------------------------*/
// TXN Extention Storage (Async Commit)
/*--------------------------------------------------------*/
/* Proposal GCN. */
#define TXN_UNDO_LOG_XES_AC_PROPOSAL_GCN (TXN_UNDO_LOG_XES_TAGS + 2)
/* The count of a global transaction branchs */
#define TXN_UNDO_LOG_XES_AC_N_GLOBALS (TXN_UNDO_LOG_XES_AC_PROPOSAL_GCN + 8)
/* The count of a global transaction branchs in a node. */
#define TXN_UNDO_LOG_XES_AC_N_LOCALS (TXN_UNDO_LOG_XES_AC_N_GLOBALS + 2)
/* Master branch trx_id.*/
#define TXN_UNDO_LOG_XES_AC_MASTER_TID (TXN_UNDO_LOG_XES_AC_N_LOCALS + 2)
/** Master branch UBA. Reused TRX_UNDO_SLOT, and it can only be represented when
master_trx_id is not empty.*/
#define TXN_UNDO_LOG_XES_AC_MASTER_SLOT_PTR TRX_UNDO_SLOT
/*--------------------------------------------------------*/

/* Unused space */
#define TXN_UNDO_LOG_XES_RESERVED (TXN_UNDO_LOG_XES_AC_MASTER_TID + 8)
/* Unused space size */
#define TXN_UNDO_LOG_XES_RESERVED_LEN 12
/* txn undo log header size */
#define TXN_UNDO_LOG_EXT_HDR_SIZE \
  (TXN_UNDO_LOG_XES_RESERVED + TXN_UNDO_LOG_XES_RESERVED_LEN)
/*-------------------------------------------------------------*/
static_assert(TXN_UNDO_LOG_EXT_HDR_SIZE == TRX_UNDO_LOG_GTID_HDR_SIZE,
              "txn and trx undo log header size must be equal!");

/** Pls reuse the reserved space */
static_assert(TXN_UNDO_LOG_EXT_HDR_SIZE == 275,
              "txn undo log header size cann't change!");
/** txn magic number */
#define TXN_MAGIC_N 91118498

/* States of an txn undo log header */
#define TXN_UNDO_LOG_ACTIVE 1
#define TXN_UNDO_LOG_COMMITED 2
#define TXN_UNDO_LOG_PURGED 3

/* 2PC Purge done state */
#define TXN_UNDO_LOG_ERASED 4

/*****************************************
 *        TXN_UNDO_LOG_EXT_STORAGE           *
 *****************************************/
/** Empty TXN_UNDO_LOG_EXT_STORAGE */
#define XES_ALLOCATED_NONE 0x00

/** bit_0: TXN have TXN_UNDO_LOG_XES_TAGS. */
#define XES_ALLOCATED_TAGS 0x01
/** bit_1: Prepare info of async commit transaction. */
#define XES_ALLOCATED_AC_PREPARE 0x02
/** bit_2: Commit info of async commit transaction. */
#define XES_ALLOCATED_AC_COMMIT 0x04

/** Initial value of TXN_UNDO_LOG_EXT_STORAGE. */
#define XES_ALLOCATED_V1 XES_ALLOCATED_TAGS

/******************************************
 *           TXN_UNDO_LOG_XES_TAGS       *
 ******************************************/
/** bit_0: Finish state of the transaction (true: ROLLBACK,
false: other state). */
#define XES_TAGS_ROLLBACK 0x01
/**
  bit_1: true if it's a ASSIGNED GCN, false it's a AUTOMATIC GCN. This bit is
  only meaningful when doing async commit, and the state is prepare.

  NOTE:
  1. The commit gcn CSR is in the utc field of cmmt mark.
  2. However, the proposal gcn CSR is in the txn_tags_1.
*/
#define XES_TAGS_AC_ASSIGNED 0x02

/******************************************
 *           txn undo page reuse percent*
 ******************************************/
#define TXN_UNDO_PAGE_REUSE_MAX_PCT_DEF 90
#define TXN_UNDO_PAGE_REUSE_LIMIT (9 * UNIV_PAGE_SIZE / 10)

#define TXN_UNDO_PAGE_REUSE_MAX_PERCENT \
  ((TXN_UNDO_PAGE_REUSE_LIMIT * 100) / UNIV_PAGE_SIZE)

namespace lizard {

/** The max percent of txn undo page that can be reused */
extern ulint txn_undo_page_reuse_max_percent;

/** Max list size of txn_undo_cached of a rsegment. */
extern ulint srv_txn_cached_list_keep_size;

/** Special rollback pointer. */
/** Special simulate space id for roll ptr. */
constexpr ulint ROLL_PTR_SPACE_ID_FAKE = 0;
/** Special simulate page no for roll ptr. */
constexpr ulint ROLL_PTR_PAGE_NO_FAKE = 0x20250303;

constexpr ulint ROLL_PTR_OFFSET_IMPORT = (ulint)0xFFFF - 1;

constexpr roll_ptr_t ROLL_PTR_IMPORT =
    ((roll_ptr_t) true) << 55 | (roll_ptr_t)ROLL_PTR_SPACE_ID_FAKE << 48 |
    (roll_ptr_t)ROLL_PTR_PAGE_NO_FAKE << 16 | ROLL_PTR_OFFSET_IMPORT;

/** ROLL PTR OFFSET: Record generated by DDL. Only panda index for now. */
constexpr ulint ROLL_PTR_OFFSET_SEC_DDL = (ulint)0xFFFF;

constexpr roll_ptr_t ROLL_PTR_SEC_DDL =
    ((roll_ptr_t) true) << 55 | (roll_ptr_t)ROLL_PTR_SPACE_ID_FAKE << 48 |
    (roll_ptr_t)ROLL_PTR_PAGE_NO_FAKE << 16 | ROLL_PTR_OFFSET_SEC_DDL;

/* Lizard transaction undo header operation */
/*-----------------------------------------------------------------------------*/
#if defined UNIV_DEBUG || defined LIZARD_DEBUG
/**
  Validate the page is undo page

  @param[in]      page      undo page
  @return         true      it's undo page
*/
bool trx_undo_page_validate(const page_t *page);

/** Confirm the consistent of scn, undo type, undo state. */
bool undo_commit_mark_validate(const trx_undo_t *undo);

/** Confirm the consistent of proposal mark. */
bool undo_proposal_mark_validate(const trx_undo_t *undo);

bool trx_undo_hdr_slot_validate(const trx_ulogf_t *log_hdr, mtr_t *mtr);

bool trx_undo_hdr_txn_validate(const page_t *undo_page,
                               const trx_ulogf_t *log_hdr, mtr_t *mtr);

bool txn_slot_validate(const txn_slot_t &txn_slot);

/** Check if an update undo log has been marked as purged.
@param[in]  rseg txn rseg
@param[in]  page_size
@return     true   if purged */
bool txn_undo_log_has_purged(const trx_rseg_t *rseg,
                             const page_size_t &page_size);

#endif  // UNIV_DEBUG || LIZARD_DEBUG

/** Confirm the commit mark is committed
 *
 * @param[in]	log hdr
 * @param[in]	mini transaction
 *
 * @retval	true	committed
 * @retval	false	not committed
 * */
extern bool trx_undo_hdr_cmmt_committed(trx_ulogf_t *log_hdr, mtr_t *mtr);

/**
  Get txn undo state at trx finish.

  @param[in]      free_limit       space left on txn undo page
  @return  TRX_UNDO_TO_PURGE or TRX_UNDO_CACHED
*/
extern ulint txn_undo_decide_state_at_finish(ulint free_limit);

/**
  Initial the NULL value on SCN and UTC when create undo log header.
  include all kinds of undo log header type.
  The redo log logic is included in "MLOG_UNDO_HDR_CREATE";

  @param[in]      log_hdr       undo log header
  @param[in]      mtr           current mtr context
*/

extern void trx_undo_hdr_init_cmmt(trx_ulogf_t *log_hdr, mtr_t *mtr);

/**
  Write the scn and utc when commit.
  Include the redo log

  @param[in]      log_hdr       undo log header
  @param[in]      commit_mark    commit scn number
  @param[in]      mtr           current mtr context
*/
extern void trx_undo_hdr_write_cmmt(trx_ulogf_t *log_hdr,
                                    commit_mark_t &cmmt_scn, mtr_t *mtr);

/**
  Read slot address.

  @param[in]      log_hdr       undo log header
  @param[in]      mtr           current mtr context
  @return         decoded slot_addr_t
*/
slot_addr_t trx_undo_hdr_read_slot(const trx_ulogf_t *log_hdr, mtr_t *mtr);

/**
  Write the slot address into undo log header
  @param[in]      undo log header
  @param[in]      slot
  @param[in]      mtr
*/
extern void trx_undo_hdr_write_slot(trx_ulogf_t *log_hdr,
                                    const slot_addr_t &slot_addr, mtr_t *mtr);
/**
  Write the slot address into undo log header
  @param[in]      undo log header
  @param[in]      trx
  @param[in]      mtr
*/
extern slot_addr_t trx_undo_hdr_write_slot(trx_ulogf_t *log_hdr,
                                           const trx_t *trx, mtr_t *mtr);
/**
  Read the scn and utc.

  @param[in]      log_hdr       undo log header
  @param[in]      mtr           current mtr context
*/
extern commit_mark_t trx_undo_hdr_read_cmmt(const trx_ulogf_t *log_hdr,
                                            mtr_t *mtr);

/**
  Check if the undo log header is reused.

  @param[in]      undo_page     undo log header page
  @param[out]     commit_mark    commit scn if have, otherwise 0
  @param[in]      mtr

  @return         bool          ture if the undo log header is reused
*/
bool txn_undo_header_reuse_if_need(const page_t *undo_page,
                                   commit_mark_t *commit_mark, mtr_t *mtr);

/**
  Add the space for the txn especially.

  @param[in]      undo_page     undo log header page
  @param[in]      log_hdr       undo log hdr
  @param[in]      mtr
*/
extern void trx_undo_hdr_add_space_for_txn(page_t *undo_page,
                                           trx_ulogf_t *log_hdr, mtr_t *mtr);

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
                               uint8 xes_storage, mtr_t *mtr);

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
                                       commit_mark_t *prev_image);
/**
  Read the txn undo log header extension information.

  @param[in]      undo page
  @param[in]      undo log hdr
  @param[in]      mtr
  @param[out]     txn_undo_ext
*/
void trx_undo_hdr_read_txn_slot(const page_t *undo_page,
                                const trx_ulogf_t *undo_header, mtr_t *mtr,
                                txn_slot_t *txn_slot);
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
                              txn_slot_t *txn_slot);
/**
  Read the scn, utc, gcn from prev image.

  @param[in]      log_hdr       undo log header
  @param[in]      mtr           current mtr context
*/
commit_mark_t txn_undo_hdr_read_prev_cmmt(const trx_ulogf_t *log_hdr,
                                          mtr_t *mtr);
/**
 * Write xa branch info.
 *
 * @param[in]	log_hdr		undo log header
 * @param[in]	branch		xa branch info
 * @param[in]	mtr		current mtr context
 */
void txn_undo_hdr_write_xa_branch(trx_ulogf_t *log_hdr,
                                  const xa_branch_t &branch, mtr_t *mtr);

/**
  Write the scn into the buffer
  @param[in/out]    ptr       buffer
  @param[in]        txn_desc  txn description
*/
void trx_write_scn(byte *ptr, const txn_desc_t *txn_desc);

/**
  Write the scn into the buffer
  @param[in/out]    ptr     buffer
  @param[in]        scn     scn id
*/
void trx_write_scn(byte *ptr, scn_t scn);

/**
  Write the UBA into the buffer
  @param[in/out]    ptr       buffer
  @param[in]        txn_desc  txn description
*/
void trx_write_undo_ptr(byte *ptr, const txn_desc_t *txn_desc);

/**
  Write the UBA into the buffer
  @param[in/out]    ptr       buffer
  @param[in]        undo_ptr  UBA
*/
void trx_write_undo_ptr(byte *ptr, undo_ptr_t undo_ptr);

/**
  Write the gcn into the buffer
  @param[in/out]    ptr       buffer
  @param[in]        txn_desc  txn description
*/
void trx_write_gcn(byte *ptr, const txn_desc_t *txn_desc);

/**
  Write the gcn into the buffer
  @param[in/out]    ptr     buffer
  @param[in]        scn     scn id
*/
void trx_write_gcn(byte *ptr, gcn_t gcn);

/**
  Read the scn
  @param[in]        ptr       buffer

  @return           scn_t  scn
*/
scn_t trx_read_scn(const byte *ptr);

/**
  Read the UBA
  @param[in]        ptr        buffer

  @return           undo_ptr_t undo_ptr
*/
undo_ptr_t trx_read_undo_ptr(const byte *ptr);

/**
  Read the gcn
  @param[in]        ptr       buffer

  @return           gcn_t  scn
*/
gcn_t trx_read_gcn(const byte *ptr);

/** Get txn undo if allocated. */
trx_undo_t *trx_undo_get_txn(const trx_t *trx);
/**
  Always assign a txn undo log for transaction.

  @param[in]        trx         current transaction

  @return           DB_SUCCESS  Success
*/
dberr_t trx_always_assign_txn_undo(trx_t *trx);

/** Allocate txn undo and return transaction slot address.
 *
 * @param[in]   trx
 * @param[out]  Slot address
 * @param[out]  trx_id
 *
 * @retval  DB_SUCCESS
 * @retval  DB_ERROR
 **/
dberr_t trx_assign_txn_undo(trx_t *trx, slot_ptr_t *slot_ptr, trx_id_t *trx_id);
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
                          ulint n_added_logs, mtr_t *mtr);
/**
  Recycle txn undo log segment
  @param[in]        rseg        rollback segment
  @param[in]        hdr_addr    txn log hdr address
*/
void txn_recycle_segment(trx_rseg_t *rseg, fil_addr_t hdr_addr);

/**
  Put the txn undo log segment into cached list after purge all.
  @param[in]        rseg        rollback segment
  @param[in]        hdr_addr    txn log hdr address
  @retval	    true	Not available slot
  @retval	    false	Success
*/
bool txn_purge_segment_to_cached_list(trx_rseg_t *rseg, fil_addr_t hdr_addr,
                                      mtr_t *mtr);

/**
  Put the txn undo log segment into free list after purge all.

  @param[in]        rseg        rollback segment
  @param[in]        hdr_addr    txn log hdr address
*/
void txn_purge_segment_to_free_list(trx_rseg_t *rseg, fil_addr_t hdr_addr,
                                    mtr_t *mtr);

/** Set txn undo log state.
@param[in,out]  log_hdr undo log header
@param[in,out]  mtr     mini transaction
@param[in]      state	  state */
inline void txn_undo_set_state(trx_ulogf_t *log_hdr, ulint state, mtr_t *mtr) {
  /* When creating a new page, hold SX latch, otherwise X latch */
  ut_ad(mtr_memo_contains_page(mtr, page_align(log_hdr),
                               MTR_MEMO_PAGE_SX_FIX | MTR_MEMO_PAGE_X_FIX));

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
  auto page = page_align(log_hdr);
  auto type = mach_read_from_2(page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE);
  auto flag = mach_read_from_1(log_hdr + TRX_UNDO_FLAGS);
  ulint old_state = mach_read_from_2(log_hdr + TXN_UNDO_LOG_STATE);

  ut_a(type == TRX_UNDO_TXN);

  /* It must be TXN undo log, or be initializing */
  ut_a(state == TXN_UNDO_LOG_ACTIVE || (flag & TRX_UNDO_FLAG_TXN) != 0);

  /** If adding new state, take care of the switch(state) like
  trx_search_history_by_xid. */
  ut_a(state == TXN_UNDO_LOG_ACTIVE || state == TXN_UNDO_LOG_COMMITED ||
       state == TXN_UNDO_LOG_PURGED || state == TXN_UNDO_LOG_ERASED);

  if (state == TXN_UNDO_LOG_COMMITED)
    ut_a(old_state == TXN_UNDO_LOG_ACTIVE);
  else if (state == TXN_UNDO_LOG_PURGED)
    ut_a(old_state == TXN_UNDO_LOG_COMMITED || /* Normal case */
         old_state == TXN_UNDO_LOG_PURGED);    /* Crash recovery */
  else if (state == TXN_UNDO_LOG_ERASED)
    ut_a(old_state == TXN_UNDO_LOG_PURGED);
  else
    ut_a(state == TXN_UNDO_LOG_ACTIVE);
#endif

  mlog_write_ulint(log_hdr + TXN_UNDO_LOG_STATE, state, MLOG_2BYTES, mtr);
}

/** Set txn undo log state to purged.
@param[in]  rseg txn rseg
@param[in]  page_size
@return     The corresponding commit_mark if it's TXN undo,
            or err if it's not TXN undo */
inline std::pair<commit_mark_t, bool> txn_undo_set_state_at_purge(
    const trx_rseg_t *rseg, const page_size_t &page_size) {
  commit_mark_t cmmt;
  if (rseg->is_txn) {
    mtr_t mtr;
    mtr_start(&mtr);

    const page_id_t page_id(rseg->space_id, rseg->last_page_no);
    page_t *undo_page = trx_undo_page_get(page_id, page_size, &mtr);
    trx_ulogf_t *undo_header = undo_page + rseg->last_offset;

    txn_undo_set_state(undo_header, TXN_UNDO_LOG_PURGED, &mtr);

    cmmt = trx_undo_hdr_read_cmmt(undo_header, &mtr);

    mtr_commit(&mtr);

    return std::make_pair(cmmt, false);
  }
  return std::make_pair(cmmt, true);
}

/** Set txn undo log state when commiting.
@param[in,out]  log_hdr undo log header
@param[in,out]  mtr     mini transaction */
extern void txn_undo_set_state_at_finish(trx_t *trx, trx_ulogf_t *log_hdr,
                                         bool is_rollback, mtr_t *mtr);

/** Set txn undo log state when initializing.
@param[in,out]  log_hdr undo log header
@param[in,out]  mtr     mini transaction */
inline void txn_undo_set_state_at_init(trx_ulogf_t *log_hdr, mtr_t *mtr) {
  txn_undo_set_state(log_hdr, TXN_UNDO_LOG_ACTIVE, mtr);
}

/**
  Set TXN_UNDO_LOG_STATE as TXN_UNDO_LOG_ERASED when erase. NOTES:
  1. Can not hold any other undo page latch because no rsegs mutex is held.
  2. Did not hold rseg mutext because only a TXN undo page is modified.

  @params[in]   txn_cursor        TXN cursor
  @params[in]   scn               the corresponding scn
  @params[in]   page_size         TXN undo page size.
*/
extern void txn_undo_set_state_at_erase(const txn_cursor_t &txn_cursor,
                                        scn_t scn,
                                        const page_size_t &page_size);

/** Gets an undo log page whith cache hint and s-latches it.

  @param[in]      page_id         Page id
  @param[in]      page_size       Page size
  @param[in]      hint            Cache hint
  @param[in,out]  mtr             Mini-transaction

  @return pointer to page s-latched */
inline page_t *trx_undo_page_get_s_latched_with_hint(
    const page_id_t &page_id, const page_size_t &page_size, Cache_hint hint,
    mtr_t *mtr) {
  return trx_undo_page_get_s_latched_low(
      page_id, page_size,
      hint == Cache_hint::MAKE_YOUNG ? Page_fetch::NORMAL : Page_fetch::SCAN,
      mtr);
}

/** Gets an undo log page block whith cache hint and s-latches it.

  @param[in]      page_id         Page id
  @param[in]      page_size       Page size
  @param[in]      hint            Cache hint
  @param[in,out]  mtr             Mini-transaction

  @return pointer to page s-latched */
inline buf_block_t *trx_undo_block_get_s_latched_with_hint(
    const page_id_t &page_id, const page_size_t &page_size, Cache_hint hint,
    mtr_t *mtr) {
  return trx_undo_block_get_s_latched_low(
      page_id, page_size,
      hint == Cache_hint::MAKE_YOUNG ? Page_fetch::NORMAL : Page_fetch::SCAN,
      mtr);
}

inline buf_block_t *trx_undo_block_get_s_latched_with_hint_guess(
    const page_id_t &page_id, const page_size_t &page_size, Cache_hint hint,
    mtr_t *mtr) {
  ulint savepoint = 0;
  buf_block_t *block = nullptr;

  savepoint = mtr_set_savepoint(mtr);

  if ((block = buf_page_get_gen(page_id, page_size, RW_NO_LATCH, nullptr,
                                Page_fetch::IGNORE_MISSING_NOWAIT,
                                UT_LOCATION_HERE, mtr)) == nullptr) {
    return nullptr;
  }

  if (!buf_page_get_known_nowait(RW_S_LATCH, block, hint, __FILE__, __LINE__,
                                 true, mtr)) {
    mtr_release_block_at_savepoint(mtr, savepoint, block);
    return nullptr;
  }

  buf_block_dbg_add_level(block, SYNC_TRX_UNDO_PAGE);

  return block;
}

/** Gets an undo log page with cache hint and x-latches it.
  @param[in]      page_id         Page id
  @param[in]      page_size       Page size
  @param[in]      hint            Cache hint.
  @param[in,out]  mtr             Mini-transaction

  @return pointer to page x-latched */
inline page_t *trx_undo_page_get_with_hint(const page_id_t &page_id,
                                           const page_size_t &page_size,
                                           Cache_hint hint, mtr_t *mtr) {
  return trx_undo_page_get_low(
      page_id, page_size,
      hint == Cache_hint::MAKE_YOUNG ? Page_fetch::NORMAL : Page_fetch::SCAN,
      mtr);
}

/**
  Only write XID on the TXN.

  @params[in]     XID         xid info
  @params[in/out] undo        TXN undo
*/
void txn_undo_write_xid(const XID *xid, trx_undo_t *undo);

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
void trx_undo_mem_init_for_txn(trx_rseg_t *rseg, trx_undo_t *undo,
                               page_t *undo_page,
                               const trx_ulogf_t *undo_header, ulint type,
                               uint32_t flag, ulint state, mtr_t *mtr);

/** When report update undo, set 2pp flag if need.
 *
 * @param[in]		index	clust index
 * @param[in]		trx	transaction context
 * @param[in/out]	undo	update undo
 * @param[in/out]	mtr
 * @param[in/out]	is_2pp */
void trx_undo_set_2pp_at_report(const dict_index_t *index, trx_t *trx,
                                      trx_undo_t *update_undo,
                                      bool is_2pp);

/**
  Reads the two-phase commit purge flag in the transaction undo log header
  @param[in]  undo_header     Pointer to the undo log header
  @param[in]  mtr             Mini-transaction
  @return     True if the 2PP flag is set, false otherwise
*/
bool trx_undo_log_is_2pp(const trx_ulogf_t *log_hdr, mtr_t *mtr);

/**
  Check if is two-phase purge flag in the undo log segment tailer.
  @param[in]    undo log header page.
  @param[in]    page size
  @param[in]    mini transaction
*/
bool trx_useg_is_2pp(const page_t *undo_page,
                           const page_size_t &page_size, mtr_t *mtr);

/**
 * Allocate semi-page list when allocate new txn/update undo log segemnt.
 *
 * @param[in/out]	txn undo page
 * @param[in]		page size
 * @param[in/out]	mtr */
void trx_useg_allocate(page_t *undo_page, const page_size_t &page_size,
                       mtr_t *mtr);

/** Verify the trx useg. */
bool trx_useg_verify(page_t *undo_page, const page_size_t &page_size,
                     mtr_t *mtr);
/**
  Get newest log header in last (oldest) log segment from free list .
  @params[in]   rseg            update undo rollback segment
  @params[out]  log header address of last log

  @retval	commit mark of last log header
*/
extern commit_mark_t txn_free_get_last_log(trx_rseg_t *rseg, fil_addr_t &addr,
                                           rseg_stat_t *stat = nullptr);

/** Calculate rsegment status of undo tablespace.
 *
 * @param[in/out]	status array.
 * */
void trx_trunc_status(std::vector<trunc_status_t> &array);

void trx_purge_status(purge_status_t &status);

/** Iterate all txn undo log header according to offset.
 *
 * @param[in]		undo header page
 * @param[in]		mini transaction
 * @param[in]		function
 * */
template <typename Functor>
bool txn_undo_log_iterate_by_offset(const page_t *undo_page, mtr_t *mtr,
                                    Functor F) {
  const trx_ulogf_t *log_hdr = nullptr;
  uint32_t last_offset;
  ut_ad(mtr->memo_contains_page_flagged(undo_page, MTR_MEMO_PAGE_S_FIX |
                                                       MTR_MEMO_PAGE_X_FIX |
                                                       MTR_MEMO_PAGE_SX_FIX));
  last_offset =
      mach_read_from_2(undo_page + TRX_UNDO_SEG_HDR + TRX_UNDO_LAST_LOG);

  /** Iterate over the txn slots on the undo page. */
  for (uint32_t txn_offset = TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE;
       txn_offset <= last_offset; txn_offset += TXN_UNDO_LOG_EXT_HDR_SIZE) {
      /** 1. get the txn header. */
    log_hdr = undo_page + txn_offset;
    if (F(undo_page, log_hdr, mtr)) return true;
  }

  return false;
}

}  // namespace lizard

/** Delcare the functions which were defined in other cc files.*/
/*=============================================================================*/
extern void trx_undo_read_xid(const trx_ulogf_t *log_hdr, XID *xid);

/** Write X/Open XA Transaction Identification (XID) to undo log header */
extern void trx_undo_write_xid(trx_ulogf_t *log_hdr, const XID *xid,
                               mtr_t *mtr);

extern trx_undo_t *trx_undo_reuse_cached(trx_t *trx, trx_rseg_t *rseg,
                                         ulint type, trx_id_t trx_id,
                                         const XID *xid,
                                         trx_undo_t::Gtid_storage gtid_storage,
                                         mtr_t *mtr);

extern dberr_t trx_undo_create(trx_t *trx, trx_rseg_t *rseg, ulint type,
                               trx_id_t trx_id, const XID *xid,
                               trx_undo_t::Gtid_storage gtid_storage,
                               trx_undo_t **undo, mtr_t *mtr);

void trx_resurrect_update_in_prepared_state(trx_t *trx, const trx_undo_t *undo);

void trx_undo_page_init(page_t *undo_page, /*!< in: undo log segment page */
                        ulint type,        /*!< in: undo log segment type */
                        mtr_t *mtr);       /*!< in: mtr */

ulint trx_undo_header_create(page_t *undo_page, /*!< in/out: undo log segment
                                                header page, x-latched; it is
                                                assumed that there is
                                                TRX_UNDO_LOG_HDR_SIZE bytes
                                                free space on it */
                             trx_id_t trx_id,   /*!< in: transaction id */
                             commit_mark_t *prev_image,
                             /*!< out: previous scn/utc
                             if have. Only used in TXN
                             undo header. Pass in as NULL
                             if don't care. */
                             mtr_t *mtr); /*!< in: mtr */

/** Remove an rseg header from the history list.
@param[in,out]	rseg_hdr	rollback segment header
@param[in]	log_hdr		undo log segment header
@param[in,out]	mtr		mini transaction. */
void trx_purge_remove_log_hdr(trx_rsegf_t *rseg_hdr, trx_ulogf_t *log_hdr,
                              mtr_t *mtr);
/** Adds space for the XA XID after an undo log old-style header.
@param[in,out]	undo_page	undo log segment header page
@param[in,out]	log_hdr		undo log header
@param[in,out]	mtr		mini transaction
@param[in]	gtid_storage    GTID storage type */
void trx_undo_header_add_space_for_xid(page_t *undo_page, trx_ulogf_t *log_hdr,
                                       mtr_t *mtr,
                                       trx_undo_t::Gtid_storage gtid_storage);

/*=============================================================================*/

#if defined UNIV_DEBUG || defined LIZARD_DEBUG

#define assert_trx_undo_ptr_initial(trx) \
  assert_undo_ptr_initial((&(trx)->txn_desc.undo_ptr))

#define assert_trx_undo_ptr_allocated(trx) \
  assert_undo_ptr_allocated((trx)->txn_desc.undo_ptr)

#define trx_undo_page_validation(page)          \
  do {                                          \
    ut_a(lizard::trx_undo_page_validate(page)); \
  } while (0)

#define trx_undo_hdr_slot_validation(undo_hdr, mtr)          \
  do {                                                       \
    ut_a(lizard::trx_undo_hdr_slot_validate(undo_hdr, mtr)); \
  } while (0)

#define trx_undo_hdr_txn_validation(undo_page, undo_hdr, mtr)          \
  do {                                                                 \
    ut_a(lizard::trx_undo_hdr_txn_validate(undo_page, undo_hdr, mtr)); \
  } while (0)

#define undo_commit_mark_validation(undo)          \
  do {                                             \
    ut_a(lizard::undo_commit_mark_validate(undo)); \
  } while (0)

#define undo_proposal_mark_validation(undo)          \
  do {                                               \
    ut_a(lizard::undo_proposal_mark_validate(undo)); \
  } while (0)

#define txn_undo_free_list_validation(rseg_hdr, undo_page, mtr)          \
  do {                                                                   \
    ut_a(lizard::txn_undo_free_list_validate(rseg_hdr, undo_page, mtr)); \
  } while (0)
#else

#define trx_undo_page_validation(page)
#define assert_trx_undo_ptr_initial(trx)
#define assert_trx_undo_ptr_allocated(trx)

#define trx_undo_hdr_txn_validation(undo_page, undo_hdr, mtr)
#define undo_commit_mark_validation(undo)
#define undo_proposal_mark_validation(undo)
#define txn_undo_free_list_validation(rseg_hdr, undo_page, mtr)
#define trx_undo_hdr_slot_validation(undo_hdr, mtr)

#endif

#endif  // lizard0undo_h
