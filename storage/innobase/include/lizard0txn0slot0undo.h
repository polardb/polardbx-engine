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

/** @file include/lizard0txn0slot0undo.h
  Lizard transactional slot undo log

 Created 2025-07-14 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0txn0slot0undo_h
#define lizard0txn0slot0undo_h

#include "sql/handler.h"

#include "page0types.h"
#include "trx0types.h"
#include "fil0fil.h"

#include "lizard0txn0slot0types.h"
#include "lizard0trx0erase0types.h"

struct trx_rseg_t;
struct trx_undo_t;

/** The struct of transaction undo for UBA */
struct txn_slot_ptr_t {
  // XID will be actively and explicitly initialized
  txn_slot_ptr_t() : rseg(nullptr), txn_undo(nullptr), xid_for_hash() {}
  /** Rollback segment in txn space */
  trx_rseg_t *rseg;
  /* transaction undo log segment */
  trx_undo_t *txn_undo;
  /** XID that is used to map rseg, and also will be persisted in TXN undo */
  XID xid_for_hash;
};

namespace lizard {

/******************************************
 *           txn slot page reuse percent*
 ******************************************/
#define TXN_SLOT_PAGE_REUSE_MAX_PCT_DEF 90
#define TXN_SLOT_PAGE_REUSE_LIMIT (9 * UNIV_PAGE_SIZE / 10)

#define TXN_SLOT_PAGE_REUSE_MAX_PERCENT \
  ((TXN_SLOT_PAGE_REUSE_LIMIT * 100) / UNIV_PAGE_SIZE)

/** The max percent of txn undo page that can be reused */
extern ulint txn_slot_page_reuse_max_percent;

/** Max list size of txn_undo_cached of a rsegment. */
extern ulint srv_txn_cached_list_keep_size;

ulint txn_slot_hdr_fix_size();
/** Read proposal mark information from txn undo log header.
 *
 * @param[in]		log header pointer
 * @param[in]		mini transaction
 *
 * @retval		proposal mark */
proposal_mark_t txn_slot_hdr_read_pmmt(const trx_ulogf_t *log_hdr, mtr_t *mtr);
/** Read branch information from txn undo log header.
 *
 * @param[in]		log header pointer
 * @param[in]		mini transaction
 *
 * @retval		branch */
xa_branch_t txn_slot_hdr_read_xa_branch(const trx_ulogf_t *log_hdr, mtr_t *mtr);
/** Read master information from txn undo log header.
 *
 * @param[in]		log header pointer
 * @param[in]		mini transaction
 *
 * @retval		master*/
xa_addr_t txn_slot_hdr_read_xa_master(const trx_ulogf_t *log_hdr, mtr_t *mtr);
/** Read state information from txn undo log header.
 *
 * @param[in]		log header pointer
 * @param[in]		mini transaction
 *
 * @retval		state */
ulint txn_slot_hdr_read_state(const trx_ulogf_t *log_hdr);
/** Write xa branch info.
 *
 * @param[in]	log_hdr		undo log header
 * @param[in]	branch		xa branch info
 * @param[in]	mtr		current mtr context  */
void txn_slot_hdr_write_xa_branch(trx_ulogf_t *log_hdr,
                                  const xa_branch_t &branch, mtr_t *mtr);
/** Write xa master info.
 *
 * @param[in]	log_hdr		undo log header
 * @param[in]	master		xa master info
 * @param[in]	mtr		current mtr context  */
void txn_slot_hdr_write_xa_master(trx_ulogf_t *log_hdr, const xa_addr_t &maddr,
                                  mtr_t *mtr);
/** Write xa pmmt info.
 *
 * @param[in]	log_hdr		undo log header
 * @param[in]	pmmt
 * @param[in]	mtr		current mtr context  */
void txn_slot_hdr_write_ac_proposal_gcn(trx_ulogf_t *log_hdr, gcn_t gcn,
                                        mtr_t *mtr);
/** Write xes info.
 *
 * @param[in]	log_hdr		undo log header
 * @param[in]	xes
 * @param[in]	mtr		current mtr context  */
void txn_slot_hdr_write_ext_storage(trx_ulogf_t *log_hdr, uint8_t xes_storage,
                                    mtr_t *mtr);
/** Write tags info.
 *
 * @param[in]	log_hdr		undo log header
 * @param[in]	tags
 * @param[in]	mtr		current mtr context  */
void txn_slot_hdr_write_xes_tags(trx_ulogf_t *log_hdr, ulint tags, mtr_t *mtr);

/** Set txn undo log state when commiting.
@param[in,out]  log_hdr undo log header
@param[in,out]  mtr     mini transaction */
void txn_slot_hdr_write_state_at_finish(trx_t *trx, trx_ulogf_t *log_hdr,
                                        bool is_rollback, mtr_t *mtr);
/**
  Set TXN_SLOT_STATE as txn_slot_t::ERASED when erase. NOTES:
  1. Can not hold any other undo page latch because no rsegs mutex is held.
  2. Did not hold rseg mutext because only a TXN undo page is modified.

  @params[in]   txn_cursor        TXN cursor
  @params[in]   scn               the corresponding scn
  @params[in]   page_size         TXN undo page size.
*/
void txn_slot_hdr_write_state_at_erase(const txn_cursor_t &txn_cursor,
                                       scn_t scn, const page_size_t &page_size);

/**
  Read the txn undo log header extension information.

  @param[in]      undo page
  @param[in]      undo log hdr
  @param[in]      mtr
  @param[out]     txn_undo_ext
*/
void txn_slot_hdr_read_txn_slot(const page_t *undo_page,
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
bool txn_slot_hdr_read_txn_slot_by_xid(const XID *xid, const page_t *undo_page,
                                       const trx_ulogf_t *log_hdr, mtr_t *mtr,
                                       txn_slot_t *txn_slot);
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
                                    commit_mark_t *prev_image);
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
dberr_t txn_slot_assign_undo(trx_t *trx, txn_slot_ptr_t *undo_ptr, ulint type);

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
                               page_t *undo_page,
                               const trx_ulogf_t *undo_header, ulint type,
                               uint32_t flag, ulint state, mtr_t *mtr);
/**
  Get txn undo state at trx finish.

  @param[in]      free_limit       space left on txn undo page
  @return  TRX_UNDO_TO_PURGE or TRX_UNDO_CACHED
*/
ulint txn_slot_decide_state_at_finish(ulint free_limit);
/**
  Cleanup txn undo log segment when commit,

  It will :
    1) Add the UBA header into rseg->history
    2) Reinit the rseg->slot as FIL_NULL
    3) Destroy or reuse the undo mem object

  @param[in]      trx         trx owning the txn undo log
  @param[in/out]  undo_ptr    txn undo log structure
  @param[in]      undo_page   txn undo log header page, x-latched
  @param[in]      mtr
*/
void txn_slot_cleanup_undo(trx_t *trx, txn_slot_ptr_t *undo_ptr,
                           page_t *undo_page, mtr_t *mtr);
/**
  Recycle txn undo log segment
  @param[in]        rseg        rollback segment
  @param[in]        hdr_addr    txn log hdr address
*/
void txn_slot_recycle_segment(trx_rseg_t *rseg, fil_addr_t hdr_addr);

/** Remove an rseg header from the history list.
@param[in,out]  rseg_hdr        Rollback segment header
@param[in]      log_hdr         Undo log segment header
@param[in,out]  mtr             Mini-transaction. */
void txn_slot_history_remove_log_hdr(trx_rseg_t *rseg, trx_rsegf_t *rseg_hdr,
                                     trx_ulogf_t *log_hdr, mtr_t *mtr);
/**
  Put the txn undo log segment into free list after purge all.

  @param[in]        rseg        rollback segment
  @param[in]        hdr_addr    txn log hdr address
*/
void txn_slot_move_segment_to_free_list(trx_rseg_t *rseg, fil_addr_t hdr_addr,
                                        mtr_t *mtr);
/**
  Get newest log header in last (oldest) log segment from free list .
  @params[in]   rseg            update undo rollback segment
  @params[out]  log header address of last log

  @retval	commit mark of last log header
*/
commit_order_t txn_slot_free_get_last_log(trx_rseg_t *rseg, fil_addr_t &addr,
                                         komodo::rseg_stat_t *stat = nullptr);

}  // namespace lizard

#endif
