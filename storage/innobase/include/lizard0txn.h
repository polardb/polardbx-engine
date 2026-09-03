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

/** @file include/lizard0txn.h
  Lizard transaction management.

 Created 2020-03-27 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0txn_h
#define lizard0txn_h

#include "page0types.h"
#include "log0types.h"

#include "lizard0cmmt0types.h"
#include "lizard0txn0types.h"

struct txn_slot_ptr_t;

namespace lizard {
/**
  Write the scn into the buffer
  @param[in/out]    ptr       buffer
  @param[in]        txn_desc  txn description
*/
void txn_desc_write_scn(byte *ptr, const txn_desc_t *txn_desc);

/**
  Write the UBA into the buffer
  @param[in/out]    ptr       buffer
  @param[in]        txn_desc  txn description
*/
void txn_desc_write_undo_ptr(byte *ptr, const txn_desc_t *txn_desc);

/**
  Write the gcn into the buffer
  @param[in/out]    ptr       buffer
  @param[in]        txn_desc  txn description
*/
void txn_desc_write_gcn(byte *ptr, const txn_desc_t *txn_desc);

/** Get txn undo if allocated. */
trx_undo_t *trx_get_txn_undo(const trx_t *trx);

/**
  Always assign a txn undo log for transaction.

  @param[in]        trx         current transaction

  @return           DB_SUCCESS  Success
*/
dberr_t trx_always_assign_txn_slot(trx_t *trx);

/** Allocate txn undo and return transaction slot address.
 *
 * @param[in]   trx
 * @param[out]  Slot address
 * @param[out]  trx_id
 *
 * @retval  DB_SUCCESS
 * @retval  DB_ERROR
 **/
dberr_t trx_assign_txn_slot(trx_t *trx, slot_ptr_t *slot_ptr, trx_id_t *trx_id);

/**
 * prepare_in_tc is treated as first phase commit within 2PC, so we should
 * mark our transaction system like trx_commit_mark, but only proposal xa
 * transaction will really do something.
 *
 * @param[in]		trx	transaction context
 * @param[in/out]	undo	undo memory structure
 * @param[in]		hdr	undo log hdr
 * @param[in]		mtr
 *
 * @retval		proposal commit mark.
 * */
proposal_mark_t trx_prepare_in_tc_mark(trx_t *trx, trx_undo_t *undo,
                                       trx_ulogf_t *log_hdr, mtr_t *mtr);

/**
  Assign a new commit scn for the transaction when commit

  @param[in]      trx       current transaction
  @param[in/out]  scn_ptr   Commit scn which was generated only once
  @param[in]      undo      txn undo log
  @param[in]      undo page txn undo log header page
  @param[in]      offset    txn undo log header offset
  @param[in]      mtr       mini transaction
  @param[out]     serialised

  @retval         scn       commit scn struture
*/
commit_mark_t trx_commit_mark(trx_t *trx, commit_mark_t *scn_ptr,
                              trx_undo_t *undo, page_t *undo_hdr_page,
                              ulint hdr_offset, bool *serialised, mtr_t *mtr);
/**
   Resurrect txn undo log segment,
   Maybe the trx didn't have m_redo update/insert undo log.
*/
void trx_resurrect_txn(trx_t *trx, trx_undo_t *undo, trx_rseg_t *rseg);

/** Prepares a transaction for given rollback segment. */
lsn_t txn_prepare_low(trx_t *trx, txn_slot_ptr_t *undo_ptr, mtr_t *mtr);
/**
  Always assign transaction rollback segment for trx
  @param[in]      trx
*/
void trx_assign_txn_rseg(trx_t *trx);
/**
  Whether the txn rollback segment has been assigned
  @param[in]      trx
*/
bool trx_is_txn_rseg_assigned(const trx_t *trx);
/**
  Whether the txn undo log has modified.
*/
bool trx_is_txn_rseg_updated(const trx_t *trx);

extern trx_rseg_t *trx_get_txn_rseg(const trx_t *trx);

/**
  Get a TXN rseg by XID.

  @retval     rollback segment
*/
trx_rseg_t *txn_rseg_assign_by_xid(const XID *xid);

/**
  Find transactions slot in the finalized state by XID.

  @param[in]  rseg         The rollseg where the transaction is being looked up.
  @params[in] xid          xid
  @param[out] txn_slot     Corresponding txn undo header

  @retval     true if the corresponding transaction is found, false otherwise.
*/
bool txn_rseg_find_txn_slot_by_xid(trx_rseg_t *rseg, const XID *xid,
                                   txn_slot_t *txn_slot);
/**
  If during an external XA, check whether the mapping relationship between xid
  and rollback segment is as expected.

  @param[in]        trx         current transaction

  @return           true        if success
*/
bool txn_rseg_check_xid_mapping(const XID *xid, const trx_rseg_t *expect_rseg);

}  // namespace lizard

#endif  // lizard0txn_h
