/*****************************************************************************

Copyright (c) 2013, 2021, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file trx/lizard0service.cc

 Partial implemation of sql/lizard/lizard_sevice.h.

 Created 2021-08-10 by Jianwei.zhao
 *******************************************************/

#include "sql/lizard/lizard_service.h"

#include "trx0undo.h"


/** Init xa attributes from txn undo when active or prepare.
 *
 * @param[in]		trx id
 * @param[in]		txn undo if allocate */
void MyXAInfo::init_by_txn_undo(const trx_id_t tid,
                                const trx_undo_t *txn_undo) {
  slot_ptr_t slot_ptr = 0;
  /** Only used for detached xa for now. */
  ut_ad(status == XA_status::DETACHED_PREPARE);
  ut_ad(is_null());

  if (txn_undo) {
    if (!txn_undo->pmmt.is_null()) {
      txn_undo->pmmt.copy_to_gcn(gcn);
      is_proposal = true;
    }
    slot_ptr = txn_undo->slot_addr.encode();
    slot = {tid, slot_ptr};
    branch = txn_undo->branch;
    maddr = txn_undo->maddr;
  } else {
    /** It seems impossible to get here for detached XA, because empty detached
    xa trx will be rollback directly when doing "xa prepare". See
    innodb_replace_trx_in_thd. */
    slot = {tid, 0};
  }
}

/** Init xa attributes from txn slot after transaction finished.
 *
 * @param[in]		txn slot */
void MyXAInfo::init_by_txn_slot(const txn_slot_t *txn_slot) {
  ut_ad(is_null());
  ut_ad(txn_slot);

  status = txn_slot->is_rollback() ? XA_status::ROLLBACK : XA_status::COMMIT;
  /** if TXN_UNDO_LOG_COMMITED or TXN_UNDO_LOG_PURGED, must be
  non proposal. */
  txn_slot->image.copy_to_gcn(gcn);
  is_proposal = false;

  slot = {txn_slot->trx_id, txn_slot->slot_ptr};
  branch = txn_slot->branch;
  maddr = txn_slot->maddr;
}
