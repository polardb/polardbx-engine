/*****************************************************************************

Copyright (c) 2013, 2024, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file include/lizard0txn0rec.h
  Lizard transactional record management.

 Created 2024-10-15 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0txn0rec_h
#define lizard0txn0rec_h

#include "buf0buf.h"

#include "lizard0txn0rec0types.h"

/** Transaction slot or tcn cache lookup result structure. */
struct txn_lookup_t {
 public:
  /**
    Unlike normal UNDOs (insert undo / update undo), there are 5 kinds of states
    of TXN. Among them, Status::ACTIVE, Status::COMMITTED and Status::PURGED
    are specified by TXN_UNDO_LOG_STATE flag (respectively, TXN_UNDO_LOG_ACTIVE,
    TXN_UNDO_LOG_COMMITED and TXN_UNDO_LOG_PURGED) in TXN header. And also, that's
    mean these TXN headers are existing.

    By contrast, Status::REUSE / Status::UNDO_CORRUPTED mean that the TXN
    headers are non-existing.

    * State::ACTIVE: A txn header is initialized as Status::ACTIVE when the
    transaction begins.

    * Status::COMMITTED: The state of txn header is set as Status::COMMITTED
    at the moment that the transaction commits.

    * Status::PURGED: At the moment that the purge sys start purging it. Notes
    that: Access to the binding normal UNDOs (insert undo / update undo) is not
    safe from then on.

    * Status::ERASED: At the moment that the erase sys start erasing it. Notes
    that: Access to the binding normal UNDOs (insert undo / update undo) is not
    safe for two phase purge tables from then on.

    * Status::REUSE: At the moment that the TXN headers are reused by another
    transactions. These TXN headers are reinited as Status::ACTIVE, but for
    those UBAs who also pointed at them, are supposed to be Status::REUSE.

    * Status::UNDO_CORRUPTED: In fact, Status::REUSE also lost their TXN
    headers, but Status::UNDO_CORRUPTED is a abnormal state for some special
    cases, for example, page corrupt or TXN file unexpectedly removed.

    So the life cycle of TXN hedaer:

    Status::ACTIVE (Trx_A) ==> Status::COMMITTED (Trx_A) ==>
      Status::PURGED (Trx_A) ==> { (Status::ERASED) (Trx_A) ==> }
        * Status::REUSE  (from Trx_A's point of view)
        * Status::ACTIVE (from Trx_B's point of view)
  */
  enum Status : char {
    ACTIVE,
    COMMITTED,
    PURGED,
    ERASED,
    REUSE,
    UNDO_CORRUPTED,
  };

 public:
  txn_lookup_t()
      : txn_slot(),
        real_image(),
        real_status(Status::ACTIVE) {}

  /** Initialize elements after lookup. */
  void init(const txn_slot_t &txn_slot_arg, const commit_mark_t &real_image_arg,
            const Status &real_status_arg) {
    txn_slot = txn_slot_arg;
    real_image = real_image_arg;
    real_status = real_status_arg;
#if defined UNIV_DEBUG || defined LIZARD_DEBUG
    if (real_status == Status::ACTIVE || real_status == Status::COMMITTED ||
        real_status == Status::PURGED || real_status == Status::ERASED) {
      if (real_status != Status::ACTIVE) {
        /** TXN reuse, current scn should be larger than prev scn */
        ut_a(txn_slot.image.scn > txn_slot.prev_image.scn);
      }
      ut_a(real_image == txn_slot.image);
    } else if (real_status == Status::REUSE) {
      ut_a(real_image == txn_slot.prev_image);
    }
#endif /* UNIV_DEBUG || LIZARD_DEBUG */
  }

  /** Judge record rollptr is valid through query type and status. */
  bool undo_missing(bool flashback_area) {
    bool valid;
    if (flashback_area && txn_slot.is_2pp) {
      valid = (real_status < Status::ERASED);
    } else {
      valid = (real_status < Status::PURGED);
    }
    return !valid;
  }

  /** Judge uba have been missing. */
  bool txn_missing() {
    if (real_status == Status::REUSE || real_status == Status::UNDO_CORRUPTED)
      return true;

    return false;
  }

  /** The raw data in txn slot. */
  txn_slot_t txn_slot;
  /**
    If the txn is still existing:
      * real_state: [Status::ACTIVE, Status::COMMITTED, Status::PURGED]
      * real_image == txn_slot.image

    If the txn is non-existing:
      * real_state: [Status::REUSE]
      * real_image == txn_slot.prev_image

    If the txn is unexpectedly lost:
      * real_state: [Status::UNDO_CORRUPTED]
      * real_image == {SCN_UNDO_CORRUPTED, US_UNDO_CORRUPTED}
  */
  commit_mark_t real_image;
  Status real_status;
};

typedef txn_lookup_t::Status txn_status_t;

namespace lizard {

/**
  Determine the real trx state.
  Return whether the trx corresponding to the record is active.

  @param[in/out]  txn_rec   txn record
  @param[in]      hint      cache hint
  @param[in]      ccr       category of commit number combination.

  @retval true    active
          false   committed
*/
extern bool txn_rec_real_state(txn_rec_t *txn_rec, Cache_hint hint,
                               ccr_t ccr = ccr_t::CCR_ALL);

/**
  Fill the txn_rec and attempt to clean out the record during the query.
  If cleaning is needed, collect the cursor, and it will be cleaned
  out when the query finishes.
  If cleaning is not needed, lookup and fill the txn_rec if necessary.

  @param[in/out]  txn_rec	  txn record
  @param[in]      pcur      btr_pcur
  @param[in]      rec       record
  @param[in]      index     index
  @param[in]      offsets   rec_get_offsets(rec)
  @param[in]      ccr       category of commit number combination.
*/
extern void txn_rec_execute_when_query(txn_rec_t *txn_rec, btr_pcur_t *pcur,
                                       const rec_t *rec,
                                       const dict_index_t *index,
                                       const ulint *offsets, ccr_t ccr);
/**
  Clean out the record during modification.
  If cleaning is needed, attempt to look up the txn_rec and perform the
  cleanout.

  @param[in]      trx_id    trx_id of the transactions
                            who updates / deletes the record
  @param[in]      rec       record
  @param[in]      index     index
  @param[in]      offsets   rec_get_offsets(rec)
  @param[in/out]  block     buffer block of the record
  @param[in/out]  mtr       mini-transaction
*/
extern void txn_rec_cleanout_when_modify(const trx_id_t trx_id, rec_t *rec,
                                         const dict_index_t *index,
                                         const ulint *offsets,
                                         const buf_block_t *block, mtr_t *mtr);

/**
  Lookup the referenced transaction state.
  1) Lookup the TXN of **txn_rec**, so get the master_uba, master_trx_id
  2) Search the real trx state of the master transaction.

  @param[in/out]  txn record
  @param[out]     referenced (master) transaction txn record.

  @retval true    active
          false   committed
*/
extern bool txn_rec_get_master_by_lookup(txn_rec_t *txn_rec,
                                         txn_rec_t *ref_txn_rec);

/**
  Check if the TXN is purged or erased. The latch of the TXN page will be held
  if precheck failed.

  @param[in/out]  txn_rec         txn_info of record
  @param[in]      flashback_area  true if it's a flashback area query
  @param[in]      txn_mtr         txn mtr

  @retval         true if txn has been purged (non flashback area) or
                  erased (flashback area)
*/
extern bool txn_rec_is_missing_history(txn_rec_t *txn_rec, bool flashback_area,
                                       mtr_t *txn_mtr);

}  // namespace lizard

#endif
