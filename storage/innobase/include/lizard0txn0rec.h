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

#include "lizard0cleanout.h"
#include "lizard0txn0rec0types.h"

namespace lizard {

class Vision;
/**
  Determine the real trx state.
  Return whether the trx corresponding to the record is active.

  @param[in/out]  txn_rec   txn record
  @param[in]      hint      cache hint
  @param[in]      ccr       category of commit number combination.

  @retval true    active
          false   committed
*/
extern bool txn_rec_real_state(txn_rec_t *txn_rec, Cache_hint hint, ccr_t ccr);

/** Determine txn slot real transaction state, and fix related block if active.
 *
 * @param[in/out]	txn_rec		txn record
 * @param[in]		fixed txn slot buf block
 * @param[in]		fix or not if active
 *
 * @retval		state and fixed block if active and do_fix.
 * */
extern std::pair<bool, buf_block_t *> txn_rec_is_active(txn_rec_t *txn_rec,
                                                         buf_block_t *block,
                                                         bool do_ref_count);

/**
  Fill the txn_rec and attempt to clean out the record during the query.
  If cleaning is needed, collect the cursor, and it will be cleaned
  out when the query finishes.
  If cleaning is not needed, lookup and fill the txn_rec if necessary.

  @param[in/out]  txn_rec	  txn record
  @param[in]      layout    rec layout
  @param[in]      ccr       category of commit number combination.
  @param[in]	  cctx      cleanout context
*/
extern void txn_rec_execute_when_query(txn_rec_t *txn_rec,
                                       const txn_layout_t &layout, ccr_t ccr,
                                       cleanout_ctx_t &cctx);

/**
  Clean out the record during modification.
  If cleaning is needed, attempt to look up the txn_rec and perform the
  cleanout.

  @param[in]      trx_id    trx_id of the transactions
                            who updates / deletes the record
  @param[in]      rec       record
  @param[in]      index     index
  @param[in]      offsets   rec_get_offsets(rec)
  @param[in]      layout    rec layout
  @param[in/out]  block     buffer block of the record
  @param[in/out]  mtr       mini-transaction
*/
extern void txn_rec_cleanout_when_modify(const trx_id_t trx_id, Slice *slc,
                                         const dict_index_t *index,
                                         const txn_layout_t &layout,
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

/** Optimistic vision see only through trx id, and try to cleanout if hit tcn
 *  cache.
 *
 *  @param[in/out]	txn rec
 *  @param[in]		layout
 *  @param[in]		vision
 *  @param[in/out]	cleanout context
 *
 *  @retval	true	see
 *  @retval	false	not sure
 */
extern bool txn_rec_try_see(txn_rec_t *txn_rec, const txn_layout_t &layout,
                            const Vision *vision, cleanout_ctx_t &cctx);

}  // namespace lizard

#endif
