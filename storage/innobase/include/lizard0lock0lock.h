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
/** @file include/lizard0lock0lock.h
 *
 * Lizard transaction lock system

 Created 2024-09-23 by Ting Yuan
 *******************************************************/

#ifndef lizard0lock0lock_h
#define lizard0lock0lock_h

#include "lock0lock.h"

namespace lizard {

/** Checks if some transaction has an implicit x-lock on a record.
 @param[in]   rec       user record
 @param[in]   index     index w/ transaction info
 @param[in]   offsets   rec_get_offsets(rec, index)
 @return transaction which has the x-lock, or nullptr if there is none;
 The caller must confirm all positive results by checking if the trx
 is still active. */
void lock_clust_or_panda_rec_some_has_impl(const rec_t *rec,
                                           const dict_index_t *index,
                                           const ulint *offsets,
                                           txn_rec_t *txn_rec);

/** Checks if some transaction has an implicit x-lock on a record.
 @param[in]   rec       user record
 @param[in]   index     index w/ transaction info
 @param[in]   offsets   rec_get_offsets(rec, index)
 @return transaction which has the x-lock, or nullptr if there is none;
 The caller must confirm all positive results by checking if the trx
 is still active. */
trx_id_t lock_clust_or_panda_rec_some_has_impl(const rec_t *rec,
                                               const dict_index_t *index,
                                               const ulint *offsets);

/**
 Checks whether an index record with transactional information can be seen in a
 consistent read.
 @param[in]   rec       user record with transactional information
 @param[in]   index     index with transactional information
 @param[in]   offsets   rec_get_offsets(rec, index)
 @param[in]   pcur      current pcursor that define position, used in cleanout
 @param[in]   vision    consistent read view
 @return true if sees, or false if an earlier version of the index record needed
 */
bool lock_clust_or_panda_rec_cons_read_sees(const rec_t *rec,
                                            dict_index_t *index,
                                            const ulint *offsets,
                                            btr_pcur_t *pcur,
                                            lizard::Vision *vision);

}  // namespace lizard
#endif
