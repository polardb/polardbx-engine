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

/** @file include/lizard0trx0rec0bamboo.h
 bamboo layout transaction undo log record.

 Created 2024-10-31 by Yichang Song
 *******************************************************/

#ifndef lizard0trx0rec0bamboo_h
#define lizard0trx0rec0bamboo_h

#include "lizard0txn.h"

#include "row0upd.h"

namespace lizard {
/**
  Read bamboo layout (scn/undo_ptr/gcn) from undo record
  @param[in]      ptr       undo record
  @param[out]     txn_info  txn info
  @retval begin of the left undo data.
*/
extern byte *trx_undo_update_rec_get_bamboo_cols(const byte *ptr,
                                                 txn_info_t *txn_info);

/** Write Bamboo layout transactional columns into undo record.
 * @param[in/out]  ptr             pointer to undo record
 * @param[in]      index           index handler
 * @param[in]      rec             record
 * @param[in]      offsets         offsets
 * @return         pointer to current position
 */
byte *trx_undo_update_rec_write_bamboo_cols(byte *ptr,
                                            const dict_index_t *index,
                                            const rec_t *rec,
                                            const ulint *offsets);
/**
  Write bamboo layout field(scn/undo_ptr) into the update vector
  @param[in]      index       index object
  @param[in]      update      update vector
  @param[in]      field_nth   the nth from SCN id field
  @param[in]      txn_info    txn information
  @param[in]      heap        memory heap
*/
void trx_undo_update_rec_by_bamboo_fields(const dict_index_t *index,
                                          upd_t *update, ulint field_nth,
                                          txn_info_t txn_info,
                                          mem_heap_t *heap);
}  // namespace lizard

#endif
