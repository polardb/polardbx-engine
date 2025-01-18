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

/** @file trx/lizard0trx0rec0clover.cc
 Clover layout transaction undo log record.

 Created 2024-10-31 by Yichang Song
 *******************************************************/
#include "row0upd.h"
#include "trx0rec.h"

#include "lizard0trx0rec.h"
#include "lizard0trx0rec0clover.h"
#include "lizard0undo.h"

namespace lizard {
/**
  Read Clover layout (scn/undo_ptr/gcn) from undo record
  @param[in]      ptr       undo record
  @param[out]     txn_info  txn info
  @retval begin of the left undo data.
*/
byte *trx_undo_update_rec_get_clover_cols(const byte *ptr,
                                          txn_info_t *txn_info) {
  txn_info->scn = mach_u64_read_next_compressed(&ptr);
  txn_info->undo_ptr = mach_u64_read_next_compressed(&ptr);
  txn_info->gcn = mach_u64_read_next_compressed(&ptr);

  return const_cast<byte *>(ptr);
}

/** Write clover layout transactional columns into undo record.
 * @param[in/out]  ptr             pointer to undo record
 * @param[in]      index           index handler
 * @param[in]      rec             record
 * @param[in]      offsets         offsets
 * @return         pointer to current position
 */
byte *trx_undo_update_rec_write_clover_cols(byte *ptr,
                                            const dict_index_t *index,
                                            const rec_t *rec,
                                            const ulint *offsets) {
  ut_ad(index->is_clustered());
  ut_ad(!index->table->is_intrinsic());
  const byte *field;
  ulint flen;

  /* Write scn. */
  field = rec_get_nth_field(nullptr, rec, offsets,
                            index->get_sys_col_pos(DATA_SCN_ID), &flen);
  ut_ad(flen == DATA_SCN_ID_LEN);
  ptr += mach_u64_write_compressed(ptr, lizard::trx_read_scn(field));

  /* Write uba. */
  field = rec_get_nth_field(nullptr, rec, offsets,
                            index->get_sys_col_pos(DATA_UNDO_PTR), &flen);
  ut_ad(flen == DATA_UNDO_PTR_LEN);
  ptr += mach_u64_write_compressed(ptr, lizard::trx_read_undo_ptr(field));

  /* Write gcn. */
  field = rec_get_nth_field(nullptr, rec, offsets,
                            index->get_sys_col_pos(DATA_GCN_ID), &flen);
  ut_ad(flen == DATA_GCN_ID_LEN);
  ptr += mach_u64_write_compressed(ptr, lizard::trx_read_gcn(field));

  return ptr;
}

/**
  Write Clover layout fields(scn/undo_ptr/gcn) into the update vector
  @param[in]      index       index object
  @param[in]      update      update vector
  @param[in]      field_nth   the nth from SCN id field
  @param[in]      txn_info    txn information
  @param[in]      heap        memory heap
*/
void trx_undo_update_rec_by_clover_fields(const dict_index_t *index,
                                          upd_t *update, ulint field_nth,
                                          txn_info_t txn_info,
                                          mem_heap_t *heap) {
  byte *buf;
  upd_field_t *upd_field;
  ut_ad(update && heap);

  upd_field = upd_get_nth_field(update, field_nth);
  buf = static_cast<byte *>(mem_heap_alloc(heap, DATA_SCN_ID_LEN));
  trx_write_scn(buf, txn_info.scn);
  upd_field_set_field_no(upd_field, index->get_sys_col_pos(DATA_SCN_ID), index);
  dfield_set_data(&(upd_field->new_val), buf, DATA_SCN_ID_LEN);

  upd_field = upd_get_nth_field(update, field_nth + 1);
  buf = static_cast<byte *>(mem_heap_alloc(heap, DATA_UNDO_PTR_LEN));
  trx_write_undo_ptr(buf, txn_info.undo_ptr);
  upd_field_set_field_no(upd_field, index->get_sys_col_pos(DATA_UNDO_PTR),
                         index);
  dfield_set_data(&(upd_field->new_val), buf, DATA_UNDO_PTR_LEN);

  upd_field = upd_get_nth_field(update, field_nth + 2);
  buf = static_cast<byte *>(mem_heap_alloc(heap, DATA_GCN_ID_LEN));
  trx_write_gcn(buf, txn_info.gcn);
  upd_field_set_field_no(upd_field, index->get_sys_col_pos(DATA_GCN_ID), index);
  dfield_set_data(&(upd_field->new_val), buf, DATA_GCN_ID_LEN);
}

}  // namespace lizard
