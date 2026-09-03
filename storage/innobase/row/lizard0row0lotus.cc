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

/** @file lizard0row0lotus.cc
 Lotus layout txn field operation.
 Row operations for the non-unique secondary index with tid column.

 Created 2025-07-03 by Ting Yuan
 *******************************************************/

#include "lizard0row0lotus.h"
#include "lizard0dict0dict.h"
#include "lock0lock.h"

namespace lizard {

bool hippo_mvcc_enabled = true;
bool hippo_lock_enabled = true;
bool hippo_purge_enabled = true;

/**
  Get the lotus offset by type
  @param[in]      index
  @param[in]      type
  @param[in]      offsets
  @return offset
*/
static inline ulint row_get_lotus_offset(const dict_index_t *index, ulint type,
                                         const Slice *slc) {
  ulint offset = ULINT_UNDEFINED;

  ut_ad(index->is_hippo());
  ut_ad(slc->is_positioned(index));

  offset = row_get_trx_id_offset(index, slc);

  if (type == DATA_TRX_ID) {
    return offset;
  } else {
    ut_a(0);
    return ULINT_UNDEFINED;
  }
}

/**
  Write lotus layout fields of the physical record.
  @param[in,out]  rec       record
  @param[in,out]  page_zip  compressed page, or NULL
  @param[in]      index     index
  @param[in]      offsets   rec_get_offsets(rec, index)
  @param[in]      trx_id    transaction id
*/
void row_upd_rec_lotus_fields_low(Slice *slc, page_zip_des_t *page_zip,
                                  const dict_index_t *index, trx_id_t trx_id,
                                  bool during_import) {
  ut_ad(!index->table->is_intrinsic());
  ut_ad(!index->is_clustered() && index->is_hippo());
  ut_ad(slc->validate());

  if (page_zip) {
    /** Hippo index doesn't support compressed page. */
    ut_a(false);
  } else {
    ulint offset = row_get_lotus_offset(index, DATA_TRX_ID, slc);

    /* During IMPORT the tid in the record can be in the future, if the
    .ibd file is being imported from another instance. Skip the sanity
    check in that case, mirroring row_upd_rec_sys_fields() which tolerates
    the ROLL_PTR_IMPORT case for the clustered/panda layouts. */
    ut_ad(during_import ||
          lock_check_trx_id_sanity(trx_read_trx_id(slc->rec() + offset), slc,
                                   index));

    mach_write_to_6(slc->rec() + offset, trx_id);
  }
}

/**
  Update the trx id field in a lotus layout index entry.
  @param[in]      index     index
  @param[in,out]  entry     index entry
  @param[in]      trx_id    transaction id
*/
void row_upd_index_entry_lotus_field(const dict_index_t *index, dtuple_t *entry,
                                     trx_id_t trx_id) {
  ut_ad(index->is_hippo());
  ut_ad(index->n_s_tfields == 1);

  ulint pos = index->get_sys_col_pos(DATA_TRX_ID);
  dfield_t *dfield = dtuple_get_nth_field(entry, pos);
  byte *field = static_cast<byte *>(dfield_get_data(dfield));
  mach_write_to_6(field, trx_id);
}

/**
  Get the lotus tid from a lotus layout record.
  @param[in]      rec       record
  @param[in]      index     hippo index
  @param[in]      offsets   rec_get_offsets(rec, index)
  @return tid
*/
trx_id_t row_get_rec_lotus_tid(const Slice *slc, const dict_index_t *index) {
  ulint offset;

  ut_ad(index->is_hippo());
  ut_ad(slc->is_positioned(index));

  ulint len;
  offset = slc->get_nth_sys_field_offs(DATA_TRX_ID, ULINT_UNDEFINED, &len);

  ut_ad(len == DATA_TRX_ID_LEN);

  return (mach_read_from_6(slc->rec() + offset));
}

/**
  Write redo log to the buffer about updates of lotus layout fields.
  @param[in]      index     index of the record
  @param[in]      trx_id    transaction id
  @param[in]      log_ptr   pointer to a buffer opened in mlog

  @return new pointer to mlog
*/
byte *row_upd_write_lotus_vals_to_log(const dict_index_t *index,
                                      trx_id_t trx_id, byte *log_ptr) {
  ut_ad(index->is_hippo());
  ut_ad(!index->is_clustered() && !index->is_panda());

  log_ptr +=
      mach_write_compressed(log_ptr, index->get_sys_col_pos(DATA_TRX_ID));

  trx_write_roll_ptr(log_ptr, 0);
  log_ptr += DATA_ROLL_PTR_LEN;

  log_ptr += mach_u64_write_compressed(log_ptr, trx_id);

  return (log_ptr);
}

/**
  Parses the log data of lotus field values.
  @param[in]      ptr       buffer
  @param[in]      end_ptr   buffer end
  @param[out]     pos       TRX_ID position in record
  @param[out]     trx_id    trx id
  @return log data end or NULL
*/
byte *row_upd_parse_lotus_vals(const byte *ptr, const byte *end_ptr, ulint *pos,
                               trx_id_t *trx_id) {
  *pos = mach_parse_compressed(&ptr, end_ptr);

  if (ptr == nullptr) {
    return (nullptr);
  }

  *trx_id = mach_u64_parse_compressed(&ptr, end_ptr);

  return (const_cast<byte *>(ptr));
}

/** Updates the lotus layout fields in a record in database recovery.
@param[in,out] rec Record
@param[in,out] page_zip Compressed page, or null
@param[in] offsets Array returned by rec_get_offsets()
@param[in] pos Trx_id position in rec
@param[in] trx_id Transaction id */
void row_upd_rec_lotus_fields_in_recovery(Slice *slc, page_zip_des_t *page_zip,
                                          ulint pos, trx_id_t trx_id) {
  ut_ad(slc->validate());

  if (page_zip) {
    /** Hippo index doesn't support compressed page. */
    ut_a(false);
  } else {
    byte *field;
    ulint len;

    field = slc->get_nth_field(nullptr, pos, &len);
    ut_ad(len == DATA_TRX_ID_LEN);
    mach_write_to_6(field, trx_id);
  }
}

/**
 * Handle trx_id field for hippo secondary index during rollback.
 *
 * For hippo secondary indexes that include a trx_id system column, we need
 * special handling during rollback operations. Since the undo log does not
 * contain the correct previous transaction ID for secondary index records,
 * we set the trx_id field to 0 as a special placeholder value.
 *
 * Important: This function clones the dfield before modifying it. This is
 * necessary because dtuple_t entries for different secondary indexes can be
 * shallow copies sharing the same underlying data buffer. Without cloning,
 * modifying one entry would corrupt data for other indexes.
 *
 * @param[in]     index   The secondary index being processed
 * @param[in,out] entry   The dtuple entry to modify
 * @param[in]     heap    Memory heap for allocation
 */
void row_upd_index_entry_lotus_field_in_rollback(const dict_index_t *index,
                                                 dtuple_t *entry,
                                                 mem_heap_t *heap) {
  ut_ad(index && !index->is_clustered());
  if (index->is_hippo()) {
    ut_ad(index->n_s_tfields == 1);
    const ulint pos = index->get_sys_col_pos(DATA_TRX_ID);
    dfield_t *dfield = dtuple_get_nth_field(entry, pos);
    dfield_copy(dfield, dfield->clone(heap));
    byte *field_ptr = static_cast<byte *>(dfield_get_data(dfield));

    /* During a secondary index rollback, the correct previous transaction ID
     * is not available. We write a special value as a placeholder. */
    mach_write_to_6(field_ptr, 0);
  }
}
}  // namespace lizard
