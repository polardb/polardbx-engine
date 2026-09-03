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

/** @file include/lizard0row0lotus.h
 Lotus layout txn field operation.
 Row operations for the non-unique secondary index with tid column.

 Created 2025-07-03 by Ting Yuan
 *******************************************************/

#ifndef lizard0row0lotus_h
#define lizard0row0lotus_h

#include <stddef.h>
#include "que0que.h"
#include "row0row.h"

#include "lizard0row0lotus0assert.h"

namespace lizard {

extern bool hippo_mvcc_enabled;
extern bool hippo_lock_enabled;
extern bool hippo_purge_enabled;

/**
  Modify lotus layout fields of record. It will handle compress pages.
  @param[in,out]  rec           record
  @param[in]      page_zip
  @param[in]      index         index
  @param[in]      offsets       rec_get_offsets(rec, index)
  @param[in]      trx_id        transaction id
  @param[in]      during_import true if called while importing a tablespace;
                                the existing tid in the record may originate
                                from a foreign instance and can be "in the
                                future", so its sanity must not be asserted.
*/
void row_upd_rec_lotus_fields_low(Slice *slc, page_zip_des_t *page_zip,
                                  const dict_index_t *index, trx_id_t trx_id,
                                  bool during_import = false);
/**
  Write lotus layout fields of the physical record.
  @param[in,out]  rec       record
  @param[in,out]  page_zip  compressed page, or NULL
  @param[in]      index     index
  @param[in]      offsets   rec_get_offsets(rec, index)
  @param[in]      trx_id    transaction id
*/
void row_upd_index_entry_lotus_field(const dict_index_t *index, dtuple_t *entry,
                                     trx_id_t trx_id);

/**
  Get the lotus tid from a lotus layout record.
  @param[in]      slc       record
  @param[in]      index     index
  @return tid
*/
trx_id_t row_get_rec_lotus_tid(const Slice *slc, const dict_index_t *index);
/**
  Write redo log to the buffer about updates of lotus layout fields.
  @param[in]      index     index of the record, must be a hippo index
  @param[in]      trx_id    transaction id
  @param[in]      log_ptr   pointer to a buffer opened in mlog

  @return new pointer to mlog
*/
byte *row_upd_write_lotus_vals_to_log(const dict_index_t *index,
                                      trx_id_t trx_id, byte *log_ptr);

/** Parses the log data of lotus field values.
@param[in]      ptr       buffer
@param[in]      end_ptr   buffer end
@param[out]     pos       TRX_ID position in record
@param[out]     trx_id    trx id
@return log data end or NULL */
byte *row_upd_parse_lotus_vals(const byte *ptr, const byte *end_ptr, ulint *pos,
                               trx_id_t *trx_id);

/** Updates the trx id field in a lotus layout record in database recovery.
@param[in,out] rec Record
@param[in,out] page_zip Compressed page, or null
@param[in] offsets Array returned by rec_get_offsets()
@param[in] pos Trx_id position in rec
@param[in] trx_id Transaction id */
void row_upd_rec_lotus_fields_in_recovery(Slice *slc, page_zip_des_t *page_zip,
                                          ulint pos, trx_id_t trx_id);

/**
 * Handle trx_id field for hippo secondary index during rollback.
 *
 * For hippo secondary indexes that include a trx_id system column, we need
 * special handling during rollback operations. Since the undo log does not
 * contain the correct previous transaction ID for secondary index records,
 * we set the trx_id field to 0 as a special placeholder value.
 *
 * This function clones the dfield before modifying it to avoid corrupting
 * shared data buffers when multiple secondary indexes share the same dtuple.
 *
 * @param[in]     index   The secondary index being processed
 * @param[in,out] entry   The dtuple entry to modify
 * @param[in]     heap    Memory heap for allocation
 */
void row_upd_index_entry_lotus_field_in_rollback(const dict_index_t *index,
                                                 dtuple_t *entry,
                                                 mem_heap_t *heap);

}  // namespace lizard

#endif /* lizard0row0lotus_h */
