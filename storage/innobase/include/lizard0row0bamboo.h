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

/** @file include/lizard0row0bamboo.h
 Bamboo layout txn field operation.

 Created 2024-12-06 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0row0bamboo_h
#define lizard0row0bamboo_h

#include "que0que.h"

namespace lizard {

/**
  Fill Bamboo Layout txn fields into index entry.
  @param[in]    thr       query
  @param[in]    entry     dtuple
  @param[in]    index     clustered index
*/
extern void row_upd_index_entry_bamboo_field(que_thr_t *thr, dtuple_t *entry,
                                             dict_index_t *index);

/**
  Write bamboo layout fields of the physical record.
  @param[in]      ptr       scn pointer
  @param[in]      scn       SCN
  @param[in]      undo_ptr  UBA
*/
extern void row_upd_rec_write_bamboo(byte *ptr, const scn_t scn,
                              const undo_ptr_t undo_ptr);
/**
  Write bamboo layout fields of the physical record.
  @param[in,out]  rec       record
  @param[in]      index     index with system columns
  @param[in]      offsets   rec_get_offsets(rec, idnex)
  @param[in]      scn       SCN
  @param[in]      undo_ptr  UBA
*/
extern void row_upd_rec_write_bamboo(rec_t *rec, const dict_index_t *index,
                                     const ulint *offsets, const scn_t scn,
                                     const undo_ptr_t uba);

/**
  Modify bamboo layout fields of record. It will handle compress pages.
  @param[in,out]  rec       record
  @param[in]      page_zip
  @param[in]      index     cluster index
  @param[in]      offsets   rec_get_offsets(rec, idnex)
  @param[in]      txn_desc  txn description
*/
extern void row_upd_rec_bamboo_fields_low(rec_t *rec, page_zip_des_t *page_zip,
                                          const dict_index_t *index,
                                          const ulint *offsets, const scn_t scn,
                                          const undo_ptr_t uba);
/**
  Updates bamboo layout field in a panda index record when
  cleanout because of update.
  @param[in/out]  rec       record
  @param[in/out]  page_zip  compressed page, or NULL
  @param[in]      index     cluster index
  @param[in]      offsets   rec_get_offsets(rec, idnex)
  @param[in]      txn_rec   scn/uba info
*/
extern void row_upd_rec_bamboo_fields_in_cleanout(rec_t *rec,
                                                  page_zip_des_t *page_zip,
                                                  const dict_index_t *index,
                                                  const ulint *offsets,
                                                  const txn_rec_t *txn_rec);

/**
  Read bamboo layout txn from record

  @param[in]      rec         record
  @param[in]      index       dict_index_t, must be cluster index
  @param[in]      offsets     rec_get_offsets(rec, index)
  @param[out]     txn_rec     lizard transaction attributes
*/
extern void row_get_bamboo_txn_rec(const rec_t *rec, const dict_index_t *index,
                                   const ulint *offsets, txn_rec_t *txn_rec);
/**
 * Bamboo Layout row has been cleanout.
  @param[in]      rec       record
  @param[in]      index     cluster index
  @parma[in]      offsets   rec_get_offsets(rec, index)

  @retval         true      cleanout
*/
extern bool row_bamboo_has_cleanout(const rec_t *rec, const dict_index_t *index,
                                    const ulint *offsets);
/**
  Debug the undo_ptr and scn in record is matched.
  @param[in]      rec       record
  @param[in]      index     cluster index
  @parma[in]      offsets   rec_get_offsets(rec, index)

  @retval         true      Success
*/
extern bool row_bamboo_is_valid(const rec_t *rec, const dict_index_t *index,
                                const ulint *offsets);

/*=============================================================================*/
/* Bamboo Layout record write/parse redo */
/*=============================================================================*/
/**
  Write redo log to the buffer about updates of scn and uba.
  @param[in]      index     index of the record
  @param[in]      txn_rec   txn info of the record
  @param[in]      log_ptr   pointer to a buffer opened in mlog
  @param[in]      mtr       mtr

  @return new pointer to mlog
*/
byte *row_upd_write_bamboo_vals_to_log(const dict_index_t *index,
                                       const txn_rec_t *txn_rec, byte *log_ptr,
                                       mtr_t *mtr MY_ATTRIBUTE((unused)));

/**
  Parses the log data of bamboo field values.
  @param[in]      ptr       buffer
  @param[in]      end_ptr   buffer end
  @param[out]     pos       SCN position in record
  @param[out]     scn       scn
  @param[out]     undo_ptr  uba

  @return log data end or NULL
*/
byte *row_upd_parse_bamboo_vals(const byte *ptr, const byte *end_ptr,
                                ulint *pos, scn_t *scn, undo_ptr_t *undo_ptr);

/**
  Updates the scn and undo_ptr field in a index record in
  database recovery.
  @param[in/out]  rec       record
  @param[in/out]  page_zip  compressed page, or NULL
  @param[in]      pos       SCN position in rec
  @param[in]      index     index
  @param[in]      offsets   rec_get_offsets(rec, idnex)
  @param[in]      scn       SCN
  @param[in]      undo_ptr  UBA
*/
extern void row_upd_rec_bamboo_fields_in_recovery(
    rec_t *rec, page_zip_des_t *page_zip, const dict_index_t *index, ulint pos,
    const ulint *offsets, const scn_t scn, const undo_ptr_t undo_ptr);

}  // namespace lizard
#endif

