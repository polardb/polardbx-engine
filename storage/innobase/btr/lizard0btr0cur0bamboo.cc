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

/** @file include/lizard0btr0cur0bamboo.h
   Lizard index tree bamboo operation.


 Created 2024-04-12 by Yichang.Song
 *******************************************************/

#include "lizard0btr0cur0bamboo.h"
#include "lizard0dbg.h"
#include "lizard0dict.h"
#include "lizard0mtr0log.h"
#include "lizard0row0bamboo.h"

namespace lizard {

/**
  Write redo log when updating bamboo layout txn field in physical records.
  @param[in]      rec        physical record
  @param[in]      index      dict that interprets the row record
  @param[in]      txn_rec    txn info from the record
  @param[in]      mtr        mtr
*/
void btr_cur_upd_bamboo_fields_sec_rec_log(const rec_t *rec,
                                           const dict_index_t *index,
                                           const txn_rec_t *txn_rec,
                                           mtr_t *mtr) {
  byte *log_ptr = nullptr;

  ut_ad(!!page_rec_is_comp(rec) == dict_table_is_comp(index->table));
  ut_ad(dict_index_is_panda(index));
  ut_ad(mtr);

  if (!mlog_open_and_write_index(mtr, rec, index, MLOG_REC_SEC_BAMBOO_UPDATE,
                                 REDO_BAMBOO_FIELDS_LEN + 2, log_ptr)) {
    return;
  }

  log_ptr = row_upd_write_bamboo_vals_to_log(index, txn_rec, log_ptr, mtr);

  mach_write_to_2(log_ptr, page_offset(rec));
  log_ptr += 2;

  mlog_close(mtr, log_ptr);
}

/**
  Parse bamboo layout txn info from redo log record, and apply it if necessary.
  @param[in]      ptr        buffer
  @param[in]      end        buffer end
  @param[in]      page       page (NULL if it's just get the length)
  @param[in]      page_zip   compressed page, or NULL
  @param[in]      index      index corresponding to page

  @return         return the end of log record or NULL
*/
byte *btr_cur_parse_bamboo_fields_upd_sec_rec(byte *ptr, byte *end_ptr,
                                              page_t *page,
                                              page_zip_des_t *page_zip,
                                              const dict_index_t *index) {
  ulint pos;
  scn_t scn;
  undo_ptr_t undo_ptr;
  ulint rec_offset;
  rec_t *rec;

  ut_ad(!page || !!page_is_comp(page) == dict_table_is_comp(index->table));

  ptr = row_upd_parse_bamboo_vals(ptr, end_ptr, &pos, &scn, &undo_ptr);

  if (ptr == nullptr) {
    return nullptr;
  }

  /** 2 bytes offset */
  if (end_ptr < ptr + 2) {
    return (nullptr);
  }

  rec_offset = mach_read_from_2(ptr);
  ptr += 2;

  ut_a(rec_offset <= UNIV_PAGE_SIZE);

  if (page) {
    mem_heap_t *heap = nullptr;
    ulint offsets_[REC_OFFS_NORMAL_SIZE];
    ulint *offsets = offsets_;
    rec = page + rec_offset;

    rec_offs_init(offsets_);

    offsets = rec_get_offsets(rec, index, offsets, ULINT_UNDEFINED,
                              UT_LOCATION_HERE, &heap);

    if (page_is_comp(page) || rec_offs_comp(offsets)) {
      assert_lizard_dict_index_check_no_check_table(index);
      ut_ad(dict_index_is_panda(index));
    } else {
      /** If it's non-compact, the info of index will not be written in redo
      log, but it can be self-explanatory because there is a offsets array in
      physical record. See the function **rec_get_offsets** */
      lizard_ut_ad(index && index->table && !index->table->col_names);
      lizard_ut_ad(!rec_offs_comp(offsets));
    }

    row_upd_rec_bamboo_fields_in_recovery(rec, page_zip, index, pos, offsets,
                                          scn, undo_ptr);

    if (UNIV_LIKELY_NULL(heap)) mem_heap_free(heap);
  }

  return ptr;
}

}  // namespace lizard
