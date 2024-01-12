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

/** @file include/lizard0page.cc
 lizard page operation.

 Created 2020-04-08 by Jianwei.zhao
 *******************************************************/

#include "lizard0page.h"
#include "dict0mem.h"
#include "mem0mem.h"
#include "page0page.h"
#include "page0zip.h"

#include "lizard0row.h"

extern void page_zip_header_cmp(const page_zip_des_t *, const byte *);

namespace lizard {

/**
  Write the scn and uba of a record on a B-tree leaf node page.
  @param[in/out]  page_zip    compressed page
  @param[in]      index       dict_index_t
  @param[in/out]  rec         record
  @param[in]      offsets     rec_get_offsets(rec, index)
  @param[in]      scn_col     column number of SCN_ID in rec
  @param[in]      scn         transaction scn
  @param[in]      undo_ptr    undo_ptr
*/
void page_zip_write_scn_and_undo_ptr(page_zip_des_t *page_zip,
                                     const dict_index_t *index, byte *rec,
                                     const ulint *offsets, ulint scn_col,
                                     const scn_t scn, const undo_ptr_t undo_ptr,
                                     const gcn_t gcn) {
  byte *field;
  byte *storage;
#ifdef UNIV_DEBUG
  page_t *page = page_align(rec);
#endif /* UNIV_DEBUG */
  ulint len;

  ut_ad(page_simple_validate_new(page));
  ut_ad(page_zip_simple_validate(page_zip));
  ut_ad(page_zip_get_size(page_zip) > PAGE_DATA + page_zip_dir_size(page_zip));
  ut_ad(rec_offs_validate(rec, NULL, offsets));
  ut_ad(rec_offs_comp(offsets));

  ut_ad(page_zip->m_start >= PAGE_DATA);
  ut_d(page_zip_header_cmp(page_zip, page));

  ut_ad(page_is_leaf(page));

  UNIV_MEM_ASSERT_RW(page_zip->data, page_zip_get_size(page_zip));

  /** Get location of scn_id slot */
  storage = page_zip_dir_start(page_zip) -
            (rec_get_heap_no_new(rec) - 1) * PAGE_ZIP_TRX_FIELDS_SIZE +
            DATA_TRX_ID_LEN + DATA_ROLL_PTR_LEN;

  /** Get pointer of scn_id in record */
  field =
      const_cast<byte *>(rec_get_nth_field(index, rec, offsets, scn_col, &len));
  ut_ad(len == DATA_SCN_ID_LEN);
  ut_ad(field + DATA_SCN_ID_LEN ==
        rec_get_nth_field(index, rec, offsets, scn_col + 1, &len));
  ut_ad(len == DATA_UNDO_PTR_LEN);

  ut_ad(field + DATA_SCN_ID_LEN + DATA_UNDO_PTR_LEN ==
        rec_get_nth_field(index, rec, offsets, scn_col + 2, &len));
  ut_ad(len == DATA_GCN_ID_LEN);

#if defined UNIV_DEBUG || defined UNIV_ZIP_DEBUG
  ut_a(!memcmp(storage, field, DATA_LIZARD_TOTAL_LEN));
#endif /* UNIV_DEBUG || UNIV_ZIP_DEBUG */

  row_upd_rec_write_scn_and_undo_ptr(field, scn, undo_ptr, gcn);

  /** Copy to compressed page */
  memcpy(storage, field, DATA_LIZARD_TOTAL_LEN);

  UNIV_MEM_ASSERT_RW(rec, rec_offs_data_size(offsets));
  UNIV_MEM_ASSERT_RW(rec - rec_offs_extra_size(offsets),
                     rec_offs_extra_size(offsets));
  UNIV_MEM_ASSERT_RW(page_zip->data, page_zip_get_size(page_zip));
}

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
/**
  Check the lizard fields in every user records of the page
  @param[in]      page        cluster index page
  @param[in]      index       cluster index

  @retval         true        Success
*/
bool lizard_page_attributes(page_t *page, const dict_index_t *index) {
  rec_t *rec;
  mem_heap_t *heap;
  ulint *offsets = NULL;

  ut_ad(page);

  if (!index->is_clustered() || index->table->is_intrinsic()) return true;

  /** Didn't check other type except of compact and branch */
  if (!page_is_comp(page) || !page_is_leaf(page)) return true;

  heap = mem_heap_create(UNIV_PAGE_SIZE + 200, UT_LOCATION_HERE);

  rec = page_rec_get_next(page_get_infimum_rec(page));

  while (page_rec_is_user_rec(rec)) {
    offsets = rec_get_offsets(rec, index, offsets, ULINT_UNDEFINED,
                              UT_LOCATION_HERE, &heap);

    /* Assert the validation */
    ut_ad(page_rec_is_comp(rec));
    assert_row_lizard_valid(rec, index, offsets);

    rec = page_rec_get_next(rec);
  }

  mem_heap_free(heap);

  return true;
}
#endif /* UNIV_DEBUG || LIZARD_DEBUG define */

}  // namespace lizard
