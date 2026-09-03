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

#include "lizard0page0dict.h"
#include "lizard0row.h"
#include "lizard0row0clover.h"

extern void page_zip_header_cmp(const page_zip_des_t *, const byte *);

namespace lizard {

/**
  Write Clover layout fields of a record on a B-tree leaf node page.
  @param[in/out]  page_zip    compressed page
  @param[in]      index       dict_index_t
  @param[in/out]  rec         record
  @param[in]      offsets     rec_get_offsets(rec, index)
  @param[in]      scn_col     column number of SCN_ID in rec
  @param[in]      scn         transaction scn
  @param[in]      undo_ptr    undo_ptr
*/
void page_zip_write_clover(page_zip_des_t *page_zip, const dict_index_t *index,
                           Slice *slc, ulint scn_col, const scn_t scn,
                           const undo_ptr_t undo_ptr, const gcn_t gcn) {
  byte *field;
  byte *storage;
#ifdef UNIV_DEBUG
  page_t *page = page_align(slc->reg());
#endif /* UNIV_DEBUG */
  ulint len;

  ut_ad(page_simple_validate_new(page));
  ut_ad(page_zip_simple_validate(page_zip));
  ut_ad(page_zip_get_size(page_zip) > PAGE_DATA + page_zip_dir_size(page_zip));
  ut_ad(slc->validate());
  ut_ad(rec_offs_comp(slc->offsets()));
  ut_ad(slc->contiguous());

  ut_ad(page_zip->m_start >= PAGE_DATA);
  ut_d(page_zip_header_cmp(page_zip, page));

  ut_ad(page_is_leaf(page));

  UNIV_MEM_ASSERT_RW(page_zip->data, page_zip_get_size(page_zip));

  /** Get location of scn_id slot */
  storage = page_zip_dir_start(page_zip) -
            (rec_get_heap_no_new(slc->rec()) - 1) * PAGE_ZIP_TRX_FIELDS_SIZE +
            DATA_TRX_ID_LEN + DATA_ROLL_PTR_LEN;

  /** Get pointer of scn_id in record */
  ut_ad(rec_offs_n_fields(slc->offsets()) >= scn_col + DATA_N_LIZARD_COLS);
  field = slc->get_nth_field(index, scn_col, &len);
  ut_ad(len == DATA_SCN_ID_LEN);
  ut_ad(field + DATA_SCN_ID_LEN ==
        slc->get_nth_field(index, scn_col + 1, &len));
  ut_ad(len == DATA_UNDO_PTR_LEN);

  ut_ad(field + DATA_SCN_ID_LEN + DATA_UNDO_PTR_LEN ==
        slc->get_nth_field(index, scn_col + 2, &len));
  ut_ad(len == DATA_GCN_ID_LEN);

#if defined UNIV_DEBUG || defined UNIV_ZIP_DEBUG
  ut_a(!memcmp(storage, field, DATA_LIZARD_TOTAL_LEN));
#endif /* UNIV_DEBUG || UNIV_ZIP_DEBUG */

  row_upd_rec_write_clover(field, scn, undo_ptr, gcn);

  /** Copy to compressed page */
  memcpy(storage, field, DATA_LIZARD_TOTAL_LEN);

  UNIV_MEM_ASSERT_RW(slc->rec(), slc->data_size());
  UNIV_MEM_ASSERT_RW(slc->rec() - slc->extra_size(), slc->extra_size());
  UNIV_MEM_ASSERT_RW(page_zip->data, page_zip_get_size(page_zip));
}

/**
  Check the lizard fields in every user records of the page
  @param[in]      page        cluster index page
  @param[in]      index       cluster index

  @retval         true        Success
*/
bool page_txn_is_valid(page_t *page, const dict_index_t *index) {
  reg_t reg;
  Slice_ctx sctx;
  ut_ad(page);

  if (index->table->is_intrinsic()) return true;
  /** Didn't check other type except of compact and branch */
  if (!page_is_comp(page) || !page_is_leaf(page)) return true;

  const txn_layout_t layout = dict_index_txn_layout(index);

  switch (layout) {
    case TL_CLOVER:
    case TL_BAMBOO:

      reg = page_rec_get_next_reg(page_get_infimum_rec(page));
      while (page_rec_is_user_rec(reg.rec)) {
        /* Assert the validation */
        ut_ad(page_rec_is_comp(reg.rec));
        ut_ad(row_txn_is_valid(sctx.get_offsets(reg, index, ULINT_UNDEFINED),
                               index, layout));

        reg = page_rec_get_next_reg(reg.rec);
      }
      return true;

    case TL_NONE:
    case TL_LOTUS:
      return true;
  }

  return true;
}

/** TODO: mtr ? <09-01-26, zanye.zjy> */

static void page_create_dict_low(page_t *page, const pd_data_t *data,
                                 ulint *offsets) {
  ulint n_slots = dict_offs_n_slots(offsets);
  ulint data_size = dict_offs_data_size(offsets);
  ut_ad(fil_page_is_spc(page));
  ut_ad(page_get_n_recs(page) == 0);
  ut_ad(page_dir_get_n_heap(page) == PAGE_HEAP_NO_USER_LOW);
  ut_ad(n_slots <= PACK_ID_MAX + 1);
  ut_ad((n_slots == 0) == (data_size == 0));

  // Note: btr_lift_page_up calls btr_page_empty on the father page, which leads
  // to this function, but at this point the father page is not yet a leaf, so
  // the assertion below would fail.
  // ut_ad(page_is_leaf(page) || n_slots == 0);

  /* 1. Write prefixes count. */
  page_dict_set_n_slots(page, n_slots);
  /* 2. Reserved byte */
  page_dict_set_flags(page, dict_offs_flags(offsets));
  /* 3. Write prefixes offset. */
  for (pack_id_t i = 0; i < n_slots; i++) {
    page_dict_set_dir_slot(page, i, dict_offs_base(offsets)[i]);
  }
  /* 4. Write prefixes. */
  if (data_size > 0) {
    memcpy(page_dict_get_data(page), data, data_size);
  }

  /* 5. Update HEAP_TOP. */
  page_header_set_ptr(page, nullptr, PAGE_HEAP_TOP, page_dict_get_end(page));

  dict_offs_store_position(page_dict_get_data(page), offsets);
  ut_ad(page_dict_validate(page, offsets));
  ut_ad(page_get_record_data_size(page) == 0);
}

static void page_create_dict_write_log(const page_t *page,
                                       const pd_data_t *data,
                                       const ulint *offsets, mtr_t *mtr) {
  ulint data_size = dict_offs_data_size(offsets);
  ulint n_slots = dict_offs_n_slots(offsets);

  byte *log_ptr = nullptr;

  if (!mlog_open(
          mtr,
          /* redo initial part + n_slots + slot_directory */
          REDO_LOG_INITIAL_INFO_SIZE + 2 + n_slots * PAGE_DICT_DIR_SLOT_SIZE,
          log_ptr)) {
    return;
  }

  log_ptr = mlog_write_initial_log_record_fast(page, MLOG_SPC_PAGE_CREATE_DICT,
                                               log_ptr, mtr);

  mach_write_to_2(log_ptr, n_slots);
  log_ptr += 2;

  for (ulint i = 0; i < n_slots; ++i) {
    mach_write_to_2(log_ptr, dict_offs_base(offsets)[i]);
    log_ptr += 2;
  }

  mlog_close(mtr, log_ptr);

  /** Write Page Dict Data */
  mlog_catenate_string(mtr, data, data_size);
}

void page_create_dict(page_t *page, const pd_data_t *data, ulint *offsets,
                      mtr_t *mtr) {
  page_create_dict_write_log(page, data, offsets, mtr);
  page_create_dict_low(page, data, offsets);
}

byte *page_parse_create_dict(byte *ptr, byte *end_ptr, buf_block_t *block,
                             mtr_t *mtr) {
  pd_data_t *data = nullptr;
  ulint offsets[DICT_OFFS_MAX_SIZE];
  ulint n_slots;
  ulint data_size;

  bzero(offsets, sizeof(offsets));

  if (ptr + 2 > end_ptr) {
    return (nullptr);
  }

  n_slots = offsets[0] = mach_read_from_2(ptr);
  ptr += 2;
  ut_a(n_slots <= PACK_ID_MAX + 1);

  if (ptr + n_slots * PAGE_DICT_DIR_SLOT_SIZE > end_ptr) {
    return (nullptr);
  }

  for (ulint i = 0; i < n_slots; ++i) {
    dict_offs_base(offsets)[i] = mach_read_from_2(ptr);
    ptr += 2;
  }

  data_size = dict_offs_data_size(offsets);
  if (data_size > 0) {
    ut_a(data_size < page_get_free_space_of_empty(true));

    if (ptr + data_size > end_ptr) {
      return (nullptr);
    }

    data = ptr;
    ptr += data_size;
  }

  if (block != nullptr) {
    page_create_dict_low(buf_block_get_frame(block), data, offsets);
  }

  return ptr;
}

}  // namespace lizard

ulint page_get_row_size(const page_t *page, const dict_index_t *index) {
  creg_t reg;
  Slice_ctx sctx;
  Slice *slc = nullptr;

  ulint all_row_size = 0;

  if (!fil_page_is_spc(page)) {
    return page_get_total_data_size(page);
  }

  reg = page_rec_get_next_creg(page_get_infimum_creg(page).rec);

  while (reg.rec != page_get_supremum_creg(page).rec) {
    slc = sctx.get_offsets(reg, index, ULINT_UNDEFINED);
    all_row_size += slc->row_size();

    reg = page_rec_get_next_creg(reg.rec);
  }

  return all_row_size;
}

