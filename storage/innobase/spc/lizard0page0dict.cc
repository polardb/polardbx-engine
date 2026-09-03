/*****************************************************************************

Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file spc/lizard0spc0area.cc
  Metadata area of Secondary Prefix Compression

 Created 2025-10-15 by Yichang Song
 *******************************************************/
#include "page0page.h"

#include "lizard0fil0types.h"
#include "lizard0page.h"
#include "lizard0page0dict.h"
#include "lizard0rem0slc0ctx.h"

bool dict_offs_validate(const pd_data_t *dict, const ulint *offsets,
                        const page_t *page) {
  pack_id_t n_slots = dict_offs_n_slots(offsets);

  if (dict) {
    ut_a((ulint)dict == offsets[1]);
  }

  if (page) {
    ut_a(n_slots == page_dict_get_n_slots(page));
    for (pack_id_t i = 0; i < n_slots; ++i) {
      ut_a(dict_offs_base(offsets)[i] == page_dict_get_dir_slot(page, i));
    }

    if (dict) {
      ut_a(!memcmp(dict, page_dict_get_data(page),
                   page_dict_get_data_size(page)));
    }
  }

  return true;
}

bool page_dict_validate(const page_t *page, const ulint *dict_offs) {
  if (!fil_page_is_spc(page)) return true;

  ulint dict_offs_[DICT_OFFS_MAX_SIZE];
  if (dict_offs == nullptr) {
    dict_offs = page_dict_get_offsets(page, dict_offs_);
  }
  ut_ad(dict_offs_validate(page_dict_get_data(page), dict_offs, page));

  if (page_dict_get_n_slots(page) == 0) {
    ut_ad(page_dict_get_data_size(page) == 0);
    ut_ad(page_dict_get_data(page) == nullptr);
  }
  if (page_dir_get_n_heap(page) == PAGE_HEAP_NO_USER_LOW) {
    ut_ad(page_header_get_ptr(page, PAGE_HEAP_TOP) == page_dict_get_end(page));
  }
  return true;
}

bool page_dict_equal(const page_t *page1, const page_t *page2) {
  ulint offs1_[DICT_OFFS_MAX_SIZE];
  ulint offs2_[DICT_OFFS_MAX_SIZE];
  ulint *offs1;
  ulint *offs2;

  const pd_data_t *data1;
  const pd_data_t *data2;

  if (fil_page_is_spc(page1) != fil_page_is_spc(page2)) {
    return false;
  }

  if (!fil_page_is_spc(page1)) {
    return true;
  }

  offs1 = page_dict_get_offsets(page1, offs1_);
  offs2 = page_dict_get_offsets(page2, offs2_);

  if (dict_offs_n_slots(offs1) != dict_offs_n_slots(offs2)) {
    return false;
  }

  for (ulint i = 0; i < dict_offs_n_slots(offs1); ++i) {
    if (dict_offs_base(offs1)[i] != dict_offs_base(offs2)[i]) return false;
  }

  if (dict_offs_data_size(offs1) != dict_offs_data_size(offs2)) {
    return false;
  }

  if (dict_offs_n_slots(offs1) > 0) {
    data1 = page_dict_get_data(page1);
    data2 = page_dict_get_data(page2);
    if (ut_memcmp(data1, data2, dict_offs_data_size(offs1))) return false;
  }

  return true;
}

bool page_rows_equal(const page_t *page1, const page_t *page2,
                     const dict_index_t *index) {
  Slice_ctx s1_ctx, s2_ctx;
  Slice *s1 = nullptr, *s2= nullptr;
  size_t row1_size, row2_size;
  rec_pack_t pack1, pack2;

  ut_a(page_get_n_recs(page1) == page_get_n_recs(page2));

  creg_t reg1 = page_rec_get_next_creg(page_get_infimum_creg(page1).rec);
  creg_t reg2 = page_rec_get_next_creg(page_get_infimum_creg(page2).rec);

  while (reg1 != page_get_supremum_creg(page1)) {
    s1 = s1_ctx.get_offsets(reg1, index, ULINT_UNDEFINED);
    s2 = s2_ctx.get_offsets(reg2, index, ULINT_UNDEFINED);

    pack1 = s1->rec_pack();
    pack2 = s2->rec_pack();

    row1_size = s1->data_size() + pack1.len;
    row2_size = s2->data_size() + pack2.len;
    ut_a(row1_size == row2_size);

    for (ulint i = 0; i < row1_size; ++i) {
      ut_a((*s1)[i] == (*s2)[i]);
    }

    reg1 = page_rec_get_next_creg(reg1.rec);
    reg2 = page_rec_get_next_creg(reg2.rec);
  }

  return true;
}
