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

/** @file btr/lizard0btr0btr.cc
 *
 * Lizard btree upgrade version

 Created 2024-08-30 by Jianwei.zhao
 *******************************************************/
#include "dict0dict.h"
#include "btr0btr.h"

#include "lizard0btr0btr.h"

namespace lizard {

/** Read page type of btree root page.
 *
 * @param[in]	dict index
 *
 * @retval	persisted root page type.
 * */
page_type_t btr_page_read_root_type(const dict_index_t *index) {
  page_type_t page_type;
  mtr_t mtr;
  mtr.start();

  page_id_t page_id(dict_index_get_space(index), dict_index_get_page(index));
  page_size_t page_size(dict_table_page_size(index->table));

  buf_block_t *root = btr_block_get(page_id, page_size, RW_S_LATCH,
                                    UT_LOCATION_HERE, index, &mtr);

  page_type = fil_page_get_type(buf_block_get_frame(root));
  mtr.commit();

  return page_type;
}

/** Whether upgrade index root page when btree create.
 *
 * @param[in]	index
 * @param[in]	index create type.
 * @param[in]	expected root page type.
 *
 * @retval	final root page create type. */
page_type_t btr_page_upgrade_root(const dict_index_t *index,
                                  page_type_t create_type,
                                  page_type_t expected_page_type) {
  ut_ad(fil_page_type_is_index(create_type));

  /** Only expect btree upgrade v1 version currently. */
  ut_ad(expected_page_type == FIL_PAGE_INDEX_PANDA ||
        expected_page_type == FIL_PAGE_TYPE_UNUSED ||
        expected_page_type == create_type);

  if (create_type == FIL_PAGE_INDEX &&
      expected_page_type == FIL_PAGE_INDEX_PANDA) {

    /** Only upgrade btree seondary unique index. */
    ut_ad(dict_index_is_unique(index) && !index->is_clustered());

    create_type = FIL_PAGE_INDEX_PANDA;
  }
  return create_type;
}

/** Whether upgrade index not-root page when btree allocate page..
 *
 * @param[in]	root page type.
 * @param[in]	index create type.
 *
 * @retval	final page create type. */
page_type_t btr_page_upgrade_not_root(const page_type_t root_page_type,
                                      page_type_t create_type) {
  ut_ad(create_type != FIL_PAGE_TYPE_UNUSED);

  if (create_type == FIL_PAGE_INDEX && root_page_type == FIL_PAGE_INDEX_PANDA) {
    create_type = FIL_PAGE_INDEX_PANDA;
  }
  return create_type;
}

/**
 * Reset the index id to 0 if btr page freed.
 *
 * @param[in] block   Pointer to the page
 * @param[in] mtr     Mini-transaction, can be NULL if redo log is not needed
 */
void btr_page_reset_index_id(buf_block_t *block, mtr_t *mtr) {
  page_t *page = buf_block_get_frame(block);
  page_zip_des_t *page_zip = buf_block_get_page_zip(block);

  ut_ad(fil_page_index_page_check(page));

  if (mtr) {
    btr_page_set_index_id(page, page_zip, 0, mtr);
  } else {
    mach_write_to_8(page + (PAGE_HEADER + PAGE_INDEX_ID), 0);
    if (page_zip) {
      page_zip_write_header(page_zip, page + (PAGE_HEADER + PAGE_INDEX_ID), 8,
                            nullptr);
    }
  }
}

}  // namespace lizard
