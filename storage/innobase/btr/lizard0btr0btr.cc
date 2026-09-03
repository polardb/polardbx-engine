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
#include "btr0sea.h"
#include "lock0lock.h"

#include "lizard0btr0btr.h"
#include "lizard0page.h"

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

  ut_ad(expected_page_type == FIL_PAGE_INDEX_PANDA ||
        expected_page_type == FIL_PAGE_INDEX_HIPPO ||
        expected_page_type == FIL_PAGE_TYPE_UNUSED ||
        expected_page_type == FIL_PAGE_INDEX_SPC ||
        expected_page_type == create_type);

  if (create_type != FIL_PAGE_INDEX) {
    return create_type;
  }

  if (expected_page_type == FIL_PAGE_INDEX_PANDA) {
    /** Only upgrade btree seondary unique index. */
    ut_ad(dict_index_is_unique(index) && !index->is_clustered());
    create_type = FIL_PAGE_INDEX_PANDA;
  } else if (expected_page_type == FIL_PAGE_INDEX_SPC) {
    ut_ad(!dict_index_is_unique(index) && !index->is_clustered());
    create_type = FIL_PAGE_INDEX_SPC;
  } else if (expected_page_type == FIL_PAGE_INDEX_HIPPO) {
    ut_ad(!index->is_clustered());
    create_type = FIL_PAGE_INDEX_HIPPO;
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

  if (create_type != FIL_PAGE_INDEX) {
    return create_type;
  }

  if (root_page_type == FIL_PAGE_INDEX_PANDA) {
    create_type = FIL_PAGE_INDEX_PANDA;
  } else if (root_page_type == FIL_PAGE_INDEX_SPC) {
    /* For quick validation */
    create_type = FIL_PAGE_INDEX_SPC;
  } else if (root_page_type == FIL_PAGE_INDEX_HIPPO) {
    create_type = FIL_PAGE_INDEX_HIPPO;
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

bool btr_page_encode_low(bool recovery, page_cur_t *cursor, dict_index_t *index,
                         Page_encoder *page_encoder, ulint n_ins_recs,
                         ulint max_ins_size_expect, mtr_t *mtr) {
  buf_block_t *block = page_cur_get_block(cursor);
#ifndef UNIV_HOTBACKUP
  buf_pool_t *buf_pool = buf_pool_from_bpage(&block->page);
#endif /* !UNIV_HOTBACKUP */
  page_t *page = buf_block_get_frame(block);
  buf_block_t *temp_block;
  page_t *temp_page;
  ulint data_size1;
  ulint data_size2;
  ulint max_ins_size_actual;
  bool success = false;
  ulint pos;

#ifndef UNIV_HOTBACKUP
  ut_ad(mtr_is_block_fix(mtr, block, MTR_MEMO_PAGE_X_FIX, index->table));
#endif /* !UNIV_HOTBACKUP */
  btr_assert_not_corrupted(block, index);
#ifdef UNIV_ZIP_DEBUG
#endif /* UNIV_ZIP_DEBUG */
  ut_ad(!buf_block_get_page_zip(block));
  data_size1 = page_get_record_data_size(page);

  /* Turn logging off */
  mtr_log_t log_mode = mtr_set_log_mode(mtr, MTR_LOG_NONE);

#ifndef UNIV_HOTBACKUP
  temp_block = buf_block_alloc(buf_pool);
#else  /* !UNIV_HOTBACKUP */
  temp_block = back_block2;
#endif /* !UNIV_HOTBACKUP */
  temp_page = temp_block->frame;

#ifndef UNIV_HOTBACKUP
  MONITOR_INC(MONITOR_INDEX_REORG_ATTEMPTS);
#endif /* !UNIV_HOTBACKUP */

  /* Copy the old page to temporary space */
  buf_frame_copy(temp_page, page);

#ifndef UNIV_HOTBACKUP
  if (!recovery) {
    btr_search_drop_page_hash_index(block);
  }
#endif /* !UNIV_HOTBACKUP */

  /* Save the cursor position. */
  pos = page_rec_get_n_recs_before(page_cur_get_rec(cursor));

  /* Recreate the page: note that global data on page (possible
  segment headers, next page-field, etc.) is preserved intact */

  page_create(block, mtr, dict_table_is_comp(index->table),
              FIL_PAGE_INDEX_SPC, page_encoder->dict_data(),
              page_encoder->dict_offsets());

  /* Copy the records from the temporary space to the recreated page;
  do not copy the lock bits yet */

  page_encoder->page_copy_rec_list_end_no_locks(
      block, temp_block, page_get_infimum_rec(temp_page), index, mtr);

  /* Multiple transactions cannot simultaneously operate on the
  same temp-table in parallel.
  max_trx_id is ignored for temp tables because it not required
  for MVCC. */
  if (page_is_leaf(page)) {
    /* Copy max trx id to recreated page */
    trx_id_t max_trx_id = page_get_max_trx_id(temp_page);
    page_set_max_trx_id(block, nullptr, max_trx_id, mtr);
    /* In crash recovery, dict_index_is_sec_or_ibuf() always
    holds, even for clustered indexes.  max_trx_id is
    unused in clustered index pages. */
    ut_ad(max_trx_id != 0 || recovery);
  }

#ifndef UNIV_HOTBACKUP
  /* No locks are acquried for intrinsic tables. */
  if (!recovery && !dict_table_is_locking_disabled(index->table)) {
    /* Update the record lock bitmaps */
    lock_move_reorganize_page(block, temp_block);
  }
#endif /* !UNIV_HOTBACKUP */

  data_size2 = page_get_record_data_size(page);
  max_ins_size_actual = page_get_max_insert_size(page, 1);

  if (max_ins_size_expect != max_ins_size_actual) {
    lizard_error(ER_LIZARD)
        << "Reencode: Page old data size " << data_size1 << ", new data size "
        << data_size2 << "expect max ins size " << max_ins_size_expect
        << ", actual max ins size " << max_ins_size_actual;
    ut_d(ut_error);
  } else {
    success = true;
  }

  /* Restore the cursor position. */
  if (pos > 0) {
    cursor->rec = page_rec_get_nth(page, pos);
  } else {
    ut_ad(cursor->rec == page_get_infimum_rec(page));
  }

  ut_ad(page_rows_equal(temp_page, page, index));

#ifndef UNIV_HOTBACKUP
  buf_block_free(temp_block);
#endif /* !UNIV_HOTBACKUP */

  /* Restore logging mode */
  mtr_set_log_mode(mtr, log_mode);

#ifndef UNIV_HOTBACKUP
  if (success) {
    byte *log_ptr = nullptr;

    /* Write the log record */
    if (mlog_open_and_write_index(mtr, page, index, MLOG_COMP_PAGE_ENCODE,
                                  2 + 2 + 2, log_ptr)) {
      mach_write_to_2(log_ptr, index->pd_capacity);
      log_ptr += 2;

      mach_write_to_2(log_ptr, n_ins_recs);
      log_ptr += 2;

      mach_write_to_2(log_ptr, max_ins_size_expect);
      mlog_close(mtr, log_ptr + 2);
    }

    ut_ad(page_dict_get_size(page) <= index->pd_capacity);

    MONITOR_INC(MONITOR_INDEX_REORG_SUCCESSFUL);
  }
#endif /* !UNIV_HOTBACKUP */

  return (success);
}

bool btr_page_encode(page_cur_t *cursor, dict_index_t *index,
                     Page_encoder *page_encoder, ulint n_ins_recs,
                     ulint max_ins_size, mtr_t *mtr) {
  return btr_page_encode_low(false, cursor, index, page_encoder, n_ins_recs,
                             max_ins_size, mtr);
}

extern byte *btr_parse_page_encode(byte *ptr, byte *end_ptr,
                                   dict_index_t *index, buf_block_t *block,
                                   mtr_t *mtr) {
  ulint max_ins_size;
  ulint max_ins_size_expect;
  page_cur_t cur;
  ulint n_ins_recs;
  ut_ad(index != nullptr);

  if (ptr + 2 > end_ptr) {
    return (nullptr);
  }
  index->set_pd_capacity(mach_read_from_2(ptr));
  ptr += 2;

  if (ptr + 2 > end_ptr) {
    return (nullptr);
  }
  n_ins_recs = mach_read_from_2(ptr);
  ptr += 2;

  if (ptr + 2 > end_ptr) {
    return (nullptr);
  }
  max_ins_size_expect = mach_read_from_2(ptr);
  ptr += 2;

  if (block != nullptr) {
    Page_encoder page_encoder(index->pd_capacity);
    max_ins_size =
        page_encoder.build(buf_block_get_frame(block), index, n_ins_recs);
    ut_a(max_ins_size == max_ins_size_expect);

    page_cur_set_before_first(block, &cur);
    btr_page_encode_low(true, &cur, index, &page_encoder, n_ins_recs,
                        max_ins_size, mtr);
  }

  return ptr;
}

}  // namespace lizard
