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

/** @file btr/lizard0btr0cur.cc
 lizard b-tree.

 Created 2024-04-10 by Ting Yuan
 *******************************************************/

#include "btr0pcur.h"
#include "btr0sea.h"

#include "lizard0btr0cur.h"
#include "lizard0dbg.h"
#include "lizard0dict.h"
#include "lizard0dict0mem.h"
#include "lizard0mtr0log.h"
#include "lizard0row.h"
#include "lizard0row0gpp.h"

namespace lizard {
/**
 * Attempts to position a persistent cursor on a clustered index record
 * based on the gpp_no read from the secondary index record.
 * @return        True if successful positioning, False otherwise
 */
bool btr_cur_guess_clust_by_gpp(dict_index_t *clust_idx,
                                const dict_index_t *sec_idx,
                                const dtuple_t *clust_ref, const rec_t *sec_rec,
                                btr_pcur_t *clust_pcur,
                                const ulint *sec_offsets, ulint latch_mode,
                                ulint &gpp_no_offset,
                                mtr_t *mtr) {
  ut_ad(sec_idx->n_s_gfields > 0);
  ut_ad(sec_offsets && sec_offsets[1] == sec_idx->n_fields);
  ut_ad(latch_mode == BTR_SEARCH_LEAF || latch_mode == BTR_MODIFY_LEAF);
  ut_ad(rec_offs_validate(sec_rec, sec_idx, sec_offsets));
  ut_ad(sec_idx->table == nullptr || !sec_idx->table->is_compressed());

  /** Phase 1: read gpp no from the sec rec. */
  bool clust_found = false;
  ulint up_match = 0;
  ulint low_match = 0;
  buf_block_t *block = nullptr;
  ulint rw_latch = latch_mode;
  ulint cur_savepoint = 0;
  ulint savepoints[2];
  ulint n_savepoint = 0;
  page_no_t gpp_no = FIL_NULL;

  gpp_no = row_get_gpp_no(sec_rec, sec_idx, sec_offsets, gpp_no_offset);
  ut_ad(gpp_no != 0);

#ifdef UNIV_DEBUG
  DBUG_EXECUTE_IF("set_gpp_null", gpp_no = FIL_NULL;);
  DBUG_EXECUTE_IF("set_gpp_enum_page_in_space", gpp_no = dbug_gpp_no;);
  DBUG_EXECUTE_IF("set_gpp_random", {
    page_no_t space_size = 100;
    if (fil_space_t *space =
            fil_space_acquire_silent(dict_index_get_space(clust_idx))) {
      space_size = space->size;

      fil_space_release(space);
    }
    std::srand(time(0));
    gpp_no = std::rand() % (2 * (space_size + 1));
  });
#endif /* UNIV_DEBUG */

  /** Avoid acquiring the page latch recursively. */
  if (gpp_no == FIL_NULL || gpp_no == page_get_page_no(page_align(sec_rec)) ||
      clust_idx->table->is_compressed()) {
    goto func_exit;
  }

  /** Phase 2: fetch the page according to gpp_no. */
  /** Fetch the page in Page_fetch::GPP_FETCH mode because the page may
   * have already been freed or out of tablespace. */
  cur_savepoint = mtr_set_savepoint(mtr);
  if ((block = buf_page_get_gen(
           page_id_t{dict_index_get_space(clust_idx), gpp_no},
           dict_table_page_size(clust_idx->table), RW_NO_LATCH, nullptr,
           Page_fetch::GPP_FETCH, UT_LOCATION_HERE, mtr)) == nullptr) {
    goto func_exit;
  }
  savepoints[n_savepoint++] = cur_savepoint;

  /** Try lock to avoid deadlock. */
  cur_savepoint = mtr_set_savepoint(mtr);
  if (!buf_page_get_known_nowait(rw_latch, block, Cache_hint::MAKE_YOUNG,
                                 __FILE__, __LINE__, true, mtr)) {
    goto func_exit;
  }
  savepoints[n_savepoint++] = cur_savepoint;

  if (!fil_page_index_page_check(
          buf_block_get_frame(block)) /* page is not index page */
      || btr_page_get_index_id(buf_block_get_frame(block)) !=
             clust_idx->id /* page is not clust index page */
      || !page_is_leaf(buf_block_get_frame(block)) /* page is not leaf page */
      || !page_has_siblings(
             buf_block_get_frame(block)) /* page has siblings (for excluding
                                            discarded root page in DDL) */
  ) {
    goto func_exit;
  }

  ut_d(buf_page_mutex_enter(block));
  ut_ad(!block->page.file_page_was_freed);
  ut_d(buf_page_mutex_exit(block));

  /** Phase 3: search the clust rec in the target page. */
  clust_pcur->m_latch_mode = latch_mode;
  clust_pcur->m_search_mode = PAGE_CUR_LE;
  clust_pcur->m_old_stored = false;

  page_cur_search_with_match(block, clust_idx, clust_ref,
                             clust_pcur->m_search_mode, &up_match, &low_match,
                             clust_pcur->get_page_cur(), nullptr);

  clust_pcur->m_pos_state = BTR_PCUR_IS_POSITIONED;
  clust_pcur->m_btr_cur.index = clust_idx;
  clust_pcur->m_btr_cur.flag = BTR_CUR_BINARY;

  /** Phase 4: rec validation. */
  if (!page_rec_is_user_rec(clust_pcur->get_rec()) ||
      low_match < dict_index_get_n_unique(clust_idx)) {
    clust_pcur->reset_btr_cur();
    goto func_exit;
  }

  clust_found = true;

func_exit:
  if (!clust_found) {
    /** Release the page if the found rec is mismatched. */
    for (ulint i = 0; i < n_savepoint; ++i) {
      mtr_release_block_at_savepoint(mtr, savepoints[i], block);
    }
  }

  sec_idx->gpp_stat(clust_found);

  return clust_found;
}

/**
  Write redo log when updating scn and uba fileds in physical records.
  @param[in]      rec        physical record
  @param[in]      index      dict that interprets the row record
  @param[in]      txn_rec    txn info from the record
  @param[in]      mtr        mtr
*/
void btr_cur_upd_lizard_fields_clust_rec_log(const rec_t *rec,
                                             const dict_index_t *index,
                                             const txn_rec_t *txn_rec,
                                             mtr_t *mtr) {
  byte *log_ptr = nullptr;

  ut_ad(!!page_rec_is_comp(rec) == dict_table_is_comp(index->table));
  ut_ad(index->is_clustered());
  ut_ad(mtr);

  if (!mlog_open_and_write_index(mtr, rec, index, MLOG_REC_CLUST_LIZARD_UPDATE,
                                 REDO_LIZARD_FIELDS_LEN + 2, log_ptr)) {
    return;
  }

  log_ptr = row_upd_write_lizard_vals_to_log(index, txn_rec, log_ptr, mtr);

  mach_write_to_2(log_ptr, page_offset(rec));
  log_ptr += 2;

  mlog_close(mtr, log_ptr);
}

/**
  Parse the txn info from redo log record, and apply it if necessary.
  @param[in]      ptr        buffer
  @param[in]      end        buffer end
  @param[in]      page       page (NULL if it's just get the length)
  @param[in]      page_zip   compressed page, or NULL
  @param[in]      index      index corresponding to page

  @return         return the end of log record or NULL
*/
byte *btr_cur_parse_lizard_fields_upd_clust_rec(byte *ptr, byte *end_ptr,
                                                page_t *page,
                                                page_zip_des_t *page_zip,
                                                const dict_index_t *index) {
  ulint pos;
  scn_t scn;
  undo_ptr_t undo_ptr;
  gcn_t gcn;
  ulint rec_offset;
  rec_t *rec;

  ut_ad(!page || !!page_is_comp(page) == dict_table_is_comp(index->table));

  ptr = row_upd_parse_lizard_vals(ptr, end_ptr, &pos, &scn, &undo_ptr, &gcn);

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
    } else {
      /** If it's non-compact, the info of index will not be written in redo
      log, but it can be self-explanatory because there is a offsets array in
      physical record. See the function **rec_get_offsets** */
      lizard_ut_ad(index && index->table && !index->table->col_names);
      lizard_ut_ad(!rec_offs_comp(offsets));
    }

    row_upd_rec_lizard_fields_in_recovery(rec, page_zip, index, pos, offsets,
                                          scn, undo_ptr, gcn);

    if (UNIV_LIKELY_NULL(heap)) mem_heap_free(heap);
  }

  return ptr;
}

/** Updates the redo log record for a gpp_no of a secondary index record.
  @param[in]      rec             secondary record
  @param[in]      index           secondary index
  @param[in]      gpp_no_offset   the offsets of gpp_no field
  @param[in]      gpp_no          page no of the cluster index
  @param[in]      mtr             mtr
*/
void btr_cur_upd_gpp_no_sec_rec_log(const rec_t *rec, const dict_index_t *index,
                                    ulint gpp_no_offset, page_no_t gpp_no,
                                    mtr_t *mtr) {
  byte *log_ptr = nullptr;
  /* 11 bytes for the initial part of a log record. */
  if (!mlog_open(mtr, 11 + 4 + 2 + 2, log_ptr)) {
    /* Logging in mtr is switched off during crash recovery:
    in that case mlog_open returns false */
    return;
  }
  log_ptr = mlog_write_initial_log_record_fast(rec, MLOG_REC_SEC_GPP_NO,
                                               log_ptr, mtr);

  mach_write_to_4(log_ptr, gpp_no);
  log_ptr += DATA_GPP_NO_LEN;

  mach_write_to_2(log_ptr, gpp_no_offset);
  log_ptr += 2;

  mach_write_to_2(log_ptr, page_offset(rec));
  log_ptr += 2;

  mlog_close(mtr, log_ptr);
}

/**
  Parses the redo log record for a gpp_no of a secondary index record.
  @param[in]      ptr        buffer
  @param[in]      end        buffer end
  @param[in]      page       page (NULL if it's just get the length)
  @param[in]      page_zip   compressed page, or NULL
  @param[in]      index      index corresponding to page

  @return         return the end of log record or NULL
*/
byte *btr_cur_parse_gpp_no_upd_sec_rec(
    byte *ptr,                /*!< in: buffer */
    byte *end_ptr,            /*!< in: buffer end */
    page_t *page,             /*!< in/out: page or NULL */
    page_zip_des_t *page_zip) /*!< in/out: compressed page, or NULL */
{
  rec_t *rec = nullptr;

  if (end_ptr < ptr + 4 + 2 + 2) {
    return (nullptr);
  }

 DBUG_EXECUTE_IF("crash_if_gpp_cleanout_redo", DBUG_SUICIDE(););
  
  auto gpp_no = mach_read_from_4(ptr);
  ptr += 4;

  auto gpp_offset = mach_read_from_2(ptr);
  ptr += 2;

  auto rec_offset = mach_read_from_2(ptr);
  ptr += 2;

  ut_a(rec_offset <= UNIV_PAGE_SIZE);

  if (page) {
    rec = page + rec_offset;

    /* We do not need to reserve search latch, as the page
    is only being recovered, and there cannot be a hash index to
    it. Besides, the delete-mark flag is being updated in place
    and the adaptive hash index does not depend on it. */

    row_upd_rec_gpp_fields_in_recovery(rec, page_zip, gpp_no, gpp_offset);
  }

  return (ptr);
}

}  // namespace lizard
