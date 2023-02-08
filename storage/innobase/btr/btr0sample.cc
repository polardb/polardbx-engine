/* Copyright (c) 2018, 2019, Alibaba and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "btr0sample.h"
#include "ha_innodb.h"
#include "row0mysql.h"

ulint sample_advise_pages = 100;

void btr_sample_t::init(row_prebuilt_t *row_prebuilt) {
  prebuilt = row_prebuilt;
  reset();
}

void btr_sample_t::enable() {
  ut_ad(!enabled);
  ut_ad(scan_mode == NO_SAMPLE);
  enabled = true;
}

void btr_sample_t::reset() {
  enabled = false;
  scan_mode = NO_SAMPLE;
}

void btr_sample_t::on_switch_part() { scan_mode = NO_SAMPLE; }

void btr_sample_t::open_curor(dict_index_t *index, mtr_t *mtr) {
  auto &pcur = prebuilt->pcur;
  auto &parent = prebuilt->parent;
  auto cursor = pcur->get_btr_cur();
  bool position_at_left = true;
  ulint height = ULINT_UNDEFINED;
  ulint savepoint;
  page_cur_t *page_cursor;
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  ut_ad(enabled);
  ut_a(scan_mode == NO_SAMPLE);
  ut_ad(!index->table->is_intrinsic());
  ut_ad(parent->m_cleanout_cursors == nullptr &&
        parent->m_cleanout_pages == nullptr);

  /* Initialize leaf and parent pcurors */
  parent->m_latch_mode = pcur->m_latch_mode = BTR_SEARCH_LEAF;
  parent->m_search_mode = pcur->m_search_mode = PAGE_CUR_G;
  parent->m_pos_state = pcur->m_pos_state = BTR_PCUR_IS_POSITIONED;
  parent->m_old_stored = pcur->m_old_stored = false;
  parent->m_trx_if_known = pcur->m_trx_if_known = nullptr;

  parent->m_btr_cur.index = pcur->m_btr_cur.index = index;

  /* Store the position of the tree latch we push to mtr so that we
  know how to release it when we have latched the leaf node */
  savepoint = mtr_set_savepoint(mtr);

  /* An alternative: traverse all branch nodes while holding S lock */
  mtr_sx_lock(dict_index_get_lock(index), mtr);

  page_cursor = btr_cur_get_page_cur(cursor);
  page_cursor->index = index;

  /* Begin from the index root */
  page_id_t page_id(dict_index_get_space(index), dict_index_get_page(index));
  const page_size_t &page_size = dict_table_page_size(index->table);

  for (;;) {
    ut_ad(cursor->m_fetch_mode == Page_fetch::NORMAL);
    auto fetch_mode = (height == 0) ? Page_fetch::SCAN : Page_fetch::NORMAL;

    auto block = buf_page_get_gen(page_id, page_size, RW_NO_LATCH, NULL,
                                  fetch_mode, __FILE__, __LINE__, mtr);

    auto page = buf_block_get_frame(block);

    ut_ad(fil_page_index_page_check(page));
    ut_ad(index->id == btr_page_get_index_id(page));

    if (height == ULINT_UNDEFINED) {
      /* We are in the root node */
      height = btr_page_get_level(page, mtr);
      /* Evaluate sampling mode */
      decide_scan_mode(block, height);
    } else {
      ut_ad(height == btr_page_get_level(page, mtr));
    }

    ut_ad(scan_mode != NO_SAMPLE);

    if (height == 0) {
      /* Leaf nodes always be s-latched */
      btr_cur_latch_leaves(block, page_id, page_size, BTR_SEARCH_LEAF, cursor,
                           mtr);
      /* Release index SX lock */
      mtr->release_sx_latch_at_savepoint(savepoint, dict_index_get_lock(index));
    }

    if (position_at_left) {
      page_cur_set_before_first(block, page_cursor);
      page_cur_move_to_next(page_cursor);
    } else {
      /* Only if all leaf blocks are skipped by sampling */
      ut_a(scan_mode == SAMPLE_BY_BLOCK);
      ut_a(height == 0);
      page_cur_set_after_last(block, page_cursor);
    }

    if (height == 0) {
      break;
    }

    ut_a(!page_is_empty(buf_block_get_frame(block)));

    if (scan_mode == SAMPLE_BY_BLOCK && height == 1) {
      /* S-latch the branch block for btr_pcur_t::store_position */
      btr_cur_latch_leaves(block, page_id, page_size, BTR_SEARCH_LEAF, cursor,
                           mtr);
      buf_block_dbg_add_level(block, SYNC_TREE_NODE);

      /* Begin from the left most branch, get a leaf with sampling pct */

      auto node_rec = page_cur_get_rec(page_cursor);
      page_cur_position(node_rec, block, parent->get_page_cur());
      parent->get_page_cur()->index = index;

      position_at_left = get_one_leaf_with_sampling(page_cursor, mtr);

      /* Release S-latch */
      btr_leaf_page_release(parent->get_block(), BTR_SEARCH_LEAF, mtr);
    }

    /* Child page id */
    auto node_ptr = page_cur_get_rec(page_cursor);
    offsets = rec_get_offsets(node_ptr, index, offsets, ULINT_UNDEFINED, &heap);
    page_id.set_page_no(btr_node_ptr_get_child_page_no(node_ptr, offsets));

    height--;
  }

  if (heap) {
    mem_heap_free(heap);
  }
}

bool btr_sample_t::get_one_leaf_with_sampling(page_cur_t *page_cursor,
                                              mtr_t *mtr) {
  bool found = false;
  auto &parent = prebuilt->parent;

  ut_ad(mtr_memo_contains(mtr, parent->get_block(), MTR_MEMO_PAGE_S_FIX));
  ut_ad(mtr_memo_contains_flagged(mtr, dict_index_get_lock(parent->index()),
                                  MTR_MEMO_SX_LOCK));

  do {
    if (parent->is_on_user_rec() && !skip()) {
      found = true;
      break;
    }
  } while (parent->move_to_next_user_rec(mtr) == DB_SUCCESS);

  auto parent_cursor = parent->get_page_cur();

  ut_a(!page_is_empty(buf_block_get_frame(parent_cursor->block)));

  page_cursor->block = parent_cursor->block;
  page_cursor->rec = parent_cursor->rec;

  if (!found) {
    ut_a(parent->is_after_last_on_page());
    page_cur_move_to_prev(page_cursor);
  }

  parent->store_position(mtr);

  return found;
}

void btr_sample_t::decide_scan_mode(buf_block_t *root, ulint tree_height) {
  ut_ad(scan_mode == NO_SAMPLE);

  auto n_recs = page_get_n_recs(buf_block_get_frame(root));

  if (tree_height == 0 || (tree_height == 1 && n_recs < sample_advise_pages)) {
    scan_mode = SAMPLE_BY_REC;
  } else {
    scan_mode = SAMPLE_BY_BLOCK;
  }
}

bool btr_sample_t::restore_position(mtr_t *mtr) {
  ut_ad(scan_mode != NO_SAMPLE);

  if (scan_mode == SAMPLE_BY_BLOCK) {
    return restore_leaf_pcur(mtr);
  }

  auto &pcur = prebuilt->pcur;
  ut_ad(pcur->m_latch_mode == BTR_SEARCH_LEAF);
  return btr_pcur_restore_position(pcur->m_latch_mode, pcur, mtr);
}

bool btr_sample_t::restore_leaf_pcur(mtr_t *mtr) {
  bool succ = false;
  if (restore_pcur_optimistic(prebuilt->pcur, true, mtr, succ)) {
    return succ;
  }

  return restore_leaf_pessimistic(mtr);
}

bool btr_sample_t::restore_pcur_optimistic(btr_pcur_t *pcur, bool is_leaf,
                                           mtr_t *mtr, bool &succ) {
  auto index = pcur->index();
  auto latch_mode = pcur->m_latch_mode;

  ut_ad(mtr->is_active());
  ut_ad(pcur->m_old_stored);
  ut_ad(pcur->is_positioned());
  ut_ad(!index->table->is_intrinsic());
  ut_ad(latch_mode == BTR_SEARCH_LEAF);

  ut_a(pcur->m_rel_pos != BTR_PCUR_AFTER_LAST_IN_TREE &&
       pcur->m_rel_pos != BTR_PCUR_BEFORE_FIRST_IN_TREE);
  ut_a(pcur->m_old_rec != nullptr);
  ut_a(pcur->m_old_n_fields > 0);

  auto no_buf_pool_resize = !buf_pool_is_obsolete(pcur->m_withdraw_clock);

  DBUG_EXECUTE_IF("force_leaf_pcur_restore_fail",
                  if (is_leaf) no_buf_pool_resize = false;);
  DBUG_EXECUTE_IF("force_branch_pcur_restore_fail",
                  if (!is_leaf) no_buf_pool_resize = false;);

  if (no_buf_pool_resize &&
      btr_cur_optimistic_latch_leaves(
          pcur->m_block_when_stored, pcur->m_modify_clock, &latch_mode,
          &pcur->m_btr_cur, __FILE__, __LINE__, mtr)) {
    pcur->m_pos_state = BTR_PCUR_IS_POSITIONED;

    ut_ad(latch_mode == pcur->m_latch_mode);

    buf_block_dbg_add_level(pcur->get_block(), SYNC_TREE_NODE);

    if (pcur->m_rel_pos == BTR_PCUR_ON) {
      /* FIX ME: checking for branch node */
      if (is_leaf) {
#ifdef UNIV_DEBUG
        auto rec = pcur->get_rec();
        auto heap = mem_heap_create(256);
        auto offsets1 = rec_get_offsets(pcur->m_old_rec, index, nullptr,
                                        pcur->m_old_n_fields, &heap);
        auto offsets2 =
            rec_get_offsets(rec, index, nullptr, pcur->m_old_n_fields, &heap);
        ut_ad(!cmp_rec_rec(pcur->m_old_rec, rec, offsets1, offsets2, index));
        mem_heap_free(heap);
#endif /* UNIV_DEBUG */
      }
      succ = true;
    } else {
      /* This is the same record as stored,
      may need to be adjusted for BTR_PCUR_BEFORE/AFTER,
      depending on search mode and direction. */
      if (pcur->is_on_user_rec()) {
        pcur->m_pos_state = BTR_PCUR_IS_POSITIONED_OPTIMISTIC;
      }
      succ = false;
    }

    return true;
  }

  return false;
}

bool btr_sample_t::restore_leaf_pessimistic(mtr_t *mtr) {
  page_cur_mode_t mode;
  auto &pcur = prebuilt->pcur;
  auto index = pcur->index();

  auto heap = mem_heap_create(256);

  auto tuple = dict_index_build_data_tuple(index, pcur->m_old_rec,
                                           pcur->m_old_n_fields, heap);

  switch (pcur->m_rel_pos) {
    case BTR_PCUR_ON:
      mode = PAGE_CUR_LE;
      break;
    case BTR_PCUR_AFTER:
      mode = PAGE_CUR_G;
      break;
    case BTR_PCUR_BEFORE:
      mode = PAGE_CUR_L;
      break;
    default:
      ut_error;
  }

  search_to_leaf(tuple, mode, mtr);

  ut_ad(pcur->m_rel_pos == BTR_PCUR_ON || pcur->m_rel_pos == BTR_PCUR_BEFORE ||
        pcur->m_rel_pos == BTR_PCUR_AFTER);

  if (pcur->m_rel_pos == BTR_PCUR_ON && pcur->is_on_user_rec() &&
      !cmp_dtuple_rec(tuple, pcur->get_rec(), index,
                      rec_get_offsets(pcur->get_rec(), index, nullptr,
                                      ULINT_UNDEFINED, &heap))) {
    /* We have to store the NEW value for the modify clock,
    since the cursor can now be on a different page!
    But we can retain the value of old_rec */

    pcur->m_block_when_stored = pcur->get_block();

    pcur->m_modify_clock =
        buf_block_get_modify_clock(pcur->m_block_when_stored);

    pcur->m_old_stored = true;

    pcur->m_withdraw_clock = buf_withdraw_clock;

    mem_heap_free(heap);

    return (true);
  }

  mem_heap_free(heap);

  pcur->store_position(mtr);

  return false;
}

void btr_sample_t::search_to_leaf(const dtuple_t *tuple, page_cur_mode_t mode,
                                  mtr_t *mtr) {
  auto &parent = prebuilt->parent;
  btr_cur_t *cursor = prebuilt->pcur->get_btr_cur();
  dict_index_t *index = cursor->index;
  bool do_sampling = (mode == PAGE_CUR_G);
  bool position_at_left = true;
  page_t *page = NULL;
  buf_block_t *block;
  Page_fetch fetch_mode;
  ulint height = ULINT_UNDEFINED;
  ulint up_match = 0;
  ulint up_bytes = 0;
  ulint low_match = 0;
  ulint low_bytes = 0;
  ulint savepoint;
  ulint rw_latch;
  page_cur_mode_t page_mode;
  page_cur_t *page_cursor;
  ulint upper_rw_latch;
  buf_block_t *tree_blocks[BTR_MAX_LEVELS];
  ulint tree_savepoints[BTR_MAX_LEVELS];
  ulint n_blocks = 0;
  ulint n_releases;
  mem_heap_t *heap = NULL;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  ut_ad(scan_mode == SAMPLE_BY_BLOCK);
  ut_ad(dict_index_check_search_tuple(index, tuple));
  ut_ad(prebuilt->pcur->m_latch_mode == BTR_SEARCH_LEAF);
  ut_ad(cursor->m_fetch_mode == Page_fetch::NORMAL);

  UNIV_MEM_INVALID(&cursor->up_match, sizeof cursor->up_match);
  UNIV_MEM_INVALID(&cursor->up_bytes, sizeof cursor->up_bytes);
  UNIV_MEM_INVALID(&cursor->low_match, sizeof cursor->low_match);
  UNIV_MEM_INVALID(&cursor->low_bytes, sizeof cursor->low_bytes);

#ifdef UNIV_DEBUG
  cursor->up_match = ULINT_UNDEFINED;
  cursor->low_match = ULINT_UNDEFINED;
#endif /* UNIV_DEBUG */
  cursor->flag = BTR_CUR_BINARY;

  btr_cur_n_non_sea++;

  /* Store the position of the tree latch we push to mtr so that we
  know how to release it when we have latched leaf node(s) */
  savepoint = mtr_set_savepoint(mtr);

  if (do_sampling) {
    mtr_sx_lock(dict_index_get_lock(index), mtr);
  } else if (!srv_read_only_mode) {
    mtr_s_lock(dict_index_get_lock(index), mtr);
  }

  if (do_sampling || srv_read_only_mode) {
    upper_rw_latch = RW_NO_LATCH;
  } else {
    upper_rw_latch = RW_S_LATCH;
  }

  page_cursor = btr_cur_get_page_cur(cursor);

  const space_id_t space = dict_index_get_space(index);
  const page_size_t page_size(dict_table_page_size(index->table));

  /* Start with the root page. */
  page_id_t page_id(space, dict_index_get_page(index));

  /* We use these modified search modes on non-leaf levels of the
  B-tree. These let us end up in the right B-tree leaf. In that leaf
  we use the original search mode. */
  switch (mode) {
    case PAGE_CUR_G:
      page_mode = PAGE_CUR_LE;
      break;
    case PAGE_CUR_L:
    case PAGE_CUR_LE:
      page_mode = mode;
      break;
    default:
      ut_error;
      break;
  }

search_loop:
  ut_ad(cursor->m_fetch_mode == Page_fetch::NORMAL);
  fetch_mode = (height == 0) ? Page_fetch::SCAN : Page_fetch::NORMAL;
  rw_latch = (height != 0) ? upper_rw_latch : (ulint)RW_NO_LATCH;

  ut_ad(n_blocks < BTR_MAX_LEVELS);
  tree_savepoints[n_blocks] = mtr_set_savepoint(mtr);
  block = buf_page_get_gen(page_id, page_size, rw_latch, nullptr, fetch_mode,
                           __FILE__, __LINE__, mtr);
  tree_blocks[n_blocks] = block;

  page = buf_block_get_frame(block);

  if (rw_latch != RW_NO_LATCH) {
#ifdef UNIV_ZIP_DEBUG
    const page_zip_des_t *page_zip = buf_block_get_page_zip(block);
    ut_a(!page_zip || page_zip_validate(page_zip, page, index));
#endif /* UNIV_ZIP_DEBUG */
    buf_block_dbg_add_level(block, SYNC_TREE_NODE);
  }

  ut_ad(fil_page_index_page_check(page));
  ut_ad(index->id == btr_page_get_index_id(page));

  if (UNIV_UNLIKELY(height == ULINT_UNDEFINED)) {
    /* We are in the root node */
    height = btr_page_get_level(page, mtr);
    cursor->tree_height = height + 1;
  }

  if (height == 0) {
    if (rw_latch == RW_NO_LATCH) {
      btr_cur_latch_leaves(block, page_id, page_size, BTR_SEARCH_LEAF, cursor,
                           mtr);
    }

    if (do_sampling) {
      /* Release index SX lock */
      mtr->release_sx_latch_at_savepoint(savepoint, dict_index_get_lock(index));
    } else if (!srv_read_only_mode) {
      /* Release the upper level blocks */
      for (n_releases = 0; n_releases < n_blocks; n_releases++) {
        mtr_release_block_at_savepoint(mtr, tree_savepoints[n_releases],
                                       tree_blocks[n_releases]);
      }
      /* Release index S lock */
      mtr->release_s_latch_at_savepoint(savepoint, dict_index_get_lock(index));
    }

    page_mode = mode;
  }

  if (height > 0 || (!do_sampling && !btr_search_enabled)) {
    /* Search for complete index fields. */
    up_bytes = low_bytes = 0;
    page_cur_search_with_match(block, index, tuple, page_mode, &up_match,
                               &low_match, page_cursor, nullptr);
  } else if (do_sampling) {
    up_bytes = low_bytes = low_match = up_match = 0;
    if (position_at_left) {
      page_cur_set_before_first(block, page_cursor);
      page_cur_move_to_next(page_cursor);
    } else {
      /* All leaf blocks are skipped by sampling */
      page_cur_set_after_last(block, page_cursor);
    }
  } else {
    ut_ad(btr_search_enabled);
    /* The adaptive hash index is only used when searching
    for leaf pages (height==0). */
    page_cur_search_with_match_bytes(block, index, tuple, page_mode, &up_match,
                                     &up_bytes, &low_match, &low_bytes,
                                     page_cursor);
  }

  ut_ad(height == btr_page_get_level(page_cur_get_page(page_cursor), mtr));

  if (height == 1 && do_sampling) {
    /* S-latch the branch block for btr_pcur_t::store_position */
    btr_cur_latch_leaves(block, page_id, page_size, BTR_SEARCH_LEAF, cursor,
                         mtr);
    buf_block_dbg_add_level(block, SYNC_TREE_NODE);

    /* Begin from the left most branch, get a leaf with sampling pct */

    auto node_rec = page_cur_get_rec(page_cursor);
    page_cur_position(node_rec, block, parent->get_page_cur());

    position_at_left = get_one_leaf_with_sampling(page_cursor, mtr);

    /* Release S-latch */
    btr_leaf_page_release(parent->get_block(), BTR_SEARCH_LEAF, mtr);
  }

  if (height > 0) {
    n_blocks++;

    height--;

    auto node_ptr = page_cur_get_rec(page_cursor);

    ut_a(!page_is_empty(page_align(node_ptr)));
    ut_a(page_rec_is_user_rec(node_ptr));

    offsets = rec_get_offsets(node_ptr, index, offsets, ULINT_UNDEFINED, &heap);

    /* Go to the child node */
    page_id.reset(space, btr_node_ptr_get_child_page_no(node_ptr, offsets));

    goto search_loop;
  }

  cursor->low_match = low_match;
  cursor->low_bytes = low_bytes;
  cursor->up_match = up_match;
  cursor->up_bytes = up_bytes;

  ut_ad(cursor->up_match != ULINT_UNDEFINED || mode != PAGE_CUR_GE);
  ut_ad(cursor->up_match != ULINT_UNDEFINED || mode != PAGE_CUR_LE);
  ut_ad(cursor->low_match != ULINT_UNDEFINED || mode != PAGE_CUR_LE);

  if (UNIV_LIKELY_NULL(heap)) {
    mem_heap_free(heap);
  }
}

bool btr_sample_t::move_to_next(mtr_t *mtr) {
  ut_ad(scan_mode != NO_SAMPLE);

  if (scan_mode == SAMPLE_BY_BLOCK) {
    return move_to_next_via_blk(mtr);
  } else {
    return move_to_next_via_rec(mtr);
  }
}

bool btr_sample_t::move_to_next_via_rec(mtr_t *mtr) {
  auto move = btr_pcur_move_to_next(prebuilt->pcur, mtr);
  while (move && skip()) {
    move = btr_pcur_move_to_next(prebuilt->pcur, mtr);
  }
  return move;
}

bool btr_sample_t::move_to_next_via_blk(mtr_t *mtr) {
  auto &pcur = prebuilt->pcur;

  ut_ad(pcur->m_pos_state == BTR_PCUR_IS_POSITIONED);
  ut_ad(pcur->m_latch_mode == BTR_SEARCH_LEAF);

  pcur->m_old_stored = false;

  if (!pcur->is_after_last_on_page()) {
    pcur->move_to_next_on_page();
    return true;
  }

  if (pcur->is_after_last_in_tree(mtr)) {
    return false;
  }

  btr_leaf_page_release(pcur->get_block(), pcur->m_latch_mode, mtr);

  return move_to_next_page(mtr);
}

bool btr_sample_t::move_to_next_page(mtr_t *mtr) {
  auto &parent = prebuilt->parent;
  auto index = parent->index();

  auto savepoint = mtr_set_savepoint(mtr);
  mtr_sx_lock(dict_index_get_lock(index), mtr);

  bool succ;
  if (restore_pcur_optimistic(parent, false, mtr, succ)) {
    if (succ || parent->m_rel_pos == BTR_PCUR_AFTER) {
      parent->move_to_next_on_page();
    }

    page_cur_t page_cursor;
    auto found = get_one_leaf_with_sampling(&page_cursor, mtr);

    get_leaf_node_by_nodeptr(page_cur_get_rec(&page_cursor), found, mtr);

    btr_leaf_page_release(parent->get_block(), parent->m_latch_mode, mtr);

    mtr->release_sx_latch_at_savepoint(savepoint, dict_index_get_lock(index));

    return found;
  }

  mtr->release_sx_latch_at_savepoint(savepoint, dict_index_get_lock(index));

  return restore_leaf_pessimistic(mtr);
}

void btr_sample_t::get_leaf_node_by_nodeptr(rec_t *node_ptr,
                                            bool position_at_left, mtr_t *mtr) {
  auto &pcur = prebuilt->pcur;
  auto index = pcur->index();
  mem_heap_t *heap = NULL;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);

  page_id_t page_id(dict_index_get_space(index), dict_index_get_page(index));

  offsets = rec_get_offsets(node_ptr, index, offsets, ULINT_UNDEFINED, &heap);
  page_id.set_page_no(btr_node_ptr_get_child_page_no(node_ptr, offsets));

  auto block = buf_page_get_gen(page_id, dict_table_page_size(index->table),
                                pcur->m_latch_mode, nullptr, Page_fetch::SCAN,
                                __FILE__, __LINE__, mtr);
  ut_a(block);

  if (position_at_left) {
    page_cur_set_before_first(block, pcur->get_page_cur());
    page_cur_move_to_next(pcur->get_page_cur());
  } else {
    page_cur_set_after_last(block, pcur->get_page_cur());
  }

  if (heap) {
    mem_heap_free(heap);
  }
}

bool btr_sample_t::skip() const {
  std::uniform_real_distribution<double> rnd(0.0, 1.0);
  return rnd(prebuilt->m_mysql_handler->m_random_number_engine) >
         (prebuilt->m_mysql_handler->m_sampling_percentage / 100.0);
}
