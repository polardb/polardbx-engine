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
#include "data0data.h"
#include "page0types.h"
#include "row0upd.h"
#include "trx0rec.h"

#include "lizard0btr0cur.h"
#include "lizard0dict0dict.h"
#include "lizard0dict0mem.h"
#include "lizard0fil0types.h"
#include "lizard0mtr0log.h"
#include "lizard0row.h"
#include "lizard0row0bamboo.h"
#include "lizard0row0gpp.h"
#include "lizard0row0lotus.h"
#include "lizard0trx.h"
#include "lizard0undo.h"
#include "lizard0undo0rec0types.h"

namespace lizard {
/**
 * Attempts to position a persistent cursor on a clustered index record
 * based on the gpp_no read from the secondary index record.
 *
 * @return        True and gpp offset if successful positioning, False otherwise
 */
std::pair<bool, ulint> btr_cur_guess_clust_by_gpp(
    dict_index_t *clust_idx, const dict_index_t *sec_idx,
    const dtuple_t *clust_ref, const Slice *sec_slc, btr_pcur_t *clust_pcur,
    page_cur_mode_t mode, ulint latch_mode,
    mtr_t *mtr) {
  ut_ad(sec_idx->n_s_gfields > 0);
  ut_ad(sec_slc && sec_slc->offsets()[1] == sec_idx->n_fields);
  ut_ad(mode == PAGE_CUR_LE || mode == PAGE_CUR_GE);
  ut_ad(latch_mode == BTR_SEARCH_LEAF || latch_mode == BTR_MODIFY_LEAF);
  ut_ad(sec_slc->is_positioned(sec_idx));
  ut_ad(sec_idx->table == nullptr || !sec_idx->table->is_compressed());

  /** Read gpp no from the sec rec. */
  bool clust_found = false;
  page_no_t gpp_no = FIL_NULL;
  ulint gpp_no_offset = ULINT_UNDEFINED;

  std::tie(gpp_no, gpp_no_offset) = row_get_gpp_no(sec_idx, sec_slc);
  ut_ad(gpp_no != 0);

  clust_pcur->m_search_mode = mode;
  clust_pcur->m_latch_mode = latch_mode;
  clust_pcur->m_old_stored = false;

  clust_found = btr_cur_guess_by_gpp_low(gpp_no, clust_idx, clust_ref,
                                         clust_pcur->get_btr_cur(), mode,
                                         latch_mode, mtr);

  if (clust_found) {
    clust_pcur->m_pos_state = BTR_PCUR_IS_POSITIONED;
    clust_pcur->m_btr_cur.index = clust_idx;
    clust_pcur->m_btr_cur.flag = BTR_CUR_BINARY;
  }

  sec_idx->gpp_stat(clust_found);

  return {clust_found, gpp_no_offset};
}

/**
 * Attempts to position a persistent cursor on a index record
 * based on the gpp_no.
 *
 * @param[in]     idx             Index
 * @param[in]     ref             Reference tuple for the index
 * @param[in,out] btr_cur         Cursor for the index
 * @param[in]     mode            search mode
 * @param[in]     latch_mode      latching mode
 * @param[in]     mtr             Mini-transaction handle
 *
 * @return        True if successful positioning, False otherwise
 */
bool btr_cur_guess_by_gpp_low(gpp_no_t gpp_no, dict_index_t *idx,
                              const dtuple_t *ref, btr_cur_t *btr_cur,
                              page_cur_mode_t mode, ulint latch_mode,
                              mtr_t *mtr) {
  ut_ad(mode == PAGE_CUR_LE || mode == PAGE_CUR_GE);
  ut_ad(latch_mode == BTR_SEARCH_LEAF || latch_mode == BTR_MODIFY_LEAF);

  bool found = false;
  ulint up_match = 0;
  ulint low_match = 0;
  buf_block_t *block = nullptr;
  ulint rw_latch = latch_mode;
  ulint cur_savepoint = 0;
  ulint savepoints[2];
  ulint n_savepoint = 0;

#ifdef UNIV_DEBUG
  DBUG_EXECUTE_IF("set_gpp_null", gpp_no = FIL_NULL;);
  DBUG_EXECUTE_IF("set_gpp_enum_page_in_space", gpp_no = dbug_gpp_no;);
  DBUG_EXECUTE_IF("set_gpp_random", {
    page_no_t space_size = 100;
    if (fil_space_t *space =
            fil_space_acquire_silent(dict_index_get_space(idx))) {
      space_size = space->size;

      fil_space_release(space);
    }
    std::srand(time(0));
    gpp_no = std::rand() % (2 * (space_size + 1));
  });
#endif /* UNIV_DEBUG */

  if (gpp_no == FIL_NULL || idx->table->is_compressed()) {
    goto func_exit;
  }

  /** Phase 1: fetch the page according to gpp_no. */
  /** Fetch the page in Page_fetch::IGNORE_MISSING mode because the page may
   * have already been freed or out of tablespace. */
  cur_savepoint = mtr_set_savepoint(mtr);
  if ((block = buf_page_get_gen(page_id_t{dict_index_get_space(idx), gpp_no},
                                dict_table_page_size(idx->table), RW_NO_LATCH,
                                nullptr, Page_fetch::IGNORE_MISSING_NOWAIT,
                                UT_LOCATION_HERE, mtr)) == nullptr) {
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
             idx->id /* page is not clust index page */
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

  /** Phase 2: search the clust rec in the target page. */
  page_cur_search_with_match(block, idx, ref, mode, &up_match, &low_match,
                             btr_cur_get_page_cur(btr_cur), nullptr);

  /** Phase 3: rec validation. */
  if (!page_rec_is_user_rec(btr_cur_get_rec((btr_cur)))) {
    goto func_exit;
  }

  /** Phase 4: match validation. */
  switch (mode) {
    case PAGE_CUR_LE:
      if (low_match < dtuple_get_n_fields_cmp(ref)) {
        goto func_exit;
      }
      break;
    case PAGE_CUR_GE:
      if (up_match < dtuple_get_n_fields_cmp(ref)) {
        goto func_exit;
      }
      break;
    default:
      ut_error;
  }

  found = true;

func_exit:
  if (!found) {
    btr_cur->index = nullptr;
    btr_cur->flag = BTR_CUR_UNSET;
    btr_cur->page_cur.rec = nullptr;
    btr_cur->page_cur.block = nullptr;

    /** Release the page if the found rec is mismatched. */
    for (ulint i = 0; i < n_savepoint; ++i) {
      mtr_release_block_at_savepoint(mtr, savepoints[i], block);
    }
  }

  return found;
}

/** Writes the redo log record for a delete mark setting of a normal secondary
 index record.
  @param[in]      rec        record
  @param[in]      val        value to set
  @param[in]      mtr        mtr
*/
void btr_cur_del_mark_set_ancient_sec_rec_log(rec_t *rec, /*!< in: record */
                                              bool val, /*!< in: value to set */
                                              mtr_t *mtr) /*!< in: mtr */
{
  byte *log_ptr = nullptr;

  if (!mlog_open(mtr, 11 + 1 + 2, log_ptr)) {
    /* Logging in mtr is switched off during crash recovery:
    in that case mlog_open returns false */
    return;
  }

  log_ptr = mlog_write_initial_log_record_fast(rec, MLOG_REC_SEC_DELETE_MARK,
                                               log_ptr, mtr);
  mach_write_to_1(log_ptr, val);
  log_ptr++;

  mach_write_to_2(log_ptr, page_offset(rec));
  log_ptr += 2;

  mlog_close(mtr, log_ptr);
}

/** Writes the redo log record for delete marking of a
  panda index record.
  @param[in]      rec        record
  @param[in]      index      index of the record
  @param[in]      trx_id     transaction id
  @param[in]      roll_ptr   roll ptr to the undo log record
  @param[in]      txn_rec    lizard info in the record
  @param[in]      mtr        mtr
  @param[in]      layout     txn layout
*/
static void btr_cur_del_mark_set_panda_sec_rec_log(
    rec_t *rec, dict_index_t *index, trx_id_t trx_id, roll_ptr_t roll_ptr,
    const txn_rec_t *txn_rec, mtr_t *mtr) {
  ut_ad(dict_index_is_panda(index));
  byte *log_ptr = nullptr;

  ut_ad(page_rec_is_comp(rec) == dict_table_is_comp(index->table));

  const bool opened = mlog_open_and_write_index(
      mtr, rec, index, MLOG_REC_SEC_PANDA_DELETE_MARK,
      1 + 1 + REDO_SYS_FIELDS_LEN + REDO_BAMBOO_FIELDS_LEN + 2, log_ptr);

  if (!opened) {
    /* Logging in mtr is switched off during crash recovery */
    return;
  }

  /** flag */
  *log_ptr++ = 0;
  /** value */
  *log_ptr++ = 1;

  log_ptr =
      row_upd_write_sys_vals_to_log(index, trx_id, roll_ptr, log_ptr, mtr);
  log_ptr =
      lizard::row_upd_write_bamboo_vals_to_log(index, txn_rec, log_ptr, mtr);

  mach_write_to_2(log_ptr, page_offset(rec));
  log_ptr += 2;

  mlog_close(mtr, log_ptr);
}

/** Writes the redo log record for delete marking of a
  secondary index rec with tid column.
  @param[in]      rec        record
  @param[in]      index      index of the record
  @param[in]      trx_id     transaction id
  @param[in]      trx_id_pos position of the trx_id in the record
  @param[in]      mtr        mtr
*/
void btr_cur_del_mark_set_hippo_sec_rec_log(rec_t *rec,
                                            const dict_index_t *index, bool val,
                                            trx_id_t trx_id, ulint trx_id_pos,
                                            mtr_t *mtr) {
  ut_ad(index->is_hippo());
  byte *log_ptr = nullptr;

  ut_ad(page_rec_is_comp(rec) == dict_table_is_comp(index->table));

  const bool opened =
      mlog_open_and_write_index(mtr, rec, index, MLOG_REC_SEC_HIPPO_DELETE_MARK,
                                1 + 1 + REDO_SYS_TID_LEN + 2, log_ptr);

  if (!opened) {
    /* Logging in mtr is switched off during crash recovery */
    return;
  }

  /** flag */
  *log_ptr++ = 0;
  /** value */
  *log_ptr++ = val;

  log_ptr += mach_write_compressed(log_ptr, trx_id_pos);

  log_ptr += mach_u64_write_compressed(log_ptr, trx_id);

  mach_write_to_2(log_ptr, page_offset(rec));
  log_ptr += 2;

  mlog_close(mtr, log_ptr);
}

/** Sets a secondary panda index record delete mark to true or false.
  @param[in]      layout     txn layout
  @param[in]      cursor     cursor
  @param[in]      thr        query thread
  @param[in]      flags      flags
  @param[in]      val        value to set
  @param[in]      mtr        mtr
 @return DB_SUCCESS, DB_LOCK_WAIT, or error number */
dberr_t btr_cur_del_mark_set_panda_sec_rec_low(const txn_layout_t &layout,
                                               btr_cur_t *cursor,
                                               que_thr_t *thr, ulint flags,
                                               bool val, mtr_t *mtr) {
  dberr_t err;
  roll_ptr_t roll_ptr;
  txn_rec_t txn_rec;
  Slice_ctx slc_ctx;
  Slice *slc;
  buf_block_t *block = btr_cur_get_block(cursor);
  reg_t reg = btr_cur_get_reg(cursor);
  page_zip_des_t *page_zip = buf_block_get_page_zip(block);
  trx_t *trx = thr_get_trx(thr);
  dict_index_t *index = cursor->index;

  ut_ad(trx_state_eq(trx, TRX_STATE_ACTIVE));
  ut_ad(page_rec_is_comp(reg.rec) == dict_table_is_comp(index->table));
  ut_ad(index->is_panda());
  ut_a(val == true); /* val must be true for panda. */
  ut_ad(thr);

  err = lock_sec_rec_modify_check_and_lock(flags, block, reg, index, thr, mtr);
  if (err != DB_SUCCESS) {
    return (err);
  }

  DBUG_PRINT("ib_cur",
             ("delete-mark=%u sec %u:%u:%u in %s(" IB_ID_FMT ") by " TRX_ID_FMT,
              unsigned(val), block->page.id.space(), block->page.id.page_no(),
              unsigned(page_rec_get_heap_no(reg.rec)), index->name(), index->id,
              trx_get_id_for_print(trx)));

  slc = slc_ctx.get_offsets(reg, index, ULINT_UNDEFINED);
  ut_ad(row_txn_is_valid(slc, index, layout));

  /** Lizard: Do the cleanout. */
  if (!(flags & BTR_NO_UNDO_LOG_FLAG)) {
    ut_ad(!index->table->is_intrinsic());
    txn_rec_cleanout_when_modify(trx->id, slc, index, layout, block, mtr);
  }

  err =
      trx_undo_report_row_operation(flags, layout, TRX_UNDO_MODIFY_OP, trx,
                                    index, nullptr, nullptr, 0, slc, &roll_ptr);
  if (err != DB_SUCCESS) {
    return (err);
  }

  btr_rec_set_deleted_flag(slc->rec(), page_zip, val);

  txn_rec.copy_from_txn_desc(trx);
  row_upd_rec_sys_fields(slc, page_zip, index, trx, roll_ptr);
  row_upd_rec_txn_fields(slc, page_zip, index, layout, &trx->txn_desc);

  btr_cur_del_mark_set_panda_sec_rec_log(slc->rec(), index, trx->id, roll_ptr,
                                         &txn_rec, mtr);
  commit_cleanout_collect(thr, cursor, slc->rec(), flags, layout);

  return (DB_SUCCESS);
}

/** Sets a secondary hippo index record delete mark to true or false.
  @param[in]      cursor     cursor
  @param[in]      thr        query thread
  @param[in]      flags      flags
  @param[in]      val        value to set
  @param[in]      sec_tid    secondary transaction id
  @param[in]      mtr        mtr
 @return DB_SUCCESS, DB_LOCK_WAIT, or error number */
dberr_t btr_cur_del_mark_set_hippo_sec_rec_low(btr_cur_t *cursor,
                                               que_thr_t *thr, ulint flags,
                                               bool val, trx_id_t sec_tid,
                                               mtr_t *mtr) {
  dberr_t err;
  Slice_ctx slc_ctx;
  Slice *slc;
  buf_block_t *block = btr_cur_get_block(cursor);
  reg_t reg = btr_cur_get_reg(cursor);
  dict_index_t *index = cursor->index;

  ut_ad(buf_block_get_page_zip(block) == nullptr);
  ut_ad(trx_state_eq(thr_get_trx(thr), TRX_STATE_ACTIVE) ||
        trx_state_eq(thr_get_trx(thr), TRX_STATE_PREPARED));
  /** sec_tid equals 0 iff in rollback. */
  ut_ad(sec_tid != 0 ||
        (thr_get_trx(thr)->in_rollback && (flags & BTR_NO_UNDO_LOG_FLAG)));
  ut_ad(page_rec_is_comp(reg.rec) == dict_table_is_comp(index->table));

  err = lock_sec_rec_modify_check_and_lock(flags, block, reg, index, thr, mtr);
  if (err != DB_SUCCESS) {
    return (err);
  }

  DBUG_PRINT("ib_cur",
             ("delete-mark=%u sec %u:%u:%u in %s(" IB_ID_FMT ") by " TRX_ID_FMT,
              unsigned(val), block->page.id.space(), block->page.id.page_no(),
              unsigned(page_rec_get_heap_no(reg.rec)), index->name(), index->id,
              trx_get_id_for_print(thr_get_trx(thr))));

  slc = slc_ctx.get_offsets(reg, index, ULINT_UNDEFINED);

  btr_rec_set_deleted_flag(reg.rec, nullptr, val);

  row_upd_rec_lotus_fields_low(slc, nullptr, index, sec_tid);

  HIPPO_ASSERT_TID_DML_REC(index, slc, thr);

  btr_cur_del_mark_set_hippo_sec_rec_log(slc->rec(), index, val, sec_tid,
                                         index->get_sys_col_pos(DATA_TRX_ID),
                                         mtr);

  return (DB_SUCCESS);
}

/** Sets a secondary ancient index record delete mark to true or false.
  @param[in]      cursor     cursor
  @param[in]      thr        query thread
  @param[in]      flags      flags
  @param[in]      val        value to set
  @param[in]      mtr        mtr
 @return DB_SUCCESS, DB_LOCK_WAIT, or error number */
dberr_t btr_cur_del_mark_set_ancient_sec_rec_low(btr_cur_t *cursor,
                                                 que_thr_t *thr, ulint flags,
                                                 bool val, mtr_t *mtr) {
  dberr_t err;
  buf_block_t *block = btr_cur_get_block(cursor);
  reg_t reg = btr_cur_get_reg(cursor);

  ut_ad(page_rec_is_comp(reg.rec) == dict_table_is_comp(cursor->index->table));

  err = lock_sec_rec_modify_check_and_lock(flags, block, reg, cursor->index,
                                           thr, mtr);
  if (err != DB_SUCCESS) {
    return (err);
  }

  DBUG_PRINT("ib_cur",
             ("delete-mark=%u sec %u:%u:%u in %s(" IB_ID_FMT ") by " TRX_ID_FMT,
              unsigned(val), block->page.id.space(), block->page.id.page_no(),
              unsigned(page_rec_get_heap_no(reg.rec)), cursor->index->name(),
              cursor->index->id, trx_get_id_for_print(thr_get_trx(thr))));

  /* We do not need to reserve search latch, as the delete-mark flag is being
   * updated in place and the adaptive hash index does not depend on it. */
  btr_rec_set_deleted_flag(reg.rec, buf_block_get_page_zip(block), val);
  btr_cur_del_mark_set_ancient_sec_rec_log(reg.rec, val, mtr);

  return (DB_SUCCESS);
}

/** Parses the redo log record for delete marking or unmarking of a panda
 index record.
  @param[in]      ptr         buffer
  @param[in]      end_ptr     buffer end
  @param[in,out]  page        page or NULL
  @param[in,out]  page_zip    compressed page, or NULL
  @param[in]      index       index corresponding to page
 @return end of log record or NULL */
byte *btr_cur_parse_del_mark_set_panda_sec_rec(byte *ptr, byte *end_ptr,
                                               page_t *page,
                                               page_zip_des_t *page_zip,
                                               dict_index_t *index) {
  ulint pos;
  trx_id_t trx_id;
  roll_ptr_t roll_ptr;
  ulint offset;
  reg_t reg;
  ulint txn_pos;
  scn_t scn;
  undo_ptr_t undo_ptr;

  ut_ad(!page || page_is_comp(page) == dict_table_is_comp(index->table));

  if (end_ptr < ptr + 2) {
    return (nullptr);
  }

  auto flags = mach_read_from_1(ptr);
  ptr++;
  auto val = mach_read_from_1(ptr);
  ptr++;

  ptr = row_upd_parse_sys_vals(ptr, end_ptr, &pos, &trx_id, &roll_ptr);

  if (ptr == nullptr) {
    return (nullptr);
  }

  ptr = lizard::row_upd_parse_bamboo_vals(ptr, end_ptr, &txn_pos, &scn,
                                          &undo_ptr);

  if (ptr == NULL) {
    return (NULL);
  }

  if (end_ptr < ptr + 2) {
    return (nullptr);
  }

  offset = mach_read_from_2(ptr);
  ptr += 2;

  ut_a(offset <= UNIV_PAGE_SIZE);

  if (page) {
    reg = {page + offset, MLOC_IN_PAGE};

    /* We do not need to reserve search latch, as the page
    is only being recovered, and there cannot be a hash index to
    it. Besides, these fields are being updated in place
    and the adaptive hash index does not depend on them. */

    btr_rec_set_deleted_flag(reg.rec, page_zip, val);

    if (!(flags & BTR_KEEP_SYS_FLAG)) {
      Slice_ctx slc_ctx;
      Slice *slc = slc_ctx.get_offsets(reg, index, ULINT_UNDEFINED);

      row_upd_rec_sys_fields_in_recovery(slc, page_zip, pos, trx_id, roll_ptr);

      lizard::row_upd_rec_bamboo_fields_in_recovery(slc, page_zip, index,
                                                    txn_pos, scn, undo_ptr);
    }
  }

  return (ptr);
}


/** Parses the redo log record for delete marking or unmarking of a
  secondary index rec with tid column.
  @param[in]      ptr         buffer
  @param[in]      end_ptr     buffer end
  @param[in,out]  page        page or NULL
  @param[in,out]  page_zip    compressed page, or NULL
  @param[in]      index       index corresponding to page
 @return end of log record or NULL */
byte *btr_cur_parse_del_mark_set_hippo_sec_rec(byte *ptr, byte *end_ptr,
                                               page_t *page,
                                               page_zip_des_t *page_zip,
                                               dict_index_t *index) {
  ulint pos;
  trx_id_t rec_tid;
  ulint offset;
  reg_t reg;

  ut_ad(!page || page_is_comp(page) == dict_table_is_comp(index->table));
  ut_ad(index->is_hippo());

  if (end_ptr < ptr + 2) {
    return (nullptr);
  }

  auto flags = mach_read_from_1(ptr);
  ptr++;
  auto val = mach_read_from_1(ptr);
  ptr++;
  ut_a(flags == 0);

  ptr = row_upd_parse_lotus_vals(ptr, end_ptr, &pos, &rec_tid);

  if (ptr == nullptr) {
    return (nullptr);
  }

  if (end_ptr < ptr + 2) {
    return (nullptr);
  }

  offset = mach_read_from_2(ptr);
  ptr += 2;

  ut_a(offset <= UNIV_PAGE_SIZE);

  if (page) {
    reg = {page + offset, MLOC_IN_PAGE};

    /* We do not need to reserve search latch, as the page
    is only being recovered, and there cannot be a hash index to
    it. Besides, these fields are being updated in place
    and the adaptive hash index does not depend on them. */

    btr_rec_set_deleted_flag(reg.rec, page_zip, val);

    Slice_ctx slc_ctx;
    Slice *slc = slc_ctx.get_offsets(reg, index, ULINT_UNDEFINED);

    row_upd_rec_lotus_fields_in_recovery(slc, page_zip, pos, rec_tid);
  }

  return (ptr);
}

void btr_cur_print_duplicate_error_in_uk_online(btr_cur_t *btr_cur,
                                                const dict_index_t *index,
                                                const dtuple_t *entry,
                                                ut::Location loc) {
  rec_t *rec;
  std::stringstream ss;

  ut_ad(dict_index_is_unique(index));

  ss << "[UK DDL Duplicate] [";
  loc.print(ss);
  ss << "],table=" << index->table_name << ", index=" << index->name << ", "
     << (dict_index_is_panda(index) ? "panda index" : "normal uk")
     << ", low_match=" << btr_cur->low_match
     << ", up_match=" << btr_cur->up_match << ", entry=";
  entry->print(ss);

  rec = btr_cur_get_rec(btr_cur);
  ss << ", cursor_rec is ";
  if (page_rec_is_user_rec(rec)) {
    ss << "user rec";
  } else if (page_rec_is_supremum(rec)) {
    ss << "supremum rec";
  } else {
    ss << "infimum rec";
  }
  if (rec_get_deleted_flag(rec, dict_table_is_comp(index->table))) {
    ss << "(del-mark)";
  }

  if (page_rec_get_next(rec)) {
    rec = page_rec_get_next(rec);
    ss << ", next rec is ";
    if (page_rec_is_user_rec(rec)) {
      ss << "user rec";
    } else if (page_rec_is_supremum(rec)) {
      ss << "supremum rec";
    } else {
      ss << "infimum rec";
    }
    if (rec_get_deleted_flag(rec, dict_table_is_comp(index->table))) {
      ss << "(del-mark)";
    }
  }
  lizard_warn(ER_LIZARD) << ss.str();
}

/**
  Perform an insert operation by writing row log for the creating index.
  It is assumed that the S-latch or SX-latch or X-latch of the index has been
  hold. Like btr_cur_optimistic_insert, the undo is also generated if needed
  before writing row log. The operation dose not succeed if there is too
  little space for undo or writing row log failed. In such case, the creating
  index will be marked as corrupted, instead of return ERROR.

  @param[in/out]  index   index that is creating
  @param[in]      trx     transaction
  @param[in/out]  entry   The entry to be inserted
  @param[in]      flags   care flags: BTR_NO_UNDO_LOG_FLAG, BTR_KEEP_SYS_FLAG
  @param[in]      layout  TL_BAMBOO or TL_NONE or TL_LOTUS
*/
void btr_cur_rlog_insert(dict_index_t *index, trx_t *trx, dtuple_t *entry,
                         ulint flags, const txn_layout_t &layout) {
  dberr_t err = DB_SUCCESS;
  roll_ptr_t roll_ptr;

  urec_trx_t urec_trx;

  ut_ad(rw_lock_own_flagged(dict_index_get_lock(index),
                            RW_LOCK_FLAG_S | RW_LOCK_FLAG_X | RW_LOCK_FLAG_SX));
  ut_ad(!index->is_clustered());

  if (index->is_corrupted()) {
    return;
  }

  urec_trx.build_for_rlog_ins();

  if (dict_index_is_panda(index)) {
    err = trx_undo_report_rlog_operation(flags, layout, TRX_UNDO_INSERT_OP, trx,
                                         index, entry, nullptr, 0, nullptr,
                                         &urec_trx, &roll_ptr);

    if (err != DB_SUCCESS) {
      row_log_set_error(index, err);
      return;
    }

    if (!(flags & BTR_KEEP_SYS_FLAG)) {
      row_upd_index_entry_sys_field(entry, index, DATA_TRX_ID, trx->id);
      row_upd_index_entry_sys_field(entry, index, DATA_ROLL_PTR, roll_ptr);
      row_log_entry_update_txn_field(entry, index, trx);
    }
  }

  row_log_online_op(index, entry, trx->id);
}

/**
  Perform an delete operation by writing row log for the creating index.
  It is assumed that the S-latch or SX-latch or X-latch of the index has been
  hold. Like btr_cur_optimistic_update, the undo is also generated if needed
  before writing row log. The operation dose not succeed if there is too
  little space for undo or writing row log failed. In such case, the creating
  index will be marked as corrupted, instead of return ERROR.

  @param[in/out]  index   index that is creating
  @param[in]      trx     transaction
  @param[in/out]  entry   The entry to be deleted
  @param[in]      flags   care flags: BTR_NO_UNDO_LOG_FLAG, BTR_KEEP_SYS_FLAG
  @param[in]      layout  TL_BAMBOO or TL_NONE or TL_LOTUS
*/
void btr_cur_rlog_delete(dict_index_t *index, trx_t *trx, dtuple_t *entry,
                         ulint flags, const txn_layout_t &layout) {
  dberr_t err;
  dfield_t *dfield;
  byte *field;
  ulint pos;

  roll_ptr_t roll_ptr;
  urec_trx_t urec_trx;

  ut_ad(rw_lock_own_flagged(dict_index_get_lock(index),
                            RW_LOCK_FLAG_S | RW_LOCK_FLAG_X | RW_LOCK_FLAG_SX));
  ut_ad(!index->is_clustered());

  if (index->is_corrupted()) {
    return;
  }

  /** Build "prev version" of system columns for delete op */
  if (dict_index_is_panda(index)) {
    if (!(flags & BTR_NO_UNDO_LOG_FLAG)) {
      ut_ad(trx);

      pos = index->get_sys_col_pos(DATA_TRX_ID);
      ut_ad(index->get_field(pos)->col ==
            index->table->get_sys_col(DATA_TRX_ID));
      dfield = dtuple_get_nth_field(entry, pos);
      field = static_cast<byte *>(dfield_get_data(dfield));
      urec_trx.trx_id = trx_read_trx_id(field);
      pos++;

      ut_ad(index->get_field(pos)->col ==
            index->table->get_sys_col(DATA_ROLL_PTR));
      urec_trx.roll_ptr = ROLL_PTR_SEC_DDL;
      pos++;

      ut_ad(index->get_field(pos)->col ==
            index->table->get_sys_col(DATA_SCN_ID));
      dfield = dtuple_get_nth_field(entry, pos);
      field = static_cast<byte *>(dfield_get_data(dfield));
      urec_trx.scn = trx_read_scn(field);
      pos++;

      ut_ad(index->get_field(pos)->col ==
            index->table->get_sys_col(DATA_UNDO_PTR));
      dfield = dtuple_get_nth_field(entry, pos);
      field = static_cast<byte *>(dfield_get_data(dfield));
      urec_trx.undo_ptr = trx_read_undo_ptr(field);
    }

    err = trx_undo_report_rlog_operation(flags, layout, TRX_UNDO_MODIFY_OP, trx,
                                         index, entry, nullptr, 0, nullptr,
                                         &urec_trx, &roll_ptr);

    if (err != DB_SUCCESS) {
      row_log_set_error(index, err);
      return;
    }

    if (!(flags & BTR_KEEP_SYS_FLAG)) {
      row_upd_index_entry_sys_field(entry, index, DATA_TRX_ID, trx->id);
      row_upd_index_entry_sys_field(entry, index, DATA_ROLL_PTR, roll_ptr);
      row_log_entry_update_txn_field(entry, index, trx);
    }
  }

  /* pass trx_id as 0 mean that it's delete. */
  row_log_online_op(index, entry, 0);
}

/**
  Perform an insert operation by writing row log for the creating index.
  See **btr_cur_rlog_insert**.

  @return false if the index is completed so actually insert on the tree is
          needed.
*/
bool btr_cur_rlog_insert_try(dict_index_t *index, trx_t *trx, dtuple_t *entry,
                             ulint flags, const txn_layout_t &layout) {
  ut_ad(rw_lock_own_flagged(dict_index_get_lock(index),
                            RW_LOCK_FLAG_S | RW_LOCK_FLAG_X | RW_LOCK_FLAG_SX));

  switch (dict_index_get_online_status(index)) {
    case ONLINE_INDEX_COMPLETE:
      /* This is a normal index. Do not log anything.
      The caller must perform the operation on the
      index tree directly. */
      return (false);
    case ONLINE_INDEX_CREATION:
      /* The index is being created online. Log the
      operation. */
      btr_cur_rlog_insert(index, trx, entry, flags, layout);
      break;
    case ONLINE_INDEX_ABORTED:
    case ONLINE_INDEX_ABORTED_DROPPED:
      /* The index was created online, but the operation was
      aborted. Do not log the operation and tell the caller
      to skip the operation. */
      break;
  }

  return (true);
}

/**
  Perform an insert operation by writing row log for the creating index.
  See **btr_cur_rlog_delete**.

  @return false if the index is completed so actually delete on the tree is
          needed.
*/
bool btr_cur_rlog_delete_try(dict_index_t *index, trx_t *trx, dtuple_t *entry,
                             ulint flags, const txn_layout_t &layout) {
  ut_ad(rw_lock_own_flagged(dict_index_get_lock(index),
                            RW_LOCK_FLAG_S | RW_LOCK_FLAG_X | RW_LOCK_FLAG_SX));

  switch (dict_index_get_online_status(index)) {
    case ONLINE_INDEX_COMPLETE:
      /* This is a normal index. Do not log anything.
      The caller must perform the operation on the
      index tree directly. */
      return (false);
    case ONLINE_INDEX_CREATION:
      /* The index is being created online. Log the
      operation. */
      btr_cur_rlog_delete(index, trx, entry, flags, layout);
      break;
    case ONLINE_INDEX_ABORTED:
    case ONLINE_INDEX_ABORTED_DROPPED:
      /* The index was created online, but the operation was
      aborted. Do not log the operation and tell the caller
      to skip the operation. */
      break;
  }

  return (true);
}

}  // namespace lizard
