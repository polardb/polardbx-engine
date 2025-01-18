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
#include "trx0rec.h"
#include "row0upd.h"

#include "lizard0btr0cur.h"
#include "lizard0dbg.h"
#include "lizard0dict.h"
#include "lizard0dict0mem.h"
#include "lizard0fil0types.h"
#include "lizard0mtr0log.h"
#include "lizard0row.h"
#include "lizard0row0bamboo.h"
#include "lizard0row0clover.h"
#include "lizard0row0gpp.h"
#include "lizard0undo0rec0types.h"
#include "lizard0undo.h"

namespace lizard {
/**
 * Attempts to position a persistent cursor on a clustered index record
 * based on the gpp_no read from the secondary index record.
 *
 * @return        True and gpp offset if successful positioning, False otherwise
 */
std::pair<bool, ulint> btr_cur_guess_clust_by_gpp(
    dict_index_t *clust_idx, const dict_index_t *sec_idx,
    const dtuple_t *clust_ref, const rec_t *sec_rec, btr_pcur_t *clust_pcur,
    const ulint *sec_offsets, ulint latch_mode, mtr_t *mtr) {
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
  ulint gpp_no_offset = ULINT_UNDEFINED;

  std::tie(gpp_no, gpp_no_offset) =
      row_get_gpp_no(sec_rec, sec_idx, sec_offsets);
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

  if (gpp_no == FIL_NULL || clust_idx->table->is_compressed()) {
    goto func_exit;
  }

  /** Phase 2: fetch the page according to gpp_no. */
  /** Fetch the page in Page_fetch::IGNORE_MISSING mode because the page may
   * have already been freed or out of tablespace. */
  cur_savepoint = mtr_set_savepoint(mtr);
  if ((block =
           buf_page_get_gen(page_id_t{dict_index_get_space(clust_idx), gpp_no},
                            dict_table_page_size(clust_idx->table), RW_NO_LATCH,
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

  return {clust_found, gpp_no_offset};
}

/** Writes the redo log record for a delete mark setting of a normal secondary
 index record.
  @param[in]      rec        record
  @param[in]      val        value to set
  @param[in]      mtr        mtr
*/
static inline void btr_cur_del_mark_set_ancient_sec_rec_log(
    rec_t *rec, /*!< in: record */
    bool val,   /*!< in: value to set */
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

/** Writes the redo log record for a delete mark setting of
  secondary index record.
  @param[in,out]  rec        record
  @param[in]      val        value to set
  @param[in]      index      index of the record
  @param[in]      trx_id     transaction id
  @param[in]      roll_ptr   roll ptr to the undo log record
  @param[in]      txn_rec    txn record info
  @param[in]      offsets    offsets of the record
  @param[in]      layout     txn layout
  @param[in]      page_zip   compressed page
  @param[in]      mtr        mtr
*/
void btr_cur_del_mark_set_sec_rec_log(rec_t *rec, bool val, dict_index_t *index,
                                      trx_id_t trx_id, roll_ptr_t roll_ptr,
                                      const txn_rec_t *txn_rec, ulint *offsets,
                                      const txn_layout_t &layout,
                                      page_zip_des_t *page_zip, mtr_t *mtr) {
  ut_ad(layout == TL_NONE || layout == TL_BAMBOO);
  switch (layout) {
    case TL_NONE:
      btr_cur_del_mark_set_ancient_sec_rec_log(rec, val, mtr);
      break;
    case TL_BAMBOO:
      btr_cur_del_mark_set_panda_sec_rec_log(rec, index, trx_id, roll_ptr,
                                             txn_rec, mtr);
      break;
    default:
      ut_error;
  }
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
  rec_t *rec;
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
    rec = page + offset;

    /* We do not need to reserve search latch, as the page
    is only being recovered, and there cannot be a hash index to
    it. Besides, these fields are being updated in place
    and the adaptive hash index does not depend on them. */

    btr_rec_set_deleted_flag(rec, page_zip, val);

    if (!(flags & BTR_KEEP_SYS_FLAG)) {
      mem_heap_t *heap = nullptr;
      ulint offsets_[REC_OFFS_NORMAL_SIZE];
      ulint *offsets = offsets_;
      rec_offs_init(offsets_);
      offsets = rec_get_offsets(rec, index, offsets_, ULINT_UNDEFINED,
                                UT_LOCATION_HERE, &heap);

      row_upd_rec_sys_fields_in_recovery(rec, page_zip, offsets, pos, trx_id,
                                         roll_ptr);

      lizard::row_upd_rec_bamboo_fields_in_recovery(
          rec, page_zip, index, txn_pos, offsets, scn, undo_ptr);

      /** TODO: Check it */
      if (UNIV_LIKELY_NULL(heap)) {
        mem_heap_free(heap);
      }
    }
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
  @param[in]      layout  TL_BAMBOO or TL_NONE
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
                                         nullptr, &urec_trx, &roll_ptr);

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
  @param[in]      layout  TL_BAMBOO or TL_NONE
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
                                         nullptr, &urec_trx, &roll_ptr);

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
