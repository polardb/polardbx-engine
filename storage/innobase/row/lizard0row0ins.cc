
#include "sql/debug_sync.h"

#include "btr0cur.h"
#include "que0que.h"
#include "row0log.h"
#include "lock0lock.h"

#include "lizard0dict.h"
#include "lizard0row.h"
#include "lizard0row0ins.h"
#include "lizard0btr0cur.h"

extern bool row_allow_duplicates(que_thr_t *thr);

extern dberr_t row_ins_set_rec_lock(lock_mode mode, ulint type,
                                    const buf_block_t *block, const rec_t *rec,
                                    dict_index_t *index, const ulint *offsets,
                                    que_thr_t *thr);

extern bool row_ins_dupl_error_with_rec(
    const rec_t *rec,      /*!< in: user record; NOTE that we assume
                           that the caller already has a record lock on
                           the record! */
    const dtuple_t *entry, /*!< in: entry to insert */
    dict_index_t *index,   /*!< in: index */
    const ulint *offsets); /*!< in: rec_get_offsets(rec, index) */

extern dberr_t row_ins_sec_index_entry_by_modify(
    ulint flags,                /*!< in: undo logging and locking flags */
    const txn_layout_t &layout, /*!< in: txn layout */
    ulint mode,                 /*!< in: BTR_MODIFY_LEAF or BTR_MODIFY_TREE,
                                depending on whether mtr holds just a leaf
                                latch or also a tree latch */
    btr_cur_t *cursor,          /*!< in: B-tree cursor */
    ulint **offsets,            /*!< in/out: offsets on cursor->page_cur.rec */
    mem_heap_t *offsets_heap,
    /*!< in/out: memory heap that can be emptied */
    mem_heap_t *heap,      /*!< in/out: memory heap */
    const dtuple_t *entry, /*!< in: index entry to insert */
    que_thr_t *thr,        /*!< in: query thread */
    mtr_t *mtr);           /*!< in: mtr; must be committed before
                            latching any further pages */

namespace lizard {

static inline bool row_ins_panda_must_modify_rec(
    const btr_cur_t *cursor, const dtuple_t *entry) {
  ut_ad(dict_index_is_panda(cursor->index));

  if (!row_index_entry_contains_null_in_unique(cursor->index, entry)) {
    return (cursor->low_match >= dict_index_get_n_unique(cursor->index) &&
            !page_rec_is_infimum(btr_cur_get_rec(cursor)));
  }

  return (cursor->low_match >= dict_index_get_n_unique_in_tree(cursor->index) &&
          !page_rec_is_infimum(btr_cur_get_rec(cursor)));
}

[[nodiscard]] static dberr_t row_ins_panda_duplicate_error(
    ulint flags, btr_cur_t *cursor, const dtuple_t *entry, que_thr_t *thr,
    mtr_t *mtr) {
  ulint n_unique;
  rec_t *rec;
  dict_index_t *index;
  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);
  trx_t *trx = thr_get_trx(thr);
  dberr_t err;

  UT_NOT_USED(mtr);

  index = cursor->index;

  /** TODO: It seems impossible that index->nulls_equal is true */
  ut_ad(!index->nulls_equal);
  if (row_index_entry_contains_null_in_unique(index, entry)) {
    return DB_SUCCESS;
  }

  n_unique = dict_index_get_n_unique(index);
  ut_ad(dtuple_get_n_fields_cmp(entry) == n_unique);

  if (cursor->low_match >= n_unique) {
    rec = btr_cur_get_rec(cursor);

    if (!page_rec_is_infimum(rec)) {
      offsets = rec_get_offsets(rec, cursor->index, offsets, ULINT_UNDEFINED,
                                UT_LOCATION_HERE, &heap);

      /* We set a lock on the possible duplicate: this
      is needed in logical logging of MySQL to make
      sure that in roll-forward we get the same duplicate
      errors as in original execution */

      if (flags & BTR_NO_LOCKING_FLAG) {
        /* Do nothing if no-locking is set */
        err = DB_SUCCESS;

        /**
         * Lizard: Currently, there are 3 scenarios that can be encountered
         * here:
         * 1. `table->is_intrinsic()`
         * 2. `table->is_temporary()`
         * 3. `table->skip_alter_undo()` (for intermediate tables during ALTER
         * TABLE copy)
         *
         * Note: The first two scenarios cannot occur because temporary tables
         * do not support the Panda index.
         */
        ut_ad(!index->table->is_intrinsic() && !index->table->is_temporary());

      } else {
        /* If the SQL-query will update or replace
        duplicate key we will take X-lock for
        duplicates ( REPLACE, LOAD DATAFILE REPLACE,
        INSERT ON DUPLICATE KEY UPDATE). */

        err = row_ins_set_rec_lock(row_allow_duplicates(thr) ? LOCK_X : LOCK_S,
                                   LOCK_REC_NOT_GAP, btr_cur_get_block(cursor),
                                   rec, cursor->index, offsets, thr);
      }

      switch (err) {
        case DB_SUCCESS_LOCKED_REC:
        case DB_SUCCESS:
          break;
        default:
          goto func_exit;
      }

      if (row_ins_dupl_error_with_rec(rec, entry, cursor->index, offsets)) {
      duplicate:
        trx->error_index = cursor->index;
        err = DB_DUPLICATE_KEY;
        goto func_exit;
      }
    }
  }

  if (cursor->up_match >= n_unique) {
    /** TODO: Don't know how to come into it. <26-09-24, zanye.zjy> */
    ut_error;

    rec = page_rec_get_next(btr_cur_get_rec(cursor));

    if (!page_rec_is_supremum(rec)) {
      offsets = rec_get_offsets(rec, cursor->index, offsets, ULINT_UNDEFINED,
                                UT_LOCATION_HERE, &heap);

      /* If the SQL-query will update or replace
      duplicate key we will take X-lock for
      duplicates ( REPLACE, LOAD DATAFILE REPLACE,
      INSERT ON DUPLICATE KEY UPDATE). */

      err = row_ins_set_rec_lock(row_allow_duplicates(thr) ? LOCK_X : LOCK_S,
                                 LOCK_REC_NOT_GAP, btr_cur_get_block(cursor),
                                 rec, cursor->index, offsets, thr);

      switch (err) {
        case DB_SUCCESS_LOCKED_REC:
        case DB_SUCCESS:
          break;
        default:
          goto func_exit;
      }

      if (row_ins_dupl_error_with_rec(rec, entry, cursor->index, offsets)) {
        goto duplicate;
      }
    }

    /* This should never happen */
    ut_error;
  }

  err = DB_SUCCESS;
func_exit:
  if (UNIV_LIKELY_NULL(heap)) {
    mem_heap_free(heap);
  }
  return (err);
}

dberr_t row_ins_panda_sec_index_entry_low(
    uint32_t flags, const txn_layout_t &layout, ulint mode, dict_index_t *index,
    mem_heap_t *offsets_heap, mem_heap_t *heap, dtuple_t *entry,
    trx_id_t trx_id, que_thr_t *thr, bool dup_chk_only) {
  DBUG_TRACE;

  btr_cur_t cursor;
  ulint search_mode = mode;
  dberr_t err = DB_SUCCESS;
  ulint n_unique;
  mtr_t mtr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  rec_offs_init(offsets_);
  rtr_info_t rtr_info;

  ut_ad(!index->is_clustered());
  ut_ad(layout == txn_layout_t::TL_BAMBOO);
  ut_ad(mode == BTR_MODIFY_LEAF || mode == BTR_MODIFY_TREE);
  ut_ad(!index->table->is_intrinsic());
  /** Seems that dup_chk_only = true only hanppen on intrinsic table. */
  ut_ad(!dup_chk_only);

  /** Lizard: There is no SCN and UBA in secondary index */
  assert_lizard_dict_index_check(index);

  cursor.thr = thr;
  cursor.rtr_info = nullptr;
  ut_ad(thr_get_trx(thr)->id != 0);

  mtr_start(&mtr);

  ut_ad(!index->table->is_temporary());
  ut_ad(!dict_index_is_spatial(index));

  lizard::Panda_entry_cmp_adjust_guard panda_cmp_adjust_guard(index, entry);

  /* Ensure that we acquire index->lock when inserting into an
  index with index->online_status == ONLINE_INDEX_COMPLETE, but
  could still be subject to rollback_inplace_alter_table().
  This prevents a concurrent change of index->online_status.
  The memory object cannot be freed as long as we have an open
  reference to the table, or index->table->n_ref_count > 0. */
  bool check = !index->is_committed();

  DBUG_EXECUTE_IF("idx_mimic_not_committed", {
    check = true;
    mode = BTR_MODIFY_TREE;
  });

  if (check) {
    DEBUG_SYNC(thr_get_trx(thr)->mysql_thd, "row_ins_sec_index_enter");
    if (mode == BTR_MODIFY_LEAF) {
      search_mode |= BTR_ALREADY_S_LATCHED;
      mtr_s_lock(dict_index_get_lock(index), &mtr, UT_LOCATION_HERE);
    } else {
      mtr_sx_lock(dict_index_get_lock(index), &mtr, UT_LOCATION_HERE);
    }

    ut_ad(!(flags & (BTR_KEEP_SYS_FLAG | BTR_NO_UNDO_LOG_FLAG)));
    if (btr_cur_rlog_insert_try(index, thr_get_trx(thr), entry, flags,
                                layout)) {
      goto func_exit;
    }
  }

  /* Note that we use PAGE_CUR_LE as the search mode, because then
  the function will return in both low_match and up_match of the
  cursor sensible values */

  /**
    Lizard:

    BTR_IGNORE_SEC_UNIQUE will try to use change buffer for unique key. But
    Panda Index can not use change buffer because that we want an accurate
    uniqueness constraint judgment, and there might be much assertions.

    It's OK becuase the duplicate check error can also happen when
    thr_get_trx(thr)->check_unique_secondary == false, (SET unique_checks = 0;)
    and the change buffer is not used.
  */
  // search_mode |= BTR_INSERT;
  // if (!thr_get_trx(thr)->check_unique_secondary) {
  //   search_mode |= BTR_IGNORE_SEC_UNIQUE;
  // }

  btr_cur_search_to_nth_level(index, 0, entry, PAGE_CUR_LE, search_mode,
                              &cursor, 0, __FILE__, __LINE__, &mtr);

  /** Panda Index can not use change buffer */
  ut_ad(cursor.flag != BTR_CUR_INSERT_TO_IBUF);

#ifdef UNIV_DEBUG
  {
    page_t *page = btr_cur_get_page(&cursor);
    rec_t *first_rec = page_rec_get_next(page_get_infimum_rec(page));

    ut_ad(page_rec_is_supremum(first_rec) ||
          rec_n_fields_is_sane(index, first_rec, entry));
  }
#endif /* UNIV_DEBUG */

  /** Only intrinsic can be allow_duplicates */
  ut_ad(!index->allow_duplicates);

  n_unique = dict_index_get_n_unique(index);

  if ((cursor.up_match >= n_unique || cursor.low_match >= n_unique)) {

    err = row_ins_panda_duplicate_error(flags, &cursor, entry, thr, &mtr);

    switch (err) {
      case DB_SUCCESS:
        break;
      case DB_DUPLICATE_KEY:
        if (!index->is_committed()) {
          ut_ad(!thr_get_trx(thr)->dict_operation_lock_mode);

          dict_set_corrupted(index);
          /* Do not return any error to the
          caller. The duplicate will be reported
          by ALTER TABLE or CREATE UNIQUE INDEX.
          Unfortunately we cannot report the
          duplicate key value to the DDL thread,
          because the altered_table object is
          private to its call stack. */
          err = DB_SUCCESS;
        }
        [[fallthrough]];
      default:
        ut_ad(!dict_index_is_spatial(index));
        goto func_exit;
    }
  }

  if (row_ins_panda_must_modify_rec(&cursor, entry)) {
    /* If the existing record is being modified and the new record
    is doesn't fit the provided slot then existing record is added
    to free list and new record is inserted. This also means
    cursor that we have cached for SELECT is now invalid. */
    if (index->last_sel_cur) {
      index->last_sel_cur->invalid = true;
    }

    /* There is already an index entry with a long enough common
    prefix, we must convert the insert into a modify of an
    existing record */
    offsets = rec_get_offsets(btr_cur_get_rec(&cursor), index, offsets,
                              ULINT_UNDEFINED, UT_LOCATION_HERE, &offsets_heap);

    err = row_ins_sec_index_entry_by_modify(flags, layout, mode, &cursor,
                                            &offsets, offsets_heap, heap, entry,
                                            thr, &mtr);

  } else {
    rec_t *insert_rec;
    big_rec_t *big_rec;

    if (mode == BTR_MODIFY_LEAF) {
      err = btr_cur_optimistic_insert(flags, layout, &cursor, &offsets,
                                      &offsets_heap, entry, &insert_rec,
                                      &big_rec, thr, &mtr);
    } else {
      ut_ad(mode == BTR_MODIFY_TREE);
      if (buf_LRU_buf_pool_running_out()) {
        err = DB_LOCK_TABLE_FULL;
        goto func_exit;
      }

      err = btr_cur_optimistic_insert(flags, layout, &cursor, &offsets,
                                      &offsets_heap, entry, &insert_rec,
                                      &big_rec, thr, &mtr);
      if (err == DB_FAIL) {
        err = btr_cur_pessimistic_insert(flags, layout, &cursor, &offsets,
                                         &offsets_heap, entry, &insert_rec,
                                         &big_rec, thr, &mtr);
      }
    }

    if (err == DB_SUCCESS && trx_id) {
      page_update_max_trx_id(btr_cur_get_block(&cursor),
                             btr_cur_get_page_zip(&cursor), trx_id, &mtr);
    }

    ut_ad(!big_rec);
  }

func_exit:
  mtr_commit(&mtr);
  DEBUG_SYNC_C("row_ins_panda_entry_inserted"); 
  return err;
}
}
