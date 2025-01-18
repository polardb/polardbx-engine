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

/** @file include/lizard0row0umod.cc
 Lizard row undo modify implementation.

 Created 2024-10-09 by Yichang Song
 *******************************************************/

#include "current_thd.h"
#include "debug_sync.h"
#include "dict0dd.h"
#include "log0chkp.h"
#include "que0que.h"
#include "row0undo.h"
#include "row0upd.h"
#include "row0vers.h"
#include "trx0rec.h"

#include "lizard0btr0cur.h"
#include "lizard0dbg.h"
#include "lizard0row.h"
#include "lizard0row0umod.h"
#include "lizard0row0undo.h"
#include "lizard0row0vers.h"
#include "lizard0undo.h"
#include "lizard0btr0cur.h"

namespace lizard {

[[nodiscard]] static dberr_t row_undo_mod_panda_rec_low(
    ulint mode, mem_heap_t **offsets_heap,
    /*!< in/out: memory heap that can be emptied */
    dict_index_t *index, que_thr_t *thr, undo_node_t *node) {
  btr_pcur_t *pcur;
  mtr_t mtr;
  btr_cur_t *btr_cur;
  dberr_t err = DB_SUCCESS;
  bool modify_leaf = (mode == BTR_MODIFY_LEAF);
  rec_t *rec;
  big_rec_t *dummy_big_rec;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets;
  rec_offs_init(offsets_);
  const txn_layout_t layout = node->layout;

  ut_ad(thr_get_trx(thr) == node->trx);
  ut_ad(node->trx->in_rollback);

  log_free_check();
  pcur = &node->pcur;
  mtr_start(&mtr);

  dict_disable_redo_if_temporary(index->table, &mtr);

  if (!index->is_committed()) {
    if (modify_leaf) {
      mode = BTR_MODIFY_LEAF | BTR_ALREADY_S_LATCHED;
      mtr_s_lock(dict_index_get_lock(index), &mtr, UT_LOCATION_HERE);
    } else {
      ut_ad(mode & BTR_MODIFY_TREE);
      mtr_sx_lock(dict_index_get_lock(index), &mtr, UT_LOCATION_HERE);
    }

    /* Log an INSERT. */
    if (btr_cur_rlog_insert_try(index, thr_get_trx(thr), node->ref,
                                BTR_NO_UNDO_LOG_FLAG | BTR_KEEP_SYS_FLAG,
                                layout)) {
      ut_a(node->is_rlog);
      ut_a(node->rec_type == TRX_UNDO_DEL_MARK_REC);
      mtr_commit(&mtr);
      return DB_SUCCESS;
    }
  }

  if (node->is_rlog) {
    ut_ad(!index->is_committed());
    /* Deal with (!index->is_committed() &&
     * index->online_status==ONLINE_INDEX_CREATION) */
    lizard::row_search_on_row_ref_for_panda(&node->pcur, mode, index, node->ref,
                                            &mtr);
    btr_cur = node->pcur.get_btr_cur();
    if (lizard::dict_index_is_panda(index) &&
        (!index->n_nullable || !dtuple_contains_null(node->ref))) {
      /* Panda Index searches b-tree only by uk if the entry does not have
       * nullable field, so it is impossible to get up_match >= uk. */
      ut_a(!(btr_cur->up_match >= dict_index_get_n_unique(index)));
    }
    if ((btr_cur->up_match >= dict_index_get_n_unique(index) ||
         (btr_cur->low_match >= dict_index_get_n_unique(index) &&
          !page_rec_is_infimum(btr_cur_get_rec(btr_cur)))) &&
        (!index->n_nullable || !dtuple_contains_null(node->ref))) {
      /*
       * We are now in the phase after row log application but before MDL
       * upgrade. During this phase:
       * - DML transactions are not blocked by the DDL operation and can
       * directly modify the B-tree.
       * - Due to the lack of precise logic for iterative search and record
       * locking (as implemented in `row_ins_scan_sec_index_for_duplicate`),
       *   We must tolerate potential duplicate key errors.
       */
      lizard::btr_cur_print_duplicate_error_in_uk_online(
          btr_cur, index, node->ref, UT_LOCATION_HERE);
      dict_set_corrupted(index);
      err = DB_SUCCESS;
    } else {
      rec = node->pcur.get_rec();
      offsets = rec_get_offsets(rec, index, offsets_, ULINT_UNDEFINED,
                                UT_LOCATION_HERE, offsets_heap);
      err =
          btr_cur_optimistic_insert(BTR_NO_LOCKING_FLAG | BTR_NO_UNDO_LOG_FLAG |
                                        BTR_KEEP_SYS_FLAG | BTR_KEEP_POS_FLAG,
                                    layout, btr_cur, &offsets, offsets_heap,
                                    node->ref, &rec, &dummy_big_rec, thr, &mtr);
      ut_a(!dummy_big_rec);
      if (err == DB_FAIL && !modify_leaf) {
        err = btr_cur_pessimistic_insert(
            BTR_NO_LOCKING_FLAG | BTR_NO_UNDO_LOG_FLAG | BTR_KEEP_SYS_FLAG |
                BTR_KEEP_POS_FLAG,
            layout, btr_cur, &offsets, offsets_heap, node->ref, &rec,
            &dummy_big_rec, thr, &mtr);
        ut_a(!dummy_big_rec);
      }
    }

    if (err == DB_SUCCESS) {
      page_update_max_trx_id(btr_cur_get_block(btr_cur),
                             btr_cur_get_page_zip(btr_cur),
                             thr_get_trx(thr)->id, &mtr);
    }

    node->pcur.close();
    mtr_commit(&mtr);
    return err;
  }

  ut_ad(!node->is_rlog);
  ut_ad(node->found_panda);
  ut_a(lizard::row_undo_reposition_panda_pcur(mode, index, node, &mtr));

  btr_cur = node->pcur.get_btr_cur();

  rec = node->pcur.get_rec();
  offsets = rec_get_offsets(rec, index, offsets_, ULINT_UNDEFINED,
                            UT_LOCATION_HERE, offsets_heap);
  ut_ad(row_get_rec_trx_id(btr_cur_get_rec(btr_cur), index, offsets) ==
        node->trx->id);
  ut_ad(row_get_rec_roll_ptr(btr_cur_get_rec(btr_cur), index, offsets) ==
        node->roll_ptr);

  if (modify_leaf) {
    ut_ad((mode & ~BTR_ALREADY_S_LATCHED) == BTR_MODIFY_LEAF);
    /* Update would release the implicit lock. Must convert to
       explicit lock before applying update undo.*/
    row_convert_impl_to_expl_if_needed(btr_cur, node);

    err = btr_cur_optimistic_update(BTR_NO_LOCKING_FLAG | BTR_NO_UNDO_LOG_FLAG |
                                        BTR_KEEP_SYS_FLAG | BTR_KEEP_POS_FLAG,
                                    layout, btr_cur, &offsets, offsets_heap,
                                    node->update, node->cmpl_info, thr,
                                    thr_get_trx(thr)->id, &mtr);
  } else {
    mem_heap_t *heap = mem_heap_create(1024, UT_LOCATION_HERE);
    err = btr_cur_pessimistic_update(
        BTR_NO_LOCKING_FLAG | BTR_NO_UNDO_LOG_FLAG | BTR_KEEP_SYS_FLAG |
            BTR_KEEP_POS_FLAG,
        layout, btr_cur, &offsets, offsets_heap, heap, &dummy_big_rec,
        node->update, node->cmpl_info, thr, thr_get_trx(thr)->id, node->undo_no,
        &mtr, pcur);
    ut_a(!dummy_big_rec);
    mem_heap_free(heap);
  }

  node->pcur.commit_specify_mtr(&mtr);
  return err;
}

[[nodiscard]] static dberr_t row_undo_mod_remove_panda_rec_low(
    ulint mode, dict_index_t *index, undo_node_t *node) {
  dberr_t err = DB_SUCCESS;
  btr_cur_t *btr_cur;
  ulint trx_id_offset;
  mtr_t mtr;

  log_free_check();
  mtr_start(&mtr);
  dict_disable_redo_if_temporary(index->table, &mtr);
  ut_ad(!node->is_rlog);
  ut_ad(node->rec_type == TRX_UNDO_UPD_DEL_REC);

  /* Find out if the record has been purged already
  or if we can remove it. */

  lizard_ut_ad(node->new_trx_id == node->txn_rec.trx_id);
  if (!lizard::row_undo_reposition_panda_pcur(mode, index, node, &mtr) ||
      lizard::row_panda_vers_must_preserve_del_marked(&node->txn_rec)) {
    goto func_exit;
  }

  btr_cur = node->pcur.get_btr_cur();

  trx_id_offset = btr_cur->index->trx_id_offset;

  if (!trx_id_offset) {
    mem_heap_t *heap = nullptr;
    ulint trx_id_col;
    const ulint *offsets;
    ulint len;

    trx_id_col = btr_cur->index->get_sys_col_pos(DATA_TRX_ID);
    ut_ad(trx_id_col > 0);
    ut_ad(trx_id_col != ULINT_UNDEFINED);

    offsets = rec_get_offsets(btr_cur_get_rec(btr_cur), btr_cur->index, nullptr,
                              trx_id_col + 1, UT_LOCATION_HERE, &heap);

    /* nullptr for index as trx_id_col is physical here */
    trx_id_offset = rec_get_nth_field_offs(nullptr, offsets, trx_id_col, &len);
    ut_ad(len == DATA_TRX_ID_LEN);
    mem_heap_free(heap);
  }

  if (trx_read_trx_id(btr_cur_get_rec(btr_cur) + trx_id_offset) !=
      node->new_trx_id) {
    /* The record must have been purged and then replaced
    with a different one. */
    return (DB_SUCCESS);
  }

  /* We are about to remove an old, delete-marked version of the
 record that may have been delete-marked by a different transaction
 than the rolling-back one. */
  ut_ad(rec_get_deleted_flag(btr_cur_get_rec(btr_cur),
                             dict_table_is_comp(node->table)));

  if (mode == BTR_MODIFY_LEAF) {
    err = btr_cur_optimistic_delete(btr_cur, 0, &mtr) ? DB_SUCCESS : DB_FAIL;
  } else {
    ut_ad(mode == (BTR_MODIFY_TREE | BTR_LATCH_FOR_DELETE));

    /* This operation is analogous to purge, we can free also
    inherited externally stored fields.
    We can also assume that the record was complete
    (including BLOBs), because it had been delete-marked
    after it had been completely inserted. Therefore, we
    are passing rollback=false, just like purge does. */

    btr_cur_pessimistic_delete(&err, false, btr_cur, 0, false, node->trx->id,
                               node->undo_no, node->rec_type, &mtr, &node->pcur,
                               nullptr);

    /* The delete operation may fail if we have little
    file space left: TODO: easiest to crash the database
    and restart with more file space */
  }

func_exit:
  node->pcur.commit_specify_mtr(&mtr);
  return err;
}

[[nodiscard]] static dberr_t row_undo_mod_panda_rec(dict_index_t *index,
                                                    que_thr_t *thr,
                                                    undo_node_t *node) {
  dberr_t err;
  mem_heap_t *offsets_heap = nullptr;
  DEBUG_SYNC(current_thd, "before_row_undo_mod_panda_rec");
  /* Try first optimistic descent to the B-tree */
  err = row_undo_mod_panda_rec_low(BTR_MODIFY_LEAF, &offsets_heap, index, thr,
                                   node);
  if (err != DB_SUCCESS) {
    /* Try then pessimistic descent to the B-tree */
    err = row_undo_mod_panda_rec_low(BTR_MODIFY_TREE, &offsets_heap, index, thr,
                                     node);
    ut_ad(err == DB_SUCCESS || err == DB_OUT_OF_FILE_SPACE);
  }
  if (err == DB_SUCCESS && node->rec_type == TRX_UNDO_UPD_DEL_REC) {
    err = row_undo_mod_remove_panda_rec_low(BTR_MODIFY_LEAF, index, node);
    if (err != DB_SUCCESS) {
      err = row_undo_mod_remove_panda_rec_low(
          BTR_MODIFY_TREE | BTR_LATCH_FOR_DELETE, index, node);
      ut_ad(err == DB_SUCCESS || err == DB_OUT_OF_FILE_SPACE);
    }
  }

  if (offsets_heap) {
    mem_heap_free(offsets_heap);
  }
  return (err);
}

/** Rollback the modified panda record.
 * @param[in,out]  node            row rollback node
 * @param[in]      thr             que thread
 * @param[in,out]  thd             current MySQL connection (for mdl)
 * @param[in,out]  mdl             MDL ticket
@return DB_SUCCESS or DB_OUT_OF_FILE_SPACE */
static dberr_t row_undo_mod_record_for_panda_func(undo_node_t *node,
                                                  que_thr_t *thr, THD *thd,
                                                  MDL_ticket *mdl) {
  dberr_t err = DB_SUCCESS;
  dict_index_t *index = nullptr;
  for (dict_index_t *ind : node->table->indexes) {
    if (ind->id == node->index_id) {
      index = ind;
      break;
    }
  }

  if (index->is_corrupted()) {
    goto cleanup;
  }

  ut_ad(node->trx->in_rollback);
  ut_ad(node->layout == TL_BAMBOO);
  ut_ad(!node->table->skip_alter_undo);
  ut_a(index && dict_index_is_panda(index) && !index->is_corrupted() &&
       !dict_index_has_virtual(index) && !index->has_new_v_col &&
       index->type != DICT_FTS);
  ut_ad(node->rec_type != TRX_UNDO_INSERT_REC);
  ut_ad(node->row == nullptr && node->ref != nullptr);

  /* Do the actual work */
  err = row_undo_mod_panda_rec(index, thr, node);

  DEBUG_SYNC_C("ib_panda_after_undo_mod");

cleanup:
  /* Cleanout */
  node->state = UNDO_NODE_FETCH_NEXT;
  dd_table_close(node->table, thd, &mdl, false);
  node->table = nullptr;
  return err;
}

dberr_t row_undo_mod_record_for_panda(undo_node_t *node, que_thr_t *thr,
                                      THD *thd, MDL_ticket *mdl) {
  return row_undo_mod_record_for_panda_func(node, thr, thd, mdl);
}

dberr_t row_undo_mod_record_for_rlog(undo_node_t *node, que_thr_t *thr,
                                     THD *thd, MDL_ticket *mdl) {
  ut_ad(node->is_rlog);
  ut_ad(node->layout == TL_BAMBOO);
  return row_undo_mod_record_for_panda_func(node, thr, thd, mdl);
}

}  // namespace lizard
