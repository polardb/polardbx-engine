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

/** @file include/lizard0row0uins.cc
 Lizard row undo insert implementation.

 Created 2024-10-08 by Yichang Song
 *******************************************************/

#include "dict0dd.h"
#include "log0chkp.h"
#include "row0undo.h"
#include "trx0rec.h"

#include "lizard0row.h"
#include "lizard0row0uins.h"
#include "lizard0row0undo.h"
#include "lizard0btr0cur.h"

namespace lizard {
[[nodiscard]] static dberr_t row_undo_ins_remove_panda_rec_low(
    ulint mode, dict_index_t *index, undo_node_t *node) {
  dberr_t err = DB_SUCCESS;
  btr_cur_t *btr_cur;
  mtr_t mtr;
  bool modify_leaf = (mode == BTR_MODIFY_LEAF);
  log_free_check();
  mtr_start(&mtr);
  dict_disable_redo_if_temporary(index->table, &mtr);

  if (!index->is_committed()) {
    if (modify_leaf) {
      mode = BTR_MODIFY_LEAF | BTR_ALREADY_S_LATCHED;
      mtr_s_lock(dict_index_get_lock(index), &mtr, UT_LOCATION_HERE);
    } else {
      ut_ad(mode == (BTR_MODIFY_TREE | BTR_LATCH_FOR_DELETE));
      mtr_sx_lock(dict_index_get_lock(index), &mtr, UT_LOCATION_HERE);
    }

    // /* Log an DELETE. */
    if (btr_cur_rlog_delete_try(index, nullptr, node->ref,
                                BTR_NO_UNDO_LOG_FLAG | BTR_KEEP_SYS_FLAG,
                                TL_BAMBOO)) {
      mtr_commit(&mtr);
      return DB_SUCCESS;
    }
  }

  if (!node->found_panda) {
    /* Only for rlog undo, we didn't search and store position in parsing. */
    ut_ad(node->is_rlog);
  }
  ut_a(lizard::row_undo_reposition_panda_pcur(mode, index, node, &mtr));

  btr_cur = node->pcur.get_btr_cur();
  ut_ad(rec_get_trx_id(btr_cur_get_rec(btr_cur), btr_cur->index) ==
        node->trx->id);
  ut_ad(!rec_get_deleted_flag(btr_cur_get_rec(btr_cur),
                              dict_table_is_comp(btr_cur->index->table)));

  if (modify_leaf) {
    row_convert_impl_to_expl_if_needed(btr_cur, node);
    err = btr_cur_optimistic_delete(btr_cur, 0, &mtr) ? DB_SUCCESS : DB_FAIL;
  } else {
    /* Passing rollback=false/trx_id=0/.../ here, because we are
       deleting a secondary index record: the distinction
       only matters when deleting a record that contains
       externally stored columns. */
    btr_cur_pessimistic_delete(&err, false, btr_cur, 0, false, 0, 0, 0, &mtr,
                               &node->pcur, nullptr);
  }

  node->pcur.commit_specify_mtr(&mtr);
  return err;
}

/** Rollback the inserted panda record.
 * @param[in,out]  index           panda index
 * @param[in,out]  node            row rollback node
@return DB_SUCCESS or DB_OUT_OF_FILE_SPACE */
[[nodiscard]] static dberr_t row_undo_ins_remove_panda_rec(dict_index_t *index,
                                                           undo_node_t *node) {
  dberr_t err;
  ulint n_tries = 0;
  /* Try first optimistic descent to the B-tree */
  err = row_undo_ins_remove_panda_rec_low(BTR_MODIFY_LEAF, index, node);
  if (err == DB_SUCCESS) {
    return (err);
  }
  /* Try then pessimistic descent to the B-tree */
retry:
  err = row_undo_ins_remove_panda_rec_low(
      BTR_MODIFY_TREE | BTR_LATCH_FOR_DELETE, index, node);

  /* The delete operation may fail if we have little
  file space left: TODO: easiest to crash the database
  and restart with more file space */

  if (err != DB_SUCCESS && n_tries < BTR_CUR_RETRY_DELETE_N_TIMES) {
    n_tries++;

    std::this_thread::sleep_for(
        std::chrono::milliseconds(BTR_CUR_RETRY_SLEEP_TIME_MS));

    goto retry;
  }

  return (err);
}

/** Rollback the inserted panda record.
 * @param[in,out]  node            row rollback node
 * @param[in,out]  thd             current MySQL connection (for mdl)
 * @param[in,out]  mdl             MDL ticket
@return DB_SUCCESS or DB_OUT_OF_FILE_SPACE */
static dberr_t row_undo_ins_remove_for_panda_func(undo_node_t *node, THD *thd,
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
  ut_ad(node->rec_type == TRX_UNDO_INSERT_REC);
  ut_ad(node->row == nullptr && node->ref != nullptr);

  /* Do the actual work */
  err = row_undo_ins_remove_panda_rec(index, node);

  DEBUG_SYNC_C("ib_panda_after_undo_ins");

cleanup:
  /* Cleanup */
  dd_table_close(node->table, thd, &mdl, false);
  return err;
}

dberr_t row_undo_ins_remove_for_panda(undo_node_t *node, THD *thd,
                                      MDL_ticket *mdl) {
  return row_undo_ins_remove_for_panda_func(node, thd, mdl);
}

dberr_t row_undo_ins_remove_for_rlog(undo_node_t *node, THD *thd,
                                     MDL_ticket *mdl) {
  ut_ad(node->layout == TL_BAMBOO);
  ut_ad(node->is_rlog);
  return row_undo_ins_remove_for_panda_func(node, thd, mdl);
}

}  // namespace lizard
