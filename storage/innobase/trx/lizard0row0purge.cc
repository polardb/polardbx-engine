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

/** @file include/lizard0row0purge.cc
 Lizard row purge implementation.

 Created 2024-09-24 by Yichang Song
 *******************************************************/

#include <tuple>

#include "current_thd.h"
#include "debug_sync.h"
#include "dict0dd.h"
#include "dict0dict.h"
#include "lizard0row0lotus.h"
#include "log0chkp.h"
#include "row0purge.h"
#include "row0row.h"
#include "sql_base.h"
#include "trx0rec.h"

#include "lizard0dict0dict.h"
#include "lizard0dict0mem.h"
#include "lizard0trx0purge.h"
#include "lizard0row.h"
#include "lizard0row0purge.h"

namespace lizard {
[[nodiscard]] static bool row_purge_remove_panda_if_poss_low(
    purge_node_t *node, dict_index_t *index, ulint mode) {
  bool success = true;
  mtr_t mtr;

  Slice_ctx sctx;
  Slice *slc = nullptr;

  ut_ad(index && dict_index_is_panda(index));
  ut_ad(!node->found_clust);

  fil_space_t *space = fil_space_acquire_silent(index->space);
  if (space == nullptr) {
    /* This can happen only for SDI in General Tablespaces.
     */
    ut_error;
    return (true);
  } else {
    fil_space_release(space);
  }

  log_free_check();
  mtr_start(&mtr);

  if (!row_search_on_row_ref_for_panda(&node->pcur, mode, index, node->ref,
                                       &mtr)) {
    goto func_exit;
  }

  slc = sctx.get_offsets(node->pcur.get_reg(), index, ULINT_UNDEFINED);

  if (node->roll_ptr != row_get_rec_roll_ptr(slc, index)) {
    /* Someone else has modified the record later: do not remove */
    goto func_exit;
  }

  ut_ad(rec_get_deleted_flag(slc->rec(), rec_offs_comp(slc->offsets())));

  if (mode == BTR_MODIFY_LEAF) {
    success = btr_cur_optimistic_delete(node->pcur.get_btr_cur(), 0, &mtr);
  } else {
    dberr_t err;
    ut_ad(mode == (BTR_MODIFY_TREE | BTR_LATCH_FOR_DELETE));

    DBUG_EXECUTE_IF("pessimistic_row_purge_panda", {
      if (!fsp_is_dd_tablespace(index->space)) {
        const char act[] =
            "now SIGNAL pessimistic_row_purge_panda_pause WAIT_FOR "
            "pessimistic_row_purge_panda_continue";
        assert(opt_debug_sync_timeout > 0);
        assert(!debug_sync_set_action(current_thd, STRING_WITH_LEN(act)));
      }
    });

    btr_cur_pessimistic_delete(&err, false, node->pcur.get_btr_cur(), 0, false,
                               node->trx_id, node->undo_no, node->rec_type,
                               &mtr, &node->pcur, node);

    switch (err) {
      case DB_SUCCESS:
        break;
      case DB_OUT_OF_FILE_SPACE:
        success = false;
        break;
      default:
        ut_error;
    }
  }

func_exit:
  mtr_commit(&mtr);
  node->pcur.close();

  return (success);
}

static bool row_purge_remove_panda_if_poss(purge_node_t *node,
                                           dict_index_t *index) {
  if (row_purge_remove_panda_if_poss_low(node, index, BTR_MODIFY_LEAF)) {
    return (true);
  }

  for (ulint n_tries = 0; n_tries < BTR_CUR_RETRY_DELETE_N_TIMES; n_tries++) {
    if (row_purge_remove_panda_if_poss_low(
            node, index, BTR_MODIFY_TREE | BTR_LATCH_FOR_DELETE)) {
      return (true);
    }

    std::this_thread::sleep_for(
        std::chrono::milliseconds(BTR_CUR_RETRY_SLEEP_TIME_MS));
  }

  return (false);
}

/** Purges the parsed panda record.
 * @param[in,out]  node            row purge node
 * @param[in]      updated_extern  whether external columns were updated
 * @param[in,out]  thd             current thread
@return true if purged, false if skipped */
bool row_purge_record_for_panda(purge_node_t *node, bool updated_extern,
                                THD *thd) {
  bool purged = false;
  dict_index_t *index = nullptr;
  if (node->phase == lizard::PURGE_SP_LIST) {
    /** Do not do purge seconary when erasing sp list. */
    purged = true;
    goto cleanup;
  }

  for (dict_index_t *ind : node->table->indexes) {
    if (ind->id == node->index_id) {
      index = ind;
      break;
    }
  }

  ut_ad(!node->is_rlog);
  ut_ad(node->layout == TL_BAMBOO);
  ut_ad(!updated_extern);
  ut_ad(!node->table->skip_alter_undo);
  ut_a(index && dict_index_is_panda(index) && !index->is_corrupted() &&
       !dict_index_has_virtual(index) && index->is_committed() &&
       !index->has_new_v_col && index->type != DICT_FTS);
  ut_ad(node->rec_type == TRX_UNDO_DEL_MARK_REC);
  ut_ad(node->row == nullptr && node->ref != nullptr);

  purged = row_purge_remove_panda_if_poss(node, index);

cleanup:
  /* Cleanup */
  ut_ad(!node->found_clust);
  if (node->table != nullptr) {
    if (node->mysql_table != nullptr) {
      close_thread_tables(thd);
      node->mysql_table = nullptr;
    }
    ut_ad(!dict_table_is_sdi(node->table->id));
    ut_ad(!node->table->is_fts_aux());
    dd_table_close(node->table, thd, &node->mdl, false);
  }

  node->layout = TL_NONE;
  node->is_rlog = false;

  return (purged);
}

/** Determines if it is possible to remove a secondary index entry. Fast path
for hippo index. Removal is possible if the trx id in the secondary index record
matches the trx id in the undo record and the record is delete-marked.

@param[in]  node            row purge node
@param[in]  index           secondary index
@param[in]  entry           secondary index entry
@param[in]  cur             cursor on secondary index
@return std::pair<trx_id_t, bool> if trx id is not 0, then removal can be
determined. Otherwise, the slow path should be used.
 */
static std::pair<trx_id_t, bool> row_purge_poss_sec_fast(purge_node_t *node,
                                                         dict_index_t *index,
                                                         const dtuple_t *entry,
                                                         btr_cur_t *cur) {
  ut_ad(!index->is_clustered());

  if (!index->is_hippo() || cur == nullptr || !hippo_purge_enabled) {
    return std::make_pair(0, false);
  }

  ut_ad(cur);
  bool can_delete = false;

  reg_t reg = btr_cur_get_reg(cur);

  Slice_ctx sctx(&node->heap);
  Slice *slc = sctx.get_offsets(reg, index, ULINT_UNDEFINED);

  trx_id_t rec_tid = row_get_rec_trx_id(slc, index);
  if (rec_tid > 0) {
    can_delete =
        (rec_tid == node->modifier_trx_id &&
         rec_get_deleted_flag(reg.rec, dict_table_is_comp(index->table)));
  }

  return std::make_pair(rec_tid, can_delete);
}

/** Determines if it is possible to remove a secondary index entry.
@param[in]  node            row purge node
@param[in]  index           secondary index
@param[in]  entry           secondary index entry
@param[in]  cur             cursor on secondary index
@return true if removal is possible, false if not
 */
bool row_purge_poss_sec(purge_node_t *node, dict_index_t *index,
                        const dtuple_t *entry, btr_cur_t *cur) {
  trx_id_t rec_tid = 0;
  bool can_delete = false;

  std::tie(rec_tid, can_delete) =
      row_purge_poss_sec_fast(node, index, entry, cur);
  if (rec_tid > 0) return can_delete;

  return row_purge_poss_sec_slow(node, index, entry, cur);
}

/**
 * Allocates and initializes the TRX_ID field in a partial row during purge
 * operations.
 *
 * During purge operations, trx_undo_rec_get_partial_row() creates a partial row
 * with only ordering fields initialized, leaving system columns (like TRX_ID)
 * as DATA_MISSING. This function properly initializes the TRX_ID field in the
 * clustered index row by:
 * 1. Setting the correct data type for the TRX_ID field
 * 2. Allocating memory for the field data
 * 3. Initializing the field with a zero transaction ID value
 *
 * Note that this does not fully initialize the entire row - only the TRX_ID
 * field is initialized. However, this is sufficient for building secondary
 * index entries for both hippo and regular secondary indexes during purge
 * operations. In particular, this TRX_ID initialization is critical for hippo
 * secondary indexes, which include the TID column in their index entries.
 *
 * @param[in] index  The clustered index (must be a clustered index)
 * @param[in,out] row  The partial row tuple to initialize
 * @param[in] heap  Memory heap for field data allocation
 */
void row_purge_alloc_trx_id_field(const dict_index_t *index, dtuple_t *row,
                                  mem_heap_t *heap) {
  ut_ad(index->is_clustered());

  const dict_col_t *col = index->table->get_sys_col(DATA_TRX_ID);
  ulint col_no = dict_col_get_no(col);
  dfield_t *dfield = dtuple_get_nth_field(row, col_no);
  ut_ad(dtype_get_mtype(dfield_get_type(dfield)) == DATA_MISSING);
  ut_ad(dtype_get_prtype(dfield_get_type(dfield)) == 0);

  col->copy_type(&dfield->type);

  ut_ad(dtype_get_mtype(dfield_get_type(dfield)) == DATA_SYS);
  ut_ad((dtype_get_prtype(dfield_get_type(dfield)) & DATA_SYS_PRTYPE_MASK) ==
        DATA_TRX_ID);

  byte *buf = static_cast<byte *>(mem_heap_alloc(heap, DATA_TRX_ID_LEN));
  mach_write_to_6(buf, 0);
  dfield_set_data(dfield, buf, DATA_TRX_ID_LEN);

  ut_ad(dfield_get_len(dfield) == DATA_TRX_ID_LEN);
}

}  // namespace lizard
