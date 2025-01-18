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

#include "current_thd.h"
#include "debug_sync.h"
#include "dict0dd.h"
#include "dict0dict.h"
#include "log0chkp.h"
#include "row0purge.h"
#include "row0row.h"
#include "sql_base.h"
#include "trx0rec.h"

#include "lizard0dict.h"
#include "lizard0dict0mem.h"
#include "lizard0purge.h"
#include "lizard0row.h"
#include "lizard0row0purge.h"

namespace lizard {
[[nodiscard]] static bool row_purge_remove_panda_if_poss_low(
    purge_node_t *node, dict_index_t *index, ulint mode) {
  bool success = true;
  mtr_t mtr;
  rec_t *rec;
  mem_heap_t *heap = nullptr;
  ulint *offsets;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  rec_offs_init(offsets_);

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

  rec = node->pcur.get_rec();

  offsets = rec_get_offsets(rec, index, offsets_, ULINT_UNDEFINED,
                            UT_LOCATION_HERE, &heap);

  if (node->roll_ptr != row_get_rec_roll_ptr(rec, index, offsets)) {
    /* Someone else has modified the record later: do not remove */
    goto func_exit;
  }

  ut_ad(rec_get_deleted_flag(rec, rec_offs_comp(offsets)));

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
  if (heap) {
    mem_heap_free(heap);
  }

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
}  // namespace lizard
