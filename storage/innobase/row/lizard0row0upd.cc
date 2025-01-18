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

/** @file row/lizard0row0upd.cc
 Lizard update of a row

 Created 2024-09-19 by jiyang.zhang
 *******************************************************/

#include "debug_sync.h"
#include "log0chkp.h"
#include "row0mysql.h"

#include "lizard0row0upd.h"
#include "lizard0dict.h"
#include "lizard0btr0cur.h"

namespace lizard {

bool row_upd_panda_only_pk_changed(const upd_node_t *node,
                                   bool change_ord_field) {
  mem_heap_t *heap = nullptr;
  const dict_index_t *index = node->index;
  dtuple_t *entry;

  if (!dict_index_is_panda(index) || node->is_delete || change_ord_field) {
    return false;
  }

  ut_ad(node->state == UPD_NODE_UPDATE_ALL_SEC);

  heap = mem_heap_create(1024, UT_LOCATION_HERE);

  entry = row_build_index_entry(node->row, node->ext, node->index, heap);

  if (row_index_entry_contains_null_in_unique(node->index, entry)) {
    mem_heap_free(heap);
    return false;
  }

  mem_heap_free(heap);
  return true;
}

dberr_t row_upd_panda_only_pk(upd_node_t *node, que_thr_t *thr) {
  mtr_t mtr;
  const rec_t *rec;
  btr_pcur_t pcur;
  dtuple_t *entry;
  dtuple_t *new_entry;
  mem_heap_t *heap = nullptr;
  mem_heap_t *offsets_heap = nullptr;
  dict_index_t *index;
  btr_cur_t *btr_cur;
  dberr_t err = DB_SUCCESS;
  trx_t *trx = thr_get_trx(thr);
  ulint flags = 0;
  const txn_layout_t layout = lizard::dict_index_txn_layout(node->index);
  enum row_search_result search_result;
  big_rec_t *dummy_big_rec = nullptr;
  trx_id_t trx_id = thr_get_trx(thr)->id;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets;
  rec_offs_init(offsets_);
  upd_t *update;
  ulint mode;

  ut_ad(trx->id != 0);

  index = node->index;

  ut_ad(!node->is_delete);

  ut_ad(!dict_index_is_spatial(index));

  ut_ad(!index->table->is_intrinsic());
  ut_ad(layout == TL_BAMBOO);
  /* if (!index->table->is_intrinsic()) */ {
    log_free_check();
  }

  mtr_start(&mtr);

  heap = mem_heap_create(1024, UT_LOCATION_HERE);

  entry = row_build_index_entry(node->row, node->ext, node->index, heap);

  ut_ad(!lizard::row_index_entry_contains_null_in_unique(index, entry));
  lizard::row_search_entry_adjust_cmp_fields(index, entry);

  ut_ad(!index->table->is_temporary());

  if (!index->is_committed()) {

    /* The index->online_status may change if the index is
    or was being created online, but not committed yet. It
    is protected by index->lock. */
    mtr_s_lock(dict_index_get_lock(index), &mtr, UT_LOCATION_HERE);

    switch (dict_index_get_online_status(index)) {
      case ONLINE_INDEX_COMPLETE:
        /* This is a normal index. Do not log anything.
        Perform the update on the index tree directly. */
        break;
      case ONLINE_INDEX_CREATION:
        /* Log a DELETE. */
        ut_ad(!(flags & (BTR_KEEP_SYS_FLAG | BTR_NO_UNDO_LOG_FLAG)));
        btr_cur_rlog_delete(index, trx, entry, flags, layout);

        /* Log an INSERT. */
        ut_ad(!node->is_delete);
        DEBUG_SYNC(thr_get_trx(thr)->mysql_thd,
                   "row_upd_panda_only_pk_before_write_ins_row_log");
        mem_heap_empty(heap);
        entry =
            row_build_index_entry(node->upd_row, node->upd_ext, index, heap);
        ut_a(entry);
          /** Set trx id for panda index */
        row_upd_index_entry_sys_field(entry, index, DATA_TRX_ID, trx->id);

        btr_cur_rlog_insert(index, trx, entry, flags, layout);
        [[fallthrough]];
      case ONLINE_INDEX_ABORTED:
      case ONLINE_INDEX_ABORTED_DROPPED:
        mtr_commit(&mtr);
        goto func_exit;
    }

    /** Panda index is unique index, so cannot use change buffer. */
    mode = BTR_MODIFY_LEAF | BTR_ALREADY_S_LATCHED;

  } else {
    /* For secondary indexes,
    index->online_status==ONLINE_INDEX_COMPLETE if
    index->is_committed(). */
    ut_ad(!dict_index_is_online_ddl(index));

    /** Panda index is unique index, so cannot use change buffer. */
    mode = BTR_MODIFY_LEAF;
  }

  ut_ad(!dict_index_is_spatial(index));

  /* Set the query thread, so that ibuf_insert_low() will be
  able to invoke thd_get_trx(). */
  pcur.get_btr_cur()->thr = thr;

  search_result = row_search_index_entry(index, entry, mode, &pcur, &mtr);

  btr_cur = pcur.get_btr_cur();

  rec = btr_cur_get_rec(btr_cur);

  switch (search_result) {
    case ROW_NOT_DELETED_REF:
    case ROW_BUFFERED:
      /* Lizard: BTR_DELETE or BTR_DELETE_MARK can not come into here. */
      ut_error;
      break;
    case ROW_NOT_FOUND:
      if (!index->is_committed()) {
        /* When online CREATE INDEX copied the update
        that we already made to the clustered index,
        and completed the secondary index creation
        before we got here, the old secondary index
        record would not exist. The CREATE INDEX
        should be waiting for a MySQL meta-data lock
        upgrade at least until this UPDATE returns.
        After that point, set_committed(true) would be
        invoked by commit_inplace_alter_table(). */

        /** TODO: Don't know how to come into here <15-10-24, zanye.zjy> */
        ut_error;

        break;
      }

      ib::error(ER_IB_MSG_1044)
          << "Record in index " << index->name << " of table "
          << index->table->name << " was not found on update: " << *entry
          << " at: " << rec_index_print(rec, index);
#ifdef UNIV_DEBUG
      mtr_commit(&mtr);
      mtr_start(&mtr);
      ut_ad(btr_validate_index(index, nullptr, false));
      ut_d(ut_error);
#endif /* UNIV_DEBUG */
      ut_o(break);
    case ROW_FOUND:
      ut_ad(err == DB_SUCCESS);
      ut_a(!rec_get_deleted_flag(rec, dict_table_is_comp(index->table)));

      break;
  }

  rec = pcur.get_rec();
  offsets = rec_get_offsets(rec, index, offsets_, ULINT_UNDEFINED,
                            UT_LOCATION_HERE, &offsets_heap);

  new_entry = row_build_index_entry(node->upd_row, node->upd_ext, index, heap);

  update = row_upd_build_sec_rec_difference_binary(rec, index, offsets,
                                                   new_entry, heap);

  ut_ad(!(node->cmpl_info & UPD_NODE_NO_SIZE_CHANGE));
  err = btr_cur_optimistic_update(flags, layout, btr_cur, &offsets,
                                  &offsets_heap, update, 0, thr, trx_id, &mtr);
  switch (err) {
    case DB_OVERFLOW:
    case DB_UNDERFLOW:
    case DB_ZIP_OVERFLOW:
      err = DB_FAIL;
    default:
      break;
  }

  if (err == DB_SUCCESS) {
    goto success;
  }

  pcur.store_position(&mtr);
  mtr.commit();

  if (err != DB_FAIL) {
    goto func_exit;
  }

  if (buf_LRU_buf_pool_running_out()) {
    err = DB_LOCK_TABLE_FULL;
    goto func_exit;
  }

  mtr.start();

  ut_a(pcur.restore_position(BTR_MODIFY_TREE, &mtr, UT_LOCATION_HERE));

  ut_ad(!dict_index_is_online_ddl(index));

  ut_ad(
      !rec_get_deleted_flag(pcur.get_rec(), dict_table_is_comp(index->table)));

  err = btr_cur_pessimistic_update(flags, layout, btr_cur, &offsets,
                                   &offsets_heap, heap, &dummy_big_rec, update,
                                   0, thr, trx_id, trx->undo_no, &mtr);
  ut_a(!dummy_big_rec);

  if (err == DB_SUCCESS) {
  success:
    if (trx_id) {
      page_update_max_trx_id(btr_cur_get_block(btr_cur),
                             btr_cur_get_page_zip(btr_cur), trx_id, &mtr);
    }
  }

  pcur.close();
  mtr.commit();

func_exit:

  if (heap) {
    mem_heap_free(heap);
  }

  if (offsets_heap) {
    mem_heap_free(offsets_heap);
  }

  return err;
}
}
