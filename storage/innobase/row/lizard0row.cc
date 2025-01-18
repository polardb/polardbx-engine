/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file row/lizard0row.cc
 lizard row operation.

 Created 2020-04-06 by Jianwei.zhao
 *******************************************************/

#include "lizard0row.h"
#include "lizard0cleanout.h"
#include "lizard0data0types.h"
#include "lizard0dict.h"
#include "lizard0mon.h"
#include "lizard0page.h"
#include "lizard0tcn.h"
#include "lizard0txn0rec.h"
#include "lizard0undo.h"

#include "my_dbug.h"

#include "lock0lock.h"
#include "que0que.h"
#include "row0ins.h"
#include "row0log.h"
#include "row0mysql.h"
#include "row0purge.h"
#include "row0row.h"
#include "row0undo.h"
#include "row0upd.h"
#include "trx0rec.h"
#include "data0data.h"

#include "lizard0btr0cur.h"
#include "lizard0dict0mem.h"
#include "lizard0row0clover.h"
#include "lizard0row0bamboo.h"

#ifdef UNIV_DEBUG
extern void page_zip_header_cmp(const page_zip_des_t *, const byte *);
#endif /* UNIV_DEBUG */

namespace lizard {

/*=============================================================================*/
/* Record insert */
/*=============================================================================*/

/**
  Allocate row buffers for txn fields.

  @param[in]      node      Insert node
*/
void ins_alloc_txn_fields(ins_node_t *node) {
  dict_table_t *table;
  byte *ptr;
  dtuple_t *row;
  mem_heap_t *heap;
  const dict_col_t *col;
  dfield_t *dfield;

  row = node->row;
  table = node->table;
  heap = node->entry_sys_heap;

  ut_ad(row && table && heap);
  ut_ad(dtuple_get_n_fields(row) == table->get_n_cols());

  /** instrinsic table didn't need txn columns. */
  if (table->is_intrinsic()) return;

  ptr = static_cast<byte *>(mem_heap_zalloc(heap, DATA_LIZARD_TOTAL_LEN));

  /* 1. Populate scn it */
  col = table->get_sys_col(DATA_SCN_ID);
  dfield = dtuple_get_nth_field(row, dict_col_get_no(col));
  dfield_set_data(dfield, ptr, DATA_SCN_ID_LEN);
  ptr += DATA_SCN_ID_LEN;

  /* 2. Populate UBA */
  col = table->get_sys_col(DATA_UNDO_PTR);
  dfield = dtuple_get_nth_field(row, dict_col_get_no(col));
  dfield_set_data(dfield, ptr, DATA_UNDO_PTR_LEN);
  ptr += DATA_UNDO_PTR_LEN;

  /* 3. Populate GCN */
  col = table->get_sys_col(DATA_GCN_ID);
  dfield = dtuple_get_nth_field(row, dict_col_get_no(col));
  dfield_set_data(dfield, ptr, DATA_GCN_ID_LEN);
}

/*=============================================================================*/
/* Record update */
/*=============================================================================*/
/**
  Fill txn fields into index entry.
  @param[in]    thr       query
  @param[in]    entry     dtuple
  @param[in]    index     index
  @param[in]	layout
*/
void row_upd_index_entry_txn_field(que_thr_t *thr, dtuple_t *entry,
                                   dict_index_t *index,
                                   const txn_layout_t &layout) {
  /** intrinsic table didn't have any txn field, pls promise it before call. */
  ut_ad(!index->table->is_intrinsic());

  switch (layout) {
    case TL_CLOVER:
      row_upd_index_entry_clover_field(thr, entry, index);
      break;
    case TL_BAMBOO:
      row_upd_index_entry_bamboo_field(thr, entry, index);
      break;
    case TL_NONE:
      ut_error;
      break;
  }
}

/**
  Modify txn fields of record.
  @param[in,out] rec   record
  @param[in]     page_zip
  @param[in]     index      cluster index
  @param[in]     offsets   rec_get_offsets(rec, idnex)
  @param[in]     layout    txn layout
  @param[in]     txn       txn description
*/
void row_upd_rec_txn_fields(rec_t *rec, page_zip_des_t *page_zip,
                            const dict_index_t *index, const ulint *offsets,
                            const txn_layout_t &layout, const txn_desc_t *txn) {
  const txn_desc_t *txn_desc;
  ut_ad(!index->table->skip_alter_undo);
  ut_ad(!index->table->is_intrinsic());
  ut_ad(index->is_clustered() || index->is_panda());
  ut_ad(txn_layout_is_arranged(layout));

  if (index->table->is_temporary()) {
    txn_desc = &txn_sys_t::instance()->txn_desc_temp;
  } else {
    txn_desc = txn;
    assert_undo_ptr_allocated(txn_desc->undo_ptr);
  }

  switch (layout) {
    case TL_CLOVER:
      row_upd_rec_clover_fields_low(rec, page_zip, index, offsets,
                                    txn_desc->cmmt.scn, txn_desc->undo_ptr,
                                    txn_desc->cmmt.gcn);
      break;
    case TL_BAMBOO:
      row_upd_rec_bamboo_fields_low(rec, page_zip, index, offsets,
                                    txn_desc->cmmt.scn, txn_desc->undo_ptr);
      break;
    default:
      ut_a(0);
  }
  return;
}

/**
  Validate the scn and undo_ptr fields in record.
  @param[in]      index     dict_index_t
  @param[in]      scn_ptr_in_rec   scn_id position in record
  @param[in]      scn_pos   scn_id no in system cols
  @param[in]      rec       record
  @param[in]      offsets   rec_get_offsets(rec, idnex)

  @retval true if verification passed, abort otherwise
*/
bool validate_lizard_fields_in_record(const dict_index_t *index,
                                      const byte *scn_ptr_in_rec, ulint scn_pos,
                                      const rec_t *rec, const ulint *offsets) {
  ulint len;

  ut_a(scn_ptr_in_rec == const_cast<byte *>(rec_get_nth_field(
                             index, rec, offsets, scn_pos, &len)));
  ut_a(len == DATA_SCN_ID_LEN);
  ut_a(scn_ptr_in_rec + DATA_SCN_ID_LEN ==
       rec_get_nth_field(index, rec, offsets, scn_pos + 1, &len));
  ut_a(len == DATA_UNDO_PTR_LEN);

  ut_a(scn_ptr_in_rec + DATA_SCN_ID_LEN + DATA_UNDO_PTR_LEN ==
       rec_get_nth_field(index, rec, offsets, scn_pos + 2, &len));
  ut_a(len == DATA_GCN_ID_LEN);

  return true;
}

/*=============================================================================*/
/* lizard fields read/write from table record */
/*=============================================================================*/
/**
  Read the txn from record

  @param[in]      rec         record
  @param[in]      index       dict_index_t, must be cluster index
  @param[in]      offsets     rec_get_offsets(rec, index)
  @param[in]      layout      rec layout
  @param[out]     txn_rec     lizard transaction attributes
*/
void row_get_txn_rec(const rec_t *rec, const dict_index_t *index,
                     const ulint *offsets, const txn_layout_t &layout,
                     txn_rec_t *txn_rec) {
  ut_ad(txn_layout_is_arranged(layout));
  ut_ad(!index->table || !index->table->is_intrinsic());

  switch (layout) {
    case TL_NONE:
      ut_ad(0);
      return;
    case TL_CLOVER:
      return row_get_clover_txn_rec(rec, index, offsets, txn_rec);
    case TL_BAMBOO:
      return row_get_bamboo_txn_rec(rec, index, offsets, txn_rec);
  }
}

// /**
//   Whether the transaction on the record has committed
//   @param[in]        trx_id
//   @param[in]        rec             current rec
//   @param[in]        index           cluster index
//   @parma[in]        offsets         rec_get_offsets(rec, index)

//   @retval           true            committed
//   @retval           false           active
// */
// bool row_is_committed(trx_id_t trx_id, const rec_t *rec,
//                       const dict_index_t *index, const ulint *offsets) {
//   /** If the trx id if less than the minimum active trx id,
//       it's sure that trx has committed.

//       Attention:
//       the minimum active trx id is changed after trx_sys structure
//       modification when commit, so it's later than txn undo header
//       modification.
//   */
//   if (gcs_load_min_active_trx_id() > trx_id) {
//     return true;
//   }

//   txn_rec_t txn_rec;
//   row_get_txn_rec(rec, index, offsets, &txn_rec);

//   return !txn_rec_real_state(&txn_rec, Cache_hint::KEEP_OLD, ccr_t::CCR_ALL);
// }

static void row_entry_adjust_cmp_fields_func(const dict_index_t *index,
                                             dtuple_t *search_entry) {
  if (dict_index_is_panda(index)) {
    ut_ad(dtuple_get_n_fields_cmp(search_entry) ==
          dict_index_get_n_unique_in_tree(index));
    if (!lizard::row_index_entry_contains_null_in_unique(index, search_entry)) {
      dtuple_set_n_fields_cmp_for_panda(search_entry,
                                        dict_index_get_n_unique(index));
    }
  }
}

void row_search_entry_adjust_cmp_fields(const dict_index_t *index,
                                        dtuple_t *search_entry) {
  row_entry_adjust_cmp_fields_func(index, search_entry);
}

void row_rlog_table_entry_adjust_cmp_fields(const dict_index_t *index,
                                            dtuple_t *rlog_table_entry) {
  row_entry_adjust_cmp_fields_func(index, rlog_table_entry);
}

/**
 * Searches the panda index record for a row, if we have the row reference.
 * @param[out]     pcur       persistent cursor, which must be closed by the
 * caller
 * @param[in]      mode       BTR_MODIFY_LEAF, ...
 * @param[in]      index      panda index
 * @param[in]      ref        row reference of panda index
 * @param[in,out]  mtr
 * @return true if found
 */
/** Searches the panda index record for a row, if we have the row reference.
 @param[in,out]
 @return true if found */
bool row_search_on_row_ref_for_panda(btr_pcur_t *pcur, ulint mode,
                                     dict_index_t *index, const dtuple_t *ref,
                                     mtr_t *mtr) {
  ulint low_match;
  rec_t *rec;
  ut_ad(dict_index_is_panda(index));
  ut_ad(dtuple_check_typed(ref));

  ut_a(dtuple_get_n_fields(ref) >= dict_index_get_n_unique_in_tree(index));
  if (dtuple_get_n_fields_cmp(ref) == dict_index_get_n_unique(index)) {
    ut_a(ref->n_panda_suffix == dict_index_get_n_unique_in_tree(index) -
                                    dict_index_get_n_unique(index));
    ut_ad(!lizard::row_index_entry_contains_null_in_unique(index, ref));
  } else {
    ut_a(dtuple_get_n_fields_cmp(ref) ==
         dict_index_get_n_unique_in_tree(index));
  }

  pcur->open(index, 0, ref, PAGE_CUR_LE, mode, mtr, UT_LOCATION_HERE);

  low_match = pcur->get_low_match();

  rec = pcur->get_rec();

  if (page_rec_is_infimum(rec)) {
    return false;
  }

  if (low_match != dtuple_get_n_fields_cmp(ref)) {
    return false;
  }

  return true;
}

void row_log_entry_update_txn_field(dtuple_t *entry, const dict_index_t *index,
                                    trx_t *trx) {
  dfield_t *dfield = nullptr;
  byte *ptr = nullptr;
  ulint pos = 0;
  const txn_desc_t *txn_desc = nullptr;

  ut_ad(entry && index);
  ut_ad(lizard::dict_index_is_panda(index));
  ut_ad(!index->table->is_temporary());

  assert_txn_desc_allocated(trx);
  txn_desc = &trx->txn_desc;

  /** 1. Populate SCN */
  pos = index->get_sys_col_pos(DATA_SCN_ID);
  dfield = dtuple_get_nth_field(entry, pos);
  ptr = static_cast<byte *>(dfield_get_data(dfield));
  trx_write_scn(ptr, txn_desc);
  pos++;

  /** 2. Populate UBA */
  ut_ad(pos == index->get_sys_col_pos(DATA_UNDO_PTR));

  dfield = dtuple_get_nth_field(entry, pos);
  ptr = static_cast<byte *>(dfield_get_data(dfield));
  trx_write_undo_ptr(ptr, txn_desc);
}

bool row_entry_panda_unique_check_with_rec(const dtuple_t *entry,
                                           const rec_t *cmp_rec,
                                           const dict_index_t *index,
                                           mem_heap_t *heap, ulint *offsets) {
  size_t matched_fields;
  bool contains_null;
  size_t n_unique;
  size_t n_order_fields;

  n_unique = dict_index_get_n_unique(index);
  n_order_fields = dict_index_get_n_unique_in_tree(index);
  ut_ad(dtuple_get_n_fields_cmp(entry) <= n_order_fields);

  contains_null = lizard::row_index_entry_contains_null_in_unique(index, entry);

  offsets = rec_get_offsets(cmp_rec, index, offsets, ULINT_UNDEFINED,
                            UT_LOCATION_HERE, &heap);
  matched_fields = 0;

  entry->compare(cmp_rec, index, offsets, &matched_fields);

  if (contains_null) {
    return matched_fields >= n_order_fields;
  } else {
    return matched_fields >= n_unique;
  }
}

bool row_panda_rec_neighbor_unique_check(const rec_t *rec,
                                         const dict_index_t *index) {
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;
  const rec_t *cmp_rec;
  mem_heap_t *heap = nullptr;
  dtuple_t *entry;

  if (!dict_index_is_panda(index)) {
    return true;
  }

  ut_a(page_rec_is_user_rec(rec));

  rec_offs_init(offsets_);
  heap = mem_heap_create(512, UT_LOCATION_HERE);
  offsets = rec_get_offsets(rec, index, offsets, ULINT_UNDEFINED,
                            UT_LOCATION_HERE, &heap);
  entry = row_rec_to_index_entry(rec, index, offsets, heap);
  ut_a(dtuple_get_n_fields_cmp(entry) ==
       dict_index_get_n_unique_in_tree(index));

  /** Check next record */
  cmp_rec = page_rec_get_next_const(rec);
  if (page_rec_is_user_rec(cmp_rec)) {
    ut_a(!row_entry_panda_unique_check_with_rec(entry, cmp_rec, index, heap,
                                                offsets));
  }

  /** Check prev record */
  cmp_rec = page_rec_get_prev_const(rec);
  if (page_rec_is_user_rec(cmp_rec)) {
    ut_a(!row_entry_panda_unique_check_with_rec(entry, cmp_rec, index, heap,
                                                offsets));
  }

  mem_heap_free(heap);
  return true;
}

/**
 * Determine the heap for building old versions in row_prebuilt_t according to
 * the index.
 * @param[in]      prebuilt      Row prebuilt.
 * @param[in]      index         The transactional index.
 * return          heap          The corresponding heap.
 */
mem_heap_t *row_sel_decide_old_vers_heap(const dict_index_t *index,
                                         row_prebuilt_t *prebuilt) {
  ut_ad(!index->table->is_temporary());
  ut_ad(index->is_clustered() || index->is_panda());
  ut_ad(prebuilt);

  mem_heap_t **heap = nullptr;

  if (index->is_clustered()) {
    heap = &prebuilt->lizard_old_vers_heap;
  } else {
    ut_ad(index->is_panda());
    heap = &prebuilt->panda_old_vers_heap;
  }

  if (*heap) {
    mem_heap_empty(*heap);
  } else {
    *heap = mem_heap_create(200, UT_LOCATION_HERE);
  }

  return *heap;
}

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
/*=============================================================================*/
/* lizard field debug */
/*=============================================================================*/
/**
  Debug the undo_ptr and scn in record is matched.
  @param[in]      rec       record
  @param[in]      index     cluster index
  @parma[in]      offsets   rec_get_offsets(rec, index)

  @retval         true      Success
*/
bool row_txn_is_valid(const rec_t *rec, const dict_index_t *index,
                      const ulint *offsets, const txn_layout_t &layout) {
  /** If we are in recovery, we don't make a validation, because purge
  sys might have not been started */
  if (recv_recovery_is_on()) return true;

  switch(layout) {
    case TL_NONE:
      return true;
    case TL_CLOVER:
      return row_clover_is_valid(rec, index, offsets);
    case TL_BAMBOO:
      return row_bamboo_is_valid(rec, index, offsets);
  }

  return true;
}

/**
  Debug row has cleanout.
  @param[in]      rec       record
  @param[in]      index     cluster index
  @parma[in]      offsets   rec_get_offsets(rec, index)

  @retval         true      Success
*/
bool row_txn_has_cleanout(const rec_t *rec, const dict_index_t *index,
                          const ulint *offsets, const txn_layout_t &layout) {
  ut_ad(txn_layout_is_arranged(layout));

  switch (layout) {
    case TL_CLOVER:
      return row_clover_has_cleanout(rec, index, offsets);
    case TL_BAMBOO:
      return row_bamboo_has_cleanout(rec, index, offsets);
    case TL_NONE:
      ut_error;
      return true;
  }

  return true;
}

#endif /* UNIV_DEBUG || LIZARD_DEBUG */

} /* namespace lizard */
