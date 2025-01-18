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

/** @file row/lizard0row0gpp.cc
 Row gpp operation.

 Created 2020-04-06 by Jianwei.zhao
 *******************************************************/

#include "lizard0row0gpp.h"
#include "lizard0dict0mem.h"
#include "lizard0btr0cur.h"
#include "lizard0dict.h"

#include "btr0pcur.h"
#include "row0ins.h"
#include "row0purge.h"
#include "row0row.h"
#include "row0undo.h"
#include "row0upd.h"

namespace lizard {

/** Whether to enable clustered index record inference during the scan. */
bool index_scan_guess_clust_enabled = true;

/** Whether to enable clustered index record inference during the purge. */
bool index_purge_guess_clust_enabled = true;

/** Whether to enable clustered index record inference during the locking. */
bool index_lock_guess_clust_enabled = true;

#ifdef UNIV_DEBUG
gpp_no_t dbug_gpp_no = PAGE_NO_MAX;
#endif /* UNIV_DEBUG */
/**
   Allocate row buffers for GPP_NO field of insert node.

   @param[in]      node      Insert node
*/
void ins_alloc_gpp_field(ins_node_t *node) {
  ut_d(dict_table_t *table = nullptr);
  byte *ptr = nullptr;
  dtuple_t *row = nullptr;
  mem_heap_t *heap = nullptr;
  dfield_t *dfield = nullptr;
  ut_ad(node);

  row = node->row;
  ut_d(table = node->table);
  heap = node->entry_sys_heap;

  ut_ad(row && table && heap);
  ut_ad(dtuple_get_n_fields(row) == table->get_n_cols());

  ptr = static_cast<byte *>(mem_heap_zalloc(heap, DATA_GPP_NO_LEN));

  dfield = dtuple_get_v_gfield(row);
  dfield_set_data(dfield, ptr, DATA_GPP_NO_LEN);
  node->gpp_no_buf = ptr;
}

/**
 * Write GPP_NO after primary key insert.
 *
 * @param[in/out]	insert node
 * @param[in]		index
 * @param[in]		index entry
 * @param[in]		row
 */
void row_ins_clust_write_gpp_no(ins_node_t *node, const dict_index_t *index,
                                dtuple_t *entry, const dtuple_t *row) {
  ut_ad(index->is_clustered());
  ut_ad(node);
  ut_ad(node->index == index);
  ut_ad(node->entry == entry);
  ut_ad(node->row == row);

  /** Have inserted on primary key. */
  ut_ad(node->gpp_no != 0);

  mach_write_to_4(node->gpp_no_buf, node->gpp_no);

  ut_ad(row->read_v_gpp_no() == node->gpp_no);
  ut_ad(entry->read_v_gpp_no() == node->gpp_no);
}

/**
 * Debug assert GPP_NO is valid when inserting second index.
 * Attention: Use macro instead of using it directly.
 *
 * @param[in]	  insert node
 * @param[in]		index
 * @param[in]		index entry
 * @param[in]		row
 */
void row_ins_sec_assert_gpp_no(ins_node_t *node, const dict_index_t *index,
                               dtuple_t *entry, const dtuple_t *row) {
  ut_ad(!index->is_clustered());
  ut_ad(node);
  ut_ad(node->index == index);
  ut_ad(node->entry == entry);
  ut_ad(node->row == row);

  /** Have inserted on primary key. */
  ut_ad(node->gpp_no != 0);

  ut_ad(entry->read_v_gpp_no() != 0);
  ut_ad(row->read_v_gpp_no() == entry->read_v_gpp_no());
  if (index->n_s_gfields > 0) {
    ut_ad(entry->read_s_gpp_no() == entry->read_v_gpp_no());
  }
  return;
}

/**
 * Write GPP_NO after primary key insert or just assert it for sec index.
 *
 * @param[in/out]	insert node
 * @param[in]		index
 * @param[in]		index entry
 * @param[in]		row
 */
void row_ins_index_write_gpp_no(ins_node_t *node, const dict_index_t *index,
                                dtuple_t *entry, const dtuple_t *row) {
  if (index->is_clustered()) {
    row_ins_clust_write_gpp_no(node, index, entry, row);
  } else {
    lizard_row_ins_sec_assert_gpp_no(node, index, entry, row);
  }
}

/**
   Allocate row buffers for GPP_NO field of update node's old row.

   @param[in]      node      Insert node
*/
void row_upd_alloc_gpp_field_for_old_row(upd_node_t *node) {
  ut_d(dict_table_t *table = nullptr);
  byte *ptr = nullptr;
  dtuple_t *row = nullptr;
  mem_heap_t *heap = nullptr;
  dfield_t *dfield = nullptr;
  ut_ad(node);
  ut_d(table = node->table);
  heap = node->heap;

  /* For old row, Set gpp_no to FIL_NULL. */
  row = node->row;
  ut_ad(row);
  ut_ad(dtuple_get_n_fields(row) == table->get_n_cols());

  ptr = static_cast<byte *>(mem_heap_zalloc(heap, DATA_GPP_NO_LEN));
  dfield = dtuple_get_v_gfield(row);
  dfield_set_data(dfield, ptr, DATA_GPP_NO_LEN);
  mach_write_to_4(ptr, FIL_NULL);
  ut_ad(node->row->read_v_gpp_no() == FIL_NULL);
}

/**
   Allocate row buffers for GPP_NO field of update node's new row.

   @param[in]      node      Insert node
*/
void row_upd_alloc_gpp_field_for_new_row(upd_node_t *node) {
  ut_d(dict_table_t *table = nullptr);
  byte *ptr = nullptr;
  dtuple_t *row = nullptr;
  mem_heap_t *heap = nullptr;
  dfield_t *dfield = nullptr;
  ut_ad(node);
  ut_d(table = node->table);
  heap = node->heap;

  /* For update row, link it with node->gpp_no_buf. */
  row = node->upd_row;
  ut_ad(row && table && heap);
  ut_ad(dtuple_get_n_fields(row) == table->get_n_cols());

  ptr = static_cast<byte *>(mem_heap_zalloc(heap, DATA_GPP_NO_LEN));

  dfield = dtuple_get_v_gfield(row);
  dfield_set_data(dfield, ptr, DATA_GPP_NO_LEN);
  node->gpp_no_buf = ptr;
}

/**
 * Write GPP_NO after primary key update.
 *
 * @param[in/out]	upd node
 * @param[in]		index
 * @param[in]   index entry
 * @param[in]		upd_row
 */
void row_upd_clust_write_gpp_no(upd_node_t *node, const dict_index_t *index,
                                dtuple_t *entry, const dtuple_t *upd_row) {
  ut_ad(index->is_clustered());
  ut_ad(node);
  ut_ad(node->upd_row == upd_row);
  ut_ad(node->row->read_v_gpp_no() == FIL_NULL);

  /** Have inserted on primary key. */
  ut_ad(node->gpp_no != 0);

  mach_write_to_4(node->gpp_no_buf, node->gpp_no);

  ut_ad(upd_row->read_v_gpp_no() == node->gpp_no);
  ut_ad(!entry || entry->read_v_gpp_no() == node->gpp_no);
}

/**
 * Debug assert GPP_NO is valid when updating second index.
 * Attention: Use macro instead of using it directly.
 *
 * @param[in]	  upd node
 * @param[in]		index
 * @param[in]   index entry
 * @param[in]		upd_row
 */
void row_upd_sec_assert_gpp_no(upd_node_t *node, const dict_index_t *index,
                               dtuple_t *entry, const dtuple_t *upd_row) {
  ut_ad(!index->is_clustered());
  ut_ad(node);
  ut_ad(node->upd_row == upd_row);
  ut_ad(node->row->read_v_gpp_no() == FIL_NULL);

  /** Have inserted on primary key. */
  ut_ad(node->gpp_no != 0);
  ut_ad(entry->read_v_gpp_no() != 0);
  ut_ad(upd_row->read_v_gpp_no() == entry->read_v_gpp_no());
  if (index->n_s_gfields > 0) {
    ut_ad(entry->read_s_gpp_no() == entry->read_v_gpp_no());
  }
}

/*=============================================================================*/
/* lizard record row log */
/*=============================================================================*/
/**
   Allocate row buffers for GPP_NO field when applying row log table

   @param[in/out]   row
   @param[in]       heap
*/
void row_log_table_alloc_gpp_field(dtuple_t *row, mem_heap_t *heap) {
  byte *ptr = nullptr;
  dfield_t *dfield = nullptr;

  ut_ad(row && heap);
  ptr = static_cast<byte *>(mem_heap_zalloc(heap, DATA_GPP_NO_LEN));
  dfield = dtuple_get_v_gfield(row);
  dfield_set_data(dfield, ptr, DATA_GPP_NO_LEN);
}

/**
 * Write GPP_NO after row log table apply.
 *
 * @param[in]		gpp_no
 * @param[in]		index
 * @param[in/out]	row
 */
void row_log_table_clust_write_gpp_no(const gpp_no_t &gpp_no,
                                      const dict_index_t *index,
                                      const dtuple_t *row) {
  ut_ad(index->is_clustered());
  ut_ad(row->v_gfield->data != nullptr);

  /** Have inserted on primary key. */
  ut_ad(gpp_no != 0);

  mach_write_to_4((byte *)row->v_gfield->data, gpp_no);

  ut_ad(row->read_v_gpp_no() == gpp_no);
}

/**
 * Assert GPP_NO is valid when applying row log table in secondary index.
 *
 * @param[in]		index
 * @param[in]   index entry
 * @param[in]		row
 * @param[in]		gpp_no
 */
void row_log_table_sec_assert_gpp_no(const dict_index_t *index, dtuple_t *entry,
                                     const dtuple_t *row,
                                     const gpp_no_t &gpp_no) {
  ut_ad(!index->is_clustered());
  /** Have inserted on primary key. */
  ut_ad(gpp_no != 0);
  ut_ad(entry->read_v_gpp_no() == gpp_no);
  ut_ad(row->read_v_gpp_no() == gpp_no);
  if (index->n_s_gfields > 0) {
    ut_ad(entry->read_s_gpp_no() == gpp_no);
  }
}

/*=============================================================================*/
/* lizard record row undo */
/*=============================================================================*/
/**
   Allocate row buffers for GPP_NO field for undo node.

   @param[in]       node      Undo node
*/
void row_undo_alloc_gpp_field(undo_node_t *node) {
  ut_d(dict_table_t *table = nullptr);
  byte *ptr = nullptr;
  dtuple_t *row = nullptr;
  mem_heap_t *heap = nullptr;
  dfield_t *dfield = nullptr;
  ut_ad(node);

  row = node->row;
  ut_d(table = node->table);
  heap = node->heap;

  ut_ad(row && table && heap);
  ptr = static_cast<byte *>(mem_heap_zalloc(heap, DATA_GPP_NO_LEN));
  dfield = dtuple_get_v_gfield(row);
  dfield_set_data(dfield, ptr, DATA_GPP_NO_LEN);
  mach_write_to_4(ptr, FIL_NULL);
  ut_ad(node->row->read_v_gpp_no() == FIL_NULL);
}

/*=============================================================================*/
/* lizard record row purge */
/*=============================================================================*/
/**
   Allocate row buffers for GPP_NO field for purge node.

   @param[in]       node      Purge node
*/
void row_purge_alloc_gpp_field(purge_node_t *node) {
  ut_d(dict_table_t *table = nullptr);
  byte *ptr = nullptr;
  dtuple_t *row = nullptr;
  mem_heap_t *heap = nullptr;
  dfield_t *dfield = nullptr;
  ut_ad(node);

  row = node->row;
  ut_d(table = node->table);
  heap = node->heap;

  ut_ad(row && table && heap);
  ptr = static_cast<byte *>(mem_heap_zalloc(heap, DATA_GPP_NO_LEN));
  dfield = dtuple_get_v_gfield(row);
  dfield_set_data(dfield, ptr, DATA_GPP_NO_LEN);
  mach_write_to_4(ptr, FIL_NULL);
  ut_ad(node->row->read_v_gpp_no() == FIL_NULL);
}



/**
  Updates the gpp_no of secondary index record when cleanout.
  @param[in/out]  rec             record
  @param[in/out]  page_zip        compressed page, or NULL
  @param[in]      index           cluster index
  @param[in]      gpp_no_offset   gpp no offset
  @param[in]      gpp_no          gpp no
*/
void row_upd_rec_gpp_no_in_cleanout(rec_t *rec, page_zip_des_t *page_zip,
                                    const dict_index_t *index,
                                    const ulint gpp_no_offset,
                                    const gpp_no_t gpp_no) {
  ut_ad(!index->is_clustered());
  ut_ad(!page_zip);
  row_write_gpp_no(rec, index, gpp_no_offset, gpp_no);
}

/**
 * Update gpp no field in secondary index record in database recovery.
 * @param[in]      rec			record
 * @param[in]      page_zip
 * @param[in]      gpp no
 * @param[in]      gpp offset		gpp no position in rec */
void row_upd_rec_gpp_fields_in_recovery(rec_t *rec, page_zip_des_t *page_zip,
                                        page_no_t gpp_no, ulint gpp_offset) {
  ut_ad(!page_zip);

  mach_write_to_4(rec + gpp_offset, gpp_no);
}

/**
 * Retrieves the offset of the GPP number in a record
 *
 * @param[in] index   Dictionary index object, non-clustered
 * @param[in] offsets Array of field offsets
 * @return            Returns the offset of the GPP number within the record
 */
ulint row_get_gpp_no_offset(const dict_index_t *index, const ulint *offsets) {
  ulint pos;
  ulint offset;
  ulint len;
  ut_ad(!index->is_clustered());
  ut_ad(index->n_fields == offsets[1]);
  ut_ad(index->n_s_gfields > 0);

  /** The GPP NO resides on the last field of the index. */
  /** Revision : GPP NO will be not last field after index page version.*/
  pos = index->get_gpp_col_pos();
  ut_ad(pos != ULINT_UNDEFINED);

  offset = rec_get_nth_field_offs(index, offsets, pos, &len);
  ut_ad(len == DATA_GPP_NO_LEN);

  return offset;
}

/**
 * Retrieves the GPP Number from a record
 *
 * @param[in] rec     Pointer to the record
 * @param[in] index   Pointer to the dictionary index object, non-clustered
 * @param[in] offsets Record field offsets array
 *
 * @return            Returns the GPP Number and offset from the record
 */
std::pair<gpp_no_t, ulint> row_get_gpp_no(const rec_t *rec,
                                          const dict_index_t *index,
                                          const ulint *offsets) {
  ut_ad(!index->is_clustered());
  ut_ad(index->n_s_gfields > 0);
  assert_lizard_dict_index_check(index);

  ulint gpp_no_offset = row_get_gpp_no_offset(index, offsets);
  gpp_no_t gpp_no = mach_read_from_4(rec + gpp_no_offset);

  return {gpp_no, gpp_no_offset};
}

void row_write_gpp_no(rec_t *rec, const dict_index_t *index,
                      const ulint gpp_no_offset, const gpp_no_t gpp_no) {
  ut_ad(!index->is_clustered());
  ut_ad(index->n_s_gfields > 0);
  assert_lizard_dict_index_check(index);
  mach_write_to_4(rec + gpp_no_offset, gpp_no);
}

/**
 * Assert GPP_NO is valid for multi-valued sec index.
 *
 * @param[in]		index
 * @param[in]		multi-value entry
 */
void row_sec_multi_value_assert_gpp_no(const dict_index_t *index,
                                       const dtuple_t *mv_entry) {
  ut_ad(!index->is_clustered());
  ut_d(gpp_no_t gpp_no = mv_entry->read_v_gpp_no());
  ut_ad(gpp_no != 0 && gpp_no != FIL_NULL);
  if (index->n_s_gfields > 0) {
    ut_ad(mv_entry->read_s_gpp_no() == gpp_no);
  }
}


/*=============================================================================*/
/* lizard row guess on gpp */
/*=============================================================================*/

/**
 * When attempting to select a secondary index record, this operation tries to
 * position a persistent cursor on the corresponding clustered index record
 * using the gpp_no value retrieved from the secondary index record.
 *
 * @param[in]     clust_idx       Clustered index
 * @param[in]     sec_idx         Secondary index
 * @param[in]     clust_ref       Reference tuple for the clustered index
 * @param[in]     sec_rec         Secondary index record
 * @param[in,out] clust_pcur      Persistent cursor for the clustered index
 * @param[out]    sec_offsets     Offsets array for the secondary record
 * @param[in]     mode            latching mode
 * @param[in]     cleanout        cleanout context
 * @param[in]     mtr             Mini-transaction handle
 *
 * @return        True if successful positioning, False otherwise
 */
bool row_sel_optimistic_guess_clust(dict_index_t *clust_idx,
                                    dict_index_t *sec_idx, dtuple_t *clust_ref,
                                    const rec_t *sec_rec,
                                    btr_pcur_t *clust_pcur, ulint *sec_offsets,
                                    ulint mode, Cleanout_ctx_t &cctx,
                                    mtr_t *mtr) {
  bool hit = false;
  ulint gpp_no_offset = ULINT_UNDEFINED;

  ut_ad(!sec_idx->is_clustered());
  ut_ad(mode == BTR_SEARCH_LEAF);

  if (!index_scan_guess_clust_enabled || sec_idx->n_s_gfields == 0) {
    return false;
  }

  ut_ad(sec_offsets);

  std::tie(hit, gpp_no_offset) =
      btr_cur_guess_clust_by_gpp(clust_idx, sec_idx, clust_ref, sec_rec,
                                 clust_pcur, sec_offsets, mode, mtr);

  /* Try to add the cursor into scan_cleanout. */
  if (!hit && cctx.is_usable()) {
    cctx.collect_gpp(gpp_no_offset);
  }

  index_scan_guess_clust_stat(hit);
  return hit;
}

/**
 * When attempting to purge a secondary index record, this operation tries to
 * position a persistent cursor on the corresponding clustered index record
 * using the gpp_no value retrieved from the secondary index record.
 *
 * @param[in]     clust_idx       Clustered index
 * @param[in]     sec_idx         Secondary index
 * @param[in]     clust_ref       Reference tuple for the clustered index
 * @param[in]     sec_rec         Secondary index record
 * @param[in,out] clust_pcur      Persistent cursor for the clustered index
 * @param[in]     mode            latching mode
 * @param[in]     mtr             Mini-transaction handle
 * @return        True if successful positioning, False otherwise
 */
bool row_purge_optimistic_guess_clust(dict_index_t *clust_idx,
                                      dict_index_t *sec_idx,
                                      dtuple_t *clust_ref, const rec_t *sec_rec,
                                      btr_pcur_t *clust_pcur, ulint mode,
                                      mtr_t *mtr) {
  mem_heap_t *heap = nullptr;
  bool hit = false;
  ut_ad(!sec_idx->is_clustered());
  ut_ad(mode == BTR_SEARCH_LEAF);

  if (!index_purge_guess_clust_enabled || sec_idx->n_s_gfields == 0) {
    return false;
  }

  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  ulint *offsets = offsets_;

  rec_offs_init(offsets_);
  offsets = rec_get_offsets(sec_rec, sec_idx, offsets, ULINT_UNDEFINED,
                            UT_LOCATION_HERE, &heap);

  std::tie(hit, std::ignore) = btr_cur_guess_clust_by_gpp(
      clust_idx, sec_idx, clust_ref, sec_rec, clust_pcur, offsets, mode, mtr);

  index_purge_guess_clust_stat(hit);

  if (heap) {
    mem_heap_free(heap);
  }
  return hit;
}

/**
 * When attempting to lock a secondary index record, this operation tries to
 * position a persistent cursor on the corresponding clustered index record
 * using the gpp_no value retrieved from the secondary index record.
 *
 * @param[in]     clust_idx       Clustered index
 * @param[in]     sec_idx         Secondary index
 * @param[in]     clust_ref       Reference tuple for the clustered index
 * @param[in]     sec_rec         Secondary index record
 * @param[in,out] clust_pcur      Persistent cursor for the clustered index
 * @param[out]    sec_offsets     Offsets array for the secondary record
 * @param[in]     mode            latching mode
 * @param[in]     mtr             Mini-transaction handle
 * @return        True if successful positioning, False otherwise
 */
bool row_lock_optimistic_guess_clust(dict_index_t *clust_idx,
                                     const dict_index_t *sec_idx,
                                     dtuple_t *clust_ref, const rec_t *sec_rec,
                                     btr_pcur_t *clust_pcur,
                                     const ulint *sec_offsets, ulint mode,
                                     mtr_t *mtr) {
  bool hit = false;
  ut_ad(!sec_idx->is_clustered());
  ut_ad(mode == BTR_SEARCH_LEAF);

  if (!index_lock_guess_clust_enabled || sec_idx->n_s_gfields == 0) {
    return false;
  }
  ut_ad(sec_offsets);

  std::tie(hit, std::ignore) =
      btr_cur_guess_clust_by_gpp(clust_idx, sec_idx, clust_ref, sec_rec,
                                 clust_pcur, sec_offsets, mode, mtr);

  index_lock_guess_clust_stat(hit);

  return hit;
}

}  // namespace lizard
