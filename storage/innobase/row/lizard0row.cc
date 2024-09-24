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

/** @file include/lizard0row.cc
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

#include "lizard0data0data.h"
#include "lizard0btr0cur.h"
#include "lizard0dict0mem.h"

#ifdef UNIV_DEBUG
extern void page_zip_header_cmp(const page_zip_des_t *, const byte *);
#endif /* UNIV_DEBUG */

namespace lizard {

/*=============================================================================*/
/* lizard record insert */
/*=============================================================================*/

/**
  Allocate row buffers for lizard fields.

  @param[in]      node      Insert node
*/
void ins_alloc_lizard_fields(ins_node_t *node) {
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

  /** instrinsic table didn't need the SCN and UBA columns. */
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

/*=============================================================================*/
/* lizard record update */
/*=============================================================================*/

/**
  Fill SCN and UBA into index entry.
  @param[in]    thr       query
  @param[in]    entry     dtuple
  @param[in]    index     cluster index
*/
void row_upd_index_entry_lizard_field(que_thr_t *thr, dtuple_t *entry,
                                      dict_index_t *index) {
  dfield_t *dfield = nullptr;
  byte *ptr = nullptr;
  ulint pos = 0;
  const txn_desc_t *txn_desc = nullptr;

  ut_ad(thr && entry && index);
  ut_ad(index->is_clustered());
  ut_ad(!index->table->is_intrinsic());

  if (index->table->is_temporary()) {
    txn_desc = &txn_sys_t::instance()->txn_desc_temp;
  } else {
    trx_t *trx = thr_get_trx(thr);
    assert_txn_desc_allocated(trx);
    txn_desc = &trx->txn_desc;
  }

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
  pos++;

  /** 3. Populate GCN */
  ut_ad(pos == index->get_sys_col_pos(DATA_GCN_ID));

  dfield = dtuple_get_nth_field(entry, pos);
  ptr = static_cast<byte *>(dfield_get_data(dfield));
  trx_write_gcn(ptr, txn_desc);
}

/**
  Get address of scn field in record.
  @param[in]      rec       record
  @paramp[in]     index     cluster index
  @param[in]      offsets   rec_get_offsets(rec, idnex)
  @retval pointer to scn_id
*/
byte *row_get_scn_ptr_in_rec(rec_t *rec, const dict_index_t *index,
                             const ulint *offsets) {
  ulint len;
  ulint scn_pos;
  byte *field;
  ut_ad(index->is_clustered());

  scn_pos = index->get_sys_col_pos(DATA_SCN_ID);
  ut_ad(rec_offs_n_fields(offsets) >= scn_pos + DATA_N_LIZARD_COLS);
  field =
      const_cast<byte *>(rec_get_nth_field(index, rec, offsets, scn_pos, &len));

  ut_ad(len == DATA_SCN_ID_LEN);
  ut_ad(field + DATA_SCN_ID_LEN ==
        rec_get_nth_field(index, rec, offsets, scn_pos + 1, &len));
  ut_ad(len == DATA_UNDO_PTR_LEN);

  ut_ad(field + DATA_SCN_ID_LEN + DATA_GCN_ID_LEN ==
        rec_get_nth_field(index, rec, offsets, scn_pos + 2, &len));
  ut_ad(len == DATA_GCN_ID_LEN);

  return field;
}

/**
  Write the scn and undo_ptr of the physical record.
  @param[in]      ptr       scn pointer
  @param[in]      txn_desc  txn description
*/
void row_upd_rec_write_scn_and_uba(byte *ptr, const scn_t scn,
                                   const undo_ptr_t uba, const gcn_t gcn) {
  trx_write_scn(ptr, scn);
  trx_write_undo_ptr(ptr + DATA_SCN_ID_LEN, uba);
  trx_write_gcn(ptr + DATA_SCN_ID_LEN + DATA_UNDO_PTR_LEN, gcn);
}

/**
  Write the scn and undo_ptr of the physical record.
  @param[in]      ptr       scn pointer
  @param[in]      scn       SCN
  @param[in]      undo_ptr  UBA
*/
void row_upd_rec_write_scn_and_undo_ptr(byte *ptr, const scn_t scn,
                                        const undo_ptr_t undo_ptr,
                                        const gcn_t gcn) {
  mach_write_to_8(ptr, scn);
  mach_write_to_8(ptr + DATA_SCN_ID_LEN, undo_ptr);
  mach_write_to_8(ptr + DATA_SCN_ID_LEN + DATA_UNDO_PTR_LEN, gcn);
}

/**
  Modify the scn and undo_ptr of record. It will handle compress pages.
  @param[in]      rec       record
  @param[in]      page_zip
  @paramp[in]     index     cluster index
  @param[in]      offsets   rec_get_offsets(rec, idnex)
  @param[in]      txn_desc  txn description
*/
static void row_upd_rec_lizard_fields_low(rec_t *rec, page_zip_des_t *page_zip,
                                          const dict_index_t *index,
                                          const ulint *offsets, const scn_t scn,
                                          const undo_ptr_t uba,
                                          const gcn_t gcn) {
  byte *field;
  ut_ad(index->is_clustered());
  assert_undo_ptr_allocated(uba);

  if (page_zip) {
    ulint pos = index->get_sys_col_pos(DATA_SCN_ID);
    page_zip_write_scn_and_undo_ptr(page_zip, index, rec, offsets, pos, scn,
                                    uba, gcn);

  } else {
    /** Get pointer of scn_id in record */
    field = row_get_scn_ptr_in_rec(rec, index, offsets);
    row_upd_rec_write_scn_and_uba(field, scn, uba, gcn);
  }
}

/**
  Modify the scn and undo_ptr of record.
  @param[in]      rec       record
  @param[in]      page_zip
  @paramp[in]     index     cluster index
  @param[in]      offsets   rec_get_offsets(rec, idnex)
  @param[in]      txn       txn description
*/
void row_upd_rec_lizard_fields(rec_t *rec, page_zip_des_t *page_zip,
                               const dict_index_t *index, const ulint *offsets,
                               const txn_desc_t *txn) {
  const txn_desc_t *txn_desc;
  ut_ad(index->is_clustered());
  ut_ad(!index->table->skip_alter_undo);
  ut_ad(!index->table->is_intrinsic());

  if (index->table->is_temporary()) {
    txn_desc = &txn_sys_t::instance()->txn_desc_temp;
  } else {
    txn_desc = txn;
    assert_undo_ptr_allocated(txn_desc->undo_ptr);
  }
  row_upd_rec_lizard_fields_low(rec, page_zip, index, offsets,
                                txn_desc->cmmt.scn, txn_desc->undo_ptr,
                                txn_desc->cmmt.gcn);
}

/**
  Updates the scn and undo_ptr field in a clustered index record when
  cleanout because of update.
  @param[in/out]  rec       record
  @param[in/out]  page_zip  compressed page, or NULL
  @param[in]      pos       SCN position in rec
  @param[in]      index     cluster index
  @param[in]      offsets   rec_get_offsets(rec, idnex)
  @param[in]      scn       SCN
  @param[in]      undo_ptr  UBA
*/
void row_upd_rec_lizard_fields_in_cleanout(rec_t *rec, page_zip_des_t *page_zip,
                                           const dict_index_t *index,
                                           const ulint *offsets,
                                           const txn_rec_t *txn_rec) {
  ut_ad(index->is_clustered());
  ut_ad(!index->table->skip_alter_undo);
  ut_ad(!index->table->is_temporary());

  lizard_ut_ad(!undo_ptr_is_active(txn_rec->undo_ptr));
  row_upd_rec_lizard_fields_low(rec, page_zip, index, offsets, txn_rec->scn,
                                txn_rec->undo_ptr, txn_rec->gcn);
}

	void row_upd_rec_gpp_no_in_cleanout(rec_t *rec, page_zip_des_t *page_zip,
                                    const dict_index_t *index,
                                    const ulint gpp_no_offset,
                                    const gpp_no_t gpp_no) {
  ut_ad(!index->is_clustered());
  ut_ad(!page_zip);
  row_write_gpp_no(rec, index, gpp_no_offset, gpp_no);
}


/**
  Updates the scn and undo_ptr field in a clustered index record in
  database recovery.
  @param[in/out]  rec       record
  @param[in/out]  page_zip  compressed page, or NULL
  @param[in]      pos       SCN position in rec
  @param[in]      index     cluster index
  @param[in]      offsets   rec_get_offsets(rec, idnex)
  @param[in]      scn       SCN
  @param[in]      undo_ptr  UBA
*/
void row_upd_rec_lizard_fields_in_recovery(rec_t *rec, page_zip_des_t *page_zip,
                                           const dict_index_t *index, ulint pos,
                                           const ulint *offsets,
                                           const scn_t scn,
                                           const undo_ptr_t undo_ptr,
                                           const gcn_t gcn) {
  /** index->type (Log_Dummy) will not be set rightly if it's non-compact
  format, see function **mlog_parse_index** */
  ut_ad(!rec_offs_comp(offsets) || index->is_clustered());
  ut_ad(rec_offs_validate(rec, NULL, offsets));

  /** Lizard: This assertion is left, because we wonder if
  there will be a false case */
  /**
    Revision:
    Since 8029, because the redo of the instant ddl v2 version did not fully
    restore the state of the data dictionary during mlog_parse_index, so
    "index->get_sys_col_pos(DATA_SCN_ID)" may have an error.

    The example found so far is:
    modify or insert a new record after instant drop column.
  */
  // lizard_ut_ad(!rec_offs_comp(offsets) ||
  //              index->get_sys_col_pos(DATA_SCN_ID) == pos);

  if (page_zip) {
    page_zip_write_scn_and_undo_ptr(page_zip, index, rec, offsets, pos, scn,
                                    undo_ptr, gcn);
  } else {
    byte *field;
    ulint len;

    field =
        const_cast<byte *>(rec_get_nth_field(index, rec, offsets, pos, &len));
    ut_ad(len == DATA_SCN_ID_LEN);

    row_upd_rec_write_scn_and_undo_ptr(field, scn, undo_ptr, gcn);
  }
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

/**
   Allocate row buffers for GPP_NO field of update node's old row.

   @param[in]      node      Insert node
*/
void upd_alloc_gpp_field_for_old_row(upd_node_t *node) {
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
void upd_alloc_gpp_field_for_new_row(upd_node_t *node) {
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

/*=============================================================================*/
/* lizard fields read/write from table record */
/*=============================================================================*/

/**
  Read the scn id from record

  @param[in]      rec         record
  @param[in]      index       dict_index_t, must be cluster index
  @param[in]      offsets     rec_get_offsets(rec, index)

  @retval         scn id
*/
scn_id_t row_get_rec_scn_id(const rec_t *rec, const dict_index_t *index,
                            const ulint *offsets) {
  ulint offset;
  ut_ad(index->is_clustered());
  assert_lizard_dict_index_check(index);

  offset = row_get_lizard_offset(index, DATA_SCN_ID, offsets);

  return trx_read_scn(rec + offset);
}

/**
  Read the undo ptr from record

  @param[in]      rec         record
  @param[in]      index       dict_index_t, must be cluster index
  @param[in]      offsets     rec_get_offsets(rec, index)

  @retval         scn id
*/
undo_ptr_t row_get_rec_undo_ptr(const rec_t *rec, const dict_index_t *index,
                                const ulint *offsets) {
  ulint offset;
  ut_ad(index->is_clustered());
  assert_lizard_dict_index_check(index);

  offset = row_get_lizard_offset(index, DATA_UNDO_PTR, offsets);
  return trx_read_undo_ptr(rec + offset);
}

/**
  Read the gcn id from record

  @param[in]      rec         record
  @param[in]      index       dict_index_t, must be cluster index
  @param[in]      offsets     rec_get_offsets(rec, index)

  @retval         gcn id
*/
gcn_t row_get_rec_gcn(const rec_t *rec, const dict_index_t *index,
                      const ulint *offsets) {
  ulint offset;
  ut_ad(index->is_clustered());
  assert_lizard_dict_index_check(index);

  offset = row_get_lizard_offset(index, DATA_GCN_ID, offsets);

  return trx_read_gcn(rec + offset);
}

/**
  Get the relative offset in record by offsets
  @param[in]      index
  @param[in]      type
  @param[in]      offsets
*/
ulint row_get_lizard_offset(const dict_index_t *index, ulint type,
                            const ulint *offsets) {
  ulint offset = 0;
  ut_ad(index->is_clustered());
  ut_ad(!index->table->is_intrinsic());

  offset = index->trx_id_offset;
  if (!offset) {
    offset = row_get_trx_id_offset(index, offsets);
   }
  
  switch (type) {
    case DATA_GCN_ID:
      offset += DATA_UNDO_PTR_LEN;
      [[fallthrough]];

    case DATA_UNDO_PTR:
      offset += DATA_SCN_ID_LEN;
      [[fallthrough]];

    case DATA_SCN_ID:
      offset += DATA_ROLL_PTR_LEN + DATA_TRX_ID_LEN;
      break;

    default:
      ut_ad(0);
  }

#if defined UNIV_DEBUG
  ulint len;
  ulint d_pos = index->get_sys_col_pos(type);
  ut_a(d_pos == index->n_uniq + type - 1);
  ulint d_offset = rec_get_nth_field_offs(index, offsets, d_pos, &len);

  if (type == DATA_SCN_ID) {
    ut_ad(len == DATA_SCN_ID_LEN);
   } else if (type == DATA_UNDO_PTR) {
    ut_ad(len == DATA_UNDO_PTR_LEN);
   } else if (type == DATA_GCN_ID) {
    ut_ad(len == DATA_GCN_ID_LEN);
   } else {
    ut_ad(0);
   }
   ut_ad(d_offset == offset);
#endif

   return offset;
}

/**
  Read the txn from record

  @param[in]      rec         record
  @param[in]      index       dict_index_t, must be cluster index
  @param[in]      offsets     rec_get_offsets(rec, index)
  @param[out]     txn_rec     lizard transaction attributes
*/
void row_get_txn_rec(const rec_t *rec, const dict_index_t *index,
                     const ulint *offsets, txn_rec_t *txn_rec) {
  ulint offset;

  ut_ad(index->is_clustered());
  ut_ad(rec_offs_validate(rec, index, offsets));

  offset = index->trx_id_offset;

  if (!offset) {
    offset = row_get_trx_id_offset(index, offsets);
  }

  txn_rec->trx_id = trx_read_trx_id(rec + offset);
  offset += (DATA_TRX_ID_LEN + DATA_ROLL_PTR_LEN);
  txn_rec->scn = trx_read_scn(rec + offset);
  offset += DATA_SCN_ID_LEN;
  txn_rec->undo_ptr = trx_read_undo_ptr(rec + offset);
  offset += DATA_UNDO_PTR_LEN;
  txn_rec->gcn = trx_read_gcn(rec + offset);
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
  pos = index->n_fields - 1;

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
 * @return            Returns the GPP Number from the record
 */
gpp_no_t row_get_gpp_no(const rec_t *rec, const dict_index_t *index,
                        const ulint *offsets, ulint &gpp_no_offset) {
  ut_ad(!index->is_clustered());
  ut_ad(index->n_s_gfields > 0);
  assert_lizard_dict_index_check(index);

  gpp_no_offset = row_get_gpp_no_offset(index, offsets);

  return mach_read_from_4(rec + gpp_no_offset);
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

/**
  Write the scn and undo ptr into the update vector
  @param[in]      trx         transaction context
  @param[in]      index       index object
  @param[in]      update      update vector
  @param[in]      field_nth   the nth from SCN id field
  @param[in]      txn_info    txn information
  @param[in]      heap        memory heap
*/
void trx_undo_update_rec_by_lizard_fields(const dict_index_t *index,
                                          upd_t *update, ulint field_nth,
                                          txn_info_t txn_info,
                                          mem_heap_t *heap) {
  byte *buf;
  upd_field_t *upd_field;
  ut_ad(update && heap);

  upd_field = upd_get_nth_field(update, field_nth);
  buf = static_cast<byte *>(mem_heap_alloc(heap, DATA_SCN_ID_LEN));
  trx_write_scn(buf, txn_info.scn);
  upd_field_set_field_no(upd_field, index->get_sys_col_pos(DATA_SCN_ID), index);
  dfield_set_data(&(upd_field->new_val), buf, DATA_SCN_ID_LEN);

  upd_field = upd_get_nth_field(update, field_nth + 1);
  buf = static_cast<byte *>(mem_heap_alloc(heap, DATA_UNDO_PTR_LEN));
  trx_write_undo_ptr(buf, txn_info.undo_ptr);
  upd_field_set_field_no(upd_field, index->get_sys_col_pos(DATA_UNDO_PTR),
                         index);
  dfield_set_data(&(upd_field->new_val), buf, DATA_UNDO_PTR_LEN);

  upd_field = upd_get_nth_field(update, field_nth + 2);
  buf = static_cast<byte *>(mem_heap_alloc(heap, DATA_GCN_ID_LEN));
  trx_write_gcn(buf, txn_info.gcn);
  upd_field_set_field_no(upd_field, index->get_sys_col_pos(DATA_GCN_ID), index);
  dfield_set_data(&(upd_field->new_val), buf, DATA_GCN_ID_LEN);
}

/**
  Read the scn and undo_ptr from undo record
  @param[in]      ptr       undo record
  @param[out]     txn_info  SCN and UBA info

  @retval begin of the left undo data.
*/
byte *trx_undo_update_rec_get_lizard_cols(const byte *ptr,
                                          txn_info_t *txn_info) {
  txn_info->scn = mach_u64_read_next_compressed(&ptr);
  txn_info->undo_ptr = mach_u64_read_next_compressed(&ptr);
  txn_info->gcn = mach_u64_read_next_compressed(&ptr);

  return const_cast<byte *>(ptr);
}

/**
  Write redo log to the buffer about updates of scn and uba.
  @param[in]      index     clustered index of the record
  @param[in]      txn_rec   txn info of the record
  @param[in]      log_ptr   pointer to a buffer opened in mlog
  @param[in]      mtr       mtr

  @return new pointer to mlog
*/
byte *row_upd_write_lizard_vals_to_log(const dict_index_t *index,
                                       const txn_rec_t *txn_rec, byte *log_ptr,
                                       mtr_t *mtr MY_ATTRIBUTE((unused))) {
  ut_ad(index->is_clustered());
  ut_ad(mtr);
  ut_ad(txn_rec);

  log_ptr +=
      mach_write_compressed(log_ptr, index->get_sys_col_pos(DATA_SCN_ID));

  log_ptr += mach_u64_write_compressed(log_ptr, txn_rec->scn);

  trx_write_undo_ptr(log_ptr, txn_rec->undo_ptr);
  log_ptr += DATA_UNDO_PTR_LEN;

  trx_write_gcn(log_ptr, txn_rec->gcn);
  log_ptr += DATA_GCN_ID_LEN;

  return log_ptr;
}

/**
  Get the real SCN of a record by UBA, and write back to records in physical
  page, when we make a btr update / delete.
  @param[in]      trx_id    trx_id of the transactions
                            who updates / deletes the record
  @param[in]      rec       record
  @param[in]      offsets   rec_get_offsets(rec)
  @param[in/out]  block     buffer block of the record
  @param[in/out]  mtr       mini-transaction
*/
void row_lizard_cleanout_when_modify_rec(const trx_id_t trx_id, rec_t *rec,
                                         const dict_index_t *index,
                                         const ulint *offsets,
                                         const buf_block_t *block, mtr_t *mtr) {
  trx_id_t rec_id;
  bool cleanout;
  txn_rec_t rec_txn;

  ut_ad(trx_id > 0);

  rec_id = row_get_rec_trx_id(rec, index, offsets);

  if (trx_id == rec_id) {
    /* update a exist row which has been modified by
    the current active transaction */
    return;
  }

  /** scn must be consistent with the undo_ptr */
  assert_row_lizard_valid(rec, index, offsets);
  ut_ad(index->is_clustered());
  ut_ad(!index->table->is_intrinsic());

  row_get_txn_rec(rec, index, offsets, &rec_txn);

  /** lookup the scn by UBA address */
  txn_rec_real_state_by_misc(&rec_txn, Cache_hint::KEEP_OLD, &cleanout);

  if (cleanout) {
    ut_ad(mtr_memo_contains_flagged(mtr, block, MTR_MEMO_PAGE_X_FIX));
    row_upd_rec_lizard_fields_in_cleanout(
        const_cast<rec_t *>(rec),
        const_cast<page_zip_des_t *>(buf_block_get_page_zip(block)), index,
        offsets, &rec_txn);

    /** Write redo log */
    if (opt_cleanout_write_redo)
      btr_cur_upd_lizard_fields_clust_rec_log(rec, index, &rec_txn, mtr);
  }
}

/**
  Whether the transaction on the record has committed
  @param[in]        trx_id
  @param[in]        rec             current rec
  @param[in]        index           cluster index
  @parma[in]        offsets         rec_get_offsets(rec, index)

  @retval           true            committed
  @retval           false           active
*/
bool row_is_committed(trx_id_t trx_id, const rec_t *rec,
                      const dict_index_t *index, const ulint *offsets) {
  /** If the trx id if less than the minimum active trx id,
      it's sure that trx has committed.

      Attention:
      the minimum active trx id is changed after trx_sys structure
      modification when commit, so it's later than txn undo header
      modification.
  */
  if (gcs_load_min_active_trx_id() > trx_id) {
    return true;
  }

  txn_rec_t txn_rec;
  row_get_txn_rec(rec, index, offsets, &txn_rec);

  return !txn_rec_real_state_by_misc(&txn_rec, Cache_hint::KEEP_OLD);
}

/**
  Parses the log data of lizard field values.
  @param[in]      ptr       buffer
  @param[in]      end_ptr   buffer end
  @param[out]     pos       SCN position in record
  @param[out]     scn       scn
  @param[out]     undo_ptr  uba

  @return log data end or NULL
*/
byte *row_upd_parse_lizard_vals(const byte *ptr, const byte *end_ptr,
                                ulint *pos, scn_t *scn, undo_ptr_t *undo_ptr,
                                gcn_t *gcn) {
  *pos = mach_parse_compressed(&ptr, end_ptr);

  if (ptr == nullptr) return nullptr;

  *scn = mach_u64_parse_compressed(&ptr, end_ptr);

  if (ptr == nullptr) return nullptr;

  if (end_ptr < ptr + DATA_UNDO_PTR_LEN) {
    return (nullptr);
  }

  *undo_ptr = trx_read_undo_ptr(ptr);
  ptr += DATA_UNDO_PTR_LEN;

  if (end_ptr < ptr + DATA_GCN_ID_LEN) {
    return nullptr;
  }

  *gcn = trx_read_gcn(ptr);
  ptr += DATA_GCN_ID_LEN;

  return const_cast<byte *>(ptr);
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
 * @param[in]     pcur            Persistent cursor for the secondary index
 * @param[in]     cursor          Point to Cursor for the secondary index
 * @param[in]     mtr             Mini-transaction handle
 * @return        True if successful positioning, False otherwise
 */
bool row_sel_optimistic_guess_clust(dict_index_t *clust_idx,
                                    dict_index_t *sec_idx, dtuple_t *clust_ref,
                                    const rec_t *sec_rec,
                                    btr_pcur_t *clust_pcur, ulint *sec_offsets,
                                    ulint mode,
                                    btr_pcur_t *pcur,
                                    SCursor **scursor,
                                    mtr_t *mtr) {
  ut_ad(!sec_idx->is_clustered());
  ut_ad(mode == BTR_SEARCH_LEAF);

  if (!index_scan_guess_clust_enabled || sec_idx->n_s_gfields == 0) {
    return false;
  }

  ut_ad(sec_offsets);

  ulint gpp_no_offset = 0;
  bool hit = btr_cur_guess_clust_by_gpp(clust_idx, sec_idx, clust_ref, sec_rec,
                                        clust_pcur, sec_offsets, mode, gpp_no_offset ,mtr);

  /* Try to add the cursor into scan_cleanout. */
  if(!hit && scursor && pcur->m_cleanout){
    *scursor = pcur->m_cleanout ->acquire_for_gpp(pcur, gpp_no_offset);
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
 * @param[out]    sec_offsets     Offsets array for the secondary record
 * @param[in]     mode            latching mode
 * @param[in]     mtr             Mini-transaction handle
 * @return        True if successful positioning, False otherwise
 */
bool row_purge_optimistic_guess_clust(dict_index_t *clust_idx,
                                      dict_index_t *sec_idx,
                                      dtuple_t *clust_ref, const rec_t *sec_rec,
                                      btr_pcur_t *clust_pcur,
                                      ulint *sec_offsets, ulint mode,
                                      mtr_t *mtr) {
  ut_ad(!sec_idx->is_clustered());
  ut_ad(mode == BTR_SEARCH_LEAF);

  if (!index_purge_guess_clust_enabled || sec_idx->n_s_gfields == 0) {
    return false;
  }

  mem_heap_t *heap = nullptr;
  ulint offsets_[REC_OFFS_NORMAL_SIZE];
  rec_offs_init(offsets_);
  if (!sec_offsets) {
    sec_offsets = rec_get_offsets(sec_rec, sec_idx, offsets_, ULINT_UNDEFINED,
                                  UT_LOCATION_HERE, &heap);
  }
  ulint gpp_no_offset = 0;
  bool hit = btr_cur_guess_clust_by_gpp(clust_idx, sec_idx, clust_ref, sec_rec,
                                        clust_pcur, sec_offsets, mode, gpp_no_offset ,mtr);

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
  ut_ad(!sec_idx->is_clustered());
  ut_ad(mode == BTR_SEARCH_LEAF);

  if (!index_lock_guess_clust_enabled || sec_idx->n_s_gfields == 0) {
    return false;
  }

  ut_ad(sec_offsets);
  ulint gpp_no_offset = 0;
  bool hit = btr_cur_guess_clust_by_gpp(clust_idx, sec_idx, clust_ref, sec_rec,
                                        clust_pcur, sec_offsets, mode, gpp_no_offset , mtr);

  index_lock_guess_clust_stat(hit);

  return hit;
}

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
/*=============================================================================*/
/* lizard field debug */
/*=============================================================================*/

/**
  Debug the scn id in record is initial state
  @param[in]      rec       record
  @param[in]      index     cluster index
  @parma[in]      offsets   rec_get_offsets(rec, index)

  @retval         true      Success
*/
bool row_scn_initial(const rec_t *rec, const dict_index_t *index,
                     const ulint *offsets) {
  txn_rec_t txn_rec;
  row_get_txn_rec(rec, index, offsets, &txn_rec);

  ut_a(txn_rec.scn == SCN_NULL);
  ut_a(txn_rec.gcn == GCN_NULL);
  return true;
}

/**
  Debug the undo_ptr and scn in record is matched.
  @param[in]      rec       record
  @param[in]      index     cluster index
  @parma[in]      offsets   rec_get_offsets(rec, index)

  @retval         true      Success
*/
bool row_lizard_valid(const rec_t *rec, const dict_index_t *index,
                      const ulint *offsets) {
  undo_addr_t undo_addr;
  txn_rec_t txn_rec;
  ulint comp;

  /** If we are in recovery, we don't make a validation, because purge
  sys might have not been started */
  if (recv_recovery_is_on()) return true;

  comp = *rec_offs_base(offsets) & REC_OFFS_COMPACT;

  /** Temporary table can not be seen in other transactions */
  if (!index->is_clustered() || index->table->is_intrinsic()) return true;
  /**
    Skip the REC_STATUS_NODE_PTR, REC_STATUS_INFIMUM, REC_STATUS_SUPREMUM
    Skip the non-compact record
  */
  if (comp && rec_get_status(rec) == REC_STATUS_ORDINARY) {
    row_get_txn_rec(rec, index, offsets, &txn_rec);

    undo_decode_undo_ptr(txn_rec.undo_ptr, &undo_addr);

    /** UBA is valid */
    undo_addr_validation(&undo_addr, index);

    /** Scn and trx state are matched */
    ut_a(txn_rec.is_active() == (txn_rec.scn == SCN_NULL));
    ut_a(txn_rec.is_active() == (txn_rec.gcn == GCN_NULL));
  }
  return true;
}

/**
  Debug the undo_ptr and scn in record is matched.
  @param[in]      rec       record
  @param[in]      index     cluster index
  @parma[in]      offsets   rec_get_offsets(rec, index)

  @retval         true      Success
*/
bool row_lizard_has_cleanout(const rec_t *rec, const dict_index_t *index,
                             const ulint *offsets) {
  undo_addr_t undo_addr;
  txn_rec_t txn_rec;

  if (!index->is_clustered() || index->table->is_intrinsic()) return true;

  /**
    Skip the REC_STATUS_NODE_PTR, REC_STATUS_INFIMUM, REC_STATUS_SUPREMUM
  */
  if (rec_get_status(rec) == REC_STATUS_ORDINARY) {
    row_get_txn_rec(rec, index, offsets, &txn_rec);
    undo_decode_undo_ptr(txn_rec.undo_ptr, &undo_addr);
    /** UBA is valid */
    undo_addr_validation(&undo_addr, index);
    /** commit */
    ut_a(txn_rec.is_committed());
    /** valid scn */
    ut_a(txn_rec.scn != SCN_NULL);
    ut_a(txn_rec.gcn != GCN_NULL);
  }
  return true;
}

#endif /* UNIV_DEBUG || LIZARD_DEBUG */

} /* namespace lizard */
