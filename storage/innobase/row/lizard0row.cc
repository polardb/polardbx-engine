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
#include "lizard0data0types.h"
#include "lizard0undo.h"
#include "lizard0dict.h"

#include "my_dbug.h"

#include "row0ins.h"
#include "row0log.h"
#include "row0row.h"
#include "que0que.h"

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
  dfield_t *dfield;
  byte *ptr;
  ulint pos;
  trx_t *trx;
  const txn_desc_t *txn_desc = nullptr;

  ut_ad(thr && entry && index);
  ut_ad(index->is_clustered());
  ut_ad(!index->table->is_intrinsic());
  trx = thr_get_trx(thr);

  if (index->table->is_temporary()) {
    txn_desc = &TXN_DESC_TEMP;
  } else {
    txn_desc = &trx->txn_desc;
  }

  /** 1. Populate SCN */
  pos = index->get_sys_col_pos(DATA_SCN_ID);
  dfield = dtuple_get_nth_field(entry, pos);
  ptr = static_cast<byte *>(dfield_get_data(dfield));
  trx_write_scn(ptr, txn_desc);

  /** 2. Populate UBA */
  pos = index->get_sys_col_pos(DATA_UNDO_PTR);
  dfield = dtuple_get_nth_field(entry, pos);
  ptr  = static_cast<byte*>(dfield_get_data(dfield));
  trx_write_undo_ptr(ptr, txn_desc);
}

/**
  Modify the scn and undo_ptr of record.
  @param[in]      rec       record
  @param[in]      page_zip
  @paramp[in]     index     cluster index
  @param[in]      offsets   rec_get_offsets(rec, idnex)
  @param[in]      trx       current trx
*/
void row_upd_rec_lizard_fields(rec_t *rec, page_zip_des_t *page_zip,
                               const dict_index_t *index, const ulint *offsets,
                               const trx_t *trx) {
  const txn_desc_t *txn_desc = nullptr;

  ut_ad(index->is_clustered());
  ut_ad(!index->table->skip_alter_undo);
  ut_ad(!index->table->is_intrinsic());

  if (index->table->is_temporary()) {
    txn_desc = &TXN_DESC_TEMP;
  } else {
    txn_desc = &trx->txn_desc;
    assert_undo_ptr_allocated(&txn_desc->undo_ptr);
  }

  row_upd_rec_lizard_fields(rec, page_zip, index, offsets, txn_desc);
}

/**
  Modify the scn and undo_ptr of record.
  @param[in]      rec       record
  @param[in]      page_zip
  @paramp[in]     index     cluster index
  @param[in]      offsets   rec_get_offsets(rec, idnex)
  @param[in]      txn_desc  txn description
*/
void row_upd_rec_lizard_fields(rec_t *rec, page_zip_des_t *page_zip,
                               const dict_index_t *index, const ulint *offsets,
                               const txn_desc_t *txn_desc) {
  ulint offset;
  ut_ad(index->is_clustered());

  if (page_zip) {
    /** TODO */
  } else {
    /** SCN_ID */
    offset = row_get_lizard_offset(index, DATA_SCN_ID, offsets);
    trx_write_scn(rec + offset, txn_desc);

    /** UNDO_PTR */
    offset = row_get_lizard_offset(index, DATA_UNDO_PTR, offsets);
    trx_write_undo_ptr(rec + offset, txn_desc);
  }
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
  Read the undo ptr state from record

  @param[in]      rec         record
  @param[in]      index       dict_index_t, must be cluster index
  @param[in]      offsets     rec_get_offsets(rec, index)

  @retval         scn id
*/
bool row_get_rec_undo_ptr_is_active(const rec_t *rec, const dict_index_t *index,
                                    const ulint *offsets) {
  undo_ptr_t undo_ptr = row_get_rec_undo_ptr(rec, index, offsets);

  return lizard_undo_ptr_is_active(undo_ptr);
}

/**
  Get the relative offset in record by offsets
  @param[in]      index
  @param[in]      type
  @param[in]      offsets
*/
ulint row_get_lizard_offset(const dict_index_t *index, ulint type,
                            const ulint *offsets) {
  ulint pos;
  ulint offset;
  ulint len;
  ut_ad(index->is_clustered());
  ut_ad(!index->table->is_intrinsic());

  pos = index->get_sys_col_pos(type);
  ut_ad(pos == index->n_uniq + type - 1);

  offset = rec_get_nth_field_offs(index, offsets, pos, &len);
  if (type == DATA_SCN_ID) {
    ut_ad(len == DATA_SCN_ID_LEN);
  } else if (type == DATA_UNDO_PTR) {
    ut_ad(len == DATA_UNDO_PTR_LEN);
  } else {
    ut_ad(0);
  }
  return offset;
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
  scn_id_t scn = row_get_rec_scn_id(rec, index, offsets);
  ut_a(scn == SCN_NULL);
  return true;
}

/**
  Debug the undo_ptr state in record is active state
  @param[in]      rec       record
  @param[in]      index     cluster index
  @parma[in]      offsets   rec_get_offsets(rec, index)

  @retval         true      Success
*/
bool row_undo_ptr_is_active(const rec_t *rec,
                            const dict_index_t *index,
                            const ulint *offsets) {
  bool is_active = row_get_rec_undo_ptr_is_active(rec, index, offsets);
  ut_a(is_active);
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
  scn_id_t scn;
  undo_ptr_t undo_ptr;
  bool is_active;
  undo_addr_t undo_addr;
  ulint comp;

  comp = *rec_offs_base(offsets) & REC_OFFS_COMPACT;

  /** Temporary table can not be seen in other transactions */
  if (!index->is_clustered() || index->table->is_intrinsic()) return true;
  /**
    Skip the REC_STATUS_NODE_PTR, REC_STATUS_INFIMUM, REC_STATUS_SUPREMUM
    Skip the non-compact record
  */
  if (comp && rec_get_status(rec) == REC_STATUS_ORDINARY) {
    scn = row_get_rec_scn_id(rec, index, offsets);
    undo_ptr = row_get_rec_undo_ptr(rec, index, offsets);

    is_active = row_get_rec_undo_ptr_is_active(rec, index, offsets);

    undo_decode_undo_ptr(undo_ptr, &undo_addr);

    /** UBA is valid */
    lizard_undo_addr_validation(&undo_addr, index);

    /** Scn and trx state are matched */
    ut_a(is_active == (scn == SCN_NULL));
  }
  return true;
}

#endif /* UNIV_DEBUG || LIZARD_DEBUG */

} /* namespace lizard */
