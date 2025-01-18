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

/** @file trx/lizard0trx0rec.cc
 lizard transaction undo log record.

 Created 2024-10-31 by Yichang Song
 *******************************************************/
#include "row0upd.h"
#include "trx0rec.h"

#include "lizard0dict.h"
#include "lizard0trx0rec.h"
#include "lizard0trx0rec0bamboo.h"
#include "lizard0trx0rec0clover.h"
#include "lizard0undo.h"

namespace lizard {
#ifdef UNIV_DEBUG
space_index_t dbug_panda_index_id = 0;
#endif /* UNIV_DEBUG */

/** Write txn columns into undo record.
 * @param[in/out]  ptr             pointer to undo record
 * @param[in]      index           index handler
 * @param[in]      rec             record
 * @param[in]      offsets         offsets
 * @return         pointer to current position
 */
byte *trx_undo_update_rec_write_txn_cols(byte *ptr, const dict_index_t *index,
                                         const rec_t *rec, const ulint *offsets,
                                         const txn_layout_t &layout) {
  ut_ad(txn_layout_is_arranged(layout));
  ut_ad(!index->table->is_intrinsic());

  switch (layout) {
    case TL_NONE:
      ut_error;
      return nullptr;
    case TL_CLOVER:
      return trx_undo_update_rec_write_clover_cols(ptr, index, rec, offsets);
    case TL_BAMBOO:
      return trx_undo_update_rec_write_bamboo_cols(ptr, index, rec, offsets);
  }

  return nullptr;
}

/**
 * Choose the index according to the type and index_id of undo record
 *
 * @param[in]     table           table handle
 * @param[in]     type_cmpl       type_cmpl info restored from undo record
 * @param[in]     index_id        index id restored from undo record
 * @return        dict_index_t
 */
dict_index_t *trx_undo_rec_choose_index(dict_table_t *table,
                                        const type_cmpl_t &type_cmpl,
                                        space_index_t index_id) {
  switch (type_cmpl.txn_layout()) {
    case TL_CLOVER:
    return table->first_index();

    case TL_BAMBOO:
      for (dict_index_t *index : table->indexes) {
        if (index->id == index_id) {
          return index;
        }
      }
      return nullptr;

    default:
      ut_error;
      return nullptr;
  }
}

/**
  Read txn fields (scn/undo_ptr/gcn) from undo record
  @param[in]      ptr       undo record
  @param[out]     txn_info  txn info
  @retval begin of the left undo data.
*/
byte *trx_undo_update_rec_get_txn_cols(const byte *ptr, txn_info_t *txn_info,
                                       const txn_layout_t &layout) {
  switch (layout) {
    case TL_NONE:
      ut_error;
      return nullptr;
    case TL_CLOVER:
      return trx_undo_update_rec_get_clover_cols(ptr, txn_info);
    case TL_BAMBOO:
      return trx_undo_update_rec_get_bamboo_cols(ptr, txn_info);
  }

  return nullptr;
}

/**
  Write the txn field(scn/undo_ptr/gcn) into the update vector
  @param[in]      index       index object
  @param[in]      update      update vector
  @param[in]      field_nth   the nth from SCN id field
  @param[in]      txn_info    txn information
  @param[in]      heap        memory heap
*/
void trx_undo_update_rec_by_txn_fields(const dict_index_t *index, upd_t *update,
                                       ulint field_nth, txn_info_t txn_info,
                                       mem_heap_t *heap,
                                       const txn_layout_t &layout) {
  ut_ad(update && heap);
  ut_ad(txn_layout_is_arranged(layout));

  switch (layout) {
    case TL_NONE:
      ut_error;
      break;
    case TL_CLOVER:
      trx_undo_update_rec_by_clover_fields(index, update, field_nth, txn_info,
                                           heap);
      break;
    case TL_BAMBOO:
      trx_undo_update_rec_by_bamboo_fields(index, update, field_nth, txn_info,
                                           heap);
      break;
  }

  return;
}

ulint Rlog_undo_rec_reporter::operator()(page_t *undo_page, mtr_t *mtr) const {
  ulint first_free;
  byte *ptr;
  ulint i;

  ut_ad(lizard::dict_index_is_panda(m_index));
  ut_ad(mach_read_from_2(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE) ==
        m_op_type);
  ut_ad(m_layout == TL_BAMBOO);

  first_free =
      mach_read_from_2(undo_page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_FREE);
  ptr = undo_page + first_free;

  ut_ad(first_free <= UNIV_PAGE_SIZE);

  size_t general_size = 2    /* next record offset */
                        + 1  /* type_cmpl */
                        + 1  /* extra type_cmpl */
                        + 11 /* undo_no */
                        + 11 /* table_id */
                        + 11 /* index_id */;
  if (trx_undo_left(undo_page, ptr) < general_size) {
    /* Not enough space for writing the general parameters */
    return (0);
  }

  /* Reserve 2 bytes for the pointer to the next undo log record */
  ptr += 2;

  /* Store first some general parameters to the undo log */
  *ptr++ = TRX_UNDO_BAMBOO_REC | TRX_UNDO_BAMBOO_ROW_LOG;
  *ptr++ = (m_op_type & TRX_UNDO_INSERT_OP) ? TRX_UNDO_INSERT_REC
                                            : TRX_UNDO_DEL_MARK_REC;

  ptr += mach_u64_write_much_compressed(ptr, m_trx->undo_no);

  ptr += mach_u64_write_much_compressed(ptr, m_index->table->id);

  ptr += mach_u64_write_much_compressed(ptr, m_index->id);

  /*----------------------------------------*/
  /* Store all fields */
  for (i = 0; i < dict_index_get_n_fields(m_index); i++) {
    const dfield_t *field = dtuple_get_nth_field(m_entry, i);
    const dict_col_t *col = m_index->fields[i].col;
    ulint flen = dfield_get_len(field);

    if (trx_undo_left(undo_page, ptr) < 5) {
      return (0);
    }

    ptr += mach_write_compressed(ptr, flen);

    if (col->mtype == DATA_SYS) {
      /* Retrieve txn fields from urec_trx, rather than entry. */
      if (trx_undo_left(undo_page, ptr) < flen) {
        return (0);
      }
      switch (col->prtype & DATA_SYS_PRTYPE_MASK) {
        case DATA_TRX_ID:
          ut_ad(flen == DATA_TRX_ID_LEN);
          trx_write_trx_id(ptr, m_urec_trx->trx_id);
          break;
        case DATA_ROLL_PTR:
          ut_ad(flen == DATA_ROLL_PTR_LEN);
          trx_write_roll_ptr(ptr, m_urec_trx->roll_ptr);
          break;
        case DATA_SCN_ID:
          ut_ad(flen == DATA_SCN_ID_LEN);
          trx_write_scn(ptr, m_urec_trx->scn);
          break;
        case DATA_UNDO_PTR:
          ut_ad(flen == DATA_UNDO_PTR_LEN);
          trx_write_undo_ptr(ptr, m_urec_trx->undo_ptr);
          break;
        default:
          ut_error;
      }
      ptr += flen;
    } else if (flen != UNIV_SQL_NULL && flen != 0) {
      if (trx_undo_left(undo_page, ptr) < flen) {
        return (0);
      }

      ut_memcpy(ptr, dfield_get_data(field), flen);
      ptr += flen;
    }
  }
  return (trx_undo_page_set_next_prev_and_add(undo_page, ptr, mtr));
}

/** Builds a row reference from an undo log record derived from row log.
 * @param[in]   ptr      remaining part of a copy of an undo log
                         record, at the start of the row reference;
                         NOTE that this copy of the undo log record must
                         be preserved as long as the row reference is
                         used, as we do NOT copy the data in the
                         record!
  * @param[in]  index   transactional index
  * @param[out] ref     row reference
  * @param[in]  heap    memory heap from which the memory needed is allocated
 @return pointer to remaining part of undo record */
byte *trx_undo_rec_get_row_ref_derived_from_row_log(byte *ptr,
                                                    dict_index_t *index,
                                                    dtuple_t **ref,
                                                    mem_heap_t *heap) {
  ulint ref_len;
  ulint i;

  ut_ad(index && ptr && ref && heap);
  ut_a(dict_index_is_panda(index));
  assert_lizard_dict_index_check(index);

  ref_len = dict_index_get_n_fields(index);

  *ref = dtuple_create(heap, ref_len);

  dict_index_copy_types(*ref, index, ref_len);

  for (i = 0; i < ref_len; i++) {
    dfield_t *dfield;
    const byte *field;
    ulint len;
    ulint orig_len;

    dfield = dtuple_get_nth_field(*ref, i);

    ptr = trx_undo_rec_get_col_val(ptr, &field, &len, &orig_len);

    dfield_set_data(dfield, field, len);
  }
  (*ref)->n_fields_cmp = dict_index_get_n_unique_in_tree(index);

  lizard::row_search_entry_adjust_cmp_fields(index, *ref);
  return (ptr);
}

}  // namespace lizard
