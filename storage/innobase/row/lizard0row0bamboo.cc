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

/** @file include/lizard0row0bamboo.h
 bamboo layout txn field operation.

 Created 2024-12-06 by Jianwei.zhao
 *******************************************************/

#include "row0row.h"

#include "lizard0row0bamboo.h"
#include "lizard0undo.h"

namespace lizard {
/**
  Fill BamBoo layout txn fields into index entry.
  @param[in]    thr       query
  @param[in]    entry     dtuple
  @param[in]    index     panda index
*/
void row_upd_index_entry_bamboo_field(que_thr_t *thr, dtuple_t *entry,
                                      dict_index_t *index) {
  dfield_t *dfield = nullptr;
  byte *ptr = nullptr;
  ulint pos = 0;
  const txn_desc_t *txn_desc = nullptr;

  ut_ad(thr && entry && index);
  /** Only non-temporary panda index support bamboo layout. */
  ut_ad(dict_index_is_panda(index));
  ut_ad(!index->table->is_temporary());

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
}

/**
  Write bamboo layout fields of the physical record.
  @param[in]      ptr       scn pointer
  @param[in]      scn       SCN
  @param[in]      undo_ptr  UBA
*/
void row_upd_rec_write_bamboo(byte *ptr, const scn_t scn,
                              const undo_ptr_t undo_ptr) {
  mach_write_to_8(ptr, scn);
  mach_write_to_8(ptr + DATA_SCN_ID_LEN, undo_ptr);
}

/**
  Write bamboo layout fields of the physical record.
  @param[in,out]  rec       record
  @param[in]      index     index with system columns
  @param[in]      offsets   rec_get_offsets(rec, idnex)
  @param[in]      scn       SCN
  @param[in]      undo_ptr  UBA
*/
void row_upd_rec_write_bamboo(rec_t *rec, const dict_index_t *index,
                              const ulint *offsets, const scn_t scn,
                              const undo_ptr_t uba) {
  ut_ad(rec_offs_validate(rec, index, offsets));
  ut_ad(dict_index_is_panda(index));

  ulint len;
  ulint offset = rec_get_nth_field_offs(
      index, offsets, index->get_sys_col_pos(DATA_SCN_ID), &len);
  ut_ad(len == DATA_SCN_ID_LEN);

  trx_write_scn(rec + offset, scn);
  trx_write_undo_ptr(rec + offset + DATA_SCN_ID_LEN, uba);
}

/**
  Modify bamboo layout fields of record. It will handle compress pages.
  @param[in,out]  rec       record
  @param[in]      page_zip
  @param[in]      index     cluster index
  @param[in]      offsets   rec_get_offsets(rec, idnex)
  @param[in]      txn_desc  txn description
*/
void row_upd_rec_bamboo_fields_low(rec_t *rec, page_zip_des_t *page_zip,
                                   const dict_index_t *index,
                                   const ulint *offsets, const scn_t scn,
                                   const undo_ptr_t uba) {
  ut_ad(dict_index_is_panda(index));
  assert_undo_ptr_allocated(uba);

  if (page_zip) {
    /** panda didn't support compress table. */
    ut_a(0);
  } else {
    row_upd_rec_write_bamboo(rec, index, offsets, scn, uba);
  }
}

/**
  Updates bamboo layout field in a panda index record when
  cleanout because of update.
  @param[in/out]  rec       record
  @param[in/out]  page_zip  compressed page, or NULL
  @param[in]      index     cluster index
  @param[in]      offsets   rec_get_offsets(rec, idnex)
  @param[in]      txn_rec   scn/uba info
*/
void row_upd_rec_bamboo_fields_in_cleanout(rec_t *rec, page_zip_des_t *page_zip,
                                           const dict_index_t *index,
                                           const ulint *offsets,
                                           const txn_rec_t *txn_rec) {
  ut_ad(dict_index_is_panda(index));
  ut_ad(!index->table->skip_alter_undo);
  ut_ad(!index->table->is_temporary());

  lizard_ut_ad(txn_rec->is_committed());
  row_upd_rec_bamboo_fields_low(rec, page_zip, index, offsets, txn_rec->scn,
                                txn_rec->undo_ptr);
}

/**
  Get the relative offset in record by offsets
  @param[in]      index
  @param[in]      type
  @param[in]      offsets
*/
static ulint row_get_bamboo_offset(const dict_index_t *index, ulint type,
                                   const ulint *offsets) {
  ulint offset = 0;
  ut_ad(!index->table->is_temporary());
  ut_ad(index->is_panda());

  offset = index->trx_id_offset;
  if (!offset) {
    offset = row_get_trx_id_offset(index, offsets);
  }

  switch (type) {
    case DATA_UNDO_PTR:
      offset += DATA_SCN_ID_LEN;
      [[fallthrough]];

    case DATA_SCN_ID:
      offset += DATA_ROLL_PTR_LEN;
      [[fallthrough]];

    case DATA_ROLL_PTR:
      offset += DATA_TRX_ID_LEN;
      [[fallthrough]];

    case DATA_TRX_ID:
      break;

    default:
      ut_ad(0);
  }

  return offset;
}

/**
  Read bamboo layout txn from record

  @param[in]      rec         record
  @param[in]      index       dict_index_t, must be cluster index
  @param[in]      offsets     rec_get_offsets(rec, index)
  @param[out]     txn_rec     lizard transaction attributes
*/
void row_get_bamboo_txn_rec(const rec_t *rec, const dict_index_t *index,
                            const ulint *offsets, txn_rec_t *txn_rec) {
  ulint offset;

  ut_ad(rec_offs_validate(rec, index, offsets));
  ut_ad(index->is_panda());

  offset = row_get_bamboo_offset(index, DATA_TRX_ID, offsets);

  txn_rec->trx_id = trx_read_trx_id(rec + offset);
  offset += (DATA_TRX_ID_LEN + DATA_ROLL_PTR_LEN);

  txn_rec->scn = trx_read_scn(rec + offset);
  offset += DATA_SCN_ID_LEN;

  txn_rec->undo_ptr = trx_read_undo_ptr(rec + offset);

  /** GCN didn't exist on bamboo layout record.*/
  txn_rec->gcn = GCN_NULL;

  /** Confirm validation of txn rec. */
  ut_ad(lizard::txn_rec_validate(txn_rec, index));
}

/**
 * Bamboo Layout row has been cleanout.
  @param[in]      rec       record
  @param[in]      index     cluster index
  @parma[in]      offsets   rec_get_offsets(rec, index)

  @retval         true      cleanout
*/
bool row_bamboo_has_cleanout(const rec_t *rec, const dict_index_t *index,
                             const ulint *offsets) {
  undo_addr_t undo_addr;
  txn_rec_t txn_rec;
  ut_ad(index->is_panda());
  ut_ad(!index->table->is_temporary());
  /**
    Skip the REC_STATUS_NODE_PTR, REC_STATUS_INFIMUM, REC_STATUS_SUPREMUM
  */
  if (rec_get_status(rec) == REC_STATUS_ORDINARY) {
    row_get_bamboo_txn_rec(rec, index, offsets, &txn_rec);
    undo_addr.decode(txn_rec.undo_ptr);
    /** UBA is valid */
    undo_addr_validation(&undo_addr, index);
    /** commit */
    ut_a(txn_rec.is_committed());
    /** valid scn */
    ut_a(txn_rec.scn != SCN_NULL);
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
bool row_bamboo_is_valid(const rec_t *rec, const dict_index_t *index,
                         const ulint *offsets) {
  undo_addr_t undo_addr;
  txn_rec_t txn_rec;
  ut_ad(index->is_panda());
  ut_ad(!index->table->is_temporary());

  /**
    Skip the REC_STATUS_NODE_PTR, REC_STATUS_INFIMUM, REC_STATUS_SUPREMUM
    Skip the non-compact record
  */
  if (rec_get_status(rec) == REC_STATUS_ORDINARY) {
    row_get_bamboo_txn_rec(rec, index, offsets, &txn_rec);

    undo_addr.decode(txn_rec.undo_ptr);

    /** UBA is valid */
    undo_addr_validation(&undo_addr, index);

    /** Scn and trx state are matched */
    ut_a(txn_rec.is_active() == (txn_rec.scn == SCN_NULL));
  }
  return true;
}

/*=============================================================================*/
/* Bamboo Layout record write/parse redo */
/*=============================================================================*/

/**
  Write redo log to the buffer about updates of scn and uba.
  @param[in]      index     index of the record
  @param[in]      txn_rec   txn info of the record
  @param[in]      log_ptr   pointer to a buffer opened in mlog
  @param[in]      mtr       mtr

  @return new pointer to mlog
*/
byte *row_upd_write_bamboo_vals_to_log(const dict_index_t *index,
                                       const txn_rec_t *txn_rec, byte *log_ptr,
                                       mtr_t *mtr MY_ATTRIBUTE((unused))) {
  ut_ad(dict_index_is_panda(index));
  ut_ad(!index->is_clustered());
  ut_ad(mtr);
  ut_ad(txn_rec);

  log_ptr +=
      mach_write_compressed(log_ptr, index->get_sys_col_pos(DATA_SCN_ID));

  log_ptr += mach_u64_write_compressed(log_ptr, txn_rec->scn);

  trx_write_undo_ptr(log_ptr, txn_rec->undo_ptr);
  log_ptr += DATA_UNDO_PTR_LEN;

  return log_ptr;
}

/**
  Parses the log data of bamboo field values.
  @param[in]      ptr       buffer
  @param[in]      end_ptr   buffer end
  @param[out]     pos       SCN position in record
  @param[out]     scn       scn
  @param[out]     undo_ptr  uba

  @return log data end or NULL
*/
byte *row_upd_parse_bamboo_vals(const byte *ptr, const byte *end_ptr,
                                ulint *pos, scn_t *scn, undo_ptr_t *undo_ptr) {
  *pos = mach_parse_compressed(&ptr, end_ptr);

  if (ptr == nullptr) return nullptr;

  *scn = mach_u64_parse_compressed(&ptr, end_ptr);

  if (ptr == nullptr) return nullptr;

  if (end_ptr < ptr + DATA_UNDO_PTR_LEN) {
    return (nullptr);
  }

  *undo_ptr = trx_read_undo_ptr(ptr);
  ptr += DATA_UNDO_PTR_LEN;

  return const_cast<byte *>(ptr);
}

/**
  Updates the scn and undo_ptr field in a index record in
  database recovery.
  @param[in/out]  rec       record
  @param[in/out]  page_zip  compressed page, or NULL
  @param[in]      pos       SCN position in rec
  @param[in]      index     index
  @param[in]      offsets   rec_get_offsets(rec, idnex)
  @param[in]      scn       SCN
  @param[in]      undo_ptr  UBA
*/
void row_upd_rec_bamboo_fields_in_recovery(rec_t *rec, page_zip_des_t *page_zip,
                                           const dict_index_t *index, ulint pos,
                                           const ulint *offsets,
                                           const scn_t scn,
                                           const undo_ptr_t undo_ptr) {
  /** index->type (Log_Dummy) will not be set rightly if it's non-compact
  format, see function **mlog_parse_index** */
  ut_ad(!rec_offs_comp(offsets) || dict_index_is_panda(index));
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
    /** Panda index didn't support compress table. */
    ut_a(0);
  } else {
    byte *field;
    ulint len;

    field =
        const_cast<byte *>(rec_get_nth_field(index, rec, offsets, pos, &len));
    ut_ad(len == DATA_SCN_ID_LEN);

    row_upd_rec_write_bamboo(field, scn, undo_ptr);
  }
}

}  // namespace lizard
