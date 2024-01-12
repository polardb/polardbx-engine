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

/** @file include/lizard0row.h
 lizard row operation.

 Created 2020-04-06 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0row_h
#define lizard0row_h

#include "lizard0data0types.h"
#include "lizard0undo0types.h"

#include "btr0cur.h"
#include "mem0mem.h"
#include "rem0types.h"

struct ins_node_t;
struct que_thr_t;
struct dtuple_t;
struct dict_index_t;
struct page_zip_des_t;
struct trx_t;
struct upd_t;
struct buf_block_t;
struct row_prebuilt_t;
struct btr_pcur_t;

/**
  Lizard Record Format:

  Based on the innodb compact record format, add scn, undo_ptr system columns:

  Table [id, name, row_id, trx_id, roll_ptr, scn, undo_ptr];

  1) Cluster index:
     [id, trx_id, roll_ptr, scn, undo_ptr, name];

  2) Secondary index:
     [name, id]

  Both durable and temporary table will add two new columns expect of
  intrinsic temproary table.


  Revision: Add gcn into record.
*/
namespace lizard {

/*=============================================================================*/
/* lizard record insert */
/*=============================================================================*/

/**
  Allocate row buffers for lizard fields.

  @param[in]      node      Insert node
*/
void ins_alloc_lizard_fields(ins_node_t *node);

/*=============================================================================*/
/* lizard record update */
/*=============================================================================*/

/**
  Validate the scn and undo_ptr fields in record.
  @param[in]      index     dict_index_t
  @param[in]      scn_ptr_in_rec   scn_id position in record
  @param[in]      scn_pos   scn_id no in system cols
  @param[in]      rec       record
  @param[in]      offsets   rec_get_offsets(rec, idnex)
  @retval true if verification passed
*/
bool validate_lizard_fields_in_record(const dict_index_t *index,
                                      const byte *scn_ptr_in_rec, ulint scn_pos,
                                      const rec_t *rec, const ulint *offsets);

/**
  Fill SCN and UBA into index entry.
  @param[in]    thr       query
  @param[in]    entry     dtuple
  @param[in]    index     cluster index
*/
void row_upd_index_entry_lizard_field(que_thr_t *thr, dtuple_t *entry,
                                      dict_index_t *index);

/**
  Get address of scn field in record.
  @param[in]      rec       record
  @paramp[in]     index     cluster index
  @param[in]      offsets   rec_get_offsets(rec, idnex)
  @retval pointer to scn_id
*/
byte *row_get_scn_ptr_in_rec(rec_t *rec, const dict_index_t *index,
                             const ulint *offsets);

/**
  Write the scn and undo_ptr of the physical record.
  @param[in]      ptr       scn pointer
  @param[in]      txn_desc  txn description
*/
void row_upd_rec_write_lizard_fields(byte *ptr, const txn_desc_t *txn_desc);

/**
  Write the scn and undo_ptr of the physical record.
  @param[in]      ptr       scn pointer
  @param[in]      scn       SCN
  @param[in]      undo_ptr  UBA
*/
void row_upd_rec_write_scn_and_undo_ptr(byte *ptr, const scn_t scn,
                                        const undo_ptr_t undo_ptr,
                                        const gcn_t gcn);

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
                               const txn_desc_t *txn);

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
                            const ulint *offsets);

/**
  Read the undo ptr from record

  @param[in]      rec         record
  @param[in]      index       dict_index_t, must be cluster index
  @param[in]      offsets     rec_get_offsets(rec, index)

  @retval         scn id
*/
undo_ptr_t row_get_rec_undo_ptr(const rec_t *rec, const dict_index_t *index,
                                const ulint *offsets);

/**
  Read the gcn id from record

  @param[in]      rec         record
  @param[in]      index       dict_index_t, must be cluster index
  @param[in]      offsets     rec_get_offsets(rec, index)

  @retval         gcn id
*/
gcn_t row_get_rec_gcn(const rec_t *rec, const dict_index_t *index,
                      const ulint *offsets);
/**
  Read the undo ptr state from record

  @param[in]      rec         record
  @param[in]      index       dict_index_t, must be cluster index
  @param[in]      offsets     rec_get_offsets(rec, index)

  @retval         scn id
*/
bool row_get_rec_undo_ptr_is_active(const rec_t *rec, const dict_index_t *index,
                                    const ulint *offsets);

/**
  Get the relative offset in record by offsets
  @param[in]      index
  @param[in]      type
  @param[in]      offsets
*/
ulint row_get_lizard_offset(const dict_index_t *index, ulint type,
                            const ulint *offsets);

/**
  Read the txn from record

  @param[in]      rec         record
  @param[in]      index       dict_index_t, must be cluster index
  @param[in]      offsets     rec_get_offsets(rec, index)
  @param[out]     txn_rec     lizard transaction attributes
*/
void row_get_txn_rec(const rec_t *rec, const dict_index_t *index,
                     const ulint *offsets, txn_rec_t *txn_rec);

/**
  Write the scn and undo ptr into the update vector
  @param[in]      index       index object
  @param[in]      update      update vector
  @param[in]      field_nth   the nth from SCN id field
  @param[in]      txn_info    txn information
  @param[in]      heap        memory heap
*/
void trx_undo_update_rec_by_lizard_fields(const dict_index_t *index,
                                          upd_t *update, ulint field_nth,
                                          txn_info_t txn_info,
                                          mem_heap_t *heap);

/*=============================================================================*/
/* lizard record write/parse redo */
/*=============================================================================*/

/**
  Read the scn and undo_ptr from undo record
  @param[in]      ptr       undo record
  @param[out]     txn_info  SCN and UBA info
  @retval begin of the left undo data.
*/
byte *trx_undo_update_rec_get_lizard_cols(const byte *ptr,
                                          txn_info_t *txn_info);

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
                                         const buf_block_t *block, mtr_t *mtr);

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
                                       mtr_t *mtr MY_ATTRIBUTE((unused)));

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
                                gcn_t *gcn);

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
                                           const txn_rec_t *txn_rec);

/**
  Updates the scn and undo_ptr field in a clustered index record in
  database recovery.
  @param[in]      rec       record
  @param[in]      page_zip
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
                                           const gcn_t gcn);

/**
  After search row complete, do the cleanout.

  @param[in]      prebuilt

  @retval         count       cleaned records count
*/
ulint row_cleanout_after_read(row_prebuilt_t *prebuilt);

/**
  Collect the page which need to cleanout

  @param[in]        trx_id
  @param[in]        trx_zeus        trx description and state
  @param[in]        rec             current rec
  @param[in]        index           cluster index
  @parma[in]        offsets         rec_get_offsets(rec, index)
  @param[in/out]    pcur            cursor

  @retval           true            collected
  @retval           false           didn't collect
*/
bool row_cleanout_collect(trx_id_t trx_id, txn_rec_t &txn_rec, const rec_t *rec,
                          const dict_index_t *index, const ulint *offsets,
                          btr_pcur_t *pcur);
/**
  Collect the tcn which need to cache.

  @param[in]        trx_id
  @param[in]        txn_rec         txn description and state
  @param[in]        rec             current rec
  @param[in]        index           cluster index
  @parma[in]        offsets         rec_get_offsets(rec, index)
  @param[in/out]    pcur            cursor

  @retval           true            collected
  @retval           false           didn't collect
*/

bool tcn_collect(trx_id_t trx_id, txn_rec_t &txn_rec, const rec_t *rec,
                 const dict_index_t *index, const ulint *offsets,
                 btr_pcur_t *pcur);

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
                      const dict_index_t *index, const ulint *offsets);

/**
  Collect rows updated in current transaction.

  @param[in]        thr             current session
  @param[in]        cursor          btr cursor
  @param[in]        rec             current rec
  @param[in]        flags           mode flags for btr_cur operations
*/
extern void commit_cleanout_collect(que_thr_t *thr, btr_cur_t *cursor,
                                    rec_t *rec, ulint flags);

/**
  Cleanout rows at transaction commit.

  @param[in]        trx             current transation
  @param[in]        txn_rec         trx info
*/
extern void commit_cleanout_do(trx_t *trx, const txn_rec_t &txn_rec);

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
                     const ulint *offsets);

/**
  Debug the undo_ptr state in record is active state
  @param[in]      rec       record
  @param[in]      index     cluster index
  @parma[in]      offsets   rec_get_offsets(rec, index)

  @retval         true      Success
*/
bool row_undo_ptr_is_active(const rec_t *rec, const dict_index_t *index,
                            const ulint *offsets);

/**
  Debug the undo_ptr and scn in record is matched.
  @param[in]      rec       record
  @param[in]      index     cluster index
  @parma[in]      offsets   rec_get_offsets(rec, index)

  @retval         true      Success
*/
bool row_lizard_valid(const rec_t *rec, const dict_index_t *index,
                      const ulint *offsets);

/**
  Debug the undo_ptr and scn in record is matched.
  @param[in]      rec       record
  @param[in]      index     cluster index
  @parma[in]      offsets   rec_get_offsets(rec, index)

  @retval         true      Success
*/
bool row_lizard_has_cleanout(const rec_t *rec, const dict_index_t *index,
                             const ulint *offsets);

#endif /* UNIV_DEBUG || LIZARD_DEBUG */

} /* namespace lizard */

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
#define assert_row_scn_initial(rec, index, offsets)     \
  do {                                                  \
    ut_a(lizard::row_scn_initial(rec, index, offsets)); \
  } while (0)

#define assert_row_undo_ptr_is_active(rec, index, offsets)     \
  do {                                                         \
    ut_a(lizard::row_undo_ptr_is_active(rec, index, offsets)); \
  } while (0)

#define assert_row_lizard_valid(rec, index, offsets)     \
  do {                                                   \
    ut_a(lizard::row_lizard_valid(rec, index, offsets)); \
  } while (0)

#define assert_row_lizard_has_cleanout(rec, index, offsets)     \
  do {                                                          \
    ut_a(lizard::row_lizard_has_cleanout(rec, index, offsets)); \
  } while (0)
#else
#define assert_row_scn_initial(rec, index, offsets)
#define assert_row_undo_ptr_is_active(rec, index, offsets)
#define assert_row_lizard_valid(rec, index, offsets)
#define assert_row_lizard_has_cleanout(rec, index, offsets)

#endif /* UNIV_DEBUG || LIZARD_DEBUG */

#endif /* lizard0row_h */
