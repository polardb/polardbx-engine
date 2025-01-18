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

#include "lizard0cleanout.h"
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
struct type_cmpl_t;

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
/* Record insert */
/*=============================================================================*/

/**
  Allocate row buffers for txn fields.

  @param[in]      node      Insert node
*/
void ins_alloc_txn_fields(ins_node_t *node);

/*=============================================================================*/
/* Record update */
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
  Fill txn fields into index entry.
  @param[in]    thr       query
  @param[in]    entry     dtuple
  @param[in]    index     panda index
  @param[in]	layout
*/
void row_upd_index_entry_txn_field(que_thr_t *thr, dtuple_t *entry,
                                   dict_index_t *index,
                                   const txn_layout_t &layout);
/**
  Modify txn fields of record.

  @param[in,out] rec   record
  @param[in]     page_zip
  @param[in]     index     cluster index
  @param[in]     offsets   rec_get_offsets(rec, idnex)
  @param[in]     layout    txn layout
  @param[in]     txn       txn description
*/
void row_upd_rec_txn_fields(rec_t *rec, page_zip_des_t *page_zip,
                            const dict_index_t *index, const ulint *offsets,
                            const txn_layout_t &layout, const txn_desc_t *txn);

void row_log_entry_update_txn_field(dtuple_t *entry, const dict_index_t *index,
                                    trx_t *trx);

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
                     txn_rec_t *txn_rec);

/*=============================================================================*/
/* lizard record write/parse redo */
/*=============================================================================*/
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
//                       const dict_index_t *index, const ulint *offsets);

extern void row_search_entry_adjust_cmp_fields(const dict_index_t *index,
                                               dtuple_t *search_entry);

extern void row_rlog_table_entry_adjust_cmp_fields(const dict_index_t *index,
                                                   dtuple_t *rlog_table_entry);

class Panda_entry_cmp_adjust_guard {
 public:
  Panda_entry_cmp_adjust_guard(const dict_index_t *index, dtuple_t *entry)
      : m_entry(nullptr), m_save_fields_cmp(0) {
    if (dict_index_is_panda(index)) {
      m_entry = entry;
      m_save_fields_cmp = dtuple_get_n_fields_cmp(m_entry);
      row_search_entry_adjust_cmp_fields(index, m_entry);
    }
  }

  ~Panda_entry_cmp_adjust_guard() {
    if (m_entry) {
      ut_ad(m_entry->magic_n == dtuple_t::MAGIC_N);
      dtuple_set_n_fields_cmp(m_entry, m_save_fields_cmp);
      m_entry->n_panda_suffix = 0;
    }
  }

 private:
  dtuple_t *m_entry;
  ulint m_save_fields_cmp;
};

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
                                     mtr_t *mtr);

extern bool row_panda_rec_neighbor_unique_check(const rec_t *rec,
                                                const dict_index_t *index);

/**
 * Determine the heap for building old versions in row_prebuilt_t according to
 * the index.
 * @param[in]      prebuilt      Row prebuilt.
 * @param[in]      index         The transactional index.
 * return          heap          The corresponding heap.
 */
mem_heap_t *row_sel_decide_old_vers_heap(const dict_index_t *index,
                                         row_prebuilt_t *prebuilt);

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
                      const ulint *offsets, const txn_layout_t &layout);

/**
  Debug row has cleanout.
  @param[in]      rec       record
  @param[in]      index     cluster index
  @parma[in]      offsets   rec_get_offsets(rec, index)

  @retval         true      Success
*/
bool row_txn_has_cleanout(const rec_t *rec, const dict_index_t *index,
                          const ulint *offsets, const txn_layout_t &layout);

#endif /* UNIV_DEBUG || LIZARD_DEBUG */

} /* namespace lizard */

#if defined UNIV_DEBUG || defined LIZARD_DEBUG

#define assert_row_txn_is_valid(rec, index, offsets, layout)     \
  do {                                                           \
    ut_a(lizard::row_txn_is_valid(rec, index, offsets, layout)); \
  } while (0)

#define assert_row_txn_has_cleanout(rec, index, offsets, layout)     \
  do {                                                               \
    ut_a(lizard::row_txn_has_cleanout(rec, index, offsets, layout)); \
  } while (0)

#else

#define assert_row_txn_is_valid(rec, index, offsets, layout)
#define assert_row_txn_has_cleanout(rec, index, offsets, layout)
#endif /* UNIV_DEBUG || LIZARD_DEBUG */

#endif /* lizard0row_h */
