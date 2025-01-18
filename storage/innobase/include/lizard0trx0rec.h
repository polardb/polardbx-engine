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

/** @file include/lizard0trx0rec.h
 lizard transaction undo log record.

 Created 2024-10-31 by Yichang Song
 *******************************************************/

#ifndef lizard0trx0rec_h
#define lizard0trx0rec_h

#include "lizard0row.h"
#include "lizard0undo0rec0types.h"

class Undo_rec_reporter {
 public:
  explicit Undo_rec_reporter(trx_t *trx, dict_index_t *index,
                             const dtuple_t *entry, txn_layout_t layout)
      : m_trx(trx), m_index(index), m_entry(entry), m_layout(layout) {}

  virtual ulint operator()(page_t *undo_page, mtr_t *mtr) const = 0;

 protected:
  trx_t *m_trx;
  dict_index_t *m_index;
  const dtuple_t *m_entry;
  txn_layout_t m_layout;
};

namespace lizard {

class Rlog_undo_rec_reporter : public Undo_rec_reporter {
 public:
  /**
   * Report undo record derived from row log.
   * @param[in]      op_type      operation type
   * @param[in]      trx          transaction
   * @param[in]      index        index
   * @param[in]      entry        entry
   * @param[in]      urec_trx     values of txn cols
   * @return offset of the inserted entry on the page if succeed, 0 if fail
   */
  explicit Rlog_undo_rec_reporter(ulint op_type, trx_t *trx,
                                  dict_index_t *index, const dtuple_t *entry,
                                  const urec_trx_t *urec_trx)
      : Undo_rec_reporter(trx, index, entry, TL_BAMBOO),
        m_op_type(op_type),
        m_urec_trx(urec_trx) {}

  virtual ulint operator()(page_t *undo_page, mtr_t *mtr) const override;

 private:
  ulint m_op_type;
  const urec_trx_t *m_urec_trx;
};

#ifdef UNIV_DEBUG
extern space_index_t dbug_panda_index_id;
#endif /* UNIV_DEBUG */

/** Write txn columns into undo record.
 * @param[in/out]  ptr             pointer to undo record
 * @param[in]      index           index handler
 * @param[in]      rec             record
 * @param[in]      offsets         offsets
 * @return         pointer to current position
 */
byte *trx_undo_update_rec_write_txn_cols(byte *ptr, const dict_index_t *index,
                                         const rec_t *rec,
                                         const ulint *offsets, const txn_layout_t &layout);
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
                                        space_index_t index_id);

/**
  Read the txn(scn/undo_ptr/gcn) from undo record
  @param[in]      ptr       undo record
  @param[out]     txn_info  txn info
  @retval begin of the left undo data.
*/
byte *trx_undo_update_rec_get_txn_cols(const byte *ptr, txn_info_t *txn_info,
                                       const txn_layout_t &layout);
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
                                       const txn_layout_t &layout);

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
                                                    mem_heap_t *heap);
}  // namespace lizard
#endif
