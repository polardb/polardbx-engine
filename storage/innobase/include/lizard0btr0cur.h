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

/** @file include/lizard0btr0cur.h
   Lizard index tree cursor


 Created 2024-04-12 by Yichang.Song
 *******************************************************/

#ifndef lizard0btr0cur_h
#define lizard0btr0cur_h

#include "btr0cur.h"
#include "data0data.h"

#include "lizard0data0types.h"
#include "lizard0dict.h"

namespace lizard {
static inline gpp_no_t btr_cur_get_page_no(const btr_cur_t *cursor) {
  return btr_cur_get_block(cursor)->get_page_no();
}

/**
 * Attempts to position a persistent cursor on a clustered index
 * record based on the gpp_no read from the secondary index record.
 *
 * @param[in]     clust_idx       Clustered index
 * @param[in]     sec_idx         Secondary index
 * @param[in]     clust_ref       Reference tuple for the clustered index
 * @param[in]     sec_rec         Secondary index record
 * @param[in,out] clust_pcur      Persistent cursor for the clustered index
 * @param[out]    sec_offsets     Offsets array for the secondary record
 * @param[in]     latch_mode      latching mode
 * @param[in]     mtr             Mini-transaction handle
 *
 * @return        True and gpp offset if successful positioning, False otherwise
 */
std::pair<bool, ulint> btr_cur_guess_clust_by_gpp(
    dict_index_t *clust_idx, const dict_index_t *sec_idx,
    const dtuple_t *clust_ref, const rec_t *sec_rec, btr_pcur_t *clust_pcur,
    const ulint *sec_offsets, ulint latch_mode, mtr_t *mtr);

/** Writes the redo log record for a delete mark setting of
  secondary index record.
  @param[in,out]  rec        record
  @param[in]      val        value to set
  @param[in]      index      index of the record
  @param[in]      trx_id     transaction id
  @param[in]      roll_ptr   roll ptr to the undo log record
  @param[in]      txn_rec    txn record info
  @param[in]      offsets    offsets of the record
  @param[in]      layout     txn layout
  @param[in]      page_zip   compressed page
  @param[in]      mtr        mtr
*/
extern void btr_cur_del_mark_set_sec_rec_log(
    rec_t *rec, bool val, dict_index_t *index, trx_id_t trx_id,
    roll_ptr_t roll_ptr, const txn_rec_t *txn_rec, ulint *offsets,
    const txn_layout_t &layout, page_zip_des_t *page_zip, mtr_t *mtr);

/** Parses the redo log record for delete marking or unmarking of a panda
 index record.
  @param[in]      ptr         buffer
  @param[in]      end_ptr     buffer end
  @param[in,out]  page        page or NULL
  @param[in,out]  page_zip    compressed page, or NULL
  @param[in]      index       index corresponding to page
 @return end of log record or NULL */
extern byte *btr_cur_parse_del_mark_set_panda_sec_rec(byte *ptr, byte *end_ptr,
                                                  page_t *page,
                                                  page_zip_des_t *page_zip,
                                                  dict_index_t *index);

extern void btr_cur_print_duplicate_error_in_uk_online(
    btr_cur_t *btr_cur, const dict_index_t *index, const dtuple_t *entry,
    ut::Location loc);

/**
  Perform an insert operation by writing row log for the creating index.
  It is assumed that the S-latch or SX-latch or X-latch of the index has been
  hold. Like btr_cur_optimistic_insert, the undo is also generated if needed
  before writing row log. The operation dose not succeed if there is too
  little space for undo or writing row log failed. In such case, the creating
  index will be marked as corrupted, instead of return ERROR.

  @param[in/out]  index   index that is creating
  @param[in]      trx     transaction
  @param[in/out]  entry   The entry to be inserted
  @param[in]      flags   care flags: BTR_NO_UNDO_LOG_FLAG, BTR_KEEP_SYS_FLAG
  @param[in]      layout  TL_BAMBOO or TL_NONE
*/
extern void btr_cur_rlog_insert(dict_index_t *index, trx_t *trx,
                                dtuple_t *entry, ulint flags,
                                const txn_layout_t &layout);

/**
  Perform a delete operation by writing row log for the creating index.
  It is assumed that the S-latch or SX-latch or X-latch of the index has been
  hold. Like btr_cur_optimistic_update, the undo is also generated if needed
  before writing row log. The operation dose not succeed if there is too
  little space for undo or writing row log failed. In such case, the creating
  index will be marked as corrupted, instead of return ERROR.

  @param[in/out]  index   index that is creating
  @param[in]      trx     transaction
  @param[in/out]  entry   The entry to be deleted
  @param[in]      flags   care flags: BTR_NO_UNDO_LOG_FLAG, BTR_KEEP_SYS_FLAG
  @param[in]      layout  TL_BAMBOO or TL_NONE
*/
extern void btr_cur_rlog_delete(dict_index_t *index, trx_t *trx,
                                dtuple_t *entry, ulint flags,
                                const txn_layout_t &layout);

/**
  Perform an insert operation by writing row log for the creating index.
  See **btr_cur_rlog_insert**.

  @return false if the index is completed so actually insert on the tree is
          needed.
*/
extern bool btr_cur_rlog_insert_try(dict_index_t *index, trx_t *trx,
                                    dtuple_t *entry, ulint flags,
                                    const txn_layout_t &layout);

/**
  Perform a delete operation by writing row log for the creating index.
  See **btr_cur_rlog_delete**.

  @return false if the index is completed so actually delete on the tree is
          needed.
*/
extern bool btr_cur_rlog_delete_try(dict_index_t *index, trx_t *trx,
                                    dtuple_t *entry, ulint flags,
                                    const txn_layout_t &layout);
}  // namespace lizard

#endif
