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
#include "btr0types.h"
#include "data0data.h"

#include "lizard0data0types.h"
#include "lizard0dict0dict.h"
#include "page0types.h"

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
 * @param[in]     mode            search mode
 * @param[in]     latch_mode      latching mode
 * @param[in]     mtr             Mini-transaction handle
 *
 * @return        True and gpp offset if successful positioning, False otherwise
 */
std::pair<bool, ulint> btr_cur_guess_clust_by_gpp(dict_index_t *clust_idx,
                                                  const dict_index_t *sec_idx,
                                                  const dtuple_t *clust_ref,
                                                  const Slice *sec_slc,
                                                  btr_pcur_t *clust_pcur,
                                                  page_cur_mode_t mode, ulint latch_mode,
    mtr_t *mtr);

/**
 * Attempts to position a persistent cursor on a index record
 * based on the gpp_no.
 *
 * @param[in]     idx             Index
 * @param[in]     ref             Reference tuple for the index
 * @param[in,out] btr_cur         Cursor for the index
 * @param[in]     mode            search mode
 * @param[in]     latch_mode      latching mode
 * @param[in]     mtr             Mini-transaction handle
 *
 * @return        True if successful positioning, False otherwise
 */
bool btr_cur_guess_by_gpp_low(gpp_no_t gpp_no, dict_index_t *idx,
                              const dtuple_t *ref, btr_cur_t *btr_cur,
                              page_cur_mode_t mode, ulint latch_mode,
                              mtr_t *mtr);

/** Sets a secondary panda index record delete mark to true or false.
  @param[in]      layout     txn layout
  @param[in]      cursor     cursor
  @param[in]      thr        query thread
  @param[in]      flags      flags
  @param[in]      val        value to set
  @param[in]      mtr        mtr
 @return DB_SUCCESS, DB_LOCK_WAIT, or error number */
dberr_t btr_cur_del_mark_set_panda_sec_rec_low(const txn_layout_t &layout,
                                               btr_cur_t *cursor,
                                               que_thr_t *thr, ulint flags,
                                               bool val, mtr_t *mtr);

/** Sets a secondary hippo index record delete mark to true or false.
  @param[in]      cursor     cursor
  @param[in]      thr        query thread
  @param[in]      flags      flags
  @param[in]      val        value to set
  @param[in]      sec_tid    secondary transaction id
  @param[in]      mtr        mtr
 @return DB_SUCCESS, DB_LOCK_WAIT, or error number */
dberr_t btr_cur_del_mark_set_hippo_sec_rec_low(btr_cur_t *cursor,
                                               que_thr_t *thr, ulint flags,
                                               bool val, trx_id_t sec_tid,
                                               mtr_t *mtr);

/** Sets a secondary ancient index record delete mark to true or false.
  @param[in]      cursor     cursor
  @param[in]      thr        query thread
  @param[in]      flags      flags
  @param[in]      val        value to set
  @param[in]      mtr        mtr
 @return DB_SUCCESS, DB_LOCK_WAIT, or error number */
dberr_t btr_cur_del_mark_set_ancient_sec_rec_low(btr_cur_t *cursor,
                                                 que_thr_t *thr, ulint flags,
                                                 bool val, mtr_t *mtr);

/** Writes the redo log record for delete marking of a
secondary index rec with tid column.
@param[in]      rec        record
@param[in]      index      index of the record
@param[in]      trx_id     transaction id
@param[in]      trx_id_pos position of the trx_id in the record
@param[in]      mtr        mtr
*/
void btr_cur_del_mark_set_hippo_sec_rec_log(rec_t *rec,
                                            const dict_index_t *index, bool val,
                                            trx_id_t trx_id, ulint trx_id_pos,
                                            mtr_t *mtr);

/** Writes the redo log record for a delete mark setting of a normal secondary
index record.
@param[in]      rec        record
@param[in]      val        value to set
@param[in]      mtr        mtr
*/
void btr_cur_del_mark_set_ancient_sec_rec_log(rec_t *rec, /*!< in: record */
                                              bool val, /*!< in: value to set */
                                              mtr_t *mtr); /*!< in: mtr */

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

/** Parses the redo log record for delete marking or unmarking of a
  secondary index rec with tid column.
  @param[in]      ptr         buffer
  @param[in]      end_ptr     buffer end
  @param[in,out]  page        page or NULL
  @param[in,out]  page_zip    compressed page, or NULL
  @param[in]      index       index corresponding to page
 @return end of log record or NULL */
extern byte *btr_cur_parse_del_mark_set_hippo_sec_rec(byte *ptr, byte *end_ptr,
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
  @param[in]      layout  TL_BAMBOO or TL_NONE or TL_LOTUS
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
  @param[in]      layout  TL_BAMBOO or TL_NONE or TL_LOTUS
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

inline void btr_cur_sea_hit() {
  if (srv_polarx_btree_search_mode) {
    polarx_btree_search_mode_stat(true);
  }
  if (UNIV_UNLIKELY(srv_btr_search_enabled)) {
    btr_cur_n_sea++;
  }
}
inline void btr_cur_sea_miss() {
  if (srv_polarx_btree_search_mode) {
    polarx_btree_search_mode_stat(false);
  }
  if (UNIV_UNLIKELY(srv_btr_search_enabled)) {
    btr_cur_n_non_sea++;
  }
}

extern dberr_t btr_cur_optimistic_insert_spc(
    ulint flags,                /*!< in: undo logging and locking flags: if not
                                zero, the parameters index and thr should be
                                specified */
    const txn_layout_t &layout, /*!< in: txn layout */
    btr_cur_t *cursor,          /*!< in: cursor on page after which to insert;
                                cursor stays valid */
    Slice **slc, Slice_ctx *sctx,
    mem_heap_t **heap,   /*!< in/out: pointer to memory heap, or NULL */
    dtuple_t *entry,     /*!< in/out: entry to insert */
    big_rec_t **big_rec, /*!< out: big rec vector whose fields have to
                         be stored externally by the caller, or
                         NULL */
    que_thr_t *thr,      /*!< in: query thread or NULL */
    mtr_t *mtr);         /*!< in/out: mini-transaction;
                         if this function returns DB_SUCCESS on
                         a leaf page of a secondary index in a
                         compressed tablespace, the caller must
                         mtr_commit(mtr) before latching
                         any further pages */

}  // namespace lizard

#endif
