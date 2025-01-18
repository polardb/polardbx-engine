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

/** @file include/lizard0row0gpp.h
 Row gpp operation.

 Created 2020-04-06 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0row0gpp_h
#define lizard0row0gpp_h

#include "lizard0row.h"

namespace lizard {

/** Whether to enable clustered index record inference during the scan. */
extern bool index_scan_guess_clust_enabled;

/** Whether to enable clustered index record inference during the purge. */
extern bool index_purge_guess_clust_enabled;

/** Whether to enable clustered index record inference during the locking. */
extern bool index_lock_guess_clust_enabled;

#ifdef UNIV_DEBUG
extern gpp_no_t dbug_gpp_no;
#endif /* UNIV_DEBUG */

/**
   Allocate row buffers for GPP_NO field.

   @param[in]      node      Insert node
*/
void ins_alloc_gpp_field(ins_node_t *node);

/**
 * Write GPP_NO after primary key insert.
 *
 * @param[in/out]	insert node
 * @param[in]		index
 * @param[in]		index entry
 * @param[in]		row
 */
void row_ins_clust_write_gpp_no(ins_node_t *node, const dict_index_t *index,
                                dtuple_t *entry, const dtuple_t *row);

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
                               dtuple_t *entry, const dtuple_t *row);

/**
 * Write GPP_NO after primary key insert or just assert it for sec index.
 *
 * @param[in/out]	insert node
 * @param[in]		index
 * @param[in]		index entry
 * @param[in]		row
 */
void row_ins_index_write_gpp_no(ins_node_t *node, const dict_index_t *index,
                                dtuple_t *entry, const dtuple_t *row);

/**
   Allocate row buffers for GPP_NO field of update node's old row.

   @param[in]      node      Insert node
*/
void row_upd_alloc_gpp_field_for_old_row(upd_node_t *node);

/**
   Allocate row buffers for GPP_NO field of update node's new row.

   @param[in]      node      Insert node
*/
void row_upd_alloc_gpp_field_for_new_row(upd_node_t *node);

/**
 * Write GPP_NO after primary key update.
 *
 * @param[in/out]	upd node
 * @param[in]		index
 * @param[in]   index entry
 * @param[in]		upd_row
 */
void row_upd_clust_write_gpp_no(upd_node_t *node, const dict_index_t *index,
                                dtuple_t *entry, const dtuple_t *upd_row);

/*=============================================================================*/
/* lizard record row log */
/*=============================================================================*/
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
                               dtuple_t *entry, const dtuple_t *upd_row);

/**
   Allocate row buffers for GPP_NO field when applying row log table

   @param[in/out]   row
   @param[in]       heap
*/
void row_log_table_alloc_gpp_field(dtuple_t *row, mem_heap_t *heap);

/**
 * Write GPP_NO after row log table apply.
 *
 * @param[in]		gpp_no
 * @param[in]		index
 * @param[in/out]	row
 */
void row_log_table_clust_write_gpp_no(const gpp_no_t &gpp_no,
                                      const dict_index_t *index,
                                      const dtuple_t *row);

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
                                     const gpp_no_t &gpp_no);

/*=============================================================================*/
/* lizard record row purge */
/*=============================================================================*/
/**
   Allocate row buffers for GPP_NO field for purge node.

   @param[in]       node      Purge node
*/
void row_purge_alloc_gpp_field(purge_node_t *node);

/*=============================================================================*/
/* lizard record row undo */
/*=============================================================================*/
/**
   Allocate row buffers for GPP_NO field for undo node.

   @param[in]       node      Undo node
*/
void row_undo_alloc_gpp_field(undo_node_t *node);

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
                                    const gpp_no_t gpp_no);
/**
 * Update gpp no field in secondary index record in database recovery.
 * @param[in]      rec			record
 * @param[in]      page_zip
 * @param[in]      gpp no
 * @param[in]      gpp offset		gpp no position in rec */
void row_upd_rec_gpp_fields_in_recovery(rec_t *rec, page_zip_des_t *page_zip,
                                        page_no_t gpp_no, ulint gpp_offset);

/**
 * Retrieves the offset of the GPP number in a record
 *
 * @param[in] index   Dictionary index object, non-clustered
 * @param[in] offsets Array of field offsets
 * @return            Returns the offset of the GPP number within the record
 */
ulint row_get_gpp_no_offset(const dict_index_t *index, const ulint *offsets);

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
                                          const ulint *offsets);

/**
 * Write the GPP Number
 *
 * @param[in] rec           Pointer to the record
 * @param[in] index         Pointer to the dictionary index object,
 * non-clustered
 * @param[in] gpp_no_offset Offset of the GPP Number
 * @param[in] gpp_no        GPP Number
 */
void row_write_gpp_no(rec_t *rec, const dict_index_t *index,
                      const ulint gpp_no_offset, const gpp_no_t gpp_no);

/**
 * Assert GPP_NO is valid for multi-valued sec index.
 *
 * @param[in]		index
 * @param[in]		multi-value entry
 */
void row_sec_multi_value_assert_gpp_no(const dict_index_t *index,
                                       const dtuple_t *mv_entry);

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
                                    mtr_t *mtr);

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
                                      mtr_t *mtr);

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
                                     mtr_t *mtr);
}  // namespace lizard
 
#if defined UNIV_DEBUG || defined LIZARD_DEBUG
#define lizard_row_ins_sec_assert_gpp_no(node, index, entry, row) \
  do {                                                            \
    lizard::row_ins_sec_assert_gpp_no(node, index, entry, row);   \
  } while (0)

#define lizard_row_upd_sec_assert_gpp_no(node, index, entry, upd_row) \
  do {                                                                \
    lizard::row_upd_sec_assert_gpp_no(node, index, entry, upd_row);   \
  } while (0)

#define lizard_row_log_table_sec_assert_gpp_no(index, entry, row, gpp_no) \
  do {                                                                    \
    lizard::row_log_table_sec_assert_gpp_no(index, entry, row, gpp_no);   \
  } while (0)

#define lizard_row_sec_multi_value_assert_gpp_no(index, mv_entry) \
  do {                                                            \
    lizard::row_sec_multi_value_assert_gpp_no(index, mv_entry);   \
  } while (0)

#else

#define lizard_row_ins_sec_assert_gpp_no(node, index, entry, row)
#define lizard_row_upd_sec_assert_gpp_no(node, index, entry, upd_row)
#define lizard_row_log_table_sec_assert_gpp_no(index, entry, row, gpp_no)
#define lizard_row_sec_multi_value_assert_gpp_no(index, mv_entry)

#endif

#endif
