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

#include "rem0types.h"
#include "lizard0data0types.h"
#include "lizard0undo0types.h"

struct ins_node_t;
struct que_thr_t;
struct dtuple_t;
struct dict_index_t;
struct page_zip_des_t;
struct trx_t;

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
  Fill SCN and UBA into index entry.
  @param[in]    thr       query
  @param[in]    entry     dtuple
  @param[in]    index     cluster index
*/
void row_upd_index_entry_lizard_field(que_thr_t *thr, dtuple_t *entry,
                                      dict_index_t *index);

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
                               const trx_t *trx);

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
                               const txn_desc_t *txn_desc);

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
bool row_undo_ptr_is_active(const rec_t *rec,
                            const dict_index_t *index,
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

#endif /* UNIV_DEBUG || LIZARD_DEBUG */

} /* namespace lizard */

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
#define assert_row_scn_initial(rec, index, offsets)            \
  do {                                                         \
    ut_a(lizard::row_scn_initial(rec, index, offsets));        \
  } while (0)

#define assert_row_undo_ptr_is_active(rec, index, offsets)     \
  do {                                                         \
    ut_a(lizard::row_undo_ptr_is_active(rec, index, offsets)); \
  } while (0)

#define assert_row_lizard_valid(rec, index, offsets)           \
  do {                                                         \
    ut_a(lizard::row_lizard_valid(rec, index, offsets));       \
  } while (0)

#else
#define assert_row_scn_initial(rec, index, offsets)
#define assert_row_undo_ptr_is_active(rec, index, offsets)
#define assert_row_lizard_valid(rec, index, offsets)

#endif /* UNIV_DEBUG || LIZARD_DEBUG */

#endif /* lizard0row_h */
