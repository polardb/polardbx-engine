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

/** @file include/lizard0dict0dict.h
 Special Lizard tablespace definition and in-memory dictionary operation.

 Created 2020-03-19 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0dict0dict_h
#define lizard0dict0dict_h

#include "sql/dd/object_id.h"

#include "univ.i"

#include "api0api.h"
#include "dict0dict.h"
#include "mem0mem.h"

struct dict_table_t;
struct dict_index_t;

#ifdef UNIV_DEBUG
namespace lizard {
/** Debug pd_capacity value. */
extern ulint dbug_pd_capacity;
}  // namespace lizard
#endif

namespace lizard {

class DField_wrapper;

/** Space ID to mark SPC index as corrupt. SPACE_UNKNOWN means disabled. */
extern ulong srv_mark_corrupt_spc_by_space_id;

/** Here define the lizard related dictionary content, including the lizard
tablespace and the implicit txn tablespaces. */
struct dict_lizard {
  /** The lizard tablespace space id */
  static constexpr space_id_t s_lizard_space_id = 0xFFFFFFFA;

  /** The dd::Tablespace::id of innodb_lizard */
  static constexpr dd::Object_id s_dd_lizard_space_id = 3;

  /** Whether the last file of lizard space is auto extend */
  static constexpr const bool s_file_auto_extend = true;

  /** The init file size (bytes) */
  static constexpr const ulint s_file_init_size = 12 * 1024 * 1024;

  /** The last file max size (bytes) */
  static constexpr const ulint s_last_file_max_size = 0;

  /** Lizard tablespace file count */
  static constexpr const ulint s_n_files = 1;

  /** The space name of lizard */
  static const char *s_lizard_space_name;

  /** The file name of lizard space */
  static const char *s_lizard_space_file_name;

  /** The lizard transaction tablespace is from max undo tablespace */
  static constexpr space_id_t s_max_txn_space_id =
      dict_sys_t::s_max_undo_space_id;

  /** The lizard transaction tablespace minmum space id */
  static constexpr space_id_t s_min_txn_space_id =
      dict_sys_t::s_max_undo_space_id - FSP_IMPLICIT_TXN_TABLESPACES + 1;

  /** Lizard: First several undo tbs will be treated as txn tablespace */
  static const char *s_default_txn_space_names[FSP_IMPLICIT_TXN_TABLESPACES];
};

/** Judge the undo tablespace is txn tablespace by name */
extern bool is_txn_space_by_name(const char *name);

/** Judge whether a column is a lizard transactional system column.
 *
 * @param[in]	column name
 *
 * @retval	true if it is DB_SCN_ID, DB_UNDO_PTR or DB_GCN_ID. */
inline bool is_lizard_column(const char *col_name) {
  ut_ad(col_name != nullptr);
  return (strncmp(col_name, "DB_SCN_ID", 9) == 0 ||
          strncmp(col_name, "DB_UNDO_PTR", 11) == 0 ||
          strncmp(col_name, "DB_GCN_ID", 9) == 0);
}

/**
  Add the SCN and UBA column into dict_table_t, for example:
  dict_table_t::col_names "...DB_SCN_ID\0DATA_UNDO_PTR\0..."

  @param[in]      table       dict_table_t
  @param[in]      heap        memory slice
*/
void dict_table_add_functional_columns(dict_table_t *table, mem_heap_t *heap);

/** Add extra functional columns in secondary index following PK Columns.
 *
 * @param[in/out]	new index.
 * @param[in]		index.
 * @param[in]		dictionary table.
 * @param[in]		page type.
 */
void dict_index_add_sec_functional_cols(dict_index_t *new_index,
                                        const dict_index_t *index,
                                        const dict_table_t *table,
                                        page_type_t expected_page_type);

/** Determine the page-dictionary capacity of a prefix compressed (SPC) index.
 *
 * @param[in]	dict index
 *
 * @retval	page-dictionary capacity in bytes, 0 if the index is not
 * prefix compressed. */
uint16_t dict_index_determine_pd_capacity(const dict_index_t *index);

/** Check whether the page-dictionary capacity stored for an index remains
compatible with the current size estimator. A smaller legacy value is allowed
only when a prefix field participates in the index. */
bool dict_index_pd_capacity_is_compatible(const dict_index_t *index);

/** Estimate the upper bound of page-dictionary bytes of an index.
 *
 * The bound is what is left on an empty page after reserving room for two
 * maximum sized records and a safety margin.
 *
 * @param[in]	dict index
 * @param[in]	strict	true to return 0 for a non prefix compressed index,
 * false to estimate the capacity no matter what the page type is.
 *
 * @retval	page-dictionary capacity in bytes. */
uint16_t dict_index_determine_pd_capacity_low(const dict_index_t *index,
                                              bool strict);

/**
  Get ordered field number from sec index for row log.
  @param[in]      index     dict_index_t
  @retval ordered field number
*/
ulint row_log_dict_index_get_ordered_n_fields(const dict_index_t *index);

/** Judge legacy fil page type that will be stored.
 *
 * Legacy file page type including:
 * 1. FIL_PAGE_RTREE
 * 2. FIL_PAGE_SDI
 * 3. FIL_PAGE_INDEX
 *
 * When creating B-Tree, importing B-Tree, or some others
 * The Legacy file page type will be upgraded as:
 * 1. FIL_PAGE_RTREE --> FIL_PAGE_RTREE
 * 2. FIL_PAGE_SDI   --> FIL_PAGE_SDI
 * 3. FIL_PAGE_INDEX --> FIL_PAGE_INDEX_PANDA
 *
 * @param[in]	dict index
 *
 * @retval	storage page type. */
extern page_type_t dict_index_legacy_ptype(
    const dict_index_t *index);

/** Judge legacy fil page type that will be stored.
 *
 * @param[in]	index type flags
 *
 * @retval	storage page type. */
extern page_type_t dict_index_legacy_ptype(ulint index_type);

/** Write the special roll ptr value for a secondary index DDL on a panda
 * index, nothing is done for other index structures.
 *
 * @param[in]		dict index
 * @param[in,out]	field wrapper to fill with the roll ptr column */
extern void dict_index_panda_alloc_roll_ptr_for_ddl(const dict_index_t *index,
                                                    DField_wrapper *df_wrapper);

/** Add stored TID column on secondary index.
 *
 * @param[in/out]	new index.
 * @param[in]		index.
 * @param[in]		dictionary table
 */
extern void dict_index_add_stored_tcol(dict_index_t *index,
                                       const dict_table_t *table);

/**
  Check the dict_table_t object

  @param[in]      table       dict_table_t

  @return         true        Success
*/
bool dict_table_check_valid(const dict_table_t *table);

/**
  Check the dict_incex object

  @param[in]      index       dict_index_t
  @param[in]      check_table false if cannot check table

  @return         true        Success
*/
bool dict_index_check_valid(const dict_index_t *index,
                            bool check_table = true);

/**
  Check the n_s_gfields and gpp_stored of a index are matched.

  @param[in]      index       dict_index_t

  @return         true        Success
*/
inline bool dict_index_check_gstored(const dict_index_t *index) {
  return ((index->n_s_gfields > 0 && index->is_gstored()) ||
          (index->n_s_gfields == 0 && !index->is_gstored()));
}

}  // namespace lizard

#endif  // lizard0dict0dict_h
