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

/** @file include/lizard0dict.h
 Special Lizard tablespace definition.

 Created 2020-03-19 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0dict_h
#define lizard0dict_h
#include "univ.i"

#include "api0api.h"
#include "dict0dict.h"
#include "mem0mem.h"
#include "sql/dd/object_id.h"

#include "lizard0txn0types.h"
#include "lizard0undo0types.h"

struct dict_table_t;
struct dict_index_t;

namespace dd {
class Table;
class Index;
}  // namespace dd

namespace lizard {

class Snapshot_vision;

/** Here define the lizard related dictionary content */

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
void dict_table_add_lizard_columns(dict_table_t *table, mem_heap_t *heap);

/**
  Add the lizard columns into data dictionary in server

  @param[in,out]	dd_table	data dictionary cache object
  @param[in,out]  primary   data dictionary primary key
*/
void dd_add_lizard_columns(dd::Table *dd_table, dd::Index *primary);

/**
  Init txn_desc with the creator trx when created.

  @param[in]      index       the index being created
  @param[in]      trx         creator transaction
  @return         DB_SUCCESS  Success
*/
dberr_t dd_index_init_txn_desc(dict_index_t *index, trx_t *trx);

/**
  Fill index txn information by from se_private_data.

  @param[in,out]  index       the index being opened.
  @param[in]      p           se_private_data from the mysql.indexes record.
  @return         true if failed
*/
bool dd_index_fill_txn_desc(dict_index_t *index, const dd::Properties &p);

/**
  Check if a index should be seen by a transaction.

  @param[in]      index       the index being opened.
  @param[in]      trx         transaction.
  @return         true if visible
*/
bool dd_index_modificatsion_visible(dict_index_t *index, const trx_t *trx,
                                    lizard::Snapshot_vision *snapshot_vision);

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
/**
  Check the dict_table_t object

  @param[in]      table       dict_table_t

  @return         true        Success
*/
bool lizard_dict_table_check(const dict_table_t *table);

/**
  Check the dict_incex object

  @param[in]      index       dict_index_t
  @param[in]      check_table false if cannot check table

  @return         true        Success
*/
bool lizard_dict_index_check(const dict_index_t *index,
                             bool check_table = true);

#endif /* UNIV_DEBUG || LIZARD_DEBUG define */

}  // namespace lizard

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
#define assert_lizard_dict_table_check(table)     \
  do {                                            \
    ut_a(lizard::lizard_dict_table_check(table)); \
  } while (0)

#define assert_lizard_dict_index_check(index)     \
  do {                                            \
    ut_a(lizard::lizard_dict_index_check(index)); \
  } while (0)

#define assert_lizard_dict_index_check_no_check_table(index) \
  do {                                                       \
    ut_a(lizard::lizard_dict_index_check(index, false));     \
  } while (0)

#else

#define assert_lizard_dict_table_check(table)
#define assert_lizard_dict_index_check(index)
#define assert_lizard_dict_index_check_no_check_table(index)

#endif /* UNIV_DEBUG || lizard_DEBUG define */

#endif  // lizard0dict_h
