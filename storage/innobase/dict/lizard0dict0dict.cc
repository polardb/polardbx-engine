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

/** @file dict/lizard0dict0dict.cc
 Special Lizard tablespace definition and in-memory dictionary operation.

 Created 2020-03-19 by Jianwei.zhao
 *******************************************************/

#include "data0data.h"
#include "dict0mem.h"
#include "my_dbug.h"
#include "row0mysql.h"

#include "lizard0data0types.h"
#include "lizard0dict0dict.h"
#include "lizard0dict0gpp.h"
#include "lizard0dict0mem.h"
#include "lizard0undo.h"

namespace lizard {

#ifdef UNIV_DEBUG
/** Debug pd_capacity value. */
ulint dbug_pd_capacity = ULINT_MAX;
#endif /* UNIV_DEBUG */

/** Minimum valid capacity of an SPC page dictionary. */
constexpr uint16_t DICT_INDEX_PD_CAPACITY_MIN = 2001;

/** The space name of lizard */
const char *dict_lizard::s_lizard_space_name = "innodb_lizard";

/** The file name of lizard space */
const char *dict_lizard::s_lizard_space_file_name = "lizard.ibd";

/** Lizard: First several undo tablespaces will be treated as txn tablespace */
const char *dict_lizard::s_default_txn_space_names[] = {
    "innodb_undo_001", "innodb_undo_002", "innodb_undo_003", "innodb_undo_004"};

/** Judge the undo tablespace is txn tablespace by name */
bool is_txn_space_by_name(const char *name) {
  if (name && (strcmp(name, dict_lizard::s_default_txn_space_names[0]) == 0 ||
               strcmp(name, dict_lizard::s_default_txn_space_names[1]) == 0 ||
               strcmp(name, dict_lizard::s_default_txn_space_names[2]) == 0 ||
               strcmp(name, dict_lizard::s_default_txn_space_names[3]) == 0))
    return true;

  return false;
}

static_assert(DATA_SCN_ID == 3, "DATA_SCN_ID != 3");
static_assert(DATA_SCN_ID_LEN == 8, "DATA_SCN_ID_LEN != 8");
static_assert(DATA_UNDO_PTR == 4, "DATA_UNDO_PTR != 4");
static_assert(DATA_UNDO_PTR_LEN == 8, "DATA_UNDO_PTR_LEN != 8");

static_assert(DATA_GCN_ID == 5, "DATA_GCN_ID != 5");
static_assert(DATA_GCN_ID_LEN == 8, "DATA_GCN_ID_LEN != 8");

/** Space ID to mark SPC index as corrupt. SPACE_UNKNOWN means disabled. */
ulong srv_mark_corrupt_spc_by_space_id = SPACE_UNKNOWN;

/**
  Add the SCN and UBA column into dict_table_t, for example:
  dict_table_t::col_names "...DB_SCN_ID\0DATA_UNDO_PTR\0..."

  @param[in]      table       dict_table_t
  @param[in]      heap        memory slice
*/
void dict_table_add_functional_columns(dict_table_t *table, mem_heap_t *heap) {
  ut_ad(table && heap);

  if (!table->is_intrinsic()) {
    const uint32_t phy_pos = UINT32_UNDEFINED;
    const uint8_t v_added = 0;
    const uint8_t v_dropped = 0;

    dict_mem_table_add_col(table, heap, "DB_SCN_ID", DATA_SYS,
                           DATA_SCN_ID | DATA_NOT_NULL, DATA_SCN_ID_LEN, false,
                           phy_pos, v_added, v_dropped);

    dict_mem_table_add_col(table, heap, "DB_UNDO_PTR", DATA_SYS,
                           DATA_UNDO_PTR | DATA_NOT_NULL, DATA_UNDO_PTR_LEN,
                           false, phy_pos, v_added, v_dropped);

    dict_mem_table_add_col(table, heap, "DB_GCN_ID", DATA_SYS,
                           DATA_GCN_ID | DATA_NOT_NULL, DATA_GCN_ID_LEN, false,
                           phy_pos, v_added, v_dropped);
  }

  /** Always add GPP_NO column on table at fixed position. */
  dict_mem_table_add_v_gcol(table, heap);
}

/** Add transactional columns [trx_id, roll_ptr, scn, uba] on a panda
 * secondary index.
 *
 * @param[in,out]	new index.
 * @param[in]		dictionary table. */
static void dict_index_add_panda_cols(dict_index_t *index,
                                      const dict_table_t *table) {
  ut_ad(!index->is_clustered() && dict_index_is_unique(index));
  ut_ad(!(index->type & DICT_SDI) && !dict_index_is_spatial(index));
  ut_ad(!table->is_intrinsic());

  /* Add TRX_ID field. */
  dict_index_add_col(index, table, table->get_sys_col(DATA_TRX_ID), 0, true);
  /* Add ROLL_PTR field. */
  dict_index_add_col(index, table, table->get_sys_col(DATA_ROLL_PTR), 0, true);
  /* Add SCN field. */
  dict_index_add_col(index, table, table->get_sys_col(DATA_SCN_ID), 0, true);
  /* Add UBA field. */
  dict_index_add_col(index, table, table->get_sys_col(DATA_UNDO_PTR), 0, true);
}

/** Add stored TID column on secondary index.
 *
 * @param[in/out]	new index.
 * @param[in]		dictionary table
 */
void dict_index_add_stored_tcol(dict_index_t *index,
                                const dict_table_t *table) {
  ut_ad(!index->is_clustered());
  ut_ad(index->n_s_tfields == 0);
  ut_ad(!dict_index_is_unique(index));
  ut_ad(!(index->type & DICT_SDI) && !dict_index_is_spatial(index));
  ut_ad(!table->is_intrinsic());

  /* Add TRX_ID field. */
  dict_index_add_col(index, table, table->get_sys_col(DATA_TRX_ID), 0, true);
  index->n_s_tfields = 1;
}

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
                                        page_type_t expected_page_type) {
  /* Add gpp fields for sec index. */
  dict_index_add_stored_gcol(new_index, index, table);

  switch (expected_page_type) {
    case FIL_PAGE_INDEX_PANDA:
      /* Add transactional fields for panda index */
      dict_index_add_panda_cols(new_index, table);
      break;
    case FIL_PAGE_INDEX_HIPPO:
      /* Add tid for hippo index. */
      dict_index_add_stored_tcol(new_index, table);
      break;
    case FIL_PAGE_TYPE_UNUSED:
    case FIL_PAGE_INDEX_SPC:
      break;
    default:
      ut_a(0);
  }
}

/** Determine the page-dictionary capacity of a prefix compressed (SPC) index.
 *
 * @param[in]	dict index
 *
 * @retval	page-dictionary capacity in bytes, 0 if the index is not
 * prefix compressed. */
uint16_t dict_index_determine_pd_capacity(const dict_index_t *index) {
  return dict_index_determine_pd_capacity_low(index, true);
}

/** Check whether the page-dictionary capacity stored for an index remains
compatible with the current size estimator. A smaller legacy value is allowed
only when a prefix field participates in the index. */
bool dict_index_pd_capacity_is_compatible(const dict_index_t *index) {
  if (!index->is_spc()) {
    return index->pd_capacity == 0;
  }

  const uint16_t capacity = dict_index_determine_pd_capacity(index);

  if (index->pd_capacity < DICT_INDEX_PD_CAPACITY_MIN ||
      index->pd_capacity > capacity) {
    return false;
  }

  if (index->pd_capacity == capacity) {
    return true;
  }

  /* pd_capacity is persisted in DD and is an upper bound on page-dictionary
  bytes, not a required size. The encoder respects a smaller bound by forcing
  dictionary merges and, if encoding cannot help, falling back to page
  reorganization or a split. It is therefore conservative: it can forgo an
  encoding opportunity, but cannot make a page exceed its dictionary budget.

  The fixed-prefix estimator correction can only increase this optional
  budget. A smaller legacy value is therefore valid only when a prefix field,
  including an appended clustered-key field, participates in the capacity
  estimate. */
  for (ulint i = 0; i < index->n_fields; ++i) {
    if (index->get_field(i)->prefix_len != 0) {
      return true;
    }
  }

  return false;
}

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
                                              bool strict) {
  if (strict && !index->is_spc()) return 0;
  dict_table_t *table = index->table;
  /* maximum allowed size of a record on a leaf page */
  size_t page_rec_max;
  /* maximum allowed size of a node pointer record */
  size_t page_ptr_max;
  get_permissible_max_size(table, index, page_rec_max, page_ptr_max);
  size_t rec_max_size;
  bool too_big = dict_index_validate_max_rec_size(
      table, index, false, page_rec_max, page_ptr_max, rec_max_size);
  ut_a(!too_big);

  int32_t capacity = page_get_free_space_of_empty(true) - rec_max_size * 2 -
                     500 /* hardcode a safety margin */;
  ut_a(capacity >= DICT_INDEX_PD_CAPACITY_MIN);

#ifdef UNIV_DEBUG
  if (DBUG_EVALUATE_IF("set_pd_capacity_specified", true, false)) {
    capacity = dbug_pd_capacity;
  }

  DBUG_EXECUTE_IF("set_pd_capacity_random", {
    std::srand(time(0));
    capacity = (std::rand() % capacity);
  });
#endif

  return capacity;
}

/**
  Get ordered field number from sec index for row log.
  @param[in]      index     dict_index_t
  @retval ordered field number
*/
ulint row_log_dict_index_get_ordered_n_fields(const dict_index_t *index) {
  ut_ad(index && !index->is_clustered());
  ut_ad(lizard::dict_index_check_gstored(index));

  return dict_index_get_n_fields(index) -
         dict_index_n_sec_functional_fields(index);
}

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
page_type_t dict_index_legacy_ptype(const dict_index_t *index) {
  return dict_index_legacy_ptype(index->type);
}

/** Judge legacy fil page type that will be stored.
 *
 * @param[in]	index type flags
 *
 * @retval	storage page type. */
page_type_t dict_index_legacy_ptype(ulint index_type) {
  if (index_type & DICT_SPATIAL) {
    return FIL_PAGE_RTREE;
  } else if (index_type & DICT_SDI) {
    return FIL_PAGE_SDI;
  } else {
    return FIL_PAGE_INDEX;
  }
}

/** Write the special roll ptr value for a secondary index DDL on a panda
 * index, nothing is done for other index structures.
 *
 * @param[in]		dict index
 * @param[in,out]	field wrapper to fill with the roll ptr column */
void dict_index_panda_alloc_roll_ptr_for_ddl(const dict_index_t *index,
                                             DField_wrapper *df_wrapper) {
  byte *field;
  ulint pos;

  if (!dict_index_is_panda(index)) {
    return;
  }

  /** Get positioin of roll ptr on the index */
  pos = index->get_sys_col_pos(DATA_ROLL_PTR);
  dict_field_t *dd_field = index->get_field(pos);
  ut_a(dd_field->col == index->table->get_sys_col(DATA_ROLL_PTR));
  ut_a(dd_field->col->len == DATA_ROLL_PTR_LEN);

  /** Copy column types and length to the field */
  df_wrapper->copy_types_and_len(dd_field->col);

  /** Write special valu for roll ptr. */
  field = static_cast<byte *>(dfield_get_data(df_wrapper->dfield()));
  trx_write_roll_ptr(field, ROLL_PTR_SEC_DDL);

  return;
}

/**
  Check the dict_table_t object

  @param[in]      table       dict_table_t

  @return         true        Success
*/
bool dict_table_check_valid(const dict_table_t *table) {
  size_t n_cols;
  size_t n_sys_cols;
  dict_col_t *col;
  const char *s;
  bool is_intrinsic;
  ulint i;
  ut_a(table);

  is_intrinsic = table->is_intrinsic();
  s = table->col_names;

  /** Check columns */
  n_cols = table->n_cols;
  if (is_intrinsic) {
    n_sys_cols = DATA_ITT_N_SYS_COLS + DATA_ITT_N_LIZARD_COLS;
    ut_a(n_cols > n_sys_cols);
    for (i = 0; i < n_cols - n_sys_cols; i++) {
      s += strlen(s) + 1;
    }
    /* Row id */
    col = table->get_col(n_cols - n_sys_cols + DATA_ROW_ID);
    ut_a(col->mtype == DATA_SYS);
    ut_a(col->prtype == (DATA_ROW_ID | DATA_NOT_NULL));
    ut_a(col->len == DATA_ROW_ID_LEN);
    ut_a(strcmp(s, "DB_ROW_ID") == 0);
    s += strlen(s) + 1;

    /* trx id */
    col = table->get_col(n_cols - n_sys_cols + DATA_TRX_ID);
    ut_a(col->mtype == DATA_SYS);
    ut_a(col->prtype == (DATA_TRX_ID | DATA_NOT_NULL));
    ut_a(col->len == DATA_TRX_ID_LEN);
    ut_a(strcmp(s, "DB_TRX_ID") == 0);
  } else {
    n_sys_cols = DATA_N_SYS_COLS + DATA_N_LIZARD_COLS;
    /** If there are only virtual columns in the table, the following
    equality is reached */
    ut_a(n_cols >= n_sys_cols);

    for (i = 0; i < n_cols - n_sys_cols; i++) {
      s += strlen(s) + 1;
    }
    /* Row id */
    col = table->get_col(n_cols - n_sys_cols + DATA_ROW_ID);
    ut_a(col->mtype == DATA_SYS);
    ut_a(col->prtype == (DATA_ROW_ID | DATA_NOT_NULL));
    ut_a(col->len == DATA_ROW_ID_LEN);
    ut_a(strcmp(s, "DB_ROW_ID") == 0);
    s += strlen(s) + 1;

    /* trx id */
    col = table->get_col(n_cols - n_sys_cols + DATA_TRX_ID);
    ut_a(col->mtype == DATA_SYS);
    ut_a(col->prtype == (DATA_TRX_ID | DATA_NOT_NULL));
    ut_a(col->len == DATA_TRX_ID_LEN);
    ut_a(strcmp(s, "DB_TRX_ID") == 0);
    s += strlen(s) + 1;

    /* roll ptr */
    col = table->get_col(n_cols - n_sys_cols + DATA_ROLL_PTR);
    ut_a(col->mtype == DATA_SYS);
    ut_a(col->prtype == (DATA_ROLL_PTR | DATA_NOT_NULL));
    ut_a(col->len == DATA_ROLL_PTR_LEN);
    ut_a(strcmp(s, "DB_ROLL_PTR") == 0);
    s += strlen(s) + 1;

    /* scn id */
    col = table->get_col(n_cols - n_sys_cols + DATA_SCN_ID);
    ut_a(col->mtype == DATA_SYS);
    ut_a(col->prtype == (DATA_SCN_ID | DATA_NOT_NULL));
    ut_a(col->len == DATA_SCN_ID_LEN);
    ut_a(strcmp(s, "DB_SCN_ID") == 0);
    s += strlen(s) + 1;

    /* undo ptr */
    col = table->get_col(n_cols - n_sys_cols + DATA_UNDO_PTR);
    ut_a(col->mtype == DATA_SYS);
    ut_a(col->prtype == (DATA_UNDO_PTR | DATA_NOT_NULL));
    ut_a(col->len == DATA_UNDO_PTR_LEN);
    ut_a(strcmp(s, "DB_UNDO_PTR") == 0);
    s += strlen(s) + 1;

    /* gcn id */
    col = table->get_col(n_cols - n_sys_cols + DATA_GCN_ID);
    ut_a(col->mtype == DATA_SYS);
    ut_a(col->prtype == (DATA_GCN_ID | DATA_NOT_NULL));
    ut_a(col->len == DATA_GCN_ID_LEN);
    ut_a(strcmp(s, "DB_GCN_ID") == 0);

    /** gpp no */
    col = table->get_col(DATA_GPP_NO);
    ut_a(col->mtype == DATA_SYS_GPP);
    ut_a(col->prtype == DATA_NOT_NULL);
    ut_a(col->len == DATA_GPP_NO_LEN);
  }
  return true;
}

/** Check cluster index structure
 *
 * @param[in]	index
 *
 * @retval	true	valid
 * */
static bool dict_index_check_clust(const dict_index_t *index) {
  size_t n_uniq;
  dict_field_t *field;
  dict_col_t *col;
  const char *col_name;

  ut_a(index->is_clustered());
  ut_a(!index->is_panda());

  /** [trxid, rollptr, scn, uba, gcn] for non-intrinsic table. */
  if (!index->table->is_intrinsic()) {
    n_uniq = index->n_uniq;
    /* trx_id */
    field = index->get_field(n_uniq);
    col = field->col;
    col_name = index->table->get_col_name(col->ind);
    ut_a(strcmp(col_name, "DB_TRX_ID") == 0);

    /* roll ptr */
    field = index->get_field(n_uniq + 1);
    col = field->col;
    col_name = index->table->get_col_name(col->ind);
    ut_a(strcmp(col_name, "DB_ROLL_PTR") == 0);

    /* scn id */
    field = index->get_field(n_uniq + 2);
    col = field->col;
    col_name = index->table->get_col_name(col->ind);
    ut_a(strcmp(col_name, "DB_SCN_ID") == 0);

    /* undo ptr */
    field = index->get_field(n_uniq + 3);
    col = field->col;
    col_name = index->table->get_col_name(col->ind);
    ut_a(strcmp(col_name, "DB_UNDO_PTR") == 0);

    /* gcn id */
    field = index->get_field(n_uniq + 4);
    col = field->col;
    col_name = index->table->get_col_name(col->ind);
    ut_a(strcmp(col_name, "DB_GCN_ID") == 0);
  } else {
    /** [trxid] for intrinsic table. */
    n_uniq = index->n_uniq;
    /* trx_id */
    field = index->get_field(n_uniq);
    col = field->col;
    col_name = index->table->get_col_name(col->ind);
    ut_a(strcmp(col_name, "DB_TRX_ID") == 0);
  }
  return true;
}

/** Check panda index structure
 *
 * @param[in]	index
 *
 * @retval	true	valid
 * */
static bool dict_index_check_panda(const dict_index_t *index) {
  dict_field_t *field;
  dict_col_t *col;
  const char *col_name;
  size_t trx_id_pos;

  ut_a(index->is_panda());
  ut_a(!index->is_clustered());

  /** [trx_id, rollptr, scn, uba] for non-temporary table. */
  if (!index->table->is_temporary()) {
    trx_id_pos =
        dict_index_get_n_unique_in_tree(index) + (index->is_gstored() ? 1 : 0);

    /* trx_id */
    field = index->get_field(trx_id_pos);
    col = field->col;
    col_name = index->table->get_col_name(col->ind);
    ut_a(col == index->table->get_sys_col(DATA_TRX_ID));
    ut_a(strcmp(col_name, "DB_TRX_ID") == 0);

    /* roll ptr */
    field = index->get_field(trx_id_pos + 1);
    col = field->col;
    col_name = index->table->get_col_name(col->ind);
    ut_a(col == index->table->get_sys_col(DATA_ROLL_PTR));
    ut_a(strcmp(col_name, "DB_ROLL_PTR") == 0);

    /* scn id */
    field = index->get_field(trx_id_pos + 2);
    col = field->col;
    col_name = index->table->get_col_name(col->ind);
    ut_a(col == index->table->get_sys_col(DATA_SCN_ID));
    ut_a(strcmp(col_name, "DB_SCN_ID") == 0);

    /* undo ptr */
    field = index->get_field(trx_id_pos + 3);
    col = field->col;
    col_name = index->table->get_col_name(col->ind);
    ut_a(col == index->table->get_sys_col(DATA_UNDO_PTR));
    ut_a(strcmp(col_name, "DB_UNDO_PTR") == 0);

    ut_a((trx_id_pos + 4) == dict_index_get_n_fields(index));
  } else {
    /** We didn't allow panda index on temporary table. */
    ut_a(0);
    return false;
  }
  return true;
}

/** Check hippo index structure
 *
 * @param[in]	index
 *
 * @retval	true	valid
 * */
static bool dict_index_check_hippo(const dict_index_t *index) {
  dict_field_t *field;
  dict_col_t *col;
  const char *col_name;
  size_t trx_id_pos;

  ut_a(index->is_hippo());
  ut_a(!index->is_clustered());

  /** [trx_id] for non-temporary table. */
  if (!index->table->is_temporary()) {
    trx_id_pos =
        dict_index_get_n_unique_in_tree(index) + (index->is_gstored() ? 1 : 0);

    /* trx_id */
    field = index->get_field(trx_id_pos);
    col = field->col;
    col_name = index->table->get_col_name(col->ind);
    ut_a(col == index->table->get_sys_col(DATA_TRX_ID));
    ut_a(strcmp(col_name, "DB_TRX_ID") == 0);

    ut_a(trx_id_pos + 1 == dict_index_get_n_fields(index));
  } else {
    /** We didn't allow hippo index on temporary table. */
    ut_a(0);
    return false;
  }
  return true;
}

/**
  Check the dict_incex object

  @param[in]      index       dict_index_t
  @param[in]      check_table false if cannot check table

  @return         true        Success
*/
bool dict_index_check_valid(const dict_index_t *index, bool check_table) {
  bool is_clust;
  dict_field_t *field;
  dict_col_t *col;
  const char *col_name;
  bool is_panda;
  ut_a(index);

  if (dict_index_is_ibuf(index)) {
    return true;
  }

  if (check_table) {
    ut_ad(lizard::dict_table_check_valid(index->table));
  }

  is_clust = index->is_clustered();
  is_panda = dict_index_is_panda(index);

  if (is_clust) {
    return dict_index_check_clust(index);
  } else if (is_panda) {
    return dict_index_check_panda(index);
  } else if (index->is_hippo()) {
    return dict_index_check_hippo(index);
  } else {
    for (size_t i = 0; i < index->n_def; i++) {
      field = index->get_field(i);
      col = field->col;
      if (col->is_virtual()) {
        col_name = dict_table_get_v_col_name(index->table,
                                             ((dict_v_col_t *)(col))->v_pos);
      } else {
        col_name = index->table->get_col_name(col->ind);
      }

      ut_a(strcmp(col_name, "DB_TRX_ID") != 0 &&
           strcmp(col_name, "DB_ROLL_PTR") != 0 &&
           strcmp(col_name, "DB_SCN_ID") != 0 &&
           strcmp(col_name, "DB_UNDO_PTR") != 0 &&
           strcmp(col_name, "DB_GCN_ID") != 0);
    }
  }
  return true;
}

}  // namespace lizard
