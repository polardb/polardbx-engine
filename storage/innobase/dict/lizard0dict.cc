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

#include "dict0dd.h"
#include "dict0mem.h"
#include "data0data.h"
#include "row0mysql.h"

#include "lizard0dict.h"
#include "lizard0btr0btr.h"
#include "lizard0data0types.h"
#include "lizard0undo.h"
#include "lizard0dict0mem.h"
#include "lizard0dd0policy.h"
#include "lizard0txn0rec.h"
#include "lizard0dict0gpp.h"

#include "sql/sql_class.h"

namespace lizard {

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

/**
  Init txn_desc with the creator trx when created.

  @param[in]      index       the index being created
  @param[in]      trx         creator transaction
  @return         DB_SUCCESS  Success
*/
dberr_t dd_index_init_txn_desc(dict_index_t *index, trx_t *trx) {
  dberr_t err = DB_SUCCESS;
  ut_ad(index->table);
  ut_ad(!mutex_own(&trx->undo_mutex));

  /** If a table is temporary, or even intrinsic, its dict info will not
  be written to dict tables. */
  if (!index->table->is_temporary()) {
    mutex_enter(&trx->undo_mutex);
    err = trx_always_assign_txn_undo(trx);
    mutex_exit(&trx->undo_mutex);

    if (err == DB_SUCCESS) {
      assert_trx_commit_mark_initial(trx);
      assert_undo_ptr_allocated(trx->txn_desc.undo_ptr);
      ut_ad(undo_ptr_is_active(trx->txn_desc.undo_ptr));

      index->txn.uba = trx->txn_desc.undo_ptr;
      index->txn.scn = trx->txn_desc.cmmt.scn;
      index->txn.gcn = trx->txn_desc.cmmt.gcn;
    }
  }

  return err;
}

/**
  Check if a index should be seen by a transaction.

  @param[in]      index       the index being opened.
  @param[in]      trx         transaction.
  @return         true if visible
*/
bool dd_index_modification_visible(
    dict_index_t *index, const trx_t *trx,
    const lizard::Snapshot_vision *snapshot_vision) {
  txn_rec_t rec_txn;
  ut_ad(trx);

  rec_txn = {
      index->trx_id,
      SCN_NULL,
      index->txn.uba.load(),
      GCN_NULL
  };

  if (undo_ptr_is_active(rec_txn.undo_ptr)) {
    mutex_enter(&dict_sys->mutex);

    if (!undo_ptr_is_active(index->txn.uba.load())) {
      rec_txn.scn = index->txn.scn.load();
      rec_txn.gcn = index->txn.gcn.load();
      rec_txn.undo_ptr = index->txn.uba.load();
      mutex_exit(&dict_sys->mutex);
      ut_ad(rec_txn.is_whole_committed());
      goto judge;
    }

    lizard::txn_rec_real_state(&rec_txn, Cache_hint::KEEP_OLD, ccr_t::CCR_ALL);
    /** It might be stored many times but they should be the same value */
    index->txn.scn.store(rec_txn.scn);
    index->txn.gcn.store(rec_txn.gcn);

    ut_ad(undo_ptr_get_slot(index->txn.uba.load()) ==
          undo_ptr_get_slot(rec_txn.undo_ptr));

    /** Copy onto index->txn from lookup. */
    index->txn.uba.store(rec_txn.undo_ptr);

    mutex_exit(&dict_sys->mutex);
  } else {
    ut_ad(index->txn.is_whole_committed());
    rec_txn.scn = index->txn.scn.load();
    rec_txn.gcn = index->txn.gcn.load();
    ut_ad(rec_txn.is_whole_committed());
  }

judge:

  if (snapshot_vision && likely(srv_flashback_query_enable)) {
    return snapshot_vision->modification_visible(&rec_txn);
  } else {
    ut_ad(!trx->vision.is_active() || !trx->vision.is_asof());
    /**
      When is_usable() is executed concurrently, SCN and UBA will be not
      consistent, the vision judgement only depend on real SCN, UBA state
      will be used to code defense, so here omit the check.
    */
    return (
        !trx->vision.is_active() ||
        trx->vision.modifications_visible(&rec_txn, index->table->name, false));
  }
}

/**
  Fill index txn information from se_private_data.

  @param[in,out]  index       the index being opened.
  @param[in]      p           se_private_data from the mysql.indexes record.
  @return         true if failed
*/
bool dd_index_fill_txn_desc(dict_index_t *index, const dd::Properties &p) {
  undo_ptr_t uba = 0;
  scn_t scn = SCN_NULL;
  gcn_t gcn = GCN_NULL;

  if (p.get(dd_index_key_strings[DD_INDEX_UBA], &uba) ||
      p.get(dd_index_key_strings[DD_INDEX_SCN], &scn))
    return true;

  /** GCN didn't add into properties first time, so judge it firstly. */
  if (p.exists(dd_index_key_strings[DD_INDEX_GCN])) {
    p.get(dd_index_key_strings[DD_INDEX_GCN], &gcn);
  }

  index->txn.uba = uba;
  index->txn.scn = scn;
  index->txn.gcn = gcn;
  return false;
}

/**
  Add the lizard columns into data dictionary in server

  @param[in,out]	dd_table	data dictionary cache object
  @param[in,out]  primary   data dictionary primary key
*/
void dd_add_lizard_columns(dd::Table *dd_table, dd::Index *primary) {
  ut_ad(dd_table && primary);

  dd::Column *db_scn_id = dd_add_hidden_column(
      dd_table, "DB_SCN_ID", DATA_SCN_ID_LEN, dd::enum_column_types::LONGLONG);
  dd_add_hidden_element(primary, db_scn_id);

  dd::Column *db_undo_ptr =
      dd_add_hidden_column(dd_table, "DB_UNDO_PTR", DATA_UNDO_PTR_LEN,
                           dd::enum_column_types::LONGLONG);
  dd_add_hidden_element(primary, db_undo_ptr);

  dd::Column *db_gcn_id = dd_add_hidden_column(
      dd_table, "DB_GCN_ID", DATA_GCN_ID_LEN, dd::enum_column_types::LONGLONG);

  dd_add_hidden_element(primary, db_gcn_id);
}

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

static void dict_index_add_sec_transactional_cols(
    dict_index_t *new_index, const dict_index_t *index,
    const dict_table_t *table, page_type_t expected_page_type) {
  switch (expected_page_type) {
    case FIL_PAGE_INDEX_PANDA:
      return dict_index_add_panda_cols(new_index, table);
    case FIL_PAGE_TYPE_UNUSED:
      break;
    default:
      ut_a(0);
  }
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
  /* Add transactional fields for sec index */
  dict_index_add_sec_transactional_cols(new_index, index, table,
                                        expected_page_type);
}

void dd_fill_dict_index_format(const Index_policy &index_policy,
                               const dict_table_t *table, dict_index_t *index,
                               page_type_t *expected_or_real_page_type) {
  /** Promise only fill once. */
  ut_a(index->is_gstored() == false);
  ut_a(index->n_s_gfields == 0);
  ut_a(index_policy.inited());

  if (index_policy.has_gpp()) {
    ut_ad(!table->is_compressed());
    ut_ad(!table->is_temporary());
    ut_ad((DBUG_EVALUATE_IF("allow_dd_tables_have_gpp", true,
                           !table->is_system_table)));
    ut_ad(!table->is_intrinsic());

    ut_ad(!(index->type & DICT_IBUF));
    ut_ad(!(index->type & DICT_SDI));
    ut_ad(!dict_index_is_spatial(index));
    ut_ad(!index->is_clustered());
  }

  index->set_gstored(index_policy.has_gpp());

  if (index_policy.page_type() == FIL_PAGE_INDEX_PANDA) {
    ut_ad(!table->is_system_table);
  }

  *expected_or_real_page_type = index_policy.page_type();

#ifdef UNIV_DEBUG
  /* Validate root page type. */
  switch (*expected_or_real_page_type) {
    case FIL_PAGE_INDEX_PANDA:
      ut_ad(!index->is_clustered());
      ut_ad(dict_index_is_unique(index));
      [[fallthrough]];
    case FIL_PAGE_TYPE_UNUSED:
      break;
    default:
      ut_ad(0);
  }
#endif
}

/**
  Get ordered field number from sec index for row log.
  @param[in]      index     dict_index_t
  @retval ordered field number
*/
ulint row_log_dict_index_get_ordered_n_fields(const dict_index_t *index) {
  ut_ad(index && !index->is_clustered());
  assert_lizard_dict_index_gstored_check(index);

  return dict_index_get_n_fields(index) -
         dict_index_n_sec_functional_fields(index);
}

void dd_write_index_format(dd::Properties *options, const dict_index_t *index) {
  ulonglong format = 0;

  /* ut_a(!dd_index_options_has_ift(options)); */

  if (index->is_gstored()) {
    format |= IFT_GPP;
  }

  options->set(OPTION_IFT, format);
}

void dd_copy_index_format(dd::Properties &new_dd_options,
                          const dd::Properties &old_dd_options) {
  ulonglong format = 0;

  ut_a(!new_dd_options.exists(OPTION_IFT));

  if (!old_dd_options.exists(OPTION_IFT)) {
    return;
  }

  old_dd_options.get(OPTION_IFT, &format);
  new_dd_options.set(OPTION_IFT, format);
}

/**
  Exchange IFT option between Partition Index and Swap Index.
  @param[in,out]   part_dd_options  dd options from dd::Partition_index
  @param[in,out]   swap_dd_options  dd options from dd::Index from Swap Table
*/
void dd_exchange_index_format(dd::Properties &part_dd_options,
                              dd::Properties &swap_dd_options) {
  ulonglong part_format = 0;
  ulonglong swap_format = 0;
  if (part_dd_options.exists(OPTION_IFT)) {
    part_dd_options.get(OPTION_IFT, &part_format);
  }
  if (swap_dd_options.exists(OPTION_IFT)) {
    swap_dd_options.get(OPTION_IFT, &swap_format);
  }
  part_dd_options.set(OPTION_IFT, swap_format);
  swap_dd_options.set(OPTION_IFT, part_format);
}

bool dd_table_options_has_fba(const dd::Properties *options) {
  bool fba = false;

  if (options->exists(TABLE_OPTION_FBA)) {
    options->get(TABLE_OPTION_FBA, &fba);
    return fba;
  } else {
    return false;
  }
}

void dd_fill_dict_table_fba(const Table_policy &table_policy,
                            dict_table_t *table) {
  /** Promise only fill once. */
  ut_a(table->is_2pp == false);
  ut_a(table_policy.inited());

  if (table_policy.has_fba()) {
    ut_ad(!table->is_temporary());
    ut_ad(!table->is_system_table);
    ut_ad(!table->is_intrinsic());
  }

  table->is_2pp = table_policy.has_fba();
}

template <typename Table>
void dd_write_table_fba(const dict_table_t *table, Table *dd_table) {
  if (!dd_table_is_partitioned(dd_table->table()) ||
      dd_part_is_first(reinterpret_cast<dd::Partition *>(dd_table))) {
    dd_table->table().options().set(TABLE_OPTION_FBA, table->is_2pp);
  }
}

template void dd_write_table_fba<dd::Table>(const dict_table_t *table,
                                            dd::Table *dd_table);

template void dd_write_table_fba<dd::Partition>(const dict_table_t *table,
                                                dd::Partition *dd_table);

template <typename Table>
void dd_copy_table_fba(const Table &old_dd_tab, Table &new_dd_tab) {
  if (!dd_table_is_partitioned(new_dd_tab.table()) ||
      dd_part_is_first(reinterpret_cast<dd::Partition *>(&new_dd_tab))) {
    new_dd_tab.table().options().set(
        TABLE_OPTION_FBA,
        dd_table_options_has_fba(&old_dd_tab.table().options()));
  }
}

template void dd_copy_table_fba<dd::Table>(const dd::Table &old_dd_tab,
                                           dd::Table &new_dd_tab);

template void dd_copy_table_fba<dd::Partition>(const dd::Partition &old_dd_tab,
                                               dd::Partition &new_dd_tab);

bool dd_check_table_fba(const dict_table_t *table, const dd::Table &dd_table) {
  return (table->is_2pp == dd_table_options_has_fba(&dd_table.options()));
}

void dd_exchange_table_fba(dd::Properties &part_dd_options,
                           dd::Properties &swap_dd_options) {
  bool part_fba = false;
  bool swap_fba = false;
  if (part_dd_options.exists(TABLE_OPTION_FBA)) {
    part_dd_options.get(TABLE_OPTION_FBA, &part_fba);
  }

  if (swap_dd_options.exists(TABLE_OPTION_FBA)) {
    swap_dd_options.get(TABLE_OPTION_FBA, &swap_fba);
  }

  part_dd_options.set(TABLE_OPTION_FBA, swap_fba);
  swap_dd_options.set(TABLE_OPTION_FBA, part_fba);
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

page_type_t dict_index_legacy_ptype(ulint index_type) {
  if (index_type & DICT_SPATIAL) {
    return FIL_PAGE_RTREE;
  } else if (index_type & DICT_SDI) {
    return FIL_PAGE_SDI;
  } else {
    return FIL_PAGE_INDEX;
  }
}

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

/** Retrieve root page type from dd index se private data.
 *
 * @param[in]	dict index
 * @param[in]	se private data
 *
 * @retval	root page type. */
page_type_t dd_index_get_page_type(const dict_index_t *index,
                                   const dd::Properties &se_private_data) {
  page_type_t page_type;
  const char *key;

  page_type = dict_index_legacy_ptype(index);
  key = dd_index_key_strings[DD_INDEX_PAGE_TYPE];

  if (se_private_data.exists(key)) {
    se_private_data.get(key, &page_type);
  }

  return page_type;
}

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
/**
  Check the dict_table_t object

  @param[in]      table       dict_table_t

  @return         true        Success
*/
bool lizard_dict_table_check(const dict_table_t *table) {
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

/**
  Check the dict_incex object

  @param[in]      index       dict_index_t
  @param[in]      check_table false if cannot check table

  @return         true        Success
*/
bool lizard_dict_index_check(const dict_index_t *index, bool check_table) {
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
    assert_lizard_dict_table_check(index->table);
  }

  is_clust = index->is_clustered();
  is_panda = dict_index_is_panda(index);

  if (is_clust) {
    return dict_index_check_clust(index);
  } else if (is_panda) {
    return dict_index_check_panda(index);
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

/**
  Validate IFT option from dd::Index or dd::Partition_index is equal to infos in
  dict_index_t.

  @param[in]      dd_options  dd_options from dd::Index or dd::Partition_index
  @param[in]      index       InnoDB index object
  @return false if totally equal, true if not equal.
*/
bool validate_dd_index_format_match(const dd::Properties &options,
                                    const dict_index_t *index) {
  ulonglong format = 0;
  if (options.exists(OPTION_IFT)) {
    options.get(OPTION_IFT, &format);
  }
  /* 1. IFT_GPP */
  if (((format & IFT_GPP) && !index->is_gstored()) ||
      (!(format & IFT_GPP) && index->is_gstored())) {
    return true;
  }
  /* 2. TODO */
  return false;
}

#endif /* UNIV_DEBUG || LIZARD_DEBUG define */
}  // namespace lizard
