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

#include "lizard0dict.h"
#include "dict0dd.h"
#include "dict0mem.h"
#include "lizard0data0types.h"
#include "lizard0undo.h"
#include "row0mysql.h"

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
void dict_table_add_lizard_columns(dict_table_t *table, mem_heap_t *heap) {
  ut_ad(table && heap);

  if (table->is_intrinsic()) return;

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
bool dd_index_modificatsion_visible(dict_index_t *index, const trx_t *trx,
                                    lizard::Snapshot_vision *snapshot_vision) {
  txn_rec_t rec_txn;
  ut_ad(trx);

  rec_txn = {
      index->trx_id,
      SCN_NULL,
      index->txn.uba.load(),
      GCN_NULL,
  };

  if (undo_ptr_is_active(rec_txn.undo_ptr)) {
    mutex_enter(&dict_sys->mutex);

    if (!undo_ptr_is_active(index->txn.uba.load())) {
      rec_txn.scn = index->txn.scn.load();
      rec_txn.gcn = index->txn.gcn.load();
      rec_txn.undo_ptr = index->txn.uba.load();
      mutex_exit(&dict_sys->mutex);
      goto judge;
    }

    lizard::txn_rec_real_state_by_misc(&rec_txn);
    /** It might be stored many times but they should be the same value */
    index->txn.scn.store(rec_txn.scn);
    index->txn.gcn.store(rec_txn.gcn);

    ut_ad(undo_ptr_get_addr(index->txn.uba.load()) ==
          undo_ptr_get_addr(rec_txn.undo_ptr));

    /** Copy onto index->txn from lookup. */
    index->txn.uba.store(rec_txn.undo_ptr);

    mutex_exit(&dict_sys->mutex);
  } else {
    ut_ad(!undo_ptr_is_active(index->txn.uba.load()));
    ut_ad(index->txn.scn.load() != SCN_NULL &&
          index->txn.gcn.load() != GCN_NULL);
    rec_txn.scn = index->txn.scn.load();
    rec_txn.gcn = index->txn.gcn.load();
  }

judge:

  if (snapshot_vision) {
    return snapshot_vision->modification_visible(&rec_txn);
  } else {
    ut_ad(!trx->vision.is_asof());
    ut_ad(trx->vision.is_active());
    /**
      When is_usable() is executed concurrently, SCN and UBA will be not
      consistent, the vision judgement only depend on real SCN, UBA state
      will be used to code defense, so here omit the check.
    */
    return trx->vision.modifications_visible(&rec_txn, index->table->name,
                                             false);
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
  size_t n_uniq;
  dict_field_t *field;
  dict_col_t *col;
  const char *col_name;

  ut_a(index);
  if (check_table) {
    assert_lizard_dict_table_check(index->table);
  }
  is_clust = index->is_clustered();

  if (is_clust) {
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
      n_uniq = index->n_uniq;
      /* trx_id */
      field = index->get_field(n_uniq);
      col = field->col;
      col_name = index->table->get_col_name(col->ind);
      ut_a(strcmp(col_name, "DB_TRX_ID") == 0);
    }
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

#endif /* UNIV_DEBUG || LIZARD_DEBUG define */
}  // namespace lizard
