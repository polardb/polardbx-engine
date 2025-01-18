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

/** @file dict/lizard0dict0gpp.cc
 GPP dictionary structure

 Created 2024-03-26 by Jianwei.zhao
 *******************************************************/

#include "dict0mem.h"
#include "dict0dict.h"

#include "lizard0dict0gpp.h"

/** Returns position of gpp column in an index.
@return position, ULINT_UNDEFINED if not contained */
ulint dict_index_t::get_gpp_col_pos() const {
  dict_col_t *col = nullptr;
  ulint col_no = ULINT_UNDEFINED;

  col = lizard::dict_table_get_v_gcol(table);
  ut_ad(col != nullptr);

  col_no = col->ind;
  ut_ad(col_no == DATA_GPP_NO);

  return get_col_pos(col_no);
}

namespace lizard {

/**
 * Return prefined dict_table_t GPP_NO column.
 *
 * @return	always valid column.
 * */
dict_col_t *dict_table_get_v_gcol(const dict_table_t *table) {
  ut_ad(table->v_gcol != nullptr);
  return table->v_gcol;
}

/**
 * Return prefined dict_index_t GPP_NO field.
 *
 * @return	always valid column.
 * */
dict_field_t *dict_index_get_v_gfield(const dict_index_t *index) {
  ut_ad(index);
  ut_ad(index->v_gfield);

  return index->v_gfield;
}

/**
 * Copy column definition
 *
 * @param[in/out]	tuple
 * @Param[in]		dict_table_t */
void dict_table_copy_g_types(dtuple_t *tuple, const dict_table_t *table) {
  dict_col_t *col = nullptr;
  dfield_t *dfield = nullptr;
  dtype_t *dtype = nullptr;
  ut_ad(table && tuple);

  col = dict_table_get_v_gcol(table);
  dfield = dtuple_get_v_gfield(tuple);
  dtype = dfield_get_type(dfield);

  dfield_set_null(dfield);
  col->copy_type(dtype);
}



/** Build column definition for GPP_NO.
 *
 * @param[in]	table
 * @param[in]	heap
 *
 * @retval	GPP_NO column
 * */
dict_col_t *dict_mem_table_add_v_gcol(dict_table_t *table, mem_heap_t *heap) {
  dict_col_t *col = nullptr;
  ut_ad(table);
  ut_ad(table->magic_n == DICT_TABLE_MAGIC_N);

  /** Predefined physical position, It's not necessary to reposition it from
   * dd_column since dd didn't save GPP_NO column definition. */
  const uint32_t phy_pos = DATA_GPP_NO;
  const uint8_t v_added = 0;
  const uint8_t v_dropped = 0;

  col = dict_table_get_v_gcol(table);

  dict_mem_fill_column_struct(col, DATA_GPP_NO, DATA_SYS_GPP, DATA_NOT_NULL,
                              DATA_GPP_NO_LEN, false, phy_pos, v_added,
                              v_dropped);

  return col;
}

/** Add virtual GPP_NO column on index as virtual column.
 *
 * @param[in/out]	index
 * @param[in]		table
 * */
void dict_index_add_virtual_gcol(dict_index_t *index,
                                 const dict_table_t *table) {
  dict_col_t *col = nullptr;
  const char *col_name = nullptr;
  dict_field_t *field = nullptr;
  ut_ad(index);

  /** GPP NO column */
  col = dict_table_get_v_gcol(table);
  ut_ad(col);
  col_name = table->get_col_name(dict_col_get_no(col));
  field = index->v_gfield;
  ut_ad(field && index->n_v_gfields == 0);

  index->n_v_gfields = 1;
  field->name = col_name;
  field->prefix_len = 0;
  field->is_ascending = true;

  field->col = col;
  field->fixed_len = col->get_fixed_size(dict_table_is_comp(table));

  ut_ad(field->fixed_len == DATA_GPP_NO_LEN);
}

/** Add stored GPP_NO column on secondary index following PK Columns.
 *
 * @param[in/out]	new index.
 * @param[in]		index.
 * @param[in]		dictionary table
 */
void dict_index_add_stored_gcol(dict_index_t *new_index,
                                const dict_index_t *index,
                                const dict_table_t *table) {
  dict_col_t *col = nullptr;
  const char *col_name = nullptr;
  dict_field_t *field = nullptr;

  ut_ad(new_index && index);
  ut_a(new_index->n_s_gfields == 0);

  if (!index->is_gstored()) return;

  /** Not support stored GPP_NO column on primary key. */
  ut_ad(!index->is_clustered());

  /** Not supoort stored GPP_NO column on compressed table. */
  ut_ad(!table->is_compressed());

  /** GPP NO column */
  col = dict_table_get_v_gcol(table);
  ut_ad(col);
  col_name = table->get_col_name(dict_col_get_no(col));

  new_index->add_field(col_name, 0, true);
  field = new_index->get_field(new_index->n_def - 1);
  field->col = col;
  field->fixed_len = col->get_fixed_size(dict_table_is_comp(table));
  ut_ad(field->fixed_len == DATA_GPP_NO_LEN);

  new_index->n_s_gfields = 1;
  new_index->set_gstored(true);
}

}  // namespace lizard
