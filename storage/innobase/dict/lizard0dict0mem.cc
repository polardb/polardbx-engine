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

/** @file dict/lizard0dict0mem.cc
 Special dictionary memory object operation.

 Created 2024-03-26 by Jianwei.zhao
 *******************************************************/
#include "dict0mem.h"

#include "lizard0dict0dict.h"
#include "lizard0dict0mem.h"

/**
 * Whether btree index is panda structure.
 *
 * @param[in]	index
 *
 * @retval	true	Panda structure index
 * @retval	false
 * */
bool dict_index_t::is_panda() const {
  if (root.page_type == FIL_PAGE_INDEX_PANDA) {
    /** Panda index only address secondary index. */
    ut_ad(!is_clustered());
    /** Panda didn't support temporary table. */
    ut_ad(!table || !table->is_temporary());
    /** Panda don't support prefix compression. */
    ut_ad(!is_spc());
    return true;
  }
  return false;
}

bool dict_index_t::is_spc() const {
  if (root.page_type == FIL_PAGE_INDEX_SPC) {
    ut_ad(!is_clustered());
    ut_ad(dict_table_is_comp(table));
    ut_ad(!table->is_compressed());
    ut_ad(!table->is_temporary());
    ut_ad(!table->is_intrinsic());
    return true;
  }
  return false;
}

/**
 * Whether trx id stored on the secondary index

 * @retval	true	secondary index has trx_id column
 * @retval	false otherwise
 * */
bool dict_index_t::is_hippo() const {
  if (root.page_type == FIL_PAGE_INDEX_HIPPO) {
    ut_ad(!is_clustered());
    ut_ad(!table || !table->is_temporary());
    return true;
  }
  return false;
}

/** Looks for column n in an index.
@param[in]      n               column number
@param[in]      inc_prefix      true=consider column prefixes too
@param[in]      is_virtual      true==virtual column
@return position in internal representation of the index;
ULINT_UNDEFINED if not contained */
ulint dict_index_t::get_col_pos(ulint n, bool inc_prefix,
                                bool is_virtual) const {
  const dict_field_t *field;
  const dict_col_t *col;
  ulint pos;
  ulint n_fields;

  ut_ad(magic_n == DICT_INDEX_MAGIC_N);

  if (is_virtual) {
    col = &(dict_table_get_nth_v_col(table, n)->m_col);
  } else {
    col = table->get_col(n);
  }

  if (is_clustered()) {
    return (dict_col_get_clust_pos(col, this));
  }

  n_fields = dict_index_get_n_fields(this);

  for (pos = 0; pos < n_fields; pos++) {
    field = get_field(pos);

    if (col == field->col && (inc_prefix || field->prefix_len == 0)) {
      return (pos);
    }
  }

  return (ULINT_UNDEFINED);
}

/** Returns the position of a functional column in an secondary index.
@param[in] type        transactional + GPP
@return position, ULINT_UNDEFINED if not contained */
ulint dict_index_t::get_sec_functional_col_pos(ulint n) const {
  const dict_field_t *field = nullptr;
  const dict_col_t *col = nullptr;
  ulint n_fields = 0;
  ut_ad(!is_clustered());
  col = table->get_col(n);
  n_fields = dict_index_get_n_fields(this);
  /** All functional columns begin after unique key fields. */
  for (ulint pos = dict_index_get_n_unique_in_tree(this); pos < n_fields;
       pos++) {
    field = get_field(pos);
    if (col == field->col) {
      /** Assert result compared with general func */
      ut_ad(pos == get_col_pos(n));
      return (pos);
    }
  }
  return (ULINT_UNDEFINED);
}

/** Returns the position of a system column in an index.
@param[in] type         DATA_ROW_ID, ...
@return position, ULINT_UNDEFINED if not contained */
ulint dict_index_t::get_sys_col_pos(ulint type) const {
  ut_ad(magic_n == DICT_INDEX_MAGIC_N);
  ut_ad(!dict_index_is_ibuf(this));

  if (is_clustered()) {
    return (dict_col_get_clust_pos(table->get_sys_col(type), this));
  }

  if (type == DATA_ROW_ID) {
    return (get_col_pos(dict_table_get_sys_col_no(table, type)));
  }
  return (get_sec_functional_col_pos(dict_table_get_sys_col_no(table, type)));
}

namespace lizard {
bool inject_stress_test_for_panda = false;

bool dict_index_fil_page_check(const dict_index_t *index, const byte *page) {
  ut_a(fil_page_index_page_check(page));

  if (dict_index_is_spatial(index)) {
    return (fil_page_get_type(page) == FIL_PAGE_RTREE);
  } else if (dict_index_is_sdi(index)) {
    return (fil_page_get_type(page) == FIL_PAGE_SDI);
  } else if (index->is_panda()) {
    return (fil_page_get_type(page) == FIL_PAGE_INDEX_PANDA);
  } else if (index->is_hippo()) {
    return (fil_page_get_type(page) == FIL_PAGE_INDEX_HIPPO);
  } else if (index->is_spc()) {
    return (fil_page_get_type(page) == FIL_PAGE_INDEX_SPC);
  } else {
    return (fil_page_get_type(page) == FIL_PAGE_INDEX);
  }
}

}  // namespace lizard
