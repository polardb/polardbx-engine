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

/** @file include/lizard0dict0mem.h
 Special dictionary memory object operation.

 Created 2024-03-26 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0dict0mem_h
#define lizard0dict0mem_h

#include "dict0mem.h"

#include "lizard0data0types.h"
#include "lizard0txn0rec0types.h"

/** Whether Btree Index Record has transactional capability or not, its totally
  depended on transactional columns;

  1) Cluster index

  It included [trx_id, rollptr, scn, uba, gcn] to address transactional
  capability, But intrinsic table is a special case, we must deal with it
  individually.

  2) Secondary index
        2.1) Normal Btree structure
            It didn't have any transactional columns.

        2.2) Panda index
           It included [trx_id, rollptr, scn, uba] columns. But temporary table
           is also a special case. we can assert it since panda not support
           temporary table.
 */
namespace lizard {
extern bool inject_stress_test_for_panda;

/**
 * Whether btree index is panda structure.
 *
 * @param[in]	index
 *
 * @retval	true	Panda structure index
 * @retval	false
 * */
inline bool dict_index_is_panda(const dict_index_t *index) {
  return index->is_panda();
}
inline bool dict_index_is_clust(const dict_index_t *index) {
  return index->is_clustered();
}

inline bool dict_index_inject_stress_test_for_panda(
    const dict_index_t *index) {
  return dict_index_is_panda(index) && inject_stress_test_for_panda;
}

/** Attention: pls use it carefully. make sure you judge table type before. */
inline txn_layout_t dict_index_txn_layout(const dict_index_t *index) {
  if (index->is_clustered())
    return txn_layout_t::TL_CLOVER;
  else if (index->is_panda())
    return txn_layout_t::TL_BAMBOO;
  else
    return txn_layout_t::TL_NONE;
}

/** Judge count of secondary functional fields according to index/table
 * structure. */
inline ulint dict_index_n_sec_functional_fields(const dict_index_t *index) {
  ut_ad(!index->is_clustered());
  if (index->table && index->table->is_intrinsic()) {
    return 0;
  }
  return txn_layout_get_n_transactional_fields(dict_index_txn_layout(index)) +
         index->n_s_gfields;
}

/**
 * Check the type of a given index page against the expected type for the
 * specified index.
 * The function handles different types of indexes that require specific
 * page types:
 * - For spatial indexes, it expects pages of type FIL_PAGE_RTREE.
 * - For SDI, it expects pages of type FIL_PAGE_SDI.
 * - For Panda indexes, it expects pages of type FIL_PAGE_INDEX_PANDA.
 * - For normal indexes, it expects the default page type FIL_PAGE_INDEX.
 *
 * @param index Pointer to the index structure.
 * @param page  Pointer to the index page.
 * @return true if the page type matches the expected type for the given index;
 *         false otherwise.
 */
bool dict_index_fil_page_check(const dict_index_t *index, const byte *page);

}  // namespace lizard
#endif
