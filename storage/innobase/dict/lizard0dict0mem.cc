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

#include "lizard0dict.h"
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
    return true;
  }
  return false;
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
  } else {
    return (fil_page_get_type(page) == FIL_PAGE_INDEX);
  }
}

}  // namespace lizard