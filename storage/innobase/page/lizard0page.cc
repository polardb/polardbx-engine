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

/** @file include/lizard0page.cc
 lizard page operation.

 Created 2020-04-08 by Jianwei.zhao
 *******************************************************/

#include "lizard0page.h"
#include "dict0mem.h"
#include "mem0mem.h"
#include "page0page.h"

#include "lizard0row.h"

namespace lizard {

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
/**
  Check the lizard fields in every user records of the page
  @param[in]      page        cluster index page
  @param[in]      index       cluster index

  @retval         true        Success
*/
bool lizard_page_attributes(page_t *page, const dict_index_t *index) {
  rec_t *rec;
  mem_heap_t *heap;
  ulint *offsets = NULL;

  ut_ad(page);

  if (!index->is_clustered() || index->table->is_intrinsic()) return true;

  /** Didn't check other type except of compact and branch */
  if (!page_is_comp(page) || !page_is_leaf(page)) return true;

  heap = mem_heap_create(UNIV_PAGE_SIZE + 200, UT_LOCATION_HERE);

  rec = page_rec_get_next(page_get_infimum_rec(page));

  while (page_rec_is_user_rec(rec)) {
    offsets = rec_get_offsets(rec, index, offsets, ULINT_UNDEFINED,
                              UT_LOCATION_HERE, &heap);

    /* Assert the validation */
    ut_ad(page_rec_is_comp(rec));
    assert_row_lizard_valid(rec, index, offsets);

    rec = page_rec_get_next(rec);
  }

  mem_heap_free(heap);

  return true;
}
#endif /* UNIV_DEBUG || LIZARD_DEBUG define */

}  // namespace lizard
