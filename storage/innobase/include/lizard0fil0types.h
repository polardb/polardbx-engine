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

/** @file include/lizard0fil0types.h
 Special new version of btree page.

 Created 2024-08-29 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0fil0types_h
#define lizard0fil0types_h

#include "fil0types.h"
#include "fil0fil.h"

/** Fil Page Mark in space */
struct page_mark_t {
 public:
  page_mark_t() : page_no(FIL_NULL), page_type(FIL_PAGE_TYPE_UNUSED) {}

  page_mark_t(page_no_t page_no_arg, page_type_t page_type_arg)
      : page_no(page_no_arg), page_type(page_type_arg) {}

  page_mark_t(const page_mark_t &other)
      : page_no(other.page_no), page_type(other.page_type) {}

  page_mark_t &operator=(const page_mark_t &other) {
    if (this != &other) {
      page_no = other.page_no;
      page_type = other.page_type;
    }
    return *this;
  }

 public:
  page_no_t page_no;
  page_type_t page_type;
};

const page_mark_t PAGE_MARK_NULL(FIL_NULL, FIL_PAGE_TYPE_UNUSED);

/** Btree structure can be changed according to different version, include
 * page format or record format. since rec status is not enough to deal with
 * different logic, and dd meta is not safe, so we define new FIL_PAGE_TYPE to
 * address it.
 *
 */
/** Different index page version. */
enum ipage_ver_t : uint16_t {
  /** Normal btree page */
  IPV0 = FIL_PAGE_INDEX,
  /** Lizard UK page */
  IPV1 = FIL_PAGE_INDEX_PANDA
};

inline bool fil_page_type_is_btree_index(page_type_t page_type) {
  return page_type == ipage_ver_t::IPV0 || page_type == ipage_ver_t::IPV1;
}

/** Check whether the page type is index (Btree or Rtree or SDI) type */
inline bool fil_page_type_is_index(page_type_t page_type) {
  return fil_page_type_is_btree_index(page_type) || page_type == FIL_PAGE_SDI ||
         page_type == FIL_PAGE_RTREE;
}
/** Check whether the page is index page (either regular Btree index or Rtree
index */
inline bool fil_page_index_page_check(const byte *page) {
  return fil_page_type_is_index(fil_page_get_type(page));
}

static_assert(ipage_ver_t::IPV0 == 17855, "btree v0 type is FIL_PAGE_INDEX");

static_assert(ipage_ver_t::IPV1 == 40, "btree v1 type is FIL_PAGE_INDEX_PANDA");

#if defined UNIV_DEBUG
/*
#define assert_lizard_page_mark_consistent(page_mark)      \
  do {                                                     \
    ut_ad((page_mark.page_no == FIL_NULL &&                \
           page_mark.page_type == FIL_PAGE_TYPE_UNUSED) || \
          (page_mark.page_no != FIL_NULL &&                \
           fil_page_type_is_index(page_mark.page_type)));  \
  } while (0)
*/
#define assert_lizard_page_mark_consistent(page_mark)     \
  do {                                                    \
    ut_ad((page_mark.page_type != FIL_PAGE_TYPE_UNUSED || \
           page_mark.page_no == FIL_NULL));               \
  } while (0)
#else
#define assert_lizard_page_mark_consistent(page_mark)
#endif /* UNIV_DEBUG */

#endif
