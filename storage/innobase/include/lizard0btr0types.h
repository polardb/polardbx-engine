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

/** @file btr/lizard0btr0types.h
 *
 * Lizard btree upgrade version

 Created 2024-08-30 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0btr0types_h
#define lizard0btr0types_h

#include "lizard0fil0types.h"

/** When btree allocate a page from root(leaf seg or not-leaf seg)
 * We need new page and root page type. */
struct btr_alloc_t {
 public:
  btr_alloc_t(buf_block_t *block, page_type_t type)
      : new_block(block), root_page_type(type) {}

  btr_alloc_t() : new_block(nullptr), root_page_type(FIL_PAGE_TYPE_UNUSED) {}

  btr_alloc_t(const btr_alloc_t &other)
      : new_block(other.new_block), root_page_type(other.root_page_type) {}

  btr_alloc_t &operator=(const btr_alloc_t &other) {
    if (this != &other) {
      new_block = other.new_block;
      root_page_type = other.root_page_type;
    }
    return *this;
  }

 public:
  /** New block was allocated from btree */
  buf_block_t *new_block;
  /** Btree root page type. */
  page_type_t root_page_type;
};

#endif
