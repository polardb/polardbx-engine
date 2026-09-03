/*****************************************************************************

Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file include/lizard0mem0mem.h
 Memory Alloc Wrapper.

 Created 2025-12-12 by Yichang Song
 *******************************************************/

#ifndef lizard0mem0mem_h
#define lizard0mem0mem_h

#include "mem0mem.h"
#include "ut0new.h"

#include "lizard0rem0types.h"

/** Wrapper of mem_heap_alloc() */
static inline boc_t mem_heap_alloc_boc(mem_heap_t *heap, ulint n) {
  boc_t boc;

  void *ptr = mem_heap_alloc(heap, n);

  if (ptr) {
    boc.buf = static_cast<byte *>(ptr);
    boc.loc = MLOC_IN_HEAP;
  }

  return boc;
}

namespace ut {

/** Wrapper of malloc_withkey() */
static inline boc_t malloc_boc_withkey(PSI_memory_key_t key,
                                       std::size_t size) noexcept {
  boc_t boc;

  void *ptr = malloc_withkey(key, size);

  if (ptr) {
    boc.buf = static_cast<byte *>(ptr);
    boc.loc = MLOC_IN_HEAP;
  }

  return boc;
}

/** Wrapper of new_arr_withkey */
static inline boc_t new_arr_boc_withkey(PSI_memory_key_t key, Count count) {
  boc_t boc;

  byte *ptr = new_arr_withkey<byte>(key, count);

  if (ptr) {
    boc.buf = ptr;
    boc.loc = MLOC_IN_HEAP;
  }

  return boc;
}

}  // namespace ut

#endif
