/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

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


#ifndef EASY_MEMPOOL_H_
#define EASY_MEMPOOL_H_

/**
 * ring buffer
 */
#include "easy_define.h"

EASY_CPP_START

#define EASY_MEMPOOL_PAGE_SIZE              512 * 1024
#define EASY_MEMPOOL_PAGE_MAX_NUM           16384   // max page num per-pool
#define EASY_MEMPOOL_PAGE_FREE_NUM          16      // max free page hold per-pool
#define EASY_MEMPOOL_ALIGNMENT              sizeof(unsigned long)

#define easy_mempool_align(d, a)        (((d) + (a - 1)) & ~(a - 1))
#define easy_mempool_align_ptr(p, a)    (uint8_t*)(((uintptr_t)(p) + ((uintptr_t) a - 1)) & ~((uintptr_t) a - 1))

typedef void *(*easy_mempool_memalign_pt)(size_t alignment, size_t size);
typedef void (*easy_mempool_free_pt)(void *ptr);

typedef struct easy_mempool_allocator_t {
    easy_mempool_memalign_pt memalign;
    easy_mempool_free_pt    free;
} easy_mempool_allocator_t;

struct                  easy_mempool_t;
typedef struct easy_mempool_t easy_mempool_t;

////////////////////////////////////////////////////////////////////////////////////////////////////

extern easy_mempool_t *easy_mempool_create(uint32_t size);
extern void easy_mempool_destroy(easy_mempool_t *pool);
extern void easy_mempool_clear(easy_mempool_t *pool);

// alloc align buffer
extern void *easy_mempool_alloc(easy_mempool_t *pool, uint32_t size);
// free buffer
extern void easy_mempool_free(easy_mempool_t *pool, void *ptr);

extern void easy_mempool_set_memlimit(easy_mempool_t *pool, int64_t limit);
extern void easy_mempool_set_allocator(easy_mempool_t *pool, easy_mempool_allocator_t *allocator);
extern int64_t easy_mempool_get_memtotal(easy_mempool_t *pool);

////////////////////////////////////////////////////////////////////////////////////////////////////

// 使用全局分配器分配内存
extern void easy_mempool_set_global_memlimit(int64_t limit);
extern int64_t easy_mempool_get_global_memtotal();
extern void *easy_mempool_global_realloc(void *ptr, size_t size);

// 使用线程缓存分配内存
extern void easy_mempool_set_thread_memlimit(int64_t limit);
extern int64_t easy_mempool_get_thread_memtotal();
extern void *easy_mempool_thread_realloc(void *ptr, size_t size);

EASY_CPP_END
#endif

