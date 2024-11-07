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


#ifndef EASY_POOL_H_
#define EASY_POOL_H_

/**
 * 简单的内存池
 */
#include <easy_define.h>
#include <easy_list.h>
#include <easy_atomic.h>

EASY_CPP_START

#ifdef EASY_DEBUG_MAGIC
#define EASY_DEBUG_MAGIC_POOL     0x4c4f4f5059534145
#define EASY_DEBUG_MAGIC_MESSAGE  0x4753454d59534145
#define EASY_DEBUG_MAGIC_SESSION  0x5353455359534145
#define EASY_DEBUG_MAGIC_CONNECT  0x4e4e4f4359534145
#define EASY_DEBUG_MAGIC_REQUEST  0x5551455259534145
#endif

#define EASY_POOL_ALIGNMENT         512
#define EASY_POOL_PAGE_SIZE         4096
#define easy_pool_alloc(pool, size)  easy_pool_alloc_ex(pool, size, sizeof(long))
#define easy_pool_nalloc(pool, size) easy_pool_alloc_ex(pool, size, 1)

typedef void *(*easy_pool_realloc_pt)(void *ptr, size_t size);
typedef struct easy_pool_large_t easy_pool_large_t;
typedef struct easy_pool_t easy_pool_t;
typedef void (easy_pool_cleanup_pt)(const void *data);
typedef struct easy_pool_cleanup_t easy_pool_cleanup_t;

struct easy_pool_large_t {
    easy_pool_large_t       *next;
    uint8_t                 *data;
};

struct easy_pool_cleanup_t {
    easy_pool_cleanup_pt    *handler;
    easy_pool_cleanup_t     *next;
    const void              *data;
};

struct easy_pool_t {
    uint8_t                 *last;
    uint8_t                 *end;
    easy_pool_t             *next;
    uint16_t                failed;
    uint16_t                flags;
    uint32_t                max;

    // pool header
    easy_pool_t             *current;
    easy_pool_large_t       *large;
    easy_atomic_t           ref;
    easy_spin_t             tlock;
    easy_pool_cleanup_t     *cleanup;
#ifdef EASY_DEBUG_MAGIC
    uint64_t                magic;
#endif
};

extern easy_pool_realloc_pt easy_pool_realloc;
extern void *easy_pool_default_realloc (void *ptr, size_t size);

extern easy_pool_t *easy_pool_create(uint32_t size);
extern void easy_pool_clear(easy_pool_t *pool);
extern void easy_pool_destroy(easy_pool_t *pool);
extern void *easy_pool_alloc_ex(easy_pool_t *pool, uint32_t size, int align);
extern void *easy_pool_calloc(easy_pool_t *pool, uint32_t size);
extern void easy_pool_set_allocator(easy_pool_realloc_pt alloc);
extern void easy_pool_set_lock(easy_pool_t *pool);
extern easy_pool_cleanup_t *easy_pool_cleanup_new(easy_pool_t *pool, const void *data, easy_pool_cleanup_pt *handler);
extern void easy_pool_cleanup_reg(easy_pool_t *pool, easy_pool_cleanup_t *cl);

extern char *easy_pool_strdup(easy_pool_t *pool, const char *str);

EASY_CPP_END
#endif
