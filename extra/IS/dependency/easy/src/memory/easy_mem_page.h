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


#ifndef EASY_MEM_PAGE_H_
#define EASY_MEM_PAGE_H_

#include <easy_define.h>
#include <easy_list.h>
#include <easy_atomic.h>

/**
 * 简单内存分配器
 */
EASY_CPP_START

#define EASY_MEM_PAGE_SHIFT     16
#define EASY_MEM_PAGE_SIZE      (1<<EASY_MEM_PAGE_SHIFT)        // 64K
#define EASY_MEM_MAX_ORDER      12                              // 最大页大小: 128M

typedef struct easy_mem_page_t easy_mem_page_t;
typedef struct easy_mem_area_t easy_mem_area_t;
typedef struct easy_mem_zone_t easy_mem_zone_t;

struct easy_mem_page_t {
    easy_list_t             lru;
};

struct easy_mem_area_t {
    easy_list_t             free_list;
    int                     nr_free;
};

struct easy_mem_zone_t {
    unsigned char           *mem_start, *mem_last, *mem_end;
    easy_mem_area_t         area[EASY_MEM_MAX_ORDER];
    uint32_t                max_order;
    int                     free_pages;
    unsigned char           *curr, *curr_end;
    unsigned char           page_flags[0];
};

// 内存创建
easy_mem_zone_t *easy_mem_zone_create(int64_t max_size);
// 内存释放
void easy_mem_zone_destroy(easy_mem_zone_t *zone);
// 内存分配
easy_mem_page_t *easy_mem_alloc_pages(easy_mem_zone_t *zone, uint32_t order);
// 内存释放
void easy_mem_free_pages(easy_mem_zone_t *zone, easy_mem_page_t *page);

EASY_CPP_END

#endif
