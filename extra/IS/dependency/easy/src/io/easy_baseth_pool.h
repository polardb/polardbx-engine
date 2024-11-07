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


#ifndef EASY_BASETH_POOL_H
#define EASY_BASETH_POOL_H

#include <easy_define.h>

/**
 * base pthread线程池
 */

EASY_CPP_START

#include "easy_io_struct.h"

#define easy_thread_pool_for_each(th, tp, offset)                   \
    for((th) = (typeof(*(th))*)&(tp)->data[offset];                 \
            (char*)(th) < (tp)->last;                               \
            th = (typeof(*th)*)(((char*)th) + (tp)->member_size))

// 第n个
static inline void *easy_thread_pool_index(easy_thread_pool_t *tp, int n)
{
    if (n < 0 || n >= tp->thread_count)
        return NULL;

    return &tp->data[n * tp->member_size];
}

static inline void *easy_thread_pool_hash(easy_thread_pool_t *tp, uint64_t hv)
{
    hv %= tp->thread_count;
    return &tp->data[hv * tp->member_size];
}

static inline void *easy_thread_pool_rr(easy_thread_pool_t *tp, int start)
{
    int                     n, t;

    if ((t = tp->thread_count - start) > 0) {
        n = easy_atomic32_add_return(&tp->last_number, 1);
        n %= t;
        n += start;
    } else {
        n = 0;
    }

    return &tp->data[n * tp->member_size];
}

// baseth
void *easy_baseth_on_start(void *args);
void easy_baseth_on_wakeup(void *args);
void easy_baseth_init(void *args, easy_thread_pool_t *tp,
                      easy_baseth_on_start_pt *start, easy_baseth_on_wakeup_pt *wakeup);
void easy_baseth_pool_on_wakeup(easy_thread_pool_t *tp);
easy_thread_pool_t *easy_baseth_pool_create(easy_io_t *eio, int thread_count, int member_size);
void easy_baseth_pool_destroy(easy_thread_pool_t *tp);

EASY_CPP_END

#endif
