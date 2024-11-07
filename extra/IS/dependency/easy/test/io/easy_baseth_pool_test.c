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


#include "easy_baseth_pool.h"
#include "easy_io.h"
#include "easy_simple_handler.h"
#include <easy_test.h>

TEST(easy_thread_pool, index)
{
    easy_io_t               *eio;
    void                    *ptr, *ptr1, *ptr2;

    // 1.
    eio = easy_io_create(2);
    ptr = easy_thread_pool_index(eio->io_thread_pool, -1);
    EXPECT_TRUE(ptr == NULL);

    ptr = easy_thread_pool_index(eio->io_thread_pool, 2);
    EXPECT_TRUE(ptr == NULL);

    ptr = easy_thread_pool_index(eio->io_thread_pool, 0);
    EXPECT_TRUE(ptr != NULL);

    // 2.
    ptr = easy_thread_pool_hash(eio->io_thread_pool, 1024);
    EXPECT_TRUE(ptr != NULL);
    ptr1 = easy_thread_pool_hash(eio->io_thread_pool, 1025);
    EXPECT_TRUE(ptr1 != NULL);
    EXPECT_TRUE(ptr1 != ptr);
    ptr2 = easy_thread_pool_hash(eio->io_thread_pool, 1026);
    EXPECT_TRUE(ptr2 != NULL);
    EXPECT_TRUE(ptr == ptr2);

    // 3.
    ptr = easy_thread_pool_rr(eio->io_thread_pool, 0);
    ptr1 = easy_thread_pool_rr(eio->io_thread_pool, 0);
    ptr2 = easy_thread_pool_rr(eio->io_thread_pool, 0);
    EXPECT_TRUE(ptr == ptr2);
    EXPECT_TRUE(ptr1 != ptr2);
    ptr2 = easy_thread_pool_rr(eio->io_thread_pool, 200);
    EXPECT_TRUE(ptr2 != NULL);
    EXPECT_TRUE(ptr2 == ptr1);

    easy_io_destroy();
}
