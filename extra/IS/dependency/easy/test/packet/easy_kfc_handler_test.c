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


#include "easy_io.h"
#include <easy_test.h>
#include <easy_time.h>
#include <easy_kfc_handler.h>
#include <sys/types.h>
#include <sys/wait.h>

/**
 * 测试 easy_kfc_handler
 */
///////////////////////////////////////////////////////////////////////////////////////////////////
TEST(easy_kfc_handler, easy_kfc_set_iplist)
{
    char                    *config = "10.1[7-8][1-3,985-5].4.1[1-3,5]6 role=server group=group1 port=80";
    easy_kfc_t              *kfc = easy_kfc_create(config, 0);

    if (kfc) {
        easy_kfc_destroy(kfc);
    }
}

