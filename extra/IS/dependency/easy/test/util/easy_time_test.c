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


#include "easy_time.h"
#include <easy_test.h>

/**
 * 测试 easy_time
 */
TEST(easy_time, localtime)
{
    time_t                  t;
    struct tm               tm1, tm2;
    int                     cnt, cnt1;

    t = time(NULL);
    localtime_r(&t, &tm1);
    easy_localtime(&t, &tm2);

    if (memcmp(&tm1, &tm2, 32))
        EXPECT_TRUE(0);

    // 也locatime比较
    t -= 86400 * 365 * 15;
    cnt = cnt1 = 0;

    for(; t > 0 && t < 0x7fff0000; t += 40000) {
        localtime_r(&t, &tm1);
        easy_localtime(&t, &tm2);
        cnt ++;

        if (memcmp(&tm1, &tm2, 32) == 0)
            cnt1 ++;
    }

    EXPECT_EQ(cnt1, cnt);

    // < 0
    t = -86400;
    t                       *= (365 * 1000);
    timezone = 3600 * 23;

    for(; t < 0; t += (86400 * 100)) {
        easy_localtime(&t, &tm2);
    }
}

TEST(easy_time, now)
{
    int64_t                 s = easy_time_now();
    usleep(1);
    int64_t                 e = easy_time_now();
    int                     t = time(NULL);
    EXPECT_TRUE(e > s);
    s /= 1000000;
    e /= 1000000;
    EXPECT_TRUE(s == t || e == t);
}
