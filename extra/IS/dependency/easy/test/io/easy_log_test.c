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


#include "easy_log.h"
#include "easy_io.h"
#include <string.h>
#include <easy_test.h>

/**
 * 测试 easy_log.c
 */
static int              test_easy_log_cnt = 0;
static void test_easy_log_print(const char *message)
{
    test_easy_log_cnt ++;
}
TEST(easy_log, print)
{
    int                     i;
    easy_log_level = (easy_log_level_t)10;
    easy_fatal_log("log\n\n\n");

    easy_log_set_print(test_easy_log_print);

    for(i = EASY_LOG_OFF; i <= EASY_LOG_ALL; i++) {
        test_easy_log_cnt = 0;
        easy_log_level = (easy_log_level_t)i;
        easy_fatal_log("a:%d\n", i);
        easy_error_log("a:%d\n", i);
        easy_warn_log("a:%d\n", i);
        easy_info_log("a:%d\n", i);
        easy_debug_log("a:%d\n", i);
        easy_trace_log("a:%d\n", i);
        EXPECT_EQ(test_easy_log_cnt, (i == EASY_LOG_ALL ? EASY_LOG_ALL - 2 : i - 1));
    }

    // other branch
    easy_fatal_log("");
    easy_io_t               *eio = (easy_io_t *)easy_malloc(sizeof(easy_io_t));
    memset(eio, 0, sizeof(easy_io_t));
    easy_baseth_self = ((easy_baseth_t *)eio);
    easy_fatal_log("");
    easy_free(eio);
}
void easy_log_format_test(int level, const char *file, int line, const char *function, const char *fmt, ...)
{
    va_list                 args;
    va_start(args, fmt);
    fprintf(stderr, fmt, args);
    va_end(args);
}
TEST(easy_log, format)
{
    easy_log_set_format(easy_log_format_test);
    test_easy_log_cnt = 0;
    easy_error_log("test_easy_log_cnt: %d\n", test_easy_log_cnt);
    easy_log_set_format(easy_log_format_default);
}

void easy_log_format_vsnprintf(char *buf, int size, const char *fmt, ...)
{
    va_list                 args;
    va_start(args, fmt);
    easy_vsnprintf(buf, size, fmt, args);
    va_end(args);
}

// test "%.*s", 0, NULL
TEST(easy_log, vsnprintf)
{
    char                    buf[1024];
    char                    *dst;

    dst = "easy_vsnprintf:";
    easy_log_format_vsnprintf(buf, 1024, "easy_vsnprintf:%.*s", 0, NULL);
    EXPECT_TRUE(strlen(dst) == strlen(buf) && memcmp(buf, dst, strlen(buf)) == 0);

    dst = "easy_vsnprintf: taob";
    easy_log_format_vsnprintf(buf, 1024, "easy_vsnprintf: %.*s", 4, "taobao");
    EXPECT_TRUE(strlen(dst) == strlen(buf) && memcmp(buf, dst, strlen(buf)) == 0);
    dst = "easy_vsnprintf: tao";
    EXPECT_TRUE(strlen(dst) != strlen(buf));
}
