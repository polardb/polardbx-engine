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


#ifndef EASY_LOG_H_
#define EASY_LOG_H_

/**
 * 简单的log输出
 */
#include <easy_define.h>
#include <easy_string.h>
#include <easy_baseth_pool.h>

EASY_CPP_START

typedef void (*easy_log_print_pt)(const char *message);
typedef void (*easy_log_format_pt)(int level, const char *file, int line, const char *function, const char *fmt, ...);
typedef enum {
    EASY_LOG_OFF = 1,
    EASY_LOG_FATAL,
    EASY_LOG_ERROR,
    EASY_LOG_WARN,
    EASY_LOG_INFO,
    EASY_LOG_DEBUG,
    EASY_LOG_TRACE,
    EASY_LOG_ALL
} easy_log_level_t;

#define easy_system_log(format, args...) if (easy_log_level >= EASY_LOG_ERROR) \
        easy_log_format_default(EASY_LOG_OFF, __FILE__, __LINE__, __FUNCTION__, format, ## args)
#define easy_fatal_log(format, args...) if (easy_log_level >= EASY_LOG_FATAL)      \
        easy_log_format(EASY_LOG_FATAL, __FILE__, __LINE__, __FUNCTION__, format, ## args)
#define easy_error_log(format, args...) if (easy_log_level >= EASY_LOG_ERROR)      \
        easy_log_format(EASY_LOG_ERROR, __FILE__, __LINE__, __FUNCTION__, format, ## args)
#define easy_warn_log(format, args...) if (easy_log_level >= EASY_LOG_WARN)        \
        easy_log_format(EASY_LOG_WARN, __FILE__, __LINE__, __FUNCTION__, format, ## args)
#define easy_info_log(format, args...) if (easy_log_level >= EASY_LOG_INFO)        \
        easy_log_format(EASY_LOG_INFO, __FILE__, __LINE__, __FUNCTION__, format, ## args)
#define easy_debug_log(format, args...) if (easy_log_level >= EASY_LOG_DEBUG)      \
        easy_log_format(EASY_LOG_DEBUG, __FILE__, __LINE__, __FUNCTION__, format, ## args)
#define easy_trace_log(format, args...) if (easy_log_level >= EASY_LOG_TRACE)      \
        easy_log_format(EASY_LOG_TRACE, __FILE__, __LINE__, __FUNCTION__, format, ## args)

// 打印backtrace
#define EASY_PRINT_BT(format, args...)                                                        \
    {char _buffer_stack_[256];{void *array[10];int i, idx=0, n = backtrace(array, 10);        \
            for (i = 0; i < n; i++) idx += lnprintf(idx+_buffer_stack_, 25, "%p ", array[i]);}\
        easy_log_format(EASY_LOG_OFF, __FILE__, __LINE__, __FUNCTION__, "%s" format, _buffer_stack_, ## args);}

extern easy_log_level_t easy_log_level;
extern easy_log_format_pt easy_log_format;
extern void easy_log_set_print(easy_log_print_pt p);
extern void easy_log_set_format(easy_log_format_pt p);
extern void easy_log_format_default(int level, const char *file, int line, const char *function, const char *fmt, ...);
extern void easy_log_print_default(const char *message);

EASY_CPP_END

#endif
