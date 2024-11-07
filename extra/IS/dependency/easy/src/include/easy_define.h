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


#ifndef EASY_DEFINE_H_
#define EASY_DEFINE_H_

/**
 * 定义一些编译参数
 */

#ifdef __cplusplus
#define EASY_CPP_START extern "C" {
#define EASY_CPP_END }
#else
# define EASY_CPP_START
# define EASY_CPP_END
#endif

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <stdio.h>
#include <stdarg.h>
#include <time.h>
#include <stdlib.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include <assert.h>
#include <stddef.h>
#include <inttypes.h>
#include <unistd.h>
#include <execinfo.h>

///////////////////////////////////////////////////////////////////////////////////////////////////
// define
#define easy_free(ptr)              if(ptr) free(ptr)
#define easy_malloc(size)           malloc(size)
#define easy_realloc(ptr, size)     realloc(ptr, size)
#ifndef likely
#define likely(x)                   __builtin_expect(!!(x), 1)
#endif
#ifndef unlikely
#define unlikely(x)                 __builtin_expect(!!(x), 0)
#endif
#define easy_align_ptr(p, a)        (uint8_t*)(((uintptr_t)(p) + ((uintptr_t) a - 1)) & ~((uintptr_t) a - 1))
#define easy_align(d, a)            (((d) + (a - 1)) & ~(a - 1))
#define easy_max(a,b)               (a > b ? a : b)
#define easy_min(a,b)               (a < b ? a : b)
#define easy_div(a,b)               ((b) ? ((a)/(b)) : 0)
#define easy_memcpy(dst, src, n)    (((char *) memcpy(dst, src, (n))) + (n))
#define easy_const_strcpy(b, s)     easy_memcpy(b, s, sizeof(s)-1)
#define easy_safe_close(fd)         {if((fd)>=0){close((fd));(fd)=-1;}}
#define easy_ignore(exp)            {int ignore __attribute__ ((unused)) = (exp);}

#define EASY_OK                     0
#define EASY_ERROR                  (-1)
#define EASY_ABORT                  (-2)
#define EASY_ASYNC                  (-3)
#define EASY_BREAK                  (-4)
#define EASY_AGAIN                  (-EAGAIN)

// DEBUG
//#define EASY_DEBUG_DOING            1
//#define EASY_DEBUG_MAGIC            1
///////////////////////////////////////////////////////////////////////////////////////////////////
// typedef
typedef struct easy_addr_t {
    uint16_t                  family;
    uint16_t                  port;
    union {
        uint32_t                addr;
        uint8_t                 addr6[16];
    } u;
    uint32_t                cidx;
} easy_addr_t;

#endif
