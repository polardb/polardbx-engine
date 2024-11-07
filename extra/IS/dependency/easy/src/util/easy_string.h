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


#ifndef EASY_STRING_H_
#define EASY_STRING_H_

/**
 * inet的通用函数
 */
#include <stdarg.h>
#include "easy_define.h"
#include "easy_pool.h"

EASY_CPP_START

extern char *easy_strncpy(char *dst, const char *src, size_t n);
extern char *easy_string_tohex(const char *str, int n, char *result, int size);
extern char *easy_string_toupper(char *str);
extern char *easy_string_tolower(char *str);
extern char *easy_string_format_size(double bytes, char *buffer, int size);
extern char *easy_strcpy(char *dest, const char *src);
extern char *easy_num_to_str(char *dest, int len, uint64_t number);
extern char *easy_string_capitalize(char *str, int len);
extern int easy_vsnprintf(char *buf, size_t size, const char *fmt, va_list args);
extern int lnprintf(char *str, size_t size, const char *fmt, ...) __attribute__ ((__format__ (__printf__, 3, 4)));

EASY_CPP_END

#endif
