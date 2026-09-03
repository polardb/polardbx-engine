/*****************************************************************************

Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file sql/komodo0assert.h
  Komodo assertion like ut0dbg

 Created 2025-12-01 by Jiyang.zhang
 *******************************************************/

#ifndef komodo0assert_h
#define komodo0assert_h

#include "my_compiler.h"

#include <cstdio>

namespace komodo {
/** Report a failed assertion.
@param[in] expr The failed assertion
@param[in] file Source file containing the assertion
@param[in] line Line number of the assertion */
[[noreturn]] void ut_dbg_assertion_failed(const char *expr, const char *file,
                                          uint64_t line);
}  // namespace komodo

/** Abort execution if EXPR does not evaluate to nonzero.
@param EXPR assertion expression that should hold */
#define kut_a(EXPR)                                               \
  do {                                                            \
    if (unlikely(false == (bool)(EXPR))) {                        \
      komodo::ut_dbg_assertion_failed(#EXPR, __FILE__, __LINE__); \
    }                                                             \
  } while (0)

#ifdef UNIV_DEBUG
/** Debug assertion. Does nothing unless UNIV_DEBUG is defined. */
#define kut_ad(EXPR) kut_a(EXPR)
/** Debug statement. Does nothing unless UNIV_DEBUG is defined. */
#define kut_d(EXPR) EXPR
/** Opposite of ut_d().  Does nothing if UNIV_DEBUG is defined. */
#define kut_o(EXPR)
#else
/** Debug assertion. Does nothing unless UNIV_DEBUG is defined. */
#define kut_ad(EXPR)
/** Debug statement. Does nothing unless UNIV_DEBUG is defined. */
#define kut_d(EXPR)
/** Opposite of ut_d().  Does nothing if UNIV_DEBUG is defined. */
#define kut_o(EXPR) EXPR
#endif

#endif
