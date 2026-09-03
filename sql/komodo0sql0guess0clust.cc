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

/** @file sql/komodo0sql0guess0clust.cc
  Classes used for guess clust index when index merge or multi range read.

 Created 2025-11-11 by yuchen
 *******************************************************/
#include "komodo0sql0guess0clust.h"
#include "handler.h"
#include "sql/komodo0handler0types.h"

namespace komodo {

/** Compare two records with GPP info. */
/**
 * @param arg Handler pointer used for comparison
 * @param a Pointer to the first record
 * @param b Pointer to the second record
 * @return A comparison function that returns [-1, 0, 1]
 */
extern "C" int refpos_order_cmp_with_gpp(const void *arg, const void *a,
                                         const void *b) {
  const handler *file = static_cast<const handler *>(arg);

  const uchar *a_ptr = static_cast<const uchar *>(a);
  const uchar *b_ptr = static_cast<const uchar *>(b);

  int cmp = file->cmp_ref(a_ptr, b_ptr);

  if (cmp == 0) {
    const size_t gpp_offset = file->ref_length;

    gpp_no_t a_gpp_no = gpp_read_from_ref(a_ptr + gpp_offset);
    gpp_no_t b_gpp_no = gpp_read_from_ref(b_ptr + gpp_offset);

    if (a_gpp_no == INNODB_FIL_NULL) {
      gpp_write_to_ref(const_cast<uchar *>(a_ptr) + gpp_offset, b_gpp_no);
    } else if (b_gpp_no == INNODB_FIL_NULL) {
      gpp_write_to_ref(const_cast<uchar *>(b_ptr) + gpp_offset, a_gpp_no);
    }
  }
  return cmp;
}

/** Helper function to get the cmp function with or without GPP info.
  @param h Handler pointer used for comparison
  @return A comparison function that returns true if a < b, false otherwise
*/
std::function<int(const uchar *a, const uchar *b)> cmp_func(handler *h) {
  /* Comparison with GPP info: bind handler as first argument and convert
   return value from int(-1,0,1) to bool(cmp < 0) */
  auto bound_cmp =
      std::bind(refpos_order_cmp_with_gpp, static_cast<const void *>(h),
                std::placeholders::_1, std::placeholders::_2);
  return [bound_cmp](const uchar *a, const uchar *b) {
    return bound_cmp(a, b) < 0;
  };
}

}  // namespace komodo