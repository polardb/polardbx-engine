/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file include/lizard0page.h
 lizard page operation.

 Created 2020-04-08 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0page_h
#define lizard0page_h

#include "lizard0scn0types.h"
#include "lizard0undo0types.h"
#include "page0types.h"

struct dict_index_t;
struct txn_desc_t;

namespace lizard {

/**
  Write the scn and uba of a record on a B-tree leaf node page.
  @param[in/out]  page_zip    compressed page
  @param[in]      index       dict_index_t
  @param[in/out]  rec         record
  @param[in]      offsets     rec_get_offsets(rec, index)
  @param[in]      scn_col     column number of SCN_ID in rec
  @param[in]      scn         transaction scn
  @param[in]      undo_ptr    undo_ptr
*/
void page_zip_write_scn_and_undo_ptr(page_zip_des_t *page_zip,
                                     const dict_index_t *index, byte *rec,
                                     const ulint *offsets, ulint scn_col,
                                     const scn_t scn, const undo_ptr_t undo_ptr,
                                     const gcn_t gcn);

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
/**
  Check the lizard fields in every user records of the page
  @param[in]      page        cluster index page
  @param[in]      index       cluster index

  @retval         true        Success
*/
bool lizard_page_attributes(page_t *page, const dict_index_t *index);
#endif /* UNIV_DEBUG || LIZARD_DEBUG define */

}  // namespace lizard

#if defined UNIV_DEBUG || defined lizard_DEBUG
#define assert_lizard_page_attributes(page, index)     \
  do {                                                 \
    ut_a(lizard::lizard_page_attributes(page, index)); \
  } while (0)
#else
#define assert_lizard_page_attributes(page, index)
#endif /* UNIV_DEBUG || LIZARD_DEBUG define */

#endif
