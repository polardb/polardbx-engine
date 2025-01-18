/*****************************************************************************

Copyright (c) 2013, 2024, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file include/lizard0btr0cur0gpp.h
   Lizard index tree gpp operation.


 Created 2024-04-12 by Yichang.Song
 *******************************************************/

#ifndef lizard0btr0cur0gpp_h
#define lizard0btr0cur0gpp_h

#include "btr0cur.h"

namespace lizard {

/** Updates the redo log record for a gpp_no of a secondary index record.
  @param[in]      rec             secondary record
  @param[in]      index           secondary index
  @param[in]      gpp_no_offset   the offsets of gpp_no field
  @param[in]      gpp_no          page no of the cluster index
  @param[in]      mtr             mtr
*/
extern void btr_cur_upd_gpp_no_sec_rec_log(const rec_t *rec,
                                           const dict_index_t *index,
                                           ulint gpp_no_offset,
                                           page_no_t gpp_no, mtr_t *mtr);

/**
  Parses the redo log record for a gpp_no of a secondary index record.
  @param[in]      ptr        buffer
  @param[in]      end        buffer end
  @param[in]      page       page (NULL if it's just get the length)
  @param[in]      page_zip   compressed page, or NULL
  @param[in]      index      index corresponding to page

  @return         return the end of log record or NULL
*/
extern byte *btr_cur_parse_gpp_no_upd_sec_rec(
    byte *ptr,                 /*!< in: buffer */
    byte *end_ptr,             /*!< in: buffer end */
    page_t *page,              /*!< in/out: page or NULL */
    page_zip_des_t *page_zip); /*!< in/out: compressed page, or NULL */

}  // namespace lizard
#endif
