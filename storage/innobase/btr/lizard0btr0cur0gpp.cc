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

#include "lizard0btr0cur0gpp.h"
#include "lizard0row0gpp.h"

namespace lizard {

/** Updates the redo log record for a gpp_no of a secondary index record.
  @param[in]      rec             secondary record
  @param[in]      index           secondary index
  @param[in]      gpp_no_offset   the offsets of gpp_no field
  @param[in]      gpp_no          page no of the cluster index
  @param[in]      mtr             mtr
*/
void btr_cur_upd_gpp_no_sec_rec_log(const rec_t *rec, const dict_index_t *index,
                                    ulint gpp_no_offset, page_no_t gpp_no,
                                    mtr_t *mtr) {
  byte *log_ptr = nullptr;
  /* 11 bytes for the initial part of a log record. */
  if (!mlog_open(mtr, 11 + 4 + 2 + 2, log_ptr)) {
    /* Logging in mtr is switched off during crash recovery:
    in that case mlog_open returns false */
    return;
  }
  log_ptr = mlog_write_initial_log_record_fast(rec, MLOG_REC_SEC_GPP_NO_UPDATE,
                                               log_ptr, mtr);

  mach_write_to_4(log_ptr, gpp_no);
  log_ptr += DATA_GPP_NO_LEN;

  mach_write_to_2(log_ptr, gpp_no_offset);
  log_ptr += 2;

  mach_write_to_2(log_ptr, page_offset(rec));
  log_ptr += 2;

  mlog_close(mtr, log_ptr);
}

/**
  Parses the redo log record for a gpp_no of a secondary index record.
  @param[in]      ptr        buffer
  @param[in]      end        buffer end
  @param[in]      page       page (NULL if it's just get the length)
  @param[in]      page_zip   compressed page, or NULL
  @param[in]      index      index corresponding to page

  @return         return the end of log record or NULL
*/
byte *btr_cur_parse_gpp_no_upd_sec_rec(
    byte *ptr,                /*!< in: buffer */
    byte *end_ptr,            /*!< in: buffer end */
    page_t *page,             /*!< in/out: page or NULL */
    page_zip_des_t *page_zip) /*!< in/out: compressed page, or NULL */
{
  rec_t *rec = nullptr;

  if (end_ptr < ptr + 4 + 2 + 2) {
    return (nullptr);
  }

 DBUG_EXECUTE_IF("crash_if_gpp_cleanout_redo", DBUG_SUICIDE(););

  auto gpp_no = mach_read_from_4(ptr);
  ptr += 4;

  auto gpp_offset = mach_read_from_2(ptr);
  ptr += 2;

  auto rec_offset = mach_read_from_2(ptr);
  ptr += 2;

  ut_a(rec_offset <= UNIV_PAGE_SIZE);

  if (page) {
    rec = page + rec_offset;

    /* We do not need to reserve search latch, as the page
    is only being recovered, and there cannot be a hash index to
    it. Besides, the delete-mark flag is being updated in place
    and the adaptive hash index does not depend on it. */

    row_upd_rec_gpp_fields_in_recovery(rec, page_zip, gpp_no, gpp_offset);
  }

  return (ptr);
}


}  // namespace lizard
