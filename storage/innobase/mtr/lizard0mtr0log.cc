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

/** @file mtr/lizard0mtr0log.cc
   Lizard Mini-transaction logging


 Created 2024-04-30 by Yichang Song
 *******************************************************/

#include "lizard0mtr0log.h"
#include "dict0mem.h"
#include "lizard0dict.h"
#include "mtr0log.h"

namespace lizard {

/** Get the sec_extra_flag from index.
  @param[in]  index           B-tree index.
  @param[out] sec_extra_flag  secondary index lizard fields extra flag
  @param[out] n_sec_fields    number of secondary index lizard fields
*/
void get_sec_extra_flag(const dict_index_t *index, uint8_t &sec_extra_flag,
                        uint8_t &n_sec_fields) {
  sec_extra_flag = 0;
  n_sec_fields = 0;
  assert_lizard_dict_index_gstored_check(index);

  if (index->n_s_gfields > 0) {
    ut_ad(index->n_s_gfields == 1);
    sec_extra_flag |= SEC_EXTRA_FLAG_MASK_GPP;
    n_sec_fields++;
  }

  if (index->is_panda()) {
    n_sec_fields += 4;
    sec_extra_flag |= SEC_EXTRA_FLAG_PANDA;
  }
}

/** Log the sec_extra_flag.
  @param[in]     flag            1 byte flag indicating whether to log
                                 sec_extra_flag or not
  @param[in]     sec_extra_flag  1 byte secondary index lizard fields extra flag
                                 to be logged
  @param[in,out] log_ptr         REDO LOG buffer pointer */
void log_index_sec_extra_flag(uint8_t flag, uint8_t sec_extra_flag,
                              byte *&log_ptr) {
  ut_ad(sec_extra_flag || !IS_SEC_EXTRA(flag));
  if (IS_SEC_EXTRA(flag)) {
    mach_write_to_1(log_ptr, sec_extra_flag);
    log_ptr += 1;
  }
}

/** Parse the sec_extra_flag.
  @param[in]  flag            1 byte flag indicating whether to parse
                              sec_extra_flag or not
  @param[in]  ptr             pointer to buffer
  @param[in]  end_ptr         pointer to end of buffer
  @param[out] sec_extra_flag  read 1 bytes sec_extra_flag
  @param[out] n_sec_fields    count number of secondary index extra fields
  @param[out] n_s_gfields     count number of secondary GPP fields
  @param[out] page_type       page type of root
  @param[out] layout          txn layout
  @return pointer to buffer. */
byte *parse_index_sec_extra_flag(uint8_t flag, byte *ptr, const byte *end_ptr,
                                 uint8_t &sec_extra_flag, uint8_t &n_sec_fields,
                                 uint8_t &n_s_gfields, page_type_t &page_type,
                                 txn_layout_t *layout) {
  sec_extra_flag = 0;
  n_sec_fields = 0;
  n_s_gfields = 0;
  page_type = FIL_PAGE_TYPE_UNUSED;
  *layout = TL_CLOVER;
  if (!IS_SEC_EXTRA(flag)) {
    return ptr;
  }

  if (end_ptr < ptr + 1) {
    return (nullptr);
  }

  sec_extra_flag = mach_read_from_1(ptr);
  ptr += 1;
  if (sec_extra_flag & SEC_EXTRA_FLAG_MASK_GPP) {
    n_sec_fields++;
    n_s_gfields = 1;
    DBUG_EXECUTE_IF("crash_if_gpp_in_redo", DBUG_SUICIDE(););
  }

  switch (sec_extra_flag & SEC_EXTRA_FLAG_MASK_ITL) {
    case SEC_EXTRA_FLAG_PANDA:
      DBUG_EXECUTE_IF("crash_if_panda_in_redo", DBUG_SUICIDE(););
      n_sec_fields += 4;
      page_type = FIL_PAGE_INDEX_PANDA;
      *layout = TL_BAMBOO;
      break;
    default:
      page_type = FIL_PAGE_INDEX;
      break;
  }
  return ptr;
}
} // namespace lizard