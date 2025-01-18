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

/** @file include/lizard0mtr0log.h
   Lizard Mini-transaction logging


 Created 2024-04-30 by Yichang Song
 *******************************************************/
#ifndef lizard0mtr0log_h
#define lizard0mtr0log_h

#include <cstdint>
#include "data0type.h"
#include "fil0fil.h"
#include "univ.i"

#include "lizard0txn0rec0types.h"

struct dict_index_t;

constexpr size_t REDO_SYS_FIELDS_LEN =
    14 /* 5(trx_id pos len) + 9(trx_id len)*/ + DATA_ROLL_PTR_LEN;

constexpr size_t REDO_CLOVER_FIELDS_LEN =
    14 /* 5(scn pos len) + 9(scn len)*/ + DATA_UNDO_PTR_LEN + DATA_GCN_ID_LEN;

constexpr size_t REDO_BAMBOO_FIELDS_LEN =
    14 /* 5(scn pos len) + 9(scn len)*/ + DATA_UNDO_PTR_LEN;

namespace lizard {
/* Secondary extra flag. */
/* The lower 1 bit is used for gpp. */
constexpr uint32_t SEC_EXTRA_FLAG_MASK_GPP = 0x01UL;
/* The higher 3 bits are used for ITL version. */
constexpr uint32_t SEC_EXTRA_FLAG_MASK_ITL = 0xE0UL;
enum SEC_EXTRA_FLAG_ITL { SEC_EXTRA_FLAG_PANDA = 0x20UL };

/** Get the sec_extra_flag from index.
  @param[in]  index           B-tree index.
  @param[out] sec_extra_flag  secondary index lizard fields extra flag
  @param[out] n_sec_fields    number of secondary index lizard fields
*/
void get_sec_extra_flag(const dict_index_t *index, uint8_t &sec_extra_flag,
                        uint8_t &n_sec_fields);

/** Log the sec_extra_flag.
  @param[in]     flag            1 byte flag indicating whether to log
                                 sec_extra_flag or not
  @param[in]     sec_extra_flag  1 byte secondary index lizard fields extra flag
                                 to be logged
  @param[in,out] log_ptr         REDO LOG buffer pointer */
void log_index_sec_extra_flag(uint8_t flag, uint8_t sec_extra_flag,
                              byte *&log_ptr);

/** Parse the sec_extra_flag.
  @param[in]  flag            1 byte flag indicating whether to parse
                              sec_extra_flag or not
  @param[in]  ptr             pointer to buffer
  @param[in]  end_ptr         pointer to end of buffer
  @param[out] sec_extra_flag  read 1 bytes sec_extra_flag
  @param[out] n_sec_fields    count number of secondary index lizard fields
  @param[out] n_s_gfields     count number of secondary GPP fields
  @param[out] page_type       page type of root
  @param[out] layout          txn layout
  @return pointer to buffer. */
byte *parse_index_sec_extra_flag(uint8_t flag, byte *ptr, const byte *end_ptr,
                                 uint8_t &sec_extra_flag, uint8_t &n_sec_fields,
                                 uint8_t &n_s_gfields, page_type_t &page_type,
                                 txn_layout_t *layout);

} // namespace lizard

#endif
