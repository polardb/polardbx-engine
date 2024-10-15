/*****************************************************************************

Copyright (c) 2013, 2021, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file read/lizard0read0xa.cc
  Lizard XA transaction structure.

 Created 2024-10-10 by Zefeng.liu
 *******************************************************/

#include "lizard0read0xa.h"
#include "lizard0xa.h"

/** @{ */

/** Bqual format: 'xxx@nnnn' */
static unsigned int XID_GROUP_SUFFIX_SIZE = 5;

static char XID_GROUP_SPLIT_CHAR = '@';

/**
  check if the xid match the format v1.

  Requirement:
  1) Buqal length must be greater than XID_GROUP_SUFFIX_SIZE
  2) Split char must be right.
  3) The suffix must be a numober except split char

  @param[in]  xid   xid to be checked
  @return true if match, otherwise false
*/
bool check_if_match_format_v1(const XID *xid) {
  if (xid->is_null()) return false;

  int prefix_len =
      xid->gtrid_length + xid->bqual_length - XID_GROUP_SUFFIX_SIZE;

  if (xid->bqual_length <= XID_GROUP_SUFFIX_SIZE ||
      xid->data[prefix_len] != XID_GROUP_SPLIT_CHAR) {
    return false;
  }
  for (unsigned int i = 1; i < XID_GROUP_SUFFIX_SIZE; i++) {
    if (!my_isdigit(&my_charset_latin1, xid->data[prefix_len + i])) {
      return false;
    }
  }
  return true;
}
/** @} */

/**
 * Before building the group id in format v1, we must check if the xid meet
 * requirements (call @check_if_match_format_v1). The trxs that meet the
 * following requirements are divided into a group in format v1:
 *
 * 1) gtrid must be equal
 * 2) bqual prefix must be equal
 * 3) formatID must be equal
 *
 * So we append bqual prefix and formatID to xa_desc_t::m_gid besides gtrid,
 * which is quite different from format v2. To specify the version, format
 * version is also appended.
 *
 * @return true if the group id is built successfully, otherwise false
 */
bool xa_desc_t::build_gid_v1() {
  ut_ad(m_gid.empty());

  /** No need to build the group id. */
  if (!check_if_match_format_v1(&m_xid)) {
    return false;
  }

  auto formatID = std::to_string(m_xid.get_format_id());

  int length = m_xid.get_gtrid_length() + m_xid.get_bqual_length() -
               XID_GROUP_SUFFIX_SIZE + formatID.size() + sizeof(FORMAT_V1) - 1;

  m_gid.reserve(length);

  /** 1. append gtrid and bqual prefix */
  m_gid.append(m_xid.get_data(), length);

  /** 2. append formatID */
  m_gid.append(formatID);

  /** 3. append format version */
  m_gid.append(FORMAT_V1, sizeof(FORMAT_V1) - 1);

  return true;
}

/**
 * Build the group id in format v2. The trxs that meet the following
 * Requirements are divided into a group in format v2:
 *
 * 1) gtrid must be equal
 *
 * So we append gtrid to xa_desc_t::m_gid. To specify the version, format
 * version is also appended.
 * @return true if the group id is built successfully, otherwise false
 */
bool xa_desc_t::build_gid_v2() {
  ut_ad(m_gid.empty());
  m_gid.reserve(m_xid.get_gtrid_length() + sizeof(FORMAT_V2) - 1);

  /** 1. append gtrid */
  m_gid.append(m_xid.get_data(), m_xid.get_gtrid_length());

  /** 2. append format version */

  m_gid.append(FORMAT_V2, sizeof(FORMAT_V2) - 1);

  return true;
}

/**
 * Build the group id. For 0-FORMAT_V1_RANGE, use the format v1.
 * Otherwise, use the format v2.
 *
 * @return true if the group id is built successfully, otherwise false
 */
bool xa_desc_t::build_gid() {
  ut_ad(!m_xid.is_null());
  ut_ad(m_group == nullptr);

  ut_ad(m_xid.get_format_id() >= 0);

  if (m_xid.get_format_id() <= FORMAT_V1_RANGE) {
    return build_gid_v1();
  } else {
    return build_gid_v2();
  }
}

void Xa_vision::update_group_ids(const Xa_group *xa_group) {
  if (xa_group->has_modified(m_group_clock)) {
    for (auto trx_id : xa_group->get_trx_ids()) {
      m_group_ids.insert(trx_id);
    }
    m_group_clock = xa_group->get_clock();
  }
}