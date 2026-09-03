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

/** @file sql/komodo0handler0types.h

  Komodo handler types

  Created 2025-12-18 by Jianwei.zhao
 *******************************************************/

#ifndef  komodo0handler0types_h
#define komodo0handler0types_h

#include "my_inttypes.h"
#include <cstddef>
#include <functional>
#include <limits>
#include "my_inttypes.h"
#include "my_sys.h"

namespace komodo {

/** Guess Primary Page NO storage type. */
using gpp_no_t = uint32_t;

/** General Guess Primary Page NO length. */
constexpr static size_t SQL_GPP_LENGTH = 4;

/** NULL value for GPP number. */
constexpr uint32_t INNODB_FIL_NULL = std::numeric_limits<uint32_t>::max();

/** Read GPP number from 4 consecutive bytes. The most significant byte is at
 * the lowest address */
inline gpp_no_t gpp_read_from_ref(const uchar *b) {
  assert(b);
  return ((static_cast<uint32_t>(b[0]) << 24) |
          (static_cast<uint32_t>(b[1]) << 16) |
          (static_cast<uint32_t>(b[2]) << 8) | static_cast<uint32_t>(b[3]));
}

/** Write GPP number from 4 consecutive bytes. The most significant byte is at
 * the lowest address */
inline void gpp_write_to_ref(uchar *b, const gpp_no_t gpp_no) {
  assert(b);
  b[0] = static_cast<uchar>(gpp_no >> 24);
  b[1] = static_cast<uchar>(gpp_no >> 16);
  b[2] = static_cast<uchar>(gpp_no >> 8);
  b[3] = static_cast<uchar>(gpp_no);
}

/** Helper function to merge rowid with GPP info. */
inline void gpp_merge_behind_ref(uchar *dst_rowid, const uchar *src_rowid,
                                 uint ref_length) {
  gpp_no_t src_gpp_no = gpp_read_from_ref(src_rowid + ref_length);
  gpp_no_t dst_gpp_no = gpp_read_from_ref(dst_rowid + ref_length);

  if (dst_gpp_no == INNODB_FIL_NULL) {
    gpp_write_to_ref(dst_rowid + ref_length, src_gpp_no);
  }
}

/** Mode of Guess Primary Page NO */
enum guess_mode_t {
  GM_NONE,                ///< No guess context
  GM_ROWID_INTERSECTION,  ///< Called from rowid intersection iterator
  GM_ROWID_UNION,         ///< Called from rowid union iterator
  GM_INDEX_MERGE,         ///< Called from index merge iterator
  GM_MRR,                 ///< Called from multi-range read iterator
  GM_FILESORT,            ///< Called from filesort iterator
  GM_MULTI_UPDATE,        ///< Called from multi-table update iterator
  GM_MULTI_DELETE,        ///< Called from multi-table delete iterator
};

/** Context of Guess Primary Page NO. */
struct guess_ctx_t {
 public:
  /** Default constructor. */
  explicit guess_ctx_t() : m_gmode(GM_NONE), m_gpp_no(INNODB_FIL_NULL) {}

  /** Construct by gpp no directly. */
  explicit guess_ctx_t(guess_mode_t gmode, gpp_no_t gpp_no)
      : m_gmode(gmode), m_gpp_no(gpp_no) {}

  /** Construct from ref address. */
  void init(guess_mode_t gmode, const uchar *key_ptr, uint key_len) {
    assert(m_gmode == GM_NONE);
    assert(m_gpp_no == INNODB_FIL_NULL);

    if (gmode != GM_NONE) {
      m_gmode = gmode;
      m_gpp_no = gpp_read_from_ref(key_ptr + key_len);
    }
  }

  /** Reset context. */
  void reset() {
    m_gmode = GM_NONE;
    m_gpp_no = INNODB_FIL_NULL;
  }

  /** Get the guess mode.
   *
   * @retval	the guess mode */
  guess_mode_t gmode() const { return m_gmode; }

  /** Whether the GPP number is NULL. 
   * 
   * @retval	true	yes */
  bool is_null() const { return m_gpp_no == INNODB_FIL_NULL; }

  /** Get the GPP number.
   *
   * @retval	the GPP number */
  gpp_no_t gpp_no() const { return m_gpp_no; }

 private:
  guess_mode_t m_gmode;
  gpp_no_t m_gpp_no;
};

}  // namespace komodo

#endif

