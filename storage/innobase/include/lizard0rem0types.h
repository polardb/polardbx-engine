/*****************************************************************************

Copyright (c) 2013, 2026, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file include/lizard0rem0types.h
 Record manager global types.

 Created 2026-01-07 by Yichang Song
 *******************************************************/

#ifndef lizard0rem0types_h
#define lizard0rem0types_h

#include "rem0types.h"

enum mem_loc_t { MLOC_NONE = 0, MLOC_IN_PAGE, MLOC_IN_HEAP };

struct creg_t;  // Forward declaration

struct reg_t {
  rec_t *rec;
  mem_loc_t loc;

  constexpr reg_t() : rec(nullptr), loc(MLOC_NONE) {}
  reg_t(rec_t *r, mem_loc_t l) : rec(r), loc(l) {
    ut_ad((rec != nullptr) == (loc != MLOC_NONE));
  }

  void reset() {
    rec = nullptr;
    loc = MLOC_NONE;
  }

  bool is_null() const {
    ut_ad((rec != nullptr) == (loc != MLOC_NONE));
    return rec == nullptr;
  }
  bool is_not_null() const { return !is_null(); }

  bool in_page() const { return loc == MLOC_IN_PAGE; }
  bool in_heap() const { return loc == MLOC_IN_HEAP; }

  bool operator==(const reg_t &other) const {
    if (rec == other.rec) {
      ut_ad(loc == other.loc);
      return true;
    }
    return false;
  }

  bool operator!=(const reg_t &other) const { return !(*this == other); }

  bool operator==(const creg_t &other) const;
  bool operator!=(const creg_t &other) const;
};

struct creg_t {
  const rec_t *rec;
  mem_loc_t loc;

  constexpr creg_t() : rec(nullptr), loc(MLOC_NONE) {}
  creg_t(const rec_t *r, mem_loc_t l) : rec(r), loc(l) {
    ut_ad((rec != nullptr) == (loc != MLOC_NONE));
  }

  creg_t(const reg_t &other) : rec(other.rec), loc(other.loc) {}

  void reset() {
    rec = nullptr;
    loc = MLOC_NONE;
  }

  bool is_null() const {
    ut_ad((rec != nullptr) == (loc != MLOC_NONE));
    return rec == nullptr;
  }
  bool is_not_null() const { return !is_null(); }

  bool in_page() const { return loc == MLOC_IN_PAGE; }
  bool in_heap() const { return loc == MLOC_IN_HEAP; }

  bool operator==(const creg_t &other) const {
    if (rec == other.rec) {
      ut_ad(loc == other.loc);
      return true;
    }
    return false;
  }

  bool operator!=(const creg_t &other) const { return !(*this == other); }

  // Comparison with reg_t
  bool operator==(const reg_t &other) const {
    if (rec == other.rec) {
      ut_ad(loc == other.loc);
      return true;
    }
    return false;
  }

  bool operator!=(const reg_t &other) const { return !(*this == other); }
};

inline bool reg_t::operator==(const creg_t &other) const {
  if (rec == other.rec) {
    ut_ad(loc == other.loc);
    return true;
  }
  return false;
}

inline bool reg_t::operator!=(const creg_t &other) const {
  return !(*this == other);
}

/** Null/invalid reg_t (e.g. allocation failed, insert failed). Check with
 is_null(). */
constexpr reg_t REG_NULL{};

/** Null/invalid creg_t. Check with is_null(). */
constexpr creg_t CREG_NULL{};

/** Buffer with memory location. */
struct boc_t {
 public:
  byte *buf{nullptr};
  mem_loc_t loc{MLOC_NONE};

 public:
  explicit boc_t() : buf(nullptr), loc(MLOC_NONE) {}

  boc_t(byte *buf_arg, const mem_loc_t lock_arg) : buf(buf_arg), loc(lock_arg) {}

  bool is_null() const { return buf == nullptr; }

  void reset() {
    buf = nullptr;
    loc = MLOC_NONE;
  }
};

#endif
