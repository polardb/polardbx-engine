/*****************************************************************************

Copyright (c) 2013, 2023, Alibaba and/or its affiliates. All Rights Reserved.

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

#ifndef LIZARD_LIZARD_RPL_GCN_INCLUDED
#define LIZARD_LIZARD_RPL_GCN_INCLUDED

#include <string>
#include "my_dbug.h"
#include <limits>

typedef uint64_t my_scn_t;
typedef uint64_t my_gcn_t;
typedef uint64_t my_utc_t;
typedef uint64_t my_trx_id_t;

/** Transaction slot address */
typedef uint64_t my_slot_ptr_t;

constexpr my_scn_t MYSQL_SCN_NULL = std::numeric_limits<my_scn_t>::max();

constexpr my_gcn_t MYSQL_GCN_NULL = std::numeric_limits<my_gcn_t>::max();

constexpr my_gcn_t MYSQL_GCN_MIN = 1024;

/** Commit number source type. */
enum my_csr_t {
  MYSQL_CSR_NONE = -1,
  MYSQL_CSR_AUTOMATIC = 0,
  MYSQL_CSR_ASSIGNED = 1
};

struct MyGCN {
 private:
  my_csr_t csr;
  my_gcn_t gcn;

 public:
  MyGCN() { reset(); }

  void set(const my_gcn_t _gcn, const my_csr_t _csr) {
    gcn = _gcn;
    csr = _csr;
  }

  bool is_empty() const {
#ifdef UNIV_DEBUG
    if (gcn == MYSQL_GCN_NULL) {
      assert(csr == MYSQL_CSR_NONE);
    }
#endif
    return gcn == MYSQL_GCN_NULL;
  }

  bool is_automatic() const {
#ifdef UNIV_DEBUG
    if (csr == MYSQL_CSR_AUTOMATIC) {
      assert(gcn != MYSQL_GCN_NULL);
    }
#endif
    return csr == MYSQL_CSR_AUTOMATIC;
  }

  bool is_assigned() const {
#ifdef UNIV_DEBUG
    if (csr == MYSQL_CSR_ASSIGNED) {
      assert(gcn != MYSQL_GCN_NULL);
    }
#endif
    return csr == MYSQL_CSR_ASSIGNED;
  }

  my_gcn_t get_gcn() const { return gcn; }

  my_csr_t get_csr() const { return csr; }

  void reset() {
    gcn = MYSQL_GCN_NULL;
    csr = MYSQL_CSR_NONE;
  }

  std::string print() const {
    char buf[64];
    const char *csr_msg = nullptr;
    switch (csr) {
      case MYSQL_CSR_NONE:
        csr_msg = "MYSQL_CSR_NONE";
        break;
      case MYSQL_CSR_ASSIGNED:
        csr_msg = "MYSQL_CSR_ASSIGNED";
        break;
      case MYSQL_CSR_AUTOMATIC:
        csr_msg = "MYSQL_CSR_AUTOMATIC";
        break;
    }

    snprintf(buf, sizeof(buf), "GCN_SRC = %s, gcn_val = %lu", csr_msg, gcn);
    return buf;
  }
};

#define MyGCN_NULL (MyGCN{})

struct MyVisionGCN {
 public:
  MyVisionGCN() { reset(); }

  void reset() {
    csr = MYSQL_CSR_NONE;
    gcn = MYSQL_GCN_NULL;
    current_scn = MYSQL_SCN_NULL;
  }

  void set(my_csr_t _csr, my_gcn_t _gcn, my_scn_t _scn) {
    assert(_csr == MYSQL_CSR_AUTOMATIC || _csr == MYSQL_CSR_ASSIGNED);
    if (_csr == MYSQL_CSR_ASSIGNED) {
      assert(_scn == MYSQL_SCN_NULL);
    } else {
      assert(_scn != MYSQL_SCN_NULL);
    }
    csr = _csr;
    gcn = _gcn;
    current_scn = _scn;
  }

  bool is_null() {
    if (csr == MYSQL_CSR_AUTOMATIC) {
      assert(gcn != MYSQL_GCN_NULL);
      assert(current_scn != MYSQL_SCN_NULL);
    } else if (csr == MYSQL_CSR_ASSIGNED) {
      assert(gcn != MYSQL_GCN_NULL);
      assert(current_scn == MYSQL_SCN_NULL);
    } else {
      assert(gcn == MYSQL_GCN_NULL);
      assert(current_scn == MYSQL_SCN_NULL);
    }

    return csr == MYSQL_CSR_NONE;
  }

  my_csr_t csr;
  my_gcn_t gcn;
  my_scn_t current_scn;
};

namespace lizard {
namespace xa {

enum Transaction_state {
  TRANS_STATE_COMMITTED = 0,
  TRANS_STATE_ROLLBACK = 1,
  TRANS_STATE_ROLLBACKING_BACKGROUND = 2,
  TRANS_STATE_UNKNOWN = 3,
};

struct Transaction_info {
  Transaction_state state;
  MyGCN gcn;
};

}  // namespace xa
}  // namespace lizard

#endif
