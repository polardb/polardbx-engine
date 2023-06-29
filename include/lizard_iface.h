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
#ifndef LIZARD_IFACE_INCLUDED
#define LIZARD_IFACE_INCLUDED

#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include <limits>
#include <numeric>

typedef uint64_t my_scn_t;
typedef uint64_t my_gcn_t;
typedef uint64_t my_utc_t;
typedef uint64_t my_trx_id_t;

/** Transaction slot address */
typedef uint64_t my_slot_ptr_t;

/** Commit number source type. */
enum my_csr_t {
  MYSQL_CSR_NONE = -1,
  MYSQL_CSR_AUTOMATIC = 0,
  MYSQL_CSR_ASSIGNED = 1
};

constexpr my_scn_t MYSQL_SCN_NULL = std::numeric_limits<my_scn_t>::max();

constexpr my_gcn_t MYSQL_GCN_NULL = std::numeric_limits<my_gcn_t>::max();

constexpr my_gcn_t MYSQL_GCN_MIN = 1024;

namespace lizard {
namespace xa {

enum Transaction_state {
  TRANS_STATE_COMMITTED = 0,
  TRANS_STATE_ROLLBACK = 1,
  TRANS_STATE_UNKNOWN = 2,
};

struct Transaction_info {
  Transaction_state state;
  my_gcn_t gcn;
};

extern bool hb_freezer_determine_freeze();

}  // namespace xa
}  // namespace lizard

#endif
