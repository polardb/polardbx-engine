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

/** @file include/lizard0gp.h
  Lizard global query.

 Created 2020-12-30 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0gp0types_h
#define lizard0gp0types_h

#include "os0event.h"
#include "ut0ut.h"

struct trx_t;

typedef std::set<trx_t *, std::less<trx_t *>, ut::allocator<trx_t *>> Trx_set;
typedef std::vector<trx_t *, ut::allocator<trx_t *>> Trx_array;

inline void copy_to(Trx_set &h, Trx_array &s) {
  for (Trx_set::const_iterator it = h.cbegin(); it != h.cend(); it++) {
    s.push_back(*it);
  }
}

struct gp_slot_t {
  /**
    Whether slot is used,
      if false that query_trx and committing trx must be both null;
      else true that query_trx or committing trx is not null; */
  bool in_use;

  /** Global query trx which will be blocked. */
  trx_t *query_trx;

  bool suspend;

  std::chrono::steady_clock::time_point suspend_time;

  std::chrono::steady_clock::duration wait_timeout;

  os_event_t event;
};

/** Global query thread state */
struct gp_state_t {
  /** whether it's blocked by prepared XA */
  bool waiting;
  /** Trx which has prepared and block global query */
  trx_t *blocking_trx;
  /** Requested slot */
  gp_slot_t *slot;

 public:
  void build(trx_t *trx) {
    ut_ad(waiting == false);
    blocking_trx = trx;
    waiting = true;
  }
  void release() {
    ut_ad(waiting == true && blocking_trx != nullptr);
    blocking_trx = nullptr;
    waiting = false;
  }
};

/** XA prepared transaction */
struct gp_wait_t {
 public:
  gp_wait_t() : blocked_trxs(), n_blocked(0) {}

  virtual ~gp_wait_t() {
    ut_a(blocked_trxs.size() == 0);
    ut_a(n_blocked.load() == 0);
  }

  void reset() {
    ut_a(blocked_trxs.size() == 0);
    ut_a(n_blocked.load() == 0);
  }

  bool validate_null() {
    if (n_blocked.load() == 0 && blocked_trxs.size() == 0) return true;
    return false;
  }

  void build(trx_t *trx) {
    blocked_trxs.insert(trx);
    n_blocked++;
  }

  void release(trx_t *trx) {
#ifndef NDEBUG
    auto it = blocked_trxs.find(trx);
    ut_ad(it != blocked_trxs.end());
#endif
    blocked_trxs.erase(trx);
    n_blocked--;
  }

 public:
  Trx_set blocked_trxs;
  std::atomic<int> n_blocked;
};

#endif
