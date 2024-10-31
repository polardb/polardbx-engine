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

/** @file include/lizard0txn0service.h
  Lizard transaction structure.

 Created 2020-03-27 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0txn0service_h
#define lizard0txn0service_h

#include <tuple>

#include "my_inttypes.h"

#include "lizard0trx0service.h"
#include "ut0dbg.h"

struct trx_t;

/** Transaction slot address */
typedef uint64_t slot_ptr_t;
typedef uint16_t branch_num_t;

/*-----------------------------------------------------------------------------*/
/** Transaction slot physical address. */
/*-----------------------------------------------------------------------------*/
struct xa_addr_t {
 public:
  xa_addr_t() : tid(0), slot_ptr(0) {}

  xa_addr_t(trx_id_t tid_arg, slot_ptr_t ptr_arg)
      : tid(tid_arg), slot_ptr(ptr_arg) {}

  void reset() {
    tid = 0;
    slot_ptr = 0;
  }

  bool is_null() const {
    ut_ad(tid || !slot_ptr);
    ut_ad(slot_ptr || !tid);
    return tid == 0 && slot_ptr == 0;
  }

  bool is_valid() const { return tid && slot_ptr; }

  /**
    Decide master address when ac commit.
    @param[in]    trx
  */
  void decide_if_ac_commit(const trx_t *trx);

  trx_id_t tid;
  slot_ptr_t slot_ptr;
};

const xa_addr_t XA_ADDR_NULL(0, 0);

/*-----------------------------------------------------------------------------*/
/** The count of XA branchs */
/*-----------------------------------------------------------------------------*/
struct xa_branch_t {
 public:
  xa_branch_t() : n_global(0), n_local(0) {}

  xa_branch_t(branch_num_t n_global_arg, branch_num_t n_local_arg)
      : n_global(n_global_arg), n_local(n_local_arg) {}

  void reset() {
    n_global = 0;
    n_local = 0;
  }

  bool is_null() const { return n_global == 0 && n_local == 0; }

  branch_num_t n_global;
  branch_num_t n_local;
};

const xa_branch_t XA_BRANCH_NULL(0, 0);

#endif
