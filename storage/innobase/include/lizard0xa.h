/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */


/** @file include/lizard0xa.h
  Lizard XA transaction structure.

 Created 2021-08-10 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0xa_h
#define lizard0xa_h

#include <string>
#include <unordered_set>

#include "trx0types.h"
#include "trx0xa.h"
#include "lizard0xa0types.h"

namespace lizard {
class Vision;
}

/**
  The description of XA transaction within trx_t
*/
typedef struct xa_desc_t {
 public:
  explicit xa_desc_t() : xid(), group() {}
  virtual ~xa_desc_t() { reset(); }

  void reset() { xid.null(); }

  bool is_null() const { return xid.is_null(); }

  const XID *my_xid() const { return &xid; }

  void build_group() { return; }

 private:
  /** Whether valid is determined by xid.is_null() */
  XID xid;

  /**
    For future:
      Building group in advance can improve the performance of collecting group_ids
      when open new vision;
  */
  std::string group;

} XAD;

/**
  Loop all the rw trx to find the same group transaction and
  push trx_id into group container.

  @param[in]    trx       current trx handler
  @param[in]    vision    current query view
*/
extern void vision_collect_trx_group_ids(const trx_t *my_trx,
                                         lizard::Vision *vision);

extern bool thd_get_transaction_group(THD *thd);

#if defined UNIV_DEBUG || defined LIZARD_DEBUG

#define assert_xad_state_initial(trx) \
  do {                                \
    ut_a(trx->xad.is_null() == true); \
  } while (0)

#else

#define assert_xad_state_initial(trx)

#endif  // defined UNIV_DEBUG || defined LIZARD_DEBUG

#endif
