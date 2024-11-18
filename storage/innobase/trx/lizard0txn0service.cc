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

/** @file include/lizard0txn0service.cc
  Lizard transaction structure.

 Created 2020-03-27 by Jianwei.zhao
 *******************************************************/

#include "lizard0txn0service.h"

#include "trx0trx.h"
/**
  Decide master address when ac commit.
  @param[in]    trx
*/
void xa_addr_t::decide_if_ac_commit(const trx_t *trx) {
  if (is_null() || !trx) {
    reset();
    return;
  }
  ut_a(trx->txn_desc.maddr.is_null());
  ut_ad(is_valid());

  if (trx->id != tid) {
    ut_a(undo_ptr_get_slot(trx->txn_desc.undo_ptr) != slot_ptr);
  } else {
    reset();
  }
}

