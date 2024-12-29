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

/** @file lock/lizard0lock.cc
 lizard lock operation.

 Created 2024-12-27 by Jianwei.zhao
 *******************************************************/

#include "lizard0lock.h"

namespace lizard {

/** Read txn rec info from index record.
 *
 * @param[in]		rec
 * @param[in]		cluster index
 * @param[in]		offsets
 * @param[out]		txn rec.
 * */
void lock_clust_rec_some_has_impl(const rec_t *rec, const dict_index_t *index,
                                  const ulint *offsets, txn_rec_t *txn_rec) {
  ut_ad(index->is_clustered());
  ut_ad(page_rec_is_user_rec(rec));
  ut_ad(!index->table->is_intrinsic());

  lizard::row_get_txn_rec(rec, index, offsets, txn_rec);
}

}  // namespace lizard
