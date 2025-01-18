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

/** @file row/lizard0row.h
 lizard row multi-version operation.

 Created 2025-01-02 by Jianwei.zhao
 *******************************************************/

#include "dict0mem.h"

#include "lizard0row0vers.h"

namespace lizard {

/** Simulate a invalid vision which took as a faked asof query,
 * then trx_undo_prev_version_build will try to get prev image
 * until txn slot state was changed to purged.
 *
 * Simulated vision is only used by row_vers_old_has_index_entry,
 * TRX_UNDO_PURGED state is enough for secondary index, so ignore
 * flashback area.
 * */
const Vision *row_vers_old_simulate_vision() {
  static Snapshot_scn_vision simulate_snapshot;
  static const Vision simulate_vision(&simulate_snapshot);
  /** It's a faked as of scn vision to get prev version instead of purge vision.
   */
  return &simulate_vision;
}

/** Jedge whether the cluster delete marked record still was needed.
 *
 * Before, it judge through purge_sys view, it promise that will never older
 * query to see it.
 *
 * after, we support asof query, so change dependency to txn slot.
 *
 *
 * @param[in/out]	txn rec
 * @param[in]		dict table
 *
 * @retval	true	maybe still usable by normal query or asof query. */
bool row_clust_vers_must_preserve_del_marked(txn_rec_t *txn_rec,
                                             const dict_table_t *table) {
  mtr_t mtr;

  /** Caller has held cluster index page lock. */
  mtr_start(&mtr);

  /** delete marked record will be not inherited after ddl, so table->is_2pp is
   * safe here. */
  bool missing = txn_rec_is_missing_history(txn_rec, table->is_2pp, &mtr);

  mtr_commit(&mtr);
  return !missing;
}

/** Jedge whether the panda delete marked record still was needed.
 *
 * Before, it judge through purge_sys view, it promise that will never older
 * query to see it.
 *
 * after, we support asof query, so change dependency to txn slot.
 *
 *
 * @param[in/out]	txn rec
 * @param[in]		dict table
 *
 * @retval	true	maybe still usable by normal query or asof query. */
bool row_panda_vers_must_preserve_del_marked(txn_rec_t *txn_rec) {
  mtr_t mtr;

  mtr_start(&mtr);

  bool missing = txn_rec_is_missing_history(txn_rec, false, &mtr);

  mtr_commit(&mtr);
  return !missing;
}

}  // namespace lizard
