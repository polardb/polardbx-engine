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

/** @file row/lizard0row0lotus0assert.cc
 HIPPO Index TRX_ID Assertion Framework.

 This framework provides assertions for verifying TRX_ID correctness
 in HIPPO secondary indexes across different scenarios.

 Created 2025-12-18 by Ting Yuan
 *******************************************************/

#include "lizard0row0lotus0assert.h"
#include "lizard0row0lotus.h"

namespace lizard {

/* ============================================================
 * 1. DML Assertions
 * ============================================================ */

bool row_hippo_assert_tid_in_dml_entry(const dict_index_t *index,
                                       const dtuple_t *entry, que_thr_t *thr) {
  if (!index->is_hippo()) return true;

  trx_t *trx = thr_get_trx(thr);
  trx_id_t entry_tid = entry->get_trx_id();

  if (trx->lock.que_state == TRX_QUE_ROLLING_BACK) {
    return (entry_tid == 0);
  } else {
    return (entry_tid != 0 && entry_tid == trx->id);
  }
}

bool row_hippo_assert_tid_in_dml_rec(const dict_index_t *index,
                                     const Slice *slc, que_thr_t *thr) {
  if (!index->is_hippo()) return true;

  trx_t *trx = thr_get_trx(thr);
  trx_id_t rec_tid = row_get_rec_lotus_tid(slc, index);

  if (trx->lock.que_state == TRX_QUE_ROLLING_BACK) {
    ut_ad(trx->in_rollback);
    return (rec_tid == 0);
  } else {
    return (rec_tid != 0 && rec_tid == trx->id);
  }
}

/* ============================================================
 * 2. Rollback Assertions
 * ============================================================ */

bool row_hippo_assert_tid_in_rollback_entry(const dict_index_t *index,
                                            const dtuple_t *entry) {
  ut_ad(!index->is_clustered());

  if (!index->is_hippo()) return true;
  return (entry->get_trx_id() == 0);
}

bool row_hippo_assert_tid_in_rollback_rec(const Slice *slc,
                                          const dict_index_t *index) {
  if (!index->is_hippo()) return true;
  return (row_get_rec_lotus_tid(slc, index) == 0);
}

/* ============================================================
 * 3. Row Log Assertions (ADD INDEX)
 * ============================================================ */

bool row_hippo_assert_tid_in_row_log_entry(const dict_index_t *index,
                                           const dtuple_t *entry,
                                           trx_id_t trx_id) {
  ut_ad(!index->is_clustered());
  if (!index->is_hippo()) return true;
  /* Allow entry->trx_id == 0 for rollback entries in row_log */
  return (entry->get_trx_id() == trx_id || entry->get_trx_id() == 0);
}

/* ============================================================
 * 4. Row Log Table Assertions (Table Rebuild)
 * ============================================================ */

bool row_hippo_assert_tid_in_row_log_table_entry(const dict_index_t *index,
                                                 const dtuple_t *entry,
                                                 trx_id_t trx_id) {
  ut_ad(!index->is_clustered());
  if (!index->is_hippo()) return true;
  return (entry->get_trx_id() == trx_id);
}

/* ============================================================
 * 5. Change Buffer Apply Assertions
 * ============================================================ */

bool row_hippo_assert_tid_in_ibuf_rec(const Slice *slc,
                                      const dict_index_t *index,
                                      trx_id_t trx_id) {
  if (!index->is_hippo()) return true;
  return (row_get_rec_lotus_tid(slc, index) == trx_id);
}

}  // namespace lizard
