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

/** @file include/lizard0row0lotus0assert.h
 HIPPO Index TRX_ID Assertion Framework.

 This framework provides assertions for verifying TRX_ID correctness
 in HIPPO secondary indexes across different scenarios.

 Created 2025-12-18 by Ting Yuan
 *******************************************************/

#ifndef lizard0row0lotus0assert_h
#define lizard0row0lotus0assert_h

#include "dict0mem.h"
#include "que0que.h"
#include "btr0cur.h"

namespace lizard {

/* Forward declarations */
extern bool inject_stress_test_for_panda;

/* ============================================================
 * HIPPO Index TRX_ID Assertion Framework
 * ============================================================
 *
 * Assertion Scenarios:
 *
 * 1. DML (row_hippo_assert_tid_in_dml_*)
 *    - Normal INSERT/UPDATE/DELETE operations
 *    - entry/rec->trx_id == trx->id
 *    - If in_rollback, allows trx_id == 0
 *
 * 2. Rollback (row_hippo_assert_tid_in_rollback_*)
 *    - Rollback operations
 *    - entry/rec->trx_id must be 0
 *
 * 3. Row Log (row_hippo_assert_tid_in_row_log_*)
 *    - Online index creation (ADD INDEX)
 *    - entry/rec->trx_id == trx_id from row_log
 *    - Allows trx_id == 0 for rollback entries
 *
 * 4. Row Log Table (row_hippo_assert_tid_in_row_log_table_*)
 *    - Online table rebuild (ALTER TABLE ... DROP PK, ADD PK)
 *    - entry/rec->trx_id == trx_id from row_log_table
 *
 * 5. Change Buffer Apply (row_hippo_assert_tid_in_ibuf_*)
 *    - Change buffer merge operations
 *    - rec->trx_id == trx_id from ibuf entry
 *
 * Mode Control:
 *   - DEBUG: always use ut_ad
 *   - RELEASE: use ut_a if inject_stress_test_for_panda is on
 * ============================================================ */

/**
 * Check if HIPPO stress test assertion should be enforced.
 */
inline bool row_hippo_stress_test_enabled(const dict_index_t *index) {
  return index->is_hippo() && inject_stress_test_for_panda;
}

/* ============================================================
 * 1. DML Assertions
 * ============================================================ */

bool row_hippo_assert_tid_in_dml_entry(const dict_index_t *index,
                                       const dtuple_t *entry, que_thr_t *thr);

bool row_hippo_assert_tid_in_dml_rec(const dict_index_t *index,
                                     const Slice *slc, que_thr_t *thr);

/* ============================================================
 * 2. Rollback Assertions
 * ============================================================ */

bool row_hippo_assert_tid_in_rollback_entry(const dict_index_t *index,
                                            const dtuple_t *entry);

bool row_hippo_assert_tid_in_rollback_rec(const Slice *slc,
                                          const dict_index_t *index);

/* ============================================================
 * 3. Row Log Assertions (ADD INDEX)
 * ============================================================ */

bool row_hippo_assert_tid_in_row_log_entry(const dict_index_t *index,
                                           const dtuple_t *entry,
                                           trx_id_t trx_id);

/* ============================================================
 * 4. Row Log Table Assertions (Table Rebuild)
 * ============================================================ */

bool row_hippo_assert_tid_in_row_log_table_entry(const dict_index_t *index,
                                                 const dtuple_t *entry,
                                                 trx_id_t trx_id);

/* ============================================================
 * 5. Change Buffer Apply Assertions
 * ============================================================ */

bool row_hippo_assert_tid_in_ibuf_rec(const Slice *slc,
                                      const dict_index_t *index,
                                      trx_id_t trx_id);

}  // namespace lizard

/* ============================================================
 * Assertion Macros
 * ============================================================ */

#ifdef UNIV_DEBUG

#define HIPPO_ASSERT_TID_DML_ENTRY(index, entry, thr) \
  ut_ad(lizard::row_hippo_assert_tid_in_dml_entry(index, entry, thr))

#define HIPPO_ASSERT_TID_DML_REC(index, slc, thr) \
  ut_ad(lizard::row_hippo_assert_tid_in_dml_rec(index, slc, thr))

#define HIPPO_ASSERT_TID_ROLLBACK_ENTRY(index, entry) \
  ut_ad(lizard::row_hippo_assert_tid_in_rollback_entry(index, entry))

#define HIPPO_ASSERT_TID_ROLLBACK_REC(index, slc) \
  ut_ad(lizard::row_hippo_assert_tid_in_rollback_rec(slc, index))

#define HIPPO_ASSERT_TID_ROW_LOG_ENTRY(index, entry, trx_id) \
  ut_ad(lizard::row_hippo_assert_tid_in_row_log_entry(index, entry, trx_id))

#define HIPPO_ASSERT_TID_ROW_LOG_TABLE_ENTRY(index, entry, trx_id) \
  ut_ad(lizard::row_hippo_assert_tid_in_row_log_table_entry(index, entry, trx_id))

#define HIPPO_ASSERT_TID_IBUF_REC(index, slc, trx_id) \
  ut_ad(lizard::row_hippo_assert_tid_in_ibuf_rec(slc, index, trx_id))

#else /* !UNIV_DEBUG */

#define HIPPO_ASSERT_TID_DML_ENTRY(index, entry, thr)                     \
  do {                                                                    \
    if (lizard::row_hippo_stress_test_enabled(index)) {                   \
      ut_a(lizard::row_hippo_assert_tid_in_dml_entry(index, entry, thr)); \
    }                                                                     \
  } while (0)

#define HIPPO_ASSERT_TID_DML_REC(index, slc, thr)                     \
  do {                                                                \
    if (lizard::row_hippo_stress_test_enabled(index)) {               \
      ut_a(lizard::row_hippo_assert_tid_in_dml_rec(index, slc, thr)); \
    }                                                                 \
  } while (0)

#define HIPPO_ASSERT_TID_ROLLBACK_ENTRY(index, entry)                     \
  do {                                                                    \
    if (lizard::row_hippo_stress_test_enabled(index)) {                   \
      ut_a(lizard::row_hippo_assert_tid_in_rollback_entry(index, entry)); \
    }                                                                     \
  } while (0)

#define HIPPO_ASSERT_TID_ROLLBACK_REC(index, slc)                     \
  do {                                                                \
    if (lizard::row_hippo_stress_test_enabled(index)) {               \
      ut_a(lizard::row_hippo_assert_tid_in_rollback_rec(slc, index)); \
    }                                                                 \
  } while (0)

#define HIPPO_ASSERT_TID_ROW_LOG_ENTRY(index, entry, trx_id)                     \
  do {                                                                           \
    if (lizard::row_hippo_stress_test_enabled(index)) {                          \
      ut_a(lizard::row_hippo_assert_tid_in_row_log_entry(index, entry, trx_id)); \
    }                                                                            \
  } while (0)

#define HIPPO_ASSERT_TID_ROW_LOG_TABLE_ENTRY(index, entry, trx_id)                     \
  do {                                                                                 \
    if (lizard::row_hippo_stress_test_enabled(index)) {                                \
      ut_a(lizard::row_hippo_assert_tid_in_row_log_table_entry(index, entry, trx_id)); \
    }                                                                                  \
  } while (0)

#define HIPPO_ASSERT_TID_IBUF_REC(index, slc, trx_id)                     \
  do {                                                                    \
    if (lizard::row_hippo_stress_test_enabled(index)) {                   \
      ut_a(lizard::row_hippo_assert_tid_in_ibuf_rec(slc, index, trx_id)); \
    }                                                                     \
  } while (0)

#endif /* UNIV_DEBUG */

#endif /* lizard0row0lotus0assert_h */
