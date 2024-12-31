/*****************************************************************************
Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/Apsara GalaxyEngine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/Apsara GalaxyEngine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/
/** @file include/lizard0tcn.h
  Lizard transaction commit number cache

 Created 2021-11-05 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0tcn_h
#define lizard0tcn_h

#include "hash0hash.h"

#include "lizard0tcn0types.h"
#include "lizard0mon.h"
#include "lizard0scn.h"
#include "lizard0scn0types.h"
#include "lizard0undo0types.h"
#include "lizard0txn0rec.h"

#include "rem0types.h"
#include "trx0types.h"

struct buf_pool_t;

struct trx_t;
struct btr_pcur_t;
struct dict_index_t;

/** Block level is deprecated */
enum tcn_cache_level { NONE_LEVEL = 0, GLOBAL_LEVEL, BLOCK_LEVEL };
enum tcn_block_cache_type { BLOCK_LRU = 0, BLOCK_RANDOM };

namespace lizard {

extern ulong srv_tcn_cache_level;
extern ulong srv_tcn_block_cache_type;
extern bool srv_tcn_cache_replace_after_commit;
extern longlong srv_tcn_cache_size;
extern const longlong srv_tcn_cache_def_size;
extern const longlong srv_tcn_cache_max_size;

typedef struct tcn_t {
 public:
  /** Transaction system commit number that has committed. */
  scn_t scn;
  /** Transaction global commit number that has committed. */
  gcn_t gcn;
  /** Transaction id that has committed. */
  unsigned long trx_id : 48;
  /** Commit number source for gcn */
  unsigned long csr : 1;
  /** Check if share commit number. */
  unsigned long is_slave : 1;
  /** Transaction Slot state */
  txn_status_t status;

  explicit tcn_t() {
    trx_id = 0;
    scn = SCN_NULL;
    gcn = GCN_NULL;
    csr = CSR_AUTOMATIC;
    is_slave = 0;
    status = txn_status_t::ACTIVE;
  }
  explicit tcn_t(const txn_rec_t &txn_rec, const txn_status_t &status_arg) {
    trx_id = txn_rec.trx_id;
    scn = txn_rec.scn;
    gcn = txn_rec.gcn;
    csr = undo_ptr_get_csr(txn_rec.undo_ptr);
    is_slave = undo_ptr_is_slave(txn_rec.undo_ptr);
    status = status_arg;
  }
  explicit tcn_t(const trx_id_t id, const commit_mark_t &cmmt,
                 const undo_ptr_t &undo_ptr, const txn_status_t &status_arg) {
    trx_id = id;
    scn = cmmt.scn;
    gcn = cmmt.gcn;
    csr = undo_ptr_get_csr(undo_ptr);
    is_slave = undo_ptr_is_slave(undo_ptr);
    status = status_arg;

    ut_ad(csr == cmmt.csr && status != txn_status_t::ACTIVE);
  }
  trx_id_t key() const { return trx_id; }
} tcn_t;

static_assert(sizeof(tcn_t) == 24, "tcn_t sizeof must be 24 bytes.");

using Cache_tcn = Cache_interface<trx_id_t, tcn_t>;

using Global_tcn = Atomic_linear_array<trx_id_t, tcn_t>;

/** Search from tcn cache and overwrite the txn rec include
 * [UBA flag, scn, gcn]
 *
 * @param[in/out]   txn rec
 * @param[in/out]   txn lookup status
 *
 * @retval          cache hit or not
 * */
bool trx_search_tcn(txn_rec_t *txn_rec, txn_status_t *txn_status);

/** Cache Commit Number if txn_rec was looked up.
 *
 * @param[in]	txn rec info that looked up
 * @param[in]	txn status */
void trx_cache_tcn(const txn_rec_t &txn_rec, const txn_status_t &status);

/** Cache Commit Number after trx commit.
 *
 * @param[in]	Committed trx
 * @param[in]	Allocated commit number
 */
void trx_cache_tcn(const trx_t *trx, bool serialised);

/** Get the number of tcn entries according to the srv_tcn_cache_size. */
ulong tcn_cache_size_align();

extern Cache_tcn *global_tcn_cache;

}  // namespace lizard

#define TCN_CACHE_AGGR(TYPE, WHAT)    \
  do {                                \
    if (!lizard::stat_enabled) break; \
    if (TYPE == NONE_LEVEL) {         \
    } else if (TYPE == BLOCK_LEVEL) { \
      BLOCK_TCN_CACHE_##WHAT;         \
    } else {                          \
      GLOBAL_TCN_CACHE_##WHAT;        \
    }                                 \
  } while (0)

#endif
