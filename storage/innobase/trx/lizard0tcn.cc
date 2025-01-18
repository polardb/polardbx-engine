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

#include "lizard0tcn.h"
#include "btr0pcur.h"
#include "buf0buf.h"
#include "lizard0dbg.h"
#include "lizard0dict0mem.h"
#include "lizard0mon.h"
#include "lizard0row.h"
#include "lizard0undo.h"
#include "lizard0undo0types.h"
#include "trx0trx.h"

namespace lizard {

ulong srv_tcn_cache_level = GLOBAL_LEVEL;
ulong srv_tcn_block_cache_type = BLOCK_LRU;
bool srv_tcn_cache_replace_after_commit = true;
longlong srv_tcn_cache_size = 0;
const longlong srv_tcn_cache_def_size = 0;
const longlong srv_tcn_cache_max_size = LLONG_MAX;

Cache_tcn *global_tcn_cache = nullptr;

/** Search from tcn cache and overwrite the txn rec include
 * [UBA flag, scn, gcn]
 *
 * @param[in/out]   txn rec
 * @param[in/out]   txn state
 *
 * @retval          cache hit or not
 * */
bool trx_search_tcn(txn_rec_t *txn_rec, txn_status_t *txn_status) {
  tcn_t tcn;
  Cache_tcn *cont = nullptr;
  ut_ad(txn_rec && txn_status);

  /* Disable tcn cache for stress test of panda index.*/
  if (lizard::inject_stress_test_for_panda) {
    return false;
  }

  switch (srv_tcn_cache_level) {
    case NONE_LEVEL:
      return false;
    case GLOBAL_LEVEL:
    case BLOCK_LEVEL:
      cont = global_tcn_cache;
      break;
    default:
      ut_ad(0);
      cont = nullptr;
  }

  if (cont) {
    tcn = cont->search(txn_rec->trx_id);

    if (tcn.trx_id == txn_rec->trx_id) {
      undo_ptr_set_commit(&txn_rec->undo_ptr, tcn.csr, tcn.is_slave);
      txn_rec->scn = tcn.scn;
      txn_rec->gcn = tcn.gcn;
      *txn_status = tcn.status;

      ut_ad(txn_rec->is_whole_committed());

      tcn_cache_stat(true);
      return true;
    }
  }
  tcn_cache_stat(false);
  return false;
}

/** Cache Commit Number after txn_rec was looked up, that maybe isn't
 * real original SCN/GCN of the txn_rec->trx_id transaction, but we still
 * and only use it.
 *
 * @param[in]	txn rec info that looked up */
void trx_cache_tcn(const txn_rec_t &txn_rec, const txn_status_t &status) {
  Cache_tcn *cont = nullptr;
  ut_a(txn_rec.is_whole_committed());
  ut_a(txn_rec.trx_id != 0);

  switch (srv_tcn_cache_level) {
    case NONE_LEVEL:
      return;
    case BLOCK_LEVEL:
    case GLOBAL_LEVEL:
      cont = global_tcn_cache;
      break;
    default:
      ut_ad(0);
      cont = nullptr;
  }
  if (cont) {
    tcn_t value(txn_rec, status);
    cont->insert(value);
  }
}

/** Cache Commit Number after trx commit, the SCN/GCN is real commit number
 * of that transaction.
 *
 * @param[in]	Committed trx
 * @param[in]	Allocated commit number
 * */
void trx_cache_tcn(const trx_t *trx, bool serialised) {
  Cache_tcn *cont = nullptr;
  if (!srv_tcn_cache_replace_after_commit) return;

  switch (srv_tcn_cache_level) {
    case NONE_LEVEL:
      return;
    case BLOCK_LEVEL:
    case GLOBAL_LEVEL:
      cont = global_tcn_cache;
      break;
    default:
      ut_ad(0);
      cont = nullptr;
  }

  if (cont) {
    if (trx->id > 0 && serialised) {
      assert_trx_commit_mark_allocated(trx);
      ut_ad(trx->txn_desc.is_whole_committed());

      tcn_t value(trx->id, trx->txn_desc.cmmt, trx->txn_desc.undo_ptr,
                  txn_status_t::COMMITTED);
      cont->insert(value);
    }
  }
}

/** Get the number of tcn entries according to the srv_tcn_cache_size. */
ulong tcn_cache_size_align() {
  /** Mapping rules between buffer pool size and the number of entries in global tcn cache*/
  static std::map<longlong, ulong> buf_2_tcn = {
      {4LL * 1024 * 1024 * 1024, 1 * 1024 * 1024},
      {8LL * 1024 * 1024 * 1024, 2 * 1024 * 1024},
      {16LL * 1024 * 1024 * 1024, 4 * 1024 * 1024},
      {32LL * 1024 * 1024 * 1024, 8 * 1024 * 1024},
      {64LL * 1024 * 1024 * 1024, 16 * 1024 * 1024},
      {LLONG_MAX, 32 * 1024 * 1024}};

  ulong tcn_entry_num;
  if (srv_tcn_cache_size == 0) {
    tcn_entry_num = buf_2_tcn.upper_bound(srv_buf_pool_curr_size)->second;
    srv_tcn_cache_size = tcn_entry_num * sizeof(tcn_t);
  } else {
    tcn_entry_num = srv_tcn_cache_size / sizeof(tcn_t);
  }
  return tcn_entry_num;
}
}  // namespace lizard
