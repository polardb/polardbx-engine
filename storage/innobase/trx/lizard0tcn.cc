/*****************************************************************************
Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
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
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/
/** @file include/lizard0tcn.h
  Lizard transaction commit number cache

 Created 2021-11-05 by Jianwei.zhao
 *******************************************************/

#include "lizard0tcn.h"
#include "btr0pcur.h"
#include "buf0buf.h"
#include "lizard0iv.h"
#include "lizard0mon.h"
#include "lizard0row.h"
#include "lizard0undo0types.h"
#include "trx0trx.h"
#include "lizard0dbg.h"
#include "lizard0undo.h"

namespace lizard {

ulong innodb_tcn_cache_level = GLOBAL_LEVEL;
ulong innodb_tcn_block_cache_type = BLOCK_LRU;
bool innodb_tcn_cache_replace_after_commit = true;

Cache_tcn *global_tcn_cache = nullptr;

/** Search from tcn cache and overwrite the txn rec include
 * [UBA flag, scn, gcn]
 *
 * @param[in/out]	txn rec
 * @param[in]		pcur
 * @param[in/out]	txn lookup
 *
 * @retval		cache hit or not
 * */
bool trx_search_tcn(txn_rec_t *txn_rec, btr_pcur_t *pcur,
                    txn_lookup_t *txn_lookup) {
  tcn_t tcn;
  Cache_tcn *cont = nullptr;
  switch (innodb_tcn_cache_level) {
    case NONE_LEVEL:
      return false;
    case GLOBAL_LEVEL:
      cont = global_tcn_cache;
      break;
    case BLOCK_LEVEL:
      if (pcur) {
        cont = pcur->get_block()->cache_tcn;
      }
      break;
    default:
      ut_ad(0);
      cont = nullptr;
  }

  if (cont) {
    tcn = cont->search(txn_rec->trx_id);

    if (tcn.trx_id == txn_rec->trx_id) {
      lizard_undo_ptr_set_commit(&txn_rec->undo_ptr);
      txn_rec->scn = tcn.scn;
      txn_rec->gcn = tcn.gcn;

      ut_a(txn_rec->scn != SCN_NULL);
      ut_a(txn_rec->gcn != GCN_NULL);
      if (txn_lookup) {
        txn_lookup->real_image = {tcn.scn, UTC_UNDO_LOST, tcn.gcn};
        txn_lookup->real_state = TXN_STATE_COMMITTED;
      }

      TCN_CACHE_AGGR(innodb_tcn_cache_level, HIT);
      return true;
    }
  }
  TCN_CACHE_AGGR(innodb_tcn_cache_level, MISS);
  return false;
}

/** Cache */
void trx_cache_tcn(trx_id_t trx_id, txn_rec_t &txn_rec, const rec_t *rec,
                   const dict_index_t *index, const ulint *offsets,
                   btr_pcur_t *pcur) {
  Cache_tcn *cont = nullptr;
  ut_a(txn_rec.scn != SCN_NULL);
  ut_a(txn_rec.gcn != GCN_NULL);

  switch (innodb_tcn_cache_level) {
    case NONE_LEVEL:
      return;
    case GLOBAL_LEVEL:
      cont = global_tcn_cache;
      break;
    case BLOCK_LEVEL:
      tcn_collect(trx_id, txn_rec, rec, index, offsets, pcur);
      cont = nullptr;
      break;
    default:
      ut_ad(0);
      cont = nullptr;
  }
  if (cont) {
    tcn_t value(txn_rec);
    cont->insert(value);
    TCN_CACHE_AGGR(innodb_tcn_cache_level, EVICT);
  }
}

/** Cache the commit info into global tcn cache after commit. */
void trx_cache_tcn(trx_t *trx) {
  if (innodb_tcn_cache_replace_after_commit &&
      innodb_tcn_cache_level == GLOBAL_LEVEL && global_tcn_cache != nullptr) {
    commit_scn_t cmmt = trx->txn_desc.cmmt;
    trx_id_t trx_id = trx->id;

    if (trx_id != 0 && cmmt.scn != SCN_NULL && cmmt.gcn != GCN_NULL) {
      tcn_t value(trx_id, cmmt);
      global_tcn_cache->insert(value);
      TCN_CACHE_AGGR(innodb_tcn_cache_level, EVICT);
    }
  }
}

void allocate_block_tcn(buf_block_t *block) {
  if (block->cache_tcn == nullptr) {
    if (innodb_tcn_block_cache_type == BLOCK_LRU)
      block->cache_tcn = new Lru_tcn();
    else
      block->cache_tcn = new Array_tcn();
  }
}
void deallocate_block_tcn(buf_block_t *block) {
  if (block->cache_tcn != nullptr) {
    delete block->cache_tcn;
    block->cache_tcn = nullptr;
  }
}

}  // namespace lizard

