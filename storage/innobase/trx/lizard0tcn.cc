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
#include "lizard0iv.h"
#include "lizard0mon.h"
#include "lizard0row.h"
#include "lizard0undo0types.h"
#include "trx0trx.h"

namespace lizard {

ulong innodb_tcn_cache_level = BLOCK_LEVEL;
ulong innodb_tcn_block_cache_type = BLOCK_LRU;

/** Search */
bool trx_search_tcn(trx_t *trx, btr_pcur_t *pcur, txn_rec_t *txn_rec,
                    txn_lookup_t *txn_lookup) {
  tcn_t tcn;
  if (innodb_tcn_cache_level == BLOCK_LEVEL) {
    Cache_tcn *cont = pcur->get_block()->cache_tcn;
    if (cont) tcn = cont->search(txn_rec->trx_id);
  } else {
    Session_tcn *cont = trx->session_tcn;
    if (cont) tcn = cont->search(txn_rec->trx_id);
  }

  if (tcn.trx_id == txn_rec->trx_id) {
    txn_rec->scn = tcn.scn;
    txn_rec->gcn = tcn.gcn;
    if (txn_lookup) {
      txn_lookup->real_image = {tcn.scn, UTC_UNDO_LOST, tcn.gcn};
      txn_lookup->real_state = TXN_STATE_COMMITTED;
    }
    TCN_CACHE_AGGR(innodb_tcn_cache_level, HIT);
    return true;
  }
  TCN_CACHE_AGGR(innodb_tcn_cache_level, MISS);
  return false;
}

/** Cache */
void trx_cache_tcn(trx_t *trx, trx_id_t trx_id, txn_rec_t &txn_rec,
                   const rec_t *rec, const dict_index_t *index,
                   const ulint *offsets, btr_pcur_t *pcur) {
  if (innodb_tcn_cache_level == BLOCK_LEVEL) {
    /** Collect tcn first, cache it when cleanout by holding X lock */
    tcn_collect(trx_id, txn_rec, rec, index, offsets, pcur);
  } else {
    /** Direct insert into session cache. */
    Session_tcn *cont = trx->session_tcn;
    if (cont) {
      tcn_t value(txn_rec);
      cont->insert(value);
      TCN_CACHE_AGGR(SESSION_LEVEL, EVICT);
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

