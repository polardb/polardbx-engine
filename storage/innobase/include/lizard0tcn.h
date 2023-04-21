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
#include "lizard0iv.h"
#include "lizard0mon.h"
#include "lizard0scn.h"
#include "lizard0scn0types.h"
#include "lizard0undo0types.h"
#include "rem0types.h"
#include "trx0types.h"

struct buf_pool_t;

struct trx_t;
struct btr_pcur_t;
struct dict_index_t;

enum tcn_cache_level { NONE_LEVEL = 0, GLOBAL_LEVEL, BLOCK_LEVEL };
enum tcn_block_cache_type { BLOCK_LRU = 0, BLOCK_RANDOM };

namespace lizard {

extern ulong innodb_tcn_cache_level;
extern ulong innodb_tcn_block_cache_type;
extern bool innodb_tcn_cache_replace_after_commit;
extern longlong innodb_tcn_cache_size;
extern const longlong innodb_tcn_cache_def_size;
extern const longlong innodb_tcn_cache_max_size;

typedef struct tcn_t {
  /** Transaction id that has committed. */
  trx_id_t trx_id;
  /** Transaction system commit number that has committed. */
  scn_t scn;
  /** Transaction global commit number that has committed. */
  gcn_t gcn;
  /** Commit number source for gcn */
  csr_t csr;

  explicit tcn_t() {
    trx_id = 0;
    scn = SCN_NULL;
    gcn = GCN_NULL;
    csr = CSR_AUTOMATIC;
  }
  explicit tcn_t(txn_commit_t cmmt) {
    trx_id = cmmt.trx_id;
    scn = cmmt.scn;
    gcn = cmmt.gcn;
    csr = undo_ptr_get_csr(cmmt.undo_ptr);
  }
  explicit tcn_t(trx_id_t id, commit_mark_t cmmt) {
    trx_id = id;
    scn = cmmt.scn;
    gcn = cmmt.gcn;
    csr = cmmt.csr;
  }
  trx_id_t key() { return trx_id; }
} tcn_t;

/** Transaction commit information */
typedef struct tcn_node_t {
 public:
  /** List node */
  UT_LIST_NODE_T(tcn_node_t) list;
  /** Hash node */
  hash_node_t hash;
  /** Transaction id that has committed. */
  trx_id_t trx_id;
  /** Transaction system commit number that has committed. */
  scn_t scn;
  /** Transaction global commit number that has committed. */
  gcn_t gcn;

  csr_t csr;

  explicit tcn_node_t() {
    hash = nullptr;
    list.prev = nullptr;
    list.next = nullptr;
    trx_id = 0;
    scn = SCN_NULL;
    gcn = GCN_NULL;
    csr = CSR_AUTOMATIC;
  }

  trx_id_t key() { return trx_id; }
  hash_node_t &hash_node() { return hash; }

  void assign(const tcn_t tcn) {
    trx_id = tcn.trx_id;
    scn = tcn.scn;
    gcn = tcn.gcn;
    csr = tcn.csr;
  }

  void copy_to(tcn_t &tcn) const {
    tcn.trx_id = trx_id;
    tcn.scn = scn;
    tcn.gcn = gcn;
    tcn.csr = csr;
  }
} tcn_node_t;

#define LRU_TCN_SIZE 20
#define ARRAY_TCN_SIZE 50
#define SESSION_TCN_SIZE 2000
#define GLOBAL_TCN_SIZE (1024 * 1024 * 4)

using Cache_tcn = Cache_interface<tcn_node_t, trx_id_t, tcn_t>;

using Lru_tcn = Lru_list<tcn_node_t, trx_id_t, tcn_t, LRU_TCN_SIZE>;

using Array_tcn = Random_array<tcn_node_t, trx_id_t, tcn_t>;

using Session_tcn = Lru_list<tcn_node_t, trx_id_t, tcn_t, SESSION_TCN_SIZE>;

using Global_tcn = Atomic_random_array<tcn_node_t, trx_id_t, tcn_t>;

template bool iv_hash_insert(iv_hash_t<tcn_node_t, LRU_TCN_SIZE> *hash,
                             tcn_node_t *elem);

template bool iv_hash_insert(iv_hash_t<tcn_node_t, SESSION_TCN_SIZE> *hash,
                             tcn_node_t *elem);

bool trx_search_tcn(txn_rec_t *txn_rec, btr_pcur_t *pcur,
                    txn_lookup_t *txn_lookup);

void trx_cache_tcn(trx_id_t trx_id, txn_rec_t &txn_rec, const rec_t *rec,
                   const dict_index_t *index, const ulint *offsets,
                   btr_pcur_t *pcur);

void trx_cache_tcn(trx_t *trx);

/** Get the number of tcn entries according to the innodb_tcn_cache_size. */
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
