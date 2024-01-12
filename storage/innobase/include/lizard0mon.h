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

/** @file include/lizard0mon.h
  Lizard monitor metrics.

 Created 2020-06-03 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0mon_h
#define lizard0mon_h

#include "univ.i"

#include "srv0srv.h"

class THD;
struct SHOW_VAR;

namespace lizard {

enum txn_lookup_entry {
  TXN_DD_INDEX_VISIBLE,
  TXN_ONLINE_DDL,
  TXN_CONS_READ_SEES,
  TXN_GCN_READ_SEES,
  TXN_PURGE_SEES,
  TXN_MODIFY_CLEANOUT,
  TXN_LOCK_CONVERT,
  TXN_BUILD_PREV_VER_ASOF,
  TXN_BUILD_PREV_VER_NORMAL,
  TXN_UNDO_BUILD_REC,
  TXN_ENTRY_COUNT
};

}

struct lizard_var_t {
  /** txn undo rollback segment free list length */
  ulint txn_undo_log_free_list_len;

  /** txn undo log cached for reuse */
  ulint txn_undo_log_cached;

  /** txn undo log segment request count */
  ulint txn_undo_log_request;

  /** txn undo log segment reuse count */
  ulint txn_undo_log_reuse;

  /** txn undo log segment get from rseg free list count */
  ulint txn_undo_log_free_list_get;

  /** txn undo log segment put into rseg free list count */
  ulint txn_undo_log_free_list_put;

  /** txn undo log segment create count */
  ulint txn_undo_log_create;

  /** txn undo log hash count */
  ulint txn_undo_log_hash_element;

  /** txn undo log hash hit count */
  ulint txn_undo_log_hash_hit;

  /** txn undo log hash miss count */
  ulint txn_undo_log_hash_miss;

  ulint txn_undo_lost_page_miss_when_safe;

  ulint txn_undo_lost_magic_number_wrong;

  ulint txn_undo_lost_ext_flag_wrong;

  ulint txn_undo_lost_trx_id_mismatch;

  ulint txn_undo_lookup_by_uba;

  ulint cleanout_page_collect;

  ulint cleanout_record_clean;

  ulint cleanout_cursor_collect;

  ulint cleanout_cursor_restore_failed;

  ulint commit_cleanout_skip;
  ulint commit_cleanout_collects;
  ulint commit_cleanout_cleaned;

  /*Max commit gcn. */
  ulint current_gcn;

  /* Max purged gcn, snapshot gcn before that is too old to asof select. */
  ulint purged_gcn;

  ulint block_tcn_cache_hit;
  ulint block_tcn_cache_miss;
  ulint block_tcn_cache_evict;

  ulint session_tcn_cache_hit;
  ulint session_tcn_cache_miss;
  ulint session_tcn_cache_evict;

  ulint global_tcn_cache_hit;
  ulint global_tcn_cache_miss;
  ulint global_tcn_cache_evict;

  // page write/flush/load/evit of types
  ulint innodb_buffer_pool_write_req_undo;
  ulint innodb_buffer_pool_write_req_txn;
  ulint innodb_buffer_pool_write_req_index;
  ulint innodb_buffer_pool_write_req_sys;
  ulint innodb_buffer_pool_flush_undo;
  ulint innodb_buffer_pool_flush_txn;
  ulint innodb_buffer_pool_flush_index;
  ulint innodb_buffer_pool_flush_sys;
  ulint innodb_buffer_pool_read_undo;
  ulint innodb_buffer_pool_read_txn;
  ulint innodb_buffer_pool_read_index;
  ulint innodb_buffer_pool_read_sys;
  ulint innodb_buffer_pool_evit_undo;
  ulint innodb_buffer_pool_evit_txn;
  ulint innodb_buffer_pool_evit_index;
  ulint innodb_buffer_pool_evit_sys;

  // txn undo page hit
  ulint innodb_buffer_pool_txn_r_hit;
  ulint innodb_buffer_pool_txn_r_disk;
  ulint innodb_buffer_pool_txn_w_hit;
  ulint innodb_buffer_pool_txn_w_disk;

  // txn loopup entry
  ulint innodb_buffer_pool_txn_lookup[lizard::TXN_ENTRY_COUNT];

  // MONITOR_LRU_GET_FREE_SEARCH
  ulint innodb_buffer_pool_lru_get_free_search;
  // MONITOR_LRU_SEARCH_SCANNED_NUM_CALL
  ulint innodb_buffer_pool_lru_search_scans;
  // MONITOR_LRU_SEARCH_SCANNED
  ulint innodb_buffer_pool_lru_search_scanned;
  // MONITOR_LRU_SINGLE_FLUSH_SCANNED_NUM_CALL
  ulint innodb_buffer_pool_lru_single_flush_scans;
  // MONITOR_LRU_SINGLE_FLUSH_SCANNED
  ulint innodb_buffer_pool_lru_single_flush_scanned;
  // MONITOR_LRU_GET_FREE_LOOPS
  ulint innodb_buffer_pool_lru_get_free_loops;
  // MONITOR_LRU_GET_FREE_WAITS
  ulint innodb_buffer_pool_lru_get_free_waits;

  // MONITOR_LRU_BATCH_SCANNED_NUM_CALL
  ulint innodb_buffer_pool_lru_batch_scans;
  // MONITOR_LRU_BATCH_SCANNED
  ulint innodb_buffer_pool_lru_batch_scanned;
  // MONITOR_LRU_BATCH_EVICT_COUNT
  ulint innodb_buffer_pool_lru_batch_evits;
  // MONITOR_LRU_BATCH_EVICT_TOTAL_PAGE
  ulint innodb_buffer_pool_lru_batch_evited;

  // MONITOR_FLUSH_BATCH_SCANNED_NUM_CALL
  ulint innodb_buffer_pool_flu_batch_scans;
  // MONITOR_FLUSH_BATCH_SCANNED
  ulint innodb_buffer_pool_flu_batch_scanned;
  // MONITOR_FLUSH_BATCH_COUNT
  ulint innodb_buffer_pool_flu_batch_count;
  // MONITOR_FLUSH_BATCH_TOTAL_PAGE
  ulint innodb_buffer_pool_flu_batch_pages;

  // MONITOR_FLUSH_NEIGHBOR_COUNT
  ulint innodb_buffer_pool_flush_neighbor_count;
  // MONITOR_FLUSH_NEIGHBOR_TOTAL_PAGE
  ulint innodb_buffer_pool_flush_neighbor_pages;

  // MONITOR_LRU_BATCH_FLUSH_COUNT
  ulint innodb_buffer_pool_lru_flush_count;
  // MONITOR_FLUSH_SYNC_TOTAL_PAGE
  ulint innodb_buffer_pool_lru_flush_pages;
  // MONITOR_FLUSH_ADAPTIVE_COUNT
  ulint innodb_buffer_pool_flush_adapt_count;
  // MONITOR_FLUSH_ADAPTIVE_TOTAL_PAGE
  ulint innodb_buffer_pool_flush_adapt_pages;
  // MONITOR_FLUSH_SYNC_COUNT
  ulint innodb_buffer_pool_flush_sync_count;
  // MONITOR_FLUSH_SYNC_TOTAL_PAGE
  ulint innodb_buffer_pool_flush_sync_pages;
  // MONITOR_FLUSH_BACKGROUND_COUNT
  ulint innodb_buffer_pool_flush_background_count;
  // MONITOR_FLUSH_BACKGROUND_TOTAL_PAGE
  ulint innodb_buffer_pool_flush_background_pages;

  /** txn undo log segment put into rseg cached list */
  ulint txn_undo_log_recycle;

  /** Search commit snapshot through scn */
  ulint commit_snapshot_scn_search_hit;
  /** Search commit snapshot through gcn */
  ulint commit_snapshot_gcn_search_hit;
};

struct lizard_stats_t {
  typedef ib_counter_t<ulint, 64> ulint_ctr_64_t;
  typedef ib_counter_t<lsn_t, 1, single_indexer_t> lsn_ctr_1_t;
  typedef ib_counter_t<ulint, 1, single_indexer_t> ulint_ctr_1_t;
  typedef ib_counter_t<lint, 1, single_indexer_t> lint_ctr_1_t;
  typedef ib_counter_t<int64_t, 1, single_indexer_t> int64_ctr_1_t;

  /** txn undo log segment request count */
  ulint_ctr_1_t txn_undo_log_request;

  /** txn undo log segment reuse count */
  ulint_ctr_1_t txn_undo_log_reuse;

  /** txn undo log segment get from rseg free list count */
  ulint_ctr_1_t txn_undo_log_free_list_get;

  /** txn undo log segment put into rseg free list count */
  ulint_ctr_1_t txn_undo_log_free_list_put;

  /** txn undo log segment create count */
  ulint_ctr_1_t txn_undo_log_create;

  /** txn undo log hash count */
  ulint_ctr_1_t txn_undo_log_hash_element;

  /** txn undo log hash hit count */
  ulint_ctr_1_t txn_undo_log_hash_hit;

  /** txn undo log hash miss count */
  ulint_ctr_1_t txn_undo_log_hash_miss;

  /** txn undo lost when missing corresponding pages when cleanout safe mode */
  ulint_ctr_1_t txn_undo_lost_page_miss_when_safe;

  /** txn undo lost because magic number is wrong */
  ulint_ctr_1_t txn_undo_lost_magic_number_wrong;

  /** txn undo lost because ext flag is wrong */
  ulint_ctr_1_t txn_undo_lost_ext_flag_wrong;

  /** txn undo lost because trx_id is mismatch */
  ulint_ctr_1_t txn_undo_lost_trx_id_mismatch;

  /** lookup scn by uba */
  ulint_ctr_1_t txn_undo_lookup_by_uba;

  ulint_ctr_1_t cleanout_page_collect;

  ulint_ctr_1_t cleanout_record_clean;

  ulint_ctr_1_t cleanout_cursor_collect;

  ulint_ctr_1_t cleanout_cursor_restore_failed;

  ulint_ctr_1_t commit_cleanout_skip;
  ulint_ctr_1_t commit_cleanout_collects;
  ulint_ctr_1_t commit_cleanout_cleaned;

  ulint_ctr_1_t block_tcn_cache_hit;
  ulint_ctr_1_t block_tcn_cache_miss;
  ulint_ctr_1_t block_tcn_cache_evict;

  ulint_ctr_1_t global_tcn_cache_hit;
  ulint_ctr_1_t global_tcn_cache_miss;
  ulint_ctr_1_t global_tcn_cache_evict;

  ulint_ctr_1_t txn_undo_page_read_hit;
  ulint_ctr_1_t txn_undo_page_read_miss;
  ulint_ctr_1_t txn_undo_page_write_hit;
  ulint_ctr_1_t txn_undo_page_write_miss;

  ulint_ctr_1_t buf_pool_flush_undo;
  ulint_ctr_1_t buf_pool_flush_txn;
  ulint_ctr_1_t buf_pool_flush_index;
  ulint_ctr_1_t buf_pool_flush_sys;
  ulint_ctr_1_t buf_pool_read_undo;
  ulint_ctr_1_t buf_pool_read_txn;
  ulint_ctr_1_t buf_pool_read_index;
  ulint_ctr_1_t buf_pool_read_sys;

  ulint_ctr_1_t buf_pool_evit_undo;
  ulint_ctr_1_t buf_pool_evit_txn;
  ulint_ctr_1_t buf_pool_evit_index;
  ulint_ctr_1_t buf_pool_evit_sys;

  ulint_ctr_1_t buf_pool_write_req_undo;
  ulint_ctr_1_t buf_pool_write_req_txn;
  ulint_ctr_1_t buf_pool_write_req_index;
  ulint_ctr_1_t buf_pool_write_req_sys;

  ulint_ctr_1_t txn_lookup[lizard::TXN_ENTRY_COUNT];

  ulint_ctr_1_t txn_undo_log_recycle;

  /** Search commit snapshot through scn */
  ulint_ctr_1_t commit_snapshot_scn_search_hit;

  /** Search commit snapshot through gcn */
  ulint_ctr_1_t commit_snapshot_gcn_search_hit;
};

namespace lizard {

extern bool stat_enabled;

extern lizard_stats_t lizard_stats;

int show_lizard_vars(THD *thd, SHOW_VAR *var, char *buff);

/* Fetch txn undo page hit or miss statistics */
void txn_undo_page_hit_stat(bool hit, const buf_block_t *block, ulint rw_latch);

/* Physical io statistics for undo/index/sys pages */
void page_physical_io_stat(enum buf_io_fix io_type, const byte *frame,
                           ulint page_type);

/* Evit statistics for undo/index/sys pages */
void page_evit_stat(const buf_page_t *bpage);

/* Page flush requests for undo/index/sys pages */
void page_write_req_stat(const buf_block_t *block);

/* Txn lookup statistics */
void txn_lookup_stat(txn_lookup_entry entry);

}  // namespace lizard
#ifdef UNIV_DEBUG

#define BLOCK_TCN_CACHE_HIT                         \
  do {                                              \
    lizard::lizard_stats.block_tcn_cache_hit.inc(); \
  } while (0)

#define BLOCK_TCN_CACHE_MISS                         \
  do {                                               \
    lizard::lizard_stats.block_tcn_cache_miss.inc(); \
  } while (0)

#define BLOCK_TCN_CACHE_EVICT                         \
  do {                                                \
    lizard::lizard_stats.block_tcn_cache_evict.inc(); \
  } while (0)

#define GLOBAL_TCN_CACHE_HIT                         \
  do {                                               \
    lizard::lizard_stats.global_tcn_cache_hit.inc(); \
  } while (0)

#define GLOBAL_TCN_CACHE_MISS                         \
  do {                                                \
    lizard::lizard_stats.global_tcn_cache_miss.inc(); \
  } while (0)

#define GLOBAL_TCN_CACHE_EVICT                         \
  do {                                                 \
    lizard::lizard_stats.global_tcn_cache_evict.inc(); \
  } while (0)

#else

#define BLOCK_TCN_CACHE_HIT
#define BLOCK_TCN_CACHE_MISS
#define BLOCK_TCN_CACHE_EVICT
#define GLOBAL_TCN_CACHE_HIT
#define GLOBAL_TCN_CACHE_MISS
#define GLOBAL_TCN_CACHE_EVICT

#endif

#define LIZARD_MONITOR_INC_TXN_CACHED(NUMBER)             \
  do {                                                    \
    if (lizard::gcs != nullptr) {                         \
      lizard::gcs->txn_undo_log_cached.fetch_add(NUMBER); \
    }                                                     \
  } while (0)

#define LIZARD_MONITOR_DEC_TXN_CACHED(NUMBER)             \
  do {                                                    \
    if (lizard::gcs != nullptr) {                         \
      lizard::gcs->txn_undo_log_cached.fetch_sub(NUMBER); \
    }                                                     \
  } while (0)

#endif  // lizard0mon_h
