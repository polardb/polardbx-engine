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

/** @file include/lizard0mon.cc
  Lizard monitor metrics.

 Created 2020-06-03 by Jianwei.zhao
 *******************************************************/

#include "mysql/status_var.h"

#include "lizard0gcs.h"
#include "lizard0mon.h"
#include "lizard0undo.h"

namespace lizard {

bool stat_enabled = false;

/** Global monitor metrics export status temporary address */
static lizard_var_t lizard_vars;

/** GLobal monitor metrics */
lizard_stats_t lizard_stats;

static void export_lizard_status(void) {
  if (gcs != nullptr) {
    lizard_vars.txn_undo_log_free_list_len = gcs->txn_undo_log_free_list_len;
    lizard_vars.txn_undo_log_cached = gcs->txn_undo_log_cached;
  } else {
    lizard_vars.txn_undo_log_free_list_len = 0;
    lizard_vars.txn_undo_log_cached = 0;
  }

  lizard_vars.txn_undo_log_request = lizard_stats.txn_undo_log_request;
  lizard_vars.txn_undo_log_reuse = lizard_stats.txn_undo_log_reuse;

  lizard_vars.txn_undo_log_free_list_get =
      lizard_stats.txn_undo_log_free_list_get;

  lizard_vars.txn_undo_log_free_list_put =
      lizard_stats.txn_undo_log_free_list_put;

  lizard_vars.txn_undo_log_create = lizard_stats.txn_undo_log_create;

  lizard_vars.txn_undo_log_hash_element =
      lizard_stats.txn_undo_log_hash_element;
  lizard_vars.txn_undo_log_hash_hit = lizard_stats.txn_undo_log_hash_hit;
  lizard_vars.txn_undo_log_hash_miss = lizard_stats.txn_undo_log_hash_miss;

  lizard_vars.txn_undo_lost_page_miss_when_safe =
      lizard_stats.txn_undo_lost_page_miss_when_safe;

  lizard_vars.txn_undo_lost_magic_number_wrong =
      lizard_stats.txn_undo_lost_magic_number_wrong;

  lizard_vars.txn_undo_lost_ext_flag_wrong =
      lizard_stats.txn_undo_lost_ext_flag_wrong;

  lizard_vars.txn_undo_lost_trx_id_mismatch =
      lizard_stats.txn_undo_lost_trx_id_mismatch;

  lizard_vars.txn_undo_lookup_by_uba = lizard_stats.txn_undo_lookup_by_uba;

  lizard_vars.cleanout_page_collect = lizard_stats.cleanout_page_collect;

  lizard_vars.cleanout_record_clean = lizard_stats.cleanout_record_clean;

  lizard_vars.cleanout_cursor_collect = lizard_stats.cleanout_cursor_collect;

  lizard_vars.cleanout_cursor_restore_failed =
      lizard_stats.cleanout_cursor_restore_failed;

  lizard_vars.commit_cleanout_skip = lizard_stats.commit_cleanout_skip;
  lizard_vars.commit_cleanout_collects = lizard_stats.commit_cleanout_collects;
  lizard_vars.commit_cleanout_cleaned = lizard_stats.commit_cleanout_cleaned;

  lizard_vars.current_gcn = gcs_load_gcn();

  lizard_vars.purged_gcn = gcs_get_purged_gcn();

  lizard_vars.block_tcn_cache_hit = lizard_stats.block_tcn_cache_hit;
  lizard_vars.block_tcn_cache_miss = lizard_stats.block_tcn_cache_miss;
  lizard_vars.block_tcn_cache_evict = lizard_stats.block_tcn_cache_evict;

  lizard_vars.global_tcn_cache_hit = lizard_stats.global_tcn_cache_hit;
  lizard_vars.global_tcn_cache_miss = lizard_stats.global_tcn_cache_miss;
  lizard_vars.global_tcn_cache_evict = lizard_stats.global_tcn_cache_evict;

  // page write/flush/load/evit of types
  lizard_vars.innodb_buffer_pool_write_req_undo =
      lizard_stats.buf_pool_write_req_undo;
  lizard_vars.innodb_buffer_pool_write_req_txn =
      lizard_stats.buf_pool_write_req_txn;
  lizard_vars.innodb_buffer_pool_write_req_index =
      lizard_stats.buf_pool_write_req_index;
  lizard_vars.innodb_buffer_pool_write_req_sys =
      lizard_stats.buf_pool_write_req_sys;
  lizard_vars.innodb_buffer_pool_flush_undo = lizard_stats.buf_pool_flush_undo;
  lizard_vars.innodb_buffer_pool_flush_txn = lizard_stats.buf_pool_flush_txn;
  lizard_vars.innodb_buffer_pool_flush_index =
      lizard_stats.buf_pool_flush_index;
  lizard_vars.innodb_buffer_pool_flush_sys = lizard_stats.buf_pool_flush_sys;
  lizard_vars.innodb_buffer_pool_read_undo = lizard_stats.buf_pool_read_undo;
  lizard_vars.innodb_buffer_pool_read_txn = lizard_stats.buf_pool_read_txn;
  lizard_vars.innodb_buffer_pool_read_index = lizard_stats.buf_pool_read_index;
  lizard_vars.innodb_buffer_pool_read_sys = lizard_stats.buf_pool_read_sys;
  lizard_vars.innodb_buffer_pool_evit_undo = lizard_stats.buf_pool_evit_undo;
  lizard_vars.innodb_buffer_pool_evit_txn = lizard_stats.buf_pool_evit_txn;
  lizard_vars.innodb_buffer_pool_evit_index = lizard_stats.buf_pool_evit_index;
  lizard_vars.innodb_buffer_pool_evit_sys = lizard_stats.buf_pool_evit_sys;

  // txn undo page hit
  lizard_vars.innodb_buffer_pool_txn_r_hit =
      lizard_stats.txn_undo_page_read_hit;
  lizard_vars.innodb_buffer_pool_txn_r_disk =
      lizard_stats.txn_undo_page_read_miss;
  lizard_vars.innodb_buffer_pool_txn_w_hit =
      lizard_stats.txn_undo_page_write_hit;
  lizard_vars.innodb_buffer_pool_txn_w_disk =
      lizard_stats.txn_undo_page_write_miss;

  // txn loopup entry
  for (size_t i = 0; i < lizard::TXN_ENTRY_COUNT; i++) {
    lizard_vars.innodb_buffer_pool_txn_lookup[i] = lizard_stats.txn_lookup[i];
  }

  // get free page
  lizard_vars.innodb_buffer_pool_lru_get_free_search =
      MONITOR_VALUE(MONITOR_LRU_GET_FREE_SEARCH);
  lizard_vars.innodb_buffer_pool_lru_search_scans =
      MONITOR_VALUE(MONITOR_LRU_SEARCH_SCANNED_NUM_CALL);
  lizard_vars.innodb_buffer_pool_lru_search_scanned =
      MONITOR_VALUE(MONITOR_LRU_SEARCH_SCANNED);
  lizard_vars.innodb_buffer_pool_lru_single_flush_scans =
      MONITOR_VALUE(MONITOR_LRU_SINGLE_FLUSH_SCANNED_NUM_CALL);
  lizard_vars.innodb_buffer_pool_lru_single_flush_scanned =
      MONITOR_VALUE(MONITOR_LRU_SINGLE_FLUSH_SCANNED);
  lizard_vars.innodb_buffer_pool_lru_get_free_loops =
      MONITOR_VALUE(MONITOR_LRU_GET_FREE_LOOPS);
  lizard_vars.innodb_buffer_pool_lru_get_free_waits =
      MONITOR_VALUE(MONITOR_LRU_GET_FREE_WAITS);

  // lru batch evits
  lizard_vars.innodb_buffer_pool_lru_batch_scans =
      MONITOR_VALUE(MONITOR_LRU_BATCH_SCANNED_NUM_CALL);
  lizard_vars.innodb_buffer_pool_lru_batch_scanned =
      MONITOR_VALUE(MONITOR_LRU_BATCH_SCANNED);
  lizard_vars.innodb_buffer_pool_lru_batch_evits =
      MONITOR_VALUE(MONITOR_LRU_BATCH_EVICT_COUNT);
  lizard_vars.innodb_buffer_pool_lru_batch_evited =
      MONITOR_VALUE(MONITOR_LRU_BATCH_EVICT_TOTAL_PAGE);

  // flush batch
  lizard_vars.innodb_buffer_pool_flu_batch_scans =
      MONITOR_VALUE(MONITOR_FLUSH_BATCH_SCANNED_NUM_CALL);
  lizard_vars.innodb_buffer_pool_flu_batch_scanned =
      MONITOR_VALUE(MONITOR_FLUSH_BATCH_SCANNED);
  lizard_vars.innodb_buffer_pool_flu_batch_count =
      MONITOR_VALUE(MONITOR_FLUSH_BATCH_COUNT);
  lizard_vars.innodb_buffer_pool_flu_batch_pages =
      MONITOR_VALUE(MONITOR_FLUSH_BATCH_TOTAL_PAGE);

  // flush neighbor
  lizard_vars.innodb_buffer_pool_flush_neighbor_count =
      MONITOR_VALUE(MONITOR_FLUSH_NEIGHBOR_COUNT);
  lizard_vars.innodb_buffer_pool_flush_neighbor_pages =
      MONITOR_VALUE(MONITOR_FLUSH_NEIGHBOR_TOTAL_PAGE);

  // adapt background flush
  lizard_vars.innodb_buffer_pool_lru_flush_count =
      MONITOR_VALUE(MONITOR_LRU_BATCH_FLUSH_COUNT);
  lizard_vars.innodb_buffer_pool_lru_flush_pages =
      MONITOR_VALUE(MONITOR_FLUSH_SYNC_TOTAL_PAGE);
  lizard_vars.innodb_buffer_pool_flush_adapt_count =
      MONITOR_VALUE(MONITOR_FLUSH_ADAPTIVE_COUNT);
  lizard_vars.innodb_buffer_pool_flush_adapt_pages =
      MONITOR_VALUE(MONITOR_FLUSH_ADAPTIVE_TOTAL_PAGE);
  lizard_vars.innodb_buffer_pool_flush_sync_count =
      MONITOR_VALUE(MONITOR_FLUSH_SYNC_COUNT);
  lizard_vars.innodb_buffer_pool_flush_sync_pages =
      MONITOR_VALUE(MONITOR_FLUSH_SYNC_TOTAL_PAGE);
  lizard_vars.innodb_buffer_pool_flush_background_count =
      MONITOR_VALUE(MONITOR_FLUSH_BACKGROUND_COUNT);
  lizard_vars.innodb_buffer_pool_flush_background_pages =
      MONITOR_VALUE(MONITOR_FLUSH_BACKGROUND_TOTAL_PAGE);

  lizard_vars.txn_undo_log_recycle = lizard_stats.txn_undo_log_recycle;

  lizard_vars.commit_snapshot_scn_search_hit =
      lizard_stats.commit_snapshot_scn_search_hit;

  lizard_vars.commit_snapshot_gcn_search_hit =
      lizard_stats.commit_snapshot_gcn_search_hit;
}

static SHOW_VAR lizard_status_variables[] = {
    {"txn_undo_log_free_list_length",
     (char *)&lizard_vars.txn_undo_log_free_list_len, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_cached", (char *)&lizard_vars.txn_undo_log_cached, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_request", (char *)&lizard_vars.txn_undo_log_request,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_reuse", (char *)&lizard_vars.txn_undo_log_reuse, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_free_list_get",
     (char *)&lizard_vars.txn_undo_log_free_list_get, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_free_list_put",
     (char *)&lizard_vars.txn_undo_log_free_list_put, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_create", (char *)&lizard_vars.txn_undo_log_create, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_hash_element",
     (char *)&lizard_vars.txn_undo_log_hash_element, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_hash_hit", (char *)&lizard_vars.txn_undo_log_hash_hit,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_hash_miss", (char *)&lizard_vars.txn_undo_log_hash_miss,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"txn_undo_lost_page_miss_when_safe",
     (char *)&lizard_vars.txn_undo_lost_page_miss_when_safe, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_lost_magic_number_wrong",
     (char *)&lizard_vars.txn_undo_lost_magic_number_wrong, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_lost_ext_flag_wrong",
     (char *)&lizard_vars.txn_undo_lost_ext_flag_wrong, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_lost_trx_id_mismatch",
     (char *)&lizard_vars.txn_undo_lost_trx_id_mismatch, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_lookup_by_uba", (char *)&lizard_vars.txn_undo_lookup_by_uba,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"cleanout_page_collect", (char *)&lizard_vars.cleanout_page_collect,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"cleanout_record_clean", (char *)&lizard_vars.cleanout_record_clean,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"cleanout_cursor_collect", (char *)&lizard_vars.cleanout_cursor_collect,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"cleanout_cursor_restore_failed",
     (char *)&lizard_vars.cleanout_cursor_restore_failed, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"commit_cleanout_skip", (char *)&lizard_vars.commit_cleanout_skip,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"commit_cleanout_collects", (char *)&lizard_vars.commit_cleanout_collects,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"commit_cleanout_cleaned", (char *)&lizard_vars.commit_cleanout_cleaned,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"undo_retention_stats", Undo_retention::status, SHOW_CHAR,
     SHOW_SCOPE_GLOBAL},

    {"current_gcn", (char *)&lizard_vars.current_gcn, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"purged_gcn", (char *)&lizard_vars.purged_gcn, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"block_tcn_cache_hit", (char *)&lizard_vars.block_tcn_cache_hit, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"block_tcn_cache_miss", (char *)&lizard_vars.block_tcn_cache_miss,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"block_tcn_cache_evict", (char *)&lizard_vars.block_tcn_cache_evict,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"global_tcn_cache_hit", (char *)&lizard_vars.global_tcn_cache_hit,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"global_tcn_cache_miss", (char *)&lizard_vars.global_tcn_cache_miss,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"global_tcn_cache_evict", (char *)&lizard_vars.global_tcn_cache_evict,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"buffer_pool_write_req_undo",
     (char *)&lizard_vars.innodb_buffer_pool_write_req_undo, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_write_req_txn",
     (char *)&lizard_vars.innodb_buffer_pool_write_req_txn, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_write_req_index",
     (char *)&lizard_vars.innodb_buffer_pool_write_req_index, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_write_req_sys",
     (char *)&lizard_vars.innodb_buffer_pool_write_req_sys, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_pages_read_undo",
     (char *)&lizard_vars.innodb_buffer_pool_read_undo, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_pages_read_txn",
     (char *)&lizard_vars.innodb_buffer_pool_read_txn, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_pages_read_index",
     (char *)&lizard_vars.innodb_buffer_pool_read_index, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_pages_read_sys",
     (char *)&lizard_vars.innodb_buffer_pool_read_sys, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_pages_flushed_undo",
     (char *)&lizard_vars.innodb_buffer_pool_flush_undo, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_pages_flushed_txn",
     (char *)&lizard_vars.innodb_buffer_pool_flush_txn, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_pages_flushed_index",
     (char *)&lizard_vars.innodb_buffer_pool_flush_index, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_pages_flushed_sys",
     (char *)&lizard_vars.innodb_buffer_pool_flush_sys, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_pages_evit_undo",
     (char *)&lizard_vars.innodb_buffer_pool_evit_undo, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_pages_evit_txn",
     (char *)&lizard_vars.innodb_buffer_pool_evit_txn, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_pages_evit_index",
     (char *)&lizard_vars.innodb_buffer_pool_evit_index, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_pages_evit_sys",
     (char *)&lizard_vars.innodb_buffer_pool_evit_sys, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_txn_read_hit",
     (char *)&lizard_vars.innodb_buffer_pool_txn_r_hit, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_txn_read_disk",
     (char *)&lizard_vars.innodb_buffer_pool_txn_r_disk, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_txn_write_hit",
     (char *)&lizard_vars.innodb_buffer_pool_txn_w_hit, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_txn_write_disk",
     (char *)&lizard_vars.innodb_buffer_pool_txn_w_disk, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_txn_lookup_dd_index",
     (char *)&lizard_vars
         .innodb_buffer_pool_txn_lookup[lizard::TXN_DD_INDEX_VISIBLE],
     SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"buffer_pool_txn_lookup_online_ddl",
     (char *)&lizard_vars.innodb_buffer_pool_txn_lookup[lizard::TXN_ONLINE_DDL],
     SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"buffer_pool_txn_lookup_cons_read_sees",
     (char *)&lizard_vars
         .innodb_buffer_pool_txn_lookup[lizard::TXN_CONS_READ_SEES],
     SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"buffer_pool_txn_lookup_gcn_read_sees",
     (char *)&lizard_vars
         .innodb_buffer_pool_txn_lookup[lizard::TXN_GCN_READ_SEES],
     SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"buffer_pool_txn_lookup_purge_sees",
     (char *)&lizard_vars.innodb_buffer_pool_txn_lookup[lizard::TXN_PURGE_SEES],
     SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"buffer_pool_txn_lookup_modify_cleanout",
     (char *)&lizard_vars
         .innodb_buffer_pool_txn_lookup[lizard::TXN_MODIFY_CLEANOUT],
     SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"buffer_pool_txn_lookup_lock_convert",
     (char *)&lizard_vars
         .innodb_buffer_pool_txn_lookup[lizard::TXN_LOCK_CONVERT],
     SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"buffer_pool_txn_lookup_pre_ver_asof",
     (char *)&lizard_vars
         .innodb_buffer_pool_txn_lookup[lizard::TXN_BUILD_PREV_VER_ASOF],
     SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"buffer_pool_txn_lookup_pre_ver_normal",
     (char *)&lizard_vars
         .innodb_buffer_pool_txn_lookup[lizard::TXN_BUILD_PREV_VER_NORMAL],
     SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"buffer_pool_txn_lookup_undo_build_rec",
     (char *)&lizard_vars
         .innodb_buffer_pool_txn_lookup[lizard::TXN_UNDO_BUILD_REC],
     SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"buffer_pool_lru_get_free_search",
     (char *)&lizard_vars.innodb_buffer_pool_lru_get_free_search, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_lru_get_free_loops",
     (char *)&lizard_vars.innodb_buffer_pool_lru_get_free_loops, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_lru_get_free_waits",
     (char *)&lizard_vars.innodb_buffer_pool_lru_get_free_waits, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_lru_search_scans",
     (char *)&lizard_vars.innodb_buffer_pool_lru_search_scans, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_lru_search_scanned",
     (char *)&lizard_vars.innodb_buffer_pool_lru_search_scanned, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_lru_single_flush_scans",
     (char *)&lizard_vars.innodb_buffer_pool_lru_single_flush_scans, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_lru_single_flush_scanned",
     (char *)&lizard_vars.innodb_buffer_pool_lru_single_flush_scanned,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"buffer_pool_lru_batch_scans",
     (char *)&lizard_vars.innodb_buffer_pool_lru_batch_scans, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_lru_batch_scanned",
     (char *)&lizard_vars.innodb_buffer_pool_lru_batch_scanned, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_lru_batch_evits",
     (char *)&lizard_vars.innodb_buffer_pool_lru_batch_evits, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_lru_batch_evited",
     (char *)&lizard_vars.innodb_buffer_pool_lru_batch_evited, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_flush_batch_scans",
     (char *)&lizard_vars.innodb_buffer_pool_flu_batch_scans, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_flush_batch_scanned",
     (char *)&lizard_vars.innodb_buffer_pool_flu_batch_scanned, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_flush_batch_count",
     (char *)&lizard_vars.innodb_buffer_pool_flu_batch_count, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_flush_batch_pages",
     (char *)&lizard_vars.innodb_buffer_pool_flu_batch_pages, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_flush_neighbor_count",
     (char *)&lizard_vars.innodb_buffer_pool_flush_neighbor_count, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_flush_neighbor_pages",
     (char *)&lizard_vars.innodb_buffer_pool_flush_neighbor_pages, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_lru_flush_count",
     (char *)&lizard_vars.innodb_buffer_pool_lru_flush_count, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_lru_flush_pages",
     (char *)&lizard_vars.innodb_buffer_pool_lru_flush_pages, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_flush_adapt_count",
     (char *)&lizard_vars.innodb_buffer_pool_flush_adapt_count, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_flush_adapt_pages",
     (char *)&lizard_vars.innodb_buffer_pool_flush_adapt_pages, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_flush_sync_count",
     (char *)&lizard_vars.innodb_buffer_pool_flush_sync_count, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_flush_sync_pages",
     (char *)&lizard_vars.innodb_buffer_pool_flush_sync_pages, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_flush_background_count",
     (char *)&lizard_vars.innodb_buffer_pool_flush_background_count, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"buffer_pool_flush_background_pages",
     (char *)&lizard_vars.innodb_buffer_pool_flush_background_pages, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_recycle", (char *)&lizard_vars.txn_undo_log_recycle,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"commit_snapshot_scn_search_hit",
     (char *)&lizard_vars.commit_snapshot_scn_search_hit, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"commit_snapshot_gcn_search_hit",
     (char *)&lizard_vars.commit_snapshot_gcn_search_hit, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {NullS, NullS, SHOW_LONG, SHOW_SCOPE_GLOBAL}};

int show_lizard_vars(THD *thd, SHOW_VAR *var, char *buff) {
  export_lizard_status();

  var->type = SHOW_ARRAY;
  var->value = (char *)&lizard_status_variables;
  var->scope = SHOW_SCOPE_GLOBAL;

  return 0;
}

/* Fetch txn undo page hit or miss statistics */
void txn_undo_page_hit_stat(bool hit, const buf_block_t *block,
                            ulint rw_latch) {
  if (!stat_enabled) return;

  auto bpage = &block->page;
  auto frame = bpage->zip.data != NULL ? bpage->zip.data : block->frame;
  auto page_type = fil_page_get_type(frame);

  if (page_type == FIL_PAGE_UNDO_LOG) {
    ut_ad(bpage->zip.data == NULL);

    auto undo_type =
        mach_read_from_2(frame + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE);

    if (undo_type == TRX_UNDO_TXN) {
      switch (rw_latch) {
        case RW_X_LATCH:  // called in txn_undo_get_free/trx_undo_reuse_cached
        case RW_SX_LATCH:
          if (hit)
            lizard_stats.txn_undo_page_write_hit.inc();
          else
            lizard_stats.txn_undo_page_write_miss.inc();
          break;
        case RW_S_LATCH:  // called in txn_undo_hdr_lookup_loose
          if (hit)
            lizard_stats.txn_undo_page_read_hit.inc();
          else
            lizard_stats.txn_undo_page_read_miss.inc();
          break;
        default:  // no other cases now
          ut_ad(0);
          break;
      }
    }
  }
}

enum stat_page_type {
  STAT_UNDO = 0,
  STAT_TXN,
  STAT_INDEX,
  STAT_SYS,
  STAT_COUNT
};

static stat_page_type get_stat_page_type(const byte *frame, ulint page_type) {
  if (page_type == FIL_PAGE_UNDO_LOG) {
    auto undo_type =
        mach_read_from_2(frame + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE);
    if (undo_type != TRX_UNDO_TXN) {
      return STAT_UNDO;
    } else {
      return STAT_TXN;
    }
  } else if (fil_page_type_is_index(page_type)) {
    return STAT_INDEX;
  } else if (fil_page_type_is_sys(page_type)) {
    return STAT_SYS;
  } else {
    return STAT_COUNT;
  }
}

/* Physical io statistics for undo/index/sys pages */
void page_physical_io_stat(enum buf_io_fix io_type, const byte *frame,
                           ulint page_type) {
  if (!stat_enabled) return;

  static lizard_stats_t::ulint_ctr_1_t *stat[] = {
      &lizard_stats.buf_pool_flush_undo,  &lizard_stats.buf_pool_flush_txn,
      &lizard_stats.buf_pool_flush_index, &lizard_stats.buf_pool_flush_sys,
      &lizard_stats.buf_pool_read_undo,   &lizard_stats.buf_pool_read_txn,
      &lizard_stats.buf_pool_read_index,  &lizard_stats.buf_pool_read_sys};

  auto type = get_stat_page_type(frame, page_type);

  if (type < STAT_COUNT) {
    int indx = type;

    if (io_type == BUF_IO_READ) {
      indx += STAT_COUNT;
    }

    stat[indx]->inc();
  }
}

/* Evit statistics for undo/index/sys pages */
void page_evit_stat(const buf_page_t *bpage) {
  if (!stat_enabled) return;

  static lizard_stats_t::ulint_ctr_1_t *stat[] = {
      &lizard_stats.buf_pool_evit_undo, &lizard_stats.buf_pool_evit_txn,
      &lizard_stats.buf_pool_evit_index, &lizard_stats.buf_pool_evit_sys};

  auto frame = bpage->zip.data != NULL ? bpage->zip.data
                                       : ((const buf_block_t *)bpage)->frame;
  auto page_type = fil_page_get_type(frame);

  auto type = get_stat_page_type(frame, page_type);

  if (type < STAT_COUNT) {
    stat[type]->inc();
  }
}

/* Page flush requests for undo/index/sys pages */
void page_write_req_stat(const buf_block_t *block) {
  if (!stat_enabled) return;

  static lizard_stats_t::ulint_ctr_1_t *stat[] = {
      &lizard_stats.buf_pool_write_req_undo,
      &lizard_stats.buf_pool_write_req_txn,
      &lizard_stats.buf_pool_write_req_index,
      &lizard_stats.buf_pool_write_req_sys};

  auto type = get_stat_page_type(block->frame, block->get_page_type());

  if (type < STAT_COUNT) {
    stat[type]->inc();
  }
}

/* Txn lookup statistics */
void txn_lookup_stat(txn_lookup_entry entry) {
  if (!stat_enabled) return;

  ut_ad(entry < TXN_ENTRY_COUNT);

  lizard_stats.txn_lookup[entry].inc();
}

}  // namespace lizard
