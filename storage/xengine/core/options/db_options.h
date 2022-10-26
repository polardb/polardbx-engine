/*
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <string>
#include <vector>

#include "xengine/options.h"

namespace xengine {
namespace common {

struct ImmutableDBOptions {
  ImmutableDBOptions();
  explicit ImmutableDBOptions(const DBOptions& options);

  void Dump() const;

  bool create_if_missing;
  bool create_missing_column_families;
  bool error_if_exists;
  bool paranoid_checks;
  util::Env* env;
  std::shared_ptr<util::RateLimiter> rate_limiter;
  std::shared_ptr<util::SstFileManager> sst_file_manager;
  util::InfoLogLevel info_log_level;
  int max_open_files;
  int max_file_opening_threads;
  std::shared_ptr<monitor::Statistics> statistics;
  bool use_fsync;
  std::vector<DbPath> db_paths;
  std::string db_log_dir;
  std::string wal_dir;
  uint32_t max_subcompactions;
  int max_background_flushes;
  size_t max_log_file_size;
  size_t log_file_time_to_roll;
  size_t keep_log_file_num;
  size_t recycle_log_file_num;
  uint64_t max_manifest_file_size;
  int table_cache_numshardbits;
  uint64_t wal_ttl_seconds;
  uint64_t wal_size_limit_mb;
  size_t manifest_preallocation_size;
  bool allow_mmap_reads;
  bool allow_mmap_writes;
  bool use_direct_reads;
  bool use_direct_io_for_flush_and_compaction;
  bool allow_fallocate;
  bool is_fd_close_on_exec;
  bool advise_random_on_open;
  size_t db_write_buffer_size;
  size_t db_total_write_buffer_size;
  std::shared_ptr<db::WriteBufferManager> write_buffer_manager;
  DBOptions::AccessHint access_hint_on_compaction_start;
  bool new_table_reader_for_compaction_inputs;
  size_t compaction_readahead_size;
  size_t random_access_max_buffer_size;
  size_t writable_file_max_buffer_size;
  bool use_adaptive_mutex;
  uint64_t bytes_per_sync;
  uint64_t wal_bytes_per_sync;
  std::vector<std::shared_ptr<EventListener>> listeners;
  bool enable_thread_tracking;
  bool allow_concurrent_memtable_write;
  bool enable_write_thread_adaptive_yield;
  uint64_t write_thread_max_yield_usec;
  uint64_t write_thread_slow_yield_usec;
  bool skip_stats_update_on_db_open;
  WALRecoveryMode wal_recovery_mode;
  bool enable_aio_wal_reader;
  bool parallel_wal_recovery;
  uint32_t parallel_recovery_thread_num;
  bool allow_2pc;
  std::shared_ptr<cache::RowCache> row_cache;
#ifndef ROCKSDB_LITE
  db::WalFilter* wal_filter;
#endif  // ROCKSDB_LITE
  bool fail_if_options_file_error;
  bool avoid_flush_during_recovery;
  uint64_t compaction_type;
  uint64_t compaction_mode;
  uint64_t cpu_compaction_thread_num;
  uint64_t fpga_compaction_thread_num;
  uint64_t fpga_device_id;
  uint64_t table_cache_size;
};

struct MutableDBOptions {
  MutableDBOptions();
  explicit MutableDBOptions(const MutableDBOptions& options) = default;
  explicit MutableDBOptions(const DBOptions& options);

  void Dump() const;

  int base_background_compactions;
  int max_background_compactions;
  int32_t filter_building_threads;
  int32_t filter_queue_stripes;
  bool avoid_flush_during_shutdown;
  uint64_t delayed_write_rate;
  uint64_t max_total_wal_size;
  uint64_t delete_obsolete_files_period_micros;
  unsigned int stats_dump_period_sec;
  bool dump_malloc_stats;

  uint64_t batch_group_slot_array_size;
  uint64_t batch_group_max_group_size;
  uint64_t batch_group_max_leader_wait_time_us;
  uint64_t concurrent_writable_file_buffer_num;
  uint64_t concurrent_writable_file_single_buffer_size;
  uint64_t concurrent_writable_file_buffer_switch_limit;
  bool use_direct_write_for_wal;
  bool query_trace_enable_count;
  bool query_trace_print_stats;
  uint64_t mutex_backtrace_threshold_ns;
  int max_background_dumps;
  uint64_t dump_memtable_limit_size; // close incremental checkpoint func when set 0
  bool auto_shrink_enabled;
  uint64_t max_free_extent_percent;
  uint64_t shrink_allocate_interval;
  uint64_t max_shrink_extent_count;
  uint64_t total_max_shrink_extent_count;
  uint64_t idle_tasks_schedule_time;
  uint64_t auto_shrink_schedule_interval;
  uint64_t estimate_cost_depth;
};

}  // namespace common
}  // namespace xengine
