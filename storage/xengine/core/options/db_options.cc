/*
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "options/db_options.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include "cache/row_cache.h"
#include "logger/logger.h"
#include "port/port.h"
#include "xengine/cache.h"
#include "xengine/env.h"
#include "xengine/sst_file_manager.h"
#include "xengine/wal_filter.h"

using namespace xengine;
using namespace util;

namespace xengine {
namespace common {

ImmutableDBOptions::ImmutableDBOptions() : ImmutableDBOptions(Options()) {}

ImmutableDBOptions::ImmutableDBOptions(const DBOptions &options)
    : create_if_missing(options.create_if_missing),
      create_missing_column_families(options.create_missing_column_families),
      error_if_exists(options.error_if_exists),
      paranoid_checks(options.paranoid_checks),
      env(options.env),
      rate_limiter(options.rate_limiter),
      sst_file_manager(options.sst_file_manager),
      info_log_level(options.info_log_level),
      max_open_files(options.max_open_files),
      max_file_opening_threads(options.max_file_opening_threads),
      statistics(options.statistics),
      use_fsync(options.use_fsync),
      db_paths(options.db_paths),
      db_log_dir(options.db_log_dir),
      wal_dir(options.wal_dir),
      max_subcompactions(options.max_subcompactions),
      max_background_flushes(options.max_background_flushes),
      max_log_file_size(options.max_log_file_size),
      log_file_time_to_roll(options.log_file_time_to_roll),
      keep_log_file_num(options.keep_log_file_num),
      recycle_log_file_num(options.recycle_log_file_num),
      max_manifest_file_size(options.max_manifest_file_size),
      table_cache_numshardbits(options.table_cache_numshardbits),
      wal_ttl_seconds(options.WAL_ttl_seconds),
      wal_size_limit_mb(options.WAL_size_limit_MB),
      manifest_preallocation_size(options.manifest_preallocation_size),
      allow_mmap_reads(options.allow_mmap_reads),
      allow_mmap_writes(options.allow_mmap_writes),
      use_direct_reads(options.use_direct_reads),
      use_direct_io_for_flush_and_compaction(
          options.use_direct_io_for_flush_and_compaction),
      allow_fallocate(options.allow_fallocate),
      is_fd_close_on_exec(options.is_fd_close_on_exec),
      advise_random_on_open(options.advise_random_on_open),
      db_write_buffer_size(options.db_write_buffer_size),
      db_total_write_buffer_size(options.db_total_write_buffer_size),
      write_buffer_manager(options.write_buffer_manager),
      access_hint_on_compaction_start(options.access_hint_on_compaction_start),
      new_table_reader_for_compaction_inputs(
          options.new_table_reader_for_compaction_inputs),
      compaction_readahead_size(options.compaction_readahead_size),
      random_access_max_buffer_size(options.random_access_max_buffer_size),
      writable_file_max_buffer_size(options.writable_file_max_buffer_size),
      use_adaptive_mutex(options.use_adaptive_mutex),
      bytes_per_sync(options.bytes_per_sync),
      wal_bytes_per_sync(options.wal_bytes_per_sync),
      listeners(options.listeners),
      enable_thread_tracking(options.enable_thread_tracking),
      allow_concurrent_memtable_write(options.allow_concurrent_memtable_write),
      enable_write_thread_adaptive_yield(
          options.enable_write_thread_adaptive_yield),
      write_thread_max_yield_usec(options.write_thread_max_yield_usec),
      write_thread_slow_yield_usec(options.write_thread_slow_yield_usec),
      skip_stats_update_on_db_open(options.skip_stats_update_on_db_open),
      wal_recovery_mode(options.wal_recovery_mode),
      enable_aio_wal_reader(options.enable_aio_wal_reader),
      parallel_wal_recovery(options.parallel_wal_recovery),
      parallel_recovery_thread_num(options.parallel_recovery_thread_num),
      allow_2pc(options.allow_2pc),
      row_cache(options.row_cache),
#ifndef ROCKSDB_LITE
      wal_filter(options.wal_filter),
#endif  // ROCKSDB_LITE
      fail_if_options_file_error(options.fail_if_options_file_error),
      avoid_flush_during_recovery(options.avoid_flush_during_recovery),
      compaction_type(options.compaction_type),
      compaction_mode(options.compaction_mode),
      cpu_compaction_thread_num(options.cpu_compaction_thread_num),
      fpga_compaction_thread_num(options.fpga_compaction_thread_num),
      fpga_device_id(options.fpga_device_id),
      table_cache_size(options.table_cache_size) {
}

void ImmutableDBOptions::Dump() const {
  __XENGINE_LOG(INFO, "                        Options.error_if_exists: %d",
                   error_if_exists);
  __XENGINE_LOG(INFO, "                      Options.create_if_missing: %d",
                   create_if_missing);
  __XENGINE_LOG(INFO, "                        Options.paranoid_checks: %d",
                   paranoid_checks);
  __XENGINE_LOG(INFO, "                                    Options.env: %p",
                   env);
  __XENGINE_LOG(INFO, "                         Options.max_open_files: %d",
                   max_open_files);
  __XENGINE_LOG(INFO, "               Options.max_file_opening_threads: %d",
                   max_file_opening_threads);
  __XENGINE_LOG(INFO, "                              Options.use_fsync: %d",
                   use_fsync);
  __XENGINE_LOG(
      INFO, "                      Options.max_log_file_size: %" ROCKSDB_PRIszt,
      max_log_file_size);
  __XENGINE_LOG(INFO,
                   "                 Options.max_manifest_file_size: %" PRIu64,
                   max_manifest_file_size);
  __XENGINE_LOG(
      INFO, "                  Options.log_file_time_to_roll: %" ROCKSDB_PRIszt,
      log_file_time_to_roll);
  __XENGINE_LOG(
      INFO, "                      Options.keep_log_file_num: %" ROCKSDB_PRIszt,
      keep_log_file_num);
  __XENGINE_LOG(
      INFO, "                   Options.recycle_log_file_num: %" ROCKSDB_PRIszt,
      recycle_log_file_num);
  __XENGINE_LOG(INFO, "                        Options.allow_fallocate: %d",
                   allow_fallocate);
  __XENGINE_LOG(INFO, "                       Options.allow_mmap_reads: %d",
                   allow_mmap_reads);
  __XENGINE_LOG(INFO, "                      Options.allow_mmap_writes: %d",
                   allow_mmap_writes);
  __XENGINE_LOG(INFO, "                       Options.use_direct_reads: %d",
                   use_direct_reads);
  __XENGINE_LOG(INFO,
                   "                       "
                   "Options.use_direct_io_for_flush_and_compaction: %d",
                   use_direct_io_for_flush_and_compaction);
  __XENGINE_LOG(INFO, "         Options.create_missing_column_families: %d",
                   create_missing_column_families);
  __XENGINE_LOG(INFO, "                             Options.db_log_dir: %s",
                   db_log_dir.c_str());
  __XENGINE_LOG(INFO, "                                Options.wal_dir: %s",
                   wal_dir.c_str());
  __XENGINE_LOG(INFO, "               Options.table_cache_numshardbits: %d",
                   table_cache_numshardbits);
  __XENGINE_LOG(INFO,
                   "                     Options.max_subcompactions: %" PRIu32,
                   max_subcompactions);
  __XENGINE_LOG(INFO, "                 Options.max_background_flushes: %d",
                   max_background_flushes);
  __XENGINE_LOG(INFO,
                   "                        Options.WAL_ttl_seconds: %" PRIu64,
                   wal_ttl_seconds);
  __XENGINE_LOG(INFO,
                   "                      Options.WAL_size_limit_MB: %" PRIu64,
                   wal_size_limit_mb);
  __XENGINE_LOG(
      INFO, "            Options.manifest_preallocation_size: %" ROCKSDB_PRIszt,
      manifest_preallocation_size);
  __XENGINE_LOG(INFO, "                    Options.is_fd_close_on_exec: %d",
                   is_fd_close_on_exec);
  __XENGINE_LOG(INFO, "                  Options.advise_random_on_open: %d",
                   advise_random_on_open);
  __XENGINE_LOG(
      INFO, "                   Options.db_write_buffer_size: %" ROCKSDB_PRIszt,
      db_write_buffer_size);
  __XENGINE_LOG(
      INFO, "             Options.db_total_write_buffer_size: %" ROCKSDB_PRIszt,
      db_total_write_buffer_size);
  __XENGINE_LOG(INFO, "        Options.access_hint_on_compaction_start: %d",
                   static_cast<int>(access_hint_on_compaction_start));
  __XENGINE_LOG(INFO, " Options.new_table_reader_for_compaction_inputs: %d",
                   new_table_reader_for_compaction_inputs);
  __XENGINE_LOG(
      INFO, "              Options.compaction_readahead_size: %" ROCKSDB_PRIszt,
      compaction_readahead_size);
  __XENGINE_LOG(
      INFO, "          Options.random_access_max_buffer_size: %" ROCKSDB_PRIszt,
      random_access_max_buffer_size);
  __XENGINE_LOG(
      INFO, "          Options.writable_file_max_buffer_size: %" ROCKSDB_PRIszt,
      writable_file_max_buffer_size);
  __XENGINE_LOG(INFO, "                     Options.use_adaptive_mutex: %d",
                   use_adaptive_mutex);
  __XENGINE_LOG(INFO, "                           Options.rate_limiter: %p",
                   rate_limiter.get());
  __XENGINE_LOG(
      INFO, "    Options.sst_file_manager.rate_bytes_per_sec: %" PRIi64,
      sst_file_manager ? sst_file_manager->GetDeleteRateBytesPerSecond() : 0);
  __XENGINE_LOG(INFO,
                   "                         Options.bytes_per_sync: %" PRIu64,
                   bytes_per_sync);
  __XENGINE_LOG(INFO,
                   "                     Options.wal_bytes_per_sync: %" PRIu64,
                   wal_bytes_per_sync);
  __XENGINE_LOG(INFO, "                      Options.wal_recovery_mode: %d",
                   wal_recovery_mode);
  __XENGINE_LOG(INFO, "                      Options.enable_aio_wal_reader: %d",
                   enable_aio_wal_reader);
  __XENGINE_LOG(INFO, "                      Options.parallel_wal_recovery: %d",
                   parallel_wal_recovery);
  __XENGINE_LOG(INFO, "                      Options.parallel_recovery_thread_num: %d",
                   parallel_recovery_thread_num);
  __XENGINE_LOG(INFO, "                 Options.enable_thread_tracking: %d",
                   enable_thread_tracking);
  __XENGINE_LOG(INFO, "        Options.allow_concurrent_memtable_write: %d",
                   allow_concurrent_memtable_write);
  __XENGINE_LOG(INFO, "     Options.enable_write_thread_adaptive_yield: %d",
                   enable_write_thread_adaptive_yield);
  __XENGINE_LOG(INFO,
                   "            Options.write_thread_max_yield_usec: %" PRIu64,
                   write_thread_max_yield_usec);
  __XENGINE_LOG(INFO,
                   "           Options.write_thread_slow_yield_usec: %" PRIu64,
                   write_thread_slow_yield_usec);
  if (row_cache) {
    __XENGINE_LOG(
        INFO, "                              Options.row_cache: %" PRIu64,
        row_cache->GetCapacity());
  } else {
    __XENGINE_LOG(INFO,
                     "                              Options.row_cache: None");
  }
#ifndef ROCKSDB_LITE
  __XENGINE_LOG(INFO, "                             Options.wal_filter: %s",
                   wal_filter ? wal_filter->Name() : "None");
#endif  // ROCKDB_LITE
  __XENGINE_LOG(INFO, "            Options.avoid_flush_during_recovery: %d",
                   avoid_flush_during_recovery);
  __XENGINE_LOG(INFO, "                        Options.compaction_type: %s",
                   compaction_type == 0 ? "Stream" : "Minor for FPGA");
  __XENGINE_LOG(INFO, "                        Options.compaction_mode: %d",
                   compaction_mode);
  __XENGINE_LOG(INFO, "              Options.cpu_compaction_thread_num: %d",
                   cpu_compaction_thread_num);
  __XENGINE_LOG(INFO, "             Options.fpga_compaction_thread_num: %d",
                   fpga_compaction_thread_num);
  __XENGINE_LOG(INFO, "                         Options.fpga_device_id: %d",
                   fpga_device_id);
  __XENGINE_LOG(INFO, "                       Options.table_cache_size: %d",
                   table_cache_size);
}

MutableDBOptions::MutableDBOptions()
    : base_background_compactions(1),
      max_background_compactions(1),
      filter_building_threads(2),
      filter_queue_stripes(16),
      avoid_flush_during_shutdown(false),
      delayed_write_rate(2 * 1024U * 1024U),
      max_total_wal_size(0),
      delete_obsolete_files_period_micros(6ULL * 60 * 60 * 1000000),
      stats_dump_period_sec(600),
      batch_group_slot_array_size(5),
      batch_group_max_group_size(80),
      batch_group_max_leader_wait_time_us(200),
      concurrent_writable_file_buffer_num(4),
      concurrent_writable_file_single_buffer_size(4 * 1024U * 1024U),
      concurrent_writable_file_buffer_switch_limit(512 * 1024U),
      use_direct_write_for_wal(true),
      query_trace_enable_count(true),
      query_trace_print_stats(true),
      mutex_backtrace_threshold_ns(100000),
      max_background_dumps(3),
      dump_memtable_limit_size(64 * 1024 * 1024),
      auto_shrink_enabled(true),
      max_free_extent_percent(10),
      shrink_allocate_interval(60 * 60),
      max_shrink_extent_count(512),
      total_max_shrink_extent_count(15 * 512),
      idle_tasks_schedule_time(60),
      auto_shrink_schedule_interval(60 * 60),
      estimate_cost_depth(0)
  {
  }

MutableDBOptions::MutableDBOptions(const DBOptions& options)
    : base_background_compactions(options.base_background_compactions),
      max_background_compactions(options.max_background_compactions),
      filter_building_threads(options.filter_building_threads),
      filter_queue_stripes(options.filter_queue_stripes),
      avoid_flush_during_shutdown(options.avoid_flush_during_shutdown),
      delayed_write_rate(options.delayed_write_rate),
      max_total_wal_size(options.max_total_wal_size),
      delete_obsolete_files_period_micros(
          options.delete_obsolete_files_period_micros),
      stats_dump_period_sec(options.stats_dump_period_sec),
      dump_malloc_stats(options.dump_malloc_stats),
      batch_group_slot_array_size(options.batch_group_slot_array_size),
      batch_group_max_group_size(options.batch_group_max_group_size),
      batch_group_max_leader_wait_time_us(
          options.batch_group_max_leader_wait_time_us),
      concurrent_writable_file_buffer_num(
          options.concurrent_writable_file_buffer_num),
      concurrent_writable_file_single_buffer_size(
          options.concurrent_writable_file_single_buffer_size),
      concurrent_writable_file_buffer_switch_limit(
          options.concurrent_writable_file_buffer_switch_limit),
      use_direct_write_for_wal(options.use_direct_write_for_wal),
      query_trace_enable_count(options.query_trace_enable_count),
      query_trace_print_stats(options.query_trace_print_stats),
      mutex_backtrace_threshold_ns(options.mutex_backtrace_threshold_ns),
      max_background_dumps(options.max_background_dumps),
      dump_memtable_limit_size(options.dump_memtable_limit_size),
      auto_shrink_enabled(options.auto_shrink_enabled),
      max_free_extent_percent(options.max_free_extent_percent),
      shrink_allocate_interval(options.shrink_allocate_interval),
      max_shrink_extent_count(options.max_shrink_extent_count),
      total_max_shrink_extent_count(options.total_max_shrink_extent_count),
      idle_tasks_schedule_time(options.idle_tasks_schedule_time),
      auto_shrink_schedule_interval(options.auto_shrink_schedule_interval),
      estimate_cost_depth(options.estimate_cost_depth) {}
void MutableDBOptions::Dump() const {
  __XENGINE_LOG(INFO, "            Options.base_background_compactions: %d",
                   base_background_compactions);
  __XENGINE_LOG(INFO, "             Options.max_background_compactions: %d",
                   max_background_compactions);
  __XENGINE_LOG(INFO, "            Options.avoid_flush_during_shutdown: %d",
                   avoid_flush_during_shutdown);
  __XENGINE_LOG(INFO,
                "                     Options.delayed_write_rate: %" PRIu64,
                delayed_write_rate);
  __XENGINE_LOG(INFO,
                "                     Options.max_total_wal_size: %" PRIu64,
                max_total_wal_size);
  __XENGINE_LOG(INFO,
                "    Options.delete_obsolete_files_period_micros: %" PRIu64,
                delete_obsolete_files_period_micros);
  __XENGINE_LOG(INFO, "                  Options.stats_dump_period_sec: %u",
                   stats_dump_period_sec);
  __XENGINE_LOG(INFO,
                "            Options.batch_group_slot_array_size: %u",
                batch_group_slot_array_size);
  __XENGINE_LOG(INFO,
                "             Options.batch_group_max_group_size: %u",
                batch_group_max_group_size);
  __XENGINE_LOG(INFO,
                "    Options.batch_group_max_leader_wait_time_us: %u",
                batch_group_max_leader_wait_time_us);
  __XENGINE_LOG(INFO,
                "    Options.concurrent_writable_file_buffer_num: %u",
                concurrent_writable_file_buffer_num);
  __XENGINE_LOG(INFO,
                "Options.concurrent_writable_file_single_buffer_size: %u",
                concurrent_writable_file_single_buffer_size);
  __XENGINE_LOG(INFO,
                "Options.concurrent_writable_file_buffer_switch_limit: %u",
                concurrent_writable_file_buffer_switch_limit);
  __XENGINE_LOG(INFO,
                "               Options.use_direct_write_for_wal: %d",
                use_direct_write_for_wal);
  __XENGINE_LOG(INFO,
                "                 Options.query_trace_enable_count: %d",
                query_trace_enable_count);
  __XENGINE_LOG(INFO,
                "                Options.query_trace_print_stats: %d",
                query_trace_print_stats);
  __XENGINE_LOG(INFO,
                "           Options.mutex_backtrace_threshold_ns: %d",
                mutex_backtrace_threshold_ns);
  __XENGINE_LOG(INFO,
                "           Options.auto_shrink_enabled: %d)",
                auto_shrink_enabled);
  __XENGINE_LOG(INFO,
                "           Options.max_free_extent_percent: %d)",
                max_free_extent_percent);
  __XENGINE_LOG(INFO,
                "           Options.shrink_allocate_interval: %d)",
                shrink_allocate_interval);
  __XENGINE_LOG(INFO,
                "           Options.max_shrink_extent_count: %d)",
                max_shrink_extent_count);
  __XENGINE_LOG(INFO,
                "           Options.total_max_shrink_extent_count: %d)",
                total_max_shrink_extent_count);
  __XENGINE_LOG(INFO,
                "           Options.idle_tasks_schedule_time: %d)",
                idle_tasks_schedule_time);
  __XENGINE_LOG(INFO,
                "           Options.auto_shrink_schedule_interval: %d)",
                auto_shrink_schedule_interval);
  __XENGINE_LOG(INFO,
                "                Options.estimate_cost_depth: %d)",
                estimate_cost_depth);

}

}  // namespace common
}  // namespace xengine
