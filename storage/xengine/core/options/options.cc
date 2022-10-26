/*
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "xengine/options.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <limits>

#include "monitoring/statistics.h"
#include "options/db_options.h"
#include "options/options_helper.h"
#include "table/block_based_table_factory.h"
#include "table/extent_table_factory.h"
#include "util/compression.h"
#include "xengine/cache.h"
#include "xengine/compaction_filter.h"
#include "xengine/comparator.h"
#include "xengine/env.h"
#include "xengine/memtablerep.h"
#include "xengine/merge_operator.h"
#include "xengine/slice.h"
#include "xengine/slice_transform.h"
#include "xengine/sst_file_manager.h"
#include "xengine/table.h"
#include "xengine/table_properties.h"
#include "xengine/wal_filter.h"

using namespace xengine;
using namespace util;
using namespace table;
using namespace cache;

namespace xengine {
namespace common {

AdvancedColumnFamilyOptions::AdvancedColumnFamilyOptions() {
  assert(memtable_factory.get() != nullptr);
}

AdvancedColumnFamilyOptions::AdvancedColumnFamilyOptions(const Options& options)
    : max_write_buffer_number(options.max_write_buffer_number),
      min_write_buffer_number_to_merge(
          options.min_write_buffer_number_to_merge),
      max_write_buffer_number_to_maintain(
          options.max_write_buffer_number_to_maintain),
      inplace_update_support(options.inplace_update_support),
      inplace_update_num_locks(options.inplace_update_num_locks),
      inplace_callback(options.inplace_callback),
      memtable_prefix_bloom_size_ratio(
          options.memtable_prefix_bloom_size_ratio),
      memtable_huge_page_size(options.memtable_huge_page_size),
      memtable_insert_with_hint_prefix_extractor(
          options.memtable_insert_with_hint_prefix_extractor),
      bloom_locality(options.bloom_locality),
      arena_block_size(options.arena_block_size),
      compression_per_level(options.compression_per_level),
      num_levels(options.num_levels),
      level0_slowdown_writes_trigger(options.level0_slowdown_writes_trigger),
      level0_stop_writes_trigger(options.level0_stop_writes_trigger),
      target_file_size_base(options.target_file_size_base),
      target_file_size_multiplier(options.target_file_size_multiplier),
      level_compaction_dynamic_level_bytes(
          options.level_compaction_dynamic_level_bytes),
      max_bytes_for_level_multiplier(options.max_bytes_for_level_multiplier),
      max_bytes_for_level_multiplier_additional(
          options.max_bytes_for_level_multiplier_additional),
      max_compaction_bytes(options.max_compaction_bytes),
      soft_pending_compaction_bytes_limit(
          options.soft_pending_compaction_bytes_limit),
      hard_pending_compaction_bytes_limit(
          options.hard_pending_compaction_bytes_limit),
      compaction_style(options.compaction_style),
      compaction_pri(options.compaction_pri),
      compaction_options_universal(options.compaction_options_universal),
      compaction_options_fifo(options.compaction_options_fifo),
      max_sequential_skip_in_iterations(
          options.max_sequential_skip_in_iterations),
      memtable_factory(options.memtable_factory),
      table_properties_collector_factories(
          options.table_properties_collector_factories),
      max_successive_merges(options.max_successive_merges),
      optimize_filters_for_hits(options.optimize_filters_for_hits),
      paranoid_file_checks(options.paranoid_file_checks),
      force_consistency_checks(options.force_consistency_checks),
      report_bg_io_stats(options.report_bg_io_stats) {
  assert(memtable_factory.get() != nullptr);
  if (max_bytes_for_level_multiplier_additional.size() <
      static_cast<unsigned int>(num_levels)) {
    max_bytes_for_level_multiplier_additional.resize(num_levels, 1);
  }
}

ColumnFamilyOptions::ColumnFamilyOptions()
    : compression(Snappy_Supported() ? kSnappyCompression : kNoCompression) {
#ifndef NDEBUG
      table_factory = std::shared_ptr<TableFactory>(new table::ExtentBasedTableFactory());
#endif
}

ColumnFamilyOptions::ColumnFamilyOptions(const Options& options)
    : AdvancedColumnFamilyOptions(options),
      comparator(options.comparator),
      merge_operator(options.merge_operator),
      compaction_filter(options.compaction_filter),
      compaction_filter_factory(options.compaction_filter_factory),
      write_buffer_size(options.write_buffer_size),
      flush_delete_percent(options.flush_delete_percent),
      compaction_delete_percent(options.compaction_delete_percent),
      flush_delete_percent_trigger(options.flush_delete_percent_trigger),
      flush_delete_record_trigger(options.flush_delete_record_trigger),
      compression(options.compression),
      bottommost_compression(options.bottommost_compression),
      compression_opts(options.compression_opts),
      level0_file_num_compaction_trigger(
          options.level0_file_num_compaction_trigger),
      level0_layer_num_compaction_trigger(
          options.level0_layer_num_compaction_trigger),
      minor_window_size(options.minor_window_size),
      level1_extents_major_compaction_trigger(
          options.level1_extents_major_compaction_trigger),
      level2_usage_percent(options.level2_usage_percent),
      prefix_extractor(options.prefix_extractor),
      max_bytes_for_level_base(options.max_bytes_for_level_base),
      disable_auto_compactions(options.disable_auto_compactions),
      table_factory(options.table_factory) {}

DBOptions::DBOptions() {}

DBOptions::DBOptions(const Options &options)
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
      max_total_wal_size(options.max_total_wal_size),
      statistics(options.statistics),
      use_fsync(options.use_fsync),
      db_paths(options.db_paths),
      db_log_dir(options.db_log_dir),
      wal_dir(options.wal_dir),
      delete_obsolete_files_period_micros(
          options.delete_obsolete_files_period_micros),
      base_background_compactions(options.base_background_compactions),
      max_background_compactions(options.max_background_compactions),
      max_subcompactions(options.max_subcompactions),
      max_background_flushes(options.max_background_flushes),
      max_background_dumps(options.max_background_dumps),
      dump_memtable_limit_size(options.dump_memtable_limit_size),
      max_log_file_size(options.max_log_file_size),
      log_file_time_to_roll(options.log_file_time_to_roll),
      keep_log_file_num(options.keep_log_file_num),
      recycle_log_file_num(options.recycle_log_file_num),
      max_manifest_file_size(options.max_manifest_file_size),
      table_cache_numshardbits(options.table_cache_numshardbits),
      WAL_ttl_seconds(options.WAL_ttl_seconds),
      WAL_size_limit_MB(options.WAL_size_limit_MB),
      manifest_preallocation_size(options.manifest_preallocation_size),
      allow_mmap_reads(options.allow_mmap_reads),
      allow_mmap_writes(options.allow_mmap_writes),
      use_direct_reads(options.use_direct_reads),
      use_direct_io_for_flush_and_compaction(
          options.use_direct_io_for_flush_and_compaction),
      allow_fallocate(options.allow_fallocate),
      is_fd_close_on_exec(options.is_fd_close_on_exec),
      skip_log_error_on_recovery(options.skip_log_error_on_recovery),
      stats_dump_period_sec(options.stats_dump_period_sec),
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
      delayed_write_rate(options.delayed_write_rate),
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
      row_cache(options.row_cache),
#ifndef ROCKSDB_LITE
      wal_filter(options.wal_filter),
#endif  // ROCKSDB_LITE
      fail_if_options_file_error(options.fail_if_options_file_error),
      dump_malloc_stats(options.dump_malloc_stats),
      avoid_flush_during_recovery(options.avoid_flush_during_recovery),
      avoid_flush_during_shutdown(options.avoid_flush_during_shutdown),
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
      compaction_type(options.compaction_type),
      compaction_mode(options.compaction_mode),
      cpu_compaction_thread_num(options.cpu_compaction_thread_num),
      fpga_compaction_thread_num(options.fpga_compaction_thread_num),
      fpga_device_id(options.fpga_device_id),
      query_trace_enable_count(options.query_trace_enable_count),
      query_trace_print_stats(options.query_trace_print_stats),
      mutex_backtrace_threshold_ns(options.mutex_backtrace_threshold_ns),
      auto_shrink_enabled(options.auto_shrink_enabled),
      max_free_extent_percent(options.max_free_extent_percent),
      shrink_allocate_interval(options.shrink_allocate_interval),
      max_shrink_extent_count(options.max_shrink_extent_count),
      total_max_shrink_extent_count(options.total_max_shrink_extent_count),
      table_cache_size(options.table_cache_size),
      auto_shrink_schedule_interval(options.auto_shrink_schedule_interval),
      estimate_cost_depth(options.estimate_cost_depth)
{
}

void DBOptions::Dump() const {
  ImmutableDBOptions(*this).Dump();
  MutableDBOptions(*this).Dump();
}  // DBOptions::Dump

void ColumnFamilyOptions::Dump() const {
  __XENGINE_LOG(INFO, "                       Options.comparator: %s",
                   comparator->Name());
  __XENGINE_LOG(INFO, "                   Options.merge_operator: %s",
                   merge_operator ? merge_operator->Name() : "None");
  __XENGINE_LOG(INFO, "                Options.compaction_filter: %s",
                   compaction_filter ? compaction_filter->Name() : "None");
  __XENGINE_LOG(INFO, "        Options.compaction_filter_factory: %s",
                compaction_filter_factory ? compaction_filter_factory->Name()
                                          : "None");
  __XENGINE_LOG(INFO, "                 Options.memtable_factory: %s",
                   memtable_factory->Name());
  __XENGINE_LOG(INFO, "                    Options.table_factory: %s",
                   table_factory->Name());
  __XENGINE_LOG(INFO, "                    table_factory options: %s",
                   table_factory->GetPrintableTableOptions().c_str());
  __XENGINE_LOG(INFO,
                "                Options.write_buffer_size: %" ROCKSDB_PRIszt,
                write_buffer_size);
  __XENGINE_LOG(INFO, "             Options.flush_delete_percent: %d",
                   flush_delete_percent);
  __XENGINE_LOG(INFO, "        Options.compaction_delete_percent: %d",
                   compaction_delete_percent);
  __XENGINE_LOG(INFO, "     Options.flush_delete_percent_trigger: %d",
                   flush_delete_percent_trigger);
  __XENGINE_LOG(INFO, "      Options.flush_delete_record_trigger: %d",
                   flush_delete_record_trigger);
  __XENGINE_LOG(INFO, "          Options.max_write_buffer_number: %d",
                   max_write_buffer_number);
  if (!compression_per_level.empty()) {
    for (unsigned int i = 0; i < compression_per_level.size(); i++) {
      __XENGINE_LOG(
          INFO, "                   Options.compression[%d]: %s", i,
          CompressionTypeToString(compression_per_level[i]).c_str());
    }
  } else {
    __XENGINE_LOG(INFO, "                      Options.compression: %s",
                     CompressionTypeToString(compression).c_str());
  }
  __XENGINE_LOG(INFO, "           Options.bottommost_compression: %s",
      bottommost_compression == kDisableCompressionOption
          ? "Disabled"
          : CompressionTypeToString(bottommost_compression).c_str());
  __XENGINE_LOG(INFO, "                 Options.prefix_extractor: %s",
      prefix_extractor == nullptr ? "nullptr" : prefix_extractor->Name());
  __XENGINE_LOG(INFO,
                "Options.memtable_insert_with_hint_prefix_extractor: %s",
                   memtable_insert_with_hint_prefix_extractor == nullptr
                       ? "nullptr"
                       : memtable_insert_with_hint_prefix_extractor->Name());
  __XENGINE_LOG(INFO, "                       Options.num_levels: %d",
                   num_levels);
  __XENGINE_LOG(INFO, " Options.min_write_buffer_number_to_merge: %d",
                   min_write_buffer_number_to_merge);
  __XENGINE_LOG(INFO, "Options.max_write_buffer_number_to_maintain: %d",
                   max_write_buffer_number_to_maintain);
  __XENGINE_LOG(INFO, "     Options.compression_opts.window_bits: %d",
                   compression_opts.window_bits);
  __XENGINE_LOG(INFO, "           Options.compression_opts.level: %d",
                   compression_opts.level);
  __XENGINE_LOG(INFO, "        Options.compression_opts.strategy: %d",
                   compression_opts.strategy);
  __XENGINE_LOG(INFO,
                "  Options.compression_opts.max_dict_bytes: %" ROCKSDB_PRIszt,
                compression_opts.max_dict_bytes);
  __XENGINE_LOG(INFO, "Options.level0_file_num_compaction_trigger: %d",
                   level0_file_num_compaction_trigger);
  __XENGINE_LOG(INFO, "Options.level1_extents_major_compaction_trigger: %d",
                   level1_extents_major_compaction_trigger);
  __XENGINE_LOG(INFO, "             Options.level2_usage_percent: %ld",
                   level2_usage_percent);
  __XENGINE_LOG(INFO, "Options.level0_layer_num_compaction_trigger: %d",
                   level0_layer_num_compaction_trigger);
  __XENGINE_LOG(INFO, "                Options.minor_window_size: %d",
                   minor_window_size);
  __XENGINE_LOG(INFO, "   Options.level0_slowdown_writes_trigger: %d",
                   level0_slowdown_writes_trigger);
  __XENGINE_LOG(INFO, "       Options.level0_stop_writes_trigger: %d",
                   level0_stop_writes_trigger);
  __XENGINE_LOG(INFO, "            Options.target_file_size_base: %" PRIu64,
                   target_file_size_base);
  __XENGINE_LOG(INFO, "      Options.target_file_size_multiplier: %d",
                   target_file_size_multiplier);
  __XENGINE_LOG(INFO, "         Options.max_bytes_for_level_base: %" PRIu64,
                   max_bytes_for_level_base);
  __XENGINE_LOG(INFO, "Options.level_compaction_dynamic_level_bytes: %d",
                   level_compaction_dynamic_level_bytes);
  __XENGINE_LOG(INFO, "   Options.max_bytes_for_level_multiplier: %f",
                   max_bytes_for_level_multiplier);
  for (size_t i = 0; i < max_bytes_for_level_multiplier_additional.size();
       i++) {
    __XENGINE_LOG(
        INFO,
        "Options.max_bytes_for_level_multiplier_addtl[%" ROCKSDB_PRIszt "]: %d",
        i, max_bytes_for_level_multiplier_additional[i]);
  }
  __XENGINE_LOG(INFO, "Options.max_sequential_skip_in_iterations: %" PRIu64,
                   max_sequential_skip_in_iterations);
  __XENGINE_LOG(INFO, "             Options.max_compaction_bytes: %" PRIu64,
                   max_compaction_bytes);
  __XENGINE_LOG(INFO,
                "                 Options.arena_block_size: %" ROCKSDB_PRIszt,
                arena_block_size);
  __XENGINE_LOG(INFO, "Options.soft_pending_compaction_bytes_limit: %" PRIu64,
                   soft_pending_compaction_bytes_limit);
  __XENGINE_LOG(INFO, "Options.hard_pending_compaction_bytes_limit: %" PRIu64,
                   hard_pending_compaction_bytes_limit);
  __XENGINE_LOG(INFO, "Options.rate_limit_delay_max_milliseconds: %u",
                   rate_limit_delay_max_milliseconds);
  __XENGINE_LOG(INFO, "         Options.disable_auto_compactions: %d",
                   disable_auto_compactions);

  const auto& it_compaction_style =
      compaction_style_to_string.find(compaction_style);
  std::string str_compaction_style;
  if (it_compaction_style == compaction_style_to_string.end()) {
    assert(false);
    str_compaction_style = "unknown_" + std::to_string(compaction_style);
  } else {
    str_compaction_style = it_compaction_style->second;
  }
  __XENGINE_LOG(INFO, "                 Options.compaction_style: %s",
                   str_compaction_style.c_str());

  const auto& it_compaction_pri = compaction_pri_to_string.find(compaction_pri);
  std::string str_compaction_pri;
  if (it_compaction_pri == compaction_pri_to_string.end()) {
    assert(false);
    str_compaction_pri = "unknown_" + std::to_string(compaction_pri);
  } else {
    str_compaction_pri = it_compaction_pri->second;
  }
  __XENGINE_LOG(INFO, "                   Options.compaction_pri: %s",
                   str_compaction_pri.c_str());
  __XENGINE_LOG(INFO, "Options.compaction_options_universal.size_ratio: %u",
                   compaction_options_universal.size_ratio);
  __XENGINE_LOG(INFO,
                "Options.compaction_options_universal.min_merge_width: %u",
                compaction_options_universal.min_merge_width);
  __XENGINE_LOG(INFO,
                "Options.compaction_options_universal.max_merge_width: %u",
                compaction_options_universal.max_merge_width);
  __XENGINE_LOG(INFO,
                   "Options.compaction_options_universal."
                   "max_size_amplification_percent: %u",
                   compaction_options_universal.max_size_amplification_percent);
  __XENGINE_LOG(
      INFO, "Options.compaction_options_universal.compression_size_percent: %d",
      compaction_options_universal.compression_size_percent);
  __XENGINE_LOG(
      INFO, "Options.compaction_options_fifo.max_table_files_size: %" PRIu64,
      compaction_options_fifo.max_table_files_size);
  std::string collector_names;
  for (const auto& collector_factory : table_properties_collector_factories) {
    collector_names.append(collector_factory->Name());
    collector_names.append("; ");
  }
  __XENGINE_LOG(INFO, "      Options.table_properties_collectors: %s",
                   collector_names.c_str());
  __XENGINE_LOG(INFO, "           Options.inplace_update_support: %d",
                   inplace_update_support);
  __XENGINE_LOG(
      INFO, "         Options.inplace_update_num_locks: %" ROCKSDB_PRIszt,
      inplace_update_num_locks);
  // TODO: easier config for bloom (maybe based on avg key/value size)
  __XENGINE_LOG(INFO, " Options.memtable_prefix_bloom_size_ratio: %f",
                   memtable_prefix_bloom_size_ratio);

  __XENGINE_LOG(INFO,
                "          Options.memtable_huge_page_size: %" ROCKSDB_PRIszt,
                memtable_huge_page_size);
  __XENGINE_LOG(INFO, "                   Options.bloom_locality: %d",
                   bloom_locality);

  __XENGINE_LOG(
      INFO, "            Options.max_successive_merges: %" ROCKSDB_PRIszt,
      max_successive_merges);
  __XENGINE_LOG(INFO, "        Options.optimize_filters_for_hits: %d",
                   optimize_filters_for_hits);
  __XENGINE_LOG(INFO, "             Options.paranoid_file_checks: %d",
                   paranoid_file_checks);
  __XENGINE_LOG(INFO, "         Options.force_consistency_checks: %d",
                   force_consistency_checks);
  __XENGINE_LOG(INFO, "               Options.report_bg_io_stats: %d",
                   report_bg_io_stats);
}  // ColumnFamilyOptions::Dump

void Options::Dump() const {
  DBOptions::Dump();
  ColumnFamilyOptions::Dump();
}  // Options::Dump

void Options::DumpCFOptions() const {
  ColumnFamilyOptions::Dump();
}  // Options::DumpCFOptions

//
// The goal of this method is to create a configuration that
// allows an application to write all files into L0 and
// then do a single compaction to output all files into L1.
Options* Options::PrepareForBulkLoad() {
  // never slowdown ingest.
  level0_file_num_compaction_trigger = (1 << 30);
  level0_layer_num_compaction_trigger = (1 << 30);
  minor_window_size = (1 << 10);
  level1_extents_major_compaction_trigger = (1 << 30);
  level2_usage_percent = 0;
  level0_slowdown_writes_trigger = (1 << 30);
  level0_stop_writes_trigger = (1 << 30);
  soft_pending_compaction_bytes_limit = 0;
  hard_pending_compaction_bytes_limit = 0;

  // no auto compactions please. The application should issue a
  // manual compaction after all data is loaded into L0.
  disable_auto_compactions = true;
  // A manual compaction run should pick all files in L0 in
  // a single compaction run.
  max_compaction_bytes = (static_cast<uint64_t>(1) << 60);

  // It is better to have only 2 levels, otherwise a manual
  // compaction would compact at every possible level, thereby
  // increasing the total time needed for compactions.
  num_levels = 2;

  // Need to allow more write buffers to allow more parallism
  // of flushes.
  max_write_buffer_number = 6;
  min_write_buffer_number_to_merge = 1;

  // When compaction is disabled, more parallel flush threads can
  // help with write throughput.
  max_background_flushes = 4;

  // Prevent a memtable flush to automatically promote files
  // to L1. This is helpful so that all files that are
  // input to the manual compaction are all at L0.
  max_background_compactions = 2;
  base_background_compactions = 2;
  max_background_dumps = 2;
  // The compaction would create large files in L1.
  target_file_size_base = 256 * 1024 * 1024;
  return this;
}

Options* Options::OptimizeForSmallDb() {
  ColumnFamilyOptions::OptimizeForSmallDb();
  DBOptions::OptimizeForSmallDb();
  return this;
}

Options* Options::OldDefaults(int rocksdb_major_version,
                              int rocksdb_minor_version) {
  ColumnFamilyOptions::OldDefaults(rocksdb_major_version,
                                   rocksdb_minor_version);
  DBOptions::OldDefaults(rocksdb_major_version, rocksdb_minor_version);
  return this;
}

DBOptions* DBOptions::OldDefaults(int rocksdb_major_version,
                                  int rocksdb_minor_version) {
  if (rocksdb_major_version < 4 ||
      (rocksdb_major_version == 4 && rocksdb_minor_version < 7)) {
    max_file_opening_threads = 1;
    table_cache_numshardbits = 4;
  }
  if (rocksdb_major_version < 5 ||
      (rocksdb_major_version == 5 && rocksdb_minor_version < 2)) {
    delayed_write_rate = 2 * 1024U * 1024U;
  }

  max_open_files = 5000;
  base_background_compactions = -1;
  wal_recovery_mode = WALRecoveryMode::kTolerateCorruptedTailRecords;
  return this;
}

ColumnFamilyOptions* ColumnFamilyOptions::OldDefaults(
    int rocksdb_major_version, int rocksdb_minor_version) {
  if (rocksdb_major_version < 4 ||
      (rocksdb_major_version == 4 && rocksdb_minor_version < 7)) {
    write_buffer_size = 4 << 20;
    flush_delete_percent = 100;
    compaction_delete_percent = 100;
    flush_delete_percent_trigger = 700000;
    flush_delete_record_trigger = 700000;
    target_file_size_base = 2 * 1048576;
    max_bytes_for_level_base = 10 * 1048576;
    soft_pending_compaction_bytes_limit = 0;
    hard_pending_compaction_bytes_limit = 0;
  }
  if (rocksdb_major_version < 5) {
    level0_stop_writes_trigger = 24;
  } else if (rocksdb_major_version == 5 && rocksdb_minor_version < 2) {
    level0_stop_writes_trigger = 30;
  }
  compaction_pri = CompactionPri::kByCompensatedSize;

  return this;
}

// Optimization functions
DBOptions* DBOptions::OptimizeForSmallDb() {
  max_file_opening_threads = 1;
  max_open_files = 5000;
  return this;
}

ColumnFamilyOptions* ColumnFamilyOptions::OptimizeForSmallDb() {
  write_buffer_size = 2 << 20;
  flush_delete_percent = 100;
  compaction_delete_percent = 100;
  target_file_size_base = 2 * 1048576;
  max_bytes_for_level_base = 10 * 1048576;
  soft_pending_compaction_bytes_limit = 256 * 1048576;
  hard_pending_compaction_bytes_limit = 1073741824ul;
  return this;
}

#ifndef ROCKSDB_LITE
ColumnFamilyOptions* ColumnFamilyOptions::OptimizeForPointLookup(
    uint64_t block_cache_size_mb) {
  prefix_extractor.reset(NewNoopTransform());
  BlockBasedTableOptions block_based_options;
  block_based_options.index_type = BlockBasedTableOptions::kHashSearch;
  block_based_options.filter_policy.reset(NewBloomFilterPolicy(10));
  block_based_options.block_cache =
      NewLRUCache(static_cast<size_t>(block_cache_size_mb * 1024 * 1024));
  table_factory.reset(new BlockBasedTableFactory(block_based_options));
  memtable_prefix_bloom_size_ratio = 0.02;
  return this;
}

ColumnFamilyOptions* ColumnFamilyOptions::OptimizeLevelStyleCompaction(
    uint64_t memtable_memory_budget) {
  write_buffer_size = static_cast<size_t>(memtable_memory_budget / 4);
  // merge two memtables when flushing to L0
  min_write_buffer_number_to_merge = 2;
  // this means we'll use 50% extra memory in the worst case, but will reduce
  // write stalls.
  max_write_buffer_number = 6;
  // start flushing L0->L1 as soon as possible. each file on level0 is
  // (memtable_memory_budget / 2). This will flush level 0 when it's bigger than
  // memtable_memory_budget.
  level0_file_num_compaction_trigger = 2;
  level0_layer_num_compaction_trigger = 16;
  minor_window_size = 64;
  level1_extents_major_compaction_trigger = 1000;
  level2_usage_percent = 70;
  // doesn't really matter much, but we don't want to create too many files
  target_file_size_base = memtable_memory_budget / 8;
  // make Level1 size equal to Level0 size, so that L0->L1 compactions are fast
  max_bytes_for_level_base = memtable_memory_budget;

  // level style compaction
  compaction_style = kCompactionStyleLevel;

  // only compress levels >= 2
  compression_per_level.resize(num_levels);
  for (int i = 0; i < num_levels; ++i) {
    if (i < 2) {
      compression_per_level[i] = kNoCompression;
    } else {
      compression_per_level[i] = kSnappyCompression;
    }
  }
  return this;
}

ColumnFamilyOptions* ColumnFamilyOptions::OptimizeUniversalStyleCompaction(
    uint64_t memtable_memory_budget) {
  write_buffer_size = static_cast<size_t>(memtable_memory_budget / 4);
  // merge two memtables when flushing to L0
  min_write_buffer_number_to_merge = 2;
  // this means we'll use 50% extra memory in the worst case, but will reduce
  // write stalls.
  max_write_buffer_number = 6;
  // universal style compaction
  compaction_style = kCompactionStyleUniversal;
  compaction_options_universal.compression_size_percent = 80;
  return this;
}

DBOptions* DBOptions::IncreaseParallelism(int total_threads) {
  max_background_compactions = total_threads - 1;
  max_background_flushes = 1;
  env->SetBackgroundThreads(total_threads, Env::LOW);
  env->SetBackgroundThreads(1, Env::HIGH);
  return this;
}

#endif  // !ROCKSDB_LITE

ReadOptions::ReadOptions()
    : verify_checksums(true),
      fill_cache(true),
      snapshot(nullptr),
      iterate_upper_bound(nullptr),
      read_tier(kReadAllTier),
      tailing(false),
      managed(false),
      total_order_seek(false),
      prefix_same_as_start(false),
      pin_data(false),
      background_purge_on_iterator_cleanup(false),
      readahead_size(0),
      ignore_range_deletions(false),
      max_skippable_internal_keys(0),
      skip_del_(true),
      read_level_(kAll){}

ReadOptions::ReadOptions(bool cksum, bool cache)
    : verify_checksums(cksum),
      fill_cache(cache),
      snapshot(nullptr),
      iterate_upper_bound(nullptr),
      read_tier(kReadAllTier),
      tailing(false),
      managed(false),
      total_order_seek(false),
      prefix_same_as_start(false),
      pin_data(false),
      background_purge_on_iterator_cleanup(false),
      readahead_size(0),
      ignore_range_deletions(false),
      max_skippable_internal_keys(0),
      skip_del_(true),
      read_level_(kAll){}

}  // namespace common
}  // namespace xengine
