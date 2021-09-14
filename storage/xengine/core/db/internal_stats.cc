// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/internal_stats.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <algorithm>
#include <string>
#include <utility>
#include <vector>
#include "db/column_family.h"
#include "db/db_impl.h"
#include "storage/storage_meta_struct.h"
#include "storage/multi_version_extent_meta_layer.h"
#include "util/string_util.h"

using namespace xengine;
using namespace common;
using namespace util;
using namespace table;

namespace xengine {
namespace db {
using namespace xengine::storage;

#ifndef ROCKSDB_LITE

const std::map<LevelStatType, LevelStat> InternalStats::compaction_level_stats =
    {
        {LevelStatType::NUM_FILES, LevelStat{"NumFiles", "Extents"}},
        {LevelStatType::COMPACTED_FILES,
         LevelStat{"CompactedFiles", "CompactedFiles"}},
        {LevelStatType::SIZE_BYTES, LevelStat{"SizeBytes", "Size"}},
        {LevelStatType::SCORE, LevelStat{"Score", "Score"}},
        {LevelStatType::READ_GB, LevelStat{"ReadGB", "Read(GB)"}},
        {LevelStatType::RN_GB, LevelStat{"RnGB", "Rn(GB)"}},
        {LevelStatType::RNP1_GB, LevelStat{"Rnp1GB", "Rnp1(GB)"}},
        {LevelStatType::WRITE_GB, LevelStat{"WriteGB", "Write(GB)"}},
        {LevelStatType::W_NEW_GB, LevelStat{"WnewGB", "Wnew(GB)"}},
        {LevelStatType::MOVED_GB, LevelStat{"MovedGB", "Moved(GB)"}},
        {LevelStatType::WRITE_AMP, LevelStat{"WriteAmp", "W-Amp"}},
        {LevelStatType::READ_MBPS, LevelStat{"ReadMBps", "Rd(MB/s)"}},
        {LevelStatType::WRITE_MBPS, LevelStat{"WriteMBps", "Wr(MB/s)"}},
        {LevelStatType::COMP_SEC, LevelStat{"CompSec", "Comp(sec)"}},
        {LevelStatType::COMP_COUNT, LevelStat{"CompCount", "Comp(cnt)"}},
        {LevelStatType::AVG_SEC, LevelStat{"AvgSec", "Avg(sec)"}},
        {LevelStatType::KEY_IN, LevelStat{"KeyIn", "KeyIn"}},
        {LevelStatType::KEY_DROP, LevelStat{"KeyDrop", "KeyDrop"}},
};
const std::map<ExtentLevelStatType, LevelStat>
    InternalStats::extent_compaction_stats = {
        {ExtentLevelStatType::NUM_EXTENTS, LevelStat{"NumExtents", "Extents"}},
        {ExtentLevelStatType::NUM_LAYERS, LevelStat{"NumLayers", "Layers"}},
        //{ExtentLevelStatType::COMPACTED_EXTENTS,
        // LevelStat{"CompactedExtents", "CompactedExtents"}},
        {ExtentLevelStatType::SIZE_BYTES, LevelStat{"SizeBytes", "Size"}},
        {ExtentLevelStatType::DATA_BYTES, LevelStat{"DataBytes", "Data"}},
        {ExtentLevelStatType::INDEX_BYTES, LevelStat{"IndexExtents", "Index"}},
        {ExtentLevelStatType::READ_GB, LevelStat{"ReadGB", "Read(GB)"}},
        {ExtentLevelStatType::WRITE_GB, LevelStat{"WriteGB", "Write(GB)"}},
        //{ExtentLevelStatType::MERGEDIN_RE, LevelStat{"MergedInputRecord",
        //"Moved(GB)"}},
        {ExtentLevelStatType::MERGEOUT_RE,
         LevelStat{"MergedOutputRecord", "MergeOutRe"}},
        {ExtentLevelStatType::MERGEOUT_EXT,
         LevelStat{"MergedOutputExtent", "MergeOutExt"}},
        {ExtentLevelStatType::REUSED_EXT, LevelStat{"ReusedExtent", "ReExt"}},
        //{ExtentLevelStatType::MERGED_EXT, LevelStat{"MergedExtent",
        //"Comp(sec)"}},
        {ExtentLevelStatType::REUSED_BL, LevelStat{"ReusedBlock", "ReBl"}},
        //{ExtentLevelStatType::MERGED_BL, LevelStat{"MergedBlock",
        //"Avg(sec)"}},
};

namespace {
const double kMB = 1048576.0;
const double kGB = kMB * 1024;
const double kMicrosInSec = 1000000.0;

void PrintLevelStatsHeader(char* buf, size_t len, const std::string& cf_name) {
  int written_size =
      snprintf(buf, len, "\n** Compaction Stats [%s] **\n", cf_name.c_str());
  auto hdr = [](ExtentLevelStatType t) {
    return InternalStats::extent_compaction_stats.at(t).header_name.c_str();
  };
  int line_size = snprintf(
      buf + written_size, len - written_size,
      "Level  %s    %s  %s  %s    %s    %s    %s   %s  %s  %s  %s\n",
      // Note that we skip COMPACTED_FILES and merge it with Files column
      hdr(ExtentLevelStatType::NUM_EXTENTS),
      hdr(ExtentLevelStatType::NUM_LAYERS),
      hdr(ExtentLevelStatType::SIZE_BYTES),
      hdr(ExtentLevelStatType::DATA_BYTES),
      hdr(ExtentLevelStatType::INDEX_BYTES), hdr(ExtentLevelStatType::READ_GB),
      hdr(ExtentLevelStatType::WRITE_GB), hdr(ExtentLevelStatType::MERGEOUT_RE),
      hdr(ExtentLevelStatType::MERGEOUT_EXT),
      hdr(ExtentLevelStatType::REUSED_EXT),
      hdr(ExtentLevelStatType::REUSED_BL));
  /*
  auto hdr = [](LevelStatType t) {
    return InternalStats::compaction_level_stats.at(t).header_name.c_str();
  };
  int line_size = snprintf(
      buf + written_size, len - written_size,
      "Level    %s   %s     %s %s  %s %s %s %s %s %s %s %s %s %s %s %s %s\n",
      // Note that we skip COMPACTED_FILES and merge it with Files column
      hdr(LevelStatType::NUM_FILES), hdr(LevelStatType::SIZE_BYTES),
      hdr(LevelStatType::SCORE), hdr(LevelStatType::READ_GB),
      hdr(LevelStatType::RN_GB), hdr(LevelStatType::RNP1_GB),
      hdr(LevelStatType::WRITE_GB), hdr(LevelStatType::W_NEW_GB),
      hdr(LevelStatType::MOVED_GB), hdr(LevelStatType::WRITE_AMP),
      hdr(LevelStatType::READ_MBPS), hdr(LevelStatType::WRITE_MBPS),
      hdr(LevelStatType::COMP_SEC), hdr(LevelStatType::COMP_COUNT),
      hdr(LevelStatType::AVG_SEC), hdr(LevelStatType::KEY_IN),
      hdr(LevelStatType::KEY_DROP));
  */
  written_size += line_size;
  snprintf(buf + written_size, len - written_size, "%s\n",
           std::string(line_size, '-').c_str());
}

#if 0
void PrepareLevelStats(std::map<LevelStatType, double>* level_stats,
                       int num_files, int being_compacted,
                       double total_file_size, double score, double w_amp,
                       const InternalStats::CompactionStats& stats) {
  uint64_t bytes_read =
      stats.bytes_read_non_output_levels + stats.bytes_read_output_level;
  int64_t bytes_new = stats.bytes_written - stats.bytes_read_output_level;
  double elapsed = (stats.micros + 1) / kMicrosInSec;

  (*level_stats)[LevelStatType::NUM_FILES] = num_files;
  (*level_stats)[LevelStatType::COMPACTED_FILES] = being_compacted;
  (*level_stats)[LevelStatType::SIZE_BYTES] = total_file_size;
  (*level_stats)[LevelStatType::SCORE] = score;
  (*level_stats)[LevelStatType::READ_GB] = bytes_read / kGB;
  (*level_stats)[LevelStatType::RN_GB] =
      stats.bytes_read_non_output_levels / kGB;
  (*level_stats)[LevelStatType::RNP1_GB] = stats.bytes_read_output_level / kGB;
  (*level_stats)[LevelStatType::WRITE_GB] = stats.bytes_written / kGB;
  (*level_stats)[LevelStatType::W_NEW_GB] = bytes_new / kGB;
  (*level_stats)[LevelStatType::MOVED_GB] = stats.bytes_moved / kGB;
  (*level_stats)[LevelStatType::WRITE_AMP] = w_amp;
  (*level_stats)[LevelStatType::READ_MBPS] = bytes_read / kMB / elapsed;
  (*level_stats)[LevelStatType::WRITE_MBPS] =
      stats.bytes_written / kMB / elapsed;
  (*level_stats)[LevelStatType::COMP_SEC] = stats.micros / kMicrosInSec;
  (*level_stats)[LevelStatType::COMP_COUNT] = stats.count;
  (*level_stats)[LevelStatType::AVG_SEC] =
      stats.count == 0 ? 0 : stats.micros / kMicrosInSec / stats.count;
  (*level_stats)[LevelStatType::KEY_IN] =
      static_cast<double>(stats.num_input_records);
  (*level_stats)[LevelStatType::KEY_DROP] =
      static_cast<double>(stats.num_dropped_records);
}
#endif

#if 0
void PrintLevelStats(char* buf, size_t len, const std::string& name,
                     const std::map<LevelStatType, double>& stat_value) {
  snprintf(buf, len,
           "%4s "      /*  Level */
           "%6d/%-3d " /*  Files */
           "%8s "      /*  Size */
           "%5.1f "    /*  Score */
           "%8.1f "    /*  Read(GB) */
           "%7.1f "    /*  Rn(GB) */
           "%8.1f "    /*  Rnp1(GB) */
           "%9.1f "    /*  Write(GB) */
           "%8.1f "    /*  Wnew(GB) */
           "%9.1f "    /*  Moved(GB) */
           "%5.1f "    /*  W-Amp */
           "%8.1f "    /*  Rd(MB/s) */
           "%8.1f "    /*  Wr(MB/s) */
           "%9.0f "    /*  Comp(sec) */
           "%9d "      /*  Comp(cnt) */
           "%8.3f "    /*  Avg(sec) */
           "%7s "      /*  KeyIn */
           "%6s\n",    /*  KeyDrop */
           name.c_str(),
           static_cast<int>(stat_value.at(LevelStatType::NUM_FILES)),
           static_cast<int>(stat_value.at(LevelStatType::COMPACTED_FILES)),
           BytesToHumanString(
               static_cast<uint64_t>(stat_value.at(LevelStatType::SIZE_BYTES)))
               .c_str(),
           stat_value.at(LevelStatType::SCORE),
           stat_value.at(LevelStatType::READ_GB),
           stat_value.at(LevelStatType::RN_GB),
           stat_value.at(LevelStatType::RNP1_GB),
           stat_value.at(LevelStatType::WRITE_GB),
           stat_value.at(LevelStatType::W_NEW_GB),
           stat_value.at(LevelStatType::MOVED_GB),
           stat_value.at(LevelStatType::WRITE_AMP),
           stat_value.at(LevelStatType::READ_MBPS),
           stat_value.at(LevelStatType::WRITE_MBPS),
           stat_value.at(LevelStatType::COMP_SEC),
           static_cast<int>(stat_value.at(LevelStatType::COMP_COUNT)),
           stat_value.at(LevelStatType::AVG_SEC),
           NumberToHumanString(
               static_cast<std::int64_t>(stat_value.at(LevelStatType::KEY_IN)))
               .c_str(),
           NumberToHumanString(static_cast<std::int64_t>(
                                   stat_value.at(LevelStatType::KEY_DROP)))
               .c_str());
}

void PrintLevelStats(char* buf, size_t len, const std::string& name,
                     int num_files, int being_compacted, double total_file_size,
                     double score, double w_amp,
                     const InternalStats::CompactionStats& stats) {
  std::map<LevelStatType, double> level_stats;
  PrepareLevelStats(&level_stats, num_files, being_compacted, total_file_size,
                    score, w_amp, stats);
  PrintLevelStats(buf, len, name, level_stats);
}

#endif

void print_level_stats(
    char* buf, size_t len, const std::string& name,
    const std::map<ExtentLevelStatType, double>& stat_value) {
  snprintf(
      buf, len,
      "%4s "     /*  Level */
      "%7d "     /*  Extents */
      "%7d "     /*  Layers */
      "%11s "    /*  Size */
      "%10s "    /*  DataSize */
      "%10s "    /*  IndexSize */
      "%8.1f "   /*  Read(GB) */
      "%9.1f "   /*  Write(GB) */
      "%11.1f "  /*  MergeOutRe */
      "%11.1f "  /*  MergeOutExt */
      "%11.1f "  /*  ReExt */
      "%7.1f\n", /*  ReBl */
      name.c_str(),
      static_cast<int>(stat_value.at(ExtentLevelStatType::NUM_EXTENTS)),
      static_cast<int>(stat_value.at(ExtentLevelStatType::NUM_LAYERS)),
      BytesToHumanString(
          static_cast<uint64_t>(stat_value.at(ExtentLevelStatType::SIZE_BYTES)))
          .c_str(),
      BytesToHumanString(
          static_cast<uint64_t>(stat_value.at(ExtentLevelStatType::DATA_BYTES)))
          .c_str(),
      BytesToHumanString(static_cast<uint64_t>(
                             stat_value.at(ExtentLevelStatType::INDEX_BYTES)))
          .c_str(),
      stat_value.at(ExtentLevelStatType::READ_GB),
      stat_value.at(ExtentLevelStatType::WRITE_GB),
      stat_value.at(ExtentLevelStatType::MERGEOUT_RE),
      stat_value.at(ExtentLevelStatType::MERGEOUT_EXT),
      stat_value.at(ExtentLevelStatType::REUSED_EXT),
      stat_value.at(ExtentLevelStatType::REUSED_BL));
}

// Assumes that trailing numbers represent an optional argument. This requires
// property names to not end with numbers.
std::pair<Slice, Slice> GetPropertyNameAndArg(const Slice& property) {
  Slice name = property, arg = property;
  size_t sfx_len = 0;
  while (sfx_len < property.size() &&
         isdigit(property[property.size() - sfx_len - 1])) {
    ++sfx_len;
  }
  name.remove_suffix(sfx_len);
  arg.remove_prefix(property.size() - sfx_len);
  return {name, arg};
}
}

static const std::string rocksdb_prefix = "xengine.";

static const std::string num_files_at_level_prefix = "num-files-at-level";
static const std::string compression_ratio_at_level_prefix =
    "compression-ratio-at-level";
static const std::string allstats = "stats";
static const std::string sstables = "sstables";
static const std::string cfstats = "cfstats";
static const std::string cfstats_no_file_histogram =
    "cfstats-no-file-histogram";
static const std::string cf_file_histogram = "cf-file-histogram";
static const std::string dbstats = "dbstats";
static const std::string levelstats = "levelstats";
static const std::string num_immutable_mem_table = "num-immutable-mem-table";
static const std::string num_immutable_mem_table_flushed =
    "num-immutable-mem-table-flushed";
static const std::string mem_table_flush_pending = "mem-table-flush-pending";
static const std::string compaction_pending = "compaction-pending";
static const std::string compactions  = "compactions";
static const std::string meta  = "meta";
static const std::string background_errors = "background-errors";
static const std::string cur_size_active_mem_table =
    "cur-size-active-mem-table";
static const std::string cur_size_all_mem_tables = "cur-size-all-mem-tables";
static const std::string size_all_mem_tables = "size-all-mem-tables";
static const std::string num_entries_active_mem_table =
    "num-entries-active-mem-table";
static const std::string num_entries_imm_mem_tables =
    "num-entries-imm-mem-tables";
static const std::string num_deletes_active_mem_table =
    "num-deletes-active-mem-table";
static const std::string num_deletes_imm_mem_tables =
    "num-deletes-imm-mem-tables";
static const std::string estimate_num_keys = "estimate-num-keys";
static const std::string estimate_table_readers_mem =
    "estimate-table-readers-mem";
static const std::string is_file_deletions_enabled =
    "is-file-deletions-enabled";
static const std::string num_snapshots = "num-snapshots";
static const std::string oldest_snapshot_time = "oldest-snapshot-time";
static const std::string num_live_versions = "num-live-versions";
static const std::string current_version_number =
    "current-super-version-number";
static const std::string estimate_live_data_size = "estimate-live-data-size";
static const std::string min_log_number_to_keep = "min-log-number-to-keep";
static const std::string base_level = "base-level";
static const std::string total_sst_files_size = "total-sst-files-size";
static const std::string estimate_pending_comp_bytes =
    "estimate-pending-compaction-bytes";
static const std::string aggregated_table_properties =
    "aggregated-table-properties";
static const std::string aggregated_table_properties_at_level =
    aggregated_table_properties + "-at-level";
static const std::string num_running_compactions = "num-running-compactions";
static const std::string num_running_flushes = "num-running-flushes";
static const std::string actual_delayed_write_rate =
    "actual-delayed-write-rate";
static const std::string is_write_stopped = "is-write-stopped";

// Memory stats parameters
using std::string;
static const string db_memory_stats = "db-memory-stats";
static const string active_mem_table_total_number =
    "active-mem-table-total-number";
static const string active_mem_table_total_memory_allocated =
    "active-mem-table-total-memory-allocated";
static const string active_mem_table_total_memory_used =
    "active-mem-table-total-memor-used";
static const string unflushed_imm_table_total_number =
    "unflushed-imm-table-total-number";
static const string unflushed_imm_table_total_memory_allocated =
    "unflushed-imm-table-total-memory-allocated";
static const string unflushed_imm_table_total_memory_used =
    "unflushed-imm-table-total-memory-used";
static const string table_reader_total_number = "table-reader-total-number";
static const string table_reader_total_memory_used =
    "table-reader-total-memory-used";
static const string block_cache_total_pinned_memory =
    "block-cache-total-number-pinned-memory";
static const string block_cache_total_memory_used =
    "block-cache-total-memory-used";
//static const string db_total_memory_allocated = "db-total-memory-allocated";

const std::string DB::Properties::kNumFilesAtLevelPrefix =
    rocksdb_prefix + num_files_at_level_prefix;
const std::string DB::Properties::kCompressionRatioAtLevelPrefix =
    rocksdb_prefix + compression_ratio_at_level_prefix;
const std::string DB::Properties::kStats = rocksdb_prefix + allstats;
const std::string DB::Properties::kSSTables = rocksdb_prefix + sstables;
const std::string DB::Properties::kCFStats = rocksdb_prefix + cfstats;
const std::string DB::Properties::kCFStatsNoFileHistogram =
    rocksdb_prefix + cfstats_no_file_histogram;
const std::string DB::Properties::kCFFileHistogram =
    rocksdb_prefix + cf_file_histogram;
const std::string DB::Properties::kDBStats = rocksdb_prefix + dbstats;
const std::string DB::Properties::kCompactionStats = rocksdb_prefix + compactions;
const std::string DB::Properties::kMeta = rocksdb_prefix + meta;
const std::string DB::Properties::kLevelStats = rocksdb_prefix + levelstats;
const std::string DB::Properties::kNumImmutableMemTable =
    rocksdb_prefix + num_immutable_mem_table;
const std::string DB::Properties::kNumImmutableMemTableFlushed =
    rocksdb_prefix + num_immutable_mem_table_flushed;
const std::string DB::Properties::kMemTableFlushPending =
    rocksdb_prefix + mem_table_flush_pending;
const std::string DB::Properties::kCompactionPending =
    rocksdb_prefix + compaction_pending;
const std::string DB::Properties::kNumRunningCompactions =
    rocksdb_prefix + num_running_compactions;
const std::string DB::Properties::kNumRunningFlushes =
    rocksdb_prefix + num_running_flushes;
const std::string DB::Properties::kBackgroundErrors =
    rocksdb_prefix + background_errors;
const std::string DB::Properties::kCurSizeActiveMemTable =
    rocksdb_prefix + cur_size_active_mem_table;
const std::string DB::Properties::kCurSizeAllMemTables =
    rocksdb_prefix + cur_size_all_mem_tables;
const std::string DB::Properties::kSizeAllMemTables =
    rocksdb_prefix + size_all_mem_tables;
const std::string DB::Properties::kNumEntriesActiveMemTable =
    rocksdb_prefix + num_entries_active_mem_table;
const std::string DB::Properties::kNumEntriesImmMemTables =
    rocksdb_prefix + num_entries_imm_mem_tables;
const std::string DB::Properties::kNumDeletesActiveMemTable =
    rocksdb_prefix + num_deletes_active_mem_table;
const std::string DB::Properties::kNumDeletesImmMemTables =
    rocksdb_prefix + num_deletes_imm_mem_tables;
const std::string DB::Properties::kEstimateNumKeys =
    rocksdb_prefix + estimate_num_keys;
const std::string DB::Properties::kEstimateTableReadersMem =
    rocksdb_prefix + estimate_table_readers_mem;
const std::string DB::Properties::kIsFileDeletionsEnabled =
    rocksdb_prefix + is_file_deletions_enabled;
const std::string DB::Properties::kNumSnapshots =
    rocksdb_prefix + num_snapshots;
const std::string DB::Properties::kOldestSnapshotTime =
    rocksdb_prefix + oldest_snapshot_time;
const std::string DB::Properties::kNumLiveVersions =
    rocksdb_prefix + num_live_versions;
const std::string DB::Properties::kCurrentSuperVersionNumber =
    rocksdb_prefix + current_version_number;
const std::string DB::Properties::kEstimateLiveDataSize =
    rocksdb_prefix + estimate_live_data_size;
const std::string DB::Properties::kMinLogNumberToKeep =
    rocksdb_prefix + min_log_number_to_keep;
const std::string DB::Properties::kTotalSstFilesSize =
    rocksdb_prefix + total_sst_files_size;
const std::string DB::Properties::kBaseLevel = rocksdb_prefix + base_level;
const std::string DB::Properties::kEstimatePendingCompactionBytes =
    rocksdb_prefix + estimate_pending_comp_bytes;
const std::string DB::Properties::kAggregatedTableProperties =
    rocksdb_prefix + aggregated_table_properties;
const std::string DB::Properties::kAggregatedTablePropertiesAtLevel =
    rocksdb_prefix + aggregated_table_properties_at_level;
const std::string DB::Properties::kActualDelayedWriteRate =
    rocksdb_prefix + actual_delayed_write_rate;
const std::string DB::Properties::kIsWriteStopped =
    rocksdb_prefix + is_write_stopped;

#define DEFINE_DB_PROPERTY(name, str_name) \
  const std::string DB::Properties::name = rocksdb_prefix + str_name;
DEFINE_DB_PROPERTY(kDBMemoryStats, db_memory_stats);
DEFINE_DB_PROPERTY(kActiveMemTableTotalNumber, active_mem_table_total_number);
DEFINE_DB_PROPERTY(kActiveMemTableTotalMemoryAllocated,
                   active_mem_table_total_memory_allocated);
DEFINE_DB_PROPERTY(kActiveMemTableTotalMemoryUsed,
                   active_mem_table_total_memory_used);
DEFINE_DB_PROPERTY(kUnflushedImmTableTotalNumber,
                   unflushed_imm_table_total_number);
DEFINE_DB_PROPERTY(kUnflushedImmTableTotalMemoryAllocated,
                   unflushed_imm_table_total_memory_allocated);
DEFINE_DB_PROPERTY(kUnflushedImmTableTotalMemoryUsed,
                   unflushed_imm_table_total_memory_used);
DEFINE_DB_PROPERTY(kTableReaderTotalNumber, table_reader_total_number);
DEFINE_DB_PROPERTY(kTableReaderTotalMemoryUsed, table_reader_total_memory_used);
DEFINE_DB_PROPERTY(kBlockCacheTotalPinnedMemory,
                   block_cache_total_pinned_memory);
DEFINE_DB_PROPERTY(kBlockCacheTotalMemoryUsed, block_cache_total_memory_used);

const std::unordered_map<std::string, DBPropertyInfo>
    InternalStats::ppt_name_to_info = {
        {DB::Properties::kNumFilesAtLevelPrefix,
         {false, &InternalStats::HandleNumExtentsAtLevel, nullptr, nullptr}},
        {DB::Properties::kCompressionRatioAtLevelPrefix,
         {false, &InternalStats::HandleCompressionRatioAtLevelPrefix, nullptr,
          nullptr}},
        {DB::Properties::kLevelStats,
         {false, &InternalStats::HandleLevelStats, nullptr, nullptr}},
        {DB::Properties::kStats,
         {false, &InternalStats::HandleStats, nullptr, nullptr}},
        {DB::Properties::kCFStats,
         {false, &InternalStats::HandleCFStats, nullptr,
          &InternalStats::HandleCFMapStats}},
        {DB::Properties::kCFStatsNoFileHistogram,
         {false, &InternalStats::HandleCFStatsNoFileHistogram, nullptr,
          nullptr}},
        {DB::Properties::kCFFileHistogram,
         {false, &InternalStats::HandleCFFileHistogram, nullptr, nullptr}},
        {DB::Properties::kDBStats,
         {false, &InternalStats::HandleDBStats, nullptr, nullptr}},
        {DB::Properties::kMeta,
         {false, &InternalStats::HandleMeta, nullptr, nullptr}},
        {DB::Properties::kSSTables,
         {false, &InternalStats::HandleSsTables, nullptr, nullptr}},
        {DB::Properties::kAggregatedTableProperties,
         {false, &InternalStats::HandleAggregatedTableProperties, nullptr,
          nullptr}},
        {DB::Properties::kAggregatedTablePropertiesAtLevel,
         {false, &InternalStats::HandleAggregatedTablePropertiesAtLevel,
          nullptr, nullptr}},
        {DB::Properties::kNumImmutableMemTable,
         {false, nullptr, &InternalStats::HandleNumImmutableMemTable, nullptr}},
        {DB::Properties::kNumImmutableMemTableFlushed,
         {false, nullptr, &InternalStats::HandleNumImmutableMemTableFlushed,
          nullptr}},
        {DB::Properties::kMemTableFlushPending,
         {false, nullptr, &InternalStats::HandleMemTableFlushPending, nullptr}},
        {DB::Properties::kCompactionPending,
         {false, nullptr, &InternalStats::HandleCompactionPending, nullptr}},
        {DB::Properties::kBackgroundErrors,
         {false, nullptr, &InternalStats::HandleBackgroundErrors, nullptr}},
        {DB::Properties::kCurSizeActiveMemTable,
         {false, nullptr, &InternalStats::HandleCurSizeActiveMemTable,
          nullptr}},
        {DB::Properties::kCurSizeAllMemTables,
         {false, nullptr, &InternalStats::HandleCurSizeAllMemTables, nullptr}},
        {DB::Properties::kSizeAllMemTables,
         {false, nullptr, &InternalStats::HandleSizeAllMemTables, nullptr}},
        {DB::Properties::kNumEntriesActiveMemTable,
         {false, nullptr, &InternalStats::HandleNumEntriesActiveMemTable,
          nullptr}},
        {DB::Properties::kNumEntriesImmMemTables,
         {false, nullptr, &InternalStats::HandleNumEntriesImmMemTables,
          nullptr}},
        {DB::Properties::kNumDeletesActiveMemTable,
         {false, nullptr, &InternalStats::HandleNumDeletesActiveMemTable,
          nullptr}},
        {DB::Properties::kNumDeletesImmMemTables,
         {false, nullptr, &InternalStats::HandleNumDeletesImmMemTables,
          nullptr}},
        {DB::Properties::kEstimateNumKeys,
         {false, nullptr, &InternalStats::HandleEstimateNumKeys, nullptr}},
        {DB::Properties::kEstimateTableReadersMem,
         {true, nullptr, &InternalStats::HandleEstimateTableReadersMem,
          nullptr}},
        {DB::Properties::kIsFileDeletionsEnabled,
         {false, nullptr, &InternalStats::HandleIsFileDeletionsEnabled,
          nullptr}},
        {DB::Properties::kNumSnapshots,
         {false, nullptr, &InternalStats::HandleNumSnapshots, nullptr}},
        {DB::Properties::kOldestSnapshotTime,
         {false, nullptr, &InternalStats::HandleOldestSnapshotTime, nullptr}},
        {DB::Properties::kNumLiveVersions,
         {false, nullptr, &InternalStats::HandleNumLiveVersions, nullptr}},
        {DB::Properties::kCurrentSuperVersionNumber,
         {false, nullptr, &InternalStats::HandleCurrentSuperVersionNumber,
          nullptr}},
        {DB::Properties::kEstimateLiveDataSize,
         {true, nullptr, &InternalStats::HandleEstimateLiveDataSize, nullptr}},
        {DB::Properties::kMinLogNumberToKeep,
         {false, nullptr, &InternalStats::HandleMinLogNumberToKeep, nullptr}},
        {DB::Properties::kBaseLevel,
         {false, nullptr, &InternalStats::HandleBaseLevel, nullptr}},
        {DB::Properties::kTotalSstFilesSize,
         {false, nullptr, &InternalStats::HandleTotalSstFilesSize, nullptr}},
        {DB::Properties::kEstimatePendingCompactionBytes,
         {false, nullptr, &InternalStats::HandleEstimatePendingCompactionBytes,
          nullptr}},
        {DB::Properties::kNumRunningFlushes,
         {false, nullptr, &InternalStats::HandleNumRunningFlushes, nullptr}},
        {DB::Properties::kNumRunningCompactions,
         {false, nullptr, &InternalStats::HandleNumRunningCompactions,
          nullptr}},
        {DB::Properties::kActualDelayedWriteRate,
         {false, nullptr, &InternalStats::HandleActualDelayedWriteRate,
          nullptr}},
        {DB::Properties::kIsWriteStopped,
         {false, nullptr, &InternalStats::HandleIsWriteStopped, nullptr}},
        {DB::Properties::kDBMemoryStats,
         {false, &InternalStats::HandleDBMemoryStats, nullptr, nullptr}},
        {DB::Properties::kActiveMemTableTotalNumber,
         {false, nullptr, &InternalStats::HandleActiveMemTableTotalNumber,
          nullptr}},
        {DB::Properties::kActiveMemTableTotalMemoryAllocated,
         {false, nullptr,
          &InternalStats::HandleActiveMemTableTotalMemoryAllocated, nullptr}},
        {DB::Properties::kActiveMemTableTotalMemoryUsed,
         {false, nullptr, &InternalStats::HandleActiveMemTableTotalMemoryUsed,
          nullptr}},
        {DB::Properties::kUnflushedImmTableTotalNumber,
         {false, nullptr, &InternalStats::HandleUnflushedImmTableTotalNumber,
          nullptr}},
        {DB::Properties::kUnflushedImmTableTotalMemoryAllocated,
         {false, nullptr,
          &InternalStats::HandleUnflushedImmTableTotalMemoryAllocated,
          nullptr}},
        {DB::Properties::kUnflushedImmTableTotalMemoryUsed,
         {false, nullptr,
          &InternalStats::HandleUnflushedImmTableTotalMemoryUsed, nullptr}},
        {DB::Properties::kTableReaderTotalNumber,
         {false, nullptr, &InternalStats::HandleTableReaderTotalNumber,
          nullptr}},
        {DB::Properties::kTableReaderTotalMemoryUsed,
         {false, nullptr, &InternalStats::HandleTableReaderTotalMemoryUsed,
          nullptr}},
        {DB::Properties::kBlockCacheTotalPinnedMemory,
         {false, nullptr, &InternalStats::HandleBlockCacheTotalPinnedMemory,
          nullptr}},
        {DB::Properties::kBlockCacheTotalMemoryUsed,
         {false, nullptr, &InternalStats::HandleBlockCacheTotalMemoryUsed,
          nullptr}},
};

const DBPropertyInfo* GetPropertyInfo(const Slice& property) {
  std::string ppt_name = GetPropertyNameAndArg(property).first.ToString();
  auto ppt_info_iter = InternalStats::ppt_name_to_info.find(ppt_name);
  if (ppt_info_iter == InternalStats::ppt_name_to_info.end()) {
    return nullptr;
  }
  return &ppt_info_iter->second;
}

bool InternalStats::GetStringProperty(const DBPropertyInfo& property_info,
                                      const Slice& property, std::string* value,
                                      DBImpl* db) {
  assert(value != nullptr);
  assert(property_info.handle_string != nullptr);
  Slice arg = GetPropertyNameAndArg(property).second;
  return (this->*(property_info.handle_string))(value, arg, db);
}

bool InternalStats::GetMapProperty(const DBPropertyInfo& property_info,
                                   const Slice& property,
                                   std::map<std::string, double>* value) {
  assert(value != nullptr);
  assert(property_info.handle_map != nullptr);
  return (this->*(property_info.handle_map))(value);
}

bool InternalStats::GetIntProperty(const DBPropertyInfo& property_info,
                                   uint64_t* value, DBImpl* db) {
  assert(value != nullptr);
  assert(property_info.handle_int != nullptr &&
         !property_info.need_out_of_mutex);
  db->mutex_.AssertHeld();
  return (this->*(property_info.handle_int))(value, db);
}

bool InternalStats::GetIntPropertyOutOfMutex(
    const DBPropertyInfo& property_info, uint64_t* value) {
  assert(value != nullptr);
  assert(property_info.handle_int != nullptr &&
         property_info.need_out_of_mutex);
  return (this->*(property_info.handle_int))(value, nullptr /* db */);
}

bool InternalStats::HandleNumExtentsAtLevel(std::string* value, Slice suffix,
                                            DBImpl* db) {
  uint64_t level = 0;
  bool ok = ConsumeDecimalNumber(&suffix, &level) && suffix.empty();
  if (!ok || static_cast<int>(level) >= storage::MAX_TIER_COUNT) {
    return true;
  } else {
    xengine::storage::StorageManager* storage_manager =
                                            cfd_->get_storage_manager();
    // db_mutex is locked outside
    const Snapshot* sn = cfd_->get_meta_snapshot();
    if (nullptr == sn) {
      return false;
    } 
    int32_t extents = 0;
    Arena arena;
    std::unique_ptr<InternalIterator, memory::ptr_destruct<InternalIterator>> iter;
    ReadOptions read_options;
    iter.reset(storage_manager->get_single_level_iterator(read_options, &arena,
                                                          sn, level));
    if (iter != nullptr) {
      iter->SeekToFirst();
      while (iter->Valid()) {
        extents++;
        iter->Next();
      }
    }
    cfd_->release_meta_snapshot(sn);
    char buf[100];
    snprintf(buf, sizeof(buf), "%d", extents);
    *value = buf;
    return true;
  }
}

bool InternalStats::HandleCompressionRatioAtLevelPrefix(std::string* value,
                                                        Slice suffix,
                                                        DBImpl* db) {
  return true;
}

bool InternalStats::HandleLevelStats(std::string* value, Slice suffix,
                                     DBImpl* db) {
  int ret = Status::kOk;
  char buf[1000];
  snprintf(buf, sizeof(buf),
           "Level Extents Size(MB)\n"
           "--------------------\n");
  value->append(buf);

  xengine::storage::StorageManager *storage_manager =
                                    cfd_->get_storage_manager();
  // db_mutex is locked outside
  const Snapshot *sn = cfd_->get_meta_snapshot();
  if (nullptr == sn) {
    return false;
  }
  
  int32_t extents_number = 0;
  int32_t extents_size = 0;

  Arena arena;
  std::unique_ptr<InternalIterator, memory::ptr_destruct<InternalIterator>> iter;
  ReadOptions read_options;
  ExtentMeta *extent_meta = nullptr;
  Slice key_slice;

  for (int32_t level = 0; level < storage::MAX_TIER_COUNT; level++) {
    iter.reset(storage_manager->get_single_level_iterator(read_options, &arena, sn, level));
    extents_number = 0;
    extents_size = 0; 
    if (iter != nullptr) {
      iter->SeekToFirst();
      while (SUCCED(ret) && iter->Valid()) {
        key_slice = iter->key();
        if (IS_NULL(extent_meta = reinterpret_cast<ExtentMeta *>(const_cast<char *>(key_slice.data())))) {
          ret = Status::kErrorUnexpected;
          XENGINE_LOG(WARN, "unexpected error, extent meta should not nullptr", K(ret));
        } else {
          extents_size += extent_meta->data_size_;
          extents_number++;
        }
        iter->Next();
      }
    }
    snprintf(buf, sizeof(buf), "%3d %8du %8.0f\n", 0,
           extents_number, extents_size / kMB);
    value->append(buf);
  }
	iter.reset(); 
  cfd_->release_meta_snapshot(sn); 
  return true;
}

bool InternalStats::HandleStats(std::string* value, Slice suffix, DBImpl* db) {
if (!HandleCFStats(value, suffix, nullptr)) {
return false;
}
if (!HandleDBStats(value, suffix, nullptr)) {
return false;
}
  return true;
}

bool InternalStats::HandleCFMapStats(std::map<std::string, double>* cf_stats) {
  DumpCFMapStats(cf_stats);
  return true;
}

bool InternalStats::HandleCFStats(std::string* value, Slice suffix,
                                  DBImpl* db) {
  DumpCFStats(value, db);
  return true;
}

bool InternalStats::HandleCFStatsNoFileHistogram(std::string* value,
                                                 Slice suffix, DBImpl* db) {
  // DumpCFStatsNoFileHistogram(value);
  dump_cfstats_nofile_histogram(value);
  return true;
}

bool InternalStats::HandleCFFileHistogram(std::string* value, Slice suffix,
                                          DBImpl* db) {
  DumpCFFileHistogram(value);
  return true;
}

bool InternalStats::HandleDBStats(std::string* value, Slice suffix,
                                  DBImpl* db) {
  DumpDBStats(value);
  return true;
}

bool InternalStats::HandleMeta(std::string* value, Slice suffix,
    DBImpl* db) {
 storage::StorageManager* storage_manager = cfd_->get_storage_manager();
 assert(storage_manager != nullptr);
 storage_manager->print_raw_meta();//print to log
 return true;
}

//TODO: @yuanfeng unused code, delete future
bool InternalStats::HandleSsTables(std::string* value, Slice suffix,
                                   DBImpl* db) {
  //auto* current = cfd_->current();
  //*value = current->DebugString();
  return true;
}

//TODO: @yuanfeng unused code, delete future
bool InternalStats::HandleAggregatedTableProperties(std::string* value,
                                                    Slice suffix, DBImpl* db) {
  std::shared_ptr<const TableProperties> tp;
  /*
  auto s = cfd_->current()->GetAggregatedTableProperties(&tp);
  if (!s.ok()) {
    return false;
  }
  *value = tp->ToString();
  */
  return true;
}

bool InternalStats::HandleAggregatedTablePropertiesAtLevel(std::string* value,
                                                           Slice suffix,
                                                           DBImpl* db) {
  uint64_t level;
  bool ok = ConsumeDecimalNumber(&suffix, &level) && suffix.empty();
  if (!ok || static_cast<int>(level) >= number_levels_) {
    return false;
  }
  std::shared_ptr<const TableProperties> tp;
  /*
  auto s = cfd_->current()->GetAggregatedTableProperties(
      &tp, static_cast<int>(level));
  if (!s.ok()) {
    return false;
  }
  *value = tp->ToString();
  */
  return true;
}

bool InternalStats::HandleNumImmutableMemTable(uint64_t* value, DBImpl* db)
{
  *value = cfd_->imm()->NumNotFlushed();
  return true;
}

bool InternalStats::HandleNumImmutableMemTableFlushed(uint64_t* value, DBImpl* db)
{
  *value = cfd_->imm()->NumFlushed();
  return true;
}

bool InternalStats::HandleMemTableFlushPending(uint64_t* value, DBImpl* db)
{
  // Return number of mem tables that are ready to flush (made immutable)
  *value = (cfd_->imm()->IsFlushPending() ? 1 : 0);
  return true;
}

bool InternalStats::HandleNumRunningFlushes(uint64_t* value, DBImpl* db)
{
  *value = db->num_running_flushes();
  return true;
}

bool InternalStats::HandleCompactionPending(uint64_t* value, DBImpl* db)
{
  // 1 if the system already determines at least one compaction is needed.
  // 0 otherwise,
  *value = (cfd_->pending_compaction() ? 1 : 0);
  return true;
}

bool InternalStats::HandleNumRunningCompactions(uint64_t* value, DBImpl* db)
{
  *value = db->num_running_compactions_;
  return true;
}

bool InternalStats::HandleBackgroundErrors(uint64_t* value, DBImpl* db)
{
  // Accumulated number of  errors in background flushes or compactions.
  *value = GetBackgroundErrorCount();
  return true;
}

bool InternalStats::HandleCurSizeActiveMemTable(uint64_t* value, DBImpl* db)
{
  // Current size of the active memtable
  *value = cfd_->mem()->ApproximateMemoryUsage();
  return true;
}

bool InternalStats::HandleCurSizeAllMemTables(uint64_t* value, DBImpl* db)
{
  // Current size of the active memtable + immutable memtables
  *value = cfd_->mem()->ApproximateMemoryUsage() +
           cfd_->imm()->ApproximateUnflushedMemTablesMemoryUsage();
  return true;
}

bool InternalStats::HandleSizeAllMemTables(uint64_t* value, DBImpl* db)
{
  *value = cfd_->mem()->ApproximateMemoryUsage() +
           cfd_->imm()->ApproximateMemoryUsage();
  return true;
}

bool InternalStats::HandleNumEntriesActiveMemTable(uint64_t* value, DBImpl* db)
{
  // Current number of entires in the active memtable
  *value = cfd_->mem()->num_entries();
  return true;
}

bool InternalStats::HandleNumEntriesImmMemTables(uint64_t* value, DBImpl* db)
{
  // Current number of entries in the immutable memtables
  *value = cfd_->imm()->current()->GetTotalNumEntries();
  return true;
}

bool InternalStats::HandleNumDeletesActiveMemTable(uint64_t* value, DBImpl* db)
{
  // Current number of entires in the active memtable
  *value = cfd_->mem()->num_deletes();
  return true;
}

bool InternalStats::HandleNumDeletesImmMemTables(uint64_t* value, DBImpl* db)
{
  // Current number of entries in the immutable memtables
  *value = cfd_->imm()->current()->GetTotalNumDeletes();
  return true;
}

bool InternalStats::HandleEstimateNumKeys(uint64_t* value, DBImpl* db) {
  // Estimate number of entries in the column family:
  // Use estimated entries in tables + total entries in memtables.
  xengine::storage::StorageManager* storage_manager =
      cfd_->get_storage_manager();
  *value = cfd_->mem()->num_entries() +
           cfd_->imm()->current()->GetTotalNumEntries() -
           (cfd_->mem()->num_deletes() +
            cfd_->imm()->current()->GetTotalNumDeletes()) *
               2/* +
           storage_manager->get_approximate_keys()*/;
  //TODO:yuanfeng
  return true;
}

bool InternalStats::HandleNumSnapshots(uint64_t* value, DBImpl* db)
{
  *value = db->snapshots_count();
  return true;
}

bool InternalStats::HandleOldestSnapshotTime(uint64_t* value, DBImpl* db)
{
  *value = static_cast<uint64_t>(db->GetOldestSnapshotTime());
  return true;
}

bool InternalStats::HandleNumLiveVersions(uint64_t* value, DBImpl* db)
{
  //TODO:yuanfeng
  //*value = cfd_->GetNumLiveVersions();
  return true;
}

bool InternalStats::HandleCurrentSuperVersionNumber(uint64_t* value, DBImpl* db)
{
  *value = cfd_->GetSuperVersionNumber();
  return true;
}

bool InternalStats::HandleIsFileDeletionsEnabled(uint64_t* value, DBImpl* db)
{
  *value = db->IsFileDeletionsEnabled();
  return true;
}

bool InternalStats::HandleBaseLevel(uint64_t* value, DBImpl* db)
{
  return true;
}

bool InternalStats::HandleTotalSstFilesSize(uint64_t* value, DBImpl* db)
{
  return true;
}

bool InternalStats::HandleEstimatePendingCompactionBytes(uint64_t* value,
                                                         DBImpl* db) {
  return true;
}

bool InternalStats::HandleEstimateTableReadersMem(uint64_t* value, DBImpl* db)
{
  return true;
}

bool InternalStats::HandleEstimateLiveDataSize(uint64_t* value, DBImpl* db)
{
  return true;
}

bool InternalStats::HandleMinLogNumberToKeep(uint64_t* value, DBImpl* db)
{
  *value = db->MinLogNumberToKeep();
  return true;
}

bool InternalStats::HandleActualDelayedWriteRate(uint64_t* value, DBImpl* db)
{
  const WriteController& wc = db->write_controller();
  if (!wc.NeedsDelay()) {
    *value = 0;
  } else {
    *value = wc.delayed_write_rate();
  }
  return true;
}

bool InternalStats::HandleIsWriteStopped(uint64_t* value, DBImpl* db)
{
  *value = db->write_controller().IsStopped() ? 1 : 0;
  return true;
}
bool InternalStats::HandleDBMemoryStats(std::string* value, Slice suffix,
                                        DBImpl* db) {
  DumpMemoryStats(value);
  return true;
}
void InternalStats::DumpMemoryStats(std::string* value) {}
bool InternalStats::HandleActiveMemTableTotalNumber(uint64_t* value, DBImpl* db)
{
  // currently eatch columnfamily has only one active memory table
  *value = 1;
  return true;
}

bool InternalStats::HandleActiveMemTableTotalMemoryAllocated(uint64_t* value,
                                                             DBImpl* db)
{
  // Current size of the active memtable
  *value = cfd_->mem()->ApproximateMemoryAllocated();
  return true;
}

bool InternalStats::HandleActiveMemTableTotalMemoryUsed(uint64_t* value,
                                                        DBImpl* db)
{
  // Current size of the active memtable
  *value = cfd_->mem()->ApproximateMemoryUsage();
  return true;
}

bool InternalStats::HandleUnflushedImmTableTotalNumber(uint64_t* value,
                                                       DBImpl* db)
{
  *value = cfd_->imm()->NumNotFlushed();
  return true;
}

bool InternalStats::HandleUnflushedImmTableTotalMemoryAllocated(
    uint64_t* value, DBImpl* db) {
  *value = cfd_->imm()->ApproximateUnflushedMemTablesMemoryAllocated();
  return true;
}
bool InternalStats::HandleUnflushedImmTableTotalMemoryUsed(uint64_t* value,
                                                           DBImpl* db)
{
  *value = cfd_->imm()->ApproximateUnflushedMemTablesMemoryUsage();
  return true;
}
bool InternalStats::HandleTableReaderTotalNumber(uint64_t* value, DBImpl* db)
{
  //TODO:yuanfeng
  //*value = cfd_->get_storage_manager()->get_table_reader_number();
  return true;
}
bool InternalStats::HandleTableReaderTotalMemoryUsed(uint64_t* value,
                                                     DBImpl* db)
{
  //TODO:yuanfeng
  //*value =
  //    cfd_->get_storage_manager()->get_approximate_memory_used_by_table_cache();
  return true;
}
bool InternalStats::HandleBlockCacheTotalPinnedMemory(uint64_t* value,
																											DBImpl* db)
{
  *value = 0;
  const ImmutableCFOptions* ioptions = cfd_->ioptions();
  auto* table_factory = ioptions->table_factory;
  if (nullptr == table_factory) {
    return true;
  }
  const BlockBasedTableOptions* const bbt_opt =
      reinterpret_cast<BlockBasedTableOptions*>(table_factory->GetOptions());
  if (nullptr == bbt_opt || nullptr == bbt_opt->block_cache.get()) {
    return true;
  }
  *value = bbt_opt->block_cache.get()->GetPinnedUsage();
  return true;
}
bool InternalStats::HandleBlockCacheTotalMemoryUsed(uint64_t* value, DBImpl* db) {
  *value = 0;
  const ImmutableCFOptions* ioptions = cfd_->ioptions();
  auto* table_factory = ioptions->table_factory;
  if (nullptr == table_factory) {
    return true;
  }
  const BlockBasedTableOptions* const bbt_opt =
      reinterpret_cast<BlockBasedTableOptions*>(table_factory->GetOptions());
  if (nullptr == bbt_opt || nullptr == bbt_opt->block_cache.get()) {
    return true;
  }
  *value = bbt_opt->block_cache.get()->GetUsage();
  return true;
}

void InternalStats::DumpDBStats(std::string* value) {
  char buf[1000];
  // DB-level stats, only available from default column family
  double seconds_up = (env_->NowMicros() - started_at_ + 1) / kMicrosInSec;
  double interval_seconds_up = seconds_up - db_stats_snapshot_.seconds_up;
  snprintf(buf, sizeof(buf),
           "\n** DB Stats **\nUptime(secs): %.1f total, %.1f interval\n",
           seconds_up, interval_seconds_up);
  value->append(buf);
  // Cumulative
  uint64_t user_bytes_written = GetDBStats(InternalStats::BYTES_WRITTEN);
  uint64_t num_keys_written = GetDBStats(InternalStats::NUMBER_KEYS_WRITTEN);
  uint64_t write_other = GetDBStats(InternalStats::WRITE_DONE_BY_OTHER);
  uint64_t write_self = GetDBStats(InternalStats::WRITE_DONE_BY_SELF);
  uint64_t wal_bytes = GetDBStats(InternalStats::WAL_FILE_BYTES);
  uint64_t wal_synced = GetDBStats(InternalStats::WAL_FILE_SYNCED);
  uint64_t write_with_wal = GetDBStats(InternalStats::WRITE_WITH_WAL);
  uint64_t write_stall_micros = GetDBStats(InternalStats::WRITE_STALL_MICROS);

  const int kHumanMicrosLen = 32;
  char human_micros[kHumanMicrosLen];

  // Data
  // writes: total number of write requests.
  // keys: total number of key updates issued by all the write requests
  // commit groups: number of group commits issued to the DB. Each group can
  //                contain one or more writes.
  // so writes/keys is the average number of put in multi-put or put
  // writes/groups is the average group commit size.
  //
  // The format is the same for interval stats.
  snprintf(buf, sizeof(buf),
           "Cumulative writes: %s writes, %s keys, %s commit groups, "
           "%.1f writes per commit group, ingest: %.2f GB, %.2f MB/s\n",
           NumberToHumanString(write_other + write_self).c_str(),
           NumberToHumanString(num_keys_written).c_str(),
           NumberToHumanString(write_self).c_str(),
           (write_other + write_self) / static_cast<double>(write_self + 1),
           user_bytes_written / kGB, user_bytes_written / kMB / seconds_up);
  value->append(buf);
  // WAL
  snprintf(buf, sizeof(buf),
           "Cumulative WAL: %s writes, %s syncs, "
           "%.2f writes per sync, written: %.2f GB, %.2f MB/s\n",
           NumberToHumanString(write_with_wal).c_str(),
           NumberToHumanString(wal_synced).c_str(),
           write_with_wal / static_cast<double>(wal_synced + 1),
           wal_bytes / kGB, wal_bytes / kMB / seconds_up);
  value->append(buf);
  // Stall
  AppendHumanMicros(write_stall_micros, human_micros, kHumanMicrosLen, true);
  snprintf(buf, sizeof(buf), "Cumulative stall: %s, %.1f percent\n",
           human_micros,
           // 10000 = divide by 1M to get secs, then multiply by 100 for pct
           write_stall_micros / 10000.0 / std::max(seconds_up, 0.001));
  value->append(buf);

  // Interval
  uint64_t interval_write_other = write_other - db_stats_snapshot_.write_other;
  uint64_t interval_write_self = write_self - db_stats_snapshot_.write_self;
  uint64_t interval_num_keys_written =
      num_keys_written - db_stats_snapshot_.num_keys_written;
  snprintf(
      buf, sizeof(buf),
      "Interval writes: %s writes, %s keys, %s commit groups, "
      "%.1f writes per commit group, ingest: %.2f MB, %.2f MB/s\n",
      NumberToHumanString(interval_write_other + interval_write_self).c_str(),
      NumberToHumanString(interval_num_keys_written).c_str(),
      NumberToHumanString(interval_write_self).c_str(),
      static_cast<double>(interval_write_other + interval_write_self) /
          (interval_write_self + 1),
      (user_bytes_written - db_stats_snapshot_.ingest_bytes) / kMB,
      (user_bytes_written - db_stats_snapshot_.ingest_bytes) / kMB /
          std::max(interval_seconds_up, 0.001)),
      value->append(buf);

  uint64_t interval_write_with_wal =
      write_with_wal - db_stats_snapshot_.write_with_wal;
  uint64_t interval_wal_synced = wal_synced - db_stats_snapshot_.wal_synced;
  uint64_t interval_wal_bytes = wal_bytes - db_stats_snapshot_.wal_bytes;

  snprintf(
      buf, sizeof(buf),
      "Interval WAL: %s writes, %s syncs, "
      "%.2f writes per sync, written: %.2f MB, %.2f MB/s\n",
      NumberToHumanString(interval_write_with_wal).c_str(),
      NumberToHumanString(interval_wal_synced).c_str(),
      interval_write_with_wal / static_cast<double>(interval_wal_synced + 1),
      interval_wal_bytes / kGB,
      interval_wal_bytes / kMB / std::max(interval_seconds_up, 0.001));
  value->append(buf);

  // Stall
  AppendHumanMicros(write_stall_micros - db_stats_snapshot_.write_stall_micros,
                    human_micros, kHumanMicrosLen, true);
  snprintf(buf, sizeof(buf), "Interval stall: %s, %.1f percent\n", human_micros,
           // 10000 = divide by 1M to get secs, then multiply by 100 for pct
           (write_stall_micros - db_stats_snapshot_.write_stall_micros) /
               10000.0 / std::max(interval_seconds_up, 0.001));
  value->append(buf);

  db_stats_snapshot_.seconds_up = seconds_up;
  db_stats_snapshot_.ingest_bytes = user_bytes_written;
  db_stats_snapshot_.write_other = write_other;
  db_stats_snapshot_.write_self = write_self;
  db_stats_snapshot_.num_keys_written = num_keys_written;
  db_stats_snapshot_.wal_bytes = wal_bytes;
  db_stats_snapshot_.wal_synced = wal_synced;
  db_stats_snapshot_.write_with_wal = write_with_wal;
  db_stats_snapshot_.write_stall_micros = write_stall_micros;
}

/**
 * Dump Compaction Level stats to a map of stat name to value in double.
 * The level in stat name is represented with a prefix "Lx" where "x"
 * is the level number. A special level "Sum" represents the sum of a stat
 * for all levels.
 */
void InternalStats::DumpCFMapStats(std::map<std::string, double>* cf_stats) {
  CompactionStats compaction_stats_sum(0);
  std::map<int, std::map<ExtentLevelStatType, double>> levels_stats;
  dump_cfmap_stats(&levels_stats, &compaction_stats_sum);
  for (auto const& level_ent : levels_stats) {
    auto level_str = "L" + ToString(level_ent.first);
    for (auto const& stat_ent : level_ent.second) {
      auto stat_type = stat_ent.first;
      auto key_str =
          level_str + "." +
          InternalStats::extent_compaction_stats.at(stat_type).property_name;
      (*cf_stats)[key_str] = stat_ent.second;
    }
  }
}

void InternalStats::dump_cfmap_stats(
    std::map<int, std::map<ExtentLevelStatType, double>>* level_stats,
    CompactionStats* compaction_stats_sum) {
  int ret = Status::kOk;
  // count the extents
  storage::StorageManager* storage_manager = cfd_->get_storage_manager();
  // locked outside
  const Snapshot *sn = cfd_->get_meta_snapshot();  
  if (nullptr == sn) {
    return;
  }

  uint64_t level0_extents = 0;
  uint64_t level1_extents = 0;
  uint64_t level0_data_size = 0;
  uint64_t level0_index_size = 0;
  uint64_t level1_data_size = 0;
  uint64_t level1_index_size = 0;
  uint64_t level2_extents = 0;
  uint64_t level2_data_size = 0;
  uint64_t level2_index_size = 0;
  uint64_t level0_layers = 0;

  Arena arena;
  std::unique_ptr<InternalIterator, memory::ptr_destruct<InternalIterator>> iter;
  ReadOptions read_options;
  ExtentMeta *extent_meta = nullptr;
  Slice key_slice;

  for (int32_t level = 0; level < storage::MAX_TIER_COUNT; level++) {
    iter.reset(storage_manager->get_single_level_iterator(read_options, &arena, sn, level));
    if (iter != nullptr) {
      iter->SeekToFirst();
      while (SUCCED(ret) && iter->Valid()) {
        key_slice = iter->key();
        if (IS_NULL(extent_meta = reinterpret_cast<ExtentMeta *>(const_cast<char *>(key_slice.data())))) {
          ret = Status::kErrorUnexpected;
          XENGINE_LOG(WARN, "unexpected error, extent meta should not been nullptr", K(ret));
        } else if (0 == level) {
          level0_extents++;
          level0_data_size += extent_meta->data_size_;
          level0_index_size += extent_meta->index_size_;
        } else if (1 == level) {
          level1_extents++;
          level1_data_size += extent_meta->data_size_;
          level1_index_size += extent_meta->index_size_;
        } else {
          level2_extents++;
          level2_data_size += extent_meta->data_size_;
          level2_index_size += extent_meta->index_size_;
        } 
        iter->Next();
      }
    }
  }

  if (level0_extents > 0 && sn->get_extent_layer_version(0) != nullptr) {
    level0_layers = sn->get_extent_layer_version(0)->get_extent_layer_size();
  }

  cfd_->release_meta_snapshot(sn);

  std::map<ExtentLevelStatType, double> level0_stats;
  std::map<ExtentLevelStatType, double> level1_stats;
  std::map<ExtentLevelStatType, double> level2_stats;
  
  uint64_t comp_l2_extents = 0;
  uint64_t comp_l1_extents =
      extent_comp_stats_.record_stats_.total_input_extents_at_l1;
  uint64_t comp_l0_extents =
      extent_comp_stats_.record_stats_.total_input_extents - comp_l1_extents - 
      comp_l2_extents;

  uint64_t reused_l2_extents = 0;
  uint64_t reused_l1_extents =
      extent_comp_stats_.record_stats_.reuse_extents_at_l1;
  uint64_t reused_l0_extents =
      extent_comp_stats_.record_stats_.reuse_extents - reused_l1_extents - 
      reused_l2_extents;

  uint64_t reused_l2_blocks = 0;
  uint64_t reused_l1_blocks =
      extent_comp_stats_.record_stats_.reuse_datablocks_at_l1;
  uint64_t reused_l0_blocks =
      extent_comp_stats_.record_stats_.reuse_datablocks - reused_l1_blocks -
      reused_l2_blocks;

  uint64_t total_output_extents =
      extent_comp_stats_.record_stats_.merge_output_extents;

  level0_stats[ExtentLevelStatType::NUM_EXTENTS] = level0_extents;
	level0_stats[ExtentLevelStatType::NUM_LAYERS] = level0_layers;
  level0_stats[ExtentLevelStatType::SIZE_BYTES] =
      level0_extents * MAX_EXTENT_SIZE;
  level0_stats[ExtentLevelStatType::DATA_BYTES] = level0_data_size;
  level0_stats[ExtentLevelStatType::INDEX_BYTES] = level0_index_size;
  level0_stats[ExtentLevelStatType::READ_GB] =
      comp_l0_extents * MAX_EXTENT_SIZE / kGB;
  level0_stats[ExtentLevelStatType::WRITE_GB] = 0;
  level0_stats[ExtentLevelStatType::MERGEOUT_RE] = 0;
  level0_stats[ExtentLevelStatType::MERGEOUT_EXT] = 0;
  level0_stats[ExtentLevelStatType::REUSED_EXT] = reused_l0_extents;
  level0_stats[ExtentLevelStatType::REUSED_BL] = reused_l0_blocks;

  level1_stats[ExtentLevelStatType::NUM_EXTENTS] = level1_extents;
	level1_stats[ExtentLevelStatType::NUM_LAYERS] =  level1_extents > 0 ? 1 : 0;
  level1_stats[ExtentLevelStatType::SIZE_BYTES] =
      level1_extents * MAX_EXTENT_SIZE;
  level1_stats[ExtentLevelStatType::DATA_BYTES] = level1_data_size;
  level1_stats[ExtentLevelStatType::INDEX_BYTES] = level1_index_size;
  level1_stats[ExtentLevelStatType::READ_GB] =
      comp_l1_extents * MAX_EXTENT_SIZE / kGB;
  level1_stats[ExtentLevelStatType::WRITE_GB] =
      total_output_extents * MAX_EXTENT_SIZE / kGB;
  level1_stats[ExtentLevelStatType::MERGEOUT_RE] =
      extent_comp_stats_.record_stats_.merge_output_records;
  level1_stats[ExtentLevelStatType::MERGEOUT_EXT] =
      extent_comp_stats_.record_stats_.merge_output_extents;
  ;
  level1_stats[ExtentLevelStatType::REUSED_EXT] = reused_l1_extents;
  level1_stats[ExtentLevelStatType::REUSED_BL] = reused_l1_blocks;

  level2_stats[ExtentLevelStatType::NUM_EXTENTS] = level2_extents;
	level2_stats[ExtentLevelStatType::NUM_LAYERS] = level2_extents > 0 ? 1 : 0;
  level2_stats[ExtentLevelStatType::SIZE_BYTES] =
      level2_extents * MAX_EXTENT_SIZE;
  level2_stats[ExtentLevelStatType::DATA_BYTES] = level2_data_size;
  level2_stats[ExtentLevelStatType::INDEX_BYTES] = level2_index_size;
  level2_stats[ExtentLevelStatType::READ_GB] =
      comp_l2_extents * MAX_EXTENT_SIZE / kGB;
  level2_stats[ExtentLevelStatType::WRITE_GB] =
      total_output_extents * MAX_EXTENT_SIZE / kGB;
  level2_stats[ExtentLevelStatType::MERGEOUT_RE] =
      extent_comp_stats_.record_stats_.merge_output_records;
  level2_stats[ExtentLevelStatType::MERGEOUT_EXT] =
      extent_comp_stats_.record_stats_.merge_output_extents;
  ;
  level2_stats[ExtentLevelStatType::REUSED_EXT] = reused_l2_extents;
  level2_stats[ExtentLevelStatType::REUSED_BL] = reused_l2_blocks;

  (*level_stats)[0] = level0_stats;
  (*level_stats)[1] = level1_stats;
  (*level_stats)[2] = level2_stats;
}

void InternalStats::DumpCFStats(std::string* value, DBImpl* db) {
  dump_cfstats_nofile_histogram(value);
//  DumpCFFileHistogram(value);
}

void InternalStats::dump_cfstats_nofile_histogram(std::string* value) {
  // char buf[2000];
  const int BUF_SIZE = 2000;
//  std::unique_ptr<char[]> heap_buf(new char[BUF_SIZE]);
//  char* buf = heap_buf.get();
  char *buf = static_cast<char *>(memory::base_malloc(BUF_SIZE));
  // Per-ColumnFamily stats
  PrintLevelStatsHeader(buf, BUF_SIZE, cfd_->GetName());
  value->append(buf);

  // Print stats for each level
  std::map<int, std::map<ExtentLevelStatType, double>> levels_stats;
  CompactionStats compaction_stats_sum(0);
  dump_cfmap_stats(&levels_stats, &compaction_stats_sum);
  for (int l = 0; l < storage::MAX_TIER_COUNT; ++l) {
    if (levels_stats.find(l) != levels_stats.end()) {
      print_level_stats(buf, BUF_SIZE, "L" + ToString(l), levels_stats[l]);
      value->append(buf);
    }
  }

  uint64_t flush_ingest = cf_stats_value_[BYTES_FLUSHED];
  uint64_t interval_flush_ingest =
      flush_ingest - cf_stats_snapshot_.ingest_bytes_flush;

  double seconds_up = (env_->NowMicros() - started_at_ + 1) / kMicrosInSec;
  double interval_seconds_up = seconds_up - cf_stats_snapshot_.seconds_up;
  snprintf(buf, BUF_SIZE, "Uptime(secs): %.1f total, %.1f interval\n",
           seconds_up, interval_seconds_up);
  value->append(buf);
  snprintf(buf, BUF_SIZE, "Flush(GB): cumulative %.3f, interval %.3f\n",
           flush_ingest / kGB, interval_flush_ingest / kGB);
  value->append(buf);
  cf_stats_snapshot_.ingest_bytes_flush = flush_ingest;
  cf_stats_snapshot_.seconds_up = seconds_up;

  // Compact
  uint64_t compact_bytes_read = extent_comp_stats_.record_stats_.total_input_bytes;
  uint64_t compact_bytes_write = extent_comp_stats_.record_stats_.total_output_bytes;
  uint64_t compact_micros = extent_comp_stats_.record_stats_.micros;
  double compact_seconds = compact_micros / kMicrosInSec;
  snprintf(buf, BUF_SIZE,
           "Cumulative compaction: %.2f GB write, %.2f MB/s write, "
           "%.2f GB read, %.2f MB/s read, %.1f seconds\n",
           compact_bytes_write / kGB, compact_bytes_write / kMB / std::max(compact_seconds,0.001),
           compact_bytes_read / kGB, compact_bytes_read / kMB / std::max(compact_seconds,0.001),
           compact_seconds);
  value->append(buf);

  // Compaction interval
  uint64_t interval_compact_bytes_write =
      compact_bytes_write - cf_stats_snapshot_.compact_bytes_write;
  uint64_t interval_compact_bytes_read =
      compact_bytes_read - cf_stats_snapshot_.compact_bytes_read;
  double interval_compact_seconds =
      (compact_micros - cf_stats_snapshot_.compact_micros) / kMicrosInSec;

  snprintf(
      buf, BUF_SIZE,
      "Interval compaction: %.2f GB write, %.2f MB/s write, "
      "%.2f GB read, %.2f MB/s read, %.1f seconds\n",
      interval_compact_bytes_write / kGB,
      interval_compact_bytes_write / kMB / std::max(interval_compact_seconds, 0.001),
      interval_compact_bytes_read / kGB,
      interval_compact_bytes_read / kMB / std::max(interval_compact_seconds, 0.001),
      interval_compact_seconds);
  value->append(buf);
  cf_stats_snapshot_.compact_bytes_write = compact_bytes_write;
  cf_stats_snapshot_.compact_bytes_read = compact_bytes_read;
  cf_stats_snapshot_.compact_micros = compact_micros;

  uint64_t merge_input_records =
      extent_comp_stats_.record_stats_.merge_input_records;
  uint64_t merge_output_records =
      extent_comp_stats_.record_stats_.merge_output_records;
  uint64_t merge_input_extents = extent_comp_stats_.record_stats_.merge_extents;
  uint64_t merge_output_extents =
      extent_comp_stats_.record_stats_.merge_output_extents;
  uint64_t merge_input_datablocks =
      extent_comp_stats_.record_stats_.merge_datablocks;
  uint64_t merge_input_keys =
      extent_comp_stats_.record_stats_.merge_input_raw_key_bytes;
  uint64_t merge_input_values =
      extent_comp_stats_.record_stats_.merge_input_raw_value_bytes;
  uint64_t merge_replace_records =
      extent_comp_stats_.record_stats_.merge_replace_records;
  uint64_t merge_delete_records =
      extent_comp_stats_.record_stats_.merge_delete_records;
  uint64_t merge_expired_records =
      extent_comp_stats_.record_stats_.merge_expired_records;
  uint64_t merge_corrupt_keys =
      extent_comp_stats_.record_stats_.merge_corrupt_keys;
  uint64_t reuse_extents = extent_comp_stats_.record_stats_.reuse_extents;
  uint64_t reuse_datablocks = extent_comp_stats_.record_stats_.reuse_datablocks;
  uint64_t single_delete_fallthru =
      extent_comp_stats_.record_stats_.single_del_fallthru;
  uint64_t single_delete_mismatch =
      extent_comp_stats_.record_stats_.single_del_mismatch;

  snprintf(buf, BUF_SIZE,
           "merge: input records %" PRIu64 ", output records %" PRIu64
           ", input extents %" PRIu64 ", output extents %" PRIu64
           ", input datablocks %" PRIu64
           ", input keys %.2f MB, input value %.2f MB"
           ", replace records %" PRIu64 ", delete records %" PRIu64
           ", expired records %" PRIu64 ", corrupt keys %" PRIu64
           "\n"
           "reuse: extents %" PRIu64 ", datablocks %" PRIu64
           "\n"
           "singledelete: fallthru %" PRIu64 ", mismatch %" PRIu64 "\n",
           merge_input_records, merge_output_records, merge_input_extents,
           merge_output_extents, merge_input_datablocks, merge_input_keys / kMB,
           merge_input_values / kMB, merge_replace_records,
           merge_delete_records, merge_expired_records, merge_corrupt_keys,
           reuse_extents, reuse_datablocks, single_delete_fallthru,
           single_delete_mismatch);
  value->append(buf);
  memory::base_free(buf);
}

void InternalStats::DumpCFFileHistogram(std::string* value) {
  char buf[2000];
  snprintf(buf, sizeof(buf),
           "\n** File Read Latency Histogram By Level [%s] **\n",
           cfd_->GetName().c_str());
  value->append(buf);

  for (int level = 0; level < 0/*no use*/; level++) {
 //   if (!file_read_latency_[level].Empty()) {
//      char buf2[5000];
//      snprintf(buf2, sizeof(buf2),
//               "** Level %d read latency histogram (micros):\n%s\n", level,
//               file_read_latency_[level].ToString().c_str());
//      value->append(buf2);
//    }
  }
}

#else

const DBPropertyInfo* GetPropertyInfo(const Slice& property) { return nullptr; }

#endif  // !ROCKSDB_LITE
}
}  // namespace xengine
