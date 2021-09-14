// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// Thread-safe (provides internal synchronization)

#pragma once
#include <stdint.h>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "db/range_del_aggregator.h"
#include "options/cf_options.h"
#include "port/port.h"
#include "table/table_reader.h"
#include "xengine/cache.h"
#include "xengine/env.h"
#include "xengine/options.h"
#include "xengine/table.h"

namespace xengine {

namespace storage {
class ExtentSpaceManager;
}

namespace env {
class Env;
}

namespace util {
class Arena;
}

namespace table {
class GetContext;
class InternalIterator;
}

namespace monitoring {
class HistogramImpl;
}

namespace db {
struct FileDescriptor;
class InternalStats;

class TableCache {
 public:
  TableCache(const common::ImmutableCFOptions& ioptions,
             const util::EnvOptions& storage_options, cache::Cache* cache,
             storage::ExtentSpaceManager* space_manager);
  ~TableCache();

  // Return an iterator for the specified file number (the corresponding
  // file length must be exactly "file_size" bytes).  If "tableptr" is
  // non-nullptr, also sets "*tableptr" to point to the Table object
  // underlying the returned iterator, or nullptr if no Table object underlies
  // the returned iterator.  The returned "*tableptr" object is owned by
  // the cache and should not be deleted, and is valid for as long as the
  // returned iterator is live.
  // @param range_del_agg If non-nullptr, adds range deletions to the
  //    aggregator. If an error occurs, returns it in a NewErrorInternalIterator
  // @param skip_filters Disables loading/accessing the filter block
  // @param level The level this table is at, -1 for "not set / don't know"
  table::InternalIterator* NewIterator(
      const common::ReadOptions& options, const util::EnvOptions& toptions,
      const InternalKeyComparator& internal_comparator,
      const FileDescriptor& file_fd, db::RangeDelAggregator* range_del_agg,
      table::TableReader** table_reader_ptr = nullptr,
      monitor::HistogramImpl* file_read_hist = nullptr,
      bool for_compaction = false, memory::SimpleAllocator* arena = nullptr,
      bool skip_filters = false, int level = -1,
      InternalStats *internal_stats = nullptr,
      const uint64_t scan_add_blocks_limit = 0);

  // If a seek to internal key "k" in specified file finds an entry,
  // call (*handle_result)(arg, found_key, found_value) repeatedly until
  // it returns false.
  // @param get_context State for get operation. If its range_del_agg() returns
  //    non-nullptr, adds range deletions to the aggregator. If an error occurs,
  //    returns non-ok status.
  // @param skip_filters Disables loading/accessing the filter block
  // @param level The level this table is at, -1 for "not set / don't know"
  common::Status Get(const common::ReadOptions& options,
                     const InternalKeyComparator& internal_comparator,
                     const FileDescriptor& file_fd, const common::Slice& k,
                     table::GetContext* get_context,
                     monitor::HistogramImpl* file_read_hist = nullptr,
                     bool skip_filters = false, int level = -1);

  // Evict any entry for the specified file number
  static void Evict(cache::Cache* cache, uint64_t file_number);

  // Clean table handle and erase it from the table cache
  // Used in DB close, or the file is not live anymore.
  void EraseHandle(const FileDescriptor& fd, cache::Cache::Handle* handle);

  // Find table reader
  // @param skip_filters Disables loading/accessing the filter block
  // @param level == -1 means not specified
  common::Status FindTable(const util::EnvOptions& toptions,
                           const InternalKeyComparator& internal_comparator,
                           const FileDescriptor& file_fd,
                           cache::Cache::Handle**, const bool no_io = false,
                           bool record_read_stats = true,
                           monitor::HistogramImpl* file_read_hist = nullptr,
                           bool skip_filters = false, int level = -1,
                           bool prefetch_index_and_filter_in_cache = true);

  // Get TableReader from a cache handle.
  table::TableReader* GetTableReaderFromHandle(cache::Cache::Handle* handle);

  // Get the table properties of a given table.
  // @no_io: indicates if we should load table to the cache if it is not present
  //         in table cache yet.
  // @returns: `properties` will be reset on success. Please note that we will
  //            return common::Status::Incomplete() if table is not present in
  //            cache and
  //            we set `no_io` to be true.
  common::Status GetTableProperties(
      const util::EnvOptions& toptions,
      const InternalKeyComparator& internal_comparator,
      const FileDescriptor& file_meta,
      std::shared_ptr<const table::TableProperties>* properties,
      bool no_io = false);

  // Return total memory usage of the table reader of the file.
  // 0 if table reader of the file is not loaded.
  size_t GetMemoryUsageByTableReader(
      const util::EnvOptions& toptions,
      const InternalKeyComparator& internal_comparator,
      const FileDescriptor& fd);

  // Return Approximate memory usage by table reader
  // 0  if table readler of the file is not loaded
  static uint64_t get_approximate_memory_usage_by_id(cache::Cache* cache,
                                                     uint64_t file_number);

  // Release the handle from a cache
  void ReleaseHandle(cache::Cache::Handle* handle);

 private:
  // Build a table reader
  common::Status GetTableReader(
      const util::EnvOptions& env_options,
      const InternalKeyComparator& internal_comparator,
      const FileDescriptor& fd, bool sequential_mode, size_t readahead,
      bool record_read_stats, monitor::HistogramImpl* file_read_hist,
      table::TableReader *&table_reader, bool skip_filters = false,
      int level = -1, bool prefetch_index_and_filter_in_cache = true);

  const common::ImmutableCFOptions& ioptions_;
  const util::EnvOptions& env_options_;
  cache::Cache* const cache_;
  std::string row_cache_id_;
  storage::ExtentSpaceManager* space_manager_;
};
}
}  // namespace xengine
