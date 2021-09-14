// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "cache/row_cache.h"
#include "db/table_cache.h"
#include "db/dbformat.h"
#include "db/version_edit.h"
#include "util/filename.h"

#include "monitoring/query_perf_context.h"
#include "storage/extent_space_manager.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/iterator_wrapper.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
#include "util/coding.h"
#include "util/file_reader_writer.h"
#include "util/stop_watch.h"
#include "util/sync_point.h"
#include "xengine/statistics.h"

using namespace xengine;
using namespace table;
using namespace cache;
using namespace util;
using namespace common;
using namespace monitor;

namespace xengine {
namespace db {

namespace {

template <class T>
static void DeleteEntry(const Slice& key, void* value) {
  T* typed_value = reinterpret_cast<T*>(value);
//  delete typed_value;
  MOD_DELETE_OBJECT(T, typed_value);
}

static void UnrefEntry(void* arg1, void* arg2) {
  Cache* cache = reinterpret_cast<Cache*>(arg1);
  Cache::Handle* h = reinterpret_cast<Cache::Handle*>(arg2);
  cache->Release(h);
}

static void DeleteTableReader(void* arg1, void* arg2) {
  TableReader* table_reader = reinterpret_cast<TableReader*>(arg1);
  delete table_reader;
}

static Slice GetSliceForFileNumber(const uint64_t* file_number) {
  return Slice(reinterpret_cast<const char*>(file_number),
               sizeof(*file_number));
}

#ifndef ROCKSDB_LITE
//
// void AppendVarint64(IterKey* key, uint64_t v) {
//  char buf[10];
//  auto ptr = EncodeVarint64(buf, v);
//  key->TrimAppend(key->Size(), buf, ptr - buf);
//}

#endif  // ROCKSDB_LITE

}  // namespace

TableCache::TableCache(const ImmutableCFOptions& ioptions,
                       const EnvOptions& env_options, Cache* const cache,
                       storage::ExtentSpaceManager* space_manager)
    : ioptions_(ioptions),
      env_options_(env_options),
      cache_(cache),
      space_manager_(space_manager) {
  if (ioptions_.row_cache) {
    // If the same cache is shared by multiple instances, we need to
    // disambiguate its entries.
//    PutVarint64(&row_cache_id_, ioptions_.row_cache->NewId()); // nouse
  }
}

TableCache::~TableCache() {}

TableReader* TableCache::GetTableReaderFromHandle(Cache::Handle* handle) {
  return reinterpret_cast<TableReader*>(cache_->Value(handle));
}

void TableCache::ReleaseHandle(Cache::Handle* handle) {
  cache_->Release(handle);
}

Status TableCache::GetTableReader(
    const EnvOptions& env_options,
    const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
    bool sequential_mode, size_t readahead, bool record_read_stats,
    HistogramImpl* file_read_hist, TableReader *&table_reader,
    bool skip_filters, int level, bool prefetch_index_and_filter_in_cache) {
  storage::ExtentId eid(fd.GetNumber());
//  unique_ptr<storage::RandomAccessExtent> extent(
//      new storage::RandomAccessExtent());
  storage::RandomAccessExtent *extent = MOD_NEW_OBJECT(memory::ModId::kDefaultMod, storage::RandomAccessExtent);
  if (!extent) {
    __XENGINE_LOG(INFO, "No memory for extent in %s\n",
                   __func__);
    return Status::MemoryLimit();
  }
  Status s = space_manager_->get_random_access_extent(eid, *extent);
  if (s.ok()) {
//    std::unique_ptr<RandomAccessFileReader> file_reader(
//        new RandomAccessFileReader(
//            extent, ioptions_.env,
//            record_read_stats ? ioptions_.statistics : nullptr, SST_READ_MICROS,
//            file_read_hist, &ioptions_, env_options));
    RandomAccessFileReader *file_reader = MOD_NEW_OBJECT(memory::ModId::kDefaultMod, RandomAccessFileReader,
        extent, ioptions_.env, record_read_stats ? ioptions_.statistics : nullptr, SST_READ_MICROS,
        file_read_hist, &ioptions_, env_options);
    s = ioptions_.table_factory->NewTableReader(
        TableReaderOptions(ioptions_, env_options, internal_comparator, &fd,
                           file_read_hist, skip_filters, level),
        file_reader, fd.GetFileSize(), table_reader,
        prefetch_index_and_filter_in_cache);
    QUERY_COUNT(CountPoint::NUMBER_FILE_OPENS);
    TEST_SYNC_POINT("TableCache::GetTableReader:0");
  }
  return s;
}

void TableCache::EraseHandle(const FileDescriptor& fd, Cache::Handle* handle) {
  ReleaseHandle(handle);
  uint64_t number = fd.GetNumber();
  Slice key = GetSliceForFileNumber(&number);
  cache_->Erase(key);
}

Status TableCache::FindTable(const EnvOptions& env_options,
                             const InternalKeyComparator& internal_comparator,
                             const FileDescriptor& fd, Cache::Handle** handle,
                             const bool no_io, bool record_read_stats,
                             HistogramImpl* file_read_hist, bool skip_filters,
                             int level,
                             bool prefetch_index_and_filter_in_cache) {
  Status s;
  uint64_t number = fd.GetNumber();
  Slice key = GetSliceForFileNumber(&number);
  *handle = cache_->Lookup(key);
  TEST_SYNC_POINT_CALLBACK("TableCache::FindTable:0",
                           const_cast<bool*>(&no_io));

  if (*handle == nullptr) {
    QUERY_TRACE_SCOPE(TracePoint::LOAD_TABLE);
    if (no_io) {  // Don't do IO and return a not-found status
      return Status::Incomplete("Table not found in table_cache, no_io is set");
    }
//    unique_ptr<TableReader> table_reader;
    TableReader *table_reader = nullptr;
    s = GetTableReader(env_options, internal_comparator, fd,
                       false /* sequential mode */, 0 /* readahead */,
                       record_read_stats, file_read_hist, table_reader,
                       skip_filters, level, prefetch_index_and_filter_in_cache);
    if (!s.ok()) {
      assert(table_reader == nullptr);
      // We do not cache error results so that if the error is transient,
      // or somebody repairs the file, we recover automatically.
    } else {
      s = cache_->Insert(key, table_reader, table_reader->get_usable_size(),
                         &DeleteEntry<TableReader>, handle);
      if (s.ok()) {
        // Release ownership of table reader.
        table_reader->set_mod_id(memory::ModId::kTableCache);
//        table_reader.release();
      }
    }
  }
  return s;
}

InternalIterator* TableCache::NewIterator(
    const ReadOptions& options, const EnvOptions& env_options,
    const InternalKeyComparator& icomparator, const FileDescriptor& fd,
    RangeDelAggregator* range_del_agg, TableReader** table_reader_ptr,
    HistogramImpl* file_read_hist, bool for_compaction, memory::SimpleAllocator* arena,
    bool skip_filters, int level, InternalStats *internal_stats,
    const uint64_t scan_add_blocks_limit) {
  Status s;
  bool create_new_table_reader = false;
  TableReader* table_reader = nullptr;
  Cache::Handle* handle = nullptr;
  if (s.ok()) {
    if (table_reader_ptr != nullptr) {
      *table_reader_ptr = nullptr;
    }
    size_t readahead = 0;
    if (for_compaction) {
#ifndef NDEBUG
      bool use_direct_reads_for_compaction = env_options.use_direct_reads;
      TEST_SYNC_POINT_CALLBACK("TableCache::NewIterator:for_compaction",
                               &use_direct_reads_for_compaction);
#endif  // !NDEBUG
      if (ioptions_.new_table_reader_for_compaction_inputs) {
        readahead = ioptions_.compaction_readahead_size;
        create_new_table_reader = true;
      }
    } else {
      readahead = options.readahead_size;
      create_new_table_reader = readahead > 0;
    }

    if (create_new_table_reader) {
//      unique_ptr<TableReader> table_reader_unique_ptr;
      s = GetTableReader(
          env_options, icomparator, fd, true /* sequential_mode */, readahead,
          !for_compaction /* record stats */, nullptr, table_reader,
          false /* skip_filters */, level);
      if (s.ok()) {
//        table_reader = table_reader_unique_ptr.release();
      }
    } else {
      table_reader = fd.table_reader;
      if (table_reader == nullptr) {
        s = FindTable(env_options, icomparator, fd, &handle,
                      options.read_tier == kBlockCacheTier /* no_io */,
                      !for_compaction /* record read_stats */, file_read_hist,
                      skip_filters, level);
        if (s.ok()) {
          table_reader = GetTableReaderFromHandle(handle);
        }
      }
    }
  }
  InternalIterator* result = nullptr;
  if (s.ok()) {
    result = table_reader->NewIterator(options, arena,
        skip_filters, scan_add_blocks_limit);
    if (create_new_table_reader) {
      assert(handle == nullptr);
      result->RegisterCleanup(&DeleteTableReader, table_reader, nullptr);
    } else if (handle != nullptr) {
      result->RegisterCleanup(&UnrefEntry, cache_, handle);
      handle = nullptr;  // prevent from releasing below
    }

    if (for_compaction) {
      table_reader->SetupForCompaction();
    }
    if (table_reader_ptr != nullptr) {
      *table_reader_ptr = table_reader;
    }
  }
  if (s.ok() && range_del_agg != nullptr && !options.ignore_range_deletions) {
    InternalIterator *range_del_iter =
        table_reader->NewRangeTombstoneIterator(options);
    if (range_del_iter != nullptr) {
      s = range_del_iter->status();
    }
    if (s.ok()) {
      s = range_del_agg->AddTombstones(range_del_iter);
    }
  }

  if (handle != nullptr) {
    ReleaseHandle(handle);
  }
  if (!s.ok()) {
    assert(result == nullptr);
    result = NewErrorInternalIterator(s, arena);
  }
  return result;
}

Status TableCache::Get(const ReadOptions& options,
                       const InternalKeyComparator& internal_comparator,
                       const FileDescriptor& fd, const Slice& k,
                       GetContext* get_context, HistogramImpl* file_read_hist,
                       bool skip_filters, int level) {
  Status s;
  TableReader* t = fd.table_reader;
  Cache::Handle* handle = nullptr;
  if (t == nullptr) {
    s = FindTable(env_options_, internal_comparator, fd, &handle,
                  options.read_tier == kBlockCacheTier /* no_io */,
                  true /* record_read_stats */, file_read_hist, skip_filters,
                  level);
    if (s.ok()) {
      t = GetTableReaderFromHandle(handle);
    }
  }
  if (s.ok() && get_context->range_del_agg() != nullptr &&
      !options.ignore_range_deletions) {
    InternalIterator *range_del_iter =
        t->NewRangeTombstoneIterator(options);
    if (range_del_iter != nullptr) {
      s = range_del_iter->status();
    }
    if (s.ok()) {
      s = get_context->range_del_agg()->AddTombstones(range_del_iter);
    }
  }
  if (s.ok()) {
    s = t->Get(options, k, get_context, skip_filters);
    STRESS_CHECK_APPEND(GET_FROM_EXTENT, s.code());
    get_context->SetReplayLog(nullptr);
  } else if (options.read_tier == kBlockCacheTier && s.IsIncomplete()) {
    // Couldn't find Table in cache but treat as kFound if no_io set
    get_context->MarkKeyMayExist();
    s = Status::OK();
  }

  if (handle != nullptr) {
    ReleaseHandle(handle);
  }
  return s;
}

Status TableCache::GetTableProperties(
    const EnvOptions& env_options,
    const InternalKeyComparator& internal_comparator, const FileDescriptor& fd,
    std::shared_ptr<const TableProperties>* properties, bool no_io) {
  Status s;
  auto table_reader = fd.table_reader;
  // table already been pre-loaded?
  if (table_reader) {
    *properties = table_reader->GetTableProperties();

    return s;
  }

  Cache::Handle* table_handle = nullptr;
  s = FindTable(env_options, internal_comparator, fd, &table_handle, no_io);
  if (!s.ok()) {
    return s;
  }
  assert(table_handle);
  auto table = GetTableReaderFromHandle(table_handle);
  *properties = table->GetTableProperties();
  ReleaseHandle(table_handle);
  return s;
}

size_t TableCache::GetMemoryUsageByTableReader(
    const EnvOptions& env_options,
    const InternalKeyComparator& internal_comparator,
    const FileDescriptor& fd) {
  Status s;
  auto table_reader = fd.table_reader;
  // table already been pre-loaded?
  if (table_reader) {
    return table_reader->ApproximateMemoryUsage();
  }

  Cache::Handle* table_handle = nullptr;
  s = FindTable(env_options, internal_comparator, fd, &table_handle, true);
  if (!s.ok()) {
    return 0;
  }
  assert(table_handle);
  auto table = GetTableReaderFromHandle(table_handle);
  auto ret = table->ApproximateMemoryUsage();
  ReleaseHandle(table_handle);
  return ret;
}
uint64_t TableCache::get_approximate_memory_usage_by_id(Cache* cache,
                                                        uint64_t file_number) {
  Slice key = GetSliceForFileNumber(&file_number);
  Cache::Handle* handle = cache->Lookup(key);
  if (nullptr == handle) {
    return 0;
  }
  auto table = reinterpret_cast<TableReader*>(cache->Value(handle));
  auto ret = sizeof(TableReader) + table->ApproximateMemoryUsage();
  cache->Release(handle);
  return ret;
}
void TableCache::Evict(Cache* cache, uint64_t file_number) {
  cache->Erase(GetSliceForFileNumber(&file_number));
}
}  // namespace db
}  // namespace xengine
