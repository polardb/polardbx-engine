/*
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include "db/table_cache.h"
#include "storage/io_extent.h"
#include "table/extent_table_reader.h"

namespace xengine
{
namespace db
{
class RangeDelAggregator;
}
namespace cache
{
class Cache;
}
namespace storage
{
class ExtentLayer;
}
namespace util
{
class Arena;
}
namespace cache
{
class Cache;
}

namespace table
{

// Delete the resource that is held by the iterator.
template <class ResourceType>
  static void delete_resource(void* arg, void* ignored) {
//    delete reinterpret_cast<ResourceType*>(arg);
    auto del = reinterpret_cast<ResourceType*>(arg);
    MOD_DELETE_OBJECT(ResourceType, del);
  }

// Release the cached entry and decrement its ref count.
static void release_cache_entry(void* arg, void* h) {
  cache::Cache* cache = reinterpret_cast<cache::Cache*>(arg);
  cache::Cache::Handle* handle = reinterpret_cast<cache::Cache::Handle*>(h);
  cache->Release(handle);
}
  
struct ScanParam
{
  ScanParam()
  {
    reset();
  }
  ~ScanParam() {}

  void reset()
  {
    memset(this, 0, sizeof(*this));
  }
  bool is_valid() const
  {
    return table_cache_ != nullptr
        && read_options_ != nullptr
        && env_options_ != nullptr
        && icomparator_ != nullptr
        && arena_ != nullptr
        && extent_layer_ != nullptr;
  }
  DECLARE_AND_DEFINE_TO_STRING(KVP(table_cache_),
                               KVP(read_options_),
                               KVP(env_options_),
                               KVP(icomparator_),
                               KVP(file_read_hist_),
                               KVP(range_del_agg_),
                               KVP(internal_stats_),
                               KVP(extent_layer_),
                               KVP(arena_),
                               KV(subtable_id_),
                               KV(layer_position_),
                               KV(for_compaction_),
                               KV(skip_filters_),
                               KV(scan_add_blocks_limit_));

  db::TableCache *table_cache_;
  const common::ReadOptions *read_options_;
  const util::EnvOptions *env_options_;
  const db::InternalKeyComparator *icomparator_;
  monitor::HistogramImpl *file_read_hist_;
  db::RangeDelAggregator *range_del_agg_;
  db::InternalStats *internal_stats_;
  storage::ExtentLayer *extent_layer_;
  util::Arena *arena_;
  int64_t subtable_id_; // IS block cache of subtable
  storage::LayerPosition layer_position_;
  int level_;
  bool for_compaction_;
  bool skip_filters_;
  uint64_t scan_add_blocks_limit_;
};

struct TableReaderHandle
{
  TableReaderHandle() : extent_id_(),
                        table_reader_(nullptr),
                        cache_handle_(nullptr),
                        table_cache_(nullptr)
  {}
  void reset()
  {
    extent_id_.reset();
    if (nullptr != table_cache_) {
      table_cache_->ReleaseHandle(cache_handle_);
    }
    cache_handle_ = nullptr;
    table_reader_ = nullptr;
    table_cache_ = nullptr;
  }
  ExtentBasedTable *reader()
  {
    return reinterpret_cast<ExtentBasedTable *>(table_reader_);
  }
  storage::ExtentId extent_id_;
  TableReader *table_reader_;
  cache::Cache::Handle *cache_handle_;
  db::TableCache *table_cache_;
};

template <typename T>
struct BlockDataHandle
{
  BlockDataHandle() : cache_(nullptr),
                      need_do_cleanup_(false),
                      is_boundary_(false)
  {}
  void reset(db::PinnedIteratorsManager *pinned_iters_mgr = nullptr)
  {
    extent_id_.reset();
    block_handle_.reset();
    if (need_do_cleanup_) {
      if (pinned_iters_mgr != nullptr) {
        // pinned_iters_mgr will take the ownership of the cache value
        if (nullptr != cache_ && nullptr != block_entry_.cache_handle) {
          pinned_iters_mgr->RegisterCleanup(&release_cache_entry,
                                            cache_,
                                            block_entry_.cache_handle);
        } else if (nullptr != block_entry_.value) {
          pinned_iters_mgr->RegisterCleanup(&delete_resource<T>, 
                                            block_entry_.value, 
                                            nullptr);
        }
      } else {
        if (nullptr != cache_ && nullptr != block_entry_.cache_handle) {
          cache_->Release(block_entry_.cache_handle);
        } else if (nullptr != block_entry_.value) {
//          delete block_entry_.value;
          MOD_DELETE_OBJECT(T, block_entry_.value);
        }
      }
    }
    block_entry_.cache_handle = nullptr;
    block_entry_.value = nullptr;
    cache_ = nullptr;
    aio_handle_.reset();
    need_do_cleanup_ = false;
    is_boundary_ = false;
  }
  storage::ExtentId extent_id_;
  BlockHandle block_handle_;
  ExtentBasedTable::CachableEntry<T> block_entry_;
  // used to release cache handle
  cache::Cache *cache_;
  util::AIOHandle aio_handle_;
  bool need_do_cleanup_;
  bool is_boundary_;
};

} // namespace table
} // namespace xengine
