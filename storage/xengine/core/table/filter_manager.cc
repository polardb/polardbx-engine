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
#include <list>

#include "db/column_family.h"
#include "db/table_cache.h"
#include "port/likely.h"
#include "table/block_based_filter_block.h"
#include "table/filter_block.h"
#include "table/filter_block.h"
#include "table/filter_manager.h"
#include "table/format.h"
#include "table/full_filter_block.h"
#include "table/internal_iterator.h"
#include "monitoring/query_perf_context.h"
#include "util/murmurhash.h"
#include "util/mutexlock.h"
#include "util/sync_point.h"
#include "cache/sharded_cache.h"
#include "cache/lru_cache.h"
#include "xengine/env.h"
#include "xengine/options.h"

using namespace xengine;
using namespace common;
using namespace util;
using namespace monitor;
using namespace cache;
using namespace db;

namespace xengine {
namespace table {

extern Cache::Handle *GetEntryFromCache(Cache *block_cache, const Slice &key,
                                        CountPoint block_cache_miss_ticker,
                                        CountPoint block_cache_hit_ticker);
// Env::Schedule only accepts a function with a void* parameter and a void*
// parameter. Wrap the classes and functions needed to schedule filter building
// task.
namespace {
// Args sent to ThreadPool::Schedule.
// Have to copy data inside because this is a async process.
class FilterBuildArgs {
 public:
  FilterBuildArgs(FilterManager *filter_manager, const std::string &key,
                  QueueElement element, std::atomic<int32_t> *quota)
      : filter_manager_(filter_manager),
        key_(key),
        element_(element),
        quota_guard_(quota) {}
  FilterManager *filter_manager_;
  std::string key_;
  QueueElement element_;
  QuotaGuard quota_guard_;
};

// Request used locally, use pointers to pass args from request_queue_ to
// thread_pool
class Request {
public:
  Request(const std::string *key, QueueElement *element)
      : key_(key), element_(element) {}
  const std::string *key_;
  QueueElement *element_;
};

}

void schedule_wrapper(void *filter_manager) {
  auto fm = static_cast<FilterManager *>(filter_manager);
  fm->schedule();
  fm->unref_cfd();
}

void build_filter_wrapper(void *filter_args) {
  FilterBuildArgs *args = static_cast<FilterBuildArgs *>(filter_args);
  args->filter_manager_->build_filter(args->key_, args->element_,
                                      std::move(args->quota_guard_));
  args->filter_manager_->unref_cfd();
  delete args;
}

void delete_cached_filter_entry(const Slice &key, void *value) {
  FilterBlockReader *filter = reinterpret_cast<FilterBlockReader *>(value);
  QUERY_COUNT_ADD(CountPoint::BLOCK_CACHE_FILTER_BYTES_EVICT, filter->size());
  delete filter;
}

FilterManager::~FilterManager() {
  is_working_ = false;
  cfd_ = nullptr;
  db_mutex_ = nullptr;
}

int FilterManager::get_filter(const Slice &request_key, Cache *cache,
                              Statistics *stats, bool no_io, int level,
                              const FileDescriptor &fd,
                              HistogramImpl *file_read_hist,
                              FilterBlockReader *&filter,
                              Cache::Handle *&cache_handle) {
  // disable filter
  return Status::kNotSupported;
}

Status FilterManager::start_build_thread(
    ColumnFamilyData *cfd, DBImpl *db,
    InstrumentedMutex *db_mutex,
    const EnvOptions *env_options, Env *env,
    const std::shared_ptr<const FilterPolicy> &filter_policy,
    const std::shared_ptr<Cache> &cache, bool whole_key_filtering,
    bool using_high_priority, int stripes, int threads,
    std::atomic<int32_t> *quota) {
  cfd_ = cfd;
  db_ = db;
  db_mutex_ = db_mutex;
  env->SetBackgroundThreads(threads, Env::FILTER);
  ioptions_ = cfd->ioptions();
  env_options_ = env_options;
  icmp_ = &(cfd->internal_comparator());
  table_factory_ = ioptions_->table_factory;
  env_ = env;
  stripes_ = stripes;
  threads_ = threads;
  request_queue_ =
      std::vector<std::unordered_map<std::string, QueueElement>>(stripes);
  mutex_ = std::vector<port::Mutex>(stripes);
  filter_policy_ = filter_policy;
  whole_key_filtering_ = whole_key_filtering;
  cache_priority_ =
      using_high_priority ? Cache::Priority::HIGH : Cache::Priority::LOW;
  cache_ = cache;
  table_cache_ = cfd->table_cache();
  quota_ = quota;
  is_working_ = false;
  // Still return OK. Just left is_workin_ false to prevent further call to
  // get_filter.
  return Status::OK();
}

bool FilterManager::is_working() { return is_working_; }

void FilterManager::schedule() {
  QuotaGuard quota_guard{quota_};
  std::unordered_map<std::string, QueueElement> result_map_array[stripes_];
  std::list<Request> most_urgent_list;
  int list_size_limit = threads_;
  for (int i = 0; i != stripes_; ++i) {
    MutexLock l_request(&mutex_[i]);
    result_map_array[i].swap(request_queue_[i]);
  }
  // Get request with most count
  for (auto &result_map : result_map_array) {
    for (std::pair<const std::string, QueueElement> &ele : result_map) {
      if (list_size_limit > 0) {
        most_urgent_list.emplace_back(&(ele.first), &(ele.second));
        --list_size_limit;
      } else {
        // Find request with smallest count and replace it if current is larger.
        std::list<Request>::iterator smallest_request =
            std::min_element(most_urgent_list.begin(), most_urgent_list.end(),
                             [](const Request &e1, const Request &e2) -> bool {
                               return e1.element_->count_ < e2.element_->count_;
                             });
        if (ele.second.count_ > smallest_request->element_->count_) {
          smallest_request->key_ = &(ele.first);
          smallest_request->element_ = &(ele.second);
        }
      }
    }
  }
  if (most_urgent_list.size() == 0) {
    scheduling_.store(false);
    return;
  }
  // Reserve one for current thread.
  const std::string *reserve_key = most_urgent_list.back().key_;
  QueueElement *reserve_ele = most_urgent_list.back().element_;
  most_urgent_list.pop_back();
  for (const Request &request : most_urgent_list) {
    if (quota_->fetch_sub(1) > 0 && Status::kOk == ref_cfd()) {
      // delete args in BuildFilterWrapper.
      env_->Schedule(
          build_filter_wrapper,
          new FilterBuildArgs{this, *request.key_, *request.element_, quota_},
          Env::Priority::FILTER);
    } else {
      quota_->fetch_add(1);
    }
  }
  scheduling_.store(false);
  // Run a build task using schedule thread to reduce Env::Schedule.
  build_filter(*reserve_key, *reserve_ele, std::move(quota_guard));
}

int FilterManager::build_filter(const std::string &request_key,
                                QueueElement &max_element,
                                QuotaGuard &&scheduler_quota_guard) {
  QuotaGuard quota_guard{std::move(scheduler_quota_guard)};
  TEST_SYNC_POINT_CALLBACK("FilterManager::BuildFilter:Start", quota_);
  Cache::Handle *table_handle;
  // Get snapshot to prevent recycle during filter building process.
  const Snapshot *meta_snapshot = get_meta_snapshot();
  // Set no_io to true to react correctly to recycle_extents which evicts table
  // reader
  // from table cache. It is possible the extent refered by max_element.fd_ has
  // been
  // recycled. So its data should be seen as corrupted and not to be loaded
  // again.
  Status s = table_cache_->FindTable(
      *env_options_, *icmp_, max_element.fd_, &table_handle, true /* no_io */,
      true /* record_read_stats */, max_element.file_read_hist_,
      true /* skip_filters */, max_element.level_,
      false /* prefetch_index_and_filter_in_cache */);
  if (!s.ok()) {
    release_meta_snapshot(meta_snapshot);
    return s.code();
  }

  TEST_SYNC_POINT_CALLBACK("FilterManager::BuildFilter:Running", nullptr);
  // Iter the table and fill the filter builder.
  TableReader *table_reader =
      table_cache_->GetTableReaderFromHandle(table_handle);

  uint64_t num_entries = table_reader->GetTableProperties()->num_entries;
  std::unique_ptr<FilterBlockBuilder> filter_builder;
  int ret =
      create_filter_builder(whole_key_filtering_, ioptions_->prefix_extractor,
                            filter_policy_.get(), num_entries, filter_builder);
  if (ret != Status::kOk) {
    table_cache_->ReleaseHandle(table_handle);
    release_meta_snapshot(meta_snapshot);
    return ret;
  }
  filter_builder->StartBlock(0);
  ReadOptions ro;
  ro.fill_cache = false;
  std::unique_ptr<InternalIterator> iter(table_reader->NewIterator(
      ro, nullptr /* arena */, true /* skip_filters */));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    // Could not ingore kTypeDeletion or kTypeSingleDeletion.
    // 'Get' needs the delete record to stop futher search.
    // Example:
    // level0:    delete key1
    // level1:    put    key1 value1
    // When search key1, Get in level 0 would return nothing if
    // the filter ignores kTypeDeletion. Then Get would continue
    // in level1, find and return value1
    filter_builder->Add(ExtractUserKey(iter->key()));
  }
  table_cache_->ReleaseHandle(table_handle);
  release_meta_snapshot(meta_snapshot);

  // Put built filter to cache.
  std::unique_ptr<char[], memory::ptr_delete<char>> buf;
  FilterBlockReader *filter_reader;
  Slice filter_slice = filter_builder->Finish(&buf);
  FAIL_RETURN(create_filter_reader(
      whole_key_filtering_, ioptions_->prefix_extractor, filter_policy_.get(),
      buf.release(), filter_slice, ioptions_->statistics, filter_reader));
  FAIL_RETURN(put_filter_to_cache(cache_.get(), Slice(request_key),
                                  filter_reader, ioptions_->statistics));
  return ret;
}

const Snapshot *FilterManager::get_meta_snapshot() {
  InstrumentedMutexLock l(db_mutex_);
  return cfd_->get_meta_snapshot(db_mutex_);
}

void FilterManager::release_meta_snapshot(const Snapshot *meta_snapshot) {
  InstrumentedMutexLock l(db_mutex_);
  cfd_->release_meta_snapshot(meta_snapshot, db_mutex_);
}

int FilterManager::lookup_filter_cache(const Slice &request_key, Cache *cache,
                                       Statistics *stats,
                                       FilterBlockReader *&filter,
                                       Cache::Handle *&cache_handle) {
  cache_handle = GetEntryFromCache(cache, request_key,
                                   CountPoint::BLOCK_CACHE_FILTER_MISS,
                                   CountPoint::BLOCK_CACHE_FILTER_HIT);
  if (cache_handle != nullptr) {
    filter = reinterpret_cast<FilterBlockReader *>(cache->Value(cache_handle));
  }
  return Status::kOk;
}

void FilterManager::append_to_request_queue(const Slice &request_key, int level,
                                            const FileDescriptor &fd,
                                            HistogramImpl *file_read_hist) {
  std::string key = request_key.ToString();
  static murmur_hash hash;
  size_t stripe = hash(key) % stripes_;
  MutexLock l(&mutex_[stripe]);
  std::unordered_map<std::string, QueueElement>::iterator queue_iter =
      request_queue_[stripe].find(key);
  if (queue_iter != request_queue_[stripe].end()) {
    ++(queue_iter->second.count_);
  } else {
    request_queue_[stripe].emplace(
        std::piecewise_construct, std::forward_as_tuple(key),
        std::forward_as_tuple(1 /* count */, level, fd, file_read_hist));
  }
}

void FilterManager::wake_up_threads() {
  // The thread pool has a queue to sotre requests, but in this case,
  // the requests are much more than number of threads, so have to
  // control the requests sent to thread pool.
  // See ThreadPool::Schedule
  // See FilterManager::Schedule 
  TEST_SYNC_POINT("FilterManager::WakeUpThreads::thread2:2");
  if (!scheduling_.exchange(true)) {
    TEST_SYNC_POINT("FilterManager::WakeUpThreads::thread1:1");
    TEST_SYNC_POINT("FilterManager::WakeUpThreads::thread1:4");
    if (quota_->fetch_sub(1) > 0 && Status::kOk == ref_cfd()) {
      env_->Schedule(schedule_wrapper, this, Env::Priority::FILTER);
    } else {
      quota_->fetch_add(1);
      scheduling_.store(false);
    }
  }
  TEST_SYNC_POINT("FilterManager::WakeUpThreads::thread2:3");
}

void FilterManager::TEST_wait_for_all_task_complete() {
  while (quota_->load() < threads_) {
    port::AsmVolatilePause();
  }
}

int FilterManager::ref_cfd() {
  int res;
  InstrumentedMutex *db_mutex = db_mutex_;
  ColumnFamilyData *cfd = cfd_;
  if (cfd != nullptr && db_mutex != nullptr && !cfd->IsDropped()) {
    db_mutex->Lock();
    cfd->Ref();
    db_mutex->Unlock();
    res = Status::kOk;
  } else {
    res = Status::kAborted;
  }
  return res;
}

void FilterManager::unref_cfd() {
  InstrumentedMutex *db_mutex = db_mutex_;
  db_mutex->Lock();
  if (cfd_->Unref()) {
    MOD_DELETE_OBJECT(ColumnFamilyData, cfd_);
  }
  db_mutex->Unlock();
}

int FilterManager::create_filter_builder(
    bool whole_key_filtering, const SliceTransform *prefix_extractor,
    const FilterPolicy *filter_policy, const size_t num_entries,
    std::unique_ptr<FilterBlockBuilder> &filter_builder) {
  FilterBitsBuilder *filter_bits_builder =
      num_entries == 0 ? filter_policy->GetFilterBitsBuilder()
                       : filter_policy->GetFixedBitsBuilder(num_entries);

  if (UNLIKELY(filter_bits_builder == nullptr)) {
    __XENGINE_LOG(ERROR, "FilterPolicy %s does not support GetFilterBitsBuilder or GetFixedBitsBuilder",
        filter_policy->Name());
    return Status::kNotSupported;
  }
  filter_builder.reset(new FullFilterBlockBuilder(
      prefix_extractor, whole_key_filtering, filter_bits_builder));
  return Status::kOk;
}

int FilterManager::create_filter_reader(bool whole_key_filtering,
                                        const SliceTransform *prefix_extractor,
                                        const FilterPolicy *filter_policy,
                                        char *data,
                                        const Slice &filter_slice,
                                        Statistics *statistics,
                                        FilterBlockReader *&filter_reader) {
  FilterBitsReader *filter_bits_reader =
      filter_policy->GetFilterBitsReader(filter_slice);
  if (UNLIKELY(filter_bits_reader == nullptr)) {
    __XENGINE_LOG(ERROR, "FilterPolicy %s does not support GetFilterBitsReader", filter_policy->Name());
    return Status::kNotSupported;
  }
  filter_reader = new FullFilterBlockReader(
      prefix_extractor, whole_key_filtering, data,
      filter_slice.size(), filter_bits_reader, statistics);
  return Status::kOk;
}

int FilterManager::put_filter_to_cache(Cache *cache, const Slice &key,
                                       FilterBlockReader *filter,
                                       Statistics *statistics) {
  size_t filter_size = filter->size();
  if (UNLIKELY(filter == nullptr || filter_size == 0)) {
    return Status::kInvalidArgument;
  }
  // Use a handle to keep it in cache.
  Status s =
      cache->Insert(key, filter, filter_size, &delete_cached_filter_entry,
                    nullptr, cache_priority_);
  uint32_t shard_id = static_cast<ShardedCache *>(cache)->get_shard_id(key);
  if (s.ok()) {

    QUERY_COUNT(CountPoint::BLOCK_CACHE_ADD);
    QUERY_COUNT(CountPoint::BLOCK_CACHE_FILTER_ADD);
    QUERY_COUNT_ADD(CountPoint::BLOCK_CACHE_BYTES_WRITE, filter_size);
    QUERY_COUNT_ADD(CountPoint::BLOCK_CACHE_FILTER_BYTES_INSERT, filter_size);

    QUERY_COUNT_SHARD(CountPoint::BLOCK_CACHE_ADD, shard_id);
    QUERY_COUNT_SHARD(CountPoint::BLOCK_CACHE_FILTER_ADD, shard_id);
  } else {
    QUERY_COUNT(CountPoint::BLOCK_CACHE_ADD_FAILURES);
    QUERY_COUNT_SHARD(CountPoint::BLOCK_CACHE_ADD_FAILURES, shard_id);
    delete filter;
  }
  return s.code();
}

}  // namespace xengine::table
}  // namespace xengine
