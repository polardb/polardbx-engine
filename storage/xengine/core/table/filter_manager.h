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
#include <atomic>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include "db/version_edit.h"
#include "db/db_impl.h"
#include "port/port.h"
#include "xengine/cache.h"
#include "xengine/slice.h"
#include "xengine/statistics.h"
#include "xengine/status.h"

namespace xengine {

namespace db {
class InternalKeyComparator;
class TableCache;
class ColumnFamilyData;
class Snapshot;
}

namespace util {
class Env;
class EnvOptions;
}

namespace monitor {
class HistogramImpl;
class InstrumentedMutex;
}

namespace table {
class FilterBlockReader;
class FilterBlockBuilder;
class BlockContents;

enum class FilterType {
  kNoFilter,
  kFullFilter,
  kBlockFilter,
};

class QueueElement {
public:
  QueueElement(uint32_t count, int level, const db::FileDescriptor &fd,
               monitor::HistogramImpl *file_read_hist)
      : count_(count),
        level_(level),
        fd_(fd),
        file_read_hist_(file_read_hist) {}
  uint32_t count_ = 0;
  int level_;
  db::FileDescriptor fd_;
  monitor::HistogramImpl *file_read_hist_;
};

class QuotaGuard {
public:
  explicit QuotaGuard(std::atomic<int32_t> *quota) : quota_(quota) {}
  explicit QuotaGuard(QuotaGuard &&quota_guard) {
    quota_ = quota_guard.quota_;
    quota_guard.quota_ = nullptr;
  }
  ~QuotaGuard() {
    if (quota_ != nullptr) {
      quota_->fetch_add(1);
    }
  }

  QuotaGuard(const QuotaGuard &) = delete;
  QuotaGuard &operator=(const QuotaGuard &) = delete;
  QuotaGuard &operator=(QuotaGuard &&) = delete;

private:
  std::atomic<int32_t> *quota_;
};


class FilterManager {
public:
  FilterManager()
      : quota_(nullptr),
        scheduling_(false),
        is_working_(false),
        table_factory_(nullptr),
        stripes_(16),
        threads_(2) {}

  ~FilterManager();

  int get_filter(const common::Slice &request_key, cache::Cache *cache,
                 monitor::Statistics *stats, bool no_io, int level,
                 const db::FileDescriptor &fd,
                 monitor::HistogramImpl *file_read_hist,
                 table::FilterBlockReader *&filter,
                 cache::Cache::Handle *&cache_handle);

  common::Status start_build_thread(
      db::ColumnFamilyData *cfd, db::DBImpl *db,
      monitor::InstrumentedMutex *db_mutex,
      const util::EnvOptions *env_options, util::Env *env,
      const std::shared_ptr<const table::FilterPolicy> &filter_policy,
      const std::shared_ptr<cache::Cache> &cache, bool whole_key_filtering,
      bool using_high_priority, int stripes, int threads,
      std::atomic<int32_t> *quota);

  bool is_working();
  // Wakeup and schedule order:
  // WakeUpThreads() schedules ScheduleWrapper()
  // and then call FilterManager::Schedule()
  // FilterManager::Schedule() schedules BuildFilterWrapper()
  // and then call FilterManager::BuildFilter().
  void wake_up_threads();
  void TEST_wait_for_all_task_complete();

private:
  int ref_cfd();
  void unref_cfd();

  int create_filter_builder(
      bool whole_key_filtering, const common::SliceTransform *prefix_extractor,
      const table::FilterPolicy *filter_policy, const size_t num_entries,
      std::unique_ptr<table::FilterBlockBuilder> &filter_builder);
  int create_filter_reader(bool whole_key_filtering,
                           const common::SliceTransform *prefix_extractor,
                           const table::FilterPolicy *filter_policy,
                           char *data,
                           const common::Slice &filter_slice,
                           monitor::Statistics *statistics,
                           table::FilterBlockReader *&filter_reader);
  int put_filter_to_cache(cache::Cache *cache, const common::Slice &key,
                          table::FilterBlockReader *filter_contents,
                          monitor::Statistics *statistics);

  int lookup_filter_cache(const common::Slice &request_key, cache::Cache *cache,
                          monitor::Statistics *stats,
                          table::FilterBlockReader *&filter,
                          cache::Cache::Handle *&cache_handle);
  void append_to_request_queue(const common::Slice &request_key, int level,
                               const db::FileDescriptor &fd,
                               monitor::HistogramImpl *file_read_hist);
  void schedule();
  int build_filter(const std::string &key, QueueElement &ele,
                   QuotaGuard &&_quota_guard);
  const db::Snapshot *get_meta_snapshot();
  void release_meta_snapshot(const db::Snapshot *meta_snapshot);
  // Wrap 2 functions for thread pool schedule.
  friend void schedule_wrapper(void *filter_manager);
  friend void build_filter_wrapper(void *filter_args);

private:
  // Have to copy the key to request_queue, because the original slice may be
  // released after the TableReader call GetFilter.
  std::vector<std::unordered_map<std::string, QueueElement>> request_queue_;
  std::vector<port::Mutex> mutex_;
  // To prevent from pushing too many requests to threadpool.
  // quota_ is init using threads_ count, and then each schedule
  // must require one thread from quota_
  std::atomic<int32_t> *quota_;
  // Token for the schedule authority.
  // Only one thread can do schedule at the same time, the schedule thread
  // will iter all maps in request_queue_, find the best task which should
  // be processed, and then schedule free threads to call BuildFilter()
  std::atomic<bool> scheduling_;
  bool is_working_;
  const common::ImmutableCFOptions *ioptions_;
  const util::EnvOptions *env_options_;
  table::TableFactory *table_factory_;
  util::Env *env_;
  int32_t stripes_;
  int32_t threads_;

  std::shared_ptr<const table::FilterPolicy> filter_policy_;
  std::shared_ptr<cache::Cache> cache_;
  bool whole_key_filtering_;
  cache::Cache::Priority cache_priority_;

  db::TableCache *table_cache_;
  const db::InternalKeyComparator *icmp_;
  db::ColumnFamilyData *cfd_;
  db::DBImpl *db_;
  monitor::InstrumentedMutex *db_mutex_;
};
}  // namespace xengine::table
}  // namespace xengine
