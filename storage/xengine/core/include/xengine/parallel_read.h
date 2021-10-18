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

#include <functional>
#include <vector>
#include <deque>
#include <mutex>
#include <condition_variable>
#include "memory/modtype_define.h"
#include "memory/stl_adapt_allocator.h"
#include "compact/range_iterator.h"
#include "xengine/options.h"
#include "xengine/utilities/transaction.h"
#include "db/column_family.h"
#include "util/arena.h"
#include "storage/storage_common.h"
#include "iterator.h"

#pragma once

class THD;

namespace xengine {
namespace db {
class ColumnFamilyHandle;
class ColumnFamilyData;
}

namespace util {
class Transaction;
class Arena;
}

namespace storage {
struct Range;
struct ExtentLayer; 
}

namespace common {
struct ReadOptions;

class ParallelReader
{
public:
  constexpr static size_t MAX_THREADS{256};
  constexpr static size_t SPLIT_FACTOR{1};

  class ExecuteCtx;
  class ScanCtx;
  class Config;

  //using F = std::function<int(const ExecuteCtx *, Slice &key, Slice &value)>;
  using F = std::function<int(const ExecuteCtx *, db::Iterator* it)>;
  using TaskQueue = std::deque<
      ExecuteCtx *,
      memory::stl_adapt_allocator<ExecuteCtx *, memory::ModId::kParallelRead>>;
  using Ranges = std::vector<
      storage::Range,
      memory::stl_adapt_allocator<storage::Range, memory::ModId::kParallelRead>>;

public:
#ifdef MYSQL_SERVER
  ParallelReader(size_t max_threads, THD *mysql_thd)
     : max_threads_(max_threads),
       arena_(util::Arena::kMinBlockSize, 0, memory::ModId::kParallelRead),
       mysql_thd_(mysql_thd) {}
#else
  ParallelReader(size_t max_threads)
     : max_threads_(max_threads),
       arena_(util::Arena::kMinBlockSize, 0, memory::ModId::kParallelRead) {}
#endif

  ~ParallelReader();
  int add_scan(util::Transaction *trx, ParallelReader::Config &config,
               ParallelReader::F &&f);

  int run();

  int parallel_read();

  bool is_queue_empty();
  void enqueue(ExecuteCtx *ctx);
  void dequeue(ExecuteCtx *&ctx);

  bool is_error_set() const {
    return (err_.load(std::memory_order_relaxed) != Status::kOk);
  }

  void set_error_state(int err) {
    err_.store(err, std::memory_order_relaxed);
  }

  /* parallel worker thread */
  void worker(size_t thread_id);

  void build_internal_key_range(const Slice user_key_start,
                                const Slice user_key_end,
                                storage::Range &range);

  void build_user_key_range(storage::Range &range, Slice &start_key,
                  Slice &end_key, bool next_key_flag);

  int successor_key(Slice &key_slice);

  /* memory allocator */
  util::Arena &get_allocator() { return arena_; }

private:
  TaskQueue task_queue_;
  mutable std::mutex queue_mutex_;

  size_t max_threads_;
  size_t get_max_threads() { return max_threads_; }

  util::Arena arena_;

  /* Scan Context */
  ScanCtx *scan_ctx_; 

  /* total number of task */
  std::atomic<uint32_t> exe_ctx_id_{0};

  /* number of tasks executed */
  std::atomic<uint32_t> task_completed_{0};

  std::atomic<int> err_{Status::kOk};

#ifdef MYSQL_SERVER
  THD *mysql_thd_;
#endif
};

/* configure about index information and range */
class ParallelReader::Config {
public:
  Config(db::ColumnFamilyHandle *column_family, storage::Range range,
         common::ReadOptions read_options)
  {
    column_family_ = column_family;
    range_ = range;
    read_options_ = read_options;
  }

  ~Config() {}

  /* subtable to scan */
  db::ColumnFamilyHandle *column_family_;

  /* read range with internal_key */
  storage::Range range_;

  /* read options used to read, include snapshot */
  common::ReadOptions read_options_;

  friend class ParallelReader::ScanCtx;
  friend class ParallelReader::ExecuteCtx;
  friend class ParallelReader;
};

/* context for query, and how to process rows with callback */
class ParallelReader::ScanCtx
{
public:
  ScanCtx(ParallelReader *reader, size_t id, util::Transaction *trx,
          Config &config, F &&f /* callback */)
      : reader_(reader), config_(config), id_(id), trx_(trx), f_(std::move(f))
  {
  }

  // int partition_memtable(range, Memtable *mem);

  int partition(db::SubTable *subtable,
                db::Snapshot *current_meta, storage::Range &range,
                Ranges &ranges);

  int partition_level(db::SubTable *subtable, const db::Snapshot *current_meta,
                      const int32_t level, storage::Range &range,
                      Ranges &ranges, int64_t &split_extent_count);

  int partition_layer(db::SubTable *subtable, const db::Snapshot *meta_snapshot,
                      const storage::LayerPosition &layer_position,
                      storage::Range &range, const int64_t split_extent_count,
                      Ranges &ranges);

  void print_split_range(Ranges &ranges);

  void get_split_extent_count(storage::ExtentLayerVersion *extent_layer_version,
                              int64_t parallel_thread, storage::Range &range,
                              int64_t &extent_count);

  int partition_twice_if_necessary(db::SubTable *subtable, Ranges &ranges,
                                   const db::Snapshot *current_meta,
                                   const int32_t level, const int64_t extent_count);

  int create_ranges(storage::Range &range, Ranges &ranges);

  int create_contexts(Ranges &ranges);

  int create_context(storage::Range &range);

private:
  ParallelReader *reader_;

  /* input scan range */
  Config config_;

  size_t id_;

  /* transaction used to scan */
  util::Transaction *trx_;

  /* row process callback */
  F f_;

  friend class ParallelReader;
  friend class ParallelReader::ExecuteCtx;
};

class ParallelReader::ExecuteCtx
{
public:
  ExecuteCtx(size_t id, ParallelReader::ScanCtx *scan_ctx, storage::Range &range)
      : id_(id), scan_ctx_(scan_ctx), range_(range)
  {
  }

  int traverse();

private:
  size_t id_;
  ParallelReader::ScanCtx *scan_ctx_;
  storage::Range range_;

public:
  /* current thread_id, used to counter */
  size_t thread_id_{std::numeric_limits<size_t>::max()};

public:
  friend class ParallelReader;
};

} //namespace common
} //namespace xengine
