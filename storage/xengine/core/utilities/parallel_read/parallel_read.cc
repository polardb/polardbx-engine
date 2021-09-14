/*
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <thread>
#include <iostream>
#include "xengine/parallel_read.h"
#include "db/column_family.h"
#include "db/db_impl.h"
#include "util/arena.h"
#include "xengine/options.h"
#include "utilities/transactions/transaction_base.h"
#include "storage/multi_version_extent_meta_layer.h"

#ifdef MYSQL_SERVER
#include "my_base.h"
#include "sql_class.h"
#endif

using namespace xengine;
using namespace common;
using namespace db;
using namespace memory;
using namespace storage;
using namespace std;

namespace xengine
{
namespace common
{
ParallelReader::~ParallelReader(){
  if (scan_ctx_)
    scan_ctx_->~ScanCtx();
}
/*
* split scan task into many smaller tasks according parallel thread,
* every task has a execution context to traverse and process xengine row.
*/
int ParallelReader::add_scan(util::Transaction *trx,
                             ParallelReader::Config &config,
                             ParallelReader::F &&f)
{
  int ret = Status::kOk;
  void *mem = nullptr;

  if (IS_NULL(config.column_family_)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret));
  } else if (IS_NULL(mem = arena_.AllocateAligned(sizeof(ScanCtx)))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for ScanCtx", K(ret));
  } else if (IS_NULL(scan_ctx_ = new (mem)
                         ScanCtx(this, 1, trx, config, std::move(f)))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for ScanCtx", K(ret));
  } else {
    auto column_family_handler = dynamic_cast<db::ColumnFamilyHandleImpl *>(
        scan_ctx_->config_.column_family_);
    auto subtable = column_family_handler->cfd();
    auto sv = column_family_handler->db()->GetAndRefSuperVersion(subtable);

    storage::Range range = config.range_;
    Ranges ranges;
    if (FAILED(scan_ctx_->partition(
            subtable, const_cast<db::Snapshot *>(sv->current_meta_),
            range, ranges))) {
      XENGINE_LOG(WARN, "fail to do partition", "range_start",
                  config.range_.start_key_.ToString(true).c_str(), "range_end",
                  config.range_.end_key_.ToString(true).c_str());
    } else if (FAILED(scan_ctx_->create_contexts(ranges))) {
      XENGINE_LOG(WARN, "fail to create parallel context", "range_start",
                  range.start_key_.ToString(true).c_str(), "range_end",
                  range.end_key_.ToString(true).c_str());
    }

    column_family_handler->db()->ReturnAndCleanupSuperVersion(subtable, sv);
  }

  return ret;
}

int ParallelReader::run()
{
  if (is_queue_empty()) {
    return 0;
  }

  parallel_read();

  //any worker failed, err_ will be set
  return err_;
}

int ParallelReader::parallel_read() {
  int ret = 0;

  std::vector<std::thread> threads;

  for (size_t i = 0; i < max_threads_; ++i) {
    threads.emplace_back(&ParallelReader::worker, this, i);
  }

  for (auto &t : threads) {
    t.join();
  }

  assert(FAILED(err_) || (exe_ctx_id_ == task_completed_ && is_queue_empty()));

  return ret;
}

bool ParallelReader::is_queue_empty()
{
  std::lock_guard<std::mutex> guard(queue_mutex_);
  return task_queue_.empty();
}

void ParallelReader::enqueue(ParallelReader::ExecuteCtx *ctx)
{
  std::lock_guard<std::mutex> guard(queue_mutex_);
  task_queue_.push_back(ctx);
}

void ParallelReader::dequeue(ParallelReader::ExecuteCtx *&ctx)
{
  std::lock_guard<std::mutex> guard(queue_mutex_);
  if (!task_queue_.empty()) {
    ctx = task_queue_.front();
    task_queue_.pop_front();
  }
}

void ParallelReader::worker(size_t thread_id) {
  int ret = common::Status::kOk;
  int32_t n_completed = 0;

  // any of worker failed, other workers should also quit
  while (ret == common::Status::kOk && !is_error_set()) {
    ExecuteCtx *ctx = nullptr;
    dequeue(ctx);
    if (ctx == nullptr) {
      break;
    }

    ctx->thread_id_ = thread_id;
    ret = ctx->traverse();
    n_completed++;
  }

  if (ret != common::Status::kOk && !is_error_set()) {
    set_error_state(ret);
  }

  task_completed_.fetch_add(n_completed, std::memory_order_relaxed);
}


/**
 * build range with internal-key from user_key, so we can search extent statistics.
 *
 * @param[in] user_start_key
 * @param[in] user_end_key
 * @param[out] range, range with internal_key
*/
void ParallelReader::build_internal_key_range(const Slice user_key_start,
                                              const Slice user_key_end,
                                              storage::Range &range)
{
  const db::InternalKey internal_key_start(user_key_start, kMaxSequenceNumber,
                                           kValueTypeForSeek);
  const db::InternalKey internal_key_end(user_key_end, kMaxSequenceNumber,
                                         kValueTypeForSeek);

  Slice start_key = common::Slice(internal_key_start.Encode());
  Slice end_key = common::Slice(internal_key_end.Encode());

  range.start_key_ = start_key.deep_copy(this->get_allocator());
  range.end_key_ = end_key.deep_copy(this->get_allocator());
}


/**
 * generate split_range for parallel scan. use user_key instead of internal_key,
 * so we need remove last 8 bytes of slice.
 *
 * @param[out] range, range with user_key
 * @param[in] start_key, internal_key slice
 * @param[in] end_key, intertnal_key slice
 * @param[in] need_key_flag, indicate whether we need next-key for end_key, 
 * because we need to build a range [start, end), 
 * while include all keys in the extent [smallest, largest]
*/
void ParallelReader::build_user_key_range(storage::Range &range,
                                           common::Slice &start_key,
                                           common::Slice &end_key,
                                           bool next_key_flag)
{
  Slice user_start_key(start_key.data(),
                       start_key.size() - sizeof(SequenceNumber));
  Slice user_end_key(end_key.data(), end_key.size() - sizeof(SequenceNumber));

  range.start_key_ = user_start_key.deep_copy(this->get_allocator());
  range.end_key_ = user_end_key.deep_copy(this->get_allocator());

  if (next_key_flag) {
    successor_key(range.end_key_);
  }
}

/**
* suppose there is no internal_key like this, 0xFFFFFF...,
* because subtable_id should not be 0xFFFFFFFF
*/
int ParallelReader::successor_key(Slice &key_slice) {
  int ret = Status::kOk;

  char *pdata = const_cast<char *>(key_slice.data());
  const size_t size = key_slice.size();
  char *p = pdata + (size - 1);
  for (; p > pdata; p--) {
    if (*p != char(0xFF)) {
      *p = *p + 1;
      break;
    }
    *p = '\0';
  }

  return ret;
}

/*
* partition the scan range into smaller ranges, so we can traverse data parallelly.
* @param[in], subtable
* @param[in], meta_snapshot
* @param[in], range with internal_key
* @param[out], ranges with user_key, maybe latter we need split it again
*/
int ParallelReader::ScanCtx::partition(db::SubTable *subtable,
                                       db::Snapshot *meta_snapshot,
                                       storage::Range &range, Ranges &ranges)
{
  int ret = Status::kOk;
  auto snapshot_impl = dynamic_cast<db::SnapshotImpl *>(meta_snapshot);
  int64_t level = (storage::MAX_TIER_COUNT - 1);
  while (level >= 0) {
    LayerPosition layer_position(level, 0);
    auto extent_layer = snapshot_impl->get_extent_layer(layer_position);
    if (nullptr != extent_layer && !extent_layer->is_empty()) {
      break;
    }

    level--;
  }

  int64_t split_extent_count = 0;
  if (level < 0) {
    // only memtable, no extent
    storage::Range split_range;
    reader_->build_user_key_range(split_range, range.start_key_, range.end_key_, false);

    ranges.push_back(split_range);
    XENGINE_LOG(INFO, "subtable only have memtable data", K(subtable->GetID()));
  } else if (FAILED(partition_level(subtable, meta_snapshot, level, range,
                                    ranges, split_extent_count))) {
    XENGINE_LOG(WARN, "fail to do partition level", "range_start",
                range.start_key_.ToString(true).c_str(), "range_end",
                range.end_key_.ToString(true).c_str(), K(ret));
  } else {
    //no data in the chosen level
    if (ranges.size() == 0) {
      storage::Range split_range;
      reader_->build_user_key_range(split_range, range.start_key_, range.end_key_, false);
      ranges.push_back(split_range);
      split_extent_count = 1;
    }

    for (int32_t i = 0; i < level; i++) {
      partition_twice_if_necessary(subtable, ranges, meta_snapshot, i,
                                   split_extent_count);
    }
  }

  print_split_range(ranges);

  return ret;
}

void ParallelReader::ScanCtx::print_split_range(Ranges &ranges)
{
  int64_t index = 1;
  for (auto range : ranges) {
    auto range_start = range.start_key_;
    auto range_end = range.end_key_;

    XENGINE_LOG(INFO, "split range information", K(index), "range_start",
                range_start.ToString(true).c_str(), "range_end",
                range_end.ToString(true).c_str());
    index++;
  }
}

/**
 * For level-0, all layers are split as a whole,
 * As we know, for data extents, level-2 is much more than level-1;
 * level-1 is much more than level-0, so, most time it's not necessary to split again.
 * But for data which are not evenly split, maybe some range has most of data, then,
 * we need to split twice.
 * @param[in] ranges, range with user_key
 * @param[in] current_meta
 * @param[in] level, split level data if necessary
 * @param[in] extent_count
*/
int ParallelReader::ScanCtx::partition_twice_if_necessary(
    db::SubTable *subtable, Ranges &ranges, const db::Snapshot *current_meta,
    const int32_t level, const int64_t split_extent_count)
{
  int ret = Status::kOk;

  const SnapshotImpl *meta_snapshot =
      dynamic_cast<const SnapshotImpl *>(current_meta);

  auto extent_layer_version = meta_snapshot->get_extent_layer_version(level);
  int64_t layer_size = extent_layer_version->get_extent_layer_size();

  for (int64_t i = 0; Status::kOk == ret && i < layer_size; ++i) {
    Ranges split_twice_ranges;
    for (auto range : ranges) {
      storage::Range internal_range;
      reader_->build_internal_key_range(range.start_key_, range.end_key_,
                                        internal_range);

      auto extent_layer = extent_layer_version->get_extent_layer(i);
      assert(extent_layer != nullptr);

      int64_t extent_count = extent_layer->get_extent_count(internal_range);
      if (extent_count > split_extent_count) {
        storage::LayerPosition layer_position(level, i);
        if (FAILED(partition_layer(subtable, current_meta, layer_position,
                                   internal_range, split_extent_count,
                                   split_twice_ranges))) {
          XENGINE_LOG(WARN, "partition twice failed", "range_start",
                      range.start_key_.ToString(true).c_str(), "range_end",
                      range.end_key_.ToString(true).c_str(), K(ret));
        }
      } else {
        split_twice_ranges.push_back(range);
      }
    }

    ranges.clear();
    ranges.insert(ranges.end(), split_twice_ranges.begin(),
                  split_twice_ranges.end());
    split_twice_ranges.clear();
  }

  return ret;
}

void ParallelReader::ScanCtx::get_split_extent_count(
    storage::ExtentLayerVersion *extent_layer_version, int64_t parallel_thread,
    storage::Range &range, int64_t &extent_count)
{
  int64_t total_extent_count = 0;
  total_extent_count = extent_layer_version->get_extent_count(range);

  if (total_extent_count >= static_cast<int64_t>(parallel_thread)) {
    extent_count = total_extent_count / parallel_thread;
  } else if (total_extent_count > 0) {
    extent_count = 1;
  } else {
    extent_count = 0;
  }
}

/*
*split range by split_extent_count
*
*@param[in] subtable
*@param[in] meta_snapshot
*@param[in] level, split level
*@param[in] range with internal_key
*@param[in] split_extent_count 
*@param[in/out] ranges, push_back new range to ranges, use deep_copy
*/
int ParallelReader::ScanCtx::partition_layer(
    db::SubTable *subtable, const db::Snapshot *meta_snapshot,
    const storage::LayerPosition &layer_position, storage::Range &range,
    const int64_t split_extent_count, Ranges &ranges)
{
  int ret = Status::kOk;
  auto storage_mgr = subtable->get_storage_manager();
  table::InternalIterator *layer_iter = nullptr;

  if (FAILED(storage_mgr->get_extent_layer_iterator(&reader_->get_allocator(), meta_snapshot, layer_position, layer_iter))) {
    XENGINE_LOG(WARN, "fail to create extent layer iterator", K(ret), K(layer_position));
  } else {
    common::Slice start_key = range.start_key_;
    common::Slice end_key = range.end_key_;
    layer_iter->set_end_key(end_key, true);
    layer_iter->Seek(start_key);

    int extent_index = 0;
    bool first_key = true;
    common::Slice last_end_key;
    while (layer_iter->Valid() && Status::kOk == ret) {
      Slice key_slice = layer_iter->key();
      storage::ExtentMeta *meta = nullptr;
      if (nullptr == (meta = reinterpret_cast<ExtentMeta *>(
                          const_cast<char *>(key_slice.data())))) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, extent meta must not nullptr",
                    K(ret));
      } else {
        if (first_key) {
          first_key = false;
          start_key = range.start_key_;
        } else if (extent_index == 0) {
          //cover all part of range.
          start_key = last_end_key;
        }

        extent_index++;
        layer_iter->Next();
        // if last extent or split point, generate new range
        if (!layer_iter->Valid() || extent_index == split_extent_count) {
          end_key = layer_iter->Valid() ? meta->largest_key_.Encode()
                                        : range.end_key_;

          storage::Range split_range;
          reader_->build_user_key_range(split_range, start_key, end_key, layer_iter->Valid());
          ranges.push_back(split_range);

          const db::InternalKey internal_key(split_range.end_key_, kMaxSequenceNumber, kValueTypeForSeek);
          last_end_key = internal_key.Encode().deep_copy(reader_->get_allocator());

          extent_index = 0;
        }
      }
    }
  }

  return ret;
}

/*
 * split range according config parallel thread
 * @param[in], subtable
 * @param[in], meta_snapshot of L0,L1,L2
 * @param[in], level
 * @param[in], range input with internal_key
 * @param[out], ranges, output with user_key
 * @param[out], split_extent_count, how many extents each task has
*/
int ParallelReader::ScanCtx::partition_level(db::SubTable *subtable,
                                             const db::Snapshot *meta_snapshot,
                                             const int32_t level,
                                             storage::Range &range,
                                             Ranges &ranges,
                                             int64_t &split_extent_count)
{
  int ret = Status::kOk;

  if (level < 0 || level > 2) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "level is invalid", K(level), K(ret));
  } else {
    auto extent_layer_version = meta_snapshot->get_extent_layer_version(level);
    get_split_extent_count(extent_layer_version, reader_->get_max_threads(),
                           range, split_extent_count);

    storage::LayerPosition layer_position(level, 0);
    if (split_extent_count > 0 &&
        FAILED(partition_layer(subtable, meta_snapshot, layer_position,
                                     range, split_extent_count,
                                     ranges))) {
      XENGINE_LOG(WARN, "partition range failed", "range_start",
                  range.start_key_.ToString(true).c_str(), "range_end",
                  range.end_key_.ToString(true).c_str(), K(ret));
    }
  }

  return ret;
}

int ParallelReader::ScanCtx::create_contexts(Ranges &ranges)
{
  int ret = 0;
  for (auto range : ranges) {
    create_context(range); 
  }

  return ret;
}

int ParallelReader::ScanCtx::create_context(storage::Range &range) {
  int ret = Status::kOk;
  
  auto ctx_id = reader_->exe_ctx_id_.fetch_add(1, std::memory_order_relaxed);

  ExecuteCtx *ctx = nullptr;
  void *mem = nullptr;
  if (IS_NULL(mem = (reader_->get_allocator()).AllocateAligned(sizeof(ExecuteCtx)))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for ExecuteCtx", K(ret));
  } else if (IS_NULL(ctx = new (mem) ExecuteCtx(ctx_id, this, range))) {
    ret =  Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for ExecuteCtx", K(ret));
  } else {
    reader_->enqueue(ctx);
  }

  return ret;
}


/*
* traverse data by WBWIterator, process rows one by one
*/
int ParallelReader::ExecuteCtx::traverse() {
  int ret = Status::kOk;

  db::Iterator *db_iter = nullptr;

  common::ReadOptions ro = scan_ctx_->config_.read_options_;
  util::TransactionBaseImpl *trx =
      dynamic_cast<util::TransactionBaseImpl *>(scan_ctx_->trx_);
  db::ColumnFamilyHandleImpl *column_family =
      dynamic_cast<db::ColumnFamilyHandleImpl *>(
          scan_ctx_->config_.column_family_);

  if (IS_NULL(db_iter = trx->GetIterator(ro, column_family))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for db_iter", K(ret));
  } else {
    db_iter->set_end_key(range_.end_key_);

    db_iter->Seek(range_.start_key_);

    while (db_iter->Valid() && ret == Status::kOk) {
#ifdef MYSQL_SERVER
      /*user kill the session*/
      THD *thd = scan_ctx_->reader_->mysql_thd_;
      if (thd && thd->killed) {
        ret = HA_ERR_QUERY_INTERRUPTED;
        break;
      }
#endif
      ret = scan_ctx_->f_(this, db_iter);
      db_iter->Next();
    }

    MOD_DELETE_OBJECT(Iterator, db_iter);
  }

  return ret;
}

} // namespace common
} // namespace xengine
