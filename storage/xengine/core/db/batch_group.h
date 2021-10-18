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

#ifndef XENGINE_DB_BATCH_GROUP_H_
#define XENGINE_DB_BATCH_GROUP_H_

#pragma once

#include <assert.h>
#include <stdint.h>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <type_traits>
#include <vector>

#include "db/log_writer.h"
#include "db/write_callback.h"
#include "monitoring/instrumented_mutex.h"
#include "util/autovector.h"
#include "xengine/async_callback.h"
#include "xengine/options.h"
#include "xengine/status.h"
#include "xengine/types.h"
#include "xengine/write_batch.h"

namespace xengine {

namespace util {
class SpinMutex;
}

namespace common {
class Status;
class WriteOptions;
}

namespace db {
//
// State transformation for WriteRequest
//  W_STATE_INIT-->W_STATE_GROUP_LEADER->W_STATE_COMPLETED
//  W_STATE_INIT-->(W_STATE_GROUP_LOCKED_WATTING)-->W_STATE_GROUP_LEADER->W_STATE_COMPLETED
//  W_STATE_INIT-->W_STATE_GROUP_FOLLOWER-->(W_STATE_GROUP_LOCKED_WATTING)-->W_STATE_COMPLETED
enum WriteState {
  // Initial state
  W_STATE_INIT = 1,
  // a write wait to be leader
  W_STATE_GROUP_FOLLOWER = 2,
  // first writerequest push to slot will become Leader
  W_STATE_GROUP_LEADER = 4,
  // The Final common::Status of all the writerequest
  W_STATE_GROUP_COMPLETED = 8,
  // Init status of all the Leader when block wait for W_STATE_GROUP_LEADER
  //              or the sync follower writer
  W_STATE_LOCKED_WAITING = 16,
};

class WriteRequest;
class GroupSlot;

struct JoinGroupStatus {
 public:
  JoinGroupStatus()
      : join_state_(W_STATE_INIT), async_commit_(false), fast_group_(0) {}

 public:
  WriteState join_state_;
  bool async_commit_;
  int fast_group_;
};

class GroupSlot {
 public:
  GroupSlot(uint64_t max_group_size)
      : max_group_size_(max_group_size), leader_writer_(nullptr) {
    assert(max_group_size_ > 0);
  }

  ~GroupSlot() { assert(nullptr == this->leader_writer_.load()); }

 public:
  void switch_leader_on_timeout(WriteRequest* current_leader);
  bool try_to_push(WriteRequest* writer, JoinGroupStatus& join_status);

 private:
  size_t max_group_size_;
  std::atomic<WriteRequest*> leader_writer_;
  util::SpinMutex slot_mutex_;
};

class BatchGroupManager {
 public:
  BatchGroupManager(uint64_t slot_array_size, uint64_t max_group_size,
                    uint64_t max_leader_wait_time_us)
      : inited_(false),
        slot_array_size_(slot_array_size),
        max_group_size_(max_group_size),
        max_leader_wait_time_us_(max_leader_wait_time_us) {
    this->active_wait_time_us_ = this->max_leader_wait_time_us_;
    this->cpu_num_ = std::thread::hardware_concurrency();
  }

  ~BatchGroupManager() {
    destroy();
    assert(slot_array_.size() == 0);
  }

  void join_batch_group(WriteRequest* writer, JoinGroupStatus& join_status);

  static void set_state(WriteRequest* w, uint16_t new_state);

  // param[WriteRequest] [in] wirter to join
  // param[goal_mask] [in]
  // param[wait_time] [in] block wait time
  static bool await_state(WriteRequest* w, uint16_t goal_mask,
                          uint64_t wait_timeout_us = 0);

  uint64_t get_cpu_num() { return this->cpu_num_; }
  // init batch group slot array
  // called begore use group manager
  int init();

  void destroy();

 private:
  bool inited_;
  size_t slot_array_size_;
  size_t max_group_size_;
  std::atomic<uint64_t> active_wait_time_us_;
  size_t max_leader_wait_time_us_;
  size_t cpu_num_;
  std::vector<GroupSlot*> slot_array_;
};

class WriteRequest {
 public:
  WriteRequest(const common::WriteOptions& write_options, WriteBatch* _batch,
               common::AsyncCallback* _async_callback, uint64_t _log_ref,
               bool _disable_memtable)
      : batch_(_batch),
        disable_wal_(write_options.disableWAL),
        log_sync_(write_options.sync),
        wal_log_ref_(_log_ref),
        disable_memtable_(_disable_memtable),
        ignore_missing_cf_(write_options.ignore_missing_column_families),
        async_commit_(write_options.async_commit),
        made_waitable_(false),
        state_(W_STATE_INIT),
        async_callback_(_async_callback),
        log_crc32_(0),
        group_run_in_parallel_(true) {}

  ~WriteRequest() {
    if (made_waitable_) {
      state_mutex().~mutex();
      state_cv().~condition_variable();
    }
    follower_vector_.clear();
  }

 public:
  void create_mutex() {
    if (!made_waitable_) {
      // Note that made_waitable is tracked separately from state
      // transitions, because we can't atomically create the mutex and
      // link into the list.
      made_waitable_ = true;
      new (&state_mutex_bytes_) std::mutex;
      new (&state_cv_bytes_) std::condition_variable;
    }
  }

  bool should_write_to_memtable() { return !disable_memtable_; }

  bool should_write_to_wal() { return !disable_wal_; }

  // No other mutexes may be acquired while holding StateMutex(), it is
  // always last in the order
  std::mutex& state_mutex() {
    assert(made_waitable_);
    return *static_cast<std::mutex*>(static_cast<void*>(&state_mutex_bytes_));
  }

  std::condition_variable& state_cv() {
    assert(made_waitable_);
    return *static_cast<std::condition_variable*>(
        static_cast<void*>(&state_cv_bytes_));
  }

  void reset_leader_status() {
    this->log_writer_used_ = nullptr;
    this->group_total_count_ = 0;
    this->group_total_byte_size_ = 0;
    this->group_merged_log_batch_.Clear();
    this->group_first_sequence_ = 0;
    this->group_last_sequence_ = 0;
    this->log_file_pos_ = 0;
    this->group_need_log_ = false;
    this->group_need_log_sync_ = false;
  }

  void add_follower(WriteRequest* follower) {
    this->follower_vector_.push_back(follower);
  }

 public:
  /******************this for writer itself************************/
  WriteBatch* batch_;
  bool disable_wal_;
  bool log_sync_;
  // input parameter to specific the log to use
  uint64_t wal_log_ref_;
  // out parameter
  uint64_t log_used_;
  bool disable_memtable_;
  bool ignore_missing_cf_;
  bool async_commit_;
  // lazy construct  mutex and cv
  bool made_waitable_;
  // write under StateMutex() or pre-link
  std::atomic<uint8_t> state_;
  // the sequence number to use for the first key
  uint64_t sequence_;
  // status of memtable inserter
  common::Status status_;
  common::AsyncCallback* async_callback_;
  std::aligned_storage<sizeof(std::mutex)>::type state_mutex_bytes_;
  std::aligned_storage<sizeof(std::condition_variable)>::type state_cv_bytes_;

  /******************following for group leader *************************/
  std::vector<WriteRequest*> follower_vector_;
  log::Writer* log_writer_used_;
  uint64_t group_total_count_;
  uint64_t group_total_byte_size_;
  WriteBatch group_merged_log_batch_;
  uint32_t log_crc32_;
  uint64_t group_first_sequence_;
  uint64_t group_last_sequence_;
  uint64_t log_file_pos_;  // wal log pos
  bool group_run_in_parallel_;
  bool group_need_log_;
  bool group_need_log_sync_;

  /******************this for statistc **************************/
  uint64_t start_time_us_;
};
}  // namespace db
}  // namespace xengine
#endif  // XENGINE_DB_BATCH_GROUP_H_
