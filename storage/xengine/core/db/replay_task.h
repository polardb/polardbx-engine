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

#include "db/db_impl.h"
#include "db/write_batch_internal.h"
#include "util/callback_util.h"

namespace xengine {
namespace db {

class ReplayThreadPool;

class ReplayTask : public util::CallbackBase {
public:
  ReplayTask(WriteBatch *write_batch,
             uint64_t log_file_number,
             bool is_last_record,
             uint64_t last_record_end_pos,
             DBImpl* db_impl,
             ReplayThreadPool* replay_thread_pool = nullptr,
             memory::ArenaAllocator *arena = nullptr)
    : write_batch_(write_batch), log_file_number_(log_file_number),
      is_last_record_(is_last_record), last_record_end_pos_(last_record_end_pos),
      db_impl_(db_impl), replay_thread_pool_(replay_thread_pool), arena_(arena) {}
  virtual ~ReplayTask() {
    MOD_DELETE_OBJECT(WriteBatch, write_batch_);
  }
  virtual void run() override;

  memory::ArenaAllocator* get_arena() {
    return arena_;
  }

  bool need_before_barrier_during_replay();
  bool need_after_barrier_during_replay();

#ifndef NDEBUG
  WriteBatch* get_write_batch() {
    return write_batch_;
  }
  uint32_t get_replay_error() {
    return replay_error_;
  }
  void set_replay_error(uint32_t replay_error) {
    replay_error_ = replay_error;
  }
#endif
protected :
  virtual int replay_log();
private:
  WriteBatch *write_batch_;
  uint64_t log_file_number_;
  bool is_last_record_;
  uint64_t last_record_end_pos_;
  DBImpl* db_impl_;
  ReplayThreadPool* replay_thread_pool_;
  memory::ArenaAllocator *arena_;

#ifndef NDEBUG
  bool replay_error_ = false;
  bool need_barrier_ = false;
#endif
};

class ReplayTaskDeleter {
public:
  void operator() (ReplayTask *replay_task) {
    if (replay_task == nullptr) {
      return;
    }
    auto arena = replay_task->get_arena();
    if (arena == nullptr) {
      delete replay_task;
    } else {
      FREE_OBJECT(ReplayTask, *arena, replay_task);
    }
  }

  void operator() (util::CallbackBase *func, memory::SimpleAllocator *arena) {
    if (func == nullptr) {
      return;
    }
    if (arena == nullptr) {
      delete func;
    } else {
      FREE_OBJECT(CallbackBase, *arena, func);
    }
  }
};

class ReplayTaskParser {
public:
  static common::Status parse_replay_writebatch_from_record(common::Slice& record,
      uint32_t crc, DBImpl *db_impl, WriteBatch **write_batch, bool allow_2pc);
};

} // namespace db
} // namespace xengine
