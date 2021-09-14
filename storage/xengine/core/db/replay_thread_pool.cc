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

#include "db/replay_thread_pool.h"
#include "xengine/xengine_constants.h"
#include "xengine/status.h"
#include "xengine/env.h"
#include "logger/logger.h"
#include "logger/log_module.h"
#include "db/replay_threadpool_executor.cc"

#include <thread>

using namespace xengine;
using namespace common;

namespace xengine {
namespace db {

template class ReplayThreadPoolExecutor<ReplayTaskDeleter>;

int ReplayThreadPool::init() {
  int ret = Status::kOk;
  if(UNLIKELY(inited_)) {
    ret = Status::kInitTwice;
  } else if (UNLIKELY(thread_count_ < 0 || thread_count_ > 1024 || IS_NULL(db_impl_))) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "ReplayThreadPool init failed", K(ret), K(thread_count_), KP(db_impl_));
  } else {
    replay_thread_pool_executor_ = MOD_NEW_OBJECT(memory::ModId::kRecovery, ReplayThreadPoolExecutor<ReplayTaskDeleter>, max_queue_size_);
    if (FAILED(replay_thread_pool_executor_->initialize(thread_count_))) {
      XENGINE_LOG(WARN, "ReplayThreadPool init failed", K(ret));
      MOD_DELETE_OBJECT(ReplayThreadPoolExecutor, replay_thread_pool_executor_);
    } else {
      XENGINE_LOG(INFO, "replay thread pool init succ", K(thread_count_));
      inited_ = true;
      last_print_status_time_ = 0;
      last_log_wait_time_ = 0;
    }
  }
  return ret;
}

int ReplayThreadPool::wait_for_all_tasks_executed() {
  int ret = Status::kOk;
  if (UNLIKELY(!inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "wait for all threads suspend failed", K(ret));
  } else {
    while (SUCC(ret) && get_submit_task_num() > get_executed_task_num()) {
      std::this_thread::yield();
      // still need pre_process_execute during waiting all tasks done for checking switching memtable
      // but here we won't meet a barrier again
      if (FAILED(pre_process_execute())) {
        XENGINE_LOG(WARN, "fail to pre_process_execute while waiting for all tasks executed", K(ret));
        break;
      }
    }
    if (SUCC(ret) && replay_thread_pool_executor_->has_error()) {
      XENGINE_LOG(WARN, "fail to wait for all tasks executed, has error");
      ret = Status::kCorruption;
    }
    if (SUCC(ret)) {
      assert(replay_thread_pool_executor_->get_waitting_task_num() == 0);
    }
  }
  return ret;
}

int ReplayThreadPool::pre_process_execute(bool need_before_barrier) {
  int ret = Status::kOk;
  if (UNLIKELY(!inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "pre_process_execute failed", K(ret));
  } else {
    if (db_impl_->check_if_need_switch_memtable()) { // check if need switch memtable
      if (FAILED(set_during_switching_memtable())) {
        XENGINE_LOG(WARN, "set replay thread pool during switching memtable failed", K(ret));
      } else if (FAILED(wait_for_all_threads_suspend())) {
        XENGINE_LOG(WARN, "wait for replay threads suspend failed", K(ret));
      } else {
        std::list<SubTable*> switched_sub_tables;
        if (FAILED(db_impl_->switch_memtable_during_parallel_recovery(switched_sub_tables))) {
          XENGINE_LOG(WARN, "switch memtables failed", K(ret));
        } else if (FAILED(finish_switching_memtable())) {
          XENGINE_LOG(WARN, "notify thread pool switching finished failed", K(ret));
        } else if (FAILED(db_impl_->flush_memtable_during_parallel_recovery(switched_sub_tables))) {
          XENGINE_LOG(WARN, "flush memtables failed", K(ret));
        }
      }
    }
    if (need_before_barrier) { // check if need barrier
      wait_for_all_tasks_executed();
    }
    if (check_print_interval()) { // print threadpool status
      print_replay_thread_pool_status();
    }
  }
  return ret;
}

int ReplayThreadPool::build_and_submit_task(WriteBatch *write_batch,
                                            uint64_t log_file_number,
                                            bool is_last_record,
                                            uint64_t last_record_end_pos,
                                            memory::ArenaAllocator &arena) {
  assert(write_batch != nullptr && write_batch->GetDataSize() >= WriteBatchInternal::kHeader);
  int ret = Status::kOk;
  if (UNLIKELY(!inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "submit task failed", K(ret));
  } else {
    ReplayTask *replay_task = ALLOC_OBJECT(ReplayTask, arena, write_batch,
        log_file_number, is_last_record, last_record_end_pos, db_impl_,
        this, &arena);
    if (replay_task == nullptr) {
      ret = Status::kMemoryLimit;
      XENGINE_LOG(WARN, "build replay task failed", K(ret));
    } else if (FAILED(pre_process_execute(replay_task->need_before_barrier_during_replay()))) {
      XENGINE_LOG(ERROR, "pre_process_execute before submit_task failed", K(ret));
    } else {
      bool need_after_barrier = replay_task->need_after_barrier_during_replay();
      if (FAILED(replay_thread_pool_executor_->submit_task(replay_task,
                                                 &arena, db_impl_->GetEnv()))) {
        XENGINE_LOG(ERROR, "submit replay task into thread pool failed", K(ret));
      } else { // submit task success
        if (need_after_barrier) { // check if need barrier
          wait_for_all_tasks_executed();
        }
      }
    }
  }
  return ret;
}

int ReplayThreadPool::stop() {
  int ret = Status::kOk;
  if (UNLIKELY(!inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "stop replay thread pool fail", K(ret));
  } else {
    if (nullptr != replay_thread_pool_executor_) {
      replay_thread_pool_executor_->stop();
    }
  }
  XENGINE_LOG(INFO, "stopped replay_thread_pool", K(ret));
  return ret;
}

int ReplayThreadPool::destroy(uint64_t* replayed_task_num) {
  int ret = Status::kOk;
  bool has_error = false;
  uint64_t replayed_num;
  if (UNLIKELY(!inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "destroy replay thread pool fail", K(ret));
  } else {
    if (nullptr != replay_thread_pool_executor_) {
      if (FAILED(replay_thread_pool_executor_->destroy())) {
        XENGINE_LOG(WARN, "destroy replay thread pool fail", K(ret));
      } else {
        has_error = replay_thread_pool_executor_->has_error();
        if (has_error) {
          ret = Status::kErrorUnexpected;
        }
        replayed_num = replay_thread_pool_executor_->get_executed_task_num();
        MOD_DELETE_OBJECT(ReplayThreadPoolExecutor, replay_thread_pool_executor_);
        replay_thread_pool_executor_ = nullptr;
        inited_ = false;
      }
      if (replayed_task_num != nullptr) {
        *replayed_task_num = replayed_num;
      }
    }
  }
  XENGINE_LOG(INFO, "replay_thread_pool destroyed", K(has_error), K(replayed_num));
  return ret;
}

int ReplayThreadPool::set_error() {
  int ret = Status::kOk;
  if (UNLIKELY(!inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "set error fail", K(ret));
  } else {
    replay_thread_pool_executor_->set_error();
  }
  return ret;
}

int ReplayThreadPool::set_during_switching_memtable() {
  int ret = Status::kOk;
  if (UNLIKELY(!inited_)) {
    ret = Status::kNotInit;
  } else {
    ret = replay_thread_pool_executor_->suspend();
  }
  XENGINE_LOG(INFO, "start switching memtable, replay_thread_pool will suspend", K(ret));
  return ret;
}

int ReplayThreadPool::finish_switching_memtable() {
  int ret = Status::kOk;
  if (UNLIKELY(!inited_)) {
    ret = Status::kNotInit;
  } else {
    ret = replay_thread_pool_executor_->resume();
  }
  XENGINE_LOG(INFO, "finish switching memtable, replay_thread_pool will resume", K(ret));
  return ret;
}

int ReplayThreadPool::wait_for_all_threads_suspend() {
  int ret = Status::kOk;
  if (UNLIKELY(!inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "wait for all threads suspend failed", K(ret));
  } else if (UNLIKELY(replay_thread_pool_executor_->has_error())) {
    ret = Status::kCorruption;
    XENGINE_LOG(WARN, "wait for all threads suspend failed", K(ret));
  } else if (UNLIKELY(!replay_thread_pool_executor_->is_suspend())) {
    ret = Status::kNotSupported;
    XENGINE_LOG(WARN, "wait for all threads suspend failed", K(ret));
  } else {
    auto env = db_impl_->GetEnv();
    last_log_wait_time_ = env->NowMicros();
    uint64_t current_timestamp = 0;
    while (SUCC(ret) &&
           replay_thread_pool_executor_->get_suspend_thread_num() < thread_count_) {
      current_timestamp = env->NowMicros();
      if (current_timestamp > last_log_wait_time_ &&
          current_timestamp - last_log_wait_time_ >= WAIT_THREADS_SUSPEND_LOG_INTERVAL) {
        last_log_wait_time_ = env->NowMicros();
        XENGINE_LOG(WARN, "wait for all threads suspend for too long", "suspend_threads_num", get_suspend_thread_num());
      }
      std::this_thread::yield();
    }
    if (replay_thread_pool_executor_->has_error()) {
      XENGINE_LOG(WARN, "fail to wait for all threads suspend, has error");
      ret = Status::kCorruption;
    }
  }
  return ret;
}

int ReplayThreadPool::wait_for_all_threads_stopped() {
  int ret = Status::kOk;
  if (UNLIKELY(!inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "wait for all threads stopped failed", K(ret));
  } else if (UNLIKELY(!replay_thread_pool_executor_->stopped())) {
    ret = Status::kNotSupported;
    XENGINE_LOG(WARN, "wait for all threads stopped failed", K(ret));
  } else {
    while (SUCC(ret) && !all_threads_stopped()) {
      if (FAILED(pre_process_execute())) {
        XENGINE_LOG(ERROR, "pre_process_parallel_replay while waiting for all threads stopped failed", K(ret));
        break;
      } else {
        std::this_thread::yield();
      }
    }
  }
  return ret;
}

bool ReplayThreadPool::all_threads_stopped() const {
  if (UNLIKELY(!inited_)) {
    return false;
  }
  bool finished = (get_stopped_thread_num() == thread_count_);
  if (finished) {
    auto left_task_num = replay_thread_pool_executor_->get_waitting_task_num();
    if (left_task_num != 0) {
      XENGINE_LOG(INFO, "replay_thread_pool has not executed all tasks", K(left_task_num));
    }
  }
  return finished;
}

bool ReplayThreadPool::check_print_interval() {
  auto env = db_impl_->GetEnv();
  if (last_print_status_time_ == 0) {
    last_print_status_time_ = env->NowMicros();
  }
  bool ret = false;
  auto current_timestamp = env->NowMicros();
  if (current_timestamp > last_print_status_time_ &&
      current_timestamp - last_print_status_time_ >= PRINT_STAT_INTERVAL) {
    last_print_status_time_ = env->NowMicros();
    ret = true;
  }
  return ret;
}

void ReplayThreadPool::print_replay_thread_pool_status() {
  XENGINE_LOG(INFO, "replay thread pool status", "submit_task_num",
      get_submit_task_num(), "executed_task_num", get_executed_task_num(),
      K(thread_count_), "suspend_threads_num", get_suspend_thread_num(),
      "stopped_threads_num", get_stopped_thread_num(), "has_error", has_error());
}

} // namespace db
} // namespace xengine
