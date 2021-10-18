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
#include "db/replay_task.h"
#include "db/replay_threadpool_executor.h"
#include "db/column_family.h"

namespace xengine {
namespace util {
class Env;
}

namespace db {
class ReplayTask;

class ReplayThreadPool {
public:
  ReplayThreadPool(int thread_count,
                   DBImpl *db_impl,
                   int max_queue_size = 20480,
                   const std::string& name = "replay_thread_pool")
      : thread_count_(thread_count),
        db_impl_(db_impl),
        max_queue_size_(max_queue_size),
        replay_thread_pool_executor_(nullptr),
        name_(name),
        inited_(false) {}
  ~ReplayThreadPool() {}
  int init();
  /* do something before executing like checking and switching memtable
   * this method should be called in the main recovery thread
   * */
  int pre_process_execute(bool need_before_barrier = false);

  /* build and submit replay task into thread pool
   * this method will wait for 'submit_log_interval' and print warning log if thread pool is still full
   * */
  int build_and_submit_task(WriteBatch *write_batch,
                            uint64_t log_file_number,
                            bool is_last_record,
                            uint64_t last_record_end_pos,
                            memory::ArenaAllocator &arena);

  /* stop thread pool to accept new tasks
   * after this method called, new task submitted into threadpool won't be scheduled
   * but submitted tasks will still run unless some error occurred
   * threads will exit after executing tasks left in task_queue
   * */
  int stop();
  /* wait for all threads stopping and destroy current thread pool
   * this method should be called after method stop(), otherwise will return error
   * returns Status::kErrorUnexpected if has_error_ is true
   * */
  int destroy(uint64_t* replayed_task_num = nullptr);
  int get_replay_thread_count() const {
    return thread_count_;
  }
  uint64_t get_submit_task_num() const {
    if (!inited_) return 0;
    return replay_thread_pool_executor_->get_submit_task_num();
  }
  uint64_t get_executed_task_num() const {
    if (!inited_) return 0;
    return replay_thread_pool_executor_->get_executed_task_num();
  }
  int get_suspend_thread_num() const {
    if (!inited_) return 0;
    return replay_thread_pool_executor_->get_suspend_thread_num();
  }
  int get_stopped_thread_num() const {
    if (!inited_) return 0;
    return replay_thread_pool_executor_->get_stopped_thread_num();
  }
  bool all_threads_stopped() const;
  int set_error();
  bool has_error() const {
    if (!inited_) return false;
    return replay_thread_pool_executor_->has_error();
  }

  /* *
   * set during_switching_memtable flag, which will make all threads in the
   * threadpool suspend
   * */
  int set_during_switching_memtable();
  /* *
   * unset during_switching_memtable flag, replay threads will resume after
   * calling this method
   * */
  int finish_switching_memtable();
  /* *
   * this method will return either when all threads are suspend or has error
   * this method can only be called after ReplayThreadPool::set_during_switching_memtable() is called
   * or will return kNotSupport otherwise
   * not thread safe, should be call by recovery main thread!
   * */
  int wait_for_all_threads_suspend();
  /* *
   * this method will return when all threads are stopped
   * this method can only be called after ReplayThreadPool::stop() is called
   * or will return kNotSupport otherwise
   * not thread safe, should be call by recovery main thread!
   * */
  int wait_for_all_threads_stopped();

  // return true if a->mem_usage > b->mem_usage
  struct SubtableMemoryUsageComparor {
    bool operator() (const SubTable *a, const SubTable *b) {
      SubTable *a_ = const_cast<SubTable*>(a);
      SubTable *b_ = const_cast<SubTable*>(b);
      if (IS_NULL(a_) || IS_NULL(b_) || IS_NULL(a_->mem()) || IS_NULL(b_->mem())) {
        // error
      } else {
        return a_->mem()->ApproximateMemoryUsage() > b_->mem()->ApproximateMemoryUsage();
      }
      return false;
    }
  };

  // return true if a->creation_seq < b->creation_seq
  struct SubtableSeqComparor {
    bool operator() (const SubTable *a, const SubTable *b) {
      SubTable *a_ = const_cast<SubTable*>(a);
      SubTable *b_ = const_cast<SubTable*>(b);
      if (IS_NULL(a_) || IS_NULL(b_) || IS_NULL(a_->mem()) || IS_NULL(b_->mem())) {
        // error
      } else {
        return a_->mem()->GetFirstSequenceNumber() < b_->mem()->GetFirstSequenceNumber();
      }
      return false;
    }
  };

#ifndef NDEBUG
  int submit_task(ReplayTask *task) {
    int ret = replay_thread_pool_executor_->submit_task(task);
    return ret;
  }
#endif

private:
  int wait_for_all_tasks_executed();
  bool check_print_interval();
  bool check_wait_interval();
  void print_replay_thread_pool_status();

private:
  int thread_count_;
  DBImpl *db_impl_;
  int max_queue_size_;
  ReplayThreadPoolExecutor<ReplayTaskDeleter> *replay_thread_pool_executor_;
  std::string name_;
  bool inited_;
  uint64_t last_print_status_time_;
  uint64_t last_log_wait_time_;
  static const uint64_t WAIT_THREADS_SUSPEND_LOG_INTERVAL = 5000 * 1000; // 5s
  static const uint64_t PRINT_STAT_INTERVAL = 10 * 60 * 1000 * 1000; // 10min
};

} // namespace db
} // namespace xengine
