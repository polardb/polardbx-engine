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
#ifndef XENGINE_DB_PIPLINE_QUEUE_MANAGER_H
#define XENGINE_DB_PIPLINE_QUEUE_MANAGER_H

#include <atomic>
#include <deque>
#include <functional>
#include <limits>
#include <list>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "db/batch_group.h"
#include "db/log_writer.h"
#include "monitoring/instrumented_mutex.h"
#include "port/port.h"
#include "util/lock_free_fixed_queue.h"
#include "xengine/db.h"
#include "xengine/env.h"
#include "xengine/status.h"

namespace xengine {
namespace db {

using util::FixedQueue;

class PiplineQueueManager {
 public:
  PiplineQueueManager(uint64_t max_queue_size = 100 * 1024)
      : queues_inited_(false),
        max_queue_size_(max_queue_size),
        log_queue_buf_(nullptr),
        log_queue_(nullptr),
        buffered_log_queue_mutex_(false),
        memtable_queue_buf_(nullptr),
        memtable_queue_(nullptr),
        commit_queue_buf_(nullptr),
        commit_queue_(nullptr),
        last_sequence_post_to_log_queue_(0),
        last_sequence_post_to_flush_queue_(0),
        last_sequence_post_to_memtable_queue_(0),
        last_flushed_log_lsn_(0) {}
  ~PiplineQueueManager() { destroy_pipline_queue(); }

 public:
  int init_pipline_queue();

  void destroy_pipline_queue();

  size_t add_copy_log_job(WriteRequest* request);
  size_t pop_copy_log_job(WriteRequest*& log_request);

  size_t add_flush_log_job(WriteRequest* request);
  size_t pop_flush_log_job(WriteRequest*& log_request);
  size_t pop_flush_log_job(uint64_t target_pos,
                           std::vector<WriteRequest*>& flush_list,
                           bool& need_sync_wal);

  size_t add_memtable_job(WriteRequest* request);
  size_t pop_memtable_job(WriteRequest*& log_request);

  size_t add_commit_job(WriteRequest* request);
  size_t pop_commit_job(WriteRequest*& log_request);

  size_t add_error_job(WriteRequest* request);
  size_t pop_error_job(WriteRequest*& log_request);

  uint64_t get_copy_log_job_num() { return this->log_queue_->get_total(); }

  uint64_t inline get_flush_log_job_num() {
    util::MutexLock lock_guard(&buffered_log_queue_mutex_);
    return this->buffered_log_queue_.size();
  }

  uint64_t get_memtable_job_num() { return this->memtable_queue_->get_total(); }

  uint64_t get_commit_job_num() { return this->commit_queue_->get_total(); }

  bool is_log_job_done(uint64_t thread_local_expected_seq) {
    bool copy_done = this->is_copy_log_job_done(thread_local_expected_seq);
    bool flush_done = this->is_flush_log_job_done(thread_local_expected_seq);
    return (copy_done && flush_done);
  }

  bool is_copy_log_job_done(uint64_t thread_local_expected_seq) {
    bool ret = false;
    if (this->last_sequence_post_to_flush_queue_.load() >=
        thread_local_expected_seq) {
      ret = true;
    }
    return ret;
  }

  bool is_flush_log_job_done(uint64_t thread_local_expected_seq) {
    bool ret = false;
    if (this->last_sequence_post_to_memtable_queue_.load() >=
        thread_local_expected_seq) {
      ret = true;
    }
    return ret;
  }

  bool is_memtable_job_done(uint64_t thread_local_expected_seq) {
    return (0 == this->memtable_queue_->get_total());
  }

  bool is_commit_job_done(uint64_t thread_local_expected_seq) {
    return (0 == this->commit_queue_->get_total());
  }

  uint64_t get_last_sequence_post_to_log_queue() {
    return this->last_sequence_post_to_log_queue_.load();
  }

  uint64_t get_last_sequence_post_to_flush_queue() {
    return this->last_sequence_post_to_flush_queue_.load();
  }

  uint64_t get_last_sequence_post_to_memtable_queue() {
    return this->last_sequence_post_to_memtable_queue_.load();
  }

 private:
  bool queues_inited_;

  uint64_t max_queue_size_;

  char* log_queue_buf_;
  FixedQueue<WriteRequest>* log_queue_;

  port::Mutex buffered_log_queue_mutex_;
  std::queue<WriteRequest*> buffered_log_queue_;

  char* memtable_queue_buf_;
  FixedQueue<WriteRequest>* memtable_queue_;

  char* commit_queue_buf_;
  FixedQueue<WriteRequest>* commit_queue_;

  std::unique_ptr<util::LockFreeQueue<WriteRequest>,
  memory::ptr_destruct_delete<util::LockFreeQueue<WriteRequest>>> error_queue_;

  std::atomic<uint64_t> last_sequence_post_to_log_queue_;
  std::atomic<uint64_t> last_sequence_post_to_flush_queue_;
  std::atomic<uint64_t> last_sequence_post_to_memtable_queue_;

  std::atomic<uint64_t> last_flushed_log_lsn_;
};
}  // end of db
}  // end of xengine
#endif
