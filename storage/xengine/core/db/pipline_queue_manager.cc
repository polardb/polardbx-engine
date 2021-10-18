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
#include "db/pipline_queue_manager.h"
#include "db/internal_stats.h"
#include "db/write_batch_internal.h"

using namespace xengine;
using namespace util;
using namespace common;

namespace xengine {
namespace db {

int PiplineQueueManager::init_pipline_queue() {
  int ret = 0;
  if (queues_inited_) return ret;

  ret = init_lock_free_queue(max_queue_size_, this->log_queue_buf_,
                             this->log_queue_);
  if (0 != ret) return ret;

  ret = init_lock_free_queue(max_queue_size_, this->memtable_queue_buf_,
                             this->memtable_queue_);
  if (0 != ret) return ret;

  // commit queue should be 10 times of other queue
  ret = init_lock_free_queue(max_queue_size_ * 10, this->commit_queue_buf_,
                             this->commit_queue_);
  if (0 != ret) return ret;

//  error_queue_.reset(new LockFreeQueue<WriteRequest>(max_queue_size_));
  error_queue_.reset(MOD_NEW_OBJECT(memory::ModId::kDefaultMod, LockFreeQueue<WriteRequest>, max_queue_size_));
  ret = error_queue_->init();
  if (0 != ret) return ret;

  this->queues_inited_ = true;
  return ret;
}

void PiplineQueueManager::destroy_pipline_queue() {
  if (!queues_inited_) return;

  assert(0 == get_copy_log_job_num());
  assert(0 == get_flush_log_job_num());
  assert(0 == get_memtable_job_num());
  assert(0 == get_commit_job_num());
  assert(0 == error_queue_->size());

  if (this->log_queue_) delete this->log_queue_;
  if (this->log_queue_buf_) free(this->log_queue_buf_);
  if (this->memtable_queue_) delete this->memtable_queue_;
  if (this->memtable_queue_buf_) free(this->memtable_queue_buf_);
  if (this->commit_queue_) delete this->commit_queue_;
  if (this->commit_queue_buf_) free(this->commit_queue_buf_);
  error_queue_->destroy();
  queues_inited_ = false;
}

size_t PiplineQueueManager::add_copy_log_job(WriteRequest* request) {
  // hold the dbimpl_->mutex before do this
  // recored sequence before put in lock free queue;
  uint64_t sequence = request->group_first_sequence_;
  while (true) {
    int ret = this->log_queue_->push(request);
    if (ret == e_OK) break;
  }
  // we will hold pipline_copy_log_mutex_, so it is safe to set
  this->last_sequence_post_to_log_queue_.store(sequence);
  return this->log_queue_->get_total();
}

size_t PiplineQueueManager::pop_copy_log_job(WriteRequest*& request) {
  size_t job_num = 0;
  int ret = this->log_queue_->pop(request);
  if (e_OK == ret) {
    job_num = 1;
  }
  assert(e_OK == ret || e_ENTRY_NOT_EXIST == ret);
  return job_num;
}

size_t PiplineQueueManager::add_flush_log_job(WriteRequest* request) {
  MutexLock lock_guard(&buffered_log_queue_mutex_);
  this->buffered_log_queue_.push(request);
  this->last_sequence_post_to_flush_queue_.store(
      request->group_first_sequence_);
  return this->buffered_log_queue_.size();
}

size_t PiplineQueueManager::pop_flush_log_job(WriteRequest*& request) {
  size_t job_num = 0;
  MutexLock lock_guard(&buffered_log_queue_mutex_);
  if (this->buffered_log_queue_.size() > 0) {
    request = this->buffered_log_queue_.front();
    this->buffered_log_queue_.pop();
    job_num = 1;
  }
  return job_num;
}

size_t PiplineQueueManager::pop_flush_log_job(
    uint64_t target_pos, std::vector<WriteRequest*>& flush_list,
    bool& need_sync_wal) {
  MutexLock lock_guard(&buffered_log_queue_mutex_);
  assert(flush_list.size() == 0);
  while (this->buffered_log_queue_.size() > 0) {
    WriteRequest* request = this->buffered_log_queue_.front();
    if (request->log_file_pos_ > target_pos) break;
    need_sync_wal = need_sync_wal || request->group_need_log_sync_;
    flush_list.push_back(request);
    this->buffered_log_queue_.pop();
  }
  return flush_list.size();
}

size_t PiplineQueueManager::add_memtable_job(WriteRequest* request) {
  // recored sequence before put in lock free queue;
  uint64_t sequence = request->group_first_sequence_;
  while (true) {  // currently we try endless
    int ret = this->memtable_queue_->push(request);
    if (e_OK == ret) break;
  }
  // we will hold pipline_flush_log_mutex_,so it is safe to set
  this->last_sequence_post_to_memtable_queue_.store(sequence);
  return this->memtable_queue_->get_total();
}

size_t PiplineQueueManager::pop_memtable_job(WriteRequest*& request) {
  size_t job_num = 0;
  int ret = this->memtable_queue_->pop(request);
  if (e_OK == ret) {
    job_num = 1;
  }
  assert(e_OK == ret || e_ENTRY_NOT_EXIST == ret);
  return job_num;
}

size_t PiplineQueueManager::add_commit_job(WriteRequest* request) {
  while (true) {
    int ret = this->commit_queue_->push(request);
    if (e_OK == ret) break;
  }
  return this->commit_queue_->get_total();
}

size_t PiplineQueueManager::pop_commit_job(WriteRequest*& request) {
  size_t job_num = 0;
  int ret = this->commit_queue_->pop(request);
  if (e_OK == ret) {
    job_num = 1;
  }
  assert(e_OK == ret || e_ENTRY_NOT_EXIST == ret);
  return job_num;
}

size_t PiplineQueueManager::add_error_job(WriteRequest* request) {
  this->error_queue_->push(request);
  return this->error_queue_->size();
}

size_t PiplineQueueManager::pop_error_job(WriteRequest*& request) {
  int job_num = this->error_queue_->pop(request);
  return job_num;
}

}  // namespace db
}  // namespace xengine
