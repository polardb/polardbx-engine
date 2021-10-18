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

#include "xengine/slice.h"
#include "util/spin_lock.h"

#include <atomic>
#include <pthread.h>

using namespace xengine;
using namespace xengine::common;

namespace xengine {
namespace util {

typedef void (*EBRDeleteFunc)(void*);

class EBRNode {
private:
  void *ptr_;
  EBRDeleteFunc func_;
  std::atomic<EBRNode *> next_;

public:
  EBRNode() : ptr_(nullptr), func_(nullptr), next_(nullptr) {}
  EBRNode(void *p, EBRDeleteFunc f) : ptr_(p), func_(f), next_(nullptr) {}
  EBRNode *next() { return next_.load(); }
  void set_next(EBRNode *n) { next_.store(n); }
  bool next_cas(EBRNode *&old, EBRNode *n) { return next_.compare_exchange_strong(old, n); }
  EBRNode *next_exchange(EBRNode *n) { return next_.exchange(n); }
  void do_reclaim() { func_(ptr_); }
};

class EBRManager;

class LocalEBR {
private:
  LocalEBR *next_;
  LocalEBR *prev_;
  std::atomic<uint32_t> local_epoch_;
  EBRNode *local_limbo_list_head_;
  EBRNode *local_limbo_list_tail_;
  uint64_t local_removed_count_;
  EBRManager *ebr_;
public:
  LocalEBR(EBRManager *ebr=nullptr)
  : next_(nullptr), 
    prev_(nullptr),
    local_epoch_(0),
    local_limbo_list_head_(nullptr),
    local_limbo_list_tail_(nullptr),
    local_removed_count_(0),
    ebr_(ebr) {}
  LocalEBR *next() { return next_; }
  LocalEBR *prev() { return prev_; }
  void set_next(LocalEBR *n) { next_ = n; }
  void set_prev(LocalEBR *p) { prev_ = p; }
  uint32_t get_local_epoch() { return local_epoch_.load(); }
  void enter_critical_area(uint32_t global_epoch);
  void leave_critical_area();
  int remove(void *p, EBRDeleteFunc f);
};

class EBRManager {
public:
  static const uint32_t MAX_EPOCH = 3;
  static const uint32_t ACTIVE_MASK = (1U << 31);
  static const int64_t RECLAIM_LIMIT = 128;

private:
  std::atomic<uint32_t> global_epoch_;
  EBRNode curr_limbo_list_;
  EBRNode limbo_list_[MAX_EPOCH];
  pthread_key_t local_epoch_;
  pthread_t pid;
  SpinLock lock_; // Protect local epoch list
  LocalEBR local_epoch_list_; // Dummy node
  std::atomic<int64_t> removed_count_;

  uint32_t get_gc_epoch() { return (global_epoch_.load() + 1) % MAX_EPOCH; }
  uint32_t get_staging_epoch() { return global_epoch_.load(); }
  LocalEBR *get_local_ebr() { return reinterpret_cast<LocalEBR *>(pthread_getspecific(local_epoch_)); } 
  SpinLock &get_lock() { return lock_; }
  void forward_epoch() { global_epoch_.store((global_epoch_.load() + 1) % MAX_EPOCH); }
  bool maybe_forward_epoch(uint32_t &gc_epoch);
  void reclaim(EBRNode *retired_list);
  bool need_reclaim() { return removed_count_.load() > RECLAIM_LIMIT; }
  int thread_register(LocalEBR *&local_epoch);
public:
  EBRManager() 
  : global_epoch_(0), 
    removed_count_(0) {
    pthread_key_create(&local_epoch_, thread_exit);
    local_epoch_list_.set_next(&local_epoch_list_);
    local_epoch_list_.set_prev(&local_epoch_list_);
  }
  static EBRManager &get_instance() { static EBRManager ebr; return ebr; }
  void enter_critical_area();
  void leave_critical_area();
  int remove(void *p, EBRDeleteFunc f);
  void add_to_limbo_list(EBRNode *head, EBRNode *tail, uint64_t removed_count);
  void maybe_do_reclaim();
  static bool is_epoch_active(uint32_t local_epoch) { return (local_epoch & ACTIVE_MASK) > 0; }
  static uint32_t epoch2active(uint32_t local_epoch) { return local_epoch | ACTIVE_MASK; }
  static void thread_exit(void* ptr);
  pthread_key_t &local_epoch_thread_key() { return local_epoch_; }
};

class EBRGaurd {
public:
  EBRGaurd() { xengine::util::EBRManager::get_instance().enter_critical_area(); }
  ~EBRGaurd() { xengine::util::EBRManager::get_instance().leave_critical_area(); }
};

} // namespace util
} // namespace xengine

#define EBRMANAGER (xengine::util::EBRManager::get_instance())
#define ENTER_CRITICAL (EBRMANAGER.enter_critical_area())
#define LEAVE_CRITICAL (EBRMANAGER.leave_critical_area())
#define EBR_REMOVE(p, f) (EBRMANAGER.remove((p), (f)))
#define EBR_MAYBE_RECLAIM (EBRMANAGER.maybe_do_reclaim())
