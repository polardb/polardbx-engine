//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "cache/lru_cache.h"

#include <algorithm>
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <thread>

#include "cache/row_cache.h"
#include "logger/logger.h"
#include "memory/mod_info.h"
#include "memory/base_malloc.h"
#include "util/mutexlock.h"
#include "xengine/env.h"
#include "monitoring/query_perf_context.h"

using namespace xengine;
using namespace common;
using namespace util;
using namespace memory;
using namespace monitor;

namespace xengine {
namespace cache {

LRUHandleTable::LRUHandleTable()
    : length_(0),
      elems_(0),
      list_(nullptr) {
  Resize();
}

LRUHandleTable::~LRUHandleTable() {
  ApplyToAllCacheEntries([](LRUHandle* h) {
    if (h->refs == 1) {
      h->Free();
    }
  });
  delete[] list_;
}

LRUHandle* LRUHandleTable::Lookup(const Slice& key, uint32_t hash) {
  return *FindPointer(key, hash);
}

LRUHandle* LRUHandleTable::Insert(LRUHandle* h) {
  LRUHandle** ptr = FindPointer(h->key(), h->hash);
  LRUHandle* old = *ptr;
  h->next_hash = (old == nullptr ? nullptr : old->next_hash);
  *ptr = h;
  if (old == nullptr) {
    ++elems_;
    if (elems_ > length_) {
      // Since each cache entry is fairly large, we aim for a small
      // average linked list length (<= 1).
      Resize();
    }
  }
  return old;
}

LRUHandle* LRUHandleTable::Remove(const Slice& key, uint32_t hash) {
  LRUHandle** ptr = FindPointer(key, hash);
  LRUHandle* result = *ptr;
  if (result != nullptr) {
    *ptr = result->next_hash;
    --elems_;
  }
  return result;
}

LRUHandle** LRUHandleTable::FindPointer(const Slice& key, uint32_t hash) {
  LRUHandle** ptr = &list_[hash & (length_ - 1)];
  while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
    ptr = &(*ptr)->next_hash;
  }
  return ptr;
}

void LRUHandleTable::Resize() {
  uint32_t new_length = 16;
  while (new_length < elems_ * 1.5) {
    new_length *= 2;
  }
  LRUHandle** new_list = new LRUHandle*[new_length];
  memset(new_list, 0, sizeof(new_list[0]) * new_length);
  uint32_t count = 0;
  for (uint32_t i = 0; i < length_; i++) {
    LRUHandle* h = list_[i];
    while (h != nullptr) {
      LRUHandle* next = h->next_hash;
      uint32_t hash = h->hash;
      LRUHandle** ptr = &new_list[hash & (new_length - 1)];
      h->next_hash = *ptr;
      *ptr = h;
      h = next;
      count++;
    }
  }
  assert(elems_ == count);
  delete[] list_;
  list_ = new_list;
  length_ = new_length;
}

void LRUHandle::Free() {
  assert((refs == 1 && InCache()) || (refs == 0 && !InCache()));
  if (deleter) {
    (*deleter)(key(), value);
  }
  base_free(this);
}

LRUCacheShard::LRUCacheShard()
    : capacity_(0),
      usage_(0),
      lru_usage_(0),
      lru_old_usage_(0),
      high_pri_pool_usage_(0),
      strict_capacity_limit_(false),
      high_pri_pool_ratio_(0),
      old_pool_ratio_(0),
      high_pri_pool_capacity_(0),
      old_pool_capacity_(0),
//      mutex_(mutex_lru_cache_key),
      lru_old_start_(nullptr),
      table_(),
      lru_cache_(nullptr),
      print_flag_(false) {
  adjust_min_capacity_ = 64 * 1024 *1024; // 64M
  lru_old_adjust_capacity_ = 2 * 1024 * 1024; // 2M
  // Make empty circular linked list
  lru_.next = &lru_;
  lru_.prev = &lru_;
  lru_low_pri_ = &lru_;
//  mutex_.set_trace_point(TracePoint::LRU_CACHE_MUTEX_WAIT);
}

LRUCacheShard::~LRUCacheShard() {}

bool LRUCacheShard::Unref(LRUHandle* e) {
  assert(e->refs > 0);
  e->refs--;
  return e->refs == 0;
}

// Call deleter and free

void LRUCacheShard::EraseUnRefEntries() {
  autovector<LRUHandle*> last_reference_list;
  {
//    WMS_MUTEX_GUARD(&mutex_);
    MutexLock l(&mutex_);
    while (lru_.next != &lru_) {
      LRUHandle* old = lru_.next;
      assert(old->InCache());
      assert(old->refs ==
             1);  // LRU list contains elements which may be evicted
      LRU_Remove(old);
      table_.Remove(old->key(), old->hash);
      old->SetInCache(false);
      Unref(old);
      usage_ -= old->charge;
      last_reference_list.push_back(old);
    }
  }

  for (auto entry : last_reference_list) {
    entry->Free();
  }
}

void LRUCacheShard::ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                           bool thread_safe) {
  if (thread_safe) {
    mutex_.Lock();
//    WMS_MUTEX_ENTER(&mutex_);
  }
  table_.ApplyToAllCacheEntries(
      [callback](LRUHandle* h) { callback(h->value, h->charge); });
  if (thread_safe) {
    mutex_.Unlock();
//    WMS_MUTEX_EXIT(&mutex_);
  }
}

void LRUCacheShard::TEST_GetLRUList(LRUHandle** lru,
    LRUHandle** lru_low_pri, LRUHandle** lru_old_start) {
  *lru = &lru_;
  *lru_low_pri = lru_low_pri_;
  *lru_old_start = lru_old_start_;
}

void LRUCacheShard::LRU_Remove(LRUHandle* e) {
  assert(e->next != nullptr);
  assert(e->prev != nullptr);
  if (lru_old_start_ == e) {
    lru_old_start_ = e->prev;
  }
  if (lru_low_pri_ == e) {
    lru_low_pri_ = e->prev;
  }
  e->next->prev = e->prev;
  e->prev->next = e->next;
  e->prev = e->next = nullptr;
  lru_usage_ -= e->charge;
  if (e->is_in_old_pool()) {
    assert(lru_old_usage_ >= e->charge);
    lru_old_usage_ -= e->charge;
  }
  if (e->InHighPriPool()) {
    assert(high_pri_pool_usage_ >= e->charge);
    high_pri_pool_usage_ -= e->charge;
  }
}

void LRUCacheShard::init_old_list() {
  assert(lru_low_pri_);
  LRUHandle *node = lru_.next;
//  int64_t subtable_id = -1;
  size_t old_usage_cap = lru_usage_ * old_pool_ratio_;
  while (node != lru_low_pri_) {
    assert(node);
    node->set_in_old_pool(true);
    node->set_old(true);
 //   record_block_step(node, LRUHandle::BeOld);
//    subtable_id_to_stats(subtable_id).cold_count_++;
    lru_old_usage_ += node->charge;
    if (lru_old_usage_ > old_usage_cap) {
      break;
    }
    node = node->next;
  }
  lru_old_start_ = node;
  assert(lru_usage_ >= lru_old_usage_);
}

void LRUCacheShard::add_lru_list_head(LRUHandle *e) {
  if (high_pri_pool_ratio_ > 0 && e->IsHighPri()) {
    // Inset "e" to head of LRU list.
    e->next = &lru_;
    e->prev = lru_.prev;
    e->prev->next = e;
    e->next->prev = e;
    e->SetInHighPriPool(true);
    high_pri_pool_usage_ += e->charge;
    MaintainPoolSize();
  } else {
    // Insert "e" to the head of low-pri pool. Note that when
    // high_pri_pool_ratio is 0, head of low-pri pool is also head of LRU list.
    e->next = lru_low_pri_->next;
    e->prev = lru_low_pri_;
    e->prev->next = e;
    e->next->prev = e;
    e->SetInHighPriPool(false);
    lru_low_pri_ = e;
  }
  e->set_in_old_pool(false);
  lru_usage_ += e->charge;
}

void LRUCacheShard::adjust_old_list() {
  size_t old_pool_cap = lru_usage_ * old_pool_ratio_;
  size_t adjust_min_capacity = old_pool_cap > lru_old_adjust_capacity_
      ? old_pool_cap - lru_old_adjust_capacity_
      : 0;
  size_t adjust_max_capacity = old_pool_cap + lru_old_adjust_capacity_;
  LRUHandle *node = lru_old_start_;
  bool finish = false;
  while (node != &lru_ && !finish) {
    if (lru_old_usage_ > adjust_max_capacity) {
      lru_old_usage_ -= node->charge;
      node->set_in_old_pool(false);
      node->set_old(false);
//      record_block_step(node, LRUHandle::BeYoung);
      node = node->prev;
      if (lru_old_usage_ < adjust_min_capacity) {
        finish = true;
      }
    } else if (node->next != &lru_
               && !node->next->InHighPriPool()
               && lru_old_usage_ < adjust_min_capacity) {
      node = node->next;
      lru_old_usage_ += node->charge;
      node->set_in_old_pool(true);
      node->set_old(true);
 //     record_block_step(node, LRUHandle::BeOld);
      if (lru_old_usage_ > adjust_max_capacity) {
        finish = true;
      }
    } else {
      finish = true;
    }
  }
  lru_old_start_ = node;
}

void LRUCacheShard::add_old_list(LRUHandle *e) {
  assert(lru_old_start_);
  e->prev = lru_old_start_;
  e->next = lru_old_start_->next;
  e->next->prev = e;
  e->prev->next = e;
  lru_old_start_ = e;
  if (lru_low_pri_ == e->prev) {
    lru_low_pri_ = e;
  }
  e->set_in_old_pool(true);
  lru_old_usage_ += e->charge;
  lru_usage_ += e->charge;
}

void LRUCacheShard::LRU_Insert(LRUHandle* e, bool old) {
  assert(e->next == nullptr);
  assert(e->prev == nullptr);
  if (old_pool_ratio_ > 0) {
    // check low_pri_usage_ and lru_old_usage_
    if (!old
        || e->IsHighPri()
        || (lru_usage_ < adjust_min_capacity_ && nullptr == lru_old_start_)) {
      e->set_old(false);
      add_lru_list_head(e);
    } else {
      e->set_old(true);
      add_old_list(e);
    }

    if (lru_usage_ >= adjust_min_capacity_) {
      if (nullptr == lru_old_start_) {
        init_old_list();
      } else {
        adjust_old_list();
      }
    }
  } else {
    add_lru_list_head(e);
  }
}

//void LRUCacheShard::record_block_step(LRUHandle *e, LRUHandle::OperateType type) const {
//  if (!print_flag_) {
//    return;
//  }
//  LRUHandle::BlockStep step;
//  step.type_ = type;
//  if (e->is_old()) {
//    step.is_old_data_ = true;
//  }
//  if (e->IsHighPri()) {
//    step.is_high_pri_ = true;
//  }
//  if (1 != e->refs) {
//    step.status_ = LRUHandle::NotInLRU;
//  } else if (e->is_in_old_pool()) {
//    step.status_ = LRUHandle::InOldPool;
//  } else if (e->InHighPriPool()) {
//    step.status_ = LRUHandle::InHighPool;
//  } else {
//    step.status_ = LRUHandle::InLowPool;
//  }
//  step.access_time_ = Env::Default()->NowMicros();
//  if (e->steps_.size() > 20 && LRUHandle::Evict != type) {
//    LRUHandle::BlockStep last_step = e->steps_.at(e->steps_.size() - 1);
//    e->steps_.clear();
//    e->steps_.push_back(last_step);
//    e->steps_.push_back(step);
//  }
//
//}

void LRUCacheShard::MaintainPoolSize() {
  while (high_pri_pool_usage_ > high_pri_pool_capacity_) {
    // Overflow last entry in high-pri pool to low-pri pool.
    lru_low_pri_ = lru_low_pri_->next;
    assert(lru_low_pri_ != &lru_);
    if (!lru_low_pri_->InHighPriPool()) {
      abort();
    }
    lru_low_pri_->SetInHighPriPool(false);
 //   record_block_step(lru_low_pri_, LRUHandle::BeLow);
    high_pri_pool_usage_ -= lru_low_pri_->charge;
  }
}

void LRUCacheShard::EvictFromLRU(size_t charge,
                                 autovector<LRUHandle*>* deleted) {
  while (usage_ + charge > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;
    assert(old->InCache());
    assert(old->refs == 1);  // LRU list contains elements which may be evicted
    LRU_Remove(old);
    table_.Remove(old->key(), old->hash);
    old->SetInCache(false);
    Unref(old);
    usage_ -= old->charge;
    deleted->push_back(old);
  }
}

void LRUCacheShard::SetCapacity(size_t capacity) {
  autovector<LRUHandle*> last_reference_list;
  {
	// called in constructor, no lock needed
    capacity_ = capacity;
    high_pri_pool_capacity_ = capacity_ * high_pri_pool_ratio_;
    EvictFromLRU(0, &last_reference_list);
  }
  // we free the entries here outside of mutex for
  // performance reasons
  for (auto entry : last_reference_list) {
    entry->Free();
  }
}

void LRUCacheShard::SetStrictCapacityLimit(bool strict_capacity_limit) {
  // called in constructor, no lock needed
  strict_capacity_limit_ = strict_capacity_limit;
}

bool LRUCacheShard::in_cache(const Slice& key, uint32_t hash)
{
  // can't delete, #bug17117542
  MutexLock l(&mutex_);
//  WMS_MUTEX_GUARD(&mutex_);
  LRUHandle* e = table_.Lookup(key, hash);
  return nullptr != e;
}

Cache::Handle* LRUCacheShard::Lookup(const Slice& key, uint32_t hash,
                                     const uint64_t seq,
                                     const uint32_t cfd_id) {
  QUERY_COUNT(CountPoint::ENGINE_LOGICAL_READ);
  MutexLock l(&mutex_);
//  WMS_MUTEX_GUARD(&mutex_);
  if (nullptr != lru_cache_ && lru_cache_->is_row_cache()) {
    const ShardedCache::SeqRange seq_range = lru_cache_->get_seq_range(cfd_id);
    if (seq < seq_range.start_seq_) {
      return nullptr;
    }
  }
  LRUHandle *e = table_.Lookup(key, hash);
  if (e != nullptr) {
    assert(e->InCache());
    if (e->refs == 1) {
      LRU_Remove(e);
    }
    e->refs++;
    e->set_old(false);
 //   record_block_step(e, LRUHandle::Read);
  }

  return reinterpret_cast<Cache::Handle*>(e);
}

//CacheStats &LRUCacheShard::subtable_id_to_stats(int64_t subtable_id) {
//  auto subtable = subtable_stats_.find(subtable_id);
//  if (subtable == subtable_stats_.end()) {
//    subtable_stats_.emplace(subtable_id, CacheStats::ZERO_STATS);
//    subtable = subtable_stats_.find(subtable_id);
//  }
//  assert(subtable != subtable_stats_.end());
//  return subtable->second;
//}

//CacheStats &LRUCacheShard::get_subtable_stats(int64_t subtable_id) {
//  return subtable_id_to_stats(subtable_id);
//}

bool LRUCacheShard::Ref(Cache::Handle* h) {
  LRUHandle* handle = reinterpret_cast<LRUHandle*>(h);
  MutexLock l(&mutex_);
//  WMS_MUTEX_GUARD(&mutex_);
  if (handle->InCache() && handle->refs == 1) {
    LRU_Remove(handle);
  }
  handle->refs++;
  return true;
}

//void LRUCacheShard::print_block_step(LRUHandle *e) const {
//  if (!print_flag_) {
//    return;
//  }
//  assert(e);
//  char buf[101];
//  std::string str;
//  uint64_t delt_time = 0;
//  if (e->steps_.size()) {
//    delt_time = Env::Default()->NowMicros()
//        - e->steps_.at(e->steps_.size() - 1).access_time_;
//  }
//  snprintf(buf, sizeof(buf),
//      "block.info: [charge,%u], [refs, %u], [delt_time, %lu]\n",
//      (unsigned int)e->charge, (unsigned int)e->refs, delt_time);
//  str.append(buf);
//
//  uint64_t last_time = 0;
//  for(size_t i = 0; i < e->steps_.size(); ++i) {
//    LRUHandle::BlockStep &step = e->steps_.at(i);
//    snprintf(buf, sizeof(buf),
//        "step:type %s, status %s, delt time %lu, is_old %d, is_high_pri %d\n",
//        OperateTypeStr.at(step.type_).c_str(),
//        BlockStatStr.at(step.status_).c_str(),
//        step.access_time_ - last_time, step.is_old_data_, step.is_high_pri_);
//    last_time = step.access_time_;
//    str.append(buf);
//  }
//  __XENGINE_LOG(INFO, "%s", str.c_str());
//}

//void LRUCacheShard::print_shard_info() {
//  XENGINE_LOG(INFO, "cache.shard.info: usage info", K(usage_), K(lru_usage_),
//      K(lru_old_usage_), K(high_pri_pool_usage_));
//  print_flag_ = true;
//  MutexLock l(&mutex_);
//  LRUHandle *node = lru_.next;
//  size_t block_cnt = 0;
//  size_t no_record = 0;
//  while (node != &lru_) {
//    ++block_cnt;
//    if (node->steps_.size()) {
//      size_t idx = node->steps_.size() - 1;
//      uint64_t cur_time = Env::Default()->NowMicros();
//      if (cur_time - node->steps_.at(idx).access_time_  > LONG_TIME_LIMIT) {
//        print_block_step(node);
//      }
//    } else {
//      ++no_record;
//    }
//    node = node->next;
//  }
//
//  XENGINE_LOG(INFO, "cache.shard.info: block count", K(block_cnt), K(no_record));
//}

void LRUCacheShard::SetHighPriorityPoolRatio(double high_pri_pool_ratio) {
  // called in constructor, no lock needed
  high_pri_pool_ratio_ = high_pri_pool_ratio;
  high_pri_pool_capacity_ = capacity_ * high_pri_pool_ratio_;
  MaintainPoolSize();
}

void LRUCacheShard::set_old_pool_ratio(double old_pool_ratio)
{
  // called in constructor, no lock needed
  old_pool_ratio_ = old_pool_ratio;
  old_pool_capacity_ = capacity_ * old_pool_ratio;
  MaintainPoolSize();
}

void LRUCacheShard::Release(Cache::Handle* handle) {
  if (handle == nullptr) {
    return;
  }
  LRUHandle* e = reinterpret_cast<LRUHandle*>(handle);
  bool last_reference = false;
  {
    MutexLock l(&mutex_);
//    WMS_MUTEX_GUARD(&mutex_);
    last_reference = Unref(e);
    if (last_reference) {
      usage_ -= e->charge;
    }
    if (e->refs == 1 && e->InCache()) {
      // The item is still in cache, and nobody else holds a reference to it
      if (usage_ > capacity_) {
        // the cache is full
        // The LRU list must be empty since the cache is full
        assert(lru_.next == &lru_);
        // take this opportunity and remove the item
        table_.Remove(e->key(), e->hash);
        e->SetInCache(false);
        Unref(e);
        usage_ -= e->charge;
        last_reference = true;
      } else {// put the item on the list to be potentially freed
        LRU_Insert(e, e->is_old());
      }
    }
    //record_block_step(e, LRUHandle::Release);
  }

  // free outside of mutex
  if (last_reference) {
   // print_block_step(e);
    e->Free();
  }
}

Status LRUCacheShard::Insert(const Slice& key, uint32_t hash, void* value,
                             size_t charge,
                             void (*deleter)(const Slice& key, void* value),
                             Cache::Handle** handle, Cache::Priority priority,
                             bool is_old, const uint64_t seq, const uint32_t cfd_id) {
  // Allocate the memory here outside of the mutex
  // If the cache is full, we'll have to release it
  // It shouldn't happen very often though.

  void *buf = base_malloc(sizeof(LRUHandle) - 1 + key.size(), ModId::kLruCache);
  LRUHandle* e = new (buf) LRUHandle();
  Status s;
  autovector<LRUHandle*> last_reference_list;

  e->value = value;
  e->deleter = deleter;
  e->charge = charge;
  e->key_length = key.size();
  e->hash = hash;
  e->refs = (handle == nullptr
                 ? 1
                 : 2);  // One from LRUCache, one for the returned handle
  e->next = e->prev = nullptr;
  e->SetInCache(true);
  e->SetPriority(priority);
  e->set_old(is_old);

  memcpy(e->key_data, key.data(), key.size());

  {
    MutexLock l(&mutex_);
//    WMS_MUTEX_GUARD(&mutex_);
    bool add = true;
    if (nullptr != lru_cache_ && lru_cache_->is_row_cache()) {
      const ShardedCache::SeqRange seq_range =
          lru_cache_->get_seq_range(cfd_id);
      if (0 != seq_range.end_seq_/*not init*/
          && seq <= seq_range.end_seq_) {
        // if current seq maybe in flush process,not add it into cache
        // because it maybe become one old version data
        if (handle == nullptr) {
          last_reference_list.push_back(e);
        } else {
          base_free(e);
          *handle = nullptr;
        }
        add = false;
      }
    }
    if (add) {
      // Free the space following strict LRU policy until enough space
      // is freed or the lru list is empty
      EvictFromLRU(charge, &last_reference_list);

      if (usage_ - lru_usage_ + charge > capacity_ &&
          (strict_capacity_limit_ || handle == nullptr)) {
        if (handle == nullptr) {
          // Don't insert the entry but still return ok, as if the entry
          // inserted
          // into cache and get evicted immediately.
          last_reference_list.push_back(e);
        } else {
          base_free(e);
          *handle = nullptr;
          s = Status::Incomplete("Insert failed due to LRU cache being full.");
        }
      } else {
        // insert into the cache
        // note that the cache might get larger than its capacity if not enough
        // space was freed
        LRUHandle* old = table_.Insert(e);
        usage_ += e->charge;
        if (old != nullptr) {
          old->SetInCache(false);
          if (Unref(old)) {
            usage_ -= old->charge;
            // old is on LRU because it's in cache and its reference count
            // was just 1 (Unref returned 0)
            LRU_Remove(old);
            last_reference_list.push_back(old);
          //  record_block_step(old, LRUHandle::EqualErase);
          }
        }
        if (handle == nullptr) {
          LRU_Insert(e, is_old);
        } else {
          *handle = reinterpret_cast<Cache::Handle*>(e);
        }
        s = Status::OK();
      }
     // record_block_step(e, LRUHandle::Insert);
    }
  }


  // we free the entries here outside of mutex for
  // performance reasons
  for (auto entry : last_reference_list) {
    if (entry != e) {
     // record_block_step(entry, LRUHandle::Evict);
      //print_block_step(entry);
    }
    entry->Free();
  }
  return s;
}

void LRUCacheShard::Erase(const Slice& key, uint32_t hash) {
  LRUHandle* e;
  bool last_reference = false;
  {
    MutexLock l(&mutex_);
//    WMS_MUTEX_GUARD(&mutex_);
    e = table_.Remove(key, hash);
    if (e != nullptr) {
      last_reference = Unref(e);
      if (last_reference) {
        usage_ -= e->charge;
      }
      if (last_reference && e->InCache()) {
        LRU_Remove(e);
      }
      e->SetInCache(false);
     // record_block_step(e, LRUHandle::Erase);
    }
  }
  // mutex not held here
  // last_reference will only be true if e != nullptr
  if (last_reference) {
    // print_block_step(e);
    e->Free();
  }
}

size_t LRUCacheShard::get_lru_usage() const {
  return lru_usage_;
}

size_t LRUCacheShard::get_high_pool_usage() const {
  return high_pri_pool_usage_;
}

size_t LRUCacheShard::get_lru_old_usage() const {
  return lru_old_usage_;
}

size_t LRUCacheShard::GetUsage() const {
  return usage_;
}

size_t LRUCacheShard::GetPinnedUsage() const {
  assert(usage_ >= lru_usage_);
  return usage_ - lru_usage_;
}

std::string LRUCacheShard::GetPrintableOptions() const {
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  {
    snprintf(buffer, kBufferSize, "    high_pri_pool_ratio: %.3lf\n",
             high_pri_pool_ratio_);
  }
  return std::string(buffer);
}

LRUCache::LRUCache(size_t capacity, int num_shard_bits,
                   bool strict_capacity_limit, double high_pri_pool_ratio,
                   double old_pool_ratio, bool row_cache_flag)
    : ShardedCache(capacity, num_shard_bits, strict_capacity_limit) {
  uint64_t num_shards = 1LL << num_shard_bits;
  shards_ = static_cast<LRUCacheShard *>(
      base_malloc(sizeof(LRUCacheShard) * num_shards, ModId::kLruCache));
  for (uint64_t i = 0; i < num_shards; ++i) {
    new (shards_ + i) LRUCacheShard();
  }
  SetCapacity(capacity);
  SetStrictCapacityLimit(strict_capacity_limit);
  for (uint64_t i = 0; i < num_shards; i++) {
    shards_[i].set_lru_cache(this);
    shards_[i].SetHighPriorityPoolRatio(high_pri_pool_ratio);
    shards_[i].set_old_pool_ratio(old_pool_ratio);
  }
  shards_num_ = num_shards;
}

LRUCache::LRUCache(size_t mod_id, size_t capacity, int num_shard_bits,
                   bool strict_capacity_limit, double high_pri_pool_ratio,
                   double old_pool_ratio, bool row_cache_flag)
    : ShardedCache(mod_id, capacity, num_shard_bits,
        strict_capacity_limit, row_cache_flag) {
  uint64_t num_shards = 1LL << num_shard_bits;
  shards_ = static_cast<LRUCacheShard *>(
      base_malloc(sizeof(LRUCacheShard) * num_shards, ModId::kLruCache));
  for (uint64_t i = 0; i < num_shards; ++i) {
    new (shards_ + i) LRUCacheShard();
  }

  SetCapacity(capacity);
  SetStrictCapacityLimit(strict_capacity_limit);
  for (uint64_t i = 0; i < num_shards; i++) {
    shards_[i].set_lru_cache(this);
    shards_[i].SetHighPriorityPoolRatio(high_pri_pool_ratio);
    shards_[i].set_old_pool_ratio(old_pool_ratio);
  }
  shards_num_ = num_shards;
}

//CacheStats LRUCache::get_subtable_cache_stats(int64_t subtable_id) {
//  CacheStats one_subtable;
//  for (uint64_t i = 0; i < shards_num_; i++) {
//    one_subtable.add(shards_[i].get_subtable_stats(subtable_id));
//  }
//  return one_subtable;
//}

LRUCache::~LRUCache() {
  if (nullptr != shards_) {
    for (uint64_t i = 0; i < shards_num_; ++i) {
      shards_[i].~LRUCacheShard();
    }
    base_free(shards_, false);
    shards_ = nullptr;
  }
}

CacheShard* LRUCache::GetShard(int shard) {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

const CacheShard* LRUCache::GetShard(int shard) const {
  return reinterpret_cast<CacheShard*>(&shards_[shard]);
}

void* LRUCache::Value(Cache::Handle* handle) {
  return reinterpret_cast<const LRUHandle*>(handle)->value;
}

size_t LRUCache::GetCharge(Cache::Handle* handle) const {
  return reinterpret_cast<const LRUHandle*>(handle)->charge;
}

uint32_t LRUCache::GetHash(Cache::Handle* handle) const {
  return reinterpret_cast<const LRUHandle*>(handle)->hash;
}

void LRUCache::DisownData() { shards_ = nullptr; }

void LRUCache::print_cache_info() const {
//  uint64_t rd = rand() % shards_num_;
//  shards_[rd].print_shard_info();
}

std::shared_ptr<Cache> NewLRUCache(size_t capacity, int num_shard_bits,
                                   bool strict_capacity_limit,
                                   double high_pri_pool_ratio,
                                   double old_pool_ratio,
                                   size_t mod_id, bool is_row_cache) {
  if (num_shard_bits >= 20) {
    return nullptr;  // the cache cannot be sharded into too many fine pieces
  }
  if (high_pri_pool_ratio < 0.0 || high_pri_pool_ratio > 1.0) {
    // invalid high_pri_pool_ratio
    return nullptr;
  }
  if (old_pool_ratio < 0.0 || old_pool_ratio > 1.0) {
    // invalid old_pool_ratio
    return nullptr;
  }
  if (num_shard_bits < 0) {
    num_shard_bits = GetDefaultCacheShardBits(capacity);
  }
  return std::make_shared<LRUCache>(mod_id, capacity, num_shard_bits,
                                    strict_capacity_limit, high_pri_pool_ratio,
                                    old_pool_ratio,
                                    is_row_cache);
}

#if 0
RowCacheRWMutex::RowCacheRWMutex() {
  write_flag_.store(false, std::memory_order_release);
  //16 is enough for 2 sockets Intel(R) Xeon(R) Platinum 8163 CPU @ 2.50GHz
  //A better implementation is using thread local lock for every reader
  // in that case active mutex shouldbe less then 16
  rwmutex_num_ = std::thread::hardware_concurrency() / 4  + 1;
  rwmutex_num_ = std::min(uint32_t(16), rwmutex_num_);
  mutex_array_ = new pthread_rwlock_t[rwmutex_num_];
  for (uint32_t i = 0; i < rwmutex_num_; i++) {
    pthread_rwlock_init(&(mutex_array_[i]), nullptr);
  }
}

RowCacheRWMutex::~RowCacheRWMutex() {
  for (uint32_t i = 0; i < rwmutex_num_; i++) {
    pthread_rwlock_destroy(&(mutex_array_[i]));
  }
  delete []mutex_array_;
}

ThreadLocalPtr tls_rc_lock_id_;
void RowCacheRWMutex::read_lock() {  
  uint64_t cpu_id = xengine::port::PhysicalCoreID();
  tls_rc_lock_id_.Reset(reinterpret_cast<uint32_t*>(cpu_id));
  uint32_t index = cpu_id % rwmutex_num_;
  pthread_rwlock_rdlock(&(mutex_array_[index]));
}

void RowCacheRWMutex::read_unlock() {
  uint64_t tls_id = reinterpret_cast<int64_t>((uint32_t*)tls_rc_lock_id_.Get());
  uint32_t index = tls_id % rwmutex_num_;
  pthread_rwlock_unlock(&(mutex_array_[index]));
}

bool RowCacheRWMutex::read_trylock() {
  bool lock_result = false;
  //test if there are writting ongoing
  if (write_flag_.load(std::memory_order_acquire))
    return lock_result;
  this->read_lock();
  lock_result = true; 
  return lock_result;
}

void RowCacheRWMutex::write_lock() {
  write_flag_.store(true, std::memory_order_release);
  //we should try to aquire all rmmutex_num_ locks here
  //but consider the following situations
  //(1)we have signal all reader to exit by write_flag_
  //(2)there is only one writer here which assured by db_mutex
  for (uint32_t i = 0; i < rwmutex_num_; i++) {
    pthread_rwlock_wrlock(&mutex_array_[i]);
  }
}

void RowCacheRWMutex::write_unlock() {
  //try to unlock
  for (uint32_t i = 0; i < rwmutex_num_; i++) {
    pthread_rwlock_unlock(&(mutex_array_[i]));
  }
  write_flag_.store(false, std::memory_order_release);
}
#endif

BiasedRWMutex::BiasedRWMutex(uint32_t concurrent_reader_number) 
  :allocator_(256, memory::ModId::kRowCache)  {
  if (concurrent_reader_number == 0) { 
    uint32_t cc_readers_number =  std::thread::hardware_concurrency();
    cc_readers_number = MAX_READER_NUMBER > cc_readers_number 
                        ? cc_readers_number : MAX_READER_NUMBER;
    cc_readers_number = MIN_READER_NUMBER < cc_readers_number 
                        ? cc_readers_number : MIN_READER_NUMBER;
    concurrent_reader_number_ = cc_readers_number;
  } else {
    concurrent_reader_number_ = concurrent_reader_number;
  } 
  //reader go fast path initiatly
  read_biase_.store(true, std::memory_order_release);
  pthread_rwlock_init(&underlying_rw_lock_, nullptr);
  void* buf = allocator_.alloc(sizeof(ReadLockPtr) * concurrent_reader_number_);
  visible_reader_ = (ReadLockPtr*)buf;
  for (uint32_t i = 0; i < concurrent_reader_number_; i++) {
    //cacheline padding can inprove performance by 8X (2M -> 16M  OPS)
    buf = allocator_.alloc(sizeof(std::atomic<pthread_rwlock_t*>*) + CACHE_LINE_SIZE);
    visible_reader_[i] = new (buf)std::atomic<pthread_rwlock_t*>(nullptr);
    visible_reader_[i]->store(nullptr, std::memory_order_release);
  } 
}

BiasedRWMutex::~BiasedRWMutex() {
  read_biase_.store(false, std::memory_order_release);
  pthread_rwlock_destroy(&underlying_rw_lock_);
  destroy();
}

void BiasedRWMutex::destroy() {
  for (uint32_t i = 0; i < concurrent_reader_number_; i++) {
    //delete visible_reader_[i];
    if (nullptr != visible_reader_[i])
      allocator_.free(visible_reader_[i]);
    visible_reader_[i] = nullptr;
  } 
  allocator_.clear();
}

uint64_t BiasedRWMutex::get_reader_index() {
  static std::atomic<uint64_t> global_index_counter(0);
  static thread_local int64_t mutex_index = -1;
  if (mutex_index == -1) {
    mutex_index = global_index_counter.fetch_add(1, std::memory_order_acquire);
  }
  return mutex_index;
}

BiasedRWMutex::LockHandle* BiasedRWMutex::read_lock() {
  std::atomic<pthread_rwlock_t*>* slot = nullptr;
  bool fast_path_succeed = false;
  //fast path
  if (LIKELY(read_biase_.load(std::memory_order_acquire))) {
    uint32_t index  = get_reader_index()  % concurrent_reader_number_;
    slot =  visible_reader_[index];
    assert(nullptr != slot);
    pthread_rwlock_t* expect = nullptr;
    if (slot->compare_exchange_weak(expect, &underlying_rw_lock_)) {
      //acquire this slot succeed, no recheck the read_biase_ flag
      if (read_biase_.load(std::memory_order_acquire)) { 
        assert(slot->load(std::memory_order_acquire) == &underlying_rw_lock_);
        fast_path_succeed = true;
      } else { 
        //recheck failed, there should be writer here ,reset slot
        slot->store(nullptr, std::memory_order_release);
        slot = nullptr;
      }    
    } else {
      slot = nullptr;
    }
  } 
  //slow path
  if (UNLIKELY(!fast_path_succeed)) {
    assert(nullptr == slot);
    pthread_rwlock_rdlock(&underlying_rw_lock_);
    //we acquire read lock succeed which means there is no writer now
    //set read_biase_ to true ,the future reader can go with fast path
    if (!read_biase_.load(std::memory_order_acquire)) {
      read_biase_.store(true, std::memory_order_release);
    }
  }
  return reinterpret_cast<BiasedRWMutex::LockHandle*>(slot);
}

void BiasedRWMutex::read_unlock(BiasedRWMutex::LockHandle* handle) {
  if (LIKELY(nullptr != handle)) {
    //fast path unlock
    std::atomic<pthread_rwlock_t*>* lock_ptr = 
      (reinterpret_cast<std::atomic<pthread_rwlock_t*>*>(handle));
    assert(lock_ptr->load(std::memory_order_acquire) == &underlying_rw_lock_);
    lock_ptr->store(nullptr, std::memory_order_release);
  } else {
    //slow path unlock
    pthread_rwlock_unlock(&underlying_rw_lock_);
  }
}

void BiasedRWMutex::write_lock() {
  pthread_rwlock_wrlock(&underlying_rw_lock_);
  if (LIKELY(read_biase_.load(std::memory_order_acquire))) {
    //signal new reader to run slow path 
    read_biase_.store(false, std::memory_order_release);
    //wait all reader exit 
    for (uint32_t i = 0; i < concurrent_reader_number_; i++) {
      while (nullptr != visible_reader_[i]->load(std::memory_order_acquire)) {
        port::AsmVolatilePause();  
      }    
    }
  }
  //now we have acquire the write lock and all reader have been exit
  //we can do writting job
}

void BiasedRWMutex::write_unlock() { 
  pthread_rwlock_unlock(&underlying_rw_lock_);
}

//RowCache::RowCache(std::shared_ptr<Cache> cache)
//  : lru_cache_(cache),
//    allocator_(CharArena::DEFAULT_PAGE_SIZE, memory::ModId::kRowCache) {
//  for (uint32_t i = 0 ; i < MAX_MUTEX_SHARD_NUMBER; i++) {
//    void* buf = allocator_.alloc(sizeof(BiasedRWMutex));
//    rwmutex_array_[i] = new (buf)BiasedRWMutex();
//  }
//}
//
//RowCache::~RowCache() {
//  destroy();
//}
//
////we can call destory() as many times as we can,it will not cause problem
//void RowCache::destroy() {
//  //TODO: should we drop the SeqRange for a subtable if it was dropped ?
//  for (std::pair<uint32_t, SeqRange*> item : cfd_seq_ranges_) {
//    SeqRange* range = item.second;
//    allocator_.free(range);
//  }
//  for (uint32_t i = 0 ; i < MAX_MUTEX_SHARD_NUMBER; i++) {
//    if (nullptr != rwmutex_array_[i]) {
//      rwmutex_array_[i]->destroy();
//      allocator_.free(rwmutex_array_[i]);
//    }
//    rwmutex_array_[i] = nullptr;
//  }
//  cfd_seq_ranges_.clear();
//  allocator_.clear();
//}
//
//const RowCache::SeqRange RowCache::get_seq_range(const uint32_t cfd_id) {
//  SeqRange seq_range;
//  BiasedRWMutex::LockHandle* lc_handle = range_rw_mutex_.read_lock();
//  //if (!range_rw_mutex_.read_trylock()) {
//  //  return seq_range;
//  //}
//  std::unordered_map<uint32_t, SeqRange*>::const_iterator iter =
//      cfd_seq_ranges_.find(cfd_id);
//  if (iter != cfd_seq_ranges_.end()
//      && nullptr != iter->second) {
//    seq_range = *iter->second;
//  }
//  range_rw_mutex_.read_unlock(lc_handle);
//  //range_rw_mutex_.read_unlock();
//  return seq_range;
//}
//
//int RowCache::set_sequence(const uint64_t start, const uint64_t end,
//                           const uint32_t cfd_id) {
//  int ret = 0;
//  range_rw_mutex_.write_lock();
//  std::unordered_map<uint32_t, SeqRange*>::iterator iter =
//      cfd_seq_ranges_.find(cfd_id);
//  if (iter != cfd_seq_ranges_.end()) {
//    if (nullptr != iter->second) {
//      iter->second->start_seq_ = std::max(start, iter->second->start_seq_);
//      iter->second->end_seq_ = std::max(end, iter->second->end_seq_);
//      if (0 != iter->second->end_seq_) {
//        assert(iter->second->start_seq_ <= iter->second->end_seq_);
//      }
//    } else {
//      ret = Status::kErrorUnexpected;
//      XENGINE_LOG(WARN, "range is null",
//          K(ret), KP(iter->second), K(start), K(end), K(cfd_id));
//    }
//  } else {
//    void* buf = allocator_.alloc(sizeof(SeqRange));
//    if (nullptr != buf) {
//      SeqRange* seq_range = new (buf) SeqRange(start, end);
//      cfd_seq_ranges_.insert(std::make_pair(cfd_id, seq_range));
//    } else {
//      ret = Status::kMemoryLimit;
//      XENGINE_LOG(WARN, "alloc memory failed", K(ret), K(sizeof(SeqRange)));
//    }
//  }
//  range_rw_mutex_.write_unlock();
//  return ret;
//}
//
//Status RowCache::insert(const common::Slice& key, void* value, size_t charge,
//                        void (*deleter)(const common::Slice& key, void* value),
//                        Cache::Handle** handle, Cache::Priority priority,
//                        const bool old, const uint64_t seq,
//                                const uint32_t cfd_id) {
//  Status ret = Status::OK();
//  uint32_t hash = util::Hash(key.data(), key.size(), 0);
//  get_mutex_shard(hash)->write_lock();
//  //BiasedRWMutexGuad(get_mutex_shard(hash), false);
//  const SeqRange seq_range = this->get_seq_range(cfd_id);
//  if (UNLIKELY((0 != seq_range.end_seq_/*not init*/ && seq <= seq_range.end_seq_))) {
//    // if current seq maybe in flush process,not add it into cache
//    // because it maybe become one old version data, do nothing
//    get_mutex_shard(hash)->write_unlock();
//    return ret;
//  }
//  //do inset row into row cache
//  ret = this->lru_cache_->Insert(key, value, charge, deleter, handle,
//                                 priority, old, seq, cfd_id);
//  get_mutex_shard(hash)->write_unlock();
//  return ret;
//}
//
//Cache::Handle* RowCache::lookup(const common::Slice& key,
//                                monitor::Statistics* stats,
//                                const uint64_t seq, const uint32_t cfd_id) {
//  Cache::Handle* ret = nullptr;
//  uint32_t hash = util::Hash(key.data(), key.size(), 0);
//  BiasedRWMutex::LockHandle *lc_handle = get_mutex_shard(hash)->read_lock();
//  const RowCache::SeqRange seq_range = this->get_seq_range(cfd_id);
//  if (seq < seq_range.start_seq_) {
//    get_mutex_shard(hash)->read_unlock(lc_handle);
//    return ret;
//  }
//  ret = this->lru_cache_->Lookup(key, stats, seq, cfd_id);
//  get_mutex_shard(hash)->read_unlock(lc_handle);
//  return ret;
//}
//
std::shared_ptr<RowCache> NewRowCache(size_t capacity, int num_shard_bits,
                                     bool strict_capacity_limit,
                                     double high_pri_pool_ratio,
                                     double old_pool_ratio,
                                     size_t mod_id, bool is_row_cache) {
  if (num_shard_bits >= 20) {
    return nullptr;  // the cache cannot be sharded into too many fine pieces
  }
  if (old_pool_ratio < 0.0 || old_pool_ratio > 1.0) {
    // invalid old_pool_ratio
    return nullptr;
  }
  if (num_shard_bits < 0) {
    size_t min_shard_size = 8 * 1024L * 1024LL;  // Every shard is at least 8M.
    size_t num_shards = capacity / min_shard_size;
    while (num_shards >>= 1) {
      if (++num_shard_bits >= ShardedCache::MAX_SHARD_BITS) {
        break;
      }
    }
  }
  return std::make_shared<RowCache>(capacity, num_shard_bits, strict_capacity_limit);
}


#ifdef TBB
XCache::XCache() : Cache() {
 table_ = new XTable();
 last_id_ = 0;
}

XCache::~XCache() {
  if (nullptr != table_)
    delete table_;
}

Status XCache::Insert(
    const common::Slice& key, void* value, size_t charge,
    void (*deleter)(const common::Slice& key, void* value),
    Cache::Handle** handle , Priority priority,
    const bool old , const uint64_t seq,
    const uint32_t cfd_id) {
  Status s;
  while (true) {
    auto insert_result = table_->insert(std::pair<std::string, Cache::Handle*>
      (std::string(key.data_, key.size_), (Cache::Handle*)(value))); 
    void *target = nullptr;
    if (!insert_result.second) {
     //key alread exists, delete and insert again
     //  Erase(key);
     //  fprintf(stderr, "XCache encounter exists key '%s' length=%ld, delete it\n", 
     //            key.ToString(true).c_str(), key.ToString(true).size());
     //  Env::Default()->SleepForMicroseconds(100000);
     //  continue;
     target = insert_result.first->second;
    } else {
     target = value;
    }
    if (handle != nullptr)
      *handle = reinterpret_cast<Cache::Handle*>(target);
    s = Status::OK();
    break;
  }
  return s;
}

Cache::Handle* XCache::Lookup(const common::Slice& key,
  monitor::Statistics* stats,
  const uint64_t snapshot,
  const uint32_t cfd_id) {
  Cache::Handle* ret = nullptr;
  XTable::iterator ite = table_->find(std::string(key.data_, key.size_));
  if (ite != table_->end()) {
    //found target
    ret = ite->second;  
  } else {
    ret = nullptr;
  }
  return ret;
}

bool XCache::check_in_cache(const common::Slice& key) {
  bool ret = false;
  XTable::iterator ite = table_->find(std::string(key.data_, key.size_));
  if (ite != table_->end()) {
    //found target
    ret = true;
  }
  return ret;
}

void* XCache::Value(Cache::Handle* handle) {
  return handle;
}

void XCache::Release(Cache::Handle* handle) {
  //do nothing now
}

uint64_t XCache::NewId() {
  return last_id_.fetch_add(1, std::memory_order_relaxed);
}

void XCache::Erase(const Slice& key) {
  MutexLock lock(&mutex_);
  table_->unsafe_erase(key.ToString());
}
std::shared_ptr<Cache> NewXCache() {
  return std::make_shared<XCache>();
}
#endif
//const CacheStats CacheStats::ZERO_STATS = CacheStats(0, 0, 0, 0, 0);
}  // namespace cache
}  // namespace xengine
