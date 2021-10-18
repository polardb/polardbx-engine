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

#include "cache/sharded_cache.h"

#include <string>
#include "logger/logger.h"
#include "memory/base_malloc.h"
#include "memory/page_arena.h"
#include "util/mutexlock.h"

using namespace xengine;
using namespace monitor;
using namespace common;
using namespace util;

namespace xengine {
using namespace memory;
namespace cache {

ShardedCache::ShardedCache(size_t capacity, int num_shard_bits,
                           bool strict_capacity_limit, bool row_cache)
    : num_shard_bits_(num_shard_bits),
      capacity_(capacity),
      strict_capacity_limit_(strict_capacity_limit),
      last_id_(1),
      is_row_cache_(row_cache),
      allocator_(CharArena::DEFAULT_PAGE_SIZE, get_mod_id()),
      range_rwlock_() {
}

ShardedCache::ShardedCache(size_t mod_id, size_t capacity, int num_shard_bits,
                           bool strict_capacity_limit, bool row_cache)
    : Cache(mod_id),
      num_shard_bits_(num_shard_bits),
      capacity_(capacity),
      strict_capacity_limit_(strict_capacity_limit),
      last_id_(1),
      is_row_cache_(row_cache) {
}

void ShardedCache::destroy() {
  cfd_seq_ranges_.clear();
  allocator_.clear();
}

void ShardedCache::SetCapacity(size_t capacity) {
  int num_shards = 1 << num_shard_bits_;
  const size_t per_shard = (capacity + (num_shards - 1)) / num_shards;
  MutexLock l(&capacity_mutex_);
  for (int s = 0; s < num_shards; s++) {
    GetShard(s)->SetCapacity(per_shard);
  }
  capacity_ = capacity;
}

void ShardedCache::SetStrictCapacityLimit(bool strict_capacity_limit) {
  int num_shards = 1 << num_shard_bits_;
  MutexLock l(&capacity_mutex_);
  for (int s = 0; s < num_shards; s++) {
    GetShard(s)->SetStrictCapacityLimit(strict_capacity_limit);
  }
  strict_capacity_limit_ = strict_capacity_limit;
}

int ShardedCache::set_sequence(const uint64_t start, const uint64_t end,
                               const uint32_t cfd_id) {
  int ret = 0;
  range_rwlock_.WriteLock();
  std::unordered_map<uint32_t, SeqRange*>::iterator iter =
      cfd_seq_ranges_.find(cfd_id);
  if (iter != cfd_seq_ranges_.end()) {
    if (nullptr != iter->second) {
      iter->second->start_seq_ = std::max(start, iter->second->start_seq_);
      iter->second->end_seq_ = std::max(end, iter->second->end_seq_);
      if (0 != iter->second->end_seq_) {
        assert(iter->second->start_seq_ <= iter->second->end_seq_);
      }
    } else {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "range is null",
          K(ret), KP(iter->second), K(start), K(end), K(cfd_id));
    }
  } else {
    void* buf = allocator_.alloc(sizeof(SeqRange));
    if (nullptr != buf) {
      SeqRange* seq_range = new (buf) SeqRange(start, end);
      cfd_seq_ranges_.insert(std::make_pair(cfd_id, seq_range));
    } else {
      ret = Status::kMemoryLimit;
      XENGINE_LOG(WARN, "alloc memory failed", K(ret), K(sizeof(SeqRange)));
    }
  }
  range_rwlock_.WriteUnlock();
  return ret;
}

Status ShardedCache::Insert(const Slice& key, void* value, size_t charge,
                            void (*deleter)(const Slice& key, void* value),
                            Handle** handle, Priority priority, bool old,
                            const uint64_t seq, const uint32_t cfd_id) {
  Status ret;
  uint32_t hash = HashSlice(key);
  ret = GetShard(Shard(hash))->Insert(
      key, hash, value, charge, deleter, handle, priority, old, seq, cfd_id);
  return ret;
}

bool ShardedCache::check_in_cache(const Slice& key) {
  int ret = 0;
  uint32_t hash = HashSlice(key);
  return GetShard(Shard(hash))->in_cache(key, hash);
}

int ShardedCache::get_shard_id(const common::Slice &key) {
  uint32_t hash = HashSlice(key);
  return Shard(hash);
}

const ShardedCache::SeqRange ShardedCache::get_seq_range(
    const uint32_t cfd_id) {
  SeqRange seq_range;
  range_rwlock_.ReadLock();
  std::unordered_map<uint32_t, SeqRange*>::const_iterator iter =
      cfd_seq_ranges_.find(cfd_id);
  if (iter != cfd_seq_ranges_.end()
      && nullptr != iter->second) {
    seq_range = *iter->second;
  }
  range_rwlock_.ReadUnlock();
  return seq_range;
}

uint64_t ShardedCache::get_start_seq(const uint32_t cfd_id)
{
  // for debug
  const SeqRange seq_range = get_seq_range(cfd_id);
  return seq_range.start_seq_;
}

uint64_t ShardedCache::get_end_seq(const uint32_t cfd_id)
{
  // for debug
  const SeqRange seq_range = get_seq_range(cfd_id);
  return seq_range.end_seq_;
}

Cache::Handle* ShardedCache::Lookup(const Slice& key, Statistics* stats,
                                    const uint64_t seq, const uint32_t cfd_id) {
  uint32_t hash = HashSlice(key);
  return GetShard(Shard(hash))->Lookup(key, hash, seq, cfd_id);
}

bool ShardedCache::Ref(Handle* handle) {
  uint32_t hash = GetHash(handle);
  return GetShard(Shard(hash))->Ref(handle);
}

void ShardedCache::Release(Handle* handle) {
  uint32_t hash = GetHash(handle);
  GetShard(Shard(hash))->Release(handle);
}

void ShardedCache::Erase(const Slice& key) {
  uint32_t hash = HashSlice(key);
  GetShard(Shard(hash))->Erase(key, hash);
}

uint64_t ShardedCache::NewId() {
  return last_id_.fetch_add(1, std::memory_order_relaxed);
}

size_t ShardedCache::GetCapacity() const {
  MutexLock l(&capacity_mutex_);
  return capacity_;
}

bool ShardedCache::HasStrictCapacityLimit() const {
  MutexLock l(&capacity_mutex_);
  return strict_capacity_limit_;
}

size_t ShardedCache::GetUsage() const {
  // We will not lock the cache when getting the usage from shards.
  int num_shards = 1 << num_shard_bits_;
  size_t usage = 0;
  for (int s = 0; s < num_shards; s++) {
    usage += GetShard(s)->GetUsage();
  }
  return usage;
}

size_t ShardedCache::GetUsage(Handle* handle) const {
  return GetCharge(handle);
}

size_t ShardedCache::GetPinnedUsage() const {
  // We will not lock the cache when getting the usage from shards.
  int num_shards = 1 << num_shard_bits_;
  size_t usage = 0;
  for (int s = 0; s < num_shards; s++) {
    usage += GetShard(s)->GetPinnedUsage();
  }
  return usage;
}

void ShardedCache::ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                          bool thread_safe) {
  int num_shards = 1 << num_shard_bits_;
  for (int s = 0; s < num_shards; s++) {
    GetShard(s)->ApplyToAllCacheEntries(callback, thread_safe);
  }
}

void ShardedCache::EraseUnRefEntries() {
  int num_shards = 1 << num_shard_bits_;
  for (int s = 0; s < num_shards; s++) {
    GetShard(s)->EraseUnRefEntries();
  }
}

std::string ShardedCache::GetPrintableOptions() const {
  std::string ret;
  ret.reserve(20000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];
  {
    MutexLock l(&capacity_mutex_);
    // snprintf(buffer, kBufferSize, "    capacity : %" XENGINE_PRIszt "\n",
    //        capacity_);
    ret.append(buffer);
    snprintf(buffer, kBufferSize, "    num_shard_bits : %d\n", num_shard_bits_);
    ret.append(buffer);
    snprintf(buffer, kBufferSize, "    strict_capacity_limit : %d\n",
             strict_capacity_limit_);
    ret.append(buffer);
  }
  ret.append(GetShard(0)->GetPrintableOptions());
  return ret;
}
int GetDefaultCacheShardBits(size_t capacity) {
  int num_shard_bits = 0;
  size_t min_shard_size = 1024L * 1024L;  // Every shard is at least 512KB.
  size_t num_shards = capacity / min_shard_size;
  while (num_shards >>= 1) {
    if (++num_shard_bits >= ShardedCache::MAX_SHARD_BITS) {
      return num_shard_bits;
    }
  }
  return num_shard_bits;
}

}  // namespace cache
}  // namespace xengine
