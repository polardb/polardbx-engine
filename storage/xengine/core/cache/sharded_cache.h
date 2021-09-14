//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <atomic>
#include <string>
#include <unordered_map>

#include "memory/page_arena.h"
#include "port/port.h"
#include "util/hash.h"
#include "memory/page_arena.h"
#include "memory/object_pool.h"
#include "xengine/cache.h"

namespace xengine {
namespace cache {

// Single cache shard interface.
class CacheShard {
 public:
  CacheShard() = default;
  virtual ~CacheShard() = default;

  virtual common::Status Insert(
      const common::Slice& key, uint32_t hash, void* value, size_t charge,
      void (*deleter)(const common::Slice& key, void* value),
      Cache::Handle** handle, Cache::Priority priority, bool old = true,
      const uint64_t seq = 0, const uint32_t cfd_id = 0) = 0;
  virtual Cache::Handle* Lookup(const common::Slice& key, uint32_t hash,
                                const uint64_t seq = 0, const uint32_t cfd_id = 0) = 0;
  virtual bool in_cache(const common::Slice& key, uint32_t hash) = 0;
  virtual bool Ref(Cache::Handle* handle) = 0;
  virtual void Release(Cache::Handle* handle) = 0;
  virtual void Erase(const common::Slice& key, uint32_t hash) = 0;
  virtual void SetCapacity(size_t capacity) = 0;
  virtual void SetStrictCapacityLimit(bool strict_capacity_limit) = 0;
  virtual size_t GetUsage() const = 0;
  virtual size_t GetPinnedUsage() const = 0;
  virtual void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                      bool thread_safe) = 0;
  virtual void EraseUnRefEntries() = 0;
  virtual std::string GetPrintableOptions() const { return ""; }
};

// Generic cache interface which shards cache by hash of keys. 2^num_shard_bits
// shards will be created, with capacity split evenly to each of the shards.
// Keys are sharded by the highest num_shard_bits bits of hash value.
class ShardedCache : public Cache {
 public:
  struct SeqRange {
    SeqRange() : start_seq_(0), end_seq_(0) {}
    SeqRange(uint64_t start_seq, uint64_t end_seq)
        : start_seq_(start_seq), end_seq_(end_seq) {}
    uint64_t start_seq_;
    uint64_t end_seq_;
  };
  ShardedCache(size_t capacity, int num_shard_bits, bool strict_capacity_limit,
               bool is_row_cache = false);
  ShardedCache(size_t mod_id, size_t capacity, int num_shard_bits,
               bool strict_capacity_limit, bool is_row_cache = false);
  virtual ~ShardedCache() = default;
  virtual void destroy();
  virtual const char* Name() const override = 0;
  virtual CacheShard* GetShard(int shard) = 0;
  virtual const CacheShard* GetShard(int shard) const = 0;
  virtual void* Value(Handle* handle) override = 0;
  virtual size_t GetCharge(Handle* handle) const = 0;
  virtual uint32_t GetHash(Handle* handle) const = 0;
  virtual void DisownData() override = 0;

  virtual void SetCapacity(size_t capacity) override;
  virtual void SetStrictCapacityLimit(bool strict_capacity_limit) override;
  virtual int set_sequence(const uint64_t start, const uint64_t end,
                           const uint32_t cfd_id) override;
  virtual common::Status Insert(
      const common::Slice& key, void* value, size_t charge,
      void (*deleter)(const common::Slice& key, void* value), Handle** handle,
      Priority priority, const bool old, const uint64_t seq,
      const uint32_t cfd_id) override;
  virtual Handle* Lookup(const common::Slice& key, monitor::Statistics* stats,
                         const uint64_t seq, const uint32_t cfd_id) override;
  virtual bool Ref(Handle* handle) override;
  virtual bool check_in_cache(const common::Slice& key) override;
  virtual void Release(Handle* handle) override;
  virtual void Erase(const common::Slice& key) override;
  virtual uint64_t NewId() override;
  virtual size_t GetCapacity() const override;
  virtual bool HasStrictCapacityLimit() const override;
  virtual size_t GetUsage() const override;
  virtual size_t GetUsage(Handle* handle) const override;
  virtual size_t GetPinnedUsage() const override;
  virtual void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                      bool thread_safe) override;
  virtual void EraseUnRefEntries() override;
  virtual std::string GetPrintableOptions() const override;

  int GetNumShardBits() const { return num_shard_bits_; }
  int get_shard_id(const common::Slice &key);

  const SeqRange get_seq_range(const uint32_t cfd_id);

  uint64_t get_start_seq(const uint32_t cfd_id);
  uint64_t get_end_seq(const uint32_t cfd_id);
  bool is_row_cache() const { return is_row_cache_; }
 public:
  static const int MAX_SHARD_BITS = 10;
 private:
  static inline uint32_t HashSlice(const common::Slice& s) {
    return util::Hash(s.data(), s.size(), 0);
  }

  uint32_t Shard(uint32_t hash) {
    // Note, hash >> 32 yields hash in gcc, not the zero we expect!
    return (num_shard_bits_ > 0) ? (hash >> (32 - num_shard_bits_)) : 0;
  }

  int num_shard_bits_;
  mutable port::Mutex capacity_mutex_;
  size_t capacity_;
  bool strict_capacity_limit_;
  std::atomic<uint64_t> last_id_;
  std::unordered_map<uint32_t, SeqRange*> cfd_seq_ranges_;
  bool is_row_cache_;
  memory::ArenaAllocator allocator_;
  port::RWMutex range_rwlock_;
};

extern int GetDefaultCacheShardBits(size_t capacity);

}  // namespace cache
}  // namespace xengine
