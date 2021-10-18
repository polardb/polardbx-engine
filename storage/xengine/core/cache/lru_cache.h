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

#include <string>
#include <unordered_map>

#include "cache/sharded_cache.h"

#include "port/port.h"
#include "util/autovector.h"
#include "memory/stl_adapt_allocator.h"
#include "monitoring/instrumented_mutex.h"
#include <atomic>

#ifdef TBB
#include "tbb/concurrent_unordered_map.h"
#endif

namespace xengine {
namespace cache {
// LRU cache implementation

// An entry is a variable length heap-allocated structure.
// Entries are referenced by cache and/or by any external entity.
// The cache keeps all its entries in table. Some elements
// are also stored on LRU list.
//
// LRUHandle can be in these states:
// 1. Referenced externally AND in hash table.
//  In that case the entry is *not* in the LRU. (refs > 1 && in_cache == true)
// 2. Not referenced externally and in hash table. In that case the entry is
// in the LRU and can be freed. (refs == 1 && in_cache == true)
// 3. Referenced externally and not in hash table. In that case the entry is
// in not on LRU and not in table. (refs >= 1 && in_cache == false)
//
// All newly created LRUHandles are in state 1. If you call
// LRUCacheShard::Release
// on entry in state 1, it will go into state 2. To move from state 1 to
// state 3, either call LRUCacheShard::Erase or LRUCacheShard::Insert with the
// same key.
// To move from state 2 to state 1, use LRUCacheShard::Lookup.
// Before destruction, make sure that no handles are in state 1. This means
// that any successful LRUCacheShard::Lookup/LRUCacheShard::Insert have a
// matching
// RUCache::Release (to move into state 2) or LRUCacheShard::Erase (for state 3)

struct LRUHandle {
  enum OperateType {
    Insert,
    Read,
    Release,
    Erase,
   // ManualEvict,
    Evict,
    EqualErase,
    BeOld,
    BeLow,
    BeYoung
  };
  enum BlockStatus {
    InOldPool,
    InHighPool,
    InLowPool,
    NotInLRU
  };
  struct BlockStep {
    BlockStep()
        : type_(Read),
          status_(NotInLRU),
          is_old_data_(false),
          is_high_pri_(false),
          access_time_(0) {

    }
    OperateType type_;
    BlockStatus status_;
    bool is_old_data_;
    bool is_high_pri_;
    uint64_t access_time_;
  };

  LRUHandle():
    value(nullptr),
    next_hash(nullptr),
    next(nullptr),
    prev(nullptr),
    charge(0),
    key_length(0),
    refs(0),
    hash(0) {
    flags &= 0;
    old_flags &= 1;
  }
  void* value;
  void (*deleter)(const common::Slice&, void* value);
  LRUHandle* next_hash;
  LRUHandle* next;
  LRUHandle* prev;
  size_t charge;  // TODO(opt): Only allow uint32_t?
  size_t key_length;
  uint32_t refs;  // a number of refs to this entry
                  // cache itself is counted as 1

  // Include the following flags:
  //   in_cache:    whether this entry is referenced by the hash table.
  //   is_high_pri: whether this entry is high priority entry.
  //   in_high_pro_pool: whether this entry is in high-pri pool.
  char flags;

  // Include the following flags:
  //   is_old: whether this entry is old entry.
  //   in_old_pool: whether this entry is in old pool.
  char old_flags;

  uint32_t hash;  // Hash of key(); used for fast sharding and comparisons
 // std::vector<BlockStep> steps_; // for statistic
  char key_data[1];  // Beginning of key

  common::Slice key() const {
    // For cheaper lookups, we allow a temporary Handle object
    // to store a pointer to a key in "value".
    if (next == this) {
      return *(reinterpret_cast<common::Slice*>(value));
    } else {
      return common::Slice(key_data, key_length);
    }
  }

  bool InCache() { return flags & 1; }
  bool IsHighPri() { return flags & 2; }
  bool InHighPriPool() { return flags & 4; }

  bool is_old() { return old_flags & 1;}
  bool is_in_old_pool() { return old_flags & 2; }

  void set_old(bool is_old_node) {
    if (is_old_node) {
      old_flags |= 1;
    } else {
      old_flags &= ~1;
    }
  }

  void set_in_old_pool(bool in_old_pool) {
    if (in_old_pool) {
      old_flags |= 2;
    } else {
      old_flags &= ~2;
    }
  }

  void SetInCache(bool in_cache) {
    if (in_cache) {
      flags |= 1;
    } else {
      flags &= ~1;
    }
  }

  void SetPriority(Cache::Priority priority) {
    if (priority == Cache::Priority::HIGH) {
      flags |= 2;
    } else {
      flags &= ~2;
    }
  }

  void SetInHighPriPool(bool in_high_pri_pool) {
    if (in_high_pri_pool) {
      flags |= 4;
    } else {
      flags &= ~4;
    }
  }

  void Free();
};

struct RowCacheHandle {
  RowCacheHandle() : schema_version_(0), data_size_(0) {}
  uint64_t schema_version_;
  size_t data_size_;
  char data_[0];
};

// We provide our own simple hash table since it removes a whole bunch
// of porting hacks and is also faster than some of the built-in hash
// table implementations in some of the compiler/runtime combinations
// we have tested.  E.g., readrandom speeds up by ~5% over the g++
// 4.4.3's builtin hashtable.
class LRUHandleTable {
 public:
  LRUHandleTable();
  ~LRUHandleTable();

  LRUHandle* Lookup(const common::Slice& key, uint32_t hash);
  LRUHandle* Insert(LRUHandle* h);
  LRUHandle* Remove(const common::Slice& key, uint32_t hash);

  template <typename T>
  void ApplyToAllCacheEntries(T func) {
    for (uint32_t i = 0; i < length_; i++) {
      LRUHandle* h = list_[i];
      while (h != nullptr) {
        auto n = h->next_hash;
        assert(h->InCache());
        func(h);
        h = n;
      }
    }
  }

 private:
  // Return a pointer to slot that points to a cache entry that
  // matches key/hash.  If there is no such cache entry, return a
  // pointer to the trailing slot in the corresponding linked list.
  LRUHandle** FindPointer(const common::Slice& key, uint32_t hash);

  void Resize();

  // The table consists of an array of buckets where each bucket is
  // a linked list of cache entries that hash into the bucket.
  uint32_t length_;
  uint32_t elems_;
  LRUHandle** list_;
};

// A single shard of sharded cache.
class LRUCacheShard : public CacheShard {
 public:
  static const uint64_t LONG_TIME_LIMIT = 10000000; // 10s
  const std::vector<std::string> OperateTypeStr {
    {"Insert"},
    {"Read"},
    {"Release"},
    {"Erase"},
    {"Evict"},
    {"EqualErase"},
    {"BeOld"},
    {"BeLow"},
    {"BeYoung"}
  };
  const std::vector<std::string> BlockStatStr {
    {"InOldPool"},
    {"InHighPool"},
    {"InLowPool"},
    {"NotInLRU"}
  };

  LRUCacheShard();
  virtual ~LRUCacheShard();

  // Separate from constructor so caller can easily make an array of LRUCache
  // if current usage is more than new capacity, the function will attempt to
  // free the needed space
  virtual void SetCapacity(size_t capacity) override;

  // Set the flag to reject insertion if cache if full.
  virtual void SetStrictCapacityLimit(bool strict_capacity_limit) override;

  // Set percentage of capacity reserved for high-pri cache entries.
  void SetHighPriorityPoolRatio(double high_pri_pool_ratio);

  // set ratio of old pool in total capacity
  void set_old_pool_ratio(double old_pool_ratio);

  // used for test
  void  set_old_list_min_capacity(const size_t capacity) {
    adjust_min_capacity_ = capacity;
  }

  // used for test
  void set_lru_old_adjust_capacity(const size_t capacity) {
    lru_old_adjust_capacity_ = capacity;
  }

  // set lru cache
  void set_lru_cache(ShardedCache* lru_cache) { lru_cache_ = lru_cache; }

  // print shard use info
  void print_shard_info();
  void print_block_step(LRUHandle *e) const;
  //void record_block_step(LRUHandle *e, LRUHandle::OperateType type) const;
  // Like Cache methods, but with an extra "hash" parameter.
  virtual common::Status Insert(
      const common::Slice& key, uint32_t hash, void* value, size_t charge,
      void (*deleter)(const common::Slice& key, void* value),
      Cache::Handle** handle, Cache::Priority priority, bool old = true,
      const uint64_t seq = 0, const uint32_t cfd_id = 0) override;
  virtual Cache::Handle* Lookup(const common::Slice& key, uint32_t hash,
                                const uint64_t seq = 0,
                                const uint32_t cfd_id = 0) override;
  virtual bool in_cache(const common::Slice& key, uint32_t hash) override;
  virtual bool Ref(Cache::Handle* handle) override;
  virtual void Release(Cache::Handle* handle) override;
  virtual void Erase(const common::Slice& key, uint32_t hash) override;

  // Although in some platforms the update of size_t is atomic, to make sure
  // GetUsage() and GetPinnedUsage() work correctly under any platform, we'll
  // protect them with mutex_.

  virtual size_t GetUsage() const override;
  virtual size_t GetPinnedUsage() const override;

  virtual void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                      bool thread_safe) override;

  virtual void EraseUnRefEntries() override;

  virtual std::string GetPrintableOptions() const override;


  void TEST_GetLRUList(LRUHandle** lru, LRUHandle** lru_low_pri, LRUHandle** lru_old_start);
  size_t get_lru_usage() const;
  size_t get_high_pool_usage() const;
  size_t get_lru_old_usage() const;
  // todo for stats
//  CacheStats &get_subtable_stats(int64_t subtable_id);

 private:
//  using SubtableCacheStats = std::unordered_map<int64_t, CacheStats, std::hash<int64_t>,
//                     std::equal_to<int64_t>,
//                     is::stl_adapt_allocator<std::pair<int64_t, CacheStats>,
//                                         memory::ModId::kInformationSchema>>;
  // init lru old list
  void init_old_list();
  // add data to lru list head
  void add_lru_list_head(LRUHandle* e);
  // add data to lru old list head
  void add_old_list(LRUHandle* e);
  // adjust lru old list
  void adjust_old_list();
  void LRU_Remove(LRUHandle* e);
  void LRU_Insert(LRUHandle* e, bool old = true);

  // Overflow the last entry in high-pri pool to low-pri pool until size of
  // high-pri pool is no larger than the size specify by high_pri_pool_pct.
  void MaintainPoolSize();

  // Just reduce the reference count by 1.
  // Return true if last reference
  bool Unref(LRUHandle* e);

  // Free some space following strict LRU policy until enough space
  // to hold (usage_ + charge) is freed or the lru list is empty
  // This function is not thread safe - it needs to be executed while
  // holding the mutex_
  void EvictFromLRU(size_t charge, util::autovector<LRUHandle*>* deleted);

//  CacheStats &subtable_id_to_stats(int64_t subtable_id);

  // Initialized before use.
  size_t capacity_;

  // Memory size for entries residing in the cache
  size_t usage_;

  // Memory size for entries residing only in the LRU list
  size_t lru_usage_;

  // Memory size for entries residing only in the LRU old list
  size_t lru_old_usage_;

  // Memory size for entries in high-pri pool.
  size_t high_pri_pool_usage_;

  // Build old list when lru usage reach to the capacity
  size_t adjust_min_capacity_;

  // Adjust lru old capacity, make it stable in a range
  size_t lru_old_adjust_capacity_;

  // Whether to reject insertion if cache reaches its full capacity.
  bool strict_capacity_limit_;

  // Ratio of capacity reserved for high priority cache entries.
  double high_pri_pool_ratio_;

  // Ratio of capacity reserved for old cache entries.
  double old_pool_ratio_;
  // High-pri pool size, equals to capacity * high_pri_pool_ratio.
  // Remember the value to avoid recomputing each time.
  double high_pri_pool_capacity_;

  // Lru-old pool size, equals to capacity * lru_old__pool_ratio.
  double old_pool_capacity_;

  // mutex_ protects the following state.
  // We don't count mutex_ as the cache's internal state so semantically we
  // don't mind mutex_ invoking the non-const actions.
//  mutable monitor::InstrumentedMutex mutex_;
  mutable port::Mutex mutex_;

  // Dummy head of LRU list.
  // lru.prev is newest entry, lru.next is oldest entry.
  // LRU contains items which can be evicted, ie reference only by cache
  LRUHandle lru_;

  // Pointer to head of low-pri pool in LRU list.
  LRUHandle* lru_low_pri_;

  // Pointer to head of lru-old pool in LRU list.
  LRUHandle* lru_old_start_;

  LRUHandleTable table_;

  // IS stats of subtables
//  SubtableCacheStats subtable_stats_;
  ShardedCache* lru_cache_;
  bool print_flag_;
};

class LRUCache : public ShardedCache {
 public:
  LRUCache(size_t capacity, int num_shard_bits, bool strict_capacity_limit,
           double high_pri_pool_ratio, double old_pool_ratio,
           bool is_row_cache = false);
  LRUCache(size_t mod_id, size_t capacity, int num_shard_bits,
           bool strict_capacity_limit, double high_pri_pool_ratio,
           double old_pool_ratio, bool is_row_cache = false);
  virtual ~LRUCache();
  virtual const char* Name() const override { return "LRUCache"; }
  virtual CacheShard* GetShard(int shard) override;
  virtual const CacheShard* GetShard(int shard) const override;
  virtual void* Value(Handle* handle) override;
  virtual size_t GetCharge(Handle* handle) const override;
  virtual uint32_t GetHash(Handle* handle) const override;
  virtual void DisownData() override;
  //virtual CacheStats get_subtable_cache_stats(int64_t subtable_id) override;
  void print_cache_info() const;
//  memory::ArenaAllocator &get_allocator() {
//    return allocator_;
//  }
 private:
  LRUCacheShard* shards_;
  uint64_t shards_num_;
};

#ifdef TBB
//TODO:beilou XCache Just for Performance Test only, 
//It's not thread safe container
class XCache : public Cache {
typedef tbb::concurrent_unordered_map<std::string, Handle*> XTable;
public:
  XCache();
  ~XCache();
  // The type of the Cache
  virtual const char* Name() const override {
    return "XCache";
  }

  common::Status Insert(
    const common::Slice& key, void* value, size_t charge,
    void (*deleter)(const common::Slice& key, void* value),
    Handle** handle = nullptr, Priority priority = Priority::LOW,
    const bool old = true, const uint64_t seq = 0,
    const uint32_t cfd_id = 0);

  Handle* Lookup(const common::Slice& key,
    monitor::Statistics* stats = nullptr,
    const uint64_t snapshot = 0,
    const uint32_t cfd_id = 0) override;

  bool check_in_cache(const common::Slice& key) override;
  // Increments the reference count for the handle if it refers to an entry in
  // the cache. Returns true if refcount was incremented; otherwise, returns
  // false.
  // REQUIRES: handle must have been returned by a method on *this.
  bool Ref(Handle* handle) override {
    bool ret = true;
    return ret;
  }

  // Release a mapping returned by a previous Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  void Release(Handle* handle) override;

  // Return the value encapsulated in a handle returned by a
  // successful Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  void* Value(Handle* handle) override;

  // If the cache contains entry for key, erase it.  Note that the
  // underlying entry will be kept around until all existing handles
  // to it have been released.
  void Erase(const common::Slice& key) override;
  // Return a new numeric id.  May be used by multiple clients who are
  // sharding the same cache to partition the key space.  Typically the
  // client will allocate a new id at startup and prepend the id to
  // its cache keys.
  uint64_t NewId() override;

  // sets the maximum configured capacity of the cache. When the new
  // capacity is less than the old capacity and the existing usage is
  // greater than new capacity, the implementation will do its best job to
  // purge the released entries from the cache in order to lower the usage
  void SetCapacity(size_t capacity) override {
  }

  // Set whether to return error on insertion when cache reaches its full
  // capacity.
  virtual void SetStrictCapacityLimit(bool strict_capacity_limit) override {

  }

  // Get the flag whether to return error on insertion when cache reaches its
  // full capacity.
  bool HasStrictCapacityLimit() const override {
    bool ret = false;
    return ret;
  }

  // returns the maximum configured capacity of the cache
  size_t GetCapacity() const override {
    size_t ret = 8 * 1024 * 1024;
    return ret;
  }

  // returns the memory size for the entries residing in the cache.
  size_t GetUsage() const override {
    size_t ret = 0;
    return ret;
  }

  // returns the memory size for a specific entry in the cache.
  size_t GetUsage(Handle* handle) const override {
    size_t ret = 0;
    return ret;
  }

  // returns the memory size for the entries in use by the system
  virtual size_t GetPinnedUsage() const override {
    size_t ret = 0;
    return ret;
  }

  // Call this on shutdown if you want to speed it up. Cache will disown
  // any underlying data and will not free it on delete. This call will leak
  // memory - call this only if you're shutting down the process.
  // Any attempts of using cache after this call will fail terribly.
  // Always delete the DB object before calling this method!
  virtual void DisownData(){
      // default implementation is noop
  };

  // Apply callback to all entries in the cache
  // If thread_safe is true, it will also lock the accesses. Otherwise, it will
  // access the cache without the lock held
  virtual void ApplyToAllCacheEntries(void (*callback)(void*, size_t),
                                      bool thread_safe) override {
  }

  // Remove all entries.
  // Prerequisit: no entry is referenced.
  virtual void EraseUnRefEntries() override {
  }
private:
  XTable* table_;
  mutable port::Mutex mutex_;
  std::atomic<uint64_t> last_id_;
};
#endif
}  // namespace cache
}  // namespace xengine
