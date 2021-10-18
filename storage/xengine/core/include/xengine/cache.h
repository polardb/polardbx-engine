/*
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A Cache is an interface that maps keys to values.  It has internal
// synchronization and may be safely accessed concurrently from
// multiple threads.  It may automatically evict entries to make room
// for new entries.  Values have a specified charge against the cache
// capacity.  For example, a cache where the values are variable
// length strings, may use the length of the string as the charge for
// the string.
//
// A builtin cache implementation with a least-recently-used eviction
// policy is provided.  Clients may use their own implementations if
// they want something more sophisticated (like scan-resistance, a
// custom eviction policy, variable cache sizing, etc.)

#pragma once

#include <stdint.h>
#include <memory>
#include <string>
#include <unordered_map>
#include "memory/mod_info.h"
#include "memory/page_arena.h"
#include "xengine/slice.h"
#include "xengine/statistics.h"
#include "xengine/status.h"
#include <atomic>

namespace xengine {
namespace cache {

class Cache;
class RowCache;

// Create a new cache with a fixed size capacity. The cache is sharded
// to 2^num_shard_bits shards, by hash of the key. The total capacity
// is divided and evenly assigned to each shard. If strict_capacity_limit
// is set, insert to the cache will fail when cache is full. User can also
// set percentage of the cache reserves for high priority entries via
// high_pri_pool_pct.
// num_shard_bits = -1 means it is automatically determined: every shard
// will be at least 512KB and number of shard bits will not exceed 6.
extern std::shared_ptr<Cache> NewLRUCache(
    size_t capacity, int num_shard_bits = -1,
    bool strict_capacity_limit = false, double high_pri_pool_ratio = 0.0,
    double old_pool_ratio = 0.0,
    size_t mod_id = 0, bool is_row_cache = false);

extern std::shared_ptr<RowCache> NewRowCache(
    size_t capacity, int num_shard_bits = -1,
    bool strict_capacity_limit = false, double high_pri_pool_ratio = 0.0,
    double old_pool_ratio = 0.0,
    size_t mod_id = 0, bool is_row_cache = false);

// Similar to NewLRUCache, but create a cache based on CLOCK algorithm with
// better concurrent performance in some cases. See util/clock_cache.cc for
// more detail.
//
// Return nullptr if it is not supported.
extern std::shared_ptr<Cache> NewClockCache(size_t capacity,
                                            int num_shard_bits = -1,
                                            bool strict_capacity_limit = false);

extern std::shared_ptr<Cache> NewXCache();


class Cache {
 public:
  // Depending on implementation, cache entries with high priority could be less
  // likely to get evicted than low priority entries.
  enum class Priority { HIGH, LOW };

  Cache(size_t mod_id = memory::ModId::kCache) : mod_id_(mod_id) {}

  // Destroys all existing entries by calling the "deleter"
  // function that was passed via the Insert() function.
  //
  // @See Insert
  virtual ~Cache() {}
  virtual void destroy() {}
  // Opaque handle to an entry stored in the cache.
  struct Handle {};

  // The type of the Cache
  virtual const char* Name() const = 0;

  // Insert a mapping from key->value into the cache and assign it
  // the specified charge against the total cache capacity.
  // If strict_capacity_limit is true and cache reaches its full capacity,
  // return Status::Incomplete.
  //
  // If handle is not nullptr, returns a handle that corresponds to the
  // mapping. The caller must call this->Release(handle) when the returned
  // mapping is no longer needed. In case of error caller is responsible to
  // cleanup the value (i.e. calling "deleter").
  //
  // If handle is nullptr, it is as if Release is called immediately after
  // insert. In case of error value will be cleanup.
  //
  // When the inserted entry is no longer needed, the key and
  // value will be passed to "deleter".
  // ptr is used to update mod_id
  virtual common::Status Insert(
      const common::Slice& key, void* value, size_t charge,
      void (*deleter)(const common::Slice& key, void* value),
      Handle** handle = nullptr, Priority priority = Priority::LOW,
      const bool old = true, const uint64_t seq = 0,
      const uint32_t cfd_id = 0) = 0;

  // If the cache has no mapping for "key", returns nullptr.
  //
  // Else return a handle that corresponds to the mapping.  The caller
  // must call this->Release(handle) when the returned mapping is no
  // longer needed.
  // If stats is not nullptr, relative tickers could be used inside the
  // function.
  virtual Handle* Lookup(const common::Slice& key,
                         monitor::Statistics* stats = nullptr,
                         const uint64_t snapshot = 0,
                         const uint32_t cfd_id = 0) = 0;

  virtual bool check_in_cache(const common::Slice& key) = 0;
  // Increments the reference count for the handle if it refers to an entry in
  // the cache. Returns true if refcount was incremented; otherwise, returns
  // false.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual bool Ref(Handle* handle) = 0;

  // Release a mapping returned by a previous Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void Release(Handle* handle) = 0;

  // Return the value encapsulated in a handle returned by a
  // successful Lookup().
  // REQUIRES: handle must not have been released yet.
  // REQUIRES: handle must have been returned by a method on *this.
  virtual void* Value(Handle* handle) = 0;

  // If the cache contains entry for key, erase it.  Note that the
  // underlying entry will be kept around until all existing handles
  // to it have been released.
  virtual void Erase(const common::Slice& key) = 0;
  // Return a new numeric id.  May be used by multiple clients who are
  // sharding the same cache to partition the key space.  Typically the
  // client will allocate a new id at startup and prepend the id to
  // its cache keys.
  virtual uint64_t NewId() = 0;

  // sets the maximum configured capacity of the cache. When the new
  // capacity is less than the old capacity and the existing usage is
  // greater than new capacity, the implementation will do its best job to
  // purge the released entries from the cache in order to lower the usage
  virtual void SetCapacity(size_t capacity) = 0;

  // Set whether to return error on insertion when cache reaches its full
  // capacity.
  virtual void SetStrictCapacityLimit(bool strict_capacity_limit) = 0;

  // set sequence for row cache
  virtual int set_sequence(const uint64_t /*start*/, const uint64_t /*end*/,
                            const uint32_t /*cfd_id*/) {
    // UNUSED(start);
    // UNUSED(end);
    // UNUSED(cfd_id);
    return 0;
  }
  // Get the flag whether to return error on insertion when cache reaches its
  // full capacity.
  virtual bool HasStrictCapacityLimit() const = 0;

  // returns the maximum configured capacity of the cache
  virtual size_t GetCapacity() const = 0;

  // returns the memory size for the entries residing in the cache.
  virtual size_t GetUsage() const = 0;

  // returns the memory size for a specific entry in the cache.
  virtual size_t GetUsage(Handle* handle) const = 0;

  // returns the memory size for the entries in use by the system
  virtual size_t GetPinnedUsage() const = 0;

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
                                      bool thread_safe) = 0;

  // Remove all entries.
  // Prerequisit: no entry is referenced.
  virtual void EraseUnRefEntries() = 0;

  virtual std::string GetPrintableOptions() const { return ""; }
  void set_mod_id(const size_t mod_id) { mod_id_ = mod_id; }
  size_t get_mod_id() const { return mod_id_; }

 private:
  size_t mod_id_;
  // No copying allowed
  Cache(const Cache&);
  Cache& operator=(const Cache&);
};

#if 0
class RowCacheRWMutex {
public:
  RowCacheRWMutex();
  ~RowCacheRWMutex();

  void read_lock();
  bool read_trylock();
  void write_lock();
  void read_unlock();
  void write_unlock();

private:
  uint32_t            rwmutex_num_;
  pthread_rwlock_t*   mutex_array_;
  std::atomic<bool>   write_flag_;

private:
  RowCacheRWMutex(const RowCacheRWMutex&);
  void operator=(const RowCacheRWMutex&);
};
#endif

//A Biased readder/writer lock implementation
//Idea was from Dave Dice and Alex Kogan's paper in ATC'2019
//<<BRAVO—Biased Locking for Reader-Writer Locks>>
class BiasedRWMutex {
public:
  struct LockHandle { };

  BiasedRWMutex(uint32_t concurrent_reader_number = 0);
  ~BiasedRWMutex();
  LockHandle* read_lock();
  void read_unlock(LockHandle* handle);
  void write_lock();
  void write_unlock();

  void destroy();
private:
  static uint64_t get_reader_index();

private:
  static uint32_t const                MAX_READER_NUMBER = 16 * 1024;//2^14
  static uint32_t const                MIN_READER_NUMBER = 4;
  static uint32_t const                CACHELINE_SIZE = 64;
  uint32_t                             concurrent_reader_number_;
  std::atomic<bool>                    read_biase_;
  static char                          read_biase_pad_[CACHELINE_SIZE];
  pthread_rwlock_t                     underlying_rw_lock_;
  static char                          rw_lock_pad_[CACHELINE_SIZE];
  //we use only concurrent_reader_number_ slots
  typedef std::atomic<pthread_rwlock_t*>* ReadLockPtr;
  ReadLockPtr*                         visible_reader_; 
  memory::ArenaAllocator                   allocator_;
};

class BiasedRWMutexGuad {
public:
  BiasedRWMutexGuad(BiasedRWMutex* mutex, bool read) : mutex_(mutex), 
                                                       read_(read) {
    assert(mutex_ != nullptr);
    if (read_) {
      handle_ = mutex->read_lock();
    } else {
      mutex_->write_lock();
    }
  }
  ~BiasedRWMutexGuad() {
    if (read_) {
      mutex_->read_unlock(handle_);
    } else {
      mutex_->write_unlock();
    }
  }
private:
  BiasedRWMutex*                mutex_;
  bool                          read_;
  BiasedRWMutex::LockHandle*    handle_;
};

//class RowCache {
//public:
//  struct SeqRange {
//    SeqRange() : start_seq_(0), end_seq_(0) {}
//    SeqRange(uint64_t start_seq, uint64_t end_seq)
//        : start_seq_(start_seq), end_seq_(end_seq) {}
//    uint64_t start_seq_;
//    uint64_t end_seq_;
//  };
//  RowCache(std::shared_ptr<Cache> lry_cache);
//  ~RowCache();
//  void destroy();
//
//  common::Status insert(const common::Slice& key, void* value, size_t charge,
//                        void (*deleter)(const common::Slice& key, void* value),
//                        Cache::Handle** handle = nullptr, 
//                        Cache::Priority priority = Cache::Priority::LOW,
//                        const bool old = true, const uint64_t seq = 0,
//                        const uint32_t cfd_id = 0);
//
//  Cache::Handle* lookup(const common::Slice& key, monitor::Statistics* stats = nullptr,
//                        const uint64_t snapshot = 0, const uint32_t cfd_id = 0); 
//
//  Cache* get_cache() { return this->lru_cache_.get(); }
//
//  int set_sequence(const uint64_t start, const uint64_t end, const uint32_t cfd_id);
//
//private:
//  const SeqRange get_seq_range(const uint32_t cfd_id);
//  inline BiasedRWMutex* get_mutex_shard(uint32_t hash) {
//    return rwmutex_array_[hash % MAX_MUTEX_SHARD_NUMBER];
//  }
//
//private:
//  BiasedRWMutex                            range_rw_mutex_; 
//  std::shared_ptr<Cache>                   lru_cache_; 
//  std::unordered_map<uint32_t, SeqRange*>  cfd_seq_ranges_;
//  memory::ArenaAllocator                       allocator_;
//  //since BiaseRWMutex provide google performance , we need less shards
//  static uint32_t const                    MAX_MUTEX_SHARD_NUMBER = 512; //2^9
//  BiasedRWMutex*                           rwmutex_array_[MAX_MUTEX_SHARD_NUMBER];
//  // No copying allowed
//  RowCache(const RowCache&);
//  RowCache& operator=(const RowCache&);
//};
}  // namespace cache
}  // namespace xengine
