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
#include "memory/chunk_allocator.h"
//#include "memory/mem_pool.h"

#include "cache/sharded_cache.h"
#include "db/column_family.h"
#include "util/mutexlock.h"
#include "util/hashtable.h"
#include "util/heap.h"

namespace xengine
{
//namespace db
//{
//class ColumnFamilyData;
//}
namespace cache
{

struct RowcHandle {
  RowcHandle(const uint32_t charge,
             const uint16_t key_length,
             const uint64_t key_seq,
             memory::ChunkAllocator *alloc = nullptr)
      : charge_(charge),
        refs_(0),
        seq_(key_seq),
        alloc_(alloc),
        key_length_(key_length) {}
  void ref() {
    assert(alloc_);
    refs_.fetch_add(1);
  }
  uint32_t charge_;
  std::atomic<uint32_t> refs_;
  uint64_t seq_;
  memory::ChunkAllocator *alloc_;
  uint32_t key_length_;
  char key_data_[1];
};

template <class HandleType>
struct HandleKey
{
  const common::Slice operator() (const HandleType *handle) const {
    assert(handle);
    return common::Slice(handle->key_data_, handle->key_length_);
  };
};

struct HashSlice
{
  uint32_t operator() (const common::Slice& s) const {
    return util::Hash(s.data(), s.size(), 0);
  };
};

struct EqualSlice
{
  bool operator() (const common::Slice& a, const common::Slice& b) const {
    return a == b;
  };
};

struct RowCacheCheck {
  RowCacheCheck():
    seq_(0),
    cfd_(nullptr) {}
  uint64_t seq_;
  const db::ColumnFamilyData *cfd_;
};

struct CheckFunc {
  bool operator() (const RowCacheCheck* check) const {
    uint64_t end_seq = check->cfd_->get_range_end();
    if (0 != end_seq/*not init*/
        && check->seq_ <= end_seq) {
      // if current seq maybe in flush process,not add it into cache
      // because it maybe become one old version data
      return false;
    }
    return true;
  };
};

class RowCacheShard {
  static const size_t MAX_ROW_SIZE = 4 * 1024;
  static const size_t AVG_ROW_LENGTH = 512;
public:
  RowCacheShard();
  ~RowCacheShard();
  struct SeqRange {
    SeqRange() : start_seq_(0), end_seq_(0) {}
    SeqRange(uint64_t start_seq, uint64_t end_seq)
        : start_seq_(start_seq), end_seq_(end_seq) {}
    uint64_t start_seq_;
    uint64_t end_seq_;
  };

  // The type of the Cache
  const char* Name() const {
    return "";
  }

  Cache::Handle* lookup_row(const common::Slice& key,
                            const uint64_t snapshot = 0,
                            const db::ColumnFamilyData *cfd = nullptr);

  bool check_in_cache(const common::Slice& key);

  bool Ref(Cache::Handle* handle);

  void Release(Cache::Handle* handle);

  void* Value(Cache::Handle* handle);

  void Erase(const common::Slice& key);

  void SetCapacity(const int64_t capacity, const int64_t shard_id);

  void SetStrictCapacityLimit(bool strict_capacity_limit);


  bool HasStrictCapacityLimit() const;

  // returns the maximum configured capacity of the cache
  int64_t GetCapacity() const;

  // returns the memory size for the entries residing in the cache.
  int64_t GetUsage() const;
  int64_t get_usage() const {
    return alloc_.get_usage();
  }
  int64_t get_allocated_size() const {
    return alloc_.get_allocated_size();
  }
  // returns the memory size for a specific entry in the cache.
  size_t GetUsage(Cache::Handle* handle) const;

  void DisownData() {
    alloc_.disown_data();
  };

  int insert_row(const common::Slice& key,
                 const void* value,
                 const size_t charge,
                 const uint64_t key_seq,
                 const db::ColumnFamilyData *cfd,
                 const uint64_t seq = 0,
                 Cache::Handle** handle = nullptr);
  bool unref(RowcHandle *e);

  void async_evict_chunks() {
    alloc_.async_evict_chunks();
  }
  void print_stats(std::string &stat_str) const;
private:
  int64_t capacity_;
  int64_t shards_num_;
  int64_t topn_value_;
  bool strict_capacity_limit_;
  int64_t usage_;
  port::Mutex mutex_;
  // for find row in cache
  util::HashTable<common::Slice, RowcHandle *, HashSlice,
  HandleKey<RowcHandle>, CheckFunc, RowCacheCheck *, EqualSlice> hash_table_;
  memory::ChunkManager alloc_; // for object alloc
};

class RowCache {
public:
  RowCache(const int64_t capacity,
           const int num_shard_bits,
           const bool strict_capacity_limit);
  ~RowCache();
  void destroy() {

  }
  void DisownData() {

  }
  void SetCapacity(size_t capacity);
  void SetStrictCapacityLimit(bool strict_capacity_limit);
  int insert_row(const common::Slice& key,
                 const void* value,
                 const size_t charge,
                 const uint64_t key_seq,
                 const db::ColumnFamilyData *cfd,
                 const uint64_t seq = 0,
                 Cache::Handle** handle = nullptr);
  Cache::Handle* lookup_row(const common::Slice& key,
                            const uint64_t snapshot = 0,
                            const db::ColumnFamilyData *cfd = nullptr);
  void Erase(const common::Slice& key);
  void Release(Cache::Handle* handle);
  bool check_in_cache(const common::Slice& key);
  int GetNumShardBits() const { return num_shard_bits_; }
  RowCacheShard* GetShard(int shard) {
    return &shards_[shard];
  }
  const RowCacheShard* GetShard(int shard) const {
    return &shards_[shard];
  }
  int64_t GetCapacity() const {
//    MutexLock l(&capacity_mutex_);
    return capacity_;
  }
  uint64_t NewId() {
//    return last_id_.fetch_add(1, std::memory_order_relaxed);
    return 0;
  }
  int64_t GetUsage() const;
  int64_t get_usage() const;
  int64_t get_allocated_size() const;
  int64_t get_items_num() const;
  void print_stats(std::string &stat_str) const;
  void async_evict_chunks();
  static void async_cache_purge(void *cache);
  void schedule_cache_purge(util::Env *env);
private:
  static inline uint32_t HashSlice(const common::Slice& s) {
    return util::Hash(s.data(), s.size(), 0);
  }
  uint32_t Shard(uint32_t hash) {
    // Note, hash >> 32 yields hash in gcc, not the zero we expect!
    return (num_shard_bits_ > 0) ? (hash >> (32 - num_shard_bits_)) : 0;
  }

  int64_t num_shard_bits_;
  int64_t shards_num_;
  RowCacheShard* shards_;
  int64_t capacity_;
  bool strict_capacity_limit_;
};
}
}
