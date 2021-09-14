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
#include "row_cache.h"
#include "logger/logger.h"
#include "xengine/env.h"
namespace xengine
{
using namespace common;
using namespace memory;
namespace cache
{

static void delete_cache_entry(void *value, void* handler) {
  RowcHandle* cache_value = reinterpret_cast<RowcHandle*>(value);
  RowCacheShard* row_cache = reinterpret_cast<RowCacheShard*>(handler);
  if (nullptr != cache_value && nullptr != row_cache) {
    Slice key = Slice(cache_value->key_data_, cache_value->key_length_);
    row_cache->Erase(key);
  }
}

RowCacheShard::RowCacheShard()
    : capacity_(0),
      shards_num_(0),
      topn_value_(0),
      strict_capacity_limit_(false),
      usage_(0),
      alloc_()

{
}

RowCacheShard::~RowCacheShard()
{
}

Cache::Handle *RowCacheShard::lookup_row(const common::Slice& key,
                                         const uint64_t snapshot,
                                         const db::ColumnFamilyData *cfd) {
  RowcHandle *handle = nullptr;
  int ret = 0;
  if (FAILED(hash_table_.find(key, handle, true))) {
    XENGINE_LOG(WARN, "failed to find row handle", K(ret));
  } else if (nullptr != handle) {
    handle->alloc_->hit_add();
  }
  return reinterpret_cast<Cache::Handle *>(handle);
}

int RowCacheShard::insert_row(const common::Slice& key,
                              const void* value,
                              const size_t value_size,
                              const uint64_t key_seq,
                              const db::ColumnFamilyData *cfd,
                              const uint64_t seq,
                              Cache::Handle** handle) {
  int ret = 0;
  if (value_size > MAX_ROW_SIZE || key.size() > MAX_ROW_SIZE) {
    return ret;
  }
  ChunkAllocator *allocator = nullptr;
  int64_t handle_size = sizeof(RowcHandle) - 1 + key.size();
  void *buf = alloc_.alloc(handle_size + value_size, allocator);
  if (nullptr == buf) {
    alloc_.evict_one_chunk();
    buf = alloc_.alloc(handle_size + value_size, allocator);
    if (nullptr == buf) {
      // do nothing
      return ret;
    }
  }
  memcpy((char *)buf + handle_size, value, value_size);
  RowcHandle  *rhandle = new (buf) RowcHandle(value_size + handle_size, key.size(), key_seq, allocator);
  memcpy(rhandle->key_data_, key.data(), key.size());
  rhandle->refs_ = 1;
  RowCacheCheck check;
  check.cfd_ = cfd;
  check.seq_ = seq;
  RowcHandle *old_handle = nullptr;
  ret = hash_table_.insert(key, rhandle, &check, old_handle);
  if (FAILED(ret)) {
    allocator->free(buf, rhandle->charge_);
    if (nullptr != handle) {
      *handle = nullptr;
    }
    allocator->set_abort();
  } else {
    usage_ += rhandle->charge_;
    if (nullptr != handle) {
      *handle = (Cache::Handle*)rhandle;
    }
    allocator->set_commit();
  }
  if (nullptr != old_handle) {
    if (unref(old_handle)) {
      usage_ -= old_handle->charge_;
      old_handle->alloc_->free(old_handle, old_handle->charge_);
    }
  }
  if (Status::kInsertCheckFailed == ret) {
    ret = Status::kOk;
  }
  return ret;
}

bool RowCacheShard::check_in_cache(const common::Slice& key) {
  RowcHandle *handle = nullptr;
  int ret = 0;
  if (FAILED(hash_table_.find(key, handle))) {
    XENGINE_LOG(WARN, "failed to find key in hashtable", K(ret), K(key));
  }
  return handle != nullptr;
}

bool RowCacheShard::Ref(Cache::Handle *handle)
{
  RowcHandle* row_handle = reinterpret_cast<RowcHandle*>(handle);
  row_handle->refs_.fetch_add(1);
  return true;
}

bool RowCacheShard::unref(RowcHandle *e) {
  assert(e);
  return 1 == e->refs_.fetch_sub(1);
}

void RowCacheShard::Release(Cache::Handle *handle) {
  if (nullptr == handle) {
    return;
  }
  RowcHandle* row_handle = reinterpret_cast<RowcHandle*>(handle);
  bool last_reference = unref(row_handle);
  if (last_reference) {
    usage_ -= row_handle->charge_;
    row_handle->alloc_->free(row_handle, row_handle->charge_);
  }
}

void* RowCacheShard::Value(Cache::Handle* handle) {
  RowcHandle *rhandle = reinterpret_cast<RowcHandle*>(handle);
  return (void *)((char *)rhandle + sizeof(rhandle) - 1 + rhandle->key_length_);
}

void RowCacheShard::Erase(const Slice &key) {
  RowcHandle *e = nullptr;
  bool last_reference = false;
  int ret = 0;
  if (FAILED(hash_table_.remove(key, e))) {
    XENGINE_LOG(WARN, "failed to remove key from hashtable", K(key));
  } else if (nullptr != e) {
    last_reference = unref(e);
    if (last_reference) {
      usage_ -= e->charge_;
      e->alloc_->free(e, e->charge_);
    }
  }
}

void RowCacheShard::SetCapacity(const int64_t capacity, const int64_t shard_id) {
  if (capacity <= capacity_) {
    // not support decrease cap
  } else {
    capacity_ = capacity;
    hash_table_.set_buckets_num(capacity_ / AVG_ROW_LENGTH);
    int64_t hashtable_usage = hash_table_.get_usage();
    int64_t chunk_size = ChunkAllocator::CHUNK_SIZE + memory::AllocMgr::kBlockMeta;
    int64_t chunk_num =  (capacity - hashtable_usage + chunk_size / 2) / chunk_size;
    if (chunk_num < 2) {
      chunk_num = 2;
    }
    alloc_.init(chunk_num, &delete_cache_entry, this);
    if (1 == shard_id) {
      XENGINE_LOG(INFO, "row cache shard info", K(capacity), K(hashtable_usage), K(chunk_size), K(chunk_num));
    }
  }
}

void RowCacheShard::SetStrictCapacityLimit(bool strict_capacity_limit) {
  strict_capacity_limit_ = strict_capacity_limit;
}

bool RowCacheShard::HasStrictCapacityLimit() const
{
  return strict_capacity_limit_;
}

int64_t RowCacheShard::GetCapacity() const
{
  return capacity_;
}

int64_t RowCacheShard::GetUsage() const
{
  int64_t alloc_usage = alloc_.get_usage();
  return usage_;
}

size_t RowCacheShard::GetUsage(Cache::Handle* handle) const
{
  if (nullptr == handle) {
    return 0;
  }
  RowcHandle* row_handle = reinterpret_cast<RowcHandle*>(handle);
  return row_handle->charge_;
}

void RowCacheShard::print_stats(std::string &stat_str) const {
  alloc_.print_chunks_stat(stat_str);
}

RowCache::RowCache(const int64_t capacity,
                   const int num_shard_bits,
                   const bool strict_capacity_limit)
{
  int64_t num_shards = 1LL << num_shard_bits;
  shards_num_ = num_shards;
  shards_ = static_cast<RowCacheShard *>(
      base_malloc(sizeof(RowCacheShard) * num_shards, ModId::kLruCache));
  for (int64_t i = 0; i < num_shards; ++i) {
    new (shards_ + i) RowCacheShard();
  }
  num_shard_bits_ = num_shard_bits;
  SetCapacity(capacity);
  SetStrictCapacityLimit(strict_capacity_limit);
}

RowCache::~RowCache() {
  for (int64_t i = 0; i < shards_num_; ++i) {
    shards_[i].~RowCacheShard();
  }
  memory::base_free(shards_, false);
}

void RowCache::SetCapacity(size_t capacity) {
  int num_shards = 1 << num_shard_bits_;
  const size_t per_shard = (capacity + (num_shards - 1)) / num_shards;
  for (int s = 0; s < num_shards; s++) {
    GetShard(s)->SetCapacity(per_shard, s);
  }
  capacity_ = capacity;
}

void RowCache::SetStrictCapacityLimit(bool strict_capacity_limit) {
  int num_shards = 1 << num_shard_bits_;
  for (int s = 0; s < num_shards; s++) {
    GetShard(s)->SetStrictCapacityLimit(strict_capacity_limit);
  }
  strict_capacity_limit_ = strict_capacity_limit;
}

int RowCache::insert_row(const common::Slice& key,
                         const void* value,
                         const size_t charge,
                         const uint64_t key_seq,
                         const db::ColumnFamilyData *cfd,
                         const uint64_t seq,
                         Cache::Handle** handle) {
  int ret = 0;
  uint32_t hash = HashSlice(key);
  return GetShard(Shard(hash))->insert_row(key, value, charge, key_seq, cfd, seq, handle);
}

Cache::Handle* RowCache::lookup_row(const common::Slice& key,
                                    const uint64_t snapshot,
                                    const db::ColumnFamilyData *cfd) {
  uint32_t hash = HashSlice(key);
  return GetShard(Shard(hash))->lookup_row(key, snapshot, cfd);
}

void RowCache::Erase(const common::Slice& key) {
  uint32_t hash = HashSlice(key);
  GetShard(Shard(hash))->Erase(key);
}

void RowCache::Release(Cache::Handle* handle) {
  const RowcHandle *rh = reinterpret_cast<const RowcHandle*>(handle);
  uint32_t hash = HashSlice(common::Slice(rh->key_data_, rh->key_length_));
  GetShard(Shard(hash))->Release(handle);
}

bool RowCache::check_in_cache(const Slice& key) {
  int ret = 0;
  uint32_t hash = HashSlice(key);
  return GetShard(Shard(hash))->check_in_cache(key);
}

int64_t RowCache::GetUsage() const {
  // We will not lock the cache when getting the usage from shards.
  int num_shards = 1 << num_shard_bits_;
  int64_t usage = 0;
  for (int s = 0; s < num_shards; s++) {
    usage += GetShard(s)->GetUsage();
  }
  return usage;
}

int64_t RowCache::get_usage() const {
  // We will not lock the cache when getting the usage from shards.
  int num_shards = 1 << num_shard_bits_;
  size_t usage = 0;
  for (int s = 0; s < num_shards; s++) {
    usage += GetShard(s)->get_usage();
  }
  return usage;
}

int64_t RowCache::get_allocated_size() const {
  int num_shards = 1 << num_shard_bits_;
  int64_t allocated_size = 0;
  for (int s = 0; s < num_shards; s++) {
    allocated_size += GetShard(s)->get_allocated_size();
  }
  return allocated_size;
}

void RowCache::async_evict_chunks() {
  int num_shards = 1 << num_shard_bits_;
  for (int64_t i = 0; i < num_shards; i++) {
    GetShard(i)->async_evict_chunks();
  }
}

void RowCache::async_cache_purge(void *cache) {
  if (nullptr != cache) {
    reinterpret_cast<RowCache *>(cache)->async_evict_chunks();
  }
}

void RowCache::schedule_cache_purge(util::Env *env) {
  if (nullptr != env) {
    int num_shards = 1 << num_shard_bits_;
    env->Schedule(&RowCache::async_cache_purge, this, util::Env::LOW, this);
  }
}

void RowCache::print_stats(std::string &stat_str) const {
//  int num_shards = 1 << num_shard_bits_;
//  for (int64_t i = 0; i < num_shards; i++) {
//    GetShard(i)->print_stats(stat_str);
//  }
  XENGINE_LOG(INFO, "row cache info", K(GetUsage()), K(get_usage()), K(get_allocated_size()));
}

}
}
