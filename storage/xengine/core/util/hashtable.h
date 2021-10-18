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
#include "logger/logger.h"
#include "util/misc_utility.h"
#include "util/fast_latch.h"
namespace xengine
{
namespace util
{
template<class ValTp>
struct HashTableNode
{
  HashTableNode():
	  next_(nullptr),
	  data_(),
	  hash_(0) {}
  HashTableNode *next_;
  ValTp data_;
  uint64_t hash_;
};

template <class ValTp, class LockTp = port::RWMutex>
struct HashTableBucket
{
  LockTp mu_;
  HashTableNode<ValTp> *node_;
};

template<class KeyTp,
         class ValTp,
         class HashFunc,
         class GetKey,
         class CheckFunc,
         class CheckType,
         class KeyEqual = std::less<KeyTp>>
class HashTable
{
public:
	const static int64_t DEFAULT_LENGTH = 1024;
	const static int64_t SHARD_LOCKS_NUM = DEFAULT_LENGTH;
	typedef size_t            size_type;
	typedef HashTableNode<ValTp> hashnode;
	typedef HashTableBucket<ValTp> hashbucket;
public:
	HashTable(const int64_t bucket_num = DEFAULT_LENGTH)
        : buckets_(nullptr),
          num_elements_(0),
          bucket_num_(bucket_num),
          init_(false),
          locks_(nullptr),
          nodes_(nullptr) {
	  usage_ = DEFAULT_LENGTH * (sizeof(hashnode *) +  sizeof(FastRWLatch) + sizeof(hashnode));
	}
	~HashTable() {
	  destroy();
	}
	void set_buckets_num(const int64_t num) {
	  bucket_num_ = num;
	  usage_ = bucket_num_ * (sizeof(hashnode *) +  sizeof(FastRWLatch) + sizeof(hashnode));
	}
	void destroy() {
		clear();
	}
	void clear() {
    if (init_) {
      for (int64_t i = 0; i < bucket_num_; ++i) {
        FastWRGuard l(&locks_[i]);
        hashnode *node = buckets_[i];
        while (nullptr != node) {
          hashnode *nextnode = node->next_;
          if (node != &nodes_[i]) {
            MOD_DELETE_OBJECT(hashnode, node);
          }
          node = nextnode;
        }
      }
      num_elements_ = 0;
      memory::base_free(buckets_);
      memory::base_free(nodes_);
      memory::base_free(locks_);
    }
	}
	hashnode **find_node_ptr(const KeyTp &key, int64_t idx, uint64_t hash) {
    hashnode **node = &buckets_[idx];
    while (nullptr != *node && ((*node)->hash_ != hash || !equal_(key, get_key_((*node)->data_)))) {
      node = &(*node)->next_;
    }
    return node;
	}
	int insert(const KeyTp &key, const ValTp &val, const CheckType check, ValTp &old_val) {
    int ret = 0;
    if (!init_) {
      construct();
    }
    int len = 0;
    uint64_t hash = hashfunc_(key);
    int64_t bucket_idx = hash & (bucket_num_ - 1);
    FastWRGuard l(&locks_[bucket_idx]);
    if (!check_func_(check)) {
      ret = common::Status::kInsertCheckFailed;
      return ret;
    }
    hashnode **node = find_node_ptr(key, bucket_idx, hash);
    hashnode *old = *node;
    if (nullptr == old) {
      hashnode *new_node = (nullptr == buckets_[bucket_idx])
          ? &nodes_[bucket_idx]
          : MOD_NEW_OBJECT(memory::ModId::kCacheHashTable, hashnode);
      new_node->data_ = val;
      new_node->hash_ = hash;
      new_node->next_ = nullptr;
      *node = new_node;
      ++num_elements_;
    } else {
      old_val = old->data_;
      old->data_ = val;
    }
    return ret;
	}
	int remove(const KeyTp &key, ValTp &val) {
    int ret = 0;
    if (!init_) {
      construct();
      val = nullptr;
      return ret;
    }
    uint64_t hash = hashfunc_(key);
    int64_t bucket_idx = hash & (bucket_num_ - 1);
    FastWRGuard l(&locks_[bucket_idx]);
    hashnode **node = find_node_ptr(key, bucket_idx, hash);
    hashnode *result = *node;
    if (nullptr != result) {
      *node = result->next_;
      --num_elements_;
      val = result->data_;
      if (result != &nodes_[bucket_idx]) {
        MOD_DELETE_OBJECT(hashnode, result);
      }
    } else {
      val = nullptr;
    }
    return ret;
	}
	int find(const KeyTp &key, ValTp &val, bool need_ref = false) {
	  int ret = 0;
    if (!init_) {
      construct();
      return ret;
    }
	  uint64_t hash = hashfunc_(key);
	  int64_t bucket_idx = hash & (bucket_num_ - 1);

	  FastRDGuard l(&locks_[bucket_idx]);
	  hashnode *node = *find_node_ptr(key, bucket_idx, hash);
	  if (nullptr != node) {
	    val = node->data_;
	    if (need_ref) {
	      val->ref();
	    }
	  }
	  return ret;
	}
	int construct() {
	  int ret = 0;
	  MutexLock l(&mutex_);
    if (!init_) {
	    buckets_ = (hashnode **)memory::base_malloc(bucket_num_ * sizeof(hashnode *), memory::ModId::kCacheHashTable);
	    memset(buckets_, 0, sizeof(hashnode *) * bucket_num_);
	    int64_t locks_size = (bucket_num_) * sizeof(FastRWLatch);
	    locks_ = (FastRWLatch *)memory::base_malloc(locks_size, memory::ModId::kCacheHashTable);
	    memset(locks_, 0, locks_size);
	    nodes_ = (hashnode *)memory::base_malloc(bucket_num_ * sizeof(hashnode), memory::ModId::kCacheHashTable);
	    memset(nodes_, 0, bucket_num_ * sizeof(hashnode));
      init_ = true;
    }
	  return ret;
	}
	size_type bucket_count() const {
	  return bucket_num_;
	}
	size_type elems_in_bucket(size_type bucket_idx) {
    size_type result = 0;
    if (bucket_idx >= bucket_num_) {
      return result;
    }
    for (hashnode* cur = buckets_[bucket_idx]; cur; cur = cur->next) {
      ++result;
    }
    return result;
	}
	bool empty() const {
	  return 0 == num_elements_;
	}
	int64_t get_usage() const {
	  return usage_;
	}
private:
	hashnode **buckets_;
	int64_t num_elements_;
	int64_t bucket_num_;
  bool init_;
  FastRWLatch *locks_;
  hashnode *nodes_;
  int64_t usage_;

  mutable port::Mutex mutex_;
  mutable HashFunc hashfunc_;
  mutable KeyEqual equal_;
  GetKey get_key_;
  CheckFunc check_func_;
};
}
}
