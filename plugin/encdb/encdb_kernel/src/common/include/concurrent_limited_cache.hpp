/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 */
#pragma once

#include <cstddef>
#include <stdexcept>
#include <unordered_map>
#include <vector>
#include "encdb_common_exception.h"
#include "memory.hpp"
#include "shared_lock.hpp"

#include "pthread.h"

namespace encdb {

template <typename key_t, typename value_t,
          typename hash_key_t = std::hash<key_t>>
class ConcurrentLimitedCache : public EncdbBaseObj {
 public:
  typedef typename std::pair<key_t, value_t> key_value_pair_t;

  ConcurrentLimitedCache(size_t max_size)
      : max_size_(max_size), fullfilled_(false) {}

  void put(const key_t &key, const value_t &value) {
    Writer w(shared_lock_);

    if (cache_map_.size() + 1 > max_size_) {
      fullfilled_ = true;
      // remove half elements from map
      auto it = this->cache_map_.begin();
      for (size_t i = 0; i < max_size_ / 2; i++, it++) {
      }
      this->cache_map_.erase(cache_map_.begin(), it);
    }
    cache_map_[key] = value;
  }

  const value_t &get(const key_t &key) {
    Reader r(shared_lock_);
    auto it = cache_map_.find(key);
    if (it == cache_map_.end()) {
      throw CacheMissException("cache miss");
    }
    return it->second;
  }

  bool exists(const key_t &key) {
    Reader r(shared_lock_);

    return cache_map_.find(key) != cache_map_.end();
  }

  void clear() {
    Writer w(shared_lock_);
    cache_map_.clear();
    fullfilled_ = false;
  }

  void remove(const key_t &key) {
    Writer w(shared_lock_);
    auto it = cache_map_.find(key);
    if (it != cache_map_.end()) {
      cache_map_.erase(it);
    }
  }

  std::vector<std::pair<key_t, value_t>> dump() {
    Reader r(shared_lock_);
    std::vector<std::pair<key_t, value_t>> ret;
    for (auto &it : cache_map_) {
      ret.push_back(std::pair<key_t, value_t>(it.first, it.second));
    }
    return ret;
  }

  bool hasBeenFull() { return fullfilled_; }

 private:
  std::unordered_map<key_t, value_t, hash_key_t> cache_map_;
  size_t max_size_;
  SharedLock shared_lock_;
  bool fullfilled_;
};

template <typename key_t, typename value_t,
          typename hash_key_t = std::hash<key_t>>
class LimitedCache : public EncdbBaseObj {
 public:
  typedef typename std::pair<key_t, value_t> key_value_pair_t;

  LimitedCache(size_t max_size) : max_size_(max_size) {}

  void put(const key_t &key, const value_t &value) {
    if (cache_map_.size() + 1 > max_size_) {
      // remove half elements from map
      auto it = this->cache_map_.begin();
      for (size_t i = 0; i < max_size_ / 2; i++, it++) {
      }
      this->cache_map_.erase(cache_map_.begin(), it);
    }
    cache_map_[key] = value;
  }

  const value_t &get(const key_t &key) {
    auto it = cache_map_.find(key);
    if (it == cache_map_.end()) {
      throw CacheMissException("cache miss");
    }
    return it->second;
  }

  bool exists(const key_t &key) {
    return cache_map_.find(key) != cache_map_.end();
  }

  void clear() { cache_map_.clear(); }

  void remove(const key_t &key) {
    auto it = cache_map_.find(key);
    if (it != cache_map_.end()) {
      cache_map_.erase(it);
    }
  }

 private:
  std::unordered_map<key_t, value_t, hash_key_t> cache_map_;
  size_t max_size_;
};

}  // namespace encdb
