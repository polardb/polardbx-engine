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
 *
 * It's a thread-safe hashmap
 */

#pragma once

#include <vector>
#include <unordered_map>
#include <mutex>

namespace xengine {
namespace util {

template<typename Key, typename Value, typename Hash>
class Bucket {
using ConstMapIterator = typename std::unordered_map<Key, Value>::const_iterator;
using MapIterator = typename std::unordered_map<Key, Value>::iterator;
public:
  Bucket() : end_(map_.cend()) {};
  ~Bucket() = default;
  bool insert(const Key &key, const Value &value) {
    std::lock_guard<std::mutex> guard(mutex_);
    return map_.emplace(key, value).second;
  }

  std::pair<MapIterator, bool> insert_and_get(const Key &key, const Value &value) {
    std::lock_guard<std::mutex> guard(mutex_);
    return map_.emplace(key, value);
  }

  bool put(const Key &key, const Value &value) {
    bool ret = false;
    std::lock_guard<std::mutex> guard(mutex_);
    ret = (map_.erase(key) == 0);
    map_.emplace(key, value);
    return ret;
  }

  bool erase(const Key &key) {
    std::lock_guard<std::mutex> guard(mutex_);
    return map_.erase(key) != 0;
  }

  template<typename Predicate>
  bool for_one(const Key &key, Predicate &p) {
    std::lock_guard<std::mutex> guard(mutex_);
    auto it = map_.find(key);
    return it == map_.end() ? false : (p(it->second), true);
  }

  template<typename Predicate>
  void for_each(Predicate &p) {
    std::lock_guard<std::mutex> guard(mutex_);
    std::for_each(map_.begin(), map_.end(), p);
  }

  ConstMapIterator find(const Key &key) {
    std::lock_guard<std::mutex> guard(mutex_);
    auto it = map_.find(key);
    if (it == map_.end()) {
      return end_;
    } else {
      return it;
    }
  }

  const Value& at(const Key& key) {
    std::lock_guard<std::mutex> guard(mutex_);
    return map_.at(key);
  }

  size_t size() {
    std::lock_guard<std::mutex> guard(mutex_);
    return map_.size();
  }

  size_t count(const Key &key) {
    std::lock_guard<std::mutex> guard(mutex_);
    return map_.count(key);
  }

  void clear() {
    std::lock_guard<std::mutex> guard(mutex_);
    map_.clear();
  }

  ConstMapIterator get_real_begin() {
    std::lock_guard<std::mutex> guard(mutex_);
    return map_.cbegin();
  }

  ConstMapIterator get_real_end() {
    std::lock_guard<std::mutex> guard(mutex_);
    return map_.cend();
  }

  void set_end(ConstMapIterator end) {
    end_ = end;
  }

  ConstMapIterator get_end() {
    return end_;
  }

private:
  std::unordered_map<Key, Value, Hash> map_;
  std::mutex mutex_;
  ConstMapIterator end_;
};

template<typename Key, typename Value, typename Hash = std::hash<Key>>
class ConcurrentHashMap {
using ConstMapIterator = typename std::unordered_map<Key, Value>::const_iterator;
public:
  class ConcurrentHashMapIterator {
  public:
    const std::pair<const Key, Value>& operator*() const {
      return *bucket_iterator_;
    }
    const std::pair<const Key, Value>* operator->() const {
      return &*bucket_iterator_;
    }
    ConcurrentHashMapIterator& operator++() {
      ++bucket_iterator_;
      next();
      return *this;
    }
    bool operator==(const ConcurrentHashMapIterator& o) const {
      return bucket_iterator_ == o.bucket_iterator_ && index_ == o.index_;
    }
    bool operator!=(const ConcurrentHashMapIterator& o) const {
      return !(*this == o);
    }

//  private:
    explicit ConcurrentHashMapIterator(ConcurrentHashMap<Key, Value>* map)
        : bucket_iterator_(map->get_bucket(0).get_real_begin()),
          index_(0),
          map_(map) {
      next();
    }
    ConcurrentHashMapIterator(ConstMapIterator bucket_iterator,
                              size_t index,
                              ConcurrentHashMap<Key, Value>* map)
        : bucket_iterator_(bucket_iterator),
          index_(index),
          map_(map) {}
    void next() {
      while (index_ < map_->get_bucket_num() &&
          bucket_iterator_ == map_->get_bucket(index_).get_real_end()) {
        ++index_;
        bucket_iterator_ = map_->get_bucket(index_).get_real_begin();
      }
    }
  private:
    ConstMapIterator bucket_iterator_;
    size_t index_;
    ConcurrentHashMap<Key, Value>* map_;
  };

  ConcurrentHashMap(size_t bucket_number = kDefaultBucketNum,
                    const Hash &hash = Hash())
    : buckets_(bucket_number + 1), hash_(hash), bucket_number_(bucket_number) {
      for (size_t i = 0; i < bucket_number_; ++i) {
        buckets_[i].set_end(buckets_[bucket_number_].get_real_end());
      }
    }
  ~ConcurrentHashMap() {
    buckets_.clear();
  }

  template<typename Predicate>
    bool for_one(const Key &key, Predicate &p) {
      return buckets_[hashcode(key)].for_one(key, p);
    }

  template<typename Predicate>
    void for_each(Predicate &p) {
      for (size_t i = 0; i != bucket_number_; ++i) {
        buckets_[i].for_each(p);
      }
    }

  bool insert(const Key &key, const Value &value) {
    return buckets_[hashcode(key)].insert(key, value);
  }

  bool put(const Key &key, const Value &value) {
    return buckets_[hashcode(key)].put(key, value);
  }

  bool erase(const Key &key) {
    return buckets_[hashcode(key)].erase(key);
  }

  ConcurrentHashMapIterator find(const Key &key) {
    auto i = hashcode(key);
    auto it = buckets_[i].find(key);
    if (it == buckets_[i].get_end()) {
      return end();
    } else {
      return ConcurrentHashMapIterator(it, i, this);
    }
  }

  Value& operator[](const Key& key) {
    auto item = buckets_[hashcode(key)].insert_and_get(key, Value());
    return item.first->second;
  }

  const Value at(const Key& key) {
    return buckets_[hashcode(key)].at(key);
  }

  size_t size() {
    size_t size = 0;
    for (auto &bucket : buckets_) {
      size += bucket.size();
    }
    return size;
  }

  size_t count(const Key &key) {
    return buckets_[hashcode(key)].count(key);
  }

  ConcurrentHashMapIterator begin() {
    return ConcurrentHashMapIterator(this);
  }

  ConcurrentHashMapIterator end() {
    return ConcurrentHashMapIterator(buckets_[bucket_number_].get_real_end(),
                                     bucket_number_,
                                     this);
  }

  void clear() {
    for (auto &bucket : buckets_) {
      bucket.clear();
    }
  }

  size_t get_bucket_num() const {
    return bucket_number_;
  }

  Bucket<Key, Value, Hash>& get_bucket(size_t index) {
    assert (index <= bucket_number_);
    return buckets_[index];
  }

private:
  inline size_t hashcode(const Key &key) {
    return hash_(key) % bucket_number_;
  }
private:
  static const unsigned kDefaultBucketNum = 1024;
  std::vector<Bucket<Key, Value, Hash> > buckets_;
  Hash hash_;
  size_t bucket_number_;
};

} // namespace util
} // namespace xengine
