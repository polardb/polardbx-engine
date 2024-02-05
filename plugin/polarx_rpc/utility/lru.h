//
// Created by zzy on 2023/1/4.
//

#pragma once

#include <cassert>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>

namespace polarx_rpc {

template <class K, class V>
class CcopyableLru final {
 private:
  size_t max_count_;
  mutable std::mutex lock_;
  std::list<std::pair<K, V>> lru_list_;
  // Note: only iterator of std::list is stable after modification.
  // https://stackoverflow.com/questions/6438086/iterator-invalidation-rules-for-c-containers
  std::unordered_map<K, typename std::list<std::pair<K, V>>::iterator> lru_map_;
  size_t count_;

 public:
  explicit CcopyableLru(size_t max_count = 128)
      : max_count_(max_count), count_(0) {}

  inline size_t get_max_count() const { return max_count_; }

  inline void set_max_count(size_t max_count) { max_count_ = max_count; }

  inline V get(const K &key) {
    std::lock_guard<std::mutex> lck(lock_);
    auto it = lru_map_.find(key);
    if (lru_map_.end() == it) return {};
    /// found and copy it
    V v(it->second->second);
    /// move it to first
    lru_list_.splice(lru_list_.begin(), lru_list_, it->second);
    /// assert not changed
    assert(it->second == lru_list_.begin());
    return v;
  }

  /// return evict count
  template <class KK, class VV>
  inline size_t put(KK &&k, VV &&v) {
    std::lock_guard<std::mutex> lck(lock_);

    /// push front new one
    lru_list_.emplace_front(std::make_pair(k, std::forward<VV>(v)));
    auto link = lru_list_.begin();
    auto ib = lru_map_.emplace(std::make_pair(std::forward<KK>(k), link));
    if (ib.second)
      ++count_;
    else {
      /// replace
      lru_list_.erase(ib.first->second);
      ib.first->second = link;
    }

    auto evicted = 0;
    while (count_ > max_count_) {
      /// remove back
      assert(!lru_list_.empty());
      auto &last = lru_list_.back();
      auto sz = lru_map_.erase(last.first);
      (void)sz;
      assert(sz > 0);
      lru_list_.pop_back();
      --count_;
      ++evicted;
    }
    return evicted;
  }

  inline void clear() {
    std::lock_guard<std::mutex> lck(lock_);
    lru_map_.clear();
    lru_list_.clear();
    count_ = 0;
  }
};

}  // namespace polarx_rpc
