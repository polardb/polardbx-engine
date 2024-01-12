/**
 * Replica Consistent Read Manager
 */

#pragma once

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <utility>

class ReplicaReadManager {
  struct ReadLsnCond {
    const uint64_t lsn;
    std::condition_variable cv;

    ReadLsnCond(uint64_t lsn) : lsn(lsn) {}

    bool operator()(ReadLsnCond *a, ReadLsnCond *b) {
      // ordered by lsn ascending in the wait queue
      return a->lsn > b->lsn;
    }
  };

  template <typename T, class Container = std::vector<T>,
            class Compare = std::less<typename Container::value_type>>
  class erasable_priority_queue
      : public std::priority_queue<T, std::vector<T>> {
   public:
    bool erase(const T &value) {
      auto iter = std::find(this->c.begin(), this->c.end(), value);
      if (iter != this->c.end()) {
        this->c.erase(iter);
        std::make_heap(this->c.begin(), this->c.end(), this->comp);
        return true;
      } else {
        return false;
      }
    }
  };

  std::mutex m_mutex;
  erasable_priority_queue<ReadLsnCond *, std::vector<ReadLsnCond *>,
                          ReadLsnCond>
      m_wait_queue;

  /**
   * Current applied index, should be monotonically increasing
   */
  std::atomic<uint64_t> m_applied_lsn;

 public:
  ReplicaReadManager() {}

  bool wait_for_lsn(uint64_t read_lsn);
  void update_lsn(uint64_t new_lsn);
};

extern ReplicaReadManager replica_read_manager;
