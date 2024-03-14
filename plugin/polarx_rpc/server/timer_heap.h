//
// Created by zzy on 2022/7/6.
//

#pragma once

#include <algorithm>
#include <cstdint>
#include <utility>
#include <vector>

#include "../common_define.h"

namespace polarx_rpc {

template <class T>
class CtimerHeap final {
  NO_COPY_MOVE(CtimerHeap)

 private:
  struct timer_t final {
    int64_t trigger_time;
    int32_t timer_id;
    uint32_t timer_type;

    T timer_object;

    bool operator<(const timer_t &another) const {
      // Reverse the heap.
      if (UNLIKELY(trigger_time == another.trigger_time))
        return timer_id - another.timer_id >
               0;  // Use signed minus to prevent overflow.
      else
        return trigger_time - another.trigger_time >
               0;  // Use signed minus to prevent overflow.
    }

    timer_t(int64_t time, int32_t id, uint32_t type, T &&object)
        : trigger_time(time),
          timer_id(id),
          timer_type(type),
          timer_object(std::forward<T>(object)) {}
  };

  int32_t now_id_;
  std::vector<timer_t> heap_;

 public:
  CtimerHeap() : now_id_(0) {}

  void push(T &&object, int64_t time, int32_t &id, uint32_t type = 0) {
    id = now_id_++;
    heap_.emplace_back(time, id, type, std::forward<T>(object));
    std::push_heap(heap_.begin(), heap_.end());
  }

  bool peak(int64_t &time) const {
    if (UNLIKELY(heap_.empty())) return false;

    time = heap_.front().trigger_time;
    return true;
  }

  bool pop(T &object, int32_t &id, uint32_t &type) {
    if (UNLIKELY(heap_.empty())) return false;

    auto &front = heap_.front();
    object = std::move(front.timer_object);
    id = front.timer_id;
    type = front.timer_type;

    std::pop_heap(heap_.begin(), heap_.end());
    heap_.pop_back();
    return true;
  }

  bool pop(int64_t now_time, T &object, int32_t &id, uint32_t &type) {
    if (UNLIKELY(heap_.empty())) return false;

    auto &front = heap_.front();
    if (UNLIKELY(now_time - front.trigger_time < 0)) return false;

    object = std::move(front.timer_object);
    id = front.timer_id;
    type = front.timer_type;

    std::pop_heap(heap_.begin(), heap_.end());
    heap_.pop_back();
    return true;
  }

  size_t size() const { return heap_.size(); }
};

}  // namespace polarx_rpc
