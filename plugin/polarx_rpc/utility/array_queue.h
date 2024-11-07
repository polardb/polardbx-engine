/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/


//
// Created by zzy on 2022/8/29.
//

#pragma once

// Modified & optimized from crossbeam.

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>

#include "../common_define.h"
#include "backoff.h"
#include "number.h"

namespace polarx_rpc {

template <class T>
class CarrayQueue {
  NO_COPY(CarrayQueue)

 private:
  // pushed from tail and popped from head
  cache_padded_t<std::atomic<size_t>> head_;
  cache_padded_t<std::atomic<size_t>> tail_;

  struct slot_t final {
    std::atomic<size_t> stamp;
    T value;
  };

  std::unique_ptr<slot_t[]> buffer_;
  const size_t cap_;
  const size_t one_lap_;

 public:
  explicit CarrayQueue(size_t cap)
      : head_(0),
        tail_(0),
        buffer_(new slot_t[cap]),
        cap_(cap),
        one_lap_(Cnumber::next_power_of_two_sz(cap + 1)) {
    static_assert(
        0 == offsetof(CarrayQueue, head_) &&
            CACHE_LINE_FETCH_ALIGN == offsetof(CarrayQueue, tail_) &&
            2 * CACHE_LINE_FETCH_ALIGN == offsetof(CarrayQueue, buffer_),
        "Bad align of CarrayQueue.");
    assert(cap_ > 0);
    for (size_t i = 0; i < cap_; ++i)
      buffer_[i].stamp.store(
          i, std::memory_order_relaxed);  // init first round stamp
    std::atomic_thread_fence(std::memory_order_release);
  }

  CarrayQueue(CarrayQueue &&another) noexcept
      : head_(another.head_().load(std::memory_order_acquire)),
        tail_(another.tail_().load(std::memory_order_acquire)),
        buffer_(std::move(another.buffer_)),
        cap_(another.cap_),
        one_lap_(another.one_lap_) {
    buffer_.reset();
  }

  CarrayQueue &operator=(CarrayQueue &&another) = delete;

  template <class V>
  inline bool push(V &&v) {
    Cbackoff backoff;
    auto tail = tail_().load(std::memory_order_relaxed);

    while (true) {
      auto index = tail & (one_lap_ - 1);
      auto lap = tail & ~(one_lap_ - 1);

      assert(index < cap_);
      auto &slot = buffer_[index];
      auto stamp = slot.stamp.load(std::memory_order_acquire);

      if (LIKELY(tail == stamp)) {
        // slot is ready for next one
        auto new_tail = index + 1 < cap_ ? tail + 1 : lap + one_lap_;
        if (LIKELY(tail_().compare_exchange_weak(tail, new_tail,
                                                 std::memory_order_seq_cst,
                                                 std::memory_order_relaxed))) {
          // win the race
          slot.value = std::forward<V>(v);
          slot.stamp.store(tail + 1, std::memory_order_release);
          return true;
        }
        backoff.spin();
      } else if (stamp + one_lap_ == tail + 1) {
        std::atomic_thread_fence(std::memory_order_seq_cst);
        // may full and recheck head
        auto head = head_().load(std::memory_order_relaxed);
        if (head + one_lap_ == tail) return false;  // full
        backoff.spin();
        // reload and recheck
        tail = tail_().load(std::memory_order_relaxed);
      } else {
        backoff.snooze();  // wait data move and stamp update
        tail = tail_().load(std::memory_order_relaxed);
      }
    }
  }

  inline bool pop(T &v) {
    Cbackoff backoff;
    auto head = head_().load(std::memory_order_relaxed);

    while (true) {
      auto index = head & (one_lap_ - 1);
      auto lap = head & ~(one_lap_ - 1);

      assert(index < cap_);
      auto &slot = buffer_[index];
      auto stamp = slot.stamp.load(std::memory_order_acquire);

      if (LIKELY(head + 1 == stamp)) {
        // slot was correctly filled
        auto new_head = index + 1 < cap_ ? head + 1 : lap + one_lap_;
        if (LIKELY(head_().compare_exchange_weak(head, new_head,
                                                 std::memory_order_seq_cst,
                                                 std::memory_order_relaxed))) {
          // win the race
          v = std::move(slot.value);
          slot.stamp.store(head + one_lap_, std::memory_order_release);
          return true;
        }
        backoff.spin();
      } else if (stamp == head) {
        std::atomic_thread_fence(std::memory_order_seq_cst);
        // may empty and recheck tail
        auto tail = tail_().load(std::memory_order_relaxed);
        if (tail == head) return false;  // empty
        backoff.spin();
        // reload and recheck
        head = head_().load(std::memory_order_relaxed);
      } else {
        backoff.snooze();  // wait data move and stamp update
        head = head_().load(std::memory_order_relaxed);
      }
    }
  }

  inline size_t capacity() const { return cap_; }

  inline bool empty() const {
    auto head = head_().load(std::memory_order_seq_cst);
    auto tail = tail_().load(std::memory_order_seq_cst);
    return tail == head;
  }

  inline bool full() const {
    auto head = head_().load(std::memory_order_seq_cst);
    auto tail = tail_().load(std::memory_order_seq_cst);
    return head + one_lap_ == tail;
  }

  inline size_t length() const {
    while (true) {
      auto tail = tail_().load(std::memory_order_seq_cst);
      auto head = head_().load(std::memory_order_seq_cst);

      if (tail_().load(std::memory_order_seq_cst) == tail) {
        auto hix = head & (one_lap_ - 1);
        auto tix = tail & (one_lap_ - 1);
        return hix < tix
                   ? tix - hix
                   : (hix > tix ? cap_ - hix + tix : (tail == head ? 0 : cap_));
      }
    }
  }

  inline size_t head() const { return head_().load(std::memory_order_seq_cst); }

  inline size_t tail() const { return tail_().load(std::memory_order_seq_cst); }
};

}  // namespace polarx_rpc
