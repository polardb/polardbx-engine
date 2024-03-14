//
// Created by zzy on 2022/8/17.
//

#pragma once

#include <atomic>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>

#include "../common_define.h"

#include "random.h"

namespace polarx_rpc {

class Chistogram final {
  NO_COPY_MOVE(Chistogram)

 private:
  static constexpr auto HISTOGRAM_NSLOTS = 128;

  /** Number of elements in each array */
  const int array_size_;
  /** Lower bound of values to track */
  const double range_min_;
  /** Upper bound of values to track */
  const double range_max_;
  /** Value to deduct to calculate histogram range based on array element */
  const double range_deduct_;
  /** Value to multiply to calculate histogram range based array element */
  const double range_mult_;

  /// buffer
  std::unique_ptr<uint64_t[]> buffers_;

  /**
   * rwlock to protect cumulative_array and cumulative_nevents from concurrent
   * updates.
   */
  std::mutex lock_;

  /**
   * Cumulative histogram array. Updated 'on demand' by
   * histogram_get_pct_intermediate(). Protected by 'lock'.
   */
  uint64_t *cumulative_array_;
  /**
   * Total number of events in cumulative_array. Updated on demand by
   * histogram_get_pct_intermediate(). Protected by 'lock'.
   */
  uint64_t cumulative_nevents_;
  /**
   * Temporary array for intermediate percentile calculations. Protected by
   * 'lock'.
   */
  uint64_t *temp_array_;
  /**
   * Intermediate histogram values are split into multiple slots and updated
   * with atomics. Aggregations into cumulative values is performed by
   * sb_histogram_get_pct_intermediate() function.
   */
  std::unique_ptr<std::atomic<uint64_t> *[]> interm_slots_;

  /**
   * Aggregate arrays from intermediate slots into cumulative_array. This should
   * be called with the histogram lock write-locked.
   */
  inline void merge_intermediate_into_cumulative() {
    auto nevents = cumulative_nevents_;
    for (auto s = 0; s < HISTOGRAM_NSLOTS; ++s) {
      for (auto i = 0; i < array_size_; ++i) {
        auto t = interm_slots_[s][i].exchange(0, std::memory_order_relaxed);
        cumulative_array_[i] += t;
        nevents += t;
      }
    }
    cumulative_nevents_ = nevents;
  }

 public:
  Chistogram(size_t size, double range_min, double range_max)
      : array_size_(size),
        range_min_(range_min),
        range_max_(range_max),
        range_deduct_(::log(range_min)),
        range_mult_((size - 1) / (::log(range_max) - range_deduct_)) {
    buffers_.reset(new uint64_t[size * (HISTOGRAM_NSLOTS + 2)]);
    interm_slots_.reset(new std::atomic<uint64_t> *[HISTOGRAM_NSLOTS]);

    auto ptr = buffers_.get();
    if (UNLIKELY(reinterpret_cast<uintptr_t>(ptr) % sizeof(uint64_t) != 0))
      throw std::runtime_error("Bad buffer not aligned");

    /// reset all first
    for (auto i = 0; i < array_size_ * (HISTOGRAM_NSLOTS + 2); ++i) ptr[i] = 0;

    cumulative_array_ = ptr;
    ptr += array_size_;

    cumulative_nevents_ = 0;

    temp_array_ = ptr;
    ptr += array_size_;

    for (auto i = 0; i < HISTOGRAM_NSLOTS; ++i) {
      interm_slots_[i] = reinterpret_cast<std::atomic<uint64_t> *>(ptr);
      ptr += array_size_;
    }
  }

  inline void update(double value) {
    auto slot = Crandom::get_instance()->next() % HISTOGRAM_NSLOTS;
    auto i = static_cast<int>(
        ::floor((::log(value) - range_deduct_) * range_mult_ + 0.5));
    if (UNLIKELY(i < 0))
      i = 0;
    else if (UNLIKELY(i >= array_size_))
      i = array_size_ - 1;
    interm_slots_[slot][i].fetch_add(1, std::memory_order_relaxed);
  }

  inline void reset() {
    std::lock_guard<std::mutex> lck(lock_);
    for (auto s = 0; s < HISTOGRAM_NSLOTS; ++s) {
      for (auto i = 0; i < array_size_; ++i)
        interm_slots_[s][i].exchange(0, std::memory_order_relaxed);
    }
    for (auto i = 0; i < array_size_; ++i) cumulative_array_[i] = 0;
    cumulative_nevents_ = 0;
  }

  std::string histogram() {
    std::lock_guard<std::mutex> lck(lock_);

    merge_intermediate_into_cumulative();

    uint64_t maxcnt = 0;
    for (auto i = 0; i < array_size_; ++i) {
      if (cumulative_array_[i] > maxcnt) maxcnt = cumulative_array_[i];
    }
    if (maxcnt == 0) return {};

    std::string result(
        "       value  ------------- distribution ------------- count\n");
    char buf[1024];
    for (auto i = 0; i < array_size_; i++) {
      if (0 == cumulative_array_[i]) continue;

      auto width =
          static_cast<int>(::floor(cumulative_array_[i] * 40. / maxcnt + 0.5));

      snprintf(buf, sizeof(buf), "%12.9f |%-40.*s %lu\n",
               ::exp(i / range_mult_ + range_deduct_), /* value */
               width,
               "****************************************", /* distribution */
               (unsigned long)cumulative_array_[i]);       /* count */
      result += buf;
    }
    return result;
  }
};

}  // namespace polarx_rpc
