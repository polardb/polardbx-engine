//
// Created by zzy on 2022/8/12.
//

#pragma once

#include <algorithm>
#include <cstdint>
#include <thread>

#if defined(__x86_64__) || defined(_M_X64)
#define IS_X64 1
#else
#define IS_X64 0
#endif

#if IS_X64
#include <immintrin.h>
#endif

namespace polarx_rpc {

static constexpr uint32_t SPIN_LIMIT = 6;
static constexpr uint32_t YIELD_LIMIT = 10;

class Cbackoff final {
 private:
  uint32_t step_{0};

  static inline void inner_spin() {
#if defined(_WIN32)
    YieldProcessor();
#elif IS_X64
    ::_mm_pause();
#else
    asm volatile("yield");
#endif
  }

 public:
  inline void reset() { step_ = 0; }

  inline void spin() {
    auto limit = 1u << std::min(step_, SPIN_LIMIT);
    for (uint32_t i = 0; i < limit; ++i) inner_spin();
    if (step_ <= SPIN_LIMIT) ++step_;
  }

  inline void snooze() {
    auto limit = 1u << step_;
    if (step_ <= SPIN_LIMIT) {
      for (uint32_t i = 0; i < limit; ++i) inner_spin();
    } else {
      for (uint32_t i = 0; i < limit; ++i) inner_spin();
      std::this_thread::yield();
    }
    if (step_ <= YIELD_LIMIT) ++step_;
  }

  inline bool is_completed() const { return step_ > YIELD_LIMIT; }
};

}  // namespace polarx_rpc
