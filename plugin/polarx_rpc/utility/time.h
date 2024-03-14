//
// Created by zzy on 2022/7/8.
//

#pragma once

#include <chrono>
#include <cstdint>
#include <thread>

#include <time.h>

#include "../common_define.h"

namespace polarx_rpc {

class Ctime final {
  NO_CONSTRUCTOR(Ctime)
  NO_COPY_MOVE(Ctime)

 public:
  static inline int64_t system_ms() {
    //    auto now_time = std::chrono::system_clock::now();
    //    return std::chrono::duration_cast<std::chrono::milliseconds>(
    //               now_time.time_since_epoch())
    //        .count();
    ::timespec time;
    ::clock_gettime(CLOCK_REALTIME, &time);
    return time.tv_sec * 1000ll + time.tv_nsec / 1000000ll;
  }

  static inline int64_t steady_ms() {
    //    auto now_time = std::chrono::steady_clock::now();
    //    return std::chrono::duration_cast<std::chrono::milliseconds>(
    //               now_time.time_since_epoch())
    //        .count();
    ::timespec time;
    ::clock_gettime(CLOCK_MONOTONIC, &time);
    return time.tv_sec * 1000ll + time.tv_nsec / 1000000ll;
  }

  static inline void sleep_ms(int64_t time_ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(time_ms));
  }

  static inline int64_t steady_us() {
    //    auto now_time = std::chrono::steady_clock::now();
    //    return std::chrono::duration_cast<std::chrono::microseconds>(
    //               now_time.time_since_epoch())
    //        .count();
    ::timespec time;
    ::clock_gettime(CLOCK_MONOTONIC, &time);
    return time.tv_sec * 1000000ll + time.tv_nsec / 1000ll;
  }

  static inline int64_t steady_ns() {
    ::timespec time;
    ::clock_gettime(CLOCK_MONOTONIC, &time);
    return time.tv_sec * 1000000000L + time.tv_nsec;
  }
};

}  // namespace polarx_rpc
