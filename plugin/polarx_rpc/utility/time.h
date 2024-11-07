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
