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
 */

#pragma once

#include "xengine/env.h"
#include <atomic>

namespace xengine
{
namespace util
{

inline bool reach_time_interval(const uint64_t interval)
{
  bool bret = false;
  static std::atomic<uint64_t> last_time(0);
  uint64_t curr_time = Env::Default()->NowMicros();
  uint64_t old_time = last_time;
  if ((interval + last_time) < curr_time
      && last_time.compare_exchange_weak(old_time, curr_time)) {
    bret = true;
  }
  return bret;
}

inline bool reach_tl_time_interval(const uint64_t interval)
{
  bool bret = false;
  static __thread uint64_t last_time = 0;
  uint64_t curr_time = Env::Default()->NowMicros();
  uint64_t old_time = last_time;
  if ((interval + last_time) < curr_time) {
    last_time = curr_time;
    bret = true;
  }
  return bret;
}

} // namespace util
} // namespace xengine
