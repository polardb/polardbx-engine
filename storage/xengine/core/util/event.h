/************************************************************************
 *
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

 * $Id:  event.h,v 1.0 12/25/2017 04:36:36 PM
 *
 ************************************************************************/

/**
 * @file event.h
 * @date 12/25/2017 04:36:36 PM
 * @version 1.0
 * @brief
 *
 **/

#pragma once
#include <semaphore.h>
#include <atomic>
#include "common.h"

namespace xengine {
namespace util {    
class Event {
 public:
  Event() { sem_init(&sem_, 0, 0); }
  void post() { sem_post(&sem_); };
  void wait() { sem_wait(&sem_); }
  int timed_wait(Duration _dur) {
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    uint64_t to_ms = _dur.count();
    auto old_nsec = ts.tv_nsec;
    ts.tv_nsec = (ts.tv_nsec + (to_ms % 1000) * 1000000) % 1000000000;
    ts.tv_sec = ts.tv_sec + to_ms / 1000 + (old_nsec > ts.tv_nsec ? 1 : 0);
    return sem_timedwait(&sem_, &ts);
  }

 private:
  enum State { INIT = 0, WAITING = 1, TIMEOUT = 2 };
  std::atomic<State> state_{INIT};
  sem_t sem_;
};
}  // namespace is
}
