/************************************************************************
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.

 * $Id:  common.h,v 1.0 05/10/2018 10:39:41 AM
 *
 ************************************************************************/

/**
 * @file common.h
 * @date 05/10/2018 10:39:41 AM
 * @version 1.0
 * @brief
 *
 **/
#ifndef IS_COMMON_COMMON_H_
#define IS_COMMON_COMMON_H_

#include <sys/syscall.h>
#include <unistd.h>
#include <linux/futex.h>
#include <chrono>
#include <errno.h>

namespace xengine {
namespace util {
using Duration = std::chrono::milliseconds;
}
}

#ifndef UNUSED
#define UNUSED(x) ((void)x)
#endif

#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x)   (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x)   (x)
#define UNLIKELY(x) (x)
#endif

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64U
#endif

#ifndef CACHE_ALIGNED
#define CACHE_ALIGNED __attribute__ ((aligned (CACHE_LINE_SIZE)))
#endif
#ifndef PAUSE
#if defined(__x86_64__)
#define PAUSE() asm("pause\n")
#elif defined(__aarch64__)
#define PAUSE() asm("yield\n")
#endif
#endif

// Get Local address for xrdma
#ifdef WITH_RDMA
# define STRINGIZE(x)      #x
# define STRINGIZE_VAL(x)  STRINGIZE(x)
# define LOCAL_ADDR        STRINGIZE_VAL(LOCAL_IP)
#else
# define LOCAL_ADDR  "127.0.0.1"
#endif

// atomic operations
#define ATOMIC_LOAD(x) ({asm volatile("" ::: "memory"); *(x);})
#define ATOMIC_BCAS(val, cmpv, newv) ({__sync_bool_compare_and_swap((val), (cmpv), (newv));})
#define ATOMIC_AND_FETCH(val, andv) ({__sync_and_and_fetch((val), (andv));})
#define ATOMIC_SUB_FETCH(val, subv) ({__sync_sub_and_fetch((val), (subv));})
#define ATOMIC_FETCH_ADD(val, subv) ({__sync_fetch_and_add((val), (subv));})
#define ATOMIC_ADD_FETCH(val, subv) ({__sync_add_and_fetch((val), (subv));})
#define ATOMIC_SET(val, newv) ({__sync_lock_test_and_set((val), (newv));})
#define ATOMIC_FETCH_AND(val, andv) ({__sync_fetch_and_and((val), (andv));})
#define ATOMIC_CAS(val, cmpv, newv) ({__sync_val_compare_and_swap((val), (cmpv), (newv));})

// futex
#define futex(...) syscall(SYS_futex, __VA_ARGS__)
inline int futex_wait(void *p, int val, const timespec *timeout)
{
  int ret = 0;
  if (0 != futex((int *)p, FUTEX_WAIT_PRIVATE, val, timeout, NULL, 0)) {
    ret = errno;
  }
  return ret;
}

inline int64_t futex_wake(void *p, int val)
{
  return futex((int *)p, FUTEX_WAKE_PRIVATE, val, NULL, NULL, 0);
}

#endif // end IS_COMMON_COMMON_H_
