//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "monitoring/instrumented_mutex.h"
#include "monitoring/thread_status_util.h"
#include "monitoring/query_perf_context.h"
#include "util/sync_point.h"
#include "logger/logger.h"
#include "memory/base_malloc.h"

using namespace xengine;
using namespace common;
using namespace db;
using namespace util;
using namespace port;
using namespace memory;

namespace xengine {
namespace monitor {

void InstrumentedMutex::Lock() {
  QUERY_COUNT(CountPoint::DB_MUTEX_WAIT);
  QUERY_TRACE_BEGIN(TracePoint::DB_MUTEX_WAIT);
  LockInternal();
  if (env_ != nullptr) {
    start_nano_ = env_->NowNanos();
  }
  QUERY_TRACE_END();
}

void InstrumentedMutex::Unlock() {
  uint64_t locked_nanos = env_ != nullptr ? env_->NowNanos() - start_nano_ : 0;
  mutex_.Unlock();
  if (backtrace_limit_nano_ != nullptr && locked_nanos > *backtrace_limit_nano_) {
    char *frame_buffer = static_cast<char*>(
          base_malloc(kMaxFrames * STRING_SIZE_PER_FRAME));
    print_backtrace_symbol(0, frame_buffer);
    __XENGINE_LOG(WARN, "Mutex %p holding too long, %luns.\n%s",
                  this, locked_nanos, frame_buffer);
    base_free(frame_buffer);
  }
}

void InstrumentedMutex::LockInternal() {
#ifndef NDEBUG
  ThreadStatusUtil::TEST_StateDelay(ThreadStatus::STATE_MUTEX_WAIT);
#endif
  mutex_.Lock();
}

void InstrumentedCondVar::Wait() {
  QUERY_COUNT(CountPoint::DB_MUTEX_WAIT);
  QUERY_TRACE_BEGIN(TracePoint::DB_MUTEX_WAIT);
  WaitInternal();
  QUERY_TRACE_END();
}

void InstrumentedCondVar::WaitInternal() {
#ifndef NDEBUG
  ThreadStatusUtil::TEST_StateDelay(ThreadStatus::STATE_MUTEX_WAIT);
#endif
  cond_.Wait();
}

bool InstrumentedCondVar::TimedWait(uint64_t abs_time_us) {
  bool result = false;
  QUERY_COUNT(CountPoint::DB_MUTEX_WAIT);
  QUERY_TRACE_BEGIN(TracePoint::DB_MUTEX_WAIT);
  result = TimedWaitInternal(abs_time_us);
  QUERY_TRACE_END();
  return result;
}

bool InstrumentedCondVar::TimedWaitInternal(uint64_t abs_time_us) {
#ifndef NDEBUG
  ThreadStatusUtil::TEST_StateDelay(ThreadStatus::STATE_MUTEX_WAIT);
#endif

  TEST_SYNC_POINT_CALLBACK("InstrumentedCondVar::TimedWaitInternal",
                           &abs_time_us);

  return cond_.TimedWait(abs_time_us);
}
}  // namespace monitor
}  // namespace xengine
