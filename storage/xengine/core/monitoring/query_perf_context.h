/*
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <pthread.h>
#include <atomic>
#include <memory>
#include <string>
#include <vector>

namespace xengine {
namespace util {
class ThreadLocalPtr;
class Env;
}  // namespace util

namespace cache {
class Cache;
class RowCache;
}  // namespace cache

}  // namespace xengine

namespace xengine {
namespace monitor {

class StatisticsManager;

// To add new TracePoint, monify trace_point.h
#define DECLARE_TRACE(trace_point) trace_point,
#define DECLARE_COUNTER(trace_point)
enum class TracePoint : int64_t {
#include "trace_point.h"
  QUERY_TIME_MAX_VALUE
};
#undef DECLARE_TRACE
#undef DECLARE_COUNTER

#define DECLARE_TRACE(trace_point)
#define DECLARE_COUNTER(trace_point) trace_point,
enum class CountPoint : int64_t {
#include "trace_point.h"
  QUERY_COUNT_MAX_VALUE
};
#undef DECLARE_TRACE
#undef DECLARE_COUNTER

using TimeType = uint64_t;
using CountType = int64_t;

struct TraceStats {
  TimeType cost_;
  CountType count_;
  int64_t point_id_;
};

struct CountStats {
  CountType count_;
  int64_t point_id_;
};

// Record stats during one query. In fact, this is thread level for easy access
// from anywhere in program. So reset should be called once a query starts.
class QueryPerfContext {
 public:
  QueryPerfContext();
  int init();
  void reset();
  void trace(TracePoint point);
  void end_trace();

  void count(CountPoint point, CountType delta = 1);

  // Dump QUERY TRACE string to the internal buffer contents_.
  // res will point to the internal buffer.
  // This function should be used when the caller can directly write it out.
  void to_string(int64_t total_time, const char *&res, int64_t &size);

  TimeType current(xengine::util::Env *env) const;

  CountType get_count(TracePoint point) const;
  TimeType get_costs(TracePoint point) const;
  CountType get_count(CountPoint point) const;
  CountType get_global_count(CountPoint point) const;
  void get_global_trace_info(TimeType *time, CountType *count) const;
  void clear_stats();

  void finish(const char *query, uint64_t query_length);

  static QueryPerfContext *new_query_context();
  static const char *empty_str() { return ""; }

  static void schedule_log_stats(void *ctx);
  static void async_log_stats(xengine::util::Env *env, const std::string &path,
                              cache::Cache *block_cache,
                              cache::RowCache *row_cache);
  static void make_key();
  static void delete_context(void *ctx);
  static void shutdown();

  static bool opt_enable_count_;
  static bool opt_print_stats_;
  static bool opt_trace_sum_;
  static bool opt_print_slow_;
  static double opt_threshold_time_;

 private:
  void print_int64_to_buffer(char *buffer, int64_t &pos, int64_t value);

  TimeType last_time_point_;
  TraceStats stats_[static_cast<int64_t>(TracePoint::QUERY_TIME_MAX_VALUE)];
  CountStats counters_[static_cast<int64_t>(CountPoint::QUERY_COUNT_MAX_VALUE)];
  static StatisticsManager *statistics_;
  static std::atomic_bool shutdown_;
  static std::atomic_int_fast32_t running_count_;
  static pthread_key_t query_trace_tls_key_;
  std::vector<TracePoint> trace_stack_;
  std::vector<TimeType> time_stack_;
  TracePoint current_point_;

  std::string contents_;
  int64_t begin_nanos_;
};

extern thread_local QueryPerfContext *tls_query_perf_context;
extern thread_local bool tls_enable_query_trace;

inline QueryPerfContext *get_tls_query_perf_context() {
  if (nullptr == tls_query_perf_context) {
    tls_query_perf_context = QueryPerfContext::new_query_context();
  }
  return tls_query_perf_context;
}

// Used in some cases where some function may have many return statements thus
// the trace point is hard to put.
class TraceGuard {
 public:
  TraceGuard(TracePoint point);
  ~TraceGuard();

 private:
  TracePoint point_;
};

const char **get_trace_point_name();
const char **get_count_point_name();
TimeType get_trace_unit(int64_t eval_milli_sec);

inline void query_trace_reset() {
  // opt_enable_count_: X-Engine has been initialized
  // opt_print_slow_ or opt_trace_sum_: data has output so collection is
  // necessary get_tls_query_perf_context(): context is allocated successfully.
  tls_enable_query_trace =
      QueryPerfContext::opt_enable_count_ &&
      (QueryPerfContext::opt_print_slow_ || QueryPerfContext::opt_trace_sum_) &&
      get_tls_query_perf_context();
  if (QueryPerfContext::opt_enable_count_ && get_tls_query_perf_context()) {
    tls_query_perf_context->reset();
  }
}

inline void query_count(CountPoint point) {
  if (QueryPerfContext::opt_enable_count_ && get_tls_query_perf_context()) {
    tls_query_perf_context->count(point);
  }
}

inline void query_count_add(CountPoint point, int64_t delta) {
  if (QueryPerfContext::opt_enable_count_ && get_tls_query_perf_context()) {
    tls_query_perf_context->count(point, delta);
  }
}

inline void query_trace_begin(TracePoint point) {
  if (tls_enable_query_trace) {
    tls_query_perf_context->trace(point);
  }
}

inline void query_trace_end() {
  if (tls_enable_query_trace) {
    tls_query_perf_context->end_trace();
  }
}

inline void query_trace_finish(const char *query, uint64_t query_length) {
  if (tls_enable_query_trace) {
    tls_query_perf_context->finish(query, query_length);
  }
}

#define DECLARE_VAR_(line, trace_point) \
  xengine::monitor::TraceGuard perf_guard##line(trace_point);

#define COMBINE1(X, Y) X##Y  // helper macro
#define COMBINE(X, Y) COMBINE1(X, Y)

// Be able to OFF all trace funcitons when compile.
#if (defined WITH_QUERY_TRACE) || (!defined NDEBUG)
#define QUERY_COUNT(count_point) xengine::monitor::query_count(count_point);
#define QUERY_COUNT_ADD(count_point, delta) \
  xengine::monitor::query_count_add(count_point, delta);
#define QUERY_COUNT_SHARD(count_point, shard)
#define QUERY_TRACE_RESET() xengine::monitor::query_trace_reset();
#define QUERY_TRACE_SCOPE(trace_point) \
  xengine::monitor::TraceGuard COMBINE(perf_guard, __LINE__)(trace_point);
#define QUERY_TRACE_BEGIN(trace_point) \
  xengine::monitor::query_trace_begin(trace_point);
#define QUERY_TRACE_END() xengine::monitor::query_trace_end();
#define QUERY_TRACE_FINISH(query, query_length) \
  xengine::monitor::query_trace_finish(query, query_length);
#else
#define QUERY_COUNT(count_point)
#define QUERY_COUNT_ADD(count_point, delta)
#define QUERY_COUNT_SHARD(count_point, shard)
#define QUERY_TRACE_RESET()
#define QUERY_TRACE_SCOPE(trace_point)
#define QUERY_TRACE_BEGIN(trace_point) ;
#define QUERY_TRACE_END() ;
#define QUERY_TRACE_FINISH(query, query_length)
#endif

}  // namespace monitor
}  // namespace xengine
