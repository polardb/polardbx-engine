// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include <algorithm>
#include <iostream>
#include <thread>
#include <vector>

#include "monitoring/histogram.h"
#include "monitoring/instrumented_mutex.h"
#include "monitoring/thread_status_util.h"
#include "monitoring/query_perf_context.h"
#include "port/port.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/testharness.h"
#include "utilities/merge_operators.h"
#include "xengine/db.h"
#include "xengine/memtablerep.h"
#include "xengine/slice_transform.h"

bool FLAGS_random_key = false;
bool FLAGS_use_set_based_memetable = false;
int FLAGS_total_keys = 100;
int FLAGS_write_buffer_size = 1000000000;
int FLAGS_max_write_buffer_number = 8;
int FLAGS_min_write_buffer_number_to_merge = 7;
bool FLAGS_verbose = false;
const int64_t MAX_TRACE_POINT = 86;
const int64_t MAX_COUNT_POINT = 85;
const char FULL_TRACE_STRING[] = "SERVER_OPERATION: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, HA_INDEX_INIT: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, HA_RND_INIT: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, HA_INDEX_READ_MAP: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, HA_SECONDARY_INDEX_READ: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, HA_READ_ROW: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, HA_CHECK: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, HA_CONVERT_FROM: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, HA_CONVERT_TO: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, HA_OPEN: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, HA_INDEX_NEXT: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, HA_RND_NEXT: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, HA_GET_FOR_UPDATE: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, DB_WRITE_BATCH_GET: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, DB_WRITE_BATCH_PUT: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, DB_WRITE_BATCH_DEL: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, DB_TRY_LOCK: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, DB_ITER_NEXT: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, DB_ITER_PREV: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, DB_ITER_NEXT_USER_ENTRY: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, META_SEARCH_LEVEL_BEGIN: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, META_SEARCH_LEVEL0: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, META_SEARCH_LEVEL1: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, META_SEARCH_LEVEL2: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, META_TO_TABLE_CACHE: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, MEMTABLE_GET: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, EXTENT_LAYER_GET: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, GET_IMPL: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, GET_LATEST_SEQ: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, GET_LATEST_UK_SEQ: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, VALIDATE_SNAPSHOT: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, GET_REF_SV: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, DB_ITER_CREATE: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, DB_ITER_REF_SV: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, DB_ITER_NEW_OBJECT: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, DB_ITER_ADD_MEM: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, DB_ITER_ADD_STORAGE: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, DB_APPROXIMATE_SIZE: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, DB_APPROXIMATE_MEM_SIZE: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, DB_ITER_SEEK: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, TABLE_PREFETCHER_SEEK: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, TABLE_PREFETCH_INDEX: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, SST_SCAN_ITER_SEEK: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, SST_SCAN_ITER_NEXT: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, EXTENT_ITERATOR_NEXT: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, INDEX_ITERATOR_NEXT: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, EXTENT_GET: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, LOAD_TABLE: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, LOAD_INDEX_BLOCK: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, DECOMPRESS_BLOCK: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, LOAD_DATA_BLOCK: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, DB_MUTEX_WAIT: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, WAL_FILE_SYNC: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, DB_SYNC_WAL: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, CDFW_FLUSH_ONE_IMM_BUFFER: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, MANIFEST_FILE_SYNC: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, TIME_PER_LOG_COPY: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, TIME_PER_LOG_WRITE: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, WRITE_MEMTABLE: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, COMMIT_JOB: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, WRITE_WAIT_LOCK: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, WRITE_RUN_IN_MUTEX: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, WRITE_DUMP_STATS: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, WRITE_ASYNC: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, WRITE_SYNC_WAIT: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, ENV_NEW_SEQ_FILE: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, ENV_NEW_RND_FILE: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, ENV_NEW_WRITE_FILE: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, ENV_REUSE_WRITE_FILE: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, ENV_NEW_RND_RW_FILE: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, ENV_NEW_DIR: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, ENV_FILE_EXISTS: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, ENV_GET_CHILDREN: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, ENV_GET_CHILDREN_ATTR: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, ENV_DELETE_FILE: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, ENV_CREATE_DIR: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, ENV_DELETE_DIR: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, ENV_GET_FILE_SIZE: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, ENV_GET_MODIFIED: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, ENV_RENAME: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, ENV_LINK_FILE: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, ENV_LOCK_FILE: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, ENV_UNLOCK_FILE: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, NEW_LOGGER: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, SYNC_RETURN_PACKAGE: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, ASYNC_RETURN_PACKAGE: 0xBEBEBEBEBEBEBEBE: 0xBEBEBEBEBEBEBEBE, USER_KEY_COMPARE: 0xBEBEBEBEBEBEBEBE, ENGINE_LOGICAL_READ: 0xBEBEBEBEBEBEBEBE, ENGINE_DISK_READ: 0xBEBEBEBEBEBEBEBE, NUMBER_OF_RESEEKS_IN_ITERATION: 0xBEBEBEBEBEBEBEBE, MEMTABLE_NEXT: 0xBEBEBEBEBEBEBEBE, MEMTABLE_PREV: 0xBEBEBEBEBEBEBEBE, INTERNAL_KEY_SKIPPED: 0xBEBEBEBEBEBEBEBE, INTERNAL_DEL_SKIPPED: 0xBEBEBEBEBEBEBEBE, INTERNAL_UPD_SKIPPED: 0xBEBEBEBEBEBEBEBE, SEARCH_LATEST_SEQ_IN_STORAGE: 0xBEBEBEBEBEBEBEBE, NUMBER_SUPERVERSION_ACQUIRES: 0xBEBEBEBEBEBEBEBE, NUMBER_SUPERVERSION_RELEASES: 0xBEBEBEBEBEBEBEBE, NUMBER_SUPERVERSION_CLEANUPS: 0xBEBEBEBEBEBEBEBE, NUMBER_KEYS_UPDATE: 0xBEBEBEBEBEBEBEBE, NUMBER_KEYS_WRITTEN: 0xBEBEBEBEBEBEBEBE, WAL_FILE_SYNCED: 0xBEBEBEBEBEBEBEBE, NUMBER_KEYS_READ: 0xBEBEBEBEBEBEBEBE, MEMTABLE_HIT: 0xBEBEBEBEBEBEBEBE, MEMTABLE_MISS: 0xBEBEBEBEBEBEBEBE, NUMBER_BYTES_READ: 0xBEBEBEBEBEBEBEBE, GET_UPDATES_SINCE_CALLS: 0xBEBEBEBEBEBEBEBE, ROW_CACHE_HIT: 0xBEBEBEBEBEBEBEBE, ROW_CACHE_MISS: 0xBEBEBEBEBEBEBEBE, ROW_CACHE_ADD: 0xBEBEBEBEBEBEBEBE, ROW_CACHE_EVICT: 0xBEBEBEBEBEBEBEBE, NUMBER_DIRECT_LOAD_TABLE_PROPERTIES: 0xBEBEBEBEBEBEBEBE, DB_MUTEX_WAIT: 0xBEBEBEBEBEBEBEBE, READ_AMP_TOTAL_READ_BYTES: 0xBEBEBEBEBEBEBEBE, READ_AMP_ESTIMATE_USEFUL_BYTES: 0xBEBEBEBEBEBEBEBE, NUMBER_FILE_OPENS: 0xBEBEBEBEBEBEBEBE, BLOCK_CACHE_ADD: 0xBEBEBEBEBEBEBEBE, BLOCK_CACHE_BYTES_WRITE: 0xBEBEBEBEBEBEBEBE, BLOCK_CACHE_INDEX_ADD: 0xBEBEBEBEBEBEBEBE, BLOCK_CACHE_INDEX_BYTES_INSERT: 0xBEBEBEBEBEBEBEBE, BLOCK_CACHE_INDEX_BYTES_EVICT: 0xBEBEBEBEBEBEBEBE, BLOCK_CACHE_DATA_ADD: 0xBEBEBEBEBEBEBEBE, BLOCK_CACHE_DATA_BYTES_INSERT: 0xBEBEBEBEBEBEBEBE, BLOCK_CACHE_FILTER_ADD: 0xBEBEBEBEBEBEBEBE, BLOCK_CACHE_FILTER_BYTES_INSERT: 0xBEBEBEBEBEBEBEBE, BLOCK_CACHE_FILTER_BYTES_EVICT: 0xBEBEBEBEBEBEBEBE, BLOCK_CACHE_ADD_FAILURES: 0xBEBEBEBEBEBEBEBE, BLOCK_CACHE_COMPRESSED_HIT: 0xBEBEBEBEBEBEBEBE, BLOCK_CACHE_COMPRESSED_MISS: 0xBEBEBEBEBEBEBEBE, BLOCK_CACHE_HIT: 0xBEBEBEBEBEBEBEBE, BLOCK_CACHE_BYTES_READ: 0xBEBEBEBEBEBEBEBE, BLOCK_CACHE_MISS: 0xBEBEBEBEBEBEBEBE, BLOCK_CACHE_INDEX_HIT: 0xBEBEBEBEBEBEBEBE, BLOCK_CACHE_INDEX_MISS: 0xBEBEBEBEBEBEBEBE, BLOCK_CACHE_DATA_HIT: 0xBEBEBEBEBEBEBEBE, BLOCK_CACHE_DATA_MISS: 0xBEBEBEBEBEBEBEBE, BLOCK_CACHE_FILTER_HIT: 0xBEBEBEBEBEBEBEBE, BLOCK_CACHE_FILTER_MISS: 0xBEBEBEBEBEBEBEBE, BLOCK_CACHE_COMPRESSED_ADD: 0xBEBEBEBEBEBEBEBE, BLOCK_CACHE_COMPRESSED_ADD_FAILURES: 0xBEBEBEBEBEBEBEBE, BLOOM_FILTER_PREFIX_CHECKED: 0xBEBEBEBEBEBEBEBE, BLOOM_FILTER_PREFIX_USEFUL: 0xBEBEBEBEBEBEBEBE, BLOOM_FILTER_USEFUL: 0xBEBEBEBEBEBEBEBE, NUMBER_BLOCK_NOT_COMPRESSED: 0xBEBEBEBEBEBEBEBE, NUMBER_BLOCK_COMPRESSED: 0xBEBEBEBEBEBEBEBE, BYTES_COMPRESSED: 0xBEBEBEBEBEBEBEBE, NUMBER_BLOCK_DECOMPRESSED: 0xBEBEBEBEBEBEBEBE, BYTES_DECOMPRESSED: 0xBEBEBEBEBEBEBEBE, PERSISTENT_CACHE_HIT: 0xBEBEBEBEBEBEBEBE, PERSISTENT_CACHE_MISS: 0xBEBEBEBEBEBEBEBE, NUMBER_RATE_LIMITER_DRAINS: 0xBEBEBEBEBEBEBEBE, WRITE_DONE_BY_OTHER: 0xBEBEBEBEBEBEBEBE, WRITE_WITH_WAL: 0xBEBEBEBEBEBEBEBE, WAL_FILE_BYTES: 0xBEBEBEBEBEBEBEBE, WRITE_DONE_BY_SELF: 0xBEBEBEBEBEBEBEBE, BYTES_PER_WRITE: 0xBEBEBEBEBEBEBEBE, PIPLINE_GROUP_SIZE: 0xBEBEBEBEBEBEBEBE, PIPLINE_CONCURRENT_RUNNING_WORKER_THERADS: 0xBEBEBEBEBEBEBEBE, PIPLINE_LOOP_COUNT: 0xBEBEBEBEBEBEBEBE, PIPLINE_LOG_QUEUE_LENGTH: 0xBEBEBEBEBEBEBEBE, PIPLINE_MEM_QUEUE_LENGTH: 0xBEBEBEBEBEBEBEBE, PIPLINE_COMMIT_QUEUE_LENGTH: 0xBEBEBEBEBEBEBEBE, BYTES_PER_LOG_COPY: 0xBEBEBEBEBEBEBEBE, ENTRY_PER_LOG_COPY: 0xBEBEBEBEBEBEBEBE, BYTES_PER_LOG_WRITE: 0xBEBEBEBEBEBEBEBE, BLOOM_MEMTABLE_HIT: 0xBEBEBEBEBEBEBEBE, BLOOM_MEMTABLE_MISS: 0xBEBEBEBEBEBEBEBE, BLOOM_SST_HIT: 0xBEBEBEBEBEBEBEBE, BLOOM_SST_MISS: 0xBEBEBEBEBEBEBEBE, DB_ITER_STORAGE_LAYER: 0xBEBEBEBEBEBEBEBE, AIO_REQUEST_SUBMIT: 0xBEBEBEBEBEBEBEBE,\n";

using namespace xengine;
using namespace common;
using namespace util;
using namespace table;
using namespace cache;
using namespace memtable;
using namespace monitor;
using namespace storage;

// Path to the database on file system
const std::string kDbName = ::test::TmpDir() + "/perf_context_test";
namespace xengine {
namespace db {

uint64_t TestGetThreadCount(monitor::TracePoint point) {
  return monitor::get_tls_query_perf_context()->get_count(point);
}

uint64_t TestGetThreadCount(monitor::CountPoint point) {
  return monitor::get_tls_query_perf_context()->get_count(point);
}

uint64_t TestGetThreadCosts(monitor::TracePoint point) {
  return monitor::get_tls_query_perf_context()->get_costs(point);
}

DB* OpenDb(bool read_only = false) {
  DB* db;
  Options options;
  options.create_if_missing = true;
  options.max_open_files = -1;
  options.write_buffer_size = FLAGS_write_buffer_size;
  options.max_write_buffer_number = FLAGS_max_write_buffer_number;
  options.min_write_buffer_number_to_merge =
      FLAGS_min_write_buffer_number_to_merge;

  if (FLAGS_use_set_based_memetable) {
#ifndef ROCKSDB_LITE
    options.prefix_extractor.reset(NewFixedPrefixTransform(0));
    options.memtable_factory.reset(NewHashSkipListRepFactory());
#endif  // ROCKSDB_LITE
  }

  Status s;
  if (!read_only) {
    s = DB::Open(options, kDbName, &db);
  } else {
    s = DB::OpenForReadOnly(options, kDbName, &db);
  }
  EXPECT_OK(s);
  return db;
}

class PerfContextTest : public testing::Test {
public:
  PerfContextTest() {
    QueryPerfContext::opt_enable_count_ = true;
    QueryPerfContext::opt_print_slow_ = true;
  }
};

TEST_F(PerfContextTest, EmptyOutputTest) {
  QueryPerfContext *ctx = QueryPerfContext::new_query_context();
  ctx->reset();
  const char *data = nullptr;
  int64_t size = 0;
  ctx->to_string(0 /*total_time*/, data, size);
  ASSERT_EQ(size, 1);
  ASSERT_EQ(strcmp(data, "\n"), 0);
  delete ctx;
}

TEST_F(PerfContextTest, FullOutputTest) {
  QueryPerfContext *ctx = QueryPerfContext::new_query_context();
  ctx->reset();
  memset((char*)ctx + 8, 0xBE, MAX_TRACE_POINT * 16 + MAX_COUNT_POINT * 8);
  const char *data = nullptr;
  int64_t size = 0;
  ctx->to_string(0 /*total_time*/, data, size);
  ASSERT_EQ(sizeof(FULL_TRACE_STRING)-1, size);
  ASSERT_EQ(0, strcmp(data, FULL_TRACE_STRING));
  delete ctx;
}

TEST_F(PerfContextTest, QueryPerfContextTest) {
  QueryPerfContext *ctx = QueryPerfContext::new_query_context();
  ctx->reset();
  ctx->trace(TracePoint::SERVER_OPERATION);
  ctx->trace(TracePoint::LOAD_TABLE);
  for (size_t i = 0; i != 10; ++i) {
    ctx->trace(TracePoint::META_SEARCH_LEVEL0);
    ctx->trace(TracePoint::META_SEARCH_LEVEL1);
  }
  const char *data = nullptr;
  int64_t size = 0;
  get_tls_query_perf_context()->to_string(0 /*total_time*/,data, size);
  fprintf(stderr, "ctx str:\n%s\n", data);
  delete ctx;
}

TEST_F(PerfContextTest, ThreadPerfContextTest) {
  std::vector<std::thread> threads;
  threads.reserve(10);
  for (size_t thread_num = 0; thread_num < 10; ++thread_num) {
    threads.push_back(std::move(std::thread([]() {
      QUERY_TRACE_RESET();
      QUERY_TRACE_BEGIN(TracePoint::SERVER_OPERATION);
      QUERY_TRACE_END();
      QUERY_TRACE_BEGIN(TracePoint::LOAD_TABLE);
      QUERY_TRACE_END();
      for (size_t i = 0; i != 10; ++i) {
        QUERY_TRACE_BEGIN(TracePoint::META_SEARCH_LEVEL0);
        QUERY_TRACE_END();
        QUERY_TRACE_BEGIN(TracePoint::META_SEARCH_LEVEL1);
        QUERY_TRACE_END();
      }
      const char *data = nullptr;
      int64_t size = 0;
      get_tls_query_perf_context()->to_string(0 /*total_time*/,data, size);
      fprintf(stderr, "ctx str:\n%s\n", data);
    })));
  }
  for (auto &t : threads) {
    t.join();
  }
}

TEST_F(PerfContextTest, ScopeContextTest) {
  std::vector<std::thread> threads;
  threads.reserve(10);
  for (size_t thread_num = 0; thread_num < 10; ++thread_num) {
    threads.push_back(std::move(std::thread([]() {
      {
        QUERY_TRACE_RESET();
        QUERY_TRACE_SCOPE(TracePoint::HA_INDEX_INIT);
        sleep(3);
        QUERY_TRACE_BEGIN(TracePoint::SERVER_OPERATION);
        sleep(2);
        QUERY_TRACE_END();
        QUERY_TRACE_BEGIN(TracePoint::LOAD_TABLE);
        sleep(1);
        QUERY_TRACE_END();
        for (size_t i = 0; i != 10; ++i) {
          QUERY_TRACE_SCOPE(TracePoint::GET_IMPL);
          usleep(30000);
          QUERY_TRACE_BEGIN(TracePoint::META_SEARCH_LEVEL0);
          usleep(20000);
          QUERY_TRACE_BEGIN(TracePoint::META_SEARCH_LEVEL1);
          usleep(10000);
          QUERY_TRACE_END();
          usleep(10000);
          QUERY_TRACE_END();
          usleep(10000);
        }
        sleep(1);
      }
      const char *data = nullptr;
      int64_t size = 0;
      get_tls_query_perf_context()->to_string(0 /*total_time*/, data, size);
      fprintf(stderr, "ctx str:\n%s\n", data);
    })));
  }
  for (auto &t : threads) {
    t.join();
  }
}

TEST_F(PerfContextTest, ScopeContextTest2) {
  QUERY_TRACE_RESET();
  {
    QUERY_TRACE_SCOPE(TracePoint::HA_INDEX_INIT);
    sleep(3);
    QUERY_TRACE_BEGIN(TracePoint::SERVER_OPERATION);
    sleep(2);
    QUERY_TRACE_END();
    QUERY_TRACE_BEGIN(TracePoint::LOAD_TABLE);
    sleep(1);
    QUERY_TRACE_END();
    for (size_t i = 0; i != 3; ++i) {
      QUERY_TRACE_SCOPE(TracePoint::GET_IMPL);
      usleep(300000);
      QUERY_TRACE_BEGIN(TracePoint::META_SEARCH_LEVEL0);
      usleep(200000);
      QUERY_TRACE_BEGIN(TracePoint::META_SEARCH_LEVEL1);
      usleep(100000);
      QUERY_TRACE_END();
      usleep(50000);
      QUERY_TRACE_END();
      usleep(50000);
    }
    sleep(1);
  }
  const char *data = nullptr;
  int64_t size = 0;
  get_tls_query_perf_context()->to_string(0 /*total_time*/, data, size);
  fprintf(stderr, "ctx str:\n%s\n", data);
}

TEST_F(PerfContextTest, CountShardTest) {
  std::vector<std::thread> threads;
  threads.reserve(10);
  for (size_t thread_num = 0; thread_num < 10; ++thread_num) {
    threads.push_back(std::move(std::thread([]() {
      {
        QUERY_TRACE_RESET();
        for (int64_t shard = 0; shard != 256; ++shard) {
          QUERY_COUNT_SHARD(CountPoint::BLOCK_CACHE_ADD, shard);
        }
        QUERY_TRACE_SCOPE(TracePoint::HA_INDEX_INIT);
        QUERY_TRACE_BEGIN(TracePoint::SERVER_OPERATION);
        QUERY_TRACE_END();
        for (int64_t shard = 0; shard != 256; ++shard) {
          QUERY_COUNT_SHARD(CountPoint::BLOCK_CACHE_INDEX_HIT, shard);
        }
        QUERY_TRACE_BEGIN(TracePoint::LOAD_TABLE);
        QUERY_TRACE_END();
        for (size_t i = 0; i != 10; ++i) {
          QUERY_TRACE_SCOPE(TracePoint::GET_IMPL);
          QUERY_TRACE_BEGIN(TracePoint::META_SEARCH_LEVEL0);
          for (int64_t shard = 0; shard != 256; ++shard) {
            QUERY_COUNT_SHARD(CountPoint::BLOCK_CACHE_DATA_MISS, shard);
          }
          QUERY_TRACE_BEGIN(TracePoint::META_SEARCH_LEVEL1);
          QUERY_TRACE_END();
          QUERY_TRACE_END();
        }
      }
      const char *data = nullptr;
      int64_t size = 0;
      get_tls_query_perf_context()->to_string(0 /*total time*/, data, size);
      fprintf(stderr, "ctx str:\n%s\n", data);
    })));
  }
  for (auto &t : threads) {
    t.join();
  }
}

TEST_F(PerfContextTest, TraceGuardTest) {
  QUERY_TRACE_RESET();
  {
    QUERY_TRACE_SCOPE(TracePoint::LOAD_TABLE);
  }
  const char *data = nullptr;
  int64_t size = 0;
  get_tls_query_perf_context()->to_string(0 /*total_time*/, data, size);
  fprintf(stderr, "ctx str:\n%s\n", data);
}

TEST_F(PerfContextTest, GetPerfTest) {
  DestroyDB(kDbName, Options());
  auto db = OpenDb();
  WriteOptions write_options;
  ReadOptions read_options;
  QUERY_TRACE_RESET();
  std::string key = "kkkkkkkkkkkkkkkkkkkkkkk";
  std::string value = "vvvvvvvvvvvvvvvvvvvvvvvv";
  std::string get_value;
  db->Put(write_options, key, value);
  db->Get(read_options, key, &get_value);
  EXPECT_EQ(value, get_value);
  FlushOptions fo;
  fo.wait = true;
  ASSERT_OK(db->Flush(fo));
  db->Get(read_options, key, &get_value);
  EXPECT_EQ(value, get_value);
  const char *data = nullptr;
  int64_t size = 0;
  get_tls_query_perf_context()->to_string(0 /*total_time*/, data, size);
  fprintf(stderr, "ctx str:\n%s\n", data);
  MOD_DELETE_OBJECT(DB, db);
}

TEST_F(PerfContextTest, ScanPerfTest) {
  DestroyDB(kDbName, Options());
  auto db = OpenDb();
  std::unique_ptr<DB, memory::ptr_destruct_delete<DB>>db_ptr(db);
  WriteOptions write_options;
  ReadOptions read_options;
  QUERY_TRACE_RESET();

  std::string get_value;
  int total_keys = 100;
  int i = 0;
  for (; i < total_keys; ++i) {
    std::string key = "k" + ToString(i);
    std::string value = "v" + ToString(i);
    db->Put(write_options, key, value);
  }

  FlushOptions fo;
  fo.wait = true;
  ASSERT_OK(db->Flush(fo));

  for (; i < total_keys * 2; ++i) {
    std::string key = "k" + ToString(i);
    std::string value = "v" + ToString(i);
    db->Put(write_options, key, value);
  }
  for (i = 0; i != total_keys * 2; ++i) {
    std::string key = "k" + ToString(i);
    std::string value = "v" + ToString(i);
    db->Get(read_options, key, &get_value);
    EXPECT_EQ(value, get_value);
  }
  std::unique_ptr<Iterator, memory::ptr_destruct_delete<Iterator>> iter {db->NewIterator(read_options)};
  i = 0;
  for (iter->SeekToFirst(); iter->Valid(); iter->Next(), ++i);
  const char *data = nullptr;
  int64_t size = 0;
  get_tls_query_perf_context()->to_string(0 /*total_time*/, data, size);
  fprintf(stderr, "ctx str:\n%s\n", data);
}

TEST_F(PerfContextTest, SeekIntoDeletion) {
  DestroyDB(kDbName, Options());
  auto db = OpenDb();
  std::unique_ptr<DB, memory::ptr_destruct_delete<DB>>db_ptr(db);
  WriteOptions write_options;
  ReadOptions read_options;

  for (int i = 0; i < FLAGS_total_keys; ++i) {
    std::string key = "k" + ToString(i);
    std::string value = "v" + ToString(i);

    db->Put(write_options, key, value);
  }

  for (int i = 0; i < FLAGS_total_keys - 1; ++i) {
    std::string key = "k" + ToString(i);
    db->Delete(write_options, key);
  }

  HistogramImpl hist_get;
  HistogramImpl hist_get_time;
  for (int i = 0; i < FLAGS_total_keys - 1; ++i) {
    std::string key = "k" + ToString(i);
    std::string value;

    QUERY_TRACE_RESET();
    StopWatchNano timer(Env::Default());
    timer.Start();
    auto status = db->Get(read_options, key, &value);
    auto elapsed_nanos = timer.ElapsedNanos();
    ASSERT_TRUE(status.IsNotFound());
    hist_get.Add(TestGetThreadCount(CountPoint::USER_KEY_COMPARE));
    hist_get_time.Add(elapsed_nanos);
  }

  if (FLAGS_verbose) {
    std::cout << "Get user key comparison: \n"
              << hist_get.ToString() << "Get time: \n"
              << hist_get_time.ToString();
  }

  {
    HistogramImpl hist_seek_to_first;
    std::unique_ptr<Iterator, memory::ptr_destruct_delete<Iterator>> iter(db->NewIterator(read_options));

    QUERY_TRACE_RESET();
    StopWatchNano timer(Env::Default(), true);
    iter->SeekToFirst();
    hist_seek_to_first.Add(TestGetThreadCount(CountPoint::USER_KEY_COMPARE));
    auto elapsed_nanos = timer.ElapsedNanos();

    if (FLAGS_verbose) {
      std::cout << "SeekToFirst uesr key comparison: \n"
                << hist_seek_to_first.ToString()
                << "ikey skipped: "
                << TestGetThreadCount(CountPoint::INTERNAL_KEY_SKIPPED)
                << "\n"
                << "idelete skipped: "
                << TestGetThreadCount(CountPoint::INTERNAL_DEL_SKIPPED) << "\n"
                << "elapsed: " << elapsed_nanos << "\n";
    }
  }

  HistogramImpl hist_seek;
  for (int i = 0; i < FLAGS_total_keys; ++i) {
    std::unique_ptr<Iterator, memory::ptr_destruct_delete<Iterator>> iter(db->NewIterator(read_options));
    std::string key = "k" + ToString(i);

    QUERY_TRACE_RESET();
    StopWatchNano timer(Env::Default(), true);
    iter->Seek(key);
    auto elapsed_nanos = timer.ElapsedNanos();
    hist_seek.Add(TestGetThreadCount(CountPoint::USER_KEY_COMPARE));
    if (FLAGS_verbose) {
      std::cout << "seek cmp: "
                << TestGetThreadCount(CountPoint::USER_KEY_COMPARE)
                << " ikey skipped "
                << TestGetThreadCount(CountPoint::INTERNAL_KEY_SKIPPED)
                << " idelete skipped "
                << TestGetThreadCount(CountPoint::INTERNAL_DEL_SKIPPED)
                << " elapsed: " << elapsed_nanos << "ns\n";
    }

    QUERY_TRACE_RESET();
    ASSERT_TRUE(iter->Valid());
    StopWatchNano timer2(Env::Default(), true);
    iter->Next();
    auto elapsed_nanos2 = timer2.ElapsedNanos();
    if (FLAGS_verbose) {
      std::cout << "next cmp: "
                << TestGetThreadCount(CountPoint::USER_KEY_COMPARE)
                << "elapsed: " << elapsed_nanos2 << "ns\n";
    }
  }

  if (FLAGS_verbose) {
    std::cout << "Seek uesr key comparison: \n" << hist_seek.ToString();
  }
}

TEST_F(PerfContextTest, StopWatchNanoOverhead) {
  // profile the timer cost by itself!
  const int kTotalIterations = 1000000;
  std::vector<uint64_t> timings(kTotalIterations);

  StopWatchNano timer(Env::Default(), true);
  for (auto& timing : timings) {
    timing = timer.ElapsedNanos(true /* reset */);
  }

  HistogramImpl histogram;
  for (const auto timing : timings) {
    histogram.Add(timing);
  }

  if (FLAGS_verbose) {
    std::cout << histogram.ToString();
  }
}

TEST_F(PerfContextTest, StopWatchOverhead) {
  // profile the timer cost by itself!
  const int kTotalIterations = 1000000;
  uint64_t elapsed = 0;
  std::vector<uint64_t> timings(kTotalIterations);

  HistogramImpl histogram;
  uint64_t prev_timing = 0;
  for (const auto timing : timings) {
    histogram.Add(timing - prev_timing);
    prev_timing = timing;
  }

  if (FLAGS_verbose) {
    std::cout << histogram.ToString();
  }
}

// make perf_context_test
// export ROCKSDB_TESTS=PerfContextTest.SeekKeyComparison
// For one memtable:
// ./perf_context_test --write_buffer_size=500000 --total_keys=10000
// For two memtables:
// ./perf_context_test --write_buffer_size=250000 --total_keys=10000
// Specify --random_key=1 to shuffle the key before insertion
// Results show that, for sequential insertion, worst-case Seek Key comparison
// is close to the total number of keys (linear), when there is only one
// memtable. When there are two memtables, even the avg Seek Key comparison
// starts to become linear to the input size.

TEST_F(PerfContextTest, SeekKeyComparison) {
  DestroyDB(kDbName, Options());
  auto db = OpenDb();
  std::unique_ptr<DB, memory::ptr_destruct_delete<DB>>db_ptr(db);
  WriteOptions write_options;
  ReadOptions read_options;

  if (FLAGS_verbose) {
    std::cout << "Inserting " << FLAGS_total_keys << " key/value pairs\n...\n";
  }

  std::vector<int> keys;
  for (int i = 0; i < FLAGS_total_keys; ++i) {
    keys.push_back(i);
  }

  if (FLAGS_random_key) {
    std::random_shuffle(keys.begin(), keys.end());
  }

  HistogramImpl hist_put_time;
  HistogramImpl hist_wal_time;
  HistogramImpl hist_time_diff;

  StopWatchNano timer(Env::Default());
  for (const int i : keys) {
    std::string key = "k" + ToString(i);
    std::string value = "v" + ToString(i);

    QUERY_TRACE_RESET();
    timer.Start();
    db->Put(write_options, key, value);
    auto put_time = timer.ElapsedNanos();
    hist_put_time.Add(put_time);
    uint64_t time = TestGetThreadCosts(TracePoint::TIME_PER_LOG_WRITE);
    hist_wal_time.Add(time);
    hist_time_diff.Add(put_time - time);
  }

  if (FLAGS_verbose) {
    std::cout << "Put time:\n"
              << hist_put_time.ToString() << "WAL time:\n"
              << hist_wal_time.ToString() << "time diff:\n"
              << hist_time_diff.ToString();
  }

  HistogramImpl hist_seek;
  HistogramImpl hist_next;

  for (int i = 0; i < FLAGS_total_keys; ++i) {
    std::string key = "k" + ToString(i);
    std::string value = "v" + ToString(i);

    std::unique_ptr<Iterator, memory::ptr_destruct_delete<Iterator>> iter(db->NewIterator(read_options));
    QUERY_TRACE_RESET();
    iter->Seek(key);
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(iter->value().ToString(), value);
    hist_seek.Add(TestGetThreadCount(TracePoint::DB_ITER_SEEK));
  }

  std::unique_ptr<Iterator, memory::ptr_destruct_delete<Iterator>> iter(db->NewIterator(read_options));
  for (iter->SeekToFirst(); iter->Valid();) {
    QUERY_TRACE_RESET();
    iter->Next();
    hist_next.Add(TestGetThreadCount(TracePoint::DB_ITER_NEXT));
  }

  if (FLAGS_verbose) {
    std::cout << "Seek:\n"
              << hist_seek.ToString() << "Next:\n"
              << hist_next.ToString();
  }
}

TEST_F(PerfContextTest, DBMutexLockCounter) {
  DestroyDB(kDbName, Options());
  auto db = OpenDb();
  std::unique_ptr<DB, memory::ptr_destruct_delete<DB>>db_ptr(db);
  WriteOptions write_options;
  ReadOptions read_options;
  for (int c = 0; c < 2; ++c) {
    port::Thread child_thread([&] {
      QUERY_TRACE_RESET();
      ASSERT_EQ(TestGetThreadCosts(TracePoint::WRITE_RUN_IN_MUTEX), 0);
      db->Put(write_options, "some_key", "some_val");
      // increment the counter only when it's a DB Mutex
      ASSERT_GT(TestGetThreadCosts(TracePoint::WRITE_RUN_IN_MUTEX), 0);
    });
    Env::Default()->SleepForMicroseconds(100);
    child_thread.join();
  }
}

TEST_F(PerfContextTest, TraceOutputTest) {
  xengine::monitor::get_tls_query_perf_context()->clear_stats();
  uint64_t time[MAX_TRACE_POINT];
  int64_t count[MAX_TRACE_POINT];
  memset(time, 0x00, sizeof(time[0]) * MAX_TRACE_POINT);
  memset(count, 0x00, sizeof(count[0]) * MAX_TRACE_POINT);
  xengine::monitor::get_tls_query_perf_context()->get_global_trace_info(time, count);
  ASSERT_EQ(time[static_cast<int64_t>(TracePoint::SERVER_OPERATION)], 0);
  ASSERT_EQ(time[static_cast<int64_t>(TracePoint::LOAD_TABLE)], 0);
  ASSERT_EQ(time[static_cast<int64_t>(TracePoint::META_SEARCH_LEVEL0)], 0);
  ASSERT_EQ(time[static_cast<int64_t>(TracePoint::META_SEARCH_LEVEL1)], 0);

  xengine::monitor::QueryPerfContext::opt_trace_sum_ = true;
  std::vector<std::thread> threads;
  threads.reserve(10);
  for (size_t thread_num = 0; thread_num < 10; ++thread_num) {
    threads.push_back(std::move(std::thread([]() {
      QUERY_TRACE_RESET();
      QUERY_TRACE_BEGIN(TracePoint::SERVER_OPERATION);
      QUERY_TRACE_END();
      QUERY_TRACE_BEGIN(TracePoint::LOAD_TABLE);
      QUERY_TRACE_END();
      for (size_t i = 0; i != 10; ++i) {
        QUERY_TRACE_BEGIN(TracePoint::META_SEARCH_LEVEL0);
        QUERY_TRACE_END();
        QUERY_TRACE_BEGIN(TracePoint::META_SEARCH_LEVEL1);
        QUERY_TRACE_END();
      }
    })));
  }
  for (auto &t : threads) {
    t.join();
  }

  memset(time, 0x00, sizeof(time[0]) * MAX_TRACE_POINT);
  memset(count, 0x00, sizeof(count[0]) * MAX_TRACE_POINT);
  xengine::monitor::get_tls_query_perf_context()->get_global_trace_info(time, count);
  ASSERT_GT(time[static_cast<int64_t>(TracePoint::SERVER_OPERATION)], 0);
  ASSERT_GT(time[static_cast<int64_t>(TracePoint::LOAD_TABLE)], 0);
  ASSERT_GT(time[static_cast<int64_t>(TracePoint::META_SEARCH_LEVEL0)], 0);
  ASSERT_GT(time[static_cast<int64_t>(TracePoint::META_SEARCH_LEVEL1)], 0);
  xengine::monitor::QueryPerfContext::opt_trace_sum_ = false;
}

TEST_F(PerfContextTest, DISABLED_MergeOperatorTime) {
  DestroyDB(kDbName, Options());
  DB* db;
  Options options;
  options.create_if_missing = true;
  options.merge_operator = MergeOperators::CreateStringAppendOperator();
  Status s = DB::Open(options, kDbName, &db);
  std::unique_ptr<DB, memory::ptr_destruct_delete<DB>>db_ptr(db);
  EXPECT_OK(s);

  std::string val;
  ASSERT_OK(db->Merge(WriteOptions(), "k1", "val1"));
  ASSERT_OK(db->Merge(WriteOptions(), "k1", "val2"));
  ASSERT_OK(db->Merge(WriteOptions(), "k1", "val3"));
  ASSERT_OK(db->Merge(WriteOptions(), "k1", "val4"));

  ASSERT_OK(db->Get(ReadOptions(), "k1", &val));
  // TODO @zhencheng : use QUERY TRACE if merge is used again.
  // EXPECT_GT(perf_context.merge_operator_time_nanos, 0);

  ASSERT_OK(db->Flush(FlushOptions()));

  ASSERT_OK(db->Get(ReadOptions(), "k1", &val));
  // TODO @zhencheng : use QUERY TRACE if merge is used again.
  // EXPECT_GT(perf_context.merge_operator_time_nanos, 0);

  ASSERT_OK(db->CompactRange(CompactRangeOptions(), nullptr, nullptr));

  ASSERT_OK(db->Get(ReadOptions(), "k1", &val));
  // TODO @zhencheng : use QUERY TRACE if merge is used again.
  // EXPECT_GT(perf_context.merge_operator_time_nanos, 0);
}
}
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
	xengine::util::test::init_logger(__FILE__);

  for (int i = 1; i < argc; i++) {
    int n;
    char junk;

    if (sscanf(argv[i], "--write_buffer_size=%d%c", &n, &junk) == 1) {
      FLAGS_write_buffer_size = n;
    }

    if (sscanf(argv[i], "--total_keys=%d%c", &n, &junk) == 1) {
      FLAGS_total_keys = n;
    }

    if (sscanf(argv[i], "--random_key=%d%c", &n, &junk) == 1 &&
        (n == 0 || n == 1)) {
      FLAGS_random_key = n;
    }

    if (sscanf(argv[i], "--use_set_based_memetable=%d%c", &n, &junk) == 1 &&
        (n == 0 || n == 1)) {
      FLAGS_use_set_based_memetable = n;
    }

    if (sscanf(argv[i], "--verbose=%d%c", &n, &junk) == 1 &&
        (n == 0 || n == 1)) {
      FLAGS_verbose = n;
    }
  }

  if (FLAGS_verbose) {
    std::cout << kDbName << "\n";
  }

  return RUN_ALL_TESTS();
}
