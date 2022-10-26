// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include "db/db_impl.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <inttypes.h>
#include <stdint.h>
#ifdef OS_SOLARIS
#include <alloca.h>
#endif
#ifdef ROCKSDB_JEMALLOC
#include "jemalloc/jemalloc.h"
#endif

#include <algorithm>
#include <climits>
#include <cstdio>
#include <map>
#include <set>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "cache/row_cache.h"
#include "cache/sharded_cache.h"
#include "compact/compaction_job.h"
#include "db/builder.h"
#include "db/db_info_dumper.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/event_helpers.h"
#include "db/flush_job.h"
#include "db/forward_iterator.h"
#include "db/job_context.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/managed_iterator.h"
#include "db/memtable.h"
#include "db/memtable_list.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/range_del_aggregator.h"
#include "db/table_cache.h"
#include "db/table_cache.h"
#include "db/table_properties_collector.h"
#include "db/transaction_log_impl.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "db/write_callback.h"
#include "logger/logger.h"
#include "memtable/hash_linklist_rep.h"
#include "memtable/hash_skiplist_rep.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/query_perf_context.h"
#include "monitoring/thread_status_updater.h"
#include "monitoring/thread_status_util.h"
#include "options/cf_options.h"
#include "options/options_helper.h"
#include "options/options_parser.h"
#include "port/likely.h"
#include "port/port.h"
#include "storage/extent_space_manager.h"
#include "storage/io_extent.h"
#include "table/block.h"
#include "table/block_based_table_factory.h"
#include "table/extent_table_factory.h"
#include "table/filter_manager.h"
#include "table/merging_iterator.h"
#include "table/table_builder.h"
#include "table/two_level_iterator.h"
#include "memory/alloc_mgr.h"
#include "util/autovector.h"
#include "util/build_version.h"
#include "util/coding.h"
#include "util/coding.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/file_reader_writer.h"
#include "util/file_util.h"
#include "util/filename.h"
#include "util/log_buffer.h"
#include "util/memory_stat.h"
#include "util/memory_stat.h"
#include "util/mutexlock.h"
#include "util/sst_file_manager_impl.h"
#include "util/stop_watch.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "xengine/cache.h"
#include "xengine/compaction_filter.h"
#include "xengine/db.h"
#include "xengine/env.h"
#include "xengine/merge_operator.h"
#include "xengine/statistics.h"
#include "xengine/status.h"
#include "xengine/table.h"
#include "xengine/version.h"
#include "xengine/write_buffer_manager.h"
#include "storage/storage_logger.h"

using namespace xengine;
using namespace common;
using namespace util;
using namespace monitor;
using namespace cache;
using namespace table;
using namespace storage;
using namespace memory;

namespace xengine {
namespace db {

#ifdef WITH_STRESS_CHECK
thread_local std::unordered_map<std::string, std::string> *STRESS_CHECK_RECORDS =
    new std::unordered_map<std::string, std::string>();
#endif

const std::string kDefaultColumnFamilyName("default");
void DumpXEngineBuildVersion();

CompressionType GetCompressionFlush(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options,
    const int64_t level) {
  assert( level >= 0);
  // Compressing memtable flushes might not help unless the sequential load
  // optimization is used for leveled compaction. Otherwise the CPU and
  // latency overhead is not offset by saving much space.
  if (ioptions.compaction_style == kCompactionStyleUniversal) {
    if (ioptions.compaction_options_universal.compression_size_percent < 0) {
      return mutable_cf_options.compression;
    } else {
      return kNoCompression;
    }
  } else if ( static_cast<uint64_t>(level) < ioptions.compression_per_level.size()) {
    // For leveled compress when min_level_to_compress != 0.
    return ioptions.compression_per_level[level];
  } else {
    return mutable_cf_options.compression;
  }
}

namespace {
void DumpSupportInfo() {
  __XENGINE_LOG(INFO, "Compression algorithms supported:");
  __XENGINE_LOG(INFO, "\tSnappy supported: %d", Snappy_Supported());
  __XENGINE_LOG(INFO, "\tZlib supported: %d", Zlib_Supported());
  __XENGINE_LOG(INFO, "\tBzip supported: %d", BZip2_Supported());
  __XENGINE_LOG(INFO, "\tLZ4 supported: %d", LZ4_Supported());
  __XENGINE_LOG(INFO, "\tZSTD supported: %d", ZSTD_Supported());
  __XENGINE_LOG(INFO, "Fast CRC32 supported: %d",
                   crc32c::IsFastCrc32Supported());
}

template <class T>
static void free_entry(const Slice& key, void* value) {
  T* typed_value = reinterpret_cast<T*>(value);
  if (nullptr != typed_value) {
    typed_value->~T();
    base_free(typed_value);
  }
}
}  // namespace

void all_sub_table_unref_handle(void *ptr)
{
  AllSubTable *all_sub_table = static_cast<AllSubTable *>(ptr);
  //there may be deadlock between ThreadExit and Scrape.
  //refs is atomic variable, and all thread will reference newer version;
  //it's still safe to free memory,without all_sub_table_mutex_.
  //std::lock_guard<std::mutex> guard(*all_sub_table->all_sub_table_mutex_);
  if (all_sub_table->unref()) {
    MOD_DELETE_OBJECT(AllSubTable, all_sub_table);
  }
}

GlobalContext::GlobalContext()
    : db_name_(),
      options_(),
      env_options_(),
      env_(nullptr),
      cache_(nullptr),
      write_buf_mgr_(nullptr),
      storage_logger_(nullptr),
      extent_space_mgr_(nullptr),
      all_sub_table_mutex_(),
      version_number_(0),
      local_all_sub_table_(nullptr),
      all_sub_table_(nullptr),
      db_dir_(nullptr)
{
  local_all_sub_table_.reset(MOD_NEW_OBJECT(ModId::kAllSubTable, ThreadLocalPtr, &all_sub_table_unref_handle));
  all_sub_table_ = MOD_NEW_OBJECT(ModId::kAllSubTable, AllSubTable);
  all_sub_table_->ref();
  all_sub_table_->all_sub_table_mutex_ = &all_sub_table_mutex_;
}

GlobalContext::GlobalContext(const std::string &db_name, common::Options &options)
    : db_name_(db_name),
      options_(options),
      env_options_(),
      env_(nullptr),
      cache_(nullptr),
      write_buf_mgr_(nullptr),
      storage_logger_(nullptr),
      extent_space_mgr_(nullptr),
      all_sub_table_mutex_(),
      version_number_(0),
      local_all_sub_table_(nullptr),
      all_sub_table_(nullptr),
      db_dir_(nullptr)
{
  local_all_sub_table_.reset(MOD_NEW_OBJECT(ModId::kAllSubTable, ThreadLocalPtr, &all_sub_table_unref_handle));
  all_sub_table_ = MOD_NEW_OBJECT(ModId::kAllSubTable, AllSubTable);
  all_sub_table_->ref();
  all_sub_table_->all_sub_table_mutex_ = &all_sub_table_mutex_;
}

GlobalContext::~GlobalContext()
{
  std::lock_guard<std::mutex> guard(*all_sub_table_->all_sub_table_mutex_);
  if (all_sub_table_->unref()) {
    MOD_DELETE_OBJECT(AllSubTable, all_sub_table_);
  }
}

bool GlobalContext::is_valid()
{
  return nullptr != cache_ && nullptr != write_buf_mgr_ && nullptr != storage_logger_ && nullptr != extent_space_mgr_;
}

void GlobalContext::reset()
{
  env_ = nullptr;
  cache_ = nullptr;
  write_buf_mgr_ = nullptr;
  storage_logger_ = nullptr;
  extent_space_mgr_ = nullptr;
  all_sub_table_->reset();
  db_dir_ = nullptr;
}

int GlobalContext::acquire_thread_local_all_sub_table(AllSubTable *&all_sub_table)
{
  int ret = Status::kOk;
  AllSubTable *all_sub_table_to_delete = nullptr;
  if (AllSubTable::kAllSubtableInUse == (all_sub_table = static_cast<AllSubTable *>(local_all_sub_table_->Swap(AllSubTable::kAllSubtableInUse)))) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, local all sub table state is wrong", K(ret));
    assert(false);
  } else if (AllSubTable::kAllSubtableObsolete == all_sub_table || all_sub_table->version_number_ != version_number_.load())  {
    std::lock_guard<std::mutex> guard(all_sub_table_mutex_);
    if (nullptr != all_sub_table && all_sub_table->unref()) {
      all_sub_table_to_delete = all_sub_table;
    }
    all_sub_table_->ref();
    all_sub_table = all_sub_table_;
    if (nullptr != all_sub_table_to_delete) {
      MOD_DELETE_OBJECT(AllSubTable, all_sub_table_to_delete);
    }
  }
  return ret;
}

int GlobalContext::release_thread_local_all_sub_table(AllSubTable *all_sub_table)
{
  int ret = Status::kOk;
  void *expected = AllSubTable::kAllSubtableInUse;
  if (nullptr == all_sub_table) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(all_sub_table));
  } else if (local_all_sub_table_->CompareAndSwap(static_cast<void *>(all_sub_table), expected)) {
    //success to return all sub table
  } else if (AllSubTable::kAllSubtableObsolete != expected) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, local all sub table state is wrong", K(ret));
    assert(false);
  } else {
    std::lock_guard<std::mutex> guard(all_sub_table_mutex_);
    if (all_sub_table->unref()) {
      MOD_DELETE_OBJECT(AllSubTable, all_sub_table);
    }
  }
  return ret;
}

//thread unsafe, need protect by all_sub_table_mutex_
void GlobalContext::reset_thread_local_all_sub_table()
{
  std::vector<void *> all_sub_tables;
  AllSubTable *all_sub_table = nullptr;
  local_all_sub_table_->Scrape(&all_sub_tables, AllSubTable::kAllSubtableObsolete);
  for (uint64_t i = 0; i < all_sub_tables.size(); ++i) {
    all_sub_table = static_cast<AllSubTable *>(all_sub_tables.at(i));
    if (AllSubTable::kAllSubtableInUse == all_sub_table) {
      //do nothing
    } else {
      if (all_sub_table->unref()) {
        MOD_DELETE_OBJECT(AllSubTable, all_sub_table);
      }
    }
  }
}

//thread unsafe, need protect by all_sub_table_mutex_
int GlobalContext::install_new_all_sub_table(AllSubTable *all_sub_table)
{
  int ret = Status::kOk;
  AllSubTable *old_all_sub_table = nullptr;
  if (nullptr == all_sub_table) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(all_sub_table));
  } else {
    old_all_sub_table = all_sub_table_;
    ++version_number_;
    all_sub_table_ = all_sub_table;
    all_sub_table_->version_number_ = version_number_;
    all_sub_table_->ref();
    reset_thread_local_all_sub_table();
    if (nullptr != old_all_sub_table && old_all_sub_table->unref()) {
      MOD_DELETE_OBJECT(AllSubTable, old_all_sub_table);
    }
  }
  return ret;
}

DBImpl::DBImpl(const DBOptions& options, const std::string& dbname)
    : env_(options.env),
      dbname_(dbname),
      initial_db_options_(SanitizeOptions(dbname, options)),
      immutable_db_options_(initial_db_options_),
      mutable_db_options_(initial_db_options_),
      stats_dump_period_sec_(mutable_db_options_.stats_dump_period_sec),
      stats_(immutable_db_options_.statistics.get()),
      extent_space_manager_(nullptr),
      storage_logger_(nullptr),
      bg_recycle_scheduled_(0),
      master_thread_running_(false),
      db_lock_(nullptr),
      mutex_(&mutable_db_options_.mutex_backtrace_threshold_ns, env_,
              immutable_db_options_.use_adaptive_mutex),
      shutting_down_(false),
      bg_cv_(&mutex_),
      logfile_number_(0),
      log_dir_synced_(false),
      log_empty_(true),
      default_cf_handle_(nullptr),
      missing_subtable_during_recovery_(),
      last_check_time_during_recovery_(0),
      no_switch_round_(0),
      deal_last_record_error_mutex_(false),
      max_sequence_during_recovery_(0),
      max_log_file_number_during_recovery_(0),
      log_sync_cv_(&mutex_),
      total_log_size_(0),
      max_total_in_memory_state_(0),
      is_snapshot_supported_(true),
      write_buffer_manager_(immutable_db_options_.write_buffer_manager.get()),
      trim_mem_flush_waited_(kFlushDone), next_trim_time_(0),
      storage_write_buffer_manager_(nullptr),
      write_thread_(immutable_db_options_.enable_write_thread_adaptive_yield
                        ? immutable_db_options_.write_thread_max_yield_usec
                        : 0,
                    immutable_db_options_.write_thread_slow_yield_usec),
      write_controller_(mutable_db_options_.delayed_write_rate),
      pipline_manager_(100 * 1024),
      pipline_parallel_worker_num_(0),
      pipline_copy_log_busy_flag_(false),
      pipline_flush_log_busy_flag_(false),
      pipline_global_error_flag_(false),
      active_thread_num_(0),
      active_thread_mutex_(false),
      active_thread_cv_(&active_thread_mutex_),
      wait_active_thread_exit_flag_(false),
      last_write_in_serialization_mode_(false),
      last_flushed_log_lsn_(0),
      batch_group_manager_(options.batch_group_slot_array_size,
                           options.batch_group_max_group_size,
                           options.batch_group_max_leader_wait_time_us),
      version_sliding_window_mutex_(false),
      last_batch_group_size_(0),
      unscheduled_flushes_(0),
      unscheduled_compactions_(0),
      unscheduled_dumps_(0),
      unscheduled_gc_(0),
      bg_compaction_scheduled_(0),
      num_running_compactions_(0),
      bg_flush_scheduled_(0),
      num_running_flushes_(0),
      bg_dump_scheduled_(0),
      num_running_dumps_(0),
      bg_purge_scheduled_(0),
      num_running_gc_(0),
      bg_gc_scheduled_(0),
      bg_ebr_scheduled_(0),
      shrink_running_(false),
      max_seq_in_rp_(0),
      disable_delete_obsolete_files_(0),
      delete_obsolete_files_last_run_(env_->NowMicros()),
      last_stats_dump_time_microsec_(0),
      next_job_id_(1),
      filter_build_quota_(options.filter_building_threads),
      has_unpersisted_data_(false),
      unable_to_flush_oldest_log_(false),
      env_options_(BuildDBOptions(immutable_db_options_, mutable_db_options_)),
      num_running_ingest_file_(0),
#ifndef ROCKSDB_LITE
      wal_manager_(immutable_db_options_, env_options_),
#endif  // ROCKSDB_LITE
      bg_work_paused_(0),
      bg_compaction_paused_(0),
      refitting_level_(false),
      opened_successfully_(false) {
  env_->GetAbsolutePath(dbname, &db_absolute_path_);
  env_->SetBackgroundThreads(1, Env::STATS);
  env_->SetBackgroundThreads(1, Env::MASTER);
  env_->SetBackgroundThreads(2, Env::RECYCLE_EXTENT);
  env_->SetBackgroundThreads(1, Env::SHRINK_EXTENT_SPACE);

  table_cache_ = NewLRUCache(immutable_db_options_.table_cache_size,
                             immutable_db_options_.table_cache_numshardbits);
  if (nullptr != table_cache_.get()) {
    table_cache_.get()->set_mod_id(ModId::kTableCache);
  }
  VersionSet *vs_ptr = MOD_NEW_OBJECT(ModId::kDBImpl, VersionSet, dbname_, &immutable_db_options_,
      env_options_, table_cache_.get(), write_buffer_manager_, &write_controller_);
  versions_.reset(vs_ptr);
  //column_family_memtables_.reset(
  //    new ColumnFamilyMemTablesImpl(versions_->GetColumnFamilySet()));
 
  int ret = Status::kOk; 
  if (immutable_db_options_.compaction_type == 1) {
    //TODO get parameter from options 
    CompactionMode comp_mode = (CompactionMode)immutable_db_options_.compaction_mode;
    // todo for fpga
//    compaction_scheduler_.reset(
//        new CompactionScheduler(comp_mode,
//          immutable_db_options_.fpga_device_id,
//          immutable_db_options_.fpga_compaction_thread_num,
//          immutable_db_options_.cpu_compaction_thread_num,
//          this->stats_,
//          immutable_db_options_.info_log.get()));
//    ret = compaction_scheduler_->init();
    __XENGINE_LOG(INFO,
        "Note: Use MinorCompaction(FPGA) instead of StreamCompaction!");
  } else {
    __XENGINE_LOG(INFO,
        "Note: Use StreamCompaction instead of MinorCompaction!");
  }

  assert(ret == Status::kOk);
  if (ret != Status::kOk) {
    abort();
  }

  QueryPerfContext *ctx = get_tls_query_perf_context();
  ctx->opt_enable_count_ = mutable_db_options_.query_trace_enable_count;
  ctx->opt_print_stats_ = mutable_db_options_.query_trace_print_stats;

//  timer_service_ = MOD_NEW_OBJECT(ModId::kDefaultMod, TimerService);
  //timer_service_ = PLACEMENT_NEW(TimerService, impl_alloc_);
  DumpXEngineBuildVersion();
  DumpDBFileSummary(immutable_db_options_, dbname_);
  immutable_db_options_.Dump();
  mutable_db_options_.Dump();
  DumpSupportInfo();
}

// Will lock the mutex_,  will wait for completion if wait is true
void DBImpl::CancelAllBackgroundWork(bool wait) {
  InstrumentedMutexLock l(&mutex_);

  __XENGINE_LOG(INFO,
                 "Shutdown: canceling all background work");

  if (!shutting_down_.load(std::memory_order_acquire) &&
      has_unpersisted_data_.load(std::memory_order_relaxed) &&
      !mutable_db_options_.avoid_flush_during_shutdown) {
    int ret = Status::kOk;
    GlobalContext* global_ctx = nullptr;
    SubTable* sub_table = nullptr;
    if (nullptr == (global_ctx = versions_->get_global_ctx())) {
      ret = Status::kCorruption;
      XENGINE_LOG(WARN, "global ctx must not nullptr", K(ret));
    } else {
      SubTableMap& all_sub_tables = global_ctx->all_sub_table_->sub_table_map_;
      for (auto iter = all_sub_tables.begin();
           Status::kOk && all_sub_tables.end() != iter; ++iter) {
        if (nullptr == (sub_table = iter->second)) {
          ret = Status::kCorruption;
          XENGINE_LOG(WARN, "subtable must not nullptr", K(ret),
                      K(iter->first));
        } else if (sub_table->IsDropped()) {
          // do nothing
          XENGINE_LOG(INFO, "subtable has been dropped", "index_id",
                      iter->first);
        } else if (sub_table->mem()->IsEmpty()) {
          // do nothing
          XENGINE_LOG(INFO, "subtable is empty", "index_id", iter->first);
        } else {
          sub_table->Ref();
          mutex_.Unlock();
          FlushMemTable(sub_table, FlushOptions());
          mutex_.Lock();
          sub_table->Unref();
        }
      }
    }
  }

  shutting_down_.store(true, std::memory_order_release);
  bg_cv_.SignalAll();
  if (!wait) {
    return;
  }
  // Wait for background work to finish
  while (bg_compaction_scheduled_ || bg_flush_scheduled_ || bg_gc_scheduled_ 
         || bg_dump_scheduled_ || master_thread_running() || bg_ebr_scheduled_) {
    bg_cv_.Wait();
  }
}

DBImpl::~DBImpl() {
  // CancelAllBackgroundWork called with false means we just set the shutdown
  // marker. After this we do a variant of the waiting and unschedule work
  // (to consider: moving all the waiting into CancelAllBackgroundWork(true))
  CancelAllBackgroundWork(false);
//  int compactions_unscheduled = env_->UnSchedule(this, Env::Priority::LOW); // has gc task
  int flushes_unscheduled = env_->UnSchedule(this, Env::Priority::HIGH);
  int dump_unscheduled = env_->UnSchedule(this, Env::Priority::LOW);

  while (filter_build_quota_.load() < mutable_db_options_.filter_building_threads) {
    port::AsmVolatilePause();
  }
  get_tls_query_perf_context()->shutdown();

  mutex_.Lock();
//  bg_compaction_scheduled_ -= compactions_unscheduled;
  bg_flush_scheduled_ -= flushes_unscheduled;
  bg_dump_scheduled_ -= dump_unscheduled;

  // Wait for background work to finish
  while (bg_compaction_scheduled_ || bg_flush_scheduled_ || bg_dump_scheduled_ ||
         bg_purge_scheduled_ || bg_recycle_scheduled_ || master_thread_running()) {
    TEST_SYNC_POINT("DBImpl::~DBImpl:WaitJob");
    bg_cv_.Wait();
  }
  EraseThreadsStatusDbInfo();
  flush_scheduler_.Clear();

//  if (immutable_db_options_.compaction_type == 1 && compaction_scheduler_) {
//    compaction_scheduler_->stop();
//  }

  STFlushJob *flush_job = nullptr;
  while (!flush_queue_.empty()) {
    if (nullptr != (flush_job = PopFirstFromFlushQueue())) {
      if (nullptr != flush_job->sub_table_) {
        flush_job->sub_table_->set_pending_flush(false);
        flush_job->sub_table_->set_pending_compaction(false);
      }
      remove_flush_job(flush_job, false);
    }
  }
  STDumpJob *dump_job = nullptr;
  while (!dump_queue_.empty()) {
    if (nullptr != (dump_job = pop_front_dump_job())) {
      remove_dump_job(dump_job);
    }
  }
  for (int i = 0; i < CompactionPriority::ALL; i++) {
    while (!compaction_queue_[i].empty()) {
      auto cf_job = compaction_queue_[i].front();
      compaction_queue_[i].pop_front();
      remove_compaction_job(cf_job, false);
    }
  }

  for (auto& info : compaction_history_) {
    MOD_DELETE_OBJECT(CompactionJobStatsInfo, info);
  }

  while (!gc_queue_.empty()) {
    auto gc_job = pop_front_gc_job();
    remove_gc_job(gc_job);
  }

  if (default_cf_handle_ != nullptr) {
    // we need to delete handle outside of lock because it does its own locking
    mutex_.Unlock();
//    delete default_cf_handle_;
    MOD_DELETE_OBJECT(ColumnFamilyHandleImpl, default_cf_handle_);
    mutex_.Lock();
  }

  // Clean up obsolete files due to SuperVersion release.
  // (1) Need to delete to obsolete files before closing because RepairDB()
  // scans all existing files in the file system and builds manifest file.
  // Keeping obsolete files confuses the repair process.
  // (2) Need to check if we Open()/Recover() the DB successfully before
  // deleting because if VersionSet recover fails (may be due to corrupted
  // manifest file), it is not able to identify live files correctly. As a
  // result, all "live" files can get deleted by accident. However, corrupted
  // manifest is recoverable by RepairDB().
  if (opened_successfully_) {
    JobContext job_context(next_job_id_.fetch_add(1));
    FindObsoleteFiles(&job_context, true);

    mutex_.Unlock();
    // manifest number starting from 2
    job_context.manifest_file_number = 1;
    if (job_context.HaveSomethingToDelete()) {
      PurgeObsoleteFiles(job_context);
    }
    job_context.Clean();
    mutex_.Lock();
  }

  for (auto l : logs_to_free_) {
//    delete l;
    MOD_DELETE_OBJECT(Writer, l);
  }
  for (auto& log : logs_) {
    log.ClearWriter();
  }
  logs_.clear();

  // Table cache may have table handles holding blocks from the block cache.
  // We need to release them before the block cache is destroyed. The block
  // cache may be destroyed inside versions_.reset(), when column family data
  // list is destroyed, so leaving handles in table cache after
  // versions_.reset() may cause issues.
  // Here we clean all unreferenced handles in table cache.
  // Now we assume all user queries have finished, so only version set itself
  // can possibly hold the blocks from block cache. After releasing unreferenced
  // handles here, only handles held by version set left and inside
  // versions_.reset(), we will release them. There, we need to make sure every
  // time a handle is released, we erase it from the cache too. By doing that,
  // we can guarantee that after versions_.reset(), table cache is empty
  // so the cache can be safely destroyed.
  table_cache_->EraseUnRefEntries();

  delete_all_recovered_transactions();

  for (auto& info : memtable_cleanup_queue_) {
    ColumnFamilyData* cfd = info.cfd_;
    if (cfd->Unref()) {
      MOD_DELETE_OBJECT(ColumnFamilyData, cfd);
    }
  }

  auto *global_ctx = (versions_ != nullptr) ? versions_->get_global_ctx() : nullptr;
  // versions need to be destroyed before table_cache since it can hold
  // references to table_cache.
  versions_.reset();
  mutex_.Unlock();
  if (db_lock_ != nullptr) {
    env_->UnlockFile(db_lock_);
  }

//  if (nullptr != gc_timer_) {
//    gc_timer_->stop();
//    MOD_DELETE_OBJECT(Timer, gc_timer_);
//  }
//
//  if (nullptr != shrink_timer_) {
//    shrink_timer_->stop();
//    MOD_DELETE_OBJECT(Timer, shrink_timer_);
//  }
//
//  if (nullptr != timer_service_) {
//    MOD_DELETE_OBJECT(TimerService, timer_service_);
//  }

  if (nullptr != storage_logger_) {
    MOD_DELETE_OBJECT(StorageLogger, storage_logger_);
  }
  MOD_DELETE_OBJECT(ExtentSpaceManager, extent_space_manager_);
  if (nullptr != global_ctx) {
    MOD_DELETE_OBJECT(GlobalContext, global_ctx);
  }
  __XENGINE_LOG(INFO, "Shutdown complete");
  // LogFlush(immutable_db_options_.info_log);
}

void DBImpl::schedule_master_thread() {
  mutex_.AssertHeld();
  assert(opened_successfully_);
  env_->Schedule(&DBImpl::bg_master_func_wrapper, this, Env::Priority::MASTER, nullptr);
  //we must update master thread stats while holding db mutex
  master_thread_running_.store(true, std::memory_order_release);
}

void DBImpl::bg_master_func_wrapper(void* db) {
  TEST_SYNC_POINT("DBImpl::bg_master_thread:start");
  reinterpret_cast<DBImpl*>(db)->bg_master_thread_func();
  TEST_SYNC_POINT("DBImpl::bg_master_thread:end");
}

//TODO:should we use master thread or use a Timer Service?
void DBImpl::bg_master_thread_func() {
  //TODO: get HD and show in mysql show processlist;
  uint64_t max_schedule_interval_in_ms = 2 * 1000; //2 seconds
  uint64_t last_schedule_ts = env_->NowMicros() / 1000;
  uint64_t current_schedule_ts = 0;

  uint64_t last_stats_dump_ts = env_->NowMicros() / 1000;
  uint64_t last_gc_ts = last_stats_dump_ts;
  uint64_t last_cache_purge_ts = last_stats_dump_ts;
  uint64_t last_auto_compaction_ts = last_stats_dump_ts;
  uint64_t last_shrink_ts = last_stats_dump_ts;
  uint64_t last_ebr_ts = last_stats_dump_ts;
  uint64_t stats_dump_period_ms = mutable_db_options_.stats_dump_period_sec * 1000;
  uint64_t cache_purge_ms = 3000; // 3s
  uint64_t gc_ms = 5 * 1000 * 60; // 5min
  uint64_t shrink_ms = 30 * 1000 * 60; // 30min
  uint64_t ebr_ms = 2 * 1000; // 2 seconds
  uint64_t last_seq = versions_.get()->LastSequence();
  uint64_t sequence_step = 256;
  bool idle_flag = true;
  XENGINE_LOG(WARN, "X-Engine Master Thread online");
  int ret = 0;
  while (!db_shutting_down() && bg_error_.ok()) {
    uint64_t auto_compaction_ms = mutable_db_options_.idle_tasks_schedule_time * 1000;
    //caculate wait time in this loop
    current_schedule_ts = env_->NowMicros() / 1000;
    int64_t last_schedule_time_cost = current_schedule_ts - last_schedule_ts;
    int64_t sleep_time = max_schedule_interval_in_ms - last_schedule_time_cost;
    sleep_time = sleep_time > 0 ? sleep_time : 0;
    //TODO  use condition sleep 
    env_->SleepForMicroseconds(sleep_time * 1000);
    last_schedule_ts = env_->NowMicros() / 1000;
    // (1) schedule bg stats information output
    if (env_->NowMicros() / 1000 > last_stats_dump_ts + stats_dump_period_ms) {
      last_stats_dump_ts = env_->NowMicros() / 1000;
      QueryPerfContext::async_log_stats(env_, db_absolute_path_,
          block_cache_.get(), immutable_db_options_.row_cache.get());
    }
    if (db_shutting_down() || !bg_error_.ok()) {
      break;
    }
    // (2) schedule bg row cache purge
    if (env_->NowMicros() / 1000 > last_cache_purge_ts + cache_purge_ms) {
      last_cache_purge_ts = env_->NowMicros() / 1000;
      RowCache *row_cache = immutable_db_options_.row_cache.get();
      if (nullptr != row_cache) {
        XENGINE_LOG(DEBUG, "BG_TASK: cache purge", K(env_->NowMicros()));
        row_cache->schedule_cache_purge(env_);
      }
    }
    // (3) schedule gc task
    if (env_->NowMicros() / 1000 > last_gc_ts + gc_ms) {
      last_gc_ts = env_->NowMicros() / 1000;
      XENGINE_LOG(DEBUG, "BG_TASK: gc task", K(env_->NowMicros()));
      schedule_gc();
    }
    //(a) check wal quota and usage
    if (total_log_size_ > GetMaxTotalWalSize() * 1.5) {
      // do switch
      WriteContext write_context;
      XENGINE_LOG(INFO, "BG_TASK: force handle wal full", K(env_->NowMicros()));
      if (SUCC(ret) && FAILED(force_handle_wal_full(&write_context))) {
        XENGINE_LOG(WARN, "failed to handle wal", K(ret), K(total_log_size_));
      }
    }
    //(b) check memory quota and usage
    AllocMgr *alloc = AllocMgr::get_instance();
    if (nullptr != alloc && alloc->check_if_memory_overflow()) {
      stats_dump_period_ms = 10 * 1000;
    } else {
      stats_dump_period_ms = mutable_db_options_.stats_dump_period_sec * 1000;
    }
    //detect system workload
    // a.check wal generate rate for write workload
    // b.check cache read request and io read for read workload
    uint64_t cur_seq = versions_.get()->LastSequence();
    if (cur_seq - last_seq > sequence_step) {
      idle_flag = false;
    }
    last_seq = cur_seq;
    if (auto_compaction_ms > 0
        && env_->NowMicros() / 1000 > last_auto_compaction_ts + auto_compaction_ms
        && SUCC(ret)) {
      last_auto_compaction_ts = env_->NowMicros() / 1000;
      if (idle_flag) {
        if (FAILED(master_schedule_compaction(CompactionScheduleType::MASTER_IDLE))) {
          XENGINE_LOG(WARN, "failed to schedule compaction", K(ret));
        } else {
        }
        // todo do idle checkpoint
      } else {
        idle_flag = true;
      }
    }
    // (4) schedule auto compaction task
    if (env_->NowMicros() / 1000 > last_auto_compaction_ts + auto_compaction_ms
        && SUCC(ret)) {
      last_auto_compaction_ts = env_->NowMicros() / 1000;
      XENGINE_LOG(DEBUG, "BG_TASK: schedule compaction", K(env_->NowMicros()));
      if (FAILED(master_schedule_compaction(CompactionScheduleType::MASTER_AUTO))) {
        XENGINE_LOG(WARN, "failed to schedule compaction", K(ret));
      }
    }
    // (5) shrink task
    if ((env_->NowMicros() / 1000) > (last_shrink_ts + mutable_db_options_.auto_shrink_schedule_interval * 1000)
        && SUCC(ret)) {
      last_shrink_ts = env_->NowMicros() / 1000;
      XENGINE_LOG(INFO, "BG_TASK: shrink task", K(env_->NowMicros()));
      schedule_shrink();
    }
    // (6) epoch based reclaim task
    if (env_->NowMicros() / 1000 > last_ebr_ts + ebr_ms && SUCC(ret)) {
      last_ebr_ts = env_->NowMicros() / 1000;
      XENGINE_LOG(INFO, "BG_TASK: ebr task", K(env_->NowMicros()));
      schedule_ebr();
    }
  }
  //we should set stat while holding mutex_
  mutex_.Lock();
  master_thread_running_.store(false, std::memory_order_release);
  bg_cv_.SignalAll();
  mutex_.Unlock();
  XENGINE_LOG(WARN, "X-Engine Master Thread go offline", K(ret), K((int)bg_error_.code()));
}

void DBImpl::MaybeIgnoreError(Status* s) const {
  if (s->ok() || immutable_db_options_.paranoid_checks) {
    // No change needed
  } else {
    __XENGINE_LOG(WARN, "Ignoring error %s",
                   s->ToString().c_str());
    *s = Status::OK();
  }
}

const Status DBImpl::CreateArchivalDirectory() {
  if (immutable_db_options_.wal_ttl_seconds > 0 ||
      immutable_db_options_.wal_size_limit_mb > 0) {
    std::string archivalPath = ArchivalDirectory(immutable_db_options_.wal_dir);
    return env_->CreateDirIfMissing(archivalPath);
  }
  return Status::OK();
}

Status DBImpl::prepare_create_storage_manager(const DBOptions& db_options,
                                      const ColumnFamilyOptions& cf_options) {
  //TODO:yuanfeng unused
  /*
  extent_space_manager_.rnew storage::ExtentSpaceManager(
      db_options, versions_->get_file_number_generator()));
  if (nullptr == extent_space_manager_) {
    __XENGINE_LOG(ERROR,
                    "Create extent space manager failed %d\n",
                    Status::kMemoryLimit);
    return Status(Status::kMemoryLimit);
  }
  */

  return Status::OK();
}

void DBImpl::PrintStatistics() {
  auto dbstats = immutable_db_options_.statistics.get();
  if (dbstats) {
    __XENGINE_LOG(INFO, "\nSTATISTICS:\n %s",
                   dbstats->ToString().c_str());
  }
}

void DBImpl::ScheduleBgLogWriterClose(JobContext* job_context) {
  if (!job_context->logs_to_free.empty()) {
    for (auto l : job_context->logs_to_free) {
      AddToLogsToFreeQueue(l);
    }
    job_context->logs_to_free.clear();
    SchedulePurge();
  }
}

Directory* DBImpl::Directories::GetDataDir(size_t path_id) {
  assert(path_id < data_dirs_.size());
  Directory* ret_dir = data_dirs_[path_id];
  if (ret_dir == nullptr) {
    // Should use db_dir_
    return db_dir_.get();
  }
  return ret_dir;
}

int DBImpl::reset_pending_shrink(const uint64_t subtable_id) {
  int ret = Status::kOk;
  SubTable *subtable = nullptr;

  InstrumentedMutexLock mutex_guard(&mutex_);
  db::AllSubTableGuard all_subtable_guard(versions_->get_global_ctx());
  const db::SubTableMap &subtable_map = all_subtable_guard.get_subtable_map();
  auto iter = subtable_map.find(subtable_id);
  if (subtable_map.end() == iter) {
    ret = Status::kNotFound;
    XENGINE_LOG(WARN, "this subtable not exist, can't reset pending shrink",
                K(ret), K(subtable_id));
  } else if (IS_NULL(subtable = iter->second)) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, subtable must not nullptr", K(ret),
                K(subtable_id));
  } else {
    subtable->set_pending_shrink(false);
    XENGINE_LOG(INFO, "success to reset pending shrink", K(subtable_id));
  }

  return ret;
}

Status DBImpl::SetOptions(const std::unordered_map<std::string, std::string>& options_map) {
  Status s;
  autovector<ColumnFamilyHandle *> cf_handles;
  mutex_.Lock();

  int ret = Status::kOk;
  GlobalContext *global_ctx = nullptr;
  SubTable *sub_table = nullptr;
  ArenaAllocator tmp_alloc(8 * 1024);
  if (nullptr == (global_ctx = versions_->get_global_ctx())) {
    ret = Status::kCorruption;
    XENGINE_LOG(WARN, "global ctx must not nullptr", K(ret));
  } else {
    SubTableMap &all_sub_tables = global_ctx->all_sub_table_->sub_table_map_;
    for (auto iter = all_sub_tables.begin(); Status::kOk == ret && iter != all_sub_tables.end(); ++iter) {
      if (nullptr == (sub_table = iter->second)) {
        ret = Status::kCorruption;
        XENGINE_LOG(WARN, "subtable must not nullptr", K(ret), K(iter->first));
      } else {
        ColumnFamilyHandle *handle = ALLOC_OBJECT(ColumnFamilyHandleImpl, tmp_alloc, sub_table, this, &mutex_);
//            new ColumnFamilyHandleImpl(sub_table, this, &mutex_);
        cf_handles.push_back(handle);
      }
    }
  }

  mutex_.Unlock();

  bool do_dump = !cf_handles.empty();
  bool do_get_dump = true;
  ColumnFamilyOptions cf_options_to_dump;
  for (auto cf_handle: cf_handles) {
    if (s.ok()) {
      s = SetOptions(cf_handle, options_map);
    }
    if (do_get_dump && s.ok()) {
      auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(cf_handle)->cfd();
      cf_options_to_dump = cfd->GetLatestCFOptions();
      do_get_dump = false;
    }
    FREE_OBJECT(ColumnFamilyHandle, tmp_alloc, cf_handle);
//    delete cf_handle;
  }
  if (s.ok() && do_dump) {
    cf_options_to_dump.Dump();
  }
  return s;
}

Status DBImpl::SetOptions(
    ColumnFamilyHandle* column_family,
    const std::unordered_map<std::string, std::string>& options_map) {
#ifdef ROCKSDB_LITE
  return Status::NotSupported("Not supported in ROCKSDB LITE");
#else
  auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();
  if (options_map.empty()) {
    __XENGINE_LOG(WARN,
                   "SetOptions() on sub table [%s], empty input",
                   cfd->GetName().c_str());
    return Status::InvalidArgument("empty input");
  }

  MutableCFOptions new_options;
  Status s;
  WriteThread::Writer w;
  {
    InstrumentedMutexLock l(&mutex_);
    s = cfd->SetOptions(options_map);
    if (s.ok()) {
      new_options = *cfd->GetLatestMutableCFOptions();
      // Append new version to recompute compaction score.
      //VersionEdit dummy_edit;
      //versions_->LogAndApply(cfd, new_options, &dummy_edit, &mutex_,
      //                       directories_.GetDbDir());
      // Trigger possible flush/compactions. This has to be before we persist
      // options to file, otherwise there will be a deadlock with writer
      // thread.
      // todo object pool
      SuperVersion *sv = MOD_NEW_OBJECT(ModId::kSuperVersion, SuperVersion);
      auto* old_sv = cfd->InstallSuperVersion(sv, &mutex_, new_options);
      MOD_DELETE_OBJECT(SuperVersion, old_sv);
//      delete old_sv;
      this->wait_all_active_thread_exit();
    }
  }

  std::ostringstream oss;
  oss << "inputs: {";
  for (const auto& o : options_map) {
    oss << o.first << ": " << o.second;
  }
  oss << "} " << (s.ok() ? "succeeded" : "failed");
  __XENGINE_LOG(INFO, "SetOptions() on sub table [ID=%u] %s",
                cfd->GetID(), oss.str().c_str());
  // LogFlush(immutable_db_options_.info_log);
  return s;
#endif  // ROCKSDB_LITE
}

Status DBImpl::SetDBOptions(
    const std::unordered_map<std::string, std::string>& options_map) {
#ifdef ROCKSDB_LITE
  return Status::NotSupported("Not supported in ROCKSDB LITE");
#else
  if (options_map.empty()) {
    __XENGINE_LOG(WARN,
                   "SetDBOptions(), empty input.");
    return Status::InvalidArgument("empty input");
  }

  MutableDBOptions new_options;
  Status s;
  WriteThread::Writer w;
  WriteContext write_context;
  {
    InstrumentedMutexLock l(&mutex_);
    s = GetMutableDBOptionsFromStrings(mutable_db_options_, options_map,
                                       &new_options);
    if (s.ok()) {
      if (new_options.max_background_compactions >
          mutable_db_options_.max_background_compactions) {
        env_->IncBackgroundThreadsIfNeeded(
            new_options.max_background_compactions, Env::Priority::LOW);
        MaybeScheduleFlushOrCompaction();
      }

      write_controller_.set_max_delayed_write_rate(
          new_options.delayed_write_rate);

      mutable_db_options_ = new_options;
      stats_dump_period_sec_.store(mutable_db_options_.stats_dump_period_sec);

      this->wait_all_active_thread_exit();
      if (total_log_size_ > GetMaxTotalWalSize()) {
        Status purge_wal_status = HandleWALFull(&write_context);
        if (!purge_wal_status.ok()) {
          __XENGINE_LOG(WARN,
                         "Unable to purge WAL files in SetDBOptions() -- %s",
                         purge_wal_status.ToString().c_str());
        }
      }
    }
  }
  __XENGINE_LOG(INFO, "SetDBOptions(), inputs:");
  for (const auto& o : options_map) {
    __XENGINE_LOG(INFO, "%s: %s\n", o.first.c_str(),
                   o.second.c_str());
  }
  if (s.ok()) {
    __XENGINE_LOG(INFO, "SetDBOptions() succeeded");
    new_options.Dump();
  } else {
    __XENGINE_LOG(WARN, "SetDBOptions failed");
  }
  // LogFlush(immutable_db_options_.info_log);
  return s;
#endif  // ROCKSDB_LITE
}

Status DBImpl::SyncWAL() {
  autovector<log::Writer*, 1> logs_to_sync;
  bool need_log_dir_sync;
  uint64_t current_log_number;

  {
    QUERY_TRACE_SCOPE(TracePoint::DB_SYNC_WAL);
    InstrumentedMutexLock l(&mutex_);
    assert(!logs_.empty());

    // This SyncWAL() call only cares about logs up to this number.
    current_log_number = logfile_number_;

    while (logs_.front().number <= current_log_number &&
           logs_.front().getting_synced) {
      log_sync_cv_.Wait();
    }
    // First check that logs are safe to sync in background.
    for (auto it = logs_.begin();
         it != logs_.end() && it->number <= current_log_number; ++it) {
      if (!it->writer->file()->writable_file()->IsSyncThreadSafe()) {
        return Status::NotSupported(
            "SyncWAL() is not supported for this implementation of WAL file",
            immutable_db_options_.allow_mmap_writes
                ? "try setting Options::allow_mmap_writes to false"
                : Slice());
      }
    }
    for (auto it = logs_.begin();
         it != logs_.end() && it->number <= current_log_number; ++it) {
      auto& log = *it;
      assert(!log.getting_synced);
      log.getting_synced = true;
      logs_to_sync.push_back(log.writer);
    }

    need_log_dir_sync = !log_dir_synced_;
  }

  QUERY_COUNT(CountPoint::WAL_FILE_SYNCED);
  Status status;
  for (log::Writer* log : logs_to_sync) {
    status = log->file()->sync(immutable_db_options_.use_fsync);
    if (!status.ok()) {
      break;
    }
  }
  if (status.ok() && need_log_dir_sync) {
    status = directories_.GetWalDir()->Fsync();
  }

  TEST_SYNC_POINT("DBImpl::SyncWAL:BeforeMarkLogsSynced:1");
  {
    InstrumentedMutexLock l(&mutex_);
    MarkLogsSynced(current_log_number, need_log_dir_sync, status);
  }
  TEST_SYNC_POINT("DBImpl::SyncWAL:BeforeMarkLogsSynced:2");

  return status;
}

void DBImpl::MarkLogsSynced(uint64_t up_to, bool synced_dir,
                            const Status& status) {
  mutex_.AssertHeld();
  if (synced_dir && logfile_number_ == up_to && status.ok()) {
    log_dir_synced_ = true;
  }
  for (auto it = logs_.begin(); it != logs_.end() && it->number <= up_to;) {
    auto& log = *it;
    assert(log.getting_synced);
    if (status.ok() && logs_.size() > 1) {
      logs_to_free_.push_back(log.ReleaseWriter());
      it = logs_.erase(it);
    } else {
      log.getting_synced = false;
      ++it;
    }
  }
  assert(!status.ok() || logs_.empty() || logs_[0].number > up_to ||
         (logs_.size() == 1 && !logs_[0].getting_synced));
  log_sync_cv_.SignalAll();
}

SequenceNumber DBImpl::GetLatestSequenceNumber() const {
  return versions_->LastSequence();
}

InternalIterator* DBImpl::NewInternalIterator(
    Arena* arena, RangeDelAggregator* range_del_agg,
    ColumnFamilyHandle* column_family) {
  ColumnFamilyData* cfd;
  if (column_family == nullptr) {
    cfd = default_cf_handle_->cfd();
  } else {
    auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
    cfd = cfh->cfd();
  }

  QUERY_TRACE_BEGIN(TracePoint::DB_ITER_REF_SV);
  mutex_.Lock();
  SuperVersion* super_version = cfd->GetSuperVersion()->Ref();
  mutex_.Unlock();
  QUERY_TRACE_END();
  ReadOptions roptions;
  return NewInternalIterator(roptions, cfd, super_version, arena,
                             range_del_agg);
}

void DBImpl::SchedulePurge() {
  mutex_.AssertHeld();
  assert(opened_successfully_);

  // Purge operations are put into High priority queue
  bg_purge_scheduled_++;
  env_->Schedule(&DBImpl::BGWorkPurge, this, Env::Priority::HIGH, nullptr);
}

void DBImpl::BackgroundCallPurge() {
  mutex_.Lock();

  // We use one single loop to clear both queues so that after existing the loop
  // both queues are empty. This is stricter than what is needed, but can make
  // it easier for us to reason the correctness.
  while (!purge_queue_.empty() || !logs_to_free_queue_.empty()) {
    if (!purge_queue_.empty()) {
      auto purge_file = purge_queue_.begin();
      auto fname = purge_file->fname;
      auto type = purge_file->type;
      auto number = purge_file->number;
      auto path_id = purge_file->path_id;
      auto job_id = purge_file->job_id;
      purge_queue_.pop_front();

      mutex_.Unlock();
      Status file_deletion_status;
      DeleteObsoleteFileImpl(file_deletion_status, job_id, fname, type, number,
                             path_id);
      mutex_.Lock();
    } else {
      assert(!logs_to_free_queue_.empty());
      log::Writer* log_writer = *(logs_to_free_queue_.begin());
      logs_to_free_queue_.pop_front();
      mutex_.Unlock();
      delete log_writer;
      mutex_.Lock();
    }
  }
  bg_purge_scheduled_--;

  bg_cv_.SignalAll();
  // IMPORTANT:there should be no code after calling SignalAll. This call may
  // signal the DB destructor that it's OK to proceed with destruction. In
  // that case, all DB variables will be dealloacated and referencing them
  // will cause trouble.
  mutex_.Unlock();
}

//int DBImpl::init_cache_purge_timer() {
//  int ret = Status::kOk;
//  if (nullptr != immutable_db_options_.row_cache.get()) {
//    std::function<void(void)> row_cache_purge = std::bind(
//            &RowCache::schedule_cache_purge, immutable_db_options_.row_cache.get(), env_);
//    uint64_t period = 3000; // 3s
//    cache_purge_timer_ = MOD_NEW_OBJECT(ModId::kRowCache, Timer, timer_service_, period /* ms */,
//                                    Timer::Repeatable, row_cache_purge);
//    if (nullptr == cache_purge_timer_) {
//      ret = Status::kMemoryLimit;
//      XENGINE_LOG(ERROR, "register dump stats timer error", K(ret));
//      assert(false);
//    } else {
//      cache_purge_timer_->start();
//    }
//  }
//  return ret;
//}


namespace {
struct IterState {
  IterState(DBImpl* _db, InstrumentedMutex* _mu, SuperVersion* _super_version,
            bool _background_purge)
      : db(_db),
        mu(_mu),
        super_version(_super_version),
        background_purge(_background_purge) {}

  DBImpl* db;
  InstrumentedMutex* mu;
  SuperVersion* super_version;
  bool background_purge;
};

static void CleanupIteratorState(void* arg1, void* arg2) {
  IterState* state = reinterpret_cast<IterState*>(arg1);
  ColumnFamilyData* cfd = reinterpret_cast<ColumnFamilyData*>(arg2);

  if (state->super_version->Unref()) {
    // Job id == 0 means that this is not our background process, but rather
    // user thread
    JobContext job_context(0);

    state->mu->Lock();
    cfd->release_meta_snapshot(state->super_version->current_meta_, 
                               state->mu);
    state->super_version->Cleanup();
    state->db->FindObsoleteFiles(&job_context, false, true);
    if (state->background_purge) {
      state->db->ScheduleBgLogWriterClose(&job_context);
    }
    state->mu->Unlock();

    MOD_DELETE_OBJECT(SuperVersion, state->super_version);
//    delete state->super_version;
    if (job_context.HaveSomethingToDelete()) {
      if (state->background_purge) {
        // PurgeObsoleteFiles here does not delete files. Instead, it adds the
        // files to be deleted to a job queue, and deletes it in a separate
        // background thread.
        state->db->PurgeObsoleteFiles(job_context, true /* schedule only */);
        state->mu->Lock();
        state->db->SchedulePurge();
        state->mu->Unlock();
      } else {
        state->db->PurgeObsoleteFiles(job_context);
      }
    }
    job_context.Clean();
  }

  delete state;
}
}  // namespace

InternalIterator* DBImpl::NewInternalIterator(
    const ReadOptions& read_options, ColumnFamilyData* cfd,
    SuperVersion* super_version, Arena* arena,
    RangeDelAggregator* range_del_agg) {
  InternalIterator* internal_iter;
  assert(arena != nullptr);
  assert(range_del_agg != nullptr);
  // Need to create internal iterator from the arena.
  MergeIteratorBuilder merge_iter_builder(
      &cfd->internal_comparator(), arena,
      !read_options.total_order_seek &&
          cfd->ioptions()->prefix_extractor != nullptr);

  Status s;
  if(kOnlyL2 != read_options.read_level_ ){
    // Collect iterator for mutable mem
    QUERY_TRACE_BEGIN(TracePoint::DB_ITER_ADD_MEM);
    merge_iter_builder.AddIterator(
        super_version->mem->NewIterator(read_options, arena));
    std::unique_ptr<InternalIterator, ptr_delete<InternalIterator>> range_del_iter;
    if (!read_options.ignore_range_deletions) {
//      range_del_iter.reset(
//          super_version->mem->NewRangeTombstoneIterator(read_options));
      s = range_del_agg->AddTombstones(super_version->mem->NewRangeTombstoneIterator(read_options));
    }
    // Collect all needed child iterators for immutable memtables
    if (s.ok()) {
      super_version->imm->AddIterators(read_options, &merge_iter_builder);
      if (!read_options.ignore_range_deletions) {
        s = super_version->imm->AddRangeTombstoneIterators(read_options, arena,
                                                           range_del_agg);
      }
    }
    QUERY_TRACE_END(); // end for DB_ITER_ADD_MEM
  }
  if (s.ok()) {
    QUERY_TRACE_SCOPE(TracePoint::DB_ITER_ADD_STORAGE);
    // Collect iterators for files in L0 - Ln
    if (read_options.read_tier != kMemtableTier) {
      cfd->get_storage_manager()->add_iterators(cfd->table_cache(), 
                                                cfd->internal_stats(),
                                                read_options,
                                                &merge_iter_builder,
                                                range_del_agg,
                                  super_version->current_meta_);
    }
    internal_iter = merge_iter_builder.Finish();
    // todo new
    IterState* cleanup =
        new IterState(this, &mutex_, super_version,
                      read_options.background_purge_on_iterator_cleanup);
    // pass the cfd to return the supper version
    internal_iter->RegisterCleanup(CleanupIteratorState, cleanup, cfd);
    return internal_iter;
  }
  return NewErrorInternalIterator(s);
}

ColumnFamilyHandle* DBImpl::DefaultColumnFamily() const {
  return default_cf_handle_;
}

Status DBImpl::Get(const ReadOptions& read_options,
                   ColumnFamilyHandle* column_family, const Slice& key,
                   PinnableSlice* value) {
  return GetImpl(read_options, column_family, key, value);
}

int DBImpl::get_from_row_cache(const ColumnFamilyData *cfd,
                               const SequenceNumber snapshot,
                               const Slice& key,
                               IterKey &row_cache_key,
                               PinnableSlice *&pinnable_val,
                               bool &done) {
  int ret = 0;
  RowCache *row_cache_ptr = immutable_db_options_.row_cache.get();
  if (nullptr != row_cache_ptr && nullptr != cfd) {
    uint32_t cfd_id = cfd->GetID();
    // 1. cfd id
    char buf[10];
    char* ptr = EncodeVarint32(buf, cfd_id);
    row_cache_key.TrimAppend(row_cache_key.Size(), buf, ptr - buf);
    // 2. key's data
    row_cache_key.TrimAppend(row_cache_key.Size(), key.data(), key.size());
    // 3. get value from row_cache
    cache::Cache::Handle *row_handle = immutable_db_options_.row_cache->lookup_row(
        row_cache_key.GetUserKey(), snapshot, cfd);
    if (nullptr != row_handle) {
      const RowcHandle* found_row = reinterpret_cast<const RowcHandle *>(row_handle);
      if (nullptr != found_row && found_row->seq_ <= snapshot) {
        done = true;
        int64_t handle_size = sizeof(RowcHandle) - 1 + found_row->key_length_;
        pinnable_val->PinSelf(Slice((char *)found_row + handle_size, found_row->charge_ - handle_size));
        QUERY_COUNT(CountPoint::ROW_CACHE_HIT);
      }
      row_cache_ptr->Release(row_handle);
    }
    if (!done) {
      QUERY_COUNT(CountPoint::ROW_CACHE_MISS);
    }
  }
  // If row cache is not open, record neither hit nor miss.
  return ret;
}

int DBImpl::add_into_row_cache(
    const void* data,
    const size_t data_size,
    const common::SequenceNumber snapshot,
    const db::ColumnFamilyData *cfd,
    const uint64_t key_seq,
    IterKey &row_cache_key) {
  int ret = 0;
  if (nullptr != data) {
    if (FAILED(immutable_db_options_.row_cache->insert_row(
        row_cache_key.GetUserKey(), data, data_size, key_seq, cfd, snapshot))) {
      XENGINE_LOG(WARN, "failed to insert new row",
          K(ret), K(snapshot), K(Slice((char *)data, data_size)));
    } else {
      QUERY_COUNT(CountPoint::ROW_CACHE_ADD);
    }
  }
  return ret;
}

Status DBImpl::GetImpl(const ReadOptions& read_options,
                       ColumnFamilyHandle* column_family, const Slice& key,
                       PinnableSlice* pinnable_val, bool* value_found) {
  QUERY_TRACE_SCOPE(TracePoint::GET_IMPL);
  assert(pinnable_val != nullptr);

  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();

  // Acquire SuperVersion
  SuperVersion* sv = GetAndRefSuperVersion(cfd);

  TEST_SYNC_POINT("DBImpl::GetImpl:1");
  TEST_SYNC_POINT("DBImpl::GetImpl:2");

  SequenceNumber snapshot;
  if (read_options.snapshot != nullptr) {
    snapshot =
        reinterpret_cast<const SnapshotImpl*>(read_options.snapshot)->number_;
  } else {
    // Since we get and reference the super version before getting
    // the snapshot number, without a mutex protection, it is possible
    // that a memtable switch happened in the middle and not all the
    // data for this snapshot is available. But it will contain all
    // the data available in the super version we have, which is also
    // a valid snapshot to read from.
    // We shouldn't get snapshot before finding and referencing the
    // super versipon because a flush happening in between may compact
    // away data for the snapshot, but the snapshot is earlier than the
    // data overwriting it, so users may see wrong results.
    snapshot = versions_->LastSequence();
  }

  STRESS_CHECK_SAVE(LAST_GET_SEQUENCE, snapshot);
  TEST_SYNC_POINT("DBImpl::GetImpl:3");
  TEST_SYNC_POINT("DBImpl::GetImpl:4");

  // Prepare to store a list of merge operations if merge occurs.
  MergeContext merge_context;
  RangeDelAggregator range_del_agg(cfd->internal_comparator(), snapshot);

  Status s;
  int ret = Status::kOk;
  // First look in the memtable, then in the immutable memtable (if any).
  // s is both in/out. When in, s could either be OK or MergeInProgress.
  // merge_operands will contain the sequence of merges in the latter case.
  LookupKey lkey(key, snapshot);

  bool skip_memtable = (read_options.read_tier == kPersistedTier &&
                        has_unpersisted_data_.load(std::memory_order_relaxed));
  bool done = false;
  if (!skip_memtable) {
    if (sv->mem->Get(lkey, pinnable_val->GetSelf(), &s, &merge_context,
                     &range_del_agg, read_options)) {
      done = true;
      pinnable_val->PinSelf();
      QUERY_COUNT(CountPoint::MEMTABLE_HIT);
    } else if ((s.ok() || s.IsMergeInProgress()) &&
               sv->imm->Get(lkey, pinnable_val->GetSelf(), &s, &merge_context,
                            &range_del_agg, read_options)) {
      done = true;
      pinnable_val->PinSelf();
      QUERY_COUNT(CountPoint::MEMTABLE_HIT);
    }
    if (!done && !s.ok() && !s.IsMergeInProgress()) {
      return s;
    }
  }

  STRESS_CHECK_SAVE(MEMTABLE_SEARCH_DONE, done);
  if (!done) {
    QUERY_COUNT(CountPoint::MEMTABLE_MISS);
  }
  // look up in row_cache
  IterKey* row_cache_key_ptr = nullptr;
  IterKey row_cache_key;
  if (!done && immutable_db_options_.row_cache) {
    //const XengineSchema *schema = cfd->get_schema();
    if (FAILED(get_from_row_cache(cfd, snapshot, key, row_cache_key, pinnable_val, done))) {
      s = Status(ret);
      XENGINE_LOG(WARN, "failed to get row from row_cache", K(ret));
    }
  }

  if (!done) {
    SequenceNumber key_seq = kMaxSequenceNumber;
    if (FAILED(cfd->get_from_storage_manager(read_options,
                                             *sv->current_meta_,
                                             lkey,
                                             *pinnable_val,
                                             done,
                                             &key_seq))) {
      s = Status(ret);
      if (Status::kNotFound != ret) {
        XENGINE_LOG(WARN, "fail to get from storage manager", K(ret));
      } else {
        //QUERY_COUNT(CountPoint::VALUE_NOT_FOUND);
        if (nullptr != value_found) {
          *value_found = done;
        }
      }
    }
    // insert into row cache
    if (s.ok() && immutable_db_options_.row_cache) {
      const char* val_data = nullptr;
      size_t val_size = 0;
      if (pinnable_val->IsPinned()) {
        val_data = pinnable_val->data();
        val_size = pinnable_val->size();
      } else if (nullptr != pinnable_val->GetSelf()) {
        val_data = pinnable_val->GetSelf()->data();
        val_size = pinnable_val->GetSelf()->size();
      }
      if (val_size) {
        // add into row cache
        if (FAILED(add_into_row_cache(
            val_data, val_size, snapshot, cfd, key_seq, row_cache_key))) {
          s = Status(ret);
          XENGINE_LOG(WARN, "failed to add into row cache", K(ret));
        }
      }
    }
  }

  ReturnAndCleanupSuperVersion(cfd, sv);

  QUERY_COUNT(CountPoint::NUMBER_KEYS_READ);
  QUERY_COUNT_ADD(CountPoint::NUMBER_BYTES_READ, pinnable_val->size());
  return s;
}

std::vector<Status> DBImpl::MultiGet(
    const ReadOptions& read_options,
    const std::vector<ColumnFamilyHandle*>& column_family,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  SequenceNumber snapshot;

  struct MultiGetColumnFamilyData {
    ColumnFamilyData* cfd;
    SuperVersion* super_version;
  };
  ArenaAllocator alloc(8 * 1024);
  std::unordered_map<uint32_t, MultiGetColumnFamilyData*> multiget_cf_data;
  // fill up and allocate outside of mutex
  for (auto cf : column_family) {
    auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(cf);
    auto cfd = cfh->cfd();
    if (multiget_cf_data.find(cfd->GetID()) == multiget_cf_data.end()) {
//      auto mgcfd = new MultiGetColumnFamilyData();
      auto mgcfd = ALLOC_OBJECT(MultiGetColumnFamilyData, alloc);
      mgcfd->cfd = cfd;
      multiget_cf_data.insert({cfd->GetID(), mgcfd});
    }
  }

  mutex_.Lock();
  if (read_options.snapshot != nullptr) {
    snapshot =
        reinterpret_cast<const SnapshotImpl*>(read_options.snapshot)->number_;
  } else {
    snapshot = versions_->LastSequence();
  }
  for (auto mgd_iter : multiget_cf_data) {
    mgd_iter.second->super_version =
        mgd_iter.second->cfd->GetSuperVersion()->Ref();
  }
  mutex_.Unlock();

  // Contain a list of merge operations if merge occurs.
  MergeContext merge_context;

  // Note: this always resizes the values array
  size_t num_keys = keys.size();
  std::vector<Status> stat_list(num_keys);
  values->resize(num_keys);

  // Keep track of bytes that we read for statistics-recording later
  uint64_t bytes_read = 0;

  // For each of the given keys, apply the entire "get" process as follows:
  // First look in the memtable, then in the immutable memtable (if any).
  // s is both in/out. When in, s could either be OK or MergeInProgress.
  // merge_operands will contain the sequence of merges in the latter case.
  for (size_t i = 0; i < num_keys; ++i) {
    merge_context.Clear();
    Status& s = stat_list[i];
    int ret = Status::kOk;
    std::string* value = &(*values)[i];

    LookupKey lkey(keys[i], snapshot);
    auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family[i]);
    RangeDelAggregator range_del_agg(cfh->cfd()->internal_comparator(),
                                     snapshot);
    auto mgd_iter = multiget_cf_data.find(cfh->cfd()->GetID());
    assert(mgd_iter != multiget_cf_data.end());
    auto mgd = mgd_iter->second;
    auto super_version = mgd->super_version;
    bool skip_memtable =
        (read_options.read_tier == kPersistedTier &&
         has_unpersisted_data_.load(std::memory_order_relaxed));
    bool done = false;
    if (!skip_memtable) {
      if (super_version->mem->Get(lkey, value, &s, &merge_context,
                                  &range_del_agg, read_options)) {
        done = true;
        // TODO(?): QUERY_COUNT(stats_, MEMTABLE_HIT)?
      } else if (super_version->imm->Get(lkey, value, &s, &merge_context,
                                         &range_del_agg, read_options)) {
        done = true;
        // TODO(?): QUERY_COUNT(stats_, MEMTABLE_HIT)?
      }
    }
    if (!done) {
      PinnableSlice pinnable_val;
      if (FAILED(mgd->cfd->get_from_storage_manager(read_options,
                                                    *super_version->current_meta_,
                                                    lkey,
                                                    pinnable_val,
                                                    done))) {
        s = Status(ret);
        if (Status::kNotFound != ret) {
          XENGINE_LOG(WARN, "fail to get from storage_manager", K(ret));
        } else {
          //TODO:yuanfeng
          ret = Status::kOk;
        }
      }
#if 0
      super_version->current->Get(read_options, lkey, &pinnable_val, &s,
                                  &merge_context, &range_del_agg);
#endif
      value->assign(pinnable_val.data(), pinnable_val.size());
      // TODO(?): QUERY_COUNT(stats_, MEMTABLE_MISS)?
    }

    if (s.ok()) {
      bytes_read += value->size();
    }
  }

  // Post processing (decrement reference counts and record statistics)
  autovector<SuperVersion*> superversions_to_delete;

  // TODO(icanadi) do we need lock here or just around Cleanup()?
  mutex_.Lock();
  for (auto mgd_iter : multiget_cf_data) {
    auto mgd = mgd_iter.second;
    if (mgd->super_version->Unref()) {
      mgd->cfd->release_meta_snapshot(mgd->super_version->current_meta_,
                                      &mutex_);
      mgd->super_version->Cleanup();
      superversions_to_delete.push_back(mgd->super_version);
    }
  }
  mutex_.Unlock();

  for (auto td : superversions_to_delete) {
    delete td;
  }
  for (auto mgd : multiget_cf_data) {
//    delete mgd.second;
    FREE_OBJECT(MultiGetColumnFamilyData, alloc, mgd.second);
  }

  return stat_list;
}

Status DBImpl::CreateColumnFamily(CreateSubTableArgs &args, ColumnFamilyHandle **handle)
{
  int ret = Status::kOk;
  ColumnFamilyData *cfd = nullptr;
  int64_t dummy_commit_lsn = 0;

  if (UNLIKELY(!args.is_valid())) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(args));
  } else if (FAILED(storage_logger_->begin(CREATE_INDEX))) {
    //TODO:yuanfeng fail?abort?
    XENGINE_LOG(WARN, "fail to begin trans", K(ret));
  } else {
    if (args.create_table_space_) {
      args.table_space_id_ = extent_space_manager_->allocate_table_space_id();
    }
    InstrumentedMutexLock guard(&mutex_);
    if (FAILED(versions_->add_sub_table(args, true, false /*is replay*/, cfd))) {
      XENGINE_LOG(WARN, "fail to add sub table", K(args));
    } else if (FAILED(storage_logger_->commit(dummy_commit_lsn))) {
      XENGINE_LOG(WARN, "fail to commit trans", K(ret));
      abort();
    } else {
      //install new super version
//      delete
      SuperVersion *old_sv = InstallSuperVersionAndScheduleWork(cfd, nullptr, *cfd->GetLatestMutableCFOptions());
      MOD_DELETE_OBJECT(SuperVersion, old_sv);
      auto *table_factory =  dynamic_cast<table::ExtentBasedTableFactory*>(cfd->ioptions()->table_factory);
      if (nullptr != table_factory) {
        BlockBasedTableOptions table_opts = table_factory->table_options();
        cfd->ioptions()->filter_manager->start_build_thread(
            cfd, this, &mutex_, &(versions_->env_options()), env_,
            table_opts.filter_policy, table_opts.block_cache,
            table_opts.whole_key_filtering,
            table_opts.cache_index_and_filter_blocks_with_high_priority,
            mutable_db_options_.filter_queue_stripes,
            mutable_db_options_.filter_building_threads,
            &filter_build_quota_);
      }
    }
  }

  if (SUCCED(ret)) {
    if (args.create_table_space_
        && FAILED(extent_space_manager_->create_table_space(args.table_space_id_))) {
      XENGINE_LOG(WARN, "fail to create table space", K(args));
    } else if (FAILED(extent_space_manager_->register_subtable(args.table_space_id_, cfd->GetID()))) {
      XENGINE_LOG(WARN, "fail to regirster subtable", K(ret), K(args));
    } else if (IS_NULL(*handle = MOD_NEW_OBJECT(ModId::kDBImpl, ColumnFamilyHandleImpl, cfd, this, &mutex_))) {
      ret = Status::kMemoryLimit;
      XENGINE_LOG(WARN, "fail to allocate memory for ColumnFamilyHandleImpl", K(ret));
    } else {
      XENGINE_LOG(INFO, "success to create subtbale", K(args));
    }
  }

  return ret;
}

Status DBImpl::DropColumnFamily(ColumnFamilyHandle *column_family)
{
  int ret = Status::kOk;
  ColumnFamilyData *cfd = nullptr;
  int64_t dummy_commit_lsn = 0;

  if (IS_NULL(column_family)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(column_family));
  } else if (IS_NULL(cfd = (reinterpret_cast<ColumnFamilyHandleImpl *>(column_family))->cfd())) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, cfd must not nullptr", K(ret));
  } else if (FAILED(storage_logger_->begin(DROP_INDEX))) {
    //TODO:yuanfeng abort?
    XENGINE_LOG(WARN, "fail to begin manifest trans", K(ret));
  } else {
    InstrumentedMutexLock guard(&mutex_);
    if (FAILED(versions_->remove_sub_table(cfd, true, false /*is replay*/))) {
      XENGINE_LOG(WARN, "fail to remove subtable", K(ret), "index_id", cfd->GetID());
    } else if (FAILED(storage_logger_->commit(dummy_commit_lsn))) {
      XENGINE_LOG(WARN, "fail to commit trans", K(ret));
      abort();
    } else {
      schedule_pending_gc(cfd);
      XENGINE_LOG(INFO, "success to remove subtable", K(cfd->GetID()));
    }
  }

  return ret;
}
/*
Status DBImpl::CreateColumnFamily(const ColumnFamilyOptions& cf_options,
                                  const std::string& column_family_name,
                                  ColumnFamilyHandle** handle) {
  Status s;
  *handle = nullptr;

  s = CheckCompressionSupported(cf_options);
  if (s.ok() && immutable_db_options_.allow_concurrent_memtable_write) {
    s = CheckConcurrentWritesSupported(cf_options);
  }
  if (!s.ok()) {
    return s;
  }

  {
    InstrumentedMutexLock l(&mutex_);

    if (versions_->GetColumnFamilySet()->GetColumnFamily(column_family_name) !=
        nullptr) {
      return Status::InvalidArgument("sub table already exists");
    }
    //VersionEdit edit;
    //edit.AddColumnFamily(column_family_name);
    //uint32_t new_id = versions_->GetColumnFamilySet()->GetNextColumnFamilyID();
    //edit.SetColumnFamily(new_id);
    //edit.SetLogNumber(logfile_number_);
    //edit.SetComparatorName(cf_options.comparator->Name());

    // LogAndApply will both write the creation in MANIFEST and create
    // ColumnFamilyData object
    {  // write thread
      this->wait_all_active_thread_exit();
      // LogAndApply will both write the creation in MANIFEST and create
      // ColumnFamilyData object
      //s = versions_->LogAndApply(nullptr, MutableCFOptions(cf_options), &edit,
      //                           &mutex_, directories_.GetDbDir(), false,
      //                           &cf_options);
    }
    if (s.ok()) {
      single_column_family_mode_ = false;
      auto* cfd =
          versions_->GetColumnFamilySet()->GetColumnFamily(column_family_name);
      assert(cfd != nullptr);
      delete InstallSuperVersionAndScheduleWork(
          cfd, nullptr, *cfd->GetLatestMutableCFOptions());

      if (!cfd->mem()->IsSnapshotSupported()) {
        is_snapshot_supported_ = false;
      }
      auto* table_factory = dynamic_cast<table::ExtentBasedTableFactory*>(
          cfd->ioptions()->table_factory);
      if (table_factory != nullptr) {
        BlockBasedTableOptions table_opts = table_factory->table_options();
        cfd->ioptions()->filter_manager->start_build_thread(
            cfd, this, &mutex_, &(versions_->env_options()), env_,
            table_opts.filter_policy, table_opts.block_cache,
            table_opts.whole_key_filtering,
            table_opts.cache_index_and_filter_blocks_with_high_priority,
            mutable_db_options_.filter_queue_stripes,
            mutable_db_options_.filter_building_threads,
            &filter_build_quota_);
        block_cache_ = table_opts.block_cache;
      }

      *handle = new ColumnFamilyHandleImpl(cfd, this, &mutex_);
      __XENGINE_LOG(INFO,
                     "Created sub table [%s] (ID %u)",
                     column_family_name.c_str(), (unsigned)cfd->GetID());
    } else {
      __XENGINE_LOG(ERROR,
                      "Creating sub table [%s] FAILED -- %s",
                      column_family_name.c_str(), s.ToString().c_str());
    }
  }  // InstrumentedMutexLock l(&mutex_)

  // this is outside the mutex
  if (s.ok()) {
    NewThreadStatusCfInfo(
        reinterpret_cast<ColumnFamilyHandleImpl*>(*handle)->cfd());
  }
  return s;
}
*/

/*
Status DBImpl::DropColumnFamily(ColumnFamilyHandle* column_family) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();
  if (cfd->GetID() == 0) {
    return Status::InvalidArgument("Can't drop default sub table");
  }

  bool cf_support_snapshot = cfd->mem()->IsSnapshotSupported();

  //VersionEdit edit;
  //edit.DropColumnFamily();
  //edit.SetColumnFamily(cfd->GetID());

  Status s;
  {
    InstrumentedMutexLock l(&mutex_);
    if (cfd->IsDropped()) {
      s = Status::InvalidArgument("sub table already dropped!\n");
    }
    if (s.ok()) {
      // we drop sub table from a single write thread
      this->wait_all_active_thread_exit();
      //s = versions_->LogAndApply(cfd, *cfd->GetLatestMutableCFOptions(), &edit,
      //                           &mutex_);
    }

    if (!cf_support_snapshot) {
      // Dropped Column Family doesn't support snapshot. Need to recalculate
      // is_snapshot_supported_.
      bool new_is_snapshot_supported = true;
      for (auto c : *versions_->GetColumnFamilySet()) {
        if (!c->IsDropped() && !c->mem()->IsSnapshotSupported()) {
          new_is_snapshot_supported = false;
          break;
        }
      }
      is_snapshot_supported_ = new_is_snapshot_supported;
    }
  }

  if (s.ok()) {
    // Note that here we erase the associated cf_info of the to-be-dropped
    // cfd before its ref-count goes to zero to avoid having to erase cf_info
    // later inside db_mutex.
    EraseThreadStatusCfInfo(cfd);
    assert(cfd->IsDropped());
    auto* mutable_cf_options = cfd->GetLatestMutableCFOptions();
    max_total_in_memory_state_ -= mutable_cf_options->write_buffer_size *
                                  mutable_cf_options->max_write_buffer_number;
    __XENGINE_LOG(INFO,
                   "Dropped sub table with id %u\n", cfd->GetID());
  } else {
    __XENGINE_LOG(ERROR, "Dropping sub table with id %u FAILED -- %s\n", cfd->GetID(), s.ToString().c_str());
  }

  return s;
}
*/

bool DBImpl::KeyMayExist(const ReadOptions& read_options,
                         ColumnFamilyHandle* column_family, const Slice& key,
                         std::string* value, bool* value_found) {
  assert(value != nullptr);
  if (value_found != nullptr) {
    // falsify later if key-may-exist but can't fetch value
    *value_found = true;
  }
  ReadOptions roptions = read_options;
  roptions.read_tier = kBlockCacheTier;  // read from block cache only
  PinnableSlice pinnable_val;
  auto s = GetImpl(roptions, column_family, key, &pinnable_val, value_found);
  value->assign(pinnable_val.data(), pinnable_val.size());

  // If block_cache is enabled and the index block of the table didn't
  // not present in block_cache, the return value will be Status::Incomplete.
  // In this case, key may still exist in the table.
  return s.ok() || s.IsIncomplete();
}

Iterator* DBImpl::NewIterator(const ReadOptions& read_options,
                              ColumnFamilyHandle* column_family) {
  QUERY_TRACE_SCOPE(TracePoint::DB_ITER_CREATE);
  if (read_options.read_tier == kPersistedTier) {
    return NewErrorIterator(Status::NotSupported(
        "ReadTier::kPersistedData is not yet supported in iterators."));
  }
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();
  if (read_options.managed) {
#ifdef ROCKSDB_LITE
    // not supported in lite version
    return NewErrorIterator(Status::InvalidArgument(
        "Managed Iterators not supported in RocksDBLite."));
#else
    if ((read_options.tailing) || (read_options.snapshot != nullptr) ||
        (is_snapshot_supported_)) {
      return new ManagedIterator(this, read_options, cfd);
    }
    // Managed iter not supported
    return NewErrorIterator(Status::InvalidArgument(
        "Managed Iterators not supported without snapshots."));
#endif
  } else if (read_options.tailing) {
#ifdef ROCKSDB_LITE
    // not supported in lite version
    return nullptr;
#else
    SuperVersion* sv = cfd->GetReferencedSuperVersion(&mutex_);
    auto iter = new ForwardIterator(this, read_options, cfd, sv);
    return NewDBIterator(
        env_, read_options, *cfd->ioptions(), cfd->user_comparator(), iter,
        kMaxSequenceNumber,
        sv->mutable_cf_options.max_sequential_skip_in_iterations,
        sv->version_number, nullptr, extent_space_manager_);
#endif
  } else {
    QUERY_TRACE_BEGIN(TracePoint::DB_ITER_NEW_OBJECT);
    SequenceNumber latest_snapshot = versions_->LastSequence();
    SuperVersion* sv = cfd->GetReferencedSuperVersion(&mutex_);

    auto snapshot =
        read_options.snapshot != nullptr
            ? reinterpret_cast<const SnapshotImpl*>(read_options.snapshot)
                  ->number_
            : latest_snapshot;
    STRESS_CHECK_SAVE(LAST_ITER_SEQUENCE, snapshot);

    // Try to generate a DB iterator tree in continuous memory area to be
    // cache friendly. Here is an example of result:
    // +-------------------------------+
    // |                               |
    // | ArenaWrappedDBIter            |
    // |  +                            |
    // |  +---> Inner Iterator   ------------+
    // |  |                            |     |
    // |  |    +-- -- -- -- -- -- -- --+     |
    // |  +--- | Arena                 |     |
    // |       |                       |     |
    // |          Allocated Memory:    |     |
    // |       |   +-------------------+     |
    // |       |   | DBIter            | <---+
    // |           |  +                |
    // |       |   |  +-> iter_  ------------+
    // |       |   |                   |     |
    // |       |   +-------------------+     |
    // |       |   | MergingIterator   | <---+
    // |           |  +                |
    // |       |   |  +->child iter1  ------------+
    // |       |   |  |                |          |
    // |           |  +->child iter2  ----------+ |
    // |       |   |  |                |        | |
    // |       |   |  +->child iter3  --------+ | |
    // |           |                   |      | | |
    // |       |   +-------------------+      | | |
    // |       |   | Iterator1         | <--------+
    // |       |   +-------------------+      | |
    // |       |   | Iterator2         | <------+
    // |       |   +-------------------+      |
    // |       |   | Iterator3         | <----+
    // |       |   +-------------------+
    // |       |                       |
    // +-------+-----------------------+
    //
    // ArenaWrappedDBIter inlines an arena area where all the iterators in
    // the iterator tree are allocated in the order of being accessed when
    // querying.
    // Laying out the iterators in the order of being accessed makes it more
    // likely that any iterator pointer is close to the iterator it points to so
    // that they are likely to be in the same cache line and/or page.
    ArenaWrappedDBIter* db_iter = NewArenaWrappedDbIterator(
        env_, read_options, *cfd->ioptions(), cfd->user_comparator(), snapshot,
        sv->mutable_cf_options.max_sequential_skip_in_iterations,
        sv->version_number, extent_space_manager_);
    QUERY_TRACE_END();

    InternalIterator* internal_iter =
        NewInternalIterator(read_options, cfd, sv, db_iter->GetArena(),
                            db_iter->GetRangeDelAggregator());
    db_iter->SetIterUnderDBIter(internal_iter);

    return db_iter;
  }
  // To stop compiler from complaining
  return nullptr;
}

Status DBImpl::NewIterators(
    const ReadOptions& read_options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
  QUERY_TRACE_SCOPE(TracePoint::DB_ITER_CREATE);
  if (read_options.read_tier == kPersistedTier) {
    return Status::NotSupported(
        "ReadTier::kPersistedData is not yet supported in iterators.");
  }
  iterators->clear();
  iterators->reserve(column_families.size());
  if (read_options.managed) {
#ifdef ROCKSDB_LITE
    return Status::InvalidArgument(
        "Managed interator not supported in RocksDB lite");
#else
    if ((!read_options.tailing) && (read_options.snapshot == nullptr) &&
        (!is_snapshot_supported_)) {
      return Status::InvalidArgument(
          "Managed interator not supported without snapshots");
    }
    for (auto cfh : column_families) {
      auto cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(cfh)->cfd();
      auto iter = new ManagedIterator(this, read_options, cfd);
      iterators->push_back(iter);
    }
#endif
  } else if (read_options.tailing) {
#ifdef ROCKSDB_LITE
    return Status::InvalidArgument(
        "Tailing interator not supported in RocksDB lite");
#else
    for (auto cfh : column_families) {
      auto cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(cfh)->cfd();
      SuperVersion* sv = cfd->GetReferencedSuperVersion(&mutex_);
      auto iter = new ForwardIterator(this, read_options, cfd, sv);
      iterators->push_back(NewDBIterator(
          env_, read_options, *cfd->ioptions(), cfd->user_comparator(), iter,
          kMaxSequenceNumber,
          sv->mutable_cf_options.max_sequential_skip_in_iterations,
          sv->version_number, nullptr, extent_space_manager_));
    }
#endif
  } else {
    SequenceNumber latest_snapshot = versions_->LastSequence();

    for (size_t i = 0; i < column_families.size(); ++i) {
      auto* cfd =
          reinterpret_cast<ColumnFamilyHandleImpl*>(column_families[i])->cfd();
      SuperVersion* sv = cfd->GetReferencedSuperVersion(&mutex_);

      auto snapshot =
          read_options.snapshot != nullptr
              ? reinterpret_cast<const SnapshotImpl*>(read_options.snapshot)
                    ->number_
              : latest_snapshot;

      ArenaWrappedDBIter* db_iter = NewArenaWrappedDbIterator(
          env_, read_options, *cfd->ioptions(), cfd->user_comparator(),
          snapshot, sv->mutable_cf_options.max_sequential_skip_in_iterations,
          sv->version_number);
      InternalIterator* internal_iter =
          NewInternalIterator(read_options, cfd, sv, db_iter->GetArena(),
                              db_iter->GetRangeDelAggregator());
      db_iter->SetIterUnderDBIter(internal_iter);
      iterators->push_back(db_iter);
    }
  }

  return Status::OK();
}

const Snapshot* DBImpl::GetSnapshot() { return GetSnapshotImpl(false); }

#ifndef ROCKSDB_LITE
const Snapshot* DBImpl::GetSnapshotForWriteConflictBoundary() {
  return GetSnapshotImpl(true);
}
#endif  // ROCKSDB_LITE

/* Get/Release Snapshot Optimize */
/*
 * Split snapshot lists into MAX_SNAP, and one list is protected by one
 * corresponding mutex.
 *
 * For Get Snapshot, try to lock one list, if all failed, then lock the
 * first list.
 * Then try to do list.newest->ref++ if list is not empty, else create a
 * new node and insert into list as the newest node.
 * Last, unlock the list
 *
 * For Release Snapshot, do node->ref-- and check if it's 0, means nobody
 * is using this node.
 * If 0, then lock the list, and Delete this node, Unlock list, free node
 * if needed.
 *
 * Some trickery issues:
 * 1. As release first do node->ref-- without lock protected, then after
 * decrease, ref == 0, but during this period, Get Snapshot has also get
 * this node and do node->ref++, and also release this node node->ref--.
 * At this time, two threads are both waiting lock with ref == 0, both
 * of them will DELETE node from list.
 *
 * Solution: For Get Snapshot, if node->ref++ == 1, which means some thread
 * called Release and waiting Lock to Delete node, then revert it and create
 * a new node.
 *
 * 2. For Release Snapshot, only one thread can be do Delete, granted above
 *
 */
#if 0
const Snapshot* DBImpl::GetSnapshotImpl(bool is_write_conflict_boundary) {
  int64_t unix_time = 0;
  env_->GetCurrentTime(&unix_time);  // Ignore error
  SnapshotImpl* s = new SnapshotImpl;

  InstrumentedMutexLock l(&mutex_);
  // returns null if the underlying memtable does not support snapshot.
  if (!is_snapshot_supported_) {
    delete s;
    return nullptr;
  }
  return snapshots_.New(s, versions_->LastSequence(), unix_time,
                        is_write_conflict_boundary);
}
#endif

const Snapshot* DBImpl::GetSnapshotImpl(bool is_write_conflict_boundary) {
  int64_t unix_time = 0;
  env_->GetCurrentTime(&unix_time);  // Ignore error
  SnapshotImpl* s = NULL;
  SnapshotImpl* head = NULL;
  const SnapshotImpl* ns = NULL;
  int64_t n_ref = 0;

  // returns null if the underlying memtable does not support snapshot.
  if (!is_snapshot_supported_) return nullptr;

  uint32_t idx = 0;
  while (idx < MAX_SNAP) {
    if (snap_mutex[idx].TryLock() == 0) break;
    idx++;
  }
  if (idx == MAX_SNAP) {
    idx = 0;
    snap_mutex[idx].Lock();
  }

  bool need_create = true;
  if (!snap_lists_[idx].empty()) {
    head = snap_lists_[idx].newest();
    if (head->number_ == versions_->LastSequence()) {
      if (0 == (n_ref = head->ref_.fetch_add(1, std::memory_order_seq_cst))) {
        head->ref_.fetch_sub(1, std::memory_order_seq_cst);
      } else if (0 < n_ref) {
        assert(head->ref_.load() > 0);
        if (head->ref_.load() <= 0) assert(0);
        ns = head;
        need_create = false;
      } else {
        assert(0);
      }
    }
  }

  if (need_create) {
//    s = new SnapshotImpl;
    s = MOD_NEW_OBJECT(ModId::kSnapshotImpl, SnapshotImpl);
    ns = snap_lists_[idx].New(s, versions_->LastSequence(), unix_time,
                              is_write_conflict_boundary, idx);
  }

  snap_mutex[idx].Unlock();

  return ns;
}

#if 0
void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  const SnapshotImpl* casted_s = reinterpret_cast<const SnapshotImpl*>(s);
  {
    InstrumentedMutexLock l(&mutex_);
    snapshots_.Delete(casted_s);
  }
  delete casted_s;
}
#endif

void DBImpl::ReleaseSnapshot(const Snapshot* s) {
  bool to_del = 0;
  int64_t n_ref = 0;

  SnapshotImpl* casted_s =
      const_cast<SnapshotImpl*>(reinterpret_cast<const SnapshotImpl*>(s));

  assert(casted_s->ref_.load() >= 1);


  if (1 == (n_ref = casted_s->ref_.fetch_sub(1, std::memory_order_seq_cst))) {
    uint32_t pos = casted_s->pos();
    assert(pos < MAX_SNAP);
    // only one thread comes here
    snap_mutex[pos].Lock();
    assert(casted_s->ref_.load() == 0);
    snap_lists_[pos].Delete(casted_s);
    to_del = true;
    snap_mutex[pos].Unlock();
  } else {
    assert(n_ref > 1);
  }

//  if (to_del) delete casted_s;
  if (to_del) {
    MOD_DELETE_OBJECT(SnapshotImpl, casted_s);
  }
}

#ifndef ROCKSDB_LITE
Status DBImpl::GetPropertiesOfAllTables(ColumnFamilyHandle* column_family,
                                        TablePropertiesCollection* props) {
  Status s;
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();

  /*
  // Increment the ref count
  mutex_.Lock();
  auto version = cfd->current();
  version->Ref();
  mutex_.Unlock();

  auto s = version->GetPropertiesOfAllTables(props);

  // Decrement the ref count
  mutex_.Lock();
  version->Unref();
  mutex_.Unlock();
  */
  return s;
}

Status DBImpl::GetPropertiesOfTablesInRange(ColumnFamilyHandle* column_family,
                                            const Range* range, std::size_t n,
                                            TablePropertiesCollection* props) {
  Status s;
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();

  /*
  // Increment the ref count
  mutex_.Lock();
  auto version = cfd->current();
  version->Ref();
  mutex_.Unlock();

  auto s = version->GetPropertiesOfTablesInRange(range, n, props);

  // Decrement the ref count
  mutex_.Lock();
  version->Unref();
  mutex_.Unlock();
  */
  return s;
}

#endif  // ROCKSDB_LITE

const std::string& DBImpl::GetName() const { return dbname_; }

Env* DBImpl::GetEnv() const { return env_; }

Options DBImpl::GetOptions(ColumnFamilyHandle* column_family) const {
  InstrumentedMutexLock l(&mutex_);
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  return Options(BuildDBOptions(immutable_db_options_, mutable_db_options_),
                 cfh->cfd()->GetLatestCFOptions());
}

DBOptions DBImpl::GetDBOptions() const {
  InstrumentedMutexLock l(&mutex_);
  return BuildDBOptions(immutable_db_options_, mutable_db_options_);
}

bool DBImpl::GetProperty(ColumnFamilyHandle* column_family,
                         const Slice& property, std::string* value) {
  const DBPropertyInfo* property_info = GetPropertyInfo(property);
  value->clear();
  auto cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();
  if (property_info == nullptr) {
    return false;
  } else if (property_info->handle_int) {
    uint64_t int_value;
    bool ret_value =
        GetIntPropertyInternal(cfd, *property_info, false, &int_value);
    if (ret_value) {
      *value = ToString(int_value);
    }
    return ret_value;
  } else if (property_info->handle_string) {
    InstrumentedMutexLock l(&mutex_);
    return cfd->internal_stats()->GetStringProperty(*property_info, property,
                                                    value, this);
  }
  // Shouldn't reach here since exactly one of handle_string and handle_int
  // should be non-nullptr.
  assert(false);
  return false;
}

bool DBImpl::GetMapProperty(ColumnFamilyHandle* column_family,
                            const Slice& property,
                            std::map<std::string, double>* value) {
  const DBPropertyInfo* property_info = GetPropertyInfo(property);
  value->clear();
  auto cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();
  if (property_info == nullptr) {
    return false;
  } else if (property_info->handle_map) {
    InstrumentedMutexLock l(&mutex_);
    return cfd->internal_stats()->GetMapProperty(*property_info, property,
                                                 value);
  }
  // If we reach this point it means that handle_map is not provided for the
  // requested property
  return false;
}

bool DBImpl::GetIntProperty(ColumnFamilyHandle* column_family,
                            const Slice& property, uint64_t* value) {
  const DBPropertyInfo* property_info = GetPropertyInfo(property);
  if (property_info == nullptr || property_info->handle_int == nullptr) {
    return false;
  }
  auto cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();
  return GetIntPropertyInternal(cfd, *property_info, false, value);
}

bool DBImpl::GetIntPropertyInternal(ColumnFamilyData* cfd,
                                    const DBPropertyInfo& property_info,
                                    bool is_locked, uint64_t* value) {
  assert(property_info.handle_int != nullptr);
  if (!property_info.need_out_of_mutex) {
    if (is_locked) {
      mutex_.AssertHeld();
      return cfd->internal_stats()->GetIntProperty(property_info, value, this);
    } else {
      InstrumentedMutexLock l(&mutex_);
      return cfd->internal_stats()->GetIntProperty(property_info, value, this);
    }
  } else {
    SuperVersion* sv = nullptr;
    if (!is_locked) {
      sv = GetAndRefSuperVersion(cfd);
    } else {
      sv = cfd->GetSuperVersion();
    }

    /*
    bool ret = cfd->internal_stats()->GetIntPropertyOutOfMutex(
        property_info, sv->current, value);
    */
    if (!is_locked) {
      ReturnAndCleanupSuperVersion(cfd, sv);
    }

    //return ret;
    return true;
  }
}

#ifndef ROCKSDB_LITE
Status DBImpl::ResetStats() {
  InstrumentedMutexLock l(&mutex_);
  int ret = Status::kOk;
  int tmp_ret = Status::kOk;
  GlobalContext *global_ctx = nullptr;
  AllSubTable *all_sub_table = nullptr;
  SubTable *sub_table = nullptr;
  if (nullptr == (global_ctx = versions_->get_global_ctx())) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, global ctx must not nullptr", K(ret));
  } else if (FAILED(global_ctx->acquire_thread_local_all_sub_table(all_sub_table))) {
    XENGINE_LOG(WARN, "fail to acquire all sub table", K(ret));
  } else if (nullptr == all_sub_table) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, all sub table must not nullptr", K(ret));
  } else {
    SubTableMap &all_sub_tables = all_sub_table->sub_table_map_;
    for (auto iter = all_sub_tables.begin(); Status::kOk == ret && iter != all_sub_tables.end(); ++iter) {
      if (nullptr == (sub_table = iter->second)) {
        ret = Status::kCorruption;
        XENGINE_LOG(WARN, "subtable must not nullptr", K(ret));
      } else {
        sub_table->internal_stats()->Clear();
      }
    }
  }

  //there will cover the error code, by design
  tmp_ret = ret;
  if (nullptr != global_ctx && FAILED(global_ctx->release_thread_local_all_sub_table(all_sub_table))) {
    XENGINE_LOG(WARN, "fail to release all sub table", K(ret), K(tmp_ret));
  }
  return Status(ret);
}
#endif  // ROCKSDB_LITE

bool DBImpl::GetAggregatedIntProperty(const Slice& property,
                                      uint64_t* aggregated_value) {
 int ret = Status::kOk;
  bool bool_ret = false;
  int tmp_ret = Status::kOk;
  GlobalContext *global_ctx = nullptr;
  AllSubTable *all_sub_table = nullptr;
  SubTable *sub_table = nullptr;
  uint64_t sum = 0;
  uint64_t value = 0;
  const DBPropertyInfo* property_info = GetPropertyInfo(property);
  if (property_info == nullptr || property_info->handle_int == nullptr) {
    bool_ret = false;
  } else if (nullptr == (global_ctx = versions_->get_global_ctx())) {
    ret = Status::kErrorUnexpected;
    bool_ret = false;
    XENGINE_LOG(WARN, "unexpected erroe, global ctx must not nullptr", K(ret));
  } else if (FAILED(global_ctx->acquire_thread_local_all_sub_table(all_sub_table))) {
    bool_ret = false;
    XENGINE_LOG(WARN, "fail to acquire all sub table", K(ret));
  } else if (nullptr == all_sub_table) {
    ret = Status::kErrorUnexpected;
    bool_ret = false;
    XENGINE_LOG(WARN, "unexpected error, all sub table must not nullptr", K(ret));
  } else {
    SubTableMap &all_sub_tables = all_sub_table->sub_table_map_;
    for (auto iter = all_sub_tables.begin(); bool_ret && iter != all_sub_tables.end(); ++iter) {                                                                                            
      if (nullptr == (sub_table = iter->second)) {
        ret = Status::kErrorUnexpected;
        bool_ret = false;
        XENGINE_LOG(WARN, "subtable must not nullptr", K(ret), "index_id", iter->first);
      } else if (!GetIntPropertyInternal(sub_table, *property_info, true, &value))  {
        bool_ret = false;
      } else {
        sum += value;
      }
    }
    *aggregated_value = sum;
  }

  //there will cover the error code, by design
  tmp_ret = ret;
  if (nullptr != global_ctx && FAILED(global_ctx->release_thread_local_all_sub_table(all_sub_table))) {                                                                                     
    bool_ret = false;
    XENGINE_LOG(WARN, "fail to release all sub table", K(ret), K(tmp_ret));
  }
  return bool_ret;
}

SuperVersion* DBImpl::GetAndRefSuperVersion(ColumnFamilyData* cfd) {
  // TODO(ljin): consider using GetReferencedSuperVersion() directly
  return cfd->GetThreadLocalSuperVersion(&mutex_);
}

// REQUIRED: this function should only be called on the write thread or if the
// mutex is held.
SuperVersion* DBImpl::GetAndRefSuperVersion(uint32_t column_family_id) {
  GlobalContext *global_ctx = nullptr;
  SubTable *sub_table = nullptr;
  SuperVersion * super_version = nullptr;
  if (nullptr == (global_ctx = versions_->get_global_ctx())) {
    XENGINE_LOG(WARN, "global ctx must not nullptr");
  } else {
    auto iter = global_ctx->all_sub_table_->sub_table_map_.find(column_family_id);
    if (global_ctx->all_sub_table_->sub_table_map_.end() == iter) {
      XENGINE_LOG(DEBUG, "sub table not exist", K(column_family_id));
    } else if (nullptr == (sub_table = iter->second)) {
      XENGINE_LOG(WARN, "subtable must not nullptr", K(column_family_id));
    } else {
      super_version = GetAndRefSuperVersion(sub_table);
    }
  }

  return super_version;
}

void DBImpl::ReturnAndCleanupSuperVersion(ColumnFamilyData* cfd,
                                          SuperVersion* sv) {
  bool unref_sv = !cfd->ReturnThreadLocalSuperVersion(sv);

  if (unref_sv) {
    // Release SuperVersion
    if (sv->Unref()) {
      {
        InstrumentedMutexLock l(&mutex_);
        if (sv->current_meta_) {
          cfd->release_meta_snapshot(sv->current_meta_,
                                     &mutex_);
        }
        sv->Cleanup();
      }
//      delete sv;
      MOD_DELETE_OBJECT(SuperVersion, sv);
      QUERY_COUNT(CountPoint::NUMBER_SUPERVERSION_CLEANUPS);
    }
    QUERY_COUNT(CountPoint::NUMBER_SUPERVERSION_RELEASES);
  }
}

// REQUIRED: this function should only be called on the write thread.
void DBImpl::ReturnAndCleanupSuperVersion(uint32_t column_family_id,
                                          SuperVersion* sv) {
  // If SuperVersion is held, and we successfully fetched a cfd using
  // GetAndRefSuperVersion(), it must still exist.
  int ret = Status::kOk;
  int tmp_ret = Status::kOk;
  GlobalContext *global_ctx = nullptr;
  AllSubTable *all_sub_table = nullptr;
  SubTable *sub_table = nullptr;
  if (nullptr == (global_ctx = versions_->get_global_ctx())) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "global ctx must not nullptr", K(ret));
  } else if (FAILED(global_ctx->acquire_thread_local_all_sub_table(all_sub_table))) {
    XENGINE_LOG(WARN, "fail to acquire all sub table", K(ret));
  } else if (nullptr == all_sub_table) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, all sub table must not nullptr", K(ret));
  } else {
    auto iter = all_sub_table->sub_table_map_.find(column_family_id);
    if (global_ctx->all_sub_table_->sub_table_map_.end() == iter) {
      XENGINE_LOG(WARN, "subtable not exist", K(column_family_id));
      assert(nullptr != sub_table);
    } else if (nullptr == (sub_table = iter->second)) {
      XENGINE_LOG(WARN, "subtable must not nullptr", K(column_family_id));
      assert(nullptr != sub_table);
    } else {
      ReturnAndCleanupSuperVersion(sub_table, sv);
    }
  }

  //there will cover the error code, by design
  tmp_ret = ret;
  if (nullptr != global_ctx && FAILED(global_ctx->release_thread_local_all_sub_table(all_sub_table))) {
    XENGINE_LOG(WARN, "fail to releaser all sub table", K(ret), K(tmp_ret));
  }
}

// return the all subtable handles to caller to process
// COUTION: caller need to release the handles
int DBImpl::get_all_subtable(
    std::vector<xengine::db::ColumnFamilyHandle*>& subtables) const
{
  int ret = Status::kOk;
  int tmp_ret = Status::kOk;
  GlobalContext* global_ctx = nullptr;
  AllSubTable* all_sub_table = nullptr;
  ColumnFamilyHandleImpl* handle = nullptr;

  if (nullptr == (global_ctx = versions_->get_global_ctx())) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, global ctx must not nullptr", K(ret));
  } else if (FAILED(global_ctx->acquire_thread_local_all_sub_table(
                 all_sub_table))) {
    XENGINE_LOG(WARN, "fail to acquire all sub table", K(ret));
  } else if (nullptr == all_sub_table) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, all sub table must not nullptr",
                K(ret));
  } else {
    for (auto item : all_sub_table->sub_table_map_) {
      handle =
          MOD_NEW_OBJECT(ModId::kInformationSchema, ColumnFamilyHandleImpl,
                         item.second, const_cast<DBImpl*>(this), &mutex_);
      subtables.emplace_back(
          static_cast<xengine::db::ColumnFamilyHandle*>(handle));
    }
  }

  // there will cover the error code, by design
  tmp_ret = ret;
  if (nullptr != global_ctx &&
      FAILED(global_ctx->release_thread_local_all_sub_table(all_sub_table))) {
    XENGINE_LOG(WARN, "fail to release all sub table", K(ret), K(tmp_ret));
  }

  return ret;
}

int DBImpl::return_all_subtable(std::vector<xengine::db::ColumnFamilyHandle*> &subtables) {
  // release the reference and space
  ColumnFamilyHandleImpl *tmp_ptr = nullptr;
  for (auto st_handle : subtables) {
    if (nullptr == st_handle) {
      continue;
    }
    tmp_ptr = static_cast<ColumnFamilyHandleImpl*>(st_handle);
    MOD_DELETE_OBJECT(ColumnFamilyHandleImpl, tmp_ptr);
  }

  return Status::kOk;
}

void DBImpl::GetApproximateMemTableStats(ColumnFamilyHandle* column_family,
                                         const Range& range,
                                         uint64_t* const count,
                                         uint64_t* const size) {
  QUERY_TRACE_SCOPE(TracePoint::DB_APPROXIMATE_MEM_SIZE);
  ColumnFamilyHandleImpl* cfh =
      reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  ColumnFamilyData* cfd = cfh->cfd();
  SuperVersion* sv = GetAndRefSuperVersion(cfd);

  // Convert user_key into a corresponding internal key.
  InternalKey k1(range.start, kMaxSequenceNumber, kValueTypeForSeek);
  InternalKey k2(range.limit, kMaxSequenceNumber, kValueTypeForSeek);
  MemTable::MemTableStats memStats =
      sv->mem->ApproximateStats(k1.Encode(), k2.Encode());
  MemTable::MemTableStats immStats =
      sv->imm->ApproximateStats(k1.Encode(), k2.Encode());
  *count = memStats.count + immStats.count;
  *size = memStats.size + immStats.size;

  ReturnAndCleanupSuperVersion(cfd, sv);
}

void DBImpl::GetApproximateSizes(ColumnFamilyHandle* column_family,
                                 const Range* range, int n, uint64_t* sizes,
                                 uint8_t include_flags) {
  QUERY_TRACE_SCOPE(TracePoint::DB_APPROXIMATE_SIZE);
  assert(include_flags & DB::SizeApproximationFlags::INCLUDE_FILES ||
         include_flags & DB::SizeApproximationFlags::INCLUDE_MEMTABLES);
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();
  SuperVersion* sv = GetAndRefSuperVersion(cfd);

  for (int i = 0; i < n; i++) {
    // Convert user_key into a corresponding internal key.
    InternalKey k1(range[i].start, kMaxSequenceNumber, kValueTypeForSeek);
    InternalKey k2(range[i].limit, kMaxSequenceNumber, kValueTypeForSeek);
    sizes[i] = 0;
    if (include_flags & DB::SizeApproximationFlags::INCLUDE_FILES) {
      sizes[i] += versions_->ApproximateSize(cfd, sv->current_meta_,
                                             k1.Encode(), k2.Encode(),
                                             0 /*start_level*/, 3 /*end_level*/,
                                             mutable_db_options_.estimate_cost_depth);
    }
    if (include_flags & DB::SizeApproximationFlags::INCLUDE_MEMTABLES) {
      sizes[i] += sv->mem->ApproximateStats(k1.Encode(), k2.Encode()).size;
      sizes[i] += sv->imm->ApproximateStats(k1.Encode(), k2.Encode()).size;
    }
  }

  ReturnAndCleanupSuperVersion(cfd, sv);
}

std::list<uint64_t>::iterator
DBImpl::CaptureCurrentFileNumberInPendingOutputs() {
  // We need to remember the iterator of our insert, because after the
  // background job is done, we need to remove that element from
  // pending_outputs_.
  //pending_outputs_.push_back(versions_->current_next_file_number());
  auto pending_outputs_inserted_elem = pending_outputs_.end();
  --pending_outputs_inserted_elem;
  return pending_outputs_inserted_elem;
}

void DBImpl::ReleaseFileNumberFromPendingOutputs(
    std::list<uint64_t>::iterator v) {
  pending_outputs_.erase(v);
}

#ifndef ROCKSDB_LITE
Status DBImpl::GetUpdatesSince(
    SequenceNumber seq, unique_ptr<TransactionLogIterator>* iter,
    const TransactionLogIterator::ReadOptions& read_options) {
  QUERY_COUNT(CountPoint::GET_UPDATES_SINCE_CALLS);
  if (seq > versions_->LastSequence()) {
    return Status::NotFound("Requested sequence not yet written in the db");
  }
  return wal_manager_.GetUpdatesSince(seq, iter, read_options, versions_.get());
}

Status DBImpl::DeleteFile(std::string name) {
  assert(false);
  uint64_t number;
  FileType type;
  WalFileType log_type;
  if (!ParseFileName(name, &number, &type, &log_type) ||
      (type != kTableFile && type != kLogFile)) {
    __XENGINE_LOG(ERROR, "DeleteFile %s failed.\n",
                    name.c_str());
    return Status::InvalidArgument("Invalid file name");
  }

  Status status;
  if (type == kLogFile) {
    // Only allow deleting archived log files
    if (log_type != kArchivedLogFile) {
      __XENGINE_LOG(ERROR,
                      "DeleteFile %s failed - not archived log.\n",
                      name.c_str());
      return Status::NotSupported("Delete only supported for archived logs");
    }
    status =
        env_->DeleteFile(immutable_db_options_.wal_dir + "/" + name.c_str());
    if (!status.ok()) {
      __XENGINE_LOG(ERROR,
                      "DeleteFile %s failed -- %s.\n", name.c_str(),
                      status.ToString().c_str());
    }
    return status;
  }

  int level;
  FileMetaData* metadata;
  ColumnFamilyData* cfd;
  //VersionEdit edit;
  JobContext job_context(next_job_id_.fetch_add(1), storage_logger_, true);

  // LogFlush(immutable_db_options_.info_log);
  // remove files outside the db-lock
  if (job_context.HaveSomethingToDelete()) {
    // Call PurgeObsoleteFiles() without holding mutex.
    PurgeObsoleteFiles(job_context);
  }
  job_context.Clean();
  return status;
}

Status DBImpl::DeleteFilesInRange(ColumnFamilyHandle* column_family,
                                  const Slice* begin, const Slice* end) {
  Status status;
  /*
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  ColumnFamilyData* cfd = cfh->cfd();

  Arena arena;
  //std::vector<MetaEntry> chosen;
  //ChangeInfo info;
  char *buf = nullptr;
  SuperVersion *old = nullptr;

  // delete all the extents of one index
  // by delete the data between index_id and index_id + 1
  // Eg: delete 0001  - 0002
  Slice start_key = Slice(StorageManager::MIN_USER_KEY);
  if (begin != nullptr && begin->size() > 0) {
    // change to internal key
    buf = arena.AllocateAligned(begin->size() + StorageManager::SEQUENCE_TYPE_LEN);
    if (nullptr == buf) {
      __XENGINE_LOG(ERROR, 
                  "Allocate buffer failed for delete index %d.", 
                  Status::kMemoryLimit);
      return Status::MemoryLimit();
    }
    memcpy(buf, begin->data(), begin->size());
    EncodeFixed64(buf + begin->size(), 
                  PackSequenceAndType(kMaxSequenceNumber, kTypeValue));
    start_key = Slice(buf, begin->size() + StorageManager::SEQUENCE_TYPE_LEN);
  }

  Slice end_key = Slice(StorageManager::MAX_USER_KEY);
  if (end != nullptr && end->size() > 0) {
    // change to internal key
    buf = arena.AllocateAligned(end->size() + StorageManager::SEQUENCE_TYPE_LEN);
    if (nullptr == buf) {
      __XENGINE_LOG(ERROR,
                  "Allocate buffer failed for delete index %d.",
                  Status::kMemoryLimit);
      return Status::MemoryLimit();
    }
    memcpy(buf, end->data(), end->size());
    EncodeFixed64(buf + end->size(),
                  PackSequenceAndType(kMaxSequenceNumber, kTypeValue));
    end_key = Slice(buf, end->size() + StorageManager::SEQUENCE_TYPE_LEN);
  }
    
  mutex_.Lock();
  // can't delete the extents compacted
  if (cfd->pending_compaction()) {
    mutex_.Unlock();
    XENGINE_LOG(INFO, "Can't drop cf when compact", K(cfd->GetName().c_str()));
    return Status::TryAgain();
  }
  cfd->set_pending_compaction(true); 
  const Snapshot *sn = cfd->get_meta_snapshot(&mutex_);    
  mutex_.Unlock();

  ReadOptions ro;
  std::unique_ptr<InternalIterator, ptr_destruct<InternalIterator>> iter;
  Level0LayerIterator *level0_iter = nullptr;
  MetaKey meta_key;
  MetaValue meta_value;
  int ret = Status::kOk;
  int64_t pos = 0;
  //info.batch_.Clear();

  if (sn->get_level0() != nullptr) {
    for (int32_t layer = 0; layer < sn->get_level0()->size(); layer++) { // level0
      level0_iter =  
      (Level0LayerIterator *)sn->get_level0()->layers()[layer]->create_iterator(ro, &arena,
                                                                      cfd->GetID());
      if (nullptr == level0_iter) {
        continue;
      }

      level0_iter->Seek2(start_key);
      while (level0_iter->Valid()) {
        pos = 0;
        ret = meta_key.deserialize(level0_iter->key().data(),
                                   level0_iter->key().size(), pos);
        if (Status::kOk == ret) {
          if (end != nullptr && cfd->internal_comparator().user_comparator()->Compare(
                                      ExtractUserKey(meta_key.largest_key_), *end) > 0) {
            break;
          }
          pos = 0;
          ret = meta_value.deserialize(level0_iter->value().data(), 
                                     level0_iter->value().size(), pos);
          if (Status::kOk == ret) {
            if (begin == nullptr || cfd->internal_comparator().user_comparator()->Compare(
                      ExtractUserKey(meta_value.smallest_key_), *begin) >= 0) {
              //info.batch_.Delete(level0_iter->key());
            } 
          }
        }
        level0_iter->Next();
      }
    } 
  } 
  for (int32_t level = 1; level < StorageManager::MAX_LEVEL; level++) { // level1/2
    iter.reset(cfd->get_storage_manager()->get_single_level_iterator(ro, &arena,
                                                                 sn, level));
    if (nullptr == iter) {
      continue;
    }

    iter->Seek(start_key);
    while (iter->Valid()) {
      if (end != nullptr && cfd->internal_comparator().user_comparator()->Compare(
                                    ExtractUserKey(iter->key()), *end) > 0) {
        // outer the bigger bound
        break;
      }
      pos = 0;
      ret = meta_value.deserialize(iter->value().data(), 
                                   iter->value().size(), pos);
      if (Status::kOk == ret) {
        if (begin == nullptr || cfd->internal_comparator().user_comparator()->Compare(
                  ExtractUserKey(meta_value.smallest_key_), *begin) >= 0) {
          // include by the smaller bound 
          //info.batch_.Delete(Slice(iter->key().data() - 2 * StorageManager::SEQUENCE_TYPE_LEN, 
          //                         iter->key().size() + 2 * StorageManager::SEQUENCE_TYPE_LEN));
        } 
      }
      iter->Next();
    } 
  }

  cfd->get_storage_manager()->fill_drop_large_objects(start_key, end_key, info);

  mutex_.Lock();
  //status = versions_->log_and_apply(cfd, &mutex_, info);
  if (!status.ok()) {
    __XENGINE_LOG(ERROR, 
                    "Delete meta for drop index failed %d.", status.code());
    cfd->set_pending_compaction(false);
    cfd->release_meta_snapshot(sn, &mutex_);
    mutex_.Unlock();
    return status;
  }
  // update super version, so delete extent not seen by compaction 
  old = cfd->InstallSuperVersion(new SuperVersion(), &mutex_,
                                 *cfd->GetLatestMutableCFOptions()); 
  cfd->set_pending_compaction(false);
  cfd->release_meta_snapshot(sn, &mutex_);
  mutex_.Unlock();
  if (old) {
    delete old;
  }
  // recycle all the cfd extents here, 
  // it's safe here because no concurrent delete one cfd
  status = cfd->get_storage_manager()->recycle_meta_value(kMaxSequenceNumber, arena);
  if (!status.ok()) {
    __XENGINE_LOG(ERROR, "Recycle extents failed cfd id %d, error %d",
                  cfd->GetID(), status.code());
  }

  ret = cfd->get_storage_manager()->recycle_large_object(kMaxSequenceNumber, arena);
  if (ret != Status::kOk) {
    status = ret;
    __XENGINE_LOG(ERROR, "Recycle large object failed, cfd id %d, error %d",
                  cfd->GetID(), ret);
  }
  */

  return status;
}

void DBImpl::GetLiveFilesMetaData(std::vector<LiveFileMetaData>* metadata) {
  InstrumentedMutexLock l(&mutex_);
  versions_->GetLiveFilesMetaData(metadata, &mutex_);
}

void DBImpl::GetColumnFamilyMetaData(ColumnFamilyHandle* column_family,
                                     ColumnFamilyMetaData* cf_meta) {
  assert(column_family);
  auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();
  auto* sv = GetAndRefSuperVersion(cfd);
  ReturnAndCleanupSuperVersion(cfd, sv);
}

#endif  // ROCKSDB_LITE

Status DBImpl::CheckConsistency() {
  mutex_.AssertHeld();
  std::vector<LiveFileMetaData> metadata;
  versions_->GetLiveFilesMetaData(&metadata, &mutex_);

  std::string corruption_messages;
  for (const auto& md : metadata) {
    // md.name has a leading "/".
    std::string file_path = md.db_path + md.name;

    uint64_t fsize = MAX_EXTENT_SIZE;
    Status s = Status::OK(); //env_->GetFileSize(file_path, &fsize);
    if (!s.ok() &&
        env_->GetFileSize(Rocks2LevelTableFileName(file_path), &fsize).ok()) {
      s = Status::OK();
    }
    if (!s.ok()) {
      corruption_messages +=
          "Can't access " + md.name + ": " + s.ToString() + "\n";
    } else if (fsize != md.size) {
      corruption_messages += "Sst file size mismatch: " + file_path +
                             ". Size recorded in manifest " +
                             ToString(md.size) + ", actual size " +
                             ToString(fsize) + "\n";
    }
  }
  if (corruption_messages.size() == 0) {
    return Status::OK();
  } else {
    return Status::Corruption(corruption_messages);
  }
}

Status DBImpl::GetDbIdentity(std::string& identity) const {
  std::string idfilename = IdentityFileName(dbname_);
  const EnvOptions soptions;
  unique_ptr<SequentialFileReader, ptr_destruct_delete<SequentialFileReader>> id_file_reader_ptr;
  unique_ptr<SequentialFile, ptr_destruct_delete<SequentialFile>> idfile_ptr;
  SequentialFileReader *id_file_reader = nullptr;
  Status s;
  {
    SequentialFile *idfile = nullptr;
    s = env_->NewSequentialFile(idfilename, idfile, soptions);
    idfile_ptr.reset(idfile);
    if (!s.ok()) {
      return s;
    }
    id_file_reader = MOD_NEW_OBJECT(ModId::kEnv, SequentialFileReader, idfile);
    id_file_reader_ptr.reset(id_file_reader);
  }

  uint64_t file_size;
  s = env_->GetFileSize(idfilename, &file_size);
  if (!s.ok()) {
    return s;
  }
  char* buffer = reinterpret_cast<char*>(alloca(file_size));
  Slice id;
  s = id_file_reader->Read(static_cast<size_t>(file_size), &id, buffer);
  if (!s.ok()) {
    return s;
  }
  identity.assign(id.ToString());
  // If last character is '\n' remove it from identity
  if (identity.size() > 0 && identity.back() == '\n') {
    identity.pop_back();
  }
  return s;
}

// Default implementation -- returns not supported status
Status DB::CreateColumnFamily(CreateSubTableArgs &args,
                              ColumnFamilyHandle** handle) {
  return Status::NotSupported("");
}
Status DB::DropColumnFamily(ColumnFamilyHandle* column_family) {
  return Status::NotSupported("");
}
Status DB::DestroyColumnFamilyHandle(ColumnFamilyHandle* column_family) {
//  delete column_family;
  MOD_DELETE_OBJECT(ColumnFamilyHandle, column_family);
  return Status::OK();
}

DB::~DB() {}

Status DB::ListColumnFamilies(const DBOptions& db_options,
                              const std::string& name,
                              std::vector<std::string>* column_families) {
  //TODO:yuanfeng
  //return VersionSet::ListColumnFamilies(column_families, name, db_options.env);
  return Status::kOk;
}

Snapshot::~Snapshot() {}

Status DestroyDB(const std::string& dbname, const Options& options) {
  const ImmutableDBOptions soptions(SanitizeOptions(dbname, options));
  Env* env = soptions.env;
  std::vector<std::string> filenames;

  // Ignore error in case directory does not exist
  env->GetChildren(dbname, &filenames);

  FileLock* lock;
  const std::string lockname = LockFileName(dbname);
  Status result = env->LockFile(lockname, &lock);
  if (result.ok()) {
    uint64_t number;
    FileType type;
    InfoLogPrefix info_log_prefix(!soptions.db_log_dir.empty(), dbname);
    for (size_t i = 0; i < filenames.size(); i++) {
      if (ParseFileName(filenames[i], &number, info_log_prefix.prefix, &type) &&
          type != kDBLockFile) {  // Lock file will be deleted at end
        Status del;
        std::string path_to_delete = dbname + "/" + filenames[i];
        if (type == kMetaDatabase) {
          del = DestroyDB(path_to_delete, options);
        } else if (type == kTableFile) {
          del = DeleteSSTFile(&soptions, path_to_delete, 0);
        } else {
          del = env->DeleteFile(path_to_delete);
        }
        if (result.ok() && !del.ok()) {
          result = del;
        }
      } else if (filenames[i][0] != '.') {
        std::string path_to_delete = dbname + "/" + filenames[i];
        env->DeleteFile(path_to_delete);
      }
    }

    for (size_t path_id = 0; path_id < options.db_paths.size(); path_id++) {
      const auto& db_path = options.db_paths[path_id];
      env->GetChildren(db_path.path, &filenames);
      for (size_t i = 0; i < filenames.size(); i++) {
        if (ParseFileName(filenames[i], &number, &type) &&
            type == kTableFile) {  // Lock file will be deleted at end
          std::string table_path = db_path.path + "/" + filenames[i];
          Status del = DeleteSSTFile(&soptions, table_path,
                                     static_cast<uint32_t>(path_id));
          if (result.ok() && !del.ok()) {
            result = del;
          }
        }
      }
    }

    std::vector<std::string> walDirFiles;
    std::string archivedir = ArchivalDirectory(dbname);
    if (dbname != soptions.wal_dir) {
      env->GetChildren(soptions.wal_dir, &walDirFiles);
      archivedir = ArchivalDirectory(soptions.wal_dir);
    }

    // Delete log files in the WAL dir
    for (const auto& file : walDirFiles) {
      if (ParseFileName(file, &number, &type) && type == kLogFile) {
        Status del = env->DeleteFile(soptions.wal_dir + "/" + file);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }

    std::vector<std::string> archiveFiles;
    env->GetChildren(archivedir, &archiveFiles);
    // Delete archival files.
    for (size_t i = 0; i < archiveFiles.size(); ++i) {
      if (ParseFileName(archiveFiles[i], &number, &type) && type == kLogFile) {
        Status del = env->DeleteFile(archivedir + "/" + archiveFiles[i]);
        if (result.ok() && !del.ok()) {
          result = del;
        }
      }
    }

    // ignore case where no archival directory is present
    env->DeleteDir(archivedir);

    env->UnlockFile(lock);  // Ignore error since state is already gone
    env->DeleteFile(lockname);
    env->DeleteDir(dbname);  // Ignore error in case dir contains other files
    env->DeleteDir(soptions.wal_dir);
  }
  return result;
}

Status DBImpl::WriteOptionsFile() {
#ifndef ROCKSDB_LITE
  mutex_.AssertHeld();

  std::vector<std::string> cf_names;
  std::vector<ColumnFamilyOptions> cf_opts;

  // Unlock during expensive operations.  New writes cannot get here
  // because the single write thread ensures all new writes get queued.
  DBOptions db_options =
      BuildDBOptions(immutable_db_options_, mutable_db_options_);
  mutex_.Unlock();

  std::string file_name =
      TempOptionsFileName(GetName(), versions_->NewFileNumber());
  Status s =
      PersistRocksDBOptions(db_options, cf_names, cf_opts, file_name, GetEnv());

  if (s.ok()) {
    s = RenameTempFileToOptionsFile(file_name);
  }
  mutex_.Lock();
  return s;
#else
  return Status::OK();
#endif  // !ROCKSDB_LITE
}

#ifndef ROCKSDB_LITE
namespace {
void DeleteOptionsFilesHelper(const std::map<uint64_t, std::string>& filenames,
                              const size_t num_files_to_keep, Env* env) {
  if (filenames.size() <= num_files_to_keep) {
    return;
  }
  for (auto iter = std::next(filenames.begin(), num_files_to_keep);
       iter != filenames.end(); ++iter) {
    if (!env->DeleteFile(iter->second).ok()) {
      __XENGINE_LOG(WARN, "Unable to delete options file %s",
                     iter->second.c_str());
    }
  }
}
}  // namespace
#endif  // !ROCKSDB_LITE

Status DBImpl::DeleteObsoleteOptionsFiles() {
#ifndef ROCKSDB_LITE
  std::vector<std::string> filenames;
  // use ordered map to store keep the filenames sorted from the newest
  // to the oldest.
  std::map<uint64_t, std::string> options_filenames;
  Status s;
  s = GetEnv()->GetChildren(GetName(), &filenames);
  if (!s.ok()) {
    return s;
  }
  for (auto& filename : filenames) {
    uint64_t file_number;
    FileType type;
    if (ParseFileName(filename, &file_number, &type) && type == kOptionsFile) {
      options_filenames.insert(
          {std::numeric_limits<uint64_t>::max() - file_number,
           GetName() + "/" + filename});
    }
  }

  // Keeps the latest 2 Options file
  const size_t kNumOptionsFilesKept = 2;
  DeleteOptionsFilesHelper(options_filenames, kNumOptionsFilesKept, GetEnv());
  return Status::OK();
#else
  return Status::OK();
#endif  // !ROCKSDB_LITE
}

Status DBImpl::RenameTempFileToOptionsFile(const std::string& file_name) {
#ifndef ROCKSDB_LITE
  Status s;

  versions_->options_file_number_ = versions_->NewFileNumber();
  std::string options_file_name =
      OptionsFileName(GetName(), versions_->options_file_number_);
  // Retry if the file name happen to conflict with an existing one.
  s = GetEnv()->RenameFile(file_name, options_file_name);

  DeleteObsoleteOptionsFiles();
  return s;
#else
  return Status::OK();
#endif  // !ROCKSDB_LITE
}

#ifdef ROCKSDB_USING_THREAD_STATUS

void DBImpl::NewThreadStatusCfInfo(ColumnFamilyData* cfd) const {
  if (immutable_db_options_.enable_thread_tracking) {
    ThreadStatusUtil::NewColumnFamilyInfo(this, cfd, cfd->GetName(),
                                          cfd->ioptions()->env);
  }
}

void DBImpl::EraseThreadStatusCfInfo(ColumnFamilyData* cfd) const {
  if (immutable_db_options_.enable_thread_tracking) {
    ThreadStatusUtil::EraseColumnFamilyInfo(cfd);
  }
}

void DBImpl::EraseThreadsStatusDbInfo() const {
  if (immutable_db_options_.enable_thread_tracking) {
    ThreadStatusUtil::EraseDatabaseInfo(this);
  }
}

#else
void DBImpl::NewThreadStatusCfInfo(ColumnFamilyData* cfd) const {}

void DBImpl::EraseThreadStatusCfInfo(ColumnFamilyData* cfd) const {}

void DBImpl::EraseThreadsStatusDbInfo() const {}
#endif  // ROCKSDB_USING_THREAD_STATUS

//
// A global method that can dump out the build version
void DumpXEngineBuildVersion() {
#if !defined(IOS_CROSS_COMPILE)
  // if we compile with Xcode, we don't run build_detect_vesion, so we don't
  // generate util/build_version.cc
  __XENGINE_LOG(INFO, "XEngine version: %d.%d.%d\n", ROCKSDB_MAJOR,
                   ROCKSDB_MINOR, ROCKSDB_PATCH);
  __XENGINE_LOG(INFO, "Git sha %s", xengine_build_git_sha);
  __XENGINE_LOG(INFO, "Compile date %s", xengine_build_compile_date);
#endif
}

#ifndef ROCKSDB_LITE
SequenceNumber DBImpl::GetEarliestMemTableSequenceNumber(SuperVersion* sv,
                                                         bool include_history) {
  // Find the earliest sequence number that we know we can rely on reading
  // from the memtable without needing to check sst files.
  SequenceNumber earliest_seq =
      sv->imm->GetEarliestSequenceNumber(include_history);
  if (earliest_seq == kMaxSequenceNumber) {
    earliest_seq = sv->mem->GetEarliestSequenceNumber();
  }
  assert(sv->mem->GetEarliestSequenceNumber() >= earliest_seq);

  return earliest_seq;
}
#endif  // ROCKSDB_LITE

#ifndef ROCKSDB_LITE
Status DBImpl::GetLatestSequenceForKey(SuperVersion* sv, const Slice& key,
                                       bool cache_only, SequenceNumber* seq,
                                       bool* found_record_for_key) {
  QUERY_TRACE_SCOPE(TracePoint::GET_LATEST_SEQ);

  Status s;
  MergeContext merge_context;
  RangeDelAggregator range_del_agg(sv->mem->GetInternalKeyComparator(),
                                   kMaxSequenceNumber);

  ReadOptions read_options;
  SequenceNumber current_seq = versions_->LastSequence();
  LookupKey lkey(key, current_seq);

  *seq = kMaxSequenceNumber;
  *found_record_for_key = false;

  // Check if there is a record for this key in the latest memtable
  sv->mem->Get(lkey, nullptr, &s, &merge_context, &range_del_agg, seq,
               read_options);

  if (!(s.ok() || s.IsNotFound() || s.IsMergeInProgress())) {
    // unexpected error reading memtable.
    __XENGINE_LOG(ERROR,
                    "Unexpected status returned from MemTable::Get: %s\n",
                    s.ToString().c_str());

    return s;
  }

  if (*seq != kMaxSequenceNumber) {
    // Found a sequence number, no need to check immutable memtables
    *found_record_for_key = true;
    return Status::OK();
  }

  // Check if there is a record for this key in the immutable memtables
  sv->imm->Get(lkey, nullptr, &s, &merge_context, &range_del_agg, seq,
               read_options);

  if (!(s.ok() || s.IsNotFound() || s.IsMergeInProgress())) {
    // unexpected error reading memtable.
    __XENGINE_LOG(ERROR,
                    "Unexpected status returned from MemTableList::Get: %s\n",
                    s.ToString().c_str());

    return s;
  }

  if (*seq != kMaxSequenceNumber) {
    // Found a sequence number, no need to check memtable history
    *found_record_for_key = true;
    return Status::OK();
  }

  // Check if there is a record for this key in the immutable memtables
  sv->imm->GetFromHistory(lkey, nullptr, &s, &merge_context, &range_del_agg,
                          seq, read_options);

  if (!(s.ok() || s.IsNotFound() || s.IsMergeInProgress())) {
    // unexpected error reading memtable.
    __XENGINE_LOG(ERROR, "Unexpected status returned from MemTableList::GetFromHistory: %s\n", s.ToString().c_str());

    return s;
  }

  if (*seq != kMaxSequenceNumber) {
    // Found a sequence number, no need to check SST files
    *found_record_for_key = true;
    return Status::OK();
  }

  // TODO(agiardullo): possible optimization: consider checking cached
  // SST files if cache_only=true?
  if (!cache_only) {
    // Check tables
    /*
    sv->current->Get(read_options, lkey, nullptr, &s, &merge_context,
                     &range_del_agg, nullptr value_found,
                     found_record_for_key, seq);

    if (!(s.ok() || s.IsNotFound() || s.IsMergeInProgress())) {
      // unexpected error reading SST files
      __XENGINE_LOG(ERROR,
                      "Unexpected status returned from Version::Get: %s\n",
                      s.ToString().c_str());

      return s;
    }
    */
    PinnableSlice dummy_value;
    bool dummy_bool = false;
    QUERY_COUNT(CountPoint::SEARCH_LATEST_SEQ_IN_STORAGE);
    s = Status(sv->cfd_.load()->get_from_storage_manager(read_options,
                                                  *sv->current_meta_,
                                                  lkey,
                                                  dummy_value,
                                                  dummy_bool,
                                                  seq));

    if (!(s.ok() || s.IsNotFound() || s.IsMergeInProgress())) {
      // unexpected error reading SST files
      __XENGINE_LOG(ERROR,
                      "Unexpected status returned from Version::Get: %s\n",
                      s.ToString().c_str());

      return s;
    }

    if (*seq != kMaxSequenceNumber) {
      *found_record_for_key = true;
      return Status::OK();
    }
  }

  return Status::OK();
}

int DBImpl::get_latest_seq_for_uk(ColumnFamilyHandle *column_family,
                                  const ReadOptions *read_opts,
                                  const common::Slice &key,
                                  common::SequenceNumber &seq)
{
  QUERY_TRACE_SCOPE(TracePoint::GET_LATEST_UK_SEQ);
  assert(column_family != nullptr);
  assert(read_opts != nullptr);
  int ret = Status::kOk;

  Iterator *db_iter = NewIterator(*read_opts, column_family);
  db_iter->Seek(key);
  if (db_iter->Valid()) {
    const Slice &scanned_key = db_iter->key();
    if (0 == memcmp(key.data(), scanned_key.data(), std::min(key.size(), scanned_key.size()))) {
      seq = db_iter->key_seq();
      assert(seq < kMaxSequenceNumber);
    } else {
      ret = Status::kNotFound;
    }
  } else {
    ret = Status::kNotFound;
  }
//  delete db_iter;
  MOD_DELETE_OBJECT(Iterator, db_iter);
  return ret;
}

void DBImpl::WaitForIngestFile() {
  mutex_.AssertHeld();
  while (num_running_ingest_file_ > 0) {
    bg_cv_.Wait();
  }
}

/*
Status DBImpl::install_flush_result(ColumnFamilyData* cfd,
                                    const ChangeInfo& change_info,
                                    bool update_current_file) {
  int ret = versions_->log_and_apply(cfd, &mutex_, change_info, update_current_file);
  if (Status::kOk == ret) {
    schedule_background_recycle();
  }
  return Status(static_cast<Status::Code>(ret));
}
*/


Status DBImpl::InstallSstExternal(ColumnFamilyHandle* column_family,
                                  MiniTables* mtables) {
  ColumnFamilyData* cfd;
  int ret = Status::kOk;
  int64_t dummy_log_seq = 0;
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  cfd = cfh->cfd();

  if (FAILED(storage_logger_->begin(xengine::storage::XengineEvent::CREATE_INDEX))) {
    XENGINE_LOG(WARN, "fail to begin create index trans", K(ret));
  } else if (FAILED(cfd->apply_change_info(*(mtables->change_info_), true))) {
    XENGINE_LOG(WARN, "fail to apply change info", K(ret));
  } else if (FAILED(storage_logger_->commit(dummy_log_seq))) {
    XENGINE_LOG(WARN, "fail to commit cerate index trans", K(ret));
  } else {
    auto mutable_cf_options = cfd->GetLatestMutableCFOptions();
    mutex_.Lock();
    SuperVersion *old_sv = InstallSuperVersionAndScheduleWork(cfd, nullptr, *mutable_cf_options);
    mutex_.Unlock();
    if (nullptr != old_sv) {
      MOD_DELETE_OBJECT(SuperVersion, old_sv);
    }

    std::vector<FileMetaData>::iterator meta_iter = mtables->metas.begin();
    std::vector<TableProperties>::iterator props_iter = mtables->props.begin();
    assert(mtables->metas.size() == mtables->props.size());

    while (meta_iter != mtables->metas.end()) {
      meta_iter = mtables->metas.erase(meta_iter);
      props_iter = mtables->props.erase(props_iter);
    }

    assert(mtables->metas.size() == 0);
    assert(mtables->props.size() == 0);
  }

  return Status(ret);
}

// retrieve all snapshot numbers. They are sorted in ascending order.
std::vector<SequenceNumber> DBImpl::GetAll(
    SequenceNumber* oldest_write_conflict_snapshot) {
  std::vector<SequenceNumber> ret;
  SnapshotImpl* s[MAX_SNAP] = {nullptr};

  if (oldest_write_conflict_snapshot != nullptr) {
    *oldest_write_conflict_snapshot = kMaxSequenceNumber;
  }

  for (int32_t i = 0; i < MAX_SNAP; i++) {
    snap_mutex[i].Lock();
    s[i] = snap_lists_[i].list();
  }

  uint64_t min = kMaxSequenceNumber;
  int32_t min_idx = -1;

  while (1) {
    min = kMaxSequenceNumber;
    min_idx = -1;
    for (int32_t i = 0; i < MAX_SNAP; i++) {
      if (s[i]->next() != snap_lists_[i].list() &&
          s[i]->next()->number_ < min) {
        min = s[i]->next()->number_;
        min_idx = i;
      }
    }

    if (min_idx == -1) break;

    ret.push_back(min);

    if (oldest_write_conflict_snapshot != nullptr &&
        *oldest_write_conflict_snapshot == kMaxSequenceNumber &&
        s[min_idx]->next()->is_write_conflict_boundary()) {
      // If this is the first write-conflict boundary snapshot in the list,
      // it is the oldest
      *oldest_write_conflict_snapshot = s[min_idx]->next()->number_;
    }

    s[min_idx] = s[min_idx]->next();
  }

  for (int32_t i = 0; i < MAX_SNAP; i++) {
    snap_mutex[i].Unlock();
  }

  return ret;
}

// without snap_mutex for perf in stats
uint64_t DBImpl::snapshots_count() const {
  uint64_t c = 0;
  for (int32_t i = 0; i < MAX_SNAP; i++) {
    c += snap_lists_[i].count();
  }
  return c;
}

int64_t DBImpl::GetOldestSnapshotTime() const {
  int64_t oldest_ut = INT64_MAX;
  for (int32_t i = 0; i < MAX_SNAP; i++) {
    snap_mutex[i].Lock();
    if (!snap_lists_[i].empty() &&
        snap_lists_[i].oldest()->unix_time() < oldest_ut)
      oldest_ut = snap_lists_[i].oldest()->unix_time();
    snap_mutex[i].Unlock();
  }
  return oldest_ut;
}

bool DBImpl::snapshot_empty() {
  bool is_empty = true;
  for (int32_t i = 0; i < MAX_SNAP; i++) {
    if (!snap_lists_[i].empty()) {
      is_empty = false;
      break;
    }
  }
  return is_empty;
}

int DBImpl::do_manual_checkpoint(int32_t &start_manifest_file_num) {
  int ret = Status::kOk;
  if (FAILED(storage_logger_->external_write_checkpoint())) {
    XENGINE_LOG(ERROR, "Do a manual checkpoint failed", K(ret));
  } else {
    // should record the file number of checkpoint
    start_manifest_file_num = storage_logger_->current_manifest_file_number();
  }
  return ret;
}

// for hot backup
int DBImpl::stream_log_extents(std::function<int(const char*, int, int64_t, int)>
                               *stream_extent, int64_t start, int64_t end, int dest_fd) {
  int ret = Status::kOk;
  FAIL_RETURN_MSG_NEW(versions_->stream_log_extents(stream_extent, start, end, dest_fd),
                      "Stream the incremental extents failed", ret);
  return ret;
}

int DBImpl::create_backup_snapshot(MetaSnapshotMap &meta_snapshot,
                                   int32_t &last_manifest_file_num,
                                   uint64_t &last_manifest_file_size,
                                   uint64_t &last_wal_file_num)
{
  int ret = Status::kOk;
  // keep create snapshot and do checkpoint atomic,
  // exclusive from apply_change_info in flush/compaction
  mutex_.Lock();
  // TODO: is it OK to switch default cfd?
  GlobalContext* global_ctx = nullptr;

  if (nullptr == (global_ctx = versions_->get_global_ctx())) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, global ctx must not nullptr", K(ret));
  } else {
    SubTable* sub_table = nullptr;
    global_ctx->all_sub_table_->get_sub_table(0, sub_table);
    WriteContext context;
    context.all_sub_table_ = global_ctx->all_sub_table_;
    if (FAILED(versions_->create_backup_snapshot(meta_snapshot))) {
      XENGINE_LOG(WARN, "Failed to create the backup snapshot", K(ret));
    } else if (FAILED(SwitchMemtable(sub_table, &context,
        true /* force create new wal file*/).code())) {
      XENGINE_LOG(WARN, "Failed to switch memtable", K(ret));
    } else {
      last_manifest_file_num = storage_logger_->current_manifest_file_number();
      last_manifest_file_size = storage_logger_->current_manifest_file_size();
      last_wal_file_num = logfile_number_;
      XENGINE_LOG(INFO, "Create a backup snapshot", K(ret),
                  K(last_manifest_file_num), K(last_manifest_file_size),
                  K(last_wal_file_num));
    }
  }

  mutex_.Unlock();
  return ret;
}

int DBImpl::record_incremental_extent_ids(const int32_t first_manifest_file_num,
                                          const int32_t last_manifest_file_num,
                                          const uint64_t last_manifest_file_size)
{
  int ret = Status::kOk;
  if (FAILED(storage_logger_->record_incremental_extent_ids(first_manifest_file_num,
                                                            last_manifest_file_num,
                                                            last_manifest_file_size))) {
    XENGINE_LOG(WARN, "Failed to record incremental extent ids", K(ret),
        K(first_manifest_file_num), K(last_manifest_file_num), K(last_manifest_file_size));
  }
  return ret;
}

int DBImpl::release_backup_snapshot(MetaSnapshotMap &meta_snapshot)
{
  int ret = Status::kOk;
  ColumnFamilyData *cfd = nullptr;
  for (auto sn : meta_snapshot) {
    cfd = sn.first;
    if (ISNULL(cfd)) {
      XENGINE_LOG(ERROR, "The cfd is nullptr, unexpected", KP(cfd));
    } else {
      cfd->release_meta_snapshot(sn.second);
      mutex_.Lock();
      if (cfd->Unref()) {
        MOD_DELETE_OBJECT(ColumnFamilyData, cfd);
      }
      mutex_.Unlock();
    }
  }
  meta_snapshot.clear();
  XENGINE_LOG(INFO, "Release the backup snapshot", K(ret));
  return ret;
}

int DBImpl::clear_all_compaction_jobs() {
  int ret = Status::kOk;
  STFlushJob *flush_job = nullptr;
  STDumpJob *dump_job = nullptr;
  CFCompactionJob *compaction_job = nullptr;

  while (SUCCED(ret) && !flush_queue_.empty()) {
    if (IS_NULL(flush_job = PopFirstFromFlushQueue())) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, flush job in the queue must not nullptr", K(ret));
    } else if (IS_NULL(flush_job->sub_table_)) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, subtable must not nullptr", K(ret));
    } else {
      flush_job->sub_table_->set_pending_flush(false);
      remove_flush_job(flush_job, false);
    }
  }

  while (SUCCED(ret) && !dump_queue_.empty()) {
    if (IS_NULL(dump_job = pop_front_dump_job())) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, dump job in the queue must not nullptr", K(ret));
    } else if (IS_NULL(dump_job->sub_table_)) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, subtable must not nullptr", K(ret));
    } else {
      dump_job->sub_table_->set_pending_dump(false);
      remove_dump_job(dump_job);
    }
  }

  for (int32_t i = 0; SUCCED(ret) && i < CompactionPriority::ALL; ++i) {
    while (SUCCED(ret) && !compaction_queue_[i].empty()) {
      if (IS_NULL(compaction_job = pop_front_compaction_job())) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, compaction job in the queue must not nullptr", K(ret));
      } else if (IS_NULL(compaction_job->cfd_)) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, subtable must not nullptr", K(ret));
      } else {
        compaction_job->cfd_->set_pending_compaction(false);
        remove_compaction_job(compaction_job, false);
      }
    }
  }
  XENGINE_LOG(INFO, "clear all the compaction job", K(ret));

  return ret;
}

int DBImpl::clear_all_jobs_and_set_pending()
{
  int ret = Status::kOk;
  InstrumentedMutexLock guard_lock(&mutex_);
  if (FAILED(clear_all_compaction_jobs())) {
    XENGINE_LOG(WARN, "fail to clear all jobs", K(ret));
  } else {
    set_all_pending_compaction();
  }
  return ret;
}

int DBImpl::check_no_jobs_and_reset_pending()
{
  int ret = Status::kOk;
  InstrumentedMutexLock guard_lock(&mutex_);
  if (!flush_queue_.empty()) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(ERROR, "unexpected error, flush queue not empty", K(ret), "size", flush_queue_.size());
  }
  if (!dump_queue_.empty()) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(ERROR, "unexpected error, dump queue not empty", K(ret), "size", dump_queue_.size());
  }
  
  for (int32_t i = 0; i < CompactionPriority::ALL; ++i) {
    if (!compaction_queue_[i].empty()) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(ERROR, "unexpected error, compaction queue not empty", K(ret), K(i), "size", compaction_queue_[i].size());
    }
  }

  if (SUCCED(ret)) {
    //if failed above, we not reset the pending flag, because the data may corruption
    reset_all_pending_compaction();
  }
  return ret;
}

#if 0  //now not called by others
int DBImpl::get_all_level0_cfs(std::vector<db::ColumnFamilyData*> &level0_cfs) {
  int ret = Status::kOk;
  InstrumentedMutexLock guard_lock(&mutex_);
  for (ColumnFamilyData *cfd : *versions_->GetColumnFamilySet()) {
    // the dropped cfd may be exist in set for a while  
    if (cfd->IsDropped()) {
      continue;
    }
    //TODO:yuanfeng
    /*
    const SnapshotImpl* current_version = reinterpret_cast<const SnapshotImpl*>(
      cfd->get_storage_manager()->get_current_version());
    if (current_version->level0_ && current_version->level0_->size() > 0) { 
      XENGINE_LOG(INFO, "Cfd has level0", K(cfd->GetID()));
      level0_cfs.push_back(cfd);  
    }
    */
  }

  return ret;
}
#endif

int DBImpl::pushdown_all_level0() {
  int ret = Status::kOk;
  // todo check if is nouse
  /*
  // compact all the L0 data to L1
  // FIXME: some error may loop for ever
  // speedup the compaction use parallel manual compaction
  int push_down_times = 0;
  std::vector<db::ColumnFamilyData*> level0_cfs;
  get_all_level0_cfs(level0_cfs); 
  while (level0_cfs.size() > 0 && push_down_times < 3600) { 
    clear_all_compaction_jobs();
    for (ColumnFamilyData *cfd : level0_cfs) {
      // only do minor compaction
      cfd->set_compaction_priority(CompactionPriority::LOW);
      cfd->set_compaction_state(ColumnFamilyData::MINOR);
      Status s = RunManualCompaction(cfd, ColumnFamilyData::kCompactAllLevels,
                        cfd->NumberLevels() - 1, 0, nullptr, nullptr, true);
      if (!s.ok()) {
        ret = s.code();
        XENGINE_LOG(ERROR, "run manual compaction failed", K(ret));
        return ret;
      } else {
        XENGINE_LOG(INFO, "complete one cf level0 compaction", K(cfd->GetID()), K(push_down_times));
      } 
    }
    level0_cfs.clear();
    // check the next round
    get_all_level0_cfs(level0_cfs);
    push_down_times++;
  }
 
  if (level0_cfs.size() > 0) {
     ret = Status::kAborted;
     XENGINE_LOG(ERROR, "try to pushdown all level0 data failed, shrink aborted", K(ret));
  }
*/
  return ret;
}

// no cfcompaction job will be added
void DBImpl::set_all_pending_compaction() {
  int ret = Status::kOk;
  SubTable *sub_table = nullptr;

  AllSubTableGuard all_subtable_guard(versions_->get_global_ctx());
  const SubTableMap &all_sub_tables = all_subtable_guard.get_subtable_map();
  for (auto iter = all_sub_tables.begin(); Status::kOk == ret && iter != all_sub_tables.end(); ++iter) {
    if (nullptr == (sub_table = iter->second)) {
      ret = Status::kCorruption;
      XENGINE_LOG(WARN, "subtable must not nullptr", K(ret), K(iter->first));
    } else if (sub_table->IsDropped()) {
      //do nothing
      XENGINE_LOG(INFO, "subtable has been dropped", "index_id", iter->first);
    } else {
      assert(!sub_table->pending_compaction());
      sub_table->set_pending_dump(true);
      sub_table->set_pending_flush(true);
      sub_table->set_pending_compaction(true);
      XENGINE_LOG(INFO, "set cf pending compaction", K(sub_table->GetID()));
    }
  }
}

void DBImpl::reset_all_pending_compaction() {
  int ret = Status::kOk;
  GlobalContext *global_ctx = nullptr;
  SubTable *sub_table = nullptr;

  AllSubTableGuard all_subtable_guard(versions_->get_global_ctx());
  const SubTableMap &all_sub_tables = all_subtable_guard.get_subtable_map();
  for (auto iter = all_sub_tables.begin(); Status::kOk == ret && iter != all_sub_tables.end(); ++iter) {
    if (nullptr == (sub_table = iter->second)) {
      ret = Status::kCorruption;
      XENGINE_LOG(WARN, "subtable must not nullptr", K(ret), K(iter->first));
    } else if (sub_table->IsDropped()) {
      //do nothing
      XENGINE_LOG(INFO, "subtable has been dropped", "index_id", iter->first);
    } else {
      assert(sub_table->pending_compaction());
      sub_table->set_pending_dump(false);
      sub_table->set_pending_flush(false);
      sub_table->set_pending_compaction(false);
      XENGINE_LOG(INFO, "reset cf pending compaction", K(sub_table->GetID()));
    }
  }
}

int DBImpl::shrink_table_space(int32_t table_space_id) {
  int ret = Status::kOk;
  std::vector<ShrinkInfo> shrink_infos;
  bool actual_shrink = false;
  bool expect_shrink_running = false;
  int64_t shrink_extent_count = 0;
  ShrinkCondition shrink_condition(0 /*max_free_extent_percent*/, 0 /*shrink_allocate_interval*/, INT64_MAX /*max_shrink_extent_count*/);

  XENGINE_LOG(INFO, "begin to do shrink table space", K(table_space_id));
  if (!shrink_running_.compare_exchange_strong(expect_shrink_running, true)) {
    XENGINE_LOG(INFO, "another shrink job is running");
  } else if (FAILED(extent_space_manager_->get_shrink_infos(table_space_id,
                                                            shrink_condition,
                                                            shrink_infos))) {
    XENGINE_LOG(WARN, "fail to get shrink infos", K(ret), K(table_space_id));
  } else {
    for (uint32_t i = 0; SUCCED(ret) && i < shrink_infos.size(); ++i) {
      const ShrinkInfo &shrink_info = shrink_infos.at(i);
      if (FAILED(shrink_extent_space(shrink_info))) {
        XENGINE_LOG(WARN, "fail to shrink extent space", K(ret), K(shrink_info));
      } else {
        XENGINE_LOG(INFO, "success to shrink extent space", K(i), K(shrink_info));
      }
    }
  }
  shrink_running_.store(false);
  XENGINE_LOG(INFO, "end to do shrink table space", K(table_space_id));

  return ret;
}

bool DBImpl::get_columnfamily_stats(ColumnFamilyHandle* column_family, int64_t &data_size,
                                    int64_t &num_entries, int64_t &num_deletes, int64_t &disk_size) {
  if (LIKELY(column_family != nullptr)) {
    ColumnFamilyData *cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family)->cfd();  
    assert(cfd != nullptr);
    StorageManager *storage_manager = cfd->get_storage_manager();
    assert(storage_manager != nullptr);
    return storage_manager->get_extent_stats(data_size, num_entries, num_deletes, disk_size);
  } else {
    XENGINE_LOG(WARN, "can't get sub tables stats of empty handle\n", K(data_size));
    return false;
  }
}

#endif  // ROCKSDB_LITE
}
}  // namespace xengine
