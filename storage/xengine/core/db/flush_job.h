// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <deque>
#include <limits>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "db/column_family.h"
#include "db/dbformat.h"
#include "db/flush_scheduler.h"
#include "db/internal_stats.h"
#include "db/job_context.h"
#include "db/log_writer.h"
#include "db/memtable_list.h"
#include "db/snapshot_impl.h"
#include "db/version_edit.h"
#include "db/write_controller.h"
#include "db/write_thread.h"
#include "monitoring/instrumented_mutex.h"
#include "options/db_options.h"
#include "port/port.h"
#include "table/scoped_arena_iterator.h"
#include "util/arena.h"
#include "util/autovector.h"
#include "util/event_logger.h"
#include "util/stop_watch.h"
#include "util/thread_local.h"
#include "xengine/db.h"
#include "xengine/env.h"
#include "xengine/memtablerep.h"
#include "xengine/transaction_log.h"

namespace xengine {

namespace util {
class Arena;
}

namespace storage {
class MtExtCompaction;
struct LayerPosition;
}

namespace db {
class TableCache;
class Version;
class VersionEdit;
class VersionSet;

class BaseFlush {
 public:
  BaseFlush(ColumnFamilyData* cfd,
            const common::ImmutableDBOptions& db_options,
            const common::MutableCFOptions& mutable_cf_options,
            common::CompressionType output_compression,
            const std::atomic<bool>* shutting_down,
            common::SequenceNumber earliest_write_conflict_snapshot,
            JobContext& job_context,
            std::vector<common::SequenceNumber> &existing_snapshots,
            util::Directory* output_file_directory,
            const util::EnvOptions *env_options,
            memory::ArenaAllocator &arena);
  virtual ~BaseFlush();
  virtual int run(MiniTables& mtables) = 0;
  virtual int prepare_flush_task(MiniTables &mtables) = 0;
  virtual void pick_memtable() = 0;
  int after_run_flush(MiniTables &mtables, int ret);
  void cancel();
  // for test
  void set_memtables(util::autovector<MemTable*> &mems) {
    mems_ = mems;
  }
  TaskType get_task_type() {
    return job_context_.task_type_;
  }
  util::autovector<MemTable*> &get_picked_memtable() {
    return mems_;
  }

//  void set_recovery_point(PaxosGroupRecoveryPoint &rp) {
//    recovery_point_ = rp;
//  }

  void set_min_hold_wal_file_id(int32_t min_hold_wal_file_id) {
    min_hold_wal_file_id_ = min_hold_wal_file_id;
  }
 public:
  int write_level0_table(MiniTables *mtables, uint64_t max_seq = 0);
  int fill_table_cache(const MiniTables &mtables);
  int delete_old_M0(const InternalKeyComparator *internal_comparator, MiniTables &mtables);
  int stop_record_flush_stats(const int64_t bytes_written,
                              const uint64_t start_micros);
  void RecordFlushIOStats();

  ColumnFamilyData* cfd_;
  const common::ImmutableDBOptions& db_options_;
  const common::MutableCFOptions& mutable_cf_options_;
  common::CompressionType output_compression_;
  const std::atomic<bool>* shutting_down_;
  common::SequenceNumber earliest_write_conflict_snapshot_;
  JobContext &job_context_;
  // Variables below are set by PickMemTable():
  util::autovector<MemTable*> mems_;
  // env option
  std::vector<common::SequenceNumber> existing_snapshots_;

  util::Directory* output_file_directory_;
  const util::EnvOptions *env_options_;
  memory::ArenaAllocator &arena_;
  util::Arena tmp_arena_;
  bool pick_memtable_called_;
  int32_t min_hold_wal_file_id_;
//  PaxosGroupRecoveryPoint recovery_point_;
};

class FlushJob : public BaseFlush {
 public:
  FlushJob(const std::string& dbname,
           ColumnFamilyData* cfd,
           const common::ImmutableDBOptions& db_options,
           JobContext& job_context,
           util::Directory* output_file_directory,
           common::CompressionType output_compression,
           monitor::Statistics* stats,
           storage::CompactionContext &context,
           memory::ArenaAllocator &arena);

  virtual ~FlushJob();

  // Require db_mutex held.
  // Once PickMemTable() is called, either Run() or Cancel() has to be call.
  virtual void pick_memtable();
  virtual int prepare_flush_task(MiniTables &mtables);
  virtual int run(MiniTables& mtables);
  void cancel();
  table::TableProperties GetTableProperties() const {
    return table_properties_;
  }
  int prepare_flush_level1_task(MiniTables &mtables);
  int run_mt_ext_task(MiniTables &mtables);
  void set_meta_snapshot(const db::Snapshot *meta_snapshot) {
    meta_snapshot_ = meta_snapshot;
  }
  storage::MtExtCompaction *get_compaction() const {
    return compaction_;
  }
 private:
  void ReportStartedFlush();
  void ReportFlushInputSize(const util::autovector<MemTable*>& mems);
  int get_memtable_range(util::autovector<table::InternalIterator*> &memtables,
                         storage::Range &wide_range);
  int build_mt_ext_compaction(
      util::autovector<table::InternalIterator*> &memtables,
      storage::RangeIterator *l1_iterator);
  int build_l1_iterator(storage::Range &wide_range, storage::RangeIterator *&iterator);
  int create_meta_iterator(const storage::LayerPosition &layer_position,
                           table::InternalIterator *&iterator);
  int parse_meta(const table::InternalIterator *iter, storage::ExtentMeta *&extent_meta);
  int get_memtable_iterators(
      common::ReadOptions &ro,
      util::autovector<table::InternalIterator*> &memtables,
      MiniTables& mtables,
      int64_t &total_size);

  const std::string& dbname_;
  monitor::Statistics* stats_;
  table::TableProperties table_properties_;
  storage::CompactionContext compaction_context_;
  const db::Snapshot* meta_snapshot_;
  storage::MtExtCompaction *compaction_;
};
}
}  // namespace xengine
