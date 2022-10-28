// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/flush_job.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>

#include <algorithm>
#include <vector>

#include "compact/compaction_job.h"
#include "compact/mt_ext_compaction.h"
#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/event_helpers.h"
#include "db/log_reader.h"
#include "db/log_writer.h"
#include "db/memtable.h"
#include "db/memtable_list.h"
#include "db/merge_context.h"
#include "db/version_set.h"
#include "logger/logger.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "memory/page_arena.h"
#include "port/likely.h"
#include "port/port.h"
#include "storage/multi_version_extent_meta_layer.h"
#include "storage/storage_manager.h"
#include "storage/storage_logger.h"
#include "table/block.h"
#include "table/merging_iterator.h"
#include "table/table_builder.h"
#include "table/two_level_iterator.h"
#include "util/coding.h"
#include "util/event_logger.h"
#include "util/file_util.h"
#include "util/filename.h"
#include "util/log_buffer.h"
#include "util/mutexlock.h"
#include "util/stop_watch.h"
#include "util/sync_point.h"
#include "util/to_string.h"
#include "xengine/db.h"
#include "xengine/env.h"
#include "xengine/statistics.h"
#include "xengine/status.h"
#include "xengine/table.h"

namespace xengine {
using namespace util;
using namespace common;
using namespace monitor;
using namespace table;
using namespace storage;
using namespace memory;
namespace db {

BaseFlush::BaseFlush(ColumnFamilyData* cfd,
                     const common::ImmutableDBOptions& db_options,
                     const common::MutableCFOptions& mutable_cf_options,
                     common::CompressionType output_compression,
                     const std::atomic<bool>* shutting_down,
                     common::SequenceNumber earliest_write_conflict_snapshot,
                     JobContext& job_context,
                     std::vector<common::SequenceNumber> &existing_snapshots,
                     util::Directory* output_file_directory,
                     const util::EnvOptions *env_options,
                     memory::ArenaAllocator &arena)
    : cfd_(cfd),
      db_options_(db_options),
      mutable_cf_options_(mutable_cf_options),
      output_compression_(output_compression),
      shutting_down_(shutting_down),
      earliest_write_conflict_snapshot_(earliest_write_conflict_snapshot),
      job_context_(job_context),
      existing_snapshots_(existing_snapshots),
      output_file_directory_(output_file_directory),
      env_options_(env_options),
      arena_(arena),
      tmp_arena_(CharArena::DEFAULT_PAGE_SIZE, 0, memory::ModId::kFlush),
      pick_memtable_called_(false),
      min_hold_wal_file_id_(0)
//      recovery_point_()
{
}

BaseFlush::~BaseFlush() {

}

int BaseFlush::write_level0_table(MiniTables *mtables, uint64_t max_seq) {
  int ret = 0;
  AutoThreadOperationStageUpdater stage_updater(ThreadStatus::STAGE_FLUSH_WRITE_L0);
  const uint64_t start_micros = db_options_.env->NowMicros();
  uint64_t bytes_written = 0;
  std::vector<InternalIterator*> memtables;
  ReadOptions ro;
  ro.total_order_seek = true;
  uint64_t total_num_entries = 0;
  uint64_t total_num_deletes = 0;
  size_t total_memory_usage = 0;
  storage::LayerPosition output_layer_position(0);
  for (MemTable* m : mems_) {
    if (IS_NULL(m)) {
      ret = Status::kErrorUnexpected;
      FLUSH_LOG(WARN, "m is null", K(ret), K((int)job_context_.task_type_), K(cfd_->GetID()));
      break;
    } else if (DUMP_TASK == job_context_.task_type_) {
      memtables.push_back(m->NewDumpIterator(ro, max_seq, tmp_arena_));
      output_layer_position.layer_index_ = storage::LayerPosition::INVISIBLE_LAYER_INDEX;
    } else {
      output_layer_position.layer_index_ = storage::LayerPosition::NEW_GENERATE_LAYER_INDEX;
      memtables.push_back(m->NewIterator(ro, &tmp_arena_));
    }
    FLUSH_LOG(INFO, "Flushing memtable info",  K((int)job_context_.task_type_), K(cfd_->GetID()),
        K(job_context_.job_id)/*, K(m->get_redo_file_id())*/);
    //must have equal schema, assure when pick mems
//    mtables->schema = &(m->get_schema());
    total_num_entries += m->num_entries();
    total_num_deletes += m->num_deletes();
    total_memory_usage += m->ApproximateMemoryUsage();
  }
  if (SUCC(ret)) {
    FLUSH_LOG(INFO, "begin to run flush job", K((int)job_context_.task_type_), K(cfd_->GetID()), K(mems_.size()),
        K(total_num_entries), K(total_num_deletes), K(total_memory_usage));

    // memtables and range_del_iters store internal iterators over each data
    // memtable and its associated range deletion memtable, respectively, at
    // corresponding indexes.
    TEST_SYNC_POINT_CALLBACK("FlushJob::WriteLevel0Table:output_compression", &output_compression_);
    InternalIterator *merge_iter = NewMergingIterator(
        &cfd_->internal_comparator(), &memtables[0],
        static_cast<int>(memtables.size()), &arena_);
    bool is_flush = TaskType::FLUSH_TASK == job_context_.task_type_;
    mtables->table_space_id_ = cfd_->get_table_space_id();
    if (FAILED(BuildTable(*cfd_->ioptions(),
                          mutable_cf_options_,
                          merge_iter,
                          mtables,
                          cfd_->internal_comparator(),
                          cfd_->int_tbl_prop_collector_factories(),
                          cfd_->GetID(),
                          cfd_->GetName(),
                          existing_snapshots_,
                          earliest_write_conflict_snapshot_,
                          output_compression_,
                          cfd_->ioptions()->compression_opts,
                          mutable_cf_options_.paranoid_file_checks,
                          cfd_->internal_stats(),
                          output_layer_position,
                          Env::IO_HIGH,
                          nullptr,
                          is_flush))) {
      if (Status::kCancelTask != ret) {
        FLUSH_LOG(WARN, "failed to build table", K(ret), K(cfd_->GetID()));
      }
    } else {
      for (auto& meta : mtables->metas) {
        bytes_written += meta.fd.GetFileSize();
        FLUSH_LOG(INFO, "GENERATE new extent for flush or dump", K((int)job_context_.task_type_), K(cfd_->GetID()), K(meta));
      }
      FLUSH_LOG(INFO, "Level-0 flush table info", K((int)job_context_.task_type_),
          K(cfd_->GetID()), K(job_context_.job_id), K(mtables->metas.size()), K(bytes_written));
    }
    if (output_file_directory_ != nullptr) {
      output_file_directory_->Fsync();
    }
    FREE_OBJECT(InternalIterator, arena_, merge_iter);
  }
  TEST_SYNC_POINT("FlushJob::WriteLevel0Table");
  if (TaskType::DUMP_TASK != job_context_.task_type_) {
    // Note that here we treat flush as level 0 compaction in internal stats
    stop_record_flush_stats(bytes_written, start_micros);
  }
  return ret;
}

int BaseFlush::after_run_flush(MiniTables &mtables, int ret) {
  //get the Gratest recovery point
  MemTable* last_mem = mems_.size() > 0 ? mems_.back() : nullptr;
  RecoveryPoint recovery_point;

//  common::XengineSchema schema;
  if (SUCC(ret)) {
    if (IS_NULL(last_mem)
        || IS_NULL(mtables.change_info_)
        || IS_NULL(cfd_)) {
      ret = Status::kErrorUnexpected;
      FLUSH_LOG(ERROR, "arguments must not nullptr", K(ret), KP(last_mem), KP(cfd_));
    } else if (shutting_down_->load(std::memory_order_acquire)
               || cfd_->is_bg_stopped()) {
      ret = Status::kShutdownInProgress;
      XENGINE_LOG(WARN, "shutting down", K(ret), K(cfd_->is_bg_stopped()));
    } else if (cfd_->IsDropped()) {
      ret = Status::kErrorUnexpected;
      FLUSH_LOG(WARN, "Column family has been dropped!", K(ret), K(cfd_->GetID()));
    } else {
      recovery_point = last_mem->get_recovery_point();
      XENGINE_LOG(INFO, "get_recovery_point", KP(last_mem), K(recovery_point));
//      schema = last_mem->get_schema();
    }
  }
  if (FAILED(ret)) {
    // 2nd param of file_number is unused
    if (nullptr != cfd_
        && nullptr != cfd_->imm()
        && nullptr != job_context_.storage_logger_) {
      if (is_flush_task(job_context_.task_type_)) {
        cfd_->imm()->RollbackMemtableFlush(mems_, 0);
      } else if (nullptr != last_mem){
        last_mem->set_dump_in_progress(false);
      }
      job_context_.storage_logger_->abort();
    } else {
      FLUSH_LOG(ERROR, "unexpected error, can not abort", K(ret), K(cfd_->GetID()));
    }
  } else if (TaskType::DUMP_TASK == job_context_.task_type_) {
    if (FAILED(cfd_->apply_change_info(*(mtables.change_info_), true /*write_log*/, false /*is_replay*/, &recovery_point))) {
      if (nullptr != job_context_.storage_logger_) {
        job_context_.storage_logger_->abort();
      }
      FLUSH_LOG(WARN, "failed to apply change info for dump", K(ret));
    } else {
      last_mem->set_temp_min_prep_log(UINT64_MAX);
    }
    last_mem->set_dump_in_progress(false);
  } else if (FAILED(cfd_->apply_change_info(*(mtables.change_info_),
      true /*write_log*/, false /*is_replay*/, &recovery_point, 
      &mems_, &job_context_.memtables_to_free))) {
    if (nullptr != job_context_.storage_logger_) {
      job_context_.storage_logger_->abort();
    }
    FLUSH_LOG(WARN, "fail to apply change info", K(ret));
  } else {
    // do nothing
  }
  if (TaskType::FLUSH_LEVEL1_TASK != job_context_.task_type_) {
    // flush level1 not alloc change_info from arena_
    FREE_OBJECT(ChangeInfo, arena_, mtables.change_info_);
  }
  return ret;
}

void BaseFlush::cancel() {
//  db_mutex_->AssertHeld();
}

int BaseFlush::stop_record_flush_stats(const int64_t bytes_written,
                                       const uint64_t start_micros) {
  int ret = 0;
  // Note that here we treat flush as level 0 compaction in internal stats
  InternalStats::CompactionStats stats(1);
  stats.micros = db_options_.env->NowMicros() - start_micros;
  stats.bytes_written = bytes_written;
  cfd_->internal_stats()->AddCompactionStats(job_context_.output_level_ /* level */, stats);
  cfd_->internal_stats()->AddCFStats(InternalStats::BYTES_FLUSHED,
                                     bytes_written);
  cfd_->internal_stats()->AddCFStats(InternalStats::BYTES_WRITE,
                                     bytes_written);
  RecordFlushIOStats();
  return ret;
}

// delete M0's extens already exist
int BaseFlush::delete_old_M0(const InternalKeyComparator *internal_comparator, MiniTables &mtables) {
  int ret = Status::kOk;
  LayerPosition layer_position(0, storage::LayerPosition::INVISIBLE_LAYER_INDEX);
  const Snapshot *current_snapshot = nullptr;
  const ExtentLayer *dump_layer = nullptr;
  const ExtentMeta *lob_extent_meta = nullptr;
  table::InternalIterator *meta_iter = nullptr;
  MetaDataSingleIterator *range_iter = nullptr;
  if (IS_NULL(current_snapshot = cfd_->get_meta_snapshot())) {
    ret = Status::kErrorUnexpected;
    FLUSH_LOG(WARN, "unexpected error, current snapshot must not nullptr", K(ret));
  } else {
    dump_layer = current_snapshot->get_extent_layer(layer_position);
    if (nullptr == dump_layer || dump_layer->extent_meta_arr_.size() <= 0) {
      // do nothing
    } else {
      // delete large object extent
      for (int32_t i = 0; i < dump_layer->lob_extent_arr_.size() && SUCC(ret); i++) {
        lob_extent_meta = dump_layer->lob_extent_arr_.at(i);
        FLUSH_LOG(INFO, "delete large object extent for dump", K(cfd_->GetID()), "extent_id", lob_extent_meta->extent_id_);
        if (FAILED(mtables.change_info_->delete_large_object_extent(lob_extent_meta->extent_id_))) {
          FLUSH_LOG(WARN, "failed to delete large object extent", K(ret), K(i));
        }
      }

      if (SUCC(ret)) {
        if (FAILED(CompactionJob::create_meta_iterator(arena_, internal_comparator, current_snapshot, layer_position, meta_iter))) {
          FLUSH_LOG(WARN, "create meta iterator failed", K(ret));
        } else {
          MetaType type(MetaType::SSTable, MetaType::Extent, MetaType::InternalKey, 0, 0, 0);
          range_iter = PLACEMENT_NEW(MetaDataSingleIterator, tmp_arena_, type, meta_iter);
          if (IS_NULL(range_iter)) {
            ret = Status::kErrorUnexpected;
            FLUSH_LOG(WARN, "dump range iter is null", K(ret));
          } else {
            range_iter->seek_to_first();
            while (range_iter->valid() && SUCC(ret)) {
              MetaDescriptor extent_meta = range_iter->get_meta_descriptor().deep_copy(tmp_arena_);
              FLUSH_LOG(DEBUG, "delete extent for dump", K(cfd_->GetID()), K(extent_meta));
              if (FAILED(mtables.change_info_->delete_extent(extent_meta.layer_position_, extent_meta.extent_id_))) {
                FLUSH_LOG(WARN, "failed to delete extent", K(ret), K(extent_meta));
              } else {
                range_iter->next();
              }
            }
          }
        }
      }
    }
  }

  if (nullptr != current_snapshot) {
    cfd_->release_meta_snapshot(current_snapshot);
  }
  PLACEMENT_DELETE(MetaDataSingleIterator, arena_, range_iter);
  return ret;
}

int BaseFlush::fill_table_cache(const MiniTables &mtables) {
  int ret = 0;
  // Verify that the table is usable
  // We set for_compaction to false and don't OptimizeForCompactionTableRead
  // here because this is a special case after we finish the table building
  // No matter whether use_direct_io_for_flush_and_compaction is true,
  // we will regrad this verification as user reads since the goal is
  // to cache it here for further user reads
  if (IS_NULL(db_options_.env)
      || IS_NULL(env_options_)
      || IS_NULL(cfd_)
      || IS_NULL(cfd_->table_cache())) {
    ret = Status::kErrorUnexpected;
    FLUSH_LOG(WARN, "invalid ptr", K(ret), KP(db_options_.env), KP(env_options_), K(cfd_->GetID()));
    return ret;
  }
  EnvOptions optimized_env_options = db_options_.env->OptimizeForCompactionTableWrite(
      *env_options_, db_options_);
  for (size_t i = 0; i < mtables.metas.size() && SUCC(ret); i++) {
    const FileMetaData* meta = &mtables.metas[i];
    std::unique_ptr<InternalIterator, memory::ptr_destruct_delete<InternalIterator>> it(
        cfd_->table_cache()->NewIterator(ReadOptions(),
                                         optimized_env_options,
                                         cfd_->internal_comparator(),
                                         meta->fd,
                                         nullptr /* range_del_agg */,
                                         nullptr,
                                         (nullptr == cfd_->internal_stats())
                                          ? nullptr
                                          : cfd_->internal_stats()->GetFileReadHist(0),
                                         false /* for compaction*/,
                                         nullptr /* arena */,
                                         false /* skip_filter */,
                                         job_context_.output_level_ /*level*/,
                                         cfd_->internal_stats()));
    if (FAILED(it->status().code())) {
      FLUSH_LOG(WARN, "iterator occur error", K(ret), K(i));
    } else if (mutable_cf_options_.paranoid_file_checks) {
      for (it->SeekToFirst(); it->Valid(); it->Next()) {
      }
      if (FAILED(it->status().code())) {
        FLUSH_LOG(WARN, "iterator is invalid", K(ret));
      }
    }
  }
  return ret;
}

void BaseFlush::RecordFlushIOStats() {
  ThreadStatusUtil::IncreaseThreadOperationProperty(
      ThreadStatus::FLUSH_BYTES_WRITTEN, IOSTATS(bytes_written));
  IOSTATS_RESET(bytes_written);
}

FlushJob::FlushJob(const std::string& dbname, ColumnFamilyData* cfd,
                   const ImmutableDBOptions& db_options,
                   JobContext& job_context,
                   Directory* output_file_directory,
                   CompressionType output_compression,
                   Statistics* stats,
                   CompactionContext &context,
                   memory::ArenaAllocator &arena)
    : BaseFlush(cfd, db_options, *context.mutable_cf_options_, output_compression,
        context.shutting_down_, context.earliest_write_conflict_snapshot_, job_context,
        context.existing_snapshots_, output_file_directory, context.env_options_, arena),
      dbname_(dbname),
      stats_(stats),
      compaction_context_(context),
      meta_snapshot_(nullptr),
      compaction_(nullptr) {
  // Update the thread status to indicate flush.
  ReportStartedFlush();
  TEST_SYNC_POINT("FlushJob::FlushJob()");
}

FlushJob::~FlushJob() {
  ThreadStatusUtil::ResetThreadStatus();
  if (nullptr != compaction_) {
    FREE_OBJECT(MtExtCompaction, arena_, compaction_);
  }
}

void FlushJob::ReportStartedFlush() {
  assert(cfd_);
  assert(cfd_->ioptions());
//  ThreadStatusUtil::set_subtable_id(cfd_->GetID(), cfd_->ioptions()->env,
//                                    db_options_.enable_thread_tracking);
  ThreadStatusUtil::SetThreadOperation(ThreadStatus::OP_FLUSH);
  ThreadStatusUtil::SetThreadOperationProperty(ThreadStatus::COMPACTION_JOB_ID,
                                               job_context_.job_id);
  IOSTATS_RESET(bytes_written);
}

void FlushJob::ReportFlushInputSize(const autovector<MemTable*>& mems) {
  uint64_t input_size = 0;
  for (auto* mem : mems) {
    input_size += mem->ApproximateMemoryUsage();
  }
  ThreadStatusUtil::IncreaseThreadOperationProperty(
      ThreadStatus::FLUSH_BYTES_MEMTABLES, input_size);
}

// under mutex
void FlushJob::pick_memtable() {
  assert(!pick_memtable_called_);
  pick_memtable_called_ = true;
//  int32_t min_hold_wal_file_id = xlog::INVALID_FILE_ID;
  // Save the contents of the earliest memtable as a new Table
  assert(cfd_);
  assert(cfd_->imm());
  cfd_->imm()->PickMemtablesToFlush(&mems_/*, min_hold_wal_file_id*/);
  if (mems_.empty()) {
    return;
  }

  ReportFlushInputSize(mems_);

  // entries mems are (implicitly) sorted in ascending order by their created
  // time. We will use the first memtable's `edit` to keep the meta info for
  // this flush.
  //MemTable* m = mems_[0];
  //edit_ = m->GetEdits();
  //edit_->SetPrevLogNumber(0);
  // SetLogNumber(log_num) indicates logs with number smaller than log_num
  // will no longer be picked up for recovery.
  //edit_->SetLogNumber(mems_.back()->GetNextLogNumber());
  //edit_->SetColumnFamily(cfd_->GetID());
}

int FlushJob::run(MiniTables& mtables) {
  int ret = Status::kOk;
  if (IS_NULL(cfd_)) {
    ret = Status::kErrorUnexpected;
    FLUSH_LOG(WARN, "cfd is null", K(ret));
  } else if (mems_.empty()) {
    FLUSH_LOG(INFO, "Nothing in memtable to flush", K(cfd_->GetID()));
  } else if (TaskType::FLUSH_LEVEL1_TASK == job_context_.task_type_) {
    if (FAILED(run_mt_ext_task(mtables))) {
      if (Status::kCancelTask != ret) {
        FLUSH_LOG(WARN, "failed to run mt ext task", K(ret));
      }
    } else {
      FLUSH_LOG(INFO, "success to do flush level1 job", K(cfd_->GetID()));
    }
  } else if (FAILED(write_level0_table(&mtables))){
    if (Status::kCancelTask != ret) {
      FLUSH_LOG(WARN, "failed to write level0 table", K(ret));
    }
  } else if (FAILED(fill_table_cache(mtables))) {
    FLUSH_LOG(WARN, "failed to fill table cache", K(ret));
  } else {
    FLUSH_LOG(INFO, "success to do normal flush job", K(cfd_->GetID()));
  }
  RecordFlushIOStats();
  return ret;
}

int FlushJob::run_mt_ext_task(MiniTables &mtables) {
  int ret = 0;
  uint64_t start_micros = db_options_.env->NowMicros();
  FLUSH_LOG(INFO, "begin to run flush to level1 task", K(mems_.size()));
  if (IS_NULL(compaction_) || IS_NULL(cfd_)) {
    ret = Status::kNotInit;
    FLUSH_LOG(WARN, "compaction or cfd is null", K(ret), K(cfd_->GetID()));
  } else if (FAILED(compaction_->run())) {
    FLUSH_LOG(WARN, "failed to do met_ext_compaction", K(ret));
  } else {
    const MiniTables &mini_tables = compaction_->get_mini_tables();
    if (FAILED(fill_table_cache(mini_tables))) {
      FLUSH_LOG(WARN, "failed to fill table cache", K(ret));
    } else {
      mtables.change_info_ = &compaction_->get_change_info();
      FLUSH_LOG(INFO, "Level-0 flush info", K(cfd_->GetID()));
      if (FAILED(delete_old_M0(compaction_context_.internal_comparator_, compaction_->get_apply_mini_tables()))) {
        FLUSH_LOG(WARN, "failed to delete old m0", K(ret));
      }
    }
    FLUSH_LOG(INFO, "complete one mt_ext compaction",
        K(ret),
        K(cfd_->GetID()),
        K(job_context_.job_id),
        K(mini_tables.metas.size()),
        K(compaction_->get_stats().record_stats_),
        K(compaction_->get_stats().perf_stats_));
  }
  return ret;
}

int FlushJob::prepare_flush_task(MiniTables &mtables) {
  int ret = Status::kOk;
  pick_memtable(); // pick memtables

  if (TaskType::FLUSH_LEVEL1_TASK == job_context_.task_type_) {
    if (FAILED(prepare_flush_level1_task(mtables))) {
      FLUSH_LOG(WARN, "failed to prepare flush level1 task", K(ret));
    }
  } else if (IS_NULL(mtables.change_info_ = ALLOC_OBJECT(ChangeInfo, arena_))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for ChangeInfo", K(ret));
  } else {
    mtables.change_info_->task_type_ = TaskType::FLUSH_TASK;
    if (FAILED(delete_old_M0(compaction_context_.internal_comparator_, mtables))) {
      FLUSH_LOG(WARN, "failed to delete old m0", K(ret));
    }
  }
  return ret;
}

int FlushJob::prepare_flush_level1_task(MiniTables &mtables) {
  int ret = 0;
  compaction_context_.output_level_ = 1;
  storage::Range wide_range;
  RangeIterator *l1_iterator = nullptr;
  autovector<InternalIterator*> mem_iters;
  ReadOptions ro;
  ro.total_order_seek = true;
  int64_t total_size = 0;
  if (FAILED(get_memtable_iterators(ro, mem_iters, mtables, total_size))) {
    FLUSH_LOG(WARN, "failed to get memtable iterators", K(ret));
  } else if (FAILED(get_memtable_range(mem_iters, wide_range))) {
    FLUSH_LOG(WARN, "failed to get memtable range", K(ret), K(wide_range));
  } else if(FAILED(build_l1_iterator(wide_range, l1_iterator))) {
    FLUSH_LOG(WARN, "failed to build l1 iterator", K(ret), K(wide_range));
  } else if (FAILED(build_mt_ext_compaction(mem_iters, l1_iterator))) {
    FLUSH_LOG(WARN, "failed to build mt_ext_compaction", K(ret), KP(l1_iterator));
  } else if (nullptr != compaction_) {
    compaction_->add_input_bytes(total_size);
  }
  FREE_OBJECT(RangeIterator, arena_, l1_iterator);
  return ret;
}

int FlushJob::create_meta_iterator(const LayerPosition &layer_position,
                                   table::InternalIterator *&iterator)
{
  int ret = Status::kOk;
  ExtentLayer *layer = nullptr;
  ExtentLayerIterator *layer_iterator = nullptr;
  iterator = nullptr;

  if (IS_NULL(layer = meta_snapshot_->get_extent_layer(layer_position))) {
    ret = Status::kErrorUnexpected;
    FLUSH_LOG(WARN, "extent layer should not nullptr", K(ret), K(layer_position));
  } else if (IS_NULL(layer_iterator = ALLOC_OBJECT(ExtentLayerIterator, arena_))) {
    ret = Status::kMemoryLimit;
    FLUSH_LOG(WARN, "alloc memory for layer_iterator failed", K(ret), K(sizeof(ExtentLayerIterator)));
  } else if (FAILED(layer_iterator->init(compaction_context_.internal_comparator_,
                                         layer_position,
                                         layer))) {
    FLUSH_LOG(WARN, "fail to init layer_iterator", K(ret), K(layer_position));
  } else {
    iterator = layer_iterator;
  }
  return ret;
}

int FlushJob::parse_meta(const table::InternalIterator *iter, ExtentMeta *&extent_meta)
{
  int ret = Status::kOk;
  Slice key_buf;
  int64_t pos = 0;
  if (nullptr == iter) {
    ret = Status::kInvalidArgument;
    FLUSH_LOG(WARN, "invalid argument", K(ret), KP(iter));
  } else {
    key_buf = iter->key();
    extent_meta = reinterpret_cast<ExtentMeta*>(const_cast<char*>(key_buf.data()));
  }
  return ret;
}

int FlushJob::build_l1_iterator(storage::Range &wide_range,
    storage::RangeIterator *&iterator) {
  int ret = 0;
  table::InternalIterator *meta_iter = nullptr;
  ExtentMeta *extent_meta = nullptr;
  ExtentLayer *level1_extent_layer = nullptr;
  ExtentLayerVersion *level1_version = nullptr;
  LayerPosition layer_position(1, 0);
  if (IS_NULL(meta_snapshot_)) {
    ret = Status::kInvalidArgument;
    FLUSH_LOG(WARN, "meta snapshot is not inited", K(ret));
  } else if (FAILED(create_meta_iterator(layer_position, meta_iter))) {
    FLUSH_LOG(WARN, "fail to create meta iterator", K(ret), KP(level1_extent_layer));
  } else if (IS_NULL(meta_iter)) {
    ret = Status::kMemoryLimit;
    FLUSH_LOG(WARN, "create meta iter failed", K(ret));
  } else {  // get l1's range
    Slice smallest_key;
    Slice largest_key;
    meta_iter->SeekToLast();
    if (!meta_iter->Valid()) {
      // do nothing, no data
      return ret;
    } else if (FAILED(parse_meta(meta_iter, extent_meta))) {
      FLUSH_LOG(WARN, "parse meta failed", K(ret));
    } else if (IS_NULL(extent_meta)) {
      ret = Status::kErrorUnexpected;
      FLUSH_LOG(WARN, "extent meta must not nullptr", K(ret));
    } else {
      largest_key = extent_meta->largest_key_.Encode().deep_copy(arena_);
      wide_range.end_key_ = largest_key;
      int64_t level1_extents = 0;
      meta_iter->SeekToFirst();
      if (!meta_iter->Valid()) {
        // do nothing, no data
        return ret;
      } else if (FAILED(parse_meta(meta_iter, extent_meta))) {
        FLUSH_LOG(WARN, "parse meta kv failed", K(ret));
      } else if (IS_NULL(extent_meta)) {
        ret = Status::kErrorUnexpected;
        FLUSH_LOG(WARN, "unexpected error, extent meta must not nullptr", K(ret));
      } else {
        smallest_key = extent_meta->smallest_key_.Encode().deep_copy(arena_);
        wide_range.start_key_ = smallest_key;
      }
    }

    if (SUCC(ret)) {
      storage::MetaType type(MetaType::SSTable, MetaType::Extent,
                             MetaType::InternalKey, 1, 1, 0);
      iterator = ALLOC_OBJECT(MetaDataIterator, arena_, type, meta_iter,
          wide_range, *compaction_context_.data_comparator_);
    }
  }
  return ret;
}

int FlushJob::get_memtable_range(
    autovector<table::InternalIterator*> &memtables, storage::Range &wide_range) {
  int ret = 0;
  Slice smallest_key;
  Slice largest_key;
  int64_t way_size = (int64_t)memtables.size();
  db::InternalKeyComparator comparator(compaction_context_.data_comparator_);
  for (int64_t i = 0; i < way_size && SUCC(ret); ++i) {
    InternalIterator *cur_iter = memtables.at(i);
    if (IS_NULL(cur_iter)) {
      ret = Status::kErrorUnexpected;
      FLUSH_LOG(WARN, "cur_iter is null", K(ret));
    } else {
      cur_iter->SeekToFirst();
      if (cur_iter->Valid()) { // set smallest_key
        if (0 == smallest_key.size()
            || comparator.Compare(cur_iter->key(), smallest_key) < 0) {
          smallest_key = cur_iter->key().deep_copy(arena_);
        }
      }
      cur_iter->SeekToLast();
      if (cur_iter->Valid()) { // set largest_key
        if (0 == largest_key.size()
            || comparator.Compare(cur_iter->key(), largest_key) > 0) {
          largest_key = cur_iter->key().deep_copy(arena_);
        }
      }
    }
  }
  if (SUCC(ret)) {
    wide_range.start_key_ = smallest_key;
    wide_range.end_key_ = largest_key;
    FLUSH_LOG(INFO, "get memtable range succ!", K(smallest_key), K(largest_key), K(way_size));
  }
  return ret;
}

int FlushJob::build_mt_ext_compaction(
    autovector<table::InternalIterator*> &memtables,
    RangeIterator *l1_iterator) {
  int ret = 0;
  MetaDescriptorList extents;
  // if level1 has no data, l1_iterator will be null
  if (nullptr != l1_iterator) {
    l1_iterator->seek_to_first();
    while (l1_iterator->valid()) {
      MetaDescriptor md = l1_iterator->get_meta_descriptor().deep_copy(arena_);
      extents.push_back(md);
      l1_iterator->next();
      if (l1_iterator->valid()) { // middle
        l1_iterator->next();
      }
    }
  }
  storage::ColumnFamilyDesc cf_desc((int32_t)cfd_->GetID(), cfd_->GetName());
  compaction_ = ALLOC_OBJECT(MtExtCompaction, arena_, compaction_context_, cf_desc, arena_);
  MemTable *last_mem = mems_.back();
  if (IS_NULL(compaction_)) {
    ret = Status::kMemoryLimit;
    FLUSH_LOG(WARN, "failed to alloc memory for compaction", K(ret));
  } else if (FAILED(compaction_->add_mem_iterators(memtables))) {
    FLUSH_LOG(WARN, "failed to add memtable iterators", K(ret));
  } else if (IS_NULL(last_mem)) {
    ret = Status::kInvalidArgument;
    FLUSH_LOG(WARN, "last mem is null", K(ret));
  } else {
//    compaction_->set_schema(&last_mem->get_schema());
    if (0 == extents.size()) {
      // level1 has no data
    } else if (FAILED(compaction_->add_merge_batch(extents, 0, extents.size()))) {
      FLUSH_LOG(WARN, "failed to add merge batch", K(ret), K(extents.size()));
    } else {
      FLUSH_LOG(INFO, "build mt_ext_task success!", K(extents.size()), K(memtables.size()));
    }
  }
  return ret;
}

int FlushJob::get_memtable_iterators(
    ReadOptions &ro,
    autovector<InternalIterator*> &memtables,
    MiniTables& mtables,
    int64_t &total_size) {
  int ret = 0;
  for (MemTable* m : mems_) {
    if (IS_NULL(m)) {
      ret = Status::kErrorUnexpected;
      FLUSH_LOG(WARN, "m is null", K(ret));
      break;
    }
    total_size += m->ApproximateMemoryUsage();
    if (nullptr != cfd_) {
      FLUSH_LOG(INFO, "Flushing memtable info", K(cfd_->GetName()),
          K(job_context_.job_id)/*, K(m->get_redo_file_id())*/);
    }
    memtables.push_back(m->NewIterator(ro, &tmp_arena_));
//    mtables.schema = &(m->get_schema()); // no use
  }
  return ret;
}
}
}  // namespace xengine
