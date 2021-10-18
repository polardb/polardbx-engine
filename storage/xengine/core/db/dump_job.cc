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
#include "db/dump_job.h"
#include "db/builder.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "logger/logger.h"
#include "storage/multi_version_extent_meta_layer.h"
#include "storage/storage_logger.h"

namespace xengine {
using namespace common;
using namespace storage;
using namespace util;
using namespace memory;
namespace db {

DumpJob::DumpJob(ColumnFamilyData* cfd,
                 const common::ImmutableDBOptions& db_options,
                 const common::MutableCFOptions& mutable_cf_options,
                 common::CompressionType output_compression,
                 std::atomic<bool>* shutting_down,
                 common::SequenceNumber earliest_write_conflict_snapshot,
                 JobContext& job_context,
                 std::vector<common::SequenceNumber> &existing_snapshots,
                 util::Directory* output_file_directory,
                 const util::EnvOptions *env_options,
                 MemTable *dump_mem,
                 const common::SequenceNumber dump_max_seq,
                 memory::ArenaAllocator &arena)
    : BaseFlush(cfd, db_options, mutable_cf_options, output_compression,
        shutting_down, earliest_write_conflict_snapshot, job_context,
        existing_snapshots, output_file_directory, env_options, arena),
      dump_mem_(dump_mem),
      dump_max_seq_(dump_max_seq)
{
}

DumpJob::~DumpJob()
{
}

int DumpJob::prepare_flush_task(MiniTables& mtables) {
  int ret = Status::kOk;
  pick_memtable();
  if (IS_NULL(mtables.change_info_ = ALLOC_OBJECT(ChangeInfo, arena_))) {
    ret = Status::kMemoryLimit;
    FLUSH_LOG(WARN, "fail to allocate memory for ChangeInfo", K(ret));
  } else if (FAILED(delete_old_M0(&cfd_->internal_comparator(), mtables))) {
    FLUSH_LOG(WARN, "failed to delete old m0", K(ret));
  } else {
    mtables.change_info_->task_type_ = TaskType::DUMP_TASK;
  }
  return ret;
}

int DumpJob::run(MiniTables& mtables) {
  int ret = 0;
  if (IS_NULL(cfd_) || IS_NULL(cfd_->get_storage_manager())) {
    ret = Status::kInvalidArgument;
    FLUSH_LOG(WARN, "cfd or storage_manager is null", K(ret), KP(cfd_));
  } else if (mems_.empty()) {
    FLUSH_LOG(INFO, "Nothing in memtable to dump", K(cfd_->GetID()));
  } else {
    FLUSH_LOG(INFO, "begin to run dump job", K(mems_.size()), K(cfd_->GetID()));
    // build one new M0
    if (SUCC(ret) && FAILED(write_level0_table(&mtables, dump_max_seq_))) {
      FLUSH_LOG(WARN, "failed to write level0 table", K(ret));
    }
  }
  return ret;
}

// under mutex
void DumpJob::pick_memtable() {
  if (nullptr != dump_mem_
      && !dump_mem_->is_dump_in_progress()
      && !dump_mem_->is_flush_in_progress()) {
    assert(dump_mem_);
    dump_mem_->set_dump_in_progress(true);
    mems_.push_back(dump_mem_);
  } else {
    bool in_flush = false;
    if (nullptr != dump_mem_) {
      in_flush = dump_mem_->is_flush_in_progress();
    }
    FLUSH_LOG(INFO, "has no valid mem to dump", KP(dump_mem_), K(in_flush));
  }
  pick_memtable_called_ = true;
}
}
}
