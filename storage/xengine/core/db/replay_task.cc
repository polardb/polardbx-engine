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

#include "db/replay_task.h"
#include "db/replay_thread_pool.h"
#include "db/write_batch_internal.h"
#include "xengine/write_batch.h"
#include "xengine/status.h"
#include "logger/logger.h"
#include "logger/log_module.h"
#include "util/filename.h"
#include "util/sync_point.h"

using namespace xengine;
using namespace common;

namespace xengine {
namespace db {
void ReplayTask::run() {
  int ret = Status::kOk;
  if (db_impl_ == nullptr) {
    ret = Status::kInvalidArgument;
    replay_thread_pool_->set_error(); // notify replay thread pool to stop running
  } else if (FAILED(replay_log())) {
    XENGINE_LOG(ERROR, "replay log fail", K(ret), K(log_file_number_),
        "sequence", WriteBatchInternal::Sequence(write_batch_),
        "dropping size", write_batch_->GetDataSize(), K(is_last_record_),
        K(last_record_end_pos_));
    replay_thread_pool_->set_error(); // notify replay thread pool to stop running
  }
  XENGINE_LOG(DEBUG, "replay wal record", K(ret), K(log_file_number_), "sequence",
      WriteBatchInternal::Sequence(write_batch_), "size", write_batch_->GetDataSize(),
      "count", write_batch_->Count());
}

int ReplayTask::replay_log() {
  int ret = Status::kOk;
#ifndef NDEBUG
  TEST_SYNC_POINT_CALLBACK("ReplayTask::replay_log::replay_error", this);
  if (replay_error_) {
    ret = Status::kCorruption;
    XENGINE_LOG(ERROR, "replay wal record failed", K(ret), K(log_file_number_),
        "sequence", WriteBatchInternal::Sequence(write_batch_));
    return ret;
  }
#endif
  SequenceNumber next_allocate_sequence = kMaxSequenceNumber;
  if (FAILED(db_impl_->parallel_recovery_write_memtable(*write_batch_,
          log_file_number_, &next_allocate_sequence))) {
    XENGINE_LOG(ERROR, "replay wal record failed", K(ret), K(log_file_number_),
        "sequence", WriteBatchInternal::Sequence(write_batch_));
  } else if (FAILED(db_impl_->update_max_sequence_and_log_number(
          next_allocate_sequence, log_file_number_))) {
    XENGINE_LOG(ERROR, "update max sequence failed", K(ret), K(log_file_number_),
        "sequence", WriteBatchInternal::Sequence(write_batch_));
  }
  return ret;
}

bool ReplayTask::need_before_barrier_during_replay() {
#ifndef NDEBUG
  TEST_SYNC_POINT_CALLBACK("ReplayTask::need_before_barrier_during_replay::need_barrier",
      &need_barrier_);
  if (need_barrier_ && write_batch_->HasDelete()) {
    return true;
  }
#endif
  return false;
}

bool ReplayTask::need_after_barrier_during_replay() {
#ifndef NDEBUG
  TEST_SYNC_POINT_CALLBACK("ReplayTask::need_after_barrier_during_replay::need_barrier",
      &need_barrier_);
  if (need_barrier_ && write_batch_->HasDelete()) {
    return true;
  }
#endif
  return false;
}

// TODO sliding window
Status ReplayTaskParser::parse_replay_writebatch_from_record(Slice& record,
    uint32_t crc, DBImpl *db_impl, WriteBatch **write_batch, bool allow_2pc) {
  if (record.size() < WriteBatchInternal::kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }
  if (crc != log::Writer::calculate_crc(record)) {
    return Status::Corruption("checksum mismatch");
  }
  *write_batch = MOD_NEW_OBJECT(memory::ModId::kWriteBatch, WriteBatch);
  if (*write_batch == nullptr) {
    return Status::MemoryLimit("fail to alloc memory for replay write_batch");
  }
  Status s;
  if (!allow_2pc) {
    s = WriteBatchInternal::SetContents(*write_batch, record);
    if (UNLIKELY(!s.ok())) {
      MOD_DELETE_OBJECT(WriteBatch, (*write_batch));
    }
    return s;
  }
  SequenceNumber sequence = util::DecodeFixed64(record.data());
  Slice key, value, blob, xid;
  int found = 0;
  int total_count = util::DecodeFixed32(record.data() + 8);
  record.remove_prefix(WriteBatchInternal::kHeader);
  WriteBatchInternal::SetSequence(*write_batch, sequence);
  while (s.ok() && !record.empty()) {
    char tag = 0;
    uint32_t column_family = 0;
    s = ReadRecordFromWriteBatch(&record, &tag, &column_family, &key, &value, &blob, &xid);
    if (!s.ok()) {
      break;
    }
    SequenceNumber prepare_seq = kMaxSequenceNumber;
    switch (tag) {
      case kTypeColumnFamilyValue:
      case kTypeValue:
        s = WriteBatchInternal::Put(*write_batch, column_family, key, value);
        found++;
        break;
      case kTypeColumnFamilyDeletion:
      case kTypeDeletion:
        s = WriteBatchInternal::Delete(*write_batch, column_family, key);
        found++;
        break;
      case kTypeColumnFamilySingleDeletion:
      case kTypeSingleDeletion:
        s = WriteBatchInternal::SingleDelete(*write_batch, column_family, key);
        found++;
        break;
      case kTypeColumnFamilyRangeDeletion:
      case kTypeRangeDeletion:
        s = WriteBatchInternal::DeleteRange(*write_batch, column_family, key, value);
        found++;
        break;
      case kTypeColumnFamilyMerge:
      case kTypeMerge:
        s = WriteBatchInternal::Merge(*write_batch, column_family, key, value);
        found++;
        break;
      case kTypeLogData:
        s = (*write_batch)->PutLogData(blob);
        break;
      case kTypeBeginPrepareXID:
        s = WriteBatchInternal::MarkBeginPrepare(*write_batch);
        break;
      case kTypeEndPrepareXID:
        if (!db_impl->insert_prepare_sequence_into_xid_map(xid.ToString(), sequence)) {
          s = Status::Corruption("failed to insert into xid map, xid=" + xid.ToString(true));
        } else {
          s = WriteBatchInternal::MarkEndPrepare(*write_batch, xid, sequence);
        }
        break;
      case kTypeCommitXID:
        if (!db_impl->get_prepare_sequence_from_xid_map(xid.ToString(), &prepare_seq)) {
          XENGINE_LOG(INFO, "xid hasn't been prepared before commit", "xid", xid.ToString(true));
          s = WriteBatchInternal::MarkCommit(*write_batch, xid, 0);
        } else {
          s = WriteBatchInternal::MarkCommit(*write_batch, xid, prepare_seq);
        }
        sequence++; // in case 2 transactions with the same xid in one write_batch
        break;
      case kTypeRollbackXID:
        if (!db_impl->get_prepare_sequence_from_xid_map(xid.ToString(), &prepare_seq)) {
          XENGINE_LOG(INFO, "xid hasn't been prepared before rollback", "xid", xid.ToString(true));
          s = WriteBatchInternal::MarkRollback(*write_batch, xid, 0);
        } else {
          s = WriteBatchInternal::MarkRollback(*write_batch, xid, prepare_seq);
        }
        sequence++; // in case 2 transactions with the same xid in one write_batch
        break;
      case kTypeNoop:
        WriteBatchInternal::InsertNoop(*write_batch);
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (UNLIKELY(found != total_count)) {
    s = Status::Corruption("WriteBatch has wrong count");
  }
  if (UNLIKELY(!s.ok())) {
    MOD_DELETE_OBJECT(WriteBatch, (*write_batch));
  }
  return s;
}

} // namespace db
} // namespace xengine
