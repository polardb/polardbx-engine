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

#include "compact/new_compaction_iterator.h"
#include "compact/task_type.h"
#include "logger/logger.h"
#include "table/internal_iterator.h"
//#include "utilities/field_extractor/field_extractor.h"

using namespace xengine;
using namespace common;
using namespace storage;
using namespace util;
using namespace table;
using namespace db;

namespace xengine {
namespace storage {

NewCompactionIterator::NewCompactionIterator(
    const util::Comparator *cmp,
    const util::Comparator *internal_cmp,
    common::SequenceNumber last_sequence,
    std::vector<common::SequenceNumber> *snapshots,
    common::SequenceNumber earliest_write_conflict_snapshot,
    bool expect_valid_internal_key,
    MultipleSEIterator *cur_iterator,
    /*const XengineSchema *schema,*/
    memory::SimpleAllocator &arena,
    storage::ChangeInfo &change_info,
    const int level,
    const std::atomic<bool> *shutting_down,
    const std::atomic<bool> *bg_stopped,
    const std::atomic<int64_t> *cancel_type,
    const bool need_check_snapshot,
    const bool background_disable_merge)
    : cmp_(cmp),
      internal_cmp_(internal_cmp),
      snapshots_(snapshots),
      earliest_write_conflict_snapshot_(earliest_write_conflict_snapshot),
      expect_valid_internal_key_(expect_valid_internal_key),
      shutting_down_(shutting_down),
      bg_stopped_(bg_stopped),
      cancel_type_(),
      valid_(false),
      has_current_user_key_(false),
      lastkey_seq_(0),
      lastkey_snapshot_(0),
      has_outputted_key_(false),
      clear_and_output_next_key_(false),
      at_next_(false),
      cur_iterator_(cur_iterator),
      output_level_(SEIterator::kDataEnd),
//      row_arena_(row_arena),
//      dst_schema_(schema),
      arena_(arena),
      change_info_(change_info),
      level_(level),
//      cur_schema_(nullptr),
      need_check_snapshot_(need_check_snapshot),
      background_disable_merge_(background_disable_merge) {
  if (snapshots_->size() == 0) {
    // optimize for fast path if there are no snapshots
    visible_at_tip_ = true;
    earliest_snapshot_ = last_sequence;
    latest_snapshot_ = 0;
  } else {
    visible_at_tip_ = false;
    earliest_snapshot_ = snapshots_->at(0);
    latest_snapshot_ = snapshots_->back();
  }
  ignore_snapshots_ = false;
}

NewCompactionIterator::~NewCompactionIterator() {}
void NewCompactionIterator::reset() {
  valid_ = false;
  has_current_user_key_ = false;
  lastkey_seq_ = 0;
  lastkey_snapshot_ = 0;
  has_outputted_key_ = false;
  clear_and_output_next_key_ = false;
  at_next_ = false;
  cur_iterator_ = nullptr;
  output_level_ = SEIterator::kDataEnd;
}

bool NewCompactionIterator::is_canceled() {
  return (cancel_type_) && (cancel_type_->load(std::memory_order_relaxed) & (1LL << change_info_.task_type_));
}

void NewCompactionIterator::ResetRecordCounts() {
  iter_stats_.num_record_drop_user = 0;
  iter_stats_.num_record_drop_hidden = 0;
  iter_stats_.num_record_drop_obsolete = 0;
  iter_stats_.num_record_drop_range_del = 0;
  iter_stats_.num_range_del_drop_obsolete = 0;
}

int NewCompactionIterator::seek_to_first() {
  int ret = 0;
  if (nullptr != cur_iterator_) {
    cur_iterator_->seek_to_first();
    output_level_ = cur_iterator_->get_output_level();
//    cur_schema_ = cur_iterator_->get_schema();
    if (!cur_iterator_->valid()) {
      // only one way and reuse all extent/block
      COMPACTION_LOG(WARN, "get next item failed.");
    } else if (FAILED(process_next_item())) {
      COMPACTION_LOG(WARN, "process next item failed.", K(ret));
    } else if (FAILED(prepare_output())) {
      COMPACTION_LOG(WARN, "prepare output failed.", K(ret));
    } else {
    }
  } else {
    ret = Status::kErrorUnexpected;
    COMPACTION_LOG(WARN, "iterator is null", K(ret));
  }
  return ret;
}

int NewCompactionIterator::next_item() {
  // use current_key, avoid to copy again
  int ret = 0;
  if (IS_NULL(cur_iterator_)) {
    ret = Status::kInvalidArgument;
    COMPACTION_LOG(WARN, "cur_iterator is null", K(ret));
  } else {
    cur_iterator_->set_last_user_key(current_key_.GetUserKey());
    if (FAILED(cur_iterator_->next())) {
      COMPACTION_LOG(WARN, "failed to get next item", K(ret));
    } else {
      output_level_ = cur_iterator_->get_output_level();
//      cur_schema_ = cur_iterator_->get_schema();
    }
  }
  return ret;
}

int NewCompactionIterator::next() {
  int ret = 0;
  if (!at_next_) { // single_del
    ret = next_item();
  } else {
    at_next_ = false;
    output_level_ = cur_iterator_->get_output_level();
//    cur_schema_ = cur_iterator_->get_schema();
  }
  if (SUCC(ret) && SEIterator::kKVLevel == output_level_) {
    ret = process_next_item();
  }

  if (SUCC(ret)) {
    if (valid_) {
      has_outputted_key_ = true;
    }
    ret = prepare_output();
  }
  return ret;
}

int NewCompactionIterator::do_single_deletion(
    common::SequenceNumber prev_snapshot) {
  int ret = 0;
  ParsedInternalKey next_ikey;
//  const XengineSchema *last_schema = cur_schema_;
  if (FAILED(next_item())) {
  } else if (IS_NULL(cur_iterator_)) {
    ret = Status::kCorruption;
    COMPACTION_LOG(WARN, "cur iterator is null", K(ret));
  } else if (SEIterator::kKVLevel == cur_iterator_->get_output_level()
      && ParseInternalKey(cur_iterator_->get_key(), &next_ikey)
      && cmp_->Equal(ikey_.user_key, next_ikey.user_key)) {
    // (a, del), (a, xx)
    // Check whether the next_key belongs to the same snapshot as the
    // SingleDelete.
    if (prev_snapshot == 0 || next_ikey.sequence > prev_snapshot) {
      if (next_ikey.type == kTypeSingleDeletion) {
        // skip the first SingleDelete
        ++iter_stats_.num_record_drop_obsolete;
        ++iter_stats_.num_single_del_mismatch;
      } else if ((ikey_.sequence <= earliest_write_conflict_snapshot_) ||
                 has_outputted_key_) {
        // drop the SingleDelete and the next record
        if (next_ikey.type != kTypeValue) {
          ++iter_stats_.num_single_del_mismatch;
        }
        ++iter_stats_.num_record_drop_hidden;
        ++iter_stats_.num_record_drop_obsolete;
        if (kTypeValueLarge == next_ikey.type) {
          record_large_objects_info(cur_iterator_->get_key(), cur_iterator_->get_value());
        }
        ret = next_item();
      } else {
        // output the SingleDelete,
        // Found a matching value, but we cannot drop both keys since
        // there is an earlier snapshot and we need to leave behind a record
        // to know that a write happened in this snapshot (Rule 2 above).
        // Clear the value and output the SingleDelete. (The value will be
        // outputted on the next iteration.)
        valid_ = true;
        clear_and_output_next_key_ = true;
      }
    } else {
      valid_ = true;
    }
  } else { // (a, del), (b, xx)
    has_current_user_key_ = false;
    // if it's bottom level, drop it
    if (ikey_.sequence <= earliest_snapshot_ && 2 == level_) {
      ++iter_stats_.num_record_drop_obsolete;
      ++iter_stats_.num_single_del_fallthru;
    } else { // Output SingleDelete
      valid_ = true;
    }
  }
  if (valid_) {
    at_next_ = true;
    // because call next() again, maybe it is kBlockLevel/kExtentLevel
    // update cur_schema , be consistent with output key.
    output_level_ = SEIterator::kKVLevel;
//    cur_schema_ = last_schema;
  }
  return ret;
}

bool NewCompactionIterator::valid() {
  assert(cur_iterator_);
  return SEIterator::kDataEnd != output_level_;
}

int NewCompactionIterator::record_large_objects_info(
    const common::Slice &large_key,
    const common::Slice &large_value) {
  UNUSED(large_key);
  int ret = Status::kOk;
  int64_t pos = 0;
  LargeValue lob_value;
  if (FAILED(lob_value.deserialize(large_value.data(), large_value.size(), pos))) {
    COMPACTION_LOG(WARN, "fail to deserialize large value", K(ret), "size", large_value.size(), K(pos));
  } else {
    for (uint32_t i = 0; SUCCED(ret) && i < lob_value.oob_extents_.size(); ++i) {
      if (FAILED(change_info_.delete_large_object_extent(lob_value.oob_extents_.at(i)))) {
        COMPACTION_LOG(WARN, "fail to delete large object extent", K(ret), K(i), "extents_size", lob_value.oob_extents_.size(), "extent_id", lob_value.oob_extents_.at(i));
      }
    }
  }

  return ret;
}

int NewCompactionIterator::deal_with_kv_with_snapshot() {
  int ret = Status::kOk;
  SequenceNumber prev_snapshot = 0;
  SequenceNumber curkey_snapshot = earliest_snapshot_;
  if (!visible_at_tip_
      && FAILED(earliest_visible_snapshot(ikey_.sequence, curkey_snapshot, prev_snapshot))) {
    COMPACTION_LOG(WARN, "get earliest visible snapshot failed.", K(ret));
  } else if (clear_and_output_next_key_) {
    if (kTypeValue != ikey_.type || curkey_snapshot != lastkey_snapshot_) {
      ret = Status::kInvalidArgument;
      COMPACTION_LOG(WARN, "invalid key.type or invalid cur_userkey_snapshot_.",
          K((int64_t)ikey_.type), K(curkey_snapshot), K(ret));
    } else {
      if (kTypeValueLarge == ikey_.type) {
        if (FAILED(record_large_objects_info(key_, value_))) {
          COMPACTION_LOG(WARN, "fail to record large objects info", K(ret));
        }
      }
      // last is SingleDelete, save space by removing the PUT's value
      value_.clear();
      valid_ = true;
      clear_and_output_next_key_ = false;
    }
  } else if (ikey_.type == kTypeSingleDeletion) {
    if (FAILED(do_single_deletion(prev_snapshot))) {
      COMPACTION_LOG(WARN, "do single deletion failed.", K(ret));
    }
  } else if (lastkey_snapshot_ == curkey_snapshot) {
    // user_key is equal and visible is equal
    if (lastkey_seq_ < ikey_.sequence) { // not ordered
      ret = Status::kInvalidArgument;
      COMPACTION_LOG(WARN, "invalid userkey sequence number.", K(ikey_.sequence), K(ret));
    } else { // drop cur record
      ++iter_stats_.num_record_drop_hidden;
      // process for large objects
      if (kTypeValueLarge == ikey_.type) {
        if (FAILED(record_large_objects_info(key_, value_))) {
          COMPACTION_LOG(WARN, "fail to record large large object info", K(ret));
        }
      }
      ret = next_item();
    }
  } else if (ikey_.type == kTypeDeletion
             && ikey_.sequence <= earliest_snapshot_
             && 2 == level_) {
    // todo optimize for level1
    // drop cur record
    ++iter_stats_.num_record_drop_obsolete;
    ret = next_item();
  } else {
    valid_ = true;
  }
  if (SUCC(ret)) {
    lastkey_seq_ = ikey_.sequence;
    lastkey_snapshot_ = curkey_snapshot;
  }
  return ret;
}

int NewCompactionIterator::deal_with_kv_without_snapshot(const bool is_equal) {
  int ret = Status::kOk;
  if (ikey_.sequence > earliest_snapshot_ 
      || ikey_.sequence > earliest_write_conflict_snapshot_) {
    ret = Status::kErrorUnexpected;
    COMPACTION_LOG(ERROR, "invalid sequence", K(ret), K(ikey_.sequence), K(earliest_snapshot_));
  } else if (ikey_.type == kTypeSingleDeletion) {
    if (FAILED(do_single_deletion(0))) {
      COMPACTION_LOG(WARN, "do single deletion failed.", K(ret));
    }
  } else if (is_equal) {
    // user_key is equal and visible is equal
    if (lastkey_seq_ < ikey_.sequence) { // not ordered
      ret = Status::kInvalidArgument;
      COMPACTION_LOG(WARN, "invalid userkey sequence number.", K(ikey_.sequence), K(ret));
    } else { // drop cur record
      ++iter_stats_.num_record_drop_hidden;
      // process for large objects
      if (kTypeValueLarge == ikey_.type) {
        record_large_objects_info(key_, value_);
      }
      ret = next_item();
    }
  } else if (ikey_.type == kTypeDeletion
             && 2 == level_) {
    // todo optimize for level1
    // drop cur record
    ++iter_stats_.num_record_drop_obsolete;
    ret = next_item();
  } else {
    valid_ = true;
  }
  if (SUCC(ret)) {
    lastkey_seq_ = ikey_.sequence;
  }
  return ret;
}

int NewCompactionIterator::process_next_item() {
  int ret = Status::kOk;
  at_next_ = false;
  valid_ = false;
  while (!valid_
         && SEIterator::kKVLevel == output_level_
         && SUCC(ret)
         && !IsShuttingDown()
         && !is_bg_stopped()) {
    key_ = cur_iterator_->get_key();
    value_ = cur_iterator_->get_value();
    iter_stats_.num_input_records++;
    if (!ParseInternalKey(key_, &ikey_)) {
      ret = Status::kCorruption;
      COMPACTION_LOG(ERROR, "key is invalid", K(ret), K(ikey_.user_key), K(ikey_));
      break;
    }
    // Update input statistics
    if (ikey_.type == kTypeDeletion || ikey_.type == kTypeSingleDeletion) {
      iter_stats_.num_input_deletion_records++;
    }
    iter_stats_.total_input_raw_key_bytes += key_.size();
    iter_stats_.total_input_raw_value_bytes += value_.size();

    Slice last_userkey = current_key_.GetUserKey();
    if (!has_current_user_key_
        ||!cmp_->Equal(ikey_.user_key, last_userkey)) {
      // update key_ point to the copy of key_
      key_ = current_key_.SetInternalKey(key_, &ikey_);
      has_current_user_key_ = true;
      has_outputted_key_ = false;
      lastkey_seq_ = kMaxSequenceNumber;
      lastkey_snapshot_ = 0;
    } else {  // update current_key's seq,type
      current_key_.UpdateInternalKey(ikey_.sequence, ikey_.type);
      key_ = current_key_.GetInternalKey();
      ikey_.user_key = current_key_.GetUserKey();
    }

    //when background_disable_merge_ is true, that means it's new subtable, and we should maintain all records before index-build ready.
    if (true == background_disable_merge_) {
       valid_ = true;
       break;
    }

    if (need_check_snapshot_) {
      if (FAILED(deal_with_kv_with_snapshot())) {
        COMPACTION_LOG(WARN, "failed to deal with kv with snapshot", K(ret), K(key_));
      }
    } else {
      if (FAILED(deal_with_kv_without_snapshot(kMaxSequenceNumber == lastkey_seq_))) {
        COMPACTION_LOG(WARN, "failed to deal with kv without snapshot", K(ret), K(key_));
      }
    }
  }

  if (SUCC(ret) && !valid_ && (IsShuttingDown() || is_bg_stopped())) {
    ret = Status::kShutdownInProgress;
    COMPACTION_LOG(WARN, "invalid shutdown or bg_stopped.", K(ret));
  }
  if (SUCC(ret) && is_canceled()) {
    ret = Status::kCancelTask;
    COMPACTION_LOG(INFO, "task has been canceled", K(ret), K(get_task_type_name(change_info_.task_type_)));
  }
  return ret;
}

int NewCompactionIterator::prepare_output() {
  int ret = Status::kOk;

  //if we disable merge, there need keep all multi-version value with different sequence,
  if (background_disable_merge_) {
    return ret;  
  }

  if (SEIterator::kKVLevel != output_level_) {
    // do nothing, no data
    /*  } else if (IS_NULL(cur_schema_) || IS_NULL(dst_schema_)) {
    ret = Status::kAborted;
    COMPACTION_LOG(WARN, "invalid cur_schema or dst_schema.", K(ret), KP(cur_schema_));
 } else if (cur_schema_->get_schema_version() < dst_schema_->get_schema_version()) {
    Slice key = key_;
    Slice tmp_value = value_;
    uint64_t num = util::DecodeFixed64(key.data() + key.size() - 8);
    unsigned char c = num & 0xff;
    ValueType type = static_cast<ValueType>(c);
    if (kTypeValue == type && value_.size()
        && FAILED(FieldExtractor::get_instance()->convert_schema(cur_schema_,
            dst_schema_, value_, tmp_value, row_arena_))) {
      COMPACTION_LOG(WARN, "switch value failed.", K(ret), K(value_));
    } else {
      value_ = tmp_value;
    } */
  }
  if (FAILED(ret)) {
  } else if (2 != level_) {
    // do nothing
  } else if (valid_ && SEIterator::kKVLevel == output_level_ &&
             ikey_.sequence < earliest_snapshot_ && ikey_.type != kTypeMerge) {
    if (kTypeDeletion == ikey_.type || kTypeSingleDeletion == ikey_.type) {
      ret = Status::kInvalidArgument;
      COMPACTION_LOG(WARN, "invalid key type.", K((int64_t)ikey_.type), K(ret));
    } else if (kTypeValueLarge != ikey_.type) {
      ikey_.sequence = 0;
      current_key_.UpdateInternalKey(0, ikey_.type);
    }
  }
  return ret;
}

int NewCompactionIterator::earliest_visible_snapshot(
    SequenceNumber in, SequenceNumber &out, SequenceNumber &prev_snapshot) {
  int ret = Status::kOk;
  if (IS_NULL(snapshots_)) {
    ret = Status::kInvalidArgument;
    COMPACTION_LOG(WARN, "invalid snapshots_.", K(ret));
  } else {
    SequenceNumber prev = kMaxSequenceNumber;
    SequenceNumber cur = prev;
    out = kMaxSequenceNumber;
    for (size_t idx = 0; idx < snapshots_->size() && SUCC(ret);
         ++idx) {
      cur = snapshots_->at(idx);
      if (prev != kMaxSequenceNumber && prev > cur) {
        ret = Status::kCorruption;
        COMPACTION_LOG(WARN, "invalid prev", K(prev), K(cur), K(ret));
      } else if (cur >= in) {
        prev_snapshot = prev == kMaxSequenceNumber ? 0 : prev;
        out = cur;
        return ret;
      }
      if (SUCC(ret)) {
        prev = cur;
      }
    }
    if (SUCC(ret)) {
      prev_snapshot = prev;
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace xengine
