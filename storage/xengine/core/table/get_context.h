// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#include <string>
#include "db/merge_context.h"
#include "db/range_del_aggregator.h"
#include "table/block.h"
#include "xengine/env.h"
#include "xengine/types.h"

namespace xengine {

namespace db {
class MergeContext;
class PinnedIteratorsManager;
}

namespace table {

class GetContext {
 public:
  enum GetState {
    kNotFound,
    kFound,
    kDeleted,
    kCorrupt,
    kMerge  // saver contains the current merge result (the operands)
  };

  GetContext(const util::Comparator* ucmp,
             const db::MergeOperator* merge_operator,
             monitor::Statistics* statistics, GetState init_state,
             const common::Slice& user_key, common::PinnableSlice* value,
             bool* value_found, db::MergeContext* merge_context,
             db::RangeDelAggregator* range_del_agg, util::Env* env,
             common::SequenceNumber* seq = nullptr,
             db::PinnedIteratorsManager* _pinned_iters_mgr = nullptr);

  void MarkKeyMayExist();

  // Records this key, value, and any meta-data (such as sequence number and
  // state) into this GetContext.
  //
  // Returns True if more keys need to be read (due to merges) or
  //         False if the complete value has been found.
  bool SaveValue(const db::ParsedInternalKey& parsed_key,
                 const common::Slice& value,
                 common::Cleanable* value_pinner = nullptr);

  void SaveLargeValue(const common::Slice& value);

  // Simplified version of the previous function. Should only be used when we
  // know that the operation is a Put.
  void SaveValue(const common::Slice& value, common::SequenceNumber seq);

  GetState State() const { return state_; }

  db::RangeDelAggregator* range_del_agg() { return range_del_agg_; }

  db::PinnedIteratorsManager* pinned_iters_mgr() { return pinned_iters_mgr_; }

  // If a non-null string is passed, all the SaveValue calls will be
  // logged into the string. The operations can then be replayed on
  // another GetContext with replayGetContextLog.
  void SetReplayLog(std::string* replay_log) { replay_log_ = replay_log; }

  // Do we need to fetch the common::SequenceNumber for this key?
  bool NeedToReadSequence() const { return (seq_ != nullptr); }

  common::PinnableSlice* get_pinnable_val() const { return pinnable_val_; }

 private:
  const util::Comparator* ucmp_;
  const db::MergeOperator* merge_operator_;
  monitor::Statistics* statistics_;

  GetState state_;
  common::Slice user_key_;
  common::PinnableSlice* pinnable_val_;
  bool* value_found_;  // Is value set correctly? Used by KeyMayExist
  db::MergeContext* merge_context_;
  db::RangeDelAggregator* range_del_agg_;
  util::Env* env_;
  // If a key is found, seq_ will be set to the common::SequenceNumber of most
  // recent
  // write to the key or kMaxSequenceNumber if unknown
  common::SequenceNumber* seq_;
  std::string* replay_log_;
  // Used to temporarily pin blocks when state_ == GetContext::kMerge
  db::PinnedIteratorsManager* pinned_iters_mgr_;
};

void replayGetContextLog(const common::Slice& replay_log,
                         const common::Slice& user_key,
                         GetContext* get_context);

}  // namespace table
}  // namespace xengine
