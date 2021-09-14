//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <algorithm>
#include <deque>
#include <string>
#include <vector>

#include "compact/compaction_iteration_stats.h"
#include "db/merge_helper.h"
#include "db/pinned_iterators_manager.h"
#include "db/range_del_aggregator.h"
#include "xengine/compaction_filter.h"
#include "memory/page_arena.h"

namespace xengine {

namespace common {
class CompactionEventListener;
}

namespace storage {

// A wrapper around Compaction. Has a much smaller interface, only what
// CompactionIterator uses. Tests can override it.
class CompactionProxy {
 public:
  virtual ~CompactionProxy() = default;
  virtual int level(size_t compaction_input_level = 0) const = 0;
  virtual bool KeyNotExistsBeyondOutputLevel(
      const common::Slice& user_key, std::vector<size_t>* level_ptrs) const = 0;
  virtual bool bottommost_level() const = 0;
  virtual int number_levels() const = 0;
  virtual common::Slice GetLargestUserKey() const = 0;
 protected:
  CompactionProxy() = default;
};


class CompactionIterator {
 public:
  // Constructor with custom CompactionProxy, used for tests.
  CompactionIterator(
      table::InternalIterator* input, const util::Comparator* cmp,
      db::MergeHelper* merge_helper, common::SequenceNumber last_sequence,
      std::vector<common::SequenceNumber>* snapshots,
      common::SequenceNumber earliest_write_conflict_snapshot, util::Env* env,
      bool expect_valid_internal_key,
      storage::ChangeInfo &change_info,
      memory::ArenaAllocator &arena,
      std::unique_ptr<CompactionProxy> compaction,
      const storage::CompactionFilter* compaction_filter = nullptr,
      common::CompactionEventListener* compaction_listener = nullptr,
      const std::atomic<bool>* shutting_down = nullptr,
      const std::atomic<bool>* bg_stopped = nullptr,
      const std::atomic<int64_t>* cancel_type = nullptr,
//      memory::ArenaAllocator* row_arena = nullptr,
//      const common::XengineSchema *schema = nullptr,
      const common::Slice *l2_largest_key = nullptr,
      const bool background_disable_merge = false);

  ~CompactionIterator();

  void ResetRecordCounts();

  // Seek to the beginning of the compaction iterator output.
  //
  // REQUIRED: Call only once.
  void SeekToFirst();

  // Produces the next record in the compaction.
  //
  // REQUIRED: SeekToFirst() has been called.
  void Next();

  // Getters
  const common::Slice& key() const { return key_; }
  const common::Slice& value() const { return value_; }
  const common::Status& status() const { return status_; }
  const db::ParsedInternalKey& ikey() const { return ikey_; }
  bool Valid() const { return valid_; }
  const common::Slice& user_key() const { return current_user_key_; }
  const CompactionIterationStats& iter_stats() const { return iter_stats_; }

 private:
  // Processes the input stream to find the next output
  void NextFromInput();

  // Do last preparations before presenting the output to the callee. At this
  // point this only zeroes out the sequence number if possible for better
  // compression.
  void PrepareOutput();

  void record_large_objects_info(const common::Slice &large_key,
                                 const common::Slice &large_value);
  // Given a sequence number, return the sequence number of the
  // earliest snapshot that this sequence number is visible in.
  // The snapshots themselves are arranged in ascending order of
  // sequence numbers.
  // Employ a sequential search because the total number of
  // snapshots are typically small.
  inline common::SequenceNumber findEarliestVisibleSnapshot(
      common::SequenceNumber in, common::SequenceNumber* prev_snapshot);

  bool IsShuttingDown() {
    // This is a best-effort facility, so memory_order_relaxed is sufficient.
    return shutting_down_ && shutting_down_->load(std::memory_order_relaxed);
  }
  bool is_bg_stopped() {
    return bg_stopped_ && bg_stopped_->load(std::memory_order_relaxed);
  }
  bool is_canceled();

  table::InternalIterator* input_;
  const util::Comparator* cmp_;
  db::MergeHelper* merge_helper_;
  const std::vector<common::SequenceNumber>* snapshots_;
  const common::SequenceNumber earliest_write_conflict_snapshot_;
  util::Env* env_;
  bool expect_valid_internal_key_;
//  db::RangeDelAggregator* range_del_agg_;
  std::unique_ptr<CompactionProxy> compaction_;
  const storage::CompactionFilter* compaction_filter_;
  common::CompactionEventListener* compaction_listener_;
  const std::atomic<bool>* shutting_down_;
  const std::atomic<bool>* bg_stopped_;
  const std::atomic<int64_t>* cancel_type_;
  bool bottommost_level_;
  bool valid_ = false;
  bool visible_at_tip_;
  common::SequenceNumber earliest_snapshot_;
  common::SequenceNumber latest_snapshot_;
  bool ignore_snapshots_;

  // State
  //
  // Points to a copy of the current compaction iterator output (current_key_)
  // if valid_.
  common::Slice key_;
  // Points to the value in the underlying iterator that corresponds to the
  // current output.
  common::Slice value_;
  // The status is OK unless compaction iterator encounters a merge operand
  // while not having a merge operator defined.
  common::Status status_;
  // Stores the user key, sequence number and type of the current compaction
  // iterator output (or current key in the underlying iterator during
  // NextFromInput()).
  db::ParsedInternalKey ikey_;
  // Stores whether ikey_.user_key is valid. If set to false, the user key is
  // not compared against the current key in the underlying iterator.
  bool has_current_user_key_ = false;
  bool at_next_ = false;  // If false, the iterator
  // Holds a copy of the current compaction iterator output (or current key in
  // the underlying iterator during NextFromInput()).
  db::IterKey current_key_;
  common::Slice current_user_key_;
  common::SequenceNumber current_user_key_sequence_;
  common::SequenceNumber current_user_key_snapshot_;

  // True if the iterator has already returned a record for the current key.
  bool has_outputted_key_ = false;

  // truncated the value of the next key and output it without applying any
  // compaction rules.  This is used for outputting a put after a single delete.
  bool clear_and_output_next_key_ = false;

  // PinnedIteratorsManager used to pin input_ Iterator blocks while reading
  // merge operands and then releasing them after consuming them.
  db::PinnedIteratorsManager pinned_iters_mgr_;
  std::string compaction_filter_value_;
  db::InternalKey compaction_filter_skip_until_;
  // "level_ptrs" holds indices that remember which file of an associated
  // level we were last checking during the last call to compaction->
  // KeyNotExistsBeyondOutputLevel(). This allows future calls to the function
  // to pick off where it left off since each subcompaction's key range is
  // increasing so a later call to the function must be looking for a key that
  // is in or beyond the last file checked during the previous call
  std::vector<size_t> level_ptrs_;
  CompactionIterationStats iter_stats_;
//  memory::ArenaAllocator* row_arena_;
//  const common::XengineSchema *dst_schema_;

  storage::ChangeInfo &change_info_;
  memory::ArenaAllocator &arena_;
  const common::Slice *l2_largest_key_;
  bool background_disable_merge_;
};
}  // namespace storage
}  // namespace xengine
