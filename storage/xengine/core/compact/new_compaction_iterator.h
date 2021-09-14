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
#pragma once

#include <algorithm>
#include <deque>
#include <string>
#include <vector>

#include "compact/compaction.h"
#include "compact/compaction_iteration_stats.h"
#include "db/merge_helper.h"
#include "db/pinned_iterators_manager.h"
#include "db/range_del_aggregator.h"
#include "xengine/compaction_filter.h"

namespace xengine {

namespace common {
class CompactionEventListener;
}

namespace storage {

class NewCompactionIterator {
 public:
  NewCompactionIterator(
      const util::Comparator* cmp,
      const util::Comparator* internal_cmp,
      common::SequenceNumber last_sequence,
      std::vector<common::SequenceNumber>* snapshots,
      common::SequenceNumber earliest_write_conflict_snapshot,
      bool expect_valid_internal_key,
      MultipleSEIterator *cur_iterator,
//      memory::ArenaAllocator& row_arena,
//      const common::XengineSchema* schema,
      memory::SimpleAllocator &arena,
      storage::ChangeInfo &change_info,
      const int level,
      const std::atomic<bool>* shutting_down = nullptr,
      const std::atomic<bool>* bg_stopped = nullptr,
      const std::atomic<int64_t> *cancel_type = nullptr,
      const bool need_check_snapshot = true,
      const bool background_disable_merge = false);
  ~NewCompactionIterator();

  void reset();
  void ResetRecordCounts();

  // Seek to the beginning of the compaction iterator output.
  // REQUIRED: Call only once.
  int seek_to_first();

  // REQUIRED: seek_to_first() has been called.
  bool valid();
  storage::SEIterator::IterLevel get_output_level() const {
    assert(cur_iterator_);
    // Note:  don't use cur_iterator_->output_level()
    // they are different when deal with single_del
    return output_level_;
  }

  const MetaDescriptor &get_reuse_meta(/*const XengineSchema *&schema*/) {
    assert(cur_iterator_);
    return cur_iterator_->get_reuse_meta(/*schema*/);
  }
  int next();
  int next_item();
  int do_compaction_filter(bool& need_skip);
  int do_single_deletion(const common::SequenceNumber prev_snapshot);
  int do_merge_operator(common::Slice& skip_until, bool& need_skip,
                        const common::SequenceNumber prev_snapshot);
  // Getters
  const common::Slice& key() const { return key_; }
  const common::Slice& value() const { return value_; }
  const db::ParsedInternalKey& ikey() const { return ikey_; }
  bool Valid() const { return valid_; }
  const CompactionIterationStats& iter_stats() const { return iter_stats_; }
 private:
  // check input stream to find the next output
  int process_next_item();
  int deal_with_kv_without_snapshot(const bool is_equal);
  int deal_with_kv_with_snapshot();

  // Do last preparations before presenting the output to the callee. At this
  // point this only zeroes out the sequence number if possible for better
  // compression.
  int prepare_output();

  // Given a sequence number, return the sequence number of the
  // earliest snapshot that this sequence number is visible in.
  // The snapshots themselves are arranged in ascending order of
  // sequence numbers.
  // Employ a sequential search because the total number of
  // snapshots are typically small.
  int earliest_visible_snapshot(common::SequenceNumber in,
                                common::SequenceNumber& out,
                                common::SequenceNumber& prev_snapshot);
  bool IsShuttingDown() {
    // This is a best-effort facility, so memory_order_relaxed is sufficient.
    return shutting_down_ && shutting_down_->load(std::memory_order_relaxed);
  }

  bool is_bg_stopped() {
    return bg_stopped_ && bg_stopped_->load(std::memory_order_relaxed);
  }

  bool is_canceled();
  int record_large_objects_info(const common::Slice &large_key,
                                 const common::Slice &large_value);

  const util::Comparator* cmp_;
  const util::Comparator* internal_cmp_;
  const std::vector<common::SequenceNumber>* snapshots_;
  const common::SequenceNumber earliest_write_conflict_snapshot_;
  bool expect_valid_internal_key_;
  const std::atomic<bool>* shutting_down_;
  const std::atomic<bool>* bg_stopped_;
  const std::atomic<int64_t>* cancel_type_;
  bool valid_;
  bool visible_at_tip_;
  bool has_current_user_key_;
  common::SequenceNumber earliest_snapshot_;
  common::SequenceNumber latest_snapshot_;
  bool ignore_snapshots_;

  // State
  // Points to a copy of the current compaction iterator output (current_key_)
  common::Slice key_;
  // Points to the value in the underlying iterator that corresponds to the
  // current output.
  common::Slice value_;
  // Stores the user key, sequence number and type of the current compaction
  // iterator output (or current key in the underlying iterator during
  // NextFromInput()).
  db::ParsedInternalKey ikey_;
  db::IterKey current_key_;  // a copy of key()

  common::SequenceNumber lastkey_seq_;
  common::SequenceNumber lastkey_snapshot_; // last_key_snapshot
  // True if the iterator has already returned a record for the current key.
  bool has_outputted_key_;
  bool clear_and_output_next_key_;  // apply after a SingleDeletion
  bool at_next_;                    // If false, the iterator

  CompactionIterationStats iter_stats_;

  MultipleSEIterator *cur_iterator_;
  storage::SEIterator::IterLevel output_level_;
//  memory::ArenaAllocator& row_arena_; // just for schema switch
//  const common::XengineSchema* dst_schema_;
  memory::SimpleAllocator &arena_;
  storage::ChangeInfo &change_info_;
  int level_; // 0/1/2
//  const common::XengineSchema* cur_schema_;
  bool need_check_snapshot_;

  //disable merge for build new-subtable
  bool background_disable_merge_;
};
}  // namespace storage
}  // namespace xengine
