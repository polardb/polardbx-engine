// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#ifndef MERGE_HELPER_H
#define MERGE_HELPER_H

#include <deque>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "db/merge_context.h"
#include "db/range_del_aggregator.h"
#include "util/stop_watch.h"
#include "xengine/compaction_filter.h"
#include "xengine/env.h"
#include "xengine/slice.h"

namespace xengine {

namespace monitor {
class Statistics;
}

namespace table {
class InternalIterator;
}

namespace util {
class Comparator;
}

namespace db {
class Iterator;
class MergeOperator;

class MergeHelper {
 public:
  MergeHelper(util::Env* env, const util::Comparator* user_comparator,
              const MergeOperator* user_merge_operator,
              const storage::CompactionFilter* compaction_filter,
              bool assert_valid_internal_key,
              common::SequenceNumber latest_snapshot, int level = 0,
              monitor::Statistics* stats = nullptr,
              const std::atomic<bool>* shutting_down = nullptr)
      : env_(env),
        user_comparator_(user_comparator),
        user_merge_operator_(user_merge_operator),
        compaction_filter_(compaction_filter),
        shutting_down_(shutting_down),
        assert_valid_internal_key_(assert_valid_internal_key),
        latest_snapshot_(latest_snapshot),
        level_(level),
        keys_(),
        filter_timer_(env_),
        total_filter_time_(0U),
        stats_(stats) {
    assert(user_comparator_ != nullptr);
  }

  // Wrapper around MergeOperator::FullMergeV2() that records perf statistics.
  // Result of merge will be written to result if status returned is OK.
  // If operands is empty, the value will simply be copied to result.
  // Returns one of the following statuses:
  // - OK: Entries were successfully merged.
  // - Corruption: Merge operator reported unsuccessful merge.
  static common::Status TimedFullMerge(
      const MergeOperator* merge_operator, const common::Slice& key,
      const common::Slice* value, const std::vector<common::Slice>& operands,
      std::string* result, monitor::Statistics* statistics, util::Env* env,
      common::Slice* result_operand = nullptr);

  // Merge entries until we hit
  //     - a corrupted key
  //     - a Put/Delete,
  //     - a different user key,
  //     - a specific sequence number (snapshot boundary),
  //     - REMOVE_AND_SKIP_UNTIL returned from compaction filter,
  //  or - the end of iteration
  // iter: (IN)  points to the first merge type entry
  //       (OUT) points to the first entry not included in the merge process
  // range_del_agg: (IN) filters merge operands covered by range tombstones.
  // stop_before: (IN) a sequence number that merge should not cross.
  //                   0 means no restriction
  // at_bottom:   (IN) true if the iterator covers the bottem level, which means
  //                   we could reach the start of the history of this user key.
  //
  // Returns one of the following statuses:
  // - OK: Entries were successfully merged.
  // - MergeInProgress: Put/Delete not encountered, and didn't reach the start
  //   of key's history. Output consists of merge operands only.
  // - Corruption: Merge operator reported unsuccessful merge or a corrupted
  //   key has been encountered and not expected (applies only when compiling
  //   with asserts removed).
  // - ShutdownInProgress: interrupted by shutdown (*shutting_down == true).
  //
  // REQUIRED: The first key in the input is not corrupted.
  common::Status MergeUntil(table::InternalIterator* iter,
                            RangeDelAggregator* range_del_agg = nullptr,
                            const common::SequenceNumber stop_before = 0,
                            const bool at_bottom = false);

  // Filters a merge operand using the compaction filter specified
  // in the constructor. Returns the decision that the filter made.
  // Uses compaction_filter_value_ and compaction_filter_skip_until_ for the
  // optional outputs of compaction filter.
  storage::CompactionFilter::Decision FilterMerge(
      const common::Slice& user_key, const common::Slice& value_slice);

  // Query the merge result
  // These are valid until the next MergeUntil call
  // If the merging was successful:
  //   - keys() contains a single element with the latest sequence number of
  //     the merges. The type will be Put or Merge. See IMPORTANT 1 note, below.
  //   - values() contains a single element with the result of merging all the
  //     operands together
  //
  //   IMPORTANT 1: the key type could change after the MergeUntil call.
  //        Put/Delete + Merge + ... + Merge => Put
  //        Merge + ... + Merge => Merge
  //
  // If the merge operator is not associative, and if a Put/Delete is not found
  // then the merging will be unsuccessful. In this case:
  //   - keys() contains the list of internal keys seen in order of iteration.
  //   - values() contains the list of values (merges) seen in the same order.
  //              values() is parallel to keys() so that the first entry in
  //              keys() is the key associated with the first entry in values()
  //              and so on. These lists will be the same length.
  //              All of these pairs will be merges over the same user key.
  //              See IMPORTANT 2 note below.
  //
  //   IMPORTANT 2: The entries were traversed in order from BACK to FRONT.
  //                So keys().back() was the first key seen by iterator.
  // TODO: Re-style this comment to be like the first one
  const std::deque<std::string>& keys() const { return keys_; }
  const std::vector<common::Slice>& values() const {
    return merge_context_.GetOperands();
  }
  uint64_t TotalFilterTime() const { return total_filter_time_; }
  bool HasOperator() const { return user_merge_operator_ != nullptr; }

  // If compaction filter returned REMOVE_AND_SKIP_UNTIL, this method will
  // return true and fill *until with the key to which we should skip.
  // If true, keys() and values() are empty.
  bool FilteredUntil(common::Slice* skip_until) const {
    if (!has_compaction_filter_skip_until_) {
      return false;
    }
    assert(compaction_filter_ != nullptr);
    assert(skip_until != nullptr);
    assert(compaction_filter_skip_until_.Valid());
    *skip_until = compaction_filter_skip_until_.Encode();
    return true;
  }

 private:
  util::Env* env_;
  const util::Comparator* user_comparator_;
  const MergeOperator* user_merge_operator_;
  const storage::CompactionFilter* compaction_filter_;
  const std::atomic<bool>* shutting_down_;
  bool assert_valid_internal_key_;  // enforce no internal key corruption?
  common::SequenceNumber latest_snapshot_;
  int level_;

  // the scratch area that holds the result of MergeUntil
  // valid up to the next MergeUntil call

  // Keeps track of the sequence of keys seen
  std::deque<std::string> keys_;
  // Parallel with keys_; stores the operands
  mutable MergeContext merge_context_;

  util::StopWatchNano filter_timer_;
  uint64_t total_filter_time_;
  monitor::Statistics* stats_;

  bool has_compaction_filter_skip_until_ = false;
  std::string compaction_filter_value_;
  InternalKey compaction_filter_skip_until_;

  bool IsShuttingDown() {
    // This is a best-effort facility, so memory_order_relaxed is sufficient.
    return shutting_down_ && shutting_down_->load(std::memory_order_relaxed);
  }
};

// MergeOutputIterator can be used to iterate over the result of a merge.
class MergeOutputIterator {
 public:
  // The MergeOutputIterator is bound to a MergeHelper instance.
  explicit MergeOutputIterator(const MergeHelper* merge_helper);

  // Seeks to the first record in the output.
  void SeekToFirst();
  // Advances to the next record in the output.
  void Next();

  common::Slice key() { return common::Slice(*it_keys_); }
  common::Slice value() { return common::Slice(*it_values_); }
  bool Valid() { return it_keys_ != merge_helper_->keys().rend(); }

 private:
  const MergeHelper* merge_helper_;
  std::deque<std::string>::const_reverse_iterator it_keys_;
  std::vector<common::Slice>::const_reverse_iterator it_values_;
};
}
}  // namespace xengine

#endif
