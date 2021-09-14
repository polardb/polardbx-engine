// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_iter.h"
#include <deque>
#include <limits>
#include <stdexcept>
#include <string>

#include "table/format.h"
#include "db/dbformat.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/pinned_iterators_manager.h"
#include "logger/logger.h"
#include "monitoring/query_perf_context.h"
#include "port/port.h"
#include "table/internal_iterator.h"
#include "util/arena.h"
#include "util/filename.h"
#include "util/mutexlock.h"
#include "util/string_util.h"
#include "xengine/env.h"
#include "xengine/iterator.h"
#include "xengine/merge_operator.h"
#include "xengine/options.h"
#include "xengine/xengine_constants.h"
#include "storage/extent_space_manager.h"

using namespace xengine;
using namespace monitor;
using namespace util;
using namespace common;
using namespace table;
using namespace storage;

namespace xengine {
namespace db {

#if 0
static void DumpInternalIter(Iterator* iter) {
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    ParsedInternalKey k;
    if (!ParseInternalKey(iter->key(), &k)) {
      fprintf(stderr, "Corrupt '%s'\n", EscapeString(iter->key()).c_str());
    } else {
      fprintf(stderr, "@ '%s'\n", k.DebugString().c_str());
    }
  }
}
#endif

// Memtables and sstables that make the DB representation contain
// (userkey,seq,type) => uservalue entries.  DBIter
// combines multiple entries for the same userkey found in the DB
// representation into a single entry while accounting for sequence
// numbers, deletion markers, overwrites, etc.
class DBIter : public Iterator {
 public:
  // The following is grossly complicated. TODO: clean it up
  // Which direction is the iterator currently moving?
  // (1) When moving forward, the internal iterator is positioned at
  //     the exact entry that yields this->key(), this->value()
  // (2) When moving backwards, the internal iterator is positioned
  //     just before all entries whose user key == this->key().
  enum Direction { kForward, kReverse };

  DBIter(Env* env, const ReadOptions& read_options,
         const ImmutableCFOptions& cf_options, const Comparator* cmp,
         InternalIterator* iter, SequenceNumber s, bool arena_mode,
         uint64_t max_sequential_skip_in_iterations, uint64_t version_number,
         storage::ExtentSpaceManager* space_manager = nullptr)
      : arena_mode_(arena_mode),
        env_(env),
        user_comparator_(cmp),
        merge_operator_(cf_options.merge_operator),
        iter_(iter),
        sequence_(s),
        oob_aligned_buf_(nullptr, memory::base_memalign_free),
        oob_aligned_size_(0),
        oob_unzip_buf_(nullptr),
        oob_unzip_size_(0),
        direction_(kForward),
        valid_(false),
        current_entry_is_merged_(false),
        statistics_(cf_options.statistics),
        version_number_(version_number),
        iterate_upper_bound_(read_options.iterate_upper_bound),
        prefix_same_as_start_(read_options.prefix_same_as_start),
        pin_thru_lifetime_(read_options.pin_data),
        total_order_seek_(read_options.total_order_seek),
        range_del_agg_(cf_options.internal_comparator, s,
                       true /* collapse_deletions */),
        space_manager_(space_manager),
        key_sequence_(kMaxSequenceNumber),
        skip_del_(read_options.skip_del_),
        is_art_based_memtable_(std::string(cf_options.memtable_factory->Name()) == "ARTFactory") {
    prefix_extractor_ = cf_options.prefix_extractor;
    max_skip_ = max_sequential_skip_in_iterations;
    max_skippable_internal_keys_ = read_options.max_skippable_internal_keys;
    if (pin_thru_lifetime_) {
      pinned_iters_mgr_.StartPinning();
    }
    if (iter_) {
      iter_->SetPinnedItersMgr(&pinned_iters_mgr_);
    }
  }
  virtual ~DBIter() {
    // Release pinned data if any
    if (pinned_iters_mgr_.PinningEnabled()) {
      pinned_iters_mgr_.ReleasePinnedData();
    }
    if (!arena_mode_) {
//      delete iter_;
      MOD_DELETE_OBJECT(InternalIterator, iter_);
    } else {
      iter_->~InternalIterator();
    }
  }
  virtual void SetIter(InternalIterator* iter) {
    assert(iter_ == nullptr);
    iter_ = iter;
    iter_->SetPinnedItersMgr(&pinned_iters_mgr_);
  }
  virtual RangeDelAggregator* GetRangeDelAggregator() {
    return &range_del_agg_;
  }

  virtual bool Valid() const override { return valid_; }
  virtual Slice key() const override {
    assert(valid_);
    return saved_key_.GetUserKeyFromUserKey();
  }
  virtual Slice value() const override {
    assert(valid_);
    if (UNLIKELY(current_entry_is_merged_)) {
      // If pinned_value_ is set then the result of merge operator is one of
      // the merge operands and we should return it.
      return pinned_value_.data() ? pinned_value_ : saved_value_;
    }
    Slice plain_value;
    ValueType type = kTypeValue;
    if (direction_ == kReverse) {
      plain_value = pinned_value_;
      type = pinned_key_type_;
    } else {
      plain_value = iter_->value();
      type = ExtractValueType(iter_->key());
    }
    if (LIKELY(type != kTypeValueLarge)) {
      return plain_value;
    }

    // retrieve data from off page extents for large object
    Slice result;
    LargeValue large_value;
    int ret = Status::kOk;
    if (Status::kOk != (ret = get_oob_large_value(plain_value, space_manager_,
            large_value, oob_aligned_buf_, oob_aligned_size_))) {
      __XENGINE_LOG(ERROR, "fail to get content of large value\n");
      return Slice();
    } else if (kNoCompression == large_value.compression_type_) {
      result.data_ = oob_aligned_buf_.get();
      result.size_ = large_value.size_;
    } else if (Status::kOk != (ret = unzip_data(oob_aligned_buf_.get(),
            large_value.size_, LargeValue::COMPRESSION_FORMAT_VERSION,
            static_cast<CompressionType>(large_value.compression_type_),
            oob_unzip_buf_, oob_unzip_size_))) {
      __XENGINE_LOG(ERROR, "fail to unzip large value\n");
      return Slice();
    } else {
      result.data_ = oob_unzip_buf_.get();
      result.size_ = oob_unzip_size_;
    }
    return result;
  }

  virtual Status status() const override {
    if (status_.ok()) {
      return iter_->status();
    } else {
      return status_;
    }
  }

  virtual Status GetProperty(std::string prop_name,
                             std::string* prop) override {
    if (prop == nullptr) {
      return Status::InvalidArgument("prop is nullptr");
    }
    if (prop_name == "rocksdb.iterator.super-version-number") {
      // First try to pass the value returned from inner iterator.
      if (!iter_->GetProperty(prop_name, prop).ok()) {
        *prop = ToString(version_number_);
      }
      return Status::OK();
    } else if (prop_name == "rocksdb.iterator.is-key-pinned") {
      if (valid_) {
        *prop = (pin_thru_lifetime_ && saved_key_.IsKeyPinned()) ? "1" : "0";
      } else {
        *prop = "Iterator is not valid.";
      }
      return Status::OK();
    }
    return Status::InvalidArgument("Undentified property.");
  }

  virtual void Next() override;
  virtual void Prev() override;
  virtual void Seek(const Slice& target) override;
  virtual void SeekForPrev(const Slice& target) override;
  virtual void SeekToFirst() override;
  virtual void SeekToLast() override;
  virtual int set_end_key(const Slice& end_key_slice) override;
  virtual SequenceNumber key_seq() const override
  {
    return key_sequence_;
  }

 protected:
  void ReverseToForward();
  void ReverseToBackward();
  void PrevInternal();
  void FindParseableKey(ParsedInternalKey* ikey, Direction direction);
  bool FindValueForCurrentKey();
  bool FindValueForCurrentKeyUsingSeek();
  void FindPrevUserKey();
  void FindNextUserKey();
  inline void FindNextUserEntry(bool skipping, bool prefix_check);
  void FindNextUserEntryInternal(bool skipping, bool prefix_check, bool uniq_check, bool &deleted);
  bool ParseKey(ParsedInternalKey* key);
  void MergeValuesNewToOld();
  bool TooManyInternalKeysSkipped(bool increment = true);

  // Temporarily pin the blocks that we encounter until ReleaseTempPinnedData()
  // is called
  void TempPinData() {
    if (!pin_thru_lifetime_) {
      pinned_iters_mgr_.StartPinning();
    }
  }

  // Release blocks pinned by TempPinData()
  void ReleaseTempPinnedData() {
    if (!pin_thru_lifetime_ && pinned_iters_mgr_.PinningEnabled()) {
      pinned_iters_mgr_.ReleasePinnedData();
    }
  }

  inline void ClearSavedValue() {
    if (saved_value_.capacity() > 1048576) {
      std::string empty;
      swap(empty, saved_value_);
    } else {
      saved_value_.clear();
    }
  }

  inline void ResetInternalKeysSkippedCounter() {
    num_internal_keys_skipped_ = 0;
  }

  const SliceTransform* prefix_extractor_;
  bool arena_mode_;
  Env* const env_;
  const Comparator* const user_comparator_;
  const MergeOperator* const merge_operator_;
  InternalIterator* iter_;
  SequenceNumber const sequence_;

  Status status_;
  IterKey saved_key_;
  IterKey saved_end_key_;
  std::string saved_value_;
  mutable std::unique_ptr<char[], void(&)(void *)> oob_aligned_buf_;
  mutable size_t oob_aligned_size_;
  mutable std::unique_ptr<char[], memory::ptr_delete<char>> oob_unzip_buf_;
  mutable size_t oob_unzip_size_;
  Slice pinned_value_;
  ValueType pinned_key_type_;  // key type of pinned_value_
  Direction direction_;
  bool valid_;
  bool current_entry_is_merged_;
  // for prefix seek mode to support prev()
  Statistics* statistics_;
  uint64_t max_skip_;
  uint64_t max_skippable_internal_keys_;
  uint64_t num_internal_keys_skipped_;
  uint64_t version_number_;
  const Slice* iterate_upper_bound_;
  IterKey prefix_start_buf_;
  Slice prefix_start_key_;
  const bool prefix_same_as_start_;
  // Means that we will pin all data blocks we read as long the Iterator
  // is not deleted, will be true if ReadOptions::pin_data is true
  const bool pin_thru_lifetime_;
  const bool total_order_seek_;
  // List of operands for merge operator.
  MergeContext merge_context_;
  RangeDelAggregator range_del_agg_;
  PinnedIteratorsManager pinned_iters_mgr_;
  storage::ExtentSpaceManager* space_manager_;
  SequenceNumber key_sequence_;
  const bool skip_del_;
  bool is_art_based_memtable_;

  // No copying allowed
  DBIter(const DBIter&);
  void operator=(const DBIter&);
};

class UniqueCheckDBIterator : public DBIter {
public:
  UniqueCheckDBIterator(Env* env, const ReadOptions& read_options,
                        const ImmutableCFOptions& cf_options,
                        const Comparator* cmp, InternalIterator* iter,
                        SequenceNumber s, bool arena_mode,
                        uint64_t max_sequential_skip, uint64_t version_number,
                        storage::ExtentSpaceManager* space_manager = nullptr)
      : DBIter(env, read_options, cf_options, cmp, nullptr, s, arena_mode,
               max_sequential_skip, version_number, space_manager)
  {
    assert(!read_options.skip_del_);
  }

  // Copied from DBIter::CheckNext
  void Next() override {
    QUERY_TRACE_SCOPE(TracePoint::DB_ITER_NEXT);
    assert(valid_);

    // Release temporarily pinned blocks from last operation
    if (UNLIKELY(!pin_thru_lifetime_ && pinned_iters_mgr_.PinningEnabled())) {
      pinned_iters_mgr_.ReleasePinnedData();
    }
    ResetInternalKeysSkippedCounter();
    if (direction_ == kReverse) {
      ReverseToForward();
    } else if (iter_->Valid() && !current_entry_is_merged_) {
      // If the current value is not a merge, the iter position is the
      // current key, which is already returned. We can safely issue a
      // Next() without checking the current key.
      // If the current key is a merge, very likely iter already points
      // to the next internal position.
      iter_->Next();
      QUERY_COUNT(CountPoint::INTERNAL_KEY_SKIPPED);
    }

    key_status_ = kNonExist;
    // Now we point to the next internal position, for both of merge and
    // not merge cases.
    if (iter_->Valid()) {
      bool deleted = false;
      // valid_ will be set by FindNextUserEntryInternal
      FindNextUserEntryInternal(false, prefix_same_as_start_, true, deleted);
      if (valid_) {
        key_status_ = deleted ? kDeleted : kExist;
      }
    }

    if (!iter_->Valid()) {
      valid_ = false;
    }
  }

  void Seek(const Slice& target) override {
    QUERY_TRACE_SCOPE(TracePoint::DB_ITER_SEEK);
    ReleaseTempPinnedData();
    ResetInternalKeysSkippedCounter();
    saved_key_.Clear();
    saved_key_.SetInternalKey(target, sequence_);
    iter_->Seek(saved_key_.GetInternalKey());
    range_del_agg_.InvalidateTombstoneMapPositions();

    key_status_ = kNonExist;
    if (iter_->Valid()) {
      if (prefix_extractor_ && prefix_same_as_start_) {
        prefix_start_key_ = prefix_extractor_->Transform(target);
      }
      direction_ = kForward;
      ClearSavedValue();

      bool deleted = false;
      FindNextUserEntryInternal(false, prefix_same_as_start_, true, deleted);
      if (valid_) {
        if (prefix_extractor_ && prefix_same_as_start_) {
          prefix_start_buf_.SetUserKey(prefix_start_key_);
          prefix_start_key_ = prefix_start_buf_.GetUserKey();
        }

        key_status_ = deleted ? kDeleted : kExist;
      } else {
        prefix_start_key_.clear();
      }
    } else {
      valid_ = false;
    }
  }

  void SeekToFirst() override {
    QUERY_TRACE_SCOPE(TracePoint::DB_ITER_SEEK);
    if (prefix_extractor_ != nullptr) {
      max_skip_ = std::numeric_limits<uint64_t>::max();
    }
    direction_ = kForward;
    ReleaseTempPinnedData();
    ResetInternalKeysSkippedCounter();
    ClearSavedValue();
    iter_->SeekToFirst();
    range_del_agg_.InvalidateTombstoneMapPositions();

    key_status_ = kNonExist;
    if (iter_->Valid()) {
      bool deleted;
      FindNextUserEntryInternal(false, false, true, deleted);
      if (valid_) {
        if (prefix_extractor_ && prefix_same_as_start_) {
          prefix_start_buf_.SetUserKey(
            prefix_extractor_->Transform(saved_key_.GetUserKey()));
          prefix_start_key_ = prefix_start_buf_.GetUserKey();
        }

        key_status_ = deleted ? kDeleted : kExist;
      }
    } else {
      valid_ = false;
    }
  }

  RecordStatus key_status() const override { return key_status_; }

  bool for_unique_check() const override { return true; }
private:
  RecordStatus key_status_ = kNonExist;
};

int DBIter::set_end_key(const Slice& end_key_slice)
{
  int ret = Status::kOk;
  if (UNLIKELY(nullptr == iter_)) {
    ret = Status::kNotInit;
  } else if (0 < end_key_slice.size()) {
    const SequenceNumber sequence = kMaxSequenceNumber;
    saved_end_key_.Clear();
    saved_end_key_.SetInternalKey(end_key_slice, sequence);
    iter_->set_end_key(saved_end_key_.GetInternalKey(), true /* need seek end key */);
  } else {
    saved_end_key_.Clear();
    iter_->set_end_key(Slice(), false /* need seek end key*/);
  }
  return ret;
}

inline bool DBIter::ParseKey(ParsedInternalKey* ikey) {
  if (!ParseInternalKey(iter_->key(), ikey)) {
    status_ = Status::Corruption("corrupted internal key in DBIter");
    __XENGINE_LOG(ERROR, "corrupted internal key in DBIter: %s",
                    iter_->key().ToString(true).c_str());
    return false;
  } else {
    return true;
  }
}

void DBIter::Next() {
  QUERY_TRACE_SCOPE(TracePoint::DB_ITER_NEXT);
  assert(valid_);

  // Release temporarily pinned blocks from last operation
  if (UNLIKELY(!pin_thru_lifetime_ && pinned_iters_mgr_.PinningEnabled())) {
    pinned_iters_mgr_.ReleasePinnedData();
  }
  ResetInternalKeysSkippedCounter();
  if (direction_ == kReverse) {
    ReverseToForward();
  } else if (iter_->Valid() && !current_entry_is_merged_) {
    // If the current value is not a merge, the iter position is the
    // current key, which is already returned. We can safely issue a
    // Next() without checking the current key.
    // If the current key is a merge, very likely iter already points
    // to the next internal position.
    iter_->Next();
    QUERY_COUNT(CountPoint::INTERNAL_KEY_SKIPPED);
  }

  // Now we point to the next internal position, for both of merge and
  // not merge cases.
  if (!iter_->Valid()) {
    valid_ = false;
    return;
  }
  FindNextUserEntry(true /* skipping the current user key */,
                    prefix_same_as_start_);
}

// PRE: saved_key_ has the current user key if skipping
// POST: saved_key_ should have the next user key if valid_,
//       if the current entry is a result of merge
//           current_entry_is_merged_ => true
//           saved_value_             => the merged value
//
// NOTE: In between, saved_key_ can point to a user key that has
//       a delete marker or a sequence number higher than sequence_
//       saved_key_ MUST have a proper user_key before calling this function
//
// The prefix_check parameter controls whether we check the iterated
// keys against the prefix of the seeked key. Set to false when
// performing a seek without a key (e.g. SeekToFirst). Set to
// prefix_same_as_start_ for other iterations.
inline void DBIter::FindNextUserEntry(bool skipping, bool prefix_check) {
  bool deleted = false;
  FindNextUserEntryInternal(skipping, prefix_check, false, deleted);
}

// Actual implementation of DBIter::FindNextUserEntry()
// uniq_check for online build index, we need check the record  whether has been delete or not
// deleted return status of record, only used when uniq_check is true.
void DBIter::FindNextUserEntryInternal(bool skipping, bool prefix_check, bool uniq_check, bool &deleted) {
  // Loop until we hit an acceptable entry to yield
  assert(iter_->Valid());
  QUERY_TRACE_SCOPE(TracePoint::DB_ITER_NEXT_USER_ENTRY);
  assert(direction_ == kForward);
  current_entry_is_merged_ = false;
  deleted = false;

  // How many times in a row we have skipped an entry with user key less than
  // or equal to saved_key_. We could skip these entries either because
  // sequence numbers were too high or because skipping = true.
  // What saved_key_ contains throughout this method:
  //  - if skipping        : saved_key_ contains the key that we need to skip,
  //                         and we haven't seen any keys greater than that,
  //  - if num_skipped > 0 : saved_key_ contains the key that we have skipped
  //                         num_skipped times, and we haven't seen any keys
  //                         greater than that,
  //  - none of the above  : saved_key_ can contain anything, it doesn't matter.
  uint64_t num_skipped = 0;

  do {
    ParsedInternalKey ikey;

    if (!ParseKey(&ikey)) {
      // Skip corrupted keys.
      iter_->Next();
      continue;
    }

#if 0
    //print key
    __XHANDLER_LOG(INFO, "yxian_debug: type: %d, user_key:%s, sequence:%lld", ikey.type, ikey.user_key.ToString(true).c_str(), ikey.sequence);
    __XHANDLER_LOG(INFO, "yxian_debug: value:%s", iter_->value().ToString(true).c_str());
#endif

    if (iterate_upper_bound_ != nullptr &&
        user_comparator_->Compare(ikey.user_key, *iterate_upper_bound_) >= 0) {
      break;
    }

    if (prefix_extractor_ && prefix_check &&
        prefix_extractor_->Transform(ikey.user_key)
                .compare(prefix_start_key_) != 0) {
      break;
    }

    if (TooManyInternalKeysSkipped()) {
      return;
    }

    if (ikey.sequence <= sequence_) {
      if (skipping &&
          user_comparator_->Compare(ikey.user_key, saved_key_.GetUserKey()) <=
              0) {
        num_skipped++;  // skip this entry
        QUERY_COUNT(CountPoint::INTERNAL_KEY_SKIPPED);
      } else {
        num_skipped = 0;
        key_sequence_ = ikey.sequence;
        switch (ikey.type) {
          case kTypeDeletion:
          case kTypeSingleDeletion:
            // Arrange to skip all upcoming entries for this key since
            // they are hidden by this deletion.
            saved_key_.SetUserKey(
                ikey.user_key,
                !iter_->IsKeyPinned() || !pin_thru_lifetime_ /* copy */);

            if (uniq_check || !skip_del_) {
              valid_ = true;
              if(uniq_check){
                deleted = true;
              }
              return;
            } else {
              skipping = true;
              QUERY_COUNT(CountPoint::INTERNAL_DEL_SKIPPED);
              break;
            }
          case kTypeValue:
          case kTypeValueLarge:
            saved_key_.SetUserKey(
                ikey.user_key,
                !iter_->IsKeyPinned() || !pin_thru_lifetime_ /* copy */);
            valid_ = true;
            return;
          case kTypeMerge:
            saved_key_.SetUserKey(
                ikey.user_key,
                !iter_->IsKeyPinned() || !pin_thru_lifetime_ /* copy */);
            // By now, we are sure the current ikey is going to yield a
            // value
            current_entry_is_merged_ = true;
            valid_ = true;
            MergeValuesNewToOld();  // Go to a different state machine
            return;
          default:
            assert(false);
            break;
        }
      }
    } else {
      // This key was inserted after our snapshot was taken.
      QUERY_COUNT(CountPoint::INTERNAL_UPD_SKIPPED);

      // Here saved_key_ may contain some old key, or the default empty key, or
      // key assigned by some random other method. We don't care.
      if (user_comparator_->Compare(ikey.user_key, saved_key_.GetUserKey()) <=
          0) {
        num_skipped++;
      } else {
        saved_key_.SetUserKey(
            ikey.user_key,
            !iter_->IsKeyPinned() || !pin_thru_lifetime_ /* copy */);
        skipping = false;
        num_skipped = 0;
      }
    }

    // If we have sequentially iterated via numerous equal keys, then it's
    // better to seek so that we can avoid too many key comparisons.
    if (num_skipped > max_skip_) {
      num_skipped = 0;
      std::string last_key;
      if (skipping) {
        // We're looking for the next user-key but all we see are the same
        // user-key with decreasing sequence numbers. Fast forward to
        // sequence number 0 and type deletion (the smallest type).
        AppendInternalKeyForNext(&last_key, ParsedInternalKey(saved_key_.GetUserKey(),
                                                              kMaxSequenceNumber, kTypeValueLarge));
        // Don't set skipping = false because we may still see more user-keys
        // equal to saved_key_.
      } else {
        // We saw multiple entries with this user key and sequence numbers
        // higher than sequence_. Fast forward to sequence_.
        // Note that this only covers a case when a higher key was overwritten
        // many times since our snapshot was taken, not the case when a lot of
        // different keys were inserted after our snapshot was taken.
        if (is_art_based_memtable_) {
          iter_->Next();
          continue;
        } else {
          AppendInternalKey(&last_key,
                            ParsedInternalKey(saved_key_.GetUserKey(), sequence_,
                                              kValueTypeForSeek));
        }
      }
      iter_->Seek(last_key);
      QUERY_COUNT(CountPoint::NUMBER_OF_RESEEKS_IN_ITERATION);
    } else {
      iter_->Next();
    }
  } while (iter_->Valid());
  valid_ = false;
}

// Merge values of the same user key starting from the current iter_ position
// Scan from the newer entries to older entries.
// PRE: iter_->key() points to the first merge type entry
//      saved_key_ stores the user key
// POST: saved_value_ has the merged value for the user key
//       iter_ points to the next entry (or invalid)
void DBIter::MergeValuesNewToOld() {
  if (!merge_operator_) {
    __XENGINE_LOG(ERROR, "Options::merge_operator is null.");
    status_ = Status::InvalidArgument("merge_operator_ must be set.");
    valid_ = false;
    return;
  }

  // Temporarily pin the blocks that hold merge operands
  TempPinData();
  merge_context_.Clear();
  // Start the merge process by pushing the first operand
  merge_context_.PushOperand(iter_->value(),
                             iter_->IsValuePinned() /* operand_pinned */);

  ParsedInternalKey ikey;
  Status s;
  for (iter_->Next(); iter_->Valid(); iter_->Next()) {
    if (!ParseKey(&ikey)) {
      // skip corrupted key
      continue;
    }

    if (!user_comparator_->Equal(ikey.user_key, saved_key_.GetUserKey())) {
      // hit the next user key, stop right here
      break;
    } else if (kTypeDeletion == ikey.type || kTypeSingleDeletion == ikey.type ||
               range_del_agg_.ShouldDelete(
                   ikey, RangeDelAggregator::RangePositioningMode::
                             kForwardTraversal)) {
      // hit a delete with the same user key, stop right here
      // iter_ is positioned after delete
      iter_->Next();
      break;
    } else if (kTypeValue == ikey.type) {
      // hit a put, merge the put value with operands and store the
      // final result in saved_value_. We are done!
      // ignore corruption if there is any.
      const Slice val = iter_->value();
      s = MergeHelper::TimedFullMerge(
          merge_operator_, ikey.user_key, &val, merge_context_.GetOperands(),
          &saved_value_, statistics_, env_, &pinned_value_);
      if (!s.ok()) {
        status_ = s;
      }
      // iter_ is positioned after put
      iter_->Next();
      return;
    } else if (kTypeMerge == ikey.type) {
      // hit a merge, add the value as an operand and run associative merge.
      // when complete, add result to operands and continue.
      merge_context_.PushOperand(iter_->value(),
                                 iter_->IsValuePinned() /* operand_pinned */);
    } else {
      assert(false);
    }
  }

  // we either exhausted all internal keys under this user key, or hit
  // a deletion marker.
  // feed null as the existing value to the merge operator, such that
  // client can differentiate this scenario and do things accordingly.
  s = MergeHelper::TimedFullMerge(merge_operator_, saved_key_.GetUserKey(),
                                  nullptr, merge_context_.GetOperands(),
                                  &saved_value_, statistics_, env_,
                                  &pinned_value_);
  if (!s.ok()) {
    status_ = s;
  }
}

void DBIter::Prev() {
  assert(valid_);
  QUERY_TRACE_SCOPE(TracePoint::DB_ITER_PREV);
  ReleaseTempPinnedData();
  ResetInternalKeysSkippedCounter();
  if (direction_ == kForward) {
    ReverseToBackward();
  }
  PrevInternal();
}

void DBIter::ReverseToForward() {
  if (prefix_extractor_ != nullptr && !total_order_seek_) {
    IterKey last_key;
    last_key.SetInternalKey(ParsedInternalKey(
        saved_key_.GetUserKey(), kMaxSequenceNumber, kValueTypeForSeek));
    iter_->Seek(last_key.GetInternalKey());
  }
  FindNextUserKey();
  direction_ = kForward;
  if (!iter_->Valid()) {
    iter_->SeekToFirst();
    range_del_agg_.InvalidateTombstoneMapPositions();
  }
}

void DBIter::ReverseToBackward() {
  if (prefix_extractor_ != nullptr && !total_order_seek_) {
    IterKey last_key;
    last_key.SetInternalKey(ParsedInternalKey(saved_key_.GetUserKey(), 0,
                                              kValueTypeForSeekForPrev));
    iter_->SeekForPrev(last_key.GetInternalKey());
  }
  if (current_entry_is_merged_) {
    // Not placed in the same key. Need to call Prev() until finding the
    // previous key.
    if (!iter_->Valid()) {
      iter_->SeekToLast();
      range_del_agg_.InvalidateTombstoneMapPositions();
    }
    ParsedInternalKey ikey;
    FindParseableKey(&ikey, kReverse);
    while (iter_->Valid() &&
           user_comparator_->Compare(ikey.user_key, saved_key_.GetUserKey()) >
               0) {
      if (ikey.sequence > sequence_) {
        QUERY_COUNT(CountPoint::INTERNAL_UPD_SKIPPED);
      } else {
        QUERY_COUNT(CountPoint::INTERNAL_KEY_SKIPPED);
      }
      iter_->Prev();
      FindParseableKey(&ikey, kReverse);
    }
  }
#ifndef NDEBUG
  if (iter_->Valid()) {
    ParsedInternalKey ikey;
    assert(ParseKey(&ikey));
    assert(user_comparator_->Compare(ikey.user_key, saved_key_.GetUserKey()) <=
           0);
  }
#endif

  FindPrevUserKey();
  direction_ = kReverse;
}

void DBIter::PrevInternal() {
  if (!iter_->Valid()) {
    valid_ = false;
    return;
  }

  ParsedInternalKey ikey;

  while (iter_->Valid()) {
    saved_key_.SetUserKey(
        ExtractUserKey(iter_->key()),
        !iter_->IsKeyPinned() || !pin_thru_lifetime_ /* copy */);
    key_sequence_ = ikey.sequence;

    if (FindValueForCurrentKey()) {
      valid_ = true;
      if (!iter_->Valid()) {
        return;
      }
      FindParseableKey(&ikey, kReverse);
      if (user_comparator_->Equal(ikey.user_key, saved_key_.GetUserKey())) {
        FindPrevUserKey();
      }
      if (valid_ && prefix_extractor_ && prefix_same_as_start_ &&
          prefix_extractor_->Transform(saved_key_.GetUserKey())
                  .compare(prefix_start_key_) != 0) {
        valid_ = false;
      }
      return;
    }

    if (TooManyInternalKeysSkipped(false)) {
      return;
    }

    if (!iter_->Valid()) {
      break;
    }
    FindParseableKey(&ikey, kReverse);
    if (user_comparator_->Equal(ikey.user_key, saved_key_.GetUserKey())) {
      FindPrevUserKey();
    }
  }
  // We haven't found any key - iterator is not valid
  // Or the prefix is different than start prefix
  assert(!iter_->Valid());
  valid_ = false;
}

// This function checks, if the entry with biggest sequence_number <= sequence_
// is non kTypeDeletion or kTypeSingleDeletion. If it's not, we save value in
// saved_value_
bool DBIter::FindValueForCurrentKey() {
  assert(iter_->Valid());
  merge_context_.Clear();
  current_entry_is_merged_ = false;
  // last entry before merge (could be kTypeDeletion, kTypeSingleDeletion or
  // kTypeValue)
  ValueType last_not_merge_type = kTypeDeletion;
  ValueType last_key_entry_type = kTypeDeletion;

  ParsedInternalKey ikey;
  FindParseableKey(&ikey, kReverse);

  // Temporarily pin blocks that hold (merge operands / the value)
  ReleaseTempPinnedData();
  TempPinData();
  size_t num_skipped = 0;
  while (iter_->Valid() && ikey.sequence <= sequence_ &&
         user_comparator_->Equal(ikey.user_key, saved_key_.GetUserKey())) {
    key_sequence_ = ikey.sequence;
    if (TooManyInternalKeysSkipped()) {
      return false;
    }

    // We iterate too much: let's use Seek() to avoid too much key comparisons
    if (num_skipped >= max_skip_) {
      return FindValueForCurrentKeyUsingSeek();
    }

    last_key_entry_type = ikey.type;
    switch (last_key_entry_type) {
      case kTypeValue:
      case kTypeValueLarge:
        if (range_del_agg_.ShouldDelete(
                ikey,
                RangeDelAggregator::RangePositioningMode::kBackwardTraversal)) {
          last_key_entry_type = kTypeRangeDeletion;
          QUERY_COUNT(CountPoint::INTERNAL_DEL_SKIPPED);
        } else {
          assert(iter_->IsValuePinned());
          pinned_value_ = iter_->value();
          pinned_key_type_ = last_key_entry_type;
        }
        merge_context_.Clear();
        last_not_merge_type = last_key_entry_type;
        break;
      case kTypeDeletion:
      case kTypeSingleDeletion:
        merge_context_.Clear();
        last_not_merge_type = last_key_entry_type;
        QUERY_COUNT(CountPoint::INTERNAL_DEL_SKIPPED);
        break;
      case kTypeMerge:
        if (range_del_agg_.ShouldDelete(
                ikey,
                RangeDelAggregator::RangePositioningMode::kBackwardTraversal)) {
          merge_context_.Clear();
          last_key_entry_type = kTypeRangeDeletion;
          last_not_merge_type = last_key_entry_type;
          QUERY_COUNT(CountPoint::INTERNAL_DEL_SKIPPED);
        } else {
          assert(merge_operator_ != nullptr);
          merge_context_.PushOperandBack(
              iter_->value(), iter_->IsValuePinned() /* operand_pinned */);
        }
        break;
      default:
        assert(false);
    }

    QUERY_COUNT(CountPoint::INTERNAL_KEY_SKIPPED);
    assert(user_comparator_->Equal(ikey.user_key, saved_key_.GetUserKey()));
    iter_->Prev();
    ++num_skipped;
    FindParseableKey(&ikey, kReverse);
  }

  Status s;
  switch (last_key_entry_type) {
    case kTypeDeletion:
    case kTypeSingleDeletion:
    case kTypeRangeDeletion:
      valid_ = false;
      return false;
    case kTypeMerge:
      current_entry_is_merged_ = true;
      if (last_not_merge_type == kTypeDeletion ||
          last_not_merge_type == kTypeSingleDeletion ||
          last_not_merge_type == kTypeRangeDeletion) {
        s = MergeHelper::TimedFullMerge(
            merge_operator_, saved_key_.GetUserKey(), nullptr,
            merge_context_.GetOperands(), &saved_value_, statistics_,
            env_, &pinned_value_);
      } else {
        assert(last_not_merge_type == kTypeValue);
        s = MergeHelper::TimedFullMerge(
            merge_operator_, saved_key_.GetUserKey(), &pinned_value_,
            merge_context_.GetOperands(), &saved_value_, statistics_,
            env_, &pinned_value_);
      }
      break;
    case kTypeValue:
    case kTypeValueLarge:
      // do nothing - we've already has value in saved_value_
      break;
    default:
      assert(false);
      break;
  }
  valid_ = true;
  if (!s.ok()) {
    status_ = s;
  }
  return true;
}

// This function is used in FindValueForCurrentKey.
// We use Seek() function instead of Prev() to find necessary value
bool DBIter::FindValueForCurrentKeyUsingSeek() {
  // FindValueForCurrentKey will enable pinning before calling
  // FindValueForCurrentKeyUsingSeek()
  assert(pinned_iters_mgr_.PinningEnabled());
  std::string last_key;
  AppendInternalKey(&last_key, ParsedInternalKey(saved_key_.GetUserKey(),
                                                 sequence_, kValueTypeForSeek));
  iter_->Seek(last_key);
  QUERY_COUNT(CountPoint::NUMBER_OF_RESEEKS_IN_ITERATION);

  // assume there is at least one parseable key for this user key
  ParsedInternalKey ikey;
  FindParseableKey(&ikey, kForward);

  if (ikey.type == kTypeDeletion || ikey.type == kTypeSingleDeletion ||
      range_del_agg_.ShouldDelete(
          ikey, RangeDelAggregator::RangePositioningMode::kBackwardTraversal)) {
    valid_ = false;
    return false;
  }
  if (IsValueOrLargeType(ikey.type)) {
    assert(iter_->IsValuePinned());
    pinned_value_ = iter_->value();
    pinned_key_type_ = ikey.type;
    valid_ = true;
    return true;
  }

  // kTypeMerge. We need to collect all kTypeMerge values and save them
  // in operands
  current_entry_is_merged_ = true;
  merge_context_.Clear();
  while (
      iter_->Valid() &&
      user_comparator_->Equal(ikey.user_key, saved_key_.GetUserKey()) &&
      ikey.type == kTypeMerge &&
      !range_del_agg_.ShouldDelete(
          ikey, RangeDelAggregator::RangePositioningMode::kBackwardTraversal)) {
    merge_context_.PushOperand(iter_->value(),
                               iter_->IsValuePinned() /* operand_pinned */);
    iter_->Next();
    FindParseableKey(&ikey, kForward);
  }

  Status s;
  if (!iter_->Valid() ||
      !user_comparator_->Equal(ikey.user_key, saved_key_.GetUserKey()) ||
      ikey.type == kTypeDeletion || ikey.type == kTypeSingleDeletion ||
      range_del_agg_.ShouldDelete(
          ikey, RangeDelAggregator::RangePositioningMode::kBackwardTraversal)) {
    s = MergeHelper::TimedFullMerge(merge_operator_, saved_key_.GetUserKey(),
                                    nullptr, merge_context_.GetOperands(),
                                    &saved_value_, statistics_, env_,
                                    &pinned_value_);
    // Make iter_ valid and point to saved_key_
    if (!iter_->Valid() ||
        !user_comparator_->Equal(ikey.user_key, saved_key_.GetUserKey())) {
      iter_->Seek(last_key);
      QUERY_COUNT(CountPoint::NUMBER_OF_RESEEKS_IN_ITERATION);
    }
    valid_ = true;
    if (!s.ok()) {
      status_ = s;
    }
    return true;
  }

  const Slice& val = iter_->value();
  s = MergeHelper::TimedFullMerge(merge_operator_, saved_key_.GetUserKey(),
                                  &val, merge_context_.GetOperands(),
                                  &saved_value_, statistics_, env_,
                                  &pinned_value_);
  valid_ = true;
  if (!s.ok()) {
    status_ = s;
  }
  return true;
}

// Used in Next to change directions
// Go to next user key
// Don't use Seek(),
// because next user key will be very close
void DBIter::FindNextUserKey() {
  if (!iter_->Valid()) {
    return;
  }
  ParsedInternalKey ikey;
  FindParseableKey(&ikey, kForward);
  while (iter_->Valid() &&
         !user_comparator_->Equal(ikey.user_key, saved_key_.GetUserKey())) {
    iter_->Next();
    FindParseableKey(&ikey, kForward);
  }
}

// Go to previous user_key
void DBIter::FindPrevUserKey() {
  if (!iter_->Valid()) {
    return;
  }
  size_t num_skipped = 0;
  ParsedInternalKey ikey;
  FindParseableKey(&ikey, kReverse);
  int cmp;
  while (iter_->Valid() &&
         ((cmp = user_comparator_->Compare(ikey.user_key,
                                           saved_key_.GetUserKey())) == 0 ||
          (cmp > 0 && ikey.sequence > sequence_))) {
    if (TooManyInternalKeysSkipped()) {
      return;
    }

    if (cmp == 0) {
      if (num_skipped >= max_skip_) {
        num_skipped = 0;
        IterKey last_key;
        last_key.SetInternalKey(ParsedInternalKey(
            saved_key_.GetUserKey(), kMaxSequenceNumber, kValueTypeForSeek));
        iter_->Seek(last_key.GetInternalKey());
        QUERY_COUNT(CountPoint::NUMBER_OF_RESEEKS_IN_ITERATION);
      } else {
        ++num_skipped;
      }
    }
    if (ikey.sequence > sequence_) {
      QUERY_COUNT(CountPoint::INTERNAL_UPD_SKIPPED);
    } else {
      QUERY_COUNT(CountPoint::INTERNAL_KEY_SKIPPED);
    }
    iter_->Prev();
    FindParseableKey(&ikey, kReverse);
  }
}

bool DBIter::TooManyInternalKeysSkipped(bool increment) {
  if ((max_skippable_internal_keys_ > 0) &&
      (num_internal_keys_skipped_ > max_skippable_internal_keys_)) {
    valid_ = false;
    status_ = Status::Incomplete("Too many internal keys skipped.");
    return true;
  } else if (increment) {
    num_internal_keys_skipped_++;
  }
  return false;
}

// Skip all unparseable keys
void DBIter::FindParseableKey(ParsedInternalKey* ikey, Direction direction) {
  while (iter_->Valid() && !ParseKey(ikey)) {
    if (direction == kReverse) {
      iter_->Prev();
    } else {
      iter_->Next();
    }
  }
}

void DBIter::Seek(const Slice& target) {
  QUERY_TRACE_SCOPE(TracePoint::DB_ITER_SEEK);
  ReleaseTempPinnedData();
  ResetInternalKeysSkippedCounter();
  saved_key_.Clear();
  saved_key_.SetInternalKey(target, sequence_);
  iter_->Seek(saved_key_.GetInternalKey());
  range_del_agg_.InvalidateTombstoneMapPositions();

  if (iter_->Valid()) {
    if (prefix_extractor_ && prefix_same_as_start_) {
      prefix_start_key_ = prefix_extractor_->Transform(target);
    }
    direction_ = kForward;
    ClearSavedValue();
    FindNextUserEntry(false /* not skipping */, prefix_same_as_start_);
    if (!valid_) {
      prefix_start_key_.clear();
    }
  } else {
    valid_ = false;
  }

  if (valid_ && prefix_extractor_ && prefix_same_as_start_) {
    prefix_start_buf_.SetUserKey(prefix_start_key_);
    prefix_start_key_ = prefix_start_buf_.GetUserKey();
  }
}

void DBIter::SeekForPrev(const Slice& target) {
  QUERY_TRACE_SCOPE(TracePoint::DB_ITER_SEEK);
  ReleaseTempPinnedData();
  ResetInternalKeysSkippedCounter();
  saved_key_.Clear();
  // now saved_key is used to store internal key.
  saved_key_.SetInternalKey(target, 0 /* sequence_number */,
                            kValueTypeForSeekForPrev);
  iter_->SeekForPrev(saved_key_.GetInternalKey());
  range_del_agg_.InvalidateTombstoneMapPositions();

  if (iter_->Valid()) {
    if (prefix_extractor_ && prefix_same_as_start_) {
      prefix_start_key_ = prefix_extractor_->Transform(target);
    }
    direction_ = kReverse;
    ClearSavedValue();
    PrevInternal();
    if (!valid_) {
      prefix_start_key_.clear();
    }
  } else {
    valid_ = false;
  }
  if (valid_ && prefix_extractor_ && prefix_same_as_start_) {
    prefix_start_buf_.SetUserKey(prefix_start_key_);
    prefix_start_key_ = prefix_start_buf_.GetUserKey();
  }
}

void DBIter::SeekToFirst() {
  QUERY_TRACE_SCOPE(TracePoint::DB_ITER_SEEK);
  // Don't use iter_::Seek() if we set a prefix extractor
  // because prefix seek will be used.
  if (prefix_extractor_ != nullptr) {
    max_skip_ = std::numeric_limits<uint64_t>::max();
  }
  direction_ = kForward;
  ReleaseTempPinnedData();
  ResetInternalKeysSkippedCounter();
  ClearSavedValue();
  iter_->SeekToFirst();
  range_del_agg_.InvalidateTombstoneMapPositions();

  if (iter_->Valid()) {
    saved_key_.SetUserKey(
        ExtractUserKey(iter_->key()),
        !iter_->IsKeyPinned() || !pin_thru_lifetime_ /* copy */);
    FindNextUserEntry(false /* not skipping */, false /* no prefix check */);
  } else {
    valid_ = false;
  }
  if (valid_ && prefix_extractor_ && prefix_same_as_start_) {
    prefix_start_buf_.SetUserKey(
        prefix_extractor_->Transform(saved_key_.GetUserKey()));
    prefix_start_key_ = prefix_start_buf_.GetUserKey();
  }
}

void DBIter::SeekToLast() {
  QUERY_TRACE_SCOPE(TracePoint::DB_ITER_SEEK);
  // Don't use iter_::Seek() if we set a prefix extractor
  // because prefix seek will be used.
  if (prefix_extractor_ != nullptr) {
    max_skip_ = std::numeric_limits<uint64_t>::max();
  }
  direction_ = kReverse;
  ReleaseTempPinnedData();
  ResetInternalKeysSkippedCounter();
  ClearSavedValue();

  iter_->SeekToLast();
  range_del_agg_.InvalidateTombstoneMapPositions();

  // When the iterate_upper_bound is set to a value,
  // it will seek to the last key before the
  // ReadOptions.iterate_upper_bound
  if (iter_->Valid() && iterate_upper_bound_ != nullptr) {
    SeekForPrev(*iterate_upper_bound_);
    range_del_agg_.InvalidateTombstoneMapPositions();
    if (!Valid()) {
      return;
    } else if (user_comparator_->Equal(*iterate_upper_bound_, key())) {
      Prev();
    }
  } else {
    PrevInternal();
  }
  if (valid_ && prefix_extractor_ && prefix_same_as_start_) {
    prefix_start_buf_.SetUserKey(
        prefix_extractor_->Transform(saved_key_.GetUserKey()));
    prefix_start_key_ = prefix_start_buf_.GetUserKey();
  }
}

Iterator* NewDBIterator(Env* env, const ReadOptions& read_options,
                        const ImmutableCFOptions& cf_options,
                        const Comparator* user_key_comparator,
                        InternalIterator* internal_iter,
                        const SequenceNumber& sequence,
                        uint64_t max_sequential_skip_in_iterations,
                        uint64_t version_number, Arena* arena,
                        ExtentSpaceManager* space_manager) {
  DBIter* db_iter = nullptr;
  if (nullptr != arena) {
    db_iter = PLACEMENT_NEW(DBIter, *arena, env, read_options, cf_options,
                            user_key_comparator, internal_iter, sequence, false,
                            max_sequential_skip_in_iterations, version_number,
                            space_manager);
  } else {
    db_iter = MOD_NEW_OBJECT(memory::ModId::kDbIter, DBIter,
                             env, read_options, cf_options, user_key_comparator,
                             internal_iter, sequence, false,
                             max_sequential_skip_in_iterations, version_number,
                             space_manager);
  }
  return db_iter;
}

Iterator* NewDBIterator(Env* env, const ReadOptions& read_options,
                        const ImmutableCFOptions& cf_options,
                        const Comparator* user_key_comparator,
                        InternalIterator* internal_iter,
                        const SequenceNumber& sequence, bool use_arena,
                        uint64_t max_sequential_skip_in_iterations,
                        uint64_t version_number, Arena* arena,
                        ExtentSpaceManager* space_manager) {
  DBIter* db_iter = nullptr;
  if (nullptr != arena) {
    db_iter = PLACEMENT_NEW(DBIter, *arena, env, read_options, cf_options,
                            user_key_comparator, internal_iter, sequence, use_arena,
                            max_sequential_skip_in_iterations, version_number,
                            space_manager);
  } else {
    db_iter = MOD_NEW_OBJECT(memory::ModId::kDbIter, DBIter,
                             env, read_options, cf_options, user_key_comparator,
                             internal_iter, sequence, false,
                             max_sequential_skip_in_iterations, version_number );
  }
  return db_iter;
}

ArenaWrappedDBIter::ArenaWrappedDBIter()
    : db_iter_(nullptr), arena_(Arena::kMinBlockSize, 0, memory::ModId::kDbIter) {}
ArenaWrappedDBIter::~ArenaWrappedDBIter() {
  if (nullptr != db_iter_) {
    db_iter_->~DBIter();
    db_iter_ = nullptr;
  }
}

void ArenaWrappedDBIter::SetDBIter(DBIter* iter) { db_iter_ = iter; }

RangeDelAggregator* ArenaWrappedDBIter::GetRangeDelAggregator() {
  return db_iter_->GetRangeDelAggregator();
}

void ArenaWrappedDBIter::SetIterUnderDBIter(InternalIterator* iter) {
  static_cast<DBIter*>(db_iter_)->SetIter(iter);
}

inline bool ArenaWrappedDBIter::Valid() const { return db_iter_->Valid(); }
inline void ArenaWrappedDBIter::SeekToFirst() { db_iter_->SeekToFirst(); }
inline void ArenaWrappedDBIter::SeekToLast() { db_iter_->SeekToLast(); }
inline void ArenaWrappedDBIter::Seek(const Slice& target) {
  db_iter_->Seek(target);
}
inline void ArenaWrappedDBIter::SeekForPrev(const Slice& target) {
  db_iter_->SeekForPrev(target);
}
inline void ArenaWrappedDBIter::Next() { db_iter_->Next(); }

inline void ArenaWrappedDBIter::Prev() { db_iter_->Prev(); }
inline Slice ArenaWrappedDBIter::key() const { return db_iter_->key(); }
inline Slice ArenaWrappedDBIter::value() const { return db_iter_->value(); }
inline Status ArenaWrappedDBIter::status() const { return db_iter_->status(); }
inline Status ArenaWrappedDBIter::GetProperty(std::string prop_name,
                                              std::string* prop) {
  return db_iter_->GetProperty(prop_name, prop);
}
void ArenaWrappedDBIter::RegisterCleanup(CleanupFunction function, void* arg1,
                                         void* arg2) {
  db_iter_->RegisterCleanup(function, arg1, arg2);
}

int ArenaWrappedDBIter::set_end_key(const Slice& end_key_slice)
{
  int ret = Status::kOk;
  if (UNLIKELY(nullptr == db_iter_)) {
    ret = Status::kNotInit;
  } else {
    ret = db_iter_->set_end_key(end_key_slice);
  }
  return ret;
}

SequenceNumber ArenaWrappedDBIter::key_seq() const { return db_iter_->key_seq(); }

Iterator::RecordStatus ArenaWrappedDBIter::key_status() const {
  return db_iter_->key_status();
}

bool ArenaWrappedDBIter::for_unique_check() const {
  return db_iter_->for_unique_check();
}

ArenaWrappedDBIter* NewArenaWrappedDbIterator(
    Env* env, const ReadOptions& read_options,
    const ImmutableCFOptions& cf_options, const Comparator* user_key_comparator,
    const SequenceNumber& sequence, uint64_t max_sequential_skip_in_iterations,
    uint64_t version_number, ExtentSpaceManager* space_manager) {
//  ArenaWrappedDBIter* iter = new ArenaWrappedDBIter();
  ArenaWrappedDBIter* iter = MOD_NEW_OBJECT(memory::ModId::kDbIter, ArenaWrappedDBIter);
  Arena* arena = iter->GetArena();

  DBIter* db_iter = nullptr;
  if (!read_options.unique_check_) {
    auto mem = arena->AllocateAligned(sizeof(DBIter));
    db_iter = new(mem)
        DBIter(env, read_options, cf_options, user_key_comparator, nullptr,
               sequence, true, max_sequential_skip_in_iterations, version_number,
               space_manager);
  } else {
    ReadOptions ro = read_options;
    ro.skip_del_ = false;
    auto mem = arena->AllocateAligned(sizeof(UniqueCheckDBIterator));
    db_iter = new(mem)
        UniqueCheckDBIterator(env, ro, cf_options, user_key_comparator, nullptr,
                            sequence, true, max_sequential_skip_in_iterations,
                            version_number, space_manager);
  }

  iter->SetDBIter(db_iter);

  return iter;
}
}
}  // namespace xengine
