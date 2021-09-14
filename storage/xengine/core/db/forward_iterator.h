// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#ifndef ROCKSDB_LITE

#include <queue>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "table/internal_iterator.h"
#include "util/arena.h"
#include "xengine/db.h"
#include "xengine/iterator.h"
#include "xengine/options.h"

namespace xengine {

namespace storage {
struct ExtentMeta;
}

namespace env {
class Env;
}

namespace db {

class DBImpl;
struct SuperVersion;
class ColumnFamilyData;
class LevelIterator;
struct FileMetaData;

class MinIterComparator {
 public:
  explicit MinIterComparator(const util::Comparator* comparator)
      : comparator_(comparator) {}

  bool operator()(table::InternalIterator* a, table::InternalIterator* b) {
    return comparator_->Compare(a->key(), b->key()) > 0;
  }

 private:
  const util::Comparator* comparator_;
};

typedef std::priority_queue<table::InternalIterator*,
                            std::vector<table::InternalIterator*>,
                            MinIterComparator>
    MinIterHeap;

/**
 * ForwardIterator is a special type of iterator that only supports Seek()
 * and Next(). It is expected to perform better than TailingIterator by
 * removing the encapsulation and making all information accessible within
 * the iterator. At the current implementation, snapshot is taken at the
 * time Seek() is called. The Next() followed do not see new values after.
 */
class ForwardIterator : public table::InternalIterator {
 public:
  ForwardIterator(DBImpl* db, const common::ReadOptions& read_options,
                  ColumnFamilyData* cfd, SuperVersion* current_sv = nullptr);
  virtual ~ForwardIterator();

  void SeekForPrev(const common::Slice& target) override {
    status_ = common::Status::NotSupported("ForwardIterator::SeekForPrev()");
    valid_ = false;
  }
  void SeekToLast() override {
    status_ = common::Status::NotSupported("ForwardIterator::SeekToLast()");
    valid_ = false;
  }
  void Prev() override {
    status_ = common::Status::NotSupported("ForwardIterator::Prev");
    valid_ = false;
  }

  virtual bool Valid() const override;
  void SeekToFirst() override;
  virtual void Seek(const common::Slice& target) override;
  virtual void Next() override;
  virtual common::Slice key() const override;
  virtual common::Slice value() const override;
  virtual common::Status status() const override;
  virtual common::Status GetProperty(std::string prop_name,
                                     std::string* prop) override;
  virtual void SetPinnedItersMgr(
      PinnedIteratorsManager* pinned_iters_mgr) override;
  virtual bool IsKeyPinned() const override;
  virtual bool IsValuePinned() const override;

  bool TEST_CheckDeletedIters(int* deleted_iters, int* num_iters);

 private:
  void Cleanup(bool release_sv);
  void SVCleanup();
  void RebuildIterators(bool refresh_sv);
  void RenewIterators();
  void BuildLevelIterators(const SuperVersion* sv);
  void ResetIncompleteIterators();
  void SeekInternal(const common::Slice& internal_key, bool seek_to_first);
  void UpdateCurrent();
  bool NeedToSeekImmutable(const common::Slice& internal_key);
  void DeleteCurrentIter();
  uint32_t FindFileInRange(const std::vector<const storage::ExtentMeta*>& files,
                           const common::Slice& internal_key, uint32_t left,
                           uint32_t right);

  bool IsOverUpperBound(const common::Slice& internal_key) const;

  // Set PinnedIteratorsManager for all children Iterators, this function should
  // be called whenever we update children Iterators or pinned_iters_mgr_.
  void UpdateChildrenPinnedItersMgr();

  // A helper function that will release iter in the proper manner, or pass it
  // to pinned_iters_mgr_ to release it later if pinning is enabled.
  void DeleteIterator(table::InternalIterator* iter, bool is_arena = false);

  DBImpl* const db_;
  const common::ReadOptions read_options_;
  ColumnFamilyData* const cfd_;
  const common::SliceTransform* const prefix_extractor_;
  const util::Comparator* user_comparator_;
  MinIterHeap immutable_min_heap_;

  SuperVersion* sv_;
  table::InternalIterator* mutable_iter_;
  std::vector<table::InternalIterator*> imm_iters_;
  std::vector<table::InternalIterator*> l0_iters_;
  std::vector<LevelIterator*> level_iters_;
  table::InternalIterator* current_;
  bool valid_;

  // Internal iterator status; set only by one of the unsupported methods.
  common::Status status_;
  // common::Status of immutable iterators, maintained here to avoid iterating
  // over
  // all of them in status().
  common::Status immutable_status_;
  // Indicates that at least one of the immutable iterators pointed to a key
  // larger than iterate_upper_bound and was therefore destroyed. Seek() may
  // need to rebuild such iterators.
  bool has_iter_trimmed_for_upper_bound_;
  // Is current key larger than iterate_upper_bound? If so, makes Valid()
  // return false.
  bool current_over_upper_bound_;

  // Left endpoint of the range of keys that immutable iterators currently
  // cover. When Seek() is called with a key that's within that range, immutable
  // iterators don't need to be moved; see NeedToSeekImmutable(). This key is
  // included in the range after a Seek(), but excluded when advancing the
  // iterator using Next().
  IterKey prev_key_;
  bool is_prev_set_;
  bool is_prev_inclusive_;

  PinnedIteratorsManager* pinned_iters_mgr_;
  util::Arena arena_;
  // one meta level has the extent of one level0 sst file
  // or the hole level 1 extents
  std::vector<std::vector<const storage::ExtentMeta*>> meta_levels_;
};
}
}  // namespace xengine
#endif  // ROCKSDB_LITE
