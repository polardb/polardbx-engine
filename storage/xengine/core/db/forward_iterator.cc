// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE
#include "db/forward_iterator.h"

#include <limits>
#include <string>
#include <utility>

#include "db/column_family.h"
#include "db/db_impl.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/job_context.h"
#include "storage/storage_manager.h"
#include "storage/storage_meta_struct.h"
#include "table/merging_iterator.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "xengine/env.h"
#include "xengine/slice.h"
#include "xengine/slice_transform.h"

using namespace xengine;
using namespace table;
using namespace common;
using namespace util;

namespace xengine {
namespace db {
using namespace xengine::storage;

// Usage:
//     LevelIterator iter;
//     iter.SetFileIndex(file_index);
//     iter.Seek(target);
//     iter.Next()
class LevelIterator : public InternalIterator {
 public:
  LevelIterator(const ColumnFamilyData* const cfd,
                const ReadOptions& read_options,
                // const std::vector<FileMetaData*>& files,
                const std::vector<const ExtentMeta*>& metas)
      : cfd_(cfd),
        read_options_(read_options),
        // files_(files),
        valid_(false),
        file_index_(std::numeric_limits<uint32_t>::max()),
        file_iter_(nullptr),
        pinned_iters_mgr_(nullptr),
        extent_index_(std::numeric_limits<uint32_t>::max()),
        metas_(metas) {}

  ~LevelIterator() {
    // Reset current pointer
    if (pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled()) {
      pinned_iters_mgr_->PinIterator(file_iter_);
    } else {
//      delete file_iter_;
      MOD_DELETE_OBJECT(InternalIterator, file_iter_);
    }
  }

  void SetFileIndex(uint32_t file_index) {
    assert(file_index < metas_.size());
    if (file_index != extent_index_) {
      extent_index_ = file_index;
      Reset();
    }
    valid_ = false;
  }

  void Reset() {
    assert(extent_index_ < metas_.size());

    // Reset current pointer
    if (pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled()) {
      pinned_iters_mgr_->PinIterator(file_iter_);
    } else {
//      delete file_iter_;
      MOD_DELETE_OBJECT(InternalIterator, file_iter_);
    }

    RangeDelAggregator range_del_agg(cfd_->internal_comparator(),
                                     {} /* snapshots */);
    file_iter_ = cfd_->table_cache()->NewIterator(
        read_options_, *(cfd_->soptions()), cfd_->internal_comparator(),
        FileDescriptor(metas_[extent_index_]->extent_id_.id(), 0,
                       MAX_EXTENT_SIZE),
        read_options_.ignore_range_deletions ? nullptr : &range_del_agg,
        nullptr /* table_reader_ptr */, nullptr, false);
    file_iter_->SetPinnedItersMgr(pinned_iters_mgr_);
    if (!range_del_agg.IsEmpty()) {
      status_ = Status::NotSupported(
          "Range tombstones unsupported with ForwardIterator");
      valid_ = false;
    }
  }
  void SeekToLast() override {
    status_ = Status::NotSupported("LevelIterator::SeekToLast()");
    valid_ = false;
  }
  void Prev() override {
    status_ = Status::NotSupported("LevelIterator::Prev()");
    valid_ = false;
  }
  bool Valid() const override { return valid_; }
  void SeekToFirst() override {
    SetFileIndex(0);
    file_iter_->SeekToFirst();
    valid_ = file_iter_->Valid();
  }
  void Seek(const Slice& internal_key) override {
    assert(file_iter_ != nullptr);
    file_iter_->Seek(internal_key);
    valid_ = file_iter_->Valid();
  }
  void SeekForPrev(const Slice& internal_key) override {
    status_ = Status::NotSupported("LevelIterator::SeekForPrev()");
    valid_ = false;
  }
  void Next() override {
    assert(valid_);
    file_iter_->Next();
    for (;;) {
      if (file_iter_->status().IsIncomplete() || file_iter_->Valid()) {
        valid_ = !file_iter_->status().IsIncomplete();
        return;
      }
      if (extent_index_ + 1 >= metas_.size()) {
        valid_ = false;
        return;
      }
      SetFileIndex(extent_index_ + 1);
      file_iter_->SeekToFirst();
    }
  }
  Slice key() const override {
    assert(valid_);
    return file_iter_->key();
  }
  Slice value() const override {
    assert(valid_);
    return file_iter_->value();
  }
  Status status() const override {
    if (!status_.ok()) {
      return status_;
    } else if (file_iter_ && !file_iter_->status().ok()) {
      return file_iter_->status();
    }
    return Status::OK();
  }
  bool IsKeyPinned() const override {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           file_iter_->IsKeyPinned();
  }
  bool IsValuePinned() const override {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
           file_iter_->IsValuePinned();
  }
  void SetPinnedItersMgr(PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
    if (file_iter_) {
      file_iter_->SetPinnedItersMgr(pinned_iters_mgr_);
    }
  }

 private:
  const ColumnFamilyData* const cfd_;
  const ReadOptions& read_options_;
  // const std::vector<FileMetaData*>& files_;

  bool valid_;
  uint32_t file_index_;
  Status status_;
  InternalIterator* file_iter_;
  PinnedIteratorsManager* pinned_iters_mgr_;
  // new storage
  uint32_t extent_index_;
  const std::vector<const ExtentMeta*>& metas_;
};

ForwardIterator::ForwardIterator(DBImpl* db, const ReadOptions& read_options,
                                 ColumnFamilyData* cfd,
                                 SuperVersion* current_sv)
    : db_(db),
      read_options_(read_options),
      cfd_(cfd),
      prefix_extractor_(cfd->ioptions()->prefix_extractor),
      user_comparator_(cfd->user_comparator()),
      immutable_min_heap_(MinIterComparator(&cfd_->internal_comparator())),
      sv_(current_sv),
      mutable_iter_(nullptr),
      current_(nullptr),
      valid_(false),
      status_(Status::OK()),
      immutable_status_(Status::OK()),
      has_iter_trimmed_for_upper_bound_(false),
      current_over_upper_bound_(false),
      is_prev_set_(false),
      is_prev_inclusive_(false),
      pinned_iters_mgr_(nullptr) {
  if (sv_) {
    RebuildIterators(false);
  }
}

ForwardIterator::~ForwardIterator() { Cleanup(true); }

namespace {
// Used in PinnedIteratorsManager to release pinned SuperVersion
static void ReleaseSuperVersionFunc(void* sv) {
//  delete reinterpret_cast<SuperVersion*>(sv);
  auto del = reinterpret_cast<SuperVersion*>(sv);
  MOD_DELETE_OBJECT(SuperVersion, del);
}
}  // namespace

void ForwardIterator::SVCleanup() {
  if (sv_ != nullptr && sv_->Unref()) {
    // Job id == 0 means that this is not our background process, but rather
    // user thread
    JobContext job_context(0);
    db_->mutex_.Lock();
    cfd_->release_meta_snapshot(sv_->current_meta_, sv_->db_mutex);
    sv_->Cleanup();
    db_->FindObsoleteFiles(&job_context, false, true);
    if (read_options_.background_purge_on_iterator_cleanup) {
      db_->ScheduleBgLogWriterClose(&job_context);
    }
    db_->mutex_.Unlock();
    if (pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled()) {
      pinned_iters_mgr_->PinPtr(sv_, &ReleaseSuperVersionFunc);
    } else {
      MOD_DELETE_OBJECT(SuperVersion, sv_);
    }
    if (job_context.HaveSomethingToDelete()) {
      db_->PurgeObsoleteFiles(
          job_context, read_options_.background_purge_on_iterator_cleanup);
    }
    job_context.Clean();
  }
}

void ForwardIterator::Cleanup(bool release_sv) {
  if (mutable_iter_ != nullptr) {
    DeleteIterator(mutable_iter_, true /* is_arena */);
  }

  for (auto* m : imm_iters_) {
    DeleteIterator(m, true /* is_arena */);
  }
  imm_iters_.clear();

  for (auto* f : l0_iters_) {
    DeleteIterator(f);
  }
  l0_iters_.clear();

  for (auto* l : level_iters_) {
    DeleteIterator(l);
  }
  level_iters_.clear();

  if (release_sv) {
    SVCleanup();
  }
}

bool ForwardIterator::Valid() const {
  // See UpdateCurrent().
  return valid_ ? !current_over_upper_bound_ : false;
}

void ForwardIterator::SeekToFirst() {
  if (sv_ == nullptr) {
    RebuildIterators(true);
  } else if (sv_->version_number != cfd_->GetSuperVersionNumber()) {
    RenewIterators();
  } else if (immutable_status_.IsIncomplete()) {
    ResetIncompleteIterators();
  }
  SeekInternal(Slice(), true);
}

bool ForwardIterator::IsOverUpperBound(const Slice& internal_key) const {
  return !(read_options_.iterate_upper_bound == nullptr ||
           cfd_->internal_comparator().user_comparator()->Compare(
               ExtractUserKey(internal_key),
               *read_options_.iterate_upper_bound) < 0);
}

void ForwardIterator::Seek(const Slice& internal_key) {
  if (IsOverUpperBound(internal_key)) {
    valid_ = false;
  }
  if (sv_ == nullptr) {
    RebuildIterators(true);
  } else if (sv_->version_number != cfd_->GetSuperVersionNumber()) {
    RenewIterators();
  } else if (immutable_status_.IsIncomplete()) {
    ResetIncompleteIterators();
  }
  SeekInternal(internal_key, false);
}

void ForwardIterator::SeekInternal(const Slice& internal_key,
                                   bool seek_to_first) {
  assert(mutable_iter_);
  // mutable
  seek_to_first ? mutable_iter_->SeekToFirst()
                : mutable_iter_->Seek(internal_key);

  // immutable
  // TODO(ljin): NeedToSeekImmutable has negative impact on performance
  // if it turns to need to seek immutable often. We probably want to have
  // an option to turn it off.
  if (seek_to_first || NeedToSeekImmutable(internal_key)) {
    immutable_status_ = Status::OK();
    if (has_iter_trimmed_for_upper_bound_ &&
        (
            // prev_ is not set yet
            is_prev_set_ == false ||
            // We are doing SeekToFirst() and internal_key.size() = 0
            seek_to_first ||
            // prev_key_ > internal_key
            cfd_->internal_comparator().InternalKeyComparator::Compare(
                prev_key_.GetInternalKey(), internal_key) > 0)) {
      // Some iterators are trimmed. Need to rebuild.
      RebuildIterators(true);
      // Already seeked mutable iter, so seek again
      seek_to_first ? mutable_iter_->SeekToFirst()
                    : mutable_iter_->Seek(internal_key);
    }
    {
      auto tmp = MinIterHeap(MinIterComparator(&cfd_->internal_comparator()));
      immutable_min_heap_.swap(tmp);
    }
    for (size_t i = 0; i < imm_iters_.size(); i++) {
      auto* m = imm_iters_[i];
      seek_to_first ? m->SeekToFirst() : m->Seek(internal_key);
      if (!m->status().ok()) {
        immutable_status_ = m->status();
      } else if (m->Valid()) {
        immutable_min_heap_.push(m);
      }
    }

    Slice user_key;
    if (!seek_to_first) {
      user_key = ExtractUserKey(internal_key);
    }

    for (int32_t level = 0; level < static_cast<int32_t>(meta_levels_.size());
         ++level) {
      const auto& meta_level = meta_levels_[level];
      if (level_iters_[level] == nullptr) {
        continue;
      }
      uint32_t f_idx = 0;
      if (!seek_to_first) {
        f_idx = FindFileInRange(meta_level, internal_key, 0,
                                static_cast<uint32_t>(meta_level.size()));
      }

      // Seek
      if (f_idx < meta_level.size()) {
        level_iters_[level]->SetFileIndex(f_idx);
        seek_to_first ? level_iters_[level]->SeekToFirst()
                      : level_iters_[level]->Seek(internal_key);

        if (!level_iters_[level]->status().ok()) {
          immutable_status_ = level_iters_[level]->status();
        } else if (level_iters_[level]->Valid()) {
          if (!IsOverUpperBound(level_iters_[level]->key())) {
            immutable_min_heap_.push(level_iters_[level]);
          } else {
            // Nothing in this level is interesting. Remove.
            has_iter_trimmed_for_upper_bound_ = true;
            DeleteIterator(level_iters_[level]);
            level_iters_[level] = nullptr;
          }
        }
      }
    }

    if (seek_to_first) {
      is_prev_set_ = false;
    } else {
      prev_key_.SetInternalKey(internal_key);
      is_prev_set_ = true;
      is_prev_inclusive_ = true;
    }

    TEST_SYNC_POINT_CALLBACK("ForwardIterator::SeekInternal:Immutable", this);
  } else if (current_ && current_ != mutable_iter_) {
    // current_ is one of immutable iterators, push it back to the heap
    immutable_min_heap_.push(current_);
  }

  UpdateCurrent();
  TEST_SYNC_POINT_CALLBACK("ForwardIterator::SeekInternal:Return", this);
}

void ForwardIterator::Next() {
  assert(valid_);
  bool update_prev_key = false;

  if (sv_ == nullptr || sv_->version_number != cfd_->GetSuperVersionNumber()) {
    std::string current_key = key().ToString();
    Slice old_key(current_key.data(), current_key.size());

    if (sv_ == nullptr) {
      RebuildIterators(true);
    } else {
      RenewIterators();
    }
    SeekInternal(old_key, false);
    if (!valid_ || key().compare(old_key) != 0) {
      return;
    }
  } else if (current_ != mutable_iter_) {
    // It is going to advance immutable iterator

    if (is_prev_set_ && prefix_extractor_) {
      // advance prev_key_ to current_ only if they share the same prefix
      update_prev_key =
          prefix_extractor_->Transform(prev_key_.GetUserKey())
              .compare(prefix_extractor_->Transform(current_->key())) == 0;
    } else {
      update_prev_key = true;
    }

    if (update_prev_key) {
      prev_key_.SetInternalKey(current_->key());
      is_prev_set_ = true;
      is_prev_inclusive_ = false;
    }
  }

  current_->Next();
  if (current_ != mutable_iter_) {
    if (!current_->status().ok()) {
      immutable_status_ = current_->status();
    } else if ((current_->Valid()) && (!IsOverUpperBound(current_->key()))) {
      immutable_min_heap_.push(current_);
    } else {
      if ((current_->Valid()) && (IsOverUpperBound(current_->key()))) {
        // remove the current iterator
        DeleteCurrentIter();
        current_ = nullptr;
      }
      if (update_prev_key) {
        mutable_iter_->Seek(prev_key_.GetInternalKey());
      }
    }
  }
  UpdateCurrent();
  TEST_SYNC_POINT_CALLBACK("ForwardIterator::Next:Return", this);
}

Slice ForwardIterator::key() const {
  assert(valid_);
  return current_->key();
}

Slice ForwardIterator::value() const {
  assert(valid_);
  return current_->value();
}

Status ForwardIterator::status() const {
  if (!status_.ok()) {
    return status_;
  } else if (!mutable_iter_->status().ok()) {
    return mutable_iter_->status();
  }

  return immutable_status_;
}

Status ForwardIterator::GetProperty(std::string prop_name, std::string* prop) {
  assert(prop != nullptr);
  if (prop_name == "rocksdb.iterator.super-version-number") {
    *prop = ToString(sv_->version_number);
    return Status::OK();
  }
  return Status::InvalidArgument();
}

void ForwardIterator::SetPinnedItersMgr(
    PinnedIteratorsManager* pinned_iters_mgr) {
  pinned_iters_mgr_ = pinned_iters_mgr;
  UpdateChildrenPinnedItersMgr();
}

void ForwardIterator::UpdateChildrenPinnedItersMgr() {
  // Set PinnedIteratorsManager for mutable memtable iterator.
  if (mutable_iter_) {
    mutable_iter_->SetPinnedItersMgr(pinned_iters_mgr_);
  }

  // Set PinnedIteratorsManager for immutable memtable iterators.
  for (InternalIterator* child_iter : imm_iters_) {
    if (child_iter) {
      child_iter->SetPinnedItersMgr(pinned_iters_mgr_);
    }
  }

  // Set PinnedIteratorsManager for L0 files iterators.
  for (InternalIterator* child_iter : l0_iters_) {
    if (child_iter) {
      child_iter->SetPinnedItersMgr(pinned_iters_mgr_);
    }
  }

  // Set PinnedIteratorsManager for L1+ levels iterators.
  for (LevelIterator* child_iter : level_iters_) {
    if (child_iter) {
      child_iter->SetPinnedItersMgr(pinned_iters_mgr_);
    }
  }
}

bool ForwardIterator::IsKeyPinned() const {
  return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
         current_->IsKeyPinned();
}

bool ForwardIterator::IsValuePinned() const {
  return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() &&
         current_->IsValuePinned();
}

void ForwardIterator::RebuildIterators(bool refresh_sv) {
  // Clean up
  Cleanup(refresh_sv);
  if (refresh_sv) {
    // New
    sv_ = cfd_->GetReferencedSuperVersion(&(db_->mutex_));
  }
  RangeDelAggregator range_del_agg(
      InternalKeyComparator(cfd_->internal_comparator()), {} /* snapshots */);
  mutable_iter_ = sv_->mem->NewIterator(read_options_, &arena_);
  sv_->imm->AddIterators(read_options_, &imm_iters_, &arena_);
  if (!read_options_.ignore_range_deletions) {
//    std::unique_ptr<InternalIterator, memory::ptr_delete<InternalIterator>> range_del_iter(
//        sv_->mem->NewRangeTombstoneIterator(read_options_));
    range_del_agg.AddTombstones(sv_->mem->NewRangeTombstoneIterator(read_options_));
    sv_->imm->AddRangeTombstoneIterators(read_options_, &arena_,
                                         &range_del_agg);
  }
  has_iter_trimmed_for_upper_bound_ = false;

  BuildLevelIterators(sv_);
  current_ = nullptr;
  is_prev_set_ = false;

  UpdateChildrenPinnedItersMgr();
  if (!range_del_agg.IsEmpty()) {
    status_ = Status::NotSupported(
        "Range tombstones unsupported with ForwardIterator");
    valid_ = false;
  }
}

void ForwardIterator::RenewIterators() {
  SuperVersion* svnew;
  assert(sv_);
  svnew = cfd_->GetReferencedSuperVersion(&(db_->mutex_));

  if (mutable_iter_ != nullptr) {
    DeleteIterator(mutable_iter_, true /* is_arena */);
  }
  for (auto* m : imm_iters_) {
    DeleteIterator(m, true /* is_arena */);
  }
  imm_iters_.clear();

  mutable_iter_ = svnew->mem->NewIterator(read_options_, &arena_);
  svnew->imm->AddIterators(read_options_, &imm_iters_, &arena_);
  RangeDelAggregator range_del_agg(
      InternalKeyComparator(cfd_->internal_comparator()), {} /* snapshots */);
  if (!read_options_.ignore_range_deletions) {
//    std::unique_ptr<InternalIterator, memory::ptr_delete<InternalIterator>> range_del_iter(
//        svnew->mem->NewRangeTombstoneIterator(read_options_));
    range_del_agg.AddTombstones(svnew->mem->NewRangeTombstoneIterator(read_options_));
    sv_->imm->AddRangeTombstoneIterators(read_options_, &arena_,
                                         &range_del_agg);
  }

  for (auto* l : level_iters_) {
    DeleteIterator(l);
  }
  level_iters_.clear();
  BuildLevelIterators(svnew);
  current_ = nullptr;
  is_prev_set_ = false;
  SVCleanup();
  sv_ = svnew;

  UpdateChildrenPinnedItersMgr();
  if (!range_del_agg.IsEmpty()) {
    status_ = Status::NotSupported(
        "Range tombstones unsupported with ForwardIterator");
    valid_ = false;
  }
}

//TODO:yuanfeng unused iterator
void ForwardIterator::BuildLevelIterators(const SuperVersion* sv) {
  // build the meta vector vector
  Arena arena;
  ReadOptions read_options;
  std::unique_ptr<InternalIterator, memory::ptr_destruct<InternalIterator>> iter;
  StorageManager* storage_manager = cfd_->get_storage_manager();
  std::vector<const ExtentMeta*> metas;

  /*
  for (int32_t level = 0; level < storage::MAX_TIER_COUNT; level++) {
    iter.reset(storage_manager->get_single_level_iterator(read_options, &arena, 
                                            sv->current_meta_, level));
    if (nullptr == iter) {
      continue;
    }                                                                                      
    iter->SeekToFirst();
    metas.clear();
    while (iter->Valid()) {
      metas.emplace_back(storage_manager->get_extent_meta(
        storage_manager->get_extent_id_from_iterator(iter.get(), level)));
      iter->Next();
    }
    meta_levels_.emplace_back(metas);
  }

  level_iters_.reserve(meta_levels_.size());
  for (int32_t level = 0; level < static_cast<int32_t>(meta_levels_.size());
       level++) {
    const auto& meta_level = meta_levels_[level];
    if ((read_options_.iterate_upper_bound != nullptr) &&
        (user_comparator_->Compare(*read_options_.iterate_upper_bound,
                                   meta_level[0]->smallest_key_.user_key()) <
         0)) {
      has_iter_trimmed_for_upper_bound_ = true;
      level_iters_.push_back(nullptr);
    } else {
      level_iters_.push_back(
          new LevelIterator(cfd_, read_options_, meta_level));
    }
  }
  */
}

void ForwardIterator::ResetIncompleteIterators() {
  for (auto* level_iter : level_iters_) {
    if (level_iter && level_iter->status().IsIncomplete()) {
      level_iter->Reset();
    }
  }

  current_ = nullptr;
  is_prev_set_ = false;
}

void ForwardIterator::UpdateCurrent() {
  if (immutable_min_heap_.empty() && !mutable_iter_->Valid()) {
    current_ = nullptr;
  } else if (immutable_min_heap_.empty()) {
    current_ = mutable_iter_;
  } else if (!mutable_iter_->Valid()) {
    current_ = immutable_min_heap_.top();
    immutable_min_heap_.pop();
  } else {
    current_ = immutable_min_heap_.top();
    assert(current_ != nullptr);
    assert(current_->Valid());
    int cmp = cfd_->internal_comparator().InternalKeyComparator::Compare(
        mutable_iter_->key(), current_->key());
    assert(cmp != 0);
    if (cmp > 0) {
      immutable_min_heap_.pop();
    } else {
      current_ = mutable_iter_;
    }
  }
  valid_ = (current_ != nullptr);
  if (!status_.ok()) {
    status_ = Status::OK();
  }

  // Upper bound doesn't apply to the memtable iterator. We want Valid() to
  // return false when all iterators are over iterate_upper_bound, but can't
  // just set valid_ to false, as that would effectively disable the tailing
  // optimization (Seek() would be called on all immutable iterators regardless
  // of whether the target key is greater than prev_key_).
  current_over_upper_bound_ = valid_ && IsOverUpperBound(current_->key());
}

bool ForwardIterator::NeedToSeekImmutable(const Slice& target) {
  // We maintain the interval (prev_key_, immutable_min_heap_.top()->key())
  // such that there are no records with keys within that range in
  // immutable_min_heap_. Since immutable structures (SST files and immutable
  // memtables) can't change in this version, we don't need to do a seek if
  // 'target' belongs to that interval (immutable_min_heap_.top() is already
  // at the correct position).

  if (!valid_ || !current_ || !is_prev_set_ || !immutable_status_.ok()) {
    return true;
  }
  Slice prev_key = prev_key_.GetInternalKey();
  if (prefix_extractor_ &&
      prefix_extractor_->Transform(target).compare(
          prefix_extractor_->Transform(prev_key)) != 0) {
    return true;
  }
  if (cfd_->internal_comparator().InternalKeyComparator::Compare(
          prev_key, target) >= (is_prev_inclusive_ ? 1 : 0)) {
    return true;
  }

  if (immutable_min_heap_.empty() && current_ == mutable_iter_) {
    // Nothing to seek on.
    return false;
  }
  if (cfd_->internal_comparator().InternalKeyComparator::Compare(
          target, current_ == mutable_iter_ ? immutable_min_heap_.top()->key()
                                            : current_->key()) > 0) {
    return true;
  }
  return false;
}

void ForwardIterator::DeleteCurrentIter() {
  for (int32_t level = 0; level < static_cast<int32_t>(meta_levels_.size());
       ++level) {
    if (level_iters_[level] == nullptr) {
      continue;
    }
    if (level_iters_[level] == current_) {
      has_iter_trimmed_for_upper_bound_ = true;
      DeleteIterator(level_iters_[level]);
      level_iters_[level] = nullptr;
    }
  }
}

bool ForwardIterator::TEST_CheckDeletedIters(int* pdeleted_iters,
                                             int* pnum_iters) {
  bool retval = false;
  int deleted_iters = 0;
  int num_iters = 0;

  for (int32_t level = 0; level < static_cast<int32_t>(meta_levels_.size());
       ++level) {
    if ((level_iters_[level] == nullptr) && (!meta_levels_[level].empty())) {
      retval = true;
      deleted_iters++;
    } else if (!meta_levels_[level].empty()) {
      num_iters++;
    }
  }
  if ((!retval) && num_iters <= 1) {
    retval = true;
  }
  if (pdeleted_iters) {
    *pdeleted_iters = deleted_iters;
  }
  if (pnum_iters) {
    *pnum_iters = num_iters;
  }
  return retval;
}

uint32_t ForwardIterator::FindFileInRange(
    const std::vector<const ExtentMeta*>& files, const Slice& internal_key,
    uint32_t left, uint32_t right) {
  while (left < right) {
    uint32_t mid = (left + right) / 2;
    const ExtentMeta* f = files[mid];
    if (cfd_->internal_comparator().InternalKeyComparator::Compare(
            f->largest_key_.Encode(), internal_key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}

void ForwardIterator::DeleteIterator(InternalIterator* iter, bool is_arena) {
  if (iter == nullptr) {
    return;
  }

  if (pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled()) {
    pinned_iters_mgr_->PinIterator(iter, is_arena);
  } else {
    if (is_arena) {
      iter->~InternalIterator();
    } else {
//      delete iter;
      MOD_DELETE_OBJECT(InternalIterator, iter);
    }
  }
}
}
}  // namespace xengine

#endif  // ROCKSDB_LITE
