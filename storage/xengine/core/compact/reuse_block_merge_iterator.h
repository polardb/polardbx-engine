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
#ifndef XENGINE_REUSE_BLOCK_MERGE_ITERATOR_H_
#define XENGINE_REUSE_BLOCK_MERGE_ITERATOR_H_

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <bitset>
#include "meta_data.h"
#include "range_iterator.h"
#include "table/format.h"
#include "table/iter_heap.h"
#include "table/iterator_wrapper.h"
//#include "storage/storage_meta_struct.h"
#include "storage/storage_manager.h"
#include "memory/allocator.h"
#include "util/autovector.h"
#include "util/heap.h"
#include "memory/page_arena.h"
#include "xengine/slice.h"
#include "xengine/status.h"
#include "xengine/xengine_constants.h"

namespace xengine {
namespace storage {

/**
 * MetaValueRangeIterator takes a range(start_key,end_key) iterator
 * and iterate start_key, end_key sequentially.
 */
// no use
//class MetaValueRangeIterator
//    : public RangeAdaptorIterator<table::InternalIterator> {
// public:
//  MetaValueRangeIterator() : RangeAdaptorIterator<table::InternalIterator>() {}
//  explicit MetaValueRangeIterator(const storage::MetaType &type,
//                                  table::InternalIterator *iter)
//      : RangeAdaptorIterator<table::InternalIterator>(type, iter) {}
//  virtual ~MetaValueRangeIterator() {}
//
//  virtual common::Slice user_key() const override { return key(); }
//
// protected:
//  virtual int extract_range(const common::Slice &key_in,
//                            const common::Slice &value_in) override {
//    meta_descriptor_.range_.end_key_ = key_in;
//    int64_t pos = 0;
//    ExtentMetaValue meta_value;
//    int r = meta_value.deserialize(value_in.data(), value_in.size(), pos);
//    if (0 == r) {
//      meta_descriptor_.range_.start_key_ = meta_value.get_start_key();
//      meta_descriptor_.block_position_ = meta_value.get_extent_list();
//    } else {
//      valid_ = false;
//    }
//    return r;
//  }
//};

class DataBlockIndexRangeIterator
    : public RangeAdaptorIterator<table::InternalIterator> {
 public:
  DataBlockIndexRangeIterator()
      : RangeAdaptorIterator<table::InternalIterator>() {}
  explicit DataBlockIndexRangeIterator(const storage::MetaType &type,
                                       table::InternalIterator *iter)
      : RangeAdaptorIterator<table::InternalIterator>(type, iter) {}
  virtual ~DataBlockIndexRangeIterator() {}

 protected:
  virtual int extract_range(const common::Slice &key_in,
                            const common::Slice &value_in) override {
    int ret = 0;
    meta_descriptor_.range_.end_key_ = key_in;
    common::Slice block_index_content = value_in;
    table::BlockHandle handle;
    handle.DecodeFrom(const_cast<common::Slice *>(&block_index_content));
    meta_descriptor_.block_position_.first = handle.offset();
    meta_descriptor_.block_position_.second = handle.size();
    // save block_stats to value_
    meta_descriptor_.value_ = block_index_content;
    int64_t delete_percent = 0;
    FAIL_RETURN(db::BlockStats::decode(block_index_content,
                                       meta_descriptor_.range_.start_key_,
                                       delete_percent));
    return ret;
  }
};

class MetaDataIterator : public RangeAdaptorIterator<table::InternalIterator> {
 public:
  // MetaDataIterator() : RangeAdaptorIterator<Iterator>() {}
  explicit MetaDataIterator(const storage::MetaType &type, table::InternalIterator *iter,
                            const storage::Range &range,
                            const util::Comparator &comparator,
                            const int64_t delete_percent = 0)
      : RangeAdaptorIterator<table::InternalIterator>(type, iter),
        meta_comparator_(comparator),
        seek_bound_(range),
        level_(type.level_),
        delete_percent_(delete_percent),
        need_skip_(false),
        arena_(memory::CharArena::DEFAULT_PAGE_SIZE, memory::ModId::kCompaction) {
    int64_t pos = 0;
    seek_key_.DecodeFrom(seek_bound_.start_key_);
    last_userkey_.clear();
  }
  virtual ~MetaDataIterator() {}

 public:
  virtual void seek_to_first() {
    assert(iter_);
    iter_->Seek(seek_bound_.start_key_);
    update();
  }
  virtual void next() override {
    assert(iter_);
    if (start_) {
      start_ = false;
    } else {
      do {
        iter_->Next();
        update();
      } while (need_skip_ && valid_);
    }
  }
 protected:
  virtual int extract_range(const common::Slice &key_in, const common::Slice &value_in);

  const util::Comparator &meta_comparator_;
  storage::Range seek_bound_;
  db::InternalKey seek_key_;
  int64_t level_;
  int64_t delete_percent_;
  bool need_skip_;
  common::Slice last_userkey_;
  memory::ArenaAllocator arena_;
};

//class MetaDataSingleIterator : public RangeAdaptorIterator<db::Iterator> {
class MetaDataSingleIterator : public RangeAdaptorIterator<table::InternalIterator> {
 public:
  explicit MetaDataSingleIterator(const storage::MetaType &type, table::InternalIterator *iter)
      : RangeAdaptorIterator<table::InternalIterator>(type, iter),
        level_(type.level_),
        arena_(memory::CharArena::DEFAULT_PAGE_SIZE, memory::ModId::kCompaction) {
  }
  virtual ~MetaDataSingleIterator() {}

 public:
  virtual void seek_to_first() {
    assert(iter_);
    iter_->SeekToFirst();
    valid_ = iter_->Valid();
    if (valid_) {
      status_ = extract_range(iter_->key(), iter_->value());
    }
  }
  virtual void next() override {
    assert(iter_);
    iter_->Next();
    valid_ = iter_->Valid();
    if (valid_) {
      status_ = extract_range(iter_->key(), iter_->value());
    }
  }
 protected:
  virtual int extract_range(const common::Slice &key_in,
                            const common::Slice &value_in) override {
    int ret = 0;
    ExtentMeta *extent_meta = nullptr;
    int64_t pos = 0;

    if (nullptr == (extent_meta = reinterpret_cast<ExtentMeta *>(const_cast<char *>(key_in.data())))) {
      ret = common::Status::kErrorUnexpected;
      COMPACTION_LOG(WARN, "unexpected error, extent meta must not nullptr", K(ret));
    } else {
      extent_meta_ = extent_meta;
      meta_descriptor_.range_.start_key_ = extent_meta->smallest_key_.Encode().deep_copy(arena_);
      meta_descriptor_.range_.end_key_ = extent_meta->largest_key_.Encode().deep_copy(arena_);
      meta_descriptor_.block_position_.first = extent_meta->extent_id_.id();
      meta_descriptor_.layer_position_ = (reinterpret_cast<ExtentLayerIterator *>(iter_))->get_layer_position();
      meta_descriptor_.extent_id_ = extent_meta->extent_id_;

      //
      meta_descriptor_.type_.sequence_ = 0;
      meta_descriptor_.key_ = key_in;
      meta_descriptor_.value_ = value_in;
    }
    return ret;
  }

  int64_t level_;
  memory::ArenaAllocator arena_;
};

template <typename IteratorWrapper>
class MinHeapComparator {
 public:
  MinHeapComparator(const util::Comparator *comparator)
      : comparator_(comparator) {}

  bool operator()(IteratorWrapper *a, IteratorWrapper *b) const {
    int cmp = comparator_->Compare(a->user_key(), b->user_key());
    if (0 == cmp)
      return !a->middle() && b->middle();
    else
      return cmp > 0;
  }

 private:
  const util::Comparator *comparator_;
};

class ReuseBlockMergeIterator {
 public:
  static const int64_t RESERVE_CHILD_NUM = 4;
  static const int64_t RESERVE_DESC_NUM = 16;
  static const int64_t MAX_CHILD_NUM = 16;
  static const int64_t MINOR_MAX_CHILD_NUM = 4;

  typedef util::BinaryHeap<RangeIterator *, MinHeapComparator<RangeIterator> >
      MergerMinIterHeap;

 public:
  ReuseBlockMergeIterator(memory::ArenaAllocator &allocator,
                          const util::Comparator &comparator);
  virtual ~ReuseBlockMergeIterator();

  // set_children will reset all iterators in merging context.
  // make sure we are not in middle of iterate loop;
  int set_children(RangeIterator **children, int64_t child_num);
  RangeIterator **get_children(int64_t &child_num) {
    child_num = child_num_;
    return children_;
  }

  bool valid() const;
  void seek_to_first();
  void seek(const common::Slice &target);
  void next();
  common::Status status() const;

  const MetaDescriptorList &get_output() const { return output_; }

 private:
  void find_next_closed_range();
  void add_output(RangeIterator &iter);
  void reset_context();
  bool all_iterator_closed() const;

 private:
  memory::ArenaAllocator &allocator_;
  const util::Comparator &comparator_;
  MergerMinIterHeap min_heap_;
  RangeIterator *children_[MAX_CHILD_NUM];
  int64_t child_num_;

  MetaDescriptorList output_;
  std::bitset<MAX_CHILD_NUM> iterator_states_;
  bool inited_;
  int status_;

  ReuseBlockMergeIterator(const ReuseBlockMergeIterator &) = delete;
  ReuseBlockMergeIterator &operator=(const ReuseBlockMergeIterator &) = delete;
};

template <typename RawIterator>
class MinRawIteratorComparator {
 public:
  MinRawIteratorComparator(const util::Comparator *comparator)
      : comparator_(comparator) {}

  bool operator()(RawIterator *a, RawIterator *b) const {
    return comparator_->Compare(a->key(), b->key()) > 0;
  }

 private:
  const util::Comparator *comparator_;
};

// only for compaction use;
// do not take charge for all iterators in merging.
class DynamicMergeIterator : public table::InternalIterator {
 public:
  typedef util::BinaryHeap<table::IteratorWrapper *,
                           table::MinIteratorComparator>
      MergerMinIterHeap;
  typedef std::function<void(table::InternalIterator *)> RecycleOnFinish;
  static const int64_t MAX_RESERVE_WRAPPER_NUM =
      ReuseBlockMergeIterator::MAX_CHILD_NUM;

  DynamicMergeIterator(const util::Comparator *comparator, RecycleOnFinish func)
      : comparator_(comparator),
        current_(nullptr),
        min_heap_(comparator_),
        recycle_func_(func),
        status_(common::Status::kOk) {}

  void add_new_child(const int32_t way, InternalIterator *iter
      /*const common::XengineSchema *schema = nullptr*/) {
    assert(nullptr != iter);
    assert(nullptr == wrappers_[way].iter());
    InternalIterator *old = wrappers_[way].Set(iter);
    assert(nullptr == old);
    wrappers_[way].SeekToFirst();
    status_ = wrappers_[way].status().code();
    if (common::Status::kOk == status_ && iter->Valid()) {
      min_heap_.push(&wrappers_[way]);
      current_ = current_forward();
//      wrappers_[way].set_schema(schema);
    }
  }

  virtual ~DynamicMergeIterator() {
    for (table::IteratorWrapper &wrapper : wrappers_) {
      recycle_func_(wrapper.Set(nullptr));
    }
  }

  virtual bool Valid() const override { return (current_ != nullptr); }

  virtual void SeekToFirst() override {
    // do nothing.. we 've already seek to first in add_new_child;
    assert(nullptr != current_);
    status_ = current_->status().code();
  }

  virtual void SeekToLast() override { assert(false); }

  virtual void Seek(const common::Slice &target) override { assert(false); }

  virtual void SeekForPrev(const common::Slice &target) override {
    assert(false);
  }

  virtual void Next() override {
    assert(Valid());

    // For the heap modifications below to be correct, current_ must be the
    // current top of the heap.
    assert(current_ == current_forward());

    // as the current points to the current record. move the iterator forward.
    current_->Next();
    status_ = current_->status().code();
    if (LIKELY(common::Status::kOk == status_ && current_->Valid())) {
      // current is still valid after the Next() call above.  Call
      // replace_top() to restore the heap property.  When the same child
      // iterator yields a sequence of keys, this is cheap.
      min_heap_.replace_top(current_);
    } else {
      // current stopped being valid, remove it from the heap.
      min_heap_.pop();
      recycle_func_(current_->iter());
      current_->Set(nullptr);
    }
    current_ = current_forward();
  }

  virtual void Prev() override { assert(false); }

  virtual common::Slice key() const override {
    assert(Valid());
    return current_->key();
  }

  virtual common::Slice value() const override {
    assert(Valid());
    return current_->value();
  }

  virtual common::Status status() const override {
    return common::Status(status_);
  }

  virtual void SetPinnedItersMgr(
      db::PinnedIteratorsManager *pinned_iters_mgr) override {
    UNUSED(pinned_iters_mgr);
  }

  virtual bool IsKeyPinned() const override {
    assert(Valid());
    return false;
  }

  virtual bool IsValuePinned() const override {
    assert(Valid());
    return false;
  }

//  virtual const common::XengineSchema* get_schema() override {
//    if (nullptr != current_) {
//      return current_->get_schema();
//    } else {
//      return nullptr;
//    }
//  }
 private:
  table::IteratorWrapper *current_forward() const {
    return !min_heap_.empty() ? min_heap_.top() : nullptr;
  }

  table::IteratorWrapper wrappers_[MAX_RESERVE_WRAPPER_NUM];

  const util::Comparator *comparator_;
  // Cached pointer to child iterator with the current key, or nullptr if no
  // child iterators are valid.  This is the top of minHeap_ or maxHeap_
  // depending on the direction.
  table::IteratorWrapper *current_;
  // Which direction is the iterator moving?
  MergerMinIterHeap min_heap_;
  RecycleOnFinish recycle_func_;
  int status_;

  // Max heap is used for reverse iteration, which is way less common than
  // forward.  Lazily initialize it to save memory.
};

} // storage
} // xengine

#endif  // XENGINE_REUSE_BLOCK_MERGE_ITERATOR_H_
