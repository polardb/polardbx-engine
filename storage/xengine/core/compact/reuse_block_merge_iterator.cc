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

#include "db/dbformat.h"
#include "reuse_block_merge_iterator.h"

using namespace xengine;
using namespace common;
using namespace util;
using namespace db;
using namespace memory;

namespace xengine {
namespace storage {

int MetaDataIterator::extract_range(const common::Slice &key_in, const common::Slice &value_in)
{
  int ret = Status::kOk;
  ExtentMeta *extent_meta = nullptr;
  int64_t pos = 0;

  if (nullptr == (extent_meta = reinterpret_cast<ExtentMeta *>(const_cast<char *>(key_in.data())))) {
    ret = Status::kErrorUnexpected;
    COMPACTION_LOG(WARN, "unexpected error, extent meta must not nullptr", K(ret));
  } else {
    extent_meta_ = extent_meta;
    meta_descriptor_.type_.sequence_ = 0;
    meta_descriptor_.range_.start_key_ = extent_meta->smallest_key_.Encode().deep_copy(arena_);
    meta_descriptor_.range_.end_key_ = extent_meta->largest_key_.Encode().deep_copy(arena_);
    meta_descriptor_.block_position_.first = extent_meta->extent_id_.id();
    meta_descriptor_.layer_position_ = (reinterpret_cast<ExtentLayerIterator *>(iter_))->get_layer_position();
    meta_descriptor_.extent_id_ = extent_meta->extent_id_;

    //TODO check use or not
    meta_descriptor_.key_ = key_in;
    meta_descriptor_.value_ = value_in;

    if (meta_comparator_.Compare(extent_meta->smallest_key_.user_key(), ExtractUserKey(seek_bound_.end_key_)) > 0) {
      // scan until meet seek_bound_'s end
      valid_ = false;
    } else {
      // check if should skip the extent
      if (valid_) {
        meta_descriptor_.delete_percent_ = 0;
        if (extent_meta->num_entries_) {
          meta_descriptor_.delete_percent_ = 100 * extent_meta->num_deletes_ / extent_meta->num_entries_;
        }

        // delete_percent_ is 0 by default, which means normal pick,
        // otherwise, it is delete triggered Major compaction.
        // For now, only Delete triggered Major will reach here, needs to track
        // the last_userkey_ to avoid adjacent extents with same user_key. 
        if (delete_percent_ > 0) {
          if (meta_descriptor_.delete_percent_ < delete_percent_
              && last_userkey_.size() /*the first extent is always picked*/
              && meta_comparator_.Compare(last_userkey_,
                extent_meta->smallest_key_.user_key()) < 0) {
            need_skip_ = true;
          } else {
            need_skip_ = false;
            last_userkey_ = extent_meta->largest_key_.user_key().deep_copy(arena_);
          }
        }
      }

      // update seek_bound_'s end_key
      if (!need_skip_ && valid_ &&
          meta_comparator_.Compare(ExtractUserKey(extent_meta->largest_key_.Encode()), ExtractUserKey(seek_bound_.end_key_)) > 0) {
        seek_bound_.end_key_ = extent_meta->largest_key_.Encode().deep_copy(arena_);
        // reset SequenceNumber of L2's search end key to 0
        // for includes all user key records
        if (seek_bound_.end_key_.size() < 8) {
          ret = common::Status::kAborted;
        } else {
          memset(const_cast<char *>(seek_bound_.end_key_.data()) + seek_bound_.end_key_.size() - 8, 0, 8);
        }
      }
    }
  }
  return ret;
}

ReuseBlockMergeIterator::ReuseBlockMergeIterator(
    ArenaAllocator &allocator, const util::Comparator &comparator)
    : allocator_(allocator),
      comparator_(comparator),
      min_heap_(&comparator),
      child_num_(0),
      inited_(false),
      status_(Status::kOk) {
  memset(children_, 0, sizeof(children_));
}

ReuseBlockMergeIterator::~ReuseBlockMergeIterator() {}

/**
 * set_children will reset all status in last iterate loop and
 * clear all children fill before.
 */
int ReuseBlockMergeIterator::set_children(RangeIterator **children,
                                          int64_t child_num) {
  if (child_num > MAX_CHILD_NUM) {
    // ROCKS_LOG_WARN("merge too many iterators [%ld] > [%ld]", child_num,
    //               MAX_CHILD_NUM);
    return Status::kInvalidArgument;
  }

  inited_ = false;
  reset_context();

  for (int i = 0; i < child_num; i++) {
    if (nullptr == children[i]) {
      // ROCKS_LOG_WARN("merge iterator [%ld] is invalid(nullptr)", i);
      return Status::kInvalidArgument;
    }
    children_[i] = children[i];
  }
  for (int i = 0; i < child_num; i++) {
    if (children_[i]->valid()) {
      min_heap_.push(children_[i]);
    }
  }
  find_next_closed_range();
  child_num_ = child_num;
  inited_ = true;
  return Status::kOk;
}

/*
// make sure we are not start iterate.
Status ReuseBlockMergeIterator::add_child(RangeIterator *iter) {
  if (nullptr == iter) {
    return Status::InvalidArgument("invalid iter (nullptr)");
  }
  if (child_num_ >= MAX_CHILD_NUM) {
    return Status::NoSpace();
  }

  children_[child_num_++] = iter;
  iterator_states_.reset();

  if (iter->valid()) {
    min_heap_.push(iter);
    find_next_closed_range();
  }
  return Status::OK();
}
*/

void ReuseBlockMergeIterator::seek_to_first() {
  if (!inited_) return;
  reset_context();
  for (int64_t i = 0; i < child_num_; i++) {
    children_[i]->seek_to_first();
    if (children_[i]->valid()) {
      min_heap_.push(children_[i]);
    }
  }
  find_next_closed_range();
}

void ReuseBlockMergeIterator::seek(const common::Slice &target) {
  if (!inited_) return;
  reset_context();
  for (int i = 0; i < child_num_; i++) {
    children_[i]->seek(target);
    if (children_[i]->valid()) {
      min_heap_.push(children_[i]);
    }
  }
  find_next_closed_range();
}

bool ReuseBlockMergeIterator::valid() const {
  return inited_ && all_iterator_closed() && output_.size() > 0;
}

common::Status ReuseBlockMergeIterator::status() const {
  return Status(status_);
}

void ReuseBlockMergeIterator::next() {
  if (!inited_) return;
  output_.clear();
  find_next_closed_range();
}

void ReuseBlockMergeIterator::find_next_closed_range() {
  if (!inited_) return;
  if (min_heap_.empty()) return;
  assert(all_iterator_closed());

  do {
    RangeIterator *iter = min_heap_.top();
    assert(iter->valid());
    if (iter->middle()) {
      iterator_states_.set(iter->get_meta_descriptor().type_.way_);
    } else {
      iterator_states_.reset(iter->get_meta_descriptor().type_.way_);
      add_output(*iter);
    }

    iter->next();
    if (iter->valid()) {
      min_heap_.replace_top(iter);
    } else {
      min_heap_.pop();
    }

    if (all_iterator_closed() && !min_heap_.empty()) {
      // peek next one check if equal to last key;
      // keep doing fetch next batch of range until got
      // disjoint range;
      iter = min_heap_.top();
      if (iter->valid() && iter->middle() &&
          (!comparator_.Equal(iter->user_key(),
                              output_.back().get_end_user_key()))) {
        break;
      }
    }
  } while (!min_heap_.empty());
}

void ReuseBlockMergeIterator::add_output(RangeIterator &iter) {
  // copy current to MetaDescriptor
  MetaDescriptor md = iter.get_meta_descriptor().deep_copy(allocator_);
//  output_.emplace_back(md);
  output_.push_back(md);
}

void ReuseBlockMergeIterator::reset_context() {
  min_heap_.clear();
  output_.clear();
  iterator_states_.reset();
}

bool ReuseBlockMergeIterator::all_iterator_closed() const {
  return iterator_states_.none();
}

}
}
