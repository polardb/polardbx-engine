// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include "memory/allocator.h"
#include "xengine/types.h"

namespace xengine {

namespace util {
class Comparator;
class Env;
class Arena;
}

namespace table {

class InternalIterator;

// Return an iterator that provided the union of the data in
// children[0,n-1].  Takes ownership of the child iterators and
// will delete them when the result iterator is deleted.
//
// The result does no duplicate suppression.  I.e., if a particular
// key is present in K child iterators, it will be yielded K times.
//
// REQUIRES: n >= 0
extern InternalIterator* NewMergingIterator(const util::Comparator* comparator,
                                            InternalIterator** children, int n,
                                            memory::SimpleAllocator* arena = nullptr,
                                            bool prefix_seek_mode = false);
extern InternalIterator* NewMergingIterator(const util::Comparator* comparator,
                                            InternalIterator** children, int n,
                                            memory::SimpleAllocator& arena,
                                            bool prefix_seek_mode = false);

class MergingIterator;

// A builder class to build a merging iterator by adding iterators one by one.
class MergeIteratorBuilder {
 public:
  // comparator: the comparator used in merging comparator
  // arena: where the merging iterator needs to be allocated from.
  explicit MergeIteratorBuilder(const util::Comparator* comparator,
                                util::Arena* arena,
                                bool prefix_seek_mode = false);
  ~MergeIteratorBuilder() {}

  // Add iter to the merging iterator.
  void AddIterator(InternalIterator* iter);
  static void add_new_iterator(InternalIterator* merging_iter,
                               InternalIterator* iter);
  static void reserve_child_num(InternalIterator* merging_iter,
                                const int64_t n);
  void reserve_child_num(const int64_t n);

  // Get arena used to build the merging iterator. It is called one a child
  // iterator needs to be allocated.
  util::Arena* GetArena() { return arena; }

  // Return the result merging iterator.
  InternalIterator* Finish();

 private:
  MergingIterator* merge_iter;
  InternalIterator* first_iter;
  bool use_merging_iter;
  util::Arena* arena;
};

}  // namespace table
}  // namespace xengine
