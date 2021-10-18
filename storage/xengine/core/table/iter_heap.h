// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#pragma once

#include "table/iterator_wrapper.h"
#include "xengine/comparator.h"

namespace xengine {
namespace table {

// When used with std::priority_queue, this comparison functor puts the
// iterator with the max/largest key on top.
class MaxIteratorComparator {
 public:
  MaxIteratorComparator(const util::Comparator* comparator)
      : comparator_(comparator) {}

  bool operator()(IteratorWrapper* a, IteratorWrapper* b) const {
    return comparator_->Compare(a->key(), b->key()) < 0;
  }

 private:
  const util::Comparator* comparator_;
};

// When used with std::priority_queue, this comparison functor puts the
// iterator with the min/smallest key on top.
class MinIteratorComparator {
 public:
  MinIteratorComparator(const util::Comparator* comparator)
      : comparator_(comparator) {}

  bool operator()(IteratorWrapper* a, IteratorWrapper* b) const {
    return comparator_->Compare(a->key(), b->key()) > 0;
  }

 private:
  const util::Comparator* comparator_;
};

}  // namespace table
}  // namespace xengine
