// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE

#pragma once

#include <string>

#include "xengine/compaction_filter.h"
#include "xengine/slice.h"

namespace xengine {
namespace util {

class RemoveEmptyValueCompactionFilter : public storage::CompactionFilter {
 public:
  const char* Name() const override;
  bool Filter(int level, const common::Slice& key,
              const common::Slice& existing_value, std::string* new_value,
              bool* value_changed) const override;
};
}  //  namespace util
}  //  namespace xengine
#endif  // !ROCKSDB_LITE
