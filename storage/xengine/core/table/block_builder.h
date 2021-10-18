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
#include <vector>

#include <stdint.h>
#include "util/aligned_buffer.h"
#include "xengine/slice.h"

namespace xengine {
namespace table {

class BlockBuilder {
 public:
  BlockBuilder(const BlockBuilder&) = delete;
  void operator=(const BlockBuilder&) = delete;

  explicit BlockBuilder(int block_restart_interval,
                        bool use_delta_encoding = true,
                        util::WritableBuffer* buf = nullptr);

  ~BlockBuilder();

  // Reset the contents as if the BlockBuilder was just constructed.
  void Reset();

  // REQUIRES: Finish() has not been called since the last call to Reset().
  // REQUIRES: key is larger than any previously added key
  void Add(const common::Slice& key, const common::Slice& value);

  // Finish building the block and return a slice that refers to the
  // block contents.  The returned slice will remain valid for the
  // lifetime of this builder or until Reset() is called.
  common::Slice Finish();

  // Returns an estimate of the current (uncompressed) size of the block
  // we are building.
  inline size_t CurrentSizeEstimate() const { return estimate_; }

  // Returns an estimated block size after appending key and value.
  size_t EstimateSizeAfterKV(size_t key_size, size_t value_size) const;

  // Return true iff no entries have been added since the last Reset()
  bool empty() const { return buffer_->empty(); }
  void set_add_cache(const bool add_cache) { add_cache_ = add_cache; }
  bool need_add_cache() const { return add_cache_; }
 private:
  const int block_restart_interval_;
  const bool use_delta_encoding_;

  util::WritableBuffer* buffer_;  // Destination buffer
  bool use_internal_buf_;

  std::vector<uint32_t> restarts_;  // Restart points
  size_t estimate_;
  int counter_;    // Number of entries emitted since restart
  bool finished_;  // Has Finish() been called?
  std::string last_key_;
  bool add_cache_;
};

}  // namespace table
}  // namespace xengine
