// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

#include "table/block_builder.h"

#include <assert.h>
#include <algorithm>
#include "db/dbformat.h"
#include "util/coding.h"
#include "xengine/comparator.h"

using namespace xengine;
using namespace util;
using namespace common;

namespace xengine {
namespace table {

BlockBuilder::BlockBuilder(int block_restart_interval, bool use_delta_encoding,
                           WritableBuffer* buf)
    : block_restart_interval_(block_restart_interval),
      use_delta_encoding_(use_delta_encoding),
      buffer_(buf),
      use_internal_buf_(false),
      restarts_(),
      counter_(0),
      finished_(false),
      add_cache_(false) {
  assert(block_restart_interval_ >= 1);
  restarts_.push_back(0);  // First restart point is at offset 0
  estimate_ = sizeof(uint32_t) + sizeof(uint32_t);
  if (nullptr == buf) {
//    buffer_ = new WritableBuffer();
    buffer_ = MOD_NEW_OBJECT(memory::ModId::kWritableBuffer, WritableBuffer);
    assert(buffer_);
    use_internal_buf_ = true;
  }
}

BlockBuilder::~BlockBuilder() {
  if (use_internal_buf_) {
//    delete buffer_;
    MOD_DELETE_OBJECT(WritableBuffer, buffer_);
  }
}

void BlockBuilder::Reset() {
  buffer_->clear();
  restarts_.clear();
  restarts_.push_back(0);  // First restart point is at offset 0
  estimate_ = sizeof(uint32_t) + sizeof(uint32_t);
  counter_ = 0;
  finished_ = false;
  add_cache_ = false;
  last_key_.clear();
}

size_t BlockBuilder::EstimateSizeAfterKV(size_t key_size,
                                         size_t value_size) const {
  size_t estimate = CurrentSizeEstimate();
  estimate += key_size + value_size;
  if (counter_ >= block_restart_interval_) {
    estimate += sizeof(uint32_t);  // a new restart entry.
  }

  estimate += sizeof(int32_t);             // varint for shared prefix length.
  estimate += VarintLength(key_size);    // varint for key length.
  estimate += VarintLength(value_size);  // varint for value length.

  return estimate;
}

Slice BlockBuilder::Finish() {
  // Append restart array
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(buffer_, restarts_[i]);
  }
  PutFixed32(buffer_, static_cast<uint32_t>(restarts_.size()));
  finished_ = true;
  return Slice(buffer_->data(), buffer_->size());
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
  assert(!finished_);
  assert(counter_ <= block_restart_interval_);
  size_t shared = 0;  // number of bytes shared with prev key
  if (counter_ >= block_restart_interval_) {
    // Restart compression
    restarts_.push_back(static_cast<uint32_t>(buffer_->size()));
    estimate_ += sizeof(uint32_t);
    counter_ = 0;

    if (use_delta_encoding_) {
      // Update state
      last_key_.assign(key.data(), key.size());
    }
  } else if (use_delta_encoding_) {
    Slice last_key_piece(last_key_);
    // See how much sharing to do with previous string
    shared = key.difference_offset(last_key_piece);

    // Update state
    // We used to just copy the changed data here, but it appears to be
    // faster to just copy the whole thing.
    last_key_.assign(key.data(), key.size());
  }

  const size_t non_shared = key.size() - shared;
  const size_t curr_size = buffer_->size();

  // Add "<shared><non_shared><value_size>" to buffer_
  const int var_3int_size = 15;
  buffer_->reserve(curr_size + var_3int_size);
  int64_t pos = 0;
  util::serialize(const_cast<char*>(buffer_->data() + curr_size), var_3int_size,
                  pos, static_cast<uint32_t>(shared),
                  static_cast<uint32_t>(non_shared),
                  static_cast<uint32_t>(value.size()));
  buffer_->resize(curr_size + pos);

  // Add string delta to buffer_ followed by value
  buffer_->append(key.data() + shared, non_shared);
  buffer_->append(value.data(), value.size());

  counter_++;
  estimate_ += buffer_->size() - curr_size;
}

}  // namespace table
}  // namespace xengine
