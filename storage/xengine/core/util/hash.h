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
// Simple hash function used for internal data structures

#pragma once
#include <stddef.h>
#include <stdint.h>

#include "xengine/slice.h"

namespace xengine {
namespace util {

extern uint32_t Hash(const char* data, size_t n, uint32_t seed);

inline uint32_t BloomHash(const common::Slice& key) {
  return Hash(key.data(), key.size(), 0xbc9f1d34);
}

inline uint32_t GetSliceHash(const common::Slice& s) {
  return Hash(s.data(), s.size(), 397);
}

// std::hash compatible interface.
struct SliceHasher {
  uint32_t operator()(const common::Slice& s) const { return GetSliceHash(s); }
};

}  // namespace util
}  // namespace xengine
