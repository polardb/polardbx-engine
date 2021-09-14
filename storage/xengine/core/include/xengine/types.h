/*
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef STORAGE_ROCKSDB_INCLUDE_TYPES_H_
#define STORAGE_ROCKSDB_INCLUDE_TYPES_H_

#include <stdint.h>

namespace xengine {
namespace common {

// Define all public custom types here.

// Represents a sequence number in a WAL file.
typedef uint64_t SequenceNumber;

}  // namespace common
}  // namespace xengine

#endif  //  STORAGE_ROCKSDB_INCLUDE_TYPES_H_
