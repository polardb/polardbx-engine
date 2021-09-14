// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_edit.h"

#include "db/version_set.h"
#include "storage/storage_manager.h"
#include "util/coding.h"
#include "util/event_logger.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "xengine/slice.h"

using namespace xengine;
using namespace common;
using namespace util;
using namespace storage;

namespace xengine {
namespace db {

// Tag numbers for serialized VersionEdit.  These numbers are written to
// disk and should not be changed.
enum Tag {
  kComparator = 1,
  kLogNumber = 2,
  kNextFileNumber = 3,
  kLastSequence = 4,
  kCompactPointer = 5,
  kDeletedFile = 6,
  kNewFile = 7,
  // 8 was used for large value refs
  kPrevLogNumber = 9,

  // these are new formats divergent from open source leveldb
  kNewFile2 = 100,
  kNewFile3 = 102,
  kNewFile4 = 103,      // 4th (the latest) format version of adding files
  kColumnFamily = 200,  // specify column family for version edit
  kColumnFamilyAdd = 201,
  kColumnFamilyDrop = 202,
  kMaxColumnFamily = 203,
  
  kExtentMetaV0 = 1001, // extent meta manifest log
  kCheckpoint = 1002, // checkpoint manifest log 
  kExtentMeta = 1003, // expand kExtentMeta to support large object
};

enum CustomTag {
  kTerminate = 1,  // The end of customized fields
  kNeedCompaction = 2,
  kPathId = 65,
};
// If this bit for the custom tag is set, opening DB should fail if
// we don't know this field.
uint32_t kCustomTagNonSafeIgnoreMask = 1 << 6;

uint64_t PackFileNumberAndPathId(uint64_t number, uint64_t path_id) {
  assert(number <= kFileNumberMask);
  return number | (path_id * (kFileNumberMask + 1));
}

DEFINE_TO_STRING(FileMetaData, KV(smallest), KV(largest), KV(smallest_seqno),
                 KV(largest_seqno), KV(fd.extent_id.offset),
                 KV(fd.extent_id.file_number), KV(fd.file_size),
                 KV(compensated_file_size), KV(num_entries), KV(num_deletions),
                 KV(raw_key_size), KV(raw_value_size), KV(fd.extent_id.offset),
                 KV(fd.extent_id.file_number));
}  // namespace db
}  // namespace xengine
