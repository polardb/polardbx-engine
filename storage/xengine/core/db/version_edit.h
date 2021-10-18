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
#include <algorithm>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include "db/dbformat.h"
#include "storage/io_extent.h"
#include "util/arena.h"
#include "util/autovector.h"
#include "xengine/cache.h"

namespace xengine {

namespace storage {
class ExtentSpaceManager;
struct ChangeInfo;
}

namespace db {

class VersionSet;

const uint64_t kFileNumberMask = 0x3FFFFFFFFFFFFFFF;

extern uint64_t PackFileNumberAndPathId(uint64_t number, uint64_t path_id);

// A copyable structure contains information needed to read data from an SST
// file. It can contains a pointer to a table reader opened for the file, or
// file number and size, which can be used to create a new table reader for it.
// The behavior is undefined when a copied of the structure is used when the
// file is not in any live version any more.
struct FileDescriptor {
  // Table reader in table_reader_handle
  table::TableReader* table_reader;
  storage::ExtentId extent_id;
  uint64_t file_size;  // File size in bytes

  FileDescriptor() : FileDescriptor(0, 0, 0) {}

  FileDescriptor(uint64_t eid, uint32_t path_id, uint64_t _file_size)
      : table_reader(nullptr), extent_id(eid), file_size(_file_size) {}

  FileDescriptor& operator=(const FileDescriptor& fd) {
    table_reader = fd.table_reader;
    extent_id = fd.extent_id;
    file_size = fd.file_size;
    return *this;
  }

  uint64_t GetNumber() const { return extent_id.id(); }
  uint32_t GetPathId() const { return 0; }
  uint64_t GetFileSize() const { return file_size; }
};

struct FileMetaData {
  int refs;
  FileDescriptor fd;
  InternalKey smallest;  // Smallest internal key served by table
  InternalKey largest;   // Largest internal key served by table
  bool being_compacted;  // Is this file undergoing compaction?
  common::SequenceNumber smallest_seqno;  // The smallest seqno in this file
  common::SequenceNumber largest_seqno;   // The largest seqno in this file

  // Needs to be disposed when refs becomes 0.
  cache::Cache::Handle* table_reader_handle;

  // Stats for compensating deletion entries during compaction

  // File size compensated by deletion entry.
  // This is updated in Version::UpdateAccumulatedStats() first time when the
  // file is created or loaded.  After it is updated (!= 0), it is immutable.
  uint64_t compensated_file_size;
  // These values can mutate, but they can only be read or written from
  // single-threaded LogAndApply thread
  uint64_t num_entries;       // the number of entries.
  uint64_t num_deletions;     // the number of deletion entries.
  uint64_t raw_key_size;      // total uncompressed key size.
  uint64_t raw_value_size;    // total uncompressed value size.
  bool init_stats_from_file;  // true if the data-entry stats of this file
                              // has initialized from file.

  bool marked_for_compaction;  // True if client asked us nicely to compact this
                               // file.

  FileMetaData()
      : refs(0),
        being_compacted(false),
        smallest_seqno(kMaxSequenceNumber),
        largest_seqno(0),
        table_reader_handle(nullptr),
        compensated_file_size(0),
        num_entries(0),
        num_deletions(0),
        raw_key_size(0),
        raw_value_size(0),
        init_stats_from_file(false),
        marked_for_compaction(false) {}

  // REQUIRED: Keys must be given to the function in sorted order (it expects
  // the last key to be the largest).
  void UpdateBoundaries(const common::Slice& key,
                        common::SequenceNumber seqno) {
    if (smallest.size() == 0) {
      smallest.DecodeFrom(key);
    }
    largest.DecodeFrom(key);
    smallest_seqno = std::min(smallest_seqno, seqno);
    largest_seqno = std::max(largest_seqno, seqno);
  }

  DECLARE_TO_STRING();
};

// A compressed copy of file meta data that just contain
// smallest and largest key's slice
struct FdWithKeyRange {
  FileDescriptor fd;
  common::Slice smallest_key;  // slice that contain smallest key
  common::Slice largest_key;   // slice that contain largest key

  FdWithKeyRange() : fd(), smallest_key(), largest_key() {}

  FdWithKeyRange(FileDescriptor _fd, common::Slice _smallest_key,
                 common::Slice _largest_key)
      : fd(_fd), smallest_key(_smallest_key), largest_key(_largest_key) {}
};

// Data structure to store an array of FdWithKeyRange in one level
// Actual data is guaranteed to be stored closely
struct LevelFilesBrief {
  size_t num_files;
  FdWithKeyRange* files;
  LevelFilesBrief() {
    num_files = 0;
    files = nullptr;
  }
};

// A memtable will be split/flushed into multiple MiniTables.
// A MiniTable usually contains one extent, but more in some cases.
//   e.g. k/v pair is too large, or the size estimation is wrong.
struct MiniTables {
  storage::ExtentSpaceManager* space_manager = nullptr;
  util::Env::IOPriority io_priority;
  std::vector<FileMetaData> metas;
  std::vector<table::TableProperties> props;
  storage::ChangeInfo *change_info_ = nullptr;
  int level = 0;
  int64_t table_space_id_ = -1;
};
}  // namespace db
}  // namespace xengine
