// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/version_builder.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <algorithm>
#include <atomic>
#include <functional>
#include <set>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "db/internal_stats.h"
#include "db/table_cache.h"
#include "db/version_set.h"
#include "port/port.h"
#include "table/table_reader.h"

using namespace xengine;
using namespace common;
using namespace util;

namespace xengine {
namespace db {

bool NewestFirstBySeqNo(FileMetaData* a, FileMetaData* b) {
  if (a->largest_seqno != b->largest_seqno) {
    return a->largest_seqno > b->largest_seqno;
  }
  if (a->smallest_seqno != b->smallest_seqno) {
    return a->smallest_seqno > b->smallest_seqno;
  }
  // Break ties by file number
  return a->fd.GetNumber() > b->fd.GetNumber();
}

namespace {
bool BySmallestKey(FileMetaData* a, FileMetaData* b,
                   const InternalKeyComparator* cmp) {
  int r = cmp->Compare(a->smallest, b->smallest);
  if (r != 0) {
    return (r < 0);
  }
  // Break ties by file number
  return (a->fd.GetNumber() < b->fd.GetNumber());
}
}  // namespace

class VersionBuilder::Rep {
 private:
  // Helper to sort files_ in v
  // kLevel0 -- NewestFirstBySeqNo
  // kLevelNon0 -- BySmallestKey
  struct FileComparator {
    enum SortMethod {
      kLevel0 = 0,
      kLevelNon0 = 1,
    } sort_method;
    const InternalKeyComparator* internal_comparator;

    bool operator()(FileMetaData* f1, FileMetaData* f2) const {
      switch (sort_method) {
        case kLevel0:
          return NewestFirstBySeqNo(f1, f2);
        case kLevelNon0:
          return BySmallestKey(f1, f2, internal_comparator);
      }
      assert(false);
      return false;
    }
  };

  struct LevelState {
    std::unordered_set<uint64_t> deleted_files;
    // Map from file number to file meta data.
    std::unordered_map<uint64_t, FileMetaData*> added_files;
  };

  const EnvOptions& env_options_;
  TableCache* table_cache_;
  LevelState* levels_;
  FileComparator level_zero_cmp_;
  FileComparator level_nonzero_cmp_;

 public:
  Rep(const EnvOptions& env_options, TableCache* table_cache)
      : env_options_(env_options),
        table_cache_(table_cache) {
    level_zero_cmp_.sort_method = FileComparator::kLevel0;
    level_nonzero_cmp_.sort_method = FileComparator::kLevelNon0;
  }

  ~Rep() {
  }

  void UnrefFile(FileMetaData* f) {
    f->refs--;
    if (f->refs <= 0) {
      if (f->table_reader_handle) {
        assert(table_cache_ != nullptr);
        table_cache_->ReleaseHandle(f->table_reader_handle);
        f->table_reader_handle = nullptr;
      }
      delete f;
    }
  }

  void CheckConsistencyForDeletes(VersionEdit* edit, uint64_t number,
                                  int level) {
#ifdef NDEBUG
#endif
    // a file to be deleted better exist in the previous version
    bool found = false;
    // maybe this file was added in a previous edit that was Applied
    if (!found) {
      auto& level_added = levels_[level].added_files;
      auto got = level_added.find(number);
      if (got != level_added.end()) {
        found = true;
      }
    }
    if (!found) {
      fprintf(stderr, "not found %" PRIu64 "\n", number);
      abort();
    }
  }

  // Apply all of the edits in *edit to the current state.
  void Apply(VersionEdit* edit) {
  }

  void LoadTableHandlers(InternalStats* internal_stats, int max_threads,
                         bool prefetch_index_and_filter_in_cache) {
  }
};

VersionBuilder::VersionBuilder(const EnvOptions& env_options,
                               TableCache* table_cache)
    : rep_(new Rep(env_options, table_cache)) {}
VersionBuilder::~VersionBuilder() { delete rep_; }
void VersionBuilder::CheckConsistencyForDeletes(VersionEdit* edit,
                                                uint64_t number, int level) {
  rep_->CheckConsistencyForDeletes(edit, number, level);
}
void VersionBuilder::Apply(VersionEdit* edit) { rep_->Apply(edit); }
void VersionBuilder::LoadTableHandlers(
    InternalStats* internal_stats, int max_threads,
    bool prefetch_index_and_filter_in_cache) {
  rep_->LoadTableHandlers(internal_stats, max_threads,
                          prefetch_index_and_filter_in_cache);
}

}  // namespace db
}  // namespace xengine
