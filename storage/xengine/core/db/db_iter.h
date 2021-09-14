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
#include <stdint.h>
#include <string>
#include "db/dbformat.h"
#include "db/range_del_aggregator.h"
#include "options/cf_options.h"
#include "util/arena.h"
#include "util/autovector.h"
#include "xengine/db.h"
#include "xengine/iterator.h"

namespace xengine {

namespace util {
class Arena;
}

namespace table {
class InternalIterator;
}

namespace db {

class DBIter;

// Return a new iterator that converts internal keys (yielded by
// "*internal_iter") that were live at the specified "sequence" number
// into appropriate user keys.
extern Iterator* NewDBIterator(util::Env* env,
                               const common::ReadOptions& read_options,
                               const common::ImmutableCFOptions& cf_options,
                               const util::Comparator* user_key_comparator,
                               table::InternalIterator* internal_iter,
                               const common::SequenceNumber& sequence,
                               uint64_t max_sequential_skip_in_iterations,
                               uint64_t version_number,
                               util::Arena* arena = nullptr,
                               storage::ExtentSpaceManager* space_manager = nullptr);

extern Iterator* NewDBIterator(util::Env* env,
                               const common::ReadOptions& read_options,
                               const common::ImmutableCFOptions& cf_options,
                               const util::Comparator* user_key_comparator,
                               table::InternalIterator* internal_iter,
                               const common::SequenceNumber& sequence,
                               bool use_arena,  // using a outer arena
                               uint64_t max_sequential_skip_in_iterations,
                               uint64_t version_number,
                               util::Arena* arena = nullptr,
                               storage::ExtentSpaceManager* space_manager = nullptr);

// A wrapper iterator which wraps DB Iterator and the arena, with which the DB
// iterator is supposed be allocated. This class is used as an entry point of
// a iterator hierarchy whose memory can be allocated inline. In that way,
// accessing the iterator tree can be more cache friendly. It is also faster
// to allocate.
class ArenaWrappedDBIter : public Iterator {
 public:
  ArenaWrappedDBIter();
  virtual ~ArenaWrappedDBIter();

  // Get the arena to be used to allocate memory for DBIter to be wrapped,
  // as well as child iterators in it.
  virtual util::Arena* GetArena() { return &arena_; }
  virtual RangeDelAggregator* GetRangeDelAggregator();

  // Set the DB Iterator to be wrapped

  virtual void SetDBIter(DBIter* iter);

  // Set the internal iterator wrapped inside the DB Iterator. Usually it is
  // a merging iterator.
  virtual void SetIterUnderDBIter(table::InternalIterator* iter);
  virtual bool Valid() const override;
  virtual void SeekToFirst() override;
  virtual void SeekToLast() override;
  virtual void Seek(const common::Slice& target) override;
  virtual void SeekForPrev(const common::Slice& target) override;
  virtual void Next() override;
  virtual void Prev() override;
  virtual common::Slice key() const override;
  virtual common::Slice value() const override;
  virtual common::Status status() const override;

  void RegisterCleanup(CleanupFunction function, void* arg1, void* arg2);
  virtual common::Status GetProperty(std::string prop_name,
                                     std::string* prop) override;
  virtual int set_end_key(const common::Slice& end_key_slice) override;
  virtual common::SequenceNumber key_seq() const override;
  virtual RecordStatus key_status() const override;
  virtual bool for_unique_check() const override;

 private:
  DBIter* db_iter_;
  util::Arena arena_;
};

// Generate the arena wrapped iterator class.
extern ArenaWrappedDBIter* NewArenaWrappedDbIterator(
    util::Env* env, const common::ReadOptions& read_options,
    const common::ImmutableCFOptions& cf_options,
    const util::Comparator* user_key_comparator,
    const common::SequenceNumber& sequence,
    uint64_t max_sequential_skip_in_iterations, uint64_t version_number,
    storage::ExtentSpaceManager* space_manager = nullptr);
}
}  // namespace xengine
