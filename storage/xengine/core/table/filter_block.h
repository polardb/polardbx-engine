// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A filter block is stored near the end of a Table file.  It contains
// filters (e.g., bloom filters) for all data blocks in the table combined
// into a single filter block.
//
// It is a base class for BlockBasedFilter and FullFilter.
// These two are both used in BlockBasedTable. The first one contain filter
// For a part of keys in sst file, the second contain filter for all keys
// in sst file.

#pragma once

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <vector>
#include "format.h"
#include "util/hash.h"
#include "memory/base_malloc.h"
#include "xengine/options.h"
#include "xengine/slice.h"
#include "xengine/slice_transform.h"
#include "xengine/table.h"

namespace xengine {
namespace table {

const uint64_t kNotValid = ULLONG_MAX;
class FilterPolicy;

// A FilterBlockBuilder is used to construct all of the filters for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
//
// The sequence of calls to FilterBlockBuilder must match the regexp:
//      (StartBlock Add*)* Finish
//
// BlockBased/Full FilterBlock would be called in the same way.
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder() {}
  virtual ~FilterBlockBuilder() {}

  virtual bool IsBlockBased() = 0;  // If is blockbased filter
  virtual void StartBlock(uint64_t block_offset) = 0;  // Start new block filter
  virtual void Add(
      const common::Slice& key) = 0;  // Add a key to current filter
  common::Slice Finish() {            // Generate Filter
    const BlockHandle empty_handle;
    common::Status dont_care_status;
    auto ret = Finish(empty_handle, &dont_care_status);
    assert(dont_care_status.ok());
    return ret;
  }
  virtual common::Slice Finish(const BlockHandle& tmp,
                               common::Status* status) = 0;
  // Generate Filter with data buffer directly moved.
  virtual common::Slice Finish(std::unique_ptr<char[],
                               memory::ptr_delete<char>>* buf) {
    return Finish();
  }

 private:
  // No copying allowed
  FilterBlockBuilder(const FilterBlockBuilder&);
  void operator=(const FilterBlockBuilder&);
};

// A FilterBlockReader is used to parse filter from SST table.
// KeyMayMatch and PrefixMayMatch would trigger filter checking
//
// BlockBased/Full FilterBlock would be called in the same way.
class FilterBlockReader {
 public:
  explicit FilterBlockReader()
      : whole_key_filtering_(true), size_(0), statistics_(nullptr) {}
  explicit FilterBlockReader(size_t s, monitor::Statistics* stats,
                             bool _whole_key_filtering)
      : whole_key_filtering_(_whole_key_filtering),
        size_(s),
        statistics_(stats) {}
  virtual ~FilterBlockReader() {}

  virtual bool IsBlockBased() = 0;  // If is blockbased filter
  /**
   * If no_io is set, then it returns true if it cannot answer the query without
   * reading data from disk. This is used in PartitionedFilterBlockReader to
   * avoid reading partitions that are not in block cache already
   *
   * Normally filters are built on only the user keys and the InternalKey is not
   * needed for a query. The index in PartitionedFilterBlockReader however is
   * built upon InternalKey and must be provided via const_ikey_ptr when running
   * queries.
   */
  virtual bool KeyMayMatch(
      const common::Slice& key, uint64_t block_offset = kNotValid,
      const bool no_io = false,
      const common::Slice* const const_ikey_ptr = nullptr) = 0;
  /**
   * no_io and const_ikey_ptr here means the same as in KeyMayMatch
   */
  virtual bool PrefixMayMatch(
      const common::Slice& prefix, uint64_t block_offset = kNotValid,
      const bool no_io = false,
      const common::Slice* const const_ikey_ptr = nullptr) = 0;
  virtual int range_may_contain(const db::KeyRange &range, bool &may_contain) { return common::Status::kNotSupported; }
  virtual size_t ApproximateMemoryUsage() const = 0;
  virtual size_t size() const { return size_; }
  virtual monitor::Statistics* statistics() const { return statistics_; }

  bool whole_key_filtering() const { return whole_key_filtering_; }

  int GetLevel() const { return level_; }
  void SetLevel(int level) { level_ = level; }

  // convert this object to a human readable form
  virtual std::string ToString() const {
    std::string error_msg("Unsupported filter \n");
    return error_msg;
  }

 protected:
  bool whole_key_filtering_;

 private:
  // No copying allowed
  FilterBlockReader(const FilterBlockReader&);
  void operator=(const FilterBlockReader&);
  size_t size_;
  monitor::Statistics* statistics_;
  int level_ = -1;
};

}  // namespace table
}  // namespace xengine
