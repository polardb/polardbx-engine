// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <stddef.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <vector>
#include "db/dbformat.h"
#include "table/filter_block.h"
#include "memory/base_malloc.h"
#include "util/hash.h"
#include "xengine/options.h"
#include "xengine/slice.h"
#include "xengine/slice_transform.h"

namespace xengine {
namespace table {

class FilterPolicy;
class FilterBitsBuilder;
class FilterBitsReader;

// A FullFilterBlockBuilder is used to construct a full filter for a
// particular Table.  It generates a single string which is stored as
// a special block in the Table.
// The format of full filter block is:
// +----------------------------------------------------------------+
// |              full filter for all keys in sst file              |
// +----------------------------------------------------------------+
// The full filter can be very large. At the end of it, we put
// num_probes: how many hash functions are used in bloom filter
//
class FullFilterBlockBuilder : public FilterBlockBuilder {
 public:
  explicit FullFilterBlockBuilder(
      const common::SliceTransform* prefix_extractor, bool whole_key_filtering,
      FilterBitsBuilder* filter_bits_builder);
  // bits_builder is created in filter_policy, it should be passed in here
  // directly. and be deleted here
  ~FullFilterBlockBuilder() {}

  virtual bool IsBlockBased() override { return false; }
  virtual void StartBlock(uint64_t block_offset) override {}
  virtual void Add(const common::Slice& key) override;
  using FilterBlockBuilder::Finish;
  virtual common::Slice Finish(const BlockHandle& tmp,
                               common::Status* status) override;
  using FilterBlockBuilder::Finish;
  virtual common::Slice Finish(std::unique_ptr<char[],
                               memory::ptr_delete<char>>* buf) override;

 protected:
  virtual void AddKey(const common::Slice& key);
  std::unique_ptr<FilterBitsBuilder> filter_bits_builder_;

 private:
  // important: all of these might point to invalid addresses
  // at the time of destruction of this filter block. destructor
  // should NOT dereference them.
  const common::SliceTransform* prefix_extractor_;
  bool whole_key_filtering_;

  uint32_t num_added_;
  std::unique_ptr<char[], memory::ptr_delete<char>> filter_data_;

  void AddPrefix(const common::Slice& key);

  // No copying allowed
  FullFilterBlockBuilder(const FullFilterBlockBuilder&);
  void operator=(const FullFilterBlockBuilder&);
};

// A FilterBlockReader is used to parse filter from SST table.
// KeyMayMatch and PrefixMayMatch would trigger filter checking
class FullFilterBlockReader : public FilterBlockReader {
 public:
  // REQUIRES: "contents" and filter_bits_reader must stay live
  // while *this is live.
  explicit FullFilterBlockReader(const common::SliceTransform* prefix_extractor,
                                 bool _whole_key_filtering,
                                 char *data,
                                 size_t data_size,
                                 FilterBitsReader* filter_bits_reader,
                                 monitor::Statistics* stats);
  explicit FullFilterBlockReader(const common::SliceTransform* prefix_extractor,
                                 bool whole_key_filtering,
                                 const common::Slice& contents,
                                 FilterBitsReader* filter_bits_reader,
                                 monitor::Statistics* statistics);
  explicit FullFilterBlockReader(const common::SliceTransform* prefix_extractor,
                                 bool whole_key_filtering,
                                 BlockContents&& contents,
                                 FilterBitsReader* filter_bits_reader,
                                 monitor::Statistics* statistics);

  // bits_reader is created in filter_policy, it should be passed in here
  // directly. and be deleted here
  ~FullFilterBlockReader() {}

  virtual bool IsBlockBased() override { return false; }
  virtual bool KeyMayMatch(
      const common::Slice& key, uint64_t block_offset = kNotValid,
      const bool no_io = false,
      const common::Slice* const const_ikey_ptr = nullptr) override;
  virtual bool PrefixMayMatch(
      const common::Slice& prefix, uint64_t block_offset = kNotValid,
      const bool no_io = false,
      const common::Slice* const const_ikey_ptr = nullptr) override;
  virtual size_t ApproximateMemoryUsage() const override;
  void BindData();

 private:
  const common::SliceTransform* prefix_extractor_;
  common::Slice contents_;
  std::unique_ptr<FilterBitsReader> filter_bits_reader_;
  BlockContents block_contents_;
  std::unique_ptr<char[], memory::ptr_delete<char>> filter_data_;

  // No copying allowed
  FullFilterBlockReader(const FullFilterBlockReader&);
  bool MayMatch(const common::Slice& entry);
  void operator=(const FullFilterBlockReader&);
};

}  // namespace table
}  // namespace xengine
