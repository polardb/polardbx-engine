// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include <list>
#include <string>
#include <unordered_map>
#include "db/dbformat.h"
#include "xengine/options.h"
#include "xengine/slice.h"
#include "xengine/slice_transform.h"

#include "table/block.h"
#include "table/block_based_table_reader.h"
#include "table/full_filter_block.h"
#include "table/index_builder.h"
#include "util/autovector.h"
#include "memory/base_malloc.h"

namespace xengine {
namespace table {

class PartitionedFilterBlockBuilder : public FullFilterBlockBuilder {
 public:
  explicit PartitionedFilterBlockBuilder(
      const common::SliceTransform* prefix_extractor, bool whole_key_filtering,
      FilterBitsBuilder* filter_bits_builder, int index_block_restart_interval,
      PartitionedIndexBuilder* const p_index_builder);

  virtual ~PartitionedFilterBlockBuilder();

  void AddKey(const common::Slice& key) override;
  using FullFilterBlockBuilder::Finish;
  virtual common::Slice Finish(const BlockHandle& last_partition_block_handle,
                               common::Status* status) override;

 private:
  // Filter data
  BlockBuilder index_on_filter_block_builder_;  // top-level index builder
  struct FilterEntry {
    std::string key;
    common::Slice filter;
  };
  std::list<FilterEntry> filters;  // list of partitioned indexes and their keys
  std::unique_ptr<IndexBuilder> value;
  std::vector<std::unique_ptr<char[], memory::ptr_delete<char>>> filter_gc;
  bool finishing_filters =
      false;  // true if Finish is called once but not complete yet.
  // The policy of when cut a filter block and Finish it
  void MaybeCutAFilterBlock();
  PartitionedIndexBuilder* const p_index_builder_;
};

class PartitionedFilterBlockReader : public FilterBlockReader {
 public:
  explicit PartitionedFilterBlockReader(
      const common::SliceTransform* prefix_extractor, bool whole_key_filtering,
      BlockContents&& contents, FilterBitsReader* filter_bits_reader,
      monitor::Statistics* stats, const util::Comparator& comparator,
      const BlockBasedTable* table);
  virtual ~PartitionedFilterBlockReader();

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

 private:
  common::Slice GetFilterPartitionHandle(const common::Slice& entry);
  BlockBasedTable::CachableEntry<FilterBlockReader> GetFilterPartition(
      common::Slice* handle, const bool no_io, bool* cached);

  const common::SliceTransform* prefix_extractor_;
  std::unique_ptr<Block> idx_on_fltr_blk_;
  const util::Comparator& comparator_;
  const BlockBasedTable* table_;
  std::unordered_map<uint64_t, FilterBlockReader*> filter_cache_;
  util::autovector<cache::Cache::Handle*> handle_list_;
  port::RWMutex mu_;
};

}  // namespace table
}  // namespace xengine
