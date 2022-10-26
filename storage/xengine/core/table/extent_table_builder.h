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
#include <limits>
#include <string>
#include <utility>
#include <vector>

#include "table/meta_blocks.h"
#include "table/table_builder.h"
#include "xengine/flush_block_policy.h"
#include "xengine/options.h"
#include "xengine/status.h"
#include "db/version_edit.h"
#include "table/extent_table_reader.h"

namespace xengine {

namespace common {
class ImmutableCFOptions;
enum CompressionType;
class CompressionOptions;
class Slice;
class Status;
}

namespace db {
class InternalKeyComparator;
class IntTblPropCollectorFactory;
class InternalKeySliceTransform;
struct MiniTables;
enum ValueType;
struct BlockStats;
}

namespace util {
class WritableBuffer;
}

namespace table {

class BlockBasedTableOptions;
class TableProperties;
class BlockHandle;
class PropertyBlockBuilder;
class Footer;
class IndexBuilder;

extern const uint64_t kExtentBasedTableMagicNumber;

class ExtentBasedTableBuilder : public TableBuilder {
 public:
  // Create a builder that will store the contents of the table into multiple
  //    mini tables
  // @param compression_dict Data for presetting the compression library's
  //    dictionary, or nullptr.
  ExtentBasedTableBuilder(
      const common::ImmutableCFOptions& ioptions,
      const BlockBasedTableOptions& table_options,
      const db::InternalKeyComparator& internal_comparator,
      const std::vector<std::unique_ptr<db::IntTblPropCollectorFactory>>*
          int_tbl_prop_collector_factories,
      uint32_t column_family_id,
      db::MiniTables* mtables,
      const common::CompressionType compression_type,
      const common::CompressionOptions& compression_opts,
      const std::string* compression_dict, const bool skip_filters,
      const std::string& column_family_name, const storage::LayerPosition &layer_position, bool is_flush = false);

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~ExtentBasedTableBuilder();

  int init();
  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  int Add(const common::Slice& key, const common::Slice& value) override;
  static int update_row_cache(const uint32_t cf_id,
                              const common::Slice& key,
                              const common::Slice& value,
                              const common::ImmutableCFOptions &ioption);
  int set_in_cache_flag();
  bool SupportAddBlock() const override;

  int AddBlock(const common::Slice& block_contents,
               const common::Slice& block_stats,
               const common::Slice& last_key,
               const bool has_trailer = true) override;

  // Return non-ok iff some error has been detected.
  common::Status status() const override;

  // Finish building the table.  Stops using the file passed to the
  // constructor after this function returns.
  // REQUIRES: Finish(), Abandon() have not been called
  int Finish() override;

  // Indicate that the contents of this builder should be abandoned.  Stops
  // using the file passed to the constructor after this function returns.
  // If the caller is not going to call Finish(), it must call Abandon()
  // before destroying this builder.
  // REQUIRES: Finish(), Abandon() have not been called
  int Abandon() override;

  // Number of calls to Add() so far.
  uint64_t NumEntries() const override;

  // Size of the file generated so far.  If invoked after a successful
  // Finish() call, returns the size of the final generated file.
  uint64_t FileSize() const override;

  bool NeedCompact() const override;

  // Get table properties
  TableProperties GetTableProperties() const override;

#ifndef NDEBUG
  void TEST_inject_ignore_flush_data();
  bool TEST_is_ignore_flush_data();
#endif
 private:
  static const int64_t PAGE_SIZE = 4096;
  static const int64_t LARGE_OBJECT_SIZE = 1024 * 1536;
  static const int64_t EXTENT_SIZE = 2 * 1024 * 1024;
  // some compressions require more memory to deflate than the original size,
  // try to avoid realloc by a little bit larger buffer, and it can also be
  // used for writing extra footers in case of >2MB sst
  static const int64_t EXTRA_BUF_SIZE = 16 * 1024;
  static const int64_t DEF_BUF_SIZE = EXTENT_SIZE + EXTRA_BUF_SIZE;
  // estimated size of properties block,  uncertainty comes from __indexstats__,
  // and more properties from up layer, this number looks good enough right now.
  static const int64_t EST_PROP_BLOCK_SIZE = 1536;

  // Some compression libraries fail when the raw size is bigger than int. If
  // uncompressed size is bigger than kCompressionSizeLimit, don't compress it
  const uint64_t kCompressionSizeLimit = std::numeric_limits<int>::max();

  // No copying allowed
  ExtentBasedTableBuilder(const ExtentBasedTableBuilder&) = delete;
  void operator=(const ExtentBasedTableBuilder&) = delete;

  int plain_add(const common::Slice& key, const common::Slice& value);
  // Compress and write block content to the file.
  // skip_data is for the cases where the data has already been written to the
  // destination, but use this function to append the block tailer.
  int write_block(const common::Slice& block_contents, BlockHandle* handle,
                  bool is_data_block, bool is_index_block);
  // Directly write data to the file.
  int write_raw_block(const common::Slice& data, common::CompressionType type,
                      BlockHandle* handle, bool is_data_block,
                      bool is_index_block, bool skip_data = false);
  int BuildPropertyBlock(PropertyBlockBuilder& builder);
  int write_sst(Footer& footer);

  int init_one_sst();
  int finish_one_sst();

  // Advanced operation: flush any buffered key/value pairs to file.
  // Can be used to ensure that two adjacent entries never live in
  // the same data block.  Most clients should not need to use this method.
  // REQUIRES: Finish(), Abandon() have not been called
  int flush_data_block();
  int flush_data_block(const common::Slice& block_contents);

  // estimate the size other than data/index block, including:
  //   props block, compression dict block, meta index block and footer
  int sst_meta_size() const;
  int sst_size_after_row(const common::Slice& key, const common::Slice& value,
                         const db::BlockStats& block_stats);
  int sst_size_after_block(const common::Slice& block_contents,
                           const common::Slice& key,
                           const db::BlockStats& block_stats);

  int update_block_stats(const common::Slice& key,
                         const common::Slice& value,
                         const db::ParsedInternalKey &ikey);
  void sync_up_block_stats(const db::BlockStats& block_stats);

  int insert_block_in_cache(const common::Slice& block_contents,
                            const common::CompressionType type,
                            const BlockHandle* handle,
                            bool is_data_block,
                            bool is_index_block);
  int build_large_object_extent_meta(const common::Slice &lob_key,
                                     const storage::ExtentId &extent_id,
                                     const int64_t data_size,
                                     storage::ExtentMeta &extent_meta);
  int write_extent_meta(const storage::ExtentMeta &extent_meta, bool is_large_object_extent);

 private:
  bool is_inited_;
  struct Rep;
  Rep* rep_;
  int status_;

  const common::ImmutableCFOptions ioptions_;
  const BlockBasedTableOptions table_options_;
  const db::InternalKeyComparator& internal_comparator_;
  const std::vector<std::unique_ptr<db::IntTblPropCollectorFactory>>*
      int_tbl_prop_collector_factories_;
  uint32_t column_family_id_;
  db::MiniTables* mtables_;
  const common::CompressionType compression_type_;
  const common::CompressionOptions compression_opts_;
  const std::string* compression_dict_;
  const std::string& column_family_name_;
  storage::LayerPosition output_position_;
  db::InternalKeySliceTransform internal_prefix_transform_;

  int num_entries_;  // entries in the previous mini tables of this table
  int offset_;       // similar as num_entries_, but offset

  db::BlockStats block_stats_;
  int meta_size_;  // size of sst contents except data/index block

  util::WritableBuffer sst_buf_;
  util::WritableBuffer block_buf_;
  util::WritableBuffer index_buf_;
  storage::ExtentId not_flushed_normal_extent_id_;
  storage::ExtentId not_flushed_lob_extent_id_;
  util::autovector<storage::ExtentId> flushed_lob_extent_ids_;
  bool is_flush_;
#ifndef NDEBUG
  bool test_ignore_flush_data_;
#endif
};

// One extent TableBuilder usually generates multiple sst files, every file is
// likely to consume one extent (2MB), but with exceptions.
// Rep here means sst file exactly.
struct ExtentBasedTableBuilder::Rep {
  uint64_t offset = 0;
  uint64_t last_offset = 0;

  BlockBuilder data_block;
  std::unique_ptr<IndexBuilder, memory::ptr_destruct_delete<IndexBuilder>> index_builder;
  std::unique_ptr<FlushBlockPolicy> flush_block_policy;

  db::FileMetaData meta;
  TableProperties props;
  std::vector<std::unique_ptr<db::IntTblPropCollector>> table_properties_collectors;
  std::string last_key;
  bool collectors_support_add_block = true;
  BlockHandle pending_handle;  // Handle to add to index block

  storage::WritableExtent extent_;
  char cache_key_prefix[ExtentBasedTable::kMaxCacheKeyPrefixSize];
  size_t cache_key_prefix_size;
  char compressed_cache_key_prefix[ExtentBasedTable::kMaxCacheKeyPrefixSize];
  size_t compressed_cache_key_prefix_size;

  Rep(const common::ImmutableCFOptions& ioptions,
      const BlockBasedTableOptions& table_options,
      const db::InternalKeyComparator& icomparator,
      const std::vector<std::unique_ptr<db::IntTblPropCollectorFactory>>* int_tbl_prop_collector_factories,
      uint32_t column_family_id,
      const db::InternalKeySliceTransform* internal_prefix_transform,
      util::WritableBuffer* block_buf, util::WritableBuffer* index_buf);
};

}  // namespace table
}  // namespace xengine
