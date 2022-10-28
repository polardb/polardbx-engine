// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/extent_table_factory.h"

#include <stdint.h>
#include <memory>
#include <string>

#include "port/port.h"
#include "table/extent_table_builder.h"
#include "table/extent_table_reader.h"
#include "table/format.h"
#include "xengine/cache.h"
#include "xengine/flush_block_policy.h"

using namespace xengine;
using namespace common;
using namespace cache;
using namespace util;
using namespace db;

namespace xengine {
namespace table {

ExtentBasedTableFactory::ExtentBasedTableFactory(
    const BlockBasedTableOptions& _table_options)
    : table_options_(_table_options) {
  if (table_options_.flush_block_policy_factory == nullptr) {
    table_options_.flush_block_policy_factory.reset(
        new FlushBlockBySizePolicyFactory());
  }
  if (table_options_.no_block_cache) {
    table_options_.block_cache.reset();
  } else if (table_options_.block_cache == nullptr) {
    table_options_.block_cache = NewLRUCache(8 << 20);
    table_options_.block_cache.get()->set_mod_id(memory::ModId::kDefaultBlockCache);
  }
  if (table_options_.block_size_deviation < 0 ||
      table_options_.block_size_deviation > 100) {
    table_options_.block_size_deviation = 0;
  }
  if (table_options_.block_restart_interval < 1) {
    table_options_.block_restart_interval = 1;
  }
  if (table_options_.index_block_restart_interval < 1) {
    table_options_.index_block_restart_interval = 1;
  }
}

Status ExtentBasedTableFactory::NewTableReader(
    const TableReaderOptions& table_reader_options,
    RandomAccessFileReader *file,
    uint64_t file_size,
    TableReader *&table_reader,
    bool prefetch_index_and_filter_in_cache,
    memory::SimpleAllocator *arena) const {
  return ExtentBasedTable::Open(
      table_reader_options.ioptions, table_reader_options.env_options,
      table_options_, table_reader_options.internal_comparator, file,
      file_size, table_reader, table_reader_options.fd_,
      table_reader_options.file_read_hist_, prefetch_index_and_filter_in_cache,
      table_reader_options.skip_filters, table_reader_options.level, arena);
}

TableBuilder* ExtentBasedTableFactory::NewTableBuilder(
    const TableBuilderOptions& table_builder_options, uint32_t column_family_id,
    WritableFileWriter* file) const {
  assert(0);
  return nullptr;
}

TableBuilder* ExtentBasedTableFactory::NewTableBuilderExt(
    const TableBuilderOptions& table_builder_options, uint32_t column_family_id,
    MiniTables* mtables) const {
  int ret = Status::kOk;
  ExtentBasedTableBuilder *tmp_table_builder = nullptr;
  ExtentBasedTableBuilder *table_builder = nullptr;

  if (IS_NULL(tmp_table_builder = new ExtentBasedTableBuilder(
          table_builder_options.ioptions, table_options_,
          table_builder_options.internal_comparator,
          table_builder_options.int_tbl_prop_collector_factories, column_family_id,
          mtables, table_builder_options.compression_type,
          table_builder_options.compression_opts,
          table_builder_options.compression_dict,
          table_builder_options.skip_filters,
          table_builder_options.column_family_name,
          table_builder_options.output_position_,
          table_builder_options.is_flush))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for ExtentBasedTableBuilder", K(ret));
  } else if (FAILED(tmp_table_builder->init())) {
    XENGINE_LOG(WARN, "fail to init table builder", K(ret));
  } else {
    table_builder = tmp_table_builder;
  }

  return table_builder;
}

Status ExtentBasedTableFactory::SanitizeOptions(
    const DBOptions& db_opts, const ColumnFamilyOptions& cf_opts) const {
  if (table_options_.index_type == BlockBasedTableOptions::kHashSearch) {
    return Status::InvalidArgument(
        "Hash index is specified for "
        "OptimizedBlockBasedTable");
  }
  if (cf_opts.compression_opts.max_dict_bytes > 0) {
    return Status::InvalidArgument(
        "max_dict_bytes is larget than 0 for "
        "OptimizedBlockBasedTable");
  }
  if (table_options_.index_type == BlockBasedTableOptions::kHashSearch &&
      cf_opts.prefix_extractor == nullptr) {
    return Status::InvalidArgument(
        "Hash index is specified for block-based "
        "table, but prefix_extractor is not given");
  }
  if (table_options_.cache_index_and_filter_blocks &&
      table_options_.no_block_cache) {
    return Status::InvalidArgument(
        "Enable cache_index_and_filter_blocks, "
        ", but block cache is disabled");
  }
  if (table_options_.pin_l0_filter_and_index_blocks_in_cache &&
      table_options_.no_block_cache) {
    return Status::InvalidArgument(
        "Enable pin_l0_filter_and_index_blocks_in_cache, "
        ", but block cache is disabled");
  }
  if (!BlockBasedTableSupportedVersion(table_options_.format_version)) {
    return Status::InvalidArgument(
        "Unsupported ExtentBasedTable format_version. Please check "
        "include/xengine/table.h for more info");
  }
  return Status::OK();
}

std::string ExtentBasedTableFactory::GetPrintableTableOptions() const {
  std::string ret;
  ret.reserve(20000);
  const int kBufferSize = 200;
  char buffer[kBufferSize];

  snprintf(buffer, kBufferSize, "  flush_block_policy_factory: %s (%p)\n",
           table_options_.flush_block_policy_factory->Name(),
           static_cast<void*>(table_options_.flush_block_policy_factory.get()));
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  cache_index_and_filter_blocks: %d\n",
           table_options_.cache_index_and_filter_blocks);
  ret.append(buffer);
  snprintf(buffer, kBufferSize,
           "  cache_index_and_filter_blocks_with_high_priority: %d\n",
           table_options_.cache_index_and_filter_blocks_with_high_priority);
  ret.append(buffer);
  snprintf(buffer, kBufferSize,
           "  pin_l0_filter_and_index_blocks_in_cache: %d\n",
           table_options_.pin_l0_filter_and_index_blocks_in_cache);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  index_type: %d\n",
           table_options_.index_type);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  hash_index_allow_collision: %d\n",
           table_options_.hash_index_allow_collision);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  checksum: %d\n", table_options_.checksum);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  no_block_cache: %d\n",
           table_options_.no_block_cache);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  block_cache: %p\n",
           static_cast<void*>(table_options_.block_cache.get()));
  ret.append(buffer);
  if (table_options_.block_cache) {
    const char* block_cache_name = table_options_.block_cache->Name();
    if (block_cache_name != nullptr) {
      snprintf(buffer, kBufferSize, "  block_cache_name: %s\n",
               block_cache_name);
      ret.append(buffer);
    }
    ret.append("  block_cache_options:\n");
    ret.append(table_options_.block_cache->GetPrintableOptions());
  }
  snprintf(buffer, kBufferSize, "  block_cache_compressed: %p\n",
           static_cast<void*>(table_options_.block_cache_compressed.get()));
  ret.append(buffer);
  if (table_options_.block_cache_compressed) {
    const char* block_cache_compressed_name =
        table_options_.block_cache_compressed->Name();
    if (block_cache_compressed_name != nullptr) {
      snprintf(buffer, kBufferSize, "  block_cache_name: %s\n",
               block_cache_compressed_name);
      ret.append(buffer);
    }
    ret.append("  block_cache_compressed_options:\n");
    ret.append(table_options_.block_cache_compressed->GetPrintableOptions());
  }
  snprintf(buffer, kBufferSize, "  persistent_cache: %p\n",
           static_cast<void*>(table_options_.persistent_cache.get()));
  ret.append(buffer);
  if (table_options_.persistent_cache) {
    snprintf(buffer, kBufferSize, "  persistent_cache_options:\n");
    ret.append(buffer);
    ret.append(table_options_.persistent_cache->GetPrintableOptions());
  }
  snprintf(buffer, kBufferSize, "  block_size: %" ROCKSDB_PRIszt "\n",
           table_options_.block_size);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  block_size_deviation: %d\n",
           table_options_.block_size_deviation);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  block_restart_interval: %d\n",
           table_options_.block_restart_interval);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  index_block_restart_interval: %d\n",
           table_options_.index_block_restart_interval);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  filter_policy: %s\n",
           table_options_.filter_policy == nullptr
               ? "nullptr"
               : table_options_.filter_policy->Name());
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  whole_key_filtering: %d\n",
           table_options_.whole_key_filtering);
  ret.append(buffer);
  snprintf(buffer, kBufferSize, "  format_version: %d\n",
           table_options_.format_version);
  ret.append(buffer);
  return ret;
}

const BlockBasedTableOptions& ExtentBasedTableFactory::table_options() const {
  return table_options_;
}

TableFactory* NewExtentBasedTableFactory(
    const BlockBasedTableOptions& _table_options) {
  return new ExtentBasedTableFactory(_table_options);
}

}  // namespace table
}  // namespace xengine
