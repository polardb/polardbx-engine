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

#include <memory>
#include <string>

#include "db/dbformat.h"
#include "xengine/flush_block_policy.h"
#include "xengine/table.h"

namespace xengine {

namespace util {
class WritableFileWriter;
class RandomAccessFileReader;
}

namespace db {
struct MiniTables;
}

namespace common {
class DBOptions;
class Status;
class ColumnFamilyOptions;
}

namespace table {
extern const std::string kHashIndexPrefixesBlock;
extern const std::string kHashIndexPrefixesMetadataBlock;
extern const std::string kPropTrue;
extern const std::string kPropFalse;

class TableFactory;
struct BlockBasedTableOptions;
class TableReaderOptions;
class TableReader;
class TableBuilder;
class TableBuilderOptions;

class ExtentBasedTableFactory : public TableFactory {
 public:
  explicit ExtentBasedTableFactory(
      const BlockBasedTableOptions& table_options = BlockBasedTableOptions());

  ~ExtentBasedTableFactory() {}

  const char* Name() const override { return "ExtentBasedTable"; }

  common::Status NewTableReader(
      const TableReaderOptions& table_reader_options,
      util::RandomAccessFileReader *file,
      uint64_t file_size,
      TableReader *&table_reader,
      bool prefetch_index_and_filter_in_cache = true,
      memory::SimpleAllocator *arena = nullptr) const override;

  TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      uint32_t column_family_id, util::WritableFileWriter* file) const override;

  TableBuilder* NewTableBuilderExt(
      const TableBuilderOptions& table_builder_options,
      uint32_t column_family_id, db::MiniTables* mtables) const override;

  // Sanitizes the specified DB Options.
  common::Status SanitizeOptions(
      const common::DBOptions& db_opts,
      const common::ColumnFamilyOptions& cf_opts) const override;

  std::string GetPrintableTableOptions() const override;

  const BlockBasedTableOptions& table_options() const;

  void* GetOptions() override { return &table_options_; }

 private:
  BlockBasedTableOptions table_options_;
};

}  // namespace table
}  // namespace xengine
