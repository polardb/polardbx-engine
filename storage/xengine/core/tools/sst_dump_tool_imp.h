// Portions Copyright (c) 2020, Alibaba Group Holding Limited
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once
#ifndef ROCKSDB_LITE

#include "xengine/sst_dump_tool.h"

#include <memory>
#include <string>
#include "db/dbformat.h"
#include "options/cf_options.h"
#include "util/file_reader_writer.h"

namespace xengine {
namespace tools {

class SstFileReader {
 public:
  explicit SstFileReader(const std::string& file_name, bool verify_checksum,
                         bool output_hex, size_t extent_offset = 1,
                         common::Options options = common::Options());

  common::Status ReadSequential(bool print_kv, uint64_t read_num, bool has_from,
                                const std::string& from_key, bool has_to,
                                const std::string& to_key,
                                bool use_from_as_prefix = false);

  common::Status ReadTableProperties(
      std::shared_ptr<const table::TableProperties>* table_properties);
  uint64_t GetReadNumber() { return read_num_; }
  table::TableProperties* GetInitTableProperties() {
    return table_properties_.get();
  }

  common::Status DumpTable(const std::string& out_filename);
  common::Status getStatus() { return init_result_; }

  int ShowAllCompressionSizes(size_t block_size);
 
  table::TableReader *get_table_reader() const { return table_reader_.get(); }

 private:
  // Get the TableReader implementation for the sst file
  common::Status GetTableReader(const std::string& file_path,
                                size_t extent_offset);
  common::Status ReadTableProperties(uint64_t table_magic_number,
                                     util::RandomAccessFileReader* file,
                                     uint64_t file_size);

  uint64_t CalculateCompressedTableSize(
      const table::TableBuilderOptions& tb_options, size_t block_size);

  common::Status SetTableOptionsByMagicNumber(uint64_t table_magic_number);
  common::Status SetOldTableOptions();

  // Helper function to call the factory with settings specific to the
  // factory implementation
  common::Status NewTableReader(
      const common::ImmutableCFOptions& ioptions,
      const util::EnvOptions& soptions,
      const db::InternalKeyComparator& internal_comparator, uint64_t file_size,
      table::TableReader *&table_reader);

  std::string file_name_;
  uint64_t read_num_;
  bool verify_checksum_;
  bool output_hex_;
  util::EnvOptions soptions_;

  // options_ and internal_comparator_ will also be used in
  // ReadSequential internally (specifically, seek-related operations)
  common::Options options_;

  common::Status init_result_;
  std::unique_ptr<table::TableReader, memory::ptr_destruct_delete<table::TableReader>> table_reader_;
  std::unique_ptr<util::RandomAccessFileReader, memory::ptr_destruct_delete<util::RandomAccessFileReader>> file_;

  const common::ImmutableCFOptions ioptions_;
  db::InternalKeyComparator internal_comparator_;
  std::unique_ptr<table::TableProperties> table_properties_;
};
}  // namespace tools
}  // namespace xengine

#endif  // ROCKSDB_LITE
