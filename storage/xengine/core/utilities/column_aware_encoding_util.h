// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once
#ifndef ROCKSDB_LITE

#include <string>
#include <vector>
#include "db/dbformat.h"
#include "options/cf_options.h"
#include "table/block_based_table_reader.h"
#include "xengine/env.h"
#include "xengine/listener.h"
#include "xengine/options.h"
#include "xengine/status.h"

namespace xengine {
namespace util {

struct ColDeclaration;
struct KVPairColDeclarations;

class ColumnAwareEncodingReader {
 public:
  explicit ColumnAwareEncodingReader(const std::string& file_name);

  void GetKVPairsFromDataBlocks(
      std::vector<table::KVPairBlock>* kv_pair_blocks);

  void EncodeBlocksToRowFormat(
      WritableFile* out_file, common::CompressionType compression_type,
      const std::vector<table::KVPairBlock>& kv_pair_blocks,
      std::vector<std::string>* blocks);

  void DecodeBlocksFromRowFormat(WritableFile* out_file,
                                 const std::vector<std::string>* blocks);

  void DumpDataColumns(const std::string& filename,
                       const KVPairColDeclarations& kvp_col_declarations,
                       const std::vector<table::KVPairBlock>& kv_pair_blocks);

  common::Status EncodeBlocks(
      const KVPairColDeclarations& kvp_col_declarations, WritableFile* out_file,
      common::CompressionType compression_type,
      const std::vector<table::KVPairBlock>& kv_pair_blocks,
      std::vector<std::string>* blocks, bool print_column_stat);

  void DecodeBlocks(const KVPairColDeclarations& kvp_col_declarations,
                    WritableFile* out_file,
                    const std::vector<std::string>* blocks);

  static void GetColDeclarationsPrimary(
      std::vector<ColDeclaration>** key_col_declarations,
      std::vector<ColDeclaration>** value_col_declarations,
      ColDeclaration** value_checksum_declaration);

  static void GetColDeclarationsSecondary(
      std::vector<ColDeclaration>** key_col_declarations,
      std::vector<ColDeclaration>** value_col_declarations,
      ColDeclaration** value_checksum_declaration);

 private:
  // Init the TableReader for the sst file
  void InitTableReader(const std::string& file_path);

  std::string file_name_;
  EnvOptions soptions_;

  common::Options options_;

  common::Status init_result_;
  std::unique_ptr<table::BlockBasedTable> table_reader_;
  std::unique_ptr<RandomAccessFileReader> file_;

  const common::ImmutableCFOptions ioptions_;
  db::InternalKeyComparator internal_comparator_;
  std::unique_ptr<table::TableProperties> table_properties_;
};

}  //  namespace util
}  //  namespace xengine

#endif  // ROCKSDB_LITE
