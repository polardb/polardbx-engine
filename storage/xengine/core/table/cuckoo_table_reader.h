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
#ifndef ROCKSDB_LITE
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "db/dbformat.h"
#include "options/cf_options.h"
#include "table/table_reader.h"
#include "util/file_reader_writer.h"
#include "xengine/env.h"
#include "xengine/options.h"

namespace xengine {

namespace util {
class Arena;
}

namespace table {

class TableReader;
class InternalIterator;

class CuckooTableReader : public TableReader {
 public:
  CuckooTableReader(const common::ImmutableCFOptions& ioptions,
                    std::unique_ptr<util::RandomAccessFileReader>&& file,
                    uint64_t file_size, const util::Comparator* user_comparator,
                    uint64_t (*get_slice_hash)(const common::Slice&, uint32_t,
                                               uint64_t));
  ~CuckooTableReader() {}

  std::shared_ptr<const TableProperties> GetTableProperties() const override {
    return table_props_;
  }

  common::Status status() const { return status_; }

  common::Status Get(const common::ReadOptions& read_options,
                     const common::Slice& key, GetContext* get_context,
                     bool skip_filters = false) override;

  InternalIterator* NewIterator(const common::ReadOptions&,
                                memory::SimpleAllocator* arena = nullptr,
                                bool skip_filters = false,
                                const uint64_t scan_add_blocks_limit = 0) override;
  void Prepare(const common::Slice& target) override;

  // Report an approximation of how much memory has been used.
  size_t ApproximateMemoryUsage() const override;

  // Following methods are not implemented for Cuckoo Table Reader
  uint64_t ApproximateOffsetOf(const common::Slice& key) override { return 0; }
  void SetupForCompaction() override {}
  // End of methods not implemented.

 private:
  friend class CuckooTableIterator;
  void LoadAllKeys(
      std::vector<std::pair<common::Slice, uint32_t>>* key_to_bucket_id);
  std::unique_ptr<util::RandomAccessFileReader> file_;
  common::Slice file_data_;
  bool is_last_level_;
  bool identity_as_first_hash_;
  bool use_module_hash_;
  std::shared_ptr<const TableProperties> table_props_;
  common::Status status_;
  uint32_t num_hash_func_;
  std::string unused_key_;
  uint32_t key_length_;
  uint32_t user_key_length_;
  uint32_t value_length_;
  uint32_t bucket_length_;
  uint32_t cuckoo_block_size_;
  uint32_t cuckoo_block_bytes_minus_one_;
  uint64_t table_size_;
  const util::Comparator* ucomp_;
  uint64_t (*get_slice_hash_)(const common::Slice& s, uint32_t index,
                              uint64_t max_num_buckets);
};

}  // namespace table
}  // namespace xengine
#endif  // ROCKSDB_LITE
