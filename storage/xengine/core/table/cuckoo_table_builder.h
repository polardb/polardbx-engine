// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#ifndef ROCKSDB_LITE
#include <stdint.h>
#include <limits>
#include <string>
#include <utility>
#include <vector>
#include "port/port.h"
#include "table/table_builder.h"
#include "util/autovector.h"
#include "xengine/status.h"
#include "xengine/table.h"
#include "xengine/table_properties.h"

namespace xengine {
namespace table {

class CuckooTableBuilder : public TableBuilder {
 public:
  CuckooTableBuilder(
      util::WritableFileWriter* file, double max_hash_table_ratio,
      uint32_t max_num_hash_func, uint32_t max_search_depth,
      const util::Comparator* user_comparator, uint32_t cuckoo_block_size,
      bool use_module_hash, bool identity_as_first_hash,
      uint64_t (*get_slice_hash)(const common::Slice&, uint32_t, uint64_t),
      uint32_t column_family_id, const std::string& column_family_name);

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~CuckooTableBuilder() {}

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  int Add(const common::Slice& key, const common::Slice& value) override;

  // Return non-ok iff some error has been detected.
  common::Status status() const override { return status_; }

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

  TableProperties GetTableProperties() const override { return properties_; }

 private:
  struct CuckooBucket {
    CuckooBucket() : vector_idx(kMaxVectorIdx), make_space_for_key_call_id(0) {}
    uint32_t vector_idx;
    // This number will not exceed kvs_.size() + max_num_hash_func_.
    // We assume number of items is <= 2^32.
    uint32_t make_space_for_key_call_id;
  };
  static const uint32_t kMaxVectorIdx = port::kMaxInt32;

  bool MakeSpaceForKey(const util::autovector<uint64_t>& hash_vals,
                       const uint32_t call_id,
                       std::vector<CuckooBucket>* buckets, uint64_t* bucket_id);
  common::Status MakeHashTable(std::vector<CuckooBucket>* buckets);

  inline bool IsDeletedKey(uint64_t idx) const;
  inline common::Slice GetKey(uint64_t idx) const;
  inline common::Slice GetUserKey(uint64_t idx) const;
  inline common::Slice GetValue(uint64_t idx) const;

  uint32_t num_hash_func_;
  util::WritableFileWriter* file_;
  const double max_hash_table_ratio_;
  const uint32_t max_num_hash_func_;
  const uint32_t max_search_depth_;
  const uint32_t cuckoo_block_size_;
  uint64_t hash_table_size_;
  bool is_last_level_file_;
  bool has_seen_first_key_;
  bool has_seen_first_value_;
  uint64_t key_size_;
  uint64_t value_size_;
  // A list of fixed-size key-value pairs concatenating into a string.
  // Use GetKey(), GetUserKey(), and GetValue() to retrieve a specific
  // key / value given an index
  std::string kvs_;
  std::string deleted_keys_;
  // Number of key-value pairs stored in kvs_ + number of deleted keys
  uint64_t num_entries_;
  // Number of keys that contain value (non-deletion op)
  uint64_t num_values_;
  common::Status status_;
  TableProperties properties_;
  const util::Comparator* ucomp_;
  bool use_module_hash_;
  bool identity_as_first_hash_;
  uint64_t (*get_slice_hash_)(const common::Slice& s, uint32_t index,
                              uint64_t max_num_buckets);
  std::string largest_user_key_ = "";
  std::string smallest_user_key_ = "";

  bool closed_;  // Either Finish() or Abandon() has been called.

  // No copying allowed
  CuckooTableBuilder(const CuckooTableBuilder&) = delete;
  void operator=(const CuckooTableBuilder&) = delete;
};

}  // namespace table
}  // namespace xengine

#endif  // ROCKSDB_LITE
