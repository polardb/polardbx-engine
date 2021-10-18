/*
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once
#include <cstdint>
#include <string>
#include <vector>

namespace xengine{
namespace fpga{
struct MultiWayBlockMergeStats {
  MultiWayBlockMergeStats() { Reset(); }
  void Reset();

  // the elapsed time in micro of this compaction.
  uint64_t elapsed_micros_;
  // the elapsed time in micro of compaction on FPGA.
  uint64_t elapsed_micros_fpga_;
  // the number of compaction input records.
  uint64_t num_input_records_;
  // the number of compaction output records.
  uint64_t num_output_records_;
  // number of records being replaced by newer record associated with same key.
  // this could be a new value or a deletion entry for that key so this field
  // sums up all updated and deleted keys
  uint64_t num_records_replaced_;
  // number of delete entry of input records
  uint64_t num_records_delete_;
  // the sum of the uncompressed input keys in bytes.
  uint64_t total_input_raw_key_bytes_;
  // the sum of the uncompressed input values in bytes.
  uint64_t total_input_raw_value_bytes_;
  // vector of each output block's largest seq no.

  std::vector<uint64_t> largest_seq_no_blocks_;
  // vector of each output block's smallest seq no.
  std::vector<uint64_t> smallest_seq_no_blocks_;
  // vector of each output block's first key.
  std::vector<std::string> first_keys_;
  // vector of each output block's last key.
  std::vector<std::string> last_keys_;
  // vector of each output block's data size.
  std::vector<size_t> data_size_blocks_;
  // vector of each output block's key size.
  std::vector<size_t> key_size_blocks_;
  // vector of each output block's value size.
  std::vector<size_t> value_size_blocks_;
  // vector of each output block's number of rows;
  std::vector<size_t> rows_blocks_;
  // vector of each output block's number of entry whose type = kTypeValue
  std::vector<size_t> entry_put_blocks_;
  // vector of each output block's number of entry whose type = kTypeDeletion
  std::vector<size_t> entry_delete_blocks_;
  // vector of each output block's number of entry whose type = kTypeMerge
  std::vector<size_t> entry_merge_blocks_;
};
}
}
