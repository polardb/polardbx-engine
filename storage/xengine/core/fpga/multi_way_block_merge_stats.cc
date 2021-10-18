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
#include "multi_way_block_merge_stats.h"

using namespace xengine;

namespace xengine{
namespace fpga{
void MultiWayBlockMergeStats::Reset() {

  elapsed_micros_ = 0;
  elapsed_micros_fpga_ = 0;
  num_input_records_ = 0;
  num_output_records_ = 0;
  num_records_replaced_ = 0;
  num_records_delete_ = 0;
  total_input_raw_key_bytes_ = 0;
  total_input_raw_value_bytes_ = 0;

  largest_seq_no_blocks_.clear();
  smallest_seq_no_blocks_.clear();
  first_keys_.clear();
  last_keys_.clear();
  data_size_blocks_.clear();
  key_size_blocks_.clear();
  value_size_blocks_.clear();
  rows_blocks_.clear();
  entry_put_blocks_.clear();
  entry_delete_blocks_.clear();
  entry_merge_blocks_.clear();
}

}
}
