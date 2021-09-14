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

#include "comp_aclr/comp_stats.h"

namespace xengine {
namespace fpga {

  /**
   * FPGACompactionTask represents a task
   * fed to FPGA CU which contains
   * 1. input blocks stream buffer pointer
   * 2. output buffer pointer which is preallocated
   * 3. group ref to which the task belongs
   */
  struct FPGACompactionTask{
    // the group to which this task belongs
    void* group_;
    size_t level_type_;
    uint64_t min_ref_seq_no_;
    char **input_blocks_;
    size_t **input_blocks_size_;
    size_t *num_input_blocks_;
    size_t num_ways_;
    char *output_blocks_;
    size_t *output_blocks_size_;
    size_t num_output_blocks_;
    CompStats *stats_;

    FPGACompactionTask(void *group,
                       size_t level_type,
                       uint64_t min_ref_seq_no,
                       char **input_blocks,
                       size_t **input_blocks_size,
                       size_t *num_input_blocks,
                       size_t num_ways,
                       char *output_blocks,
                       size_t *output_blocks_size,
                       size_t num_output_blocks,
                       CompStats *stats)
        : group_(group),
          level_type_(level_type),
          min_ref_seq_no_(min_ref_seq_no),
          input_blocks_(input_blocks),
          input_blocks_size_(input_blocks_size),
          num_input_blocks_(num_input_blocks),
          num_ways_(num_ways),
          output_blocks_(output_blocks),
          output_blocks_size_(output_blocks_size),
          num_output_blocks_(num_output_blocks),
          stats_(stats){}

    ~FPGACompactionTask() {
      for (size_t i = 0;i < num_ways_; ++ i) {
        delete[] input_blocks_[i];
        delete[] input_blocks_size_[i];
      }
      delete[] input_blocks_;
      delete[] input_blocks_size_;

      delete num_input_blocks_;
      delete output_blocks_;
      delete output_blocks_size_;
    }
  };

}
}
