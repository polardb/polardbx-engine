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
#include <vector>
#include <set>
#include "xengine/slice.h"
#include "db/dbformat.h"
#include "table/block_builder.h"
#include "table/block.h"
#include "comp_stats.h"

using namespace xengine;
using namespace common;
using namespace db;
using namespace table;
using namespace util;

namespace xengine {
namespace fpga{

static const size_t RESTART_INTERVAL = 16;
static const size_t SIZE_PER_BLOCK = 16*1024;

class MultiWayBlockMerge
{
public:
  explicit MultiWayBlockMerge(size_t level_type,
                              uint64_t min_ref_seq_no,
                              char **input_blocks,
                              size_t **input_blocks_size,
                              size_t *num_input_blocks,
                              size_t num_ways,
                              char *output_blocks,
                              size_t *output_blocks_size,
                              size_t &num_output_blocks,
                              CompStats& multi_way_block_merge_stats)
                : level_type_(level_type),
                  min_ref_seq_no_(min_ref_seq_no),
                  input_blocks_(input_blocks),
                  input_blocks_size_(input_blocks_size),
                  num_input_blocks_(num_input_blocks),
                  num_ways_(num_ways),
                  output_blocks_(output_blocks),
                  output_blocks_size_(output_blocks_size),
                  num_output_blocks_(num_output_blocks),
                  multi_way_block_merge_stats_(multi_way_block_merge_stats){
                    //num_ways_ = 0;
                    //for (size_t i = 0;i < num_ways; ++i) {
                    //  if (num_input_blocks[i] == 0) {
                    //    break;
                    //  }
                    //  num_ways_++;
                    //}
                    //fprintf(stderr, "ajust num_way param=%ld actur=%ld\n",
                    //                num_ways, num_ways_);
                  };
  ~MultiWayBlockMerge(){}

  void MultiBlockMerge();

private:
  size_t level_type_;
  uint64_t min_ref_seq_no_;
  char **input_blocks_;
  size_t **input_blocks_size_;
  size_t *num_input_blocks_;
  size_t num_ways_;
  char *output_blocks_;
  size_t *output_blocks_size_;
  size_t &num_output_blocks_;
  CompStats &multi_way_block_merge_stats_;


private:

  std::string SingleBlockMerge(
    Slice *block_1,
    Slice *block_2);


  std::string MultiBlockMergeKWay(Slice ***blocks);

  void PartBlockMerge(
    std::vector<size_t> index_set,
    bool left_part,
    ParsedInternalKey *pkeys,
    std::string *keys,
    std::string *values,
    //Slice* keys,
    //Slice* values,
    BlockBuilder *builder);

  //void PutRes(std::string res);

  //bool VerifyResEquality(std::string res);

  // get next KV
  // return the KV pair with the max seqno
  bool GetNextKV(
    BlockIter *iter,
    std::string &key,
    std::string &value);

  bool GetNextKV(
    BlockIter *iter,
    Slice& key,
    Slice& value);

  void GetInputBlocksStats();
};
}
}

