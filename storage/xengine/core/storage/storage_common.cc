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

/* clang-format off */
#include "storage_common.h"

namespace xengine
{
namespace storage
{
DEFINE_SERIALIZATION(ExtentId, file_number, offset);
DEFINE_TO_STRING(ExtentId, KV(file_number), KV(offset));
const int32_t LayerPosition::INVISIBLE_LAYER_INDEX = INT32_MAX;
const int32_t LayerPosition::NEW_GENERATE_LAYER_INDEX = INT32_MAX - 1;
DEFINE_COMPACTIPLE_SERIALIZATION(LayerPosition, level_, layer_index_);


EstimateCostStats::EstimateCostStats()
    : subtable_id_(0),
      cost_size_(0),
      total_extent_cnt_(0),
      total_open_extent_cnt_(0),
      recalc_last_extent_(false)
{
}

EstimateCostStats::~EstimateCostStats()
{}

void EstimateCostStats::reset()
{
  subtable_id_ = 0;
  cost_size_ = 0;
  total_extent_cnt_ = 0;
  total_open_extent_cnt_ = 0;
  recalc_last_extent_ = false;
}

DEFINE_TO_STRING(EstimateCostStats, KV_(subtable_id), KV_(cost_size), KV_(total_extent_cnt),
    KV_(total_open_extent_cnt), KV_(recalc_last_extent));

} //namespace storage
} //namespace xengine

/* clang-format on */
