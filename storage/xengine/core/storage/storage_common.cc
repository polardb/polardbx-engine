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
} //namespace storage
} //namespace xengine
