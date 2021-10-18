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

#ifndef UTIL_MEMORY_STAT_H
#define UTIL_MEMORY_STAT_H

#include <map>
#include <string>
#include <unordered_set>
#include <vector>

#include "xengine/cache.h"
#include "xengine/db.h"

namespace xengine {

namespace util {
class MemoryStat {
 public:
  enum MemoryStatType : int {
    kActiveMemTableTotalNumber = 0,
    kActiveMemTableTotalMemoryAllocated = 1,
    kActiveMemTableTotalMemoryUsed = 2,
    kUnflushedImmTableTotalNumber = 3,
    kUnflushedImmTableTotalMemoryAllocated = 4,
    kUnflushedImmTableTotalMemoryUsed = 5,
    kTableReaderTotalNumber = 6,
    kTableReaderTotalMemoryUsed = 7,
    kBlockCacheTotalPinnedMemory = 8,
    kBlockCacheTotalMemoryUsed = 9,
    kActiveWALTotalNumber = 10,
    kActiveWALTotalBufferSize = 11,
    kDBTotalMemoryAllocated = 12,
    kNumberStatTypes = 13,
  };
  static common::Status GetApproximateMemoryStatByType(
      const std::vector<db::DB*>& dbs,
      std::map<MemoryStatType, uint64_t>& stat_by_type);
};
}  // namespace util
}  // namespace xengine
#endif
