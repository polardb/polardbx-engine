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

#include "util/memory_stat.h"
#include "db/db_impl.h"
#include "db/version_set.h"

using namespace xengine::db;
using namespace xengine::common;

namespace xengine {
namespace util {

Status MemoryStat::GetApproximateMemoryStatByType(
    const std::vector<DB*>& dbs,
    std::map<MemoryStat::MemoryStatType, uint64_t>& stat_by_type) {
  Status status = Status::OK();

  stat_by_type.clear();
  // MemTable Memory usage
  for (auto* db : dbs) {
    uint64_t usage = 0;
    if (db->GetAggregatedIntProperty(DB::Properties::kActiveMemTableTotalNumber,
                                     &usage)) {
      stat_by_type[MemoryStat::kActiveMemTableTotalNumber] += usage;
    }
    if (db->GetAggregatedIntProperty(
            DB::Properties::kActiveMemTableTotalMemoryAllocated, &usage)) {
      stat_by_type[MemoryStat::kActiveMemTableTotalMemoryAllocated] += usage;
      stat_by_type[MemoryStat::kDBTotalMemoryAllocated] += usage;
    }
    if (db->GetAggregatedIntProperty(
            DB::Properties::kActiveMemTableTotalMemoryUsed, &usage)) {
      stat_by_type[MemoryStat::kActiveMemTableTotalMemoryUsed] += usage;
    }
    if (db->GetAggregatedIntProperty(
            DB::Properties::kUnflushedImmTableTotalNumber, &usage)) {
      stat_by_type[MemoryStat::kUnflushedImmTableTotalNumber] += usage;
    }
    if (db->GetAggregatedIntProperty(
            DB::Properties::kUnflushedImmTableTotalMemoryAllocated, &usage)) {
      stat_by_type[MemoryStat::kUnflushedImmTableTotalMemoryAllocated] += usage;
      stat_by_type[MemoryStat::kDBTotalMemoryAllocated] += usage;
    }
    if (db->GetAggregatedIntProperty(
            DB::Properties::kUnflushedImmTableTotalMemoryUsed, &usage)) {
      stat_by_type[MemoryStat::kUnflushedImmTableTotalMemoryUsed] += usage;
    }
    ColumnFamilyHandle* handle = (db)->DefaultColumnFamily();
    if (nullptr != handle &&
        db->GetIntProperty(handle, DB::Properties::kTableReaderTotalNumber,
                           &usage)) {
      stat_by_type[MemoryStat::kTableReaderTotalNumber] += usage;
    } else {
      stat_by_type[MemoryStat::kTableReaderTotalNumber] += 0;
    }

    if (nullptr != handle &&
        db->GetIntProperty(handle, DB::Properties::kTableReaderTotalMemoryUsed,
                           &usage)) {
      stat_by_type[MemoryStat::kTableReaderTotalMemoryUsed] += usage;
      stat_by_type[MemoryStat::kDBTotalMemoryAllocated] += usage;
    }

    if (nullptr != handle &&
        db->GetIntProperty(handle, DB::Properties::kBlockCacheTotalPinnedMemory,
                           &usage)) {
      stat_by_type[MemoryStat::kBlockCacheTotalPinnedMemory] += usage;
    }
    if (nullptr != handle &&
        db->GetIntProperty(handle, DB::Properties::kBlockCacheTotalMemoryUsed,
                           &usage)) {
      stat_by_type[MemoryStat::kBlockCacheTotalMemoryUsed] += usage;
      stat_by_type[MemoryStat::kDBTotalMemoryAllocated] += usage;
    }
  }
  return status;
}
}  // namespace util
}  // namespace xengine
