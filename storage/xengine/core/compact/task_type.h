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

#ifndef XENGINE_INCLUDE_TASK_TYPE_H_
#define XENGINE_INCLUDE_TASK_TYPE_H_
#include <stdint.h>
#include <string>

namespace xengine
{
namespace db
{
enum TaskType
{
  INVALID_TYPE_TASK = 0,
  FLUSH_TASK = 1,
  // There are many kinds of Compaction types. 
  // Each type has specified input and output level
  INTRA_COMPACTION_TASK = 2,                  // L0 layers intra compaction
  MINOR_COMPACTION_TASK = 3,                  // L0 compaction to L1 (FPGA)
  SPLIT_TASK = 4,                             // L1 split extent to L1
  MAJOR_COMPACTION_TASK = 5,                  // L1 compaction to L2
  MAJOR_SELF_COMPACTION_TASK = 6,             // L2 compaction to L2
  BUILD_BASE_INDEX_TASK = 7,                 //build base index, append data to level2
  //
  // Note we should resever these orders to be backwords compatible.   
  STREAM_COMPACTION_TASK = 8,                 // L0 compaction to L1
  MINOR_DELETE_COMPACTION_TASK = 9,           // L0 compaction to L1 (Delete triggered)
  MAJOR_DELETE_COMPACTION_TASK = 10,           // L1 compaction to L2 (Delete triggered)
  MAJOR_AUTO_SELF_COMPACTION_TASK = 11,       // L2 compaction to L2 (Auto triggered with time period)
  MANUAL_MAJOR_TASK = 12,                     // L1 compaction to L2 (manual trigger, all extents to L2)
  MANUAL_FULL_AMOUNT_TASK = 13,               // compact all extents to level2
  SHRINK_EXTENT_SPACE_TASK = 14,              // reclaim the free extent space 
  DELETE_MAJOR_SELF_TASK = 15,                // compact all delete records in level2
  FLUSH_LEVEL1_TASK = 16,                     // flush memtables to level1
  MINOR_AUTO_SELF_TASK = 17,                  // level1 compaction to level1
  DUMP_TASK = 18,                             // dump memtable to M0
  SWITCH_M02L0_TASK = 19,                     // switch M0 to L0
  MAX_TYPE_TASK
};

#define ALL_TASK_VALUE_FOR_CANCEL 0xFFFFFFFF
const std::string TASK_TYPE_NAME[] = {
  "INVALID TASK",           // 0
  "FLUSH TASK",             // 1
  "INTRA COMPACTION",       // 2
  "MINOR COMPACTION",       // 3
  "SPLIT COMPACTION",       // 4
  "MAJOR COMPACTION",       // 5
  "MAJOR SELF COMPACTION",  // 6
  "BUILD INDEX",            // 7
  "STREAM COMPACTION",      // 8
  "MINOR DELETE COMPACTION",// 9
  "MAJOR DELETE COMPACTION",    // 10
  "MAJOR AUTO SELF COMPACTION", // 11
  "MANUAL MAJOR COMPACTION",    // 12
  "MANUAL FULL COMPACTION",     // 13
  "SHRINK EXTENT SPACE_TASK",   // 14
  "DELETE MAJOR SELF_TASK",     // 15
  "FLUSH LEVEL1 TASK",          // 16
  "MINOR AUTO SELF TASK",       // 17
  "DUMP TASK",                  // 18
  "SWITCH M02L0 TASK",          // 19
  ""                            // MAX_TYPE_TASK
};

bool is_valid_task_type(int64_t type);
bool is_internal_level_task(int64_t type);

bool is_cross_level_task(int64_t type);

// Intra and Minor compaction need to install only once
bool is_batch_install_task(int64_t type);

// Intra and Minor related
bool is_minor_task(int64_t type);
// Major releated
bool is_major_task(int64_t type);
// major self
bool is_major_self_task(int64_t type);

bool is_major_task_with_L1(int64_t type);

bool is_shrink_extent_space_task(int64_t type);
// cann't wait, need to do quickly
bool is_urgent_task(int64_t type);
// touched level0's extents
bool touch_level0_task(int64_t type);
// touched level1's extents
bool touch_level1_task(int64_t type);
// touched level2's extents
bool touch_level2_task(int64_t type);
bool is_flush_task(int64_t type);
std::string get_task_type_name(const int64_t type);

} // namespace db
} // namespace xengine

#endif
