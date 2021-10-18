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

#include "task_type.h"

namespace xengine
{
namespace db
{
bool is_valid_task_type(int64_t type)
{
  return type > INVALID_TYPE_TASK && type < MAX_TYPE_TASK;
}
bool is_internal_level_task(int64_t type)
{
  return FLUSH_TASK == type ||
    FLUSH_LEVEL1_TASK == type ||
    INTRA_COMPACTION_TASK == type ||
    SPLIT_TASK == type ||
    MAJOR_SELF_COMPACTION_TASK == type ||
    MAJOR_AUTO_SELF_COMPACTION_TASK == type ||
    DELETE_MAJOR_SELF_TASK == type ||
    BUILD_BASE_INDEX_TASK == type ||
    MINOR_AUTO_SELF_TASK == type;
}

bool is_cross_level_task(int64_t type)
{
  return MINOR_COMPACTION_TASK == type ||
    MINOR_DELETE_COMPACTION_TASK == type ||
    STREAM_COMPACTION_TASK == type ||
    MAJOR_COMPACTION_TASK == type ||
    MAJOR_DELETE_COMPACTION_TASK == type ||
    MANUAL_MAJOR_TASK == type;
}

bool is_batch_install_task(int64_t type)
{
  return MINOR_COMPACTION_TASK == type ||
    MINOR_DELETE_COMPACTION_TASK == type ||
    STREAM_COMPACTION_TASK == type ||
    INTRA_COMPACTION_TASK == type ||
    MINOR_AUTO_SELF_TASK == type;
}

bool is_minor_task(int64_t type)
{
  return MINOR_COMPACTION_TASK == type ||
    MINOR_DELETE_COMPACTION_TASK == type ||
    STREAM_COMPACTION_TASK == type ||
    INTRA_COMPACTION_TASK == type;
}

bool is_major_task(int64_t type)
{
  return MAJOR_COMPACTION_TASK == type ||
    MAJOR_DELETE_COMPACTION_TASK == type ||
    MAJOR_SELF_COMPACTION_TASK == type ||
    MAJOR_AUTO_SELF_COMPACTION_TASK == type ||
    DELETE_MAJOR_SELF_TASK == type ||
    MANUAL_MAJOR_TASK == type;
}

bool is_major_self_task(int64_t type)
{
  return MAJOR_SELF_COMPACTION_TASK == type ||
      MAJOR_AUTO_SELF_COMPACTION_TASK == type ||
      DELETE_MAJOR_SELF_TASK == type;

}

bool is_major_task_with_L1(int64_t type)
{
  return MAJOR_COMPACTION_TASK == type ||
      MAJOR_DELETE_COMPACTION_TASK == type ||
      MANUAL_MAJOR_TASK == type;
}

bool is_shrink_extent_space_task(int64_t type) 
{
  return SHRINK_EXTENT_SPACE_TASK == type;
}

bool is_flush_task(int64_t type) {
  return FLUSH_TASK == type || FLUSH_LEVEL1_TASK == type;
}

bool is_urgent_task(int64_t type)
{
  return TaskType::FLUSH_LEVEL1_TASK == type ||
    TaskType::FLUSH_TASK == type ||
    TaskType::INTRA_COMPACTION_TASK == type || 
    MINOR_DELETE_COMPACTION_TASK == type ;
}

bool touch_level0_task(int64_t type)
{
  return FLUSH_TASK == type ||
    INTRA_COMPACTION_TASK == type ||
    MINOR_COMPACTION_TASK == type ||
    STREAM_COMPACTION_TASK == type ||
    MINOR_DELETE_COMPACTION_TASK == type ||
    MANUAL_FULL_AMOUNT_TASK == type ;
}

bool touch_level1_task(int64_t type)
{
  return MINOR_COMPACTION_TASK == type ||
    SPLIT_TASK == type ||
    MAJOR_COMPACTION_TASK == type ||
    STREAM_COMPACTION_TASK == type ||
    MINOR_DELETE_COMPACTION_TASK == type ||
    MAJOR_DELETE_COMPACTION_TASK == type ||
    MANUAL_MAJOR_TASK == type ||
    MANUAL_FULL_AMOUNT_TASK == type ||
    FLUSH_LEVEL1_TASK == type ||
    MINOR_AUTO_SELF_TASK == type;
}

bool touch_level2_task(int64_t type)
{
  return MAJOR_COMPACTION_TASK == type ||
    MAJOR_DELETE_COMPACTION_TASK == type ||
    MANUAL_MAJOR_TASK == type ||
    MANUAL_FULL_AMOUNT_TASK == type ||
    MAJOR_SELF_COMPACTION_TASK == type ||
    MAJOR_AUTO_SELF_COMPACTION_TASK == type ||
    DELETE_MAJOR_SELF_TASK == type;
}

std::string get_task_type_name(const int64_t type) {
  if (is_valid_task_type(type)) {
    return TASK_TYPE_NAME[type];
  }
  return TASK_TYPE_NAME[0];
}

}
} // namespace xengine
