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
#include "compaction_tasks_picker.h"
#include "compaction_job.h"
#include "db/snapshot_impl.h"
#include "logger/logger.h"
#include "options/cf_options.h"
#include "storage/multi_version_extent_meta_layer.h"
#include "storage/storage_manager.h"
#include "xengine/xengine_constants.h"
#include "xengine/env.h"

using namespace xengine;
using namespace storage;
using namespace util;
using namespace memory;
namespace xengine {
namespace db {

CompactionTasksPicker::CompactionTasksPicker(
    common::MutableCFOptions &mutable_cf_options,
    const uint64_t compaction_type,
    const uint32_t cf_id,
    const bool dynamic_trigger_adjust)
    : last_pick_task_(),
      mc_task_type_(MAX_TYPE_TASK),
      l1_usage_percent_(100),
      l2_usage_percent_(100),
      delete_extents_size_(0),
      l0_num_val_(0),
      l1_num_val_(0),
      l2_num_val_(0),
      pending_priority_l0_layer_sequence_(-1),
      current_priority_l0_layer_sequence_(-1),
      delete_triggered_compaction_(false),
      last_check_time_(0),
      cf_id_(cf_id),
      mcf_options_(mutable_cf_options),
      minor_merge_limit_(0),
      minor_compaction_type_(compaction_type),
      dynamic_trigger_adjust_(dynamic_trigger_adjust)
{
  last_check_time_ = Env::Default()->NowMicros();
}

DEFINE_TO_STRING(CompactionTasksPicker::TaskInfo, KV_((int)task_type), KV_(priority_value),
    KV_(extents_size), KV_(output_level), KV_(l1_pick_pos), KV_(need_split));

// If level_compaction_dynamic_level_bytes is set, we would adjust level 1
// trigger dynamic between [level0_file_trigger, total_count / (multiplier + 1)];
// Level0 trigger is too small to adjust.
int64_t CompactionTasksPicker::get_level1_file_num_compaction_trigger(
    const int64_t l0_num,
    const int64_t l1_num,
    const int64_t l2_num) const {
  int64_t level1_file_trigger = mcf_options_.level1_extents_major_compaction_trigger;
  if (dynamic_trigger_adjust_) {
    level1_file_trigger = std::min(
        (l0_num + l1_num + l2_num ) / (mcf_options_.target_file_size_multiplier + 1),
        level1_file_trigger);
    level1_file_trigger = std::max(level1_file_trigger, (int64_t)mcf_options_.level0_file_num_compaction_trigger);
  }
  return level1_file_trigger;
}

int64_t CompactionTasksPicker::get_level0_file_num_compaction_trigger(
    const int64_t l0_num,
    const int64_t l1_num,
    const int64_t l2_num) const {
  int64_t level0_file_trigger = mcf_options_.level0_file_num_compaction_trigger;
  if (dynamic_trigger_adjust_) {
    int64_t multiplier = mcf_options_.target_file_size_multiplier > 0 ? mcf_options_.target_file_size_multiplier : 10;
    level0_file_trigger = std::min(
        get_level1_file_num_compaction_trigger(l0_num, l1_num, l2_num) / (mcf_options_.target_file_size_multiplier),
        level0_file_trigger);
    level0_file_trigger = std::max(level0_file_trigger, (int64_t)mcf_options_.level0_file_num_compaction_trigger / 5);
  }
  return level0_file_trigger;
}

TaskType CompactionTasksPicker::pick_one_manual_task(const SnapshotImpl* snapshot,
                                                     const storage::StorageManager &storage_manager) {
  TaskType type = TaskType::MAX_TYPE_TASK;
  if (IS_NULL(snapshot)
      || IS_NULL(snapshot->get_extent_layer_version(0))
      || IS_NULL(snapshot->get_extent_layer_version(1))
      || IS_NULL(snapshot->get_extent_layer_version(2))) {
    return type;
  }
  int64_t level0_num = snapshot->get_extent_layer_version(0)->get_total_normal_extent_count();
  int64_t level1_num = snapshot->get_extent_layer_version(1)->get_total_normal_extent_count();;
  int64_t level2_num = snapshot->get_extent_layer_version(2)->get_total_normal_extent_count();;
  switch (mc_task_type_) {
    case FLUSH_TASK: {
      COMPACTION_LOG(INFO, "MANUAL:not support flush task");
      break;
    }
    case INTRA_COMPACTION_TASK: {
      if (level0_num <= 0) {
        COMPACTION_LOG(INFO, "MANUAL:level0 has no extent, no need do intra compaction");
      } else {
        type = INTRA_COMPACTION_TASK;
      }
      break;
    }
    case MINOR_COMPACTION_TASK: {
      if (level0_num <= 0) {
        COMPACTION_LOG(INFO, "MANUAL:level0 has no extent, no need do minor compaction");
      } else if (1 != minor_compaction_type_) {
        COMPACTION_LOG(INFO, "MANUAL:can't do minor compaction", K(minor_compaction_type_));
      } else {
        type = MINOR_COMPACTION_TASK;
      }
      break;
    }
    case SPLIT_TASK: {
      COMPACTION_LOG(INFO, "MANUAL:not support split task");
      break;
    }
    case MAJOR_COMPACTION_TASK: {
      if (mcf_options_.bottommost_level < 2) {                            
        COMPACTION_LOG(INFO, "MANUAL:bottomest is smaller than 2, can't do major compaction", K(mcf_options_.bottommost_level));
      } else if (level1_num <= 0) {
        COMPACTION_LOG(INFO, "MANUAL:level1 has no extent, no need do major compaction");
      } else {
        type = MAJOR_COMPACTION_TASK;
      }
      break;
    }
    case MAJOR_SELF_COMPACTION_TASK: {
      if (mcf_options_.bottommost_level < 2) {
        COMPACTION_LOG(INFO, "MANUAL:bottomest is smaller than 2, can't do major compaction", K(mcf_options_.bottommost_level));
      } else if (level2_num <= 0) {
        COMPACTION_LOG(INFO, "MANUAL:level2 has no extent, no need do major self compaction");
      } else {
        type = MAJOR_SELF_COMPACTION_TASK;
      }
      break;
    }
    case BUILD_BASE_INDEX_TASK: {
      COMPACTION_LOG(INFO, "MANUAL:not supprt build index task");
      break;
    }
    case STREAM_COMPACTION_TASK: {
      if (level0_num <= 0) {
        COMPACTION_LOG(INFO, "MANUAL:level0 has no extent, no need do stream compaction");
      } else if (0 != minor_compaction_type_) {
        COMPACTION_LOG(INFO, "MANUAL:can't do stream compaction", K(minor_compaction_type_));
      } else {
        type = STREAM_COMPACTION_TASK;
      }
      break;
    }
    case MINOR_DELETE_COMPACTION_TASK: {
      if (level0_num <= 0) {
        COMPACTION_LOG(INFO, "MANUAL:level0 has no extent, no need do minor delete compaction");
      } else {
        type = MINOR_DELETE_COMPACTION_TASK;
      }
      break;
    }
    case MAJOR_DELETE_COMPACTION_TASK: {
      if (mcf_options_.bottommost_level < 2) {
        COMPACTION_LOG(INFO, "MANUAL:bottomest is smaller than 2, can't do major compaction", K(mcf_options_.bottommost_level));
      } else if (level1_num <= 0) {
        COMPACTION_LOG(INFO, "MANUAL:level1 has no extent, no need do major delete compaction");
      } else {
        type = MAJOR_DELETE_COMPACTION_TASK;
      }
      break;
    }
    case MAJOR_AUTO_SELF_COMPACTION_TASK: {
      if (mcf_options_.bottommost_level < 2) {
        COMPACTION_LOG(INFO, "MANUAL:bottomest is smaller than 2, can't do major compaction", K(mcf_options_.bottommost_level));
      } else if (level2_num <= 0) {
        COMPACTION_LOG(INFO, "MANUAL:level2 has no extent, no need do auto_major_self compaction");
      } else {
        type = MAJOR_AUTO_SELF_COMPACTION_TASK;
      }
      break;
    }
    case DELETE_MAJOR_SELF_TASK: {
      int64_t usage_percent = 0;
      int64_t delete_size = 0;
      int ret = 0;
      if (mcf_options_.bottommost_level < 2) {
        COMPACTION_LOG(INFO, "MANUAL:bottomest is smaller than 2, can't do major compaction", K(mcf_options_.bottommost_level));
      } else if (FAILED(storage_manager.get_level_usage_percent(snapshot, 2, usage_percent, delete_size))) {
        COMPACTION_LOG(WARN, "failed to get level usage percent", K(usage_percent));
      } else if (delete_size <= 0) {
        COMPACTION_LOG(INFO, "MANUAL:level2 has no delete, no need do delete major self compaction");
      } else {
        type = DELETE_MAJOR_SELF_TASK;
      }
      break;
    }
    case MANUAL_MAJOR_TASK: {
      // will compact all level1's extents to level2,
      // is different from normal major task
      if (mcf_options_.bottommost_level < 2) {
        COMPACTION_LOG(INFO, "MANUAL:bottomest is smaller than 2, can't do major compaction", K(mcf_options_.bottommost_level));
      } else if (level1_num <= 0) {
        COMPACTION_LOG(INFO, "MANUAL:level1 has no extent, no need do manual major compaction");
      } else {
        type = MANUAL_MAJOR_TASK;
      }
      break;
    }
    case MANUAL_FULL_AMOUNT_TASK: {
      if (level0_num > 0) {
        if (1 == minor_compaction_type_) {
          type = TaskType::MINOR_COMPACTION_TASK;    // L0 -> L1
          COMPACTION_LOG(INFO, "MANUAL:full amount,will do minor compaction");
        } else {
          type = TaskType::STREAM_COMPACTION_TASK;    // L0 -> L1
        }
      } else if (level1_num > 0 && 2 ==  mcf_options_.bottommost_level) {
        type = MAJOR_COMPACTION_TASK;
        COMPACTION_LOG(INFO, "MANUAL:full amount,will do major compaction");
      } else {
        mc_task_type_ = MAX_TYPE_TASK;
      }
      break;
    }
    default: {
      COMPACTION_LOG(INFO, "MANUAL:invalid task type", K((int)mc_task_type_));
    }
  }
  COMPACTION_LOG(INFO, "MANUAL: will do manual task", K(TASK_TYPE_NAME[mc_task_type_]), K((int)mc_task_type_), K(cf_id_));
  if (MANUAL_FULL_AMOUNT_TASK != mc_task_type_) {
    mc_task_type_ = MAX_TYPE_TASK;
  }
  return type;
}

void CompactionTasksPicker::calc_normal_tasks(const int64_t level0_layer_val,
                                              const int64_t level0_num_val,
                                              const int64_t level0_size,
                                              const int64_t level1_num_val,
                                              const int64_t level1_num_trigger,
                                              const int64_t level2_num_val,
                                              util::autovector<TaskInfo> &task_list) {
  int64_t min_val = 0;
  int64_t level0_layer_trigger = mcf_options_.level0_layer_num_compaction_trigger;
  int64_t level0_num_trigger = mcf_options_.level0_file_num_compaction_trigger;
  // level has one layer less though has no extent
  if (level0_layer_trigger > 0 && level0_num_val >= level0_layer_trigger) {
    TaskInfo intraL0_task;
    intraL0_task.task_type_ = INTRA_COMPACTION_TASK;
    intraL0_task.priority_value_ = level0_layer_val * 1.0 / level0_layer_trigger + std::max(min_val, level0_layer_val - L0MaxLayer);
    intraL0_task.extents_size_ = level0_size;
    task_list.push_back(intraL0_task);
  }

  if (level0_num_trigger > 0 && level0_num_val >= level0_layer_trigger) {
    TaskInfo minor_task;
    minor_task.task_type_ = 1 == minor_compaction_type_ ? MINOR_COMPACTION_TASK : STREAM_COMPACTION_TASK;
    minor_task.priority_value_ = level0_num_val * 1.0 / level0_num_trigger + std::max(min_val, level0_num_val - L0MaxNum);
    minor_task.extents_size_ = level0_size + level1_num_val;
    task_list.push_back(minor_task);
  }

  if (level1_num_trigger > 0 && level1_num_val > 0 && 2 == mcf_options_.bottommost_level) {
    TaskInfo major_task;
    major_task.task_type_ = MAJOR_COMPACTION_TASK;
    major_task.priority_value_ = level1_num_val * 1.0 / level1_num_trigger - level2_num_val * 1.0 / level1_num_val / 10 + std::max(min_val, level1_num_val - L1MaxNum);
    major_task.extents_size_ = level1_num_val * 0.4 + std::min(level2_num_val, level1_num_val * 4);
    task_list.push_back(major_task);
  }
}

void CompactionTasksPicker::calc_delete_tasks(const int64_t level0_num_val,
                                              const int64_t oldest_layer_seq,
                                              const int64_t level0_size,
                                              const int64_t level1_num_val,
                                              const int64_t level1_delete_size,
                                              util::autovector<TaskInfo> &task_list) {
  // fetch a current_priority_l0_layer_seq_ from pending_priority_l0_layer_seq
  if (current_priority_l0_layer_sequence_ == -1) {
    current_priority_l0_layer_sequence_ = pending_priority_l0_layer_sequence_;
    pending_priority_l0_layer_sequence_ = -1;
  }
  if (level0_num_val > 0
      && oldest_layer_seq <= current_priority_l0_layer_sequence_) {
    TaskInfo delete_minor_task;
    delete_minor_task.task_type_ = MINOR_DELETE_COMPACTION_TASK;
    delete_minor_task.priority_value_ = L0MaxLayer;
    delete_minor_task.extents_size_ = level0_size + level1_num_val;
    task_list.push_back(delete_minor_task);
  } else if (2 == mcf_options_.bottommost_level && level1_num_val > 0) {
    TaskInfo delete_major_task;
    delete_major_task.task_type_ = MAJOR_DELETE_COMPACTION_TASK;
    delete_major_task.priority_value_ = L0MaxLayer * 10;
    delete_major_task.extents_size_ = level1_delete_size * 2;
    task_list.push_back(delete_major_task);
    delete_triggered_compaction_.store(false, std::memory_order_relaxed);
    current_priority_l0_layer_sequence_ = -1;
  } else {
    delete_triggered_compaction_.store(false, std::memory_order_relaxed);
    COMPACTION_LOG(INFO, "no need to build delete triggered job", K(cf_id_),
        K(current_priority_l0_layer_sequence_),
        K(pending_priority_l0_layer_sequence_),
        K(oldest_layer_seq),
        K(level0_num_val),
        K(level1_num_val));
  }
}

void CompactionTasksPicker::calc_auto_tasks(const int64_t level1_num_val,
                                            const int64_t level2_num_val,
                                            util::autovector<TaskInfo> &task_list) {
  int64_t min_val = 0;
  int64_t usage_percent_param = mcf_options_.level2_usage_percent;
  if (level1_num_val > 64) {
    TaskInfo minor_self_task;
    minor_self_task.task_type_ = MINOR_AUTO_SELF_TASK;
    minor_self_task.priority_value_ = (usage_percent_param - l1_usage_percent_ + 10) / 10.0 + std::max(min_val, minUsage - l1_usage_percent_);
    minor_self_task.extents_size_ = level1_num_val;
    task_list.push_back(minor_self_task);
  }

  if (level2_num_val > 128 && 2 == mcf_options_.bottommost_level) {
    TaskInfo major_self_task;
    major_self_task.task_type_ = MAJOR_AUTO_SELF_COMPACTION_TASK;
    major_self_task.priority_value_ = (usage_percent_param - l2_usage_percent_ + 10) / 10.0 + std::max(min_val, minUsage - l2_usage_percent_);
    major_self_task.extents_size_ = std::min(level2_num_val * CompactionJob::LEVEL_EXTENTS_PERCENT / 100, /*CompactionJob::EXTENTS_CNT_LIMIT*/ (int64_t)50000);
    task_list.push_back(major_self_task);
  }

  if (delete_extents_size_ >= 1 && 2 == mcf_options_.bottommost_level) {
    TaskInfo delete_major_self_task;
    delete_major_self_task.task_type_ = DELETE_MAJOR_SELF_TASK;
    delete_major_self_task.priority_value_ = delete_extents_size_ * 1.0 / DEL_BASE + 1;
    delete_major_self_task.extents_size_ = delete_extents_size_;
    task_list.push_back(delete_major_self_task);
  }
}

bool CompactionTasksPicker::is_normal_tree(const SnapshotImpl* snapshot) {
  bool bret = false;
  assert(snapshot);
  ExtentLayerVersion *l0_version = snapshot->get_extent_layer_version(0);
  ExtentLayerVersion *l1_version = snapshot->get_extent_layer_version(1);
  ExtentLayerVersion *l2_version = snapshot->get_extent_layer_version(2);
  assert(l0_version);
  assert(l1_version);
  assert(l2_version);
  if (IS_NULL(snapshot) || IS_NULL(l0_version) || IS_NULL(l1_version) || IS_NULL(l2_version)) {
    COMPACTION_LOG(ERROR, "snapshot is null", KP(l0_version), KP(l1_version), KP(l2_version));
  } else {
    util::autovector<TaskInfo> task_list;
    int64_t level0_layer_trigger = mcf_options_.level0_layer_num_compaction_trigger;
    int64_t level0_layer_val = l0_version->get_extent_layer_size();
    int64_t level0_num_val = l0_version->get_total_normal_extent_count();
    int64_t level1_num_val = l1_version->get_total_normal_extent_count();
    int64_t level2_num_val = l2_version->get_total_normal_extent_count();
    int64_t level0_num_trigger = get_level0_file_num_compaction_trigger(level0_num_val, level1_num_val, level2_num_val);
    int64_t level1_trigger = get_level1_file_num_compaction_trigger(level0_num_val, level1_num_val, level2_num_val);
    int64_t intraL0_extent_size = l0_version->get_layers_extent_size(16);

    calc_normal_tasks(level0_layer_val, level0_num_val, intraL0_extent_size, level1_num_val, level1_trigger,
        level2_num_val, task_list);

    if (task_list.size() > 0) {
      std::sort(task_list.begin(), task_list.end(), TaskInfo::greater);
      if (task_list.at(0).priority_value_ < 2) {
        bret = true;
      }
    } else {
      bret = true;
    }
  }
  return bret;

}

void CompactionTasksPicker::fill_task_info(const int64_t level0_trigger,
                                           const int64_t level1_trigger,
                                           const int64_t level1_num,
                                           TaskInfo &task_info) {
  task_info.l1_pick_pos_ = level1_trigger;
  if (TaskType::MANUAL_MAJOR_TASK == task_info.task_type_
      || TaskType::MAJOR_DELETE_COMPACTION_TASK == task_info.task_type_) {
    task_info.l1_pick_pos_ = 0;
  } else {
    int64_t level1_extent = level1_num;
    task_info.l1_pick_pos_ = std::min((int64_t)(0.4 * level1_extent), level1_trigger);
    if (level1_extent < level0_trigger) {
      task_info.l1_pick_pos_ = 0;
    }
  }
  bool has_split = false;
  if (level1_num - task_info.l1_pick_pos_ <= 16) {
    task_info.need_split_ = true;
  }
}

int CompactionTasksPicker::pick_one_task_idle(TaskInfo &pick_task) {
  int ret = common::Status::kOk;
  util::autovector<TaskInfo> task_list;
  pick_task.reset();
  if (l0_num_val_ > 0) {
    TaskInfo minor_task;
    minor_task.task_type_ = 1 == minor_compaction_type_ ? MINOR_COMPACTION_TASK : STREAM_COMPACTION_TASK;
    minor_task.priority_value_ = 1;
    minor_task.extents_size_ = l0_num_val_ + l1_num_val_;
    pick_task = minor_task;
  } else if (l1_num_val_ > 0 && (2 == mcf_options_.bottommost_level)) {
    TaskInfo major_task;
    major_task.task_type_ = MAJOR_COMPACTION_TASK;
    major_task.priority_value_ = 1;
    major_task.extents_size_ = l1_num_val_ + l2_num_val_;
    pick_task = major_task;
    pick_task.l1_pick_pos_ = 0;
  } else if (delete_extents_size_ > 0 && (2 == mcf_options_.bottommost_level)) {
    TaskInfo delete_task;
    delete_task.task_type_ = DELETE_MAJOR_SELF_TASK;
    delete_task.priority_value_ = 1;
    delete_task.extents_size_ = delete_extents_size_;
    pick_task = delete_task;
  } else if (l2_usage_percent_ < 90 && l2_num_val_ > 128
      && (2 == mcf_options_.bottommost_level)) {
    TaskInfo major_self_task;
    major_self_task.task_type_ = MAJOR_SELF_COMPACTION_TASK;
    major_self_task.priority_value_ = 1;
    major_self_task.extents_size_ = l2_num_val_;
    pick_task = major_self_task;
  }
  last_pick_task_ = pick_task;
  COMPACTION_LOG(INFO, "PICK_TASK: idle task info", "GetID()", cf_id_, K(l1_num_val_), K(l2_num_val_),
      K(l1_usage_percent_), K(l2_usage_percent_), K(delete_extents_size_), K((int)pick_task.task_type_));
  return ret;
}

int CompactionTasksPicker::pick_auto_task(TaskInfo &pick_task) {
  int ret = common::Status::kOk;
  util::autovector<TaskInfo> task_list;
  pick_task.reset();
  calc_auto_tasks(l1_num_val_, l2_num_val_, task_list);
  if (task_list.size() > 0) {
    std::sort(task_list.begin(), task_list.end(), TaskInfo::greater);
    pick_task = task_list.at(0);
    last_pick_task_ = pick_task;
    COMPACTION_LOG(INFO, "PICK_TASK: auto task info", "GetID()", cf_id_, K(l1_num_val_), K(l2_num_val_),
        K(l1_usage_percent_), K(l2_usage_percent_), K(delete_extents_size_), K((int)pick_task.task_type_));
  }

  int64_t cur_time = Env::Default()->NowMicros();
  last_check_time_ = cur_time;
  return ret;
}

int CompactionTasksPicker::pick_one_task(const SnapshotImpl* snapshot,
                                         const storage::StorageManager &storage_manager,
                                         TaskInfo &pick_task,
                                         CompactionPriority &priority) {
  int ret = common::Status::kOk;
  util::autovector<TaskInfo> task_list;
  pick_task.reset();
  ExtentLayerVersion *l0_version = snapshot->get_extent_layer_version(0);
  ExtentLayerVersion *l1_version = snapshot->get_extent_layer_version(1);
  ExtentLayerVersion *l2_version = snapshot->get_extent_layer_version(2);
  if (IS_NULL(snapshot)
      || IS_NULL(l0_version)
      || IS_NULL(l1_version)
      || IS_NULL(l2_version)) {
    ret = common::Status::kErrorUnexpected;
    COMPACTION_LOG(WARN, "snapshot is null", K(ret), KP(l0_version), KP(l1_version), KP(l2_version));
  } else {
    int64_t level0_layer_trigger = mcf_options_.level0_layer_num_compaction_trigger;
//    int64_t level0_num_trigger = mcf_options_.level0_file_num_compaction_trigger;

    int64_t level0_layer_val = l0_version->get_extent_layer_size();
    int64_t level0_num_val = l0_version->get_total_normal_extent_count();
    int64_t level1_num_val = l1_version->get_total_normal_extent_count();
    int64_t level2_num_val = l2_version->get_total_normal_extent_count();
    int64_t level0_num_trigger = get_level0_file_num_compaction_trigger(level0_num_val, level1_num_val, level2_num_val);
    int64_t level1_trigger = get_level1_file_num_compaction_trigger(level0_num_val, level1_num_val, level2_num_val);

    int64_t intraL0_extent_size = l0_version->get_layers_extent_size(16);
    int64_t minor_level0_extent_size = l0_version->get_layers_extent_size(15);

    if (TaskType::MAX_TYPE_TASK != mc_task_type_) {
      pick_task.task_type_ = pick_one_manual_task(snapshot, storage_manager);
      pick_task.priority_value_ = INT64_MAX;
      task_list.push_back(pick_task);
    } else {
      calc_normal_tasks(level0_layer_val, level0_num_val, intraL0_extent_size, level1_num_val, level1_trigger,
          level2_num_val, task_list);
      if (delete_triggered_compaction_.load(std::memory_order_acquire)) {
        int64_t delete_size = 0 ;
        int64_t oldest_layer_seq = storage_manager.level0_oldest_layer_sequence(snapshot);
        if (FAILED(storage_manager.get_level_delete_size(snapshot, 1, delete_size))) {
          COMPACTION_LOG(WARN, "failed to get level1 delete size", K(ret), K(delete_size));
        } else {
          calc_delete_tasks(level0_num_val, oldest_layer_seq, intraL0_extent_size, level1_num_val, delete_size, task_list);
        }
      }

      int64_t cur_time = Env::Default()->NowMicros();
      if (cur_time - last_check_time_ > AUTO_TASK_TIME_SPAN) {
        calc_auto_tasks(level1_num_val, level2_num_val, task_list);
        last_check_time_ = cur_time;
        COMPACTION_LOG(INFO, "PICK_TASK: auto task check into", K(delete_extents_size_), K(cf_id_));
      }
    }
    if (SUCC(ret) && task_list.size() > 0) {
      std::sort(task_list.begin(), task_list.end(), TaskInfo::greater);
      if (task_list.at(0).priority_value_ > 1) {
        pick_task = task_list.at(0);
      }
      if (is_major_task(pick_task.task_type_)) {
        fill_task_info(level0_num_trigger, level1_trigger, level1_num_val, pick_task);
      }
      // todo set output_level
    }
    COMPACTION_LOG(INFO, "PICK_TASK: pick info", "GetID()", cf_id_, K(level0_layer_trigger), K(level0_num_trigger), K(level1_trigger),
        K(level0_layer_val), K(level0_num_val), K(level1_num_val), K(level2_num_val), K(intraL0_extent_size),
        K(minor_level0_extent_size), K(delete_triggered_compaction_), K(current_priority_l0_layer_sequence_),
        K(pending_priority_l0_layer_sequence_), K(last_pick_task_.priority_value_),
        K(last_pick_task_.extents_size_), K((int)last_pick_task_.task_type_),
        K(pick_task.priority_value_), K(pick_task.extents_size_), K((int)pick_task.task_type_));
  }
  if (SUCC(ret)) {
    last_pick_task_ = pick_task;
    if (pick_task.priority_value_ > L0MaxLayer - 1) {
      priority = HIGH;
    } else {
      priority = LOW;
    }
  }
  return ret;
}

bool CompactionTasksPicker::need_do_task(const CompactionScheduleType type) const {
  bool bret = false;
  if (MASTER_AUTO == type) {
    bret = l1_usage_percent_ < mcf_options_.level2_usage_percent
           || l2_usage_percent_ < mcf_options_.level2_usage_percent
           || delete_extents_size_ > 0;
  } else if (MASTER_IDLE == type) {
    bret = l0_num_val_ > 0 || l1_num_val_ > 0 || l2_usage_percent_ < 90 || delete_extents_size_ > 0;
  }
  return bret;
}

int64_t CompactionTasksPicker::get_minor_merge_limit() const {
  return mcf_options_.compaction_task_extents_limit;
}

int64_t CompactionTasksPicker::get_major_merge_limit() const {
  return mcf_options_.compaction_task_extents_limit;
}

int64_t CompactionTasksPicker::get_major_self_limit() const {
  return mcf_options_.compaction_task_extents_limit * 5;
}
}
}
