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
//#ifndef XENGINE_INCLUDE_COLUMN_FAMILY_H_
//#define XENGINE_INCLUDE_COLUMN_FAMILY_H_
#include <atomic>
#include "compact/task_type.h"
#include "util/autovector.h"
#include "util/to_string.h"

namespace xengine {
namespace storage {
class StorageManager;
}
namespace common {
struct MutableCFOptions;
}
namespace db {

class SnapshotImpl;

enum CompactionPriority {
  LOW = 0,
  HIGH = 1,
  ALL = 2
};

enum CompactionScheduleType {
  NORMAL,
  MASTER_AUTO,
  MASTER_IDLE
};

class CompactionTasksPicker {
  static const int64_t L0MaxLayer = 200;
  static const int64_t L0MaxNum = 1000;
  static const int64_t L1MaxNum = 5000;
  static const int64_t minUsage = 50;
  static const int64_t AUTO_TASK_TIME_SPAN = 900000000;
  static const int64_t DEL_BASE = 100;
 public:
  struct TaskInfo {
    TaskInfo():
      task_type_(TaskType::MAX_TYPE_TASK),
      priority_value_(0),
      extents_size_(0),
      output_level_(0),
      l1_pick_pos_(0),
      need_split_(false) {
    }
    void reset() {

    }
    static bool greater(const TaskInfo &a, const TaskInfo &b) {
      return a.priority_value_ > b.priority_value_;
    }
    TaskType task_type_;
    double priority_value_;
    int64_t extents_size_;
    int64_t output_level_;
    int64_t l1_pick_pos_;
    bool need_split_;
    // todo to_string
    DECLARE_TO_STRING();
  };
  CompactionTasksPicker(common::MutableCFOptions &mutable_cf_options,
                        const uint64_t compaction_type,
                        const uint32_t cf_id,
                        const bool dynamic_trigger_adjust);
  ~CompactionTasksPicker() {}
  int pick_one_task(const SnapshotImpl* snapshot,
                    const storage::StorageManager &storage_manager,
                    TaskInfo &task_info,
                    CompactionPriority &priority);
  // for scheduler
  int pick_task_list(const SnapshotImpl* snapshot,
                     const storage::StorageManager &storage_manager,
                     util::autovector<TaskInfo> &task_list,
                     CompactionPriority &priority);
  void fill_task_info(const int64_t level0_trigger,
                      const int64_t level1_trigger,
                      const int64_t level_num,
                      TaskInfo &tasl_info);
  bool is_normal_tree(const SnapshotImpl* snapshot);
  int64_t  get_level1_file_num_compaction_trigger(const int64_t l0_num,
                                                  const int64_t l1_num,
                                                  const int64_t l2_num) const;
  int64_t  get_level0_file_num_compaction_trigger(const int64_t l0_num,
                                                  const int64_t l1_num,
                                                  const int64_t l2_num) const;
  void set_mc_task_type(const TaskType mc_task_type) {
    mc_task_type_ = mc_task_type;
  }
  void set_pending_priority_l0_layer_sequence(const int64_t seq) {
    pending_priority_l0_layer_sequence_ = seq;
  }
  void set_current_priority_l0_layer_sequence(const int64_t seq) {
    current_priority_l0_layer_sequence_ = seq;
  }
  void set_delete_compaction_trigger(const bool value) {
    delete_triggered_compaction_ = value;
  }
  void set_cf_id(const uint32_t id) {
    cf_id_ = id;
  }
  void set_autocheck_info(const int64_t delete_extents_size,
                          const int64_t l1_usage_percent,
                          const int64_t l2_usage_percent) {
    delete_extents_size_ = delete_extents_size;
    l1_usage_percent_ = l1_usage_percent;
    l2_usage_percent_ = l2_usage_percent;
  }
  void set_level_info(const int64_t l0_num_val,
                      const int64_t l1_num_val,
                      const int64_t l2_num_val) {
    l0_num_val_ = l0_num_val;
    l1_num_val_ = l1_num_val;
    l2_num_val_ = l2_num_val;
  }
  int64_t get_minor_merge_limit() const;

  int64_t get_major_merge_limit() const;

  int64_t get_major_self_limit() const;
  // simple check
  bool need_do_task(const CompactionScheduleType type) const;
 private:
  TaskType pick_one_manual_task(const SnapshotImpl* snapshot,
                                const storage::StorageManager &storage_manager);
  void calc_normal_tasks(const int64_t level0_layer_val,
                         const int64_t level0_num_val,
                         const int64_t level0_size,
                         const int64_t level1_num_val,
                         const int64_t level1_num_trigger,
                         const int64_t level2_num_val,
                         util::autovector<TaskInfo> &task_list);
  void calc_delete_tasks(const int64_t level0_num_val,
                         const int64_t oldest_layer_seq,
                         const int64_t level0_size,
                         const int64_t level1_num_val,
                         const int64_t level1_delete_size,
                         util::autovector<TaskInfo> &task_list);
  void calc_auto_tasks(const int64_t level1_num_val,
                       const int64_t level2_num_val,
                       util::autovector<TaskInfo> &task_list);
  int pick_auto_task(TaskInfo &pick_task);
  int pick_one_task_idle(TaskInfo &pick_task);

  TaskInfo last_pick_task_;
  TaskType mc_task_type_;
 protected:
  friend class ColumnFamilyData;
  // Valid only when high priority
  // Flush will update pending_priority_l0_layer_sequence, while Compaction
  // will fetch one pending sequence to current_l0_layer_sequence and do a
  // round of deletion triggered compaction(L0->L1->L2).
  int64_t l1_usage_percent_;
  int64_t l2_usage_percent_;
  int64_t delete_extents_size_;
  int64_t l0_num_val_;
  int64_t l1_num_val_;
  int64_t l2_num_val_;
  int64_t pending_priority_l0_layer_sequence_;
  int64_t current_priority_l0_layer_sequence_;
  std::atomic<bool> delete_triggered_compaction_;
  int64_t last_check_time_;
  uint32_t cf_id_;
  const common::MutableCFOptions &mcf_options_;
  int64_t minor_merge_limit_;
  uint64_t minor_compaction_type_; // 0stream/1minor(for fpga)
  bool dynamic_trigger_adjust_;
};
}
}
