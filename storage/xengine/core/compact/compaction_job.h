//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
#ifndef XENGINE_STORAGE_COMPACTION_JOB_H_
#define XENGINE_STORAGE_COMPACTION_JOB_H_

#include "split_compaction.h"
#include "minor_compaction.h"

namespace xengine {

namespace common {
class Iterator;
}

namespace db {
class VersionSet;
class ColumnFamilyData;
}

namespace monitor {
class InstrumentedMutex;
}

namespace storage {

class ExtentLayer;

// mock for compaction
class ExtentLayerIterator;
class ExtentLayer;

// CompactionJob represents a compact task
// which scheduled by background compaction thread
// CompactionJob will split job to some small sub-tasks
// for execute parallelly.
class CompactionJob {
 public:
  const static int64_t LEVEL_EXTENTS_PERCENT = 20;
  const static int64_t FULL_EXTENTS_PERCENT = 95;
  const static int64_t EXTENTS_CNT_LIMIT = 50000;
  const static int64_t EXTENT_SIZE = 2 * 1024 * 1024;
  const static int64_t FULL_EXTENT_SIZE =
        EXTENT_SIZE * FULL_EXTENTS_PERCENT / 100;
  struct CompactWay {
    int64_t sequence_number_;
    int32_t extent_count_;
    int32_t level_;
    storage::Range range_;
    CompactWay() : sequence_number_(0), extent_count_(0), level_(0), range_() {}
  };
 public:
  CompactionJob(memory::ArenaAllocator &arena);
  ~CompactionJob();

  int init(const CompactionContext &context, const ColumnFamilyDesc &cf,
           StorageManager *sm, const db::Snapshot* meta_snapshot);
  void destroy();
  void destroy_compaction(Compaction *compaction);

 public:
  /**
   * this three functions for build a complete compaction task
   * and run/install for once.
   */
  // build compaction task plan, no need hold db_lock_
  int prepare();
  // run compaction task, do not hold the db_lock_
  int run();

  /**
   * CompactionJob build multiple tasks run parallelly
   * Compaction Thread should install compaction result meta byself.
   * do not use prepare/run/install simultaneously.
   */
  int prepare_major_task(const int64_t level1_pick_start_pos,
                         const int64_t extent_limit,
                         const bool has_splitted = false,
                         const size_t delete_percent = 0);
  int prepare_major_self_task(const int64_t extents_limit,
                              bool is_auto = false,
                              const bool is_delete = false);

  int prepare_minor_self_task();
  int prepare_minor_task(const int64_t merge_limit);
  static int create_meta_iterator(memory::ArenaAllocator &arena,
                                  const db::InternalKeyComparator *internal_comparator,
                                  const db::Snapshot *snapshot,
                                  const storage::LayerPosition &layer_position,
                                  table::InternalIterator *&iterator);
  int64_t get_task_size() const { return compaction_tasks_.size(); }
  int32_t get_task_type() const {
    return current_task_type_;
  }

  Compaction *get_next_task() {
    Compaction *one = nullptr;
    if (!compaction_tasks_.empty()) {
      one = *compaction_tasks_.begin();
      compaction_tasks_.pop_front();

      report_started_compaction();
      set_compaction_type(get_task_type());
      set_compaction_input_extent(one->get_input_extent());
    }
    return one;
  }

  bool all_task_completed() const { return task_to_run_ == 0; }
  const Compaction::Statstics &get_stats() const { return compaction_stats_; }
  void update_snapshot(const db::Snapshot* snapshot) { meta_snapshot_ = snapshot; }
  storage::ChangeInfo &get_change_info() { return change_info_; }
  int append_change_info(const ChangeInfo& other);

  bool need_delete_compaction() { return need_delete_compaction_; }
  void set_need_delete_compaction(bool value) {
    need_delete_compaction_ = value; 
  }

  bool priority_layer_compacted() { return priority_layer_compacted_; }

#ifndef NDEBUG
  int TEST_get_all_l0_range(memory::ArenaAllocator &arena, int64_t all_way_size,
      CompactWay *compact_way,
      int64_t &way_size, storage::Range &wide_range, bool verbose = false);
#endif

 private:
  int parse_meta(const table::InternalIterator *iter, ExtentMeta *&extent_meta);
  int build_compaction_iterators(memory::ArenaAllocator &arena,
                                 const CompactWay *compact_way,
                                 const int64_t total_way_size,
                                 const int64_t max_compact_way,
                                 storage::RangeIterator **iterators,
                                 int64_t &iter_size);
  int build_plus_l1_compaction_iterators(memory::ArenaAllocator &arena,
                                         const CompactWay *compact_way,
                                         const int64_t way_size,
                                         const storage::Range &wide_range,
                                         storage::RangeIterator **iterators,
                                         int64_t &iter_size);
  int build_major_l1_iterator(const int64_t level1_pick_start_pos,
                              storage::Range &wide_range,
                              MetaDataIterator *&range_iter,
                              size_t delete_percent);
  int build_major_l2_iterator(const storage::Range &wide_range,
                              MetaDataIterator *&range_iter);
  int build_self_iterator(const int64_t level,
                          MetaDataSingleIterator *&range_iter,
                          size_t delete_percent = 0);
  //int get_all_l0_range(util::Arena &arena, CompactWay *compact_way,
  //                     int64_t &way_size, storage::Range &wide_range);
  int get_all_l0_range(memory::ArenaAllocator &arena,
                       const int64_t all_way_size,
                       CompactWay *compact_way,
                       int64_t &way_size,
                       storage::Range &wide_range);
  int build_multiple_compaction(memory::ArenaAllocator &arena,
                                storage::RangeIterator **iterators,
                                int64_t iter_size, const int64_t merge_limit);
  int build_multiple_major_compaction(const int64_t merge_limit,
                                      const size_t delete_percent,
                                      storage::RangeIterator *iterator1,
                                      storage::RangeIterator *l2_iterator);
  int build_multiple_major_compaction_again(const int64_t merge_limit,
                                            const size_t delete_percent,
                                            storage::RangeIterator *iterator1,
                                            storage::RangeIterator *l2_iterator);
  int build_multiple_major_self_compaction(const size_t delete_percent,
                                           const int64_t extents_limit,
                                           storage::RangeIterator *l2_iterator);
  int deal_with_split_batch(const int64_t merge_limit,
                            const MetaDescriptorList &extents,
                            SplitCompaction *&split_task);
  int pick_extents(
      const int64_t level,
    storage::RangeIterator *l2_iterator,
    MetaDescriptorList &extents);
  int alloc_compaction_task(
      const size_t delete_percent,
      GeneralCompaction *&compaction,
      MetaDescriptorList &extents);
  int build_auto_self_compaction(const int64_t level, storage::RangeIterator *l2_iterator);
  int build_delete_major_self_compaction(const int64_t extents_limit,
                                         storage::RangeIterator *iterator2);
  int add_split_extent_info(const MetaDescriptor &extent);
  void add_compaction_task(Compaction *task) {
    ++task_to_run_;
    compaction_tasks_.push_back(task);
  }
  void destroy_range_iterators(RangeIterator **iters, const int64_t num);

  void report_started_compaction();
 
  void set_compaction_type(int compaction_type);

  void set_compaction_input_extent(const int64_t *input_extent);

 private:
  using CompactionTask = std::deque<Compaction *, memory::stl_adapt_allocator<Compaction *>>;

  memory::ArenaAllocator &arena_;
  CompactionContext context_;
  ColumnFamilyDesc cf_desc_;
  StorageManager *storage_manager_;       // meta data store.
  const db::Snapshot* meta_snapshot_;     // current snapshot for meta data.
  CompactionTask compaction_tasks_;
  bool priority_layer_compacted_;         // priority L0 layer is compacted
  // cumulative statistics of all compaction tasks in this job
  Compaction::Statstics compaction_stats_;
  int64_t task_to_run_;
  int current_task_type_;
  bool inited_;
  MetaDescriptorList extents_list_;
  BlockPositionList merge_batch_indexes_;
  // Minor and Intra Compaction will store each task's result, and the last
  // task will install the whole result.
  storage::ChangeInfo change_info_;
  bool need_delete_compaction_;

 private:
  CompactionJob(const CompactionJob &) = delete;
  CompactionJob(CompactionJob &&) = delete;
  CompactionJob &operator=(const CompactionJob &) = delete;
};
}  // namespace storage
}  // namespace xengine

#endif  // XENGINE_STORAGE_COMPACTION_JOB_H_
