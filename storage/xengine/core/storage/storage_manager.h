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

//#ifndef XENGINE_INCLUDE_STORAGE_MANAGER_H_
//#define XENGINE_INCLUDE_STORAGE_MANAGER_H_
#pragma once
#include "change_info.h"
#include "extent_space_manager.h"
#include "large_object_extent_manager.h"
#include "db/table_cache.h"
#include "db/snapshot_impl.h"
#include "table/two_level_iterator.h"
namespace xengine
{
namespace db
{
 class InternalStats;
 class ColumnFamilyData;
 class SnapshotImpl;
}
namespace table
{
  class MergeIteratorBuilder;
}
namespace storage
{
struct LayerPosition;
struct ExtentLayer;
struct ExtentLayerVersion;
class StorageManager;

// Two level iterator of extents
class ExtentIteratorState : public table::TwoLevelIteratorState {
 public:
  ExtentIteratorState(db::TableCache *table_cache,
                      const common::ReadOptions &read_options,
                      const util::EnvOptions &env_options,
                      const db::InternalKeyComparator &icomparator,
                      monitor::HistogramImpl *file_read_hist,
                      bool for_compaction, bool prefix_enabled,
                      bool skip_filters, int level,
                      db::RangeDelAggregator *range_del_agg,
                      int64_t subtable_id,
                      const int64_t scan_add_blocks_limit)
      : table::TwoLevelIteratorState(prefix_enabled),
        table_cache_(table_cache),
        read_options_(read_options),
        env_options_(env_options),
        icomparator_(icomparator),
        file_read_hist_(file_read_hist),
        for_compaction_(for_compaction),
        skip_filters_(skip_filters),
        level_(level),
        range_del_agg_(range_del_agg),
        subtable_id_(subtable_id),
        scan_add_blocks_limit_(scan_add_blocks_limit) {}

  table::InternalIterator *NewSecondaryIterator(
      const common::Slice &meta_handle, uint64_t* add_blocks = nullptr) override {
    UNUSED(add_blocks);
    int64_t file_id = 0;
    ExtentId *eid = (ExtentId *)meta_handle.data();
    file_id = eid->id();

    // todo internal_stats_
    // now only one extent id
    db::FileDescriptor fd(file_id, subtable_id_, MAX_EXTENT_SIZE);
    return table_cache_->NewIterator(
        read_options_, env_options_, icomparator_, fd, range_del_agg_,
        nullptr /* don't need reference to table */, file_read_hist_,
        for_compaction_, nullptr /* arena */, skip_filters_,
        level_, nullptr, scan_add_blocks_limit_);
  }

  bool PrefixMayMatch(const common::Slice &internal_key) override {
    return true;
  }

 private:
  db::TableCache *table_cache_;
  const common::ReadOptions read_options_;
  const util::EnvOptions &env_options_;
  const db::InternalKeyComparator &icomparator_;
  monitor::HistogramImpl *file_read_hist_;
  bool for_compaction_;
  bool skip_filters_;
  int level_;
  db::RangeDelAggregator *range_del_agg_;
  int64_t subtable_id_; // IS block cache of subtable
  int64_t scan_add_blocks_limit_;
};

struct RecycleArgs
{
  StorageManager *storage_manager_;

  RecycleArgs(StorageManager *storage_manager) : storage_manager_(storage_manager) {}
  ~RecycleArgs() {}
};

class StorageManager
{
public:
	explicit StorageManager(const util::EnvOptions &env_options,
                          const common::ImmutableCFOptions &imm_cf_options,
                          const common::MutableCFOptions &mutable_cf_options);
	virtual ~StorageManager();
  void destroy();

	int init(util::Env *env, ExtentSpaceManager *extent_space_manager, cache::Cache *cache);
	int apply(const ChangeInfo &change_info, bool for_recovery);
  int get(const common::Slice &key,
          const db::Snapshot &current_meta,
          std::function<int(const ExtentMeta *extent_meta, int32_t level, bool &found)> save_value);
  int add_iterators(db::TableCache *table_cache,
                    db::InternalStats *internal_stats,
                    const common::ReadOptions &read_options,
                    table::MergeIteratorBuilder *merge_iter_builder,
                    db::RangeDelAggregator *range_del_agg,
                    const db::Snapshot *current_meta);
  const db::Snapshot *get_current_version() const { return current_meta_; }
  int64_t get_current_total_extent_count() const;
  // todo
//  void set_scan_add_blocks_limit(const uint64_t scan_add_blocks_limit) {
//    mutable_cf_options_.scan_add_blocks_limit = scan_add_blocks_limit;
//  }
  table::InternalIterator *get_single_level_iterator(const common::ReadOptions &read_options,
                                                     util::Arena *arena,
                                                     const db::Snapshot *current_meta,
                                                     int64_t level) const;

  int get_extent_layer_iterator(util::Arena *arena,
                                const db::Snapshot *snapshot,
                                const LayerPosition &layer_position,
                                table::InternalIterator *&iterator) const;

  int64_t level0_oldest_layer_sequence(const db::Snapshot *current_meta) const;
  int get_level_usage_percent(const db::Snapshot *current_meta,
                              const int32_t level,
                              int64_t &usage_percent,
                              int64_t &delete_size) const;
  int get_level_delete_size(const db::Snapshot *current_meta,
                            const int32_t level,
                            int64_t &delete_size) const;
  //calculate the range size
  int64_t approximate_size(const db::ColumnFamilyData *cfd,
                           const common::Slice &start,
                           const common::Slice &end,
                           int32_t start_level,
                           int32_t end_level,
                           const db::Snapshot *sn,
                           int64_t estimate_cost_depth);
  ExtentMeta *get_extent_meta(const ExtentId &extent_id);
	void print_raw_meta();
  const db::SnapshotImpl *acquire_meta_snapshot();
  void release_meta_snapshot(const db::SnapshotImpl *meta_snapshot);
  static void async_recycle(void *args);
  int recycle();
  int recover_extent_space();
  bool get_extent_stats(int64_t &data_size, int64_t &num_entries, int64_t &num_deletes, int64_t &disk_size);
  int release_extent_resource(bool for_recovery);
  bool can_gc()
  {
    //make sure, only one version snapshot current, and all old version snapshot has been recycle
    std::lock_guard<std::mutex> meta_mutex_guard(meta_mutex_);
    return (1 == meta_snapshot_list_.count() && 0 == bg_recycle_count_ && 0 == waiting_delete_versions_.size());
  }
  bool can_shrink()
  {
    //make sure, only single version snapshot current, and all old version snapshot has been recycle
    //and the subtable not contain large object
    std::lock_guard<std::mutex> meta_mutex_guard(meta_mutex_);
    return (1 == meta_snapshot_list_.count() && 0 == bg_recycle_count_ && 0 == waiting_delete_versions_.size())
           && 0 == lob_extent_mgr_->get_lob_extent_count(); 
  }
  int deserialize_and_dump(const char *buf, int64_t buf_len, int64_t &pos,
                           char *str_buf, int64_t str_buf_len, int64_t &str_pos);
  int get_extent_infos(const int64_t index_id, ExtentIdInfoMap &extent_info_map);
  DECLARE_SERIALIZATION();
  DECLARE_TO_STRING();
private:
  int init_extent_layer_versions(ExtentSpaceManager *extent_space_manager,
                                 db::InternalKeyComparator *internalkey_comparator);
	int normal_apply(const ChangeInfo &change_info);
  int apply_large_object(const ChangeInfo &change_info);
  int build_new_version(ExtentLayerVersion *old_version,
                        const ExtentChangeArray &extent_changes,
                        const ExtentChangeArray &lob_extent_change,
                        ExtentLayerVersion *&new_version);
  int prepare_build_new_version(const ExtentChangeArray &extent_changes,
                                ExtentChangesMap &extent_changes_per_layer);
  int actual_build_new_version(const ExtentLayerVersion *old_version,
                               const ExtentChangesMap &extent_changes_per_layer,
                               const ExtentChangeArray &lob_extent_change,
                               ExtentLayerVersion *&new_version);
  int copy_on_write_accord_old_version(const ExtentLayerVersion *old_version,
                                       const ExtentChangesMap &extent_changes_per_layer,
                                       const ExtentChangeArray &lob_extent_change,
                                       ExtentLayerVersion *&new_version);
  int build_new_extent_layer_if_need(const ExtentChangesMap &extent_changes_per_layer,
                                     ExtentLayerVersion *&new_version);
  int deal_dump_extent_layer_if_need(const ExtentLayerVersion *old_version,
                                     const ExtentChangesMap &extent_changes_per_layer,
                                     const ExtentChangeArray &lob_extent_change,
                                     ExtentLayerVersion *&new_version);
  int record_lob_extent_info_to_dump_extent_layer(const ExtentChangeArray &lob_extent_change, ExtentLayer *&dump_extent_layer);
  int modify_extent_layer(ExtentLayer *extent_layer, const ExtentChangeArray &extent_changes);
  int add_iterator_for_layer(const LayerPosition &layer_position,
                             db::TableCache *table_cache,
                             db::InternalStats *internal_stats,
                             const common::ReadOptions &read_options,
                             table::MergeIteratorBuilder *merge_iter_builder,
                             db::RangeDelAggregator *range_del_agg,
                             ExtentLayer *extent_layer);
  int create_extent_layer_iterator(util::Arena *arena,
                                   const db::Snapshot *snapshot,
                                   const LayerPosition &layer_position,
                                   table::InternalIterator *&iterator) const;
  int64_t one_layer_approximate_size(const db::ColumnFamilyData *cfd,
                                     const common::Slice &start,
                                     const common::Slice &end,
                                     int32_t level,
                                     table::InternalIterator *iter,
                                     int64_t estimate_cost_depth,
                                     EstimateCostStats &cost_stats);
  int update_current_meta_snapshot(ExtentLayerVersion **new_extent_layer_versions);
  const db::SnapshotImpl *acquire_meta_snapshot_unsafe();
  void release_meta_snapshot_unsafe(const db::SnapshotImpl *meta_snapshot);
  bool is_filter_skipped(int32_t level)
  {
    return (immutable_cfoptions_.optimize_filters_for_hits && (level > 0));
  }
  int recycle_unsafe(bool recovery);
  int recycle_extent_layer_version(ExtentLayerVersion *extent_layer_version, bool for_recovery);
  int recycle_extent_layer(ExtentLayer *extent_layer, bool for_recovery);
  int recycle_lob_extent(bool for_recovery);
  int do_recycle_extent(ExtentMeta *extent_meta, bool for_recovery);
  void print_raw_meta_unsafe();
  int build_new_extent_layer(util::autovector<ExtentId> &extent_ids, ExtentLayer *&extent_layer);
  int deserialize_extent_layer(const char *buf, int64_t buf_len, int64_t &pos,
                               common::SequenceNumber &sequence_number,
                               util::autovector<ExtentId> &extent_ids,
                               util::autovector<ExtentId> &lob_extent_ids);
  int calc_extent_stats_unsafe();
private:
  static const int64_t STORAGE_MANAGER_VERSION = 1;
private:
  bool is_inited_;
  const util::EnvOptions &env_options_;
  const common::ImmutableCFOptions &immutable_cfoptions_;
  const common::MutableCFOptions &mutable_cf_options_;
  util::Env *env_;
  cache::Cache *table_cache_;
  int32_t column_family_id_;
  int64_t bg_recycle_count_;
  db::InternalKeyComparator *internalkey_comparator_;
  ExtentSpaceManager *extent_space_manager_;
  mutable std::mutex meta_mutex_;
  common::SequenceNumber meta_version_;
  ExtentLayerVersion *extent_layer_versions_[MAX_TIER_COUNT];
  db::SnapshotImpl *current_meta_;
  db::SnapshotList meta_snapshot_list_;
  util::autovector<ExtentLayerVersion *> waiting_delete_versions_;
  bool extent_stats_updated_;
  ExtentStats extent_stats_;
  LargeObjectExtentMananger *lob_extent_mgr_;
};

} //namespace storage
} //namespace xengine
//#endif
