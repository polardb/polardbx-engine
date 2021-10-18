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

#ifndef XENGINE_INCLUDE_EXTENT_SPACE_MANAGER_H_
#define XENGINE_INCLUDE_EXTENT_SPACE_MANAGER_H_
#include "util/spin_rwlock.h"
#include "table_space.h"
#include "io_extent.h"

namespace xengine
{
namespace storage
{
class StorageLogger;
struct ExtentMeta;
struct CheckpointHeader;
class ExtentMetaManager;
class ExtentSpaceManager
{
public:
  ExtentSpaceManager(util::Env *env,
                     const util::EnvOptions &env_options,
                     const common::DBOptions &db_options);
  ~ExtentSpaceManager();
  void destroy();

  int init(StorageLogger *storage_logger);

  //tablespace relative function
  int create_table_space(const int64_t table_space_id);
  int open_table_space(const int64_t table_space_id);
  int recycle_dropped_table_space();
  int register_subtable(const int64_t table_space_id, const int64_t index_id);
  int unregister_subtable(const int64_t table_space_id, const int64_t index_id);
  int64_t allocate_table_space_id() { return ++table_space_id_; }
  
  //extent relative function
  int allocate(const int64_t table_space_id,
               const int32_t extent_space_type,
               WritableExtent &extent);
  int recycle(const int64_t table_space_id,
              const int32_t extent_space_type,
              const ExtentId extent_id,
              bool has_meta = true);
  // mark the extent used, only used during recovery
  int reference(const int64_t table_space_id,
                const int32_t extent_space_type,
                const ExtentId extent_id);
  common::Status get_random_access_extent(ExtentId extent_id, RandomAccessExtent &);

  //ExtentMeta wrapper relative function
  int get_meta(const ExtentId &extent_id, ExtentMeta *&extent_meta);
  int write_meta(const ExtentMeta &extent_meta, bool write_log);
  int recycle_meta(const ExtentId extent_id);
  int do_checkpoint(util::WritableFile *checkpoint_writer, CheckpointHeader *header);
  int load_checkpoint(util::RandomAccessFile *checkpoint_reader, CheckpointHeader *header);
  int replay(int64_t log_type, char *log_data, int64_t log_len);

  //shrink relative function
  int get_shrink_infos(const ShrinkCondition &shrink_condition, std::vector<ShrinkInfo> &shrink_infos);
  int get_shrink_infos(const int64_t table_space_id,
                       const ShrinkCondition &shrink_condition,
                       std::vector<ShrinkInfo> &shrink_infos);
  int get_shrink_info(const int64_t table_space_id,
                      const int32_t extent_space_type,
                      const ShrinkCondition &shrink_condition,
                      ShrinkInfo &shrink_info);
  int move_extens_to_front(const ShrinkInfo &shrink_info, std::unordered_map<int64_t, ExtentIOInfo> &replace_map);
  int shrink_extent_space(const ShrinkInfo &shrink_info);

  //statistic relative function
  int get_data_file_stats(std::vector<DataFileStatistics> &data_file_stats);
  int get_data_file_stats(const int64_t table_space_id,
                          std::vector<DataFileStatistics> &data_file_stats);

  //recover relative function
  int open_all_data_file();
  int rebuild();

private:
  int internal_create_table_space(const int64_t table_space_id);
  TableSpace *get_table_space(int64_t table_space_id);
  int open_specific_directory(const std::string &dir_path);
  int get_data_file_numbers(const std::string &dir_path, std::vector<int64_t> &data_file_numbers);
  int open_data_files(const std::string &dir_path, const std::vector<int64_t> &data_file_numbers);
  void update_max_file_number(const int64_t file_number);
  void update_max_table_space_id(const int64_t table_space_id);
  int clear_garbage_files();

private:
  bool is_inited_;
  util::Env *env_;
  util::EnvOptions env_options_;
  const common::DBOptions db_options_;
  ExtentMetaManager *extent_meta_mgr_;
  //protect the map contain table space
  util::SpinRWLock table_space_lock_;
  std::atomic<int64_t> table_space_id_;
  std::map<int64_t, TableSpace *> table_space_map_;
  std::map<int64_t, TableSpace *> wait_remove_table_space_map_;
  //protect extent_io_info_map
  util::SpinRWLock io_info_map_lock_;
  std::unordered_map<int64_t, ExtentIOInfo> extent_io_info_map_;
  std::vector<std::string> garbage_files_;
};

} //namespace storage
} //namespace xengine

#endif
