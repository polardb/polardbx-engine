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

#ifndef XENGINE_INCLUDE_TABLE_SPACE_H_
#define XENGINE_INCLUDE_TABLE_SPACE_H_

#include <mutex>
#include <set>
#include "extent_space.h"

namespace xengine
{
namespace storage
{
class TableSpace
{
public:
  TableSpace(util::Env *env, const util::EnvOptions &env_options);
  ~TableSpace();
  void destroy();

  //ddl relative function
  int create(const CreateTableSpaceArgs &args);
  int remove();
  int register_subtable(const int64_t index_id);
  int unregister_subtable(const int64_t index_id);

  //extent relatice function
  int allocate(const int32_t extent_space_type, ExtentIOInfo &io_info);
  int recycle(const int32_t extent_space_type, const ExtentId extent_id);
  // mark the extent used, only used during recovery
  int reference(const int32_t extent_space_type,
                const ExtentId extent_id,
                ExtentIOInfo &io_info);

  //shrink relative function
  int move_extens_to_front(const ShrinkInfo &shrink_info, std::unordered_map<int64_t, ExtentIOInfo> &replace_map);
  int shrink(const ShrinkInfo &shrink_info);
  int get_shrink_info_if_need(const ShrinkCondition &shrink_condition, std::vector<ShrinkInfo> &shrink_infos);
  int get_shrink_info(const int32_t extent_space_type,
                      const ShrinkCondition &shrink_condition,
                      ShrinkInfo &shrink_info);

  //statistic relative function
  bool is_free();
  bool is_dropped();
  int get_data_file_stats(std::vector<DataFileStatistics> &data_file_stats);

  //recovery relative function
  int add_data_file(DataFile *data_file);
  int rebuild();

private:
  ExtentSpace *get_extent_space(const int32_t extent_space_type);
  bool is_free_unsafe();
  bool is_dropped_unsafe();
private:
  bool is_inited_;
  util::Env *env_;
  const util::EnvOptions env_options_;
  int64_t table_space_id_;
  std::mutex table_space_mutex_;
  std::vector<common::DbPath> db_paths_;
  std::map<int32_t, ExtentSpace *> extent_space_map_;
  std::set<int64_t> index_id_set_;
};

} //namespace storage
} //namespace xengine

#endif
