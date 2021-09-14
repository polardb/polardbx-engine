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

#ifndef XENGINE_INCLUDE_SHRINK_JOB_H_
#define XENGINE_INCLUDE_SHRINK_JOB_H_

#include "db/column_family.h"

namespace xengine
{
namespace monitor
{
  class InstrumentedMutex;
}
namespace db
{
  struct GlobalContext;
}
namespace storage
{
struct ChangeInfo;
class ShrinkJob
{
public:
  ShrinkJob();
  ~ShrinkJob();

  int init(monitor::InstrumentedMutex *mutex,
           db::GlobalContext *global_ctx,
           const ShrinkInfo &shrink_info);
  int run();

private:
  int before_shrink(bool &can_shrink);
  int do_shrink();
  int after_shrink();
  int get_extent_infos();
  int move_extent();
  int install_shrink_result();
  int write_extent_metas();
  int apply_change_infos();
  int update_super_version();
  int shrink_physical_space();
  bool can_physical_shrink();
  int double_check_shrink_info(bool &can_shrink);
private:
  typedef std::unordered_map<int64_t, ExtentIOInfo> ExtentReplaceMap;
  typedef std::unordered_map<int64_t, ChangeInfo> ChangeInfoMap;
private:
  bool is_inited_;
  monitor::InstrumentedMutex *mutex_;
  db::GlobalContext *global_ctx_;
  ShrinkInfo shrink_info_;
  std::unordered_map<int64_t, db::SubTable *> subtable_map_;
  ExtentIdInfoMap extent_info_map_;
  ExtentReplaceMap extent_replace_map_;
  ChangeInfoMap change_info_map_;
};

} //namespace storage
} //namespace xengine
#endif
