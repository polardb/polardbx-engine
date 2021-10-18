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
#include <unordered_map>
#include <vector>
#include "util/serialization.h"
#include "xengine/types.h"
#include "io_extent.h"


namespace xengine
{
namespace storage
{
class ExtentSpaceManager;
struct ChangeInfo;
struct ExtentMeta;

class LargeObjectExtentMananger
{
public:
  LargeObjectExtentMananger();
  ~LargeObjectExtentMananger();
  void destroy();
  int init(ExtentSpaceManager *extent_space_mgr);

  int apply(const ChangeInfo &change_info, common::SequenceNumber sequence_number);
  int recycle(common::SequenceNumber sequence_number, bool for_recovery);
  //recycle the lob extent not expilict delete, when the sutable has been dropped
  int force_recycle(bool for_recovery);
  int64_t get_lob_extent_count() const { return lob_extent_.size(); }
  int recover_extent_space();
  int deserialize_and_dump(const char *buf, int64_t buf_len, int64_t &pos,
                           char *str_buf, int64_t str_buf_len, int64_t &str_pos);
  DECLARE_SERIALIZATION();
private:
  int add_extent(ExtentMeta *extent_meta);
  int delete_extent(ExtentMeta *extent_meta);
  int update(common::SequenceNumber sequence_number);
  int recycle_extents(const std::vector<ExtentMeta *> &extents, bool for_recovery);
  int recycle_extent(ExtentMeta *extent_meta, bool for_recovery);
  int get_all_extent_ids(std::vector<ExtentId> &extent_ids) const;
  int build_lob_extent(std::vector<ExtentId> &extent_ids);
private:
    static const int64_t LARGE_OBJECT_EXTENT_MANAGER_VERSION = 1;
private:
  bool is_inited_;
  ExtentSpaceManager *extent_space_mgr_;
  std::unordered_map<int64_t, ExtentMeta*> lob_extent_;
  std::vector<ExtentMeta *> delete_lob_extent_;
  std::unordered_map<common::SequenceNumber, std::vector<ExtentMeta *>> lob_extent_wait_to_recycle_;
};

} //namespace storage
} //namespace xengine
