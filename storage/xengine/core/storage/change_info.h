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
#include "compact/task_type.h"
#include "io_extent.h"
#include "storage_common.h"
namespace xengine
{
namespace storage
{
struct ExtentChange
{
  static const int64_t EXTENT_CHANGE_VERSION = 1;
	static const uint8_t F_INIT = 0X0;
	static const uint8_t F_ADD = 0X1;
	static const uint8_t F_DEL = 0X2;
  LayerPosition layer_position_;
	ExtentId extent_id_;
	uint8_t flag_;

  ExtentChange() : layer_position_(), extent_id_(), flag_(F_INIT) {}
	explicit ExtentChange(const LayerPosition &layer_position, const ExtentId &extent_id, uint8_t flag)
      : layer_position_(layer_position),
        extent_id_(extent_id),
        flag_(flag)
  {
  }
	~ExtentChange() {}
  bool is_add() const { return flag_ & F_ADD; }
  bool is_delete() const { return flag_ & F_DEL; }

  DECLARE_AND_DEFINE_TO_STRING(KV_(layer_position), KV_(extent_id), KV_(flag));
  DECLARE_COMPACTIPLE_SERIALIZATION(EXTENT_CHANGE_VERSION);
};

typedef std::unordered_map<int64_t, std::vector<ExtentChange>> ExtentChangesMap;
typedef std::vector<ExtentChange> ExtentChangeArray;
struct ChangeInfo
{
	static const int64_t CHANGE_INFO_VERSION = 1;
	std::unordered_map<int64_t, std::vector<ExtentChange>> extent_change_info_;
  ExtentChangeArray lob_extent_change_info_;
  int64_t task_type_;

	ChangeInfo();
	~ChangeInfo();
  void clear();
	int add_extent(const LayerPosition &layer_position, const ExtentId &extent_id);
	int delete_extent(const LayerPosition &layer_position, const ExtentId &extent_id);
  int replace_extent(const LayerPosition &layer_position, const ExtentId &old_extent, const ExtentId &new_extent);
  int add_large_object_extent(const ExtentId &extent_id);
  int delete_large_object_extent(const ExtentId &extent_id);
  int merge(const ChangeInfo &change_info);
  //TODO:yuanfeng, extent DEFINE_TO_STRING to support unordered_map
  int64_t to_string(char *buf, const int64_t buf_len) const
  {
    int64_t pos = 0;
    util::databuff_printf(buf, buf_len, pos, "{");
    for (auto iter = extent_change_info_.begin(); extent_change_info_.end() != iter; ++iter) {
      util::databuff_printf(buf, buf_len, pos, "{");
      util::databuff_print_json_wrapped_kv(buf, buf_len, pos, "level", iter->first);
      for (uint32_t i = 0; i < iter->second.size(); ++i) {
        util::databuff_print_json_wrapped_kv(buf, buf_len, pos, "extent_change", iter->second.at(i));
      }
      util::databuff_printf(buf, buf_len, pos, "}");
    }
    util::databuff_print_json_wrapped_kv(buf, buf_len, pos, "lob", "");
    util::databuff_printf(buf, buf_len, pos, "{");
    for (uint32_t i = 0; i < lob_extent_change_info_.size(); ++i) {
      util::databuff_print_json_wrapped_kv(buf, buf_len, pos, "lob_extent_change", lob_extent_change_info_.at(i));
    }
    util::databuff_printf(buf, buf_len, pos, "}");
    util::databuff_print_json_wrapped_kv(buf, buf_len, pos, "task_type", (int)task_type_);
    util::databuff_printf(buf, buf_len, pos, "}");

    return pos;
  }
  DECLARE_COMPACTIPLE_SERIALIZATION(CHANGE_INFO_VERSION);
private:
	int push_extent_change(const int64_t level, const ExtentChange &extent_change);
  int push_large_object_extent_change(const ExtentChange &extent_change);
};

} //namespace storage
} //namespace xengine
