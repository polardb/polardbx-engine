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

#include <string>
#include <sstream>
#include <unordered_map>
#include "logger/logger.h"

namespace xengine
{
namespace common
{

struct DebugInfoEntry
{
  std::string key_desc_;
  int64_t item_id_;
  int64_t timestamp_;
  std::string debug_info_1_;
  std::string debug_info_2_;
  std::string debug_info_3_;
  std::string debug_info_4_;
  std::string debug_info_5_;
  std::string debug_info_6_;

  DebugInfoEntry() :
    key_desc_(),
    item_id_(0),
    timestamp_(0),
    debug_info_1_(),
    debug_info_2_(),
    debug_info_3_(),
    debug_info_4_(),
    debug_info_5_(),
    debug_info_6_() {}

  std::string print()
  {
    std::stringstream ss;
    ss << "DebugInfoEntry = {key_desc_=" << key_desc_
       << ", item_id_=" << item_id_
       << ", timestamp_=" << timestamp_
       << ", debug_info_1_=" << debug_info_1_
       << ", debug_info_2_=" << debug_info_2_
       << ", debug_info_3_=" << debug_info_3_
       << ", debug_info_4_=" << debug_info_4_
       << ", debug_info_5_=" << debug_info_5_
       << ", debug_info_6_=" << debug_info_6_;
    return ss.str();
  }
};

class DebugInfoStation
{
public:
  DebugInfoStation() : entry_map_(), map_mutex_() {}
  ~DebugInfoStation() {}
  static DebugInfoStation* get_instance()
  {
    static DebugInfoStation s;
    return &s;
  }
  void replace_entry(const std::string &key, const DebugInfoEntry &entry)
  {
    std::lock_guard<std::mutex> lg(map_mutex_);
    entry_map_[key] = entry;
    XENGINE_LOG(INFO, "DebugInfoStation replace_entry", "key", key.c_str(),
                "info_1", entry.debug_info_1_.c_str(), "info_2", entry.debug_info_2_.c_str(),
                "info_3", entry.debug_info_3_.c_str(), "info_4", entry.debug_info_4_.c_str(),
                "info_5", entry.debug_info_5_.c_str(), "info_6", entry.debug_info_6_.c_str());
  }
  template<typename DebugInfoEntryReader>
  void foreach(DebugInfoEntryReader &entry_reader)
  {
    std::lock_guard<std::mutex> lg(map_mutex_);
    int ret = Status::kOk;
    XENGINE_LOG(INFO, "DebugInfoStation Print Start");
    for (auto it = entry_map_.begin(); Status::kOk == ret && it != entry_map_.end(); it++) {
      auto &key = it->first;
      auto &entry = it->second;
      ret = entry_reader(it->first, it->second);
      XENGINE_LOG(INFO, "DebugInfoStation replace_entry", "key", key.c_str(),
                  "info_1", entry.debug_info_1_.c_str(), "info_2", entry.debug_info_2_.c_str(),
                  "info_3", entry.debug_info_3_.c_str(), "info_4", entry.debug_info_4_.c_str(),
                  "info_5", entry.debug_info_5_.c_str(), "info_6", entry.debug_info_6_.c_str());
    }
    XENGINE_LOG(INFO, "DebugInfoStation Print End");
  }
private:
  std::unordered_map<std::string, DebugInfoEntry> entry_map_;
  std::mutex map_mutex_;
};

} // ns common
} // ns xengine
