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

#ifndef XENGINE_INCLUDE_STORAGE_COMMON_H_
#define XENGINE_INCLUDE_STORAGE_COMMON_H_
#include <unordered_map>
#include <set>
#include "memory/stl_adapt_allocator.h"
#include "xengine/options.h"
#include "util/autovector.h"
#include "util/to_string.h"
#include "util/serialization.h"
#include "xengine/xengine_constants.h"

namespace xengine
{
namespace storage
{
struct ExtentInfo;

// hash table of extent id to meta
typedef std::unordered_map<int64_t, ExtentInfo, std::hash<int64_t>, std::equal_to<int64_t>,
      memory::stl_adapt_allocator<std::pair<const int64_t, ExtentInfo>, memory::ModId::kStorageMgr>> ExtentIdInfoMap;

// hash table of sst file number to used extent ids
typedef std::unordered_map<int32_t, util::autovector<int32_t>, std::hash<int32_t>, std::equal_to<int32_t>,
      memory::stl_adapt_allocator<std::pair<const int32_t, util::autovector<int32_t>>, memory::ModId::kStorageMgr>> SSTExtentIdMap;

struct ExtentId {
  int32_t file_number;  // file number
  int32_t offset;       // extent offset in file
  ExtentId() : file_number(0), offset(0) {};
  ExtentId(const ExtentId &r) {
    file_number = r.file_number;
    offset = r.offset;
  }
  ExtentId(const int64_t eid) {
    file_number = static_cast<int32_t>(eid >> 32);
    offset = static_cast<int32_t>(eid);
  }

  ExtentId(int32_t fn, int32_t off) : file_number(fn), offset(off) {};
  DECLARE_SERIALIZATION();  // write to memtable
  inline int64_t id() const {
    int64_t hi = file_number;
    int64_t lo = offset;
    int64_t value = hi << 32 | lo;
    return value;
  }

  bool is_equal(const ExtentId &eid) const {
    return (this->file_number == eid.file_number) &&
           (this->offset == eid.offset);
  }
  
  void reset() {
    file_number = 0;
    offset = 0;
  }

  DECLARE_TO_STRING();
};

struct LayerPosition
{
public:
  static const int64_t LAYER_POSITION_VERSION = 0;
  static const int32_t INVISIBLE_LAYER_INDEX;
  static const int32_t NEW_GENERATE_LAYER_INDEX; 
public:
  int32_t level_;
  int32_t layer_index_;

  LayerPosition() : level_(0), layer_index_(0)
  {
  }
  explicit LayerPosition(const int32_t level)
      : level_(level),
        layer_index_(0)
  {
  }
  LayerPosition(const int32_t level, const int32_t layer_index)
      : level_(level),
        layer_index_(layer_index)
  {
  }
  explicit LayerPosition(const int64_t &position)
  {
    level_ = position >> 32;
    layer_index_ = static_cast<int32_t>(position);
  }
  LayerPosition(const LayerPosition &layer_position)
      : level_(layer_position.level_),
        layer_index_(layer_position.layer_index_)
  {
  }
  
  void reset()
  {
    level_ = 0;
    layer_index_ = 0;
  }

  bool is_valid() const
  {
    return (level_ >= 0 && level_ < storage::MAX_TIER_COUNT)
           && (layer_index_ >= 0);
  }
  int64_t position() const
  {
    return (((int64_t)level_) << 32) | layer_index_;
  }
  int32_t get_level() const { return level_; }
  int32_t get_layer_index() const { return layer_index_; }
  bool is_new_generate_layer() const { return NEW_GENERATE_LAYER_INDEX == layer_index_; }

  DECLARE_AND_DEFINE_TO_STRING(KV_(level), KV_(layer_index));
  DECLARE_COMPACTIPLE_SERIALIZATION(LAYER_POSITION_VERSION);
};

struct ExtentInfo
{
  int64_t index_id_;
  LayerPosition layer_position_;
  ExtentId extent_id_;

  ExtentInfo() : index_id_(0), layer_position_(), extent_id_()
  {
  }
  ExtentInfo(const int64_t index_id, const LayerPosition &layer_position, const ExtentId &extent_id)
      : index_id_(index_id),
        layer_position_(layer_position),
        extent_id_(extent_id)
  {
  };
  ~ExtentInfo()
  {
  }
  void set(const int64_t index_id, const LayerPosition &layer_position, const ExtentId &extent_id)
  {
    index_id_ = index_id;
    layer_position_ = layer_position;
    extent_id_ = extent_id;
  }

  DECLARE_AND_DEFINE_TO_STRING(KV_(index_id), KV_(layer_position), KV_(extent_id));
};

struct ExtentStats
{
  int64_t data_size_;
  int64_t num_entries_;
  int64_t num_deletes_;
  int64_t disk_size_;

  explicit ExtentStats() : data_size_(0), 
                           num_entries_(0), 
                           num_deletes_(0), 
                           disk_size_(0) {}

  explicit ExtentStats(int64_t data_size,
                       int64_t num_entries,
                       int64_t num_deletes,
                       int64_t disk_size)
      : data_size_(data_size),
        num_entries_(num_entries),
        num_deletes_(num_deletes),
        disk_size_(disk_size)
  {
  }

  inline void reset()
  {
    data_size_ = 0;
    num_entries_ = 0;
    num_deletes_ = 0;
    disk_size_ = 0;
  }

  bool operator==(const ExtentStats &extent_stats)
  {
    return data_size_ == extent_stats.data_size_
           && num_entries_ == extent_stats.num_entries_
           && num_deletes_ == extent_stats.num_deletes_
           && disk_size_ == extent_stats.disk_size_;
  }

  inline void merge(const ExtentStats &extent_stats)
  {
    data_size_ += extent_stats.data_size_;
    num_entries_ += extent_stats.num_entries_;
    num_deletes_ += extent_stats.num_deletes_;
    disk_size_ += extent_stats.disk_size_;
  }

  DECLARE_AND_DEFINE_TO_STRING(KV_(data_size), KV_(num_entries), KV_(num_deletes), KV_(disk_size));
};

enum ExtentSpaceType
{
  HOT_EXTENT_SPACE = 0,
  WARM_EXTENT_SPACE = 1,
  COLD_EXTENT_SPACE = 2,
  MAX_EXTENT_SPACE
};

inline bool is_valid_extent_space_type(const int32_t extent_space_type)
{
  return extent_space_type >= HOT_EXTENT_SPACE && extent_space_type < MAX_EXTENT_SPACE;
}

struct CreateTableSpaceArgs
{
  int64_t table_space_id_;
  std::vector<common::DbPath> db_paths_;

  CreateTableSpaceArgs()
    : table_space_id_(0),
      db_paths_()
  {
  }
  CreateTableSpaceArgs(int64_t table_space_id,
                       const std::vector<common::DbPath> db_paths)
      : table_space_id_(table_space_id),
        db_paths_(db_paths)
  {
  }
  ~CreateTableSpaceArgs()
  {
  }

  bool is_valid() const
  {
    return table_space_id_ >= 0
           && db_paths_.size() > 0;
  }

  DECLARE_AND_DEFINE_TO_STRING(KV_(table_space_id));
};

struct CreateExtentSpaceArgs
{
  int64_t table_space_id_;
  int32_t extent_space_type_;
  common::DbPath db_path_;

  CreateExtentSpaceArgs() : table_space_id_(0),
                            extent_space_type_(HOT_EXTENT_SPACE),
                            db_path_()
  {
  }
  CreateExtentSpaceArgs(int64_t table_space_id,
                        int32_t extent_space_type,
                        common::DbPath db_path)
      : table_space_id_(table_space_id),
        extent_space_type_(extent_space_type),
        db_path_(db_path)
  {
  }
  ~CreateExtentSpaceArgs()
  {
  }

  bool is_valid() const
  {
    return table_space_id_ >= 0
           && (extent_space_type_ >= HOT_EXTENT_SPACE && extent_space_type_ <= COLD_EXTENT_SPACE);
  }

  DECLARE_AND_DEFINE_TO_STRING(KV_(table_space_id), KV_(extent_space_type));
};

struct CreateDataFileArgs
{
  int64_t table_space_id_;
  int32_t extent_space_type_;
  int64_t file_number_;
  std::string data_file_path_;

  CreateDataFileArgs()
      : table_space_id_(0),
        extent_space_type_(0),
        file_number_(0),
        data_file_path_()
  {
  }
  CreateDataFileArgs(int64_t table_space_id,
                     int32_t extent_space_type,
                     int64_t file_number,
                     std::string data_file_path)
      : table_space_id_(table_space_id),
        extent_space_type_(extent_space_type),
        file_number_(file_number),
        data_file_path_(data_file_path)
  {
  }
  ~CreateDataFileArgs()
  {
  }

  bool is_valid() const
  {
    return table_space_id_ >= 0
           && (extent_space_type_ >= HOT_EXTENT_SPACE && extent_space_type_ <= HOT_EXTENT_SPACE)
           && file_number_ >= 0;
  }
  DECLARE_AND_DEFINE_TO_STRING(KV_(table_space_id), KV_(extent_space_type), KV_(file_number), KV_(data_file_path));
};

struct ShrinkCondition
{
  int64_t max_free_extent_percent_;
  int64_t shrink_allocate_interval_;
  int64_t max_shrink_extent_count_;

  ShrinkCondition()
      : max_free_extent_percent_(0),
        shrink_allocate_interval_(0),
        max_shrink_extent_count_(0)
  {
  }
  ShrinkCondition(int64_t max_free_extent_percent, int64_t shrink_allocate_interval, int64_t max_shrink_extent_count)
      : max_free_extent_percent_(max_free_extent_percent),
        shrink_allocate_interval_(shrink_allocate_interval),
        max_shrink_extent_count_(max_shrink_extent_count)
  {
  }
  ~ShrinkCondition()
  {
  }

  bool is_valid() const
  {
    return max_free_extent_percent_ >=0 && shrink_allocate_interval_ >= 0 && max_free_extent_percent_ >= 0;
  }

  bool operator==(const ShrinkCondition &shrink_condition)
  {
    return max_free_extent_percent_ == shrink_condition.max_free_extent_percent_
           && shrink_allocate_interval_ == shrink_condition.shrink_allocate_interval_
           && max_shrink_extent_count_ == shrink_condition.max_shrink_extent_count_;
  }

  DECLARE_AND_DEFINE_TO_STRING(KV_(max_free_extent_percent), KV_(shrink_allocate_interval), KV_(max_shrink_extent_count));
};

struct ShrinkInfo
{
  ShrinkCondition shrink_condition_;
  int64_t table_space_id_;
  int32_t extent_space_type_;
  std::set<int64_t> index_id_set_;
  int64_t total_need_shrink_extent_count_;
  int64_t shrink_extent_count_;

  ShrinkInfo()
      : shrink_condition_(),
        table_space_id_(0),
        extent_space_type_(0), 
        index_id_set_(),
        total_need_shrink_extent_count_(0),
        shrink_extent_count_(0)
  {
  }
  ShrinkInfo(const ShrinkInfo &shrink_info)
      : shrink_condition_(shrink_info.shrink_condition_),
        table_space_id_(shrink_info.table_space_id_),
        extent_space_type_(shrink_info.extent_space_type_),
        index_id_set_(shrink_info.index_id_set_),
        total_need_shrink_extent_count_(shrink_info.total_need_shrink_extent_count_),
        shrink_extent_count_(shrink_info.shrink_extent_count_)
  {
  }
  ~ShrinkInfo()
  {
  }

  bool is_valid() const
  {
    return shrink_condition_.is_valid()
           && table_space_id_ >= 0
           && is_valid_extent_space_type(extent_space_type_)
           && total_need_shrink_extent_count_ > 0
           && shrink_extent_count_ > 0;
  }

  bool operator==(const ShrinkInfo &shrink_info)
  {
    return shrink_condition_ == shrink_info.shrink_condition_
           && table_space_id_ == shrink_info.table_space_id_
           && extent_space_type_ == shrink_info.extent_space_type_
           && index_id_set_ == shrink_info.index_id_set_
           && total_need_shrink_extent_count_ == shrink_info.total_need_shrink_extent_count_
           && shrink_extent_count_ == shrink_info.shrink_extent_count_;
  }

  DECLARE_AND_DEFINE_TO_STRING(KV_(shrink_condition), KV_(table_space_id), KV_(extent_space_type), KV_(total_need_shrink_extent_count), KV_(shrink_extent_count));
};

struct ExtentIOInfo
{
  int fd_;
  ExtentId extent_id_;
  int64_t extent_size_;
  int64_t block_size_;
  int64_t unique_id_;

  ExtentIOInfo()
      : fd_(-1),
        extent_id_(),
        extent_size_(0),
        block_size_(0),
        unique_id_(0)
  {
  }
  ExtentIOInfo(const int fd,
               const ExtentId extent_id,
               int64_t extent_size,
               int64_t block_size,
               int64_t unique_id)
      : fd_(fd),
        extent_id_(extent_id),
        extent_size_(extent_size),
        block_size_(block_size),
        unique_id_(unique_id)
  {
  }
  ~ExtentIOInfo()
  {
  }
  
  bool is_valid() const
  {
    return fd_ >= 0 && extent_size_ > 0 && block_size_ > 0 && unique_id_ >= 0;
  }

  void reset()
  {
    fd_ = -1;
    extent_id_.reset();
    extent_size_ = 0;
    block_size_ = 0;
    unique_id_ = 0;
  }
  void set_param(const int fd,
                 const ExtentId extent_id,
                 const int64_t extent_size,
                 const int64_t block_size,
                 const int64_t unique_id)
  {
    fd_ = fd;
    extent_id_ = extent_id;
    extent_size_ = extent_size;
    block_size_ = block_size;
    unique_id_ = unique_id;
  }
  void set_fd(const int fd) { fd_ = fd; }
  void set_extent_id(const ExtentId extent_id) { extent_id_ = extent_id; }
  void set_extent_size(const int64_t extent_size) { extent_size_ = extent_size; }
  void set_block_size(const int64_t block_size) { block_size_ = block_size; }
  void set_unique_id(const int64_t unique_id) { unique_id_ = unique_id; }
  int64_t get_offset() const { return extent_id_.offset * extent_size_; }
  int get_fd() const { return fd_; }
  ExtentId get_extent_id() const { return extent_id_; }
  int64_t get_extent_size() const { return extent_size_; }
  int64_t get_block_size() const { return block_size_; }
  int64_t get_unique_id() const { return unique_id_; }
  
  DECLARE_AND_DEFINE_TO_STRING(KV_(fd), KV_(extent_id), KV_(extent_size), KV_(block_size), KV_(unique_id));
};

struct DataFileStatistics
{
  int64_t table_space_id_;
  int32_t extent_space_type_;
  int64_t file_number_;
  int64_t total_extent_count_;
  int64_t used_extent_count_;
  int64_t free_extent_count_;

  DataFileStatistics()
      : table_space_id_(0),
        extent_space_type_(0),
        file_number_(0),
        total_extent_count_(0),
        used_extent_count_(0),
        free_extent_count_(0)
  {
  }
  DataFileStatistics(int64_t table_space_id,
                     int32_t extent_space_type,
                     int64_t file_number,
                     int64_t total_extent_count,
                     int64_t used_extent_count,
                     int64_t free_extent_count)
      : table_space_id_(table_space_id),
        extent_space_type_(extent_space_type),
        file_number_(file_number),
        total_extent_count_(total_extent_count),
        used_extent_count_(used_extent_count),
        free_extent_count_(free_extent_count)
  {
  }
  ~DataFileStatistics()
  {
  }

  void reset()
  {
    table_space_id_ = 0;
    extent_space_type_ = 0;
    file_number_ = 0;
    total_extent_count_ = 0;
    used_extent_count_ = 0;
    free_extent_count_ = 0;
  }

  DECLARE_AND_DEFINE_TO_STRING(KV_(table_space_id), KV_(extent_space_type), KV_(total_extent_count),
      KV_(used_extent_count), KV_(free_extent_count));
};

struct EstimateCostStats {
  int64_t subtable_id_;
  int64_t cost_size_;
  int64_t total_extent_cnt_;
  int64_t total_open_extent_cnt_;
  bool recalc_last_extent_;

  EstimateCostStats();
  ~EstimateCostStats();

  void reset();
  DECLARE_TO_STRING();
};

} //namespace xengine
} //namespace xengine

#endif
