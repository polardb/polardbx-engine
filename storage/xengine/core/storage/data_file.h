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

#ifndef XENGINE_INCLUDE_DATA_FILE_H_
#define XENGINE_INCLUDE_DATA_FILE_H_

#include <bitset>
#include "util/heap.h"
#include "util/serialization.h"
#include "util/to_string.h"
#include "xengine/env.h"
#include "io_extent.h"
#include "storage_common.h"

namespace xengine
{
namespace storage
{
struct ChangeInfo;
struct DataFileHeader
{
public:
  static const int64_t DATA_FILE_HEADER_VERSION_V1 = 1;
  static const int64_t DATA_FILE_HEADER_VERSION = 2;
  static const int64_t DATA_FILE_HEADER_MAGIC_NUMBER = 0x0F1E2D3C4B5A6978;
public:
  int32_t header_size_;
  int32_t header_version_;
  int32_t start_extent_;
  int32_t extent_size_;
  int32_t data_block_size_;
  int32_t file_number_;
  int32_t used_extent_number_;
  int32_t total_extent_number_;
  int32_t bitmap_offset_;
  int64_t space_file_size_;
  int64_t create_timestamp_;
  int64_t modified_timestamp_;
  int64_t magic_number_;
  char filename_[MAX_FILE_PATH_SIZE];
  char bitmap_[MAX_EXTENT_NUM];
  char reserved_[RESERVED_SIZE];
  int64_t checksum_;
  int64_t table_space_id_;
  int32_t extent_space_type_;

  DataFileHeader();
  ~DataFileHeader();
  bool is_valid() const
  {
    return header_size_ > 0 && file_number_ >= 0 && table_space_id_ >= 0
      && extent_space_type_ >= 0;
  }
  DataFileHeader &operator=(const DataFileHeader &data_file_header);
  bool operator==(const DataFileHeader &data_file_header) const;

  DECLARE_TO_STRING();
  DECLARE_SERIALIZATION();
};

class DataFile
{
public:
  DataFile(util::Env *env, const util::EnvOptions &env_options);
  ~DataFile();
  void destroy();

  //file relative function
  int create(const CreateDataFileArgs &arg);
  int open(const std::string &file_path);
  int remove();
  int close();

  //extent relative function
  int allocate(ExtentIOInfo &io_info);
  int recycle(const ExtentId extent_id);
  // mark the extent used, only used during recovery
  int reference(const ExtentId extent_id, ExtentIOInfo &io_info);

  //shrink relative function
  int shrink(const int64_t shrink_extent_count);

  //statistic relative function
  int get_extent_io_info(const ExtentId extent_id, ExtentIOInfo &io_info);
  int64_t get_table_space_id() const { return data_file_header_.table_space_id_; }
  int32_t get_extent_space_type() const { return data_file_header_.extent_space_type_; }
  int64_t get_file_number() const { return data_file_header_.file_number_; }
  int64_t get_free_extent_count() const { return free_extent_count_; }
  int64_t get_total_extent_count() const { return total_extent_count_; }
  int get_data_file_statistics(DataFileStatistics &data_file_statistic);
  int64_t calc_tail_continuous_free_extent_count() const;
  bool has_free_extent() const { return !offset_heap_.empty(); }
  bool is_free() const { return free_extent_count_ == (total_extent_count_ - 1); }
  bool is_full() const
  {
    return MAX_TOTAL_EXTENT_COUNT == total_extent_count_
           && offset_heap_.empty();
  }
  bool is_free_extent(const ExtentId &extent_id) const
  { 
    return is_free_extent(extent_id.offset);
  }

  //recovery relative function
  int rebuild();
  bool is_garbage_file() const { return garbage_file_; }

#ifndef NDEBUG
  //test function
  void TEST_inject_fallocate_failed();
  bool TEST_is_fallocate_failed();
  void TEST_inject_double_write_header_failed();
  bool TEST_is_double_write_header_failed();
#endif
private:
  //int create_file(const std::string file_path);
  int init_header(const CreateDataFileArgs &arg);
  int double_write_header();
  int double_load_header(const std::string &file_path);
  int init_fallocate_file();
  int load_header(const common::Slice &header_buf, const int64_t offset, DataFileHeader &data_file_header);
  int allocate_extent(ExtentId &extent_id); 
  int expand_space(int64_t expand_extent_cnt);
  bool need_expand_space() const;
  int64_t calc_expand_extent_count();
  int truncate(const int64_t truncate_extent_count);
  int rebuild_free_extent();
  int update_data_file_size(const int64_t total_extent_count);
  int internal_get_random_access_extent(const ExtentId &extent_id,
                                        RandomAccessExtent &random_access_extent);
  int move_extent(const int32_t origin_offset,
                  char *extent_buf,
                  ChangeInfo &change_info);
  bool is_free_extent(const int32_t offset) const
  {
    return !extents_bit_map_.test(offset);
  }
  bool is_used_extent(const int32_t offset) const
  {
    return extents_bit_map_.test(offset);
  }
  void set_free_status(const int32_t offset)
  {
    extents_bit_map_.set(offset, false);
  }
  void set_used_status(const int32_t offset)
  {
    extents_bit_map_.set(offset, true);
  }

  void inc_free_extent_count(int64_t extent_count)
  {
    free_extent_count_ += extent_count;
  }
  void dec_free_extent_count(int64_t extent_count)
  {
    free_extent_count_ -= extent_count;
  }
  int check_garbage_file(const std::string &file_path, const common::Slice &header_buf);
private:
  typedef util::BinaryHeap<int32_t, std::greater<int32_t> > ExtentOffsetHeap;
  static const int64_t HEADER_BUFFER_SIZE = 16 * 1024; //16KB
  static const int64_t MASTER_HEADER_OFFSET = 0;
  static const int64_t BACKUP_HEADER_OFFSET = 16 * 1024; //16KB
  static const int64_t INITIALIZE_EXTENT_COUNT = 2;
  static const int64_t MAX_EXPAND_EXTENT_COUNT = 32;
  static const int64_t MAX_TOTAL_EXTENT_COUNT = 5120;
  static const double DEFAULT_EXPAND_PERCENT;
private:
  bool is_inited_;
  util::Env *env_;
  const util::EnvOptions &env_options_;
  util::RandomRWFile *file_;
  int fd_;
  DataFileHeader data_file_header_;
  //TODO:yuanfeng, set after recovery
  std::string data_file_path_;
  int64_t total_extent_count_;
  int64_t free_extent_count_;
  std::bitset<MAX_TOTAL_EXTENT_COUNT + 1> extents_bit_map_;
  ExtentOffsetHeap offset_heap_;
  bool garbage_file_;
#ifndef NDEBUG
  bool test_fallocated_failed_;
  bool test_double_write_header_failed_;
#endif
};

} //namespace storage
} //namespace xengine

#endif
