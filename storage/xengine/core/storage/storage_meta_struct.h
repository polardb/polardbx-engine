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

#ifndef XENGINE_INCLUDE_STORAGE_META_STRUCT_H_
#define XENGINE_INCLUDE_STORAGE_META_STRUCT_H_
#include "memory/allocator.h"
#include "storage/io_extent.h"
#include "db/dbformat.h"
#include "db/recovery_point.h"

namespace xengine
{
namespace db
{
  struct FileMetaData;
}
namespace storage
{
struct ExtentId;
//for compatibility, the variables in this struct must not been deleted or moved.
//new variables should only been added at the end.
class SubTableMeta
{
public:
  static const int32_t SSTABLE_META_VERSION = 1;
  int64_t index_id_;//primary table: index_id_ == table_id_. index_table: index_id_ = index_number
  int32_t index_no_;
  int64_t index_type_;
  int64_t row_count_;
  int64_t data_size_;
  db::RecoveryPoint recovery_point_;
  int64_t table_space_id_;

  SubTableMeta();
  ~SubTableMeta();
  void reset();
  bool is_valid();

  DECLARE_COMPACTIPLE_SERIALIZATION(SSTABLE_META_VERSION);
  DECLARE_TO_STRING();
};

//for compatibility, the variables in this struct must not been deleted or moved.
//new variables should only been added at the end.
struct ExtentMeta
{
public:
  static const int64_t EXTENT_META_VERSION = 1;
  static const uint8_t F_INIT_EXTENT = 0X0;
  static const uint8_t F_NORMAL_EXTENT = 0X1;
  static const uint8_t F_LARGE_OBJECT_EXTENT = 0X2;
public:
  uint8_t attr_; //0:init extent; 1: normal extent; 2: large_object extent
  db::InternalKey smallest_key_;
  db::InternalKey largest_key_;
  ExtentId extent_id_;
  common::SequenceNumber smallest_seqno_;
  common::SequenceNumber largest_seqno_;
  int32_t refs_;
  int32_t data_size_;
  int32_t index_size_;
  int32_t num_data_blocks_;
  int32_t num_entries_;
  int32_t num_deletes_;
  int64_t table_space_id_;
  int32_t extent_space_type_;

  ExtentMeta();
  ExtentMeta(uint8_t attr,
             ExtentId extent_id,
             const db::FileMetaData &file_meta,
             const table::TableProperties &table_properties,
             int64_t table_space_id,
             int32_t extent_space_type);
  ExtentMeta(const ExtentMeta &extent_meta);
  ~ExtentMeta();
  int deep_copy(ExtentMeta *&extent_meta) const;
  int deep_copy(memory::SimpleAllocator &allocator, ExtentMeta *&extent_meta) const;
  int64_t get_deep_copy_size() const;
  inline void ref() { ++refs_; }
  inline bool unref() { return --refs_ <= 0 ? true : false; }
  inline bool is_largest_object_extent() { return attr_ & F_LARGE_OBJECT_EXTENT; }
  inline bool is_normal_extent() { return attr_ & F_NORMAL_EXTENT; }
  DECLARE_TO_STRING();
  DECLARE_COMPACTIPLE_SERIALIZATION(EXTENT_META_VERSION);
};

} //namespace storage
} //namespace xengine

#endif
