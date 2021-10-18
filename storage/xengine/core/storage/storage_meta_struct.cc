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

#include "logger/logger.h"
#include "storage/storage_meta_struct.h"
#include "db/version_edit.h"
#include "memory/modtype_define.h"
#include "storage_common.h"

namespace xengine
{
using namespace util;
namespace storage
{
SubTableMeta::SubTableMeta()
    : index_id_(-1),
      index_no_(-1),
      index_type_(0),
      row_count_(0),
      data_size_(0),
      recovery_point_(),
      table_space_id_(0)
{
}

SubTableMeta::~SubTableMeta()
{
}

void SubTableMeta::reset()
{
  index_id_ = -1;
  index_no_ = -1;
  row_count_ = 0;
  data_size_ = 0;
  recovery_point_.reset();
  table_space_id_ = 0;
}

bool SubTableMeta::is_valid()
{
  return index_id_ >= 0 && index_no_ >= 0 && index_type_ >= 0 && row_count_ >= 0 && data_size_ >= 0 && recovery_point_.is_valid() && table_space_id_ >= 0;
}


DEFINE_COMPACTIPLE_SERIALIZATION(SubTableMeta, index_id_, index_no_, index_type_, row_count_, data_size_, recovery_point_, table_space_id_);
DEFINE_TO_STRING(SubTableMeta, KV_(index_id), KV_(index_no), KV_(index_type), KV_(row_count), KV_(data_size), KV_(recovery_point), KV_(table_space_id));

ExtentMeta::ExtentMeta()
    : attr_(0),
      smallest_key_(),
      largest_key_(),
      extent_id_(),
      smallest_seqno_(0),
      largest_seqno_(0),
      refs_(0),
      data_size_(0),
      index_size_(0),
      num_data_blocks_(0),
      num_entries_(0),
      table_space_id_(0),
      extent_space_type_(HOT_EXTENT_SPACE)
{
}

ExtentMeta::ExtentMeta(uint8_t attr,
                       ExtentId extent_id,
                       const db::FileMetaData &file_meta,
                       const table::TableProperties &table_properties,
                       int64_t table_space_id,
                       int32_t extent_space_type)
    : attr_(attr),
      smallest_key_(file_meta.smallest),
      largest_key_(file_meta.largest),
      extent_id_(extent_id),
      smallest_seqno_(file_meta.smallest_seqno),
      largest_seqno_(file_meta.largest_seqno),
      refs_(0),
      data_size_(table_properties.data_size),
      index_size_(table_properties.index_size),
      num_data_blocks_(table_properties.num_data_blocks),
      num_entries_(table_properties.num_entries),
      num_deletes_(table_properties.num_deletes),
      table_space_id_(table_space_id),
      extent_space_type_(extent_space_type)
{
}

ExtentMeta::ExtentMeta(const ExtentMeta &extent_meta)
    : attr_(extent_meta.attr_),
      smallest_key_(extent_meta.smallest_key_),
      largest_key_(extent_meta.largest_key_),
      extent_id_(extent_meta.extent_id_),
      smallest_seqno_(extent_meta.smallest_seqno_),
      largest_seqno_(extent_meta.largest_seqno_),
      refs_(0),
      data_size_(extent_meta.data_size_),
      index_size_(extent_meta.index_size_),
      num_data_blocks_(extent_meta.num_data_blocks_),
      num_entries_(extent_meta.num_entries_),
      num_deletes_(extent_meta.num_deletes_),
      table_space_id_(extent_meta.table_space_id_),
      extent_space_type_(extent_meta.extent_space_type_)
{
}

ExtentMeta::~ExtentMeta()
{
}

int ExtentMeta::deep_copy(ExtentMeta *&extent_meta) const
{
  int ret = common::Status::kOk;
  extent_meta = nullptr;
  char *tmp_buf = nullptr;
  int64_t size = 0;

  size = get_deep_copy_size();
  if (nullptr == (tmp_buf = reinterpret_cast<char *>(base_malloc(size, memory::ModId::kExtentSpaceMgr)))) {
    ret = common::Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for tmp buf", K(ret), K(size));
  } else if (nullptr == (extent_meta = new (tmp_buf) ExtentMeta())) {
    ret = common::Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "fail to constructe ExtentMeta", K(ret));
  } else {
    extent_meta->attr_ = attr_;
    extent_meta->smallest_key_ = smallest_key_;
    extent_meta->largest_key_ = largest_key_;
    extent_meta->extent_id_ = extent_id_;
    extent_meta->smallest_seqno_ = smallest_seqno_;
    extent_meta->largest_seqno_ = largest_seqno_;
    extent_meta->refs_ = refs_;
    extent_meta->data_size_ = data_size_;
    extent_meta->index_size_ = index_size_;
    extent_meta->num_data_blocks_ = num_data_blocks_;
    extent_meta->num_entries_ = num_entries_;
    extent_meta->num_deletes_ = num_deletes_;
    extent_meta->table_space_id_ = table_space_id_;
    extent_meta->extent_space_type_ = extent_space_type_;
  }

  return ret;
}

int ExtentMeta::deep_copy(memory::SimpleAllocator &allocator, ExtentMeta *&extent_meta) const
{
  int ret = common::Status::kOk;
  char *buf = nullptr;
  int64_t size = 0;
  extent_meta = nullptr;

  size = get_deep_copy_size();
  if (nullptr == (buf = reinterpret_cast<char *>(allocator.alloc(size)))) {
    ret = common::Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for buf", K(ret));
  } else if (nullptr == (extent_meta = new (buf) ExtentMeta())) {
    ret = common::Status::kCorruption;
    XENGINE_LOG(WARN, "fail to constructor extent meta", K(ret));
  } else {
    extent_meta->attr_ = attr_;
    extent_meta->smallest_key_ = smallest_key_;
    extent_meta->largest_key_ = largest_key_;
    extent_meta->extent_id_ = extent_id_;
    extent_meta->smallest_seqno_ = smallest_seqno_;
    extent_meta->largest_seqno_ = largest_seqno_;
    extent_meta->refs_ = refs_;
    extent_meta->data_size_ = data_size_;
    extent_meta->index_size_ = index_size_;
    extent_meta->num_data_blocks_ = num_data_blocks_;
    extent_meta->num_entries_ = num_entries_;
    extent_meta->num_deletes_ = num_deletes_;
    extent_meta->table_space_id_ = table_space_id_;
    extent_meta->extent_space_type_ = extent_space_type_;
  }
  return ret;
}

int64_t ExtentMeta::get_deep_copy_size() const
{
  return sizeof(ExtentMeta);
}
DEFINE_COMPACTIPLE_SERIALIZATION(ExtentMeta, attr_, smallest_key_, largest_key_, extent_id_, smallest_seqno_, largest_seqno_,
    data_size_, index_size_, num_data_blocks_, num_entries_, num_deletes_,
    table_space_id_, extent_space_type_);
DEFINE_TO_STRING(ExtentMeta, KV_(attr), KV_(smallest_key), KV_(largest_key), KV_(extent_id), KV_(smallest_seqno), KV_(largest_seqno), KV_(refs), KV_(data_size), KV_(index_size),
    KV_(num_data_blocks), KV_(num_entries), KV_(num_deletes), KV_(table_space_id), KV_(extent_space_type));
} //namespace storage
} //namespace xengine
