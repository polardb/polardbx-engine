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

#include "env/io_posix.h"
#include "util/increment_number_allocator.h"
#include "util/sync_point.h"
#include "xengine/xengine_constants.h"
#include "change_info.h"
#include "data_file.h"

namespace xengine
{
using namespace common;
using namespace util;
namespace storage
{
DataFileHeader::DataFileHeader()
    : header_size_(sizeof(DataFileHeader)),
      header_version_(DATA_FILE_HEADER_VERSION),
      start_extent_(1),
      extent_size_(MAX_EXTENT_SIZE),
      data_block_size_(DATA_BLOCK_SIZE),
      file_number_(-1),
      used_extent_number_(0),
      total_extent_number_(0),
      bitmap_offset_(0),
      space_file_size_(0),
      create_timestamp_(0),
      modified_timestamp_(0),
      magic_number_(DATA_FILE_HEADER_MAGIC_NUMBER),
      checksum_(0),
      table_space_id_(0),
      extent_space_type_(HOT_EXTENT_SPACE)
{
  memset(filename_, 0, MAX_FILE_PATH_SIZE);
  memset(bitmap_, 0, MAX_EXTENT_NUM);
  memset(reserved_, 0, RESERVED_SIZE);
}

DataFileHeader::~DataFileHeader()
{
}

DataFileHeader &DataFileHeader::operator=(const DataFileHeader &data_file_header)
{
  header_size_ = data_file_header.header_size_;
  header_version_ = data_file_header.header_version_;
  start_extent_ = data_file_header.start_extent_;
  extent_size_ = data_file_header.extent_size_;
  data_block_size_ = data_file_header.data_block_size_;
  file_number_ = data_file_header.file_number_;
  used_extent_number_ = data_file_header.used_extent_number_;
  total_extent_number_ = data_file_header.total_extent_number_;
  bitmap_offset_ = data_file_header.bitmap_offset_;
  space_file_size_ = data_file_header.space_file_size_;
  create_timestamp_ = data_file_header.create_timestamp_;
  modified_timestamp_ = data_file_header.modified_timestamp_;
  magic_number_ = data_file_header.magic_number_;
  checksum_ = data_file_header.checksum_;
  table_space_id_ = data_file_header.table_space_id_;
  extent_space_type_ = data_file_header.extent_space_type_;
  return *this;  
}

bool DataFileHeader::operator==(const DataFileHeader &data_file_header) const
{
  return header_size_ == data_file_header.header_size_
         && header_version_ == data_file_header.header_version_
         && extent_size_ == data_file_header.extent_size_
         && data_block_size_ == data_file_header.data_block_size_
         && file_number_ == data_file_header.file_number_
         && magic_number_ == data_file_header.magic_number_
         && table_space_id_ == data_file_header.table_space_id_
         && extent_space_type_ == data_file_header.extent_space_type_;
}

DEFINE_TO_STRING(DataFileHeader, KV_(header_size), KV_(header_version), KV_(start_extent),
    KV_(extent_size), KV_(data_block_size), KV_(file_number),
    KV_(used_extent_number), KV_(total_extent_number), KV_(bitmap_offset),
    KV_(space_file_size), KV_(create_timestamp), KV_(modified_timestamp),
    KV_(magic_number), KV_(filename), KV_(bitmap), KV_(table_space_id), KV_(extent_space_type));

int DataFileHeader::serialize(char *buf, int64_t buf_length, int64_t &pos) const
{
  int ret = Status::kOk;

  if (FAILED(util::serialize(buf, buf_length, pos, header_size_))) {
    XENGINE_LOG(WARN, "fail to serialize header size", K(ret));
  } else if (FAILED(util::serialize(buf, buf_length, pos, header_version_))) {
    XENGINE_LOG(WARN, "fail to serialize header version", K(ret));
  } else if (FAILED(util::serialize(buf, buf_length, pos, start_extent_))) {
    XENGINE_LOG(WARN, "fail to serialize start extent", K(ret));
  } else if (FAILED(util::serialize(buf, buf_length, pos, extent_size_))) {
    XENGINE_LOG(WARN,  "fail to serialize extent size", K(ret));
  } else if (FAILED(util::serialize(buf, buf_length, pos, data_block_size_))) {
    XENGINE_LOG(WARN, "fail to serialize data block size", K(ret));
  } else if (FAILED(util::serialize(buf, buf_length, pos, file_number_))) {
    XENGINE_LOG(WARN, "fail to serialize file number", K(ret));
  } else if (FAILED(util::serialize(buf, buf_length, pos, used_extent_number_))) {
    XENGINE_LOG(WARN, "fail to serialize used extent number", K(ret));
  } else if (FAILED(util::serialize(buf, buf_length, pos, total_extent_number_))) {
    XENGINE_LOG(WARN, "fail to serialize total extent number", K(ret));
  } else if (FAILED(util::serialize(buf, buf_length, pos, bitmap_offset_))) {
    XENGINE_LOG(WARN, "fail to serialize bitmap offset", K(ret));
  } else if (FAILED(util::serialize(buf, buf_length, pos, space_file_size_))) {
    XENGINE_LOG(WARN, "fail to serialize space file size", K(ret));
  } else if (FAILED(util::serialize(buf, buf_length, pos, create_timestamp_))) {
    XENGINE_LOG(WARN, "fail to serialize create timestamp", K(ret));
  } else if (FAILED(util::serialize(buf, buf_length, pos, modified_timestamp_))) {
    XENGINE_LOG(WARN, "fail to serialize modified timestamp", K(ret));
  } else if (FAILED(util::serialize(buf, buf_length, pos, magic_number_))) {
    XENGINE_LOG(WARN, "fail to serialize magic number", K(ret));
  } else if (FAILED(util::serialize(buf, buf_length, pos, filename_))) {
    XENGINE_LOG(WARN, "fail to serialize filename", K(ret));
  } else if (FAILED(util::serialize(buf, buf_length, pos, bitmap_))) {
    XENGINE_LOG(WARN, "fail to serialize bitmap", K(ret));
  } else {
    if (DATA_FILE_HEADER_VERSION == header_version_) {
      if (FAILED(util::serialize(buf, buf_length, pos, table_space_id_))) {
        XENGINE_LOG(WARN, "fail to serialize table space id", K(ret));
      } else if (FAILED(util::serialize(buf, buf_length, pos, extent_space_type_))) {
        XENGINE_LOG(WARN, "fail to seriaize extent space type", K(ret));
      }
    }
  }

  return ret;
}

int DataFileHeader::deserialize(const char *buf, int64_t buf_length, int64_t &pos)
{
  int ret = Status::kOk;

  if (FAILED(util::deserialize(buf, buf_length, pos, header_size_))) {
    XENGINE_LOG(WARN, "fail to deserialize header size", K(ret));
  } else if (FAILED(util::deserialize(buf, buf_length, pos, header_version_))) {
    XENGINE_LOG(WARN, "fail to deserialize header version", K(ret));
  } else if (FAILED(util::deserialize(buf, buf_length, pos, start_extent_))) {
    XENGINE_LOG(WARN, "fail to deserialize start extent", K(ret));
  } else if (FAILED(util::deserialize(buf, buf_length, pos, extent_size_))) {
    XENGINE_LOG(WARN,  "fail to deserialize extent size", K(ret));
  } else if (FAILED(util::deserialize(buf, buf_length, pos, data_block_size_))) {
    XENGINE_LOG(WARN, "fail to deserialize data block size", K(ret));
  } else if (FAILED(util::deserialize(buf, buf_length, pos, file_number_))) {
    XENGINE_LOG(WARN, "fail to deserialize file number", K(ret));
  } else if (FAILED(util::deserialize(buf, buf_length, pos, used_extent_number_))) {
    XENGINE_LOG(WARN, "fail to deserialize used extent number", K(ret));
  } else if (FAILED(util::deserialize(buf, buf_length, pos, total_extent_number_))) {
    XENGINE_LOG(WARN, "fail to deserialize total extent number", K(ret));
  } else if (FAILED(util::deserialize(buf, buf_length, pos, bitmap_offset_))) {
    XENGINE_LOG(WARN, "fail to deserialize bitmap offset", K(ret));
  } else if (FAILED(util::deserialize(buf, buf_length, pos, space_file_size_))) {
    XENGINE_LOG(WARN, "fail to deserialize space file size", K(ret));
  } else if (FAILED(util::deserialize(buf, buf_length, pos, create_timestamp_))) {
    XENGINE_LOG(WARN, "fail to deserialize create timestamp", K(ret));
  } else if (FAILED(util::deserialize(buf, buf_length, pos, modified_timestamp_))) {
    XENGINE_LOG(WARN, "fail to deserialize modified timestamp", K(ret));
  } else if (FAILED(util::deserialize(buf, buf_length, pos, magic_number_))) {
    XENGINE_LOG(WARN, "fail to deserialize magic number", K(ret));
  } else if (FAILED(util::deserialize(buf, buf_length, pos, filename_))) {
    XENGINE_LOG(WARN, "fail to deserialize filename", K(ret));
  } else if (FAILED(util::deserialize(buf, buf_length, pos, bitmap_))) {
    XENGINE_LOG(WARN, "fail to deserialize bitmap", K(ret));
  } else {
    if (DATA_FILE_HEADER_VERSION == header_version_) {
      if (FAILED(util::deserialize(buf, buf_length, pos, table_space_id_))) {
        XENGINE_LOG(WARN, "fail to deserialize table space id", K(ret));
      } else if (FAILED(util::deserialize(buf, buf_length, pos, extent_space_type_))) {
        XENGINE_LOG(WARN, "fail to seriaize extent space type", K(ret));
      }
    }
  }

  return ret;
}

int64_t DataFileHeader::get_serialize_size() const
{
  int64_t size = 0;
  size += util::get_serialize_size(header_size_);
  size += util::get_serialize_size(header_version_);
  size += util::get_serialize_size(start_extent_);
  size += util::get_serialize_size(extent_size_);
  size += util::get_serialize_size(data_block_size_);
  size += util::get_serialize_size(file_number_);
  size += util::get_serialize_size(used_extent_number_);
  size += util::get_serialize_size(total_extent_number_);
  size += util::get_serialize_size(bitmap_offset_);
  size += util::get_serialize_size(space_file_size_);
  size += util::get_serialize_size(create_timestamp_);
  size += util::get_serialize_size(modified_timestamp_);
  size += util::get_serialize_size(magic_number_);
  size += util::get_serialize_size(filename_);
  size += util::get_serialize_size(bitmap_);

  if (DATA_FILE_HEADER_VERSION == header_version_) {
    size += util::get_serialize_size(table_space_id_);
    size += util::get_serialize_size(extent_space_type_);
  }

  return size;
}

const double DataFile::DEFAULT_EXPAND_PERCENT = 0.01;
const int64_t DataFile::MAX_EXPAND_EXTENT_COUNT;
DataFile::DataFile(util::Env *env, const util::EnvOptions &env_options)
    : is_inited_(false),
      env_(env),
      env_options_(env_options),
      file_(nullptr),
      fd_(-1),
      data_file_header_(),
      data_file_path_(),
      total_extent_count_(0),
      free_extent_count_(0),
      extents_bit_map_(),
      offset_heap_(std::greater<int32_t>()),
      garbage_file_(false)
{
#ifndef NDEBUG
  test_fallocated_failed_ = false;
  test_double_write_header_failed_ = false;
#endif
}

DataFile::~DataFile()
{
  destroy();
}
  
void DataFile::destroy()
{
  if (is_inited_) {
    close();
    offset_heap_.clear();
    PosixRandomRWFile *file_ptr = reinterpret_cast<PosixRandomRWFile *>(file_);
    if (nullptr != env_options_.arena) {
      FREE_OBJECT(PosixRandomRWFile, *env_options_.arena, file_ptr);
    } else {
      MOD_DELETE_OBJECT(PosixRandomRWFile, file_ptr);
    }
    file_ = nullptr;
    is_inited_ = false;
  }
}

int DataFile::create(const CreateDataFileArgs &arg)
{
  int ret = Status::kOk;

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    XENGINE_LOG(WARN, "DataFile has been inited", K(ret), K(arg));
  } else if (UNLIKELY(!arg.is_valid())) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(arg));
  } else if (SUCCED(env_->FileExists(arg.data_file_path_).code())) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(ERROR, "unexpected error, data file should not exist", K(ret), K(arg));
  } else if (FAILED(env_->NewRandomRWFile(arg.data_file_path_, file_, env_options_).code())) {
    XENGINE_LOG(ERROR, "fail to create data file", K(ret), K(arg));
  } else if (FAILED(init_fallocate_file())) {
    XENGINE_LOG(ERROR, "fail to initialize fallocate datafile", K(ret));
  } else if (FAILED(init_header(arg))) {
    XENGINE_LOG(ERROR, "fail to init header", K(ret), K(arg));
  } else if (FAILED(double_write_header())) {
    XENGINE_LOG(ERROR, "fail to double write header", K(ret));
  } else {
    data_file_path_ = arg.data_file_path_;
    total_extent_count_ = INITIALIZE_EXTENT_COUNT;
    free_extent_count_ = INITIALIZE_EXTENT_COUNT - 1;
    offset_heap_.push(1);
    is_inited_ = true;
    XENGINE_LOG(INFO, "success to create data file", K(arg));
  }

  return ret;
}

int DataFile::open(const std::string &file_path)
{
  int ret = Status::kOk;
  uint64_t file_size = 0;

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    XENGINE_LOG(WARN, "DataFile has been inited", K(ret), K(file_path));
  } else if (FAILED(env_->FileExists(file_path).code())) {
    XENGINE_LOG(ERROR, "DataFile should been existed", K(ret), K(file_path));
  } else if (FAILED(env_->NewRandomRWFile(file_path, file_, env_options_).code())) {
    XENGINE_LOG(ERROR, "fail to create file", K(ret), K(file_path));
  } else if (FAILED(double_load_header(file_path))) {
    XENGINE_LOG(ERROR, "fail to double load header", K(ret));
  } else if (FAILED(env_->GetFileSize(file_path, &file_size).code())) {
    XENGINE_LOG(ERROR, "fail to get file size", K(ret));
  } else {
    data_file_path_ = file_path;
    total_extent_count_ = file_size / MAX_EXTENT_SIZE;
    is_inited_ = true;
    XENGINE_LOG(INFO, "success to open datafile", K(file_path), K(file_size), K_(total_extent_count));
  }

  return ret;
}

int DataFile::remove()
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "DataFile should been inited first", K(ret));
  } else if (!is_free()) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, DataFile not free, can't been remove", K(ret), K_(total_extent_count), K_(free_extent_count));
  } else {
    close();
    if (FAILED(env_->DeleteFile(data_file_path_).code())) {
      XENGINE_LOG(WARN, "fail to delete file", K(ret));
    } else {
      XENGINE_LOG(INFO, "success to delete datafile", K_(data_file_path));
    }
  }

  return ret;
}

int DataFile::close()
{
  int ret = Status::kOk;
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "DataFile should been inited first", K(ret));
  } else if (-1 == file_->get_fd()) {
    XENGINE_LOG(INFO, "the file has closed", "file_number", data_file_header_.file_number_);
  } else if (FAILED(file_->Close().code())) {
    XENGINE_LOG(WARN, "fail to close file", K(ret));
  } else {
    XENGINE_LOG(INFO, "success to close file", "file_number", data_file_header_.file_number_);
  }
  return ret;
}

int DataFile::allocate(ExtentIOInfo &io_info)
{
  int ret = Status::kOk;
  ExtentId extent_id;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "DataFile should been inited first", K(ret));
  } else if (FAILED(allocate_extent(extent_id))) {
    XENGINE_LOG(WARN, "fail to allocate extent id", K(ret));
  } else {
    io_info.set_param(file_->get_fd(),
                      extent_id,
                      data_file_header_.extent_size_,
                      data_file_header_.data_block_size_,
                      UniqueIdAllocator::get_instance().alloc());
  }

  return ret;
}

int DataFile::recycle(const ExtentId extent_id)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "DataFile should been inited first", K(ret));
  } else if (UNLIKELY(data_file_header_.file_number_ != extent_id.file_number)
             || UNLIKELY(total_extent_count_ <= extent_id.offset)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K_(data_file_header), K(extent_id), K_(total_extent_count));
  } else if (is_free_extent(extent_id.offset)) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, the extent is free", K(ret), K(extent_id));
  } else {
    offset_heap_.push(extent_id.offset);
    set_free_status(extent_id.offset);
    inc_free_extent_count(1);
  }

  return ret;
}

int DataFile::reference(const ExtentId extent_id, ExtentIOInfo &io_info)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "DataFile shoule been inited first", K(ret));
  } else if (UNLIKELY(data_file_header_.file_number_ != extent_id.file_number)
             || UNLIKELY(total_extent_count_ <= extent_id.offset)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K_(data_file_header), K(extent_id), K_(total_extent_count));
  } else if (is_used_extent(extent_id.offset)) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, the extent is used", K(ret), K(extent_id));
  } else {
    set_used_status(extent_id.offset);
    io_info.set_param(file_->get_fd(),
                      extent_id,
                      data_file_header_.extent_size_,
                      data_file_header_.data_block_size_,
                      UniqueIdAllocator::get_instance().alloc());
  }

  return ret;
}

int DataFile::shrink(const int64_t shrink_extent_count)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "DataFile should been inited first", K(ret));
  } else if (UNLIKELY(shrink_extent_count <= 0)
             || UNLIKELY(shrink_extent_count > free_extent_count_)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(shrink_extent_count), K_(free_extent_count));
  } else {
    for (int32_t i = 1; SUCCED(ret) && i <= shrink_extent_count; ++i) {
      if (is_used_extent(total_extent_count_ - i)) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "this extent should been free", K(ret), K_(total_extent_count), K(i));
      }
    }

    if (SUCCED(ret)) {
      if (FAILED(truncate(shrink_extent_count))) {
        XENGINE_LOG(WARN, "fail to truncate data file", K(ret), K_(data_file_header), K(shrink_extent_count));
      }
    }
  }

  return ret;
}

int DataFile::get_extent_io_info(const ExtentId extent_id, ExtentIOInfo &io_info)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "DataFile should been inited first", K(ret), K(extent_id));
  } else if (UNLIKELY(data_file_header_.file_number_ != extent_id.file_number)
      || UNLIKELY(total_extent_count_ <= extent_id.offset)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(extent_id), K_(data_file_header));
  } else {
    io_info.set_param(file_->get_fd(),
                      extent_id,
                      data_file_header_.extent_size_,
                      data_file_header_.data_block_size_,
                      UniqueIdAllocator::get_instance().alloc());
  }

  return ret;
}

int DataFile::get_data_file_statistics(DataFileStatistics &data_file_statistic)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "DataFile should been inited first", K(ret));
  } else {
    data_file_statistic.table_space_id_ = data_file_header_.table_space_id_;
    data_file_statistic.extent_space_type_ = data_file_header_.extent_space_type_;
    data_file_statistic.file_number_ = data_file_header_.file_number_;
    data_file_statistic.total_extent_count_ = total_extent_count_;
    data_file_statistic.used_extent_count_ = total_extent_count_ - free_extent_count_;
    data_file_statistic.free_extent_count_ = free_extent_count_;
  }

  return ret;
}

int64_t DataFile::calc_tail_continuous_free_extent_count() const
{
  int64_t continuous_free_extent_count = 0;
  for (int64_t offset = total_extent_count_ - 1; offset > 0; --offset) {
    if (is_free_extent(offset)) {
      ++continuous_free_extent_count;
    } else {
      break;
    }
  }

  return continuous_free_extent_count;
}

int DataFile::rebuild()
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "DataFile should been inited first", K(ret));
  } else if (FAILED(rebuild_free_extent())) {
    XENGINE_LOG(WARN, "fail to rebuild free extent", K(ret));
  }

  return ret;
}

#ifndef NDEBUG
void DataFile::TEST_inject_fallocate_failed()
{
  test_fallocated_failed_ = true;
}

bool DataFile::TEST_is_fallocate_failed()
{
  return test_fallocated_failed_;
}

void DataFile::TEST_inject_double_write_header_failed()
{
  test_double_write_header_failed_ = true;
}

bool DataFile::TEST_is_double_write_header_failed()
{
  return test_double_write_header_failed_;
}
#endif

int DataFile::init_header(const CreateDataFileArgs &arg)
{
  data_file_header_.table_space_id_ = arg.table_space_id_;
  data_file_header_.extent_space_type_ = arg.extent_space_type_;
  data_file_header_.file_number_ = arg.file_number_;
  
  return Status::kOk;
}

int DataFile::double_write_header()
{
  int ret = Status::kOk;
  char *header_buf = nullptr;
  const int64_t header_buf_size = MAX_EXTENT_SIZE;
  int64_t pos = 0;

#ifndef NDEBUG
  TEST_SYNC_POINT_CALLBACK("DataFile::create::inject_double_write_header_failed", this);
  if (TEST_is_double_write_header_failed()) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(ERROR, "inject double write header error", K(ret));
    return ret;
  }
#endif

  if (UNLIKELY(!data_file_header_.is_valid())) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K_(data_file_header));
  } else if (IS_NULL(header_buf = reinterpret_cast<char *>(memory::base_memalign(DIO_ALIGN_SIZE, header_buf_size, memory::ModId::kExtentSpaceMgr)))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for header buffer", K(ret));
  } else {
    memset(header_buf, 0, header_buf_size);
    if (data_file_header_.get_serialize_size() >= BACKUP_HEADER_OFFSET) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, DataFileHeader is too large", K(ret), "header_size", data_file_header_.get_serialize_size());
    } else {
      //serialize master header
      pos = MASTER_HEADER_OFFSET;
      if (FAILED(data_file_header_.serialize(header_buf, header_buf_size, pos))) {
        XENGINE_LOG(WARN, "fail to serialize space header", K(ret));
      }

      //serialize backup header
      if (SUCCED(ret)) {
        pos = BACKUP_HEADER_OFFSET;
        if (FAILED(data_file_header_.serialize(header_buf, header_buf_size, pos))) {
          XENGINE_LOG(WARN, "fail to serialize datafile header", K(ret));
        }
      }

      //wirte to datafile
      if (SUCCED(ret)) {
        if (FAILED(file_->Write(MASTER_HEADER_OFFSET, Slice(header_buf, header_buf_size)).code())) {
          XENGINE_LOG(WARN, "fail to write header to datafile", K(ret));
        }
      }
    }
  }

  if (nullptr != header_buf) {
    memory::base_memalign_free(header_buf);
    header_buf = nullptr;
  }

  return ret;
}

int DataFile::double_load_header(const std::string &file_path)
{
  int ret = Status::kOk;
  int tmp_ret = Status::kOk;
  DataFileHeader master_header;
  DataFileHeader backup_header;
  uint64_t header_buf_size = MAX_EXTENT_SIZE;
  char *header_buf = nullptr;
  Slice header_result;
  int64_t backup_header_offset = 0;

  if (IS_NULL(header_buf = reinterpret_cast<char *>(
          memory::base_memalign(DIO_ALIGN_SIZE, header_buf_size, memory::ModId::kExtentSpaceMgr)))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for header buf", K(ret));
  } else {
    memset(header_buf, 0, header_buf_size);
    if (FAILED(file_->Read(0, header_buf_size, &header_result, header_buf).code())) {
      XENGINE_LOG(WARN, "fail to read header buf", K(ret));
    } else if (header_buf_size != header_result.size()) {
      ret = Status::kIOError;
      XENGINE_LOG(WARN, "fail to read expect size header", K(ret), K(header_buf_size), "result_size", header_result.size());
    } else if (FAILED(load_header(header_result, MASTER_HEADER_OFFSET, master_header))) {
      XENGINE_LOG(WARN, "fail to load master header", K(ret));
    } else {
      if (DataFileHeader::DATA_FILE_HEADER_VERSION_V1 == master_header.header_version_) {
        //compactiple old format
        backup_header_offset = MAX_EXTENT_SIZE / 2;
      } else if (DataFileHeader::DATA_FILE_HEADER_VERSION == master_header.header_version_) {
        backup_header_offset = BACKUP_HEADER_OFFSET;
      } else {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, the data file header maybe wrong", K(ret), K(master_header));
      }

      if (SUCCED(ret)) {
        if (FAILED(load_header(header_result, backup_header_offset, backup_header))) {
          XENGINE_LOG(WARN, "fail to load backup header", K(ret));
        } else if (!(master_header == backup_header)) {
          ret = Status::kErrorUnexpected;
          XENGINE_LOG(WARN, "unexpected error, master_header and backup_header mismatch", K(ret), K(master_header), K(backup_header));
        } else {
          data_file_header_ = master_header;
          if (DataFileHeader::DATA_FILE_HEADER_VERSION_V1 == master_header.header_version_) {
            //update the old_format DataFileHeader to new_format
            data_file_header_.header_version_ = DataFileHeader::DATA_FILE_HEADER_VERSION;
            if (FAILED(double_write_header())) {
              XENGINE_LOG(WARN, "fail to double write header", K(ret));
            }
          }
        }
      }
    }
  }

  if (FAILED(ret)) {
    if (Status::kOk != (tmp_ret = check_garbage_file(file_path, header_result))) {
      XENGINE_LOG(ERROR, "fail to check garbage file", K(ret), K(tmp_ret));
    }
  }

  if (nullptr != header_buf) {
    memory::base_memalign_free(header_buf);
    header_buf = nullptr;
  }
  return ret;
}

int DataFile::load_header(const Slice &header_buf, const int64_t offset, DataFileHeader &data_file_header)
{
  int ret = Status::kOk;
  int64_t pos = offset;

  if (IS_NULL(header_buf.data()) || UNLIKELY(0 == header_buf.size())) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(header_buf.data()), "size", header_buf.size());
  } else if (FAILED(data_file_header.deserialize(header_buf.data(), header_buf.size(), pos))) {
    XENGINE_LOG(ERROR, "fail to deserialize space header", K(ret), K(pos));
  } else if (DataFileHeader::DATA_FILE_HEADER_MAGIC_NUMBER != data_file_header.magic_number_) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(ERROR, "unexpected error, DataFileHeader's magic number mismatch", K(ret), K(data_file_header));
  }

  return ret;
}

int DataFile::init_fallocate_file()
{
  int ret = Status::kOk;
  int mode = 0;
  int64_t initialize_size = INITIALIZE_EXTENT_COUNT * MAX_EXTENT_SIZE;

#ifndef NDEBUG
  TEST_SYNC_POINT_CALLBACK("DataFile::create::inject_fallocated_failed", this);
  if (TEST_is_fallocate_failed()) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(ERROR, "inject fallocate file error", K(ret));
    return ret;
  }
#endif

  return file_->fallocate(mode, 0, initialize_size);
}

int DataFile::allocate_extent(ExtentId &extent_id)
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "DataFile should been inited first", K(ret));
  } else {
    //expand space if need
    if (need_expand_space()) {
      if (FAILED(expand_space(calc_expand_extent_count()))) {
        XENGINE_LOG(WARN, "fail to expand space", K(ret));
      }
    }

    //actual allocate extent
    if (SUCCED(ret)) {
      if (offset_heap_.empty()) {
        ret = Status::kNoSpace;
        XENGINE_LOG(INFO, "datafile has no space", K_(total_extent_count));
      } else {
        extent_id.file_number = data_file_header_.file_number_;
        extent_id.offset = offset_heap_.top();
        if (is_used_extent(extent_id.offset)) {
          ret = Status::kErrorUnexpected;
          XENGINE_LOG(WARN, "unexpected error, the extent has been used", K(ret), K(extent_id));
          abort();
        } else {
          set_used_status(extent_id.offset);
          offset_heap_.pop();
          dec_free_extent_count(1);
          XENGINE_LOG(DEBUG, "success to allocate extent", K(extent_id));
        }
      }
    }
  }

  return ret;
}

int DataFile::expand_space(int64_t expand_extent_cnt)
{
  int ret = Status::kOk;
  int mode = 0;
  int64_t expand_file_size = expand_extent_cnt * MAX_EXTENT_SIZE;
  int64_t origin_file_size = total_extent_count_ * MAX_EXTENT_SIZE;
  int64_t new_file_size = origin_file_size + expand_file_size;
  int64_t origin_total_extent_count = total_extent_count_;
  int64_t new_total_extent_count = total_extent_count_ + expand_extent_cnt;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "DataFile should been inited first", K(ret));
  } else if (UNLIKELY(expand_extent_cnt <= 0)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(expand_extent_cnt));
  } else if (FAILED(file_->fallocate(mode, origin_file_size, expand_file_size))) {
    XENGINE_LOG(WARN, "fail to fallocate file", K(ret)/*, K_(data_file_header)*/, K(expand_extent_cnt), K(expand_file_size));
  } else if (FAILED(update_data_file_size(new_total_extent_count))) {
    XENGINE_LOG(WARN, "fail to update data file header", K(ret), K(new_file_size), K(new_total_extent_count));
  } else {
    //add expand offset to heap
    for (int32_t i = origin_total_extent_count; SUCCED(ret) && i < new_total_extent_count; ++i) {
      offset_heap_.push(i);
      XENGINE_LOG(DEBUG, "success to push offset", "file_number", data_file_header_.file_number_, K(i));
    }
    inc_free_extent_count(expand_extent_cnt);
    uint64_t actual_file_size = 0;
    env_->GetFileSize(data_file_path_, &actual_file_size);
    XENGINE_LOG(INFO, "success to expand space", "file_number", data_file_header_.file_number_,
        K(origin_total_extent_count), K(origin_file_size), K(new_total_extent_count), K(new_file_size), K(actual_file_size));
  }

  return ret;
}

bool DataFile::need_expand_space() const
{
  return (total_extent_count_ < MAX_TOTAL_EXTENT_COUNT)
         && offset_heap_.empty();
}

int64_t DataFile::calc_expand_extent_count()
{
  int64_t expand_extent_cnt = 0;
  int64_t expand_limit =
    std::min(MAX_TOTAL_EXTENT_COUNT - total_extent_count_,
             MAX_EXPAND_EXTENT_COUNT);
  if (0 == expand_limit) {
    //do nothing, reach the data file size limit
  } else if (1 >= (expand_extent_cnt = (total_extent_count_ * DEFAULT_EXPAND_PERCENT))) {
    //expand one extent at least
    expand_extent_cnt = 1;
  } else {
    //expand MAX_EXPAND_EXTENT_COUNT extent at most
    expand_extent_cnt = std::min(expand_extent_cnt, expand_limit);
  }
  return expand_extent_cnt;
}

int DataFile::truncate(const int64_t truncate_extent_count)
{
  int ret = Status::kOk;
  int64_t origin_file_size = total_extent_count_ * MAX_EXTENT_SIZE;
  int64_t origin_total_extent_count = total_extent_count_;
  int64_t truncate_file_size = truncate_extent_count * MAX_EXTENT_SIZE;
  int64_t new_file_size = origin_file_size - truncate_file_size;
  int64_t new_total_extent_count = total_extent_count_ - truncate_extent_count;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "DataFile should been inited first", K(ret));
  } else if (UNLIKELY(truncate_extent_count <= 0)
             || UNLIKELY(truncate_extent_count >= total_extent_count_)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(truncate_extent_count), K_(total_extent_count));
  } else if (FAILED(file_->ftruncate(new_file_size))) {
    XENGINE_LOG(WARN, "fail to truncate file", K(ret), K_(data_file_header), K(new_file_size));
  } else if (FAILED(update_data_file_size(new_total_extent_count))) {
    XENGINE_LOG(WARN, "fail to update data file size", K(new_file_size), K(new_total_extent_count));
  } else if (FAILED(rebuild_free_extent())) {
    XENGINE_LOG(WARN, "fail to rebuild extent", K(ret), K(new_file_size), K(new_total_extent_count));
  } else {
    uint64_t actual_file_size = 0;
    env_->GetFileSize(data_file_path_, &actual_file_size);
    XENGINE_LOG(INFO, "success to truncate data file", "file_nummber", data_file_header_.file_number_,
        K(origin_file_size), K(origin_total_extent_count), K(new_file_size), K(new_total_extent_count), K(actual_file_size));
  }

  return ret;
}

int DataFile::rebuild_free_extent()
{
  int ret = Status::kOk;

  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "DataFile should been inited first", K(ret));
  } else {
    free_extent_count_ = 0;
    offset_heap_.clear();
    for (int32_t offset = 1; offset < total_extent_count_; ++offset) {
      if (is_free_extent(offset)) {
        offset_heap_.push(offset);
        inc_free_extent_count(1);
      }
    }
  }

  return ret;
}

int DataFile::update_data_file_size(const int64_t total_extent_count)
{
  int ret = Status::kOk;

  if (UNLIKELY(total_extent_count <= 0)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(total_extent_count));
  } else {
    total_extent_count_ = total_extent_count;
  }

  return ret;
}

/**DataFile may in garbage state, when create at mysqld abnormal exit like oom, crash...
 there datafile may only create and occupy zero size, or fallocate initialize_size with FALLOC_FL_ZERO_RANGE mode.
 Garbase file not has any valid data, can been clean safety*/
int DataFile::check_garbage_file(const std::string &file_path, const Slice &header_buf)
{
  int ret = Status::kOk;
  uint64_t file_size = 0;
  char *garbage_header_buf = nullptr;
  int64_t garbage_header_size = MAX_EXTENT_SIZE;

  if (FAILED(env_->GetFileSize(file_path, &file_size).code())) {
    XENGINE_LOG(WARN, "fail to get file size", K(ret));
  } else if (0 == file_size) {
    garbage_file_ = true;
    XENGINE_LOG(INFO, "garbase file, occupy zero size", K(file_path));
  } else {
    XENGINE_LOG(INFO, "not empty garbage file", K(file_path), K(file_size));
  }

  if (SUCCED(ret) && !garbage_file_) {
    if (IS_NULL(garbage_header_buf = reinterpret_cast<char *>(
            memory::base_memalign(DIO_ALIGN_SIZE, garbage_header_size, memory::ModId::kExtentSpaceMgr)))) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "fail to allocate memory for garbage header buf", K(ret));
    } else {
      memset(garbage_header_buf, 0, garbage_header_size);
      if ((0 == (memcmp(garbage_header_buf, header_buf.data(), header_buf.size())))
          && ((INITIALIZE_EXTENT_COUNT * MAX_EXTENT_SIZE) >= file_size)) {
        garbage_file_ = true;
        XENGINE_LOG(INFO, "garbage file, header filled with zero", K(file_path), K(file_size));
      } else {
        XENGINE_LOG(INFO, "not empty header garbage file", K(file_path), K(file_size));
      }
    }
  }

  if (nullptr != garbage_header_buf) {
    memory::base_memalign_free(garbage_header_buf);
    garbage_header_buf = nullptr;
  }

  return ret;
}

} //namespace storage
} //namespace xengine
