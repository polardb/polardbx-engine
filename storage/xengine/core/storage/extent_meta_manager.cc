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

#include "extent_meta_manager.h"
#include "storage_log_entry.h"
#include "storage_logger.h"
#include "xengine/xengine_constants.h"
#include "xengine/status.h"

namespace xengine
{
using namespace common;
namespace storage
{
ExtentMetaManager::ExtentMetaManager()
    : is_inited_(false),
      storage_logger_(nullptr),
      meta_mutex_(),
      extent_meta_map_()
{
}

ExtentMetaManager::~ExtentMetaManager()
{
  destroy();
}

void ExtentMetaManager::destroy()
{
  if (is_inited_) {
    for (auto iter = extent_meta_map_.begin(); iter != extent_meta_map_.end(); ++iter) {
      free_extent_meta(iter->second);
    }
    extent_meta_map_.clear();
    is_inited_ = false;
  }
}

int ExtentMetaManager::init(StorageLogger *storage_logger)
{
  int ret = Status::kOk;

  if (UNLIKELY(is_inited_)) {
    ret = Status::kInitTwice;
    XENGINE_LOG(WARN, "ExtentMetaManager has been inited", K(ret));
  } else if (IS_NULL(storage_logger)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(storage_logger));
  } else {
    storage_logger_ = storage_logger;
    is_inited_ = true;
  }

  return ret;
}

int ExtentMetaManager::write_meta(const ExtentMeta &extent_meta, bool write_log)
{
  int ret = Status::kOk;
  ExtentMeta *extent_meta_ptr = nullptr;
  ModifyExtentMetaLogEntry log_entry(extent_meta);

  if (write_log && FAILED(storage_logger_->write_log(REDO_LOG_MODIFY_EXTENT_META, log_entry))) {
    XENGINE_LOG(WARN, "fail to write change extent meta log", K(ret));
  } else if (FAILED(extent_meta.deep_copy(extent_meta_ptr))) {
    XENGINE_LOG(WARN, "fail to deep copy extent_meta", K(ret));
  } else if (IS_NULL(extent_meta_ptr)) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, deep copy extent meta must not nullptr", K(ret), KP(extent_meta_ptr));
  } else {
    util::SpinWLockGuard guard(meta_mutex_);
    if (!(extent_meta_map_.emplace(extent_meta_ptr->extent_id_.id(), extent_meta_ptr).second)) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, fail to write extent meta", K(ret), K(*extent_meta_ptr), K(write_log));
    } else {
      XENGINE_LOG(INFO, "success to write extent meta", K(*extent_meta_ptr), K(write_log));
    }
  }

  return ret;

}

int ExtentMetaManager::recycle_meta(const ExtentId extent_id)
{
  int ret = Status::kOk;
  ExtentMeta *extent_meta = nullptr;
  util::SpinWLockGuard guard(meta_mutex_);
  auto iter = extent_meta_map_.find(extent_id.id());
  if (extent_meta_map_.end() == iter) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(ERROR, "the extent meta to recycle not exist", K(ret), K(extent_id));
  } else if (IS_NULL(extent_meta = iter->second)) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(ERROR, "the extent meta must not nullptr", K(ret), K(extent_id));
  } else if (1 != extent_meta_map_.erase(extent_id.id())) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(ERROR, "fail to erase extent id", K(ret), K(extent_id));
  } else {
    free_extent_meta(extent_meta);
  }
  return ret;
}

ExtentMeta *ExtentMetaManager::get_meta(const ExtentId &extent_id)
{
  ExtentMeta *extent_meta = nullptr;
  util::SpinRLockGuard r_guard(meta_mutex_);
  auto iter = extent_meta_map_.find(extent_id.id());
  if (extent_meta_map_.end() != iter) {
    extent_meta = iter->second;
  }
  return extent_meta;
}

int ExtentMetaManager::do_checkpoint(util::WritableFile *checkpoint_writer,
                                     CheckpointHeader *header)
{
  int ret = Status::kOk;
  char *buf = nullptr;
  int64_t buf_size = DEFAULT_BUFFER_SIZE;
  int64_t offset = 0;
  ExtentMeta * extent_meta = nullptr;
  CheckpointBlockHeader *block_header = nullptr;

  if (IS_NULL(checkpoint_writer) || IS_NULL(header)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(checkpoint_writer), KP(header));
  } else if (IS_NULL(buf = reinterpret_cast<char *>(memory::base_memalign(PAGE_SIZE, buf_size,  memory::ModId::kExtentSpaceMgr)))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for buf", K(ret), K(PAGE_SIZE), K(buf_size));
  } else if (FAILED(reserve_checkpoint_block_header(buf, buf_size, offset, block_header))) {
    XENGINE_LOG(WARN, "fail to reserve checkpoint block header", K(ret));
  } else {
    util::SpinRLockGuard guard(meta_mutex_);
    header->extent_count_ = extent_meta_map_.size();
    header->extent_meta_block_offset_ = checkpoint_writer->GetFileSize();
    for (auto iter = extent_meta_map_.begin(); SUCCED(ret) && iter != extent_meta_map_.end(); ++iter) {
      if (IS_NULL(extent_meta = iter->second)) {
        ret = Status::kCorruption;
        XENGINE_LOG(WARN, "extent meta must not nullptr", K(ret));
      } else if ((buf_size - offset) < extent_meta->get_serialize_size()) {
        block_header->data_size_ = offset - block_header->data_offset_;
        if (FAILED(checkpoint_writer->PositionedAppend(Slice(buf, buf_size), checkpoint_writer->GetFileSize()).code())) {
          XENGINE_LOG(WARN, "fail to append buffer", K(ret), K(header));
        } else if (FAILED(reserve_checkpoint_block_header(buf, buf_size, offset, block_header))) {
          XENGINE_LOG(WARN, "fail to reserve checkpoint block header", K(ret));
        } else if (FAILED(extent_meta->serialize(buf, buf_size, offset))) {
            XENGINE_LOG(WARN, "fail to serialize extent meta", K(ret));
        } else {
          ++(header->extent_meta_block_count_);
        }
      } else if (FAILED(extent_meta->serialize(buf, buf_size, offset))) {
        XENGINE_LOG(WARN, "fail to serialize extent meta", K(ret));
      }
      ++(block_header->entry_count_);
    }

    //flush last block
    if (Status::kOk == ret && block_header->entry_count_ > 0) {
      block_header->data_size_ = offset - block_header->data_offset_;
      if (FAILED(checkpoint_writer->PositionedAppend(Slice(buf, buf_size), checkpoint_writer->GetFileSize()).code())) {
        XENGINE_LOG(WARN, "fail to append buffer", K(ret));
      } else {
        ++(header->extent_meta_block_count_);
      }
    }
  }

  //resource clean
  if (nullptr != buf) {
    memory::base_memalign_free(buf);
    buf = nullptr;
  }

  return ret;
}

int ExtentMetaManager::load_checkpoint(util::RandomAccessFile *checkpoint_reader,
                                       CheckpointHeader *header)
{
  int ret = Status::kOk;
  char *buf = nullptr;
  int64_t buf_size = DEFAULT_BUFFER_SIZE;
  int64_t pos = 0;
  CheckpointBlockHeader *block_header = nullptr;
  ExtentMeta *extent_meta = nullptr;
  Slice result;
  int64_t block_index = 0;
  int64_t offset = header->extent_meta_block_offset_;

  XENGINE_LOG(INFO, "begin to load extent space manager checkpoint");
  if (IS_NULL(checkpoint_reader) || IS_NULL(header)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(checkpoint_reader), KP(header));
  } else if (IS_NULL(buf = reinterpret_cast<char *>(memory::base_memalign(PAGE_SIZE, buf_size, memory::ModId::kExtentSpaceMgr)))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for buf", K(ret), K(buf_size));
  } else {
    util::SpinWLockGuard guard(meta_mutex_);
    while (SUCCED(ret) && block_index < header->extent_meta_block_count_) {
      pos = 0;
      if (FAILED(checkpoint_reader->Read(offset, buf_size, &result, buf).code())) {
          XENGINE_LOG(WARN, "fail to read buf", K(ret), K(buf_size));
      } else {
        block_header = reinterpret_cast<CheckpointBlockHeader *>(buf);
        pos = block_header->data_offset_;
        for (int64_t i = 0; SUCCED(ret) && i < block_header->entry_count_; ++i) {
          ExtentMeta extent_meta_tmp;
          if (FAILED(extent_meta_tmp.deserialize(buf, buf_size, pos))) {
            XENGINE_LOG(WARN, "fail to deserialize extent meta", K(ret));
          } else if (FAILED(extent_meta_tmp.deep_copy(extent_meta))) {
            XENGINE_LOG(WARN, "fail to deep copy extent meta", K(ret));
          } else if (!(extent_meta_map_.emplace(extent_meta->extent_id_.id(), extent_meta).second)) {
            ret = Status::kCorruption;
            XENGINE_LOG(WARN, "fail to emplace extent meta", K(ret));
          }
        }
        ++block_index;
        offset += buf_size;
      }
    }

    //check checkpoint
    if (SUCCED(ret)) {
      if (header->extent_count_ != static_cast<int64_t>(extent_meta_map_.size())) {
        ret = Status::kCorruption;
        XENGINE_LOG(WARN, "extent meta count is unexpected", K(ret), K(*header), K(extent_meta_map_.size()));
      }
    }
  }
  XENGINE_LOG(INFO, "success to load extent space manager checkpoint");

  //resource clean
  if (nullptr != buf) {
    memory::base_memalign_free(buf);
    buf = nullptr;
  }

  return ret;
}

int ExtentMetaManager::replay(int64_t log_type, char *log_data, int64_t log_length)
{
  int ret = Status::kOk;

  if (!is_extent_log(log_type) || IS_NULL(log_data) || log_length < 0) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(log_type), KP(log_data), K(log_length));
  } else {
    switch (log_type) {
      case REDO_LOG_MODIFY_EXTENT_META:
        if (FAILED(replay_extent_meta_log(log_data, log_length))) {
          XENGINE_LOG(WARN, "fail to replay extent meta log", K(ret), K(log_type), K(log_length));
        }
        break;
      default:
        ret = Status::kNotSupported;
        XENGINE_LOG(WARN, "unknow log type", K(ret), K(log_type));
    }
  }

  return ret;
}

int ExtentMetaManager::clear_free_extent_meta()
{
  int ret = Status::kOk;
  ExtentMeta *extent_meta = nullptr;

  auto iterator = extent_meta_map_.begin();
  while (Status::kOk == ret && iterator != extent_meta_map_.end()) {
    if (IS_NULL(extent_meta = iterator->second)) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "extent meta must not nullptr", K(ret), K(iterator->first));
    } else if (0 != extent_meta->refs_ && 1 != extent_meta->refs_) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(ERROR, "unexpected error, extent meta refs must been 0 or 1 when recovery", K(ret), KP(extent_meta), K(*extent_meta));
    } else if (0 == extent_meta->refs_) {
      XENGINE_LOG(INFO, "clear free extent meta from extent space mgr", K(iterator->first), K(iterator->second));
      iterator = extent_meta_map_.erase(iterator);
      free_extent_meta(extent_meta);
    } else {
      ++iterator;
    }
  }

  return ret;

}

void ExtentMetaManager::free_extent_meta(ExtentMeta *&extent_meta)
{
  extent_meta->~ExtentMeta();
  memory::base_free(extent_meta);
  extent_meta = nullptr;
}

int ExtentMetaManager::reserve_checkpoint_block_header(char *buf,
                                                       int64_t buf_size,
                                                       int64_t &offset,
                                                       CheckpointBlockHeader *&block_header)
{
  int ret = Status::kOk;

  if (nullptr == buf || buf_size <= 0 || offset < 0) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(buf_size), K(offset));
  } else {
    memset(buf, 0, buf_size);
    offset = 0;
    block_header = reinterpret_cast<CheckpointBlockHeader *>(buf);
    offset += sizeof(CheckpointBlockHeader);
    block_header->type_ = 0; //extent meta block
    block_header->block_size_ = buf_size;
    block_header->data_offset_ = offset;
  }

  return ret;
}

int ExtentMetaManager::replay_extent_meta_log(char *log_data, int64_t log_length)
{
  int ret = Status::kOk;
  int64_t pos = 0;
  ModifyExtentMetaLogEntry log_entry;

  if (nullptr == log_data || log_length < 0) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(log_length));
  } else if (FAILED(log_entry.deserialize(log_data, log_length, pos))) {
    XENGINE_LOG(WARN, "fail to deserialize log entry", K(ret), K(log_length));
  } else if (FAILED(replay_write_meta(log_entry.extent_meta_))) {
    XENGINE_LOG(WARN, "fail to write meta", K(ret));
  } else {
    XENGINE_LOG(DEBUG, "success to replay extent log", K(log_entry.extent_meta_));
  }

  return ret;
}

int ExtentMetaManager::replay_write_meta(const ExtentMeta &extent_meta)
{
  int ret = Status::kOk;
  ExtentMeta *extent_meta_ptr = nullptr;

  util::SpinWLockGuard guard(meta_mutex_);
  ExtentId extent_id = extent_meta.extent_id_;
  auto iterator = extent_meta_map_.find(extent_id.id());
  if (extent_meta_map_.end() != iterator) {
    if (IS_NULL(extent_meta_ptr = iterator->second)) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "extent meta ptr must not nullptr", K(ret), K(extent_id));
    } else if (1 != extent_meta_map_.erase(extent_id.id())) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "fail to erase extent id", K(ret), K(extent_id));
    } else {
      free_extent_meta(extent_meta_ptr);
    }
  }
  if (SUCCED(ret)) {
    if (FAILED(extent_meta.deep_copy(extent_meta_ptr))) {
      XENGINE_LOG(WARN, "fail to deep copy extent meta", K(ret));
    } else if (IS_NULL(extent_meta_ptr)) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, deep copy extent meta must not nullptr", K(ret), KP(extent_meta_ptr));
    } else if (!(extent_meta_map_.emplace(extent_meta_ptr->extent_id_.id(), extent_meta_ptr).second)) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "fail to emplace extent meta", K(ret), K(*extent_meta_ptr));
    }
  }

  return ret;
}

} //namespace storage
} //namespace xengine
