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

#include "xengine/status.h"
#include "logger/logger.h"
#include "util/string_util.h"
#include "db/log_reader.h"
#include "db/version_set.h"
#include "storage_logger.h"
#include "xengine/utilities/hotbackup.h"

namespace xengine
{
using namespace db;
using namespace common;
using namespace util;
using namespace memory;
namespace storage
{
__thread int64_t StorageLogger::local_trans_id_ = 0;
StorageLoggerBuffer::StorageLoggerBuffer()
    : data_(nullptr),
      pos_(0),
      capacity_(0)
{
}

StorageLoggerBuffer::~StorageLoggerBuffer()
{
}

int StorageLoggerBuffer::assign(char *buf, int64_t buf_len)
{
  int ret = Status::kOk;
  if (nullptr == buf || buf_len < 0) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument.", K(ret), K(buf_len)); //TODO:@yuanfeng print buf
  } else {
    data_ = buf;
    pos_ = 0;
    capacity_ = buf_len;
  }
  return ret;
}

int StorageLoggerBuffer::append_log(ManifestLogEntryHeader &log_entry_header, const char *log_data, const int64_t log_len)
{
  int ret = Status::kOk;

  if (!log_entry_header.is_valid() || log_len < 0 || (nullptr == log_data && log_len > 0)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(log_entry_header), K(log_len));
  } else {
    int64_t header_size = log_entry_header.get_serialize_size();
    if ((header_size + log_len) > remain()) {
      ret = Status::kNoSpace;
      XENGINE_LOG(INFO, "log buffer not enough", K(ret), K_(capacity), K_(pos), K(header_size), K(log_len));
    } else if (Status::kOk != (ret = log_entry_header.serialize(data_, capacity_, pos_))) {
      XENGINE_LOG(WARN, "fail to serialize log entry header", K(ret), K_(capacity), K_(pos));
    } else {
      memcpy(data_ + pos_, log_data, log_len);
      pos_ += log_len;
    }
  }

  return ret;
}

int StorageLoggerBuffer::append_log(ManifestLogEntryHeader &log_entry_header, ManifestLogEntry &log_entry)
{
  int ret = Status::kOk;

  if (!log_entry_header.is_valid()) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument.", K(ret), K(log_entry_header));
  } else {
    int64_t header_size = log_entry_header.get_serialize_size();
    int64_t entry_size = log_entry.get_serialize_size();
    if ((header_size + entry_size) > remain()) {
      ret = Status::kNoSpace;
      XENGINE_LOG(INFO, "log buffer not enough", K(ret), K(header_size), K(entry_size));
    } else if (Status::kOk != (ret = log_entry_header.serialize(data_, capacity_, pos_))) {
      XENGINE_LOG(WARN, "fail to serialize log entry header", K(ret), K_(capacity), K_(pos));
    } else if (Status::kOk != (ret = log_entry.serialize(data_, capacity_, pos_))) {
      XENGINE_LOG(WARN, "fail to serialize log entry", K(ret), K_(capacity), K_(pos));
    }
  }

  return ret;
}

int StorageLoggerBuffer::read_log(ManifestLogEntryHeader &log_entry_header, char *&log_data, int64_t &log_len)
{
  int ret = Status::kOk;

  if (pos_ >= capacity_) {
    ret = Status::kIterEnd;
  } else if (FAILED(log_entry_header.deserialize(data_, capacity_, pos_))) {
    XENGINE_LOG(WARN, "fail to deserialize ManifestLogEntryHeader", K(ret));
  } else {
    log_data = data_ + pos_;
    log_len = log_entry_header.log_entry_length_;
    pos_ += log_len;
  }

  return ret;
}

CheckpointBlockHeader::CheckpointBlockHeader()
    : type_(0),
      block_size_(0),
      entry_count_(0),
      data_offset_(0),
      data_size_(0)
{
  memset(reserve_, 0, 2 * sizeof(int64_t));
}
CheckpointBlockHeader::~CheckpointBlockHeader()
{
}
void CheckpointBlockHeader::reset()
{
  type_ = 0;
  block_size_ = 0;
  entry_count_ = 0;
  data_offset_ = 0;
  data_size_ = 0;
}
DEFINE_TO_STRING(CheckpointBlockHeader, KV_(type), KV_(block_size), KV_(entry_count), KV_(data_offset), KV_(data_size), KV(reserve_[0]), KV(reserve_[1]));

StorageLogger::TransContext::TransContext(bool need_reused)
    : event_(INVALID_EVENT),
      log_buffer_(),
      log_count_(0),
      need_reused_(need_reused)
{
}

StorageLogger::TransContext::~TransContext()
{
}

void StorageLogger::TransContext::reuse()
{
  event_ = INVALID_EVENT;
  log_buffer_.reuse();
  log_count_ = 0;
}

int StorageLogger::TransContext::append_log(int64_t trans_id,
                                            ManifestRedoLogType log_type,
                                            const char *log_data,
                                            const int64_t log_len)
{
  int ret = Status::kOk;
  ManifestLogEntryHeader log_entry_header;

  if (trans_id < 0 || log_type < 0 || log_len < 0 || (log_len > 0 && nullptr == log_data)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument.", K(ret), K(trans_id), K(log_len));
  } else {
    log_entry_header.trans_id_ = trans_id;
    log_entry_header.log_entry_seq_ = log_count_;
    log_entry_header.log_entry_type_ = log_type;
    log_entry_header.log_entry_length_ = log_len;
    if (Status::kOk != (ret = log_buffer_.append_log(log_entry_header, log_data, log_len))) {
      XENGINE_LOG(WARN, "fail to append log to log buffer", K(ret), K(log_entry_header), K(log_len));
    } else {
      ++log_count_;
    }
  }

  return ret;
}

int StorageLogger::TransContext::append_log(int64_t trans_id,
                                            ManifestRedoLogType log_type,
                                            ManifestLogEntry &log_entry)
{
  int ret = Status::kOk;
  ManifestLogEntryHeader log_entry_header;

  if (trans_id < 0 || log_type < 0) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(trans_id));
  } else {
    log_entry_header.trans_id_ = trans_id;
    log_entry_header.log_entry_seq_ = log_count_;
    log_entry_header.log_entry_type_ = log_type;
    log_entry_header.log_entry_length_ = log_entry.get_serialize_size();
    if (Status::kOk != (ret = log_buffer_.append_log(log_entry_header, log_entry))) {
      XENGINE_LOG(WARN, "fail to append log to log buffer", K(ret), K(log_entry_header));
    } else {
      ++log_count_;
    }
  }

  return ret;
}

CheckpointHeader::CheckpointHeader()
    : start_log_id_(0),
      extent_count_(0),
      sub_table_count_(0),
      extent_meta_block_count_(0),
      sub_table_meta_block_count_(0),
      extent_meta_block_offset_(0),
      sub_table_meta_block_offset_(0)
{
}
CheckpointHeader::~CheckpointHeader()
{
}
//DEFINE_COMPACTIPLE_SERIALIZATION(CheckpointHeader, extent_count_, partition_group_count_);
DEFINE_TO_STRING(CheckpointHeader, KV_(start_log_id), KV_(extent_count), KV_(sub_table_count), KV_(extent_meta_block_count),
    KV_(sub_table_meta_block_count), KV_(extent_meta_block_offset), KV_(sub_table_meta_block_offset));

StorageLogger::StorageLogger()
    : is_inited_(false),
      env_(nullptr),
      db_name_(),
      env_options_(),
      db_options_(),
      log_file_number_(0),
      version_set_(nullptr),
      extent_space_manager_(nullptr),
      curr_manifest_log_size_(0),
      log_writer_(nullptr),
      max_manifest_log_file_size_(0),
      log_number_(0),
      global_trans_id_(0),
      map_mutex_(),
      trans_ctx_map_(),
      log_sync_mutex_(),
      log_buf_(nullptr),
      trans_ctxs_(),
      allocator_(),
      checkpoint_writer_(nullptr),
      current_manifest_file_number_(0),
      trans_pool_mutex_(),
      active_trans_cnt_(0)
{
}

StorageLogger::~StorageLogger()
{
  destroy();
}

int StorageLogger::init(Env *env,
                        std::string db_name,
                        EnvOptions env_options,
                        ImmutableDBOptions db_options,
                        db::VersionSet *version_set,
                        ExtentSpaceManager *extent_space_manager,
                        int64_t max_manifest_log_file_size)
{
  int ret = Status::kOk;
  TransContext *trans_ctx = nullptr;
  char *tmp_buf = nullptr;

  if (is_inited_) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "StorageLogger has been inited.", K(ret));
  } else if (IS_NULL(env)
             || IS_NULL(version_set)
             || IS_NULL(extent_space_manager)
             || 0 >= max_manifest_log_file_size) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(env), KP(version_set),
        KP(extent_space_manager), K(max_manifest_log_file_size));
  } else if (IS_NULL(tmp_buf = reinterpret_cast<char *>(allocator_.alloc(sizeof(TransContext *) * DEFAULT_TRANS_CONTEXT_COUNT)))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for trans ctxs buf", K(ret));
  } else if (FAILED(trans_ctxs_.init(DEFAULT_TRANS_CONTEXT_COUNT, tmp_buf))) {
    XENGINE_LOG(WARN, "fail to init trans_ctxs", K(ret));
  } else if (IS_NULL(log_buf_ = reinterpret_cast<char *>(allocator_.alloc(MAX_LOG_SIZE)))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for log buf", K(ret));
  } else {
    for (int64_t i = 0; SUCCED(ret) && i < DEFAULT_TRANS_CONTEXT_COUNT; ++i) {
      if (FAILED(construct_trans_ctx(true /*need_reused*/, trans_ctx))) {
        XENGINE_LOG(WARN, "fail to construct trans ctx", K(ret));
      } else if (IS_NULL(trans_ctx)) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected error, trans ctx must not nullptr", K(ret));
      } else if (FAILED(trans_ctxs_.push(trans_ctx))) {
        XENGINE_LOG(WARN, "fail to push TransContext", K(ret));
      }
    }
  }

  if (Status::kOk == ret) {
    env_ = env;
    db_name_ = db_name;
    env_options_ = env_options;
    db_options_ = db_options;
    version_set_ = version_set;
    extent_space_manager_ = extent_space_manager;
    max_manifest_log_file_size_ = max_manifest_log_file_size;
    is_inited_ = true;
  }
  XENGINE_LOG(INFO, "StorageLogger init finish", K(ret), K(log_file_number_),
              K(db_name_.c_str()), K(max_manifest_log_file_size_));
  return ret;
}

void StorageLogger::destroy()
{
  if (is_inited_) {
    TransContext *trans_ctx = nullptr;
    while (Status::kOk == trans_ctxs_.pop(trans_ctx)) {
      deconstruct_trans_context(trans_ctx);
    }
    allocator_.free(log_buf_);
    destroy_log_writer(log_writer_);
    is_inited_ = false;
  }
}
int StorageLogger::begin(enum XengineEvent event)
{
  int ret = Status::kOk;
  TransContext *trans_ctx = nullptr;
  int64_t trans_id = 0;
  ManifestLogEntryHeader header;
  bool can_write_checkpoint = false;

  std::lock_guard<std::mutex> guard(trans_pool_mutex_);
  if (!is_inited_) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "StorageLogger not been inited", K(ret));
  } else if (0 != local_trans_id_) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, may have dirty trans", K(ret), K_(local_trans_id));
  } else {
    //write checkpoint is a low priority task, should avoid influences other system task as far as possible
    //satisfy the prev condition curr_manifest_log_size_ more than max_manifest_log_file_size_ and satisfy one of the following condition, can write checkpoint
    //condition 1: has no active trans now
    //condition 2: curr_manifest_log_size_ is too large (more than FORCE_WRITE_CHECKPOINT_MULTIPLE of
    //max_manifest_log_file_size_), which may cause recovery very slow
    if (curr_manifest_log_size_.load() > max_manifest_log_file_size_) {
      XENGINE_LOG(INFO, "check if can write checkpoint", "active_trans_cnt", get_active_trans_cnt(),
          K_(curr_manifest_log_size), K_(max_manifest_log_file_size));
      if (0 == get_active_trans_cnt())  {
        can_write_checkpoint = true;
      } else {
        if (curr_manifest_log_size_.load() > FORCE_WRITE_CHECKPOINT_MULTIPLE * max_manifest_log_file_size_) {
          wait_trans_barrier();
          can_write_checkpoint = true;
        }
      }

      if (can_write_checkpoint) {
        if (FAILED(internal_write_checkpoint())) {
          XENGINE_LOG(WARN, "fail to internal write checkpoint", K(ret));
        }
      }
    }

    if (SUCCED(ret)) {
      XENGINE_LOG(INFO, "begin manifest trans");
      if (FAILED(alloc_trans_ctx(trans_ctx))) {
        XENGINE_LOG(WARN, "fail to alloc trans ctx", K(ret));
      } else if (IS_NULL(trans_ctx)) {
        ret = Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "trans_ctx must not nullptr", K(ret));
      } else {
        trans_id = ++global_trans_id_;
        trans_ctx->event_ = event;
        if (Status::kOk != (ret = trans_ctx->append_log(trans_id,
                                                        REDO_LOG_BEGIN,
                                                        nullptr,
                                                        0))) {
          XENGINE_LOG(WARN, "fail to append begin log", K(ret));
        } else {
          std::lock_guard<std::mutex> lock_guard(map_mutex_);
          if (!(trans_ctx_map_.emplace(std::make_pair(trans_id, trans_ctx)).second)) {
            XENGINE_LOG(WARN, "fail to emplace trans context", K(ret), K(trans_id));
          } else {
            local_trans_id_ = trans_id;
            inc_active_trans_cnt();
            XENGINE_LOG(INFO, "inc active trans cnt", "active_trans_cnt", active_trans_cnt_.load());
          }
        }
      }
    }
  }

  //resource clean
  if (FAILED(ret)) {
    //avoid ret overwrite
    int tmp_ret = Status::kOk;
    if (nullptr != trans_ctx) {
      std::lock_guard<std::mutex> lock_guard(map_mutex_);
      trans_ctx_map_.erase(trans_id);
    }
    if (nullptr != trans_ctx) {
      if (Status::kOk != (tmp_ret = free_trans_ctx(trans_ctx))) {
        XENGINE_LOG(WARN, "fail to free trans ctx", K(tmp_ret));
      }
    }
    local_trans_id_ = 0;
  }

  return ret;
}

int StorageLogger::commit(int64_t &log_seq_number)
{
  int ret = Status::kOk;
  TransContext *trans_ctx = nullptr;
  std::unordered_map<int64_t, TransContext *>::iterator trans_ctx_iterator;

  if (!is_inited_) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "StorageLogger should been inited", K(ret));
  } else {
    std::lock_guard<std::mutex> lock_guard(map_mutex_);
    if (trans_ctx_map_.end() == (trans_ctx_iterator = trans_ctx_map_.find(local_trans_id_))) {
      ret = Status::kCorruption;
      XENGINE_LOG(WARN, "can not find trans context", K(ret), K_(local_trans_id));
    } else if (nullptr == (trans_ctx = trans_ctx_iterator->second)) {
      ret = Status::kCorruption;
      XENGINE_LOG(WARN, "trans context must not null", K(ret), K_(local_trans_id));
    }
  }

  if (Status::kOk == ret) {
    if (Status::kOk != (ret = trans_ctx->append_log(local_trans_id_, REDO_LOG_COMMIT, nullptr, 0))) {
      if (Status::kNoSpace != ret) {
        XENGINE_LOG(WARN, "fail to append commit log", K(ret), K_(local_trans_id));
      } else if (Status::kOk != (ret = flush_log(*trans_ctx))) {
        XENGINE_LOG(WARN, "fail to flush log", K(ret));
      } else if (Status::kOk != (ret = trans_ctx->append_log(local_trans_id_, REDO_LOG_COMMIT, nullptr, 0))) {
        XENGINE_LOG(WARN, "fail to append log", K(ret), K_(local_trans_id));
      }
    }
  }

  if (Status::kOk == ret) {
    if (Status::kOk != (ret = flush_log(*trans_ctx, &log_seq_number))) {
      XENGINE_LOG(WARN, "fail to flush log", K(ret), K_(local_trans_id));
    } else if (FAILED(free_trans_ctx(trans_ctx))) {
      XENGINE_LOG(WARN, "fail to free trans ctx", K(ret));
    } else {
      std::lock_guard<std::mutex> lock_guard(map_mutex_);
      if (1 != trans_ctx_map_.erase(local_trans_id_)) {
        ret = Status::kCorruption;
        XENGINE_LOG(WARN, "fail to erase trans ctx", K(ret), K_(local_trans_id));
      } else {
        local_trans_id_ = 0;
      }
    }
  }

  //anyway, decrease active trans count
  dec_active_trans_cnt();
  XENGINE_LOG(INFO, "dec active trans cnt", "active_trans_cnt", active_trans_cnt_.load());
  return ret;
}

int StorageLogger::abort()
{
  int ret = Status::kOk;
  TransContext *trans_ctx = nullptr;

  if (0 == local_trans_id_) {
    XENGINE_LOG(INFO, "trans ctx has been cleaned, no need to do more thing");
  } else {
    std::lock_guard<std::mutex> lock_guard(map_mutex_);
    auto iter = trans_ctx_map_.find(local_trans_id_);
    if (trans_ctx_map_.end() == iter) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "fail to find the trans", K(ret), K_(local_trans_id));
    } else if (nullptr == (trans_ctx = iter->second)) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "trans ctx must not nullptr", K(ret), K_(local_trans_id));
    } else if (1 != trans_ctx_map_.erase(local_trans_id_)) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "fail to erase trans ctx", K(ret), K_(local_trans_id));
    } else if (FAILED(free_trans_ctx(trans_ctx))) {
      XENGINE_LOG(WARN, "fail to free trans ctx", K(ret), K_(local_trans_id));
    } else {
      local_trans_id_ = 0;
    }
  }

  //anyway, decrease active trans count
  dec_active_trans_cnt();
  XENGINE_LOG(INFO, "dec active trans cnt", "active_trans_cnt", active_trans_cnt_.load());

  return ret;
}

int StorageLogger::write_log(const ManifestRedoLogType log_type, ManifestLogEntry &log_entry)
{
  int ret = Status::kOk;
  TransContext *trans_ctx = nullptr;
  std::unordered_map<int64_t, TransContext *>::iterator trans_ctx_iterator;

  if (!is_inited_) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "StorageLogger should been inited first", K(ret));
  } else if (log_type < 0) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret));
  } else {
    std::lock_guard<std::mutex> guard(map_mutex_);
    if (trans_ctx_map_.end() == (trans_ctx_iterator = trans_ctx_map_.find(local_trans_id_))) {
      ret = Status::kCorruption;
      XENGINE_LOG(WARN, "can not find trans context", K(ret), K_(local_trans_id));
    } else if (nullptr == (trans_ctx = trans_ctx_iterator->second)){
      ret = Status::kCorruption;
      XENGINE_LOG(WARN, "trans context must not null", K(ret), K_(local_trans_id));
    }
  }

  if (Status::kOk == ret) {
    if (Status::kOk != (ret = trans_ctx->append_log(local_trans_id_, log_type, log_entry))) {
      if (Status::kNoSpace != ret) {
        XENGINE_LOG(WARN, "fail to append log", K(ret), K_(local_trans_id));
      } else if (Status::kOk != (ret = flush_log(*trans_ctx))) {
        XENGINE_LOG(WARN, "fail to flush log", K(ret));
      } else if (Status::kOk != (ret = trans_ctx->append_log(local_trans_id_, log_type, log_entry))) {
        if (Status::kNoSpace != ret) {
          XENGINE_LOG(WARN, "fail to append log", K(ret), K_(local_trans_id));
        } else if (FAILED(flush_large_log_entry(*trans_ctx, log_type, log_entry))) {
          XENGINE_LOG(ERROR, "fail to flush large log entry", K(ret));
        }
      }
    }
  }

  return ret;
}

int StorageLogger::external_write_checkpoint()
{
  int ret = Status::kOk;

  std::lock_guard<std::mutex> guard(trans_pool_mutex_);
  wait_trans_barrier();
  if (FAILED(internal_write_checkpoint())) {
    XENGINE_LOG(WARN, "fail to write checkpoint", K(ret));
  }

  return ret;
}

int StorageLogger::replay(memory::ArenaAllocator &arena)
{
  int ret = Status::kOk;

  if (FAILED(load_checkpoint(arena))) {
    XENGINE_LOG(WARN, "fail to load checkpoint", K(ret));
  } else if (FAILED(replay_after_ckpt(arena))) {
    XENGINE_LOG(WARN, "fail to replay after checkpoint", K(ret));
  } else if (FAILED(update_log_writer(++log_file_number_))) {
    XENGINE_LOG(WARN, "fail to create log writer", K(ret), K_(log_file_number));
  } else if (FAILED(version_set_->recover_M02L0())) {
    XENGINE_LOG(WARN, "failed to recover m0 to l0", K(ret));
  } else if (FAILED(version_set_->recover_extent_space_manager())) {
    XENGINE_LOG(ERROR, "fail to recover extent space manager", K(ret));
  } else {
    XENGINE_LOG(INFO, "success to replay", K_(log_file_number));
  }

  return ret;
}

int StorageLogger::alloc_trans_ctx(TransContext *&trans_ctx)
{
  int ret = Status::kOk;

  if (!is_inited_) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "StorageLogger should been inited first", K(ret));
  } else if (FAILED(trans_ctxs_.pop(trans_ctx))) {
    if (e_ENTRY_NOT_EXIST != ret) {
      XENGINE_LOG(WARN, "fail to pop trans context", K(ret));
    } else if (FAILED(construct_trans_ctx(false /*need_reused*/, trans_ctx))) {
      XENGINE_LOG(WARN, "fail to construc new trans ctx", K(ret));
    } else if (IS_NULL(trans_ctx)) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, trans_ctx must not nullptr", K(ret));
    } else {
      XENGINE_LOG(DEBUG, "alloc trans_ctx from system");
    }
  } else {
    XENGINE_LOG(DEBUG, "alloc trans_ctx from trans_ctxs");
  }

  return ret;
}

int StorageLogger::free_trans_ctx(TransContext *trans_ctx)
{
  int ret = Status::kOk;

  if (!is_inited_) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "StorageLogger should been inited first", K(ret));
  } else if (nullptr == trans_ctx) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(trans_ctx));
  } else {
    if (trans_ctx->need_reused()) {
      trans_ctx->reuse();
      if (FAILED(trans_ctxs_.push(trans_ctx))) {
        XENGINE_LOG(WARN, "fail to push trans_ctx", K(ret));
      } else {
        XENGINE_LOG(DEBUG, "free trans_ctx to trans_ctxs");
      }
    } else {
      if (FAILED(deconstruct_trans_context(trans_ctx))) {
        XENGINE_LOG(WARN, "fail to deconstruct trans context", K(ret));
      } else {
        XENGINE_LOG(DEBUG, "free trans_ctx to system");
      }
    }
  }

  return ret;
}

int StorageLogger::construct_trans_ctx(bool need_reused, TransContext *&trans_ctx)
{
  int ret = Status::kOk;
  char *log_buf = nullptr;
  trans_ctx = nullptr;

  if (IS_NULL(trans_ctx = MOD_NEW_OBJECT(ModId::kStorageLogger, TransContext, need_reused))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for TransContext", K(ret));
  } else if (IS_NULL(log_buf = reinterpret_cast<char *>(base_malloc(DEFAULT_LOGGER_BUFFER_SIZE, ModId::kStorageLogger)))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for log buf", K(ret));
  } else if (FAILED(trans_ctx->log_buffer_.assign(log_buf, DEFAULT_LOGGER_BUFFER_SIZE))) {
    XENGINE_LOG(WARN, "fail to set TransContext buffer", K(ret));
  }

  return ret;
}

int StorageLogger::deconstruct_trans_context(TransContext *trans_ctx)
{
  int ret = Status::kOk;

  if (IS_NULL(trans_ctx)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), KP(trans_ctx));
  } else {
    base_free(trans_ctx->log_buffer_.data());
    MOD_DELETE_OBJECT(TransContext, trans_ctx);
  }

  return ret;
}

int StorageLogger::flush_log(TransContext &trans_ctx, int64_t *log_seq_num)
{
  int ret = Status::kOk;
  int64_t pos = 0;
  LogHeader log_header;
  std::string record;

  if (!is_inited_) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "StorageLogger should been inited", K(ret));
  } else {
    //this critital sections a little large, But there should not has performace problem, because log buffer include many logs
    std::lock_guard<std::mutex> guard(log_sync_mutex_);
    log_header.event_ = trans_ctx.event_;
    log_header.log_id_ = log_number_;
    log_header.log_length_ = trans_ctx.log_buffer_.length();
    if (Status::kOk != (ret = log_header.serialize(log_buf_, MAX_LOG_SIZE, pos))) {
      XENGINE_LOG(WARN, "fail to serialize LogHeader", K(ret));
    } else {
      memcpy(log_buf_ + pos, trans_ctx.log_buffer_.data(), trans_ctx.log_buffer_.length());
      pos += trans_ctx.log_buffer_.length();

      if (Status::kOk != (ret = log_writer_->AddRecord(Slice(log_buf_, pos)).code())) {
        XENGINE_LOG(WARN, "fail to add record", K(ret), K(pos));
      } else if (FAILED(log_writer_->sync())) {
        XENGINE_LOG(WARN, "fail to sync manifest log", K(ret));
      } else {
        if (nullptr != log_seq_num) {
          *log_seq_num = log_number_;
        }
        ++log_number_;
        //accumulate current manifest log size
        curr_manifest_log_size_.fetch_add(pos);
        trans_ctx.log_buffer_.reuse();
      }
    }
  }

  return ret;
}

int StorageLogger::flush_large_log_entry(TransContext &trans_ctx, ManifestRedoLogType log_type, ManifestLogEntry &log_entry)
{
  int ret = Status::kOk;
  char *manifest_log_entry_buf = nullptr;
  int64_t manifest_log_entry_buf_size = 0;
  int64_t manifest_log_entry_buf_pos = 0;
  char *log_buf = nullptr;
  int64_t log_buf_size = 0;
  int64_t log_buf_pos = 0;
  LogHeader log_header;
  ManifestLogEntryHeader log_entry_header;

  if (!is_inited_) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "StorageLogger should been inited", K(ret));
  } else if (log_type < 0) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret));
  } else {
    //ManifestLogEntryHeader size + ManifestLogEntry size + reserved size(512byte)
    manifest_log_entry_buf_size = sizeof(ManifestLogEntryHeader) + log_entry.get_serialize_size() + 512;
    //LogHeader size + manifest_log_entry_buf_size + reserved size(512byte)
    log_buf_size = sizeof(LogHeader) + manifest_log_entry_buf_size + 512;
    if (nullptr == (manifest_log_entry_buf = reinterpret_cast<char *>(base_malloc(manifest_log_entry_buf_size, ModId::kStorageLogger)))) {
      ret = Status::kMemoryLimit;
      XENGINE_LOG(WARN, "fail to allocate memory for manifest_log_entry_buf", K(ret), K(manifest_log_entry_buf_size));
    } else if (nullptr == (log_buf = reinterpret_cast<char *>(base_malloc(log_buf_size, ModId::kStorageLogger)))) {
      ret = Status::kMemoryLimit;
      XENGINE_LOG(WARN, "fail to allocate memory for log buffer", K(ret));
    } else {
      std::lock_guard<std::mutex> guard(log_sync_mutex_);
      log_entry_header.trans_id_ = local_trans_id_;
      log_entry_header.log_entry_seq_ = trans_ctx.log_count_;
      log_entry_header.log_entry_type_ = log_type;
      log_entry_header.log_entry_length_ = log_entry.get_serialize_size();
      if (FAILED(log_entry_header.serialize(manifest_log_entry_buf, manifest_log_entry_buf_size, manifest_log_entry_buf_pos))) {
        XENGINE_LOG(WARN, "fail to serialize manifest log entry header", K(ret));
      } else if (FAILED(log_entry.serialize(manifest_log_entry_buf, manifest_log_entry_buf_size, manifest_log_entry_buf_pos))) {
        XENGINE_LOG(WARN, "fail to serialize manifest log entry", K(ret));
      } else {
        log_header.event_ = trans_ctx.event_;
        log_header.log_id_ = log_number_;
        log_header.log_length_ = manifest_log_entry_buf_pos;
        if (FAILED(log_header.serialize(log_buf, log_buf_size, log_buf_pos))) {
          XENGINE_LOG(WARN, "failed to serialize LogHeader", K(ret));
        } else if (log_buf_pos + manifest_log_entry_buf_pos > log_buf_size) {
          ret = Status::kErrorUnexpected;
          XENGINE_LOG(WARN, "log buf must enough", K(ret), K(log_buf_pos), K(manifest_log_entry_buf_pos), K(log_buf_size));
        } else {
          memcpy(log_buf + log_buf_pos, manifest_log_entry_buf, manifest_log_entry_buf_pos);
          log_buf_pos += manifest_log_entry_buf_pos;
          if (FAILED(log_writer_->AddRecord(Slice(log_buf, log_buf_pos)).code())) {
            XENGINE_LOG(WARN, "fail to add record", K(ret), K(log_buf_pos));
          } else if (FAILED(log_writer_->sync())) {
            XENGINE_LOG(WARN, "fail to sync manifest log", K(ret));
          } else {
            ++(trans_ctx.log_count_);
            ++log_number_;
          }
        }
      }
    }
  }

  if (nullptr != manifest_log_entry_buf) {
    base_free(manifest_log_entry_buf);
    manifest_log_entry_buf = nullptr;
  }
  if (nullptr != log_buf) {
    base_free(log_buf);
    log_buf = nullptr;
  }

  return ret;
}

int StorageLogger::load_checkpoint(memory::ArenaAllocator &arena)
{
  int ret = Status::kOk;
  std::string checkpoint_name;
  uint64_t log_number = 0;
  int64_t file_offset = 0;
//  unique_ptr<char[]> b(new char[DEFAULT_BUFFER_SIZE]);
//  char *header_buffer = b.get();
  char *header_buffer = (char *)memory::base_malloc(DEFAULT_BUFFER_SIZE, memory::ModId::kStorageLogger);
  CheckpointHeader *header = nullptr;
  Slice result;
//  std::unique_ptr<util::RandomAccessFile> checkpoint_reader;
  util::RandomAccessFile *checkpoint_reader = nullptr;
  EnvOptions opt_env_opts = env_options_;
  opt_env_opts.use_direct_reads = false;
  opt_env_opts.arena = &arena;
  memset(header_buffer, 0, DEFAULT_BUFFER_SIZE);
  header = reinterpret_cast<CheckpointHeader *>(header_buffer);

  if (!is_inited_) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "StorageLogger should been inited first", K(ret));
  } else if (FAILED(parse_current_checkpoint_file(checkpoint_name, log_number))) {
    XENGINE_LOG(WARN, "fail to parse current checkpoint file", K(ret));
  } else if (FAILED(env_->NewRandomAccessFile(db_name_ + "/" + checkpoint_name, checkpoint_reader, opt_env_opts).code())) {
    XENGINE_LOG(WARN, "fail to create checkpoint reader", K(ret));
  } else if (FAILED(checkpoint_reader->Read(file_offset, DEFAULT_BUFFER_SIZE, &result, header_buffer).code())) {
    XENGINE_LOG(WARN, "fail to read checkpoint header", K(ret), K(file_offset));
  } else if (FAILED(extent_space_manager_->load_checkpoint(checkpoint_reader, header))) {
    XENGINE_LOG(WARN, "fail to load extent meta checkpoint", K(ret));
  } else if (FAILED(version_set_->load_checkpoint(checkpoint_reader, header))) {
    XENGINE_LOG(WARN, "fail to load partition group meta checkpoint", K(ret));
  } else {
    XENGINE_LOG(INFO, "success to load checkpoint", K(checkpoint_name.c_str()), K(log_number));
  }
  FREE_OBJECT(RandomAccessFile, arena, checkpoint_reader);
  memory::base_free(header_buffer);
  return ret;
}

// get the valid manifest range
int StorageLogger::manifest_file_range(int32_t &begin, int32_t &end, int64_t &end_pos) {
  int ret = Status::kOk;
  std::string manifest_name;
  if (FAILED(manifest_file_in_current(manifest_name))) {
    XENGINE_LOG(WARN, "fail to read manifest information", K(ret));
  } else {
    manifest_name.pop_back(); // remove the \n
    begin = std::stoi(manifest_name.c_str() + 9);
    end = current_manifest_file_number_;
    end_pos = current_manifest_file_size();
  }

  return ret;
}

#ifndef NDEBUG
void StorageLogger::TEST_reset() {
  StorageLogger::local_trans_id_ = 0;
}
#endif

int StorageLogger::stream_log_extents(
  std::function<int(const char*, int, int64_t, int)> *stream_extent,
                         int32_t start_file, int64_t start_pos,
                         int32_t end_file, int64_t end_pos, int dest_fd)
{
  int ret = Status::kOk;
  Slice record;
  std::string scrath;
  log::Reader *reader = nullptr;
  LogHeader log_header;
  StorageLoggerBuffer log_buffer;
  char *log_buf = nullptr;
  ManifestLogEntryHeader log_entry_header;
  char *log_data = nullptr;
  int64_t log_len = 0;
  std::unordered_set<int64_t> commited_trans_ids;
  int64_t pos = 0;
  std::string manifest_name;
  int64_t log_file_number = 0;

  XENGINE_LOG(INFO, "begin to replay after checkpoint");
  if (!is_inited_) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "StorageLogger should been inited first", K(ret));
  } else if (FAILED(get_commited_trans(commited_trans_ids))) {
    XENGINE_LOG(WARN, "fail to get commited trans", K(ret));
  } else if (FAILED(parse_current_file(manifest_name))) {
    XENGINE_LOG(WARN, "fail to parse current file", K(ret));
  } else {
    std::unordered_map<int32_t, int32_t> fds_map; // all opened fds
    std::unordered_map<int32_t, int32_t>::iterator fd_iter;
    int fd = -1;
    int flags = O_RDONLY;
    int32_t file_number = -1;
    std::string fname;
    log_file_number = log_file_number_;
    assert(log_file_number >= start_file);
    manifest_name = DescriptorFileName(db_name_, log_file_number);
    memory::ArenaAllocator arena(8 * 1024, memory::ModId::kStorageLogger);
    while (Status::kOk == (env_->FileExists(manifest_name).code())) {
      XENGINE_LOG(INFO, "replay manifest file", K(log_file_number));
      if (FAILED(create_log_reader(manifest_name, reader, arena, (log_file_number == start_file ? start_pos : 0)))) {
        XENGINE_LOG(WARN, "fail to create log reader", K(ret));
      } else {
        while (Status::kOk == ret && reader->ReadRecord(&record, &scrath)) {
          // end file
          if (log_file_number == end_file && reader->LastRecordOffset() >= static_cast<uint64_t>(end_pos)) {
            break;
          }
          pos = 0;
          log_buf = const_cast<char *>(record.data());
          if (FAILED(log_header.deserialize(log_buf, record.size(), pos))) {
            XENGINE_LOG(WARN, "fail to deserialize log header", K(ret));
          } else {
            log_buffer.assign(log_buf + pos, log_header.log_length_);
            while(Status::kOk == ret &&
              Status::kOk == (ret = log_buffer.read_log(log_entry_header, log_data, log_len))) {
              if (commited_trans_ids.end() == commited_trans_ids.find(log_entry_header.trans_id_)) {
                XENGINE_LOG(WARN, "this log not commited, ignore it", K(ret), K(log_entry_header));
              } else {
                if (is_trans_log(log_entry_header.log_entry_type_)) {
                  //trans log, not need replay
                  XENGINE_LOG(INFO, "trans log", K(log_entry_header));
                } else if (is_partition_log(log_entry_header.log_entry_type_)) {
                  XENGINE_LOG(INFO, "Ignore not extent log\n");
                } else if (is_extent_log(log_entry_header.log_entry_type_)) {
                  ModifyExtentMetaLogEntry log_entry;
                  pos = 0;
                  if (nullptr == log_data || log_len < 0) {
                    ret = Status::kInvalidArgument;
                    XENGINE_LOG(WARN, "invalid argument", K(ret), K(log_len));
                    break;
                  } else if ((ret = log_entry.deserialize(log_data, log_len, pos))
                          != Status::kOk) {
                    XENGINE_LOG(WARN, "fail to deserialize log entry", K(ret), K(log_len));
                    break;
                  } else {
                    file_number = log_entry.extent_meta_.extent_id_.file_number;
                    fd_iter = fds_map.find(file_number);
                    // open the file to stream
                    if (fd_iter == fds_map.end()) {
                      fname = MakeTableFileName(db_name_, file_number);
                      do {
                        fd = open(fname.c_str(), flags, 0644);
                      } while (fd < 0 && errno == EINTR);
                      if (fd < 0) {
                        XENGINE_LOG(WARN, "Open the sst file to stream failed errno", K(errno));
                        continue;
                      }
                      fds_map[file_number] = fd;
                      fd_iter = fds_map.find(file_number);
                    }
                    assert(fd_iter != fds_map.end());
                    fname = MakeTableFileName("", file_number);
                    ret = (*stream_extent)(fname.c_str() + 1 /* remove the begin '/' */ ,
                    fd_iter->second,
                    MAX_EXTENT_SIZE * log_entry.extent_meta_.extent_id_.offset, dest_fd);
                    if (ret != Status::kOk) {
                      XENGINE_LOG(WARN, "Stream extent for backup failed",
                              K(file_number), K(log_entry.extent_meta_.extent_id_.offset));
                      break;
                    }
                  }
                } else {
                  ret = Status::kNotSupported;
                  XENGINE_LOG(WARN, "not support log type", K(ret), K(log_entry_header));
                }
              }
            }
            if (Status::kIterEnd != ret) {
              XENGINE_LOG(WARN, "fail to read log", K(ret));
            } else {
              ret = Status::kOk;
            }
          }
        }

      }
      destroy_log_reader(reader, &arena);
      ++log_file_number;
      if (log_file_number > end_file) {
        break;
      }
      manifest_name = DescriptorFileName(db_name_, log_file_number);
    }
    // close the files
    for (auto &file : fds_map) {
      close(file.second);
    }
    log_file_number_ = log_file_number - 1;
  }

  XENGINE_LOG(INFO, "success to replay after checkpoint", K_(log_file_number));

  return ret;
}

int StorageLogger::replay_after_ckpt(memory::ArenaAllocator &arena)
{
  int ret = Status::kOk;
  Slice record;
  std::string scrath;
  log::Reader *reader = nullptr;
  LogHeader log_header;
  StorageLoggerBuffer log_buffer;
  char *log_buf = nullptr;
  ManifestLogEntryHeader log_entry_header;
  char *log_data = nullptr;
  int64_t log_len = 0;
  std::unordered_set<int64_t> commited_trans_ids;
  int64_t pos = 0;
  std::string manifest_name;
  int64_t log_file_number = 0;

  XENGINE_LOG(INFO, "begin to replay after checkpoint");
  if (!is_inited_) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "StorageLogger should been inited first", K(ret));
  } else if (FAILED(get_commited_trans(commited_trans_ids))) {
    XENGINE_LOG(WARN, "fail to get commited trans", K(ret));
  } else if (FAILED(parse_current_file(manifest_name))) {
    XENGINE_LOG(WARN, "fail to parse current file", K(ret));
  } else {
    log_file_number = log_file_number_;
    manifest_name = DescriptorFileName(db_name_, log_file_number);
    //only when file exists, do actual thing, so we not set ret
    while (Status::kOk == ret && Status::kOk == (env_->FileExists(manifest_name).code())) {
      XENGINE_LOG(INFO, "replay manifest file", K(log_file_number));
      if (FAILED(create_log_reader(manifest_name, reader, arena))) {
        XENGINE_LOG(WARN, "fail to create log reader", K(ret));
      } else {
        while (Status::kOk == ret && reader->ReadRecord(&record, &scrath)) {
          pos = 0;
          log_buf = const_cast<char *>(record.data());
          if (FAILED(log_header.deserialize(log_buf, record.size(), pos))) {
            XENGINE_LOG(WARN, "fail to deserialize log header", K(ret));
          } else {
            log_buffer.assign(log_buf + pos, log_header.log_length_);
            while(Status::kOk == ret &&
              Status::kOk == (ret = log_buffer.read_log(log_entry_header, log_data, log_len))) {
              if (commited_trans_ids.end() == commited_trans_ids.find(log_entry_header.trans_id_)) {
                XENGINE_LOG(WARN, "this log not commited, ignore it", K(ret), K(log_entry_header));
              } else {
                if (is_trans_log(log_entry_header.log_entry_type_)) {
                  //trans log, not need replay
                  XENGINE_LOG(INFO, "trans log", K(log_entry_header));
                } else if (is_partition_log(log_entry_header.log_entry_type_)) {
                  if (FAILED(version_set_->replay(log_entry_header.log_entry_type_, log_data, log_len))) {
                    XENGINE_LOG(WARN, "fail to replay partition log", K(ret), K(log_entry_header));
                  }
                } else if (is_extent_log(log_entry_header.log_entry_type_)) {
                  if (FAILED(extent_space_manager_->replay(log_entry_header.log_entry_type_, log_data, log_len))) {
                    XENGINE_LOG(WARN, "fail to replay extent meta log", K(ret), K(log_entry_header));
                  }
                } else {
                  ret = Status::kNotSupported;
                  XENGINE_LOG(WARN, "not support log type", K(ret), K(log_entry_header));
                }
              }
            }
            if (Status::kIterEnd != ret) {
              XENGINE_LOG(WARN, "fail to read log", K(ret));
            } else {
              ret = Status::kOk;
              curr_manifest_log_size_.fetch_add(record.size());
            }
          }
        }
      }
      destroy_log_reader(reader, &arena);
      ++log_file_number;
      manifest_name = DescriptorFileName(db_name_, log_file_number);
    }
    log_file_number_ = log_file_number - 1;
  }

  XENGINE_LOG(INFO, "success to replay after checkpoint", K_(log_file_number));

  return ret;
}

int StorageLogger::internal_write_checkpoint()
{
  int ret = Status::kOk;
  CheckpointHeader *header = nullptr;
//  std::unique_ptr<WritableFile> checkpoint_writer;
  WritableFile *checkpoint_writer = nullptr;
  int64_t checkpoint_file_number = log_file_number_++;
  int64_t manifest_file_number = log_file_number_++;
  std::string checkpoint_path = checkpoint_name(db_name_, checkpoint_file_number);
  char buf[MAX_FILE_PATH_SIZE];
  EnvOptions opt_env_opts = env_options_;
  opt_env_opts.use_mmap_writes = false;
  opt_env_opts.use_direct_writes = true;

  char *header_buffer = nullptr;
  XENGINE_LOG(INFO, "begin to write checkpoint");
  if (nullptr == (header_buffer = reinterpret_cast<char *>(base_memalign(PAGE_SIZE, DEFAULT_BUFFER_SIZE, ModId::kDefaultMod)))) {
    ret = Status::kMemoryLimit;
    XENGINE_LOG(WARN, "fail to allocate memory for header buffer", K(ret));
  } else {
    memset(header_buffer, 0, DEFAULT_BUFFER_SIZE);
    header = reinterpret_cast<CheckpointHeader *>(header_buffer);
    if (FAILED(update_log_writer(manifest_file_number))) {
      XENGINE_LOG(WARN, "fail to create new log writer", K(ret));
    } else if (FAILED(NewWritableFile(env_, checkpoint_path, checkpoint_writer, opt_env_opts).code())) {
      XENGINE_LOG(WARN, "fail to create checkpoint writer", K(ret));
    } else if (FAILED(checkpoint_writer->PositionedAppend(Slice(header_buffer, DEFAULT_BUFFER_SIZE), 0).code())) {
      XENGINE_LOG(WARN, "fail to write checkpoint header", K(ret));
    } else if (FAILED(extent_space_manager_->do_checkpoint(checkpoint_writer, header))) {
      XENGINE_LOG(WARN, "fail to do extent checkpoint", K(ret));
    } else if (FAILED(version_set_->do_checkpoint(checkpoint_writer, header))) {
      XENGINE_LOG(WARN, "fail to do partition group checkpoint", K(ret));
    } else {
      header->start_log_id_ = log_number_;
      XENGINE_LOG(INFO, "doing the checkpoint", K(log_number_));
      if (FAILED(checkpoint_writer->PositionedAppend(Slice(header_buffer, DEFAULT_BUFFER_SIZE), 0).code())) {
        XENGINE_LOG(WARN, "fail to rewrite checkpoint header", K(ret));
      } else if (FAILED(checkpoint_writer->Fsync().code())) {
        XENGINE_LOG(WARN, "fail to sync the data", K(ret), K(checkpoint_file_number), K(manifest_file_number));
      } else if (FAILED(write_current_checkpoint_file(checkpoint_file_number))) {
        XENGINE_LOG(WARN, "fail to write current checkpoint file", K(ret), K(checkpoint_file_number));
      } else if (FAILED(write_current_file(manifest_file_number))) {
        XENGINE_LOG(WARN, "fail to write current file", K(ret), K(manifest_file_number));
      } else {
        //reset current manifest log size to zero after do checkpoint
        curr_manifest_log_size_.store(0);
      }
    }
  }
  XENGINE_LOG(INFO, "success to do write checkpoint");

  //resource clean
  if (nullptr != header_buffer) {
    base_memalign_free(header_buffer);
    header_buffer = nullptr;
  }
  MOD_DELETE_OBJECT(WritableFile, checkpoint_writer);
  return ret;
}

int StorageLogger::update_log_writer(int64_t manifest_file_number)
{
  int ret = Status::kOk;
  db::log::Writer *log_writer = nullptr;

  if (FAILED(create_log_writer(manifest_file_number, log_writer))) {
    XENGINE_LOG(WARN, "fail to create log writer", K(ret), K(manifest_file_number));
  } else {
    destroy_log_writer(log_writer_);
    log_writer_ = log_writer;
    current_manifest_file_number_ = manifest_file_number;
  }

  return ret;
}

std::string StorageLogger::checkpoint_name(const std::string &dbname, int64_t file_number)
{
  char buf[MAX_FILE_PATH_SIZE];
  snprintf(buf, MAX_FILE_PATH_SIZE, "/CHECKPOINT-%ld", file_number);
  return dbname + buf;
}

std::string StorageLogger::manifest_log_name(int64_t file_number)
{
  char buf[MAX_FILE_PATH_SIZE];
  snprintf(buf, MAX_FILE_PATH_SIZE, "/MANIFEST-%06ld", file_number);
  return db_name_ + buf;
}

int StorageLogger::write_current_checkpoint_file(int64_t checkpoint_file_number)
{
  int ret = Status::kOk;
  char buf[MAX_FILE_PATH_SIZE];
  std::string tmp_file = TempFileName(db_name_, checkpoint_file_number);

  if (0 >= (snprintf(buf, MAX_FILE_PATH_SIZE, "CHECKPOINT-%ld:%ld\n", checkpoint_file_number, log_number_.load()))) {
    ret = Status::kCorruption;
    XENGINE_LOG(WARN, "fail to write to buf", K(ret));
  } else if (FAILED(WriteStringToFile(env_, buf, tmp_file, true).code())) {
    XENGINE_LOG(WARN, "fail to write string to file", K(ret));
  } else {
    Status s = env_->FileExists(db_name_ + "/CURRENT_CHECKPOINT");
    if (s.IsNotFound()) {
      //do nothing
    } else if (FAILED(env_->RenameFile(db_name_ + "/CURRENT_CHECKPOINT", db_name_ + "/CURRENT_CHECKPOINT.back").code())) {
      XENGINE_LOG(WARN, "fail to rename current checkpoint file name", K(ret));
    }
    if (Status::kOk == ret) {
      if (FAILED(env_->RenameFile(tmp_file, db_name_ + "/CURRENT_CHECKPOINT").code())) {
        XENGINE_LOG(WARN, "fail to rename current checkpoint file name", K(ret));
      } else {
        //TODO: @yuanfeng FSYNC
      }
    }
  }

  return ret;
}

int StorageLogger::write_current_file(int64_t manifest_file_number)
{
  int ret = Status::kOk;
  char buf[MAX_FILE_PATH_SIZE];
  std::string tmp_file = TempFileName(db_name_, manifest_file_number);

  if (0 >= (snprintf(buf, MAX_FILE_PATH_SIZE, "MANIFEST-%06ld\n", manifest_file_number))) {
    ret = Status::kCorruption;
    XENGINE_LOG(WARN, "fail to write to buf", K(ret));
  } else if (FAILED(WriteStringToFile(env_, buf, tmp_file, true).code())) {
    XENGINE_LOG(WARN, "fail to write string to file", K(ret));
  } else if (FAILED(env_->RenameFile(tmp_file, db_name_ + "/CURRENT").code())) {
    XENGINE_LOG(WARN, "fail to write current file", K(ret));
  }
  return ret;
}

int StorageLogger::parse_current_checkpoint_file(std::string &checkpoint_name, uint64_t &log_number)
{
  int ret = Status::kOk;
  std::string checkpoint_information;
  checkpoint_name.clear();
  log_number = 0;

  if (FAILED(ReadFileToString(env_, db_name_ + "/CURRENT_CHECKPOINT", &checkpoint_information).code())) {
    XENGINE_LOG(WARN, "fail to read checkpoint information", K(ret), K(db_name_.data()));
  } else {
    std::vector<std::string> file_content = StringSplit(checkpoint_information, ':');
    checkpoint_name = file_content[0];
    Slice log_number_str(file_content[1]);
    if (!ConsumeDecimalNumber(&log_number_str, &log_number)) {
      ret = Status::kCorruption;
      XENGINE_LOG(WARN, "fail to parse log number", K(ret));
    } else {
      log_number_.store(log_number);
    }
  }

  return ret;
}

int StorageLogger::parse_current_file(std::string &manifest_name)
{
  int ret = Status::kOk;
  manifest_name.clear();

  if (FAILED(ReadFileToString(env_, CurrentFileName(db_name_), &manifest_name).code())) {
      XENGINE_LOG(WARN, "fail to read manifest information", K(ret));
  } else if ('\n' != manifest_name.back()) {
    ret = Status::kCorruption;
    XENGINE_LOG(WARN, "manifest name's last charactor not \\n", K(ret));
  } else {
    manifest_name.pop_back();
    //remove the prefix str "MANIFEST-"
    log_file_number_ = std::stoi(manifest_name.c_str() + 9);
    XENGINE_LOG(INFO, "current manifest log number", K_(log_file_number));
  }

  return ret;
}

int StorageLogger::manifest_file_in_current(std::string &manifest_name)
{
  int ret = Status::kOk;
  manifest_name.clear();

  if (FAILED(ReadFileToString(env_, CurrentFileName(db_name_), &manifest_name).code())) {
      XENGINE_LOG(WARN, "fail to read manifest information", K(ret));
  }

   return ret;
}

int StorageLogger::create_log_writer(int64_t manifest_file_number, db::log::Writer *&writer)
{
  int ret = Status::kOk;
//  unique_ptr<WritableFile> manifest_file;
  WritableFile *manifest_file = nullptr;
//  util::ConcurrentDirectFileWriter *file_writer_tmp = nullptr;
  util::ConcurrentDirectFileWriter *file_writer;
  char *buf = nullptr;
  std::string manifest_log_path = manifest_log_name(manifest_file_number);
  EnvOptions opt_env_opts = env_->OptimizeForManifestWrite(env_options_);

  if (FAILED(NewWritableFile(env_, manifest_log_path, manifest_file, opt_env_opts).code())) {
    XENGINE_LOG(WARN, "fail ro create writable file", K(ret));
  } else {
    manifest_file->SetPreallocationBlockSize(db_options_.manifest_preallocation_size);
    //why use new ? because this memory contol by unique_ptr,and unique_ptr use default delete to destruct and free memory, so use new to match delete
    //why not define a deletor to unique_ptr? because this unique ptr will move to another object Writer(Writer's param use unique_ptr with default delete), and Reader used many other code
//    if (nullptr == (file_writer = new util::ConcurrentDirectFileWriter(manifest_file, opt_env_opts))) {
    if (IS_NULL(file_writer = MOD_NEW_OBJECT(memory::ModId::kStorageLogger,
        util::ConcurrentDirectFileWriter, manifest_file, opt_env_opts))) {
      ret = Status::kMemoryLimit;
      XENGINE_LOG(WARN, "fail to allocate memory for ConcurrentDirectFileWriter", K(ret));
    } else {
//      file_writer.reset(file_writer_tmp);
      if (FAILED(file_writer->init_multi_buffer().code())) {
        XENGINE_LOG(WARN, "fail to init multi buffer", K(ret));
      } else if (nullptr == (writer = MOD_NEW_OBJECT(ModId::kStorageLogger,
                                                     db::log::Writer,
                                                     file_writer,
                                                     0,
                                                     false))) {
        ret = Status::kMemoryLimit;
        XENGINE_LOG(WARN, "fail to constructor writer", K(ret));
      }
    }
  }

  return ret;
}

int StorageLogger::create_log_reader(const std::string &manifest_name,
                                     db::log::Reader *&reader,
                                     memory::ArenaAllocator &arena,
                                     int64_t start)
{
  int ret = Status::kOk;
  char *buf = nullptr;
  util::SequentialFileReader *file_reader = nullptr;
  SequentialFile *manifest_file = nullptr;
  //std::unique_ptr<SequentialFileReader, memory::ptr_destruct_delete<SequentialFileReader>> manifest_file_reader;
  SequentialFileReader *manifest_file_reader = nullptr;

  reader = nullptr;
  EnvOptions tmp_option = env_options_;
  tmp_option.arena = &arena;
  if (FAILED(env_->NewSequentialFile(manifest_name, manifest_file, tmp_option).code())) {
    XENGINE_LOG(WARN, "fail to create manifest file", K(ret), K(manifest_name));
  //why use new ? because this memory contol by unique_ptr,and unique_ptr use default delete to destruct and free memory, so use new to match delete
  //why not define a deletor to unique_ptr? because this unique ptr will move to another object Reader(Reader's param use unique_ptr with default delete), and Reader used many other code
//  } else if (nullptr == (manifest_file_reader = new SequentialFileReader(manifest_file))) {
  } else if (nullptr == (manifest_file_reader = ALLOC_OBJECT(SequentialFileReader, arena, manifest_file, true))) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "fail to construct SequentialFileReader", K(ret));
  } else {
//    manifest_file_reader.reset(file_reader);
    if (nullptr == (reader = ALLOC_OBJECT(db::log::Reader,
                                          arena,
                                          manifest_file_reader,
                                          nullptr,
                                          true,
                                          start /* initial_offset */,
                                          0 /* log_number */,
                                          true /* use allocator */))) {
      XENGINE_LOG(WARN, "fail to constructor reader", K(ret), K(manifest_name));
    } else {
      XENGINE_LOG(INFO, "success to create log reader", K(ret), K(manifest_name));
    }
  }
  return ret;
}

void StorageLogger::destroy_log_reader(db::log::Reader *&reader, memory::SimpleAllocator *allocator)
{
  if (nullptr != allocator) {
    reader->delete_file(allocator);
    FREE_OBJECT(Reader, *allocator, reader);
  } else {
    MOD_DELETE_OBJECT(Reader, reader);
  }
}

void StorageLogger::destroy_log_writer(db::log::Writer *&writer, memory::SimpleAllocator *allocator)
{
  if (nullptr != writer) {
    writer->sync();
    if (nullptr != allocator) {
      FREE_OBJECT(Writer, *allocator, writer);
    } else {
      MOD_DELETE_OBJECT(Writer, writer);
    }
  }
}

int StorageLogger::get_commited_trans(std::unordered_set<int64_t> &commited_trans_ids)
{
  int ret = Status::kOk;
  commited_trans_ids.clear();
  std::string manifest_name;
  int32_t log_file_number = 0;

  if (!is_inited_) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "StorageLogger should been inited first", K(ret));
  } else if (FAILED(parse_current_file(manifest_name))) {
    XENGINE_LOG(WARN, "fail to parse current file", K(ret));
  } else {
    log_file_number = log_file_number_;
    //change manifest_name from relative path to absolute path. a little ugly QAQ
    manifest_name = DescriptorFileName(db_name_, log_file_number);
    //only when file exists, do actual thing, so we not set ret
    while (Status::kOk == ret && Status::kOk == (env_->FileExists(manifest_name).code())) {
      if (FAILED(get_commited_trans_from_file(manifest_name, commited_trans_ids))) {
        XENGINE_LOG(WARN, "failed to get commited trans from file", K(ret), K(manifest_name));
      } else {
        ++log_file_number;
        manifest_name = DescriptorFileName(db_name_, log_file_number);
      }
    }
  }

  return ret;
}

int StorageLogger::record_incremental_extent_ids(const int32_t first_manifest_file_num,
                                                 const int32_t last_manifest_file_num,
                                                 const uint64_t last_manifest_file_size)
{
  int ret = Status::kOk;
  const std::string backup_tmp_dir_path = db_name_ + util::BACKUP_TMP_DIR;
  std::vector<int32_t> manifest_file_nums;
  std::unordered_set<int64_t> commited_trans_ids;

  XENGINE_LOG(INFO, "Begin to read incremental MANIFEST files", K(ret));
  if (!is_inited_) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "StorageLogger should been inited first", K(ret));
  } else if (FAILED(check_manifest_for_backup(backup_tmp_dir_path,
                                              first_manifest_file_num,
                                              last_manifest_file_num,
                                              manifest_file_nums))) {
    XENGINE_LOG(WARN, "Failed to check manifest files for backup", K(ret));
  } else if (FAILED(get_commited_trans_for_backup(backup_tmp_dir_path,
                                                  manifest_file_nums,
                                                  commited_trans_ids))) {
    XENGINE_LOG(WARN, "Failed to get commited transactions for backup", K(ret));
  } else if (FAILED(read_manifest_for_backup(backup_tmp_dir_path,
                                             backup_tmp_dir_path + util::BACKUP_EXTENT_IDS_FILE,
                                             manifest_file_nums,
                                             commited_trans_ids,
                                             last_manifest_file_size))) {
    XENGINE_LOG(WARN, "Failed to read manifest for backup", K(ret));
  } else {
    XENGINE_LOG(INFO, "Success to read incremental MANIFEST files", K(ret));
  }
  return ret;
}

int StorageLogger::check_manifest_for_backup(const std::string &backup_tmp_dir_path,
                                             const int32_t first_manifest_file_num,
                                             const int32_t last_manifest_file_num,
                                             std::vector<int32_t> &manifest_file_nums)
{
  int ret = Status::kOk;
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "StorageLogger should been inited first", K(ret));
  } else {
    // check all MANIFEST files exist and build hard links
    manifest_file_nums.clear();
    int32_t log_file_num = 0;
    for (log_file_num = first_manifest_file_num; SUCC(ret) && log_file_num <= last_manifest_file_num; log_file_num++) {
      std::string manifest_path = DescriptorFileName(backup_tmp_dir_path, log_file_num);
      std::string checkpoint_path = checkpoint_name(backup_tmp_dir_path, log_file_num);
      if (FAILED(env_->FileExists(manifest_path).code())) {
        // overwrite ret
        if (FAILED(env_->FileExists(checkpoint_path).code())) {
          XENGINE_LOG(ERROR, "MANIFEST file was deleted during backup", K(ret), K(manifest_path), K(checkpoint_path));
        } else {
          XENGINE_LOG(INFO, "There was a checkpoint during backup", K(ret), K(checkpoint_path));
        }
      } else {
        manifest_file_nums.push_back(log_file_num);
        XENGINE_LOG(INFO, "Found a MANIFEST file", K(ret), K(manifest_path));
      }
    }
  }
  return ret;
}

int StorageLogger::get_commited_trans_for_backup(const std::string &backup_tmp_dir_path,
                                                 const std::vector<int32_t> &manifest_file_nums,
                                                 std::unordered_set<int64_t> &commited_trans_ids)
{
  int ret = Status::kOk;
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "StorageLogger should been inited first", K(ret));
  } else {
    std::string manifest_name;
    commited_trans_ids.clear();
    for (size_t i = 0; SUCC(ret) && i < manifest_file_nums.size(); i++) {
      manifest_name = DescriptorFileName(backup_tmp_dir_path, manifest_file_nums[i]);
      if (FAILED(env_->FileExists(manifest_name).code())) {
        XENGINE_LOG(ERROR, "MANIFEST file not exist", K(ret), K(manifest_name));
      } else if (FAILED(get_commited_trans_from_file(manifest_name, commited_trans_ids))) {
        XENGINE_LOG(WARN, "failed to get commited trans from file", K(ret), K(manifest_name));
      }
    }
  }
  return ret;
}

int StorageLogger::read_manifest_for_backup(const std::string &backup_tmp_dir_path,
                                            const std::string &extent_ids_path,
                                            const std::vector<int32_t> &manifest_file_nums,
                                            const std::unordered_set<int64_t> &commited_trans_ids,
                                            const uint64_t last_manifest_file_size)
{
  // read all MANIFEST files and record all incremental extents' id
  int ret = Status::kOk;
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "StorageLogger should been inited first", K(ret));
  } else {
    log::Reader *log_reader = nullptr;
    Slice record;
    std::string scrath;
    StorageLoggerBuffer log_buffer;
    LogHeader log_header;
    ManifestLogEntryHeader log_entry_header;
    char *log_data = nullptr;
    int64_t pos = 0;
    char *log_buf = nullptr;
    int64_t log_len = 0;
    //std::unordered_set<int64_t> extent_ids;
    std::unordered_map<int64_t, std::unordered_set<int64_t>> extent_ids_map;

    // parse MANIFEST files and record all modified extent ids
    memory::ArenaAllocator arena(8 * 1024, memory::ModId::kStorageLogger);
    for (size_t i = 0; SUCC(ret) && i < manifest_file_nums.size(); i++) {
      // parse one MANIFEST file
      std::string manifest_name = DescriptorFileName(backup_tmp_dir_path, manifest_file_nums[i]);
      if (FAILED(env_->FileExists(manifest_name).code())) {
        XENGINE_LOG(ERROR, "MANIFEST file not exist", K(ret), K(manifest_name));
      } else if (FAILED(create_log_reader(manifest_name, log_reader, arena))) {
        XENGINE_LOG(WARN, "fail to create log reader", K(ret));
      } else {
        while (SUCC(ret) && log_reader->ReadRecord(&record, &scrath)) {
          // parse one record in MANIFEST file
          pos = 0;
          log_buf = const_cast<char *>(record.data());
          if (i == manifest_file_nums.size() - 1 && log_reader->LastRecordOffset() >= last_manifest_file_size) {
            // skip
            break;
          } else if (FAILED(log_header.deserialize(log_buf, record.size(), pos))) {
            XENGINE_LOG(WARN, "fail to deserialize log header", K(ret));
          } else {
            log_buffer.assign(log_buf + pos, log_header.log_length_);
            while(SUCC(ret) && SUCC(log_buffer.read_log(log_entry_header, log_data, log_len))) {
              // parse one log entry in record
              if (commited_trans_ids.end() == commited_trans_ids.find(log_entry_header.trans_id_)) {
                XENGINE_LOG(DEBUG, "this log not commited, ignore it", K(ret), K(log_entry_header));
              } else if (is_partition_log(log_entry_header.log_entry_type_)) {
                if (nullptr == log_data || log_len < 0) {
                  ret = Status::kInvalidArgument;
                  XENGINE_LOG(WARN, "invalid argument", K(ret), K(log_len));
                } else if (REDO_LOG_MODIFY_SSTABLE == log_entry_header.log_entry_type_) {
                  ChangeInfo change_info;
                  ModifySubTableLogEntry log_entry(change_info);
                  pos = 0;
                  if (FAILED(log_entry.deserialize(log_data, log_len, pos))) {
                    XENGINE_LOG(WARN, "fail to deserialize log entry", K(ret), K(log_len), K(manifest_name));
                  } else if (FAILED(process_change_info_for_backup(log_entry.change_info_,
                                                                   extent_ids_map[log_entry.index_id_]))) {
                    XENGINE_LOG(WARN, "Failed to process change info for backup", K(ret), K(manifest_name));
                  }
                } else if (REDO_LOG_REMOVE_SSTABLE == log_entry_header.log_entry_type_) {
                  // remove all extent ids from extent_ids_map if a table dropped
                  ChangeSubTableLogEntry log_entry;
                  pos = 0;
                  if (FAILED(log_entry.deserialize(log_data, log_len, pos))) {
                    XENGINE_LOG(WARN, "fail to deserialize log entry", K(ret), K(log_len), K(manifest_name));
                  } else {
                    extent_ids_map.erase(log_entry.index_id_);
                    XENGINE_LOG(INFO, "Found a dropped subtable", K(log_entry.index_id_));
                  }
                } else {
                  // skip
                }
              } else {
                // skip
              }
            }
            if (Status::kIterEnd != ret) {
              XENGINE_LOG(WARN, "fail to read log", K(ret), K(manifest_name));
            } else {
              ret = Status::kOk;
            }
          }
        }
      }
      destroy_log_reader(log_reader, &arena);
    }
    // write all extent ids to the file
    if (SUCC(ret)) {
//      std::unique_ptr<WritableFile> extent_ids_writer;
      WritableFile *extent_ids_writer = nullptr;
      EnvOptions opt_env_opts = env_options_;
      opt_env_opts.use_mmap_writes = false;
      opt_env_opts.use_direct_writes = false;
      if (FAILED(NewWritableFile(env_, extent_ids_path, extent_ids_writer, opt_env_opts).code())) {
        XENGINE_LOG(WARN, "Failed to create extent ids writer", K(ret), K(extent_ids_path));
      } else {
        int64_t extent_ids_size = 0;
        for (auto iter = extent_ids_map.begin(); SUCC(ret) && iter != extent_ids_map.end(); iter++) {
          for (int64_t extent_id : iter->second) {
            XENGINE_LOG(INFO, "backup write incremental extent", K(ExtentId(extent_id)));
            if (FAILED(extent_ids_writer->Append(Slice(reinterpret_cast<char *>(&extent_id), sizeof(extent_id))).code())) {
              XENGINE_LOG(WARN, "Failed to write extent id", K(ret), K(extent_id));
              break;
            }
            ++extent_ids_size;
          }
        }
        if (FAILED(ret)) {
        } else if (FAILED(extent_ids_writer->Sync().code())) {
          XENGINE_LOG(WARN, "Failed to sync extent ids writer", K(ret));
        } else if (FAILED(extent_ids_writer->Close().code())) {
          XENGINE_LOG(WARN, "Failed to close extent ids writer", K(ret));
        } else {
          XENGINE_LOG(INFO, "Success to write extent_ids file", K(ret), K(extent_ids_size));
        }
      }
      MOD_DELETE_OBJECT(WritableFile, extent_ids_writer);
    }
  }
  return ret;
}

int StorageLogger::process_change_info_for_backup(ChangeInfo &change_info,
                                                  std::unordered_set<int64_t> &extent_ids)
{
  int ret = Status::kOk;
  // process change info, only records the added extent ids
  for (int64_t level = 0; level < storage::MAX_TIER_COUNT; level++) {
    // we should iterate the change_info.extent_change_info_ from level 0 to
    // level 2 in order, since there might be some reused extents
    const ExtentChangeArray &extent_changes = change_info.extent_change_info_[level];
    for (size_t i = 0; SUCC(ret) && i < extent_changes.size(); ++i) {
      const ExtentChange ec = extent_changes.at(i);
      if (ec.is_add()) {
        extent_ids.insert(ec.extent_id_.id());
        XENGINE_LOG(INFO, "backup insert incremental extent", K(ec));
      } else if (ec.is_delete()) {
        extent_ids.erase(ec.extent_id_.id());
        XENGINE_LOG(INFO, "backup delete incremental extent", K(ec));
      } else {
        ret = Status::kNotSupported;
        XENGINE_LOG(WARN, "unsupport extent change type", K(ret), K(ec));
      }
    }
  }

  // process large object change info
  for (size_t i = 0; SUCC(ret) && i < change_info.lob_extent_change_info_.size(); ++i) {
    const ExtentChange ec = change_info.lob_extent_change_info_.at(i);
    if (ec.is_add()) {
      extent_ids.insert(ec.extent_id_.id());
      XENGINE_LOG(INFO, "backup insert lob incremental extent", K(ec));
    } else if (ec.is_delete()) {
      extent_ids.erase(ec.extent_id_.id());
      XENGINE_LOG(INFO, "backup delete lob incremental extent", K(ec));
    } else {
      ret = Status::kNotSupported;
      XENGINE_LOG(WARN, "unsupport extent change type", K(ret), K(ec));
    }
  }
  return ret;
}

int StorageLogger::get_commited_trans_from_file(const std::string &manifest_name,
                                                std::unordered_set<int64_t> &commited_trans_ids)
{
  int ret = Status::kOk;
  log::Reader *log_reader = nullptr;
  Slice record;
  char *record_buf = nullptr;
  std::string scrath;
  StorageLoggerBuffer log_buffer;
  LogHeader log_header;
  ManifestLogEntryHeader log_entry_header;
  char *log_data = nullptr;
  int64_t log_length  = 0;
  std::unordered_set<int64_t> begin_trans_ids;
  int64_t pos = 0;
  memory::ArenaAllocator arena(8 * 1024, memory::ModId::kStorageLogger);
  if (UNLIKELY(!is_inited_)) {
    ret = Status::kNotInit;
    XENGINE_LOG(WARN, "StorageLogger should been inited first", K(ret));
  } else if (FAILED(create_log_reader(manifest_name, log_reader, arena))) {
    XENGINE_LOG(WARN, "fail to create log reader", K(ret), K(manifest_name));
  } else {
    while (SUCC(ret) && log_reader->ReadRecord(&record, &scrath)) {
      pos = 0;
      record_buf = const_cast<char *>(record.data());
      if (FAILED(log_header.deserialize(record_buf, record.size(), pos))) {
        XENGINE_LOG(WARN, "fail to deserialize log header", K(ret), K(pos));
      } else {
        log_buffer.assign(record_buf + pos, log_header.log_length_);
        while (SUCC(ret) && SUCC(log_buffer.read_log(log_entry_header, log_data, log_length))) {
          XENGINE_LOG(DEBUG, "read one manifest log", K(log_entry_header), K(pos));
          if (REDO_LOG_BEGIN == log_entry_header.log_entry_type_) {
            if (!(begin_trans_ids.insert(log_entry_header.trans_id_).second)) {
              ret = Status::kErrorUnexpected;
              XENGINE_LOG(WARN, "fail to insert trans id", K(ret), K(log_entry_header));
            } else {
              if (log_entry_header.trans_id_ > global_trans_id_) {
                global_trans_id_ = log_entry_header.trans_id_;
                XENGINE_LOG(INFO, "success to insert trans id to begin_trans_ids", K(log_entry_header));
              }
            }
          } else if (REDO_LOG_COMMIT == log_entry_header.log_entry_type_) {
            if (!(commited_trans_ids.insert(log_entry_header.trans_id_).second)) {
              ret = Status::kErrorUnexpected;
              XENGINE_LOG(WARN, "fail to insert trans id to commited_trans_ids", K(log_entry_header));
            }
          }
        }
        if (UNLIKELY(Status::kIterEnd != ret)) {
          XENGINE_LOG(WARN, "fail to read log", K(ret), K(manifest_name));
        } else {
          ret = Status::kOk;
        }
      }
    }
  }
  destroy_log_reader(log_reader, &arena);
  return ret;
}

void StorageLogger::wait_trans_barrier()
{
  while (active_trans_cnt_.load() > 0) {
    PAUSE();
  }
}

} // namespace storage
} // namespace xengine
