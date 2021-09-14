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


#include "util/concurrent_direct_file_writer.h"

#include <algorithm>
#include <mutex>

#include "db/log_writer.h"
#include "logger/logger.h"
#include "monitoring/histogram.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/query_perf_context.h"
#include "port/port.h"
#include "util/random.h"
#include "util/rate_limiter.h"
#include "util/sync_point.h"
#include "xengine/xengine_constants.h"

using namespace xengine::monitor;
using namespace xengine::port;
using namespace xengine::db;
using namespace xengine::common;

namespace xengine {
namespace common {
class Status;
class Slice;
class WritableFile;
class IOStatsContext;
class EnvOptions;
}  // namespace common

namespace monitor {
class Statistics;
class PerfStepTimer;
}  // namespace monitor

namespace util {
extern void TestKillRandom(std::string kill_point, int odds,
                           const std::string& srcfile, int srcline);
class AlignedBuffer;

ConcurrentDirectFileWriter::ConcurrentDirectFileWriter(
    WritableFile *file, const EnvOptions& options)
    : writable_file_(file),
      use_direct_write_(writable_file_->use_direct_io()),
      buffer_inited_(false),
      max_buffer_num_(options.concurrent_writable_file_buffer_num),
      max_buffer_size_(options.concurrent_writable_file_single_buffer_size),
      max_switch_buffer_limit_(
          options.concurrent_writable_file_buffer_switch_limit),
      file_size_(0),
      next_write_offset_(0),
      pending_sync_(false),
      buffer_copy_mutex_(false),
      buffer_switch_mutex_(false),
      buffer_flush_mutex_(false),
      mutable_buffer_(nullptr),
      free_buffer_array_num_(0),
      imm_buffer_array_num_(0),
      last_imm_file_pos_(0),
      last_flush_file_pos_(0),
      last_leftover_size_(0),
      skip_flush_(false),
      use_allocator_(false) {
  if (this->max_buffer_num_ <
      2)  // there is should at least two buffer to switch
    this->max_buffer_num_ = 2;
  this->max_switch_buffer_limit_ =
      Roundup(this->max_switch_buffer_limit_, kDefaultPageSize);
  this->max_buffer_size_ = Roundup(this->max_buffer_size_, kDefaultPageSize);
  if (this->max_switch_buffer_limit_ > this->max_buffer_size_)
    this->max_switch_buffer_limit_ = this->max_buffer_size_;
  // we place init here to support unit test
  this->init_multi_buffer();
}

// return total buffer_size
Status ConcurrentDirectFileWriter::init_multi_buffer() {
  if (this->buffer_inited_) return Status::OK();
  size_t total_buffer_size = 0;
  bool fail = false;
  for (uint64_t i = 0; i < max_buffer_num_; i++) {
    AlignedBuffer* buf = new AlignedBuffer();
    if (nullptr == buf) {
      fail = true;
      break;
    }
    if (use_direct_io()) {
      buf->Alignment(kDefaultPageSize);
    } else {
      buf->Alignment(1);
    }
    buf->AllocateNewBuffer(max_buffer_size_);
    buf->Clear();
    total_buffer_array_.push_back(buf);
    total_buffer_size += max_buffer_size_;
  }
  if (fail) {
    for (AlignedBuffer* buffer : total_buffer_array_) delete buffer;
    return Status::MemoryLimit(
        "ConcurrentDirectFileWriter::init_multi_buffer failed");
  }
  for (AlignedBuffer* buffer : total_buffer_array_) {
    this->add_free_buffer(buffer);
  }
  assert(get_free_buffer_num() == max_buffer_num_);
  assert(total_buffer_size = max_buffer_num_ * max_buffer_size_);
  mutable_buffer_ = this->fetch_free_buffer();
  this->buffer_inited_ = true;
  return Status::OK();
}

void ConcurrentDirectFileWriter::destory_multi_buffer() {
  if (!buffer_inited_) return;
  for (auto buffer : total_buffer_array_) {
    delete buffer;
  }
  total_buffer_array_.clear();
  free_buffer_array_.clear();
  imm_buffer_array_.clear();
}

uint64_t ConcurrentDirectFileWriter::get_imm_buffer_num() {
  return imm_buffer_array_num_.load();
}

uint64_t ConcurrentDirectFileWriter::get_free_buffer_num() {
  return this->free_buffer_array_num_.load();
}

int ConcurrentDirectFileWriter::try_to_flush_one_imm_buffer() {
  if (0 == this->get_imm_buffer_num()) {
    this->switch_buffer();
  }
  return flush_one_imm_buffer();
}
int ConcurrentDirectFileWriter::flush_one_imm_buffer() {
  QUERY_TRACE_BEGIN(TracePoint::CDFW_FLUSH_ONE_IMM_BUFFER);
  MutexLock lock_guard(&buffer_flush_mutex_);
  int ret = this->flush_one_imm_buffer_unsafe();
  QUERY_TRACE_END();
  return ret;
}
// be sure that imm log buffer number is not 0
ImmBufferUnit ConcurrentDirectFileWriter::fetch_imm_buffer_unit() {
  MutexLock lock_guard(&buffer_switch_mutex_);
  std::vector<ImmBufferUnit>::iterator imm_ite =
      this->imm_buffer_array_.begin();
  assert(imm_ite != this->imm_buffer_array_.end());
  ImmBufferUnit buffer = *imm_ite;
  assert(buffer.buf_ != nullptr);
  assert(buffer.buf_->CurrentSize() > 0);
  assert(buffer.target_file_pos_ > 0);
  this->imm_buffer_array_.erase(imm_ite);
  this->imm_buffer_array_num_.store(imm_buffer_array_.size());
  return buffer;
}

void ConcurrentDirectFileWriter::add_imm_buffer(const ImmBufferUnit buffer) {
  MutexLock lock_guard(&buffer_switch_mutex_);
  imm_buffer_array_.push_back(buffer);
  this->imm_buffer_array_num_.store(imm_buffer_array_.size());
}

// be sure there is free log buffer in free_buffer_array
AlignedBuffer* ConcurrentDirectFileWriter::fetch_free_buffer() {
  AlignedBuffer* ret = nullptr;
  MutexLock lock_guard(&buffer_switch_mutex_);
  assert(free_buffer_array_.size() != 0);
  ret = free_buffer_array_.back();
  free_buffer_array_.pop_back();
  ret->Clear();
  assert(ret != nullptr);
  this->free_buffer_array_num_.store(free_buffer_array_.size());
  return ret;
}

void ConcurrentDirectFileWriter::add_free_buffer(AlignedBuffer* buf) {
  assert(buf != nullptr);
  MutexLock lock_guard(&buffer_switch_mutex_);
  free_buffer_array_.push_back(buf);
  this->free_buffer_array_num_.store(free_buffer_array_.size());
}

// be sure you hold the buffer_flush_mutex_
int ConcurrentDirectFileWriter::flush_one_imm_buffer_unsafe() {
  int error = 0;
  if (this->get_imm_buffer_num() == 0) return error;

  ImmBufferUnit buffer = this->fetch_imm_buffer_unit();

  Status s = this->write_buffered_direct(buffer);

  if (!s.ok()) {  // flush failed we just return 0, not the last flush offset;
    error = -1;
  }
  buffer.buf_->Clear();
  this->add_free_buffer(buffer.buf_);
  return error;
}

// hold buffer_copy_mutex_
// Make sure that there is enough free_buffer_size != 0
int ConcurrentDirectFileWriter::switch_buffer_unsafe() {
  int error = 0;
  if (mutable_buffer_->CurrentSize() == 0 ||
      file_size_.load() == last_imm_file_pos_) {
    return error;
  }

  if (0 == this->get_free_buffer_num() &&
      0 != this->flush_one_imm_buffer()) {  // flush to disk error
    error = -1;
    return error;
  }

  //(1) first fetch new buffer
  assert(this->get_free_buffer_num() > 0);
  AlignedBuffer* new_buffer = this->fetch_free_buffer();
  new_buffer->Clear();

  //(2) move tail from mutable_buffer_ to new_buffer for 4K aligment
  size_t alignment = mutable_buffer_->Alignment();
  size_t buffer_advance =
      TruncateToPageBoundary(alignment, mutable_buffer_->CurrentSize());
  size_t leftover_tail = mutable_buffer_->CurrentSize() - buffer_advance;
  size_t pad_size = (alignment - leftover_tail) % alignment;

  assert(alignment > leftover_tail);
  // copy tail to new buffer
  new_buffer->Append(mutable_buffer_->BufferStart() + buffer_advance,
                     leftover_tail);
  //(3)put imm buffer to imm_array
  ImmBufferUnit imm_buffer(mutable_buffer_, this->file_size_.load(),
                           this->last_leftover_size_, pad_size);
  this->last_leftover_size_ = leftover_tail;
  this->add_imm_buffer(imm_buffer);
  assert((this->file_size_.load() - this->last_flush_file_pos_.load()) <=
         this->max_buffer_num_ * this->max_buffer_size_);
  //(4)update imm file post
  this->last_imm_file_pos_.store(this->file_size_.load());
  //(5) set mutable_buffer_ to new_buffer;
  mutable_buffer_ = new_buffer;
  return error;
}

// out synchronization needed  when do switch_buffer and apend() concurrently;
int ConcurrentDirectFileWriter::switch_buffer() {
  MutexLock lock_guard(&buffer_copy_mutex_);
  return this->switch_buffer_unsafe();
}

Status ConcurrentDirectFileWriter::append(const Slice& data) {
  MutexLock lock_guard(&buffer_copy_mutex_);
  if (0 == this->append_unsafe(data)) {
    return Status::OK();
  } else {
    return Status::IOError("append data failed");
  }
}

int ConcurrentDirectFileWriter::append(const Slice& header, const Slice& body) {
  int ret = 0;
  MutexLock lock_guard(&buffer_copy_mutex_);
  ret = this->append_unsafe(header);
  if (0 == ret) ret = this->append_unsafe(body);
  return ret;
}

int ConcurrentDirectFileWriter::append_unsafe(const Slice& data) {
  const char* src = data.data();
  size_t left = data.size();
  int ret = 0;
  while (left > 0 && 0 == ret) {
    size_t appended = mutable_buffer_->Append(src, left);
    file_size_.fetch_add(appended);
    left -= appended;
    src += appended;
    ret = this->switch_buffer_if_needed(mutable_buffer_);
  }
  return ret;
}

int ConcurrentDirectFileWriter::switch_buffer_if_needed(
    AlignedBuffer* mutable_buffer) {
  int ret = 0;
  assert(nullptr != mutable_buffer);
  if (mutable_buffer->CurrentSize() < this->max_switch_buffer_limit_) {
    return ret;
  }
  // try to switch
  if (this->switch_buffer_unsafe()) {
    ret = -1;
  }
  if (ret != 0) {
    XENGINE_LOG(ERROR, "failed to switch buffer", K(ret));
  }
  return ret;
}

Status ConcurrentDirectFileWriter::close() {
  // Do not quit immediately on failure the file MUST be closed
  Status s;

  // Possible to close it twice now as we MUST close
  // in __dtor, simply flushing is not enough
  // Windows when pre-allocating does not fill with zeros
  // also with unbuffered access we also set the end of data.
  if (!writable_file_) {
    return s;
  }

  s = flush();  // flush cache to OS

  Status interim;
  // In direct I/O mode we write whole pages so
  // we need to let the file know where data ends.
  // assert(use_direct_io());

  interim = writable_file_->Truncate(this->file_size_.load());
  if (!interim.ok() && s.ok()) {
    s = interim;
  }

  TEST_KILL_RANDOM("ConcurrentDirectFileWriter::close:0", rocksdb_kill_odds);

  interim = writable_file_->Close();
  if (!interim.ok() && s.ok()) {
    s = interim;
  }

//  writable_file_.reset();

  TEST_KILL_RANDOM("ConcurrentDirectFileWriter::close:1", rocksdb_kill_odds);

  this->destory_multi_buffer();
  if (use_allocator_) {
    writable_file_->~WritableFile();
    writable_file_ = nullptr;
  } else {
    MOD_DELETE_OBJECT(WritableFile, writable_file_);
  }
  return s;
}

Status ConcurrentDirectFileWriter::flush() {
  uint64_t ret = 0;
  MutexLock copy_lock_guard(&buffer_copy_mutex_);
  MutexLock flush_lock_guard(&buffer_flush_mutex_);

  if (this->last_flush_file_pos_.load() ==
      this->file_size_.load()) {  // all data flushed
    return Status::OK();
  }

  while (this->get_imm_buffer_num() != 0) {  // flush all imm buffer
    if (this->flush_one_imm_buffer_unsafe()) {
      return Status::IOError();
    }
  }

  TEST_KILL_RANDOM("ConcurrentDirectFileWriter::flush:0", rocksdb_kill_odds);
  if (this->last_flush_file_pos_.load() != this->last_imm_file_pos_.load()) {
    return Status::IOError();
  }
  // now flush the mutable log buffer if needeed
  if (this->last_flush_file_pos_ <
      this->file_size_) {  // we still have new write
    assert(this->get_free_buffer_num() > 0);
    if (this->switch_buffer_unsafe()) {
      return Status::IOError();
    }
    if (this->flush_one_imm_buffer_unsafe()) {
      return Status::IOError();
    }
    assert(this->get_imm_buffer_num() == 0);
  }
  TEST_KILL_RANDOM("ConcurrentDirectFileWriter::flush:1", rocksdb_kill_odds);
  assert(this->last_imm_file_pos_.load() == this->file_size_.load());
  assert(this->last_flush_file_pos_ == this->file_size_.load());
  return writable_file_->Flush();
}

// make sure you have flush all data here
Status ConcurrentDirectFileWriter::sync(bool use_fsync) {
  Status s;
  s = this->flush();
  if (!s.ok()) return s;
  MutexLock lock_guard(&buffer_flush_mutex_);
  TEST_KILL_RANDOM("ConcurrentDirectFileWriter::sync:0", rocksdb_kill_odds);
  // assert(use_direct_io());
  if (pending_sync_) {
    s = sync_internal(use_fsync);
    if (!s.ok()) {
      XENGINE_LOG(ERROR, "ConcurrentDirectFileWriter::sync failed!",
                  "status=", s.ToString());
      return s;
    }
  }
  TEST_KILL_RANDOM("ConcurrentDirectFileWriter::sync:1", rocksdb_kill_odds);
  pending_sync_ = false;
  return Status::OK();
}

void ConcurrentDirectFileWriter::delete_write_file(memory::SimpleAllocator *arena) {
  if (nullptr != arena) {
    FREE_OBJECT(WritableFile, *arena, writable_file_);
  } else {
    MOD_DELETE_OBJECT(WritableFile, writable_file_);
  }
}

// make sure you have flush all data here
Status ConcurrentDirectFileWriter::sync_with_out_flush(bool use_fsync) {
  MutexLock lock_guard(&buffer_flush_mutex_);
  Status s;
  TEST_KILL_RANDOM("ConcurrentDirectFileWriter::sync_with_out_flush:0",
                   rocksdb_kill_odds);
  // assert(use_direct_io());
  if (pending_sync_) {
    s = sync_internal(use_fsync);
    if (!s.ok()) {
      XENGINE_LOG(ERROR,
                  "ConcurrentDirectFileWriter::sync_with_out_flush failed!",
                  "status=", s.ToString());
      return s;
    }
  }
  TEST_KILL_RANDOM("ConcurrentDirectFileWriter::sync_with_out_flush:1",
                   rocksdb_kill_odds);
  pending_sync_ = false;
  return Status::OK();
}

Status ConcurrentDirectFileWriter::sync_internal(bool use_fsync) {
  Status s;
  TEST_SYNC_POINT("ConcurrentDirectFileWriter::sync_internal:0");
  if (use_fsync) {
    s = writable_file_->Fsync();
  } else {
    s = writable_file_->Sync();
  }
  if (!s.ok()) {
    XENGINE_LOG(ERROR,
                "ConcurrentDirectFileWriter::sync_with_out_flush failed!",
                K(use_fsync), "status=", s.ToString());
  }
  return s;
}

// hold buffer_flush_mutex_
Status ConcurrentDirectFileWriter::write_buffered_direct(
    const ImmBufferUnit& imm_buffer) {
  Status s;
  AlignedBuffer* buf = imm_buffer.buf_;
  buf->PadToAlignmentWith(0);
  // assert(imm_buffer.target_file_pos_ == this->last_flush_file_pos_.load() +
  //                               imm_buffer.buf_->CurrentSize() -
  //                               imm_buffer.last_leftover_ -
  //                               imm_buffer.pad_size_);
  if (imm_buffer.target_file_pos_ !=
      this->last_flush_file_pos_.load() + imm_buffer.buf_->CurrentSize() -
          imm_buffer.last_leftover_ - imm_buffer.pad_size_) {
    __XENGINE_LOG(ERROR,
                  "write failed last_flush_file_pos = %lu buffer_size=%lu "
                  "last_left_over=%lu pad_size=%lu",
                  this->last_flush_file_pos_.load(),
                  imm_buffer.buf_->CurrentSize(), imm_buffer.last_leftover_,
                  imm_buffer.pad_size_);
    return Status::IOError();
  }
  const size_t alignment = buf->Alignment();
  size_t file_advance = imm_buffer.pad_size_ == 0
                            ? buf->CurrentSize()
                            : (buf->CurrentSize() - alignment);
  assert((this->next_write_offset_ % alignment) == 0);
  assert((buf->CurrentSize() % alignment) == 0);
  assert(file_advance % alignment == 0);
  assert(next_write_offset_ <= imm_buffer.target_file_pos_);
  const char* src = buf->BufferStart();
  uint64_t write_offset = next_write_offset_;
  size_t left = buf->CurrentSize();

  while (left > 0) {
    size_t size = left;
    {
      // direct writes must be positional
      if (this->use_direct_write_) {
        s = writable_file_->PositionedAppend(Slice(src, size), write_offset);
      } else {
        assert(this->last_flush_file_pos_.load() == write_offset);
        s = writable_file_->Append(Slice(src, size));
      }

      if (!s.ok()) {  // we should log err msg here
        XENGINE_LOG(ERROR, "write failed", K(use_direct_write_),
                    K(write_offset), K(size), "status=", s.ToString());
        return s;
      }
    }
    left -= size;
    src += size;
    write_offset += size;
    assert((next_write_offset_ % alignment) == 0);
  }
  if (s.ok()) {
    // we have already copy tail to new buffer
    // This is where we start writing next time which may or not be
    // the actual file size on disk. They match if the buffer size
    // is a multiple of whole pages otherwise filesize_ is leftover_tail
    // behind
    next_write_offset_ += file_advance;
    this->last_flush_file_pos_.store(imm_buffer.target_file_pos_);
  }
  pending_sync_ = true;
  return s;
}

#if 0  // GCOV
Status init_writer_buffer(
    std::unique_ptr<ConcurrentDirectFileWriter>& file_writer,
    const size_t max_write_buffer_num, const size_t max_write_buffer_size) {
  assert(file_writer != nullptr);
  return file_writer->init_multi_buffer();
}
#endif  // GCOV
}
}  // namespace xengine
