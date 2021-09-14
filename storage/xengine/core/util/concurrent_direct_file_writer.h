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

#ifndef XENGINE_UTIL_CONCURRENT_DIRECT_FILE_WRITER_H_
#define XENGINE_UTIL_CONCURRENT_DIRECT_FILE_WRITER_H_

#include <atomic>
#include <string>
#include "port/port.h"
#include "util/aligned_buffer.h"
#include "util/lock_free_fixed_queue.h"
#include "xengine/env.h"

namespace xengine {

namespace common {
class Status;
class Slice;
}

namespace monitor {
class Statistics;
class HistogramImpl;
}

namespace util {

class WritableFile;
class EnvOptions;
class AlignedBuffer;

// Immutable  buffer unit
// use only in ConcurrentDirectFileWriter;
struct ImmBufferUnit {
 public:
  ImmBufferUnit(AlignedBuffer* buf, uint64_t target_file_pos,
                uint64_t last_leftover, uint64_t pad_size)
      : buf_(buf),
        target_file_pos_(target_file_pos),
        last_leftover_(last_leftover),
        pad_size_(pad_size) {}
  // imm log buffer ,may  pad  with '0',
  AlignedBuffer* buf_;
  // if flush this imm buffer, at least tarte_file_pos_ offset is safe
  uint64_t target_file_pos_;
  // last leftover bytes be moved to this imm buffer
  uint64_t last_leftover_;
  uint64_t pad_size_;
};

//
// ConcurrentDirectFileWriter
// with multi log buffer ,we can concurrently append to log buffern and flush to
// disk
//
class ConcurrentDirectFileWriter {
 public:
  ConcurrentDirectFileWriter(WritableFile *file,
                             const EnvOptions& options);
  ConcurrentDirectFileWriter(const ConcurrentDirectFileWriter&) = delete;
  ConcurrentDirectFileWriter& operator=(const ConcurrentDirectFileWriter&) =
      delete;

#ifndef NDEBUG
  virtual ~ConcurrentDirectFileWriter() { close(); }
  virtual common::Status sync(bool use_fsync);
#else
  ~ConcurrentDirectFileWriter() { close(); }
  common::Status sync(bool use_fsync);
#endif

  void delete_write_file(memory::SimpleAllocator *arena = nullptr);
  common::Status init_multi_buffer();
  void destory_multi_buffer();
  // switch mutable log buffer with log
  // return last immutable pos of tis file
  // flush on immutable log buffer
  // @return uint64_t [succeed] the last flush file pos
  //                  [failed]  return zero
  int switch_buffer();
  int flush_one_imm_buffer();
  int try_to_flush_one_imm_buffer();

  uint64_t get_imm_buffer_pos() { return last_imm_file_pos_.load(); }
  uint64_t get_flush_pos() { return last_flush_file_pos_.load(); }
  // return last immutable pos of tis file
  uint64_t get_imm_buffer_num();
  uint64_t get_free_buffer_num();
  common::Status append(const common::Slice& data);
  int append(const common::Slice& header, const common::Slice& body);
  uint64_t get_file_size() { return file_size_.load(); }
  uint64_t get_multi_buffer_num() { return this->max_buffer_num_; };
  uint64_t get_multi_buffer_size() { return this->max_buffer_size_; };
  uint64_t get_switch_buffer_limit() { return this->max_switch_buffer_limit_; }

  common::Status flush();
  common::Status close();
  common::Status sync_with_out_flush(bool use_fsync);
  void set_skip_flush(bool skip_flush) { this->skip_flush_ = skip_flush; }
  util::WritableFile* writable_file() const { return writable_file_; }
  bool use_allocator() const { return use_allocator_; }
 private:
  ImmBufferUnit fetch_imm_buffer_unit();
  void add_imm_buffer(const ImmBufferUnit buffer);
  AlignedBuffer* fetch_free_buffer();
  void add_free_buffer(AlignedBuffer* buf);
  // append without lock
  int append_unsafe(const common::Slice& data);
  int switch_buffer_if_needed(AlignedBuffer* mutable_buffer);
  // switch without buffer_copy_mutex_
  int switch_buffer_unsafe();
  // flush one imm log buffer without buffer_flush_mutex_
  int flush_one_imm_buffer_unsafe();

  common::Status write_buffered_direct(const ImmBufferUnit& imm_buffer);
  bool use_direct_io() { return writable_file_->use_direct_io(); }

  common::Status sync_internal(bool use_fsync);

 private:
  util::WritableFile *writable_file_; // use the same allocate way with owner
  bool use_direct_write_;
  bool buffer_inited_;
  size_t max_buffer_num_;
  size_t max_buffer_size_;
  // we switch mutable buffer when it reach max_switch_limit_;
  // defaultly max_switch_limit_ = max_buffer_size_
  // but some times max_switch_limit_ set to a smalled value
  //    we may have better performance, since the flush thread
  //    will not try to switch
  // I think 512KB would be a good choice
  size_t max_switch_buffer_limit_;
  // Actually written data size can be used for truncate
  // not counting padding data
  std::atomic<uint64_t> file_size_;
  // This is necessary when we use unbuffered access
  // and writes must happen on aligned offsets
  // so we need to go back and write that page again
  std::atomic<uint64_t> next_write_offset_;
  bool pending_sync_;
  // hold this mutex when you access mutable buffer
  port::Mutex buffer_copy_mutex_;
  // switch mutex for imm_buffer_array_ and fre_buffer_array_;
  port::Mutex buffer_switch_mutex_;
  // flush mutex
  port::Mutex buffer_flush_mutex_;
  // currently used buffer for append
  AlignedBuffer* mutable_buffer_;
  std::vector<AlignedBuffer*> total_buffer_array_;
  std::vector<AlignedBuffer*> free_buffer_array_;
  std::atomic<uint64_t> free_buffer_array_num_;
  std::vector<ImmBufferUnit> imm_buffer_array_;
  std::atomic<uint64_t> imm_buffer_array_num_;
  // we update last_imm_file_pos when we do switch_buffer succeefuly
  std::atomic<uint64_t> last_imm_file_pos_;
  std::atomic<uint64_t> last_flush_file_pos_;
  // last leftover tail size when do switch log buffer
  // currently this field just for corect check
  uint64_t last_leftover_size_;
  bool skip_flush_;
  bool use_allocator_;
};

#if 0  // GCOV
common::Status init_writer_buffer(
    std::unique_ptr<ConcurrentDirectFileWriter>& file_writer,
    const size_t max_write_buffer_num, const size_t max_write_buffer_size);
#endif  // GCOV
}  // namespace util
}  // namespace xengine

#endif
