// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#include <atomic>
#include <string>
#include "port/port.h"
#include "util/aligned_buffer.h"
#include "xengine/env.h"

namespace xengine {
namespace monitor {
class Statistics;
class HistogramImpl;
}  // namespace monitor

namespace common {
class ImmutableCFOptions;
}  // namespace common

namespace util {

class AIOHandle;
class RateLimiter;

std::unique_ptr<RandomAccessFile, memory::ptr_destruct_delete<RandomAccessFile>> NewReadaheadRandomAccessFile(
    std::unique_ptr<RandomAccessFile, memory::ptr_destruct_delete<RandomAccessFile>>&& file, size_t readahead_size);

class SequentialFileReader {
 private:
  SequentialFile *file_;
  std::atomic<size_t> offset_;  // read offset
  std::atomic<size_t> prefetch_offset_;  // prefetch offset for aio
  bool use_allocator_;
 public:
  explicit SequentialFileReader(SequentialFile *file, bool use_allocator = false)
      : file_(file), offset_(0), prefetch_offset_(0), use_allocator_(use_allocator) {}
  ~SequentialFileReader();
//  SequentialFileReader(SequentialFileReader&& o) ROCKSDB_NOEXCEPT {
//    *this = std::move(o);
//  }
//
  SequentialFileReader& operator=(SequentialFileReader&& o) ROCKSDB_NOEXCEPT {
//    file_ = std::move(o.file_);
    file_ = o.release_file();
    return *this;
  }

  SequentialFileReader(const SequentialFileReader&) = delete;
  SequentialFileReader& operator=(const SequentialFileReader&) = delete;

  common::Status Read(size_t n, common::Slice* result, char* scratch);

  common::Status Skip(uint64_t n);

  SequentialFile* file() { return file_; }

  SequentialFile* release_file() {
    auto rfile = file_;
    file_ = nullptr;
    return rfile;
  }

  bool use_direct_io() const { return file_->use_direct_io(); }

  // aio
  int prefetch(size_t n, AIOHandle *aio_handle);
  int read(size_t n, common::Slice* result, char* scratch, AIOHandle *aio_handle);
  
 protected:
  common::Status DirectRead(size_t n, common::Slice* result, char* scratch);
};

class RandomAccessFileReader {
 private:
//  std::unique_ptr<RandomAccessFile> file_;
  RandomAccessFile *file_; // maybe have better way to manager file's mem?
  Env* env_;
  monitor::Statistics* stats_;
  uint32_t hist_type_;
  monitor::HistogramImpl* file_read_hist_;

 public:
  // used to read the next extent in the same logic file
  const common::ImmutableCFOptions* ioptions_;
  const EnvOptions env_options_;
  bool use_allocator_;

  explicit RandomAccessFileReader(
      RandomAccessFile *raf, Env* env = nullptr,
      monitor::Statistics* stats = nullptr, uint32_t hist_type = 0,
      monitor::HistogramImpl* file_read_hist = nullptr,
      const common::ImmutableCFOptions* ioptions = nullptr,
      EnvOptions env_options = EnvOptions(), bool use_allocator = false)
      : file_(raf),
        env_(env),
        stats_(stats),
        hist_type_(hist_type),
        file_read_hist_(file_read_hist),
        ioptions_(ioptions),
        env_options_(env_options),
        use_allocator_(use_allocator) {}
// todo
//  RandomAccessFileReader(RandomAccessFileReader&& o) ROCKSDB_NOEXCEPT {
//    *this = std::move(o);
//  }

  // todo
  RandomAccessFileReader& operator=(RandomAccessFileReader&& o)
      ROCKSDB_NOEXCEPT {
//    file_ = std::move(o.file_);
//    env_ = std::move(o.env_);
    file_ = o.release_file();
    env_ = o.env_;
    stats_ = std::move(o.stats_);
    hist_type_ = std::move(o.hist_type_);
    file_read_hist_ = std::move(o.file_read_hist_);
    return *this;
  }

  RandomAccessFileReader(const RandomAccessFileReader&) = delete;
  RandomAccessFileReader& operator=(const RandomAccessFileReader&) = delete;

  ~RandomAccessFileReader();
  common::Status Read(uint64_t offset, size_t n, common::Slice* result,
                      char* scratch) const;

  common::Status Prefetch(uint64_t offset, size_t n) const {
    return file_->Prefetch(offset, n);
  }

  RandomAccessFile* file() { return file_; }
  RandomAccessFile* release_file() {
    auto rfile = file_;
    file_ = nullptr;
    return rfile;
  }
  bool use_direct_io() const { return file_->use_direct_io(); }

  int read(int64_t offset,
           int64_t n,
           common::Slice *result,
           char *scratch,
           AIOHandle *aio_handle);

  int prefetch(const int64_t offset,
               const int64_t size,
               AIOHandle *aio_handle);


 protected:
  common::Status DirectRead(uint64_t offset, size_t n, common::Slice* result,
                            char* scratch) const;
};

// Use posix write to write data to a file.
class WritableFileWriter {
 private:
  WritableFile *writable_file_; // use mod_new/allocator
  AlignedBuffer buf_;
  size_t max_buffer_size_;
  // Actually written data size can be used for truncate
  // not counting padding data
  uint64_t filesize_;
  // This is necessary when we use unbuffered access
  // and writes must happen on aligned offsets
  // so we need to go back and write that page again
  uint64_t next_write_offset_;
  bool pending_sync_;
  uint64_t last_sync_size_;
  uint64_t bytes_per_sync_;
  RateLimiter* rate_limiter_;
  monitor::Statistics* stats_;
  bool use_allocator_;

 public:
  WritableFileWriter(WritableFile *file,
                     const EnvOptions& options,
                     monitor::Statistics* stats = nullptr,
                     bool use_allocator = false)
      : writable_file_(file),
        buf_(),
        max_buffer_size_(options.writable_file_max_buffer_size),
        filesize_(0),
        next_write_offset_(0),
        pending_sync_(false),
        last_sync_size_(0),
        bytes_per_sync_(options.bytes_per_sync),
        rate_limiter_(options.rate_limiter),
        stats_(stats),
        use_allocator_(use_allocator) {
    buf_.Alignment(writable_file_->GetRequiredBufferAlignment());
    buf_.AllocateNewBuffer(std::min((size_t)65536, max_buffer_size_));
  }

  WritableFileWriter(const WritableFileWriter&) = delete;

  WritableFileWriter& operator=(const WritableFileWriter&) = delete;

#ifndef NDEBUG
  virtual ~WritableFileWriter() { Close(); }

  virtual common::Status Sync(bool use_fsync);
#else
  ~WritableFileWriter() { Close(); }

  common::Status Sync(bool use_fsync);
#endif

  common::Status Append(const common::Slice& data);

  common::Status Flush();

  common::Status Close();

  WritableFile* release_file() {
    auto rfile = writable_file_;
    writable_file_ = nullptr;
    return rfile;
  }
  // Sync only the data that was already Flush()ed. Safe to call concurrently
  // with Append() and Flush(). If !writable_file_->IsSyncThreadSafe(),
  // returns NotSupported status.
  common::Status SyncWithoutFlush(bool use_fsync);

  uint64_t GetFileSize() { return filesize_; }

  common::Status InvalidateCache(size_t offset, size_t length) {
    return writable_file_->InvalidateCache(offset, length);
  }

  WritableFile* writable_file() const { return writable_file_; }

  bool use_direct_io() { return writable_file_->use_direct_io(); }

 private:
// Used when os buffering is OFF and we are writing
// DMA such as in Direct I/O mode
#ifndef ROCKSDB_LITE
  common::Status WriteDirect();
#endif  // !ROCKSDB_LITE
  // Normal write
  common::Status WriteBuffered(const char* data, size_t size);
  common::Status RangeSync(uint64_t offset, uint64_t nbytes);
  size_t RequestToken(size_t bytes, bool align);
  common::Status SyncInternal(bool use_fsync);
};

extern common::Status NewWritableFile(Env* env, const std::string& fname,
                                      WritableFile *&result,
                                      const EnvOptions& options);
}  // namespace util
}  // namespace xengine
