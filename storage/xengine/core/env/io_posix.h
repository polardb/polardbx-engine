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
#include <errno.h>
#include <unistd.h>
#include <atomic>
#include <string>
#include "xengine/env.h"

#include <sys/stat.h>
#include <sys/statfs.h>

// For non linux platform, the following macros are used only as place
// holder.
#if !(defined OS_LINUX) && !(defined CYGWIN)
#define POSIX_FADV_NORMAL 0     /* [MC1] no further special treatment */
#define POSIX_FADV_RANDOM 1     /* [MC1] expect random page refs */
#define POSIX_FADV_SEQUENTIAL 2 /* [MC1] expect sequential page refs */
#define POSIX_FADV_WILLNEED 3   /* [MC1] will need these pages */
#define POSIX_FADV_DONTNEED 4   /* [MC1] dont need these pages */
#endif

namespace xengine {
namespace util {

common::Status IOError(const std::string& context, int err_number) __attribute__((unused));

class PosixHelper {
 public:
  static size_t GetUniqueIdFromFile(int fd, char* id, size_t max_size);
};

class AIOInfo;
class PosixSequentialFile : public SequentialFile {
 private:
  std::string filename_;
  FILE* file_;
  int fd_;
  bool use_direct_io_;
  size_t logical_sector_size_;

 public:
  PosixSequentialFile(const std::string& fname, FILE* file, int fd,
                      const EnvOptions& options);
  virtual ~PosixSequentialFile();

  virtual common::Status Read(size_t n, common::Slice* result,
                              char* scratch) override;
  virtual common::Status PositionedRead(uint64_t offset, size_t n,
                                        common::Slice* result,
                                        char* scratch) override;
  virtual common::Status Skip(uint64_t n) override;
  virtual common::Status InvalidateCache(size_t offset, size_t length) override;
  virtual bool use_direct_io() const override { return use_direct_io_; }
  virtual size_t GetRequiredBufferAlignment() const override {
    return logical_sector_size_;
  }
  virtual int fill_aio_info(uint64_t offset, size_t size, AIOInfo &aio_info) const;
  virtual int file_size() const {
    struct stat buf;
    fstat(fd_, &buf);
    return buf.st_size;
  }
};

class PosixRandomAccessFile : public RandomAccessFile {
 protected:
  std::string filename_;
  int fd_;
  bool use_direct_io_;
  size_t logical_sector_size_;

 public:
  PosixRandomAccessFile(const std::string& fname, int fd,
                        const EnvOptions& options);
  virtual ~PosixRandomAccessFile();

  virtual common::Status Read(uint64_t offset, size_t n, common::Slice* result,
                              char* scratch) const override;
#if defined(OS_LINUX) || defined(OS_MACOSX)
  virtual size_t GetUniqueId(char* id, size_t max_size) const override;
#endif
  virtual void Hint(AccessPattern pattern) override;
  virtual common::Status InvalidateCache(size_t offset, size_t length) override;
  virtual bool use_direct_io() const override { return use_direct_io_; }
  virtual size_t GetRequiredBufferAlignment() const override {
    return logical_sector_size_;
  }
};

class PosixWritableFile : public WritableFile {
 protected:
  const std::string filename_;
  const bool use_direct_io_;
  int fd_;
  uint64_t filesize_;
  size_t logical_sector_size_;
#ifdef ROCKSDB_FALLOCATE_PRESENT
  bool allow_fallocate_;
  bool fallocate_with_keep_size_;
#endif

 public:
  explicit PosixWritableFile(const std::string& fname, int fd,
                             const EnvOptions& options);
  virtual ~PosixWritableFile();

  // Need to implement this so the file is truncated correctly
  // with direct I/O
  virtual common::Status Truncate(uint64_t size) override;
  virtual common::Status Close() override;
  virtual common::Status Append(const common::Slice& data) override;
  virtual common::Status PositionedAppend(const common::Slice& data,
                                          uint64_t offset) override;
  virtual common::Status Flush() override;
  virtual common::Status Sync() override;
  virtual common::Status Fsync() override;
  virtual bool IsSyncThreadSafe() const override;
  virtual bool use_direct_io() const override { return use_direct_io_; }
  virtual uint64_t GetFileSize() override;
  virtual common::Status InvalidateCache(size_t offset, size_t length) override;
  virtual size_t GetRequiredBufferAlignment() const override {
    return logical_sector_size_;
  }
#ifdef ROCKSDB_FALLOCATE_PRESENT
  virtual common::Status Allocate(uint64_t offset, uint64_t len) override;
#endif
#ifdef OS_LINUX
  virtual common::Status RangeSync(uint64_t offset, uint64_t nbytes) override;
  virtual size_t GetUniqueId(char* id, size_t max_size) const override;
#endif
};

// mmap() based random-access
class PosixMmapReadableFile : public RandomAccessFile {
 private:
  int fd_;
  std::string filename_;
  void* mmapped_region_;
  size_t length_;

 public:
  PosixMmapReadableFile(const int fd, const std::string& fname, void* base,
                        size_t length, const EnvOptions& options);
  virtual ~PosixMmapReadableFile();
  virtual common::Status Read(uint64_t offset, size_t n, common::Slice* result,
                              char* scratch) const override;
  virtual common::Status InvalidateCache(size_t offset, size_t length) override;
};

class PosixMmapFile : public WritableFile {
 private:
  std::string filename_;
  int fd_;
  size_t page_size_;
  size_t map_size_;       // How much extra memory to map at a time
  char* base_;            // The mapped region
  char* limit_;           // Limit of the mapped region
  char* dst_;             // Where to write next  (in range [base_,limit_])
  char* last_sync_;       // Where have we synced up to
  uint64_t file_offset_;  // Offset of base_ in file
#ifdef ROCKSDB_FALLOCATE_PRESENT
  bool allow_fallocate_;  // If false, fallocate calls are bypassed
  bool fallocate_with_keep_size_;
#endif

  // Roundup x to a multiple of y
  static size_t Roundup(size_t x, size_t y) { return ((x + y - 1) / y) * y; }

  size_t TruncateToPageBoundary(size_t s) {
    s -= (s & (page_size_ - 1));
    assert((s % page_size_) == 0);
    return s;
  }

  common::Status MapNewRegion();
  common::Status UnmapCurrentRegion();
  common::Status Msync();

 public:
  PosixMmapFile(const std::string& fname, int fd, size_t page_size,
                const EnvOptions& options);
  ~PosixMmapFile();

  // Means Close() will properly take care of truncate
  // and it does not need any additional information
  virtual common::Status Truncate(uint64_t size) override {
    return common::Status::OK();
  }
  virtual common::Status Close() override;
  virtual common::Status Append(const common::Slice& data) override;
  virtual common::Status Flush() override;
  virtual common::Status Sync() override;
  virtual common::Status Fsync() override;
  virtual uint64_t GetFileSize() override;
  virtual common::Status InvalidateCache(size_t offset, size_t length) override;
#ifdef ROCKSDB_FALLOCATE_PRESENT
  virtual common::Status Allocate(uint64_t offset, uint64_t len) override;
#endif
};

class PosixRandomRWFile : public RandomRWFile {
 public:
  explicit PosixRandomRWFile(const std::string& fname, int fd,
                             const EnvOptions& options);
  virtual ~PosixRandomRWFile();

  virtual common::Status Write(uint64_t offset,
                               const common::Slice& data) override;

  virtual common::Status Read(uint64_t offset, size_t n, common::Slice* result,
                              char* scratch) const override;
  virtual int fallocate(int mode, int64_t offset, int64_t length);
  virtual int ftruncate(int64_t length);
  virtual common::Status Flush() override;
  virtual common::Status Sync() override;
  virtual common::Status Fsync() override;
  virtual common::Status Close() override;
  virtual int get_fd() override { return fd_; }

 private:
  const std::string filename_;
  int fd_;
};

class PosixDirectory : public Directory {
 public:
  explicit PosixDirectory(int fd) : fd_(fd) {}
  ~PosixDirectory();
  virtual common::Status Fsync() override;

 private:
  int fd_;
};

}  // namespace util
}  // namespace xengine
