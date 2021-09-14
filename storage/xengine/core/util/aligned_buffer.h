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

#include <algorithm>
#include "port/port.h"
#include "memory/base_malloc.h"
#include "memory/mod_info.h"
#include "xengine/status.h"

namespace xengine {
namespace util {

inline size_t TruncateToPageBoundary(size_t page_size, size_t s) {
  s -= (s & (page_size - 1));
  assert((s % page_size) == 0);
  return s;
}

inline size_t Roundup(size_t x, size_t y) { return ((x + y - 1) / y) * y; }

// This class is to manage an aligned user
// allocated buffer for direct I/O purposes
// though can be used for any purpose.
class AlignedBuffer {
  size_t alignment_;
  std::unique_ptr<char[], memory::ptr_delete<char>> buf_;
  size_t capacity_;
  size_t cursize_;
  char* bufstart_;

 public:
  AlignedBuffer()
      : alignment_(), capacity_(0), cursize_(0), bufstart_(nullptr) {}

  AlignedBuffer(AlignedBuffer&& o) ROCKSDB_NOEXCEPT { *this = std::move(o); }

  AlignedBuffer& operator=(AlignedBuffer&& o) ROCKSDB_NOEXCEPT {
    alignment_ = std::move(o.alignment_);
    buf_ = std::move(o.buf_);
    capacity_ = std::move(o.capacity_);
    cursize_ = std::move(o.cursize_);
    bufstart_ = std::move(o.bufstart_);
    return *this;
  }

  AlignedBuffer(const AlignedBuffer&) = delete;

  AlignedBuffer& operator=(const AlignedBuffer&) = delete;

  static bool isAligned(const void* ptr, size_t alignment) {
    return reinterpret_cast<uintptr_t>(ptr) % alignment == 0;
  }

  static bool isAligned(size_t n, size_t alignment) {
    return n % alignment == 0;
  }

  size_t Alignment() const { return alignment_; }

  size_t Capacity() const { return capacity_; }

  size_t CurrentSize() const { return cursize_; }

  const char* BufferStart() const { return bufstart_; }

  char* BufferStart() { return bufstart_; }

  void Clear() { cursize_ = 0; }

  void Alignment(size_t alignment) {
    assert(alignment > 0);
    assert((alignment & (alignment - 1)) == 0);
    alignment_ = alignment;
  }

  // Allocates a new buffer and sets bufstart_ to the aligned first byte
  void AllocateNewBuffer(size_t requestedCapacity) {
    assert(alignment_ > 0);
    assert((alignment_ & (alignment_ - 1)) == 0);

    size_t size = Roundup(requestedCapacity, alignment_);
    char* ptr =
        static_cast<char*>(base_malloc(size + alignment_, memory::ModId::kFlushBuffer));
    assert(ptr);
    buf_.reset(ptr);
    char* p = buf_.get();
    bufstart_ = reinterpret_cast<char*>(
        (reinterpret_cast<uintptr_t>(p) + (alignment_ - 1)) &
        ~static_cast<uintptr_t>(alignment_ - 1));
    capacity_ = size;
    cursize_ = 0;
  }
  // Used for write
  // Returns the number of bytes appended
  size_t Append(const char* src, size_t append_size) {
    size_t buffer_remaining = capacity_ - cursize_;
    size_t to_copy = std::min(append_size, buffer_remaining);

    if (to_copy > 0) {
      memcpy(bufstart_ + cursize_, src, to_copy);
      cursize_ += to_copy;
    }
    return to_copy;
  }

  size_t Read(char* dest, size_t offset, size_t read_size) const {
    assert(offset < cursize_);
    size_t to_read = std::min(cursize_ - offset, read_size);
    if (to_read > 0) {
      memcpy(dest, bufstart_ + offset, to_read);
    }
    return to_read;
  }

  /// Pad to alignment
  void PadToAlignmentWith(int padding) {
    size_t total_size = Roundup(cursize_, alignment_);
    size_t pad_size = total_size - cursize_;

    if (pad_size > 0) {
      assert((pad_size + cursize_) <= capacity_);
      memset(bufstart_ + cursize_, padding, pad_size);
      cursize_ += pad_size;
    }
  }

  // After a partial flush move the tail to the beginning of the buffer
  void RefitTail(size_t tail_offset, size_t tail_size) {
    if (tail_size > 0) {
      memmove(bufstart_, bufstart_ + tail_offset, tail_size);
    }
    cursize_ = tail_size;
  }

  // Returns place to start writing
  char* Destination() { return bufstart_ + cursize_; }

  void Size(size_t cursize) { cursize_ = cursize; }
};

//   |<------------------ capacity ----------------------->|
//   |< aligned                          |- unit -|
//   |--------|--------|--------|--------|--------|--------|
//              |-- curr set (str like) -----|
//              |<---------- size ---------->|
class WritableBuffer {
 public:
  WritableBuffer(size_t align, size_t unitsize, bool reuse)
      : buf_(nullptr),
        curr_(nullptr),
        size_(0),
        capacity_(0),
        reuse_on_clear_(reuse),
        align_(align),
        unitsize_(unitsize),
        realloc_num_(0),
        realloc_bytes_(0) {}
  WritableBuffer()
      : WritableBuffer(default_alignment, default_unit_size, true) {}
  ~WritableBuffer() {
    if (buf_) memory::base_memalign_free(buf_);
#ifndef NDEBUG
    // suppress the debug info when the penalty is low
   // if (realloc_num_ > 16 || realloc_bytes_ > 16 * 1024) {
   //   fprintf(stderr, "buffer realloc num: %ld, bytes: %ld\n", realloc_num_,
   //           realloc_bytes_);
   // }
#endif
  }

  // on the whole set
  const char* all_data() const { return buf_; }
  size_t all_size() const { return (curr_ - buf_) + size_; }

  // on the current set
  char& operator[](size_t i) {
    assert(i < size_);
    return curr_[i];
  }
  const char& operator[](size_t i) const {
    assert(i < size_);
    return curr_[i];
  }
  const char* data() const { return curr_; }
  const char* c_str() const { return curr_; }
  size_t size() const { return size_; }
  bool empty() const { return size_ == 0; }
  size_t capacity() const { return capacity_; }

  int reserve(size_t sz) {
    size_t offset = curr_ - buf_;
    size_t total_size = offset + sz;
    if (total_size > capacity_) {
      size_t alloc_size = Roundup(total_size, unitsize_);
      char* newbuf = nullptr;
      newbuf = static_cast<char*>(
          base_memalign(align_, alloc_size, memory::ModId::kWritableBuffer));
      if (nullptr == newbuf) {
        return common::Status::kNoSpace;
      } else if (buf_) {
        memcpy(newbuf, buf_, offset + size_);
        memory::base_memalign_free(buf_);
        realloc_num_++;
        realloc_bytes_ += offset + size_;
      }
      buf_ = newbuf;
      curr_ = buf_ + offset;
      capacity_ = alloc_size;
    }
    return common::Status::kOk;
  }

  int resize(size_t sz) {
    int ret = reserve(sz);
    if (ret == common::Status::kOk) size_ = sz;
    return ret;
  }

  int append(const char* s, size_t n) {
    int ret = reserve(size_ + n);
    if (ret != common::Status::kOk) return ret;
    char* dst = curr_ + size_;
    memcpy(dst, s, n);
    size_ += n;
    return common::Status::kOk;
  }

  int append(const common::Slice& s) { return append(s.data(), s.size()); }

  void clear() {
    curr_ = reuse_on_clear_ ? buf_ : (curr_ + size_);
    size_ = 0;
  }

  void reset() {
    curr_ = buf_;
    size_ = 0;
  }

 private:
  static const int default_alignment = 8;
  static const int default_unit_size = 4096 * 4;

  char* buf_;        // base buf has alignment requirement
  char* curr_;       // start address of current working set in the buffer
  size_t size_;      // size of current working set
  size_t capacity_;  // total room in buf_

  bool reuse_on_clear_;
  size_t align_;
  size_t unitsize_;

  // perf stats
  size_t realloc_num_;
  size_t realloc_bytes_;
};
}  // namespace util
}  // namespace xengine
