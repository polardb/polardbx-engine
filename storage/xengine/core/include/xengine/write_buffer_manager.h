/*
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBufferManager is for managing memory allocation for one or more
// MemTables.

#pragma once

#include <atomic>
#include <cstddef>

namespace xengine {
namespace db {

class WriteBufferManager {
 public:
  // _buffer_size = 0 indicates no limit. Memory won't be tracked,
  // memory_usage() won't be valid and ShouldFlush() will always return true.
  explicit WriteBufferManager(size_t _active_buffer_size,
                              size_t _total_buffer_size = 0)
      : active_buffer_size_(_active_buffer_size),
        active_memory_used_(0),
        total_buffer_size_(_total_buffer_size),
        total_memory_used_(0) {}

  ~WriteBufferManager() {}

  bool enabled() const {
    return active_buffer_size_ != 0 || total_buffer_size_ != 0;
  }

  // control the active memtable usage
  // Only valid if enabled()
  size_t memory_usage() const {
    return active_memory_used_.load(std::memory_order_relaxed);
  }
  size_t buffer_size() const { return active_buffer_size_; }

  // Should only be called from write thread
  bool should_flush() const {
    return active_buffer_size_ && memory_usage() >= active_buffer_size_;
  }

  void discharge_active_mem(size_t mem) {
    if (active_buffer_size_ != 0) {
      active_memory_used_.fetch_sub(mem, std::memory_order_relaxed);
    }
  }

  // control overall memtable usage
  size_t total_memory_usage() const {
    return total_memory_used_.load(std::memory_order_relaxed);
  }
  size_t total_buffer_size() const { return total_buffer_size_; }

  // total memory usage is too large
  bool should_trim() const {
    return total_buffer_size_ && total_memory_usage() >= total_buffer_size_;
  }

  void discharge_total_mem(size_t mem) {
    if (total_buffer_size_ != 0) {
      total_memory_used_.fetch_sub(mem, std::memory_order_relaxed);
    }
  }

  // Should only be called from write thread
  void charge_mem(size_t mem) {
    if (active_buffer_size_) {
      active_memory_used_.fetch_add(mem, std::memory_order_relaxed);
    }
    if (total_buffer_size_) {
      total_memory_used_.fetch_add(mem, std::memory_order_relaxed);
    }
  }

 private:
  const size_t active_buffer_size_;
  std::atomic<size_t> active_memory_used_;

  const size_t total_buffer_size_;
  std::atomic<size_t> total_memory_used_;

  // No copying allowed
  WriteBufferManager(const WriteBufferManager&) = delete;
  WriteBufferManager& operator=(const WriteBufferManager&) = delete;
};
}  // namespace db
}  // namespace xengine
