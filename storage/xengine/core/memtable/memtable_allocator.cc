//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "memtable/memtable_allocator.h"

#include <assert.h>
#include "util/arena.h"
#include "xengine/write_buffer_manager.h"

using namespace xengine;
using namespace util;
using namespace db;
using namespace memory;

namespace xengine {
namespace memtable {

MemTableAllocator::MemTableAllocator(Allocator* allocator,
                                     WriteBufferManager* write_buffer_manager)
    : allocator_(allocator),
      write_buffer_manager_(write_buffer_manager),
      bytes_allocated_(0),
      active_mem_discharged_(false) {}

MemTableAllocator::~MemTableAllocator() {
  if (write_buffer_manager_ && write_buffer_manager_->enabled()) {
    DoneAllocating();
    write_buffer_manager_->discharge_total_mem(
        bytes_allocated_.load(std::memory_order_relaxed));
  }
}

char* MemTableAllocator::Allocate(size_t bytes) {
  if (write_buffer_manager_ && write_buffer_manager_->enabled()) {
    bytes_allocated_.fetch_add(bytes, std::memory_order_relaxed);
    write_buffer_manager_->charge_mem(bytes);
  }
  return allocator_->Allocate(bytes);
}

char* MemTableAllocator::AllocateAligned(size_t bytes, size_t huge_page_size) {
  if (write_buffer_manager_ && write_buffer_manager_->enabled()) {
    bytes_allocated_.fetch_add(bytes, std::memory_order_relaxed);
    write_buffer_manager_->charge_mem(bytes);
  }
  return allocator_->AllocateAligned(bytes, huge_page_size);
}

void MemTableAllocator::DoneAllocating() {
  if (write_buffer_manager_ && write_buffer_manager_->enabled()) {
    if (!active_mem_discharged_) {
      write_buffer_manager_->discharge_active_mem(
          bytes_allocated_.load(std::memory_order_relaxed));
      active_mem_discharged_ = true;
    }
  } else {
    assert(bytes_allocated_.load(std::memory_order_relaxed) == 0);
  }
}

size_t MemTableAllocator::BlockSize() const { return allocator_->BlockSize(); }

}  // namespace memtable
}  // namespace xengine
