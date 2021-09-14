// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "util/arena.h"
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
#ifdef OS_FREEBSD
#include <malloc_np.h>
#else
#include <malloc.h>
#endif
#endif
#ifndef OS_WIN
#include <sys/mman.h>
#endif
#include <algorithm>
#include "port/port.h"
#include "xengine/env.h"
#include "logger/logger.h"

using namespace xengine::common;
using namespace xengine::port;

namespace xengine {
namespace util {

// MSVC complains that it is already defined since it is static in the header.
#ifndef _MSC_VER
const size_t Arena::kInlineSize;
#endif

const size_t Arena::kMinBlockSize = 4096;
const size_t Arena::kMaxBlockSize = 2u << 30;
static const int kAlignUnit = sizeof(void*);

size_t OptimizeBlockSize(size_t block_size) {
  // Make sure block_size is in optimal range
  block_size = std::max(Arena::kMinBlockSize, block_size);
  block_size = std::min(Arena::kMaxBlockSize, block_size);

  // make sure block_size is the multiple of kAlignUnit
  if (block_size % kAlignUnit != 0) {
    block_size = (1 + block_size / kAlignUnit) * kAlignUnit;
  }
  assert(block_size >= Arena::kMinBlockSize &&
         block_size <= Arena::kMaxBlockSize &&
         block_size % kAlignUnit == 0);

  // Counteract kBlockMeta bytes allocated from base_malloc.
  block_size -= memory::AllocMgr::kBlockMeta;
  return block_size;
}

Arena::Arena(size_t block_size, size_t huge_page_size, const size_t mod_id)
    : kBlockSize(OptimizeBlockSize(block_size)) {
  alloc_bytes_remaining_ = sizeof(inline_block_);
  blocks_memory_ += alloc_bytes_remaining_;
  aligned_alloc_ptr_ = inline_block_;
  unaligned_alloc_ptr_ = inline_block_ + alloc_bytes_remaining_;
  mod_id_ = mod_id;
  hold_size_ = 0;
#ifdef MAP_HUGETLB
  hugetlb_size_ = huge_page_size;
  if (hugetlb_size_ && kBlockSize > hugetlb_size_) {
    hugetlb_size_ = ((kBlockSize - 1U) / hugetlb_size_ + 1U) * hugetlb_size_;
  }
#endif
}

Arena::~Arena() {
  memory::update_hold_size(-hold_size_, mod_id_);
  hold_size_ = 0;
  for (const auto& block : blocks_) {
    memory::base_free(block);
  }
#ifdef MAP_HUGETLB
  for (const auto& mmap_info : huge_blocks_) {
    auto ret = munmap(mmap_info.addr_, mmap_info.length_);
    if (ret != 0) {
      // TODO(sdong): Better handling
    }
  }
#endif
}

char* Arena::AllocateFallback(size_t bytes, bool aligned) {
  if (bytes > kBlockSize / 4) {
    ++irregular_block_num;
    // Object is more than a quarter of our block size.  Allocate it separately
    // to avoid wasting too much space in leftover bytes.
    return AllocateNewBlock(bytes);
  }

  // We waste the remaining space in the current block.
  size_t size = 0;
  char* block_head = nullptr;
#ifdef MAP_HUGETLB
  if (hugetlb_size_) {
    size = hugetlb_size_;
    block_head = AllocateFromHugePage(size);
  }
#endif
  if (!block_head) {
    size = kBlockSize;
    block_head = AllocateNewBlock(size);
  }
  alloc_bytes_remaining_ = size - bytes;

  if (aligned) {
    aligned_alloc_ptr_ = block_head + bytes;
    unaligned_alloc_ptr_ = block_head + size;
    return block_head;
  } else {
    aligned_alloc_ptr_ = block_head;
    unaligned_alloc_ptr_ = block_head + size - bytes;
    return unaligned_alloc_ptr_;
  }
}

char* Arena::AllocateFromHugePage(size_t bytes) {
#ifdef MAP_HUGETLB
  if (hugetlb_size_ == 0) {
    return nullptr;
  }
  // already reserve space in huge_blocks_ before calling mmap().
  // this way the insertion into the vector below will not throw and we
  // won't leak the mapping in that case. if reserve() throws, we
  // won't leak either
  huge_blocks_.reserve(huge_blocks_.size() + 1);

  void* addr = mmap(nullptr, bytes, (PROT_READ | PROT_WRITE),
                    (MAP_PRIVATE | MAP_ANONYMOUS | MAP_HUGETLB), -1, 0);

  if (addr == MAP_FAILED) {
    return nullptr;
  }
  // the following shouldn't throw because of the above reserve()
  huge_blocks_.emplace_back(MmapInfo(addr, bytes));
  blocks_memory_ += bytes;
  return reinterpret_cast<char*>(addr);
#else
  return nullptr;
#endif
}

char* Arena::AllocateAligned(size_t bytes, size_t huge_page_size) {
  assert((kAlignUnit & (kAlignUnit - 1)) ==
         0);  // Pointer size should be a power of 2

#ifdef MAP_HUGETLB
  if (huge_page_size > 0 && bytes > 0) {
    // Allocate from a huge page TBL table.
    size_t reserved_size =
        ((bytes - 1U) / huge_page_size + 1U) * huge_page_size;
    assert(reserved_size >= bytes);

    char* addr = AllocateFromHugePage(reserved_size);
    if (addr == nullptr) {
      __XENGINE_LOG(WARN, "AllocateAligned fail to allocate huge TLB pages: %s", strerror(errno));
      // fail back to malloc
    } else {
      return addr;
    }
  }
#endif

  size_t current_mod =
      reinterpret_cast<uintptr_t>(aligned_alloc_ptr_) & (kAlignUnit - 1);
  size_t slop = (current_mod == 0 ? 0 : kAlignUnit - current_mod);
  size_t needed = bytes + slop;
  char* result;
  if (needed <= alloc_bytes_remaining_) {
    result = aligned_alloc_ptr_ + slop;
    aligned_alloc_ptr_ += needed;
    alloc_bytes_remaining_ -= needed;
    if (memory::ModId::kMemtable != mod_id_ && blocks_.size() > 0) {
      memory::update_hold_size(needed, mod_id_);
      hold_size_ += needed;
    }
  } else {
    // AllocateFallback always returns aligned memory
    result = AllocateFallback(bytes, true /* aligned */);
    if (nullptr != result && memory::ModId::kMemtable != mod_id_ && blocks_.size() > 0) {
      memory::update_hold_size(bytes, mod_id_);
      hold_size_ += bytes;
    }
  }
  assert((reinterpret_cast<uintptr_t>(result) & (kAlignUnit - 1)) == 0);
  return result;
}

char* Arena::AllocateNewBlock(size_t block_bytes) {
  // already reserve space in blocks_ before allocating memory via new.
  // this way the insertion into the vector below will not throw and we
  // won't leak the allocated memory in that case. if reserve() throws,
  // we won't leak either
  blocks_.reserve(blocks_.size() + 1);

  char* block =
      static_cast<char*>(memory::base_malloc(block_bytes, mod_id_));
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
  // obtain size of block of memory allocated from heap
  blocks_memory_ += memory::base_malloc_usable_size(block);
#else
  // Include kBlockMeta bytes allocated from base_malloc.
  blocks_memory_ += (block_bytes + memory::AllocMgr::kBlockMeta);
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
  // the following shouldn't throw because of the above reserve()
  blocks_.push_back(block);
  return block;
}
}  // namespace util
}  // namespace xengine
