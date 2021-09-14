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

#include "alloc_mgr.h"
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
#include <assert.h>
#include <stdio.h>
#include <algorithm>
#include <iostream>
#include "util/misc_utility.h"

#if defined(MAP_ANONYMOUS)
#define OS_MAP_ANON MAP_ANONYMOUS
#elif defined(MAP_ANON)
#define OS_MAP_ANON MAP_ANON
#endif
//#define MAP_ERROR (*__error())

namespace xengine {
namespace memory {
AllocMgr::AllocMgr() : mod_set_() {}

AllocMgr::~AllocMgr() {}

int64_t AllocMgr::get_allocated_size(const size_t mod_id) const {
  return mod_set_.get_allocated_size(mod_id);
}

int64_t AllocMgr::get_hold_size(const size_t mod_id) const {
  return mod_set_.get_hold_size(mod_id);
}

int64_t AllocMgr::get_alloc_count(const size_t mod_id) const {
  return mod_set_.get_alloc_count(mod_id);
}

// export the memory usage for information schema
void AllocMgr::get_memory_usage(MemItemDump *items, 
                                std::string *mod_names) {
  for (size_t i = 0; i < ModMemSet::kModMaxCnt; ++i) {
    items[i] = mod_set_.extract_value(i);
    items[i].id_ = i;
  }
  std::sort(items, items + ModMemSet::kModMaxCnt);
  for (size_t i = 0; i < ModMemSet::kModMaxCnt; ++i) {
    mod_names[i] = mod_set_.get_mod_name(items[i].id_);
  } 
}

bool AllocMgr::check_if_memory_overflow() const {
  bool bret = false;
  MemItemDump items[ModMemSet::kModMaxCnt];
  for (size_t i = 0; i < ModMemSet::kModMaxCnt; ++i) {
    items[i] = mod_set_.extract_value(i);
    if (mod_set_.size_limit_[i] > 0
        && items[i].alloc_size_ > mod_set_.size_limit_[i] * LIMIT_RATIO / 100) {
      // warn
      XENGINE_LOG(WARN, "MEMORY_CHECK: memory size beyond limit", K(i), K(items[i].alloc_size_));
      bret = true;
    }
  }
  return bret;
}


void AllocMgr::print_memory_usage(std::string &stat_str) {
  char buf[300];
  int64_t total_hold_size = 0;
  int64_t total_alloc_size = 0;
  int64_t total_alloc_cnt = 0;
  int64_t total_malloc_size = 0;
  MemItemDump items[ModMemSet::kModMaxCnt];

  for (size_t i = 0; i < ModMemSet::kModMaxCnt; ++i) {
    items[i] = mod_set_.extract_value(i);
    items[i].id_ = i;
  }
  std::sort(items, items + ModMemSet::kModMaxCnt);
  snprintf(buf, sizeof(buf),
      "\nMalloc:size allocated from jemalloc, Alloc: size allocated from AllocMgr, Used: actual used size"
      ", alloCnt: the count that allocated but not free, avgAlloc: Alloc/AllocCnt, "
      "TotalAllocCnt: all allocated count include free");
  stat_str.append(buf);
  for (size_t i = 0; i < ModMemSet::kModMaxCnt; ++i) {
    MemItemDump &item = items[i];
    snprintf(buf, sizeof(buf),
             "\n%-19s Malloc=%12ld Alloc=%12ld Used=%12ld avgAlloc=%8ld allocCnt=%8ld TotalAllocCnt=%10ld",
             mod_set_.get_mod_name(item.id_), item.malloc_size_, item.alloc_size_, item.hold_size_,
             item.avg_alloc_, item.alloc_cnt_, item.total_alloc_cnt_);
    stat_str.append(buf);
    total_hold_size += item.hold_size_;
    total_alloc_size += item.alloc_size_;
    total_alloc_cnt += item.alloc_cnt_;
    total_malloc_size += item.malloc_size_;
  }
  snprintf(buf, sizeof(buf),
           "\ntotal_malloc_memory=%lu, total_alloc_memory=%lu,"
           "total_hold_memory = %lu, total_alloc_count = %lu",
           total_malloc_size, total_alloc_size, total_hold_size, total_alloc_cnt);
  stat_str.append(buf);
}

// upper align
uint64_t AllocMgr::get_aligned_value(void *ptr, const size_t align) const {
  return ((uint64_t)ptr + align - 1) & (~(align - 1));
}

void AllocMgr::update_mod_id(const void *ptr, const size_t cur_mod) {
  if (nullptr != ptr) {
    Block *block = reinterpret_cast<Block *>((char *)ptr - kBlockMeta);
    assert(block);
    if (kMagicNumber == block->magic_) {
      int64_t tsize = 0;
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
      tsize = malloc_usable_size(block);
#else
      tsize = block->size_;
#endif
      mod_set_.update_alloc_size(-block->size_, -tsize, block->mod_id_);
      mod_set_.update_alloc_size(block->size_, tsize, cur_mod);
      block->mod_id_ = cur_mod;
    }
  }
}

void AllocMgr::update_hold_size(const int64_t size, const size_t mod_id)
{
  mod_set_.update_hold_size(size, mod_id);
}

size_t AllocMgr::bmalloc_usable_size(void *ptr) {
  size_t size = 0;
  if (nullptr != ptr) {
    Block *block = reinterpret_cast<Block *>((char *)ptr - kBlockMeta);
    if (nullptr != block) {
      assert(kMagicNumber == block->magic_);
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
      size = malloc_usable_size(block);
#else
      size = block->size_;
#endif
    }
  }
  return size;
}

void *AllocMgr::balloc_chunk(const int64_t size, const size_t mod_id) {
  void *rtptr = nullptr;
#ifndef OS_WIN
  rtptr = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | OS_MAP_ANON, -1, 0);
  if (rtptr == XE_MAP_FAILED) {
    XENGINE_LOG(ERROR, "mmap for chunk failed", K(size), K(errno));
    return nullptr;
  }
#else
  rtptr = ::malloc(size);
#endif
  Block *block = new (rtptr) Block();
  set_block(size, mod_id, block);
  if (nullptr != block) {
    rtptr = block->buf_;
  }
  return rtptr;
}

void AllocMgr::bfree_chunk(void *ptr, bool update) {
  if (nullptr != ptr) {
    Block *block = reinterpret_cast<Block *>((char *)ptr - kBlockMeta);
    if (nullptr != block) {
      if (kMagicNumber != block->magic_) {
        XENGINE_LOG(ERROR, "check magic number failed", K(block->magic_));
        BACKTRACE(ERROR, "alloc memory failed! ");
      }
      assert(kMagicNumber == block->magic_);
      size_t free_size = block->size_;
      size_t mod_id = block->mod_id_;
      int64_t tsize = free_size;
      // munmap
#ifndef OS_WIN
      munmap(block, free_size);
#else
      ::free(block);
#endif
      if (update) {
        mod_set_.update_alloc_size(-free_size, -tsize, mod_id);
      }
    }
  }
}

void *AllocMgr::balloc(const int64_t size, const size_t mod_id) {
  void *rtptr = nullptr;
  if (size > 0) {
    int64_t all_size = size + kBlockMeta;
    rtptr = ::malloc(all_size);
    Block *block = new (rtptr) Block();
    set_block(all_size, mod_id, block);
    if (nullptr != block) {
      rtptr = block->buf_;
    }
  }
  return rtptr;
}

void AllocMgr::set_block(const int64_t size, const size_t mod_id, Block *block) {
  if (nullptr != block) {
    block->size_ = size;
    block->mod_id_ = mod_id;
    int64_t tsize = 0;
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
    tsize = malloc_usable_size(block);
#else
    tsize = block->size_;
#endif
    mod_set_.update_alloc_size(size, tsize, mod_id);
  } else {
    XENGINE_LOG(ERROR, "alloc memory failed", K(size), K(mod_id));
    BACKTRACE(ERROR, "alloc memory failed! ");
  }
}

void *AllocMgr::memalign_alloc(size_t alignment, int64_t size, size_t mod_id) {
  void *rtptr = nullptr;
  void *ptr = balloc(size + alignment, mod_id);
  // calculate aligned addr
  if (nullptr != ptr) {
    rtptr = reinterpret_cast<void *>(get_aligned_value(ptr, alignment));
    if (rtptr == ptr) {
      rtptr = reinterpret_cast<void *>((char *)rtptr + alignment);
    }
    uint64_t ptr_offset = (char *)rtptr - (char *)ptr;
    uint8_t *flag = reinterpret_cast<uint8_t *>((char *)rtptr - 1);
    assert(nullptr != flag);
    // save offset value
    if (ptr_offset < kNeedByte) {
      *flag = static_cast<uint8_t>(ptr_offset) & kMaxUInt8;
    } else {
      *flag = kFlagNumber;
      uint64_t *start = reinterpret_cast<uint64_t *>((char *)(rtptr)-kNeedByte);
      *start = ptr_offset;
    }
  }
  return rtptr;
}

void AllocMgr::memlign_free(void *ptr) {
  if (nullptr != ptr) {
    char *fptr = nullptr;
    uint8_t *flag = reinterpret_cast<uint8_t *>((char *)ptr - 1);
    assert(flag);
    if (kFlagNumber != *flag) {
      fptr = (char *)ptr - (*flag);
    } else {
      uint64_t *start = reinterpret_cast<uint64_t *>((char *)ptr - kNeedByte);
      assert(start);
      fptr = (char *)ptr - (*start);
    }
    bfree(fptr);
  }
}

void *AllocMgr::brealloc(void *ptr, const int64_t size, const size_t mod_id) {
  void *rtptr = nullptr;
  if (nullptr != ptr) {
    Block *block = reinterpret_cast<Block *>((char *)ptr - kBlockMeta);
    assert(nullptr != block && kMagicNumber == block->magic_);
    int64_t psize = block->size_;
    int64_t ptsize = 0;
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
      ptsize = malloc_usable_size(block);
#else
      ptsize = block->size_;
#endif
    size_t all_size = 0 == size ? size : size + kBlockMeta;
    rtptr = ::realloc(block, all_size);
    if (0 == all_size) {
      mod_set_.update_alloc_size(-psize, -ptsize, mod_id);
    } else if (nullptr != rtptr) {
      block = new (rtptr) Block();
      block->size_ = all_size;
      block->mod_id_ = mod_id;
      rtptr = block->buf_;
      int64_t tsize = 0;
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
      tsize = malloc_usable_size(block);
#else
      tsize = block->size_;
#endif
      mod_set_.update_alloc_size(-psize, -ptsize, mod_id);
      mod_set_.update_alloc_size(all_size, tsize, mod_id);
    } else {
      XENGINE_LOG(ERROR, "realloc memory failed", K(size), K(mod_id));
      BACKTRACE(ERROR, "realloc memory failed! ");
    }
  } else {
    rtptr = balloc(size, mod_id);
  }
  return rtptr;
}

void AllocMgr::bfree(void *ptr, bool update) {
  if (nullptr != ptr) {
    Block *block = reinterpret_cast<Block *>((char *)ptr - kBlockMeta);
    if (nullptr != block) {
      if (kMagicNumber != block->magic_) {
        XENGINE_LOG(ERROR, "check magic number failed", K(block->magic_));
        BACKTRACE(ERROR, "alloc memory failed! ");
      }
      assert(kMagicNumber == block->magic_);
      size_t free_size = block->size_;
      size_t mod_id = block->mod_id_;
      int64_t tsize = 0;
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
      tsize = malloc_usable_size(block);
#else
      tsize = block->size_;
#endif
      ::free(block);
      if (update) {
        mod_set_.update_alloc_size(-free_size, -tsize, mod_id);
      }
    }
  }
}

void AllocMgr::set_mod_size_limit(const size_t mod_id, const int64_t size_limit) {
  mod_set_.set_mod_size_limit(mod_id, size_limit);
}
AllocMgr *AllocMgr::get_instance() {
  static AllocMgr alloc_mgr;
  return &alloc_mgr;
}
}  // namespace memory
}  // namespace xengine
