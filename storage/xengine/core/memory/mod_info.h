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
#pragma once
#include <stddef.h>
#include <stdint.h>
#include <atomic>

namespace xengine {
namespace util {
class ThreadLocalPtr;
}
namespace memory {



struct ModId {
  enum ModType {
#define DEFINE_MOD_TYPE(name) name,
#include "modtype_define.h"
#undef DEFINE_MOD_TYPE
  };
};

struct ThreadModMemItem {
  ThreadModMemItem(int64_t* merge_alloc_size,
                   int64_t* merge_hold_size,
                   int64_t* merge_malloc_size,
                   int64_t* merge_alloc_cnt,
                   int64_t* merge_free_cnt)
      : alloc_size_(0),
        hold_size_(0),
        malloc_size_(0),
        alloc_cnt_(0),
        free_cnt_(0),
        merge_alloc_size_(merge_alloc_size),
        merge_hold_size_(merge_hold_size),
        merge_malloc_size_(merge_malloc_size),
        merge_alloc_cnt_(merge_alloc_cnt),
        merge_free_cnt_(merge_free_cnt)
        {}
  int64_t alloc_size_;
  int64_t hold_size_;
  int64_t malloc_size_;
  int64_t alloc_cnt_;
  int64_t free_cnt_;
  int64_t* merge_alloc_size_;
  int64_t* merge_hold_size_;
  int64_t* merge_malloc_size_;
  int64_t* merge_alloc_cnt_;
  int64_t* merge_free_cnt_;
};

struct ModMemItem {
  ModMemItem();
  ~ModMemItem();
  static void merge_thread_value(void *ptr) {
    ThreadModMemItem *item_ptr = static_cast<ThreadModMemItem *>(ptr);
    *item_ptr->merge_alloc_size_ += item_ptr->alloc_size_;
    *item_ptr->merge_malloc_size_ += item_ptr->malloc_size_;
    *item_ptr->merge_hold_size_ += item_ptr->hold_size_;
    *item_ptr->merge_alloc_cnt_ += item_ptr->alloc_cnt_;
    *item_ptr->merge_free_cnt_ += item_ptr->free_cnt_;
    delete item_ptr;
    //base_free(item_ptr);
  }
  int64_t alloc_size_;
  int64_t hold_size_;
  int64_t malloc_size_;
  int64_t alloc_cnt_;
  int64_t free_cnt_;
  // Holds thread-specific pointer to ModMemItem
  util::ThreadLocalPtr *thread_value_;
};

struct MemItemDump {
  MemItemDump()
      : id_(0),
        alloc_size_(0),
        hold_size_(0),
        malloc_size_(0),
        avg_alloc_(0),
        alloc_cnt_(0),
        total_alloc_cnt_(0) {}

  bool operator<(const MemItemDump &another) const {
    if (this->malloc_size_ == another.malloc_size_) {
      return this->alloc_size_ > another.alloc_size_;
    } else {
      return this->malloc_size_ > another.malloc_size_;
    }
  }

  size_t id_;
  int64_t alloc_size_;
  int64_t hold_size_;
  int64_t malloc_size_;
  int64_t avg_alloc_;
  int64_t alloc_cnt_;
  int64_t total_alloc_cnt_;
};

struct ModMemSet {
  static const size_t kModMaxCnt = ModId::kMaxMod;
  ModMemSet() {
#define DEFINE_MOD_TYPE(mod_id) set_mod_name(ModId::mod_id, #mod_id);
#include "modtype_define.h"
#undef DEFINE_MOD_TYPE
  }

  void set_mod_name(const size_t id, const char *name) {
    if (id >= kModMaxCnt) {
    } else {
      name_[id] = name;
      size_limit_[id] = 0;
    }
  }

  const char *get_mod_name(const size_t id) const {
    const char *name = "NULL";
    if (id < kModMaxCnt) {
      name = name_[id];
    }
    return name;
  }

  void set_mod_size_limit(const size_t id, const int64_t size_limit) {
    if (id >= kModMaxCnt) {
    } else {
      size_limit_[id] = size_limit;
    }
  }
  ThreadModMemItem* get_thread_item(const size_t mod_id);
  int64_t get_allocated_size(const size_t mod_id);
  int64_t get_allocated_size(const size_t mod_id) const;
  int64_t get_hold_size(const size_t mod_id) const;
  int64_t get_malloc_size(const size_t mod_id) const;
  int64_t get_alloc_count(const size_t mod_id) const;
  int64_t get_free_count(const size_t mod_id) const;
  MemItemDump extract_value(const size_t mod_id) const;
  void update_alloc_size(const int64_t size, const int64_t malloc_size, const size_t mod_id);
  void update_hold_size(const int64_t size, const size_t mod_id);

  ModMemItem items_[kModMaxCnt];
  const char *name_[kModMaxCnt];
  int64_t size_limit_[kModMaxCnt];
};
}
}
