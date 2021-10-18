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
#include "mod_info.h"
#include <assert.h>
#include "util/thread_local.h"

namespace xengine {
using namespace util;
namespace memory {

ModMemItem::ModMemItem()
    : alloc_size_(0),
      hold_size_(0),
      malloc_size_(0),
      alloc_cnt_(0),
      free_cnt_(0) {
  thread_value_ = new ThreadLocalPtr(&merge_thread_value);
}

ModMemItem::~ModMemItem() {
  delete thread_value_;
  thread_value_ = nullptr;
}

ThreadModMemItem* ModMemSet::get_thread_item(const size_t mod_id) {
  if (nullptr == items_[mod_id].thread_value_) {
    return nullptr;
  }
  ThreadModMemItem* item_ptr =
      static_cast<ThreadModMemItem*>(items_[mod_id].thread_value_->Get());
  if (nullptr == item_ptr) {
    item_ptr = new ThreadModMemItem(&items_[mod_id].alloc_size_,
              &items_[mod_id].hold_size_, &items_[mod_id].malloc_size_,
              &items_[mod_id].alloc_cnt_, &items_[mod_id].free_cnt_);
    items_[mod_id].thread_value_->Reset(item_ptr);
  }
  return item_ptr;
}

int64_t ModMemSet::get_allocated_size(const size_t mod_id) const {
  int64_t thread_local_size = 0;
  items_[mod_id].thread_value_->Fold(
      [](void *curr_ptr, void *res) {
        int64_t *sum_ptr = static_cast<int64_t *>(res);
        *sum_ptr += static_cast<ThreadModMemItem *>(curr_ptr)->alloc_size_;
      },
      &thread_local_size);
  return thread_local_size + items_[mod_id].alloc_size_;
}

int64_t ModMemSet::get_hold_size(const size_t mod_id) const {
  int64_t thread_local_size = 0;
  items_[mod_id].thread_value_->Fold(
      [](void *curr_ptr, void *res) {
        int64_t *sum_ptr = static_cast<int64_t *>(res);
        *sum_ptr += static_cast<ThreadModMemItem *>(curr_ptr)->hold_size_;
      },
      &thread_local_size);
  return thread_local_size + items_[mod_id].hold_size_;
}

int64_t ModMemSet::get_malloc_size(const size_t mod_id) const {
  int64_t thread_local_size = 0;
  items_[mod_id].thread_value_->Fold(
      [](void *curr_ptr, void *res) {
        int64_t *sum_ptr = static_cast<int64_t *>(res);
        *sum_ptr += static_cast<ThreadModMemItem *>(curr_ptr)->malloc_size_;
      },
      &thread_local_size);
  return thread_local_size + items_[mod_id].malloc_size_;
}

int64_t ModMemSet::get_alloc_count(const size_t mod_id) const {
  int64_t thread_local_cnt = 0;
  items_[mod_id].thread_value_->Fold(
      [](void *curr_ptr, void *res) {
        int64_t *sum_ptr = static_cast<int64_t *>(res);
        *sum_ptr += static_cast<ThreadModMemItem *>(curr_ptr)->alloc_cnt_;
      },
      &thread_local_cnt);
  return thread_local_cnt + items_[mod_id].alloc_cnt_;
}

int64_t ModMemSet::get_free_count(const size_t mod_id) const {
  int64_t thread_local_cnt = 0;
  items_[mod_id].thread_value_->Fold(
      [](void *curr_ptr, void *res) {
        int64_t *sum_ptr = static_cast<int64_t *>(res);
        *sum_ptr += static_cast<ThreadModMemItem *>(curr_ptr)->free_cnt_;
      },
      &thread_local_cnt);
  return thread_local_cnt + items_[mod_id].free_cnt_;
}

MemItemDump ModMemSet::extract_value(const size_t mod_id) const {
  MemItemDump mem_item;
  items_[mod_id].thread_value_->Fold(
      [](void *curr_ptr, void *res) {
        MemItemDump *sum_ptr = static_cast<MemItemDump *>(res);
        ThreadModMemItem *item_ptr = static_cast<ThreadModMemItem *>(curr_ptr);
        sum_ptr->alloc_size_ += item_ptr->alloc_size_;
        sum_ptr->hold_size_ += item_ptr->hold_size_;
        sum_ptr->malloc_size_ += item_ptr->malloc_size_;
        sum_ptr->total_alloc_cnt_ += item_ptr->alloc_cnt_;
        sum_ptr->alloc_cnt_ += item_ptr->alloc_cnt_ - item_ptr->free_cnt_;
      },
      &mem_item);
  mem_item.alloc_size_ += items_[mod_id].alloc_size_;
  mem_item.malloc_size_ += items_[mod_id].malloc_size_;
  mem_item.hold_size_ += items_[mod_id].hold_size_;
  mem_item.alloc_cnt_ += items_[mod_id].alloc_cnt_ - items_[mod_id].free_cnt_;
  mem_item.total_alloc_cnt_ += items_[mod_id].alloc_cnt_;
  if (0 == mem_item.hold_size_) {
    mem_item.hold_size_ = mem_item.alloc_size_;
  }
  if (mem_item.alloc_cnt_ > 0) {
    mem_item.avg_alloc_ = mem_item.alloc_size_ / mem_item.alloc_cnt_;
  } else {
    mem_item.avg_alloc_ = 0;
  }
  return mem_item;
}

void ModMemSet::update_alloc_size(const int64_t size, const int64_t malloc_size, const size_t mod_id) {
  if (mod_id >= kModMaxCnt) {
  } else {
    ThreadModMemItem *item_ptr = get_thread_item(mod_id);
    if (nullptr != item_ptr) {
      item_ptr->alloc_size_ += size;
      item_ptr->malloc_size_ += malloc_size;
      if (size < 0) {
        ++item_ptr->free_cnt_;
      } else {
        ++item_ptr->alloc_cnt_;
      }
    }
  }
}

void ModMemSet::update_hold_size(const int64_t size, const size_t mod_id) {
  if (mod_id >= kModMaxCnt) {
  } else {
    ThreadModMemItem *item_ptr = get_thread_item(mod_id);
    if (nullptr != item_ptr) {
      item_ptr->hold_size_ += size;
    }
  }
}
}
}
