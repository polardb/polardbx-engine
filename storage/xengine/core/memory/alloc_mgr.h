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

#include <string>
#include "mod_info.h"

namespace xengine {
namespace memory {

#define XE_MAP_FAILED  ((void *) -1)
class AllocMgr {
 public:
  static const size_t kMagicNumber = 0XABECCEBA;
  static const uint8_t kMaxUInt8 = 0x7f;
  static const uint8_t kFlagNumber = 0x10;
  static const uint8_t kNeedByte = sizeof(uint64_t) + 1;
  static const int64_t LIMIT_RATIO = 120;
  struct Block {
    Block() : magic_(kMagicNumber), size_(0), mod_id_(0) {}
    int64_t get_size() const { return size_; }
    size_t get_magic_number() const { return magic_; }
    size_t get_mod_id() const { return mod_id_; }

    const size_t magic_;
    int64_t size_;
    size_t mod_id_;
    char buf_[0];
  };

  static const size_t kBlockMeta = sizeof(Block);

 public:
  AllocMgr();
  virtual ~AllocMgr();
  void *balloc(const int64_t size, const size_t mod_id = 0);
  void *balloc_chunk(const int64_t size, const size_t mod_id = 0);
  void *brealloc(void *ptr, const int64_t size, const size_t mod_id = 0);
  void bfree(void *ptr, bool update = true);
  void bfree_chunk(void *ptr, bool update = true);

  void *memalign_alloc(const size_t alignment, const int64_t size,
                       const size_t mod_id = 0);
  void memlign_free(void *ptr);

  void update_mod_id(const void *ptr, const size_t cur_mod);
  void update_hold_size(const int64_t size, const size_t mod_id = 0);
  size_t bmalloc_usable_size(void *ptr);
  size_t get_aligned_value(void *ptr, const size_t alignment) const;

  int64_t get_allocated_size(const size_t mod_id) const;
  int64_t get_hold_size(const size_t mod_id) const;
  int64_t get_alloc_count(const size_t mod_id) const;
  void print_memory_usage(std::string &stat_str);
  void get_memory_usage(MemItemDump *items, 
                        std::string *mod_names);
  bool check_if_memory_overflow() const;
  void set_mod_size_limit(const size_t mod_id, const int64_t size_limit);
  static AllocMgr *get_instance();

 private:
  void set_block(const int64_t size, const size_t mod_id, Block *block);

  ModMemSet mod_set_;
};
}
}
