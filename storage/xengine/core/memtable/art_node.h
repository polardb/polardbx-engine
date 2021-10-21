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

#include "port/likely.h"
#include "xengine/status.h"
#include "xengine/xengine_constants.h"
#include "util/opt_latch.h"
#include "logger/log_module.h"
#include "memory/base_malloc.h"
#include "util/coding.h"
#include "xengine/slice.h"
#include "util/common.h"
#include "logger/logger.h"

#ifdef __x86_64__
#include <x86intrin.h>
#include <emmintrin.h>
#include <immintrin.h>
#endif
#include <cstdio>
#include <queue>

namespace xengine {
namespace memtable {

/*
 * Value node for ART.
 * 
 * There are two types of nodes in ART, which are inner node and value node.
 * Value node keeps prefixed length key and value, as well as prev and next ptr for doubly linked value list.
 * Because ART uses suffix compression, so we must keep the whole key in value node.
 * Inner node has four types with different fanouts, which are fanout of 4, 16, 48, 256.
 * Different types of inner nodes have different storage formats.
 */
class ARTValue {
private:
  static const uint64_t LOCK_MASK = 1UL << 63;
  static const uint64_t COMMIT_MASK = 1UL << 63;
  static const uint64_t PTR_MASK = ~(1UL << 63);

  /*
   * ART comsumes more memory than skiplist because of the trie structure.
   * For the purpose of saving memory, we encode the lock and commit flag into the prev and next ptr.
   * 
   * Next ptr format: | lock(1bit) | next ptr(63bit) |
   * Prev ptr format: | commit flag(1bit) | prev ptr(63bit) |
   */
  std::atomic<uint64_t> prev_and_commit_;
  std::atomic<uint64_t> next_and_lock_;
  // length prefixed key and length prefixed value
  char entry_[0];
  
public:
  ARTValue() : prev_and_commit_(0), next_and_lock_(0) {}
  ~ARTValue() {}
  const Slice key() const { return GetLengthPrefixedSlice(entry_); }
  const Slice value() const;
  const char *entry() const { return entry_; }
  ARTValue *prev() const;
  ARTValue *next() const;
  void set_prev(const ARTValue *p);
  void set_next(const ARTValue *n);
  bool is_locked(const uint64_t lock) const { return (lock & LOCK_MASK) > 0; }
  void lock();
  void unlock() { next_and_lock_.fetch_xor(LOCK_MASK); }
  // Commit when trie insert and value list insert are all done
  void commit() { prev_and_commit_.fetch_or(COMMIT_MASK); }
  bool is_commit() const { return (prev_and_commit_.load() & COMMIT_MASK) > 0; }
  ARTValue *wait_for_commit();
};

/*
 * Base class of inner node for ART.
 * For details in inner node, please see comments of 'ARTValue'.
 */
class ARTNodeBase {
public:
  static const int32_t PREFIX_BUF_SIZE = 16;
  static const uint32_t PREFIX_ALIGN_SIZE = 8;
  static const int32_t PREFIX_LEN_OFFSET = 48;
  static const uint64_t PREFIX_CONTENT_MASK = (1UL << PREFIX_LEN_OFFSET) - 1;
private:
  util::OptRWLatch lock_;
  int16_t count_; // Nums of keys in the node
  // For prefix compression
  uint8_t *alloc_ptr_;
  // | prefix len (16 bits) | prefix content ptr (48 bits) |
  std::atomic<uint64_t> prefix_len_and_content_;
  uint8_t prefix_buf_[PREFIX_BUF_SIZE];
  /*
   * If there are two keys k1 and k2 in the ART,
   * and k1 is 'ABC' and k2 is 'ABCDE'.
   * It is obviously that k1 and k2 share the same prefix,
   * but don't have different suffixes.
   * So we use this field 'value_ptr_' to store a pointer to k1 
   * in the node corresponding to character 'D' in k2.
   * 
   * If there isn't such a case, the 'value_ptr_' will be nullptr.
   * 
   *     ______________________
   *    |__prefix___|___'AB'___|
   *    |_value_ptr_|_nullptr__|
   *    |____key____|___'C'____|
   *    |___child___|____\_____|  
   *                      \
   *                 ______\__________
   *                |___prefix__|_''__|
   *                |_value_ptr_|__---|---artvalue k1 'ABC' 
   *                |____key____|_'D'_|
   *                |___child___|__/__|
   *                              /
   *                     artvalue k2 'ABCDE'
   */
  void *value_ptr_;

protected:
  void inc_count() { count_ += 1; }

public:
  ARTNodeBase() 
  : lock_(), 
    count_(0),
    alloc_ptr_(nullptr),
    prefix_len_and_content_(0),
    value_ptr_(nullptr) {}
  virtual ~ARTNodeBase() {
    // The prefix len of the node is larger than prefix buf size,
    // so we alloc an extra buf for prefix.
    // When the node is in destruction, we should free the memory.
    if (alloc_ptr_ != prefix_buf_) {
      memory::base_memalign_free(alloc_ptr_);
    }
  }
  int check_version(uint64_t version_snapshot) { return lock_.check_version(version_snapshot); }
  int try_rdlock(uint64_t &version_snapshot) { return lock_.try_rdlock(version_snapshot); }
  int try_upgrade_rdlock(uint64_t version_snapsot) { return lock_.try_upgrade_rdlock(version_snapsot); }
  int try_wrlock() { return lock_.try_wrlock(); }
  int rdunlock(uint64_t version_snapshot) { return lock_.rdunlock(version_snapshot); }
  void wrunlock() { lock_.wrunlock(); }
  void wrunlock_with_obsolete() { lock_.wrunlock_with_obsolete(); }
  int count() { return count_; }
  bool has_value_ptr() { return value_ptr_ != nullptr; }
  void *value_ptr() { return value_ptr_; }
  void set_value_ptr(void *p) { value_ptr_ = p; }
  bool is_empty() { return count_ == 0; }
  uint8_t *prefix(uint64_t prefix_len_and_content) { return reinterpret_cast<uint8_t *>(prefix_len_and_content & PREFIX_CONTENT_MASK); }
  uint8_t *prefix() { return prefix(prefix_len_and_content_.load()); }
  uint16_t prefix_len(uint64_t prefix_len_and_content) { return prefix_len_and_content >> PREFIX_LEN_OFFSET; }
  uint16_t prefix_len() { return prefix_len(prefix_len_and_content_.load()); }
  int set_prefix(const uint8_t *prefix, uint64_t len);
  int check_prefix(const uint8_t *raw_key, uint32_t &byte_pos, uint32_t user_key_len);
  virtual int init(const uint8_t *prefix, uint16_t prefix_len) = 0;
  virtual bool is_full() = 0;
  virtual size_t memory_usage() = 0;
  virtual void release() = 0;
  virtual int insert(uint8_t key, void *child) = 0;
  virtual int update(uint8_t key, void *child) = 0;
  virtual void *get(uint8_t key) = 0;
  virtual void *get_with_less_than_percent(uint8_t key, double &percent) = 0;
  virtual int expand(ARTNodeBase *&new_node) = 0;
  virtual void *maximum() = 0;
  virtual void *minimum() = 0;
  virtual void *less_than(uint8_t key) = 0;
  virtual void *greater_than(uint8_t key) = 0;
  virtual void dump_struct_info(FILE *fp, int64_t level, int64_t &last_node_id, int64_t &last_allocated_node_id, std::queue<ARTNodeBase *> &bfs_queue) = 0;
};


/*
 * Child ptr in inner node can point to artvalue or inner node.
 * We use the MSB to check which type the child ptr points to.
 * MSB set: point to artvalue MSB clear: point to inner node.
 */
#define ARTVALUE_PTR_MASK (1UL << 63)
#define IS_ARTVALUE(ptr) ((reinterpret_cast<uint64_t>((ptr)) & ARTVALUE_PTR_MASK) > 0)
#define VOID2INNERNODE(ptr) (reinterpret_cast<ARTNodeBase *>((ptr)))
#define VOID2ARTVALUE(ptr) ({ \
  uint64_t raw_ptr = reinterpret_cast<uint64_t>((ptr)) ^ ARTVALUE_PTR_MASK; \
  reinterpret_cast<ARTValue *>(raw_ptr)->wait_for_commit(); })
#define ARTVALUE2VOID(ptr) ({ \
  uint64_t raw_ptr = reinterpret_cast<uint64_t>((ptr)); \
  reinterpret_cast<void *>(raw_ptr | ARTVALUE_PTR_MASK); })
#define RELEASE(ptr) ({ \
  ARTNodeBase *p = VOID2INNERNODE(ptr); \
  p->release(); \
  MOD_DELETE_OBJECT(ARTNodeBase, p); })

class ARTNode256 : public ARTNodeBase {
private:
  static const int32_t FANOUT = 256;

  void *children_[FANOUT];

public:
  ARTNode256() {}
  virtual ~ARTNode256() {}
  virtual int init(const uint8_t *prefix, uint16_t prefix_len) override;
  virtual bool is_full() override { return count() == FANOUT; }
  virtual size_t memory_usage() override { return prefix_len() > PREFIX_BUF_SIZE ? prefix_len() + sizeof(ARTNode256) : sizeof(ARTNode256); }
  virtual void release() override;
  virtual int insert(uint8_t key, void *child) override;
  virtual int update(uint8_t key, void *child) override;
  virtual void *get(uint8_t key) override { return children_[key]; }
  virtual void *get_with_less_than_percent(uint8_t key, double &percent) override;
  virtual int expand(ARTNodeBase *&new_node) override;
  virtual void *maximum() override;
  virtual void *minimum() override;
  virtual void *less_than(uint8_t key) override;
  virtual void *greater_than(uint8_t key) override;
  virtual void dump_struct_info(FILE *fp, int64_t level, int64_t &last_node_id, int64_t &last_allocated_node_id, std::queue<ARTNodeBase *> &bfs_queue) override;
};

class ARTNode48 : public ARTNodeBase {
private:
  static const int32_t CHILDREN_MAP_SIZE = 256;
  static const int32_t FANOUT = 48;
  static const uint8_t EMPTY_FLAG = 255;

  uint8_t children_map_[CHILDREN_MAP_SIZE];
  void *children_[FANOUT];

  int copy_to_larger(ARTNodeBase *larger);

public:
  ARTNode48() {}
  virtual ~ARTNode48() {}
  virtual int init(const uint8_t *prefix, uint16_t prefix_len) override;
  virtual bool is_full() override { return count() == FANOUT; }
  virtual size_t memory_usage() override { return prefix_len() > PREFIX_BUF_SIZE ? prefix_len() + sizeof(ARTNode48) : sizeof(ARTNode48); }
  virtual void release() override;
  virtual int insert(uint8_t key, void *child) override;
  virtual int update(uint8_t key, void *child) override;
  virtual void *get(uint8_t key) override { return children_map_[key] != EMPTY_FLAG ? children_[children_map_[key]] : nullptr; }
  virtual void *get_with_less_than_percent(uint8_t key, double &percent) override;
  virtual int expand(ARTNodeBase *&new_node) override;
  virtual void *maximum() override;
  virtual void *minimum() override;
  virtual void *less_than(uint8_t key) override;
  virtual void *greater_than(uint8_t key) override;
  virtual void dump_struct_info(FILE *fp, int64_t level, int64_t &last_node_id, int64_t &last_allocated_node_id, std::queue<ARTNodeBase *> &bfs_queue) override;
};

class ARTNode16 : public ARTNodeBase {
private:
#ifdef __x86_64__
  typedef __m128i SIMD_REG;
#endif
  static const int32_t FANOUT = 16;
  static const uint8_t FLIP_MASK = 0x80;

  uint8_t key_[FANOUT];
  void *children_[FANOUT];

  uint8_t flip_sign(uint8_t key);
  bool find_pos(uint8_t key, int32_t &pos);
  int copy_to_larger(ARTNodeBase *larger);

public:
  ARTNode16() {}
  virtual ~ARTNode16() {}
  virtual int init(const uint8_t *prefix, uint16_t prefix_len) override;
  virtual bool is_full() override { return count() == FANOUT; }
  virtual size_t memory_usage() override { return prefix_len() > PREFIX_BUF_SIZE ? prefix_len() + sizeof(ARTNode16) : sizeof(ARTNode16); }
  virtual void release() override;
  virtual int insert(uint8_t key, void *child) override;
  virtual int update(uint8_t key, void *child) override;
  virtual void *get(uint8_t key) override;
  virtual void *get_with_less_than_percent(uint8_t key, double &percent) override;
  virtual int expand(ARTNodeBase *&new_node) override;
  virtual void *minimum() override;
  virtual void *maximum() override { return is_empty() == false ? children_[count() - 1] : nullptr; }
  virtual void *less_than(uint8_t key) override;
  virtual void *greater_than(uint8_t key) override;
  virtual void dump_struct_info(FILE *fp, int64_t level, int64_t &last_node_id, int64_t &last_allocated_node_id, std::queue<ARTNodeBase *> &bfs_queue) override;
};

class ARTNode4 : public ARTNodeBase {
private:
  static const int32_t FANOUT = 4;

  uint8_t key_[FANOUT];
  void *children_[FANOUT];

  /*
   * Find the position of the first key which is greater than or equal to target
   */
  bool find_pos(uint8_t key, int32_t &pos);
  int copy_to_larger(ARTNodeBase *larger);

public:
  ARTNode4() {}
  virtual ~ARTNode4() {}
  virtual int init(const uint8_t *prefix, uint16_t prefix_len) override;
  virtual bool is_full() override { return count() == FANOUT; }
  virtual size_t memory_usage() override { return prefix_len() > PREFIX_BUF_SIZE ? prefix_len() + sizeof(ARTNode4) : sizeof(ARTNode4); }
  virtual void release() override;
  virtual int insert(uint8_t key, void *child) override;
  virtual int update(uint8_t key, void *child) override;
  virtual void *get(uint8_t key) override;
  virtual void *get_with_less_than_percent(uint8_t key, double &percent) override;
  virtual int expand(ARTNodeBase *&new_node) override;
  virtual void *minimum() override;
  virtual void *maximum() override { return is_empty() == false ? children_[count() - 1] : nullptr; }
  virtual void *less_than(uint8_t key) override;
  virtual void *greater_than(uint8_t key) override;
  virtual void dump_struct_info(FILE *fp, int64_t level, int64_t &last_node_id, int64_t &last_allocated_node_id, std::queue<ARTNodeBase *> &bfs_queue) override;
};

} // namespace memtable
} // namespace xengine
