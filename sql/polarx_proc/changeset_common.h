/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/


//
// Created by wumu on 2022/10/19.
//

#ifndef MYSQL_CHANGESET_COMMON_H
#define MYSQL_CHANGESET_COMMON_H

#include <atomic>
#include <list>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <algorithm>
#include <cstdint>
#include <thread>

#include <mysql.h>

#include "my_inttypes.h"
#include "my_sys.h"
#include "mysql/service_mysql_alloc.h"
#include "sql/psi_memory_key.h"

#define CHANGESET_PK_HEADER_SIZE 56

#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x) (__builtin_expect(!!(x), 1))
#define UNLIKELY(x) (__builtin_expect(!!(x), 0))
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif

#if defined(__x86_64__) || defined(_M_X64)
#define IS_X64 1
#else
#define IS_X64 0
#endif

#if IS_X64
#include <immintrin.h>
#endif

namespace im {

// 1024 * 1024 * 4
static const uint64_t MAX_MEMORY_SIZE = 1 << 22;

// 1024 * 1024 * 32
static const uint64_t MAX_MEMORY_LIMIT = 1 << 25;

static const double BUCKET_CAPACITY = 1000;

static const double BUCKET_FILL_RATE = 10000;

enum ChangeType { DELETE, INSERT };

struct FileNode {
  const char * file_name;
  uint64_t pk_nums;
  FileNode(const char * file_name, const uint64_t pk_nums) {
    this->file_name = file_name;
    this->pk_nums = pk_nums;
  }
};

struct FileHeader {
  uint64_t pk_nums;
};

struct RowHeader {
  ChangeType type;
  uint32 length;
};

class DBTableName {
 public:
  DBTableName() = default;
  DBTableName(std::string a, std::string b)
      : db_name(std::move(a)), tb_name(std::move(b)) {}

  std::string db_name;
  std::string tb_name;

  bool operator==(const DBTableName& t) const {
    return t.db_name == db_name && t.tb_name == tb_name;
  }

  bool operator<(const DBTableName& t) const {
    if (db_name == t.db_name) {
      return tb_name < t.tb_name;
    }
    return db_name < t.db_name;
  }
};

/**
 * @brief Change encapsulate primary key of changed rows
 *
 */
class Change {
 public:
  Change(ChangeType t, std::string pk)
      : pk(std::move(pk)), type(t), next(nullptr) {}

  std::string &get_primary_key() { return this->pk; }

  ChangeType get_change_type() { return type; }

  Change *get_next() { return next.get(); }

  void set_next(std::unique_ptr<Change> &ptr) { this->next = std::move(ptr); }

  void *operator new(size_t size) {
    return my_malloc(key_memory_CS_PRIMARY_KEY_CACHE, size,
                     MYF(MY_WME | ME_FATALERROR));
  }

  void operator delete(void *ptr) { my_free(ptr); }

 private:
  std::string pk;
  ChangeType type;  // type of this change
  std::unique_ptr<Change> next;
};

class ChangeSetCache {
 public:
  uint64_t insert_count;
  uint64_t update_count;
  uint64_t delete_count;

  my_off_t current_pos;
  mutable std::list<std::pair<my_off_t, std::unique_ptr<Change>>> current_pks;

  void *operator new(size_t size) {
    return my_malloc(key_memory_CS_PRIMARY_KEY_CACHE, size,
                     MYF(MY_WME | ME_FATALERROR));
  }

  void operator delete(void *ptr) { my_free(ptr); }

  ChangeSetCache() {
    insert_count = 0;
    update_count = 0;
    delete_count = 0;
    current_pos = 0;
  }

  void add_save_point(my_off_t pos) { current_pos = pos; }

  void rollback_to_save_point(my_off_t pos) {
    for (auto it = current_pks.begin(); it != current_pks.end(); ++it) {
      if ((*it).first >= pos) {
        current_pks.erase(it--);
      }
    }
  }

  void add_delete(const std::string &pk) {
    auto change = std::make_unique<Change>(DELETE, pk);
    current_pks.emplace_back(current_pos, std::move(change));
    delete_count++;
  }

  void add_insert(const std::string &pk) {
    auto change = std::make_unique<Change>(INSERT, pk);
    current_pks.emplace_back(current_pos, std::move(change));
    insert_count++;
  }

  void add_update(const std::string &pk_before, const std::string &pk_after) {
    auto change_delete = std::make_unique<Change>(DELETE, pk_before);
    auto change_insert = std::make_unique<Change>(INSERT, pk_after);

    current_pks.emplace_back(current_pos, std::move(change_delete));
    current_pks.emplace_back(current_pos, std::move(change_insert));
    update_count++;
  }
};

static constexpr uint32_t SPIN_LIMIT = 6;
static constexpr uint32_t YIELD_LIMIT = 10;

class Cbackoff final {
 private:
  uint32_t step_{0};

  static inline void inner_spin() {
#if defined(_WIN32)
    YieldProcessor();
#elif IS_X64
    ::_mm_pause();
#else
    asm volatile("yield");
#endif
  }

 public:
  inline void reset() { step_ = 0; }

  inline void spin() {
    auto limit = 1u << std::min(step_, SPIN_LIMIT);
    for (uint32_t i = 0; i < limit; ++i) inner_spin();
    if (step_ <= SPIN_LIMIT) ++step_;
  }

  inline void snooze() {
    auto limit = 1u << step_;
    if (step_ <= SPIN_LIMIT) {
      for (uint32_t i = 0; i < limit; ++i) inner_spin();
    } else {
      for (uint32_t i = 0; i < limit; ++i) inner_spin();
      std::this_thread::yield();
    }
    if (step_ <= YIELD_LIMIT) ++step_;
  }

  inline bool is_completed() const { return step_ > YIELD_LIMIT; }
};

// Nonfair reenter-able writer-first spin read-write-lock.
class CspinRWLock final {
 private:
  static constexpr uintptr_t RWLOCK_PART_BITS = sizeof(uintptr_t) * 4;
  static constexpr uintptr_t RWLOCK_PART_OVERFLOW = static_cast<uintptr_t>(1)
                                                    << RWLOCK_PART_BITS;
  static constexpr uintptr_t RWLOCK_PART_MASK = RWLOCK_PART_OVERFLOW - 1;
  static constexpr uintptr_t RWLOCK_PART_MAX = RWLOCK_PART_OVERFLOW - 1;
  static constexpr uintptr_t RWLOCK_WRITE_ADD = 1;
  static constexpr uintptr_t RWLOCK_READ_ADD = RWLOCK_PART_OVERFLOW;
#define RWLOCK_READ_COUNT(_x) ((_x) >> RWLOCK_PART_BITS)
#define RWLOCK_WRITE_COUNT(_x) ((_x) & RWLOCK_PART_MASK)

  std::atomic<uintptr_t>
      lock_counter_;  /// high part read lock, low part write lock
  std::atomic<std::thread::id> writer_;

 public:
  CspinRWLock() : lock_counter_(0), writer_(std::thread::id()) {}

  inline bool read_lock(const bool wait, const size_t spin_cnt) {
    Cbackoff backoff;
    auto old_cnt = lock_counter_.load(std::memory_order_acquire);
    /// CAS or spin wait
    while (true) {
      if (UNLIKELY(RWLOCK_WRITE_COUNT(old_cnt) > 0)) {
        /// someone hold the write lock, or wait for write
        /// check self reenter first(after acquire lock_counter_ and my thread,
        /// so relaxed)
        if (UNLIKELY(writer_.load(std::memory_order_relaxed) ==
                     std::this_thread::get_id())) {
          assert(RWLOCK_READ_COUNT(old_cnt) < RWLOCK_PART_MAX);
          /// reenter(Single thread scope, so relaxed)
          const auto before_cnt = lock_counter_.fetch_add(
              RWLOCK_READ_ADD, std::memory_order_relaxed);
          (void)before_cnt;
          assert(before_cnt == old_cnt);
          return true;
        }
        /// other thread hold the write lock, or wait for write
        if (UNLIKELY(!wait)) return false;
        size_t spin = 0;
        do {
          if (UNLIKELY(++spin >= spin_cnt)) {
            backoff.snooze();
            spin = 0;
          }
        } while (
            LIKELY(RWLOCK_WRITE_COUNT(old_cnt = lock_counter_.load(
                                          std::memory_order_acquire)) > 0));
      }
      assert(0 == RWLOCK_WRITE_COUNT(old_cnt));
      if (UNLIKELY(RWLOCK_READ_COUNT(old_cnt) >= RWLOCK_PART_MAX)) {
        ::fprintf(stderr, "Concurrent read count over limit in CspinRWLock.");
        ::abort();
      }
      if (LIKELY(lock_counter_.compare_exchange_weak(
              old_cnt, old_cnt + RWLOCK_READ_ADD, std::memory_order_acquire,
              std::memory_order_relaxed)))
        return true;
    }
  }

  inline void read_unlock() {
    const auto before_cnt =
        lock_counter_.fetch_sub(RWLOCK_READ_ADD, std::memory_order_release);
    (void)before_cnt;
    assert(RWLOCK_READ_COUNT(before_cnt) >= 1);
  }

  inline bool write_lock(const bool wait, const size_t spin_cnt) {
    Cbackoff backoff;
    auto old_cnt = lock_counter_.load(std::memory_order_acquire);
    /// CAS or spin wait
    while (true) {
      if (LIKELY(0 == old_cnt)) {  /// no holder
        if (LIKELY(lock_counter_.compare_exchange_weak(
                old_cnt, old_cnt + RWLOCK_WRITE_ADD, std::memory_order_acquire,
                std::memory_order_relaxed))) {
          writer_.store(std::this_thread::get_id(), std::memory_order_relaxed);
          return true;
        }
      } else if (LIKELY(0 == RWLOCK_WRITE_COUNT(old_cnt))) {
        /// no zero, zero writer, so someone hold read lock
        if (UNLIKELY(!wait)) return false;
        /// add write mark and wait reader exit
        if (LIKELY(lock_counter_.compare_exchange_weak(
                old_cnt, old_cnt + RWLOCK_WRITE_ADD, std::memory_order_acquire,
                std::memory_order_relaxed))) {
          size_t spin = 0;
          do {
            if (UNLIKELY(++spin >= spin_cnt)) {
              backoff.snooze();
              spin = 0;
            }
          } while (
              LIKELY(RWLOCK_READ_COUNT(old_cnt = lock_counter_.load(
                                           std::memory_order_acquire)) > 0));
          /// write thread info after enter the lock(relaxed)
          writer_.store(std::this_thread::get_id(), std::memory_order_relaxed);
          return true;
        }
      } else {
        /// someone hold write lock, or wait for write
        /// check self reenter first(after acquire lock_counter_ and my thread,
        /// so relaxed)
        if (UNLIKELY(writer_.load(std::memory_order_relaxed) ==
                     std::this_thread::get_id())) {
          if (UNLIKELY(RWLOCK_WRITE_COUNT(old_cnt) >= RWLOCK_PART_MAX)) {
            ::fprintf(stderr, "Reenter write count over limit in CspinRWLock.");
            ::abort();
          }
          /// reenter(single thread scope, so relaxed)
          const auto before_cnt = lock_counter_.fetch_add(
              RWLOCK_WRITE_ADD, std::memory_order_relaxed);
          (void)before_cnt;
          assert(before_cnt == old_cnt);
          return true;
        }
        /// other thread hold the write lock, or wait for write
        if (UNLIKELY(!wait)) return false;
        size_t spin = 0;
        do {
          if (UNLIKELY(++spin >= spin_cnt)) {
            backoff.snooze();
            spin = 0;
          }
        } while (
            LIKELY(RWLOCK_WRITE_COUNT(old_cnt = lock_counter_.load(
                                          std::memory_order_acquire)) > 0));
      }
    }
  }

  inline void write_unlock() {
    /// single thread scope, so relaxed
    auto old_cnt = lock_counter_.load(std::memory_order_relaxed);
    assert(RWLOCK_WRITE_COUNT(old_cnt) >= 1);
    if (LIKELY(1 == RWLOCK_WRITE_COUNT(old_cnt)))  /// last one
      writer_.store(std::thread::id(), std::memory_order_relaxed);
    const auto before_cnt =
        lock_counter_.fetch_sub(RWLOCK_WRITE_ADD, std::memory_order_release);
    (void)before_cnt;
    assert(before_cnt == old_cnt);
  }
};

class CautoSpinRWLock final {
 private:
  CspinRWLock &lock_;
  bool write_;

 public:
  explicit CautoSpinRWLock(CspinRWLock &lock, bool write, size_t spin_cnt)
      : lock_(lock), write_(write) {
    if (UNLIKELY(write_))
      lock_.write_lock(true, spin_cnt);
    else
      lock_.read_lock(true, spin_cnt);
  }

  ~CautoSpinRWLock() {
    if (UNLIKELY(write_))
      lock_.write_unlock();
    else
      lock_.read_unlock();
  }

  bool downgrade() {
    if (UNLIKELY(!write_)) return false;

    if (UNLIKELY(!lock_.read_lock(false, 0))) return false;
    lock_.write_unlock();

    write_ = false;
    return true;
  }
};
}  // namespace im

namespace std {
template <>
struct hash<im::DBTableName> {
  std::size_t operator()(const im::DBTableName &k) const {
    using std::hash;
    using std::string;

    // Compute individual hash values for first,
    // second and third and combine them using XOR
    // and bit shifting:

    return ((hash<string>()(k.db_name) ^ (hash<string>()(k.tb_name) << 1)) >>
            1);
  }
};
}  // namespace std

#endif  // MYSQL_CHANGESET_COMMON_H
