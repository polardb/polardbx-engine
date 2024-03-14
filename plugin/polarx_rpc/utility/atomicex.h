//
// Created by zzy on 2022/7/19.
//

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <thread>
#include <cassert>

#include "../common_define.h"

#include "backoff.h"

namespace polarx_rpc {

class CspinLock final {
  NO_COPY_MOVE(CspinLock)

 private:
  std::atomic_flag lock_;

 public:
  inline void lock(size_t spin_cnt) {
    Cbackoff backoff;
    size_t spin = 0;
    while (UNLIKELY(lock_.test_and_set(std::memory_order_acquire))) {
      if (UNLIKELY(++spin >= spin_cnt)) {
        backoff.snooze();
        spin = 0;
      }
    }
  }

  inline bool try_lock() {
    return !lock_.test_and_set(std::memory_order_acquire);
  }

  inline void unlock() { lock_.clear(std::memory_order_release); }

  CspinLock() : lock_(false) {}
};

class CautoSpinLock final {
  NO_COPY(CautoSpinLock)

 private:
  CspinLock *lock_;

 public:
  CautoSpinLock() : lock_(nullptr) {}

  explicit CautoSpinLock(CspinLock &lock, size_t spin_cnt) : lock_(&lock) {
    lock_->lock(spin_cnt);
  }

  /// only move allowed
  CautoSpinLock(CautoSpinLock &&another) noexcept : lock_(another.lock_) {
    another.lock_ = nullptr;
  }

  /// no copy or assign
  CautoSpinLock &operator=(CautoSpinLock &&another) = delete;

  ~CautoSpinLock() {
    if (LIKELY(lock_ != nullptr)) lock_->unlock();
  }

  inline bool lock(CspinLock &lock, size_t spin_cnt) {
    if (LIKELY(nullptr == lock_)) {
      lock_ = &lock;
      lock_->lock(spin_cnt);
      return true;
    }
    return false;
  }

  inline bool try_lock(CspinLock &lock) {
    if (LIKELY(nullptr == lock_)) {
      if (lock.try_lock()) {
        lock_ = &lock;
        return true;
      }
    }
    return false;
  }

  inline void unlock() {
    if (LIKELY(lock_ != nullptr)) {
      lock_->unlock();
      lock_ = nullptr;
    }
  }
};

class CmcsSpinLock final {
  NO_COPY_MOVE(CmcsSpinLock)

 public:
  struct mcs_spin_node_t final {
    std::atomic<mcs_spin_node_t *> next;
    std::atomic_flag locked;

    /// leave it uninitialized
    mcs_spin_node_t() {}
  };

 private:
  std::atomic<mcs_spin_node_t *> lock_;

 public:
  CmcsSpinLock() : lock_(nullptr) {}

  inline void lock(mcs_spin_node_t &cur_node, const size_t spin_cnt) {
    /// init current node
    cur_node.next.store(nullptr, std::memory_order_relaxed);
    cur_node.locked.test_and_set(std::memory_order_relaxed);
    /// CAS lock(acquire for lock enter, release for cur_node info)
    auto prev = lock_.exchange(&cur_node, std::memory_order_acq_rel);
    if (LIKELY(nullptr == prev)) return;
    /// need spin and wait
    prev->next.store(&cur_node, std::memory_order_release);
    Cbackoff backoff;
    size_t spin = 0;
    /// queued and most likely to spin wait
    while (LIKELY(cur_node.locked.test_and_set(std::memory_order_acquire))) {
      if (UNLIKELY(++spin >= spin_cnt)) {
        backoff.snooze();
        spin = 0;
      }
    }
  }

  inline bool try_lock(mcs_spin_node_t &cur_node) {
    /// init current node
    cur_node.next.store(nullptr, std::memory_order_relaxed);
    cur_node.locked.test_and_set(std::memory_order_relaxed);
    mcs_spin_node_t *expected = nullptr;
    /// CAS lock(acquire for lock enter, release for cur_node info)
    return lock_.compare_exchange_strong(expected, &cur_node,
                                         std::memory_order_acq_rel,
                                         std::memory_order_relaxed);
  }

  inline void unlock(mcs_spin_node_t &cur_node) {
    auto next = cur_node.next.load(std::memory_order_acquire);
    if (LIKELY(nullptr == next)) {
      auto expected = &cur_node;
      if (LIKELY(lock_.compare_exchange_strong(expected, nullptr,
                                               std::memory_order_release,
                                               std::memory_order_relaxed)))
        return;
      Cbackoff backoff;
      /// get next queued
      while (nullptr == (next = cur_node.next.load(std::memory_order_acquire)))
        backoff.snooze();
    }
    next->locked.clear(std::memory_order_release);
  }
};

class CautoMcsSpinLock final {
  NO_COPY_MOVE(CautoMcsSpinLock)

 private:
  CmcsSpinLock *lock_;
  CmcsSpinLock::mcs_spin_node_t node_;

 public:
  CautoMcsSpinLock() : lock_(nullptr) {}

  explicit CautoMcsSpinLock(CmcsSpinLock &lock, size_t spin_cnt)
      : lock_(&lock) {
    lock_->lock(node_, spin_cnt);
  }

  ~CautoMcsSpinLock() {
    if (LIKELY(lock_ != nullptr)) lock_->unlock(node_);
  }

  inline bool lock(CmcsSpinLock &lock, size_t spin_cnt) {
    if (LIKELY(nullptr == lock_)) {
      lock_ = &lock;
      lock_->lock(node_, spin_cnt);
      return true;
    }
    return false;
  }

  inline bool try_lock(CmcsSpinLock &lock) {
    if (LIKELY(nullptr == lock_)) {
      if (lock.try_lock(node_)) {
        lock_ = &lock;
        return true;
      }
    }
    return false;
  }

  inline void unlock() {
    if (LIKELY(lock_ != nullptr)) {
      lock_->unlock(node_);
      lock_ = nullptr;
    }
  }
};

// Nonfair reenter-able writer-first spin read-write-lock.
class CspinRWLock final {
  NO_COPY_MOVE(CspinRWLock)

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
  NO_COPY_MOVE(CautoSpinRWLock)

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

}  // namespace polarx_rpc
