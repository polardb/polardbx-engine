//
// Created by zzy on 2021/11/22.
//

#pragma once

#include <atomic>
#include <cstdint>
#include <thread>

#include "my_macros.h"

namespace gx {

#if defined(__GNUC__)
/* Tell the compiler that 'expr' probably evaluates to 'constant'. */
#define UNIV_EXPECT(expr, constant) __builtin_expect(expr, constant)
#else
#define UNIV_EXPECT(expr, value) (expr)
#endif

/* Tell the compiler that cond is likely to hold */
#define UNIV_LIKELY(cond) UNIV_EXPECT(cond, TRUE)
/* Tell the compiler that cond is unlikely to hold */
#define UNIV_UNLIKELY(cond) UNIV_EXPECT(cond, FALSE)

const uint64_t LOCK_SPIN_COUNT = 2000;

class CspinLock final {
  CspinLock(const CspinLock &) = delete;
  CspinLock(CspinLock &&) = delete;
  CspinLock &operator=(const CspinLock &) = delete;
  CspinLock &operator=(CspinLock &&) = delete;

 private:
  std::atomic_flag lock_ = ATOMIC_FLAG_INIT;

 public:
  inline void lock() {
    size_t spin = 0;
    while (UNIV_UNLIKELY(lock_.test_and_set(std::memory_order_acquire))) {
      if (UNIV_UNLIKELY(++spin == LOCK_SPIN_COUNT)) {
        std::this_thread::yield();
        spin = 0;
      }
    }
  }

  inline bool try_lock() {
    return !lock_.test_and_set(std::memory_order_acquire);
  }

  inline void unlock() { lock_.clear(std::memory_order_release); }

  CspinLock() = default;
};

class CautoSpinLock final {
  CautoSpinLock(const CautoSpinLock &) = delete;
  CautoSpinLock &operator=(const CautoSpinLock &) = delete;
  CautoSpinLock &operator=(CautoSpinLock &&) = delete;

 private:
  CspinLock *lock_;

 public:
  CautoSpinLock() : lock_(nullptr) {}
  explicit CautoSpinLock(CspinLock &lock) : lock_(&lock) { lock_->lock(); }

  CautoSpinLock(CautoSpinLock &&another) noexcept : lock_(another.lock_) {
    another.lock_ = nullptr;
  }

  ~CautoSpinLock() {
    if (UNIV_LIKELY(lock_ != nullptr)) lock_->unlock();
  }

  inline bool lock(CspinLock &lock) {
    if (UNIV_LIKELY(nullptr == lock_)) {
      lock_ = &lock;
      lock_->lock();
      return true;
    }
    return false;
  }

  inline bool try_lock(CspinLock &lock) {
    if (UNIV_LIKELY(nullptr == lock_)) {
      if (lock.try_lock()) {
        lock_ = &lock;
        return true;
      }
    }
    return false;
  }

  inline void unlock() {
    if (UNIV_LIKELY(lock_ != nullptr)) {
      lock_->unlock();
      lock_ = nullptr;
    }
  }
};

class CspinRWLock final {
  CspinRWLock(const CspinRWLock &) = delete;
  CspinRWLock(CspinRWLock &&) = delete;
  void operator=(const CspinRWLock &) = delete;

 private:
  static const uintptr_t RWLOCK_PART_BITS = sizeof(uintptr_t) * 4;
  static const uintptr_t RWLOCK_PART_OVERFLOW = static_cast<uintptr_t>(1)
                                                << RWLOCK_PART_BITS;
  static const uintptr_t RWLOCK_PART_MASK = RWLOCK_PART_OVERFLOW - 1;
  static const uintptr_t RWLOCK_PART_MAX = RWLOCK_PART_OVERFLOW - 1;
  static const uintptr_t RWLOCK_WRITE_ADD = 1;
  static const uintptr_t RWLOCK_READ_ADD = RWLOCK_PART_OVERFLOW;
#define RWLOCK_READ_COUNT(_x) ((_x) >> RWLOCK_PART_BITS)
#define RWLOCK_WRITE_COUNT(_x) ((_x)&RWLOCK_PART_MASK)

  std::atomic<uintptr_t>
      lock_counter_;  // High part read lock, low part write lock.
  std::atomic<std::thread::id> writer_;

 public:
  CspinRWLock() : lock_counter_(0), writer_(std::thread::id()) {}

  bool read_lock(bool wait = true) {
    auto old_cnt = lock_counter_.load(std::memory_order_acquire);
    // CAS or spin wait.
    while (true) {
      if (UNIV_UNLIKELY(RWLOCK_WRITE_COUNT(old_cnt) > 0)) {
        // Someone hold the write lock, or wait for write.
        // Check self reenter first.
        if (UNIV_UNLIKELY(writer_.load(std::memory_order_acquire) ==
                          std::this_thread::get_id())) {
          // Reenter(Single thread scope).
          assert(RWLOCK_READ_COUNT(old_cnt) < RWLOCK_PART_MAX);
          if (UNIV_UNLIKELY(!lock_counter_.compare_exchange_strong(
                  old_cnt, old_cnt + RWLOCK_READ_ADD))) {
            assert(false);
            lock_counter_ += RWLOCK_READ_ADD;
          }
          return true;
        }
        // Other thread hold the write lock, or wait for write.
        if (UNIV_UNLIKELY(!wait)) return false;
        size_t spin = 0;
        do {
          if (UNIV_UNLIKELY(++spin == LOCK_SPIN_COUNT)) {
            std::this_thread::yield();
            spin = 0;
          }
        } while (UNIV_LIKELY(
            RWLOCK_WRITE_COUNT(
                old_cnt = lock_counter_.load(std::memory_order_acquire)) > 0));
      }
      assert(RWLOCK_READ_COUNT(old_cnt) < RWLOCK_PART_MAX);
      if (UNIV_LIKELY(lock_counter_.compare_exchange_weak(
              old_cnt, old_cnt + RWLOCK_READ_ADD)))
        return true;
    }
  }

  void read_unlock() { lock_counter_ -= RWLOCK_READ_ADD; }

  bool write_lock(bool wait = true) {
    auto old_cnt = lock_counter_.load(std::memory_order_acquire);
    // CAS or spin wait.
    while (true) {
      if (UNIV_LIKELY(0 == old_cnt)) {  // No holder.
        if (UNIV_LIKELY(lock_counter_.compare_exchange_weak(
                old_cnt, old_cnt + RWLOCK_WRITE_ADD))) {
          writer_.store(std::this_thread::get_id(), std::memory_order_release);
          return true;
        }
      } else if (UNIV_LIKELY(0 == RWLOCK_WRITE_COUNT(old_cnt))) {
        // No zero, zero writer, so someone hold read lock.
        if (UNIV_UNLIKELY(!wait)) return false;
        // Add write mark and wait reader exit.
        if (UNIV_LIKELY(lock_counter_.compare_exchange_weak(
                old_cnt, old_cnt + RWLOCK_WRITE_ADD))) {
          writer_.store(std::this_thread::get_id(), std::memory_order_release);
          size_t spin = 0;
          do {
            if (UNIV_UNLIKELY(++spin == LOCK_SPIN_COUNT)) {
              std::this_thread::yield();
              spin = 0;
            }
          } while (UNIV_LIKELY(
              RWLOCK_READ_COUNT(old_cnt = lock_counter_.load(
                                    std::memory_order_acquire)) > 0));
          return true;
        }
      } else {
        // Someone hold write lock, or wait for write.
        // Check self reenter first.
        if (UNIV_UNLIKELY(writer_.load(std::memory_order_acquire) ==
                          std::this_thread::get_id())) {
          // Reenter(Single thread scope).
          if (UNIV_UNLIKELY(!lock_counter_.compare_exchange_strong(
                  old_cnt, old_cnt + RWLOCK_WRITE_ADD))) {
            assert(false);
            lock_counter_ += RWLOCK_WRITE_ADD;
          }
          return true;
        }
        // Other thread hold the write lock, or wait for write.
        if (UNIV_UNLIKELY(!wait)) return false;
        size_t spin = 0;
        do {
          if (UNIV_UNLIKELY(++spin == LOCK_SPIN_COUNT)) {
            std::this_thread::yield();
            spin = 0;
          }
        } while (UNIV_LIKELY(
            RWLOCK_WRITE_COUNT(
                old_cnt = lock_counter_.load(std::memory_order_acquire)) > 0));
      }
    }
  }

  void write_unlock() {
    // Single thread scope.
    auto old_cnt = lock_counter_.load(std::memory_order_acquire);
    if (UNIV_LIKELY(1 == RWLOCK_WRITE_COUNT(old_cnt)))  // Last one.
      writer_.store(std::thread::id(), std::memory_order_release);
    if (UNIV_UNLIKELY(!lock_counter_.compare_exchange_strong(
            old_cnt, old_cnt - RWLOCK_WRITE_ADD))) {
      assert(false);
      lock_counter_ -= RWLOCK_WRITE_ADD;
    }
  }
};

class CautoSpinRWLock final {
  CautoSpinRWLock(const CautoSpinRWLock &) = delete;
  CautoSpinRWLock(CautoSpinRWLock &&) = delete;
  void operator=(const CautoSpinRWLock &) = delete;

 private:
  CspinRWLock &lock_;
  bool write_;

 public:
  explicit CautoSpinRWLock(CspinRWLock &lock, bool write = false)
      : lock_(lock), write_(write) {
    if (UNIV_UNLIKELY(write_))
      lock_.write_lock(true);
    else
      lock_.read_lock(true);
  }

  ~CautoSpinRWLock() {
    if (UNIV_UNLIKELY(write_))
      lock_.write_unlock();
    else
      lock_.read_unlock();
  }

  bool downgrade() {
    if (UNIV_UNLIKELY(!write_)) return false;

    if (UNIV_UNLIKELY(!lock_.read_lock(false))) return false;
    lock_.write_unlock();

    write_ = false;
    return true;
  }
};

}  // namespace gx
