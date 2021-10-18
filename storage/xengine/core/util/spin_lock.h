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

#ifndef IS_CONCURRENCY_SPIN_LOCK_H_
#define IS_CONCURRENCY_SPIN_LOCK_H_ 1
#include <pthread.h>
namespace xengine
{

namespace util
{
/**
 * A simple wrapper of pthread spin lock
 *
 */
class SpinLock
{
public:
  SpinLock();
  ~SpinLock();
  int lock();
  int trylock();
  int unlock();

private:
  // disallow copy
  SpinLock(const SpinLock &other);
  SpinLock &operator=(const SpinLock &other);

private:
  // data members
  pthread_spinlock_t lock_;
};

inline SpinLock::SpinLock()
{
  int ret = pthread_spin_init(&lock_, PTHREAD_PROCESS_PRIVATE);
  if (0 != ret) {
    assert(false);
  }
}

inline SpinLock::~SpinLock() { pthread_spin_destroy(&lock_); }
inline int SpinLock::lock() { return pthread_spin_lock(&lock_); }
inline int SpinLock::trylock() { return pthread_spin_trylock(&lock_); }
inline int SpinLock::unlock() { return pthread_spin_unlock(&lock_); }
////////////////////////////////////////////////////////////////
// A lock class that do nothing, used as template argument
class NullLock
{
public:
  NullLock(){};
  ~NullLock(){};
  int lock() { return 0; };
  int unlock() { return 0; };
private:
  // disallow copy
  NullLock(const NullLock &other);
  NullLock &operator=(const NullLock &other);
};

////////////////////////////////////////////////////////////////
template <typename LockT>
class LockGuard
{
public:
  LockGuard(LockT &lock);
  ~LockGuard();

  // disallow copy
  LockGuard(const LockGuard &other) = delete;
  LockGuard &operator=(const LockGuard &other) = delete;
  // disallow new
  void *operator new(std::size_t size) = delete;
  void *operator new(std::size_t size,
                     const std::nothrow_t &nothrow_constant) throw() = delete;
  void *operator new(std::size_t size, void *ptr) throw() = delete;

private:
  // data members
  LockT &lock_;
};

template <typename LockT>
inline LockGuard<LockT>::LockGuard(LockT &lock) : lock_(lock)
{
  int ret = lock_.lock();
  if (0 != ret) {
    assert(false);
  }
}

template <typename LockT>
inline LockGuard<LockT>::~LockGuard()
{
  lock_.unlock();
}

typedef LockGuard<SpinLock> SpinLockGuard;
}  // end namespace util
}

#endif /* IS_CONCURRENCY_SPIN_LOCK_H_ */
