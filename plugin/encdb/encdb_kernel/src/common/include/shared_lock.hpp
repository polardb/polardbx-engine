/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 */
#pragma once

#include <pthread.h>
#include "memory.hpp"

namespace encdb {

class SharedLock : public EncdbBaseObj {
 public:
  void lock() { pthread_rwlock_wrlock(&rwlock_); }

  void unlock() { pthread_rwlock_unlock(&rwlock_); }

  void lockShared() { pthread_rwlock_rdlock(&rwlock_); }

  void unlockShared() { pthread_rwlock_unlock(&rwlock_); }

  explicit SharedLock() { pthread_rwlock_init(&rwlock_, NULL); }

  ~SharedLock() { pthread_rwlock_destroy(&rwlock_); }

 private:
  pthread_rwlock_t rwlock_;
};

class Reader : public EncdbBaseObj {
 public:
  Reader(SharedLock &l) : lock_(l) { lock_.lockShared(); }
  ~Reader() { lock_.unlockShared(); }

 private:
  SharedLock &lock_;
};

class Writer : public EncdbBaseObj {
 public:
  Writer(SharedLock &l) : lock_(l) { lock_.lock(); }
  ~Writer() { lock_.unlock(); }

 private:
  SharedLock &lock_;
};

}  // namespace encdb
