//
// Created by wumu on 2022/5/16.
//

#ifndef MYSQL_LOCK_H
#define MYSQL_LOCK_H

#include <pthread.h>

namespace im {

class CondVar;

class Mutex {
 public:
  Mutex() { pthread_mutex_init(&mu_, nullptr); }

  ~Mutex() { pthread_mutex_destroy(&mu_); }

  void Lock() { pthread_mutex_lock(&mu_); }

  void Unlock() { pthread_mutex_unlock(&mu_); }

 private:
  friend class CondVar;
  pthread_mutex_t mu_{};

  Mutex(const Mutex &) = delete;
  Mutex &operator=(const Mutex &) = delete;
};

class CondVar {
 public:
  explicit CondVar(Mutex *mu) : mu_(mu) { pthread_cond_init(&cv_, nullptr); }
  ~CondVar() { pthread_cond_destroy(&cv_); }
  void Wait() { pthread_cond_wait(&cv_, &mu_->mu_); }
  void Signal() { pthread_cond_signal(&cv_); }
  void SignalAll() { pthread_cond_broadcast(&cv_); }

 private:
  pthread_cond_t cv_{};
  Mutex *mu_;
};

// Helper class that locks a mutex on construction and unlocks the mutex when
// the destructor of the MutexLock object is invoked.
//
// Typical usage:
//
//   void MyClass::MyMethod() {
//     MutexLock l(&mu_);       // mu_ is an instance variable
//     ... some complex code, possibly with multiple return paths ...
//   }

class MutexLock {
 public:
  explicit MutexLock(Mutex *mu) : mu_(mu) { this->mu_->Lock(); }

  ~MutexLock() { this->mu_->Unlock(); }

 private:
  Mutex *const mu_;
  // No copying allowed
  MutexLock(const MutexLock &) = delete;
  void operator=(const MutexLock &) = delete;
};

}  // namespace im

#endif  // MYSQL_LOCK_H
