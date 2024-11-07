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
