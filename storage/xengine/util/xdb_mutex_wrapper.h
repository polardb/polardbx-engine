/*
   Portions Copyright (c) 2020, Alibaba Group Holding Limited
   Copyright (c) 2015, Facebook, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

#pragma once

/* C++ standard header file */
#include <chrono>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <unordered_map>

/* MySQL header files */
#include "./my_sys.h"
#include "mysql/plugin.h"

/* XENGINE header files */
#include "xengine/utilities/transaction_db_mutex.h"

namespace myx {

class Xdb_mutex : public xengine::util::TransactionDBMutex {
  Xdb_mutex(const Xdb_mutex &p) = delete;
  Xdb_mutex &operator=(const Xdb_mutex &p) = delete;

public:
  Xdb_mutex();
  virtual ~Xdb_mutex();

  /*
    Override parent class's virtual methods of interrest.
  */

  // Attempt to acquire lock.  Return OK on success, or other Status on failure.
  // If returned status is OK, TransactionDB will eventually call UnLock().
  virtual xengine::common::Status Lock() override;

  // Attempt to acquire lock.  If timeout is non-negative, operation should be
  // failed after this many microseconds.
  // Returns OK on success,
  //         TimedOut if timed out,
  //         or other Status on failure.
  // If returned status is OK, TransactionDB will eventually call UnLock().
  virtual xengine::common::Status
  TryLockFor(int64_t timeout_time MY_ATTRIBUTE((__unused__))) override;

  // Unlock Mutex that was successfully locked by Lock() or TryLockUntil()
  virtual void UnLock() override;

private:
  mysql_mutex_t m_mutex;
  friend class Xdb_cond_var;

#ifndef STANDALONE_UNITTEST
  void set_unlock_action(const PSI_stage_info *const old_stage_arg);
  std::unordered_map<THD *, std::shared_ptr<PSI_stage_info>> m_old_stage_info;
#endif
};

class Xdb_cond_var : public xengine::util::TransactionDBCondVar {
  Xdb_cond_var(const Xdb_cond_var &) = delete;
  Xdb_cond_var &operator=(const Xdb_cond_var &) = delete;

public:
  Xdb_cond_var();
  virtual ~Xdb_cond_var();

  /*
    Override parent class's virtual methods of interrest.
  */

  // Block current thread until condition variable is notified by a call to
  // Notify() or NotifyAll().  Wait() will be called with mutex locked.
  // Returns OK if notified.
  // Returns non-OK if TransactionDB should stop waiting and fail the operation.
  // May return OK spuriously even if not notified.
  virtual xengine::common::Status
  Wait(const std::shared_ptr<xengine::util::TransactionDBMutex> mutex) override;

  // Block current thread until condition variable is notifiesd by a call to
  // Notify() or NotifyAll(), or if the timeout is reached.
  // If timeout is non-negative, operation should be failed after this many
  // microseconds.
  // If implementing a custom version of this class, the implementation may
  // choose to ignore the timeout.
  //
  // Returns OK if notified.
  // Returns TimedOut if timeout is reached.
  // Returns other status if TransactionDB should otherwis stop waiting and
  //  fail the operation.
  // May return OK spuriously even if not notified.
  virtual xengine::common::Status
  WaitFor(const std::shared_ptr<xengine::util::TransactionDBMutex> mutex,
          int64_t timeout_time) override;

  // If any threads are waiting on *this, unblock at least one of the
  // waiting threads.
  virtual void Notify() override;

  // Unblocks all threads waiting on *this.
  virtual void NotifyAll() override;

private:
  mysql_cond_t m_cond;
};

class Xdb_mutex_factory : public xengine::util::TransactionDBMutexFactory {
public:
  Xdb_mutex_factory(const Xdb_mutex_factory &) = delete;
  Xdb_mutex_factory &operator=(const Xdb_mutex_factory &) = delete;
  Xdb_mutex_factory() {}
  /*
    Override parent class's virtual methods of interrest.
  */

  virtual std::shared_ptr<xengine::util::TransactionDBMutex>
  AllocateMutex() override {
    return std::make_shared<Xdb_mutex>();
  }

  virtual std::shared_ptr<xengine::util::TransactionDBCondVar>
  AllocateCondVar() override {
    return std::make_shared<Xdb_cond_var>();
  }

  virtual ~Xdb_mutex_factory() {}
};

} // namespace myx
