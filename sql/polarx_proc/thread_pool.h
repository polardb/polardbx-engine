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
// Created by wumu on 2022/5/13.
//

#ifndef MYSQL_THREAD_POOL_H
#define MYSQL_THREAD_POOL_H

#include <unistd.h>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <deque>
#include <vector>

#include "lock.h"


namespace im {
struct TaskItem {
  void (*function)(void *);

  void *arg;

  TaskItem(void (*function1)(void *), void *arg1)
      : function(function1), arg(arg1) {}

  ~TaskItem() = default;
};

class ThreadPool {
 public:
  explicit ThreadPool(uint32_t count) : cv_(&mu_), thread_count_(count) {
    no_doing_thread_count_ = 0;
    shutdown_ = false;
    threads_.reserve(count);
    threads_.insert(threads_.begin(), count, 0);
    for (uint32_t i = 0; i < thread_count_; i++) {
      if (pthread_create(&(threads_[i]), nullptr, &ThreadPool::BGThreadWrapper,
                         this) != 0) {
        exit(-1);
      }
    }
  }

  ~ThreadPool() {
    mu_.Lock();
    shutdown_ = true;
    cv_.SignalAll();
    mu_.Unlock();
    for (auto it : threads_) {
      pthread_join(it, nullptr);
    }
  }

  static void *BGThreadWrapper(void *arg) {  //运行后台线程的容器
    // ignore some signal
    sigset_t mysqld_signal_mask;
    sigemptyset(&mysqld_signal_mask);
    /*
      Block SIGQUIT, SIGHUP, SIGTERM, SIGUSR1 and SIGUSR2.
      The signal handler thread does sigwait() on these.
     */
    sigaddset(&mysqld_signal_mask, SIGQUIT);
    sigaddset(&mysqld_signal_mask, SIGHUP);
    sigaddset(&mysqld_signal_mask, SIGTERM);
    sigaddset(&mysqld_signal_mask, SIGTSTP);
    sigaddset(&mysqld_signal_mask, SIGUSR1);
    sigaddset(&mysqld_signal_mask, SIGUSR2);
    pthread_sigmask(SIG_BLOCK, &mysqld_signal_mask, nullptr);
    reinterpret_cast<ThreadPool *>(arg)->BGThread();
    return nullptr;
  }

  void BGThread() {  //后台线程循环执行任务
    while (true) {
      mu_.Lock();
      while (deque_.empty() && (!shutdown_)) {
        no_doing_thread_count_++;
        cv_.Wait();
        no_doing_thread_count_--;
      }
      if (shutdown_) {
        mu_.Unlock();
        break;
      }
      void (*function)(void *) = deque_.front().function;
      void *arg = deque_.front().arg;
      deque_.pop_front();
      mu_.Unlock();
      (*function)(arg);
    }
  }

  void Schedule(void (*function)(void *), void *arg) {  //添加任务，进行调度
    mu_.Lock();
    if (deque_.empty()) {
      cv_.Signal();
    } else {
      cv_.SignalAll();
    }
    deque_.emplace_back(function, arg);
    mu_.Unlock();
  }

  static void *StartThreadWrapper(void *arg) {
    auto *task = reinterpret_cast<TaskItem *>(arg);
    task->function(task->arg);
    delete task;
    return nullptr;
  }

  static pthread_t StartThread(void (*function)(void *),
                               void *arg) {  //直接新建线程运行任务，
    pthread_t t;
    auto *task = new TaskItem(function, arg);
    pthread_create(&t, nullptr, &StartThreadWrapper, task);
    return t;
  }

  void WaitForBGJob() {  //等待后台任务完成
    while (true) {
      mu_.Lock();
      if (deque_.empty() && no_doing_thread_count_ == thread_count_) {
        mu_.Unlock();
        break;
      }
      mu_.Unlock();
      sleep(2);
    }
  }

 private:
  Mutex mu_;
  CondVar cv_;
  std::deque<TaskItem> deque_;
  std::vector<pthread_t> threads_;
  uint32_t thread_count_;
  bool shutdown_;
  uint32_t no_doing_thread_count_;
};

extern ThreadPool *thread_pool;

extern int InitThreadPool(uint32_t count);

}  // namespace im

#endif  // MYSQL_THREAD_POOL_H
