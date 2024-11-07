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
// Created by zzy on 2022/7/5.
//

#pragma once

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <mutex>
#include <set>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <sched.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <sys/syscall.h>
#include <sys/sysinfo.h>
#include <sys/types.h>
#include <unistd.h>

#include "../common_define.h"
#include "../session/session.h"
#include "../utility/array_queue.h"
#include "../utility/atomicex.h"
#include "../utility/cpuinfo.h"
#include "../utility/perf.h"
#include "../utility/time.h"

#include "epoll_group_ctx.h"
#include "server_variables.h"
#include "timer_heap.h"

/** Note: for linux only */
#ifndef SO_REUSEPORT
#define SO_REUSEPORT 15
#endif

namespace polarx_rpc {

static constexpr uint32_t MAX_EPOLL_GROUPS = 128;
static constexpr uint32_t MAX_EPOLL_EXTRA_GROUPS = 32;
static constexpr uint32_t MAX_EPOLL_THREADS_PER_GROUP = 128;
static constexpr uint32_t MIN_EPOLL_WAIT_TOTAL_THREADS = 4;
static constexpr uint32_t MAX_EPOLL_WAIT_TOTAL_THREADS = 128;
static constexpr uint32_t MAX_EPOLL_EVENTS_PER_THREAD = 16;

static constexpr uint32_t MAX_EPOLL_TIMEOUT = 60 * 1000;  /// 60s

static constexpr uint32_t MAX_TCP_KEEP_ALIVE = 7200;
static constexpr uint32_t MIN_TCP_LISTEN_QUEUE = 1;
static constexpr uint32_t MAX_TCP_LISTEN_QUEUE = 4096;

static constexpr uint32_t MIN_WORK_QUEUE_CAPACITY = 128;
static constexpr uint32_t MAX_WORK_QUEUE_CAPACITY = 4096;

class CmtEpoll;

/**
 * General interface for epoll callback.
 */
class CepollCallback {
 public:
  virtual ~CepollCallback() = default;

  virtual void set_fd(int fd) = 0;

  /// for reclaim in epoll callback
  virtual void fd_pre_register() {}

  /// for rollback register if add epoll fail
  virtual bool fd_rollback_register() { return true; }

  /// notify for adding reference
  virtual void pre_events() {}

  /// destruct the context when return false
  virtual bool events(uint32_t events, int index, int total) = 0;

  virtual bool send(const void *data, size_t length) { return false; }
};

/**
 * Timer/worker task.
 */
struct task_t final {
 private:
  void *run_ctx_;
  void (*run_)(void *);
  void *del_ctx_;
  void (*del_)(void *);

 public:
  task_t()
      : run_ctx_(nullptr), run_(nullptr), del_ctx_(nullptr), del_(nullptr) {}
  task_t(void *run_ctx, void (*run)(void *), void *del_ctx, void (*del)(void *))
      : run_ctx_(run_ctx), run_(run), del_ctx_(del_ctx), del_(del) {}

  task_t(const task_t &another) = default;
  task_t(task_t &&another) noexcept
      : run_ctx_(another.run_ctx_),
        run_(another.run_),
        del_ctx_(another.del_ctx_),
        del_(another.del_) {
    another.run_ctx_ = nullptr;
    another.run_ = nullptr;
    another.del_ctx_ = nullptr;
    another.del_ = nullptr;
  }

  ~task_t() = default;

  task_t &operator=(const task_t &another) = default;

  task_t &operator=(task_t &&another) noexcept {
    run_ctx_ = another.run_ctx_;
    run_ = another.run_;
    del_ctx_ = another.del_ctx_;
    del_ = another.del_;
    another.run_ctx_ = nullptr;
    another.run_ = nullptr;
    another.del_ctx_ = nullptr;
    another.del_ = nullptr;
    return *this;
  }

  explicit operator bool() const { return run_ != nullptr; }

  void call() const {
    if (run_ != nullptr) run_(run_ctx_);
  }

  void fin() const {
    if (del_ != nullptr) del_(del_ctx_);
  }
};

/// The inherited class should has private destructor to prevent alloc on stack.
template <class T>
class Ctask {
  NO_COPY_MOVE(Ctask)

 protected:
  Ctask() = default;
  virtual ~Ctask() = default;

 private:
  static void run_routine(void *ctx) {
    auto task = reinterpret_cast<T *>(ctx);
    task->run();
  }

  static void del_routine(void *ctx) {
    auto task = reinterpret_cast<T *>(ctx);
    delete task;
  }

 public:
  // Caution: Must call this function with object by new.
  task_t gen_task() {
    return {this, Ctask::run_routine, this, Ctask::del_routine};
  }
};

class CtcpConnection;

class CmtEpoll final {
  NO_COPY_MOVE(CmtEpoll)

 private:
  /// group info
  const uint32_t group_id_;

  /// base epoll object
  int epfd_;

  /// timer task
  CmcsSpinLock timer_lock_;
  CtimerHeap<task_t> timer_heap_;

  /// work queue
  int eventfd_;
  CarrayQueue<task_t> work_queue_;

  /// worker wait counter
  std::atomic<intptr_t> wait_cnt_;
  std::atomic<intptr_t> loop_cnt_;

  /// extra data for epoll group
  epoll_group_ctx_t extra_ctx_;
  std::atomic<int64_t> last_cleanup_;

  /// affinity for dynamic threads
  std::atomic<int> affinity_version_;
  std::mutex affinity_lock_;
  bool with_affinity_;
  cpu_set_t cpus_{{}};
  std::string cores_str_;

  /// dynamic threads scale
  int base_thread_count_;
  std::atomic<int> stall_count_;
  std::atomic<int> worker_count_;  /// work with epoll
  std::atomic<int> tasker_count_;  /// work without epoll
  std::atomic<int64_t> last_scale_time_;
  std::atomic<int64_t> last_tasker_time_;
  std::mutex scale_lock_;
  std::atomic<int> session_count_;  /// all session under this epoll

  /// watch dog deadlock check
  size_t last_head_;
  intptr_t last_loop_;

  /// tcp set for visiting
  std::mutex tcp_lock_;  /// no multiple reader so just mutex
  std::set<CtcpConnection *> tcp_set_{};

  /// listener
  std::unique_ptr<CepollCallback> listener_{};

  static inline int nonblock(int fd, int set) {
    int flags;
    int r;
    do {
      r = ::fcntl(fd, F_GETFL);
    } while (UNLIKELY(r == -1 && errno == EINTR));

    if (UNLIKELY(r == -1)) return -errno;

    /** Bail out now if already set/clear. */
    if (!!(r & O_NONBLOCK) == !!set) return 0;

    if (set != 0)
      flags = r | O_NONBLOCK;
    else
      flags = r & ~O_NONBLOCK;

    do {
      r = ::fcntl(fd, F_SETFL, flags);
    } while (UNLIKELY(r == -1 && errno == EINTR));

    if (UNLIKELY(r != 0)) return -errno;
    return 0;
  }

  static inline int nodelay(int fd, int on) {
    if (UNLIKELY(::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on)) !=
                 0))
      return -errno;
    return 0;
  }

  static inline int keepalive(int fd, int on, unsigned int delay) {
    if (UNLIKELY(::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &on, sizeof(on)) !=
                 0))
      return -errno;

#ifdef TCP_KEEPIDLE
    if (on &&
        ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &delay, sizeof(delay)) != 0)
      return -errno;
#endif

      /** Solaris/SmartOS, if you don't support keep-alive,
       * then don't advertise it in your system headers...
       */
#if defined(TCP_KEEPALIVE) && !defined(__sun)
    if (on && ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPALIVE, &delay,
                           sizeof(delay)) != 0)
      return -errno;
#endif
    return 0;
  }

  inline void set_affinity(uint32_t thread_id, bool base_thread,
                           bool epoll_wait) {
    std::lock_guard<std::mutex> lck(affinity_lock_);

    if (!with_affinity_ || 0 == CPU_COUNT(&cpus_)) return;

    /// two cases:
    ///   1. single CPU affinity(base thread and no multi_affinity_in_group)
    ///   2. multi CPU affinity
    const auto tid = syscall(SYS_gettid);

    if (base_thread && !multi_affinity_in_group) {
      /// check and find correct one in cpus
      auto idx = thread_id % CPU_COUNT(&cpus_);
      auto affinity = -1;
      for (auto i = 0; i < CPU_SETSIZE; ++i) {
        if (CPU_ISSET(i, &cpus_)) {
          if (0 == idx--) {
            affinity = i;
            break;
          }
        }
      }

      /// bind if found
      if (affinity >= 0) {
        cpu_set_t cpu;
        CPU_ZERO(&cpu);
        CPU_SET(affinity, &cpu);
        const auto iret =
            pthread_setaffinity_np(pthread_self(), sizeof(cpu), &cpu);

        std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
        if (plugin_info.plugin_info != nullptr) {
          if (0 == iret)
            my_plugin_log_message(
                &plugin_info.plugin_info, MY_WARNING_LEVEL,
                "MtEpoll bind worker thread(tid:%lu) %u:%u(%u,%u) to CPU %d.",
                tid, group_id_, thread_id, base_thread, epoll_wait, affinity);
          else
            my_plugin_log_message(&plugin_info.plugin_info, MY_WARNING_LEVEL,
                                  "MtEpoll bind worker thread(tid:%lu) "
                                  "%u:%u(%u,%u) to CPU %d failed. %d",
                                  tid, group_id_, thread_id, base_thread,
                                  epoll_wait, affinity, iret);
        }
      }
    } else {
      /// bind for dynamic thread or multi bind base thread
      const auto iret =
          pthread_setaffinity_np(pthread_self(), sizeof(cpus_), &cpus_);

      std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
      if (plugin_info.plugin_info != nullptr) {
        if (0 == iret)
          my_plugin_log_message(
              &plugin_info.plugin_info, MY_WARNING_LEVEL,
              "MtEpoll bind%s worker thread(tid:%lu) %u:%u(%u,%u) to CPUs %s.",
              base_thread ? "" : " dynamic", tid, group_id_, thread_id,
              base_thread, epoll_wait, cores_str_.c_str());
        else
          my_plugin_log_message(&plugin_info.plugin_info, MY_WARNING_LEVEL,
                                "MtEpoll bind%s worker thread(tid:%lu) "
                                "%u:%u(%u,%u) to CPUs %s failed. %d",
                                base_thread ? "" : " dynamic", tid, group_id_,
                                thread_id, base_thread, epoll_wait,
                                cores_str_.c_str(), iret);
      }
    }
  }

  void loop(uint32_t thread_id, bool base_thread, bool epoll_wait,
            bool is_worker) {
    plugin_info.threads.fetch_add(1, std::memory_order_release);

    /// init local CPU affinity version
    auto local_affinity_version = 0;

    std::vector<task_t> timer_tasks;
    CmcsSpinLock::mcs_spin_node_t timer_lock_node;
    CsessionBase::init_thread_for_session();
    epoll_event events[MAX_EPOLL_EVENTS_PER_THREAD];
    while (true) {
      /// try pop and run task first
      while (true) {
        task_t t;  /// pop one task at a time(more efficient with multi-thread)
        int64_t start_time = 0;
        if (enable_perf_hist) start_time = Ctime::steady_ns();

        /// get one from queue.
        work_queue_.pop(t);

        if (start_time != 0) {
          auto task_end_time = Ctime::steady_ns();
          auto work_queue_time = task_end_time - start_time;
          g_work_queue_hist.update(static_cast<double>(work_queue_time) / 1e9);
        }

        if (!t) break;
        t.call();
        t.fin();
      }

      if (!base_thread) {
        if (UNLIKELY(shrink_thread_pool(is_worker))) break;
      } else if (plugin_info.exit.load(std::memory_order_acquire)) {
        /// shutdown exit
        std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
        if (plugin_info.plugin_info != nullptr)
          my_plugin_log_message(&plugin_info.plugin_info, MY_WARNING_LEVEL,
                                "MtEpoll worker thread(tid:%lu) "
                                "%u:%u(%u,%u) shutdown exit.",
                                syscall(SYS_gettid), group_id_, thread_id,
                                base_thread, epoll_wait);
        break;
      }

      /// limits the events
      auto max_events = epoll_events_per_thread;
      if (UNLIKELY(max_events <= 0))
        max_events = 1;
      else if (UNLIKELY(max_events > MAX_EPOLL_EVENTS_PER_THREAD))
        max_events = MAX_EPOLL_EVENTS_PER_THREAD;

      auto timeout = epoll_timeout;
      if (UNLIKELY(timeout <= 0))
        timeout = 1;  /// busy waiting not allowed
      else if (UNLIKELY(timeout > MAX_EPOLL_TIMEOUT))
        timeout = MAX_EPOLL_TIMEOUT;

      /// only one thread with correct timeout to trigger timer is ok
      if (timer_lock_.try_lock(timer_lock_node)) {
        int64_t next_trigger;
        auto has_next = timer_heap_.peak(next_trigger);
        if (has_next) {
          /// adjust timeout
          auto now_time = Ctime::steady_ms();
          if (LIKELY(next_trigger - now_time > 0)) {
            timeout =
                std::min(timeout, static_cast<uint32>(next_trigger - now_time));
            DBG_LOG(
                ("polarx_rpc thread %u:%u enter epoll with timer timeout %ums",
                 group_id_, thread_id, timeout));
          } else {
            timeout = 0;
            DBG_LOG(
                ("polarx_rpc thread %u:%u enter epoll with expired timer task",
                 group_id_, thread_id));
          }
        } else {
          DBG_LOG(("polarx_rpc thread %u:%u enter epoll with no timer task",
                   group_id_, thread_id));
        }
        timer_lock_.unlock(timer_lock_node);
      } else {
        DBG_LOG(
            ("polarx_rpc thread %u:%u enter epoll with failed timer lock race",
             group_id_, thread_id));
      }

      wait_cnt_.fetch_add(1, std::memory_order_release);
      if (!work_queue_.empty()) {
        wait_cnt_.fetch_sub(1, std::memory_order_release);
        continue;  /// dealing task first
      }
      int n;
      if (epoll_wait)
        n = ::epoll_wait(epfd_, events, static_cast<int>(max_events),
                         static_cast<int>(timeout));
      else {
        ::pollfd fds{eventfd_, POLLIN, 0};
        n = poll(&fds, 1, static_cast<int>(timeout));
        if (n > 0) {
          /// fake one
          assert(1 == n);
          events[0].data.fd = eventfd_;
          events[0].events = EPOLLIN;
        }
      }
      loop_cnt_.fetch_add(1, std::memory_order_relaxed);
      wait_cnt_.fetch_sub(1, std::memory_order_release);

      if (0 == n) {
        DBG_LOG(("polarx_rpc thread %u:%u leave epoll timeout, timeout %ums",
                 group_id_, thread_id, timeout));
      } else {
        DBG_LOG(("polarx_rpc thread %u:%u leave epoll with %d events",
                 group_id_, thread_id, n));
      }

      auto total = 0;
      for (auto i = 0; i < n; ++i) {
        if (events[i].data.fd == eventfd_) {
          /// consume the event fd as soon as possible
          /// which makes more threads notified if many tasks inserted
          uint64_t dummy;
          const auto result = ::read(eventfd_, &dummy, sizeof(dummy));
          (void)result;
          DBG_LOG(
              ("polarx_rpc thread %u:%u notified work", group_id_, thread_id));
        } else {
          auto cb = reinterpret_cast<CepollCallback *>(events[i].data.ptr);
          assert(cb != nullptr);
          cb->pre_events();
          ++total;
        }
      }

      /// Use `pre_events` to add reference count before actual invoke `events`
      /// to prevent time waste in events, which may cause dealing ptr if tcp
      /// connection context.
      auto index = 0;
      for (auto i = 0; i < n; ++i) {
        if (events[i].data.fd == eventfd_) continue;  /// ignore it
        auto cb = reinterpret_cast<CepollCallback *>(events[i].data.ptr);
        assert(cb != nullptr);
        auto bret = cb->events(events[i].events, index, total);
        if (!bret) delete cb;
        ++index;
      }

      /// timer task only one thread is ok
      if (timer_lock_.try_lock(timer_lock_node)) {
        int64_t timer_start_time = 0;
        if (enable_perf_hist) timer_start_time = Ctime::steady_ns();

        timer_tasks.clear();
        auto now_time = Ctime::steady_ms();
        task_t task;
        int32_t id;
        uint32_t type;
        while (timer_heap_.pop(now_time, task, id, type))
          timer_tasks.emplace_back(std::move(task));
        timer_lock_.unlock(timer_lock_node);

        /// run outside the lock
        for (const auto &t : timer_tasks) {
          t.call();
          t.fin();
        }

        if (timer_start_time != 0) {
          auto timer_end_time = Ctime::steady_ns();
          auto timer_time = timer_end_time - timer_start_time;
          g_timer_hist.update(static_cast<double>(timer_time) / 1e9);
        }
      }

      /// do clean up on extra context
      auto last_time = last_cleanup_.load(std::memory_order_relaxed);
      auto now_time = Ctime::steady_ms();
      if (UNLIKELY(now_time - last_time > epoll_group_ctx_refresh_time)) {
        /// every 10s
        if (last_cleanup_.compare_exchange_strong(last_time, now_time)) {
          /// only one thread do this
          int64_t cleanup_start_time = 0;
          if (enable_perf_hist) cleanup_start_time = Ctime::steady_ns();

          uintptr_t first = 0;
          for (auto i = 0; i < extra_ctx_.BUFFERED_REUSABLE_SESSION_COUNT;
               ++i) {
            std::unique_ptr<reusable_session_t> s;
            auto bret = extra_ctx_.reusable_sessions.pop(s);
            if (!bret) break;
            /// 10 min lifetime
            if (now_time - s->start_time_ms > shared_session_lifetime)
              s.reset();  /// release
            else {
              auto ptr_val = reinterpret_cast<uintptr_t>(s.get());
              extra_ctx_.reusable_sessions.push(std::move(s));  /// put it back
              if (0 == first)
                first = ptr_val;
              else if (ptr_val == first)
                break;  /// all checked
            }
          }

          if (cleanup_start_time != 0) {
            auto cleanup_end_time = Ctime::steady_ns();
            auto cleanup_time = cleanup_end_time - cleanup_start_time;
            g_cleanup_hist.update(static_cast<double>(cleanup_time) / 1e9);
          }
        }
      }

      /// and refresh bind cores
      auto ver = affinity_version_.load(std::memory_order_acquire);
      if (ver != local_affinity_version) {
        set_affinity(thread_id, base_thread, epoll_wait);
        local_affinity_version = ver;
      }
    }
    CsessionBase::deinit_thread_for_session();

    plugin_info.threads.fetch_sub(1, std::memory_order_release);
  }

  explicit CmtEpoll(uint32_t group_id, size_t work_queue_depth)
      : group_id_(group_id),
        work_queue_(work_queue_depth),
        wait_cnt_(0),
        loop_cnt_(0),
        last_cleanup_(0),
        affinity_version_(0),
        with_affinity_(false),
        base_thread_count_(0),
        stall_count_(0),
        worker_count_(0),
        tasker_count_(0),
        last_scale_time_(0),
        last_tasker_time_(0),
        session_count_(0),
        last_head_(0),
        last_loop_(0) {
    /// clear cpu set
    CPU_ZERO(&cpus_);

    /// init epoll
    epfd_ = ::epoll_create(0xFFFF);  // 65535
    if (UNLIKELY(epfd_ < 0)) throw std::runtime_error(std::strerror(errno));

    /// init eventfd
    eventfd_ = ::eventfd(0, EFD_NONBLOCK);
    if (UNLIKELY(eventfd_ < 0)) {
      ::close(epfd_);
      throw std::runtime_error(std::strerror(errno));
    }

    /// register it
    ::epoll_event event;
    event.data.fd = eventfd_;
    event.events = EPOLLIN | EPOLLET;  /// only notify one
    auto iret = ::epoll_ctl(epfd_, EPOLL_CTL_ADD, eventfd_, &event);
    if (UNLIKELY(iret != 0)) {
      ::close(eventfd_);
      ::close(epfd_);
      throw std::runtime_error(std::strerror(errno));
    }
  }

  ~CmtEpoll() {
    /// never exit
    ::abort();
  }

  inline void init_thread(uint32_t threads, int epoll_wait_threads,
                          int epoll_wait_gap) {
    /// record threads count first
    base_thread_count_ = worker_count_ = static_cast<int>(threads);
    global_thread_count() += static_cast<int>(threads);

    /// create threads
    for (uint32_t thread_id = 0; thread_id < threads; ++thread_id) {
      auto is_epoll_wait =
          0 == thread_id % epoll_wait_gap && --epoll_wait_threads >= 0;
      /// all thread is base thread when init
      std::thread thread(&CmtEpoll::loop, this, thread_id, true, is_epoll_wait,
                         true);
      thread.detach();
    }
  }

  static inline int get_core_number() {
#if defined(_WIN32)
    SYSTEM_INFO info;
    GetSystemInfo(&info);
    return (int)info.dwNumberOfProcessors;
#elif defined(__APPLE__)
    auto ncpu = 1;
    auto len = sizeof(ncpu);
    ::sysctlbyname("hw.activecpu", &ncpu, &len, nullptr, 0);
    return ncpu;
#else
    return ::get_nprocs();
#endif
  }

  static inline std::mutex &available_cpus_lock() {
    static std::mutex lock;
    return lock;
  }

  static inline cpu_set_t &available_cpus() {
    static cpu_set_t cpus = {0};
    return cpus;
  }

 public:
  static inline std::atomic<int> &global_thread_count() {
    static std::atomic<int> g_cnt(0);
    return g_cnt;
  }

  static inline CmtEpoll **get_instance(size_t &instance_count) {
    static std::once_flag once;
    static CmtEpoll **inst = nullptr;
    static size_t inst_cnt = 0;
    if (UNLIKELY(nullptr == inst || 0 == inst_cnt)) {
      std::call_once(once, []() {
        /// recheck all variables to prevent part read when modifying
        auto threads = epoll_threads_per_group;
        if (UNLIKELY(threads <= 0))
          threads = 1;
        else if (UNLIKELY(threads > MAX_EPOLL_THREADS_PER_GROUP))
          threads = MAX_EPOLL_THREADS_PER_GROUP;

        auto groups = epoll_groups;
        if (groups <= 0) {
          auto cores = get_core_number();
          if (auto_cpu_affinity) {
            if (force_all_cores) {
              /// get all cores from /proc/cpuinfo
              auto info_map = CcpuInfo::get_cpu_info();
              if (info_map.size() > 0) cores = info_map.size();
            } else {
              /// get all available cores from thread context
              cpu_set_t cpu;
              CPU_ZERO(&cpu);
              auto iret = sched_getaffinity(getpid(), sizeof(cpu), &cpu);
              if (LIKELY(0 == iret)) cores = CPU_COUNT(&cpu);
            }
          }

          /// log it
          {
            std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
            if (plugin_info.plugin_info != nullptr)
              my_plugin_log_message(&plugin_info.plugin_info, MY_WARNING_LEVEL,
                                    "MtEpoll start with detected %u CPUs.",
                                    cores);
          }

          /// calc group number
          groups = cores / threads + (0 == cores % threads ? 0 : 1);
          if (groups < min_auto_epoll_groups) {
            /// recalculate groups with cores
            auto probe = cores;
            do {
              probe += cores;
              groups = probe / threads + (0 == probe % threads ? 0 : 1);
            } while (groups < min_auto_epoll_groups);
          }
          /// dealing extra group
          auto extra = epoll_extra_groups;
          if (extra > MAX_EPOLL_EXTRA_GROUPS) extra = MAX_EPOLL_EXTRA_GROUPS;
          groups += extra;
        }
        if (UNLIKELY(groups > MAX_EPOLL_GROUPS)) groups = MAX_EPOLL_GROUPS;

        auto total_epoll_wait_threads = max_epoll_wait_total_threads;
        if (0 == total_epoll_wait_threads)
          total_epoll_wait_threads = groups * threads;
        else if (UNLIKELY(total_epoll_wait_threads <
                          MIN_EPOLL_WAIT_TOTAL_THREADS))
          total_epoll_wait_threads = MIN_EPOLL_WAIT_TOTAL_THREADS;
        else if (UNLIKELY(total_epoll_wait_threads >
                          MAX_EPOLL_WAIT_TOTAL_THREADS))
          total_epoll_wait_threads = MAX_EPOLL_WAIT_TOTAL_THREADS;

        if (total_epoll_wait_threads < groups)
          /// at least one thread wait on epoll
          total_epoll_wait_threads = groups;

        auto epoll_wait_threads_per_group = 1;
        /// select some thread in epoll to do epoll_wait
        while (epoll_wait_threads_per_group < static_cast<int>(threads) &&
               (epoll_wait_threads_per_group + 1) * groups <=
                   total_epoll_wait_threads)
          ++epoll_wait_threads_per_group;
        auto epoll_wait_threads_gap =
            static_cast<int>(threads) / epoll_wait_threads_per_group;

        auto work_queue_capacity = epoll_work_queue_capacity;
        if (UNLIKELY(work_queue_capacity < MIN_WORK_QUEUE_CAPACITY))
          work_queue_capacity = MIN_WORK_QUEUE_CAPACITY;
        else if (UNLIKELY(work_queue_capacity > MAX_WORK_QUEUE_CAPACITY))
          work_queue_capacity = MAX_WORK_QUEUE_CAPACITY;

        auto tmp = new CmtEpoll *[groups];
        for (uint32_t group_id = 0; group_id < groups; ++group_id) {
          tmp[group_id] = new CmtEpoll(group_id, work_queue_capacity);
          tmp[group_id]->init_thread(threads, epoll_wait_threads_per_group,
                                     epoll_wait_threads_gap);
        }

        {
          std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
          if (plugin_info.plugin_info != nullptr)
            my_plugin_log_message(
                &plugin_info.plugin_info, MY_WARNING_LEVEL,
                "MtEpoll start with %u groups with each group %u threads.",
                groups, threads);
        }

        inst_cnt = groups;
        inst = tmp;
      });
    }
    instance_count = inst_cnt;
    return inst;
  }

  /// Caution: invoker should have all available cores
  static inline void rebind_core() {
    if (!auto_cpu_affinity) return;

    size_t inst_cnt;
    const auto insts = get_instance(inst_cnt);

    /// get now CPU
    cpu_set_t new_cpus;
    CPU_ZERO(&new_cpus);
    auto iret = sched_getaffinity(getpid(), sizeof(new_cpus), &new_cpus);
    if (UNLIKELY(iret != 0)) return;

    /// compare within scoped lock
    std::lock_guard<std::mutex> lck(available_cpus_lock());
    auto &cpus = available_cpus();
    if (LIKELY(CPU_EQUAL(&cpus, &new_cpus))) return;

    /// CPUs changed
    std::ostringstream oss;
    oss << "CPUs changed from [";
    auto first = true;
    for (auto i = 0; i < CPU_SETSIZE; ++i) {
      if (CPU_ISSET(i, &cpus)) {
        if (first) {
          oss << i;
          first = false;
        } else
          oss << ',' << i;
      }
    }
    oss << "] to [";
    first = true;
    for (auto i = 0; i < CPU_SETSIZE; ++i) {
      if (CPU_ISSET(i, &new_cpus)) {
        if (first) {
          oss << i;
          first = false;
        } else
          oss << ',' << i;
      }
    }
    oss << "].";

    {
      std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
      if (plugin_info.plugin_info != nullptr)
        my_plugin_log_message(&plugin_info.plugin_info, MY_WARNING_LEVEL, "%s",
                              oss.str().c_str());
    }

    /// now re-calculate all cores assignment
    auto threads = epoll_threads_per_group;
    if (UNLIKELY(threads <= 0))
      threads = 1;
    else if (UNLIKELY(threads > MAX_EPOLL_THREADS_PER_GROUP))
      threads = MAX_EPOLL_THREADS_PER_GROUP;

    std::vector<CcpuInfo::cpu_info_t> affinities;
    auto info_map = CcpuInfo::get_cpu_info();
    for (auto i = 0; i < CPU_SETSIZE; ++i) {
      auto it = info_map.find(i);
      if (CPU_ISSET(i, &new_cpus) ||
          (force_all_cores && it != info_map.end())) {
        if (it == info_map.end())
          /// no cpu info, just set to 0
          affinities.emplace_back(CcpuInfo::cpu_info_t{i, 0, 0});
        else
          affinities.emplace_back(it->second);
      }
    }
    /// sort before duplicate
    /// result: 2314 -> 1234
    std::sort(affinities.begin(), affinities.end());
    /// if affinities not enough for base groups, just duplicate it
    if (inst_cnt * threads > affinities.size()) {
      auto duplicates = inst_cnt * threads / affinities.size();
      if (duplicates > 1) {
        /// result: 1234 -> 12341234
        std::vector<CcpuInfo::cpu_info_t> final_affinities;
        final_affinities.reserve(duplicates * affinities.size());
        for (size_t i = 0; i < duplicates; ++i) {
          for (const auto &item : affinities)
            final_affinities.emplace_back(item);
        }
        affinities = final_affinities;
      }
    }

    /// now assign
    for (size_t inst_id = 0; inst_id < inst_cnt; ++inst_id) {
      auto &inst = *insts[inst_id];

      {
        std::lock_guard<std::mutex> inst_lck(inst.affinity_lock_);
        if ((inst_id + 1) * threads <= affinities.size()) {
          /// bind to assigned
          CPU_ZERO(&inst.cpus_);
          oss.str("");
          oss << '[';
          for (uint32_t thread_id = 0; thread_id < threads; ++thread_id) {
            auto affinity = affinities[inst_id * threads + thread_id].processor;
            if (!CPU_ISSET(affinity, &inst.cpus_)) {
              CPU_SET(affinity, &inst.cpus_);  /// add to group set
              if (thread_id != 0) oss << ',';
              oss << affinity;
            }
          }
          oss << ']';
          inst.cores_str_ = oss.str();
          inst.with_affinity_ = true;
        } else {
          /// bind to all
          CPU_ZERO(&inst.cpus_);
          oss.str("");
          oss << '[';
          first = true;
          for (auto i = 0; i < CPU_SETSIZE; ++i) {
            if (CPU_ISSET(i, &new_cpus)) {
              CPU_SET(i, &inst.cpus_);  /// add to group set
              if (first) {
                oss << i;
                first = false;
              } else
                oss << ',' << i;
            }
          }
          oss << ']';
          inst.cores_str_ = oss.str();
          inst.with_affinity_ = true;
        }
      }

      /// add version outside lock to prevent potential wait
      inst.affinity_version_.fetch_add(1, std::memory_order_relaxed);
    }

    /// update original
    CPU_ZERO(&cpus);
    for (auto i = 0; i < CPU_SETSIZE; ++i) {
      if (CPU_ISSET(i, &new_cpus)) CPU_SET(i, &cpus);
    }
  }

  inline const uint32_t &group_id() const { return group_id_; }

  /// 0 if success else -errno
  inline int add_fd(int fd, uint32_t events, CepollCallback *cb,
                    bool tcp = true) const {
    auto iret = nonblock(fd, 1);
    if (UNLIKELY(iret != 0)) return iret;
    if (tcp && UNLIKELY((iret = nodelay(fd, 1)) != 0)) return iret;
    auto tmp = tcp_keep_alive;
    if (UNLIKELY(tmp > MAX_TCP_KEEP_ALIVE)) tmp = MAX_TCP_KEEP_ALIVE;
    if (tcp && tmp > 0 && UNLIKELY((iret = keepalive(fd, 1, tmp)) != 0))
      return iret;

    ::epoll_event event;
    event.data.ptr = cb;
    event.events = events;
    cb->set_fd(fd);
    cb->fd_pre_register();  /// pre register before epoll add
    DBG_LOG(("polarx_rpc epoll add fd %d", fd));
    iret = ::epoll_ctl(epfd_, EPOLL_CTL_ADD, fd, &event);
    DBG_LOG(("polarx_rpc epoll add fd %d done ret %d", fd, iret));
    if (UNLIKELY(iret != 0)) {
      auto bret = cb->fd_rollback_register();
      if (!bret) delete cb;
      return -errno;
    }
    return 0;
  }

  /// 0 if success else -errno
  inline int reset_fd(int fd, uint32_t events, CepollCallback *cb) const {
    ::epoll_event event;
    event.data.ptr = cb;
    event.events = events;
    DBG_LOG(("polarx_rpc epoll mod fd %d", fd));
    auto iret = ::epoll_ctl(epfd_, EPOLL_CTL_MOD, fd, &event);
    DBG_LOG(("polarx_rpc epoll mod fd %d done ret %d", fd, iret));
    return LIKELY(0 == iret) ? 0 : -errno;
  }

  /// 0 if success else -errno
  inline int del_fd(int fd) const {
    epoll_event dummy;
    ::memset(&dummy, 0, sizeof(dummy));
    DBG_LOG(("polarx_rpc epoll del fd %d", fd));
    auto iret = ::epoll_ctl(epfd_, EPOLL_CTL_DEL, fd, &dummy);
    DBG_LOG(("polarx_rpc epoll del fd %d done ret %d", fd, iret));
    return LIKELY(0 == iret) ? 0 : -errno;
  }

  static inline int check_port(uint16_t port) {
    auto fd = ::socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (UNLIKELY(fd < 0)) return -errno;
    sockaddr_in address;
    ::memset(&address, 0, sizeof(address));
    if (::inet_pton(AF_INET, "127.0.0.1", &address.sin_addr.s_addr) != 1) {
      auto err = errno;
      ::close(fd);
      return -err;
    }
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    if (0 == ::connect(fd, reinterpret_cast<struct sockaddr *>(&address),
                       sizeof(address))) {
      ::close(fd);
      return -EADDRINUSE;
    }
    auto err = errno;
    ::close(fd);
    return ECONNREFUSED == err ? 0 : -err;
  }

  /// 0 if success else -errno
  inline int listen_port(uint16_t port, CepollCallback *cb,
                         bool reuse = false) const {
    sockaddr_in address;
    ::memset(&address, 0, sizeof(address));
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(port);
    auto fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (UNLIKELY(fd < 0)) return -errno;
    int sock_op = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &sock_op, sizeof(sock_op));
    if (reuse)
      ::setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &sock_op, sizeof(sock_op));
    if (UNLIKELY(::bind(fd, (struct sockaddr *)&address, sizeof(address)) !=
                 0)) {
      auto err = errno;
      ::close(fd);
      return -err;
    }

    auto depth = tcp_listen_queue;
    if (UNLIKELY(depth < MIN_TCP_LISTEN_QUEUE))
      depth = MIN_TCP_LISTEN_QUEUE;
    else if (UNLIKELY(depth > MAX_TCP_LISTEN_QUEUE))
      depth = MAX_TCP_LISTEN_QUEUE;
    if (UNLIKELY(::listen(fd, depth) != 0)) {
      auto err = errno;
      ::close(fd);
      return -err;
    }
    int iret;
    if (UNLIKELY((iret = add_fd(fd, EPOLLIN | EPOLLET, cb)) != 0)) {
      ::close(fd);
      return iret;
    }
    return 0;
  }

  inline void push_listener(std::unique_ptr<CepollCallback> &&listener) {
    listener_ = std::forward<std::unique_ptr<CepollCallback>>(listener);
  }

  inline void push_trigger(task_t &&task, int64_t trigger_time) {
    int32_t id;
    int64_t last_time;

    /// reuse work queue spin conf
    CautoMcsSpinLock lck(timer_lock_, mcs_spin_cnt);
    if (UNLIKELY(!timer_heap_.peak(last_time))) last_time = trigger_time + 1;
    timer_heap_.push(std::forward<task_t>(task), trigger_time, id);
    lck.unlock();

    if (UNLIKELY(last_time - trigger_time >= 0)) {
      /// need notify to restart new thread to wait with smaller timeout
      uint64_t dummy = 1;
      const auto result = ::write(eventfd_, &dummy, sizeof(dummy));
      (void)result;
    }
  }

  inline bool push_work(task_t &&task) {
    auto bret = work_queue_.push(std::forward<task_t>(task));
    if (!bret) return false;

    /// read with write barrier
    auto waiting = wait_cnt_.fetch_add(0, std::memory_order_acq_rel);
    if (waiting > 0) {
      /// notify if some one is in epoll
      uint64_t dummy = 1;
      const auto result = ::write(eventfd_, &dummy, sizeof(dummy));
      (void)result;
    }
    return true;
  }

  inline epoll_group_ctx_t &get_extra_ctx() { return extra_ctx_; }

  inline void add_stall_count() { ++stall_count_; }

  inline void sub_stall_count() { --stall_count_; }

  /// thread pool auto scale and shrink

  inline bool worker_stall_since_last_check() {
    auto head = work_queue_.head();
    if (LIKELY(head != last_head_)) {
      last_head_ = head;
      return false;
    }
    /// consumer not moved
    auto tail = work_queue_.tail();
    if (head != tail)  /// not empty
      return true;
    /// check epoll wait exists
    auto loop = loop_cnt_.load(std::memory_order_acquire);
    auto waits = wait_cnt_.load(std::memory_order_acquire);
    if (likely(waits > 0)) {
      last_loop_ = loop;
      return false;
    }
    if (LIKELY(loop != last_loop_)) {
      last_loop_ = loop;
      return false;
    }
    return true;  /// empty task but no thread wait on epoll
  }

  inline void force_scale_thread_pool() {
    last_scale_time_.store(Ctime::steady_ms(), std::memory_order_release);

    std::lock_guard<std::mutex> lck(scale_lock_);

    if (worker_count_.load(std::memory_order_acquire) >=
        session_count_.load(std::memory_order_acquire) + base_thread_count_) {
      if (enable_thread_pool_log) {
        std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
        if (plugin_info.plugin_info != nullptr)
          my_plugin_log_message(
              &plugin_info.plugin_info, MY_WARNING_LEVEL,
              "MtEpoll %u thread pool force scale over limit, worker %d tasker "
              "%d, session %d. Total threads %d.",
              group_id_, worker_count_.load(std::memory_order_acquire),
              tasker_count_.load(std::memory_order_acquire),
              session_count_.load(std::memory_order_acquire),
              global_thread_count().load(std::memory_order_acquire));
      }
      return;  /// ignore if worker more than session
    }

    /// force scale one thread
    ++worker_count_;
    ++global_thread_count();
    std::thread thread(&CmtEpoll::loop, this, 999, false, true, true);
    thread.detach();

    if (enable_thread_pool_log) {
      std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
      if (plugin_info.plugin_info != nullptr)
        my_plugin_log_message(
            &plugin_info.plugin_info, MY_WARNING_LEVEL,
            "MtEpoll %u thread pool force scale to worker %d tasker %d. Total "
            "threads %d.",
            group_id_, worker_count_.load(std::memory_order_acquire),
            tasker_count_.load(std::memory_order_acquire),
            global_thread_count().load(std::memory_order_acquire));
    }
  }

  inline const std::atomic<int> &session_count() const {
    return session_count_;
  }

  inline std::atomic<int> &session_count() { return session_count_; }

  inline void balance_tasker() {
    auto pending = work_queue_.length();
    auto workers = worker_count_.load(std::memory_order_acquire);
    assert(workers >= 0);
    auto taskers = tasker_count_.load(std::memory_order_acquire);
    assert(taskers >= 0);

    auto multiply = epoll_group_tasker_multiply;
    auto multiply_low = multiply / 2;
    if (multiply_low < 1) multiply_low = 1;

    if (pending * 2 > work_queue_.capacity() ||
        pending > multiply_low * (workers + taskers)) {
      last_tasker_time_.store(Ctime::steady_ms(), std::memory_order_release);

      if (pending * 2 <= work_queue_.capacity() &&
          pending <= multiply * (workers + taskers))
        return;  /// still under thresh

      /// need balance
      std::lock_guard<std::mutex> lck(scale_lock_);

      workers = worker_count_.load(std::memory_order_acquire);
      assert(workers >= 0);
      taskers = tasker_count_.load(std::memory_order_acquire);
      assert(taskers >= 0);
      auto sessions = session_count_.load(std::memory_order_acquire);
      assert(taskers >= 0);

      if (workers + taskers < sessions &&
          workers + taskers < static_cast<int>(pending)) {
        auto extend = (pending - workers - taskers) / multiply;
        if (0 == extend) extend = 1;
        if (extend > epoll_group_tasker_extend_step)
          extend = epoll_group_tasker_extend_step;

        tasker_count_ += extend;
        global_thread_count() += extend;
        for (size_t i = 0; i < extend; ++i) {
          std::thread thread(&CmtEpoll::loop, this, 999, false,
                             enable_epoll_in_tasker, false);
          thread.detach();
        }

        if (enable_thread_pool_log) {
          std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
          if (plugin_info.plugin_info != nullptr)
            my_plugin_log_message(
                &plugin_info.plugin_info, MY_WARNING_LEVEL,
                "MtEpoll %u thread pool tasker scale to %d, worker %d. Total "
                "threads %d.",
                group_id_, tasker_count_.load(std::memory_order_acquire),
                worker_count_.load(std::memory_order_acquire),
                global_thread_count().load(std::memory_order_acquire));
        }
      }
    }
  }

  inline void try_scale_thread_pool(int wait_type) {
    auto thresh = static_cast<int>(epoll_group_thread_scale_thresh);
    if (UNLIKELY(thresh < 0))
      thresh = 0;
    else if (UNLIKELY(thresh >= base_thread_count_)) {
      thresh = base_thread_count_ - 1;
      assert(thresh >= 0);
    }
    auto stalled = stall_count_.load(std::memory_order_acquire);
    assert(stalled >= 0);
    auto workers = worker_count_.load(std::memory_order_acquire);
    assert(workers >= 0);
    auto prefer_thread_count =
        static_cast<int>(base_thread_count_ + epoll_group_dynamic_threads);

    /// refresh the last time if needed
    if (stalled > workers - base_thread_count_ + thresh)
      last_scale_time_.store(Ctime::steady_ms(), std::memory_order_release);
    else if (workers >= prefer_thread_count) {
      if (stalled > workers / 4)
        last_scale_time_.store(Ctime::steady_ms(), std::memory_order_release);
      return;  /// do nothing
    }

    /// do scale if needed(recheck in lock)
    std::lock_guard<std::mutex> lck(scale_lock_);
    stalled = stall_count_.load(std::memory_order_acquire);
    assert(stalled >= 0);
    workers = worker_count_.load(std::memory_order_acquire);
    assert(workers >= 0);

    if (workers >=
        session_count_.load(std::memory_order_acquire) + base_thread_count_) {
      if (enable_thread_pool_log) {
        std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
        if (plugin_info.plugin_info != nullptr)
          my_plugin_log_message(
              &plugin_info.plugin_info, MY_WARNING_LEVEL,
              "MtEpoll %u thread pool scale over limit, worker %d tasker %d, "
              "session %d. Total threads %d.",
              group_id_, worker_count_.load(std::memory_order_acquire),
              tasker_count_.load(std::memory_order_acquire),
              session_count_.load(std::memory_order_acquire),
              global_thread_count().load(std::memory_order_acquire));
      }
      return;  /// ignore if worker more than session
    }

    auto scaled = false;
    if (stalled > workers - base_thread_count_ + thresh) {
      /// need extra thread to handle new request
      ++worker_count_;
      ++global_thread_count();
      std::thread thread(&CmtEpoll::loop, this, 999, false, true, true);
      thread.detach();
      scaled = true;
    } else if (workers < prefer_thread_count) {
      do {
        ++worker_count_;
        ++global_thread_count();
        std::thread thread(&CmtEpoll::loop, this, 999, false, true, true);
        thread.detach();
      } while (worker_count_.load(std::memory_order_acquire) <
               prefer_thread_count);
      scaled = true;
    }

    if (scaled && enable_thread_pool_log) {
      std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
      if (plugin_info.plugin_info != nullptr)
        my_plugin_log_message(
            &plugin_info.plugin_info, MY_WARNING_LEVEL,
            "MtEpoll %u thread pool scale to worker %d tasker %d. Total "
            "threads %d. (wait_type %d)",
            group_id_, worker_count_.load(std::memory_order_acquire),
            tasker_count_.load(std::memory_order_acquire),
            global_thread_count().load(std::memory_order_acquire), wait_type);
    }
  }

  inline bool shrink_thread_pool(bool is_worker) {
    if (!is_worker) {
      /// tasker thread
      if (Ctime::steady_ms() -
              last_tasker_time_.load(std::memory_order_acquire) <=
          epoll_group_dynamic_threads_shrink_time)
        return false;

      /// free it
      --tasker_count_;
      --global_thread_count();

      if (enable_thread_pool_log) {
        std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
        if (plugin_info.plugin_info != nullptr)
          my_plugin_log_message(
              &plugin_info.plugin_info, MY_WARNING_LEVEL,
              "MtEpoll %u thread pool shrink to worker %d tasker %d. Total "
              "thread %d.",
              group_id_, worker_count_.load(std::memory_order_acquire),
              tasker_count_.load(std::memory_order_acquire),
              global_thread_count().load(std::memory_order_acquire));
      }
      return true;
    }

    auto bret = false;
    auto prefer_thread_count =
        static_cast<int>(base_thread_count_ + epoll_group_dynamic_threads);
    auto thresh = static_cast<int>(epoll_group_thread_scale_thresh);
    if (UNLIKELY(thresh < 0))
      thresh = 0;
    else if (UNLIKELY(thresh >= base_thread_count_)) {
      thresh = base_thread_count_ - 1;
      assert(thresh >= 0);
    }
    auto stalled = stall_count_.load(std::memory_order_acquire);
    assert(stalled >= 0);
    auto workers = worker_count_.load(std::memory_order_acquire);
    assert(workers >= 0);

    /// enter mutex only when we need to do
    if (stalled < workers - base_thread_count_ + thresh &&
        Ctime::steady_ms() - last_scale_time_.load(std::memory_order_acquire) >
            epoll_group_dynamic_threads_shrink_time &&
        workers > prefer_thread_count) {
      /// shrink only when no waiting exists and no multiple stall for a while
      std::lock_guard<std::mutex> lck(scale_lock_);
      /// recheck waiting
      stalled = stall_count_.load(std::memory_order_acquire);
      if (worker_count_.load(std::memory_order_acquire) > prefer_thread_count &&
          stalled < prefer_thread_count - 1) {
        --worker_count_;
        --global_thread_count();
        bret = true;

        if (enable_thread_pool_log) {
          std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
          if (plugin_info.plugin_info != nullptr)
            my_plugin_log_message(
                &plugin_info.plugin_info, MY_WARNING_LEVEL,
                "MtEpoll %u thread pool shrink to worker %d tasker %d. Total "
                "threads %d.",
                group_id_, worker_count_.load(std::memory_order_acquire),
                tasker_count_.load(std::memory_order_acquire),
                global_thread_count().load(std::memory_order_acquire));
        }
      }
    }
    return bret;
  }

  inline void register_tcp(CtcpConnection *tcp) {
    std::lock_guard<std::mutex> lck(tcp_lock_);
    tcp_set_.emplace(tcp);
  }

  inline void unregister_tcp(CtcpConnection *tcp) {
    std::lock_guard<std::mutex> lck(tcp_lock_);
    tcp_set_.erase(tcp);
  }

  template <class Visitor>
  inline void visit_tcp(Visitor &&visitor) {
    std::lock_guard<std::mutex> lck(tcp_lock_);
    for (const auto &tcp : tcp_set_) visitor(tcp);
  }
};

}  // namespace polarx_rpc
