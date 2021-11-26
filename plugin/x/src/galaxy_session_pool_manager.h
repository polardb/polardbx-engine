//
// Created by zzy on 2021/11/19.
//

#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <queue>
#include <thread>
#include <vector>

#include "plugin/x/ngs/include/ngs/error_code.h"
#include "plugin/x/ngs/include/ngs/galaxy_session.h"
#include "plugin/x/ngs/include/ngs/interface/session_interface.h"
#include "plugin/x/ngs/include/ngs/memory.h"
#include "plugin/x/src/helper/multithread/mutex.h"
#include "plugin/x/src/helper/multithread/synchronize.h"
#include "plugin/x/src/variables/galaxy_variables.h"
#include "plugin/x/src/xpl_log.h"
#include "plugin/x/src/xpl_performance_schema.h"

#include "galaxy_atomicex.h"
#include "galaxy_session_context.h"

class THD;

namespace xpl {

class Galaxy_session_pool_manager {
 public:
  Galaxy_session_pool_manager() : m_exit(false) {
    // Read it out first.
    auto thread_count = static_cast<int>(
        gx::Galaxy_system_variables::m_galaxy_worker_threads_per_tcp);
    assert(thread_count > 0);
    m_worker_count.store(thread_count, std::memory_order_release);
    for (auto i = 0; i < thread_count; ++i)
      m_workers.emplace_back(&Galaxy_session_pool_manager::run, this);

    // Log creation info.
    if (gx::Galaxy_system_variables::m_enable_galaxy_session_pool_log) {
      char buf[0x100];
      snprintf(buf, sizeof(buf),
               "Galaxy session pool %p start with thread pool init to %zu.",
               this, m_workers.size());
      log_warning(ER_XPLUGIN_ERROR_MSG, buf);
    }
  }

  ~Galaxy_session_pool_manager() { on_free(); }

  void on_free();

  ngs::Error_code new_session(ngs::Session_interface &base_session,
                              gx::GSession_id sid,
                              std::shared_ptr<Galaxy_session_context> &ptr);

  inline std::shared_ptr<Galaxy_session_context> operator[](
      gx::GSession_id sid) {
    gx::CautoSpinRWLock lck(m_session_lock);
    auto it = m_sessions.find(sid);
    if (it != m_sessions.end()) return it->second;
    return {};
  }

  inline void push_task(gx::GSession_id sid) {
    auto lck(m_work_sync.block());
    m_work.emplace(sid);
    lck.notify();
  }

  inline void remove_and_kill(gx::GSession_id sid) {
    std::shared_ptr<Galaxy_session_context> session;
    {
      gx::CautoSpinRWLock lck(m_session_lock, true);
      auto it = m_sessions.find(sid);
      if (it != m_sessions.end()) {
        session.swap(it->second);
        m_sessions.erase(it);
      }
    }
    if (session) session->remote_kill();
  }

  void wait_begin(THD *thd, int wait_type);
  void wait_end(THD *thd);
  void post_kill(THD *thd);

  void scale_thread_pool();
  bool shrink_thread_pool();

 private:
  void run();

  gx::CspinRWLock m_session_lock;
  std::map<gx::GSession_id, std::shared_ptr<Galaxy_session_context>> m_sessions;

  std::atomic_bool m_exit;

  Synchronize m_work_sync{KEY_mutex_gx_session_pool_work,
                          KEY_cond_gx_session_pool_work};
  std::queue<gx::GSession_id> m_work;

  Mutex m_kill_mutex{KEY_mutex_gx_session_pool_kill};
  std::vector<std::thread> m_workers;

  // For fast compare rather than enter mutex.
  // Use int to prevent bad unsigned compare.
  std::atomic<int> m_worker_count{0};
  std::atomic<int> m_wait_count{0};

  std::atomic<int64_t> m_last_multiple_wait_time{0};
};

}  // namespace xpl
