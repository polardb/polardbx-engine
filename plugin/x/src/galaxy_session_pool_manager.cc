//
// Created by zzy on 2021/11/19.
//

#include <chrono>
#include <utility>

#include "mutex_lock.h"
#include "mysql/service_ssl_wrapper.h"
#include "mysql/service_thd_wait.h"
#include "sql/conn_handler/connection_handler_manager.h"
#include "sql/sql_class.h"

#include "plugin/x/src/variables/galaxy_variables.h"
#include "plugin/x/src/xpl_log.h"
#include "plugin/x/src/xpl_resultset.h"

#include "galaxy_session_pool_manager.h"

namespace xpl {

/**
 * Callbacks for dynamic thread schedule.
 */

static void thd_wait_begin(THD *thd, int wait_type) {
  assert(thd->polarx_rpc_context != nullptr);
  reinterpret_cast<Galaxy_session_pool_manager *>(thd->polarx_rpc_context)
      ->wait_begin(thd, wait_type);
}

static void thd_wait_end(THD *thd) {
  assert(thd->polarx_rpc_context != nullptr);
  reinterpret_cast<Galaxy_session_pool_manager *>(thd->polarx_rpc_context)
      ->wait_end(thd);
}

static void post_kill_notification(THD *thd) {
  assert(thd->polarx_rpc_context != nullptr);
  reinterpret_cast<Galaxy_session_pool_manager *>(thd->polarx_rpc_context)
      ->post_kill(thd);
}

static THD_event_functions galaxy_monitor = {thd_wait_begin, thd_wait_end,
                                             post_kill_notification};

/**
 * Session pool functions.
 */

ngs::Error_code Galaxy_session_pool_manager::new_session(
    ngs::Session_interface &base_session, gx::GSession_id sid,
    std::shared_ptr<Galaxy_session_context> &ptr) {
  std::shared_ptr<Galaxy_session_context> session;
  auto err =
      Galaxy_session_context::allocate(base_session, sid, *this, session);
  if (err) return err;

  {
    gx::CautoSpinRWLock lck(m_session_lock, true);
    // Check within lock.
    if (m_exit.load(std::memory_order_acquire))
      return ngs::Error(ER_XPLUGIN_ERROR_MSG, "Session manager exiting");
    auto ib = m_sessions.insert(std::make_pair(sid, session));
    if (!ib.second) return ngs::Error(ER_XPLUGIN_ERROR_MSG, "Duplicate sid");
  }

  {
    // Log session creation.
    char buf[0x100];
    snprintf(buf, sizeof(buf), "Galaxy session %p sid %zu init pool %p.",
             session.get(), sid, this);
    log_info(ER_XPLUGIN_ERROR_MSG, buf);
  }

  ptr.swap(session);
  return ngs::Success();
}

class Auto_detacher final {
 public:
  explicit Auto_detacher(Galaxy_session_context &context,
                         Galaxy_session_pool_manager &mgr)
      : m_context(context), m_run(false) {
    auto thd = m_context.data_context().get_thd();
    if (thd != nullptr)
      thd->register_polarx_rpc_monitor(&galaxy_monitor, &mgr);
  }

  ~Auto_detacher() {
    auto thd = m_context.data_context().get_thd();
    if (thd != nullptr) thd->clear_polarx_rpc_monitor();
    if (m_run) m_context.detach();
  }

  void set_run() { m_run = true; }

 private:
  Galaxy_session_context &m_context;
  bool m_run;
};

void Galaxy_session_pool_manager::run() {
  srv_session_init_thread(plugin_handle);

#if defined(__APPLE__)
  pthread_setname_np("galaxy_worker");
#elif defined(HAVE_PTHREAD_SETNAME_NP)
  pthread_setname_np(pthread_self(), "galaxy_worker");
#endif

  while (!m_exit.load(std::memory_order_acquire)) {
    // Auto exit if no more thread needed.
    if (shrink_thread_pool()) break;

    // Get task.
    gx::GSession_id sid;
    {
      auto lck(m_work_sync.block());
      if (m_work.empty()) {
        do {
          lck.wait();
          if (m_exit.load(std::memory_order_acquire)) {
            lck.broadcast();  // Cascade notify.
            break;
          }
        } while (m_work.empty());
      }
      if (m_exit.load(std::memory_order_acquire)) break;
      sid = m_work.front();
      m_work.pop();
    }

    std::shared_ptr<Galaxy_session_context> session;
    {
      gx::CautoSpinRWLock lck(m_session_lock);
      auto it = m_sessions.find(sid);
      if (it != m_sessions.end()) session = it->second;
    }

    if (session) {
      // Do work.
    _retry:
      auto success_enter = false;
      // Lock it before attach.
      {
        gx::CautoSpinLock lck;
        if (lck.try_lock(session->working_lock())) {
          // Attach and detach inside the scope of working lock.
          Auto_detacher detacher(*session, *this);
          success_enter = true;
          try {
            while (true) {
              std::queue<std::pair<
                  uint8, ngs::Memory_instrumented<ngs::Message>::Unique_ptr>>
                  msgs;
              {
                gx::CautoSpinLock queue_lck(session->queue_lock());
                msgs.swap(session->queue());
              }
              if (msgs.empty()) break;
              while (!msgs.empty()) {
                // Exit anytime and use auto lock to keep safe.
                if (m_exit.load(std::memory_order_acquire)) break;

                // Check session killed.
                if (session->killed().load(std::memory_order_acquire)) {
                  {
                    gx::CautoSpinRWLock session_lck(m_session_lock, true);
                    m_sessions.erase(sid);
                  }
                  // Ignore all unfinished messages and free by smarter pointer.
                  success_enter = false;
                  break;
                }

                // Do execute one msg.
                auto &msg = msgs.front();
                if (Mysqlx::ClientMessages::SESS_CLOSE == msg.first) {
                  // Normal close session.
                  session->killed().store(true, std::memory_order_release);
                  {
                    gx::CautoSpinRWLock session_lck(m_session_lock, true);
                    m_sessions.erase(sid);
                  }
                  // Send close ok.
                  session->encoder().send_ok();

                  // Ignore other msgs. No recheck needed.
                  success_enter = false;
                  break;  // Break the message consume.
                } else {
                  bool run = false;
                  try {
                    session->dispatch(msg.first, *msg.second, run);
                  } catch (ngs::Error_code &err) {
                    session->encoder().send_error(err);
                  }  // Other error will throw.
                  if (run) detacher.set_run();
                }

                // Done, pop, and next.
                msgs.pop();
              }
              // May break from internal loop and will recheck and exit on outer
              // loop.
              if (m_exit.load(std::memory_order_acquire)) break;
            }
          } catch (...) {
            log_error(ER_XPLUGIN_ERROR_MSG, "Session unexpected killed");
            // Do detach on any error.
            detacher.set_run();
            auto fatal =
                ngs::Fatal(ER_SESSION_WAS_KILLED, "Session unexpected killed");
            session->encoder().send_error(fatal);
          }
        }
      }

      if (success_enter) {
        // Recheck without enter to prevent missing notify.
        auto pending_task = false;
        {
          gx::CautoSpinLock lck(session->queue_lock());
          pending_task = !session->queue().empty();
        }
        if (pending_task) goto _retry;
      }
    }
  }

  ssl_wrapper_thread_cleanup();
  srv_session_deinit_thread();
}

void Galaxy_session_pool_manager::on_free() {
  std::vector<std::thread> waits;
  {
    MUTEX_LOCK(lck, m_kill_mutex);

    auto before = false;
    m_exit.compare_exchange_strong(before, true);

    std::vector<std::shared_ptr<Galaxy_session_context>> free_sessions;
    {
      gx::CautoSpinRWLock session_lck(m_session_lock, true);
      free_sessions.reserve(m_sessions.size());
      for (auto &pair : m_sessions) {
        pair.second->killed().store(true, std::memory_order_release);
        // Move out and do further operation.
        free_sessions.emplace_back(std::move(pair.second));
      }
      m_sessions.clear();
    }

    // Do kill outside the lock.
    for (const auto &session : free_sessions) session->remote_kill();
    free_sessions.clear();

    // Notify worker.
    {
      auto worker_lck(m_work_sync.block());
      worker_lck.broadcast();
    }

    // Wait outside.
    waits.swap(m_workers);
    m_worker_count.store(0, std::memory_order_release);
    assert(static_cast<int>(m_workers.size()) == m_worker_count);

    // Log exit once.
    if (!before &&
        gx::Galaxy_system_variables::m_enable_galaxy_session_pool_log) {
      char buf[0x100];
      snprintf(buf, sizeof(buf), "Galaxy session pool %p deinit.", this);
      log_warning(ER_XPLUGIN_ERROR_MSG, buf);
    }
  }

  // Wait threads done.
  for (auto &worker : waits) worker.join();
  waits.clear();
}

static inline int64_t stable_ms() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

/**
 * @param wait_type
 * typedef enum _thd_wait_type_e {
 *   THD_WAIT_SLEEP = 1,
 *   THD_WAIT_DISKIO = 2,
 *   THD_WAIT_ROW_LOCK = 3,
 *   THD_WAIT_GLOBAL_LOCK = 4,
 *   THD_WAIT_META_DATA_LOCK = 5,
 *   THD_WAIT_TABLE_LOCK = 6,
 *   THD_WAIT_USER_LOCK = 7,
 *   THD_WAIT_BINLOG = 8,
 *   THD_WAIT_GROUP_COMMIT = 9,
 *   THD_WAIT_SYNC = 10,
 *   THD_WAIT_LAST = 11
 * } thd_wait_type;
 */
void Galaxy_session_pool_manager::wait_begin(THD *thd, int wait_type) {
  auto before = thd->polarx_rpc_enter.fetch_add(1, std::memory_order_acquire);
  if (0 == before) {
    auto recoverable_wait = false;
    switch (wait_type) {
      case THD_WAIT_DISKIO:
      case THD_WAIT_BINLOG:
      case THD_WAIT_GROUP_COMMIT:
      case THD_WAIT_SYNC:
        recoverable_wait = true;
        break;
      default:
        break;
    }

    if (recoverable_wait)
      thd->polarx_rpc_record = false;
    else {
      thd->polarx_rpc_record = true;
      ++m_wait_count;
      // Fast push last time.
      if (m_wait_count.load(std::memory_order_acquire) >=
          m_worker_count.load(std::memory_order_acquire) * 2 / 3)
        m_last_multiple_wait_time.store(stable_ms(), std::memory_order_release);
      // Do scale if needed.
      // Always do scale check in lock to prevent concurrency problem with
      // shrink.
      scale_thread_pool();
    }
  }
}

void Galaxy_session_pool_manager::wait_end(THD *thd) {
  auto before =
      thd->polarx_rpc_enter.fetch_sub(1, std::memory_order_release);
  if (1 == before && thd->polarx_rpc_record &&
      thd->polarx_rpc_record) {
    --m_wait_count;
    thd->polarx_rpc_record = false;
  }
}

void Galaxy_session_pool_manager::post_kill(THD *thd) {}

void Galaxy_session_pool_manager::scale_thread_pool() {
  MUTEX_LOCK(lck, m_kill_mutex);

  // Do nothing if exiting.
  if (m_exit.load(std::memory_order_acquire)) return;

  // Check working & waiting.
  auto prefer_thread_count =
      gx::Galaxy_system_variables::m_galaxy_worker_threads_per_tcp;
  auto waiting = m_wait_count.load(std::memory_order_acquire);
  assert(waiting >= 0);

  auto scaled = false;
  if (waiting >= static_cast<int>(m_workers.size())) {
    assert(waiting == static_cast<int>(m_workers.size()));
    // Need extra thread to handle new request.
    ++m_worker_count;
    m_workers.emplace_back(&Galaxy_session_pool_manager::run, this);
    scaled = true;
  } else if (m_workers.size() < prefer_thread_count) {
    do {
      ++m_worker_count;
      m_workers.emplace_back(&Galaxy_session_pool_manager::run, this);
    } while (m_workers.size() < prefer_thread_count);
    scaled = true;
  }

  if (scaled && gx::Galaxy_system_variables::m_enable_galaxy_session_pool_log) {
    char buf[0x100];
    snprintf(buf, sizeof(buf),
             "Galaxy session pool %p thread pool scale to %zu.", this,
             m_workers.size());
    log_warning(ER_XPLUGIN_ERROR_MSG, buf);
  }

  assert(static_cast<int>(m_workers.size()) == m_worker_count);
}

bool Galaxy_session_pool_manager::shrink_thread_pool() {
  auto bret = false;
  auto prefer_thread_count = static_cast<int>(
      gx::Galaxy_system_variables::m_galaxy_worker_threads_per_tcp);
  auto waiting = m_wait_count.load(std::memory_order_acquire);
  assert(waiting >= 0);
  auto need_shrink =
      stable_ms() - m_last_multiple_wait_time.load(std::memory_order_acquire) >
      gx::Galaxy_system_variables::m_galaxy_worker_threads_shrink_time;

  // Enter mutex only when we need to do.
  if (0 == waiting && need_shrink &&
      m_worker_count.load(std::memory_order_acquire) > prefer_thread_count) {
    // Shrink only when no waiting exists and no multiple wait for a while.
    MUTEX_LOCK(lck, m_kill_mutex);
    // Recheck waiting.
    waiting = m_wait_count.load(std::memory_order_acquire);
    if (static_cast<int>(m_workers.size()) > prefer_thread_count &&
        waiting < prefer_thread_count) {
      for (auto it = m_workers.begin(); it != m_workers.end(); ++it) {
        if (it->get_id() == std::this_thread::get_id()) {
          it->detach();
          m_workers.erase(it);
          --m_worker_count;
          bret = true;
          break;
        }
      }

      if (gx::Galaxy_system_variables::m_enable_galaxy_session_pool_log) {
        char buf[0x100];
        snprintf(buf, sizeof(buf),
                 "Galaxy session pool %p thread pool shrink to %zu.", this,
                 m_workers.size());
        log_warning(ER_XPLUGIN_ERROR_MSG, buf);
      }
    }
    assert(static_cast<int>(m_workers.size()) == m_worker_count);
  }
  return bret;
}

}  // namespace xpl
