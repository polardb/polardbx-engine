//
// Created by zzy on 2022/7/27.
//

#include <atomic>
#include <mutex>

#include "../global_defines.h"
#ifndef MYSQL8
#define MYSQL_SERVER
#endif
#include "mysql/service_command.h"
#include "sql/conn_handler/connection_handler_manager.h"
#include "sql/sql_class.h"
#include "sql/srv_session.h"

#include "../coders/buffering_command_delegate.h"
#include "../coders/command_delegate.h"
#include "../coders/custom_command_delegates.h"
#include "../coders/notices.h"
#include "../executor/executor.h"
#include "../polarx_rpc.h"
#include "../server/server_variables.h"
#include "../server/tcp_connection.h"
#include "../sql_query/sql_statement_builder.h"

#include "request_cache.h"
#include "session.h"
#include "session_manager.h"

namespace polarx_rpc {

std::atomic<int> g_session_count(0);

Csession::~Csession() {
  /// mysql_session_ free in ~CsessionBase
  g_session_count.fetch_sub(1, std::memory_order_release);
}

/**
 * Callbacks for dynamic thread schedule.
 */

static void thd_wait_begin(THD *thd, int wait_type) {
  assert(thd->polarx_rpc_context != nullptr);
  reinterpret_cast<Csession *>(thd->polarx_rpc_context)
      ->wait_begin(thd, wait_type);
}

static void thd_wait_end(THD *thd) {
  assert(thd->polarx_rpc_context != nullptr);
  reinterpret_cast<Csession *>(thd->polarx_rpc_context)->wait_end(thd);
}

static void post_kill_notification(THD *thd) {
  assert(thd->polarx_rpc_context != nullptr);
  reinterpret_cast<Csession *>(thd->polarx_rpc_context)->post_kill(thd);
}

static THD_event_functions polarx_rpc_monitor = {thd_wait_begin, thd_wait_end,
                                                 post_kill_notification};

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
void Csession::wait_begin(THD *thd, int wait_type) {
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
      epoll_.add_stall_count();
      /// scale if needed
      epoll_.try_scale_thread_pool(wait_type);
    }
  }
}

void Csession::wait_end(THD *thd) {
  auto before = thd->polarx_rpc_enter.fetch_sub(1, std::memory_order_release);
  if (1 == before && thd->polarx_rpc_record) {
    epoll_.sub_stall_count();
    thd->polarx_rpc_record = false;
  }
}

void Csession::post_kill(THD *thd) {}

bool Csession::flush() {
  if (shutdown_.load(std::memory_order_acquire)) {
    encoder_.reset(); /// clear buffer even session was killed
    return false;
  }
  return encoder_.flush(tcp_);
}

class CautoDetacher final {
private:
  Csession &session_;
  bool run_;

public:
  explicit CautoDetacher(Csession &session) : session_(session), run_(false) {
    auto thd = session_.get_thd();
    if (LIKELY(thd != nullptr))
      thd->register_polarx_rpc_monitor(&polarx_rpc_monitor, &session_);
  }

  ~CautoDetacher() {
    auto thd = session_.get_thd();
    if (thd != nullptr)
      thd->clear_polarx_rpc_monitor();
    if (run_)
      session_.detach();
  }

  void set_run() { run_ = true; }
};

void Csession::run() {
_retry:
  auto success_enter = false;
  {
    CautoSpinLock lck;
    if (lck.try_lock(working_)) {
      /// attach and detach inside the scope of working lock
      CautoDetacher detacher(*this);
      success_enter = true;
      try {
        while (true) {
          std::queue<msg_t> msgs;
          {
            CautoMcsSpinLock queue_lck(queue_lock_, mcs_spin_cnt);
            msgs.swap(message_queue_);
            if (enable_perf_hist && schedule_time_ != 0) {
              auto delay_time = Ctime::steady_ns() - schedule_time_;
              g_schedule_hist.update(static_cast<double>(delay_time) / 1e9);
            }
            schedule_time_ = 0; /// clear it anyway
          }
          if (msgs.empty())
            break;
          while (!msgs.empty()) {
            /// exit anytime and use auto lock to keep safe
            if (shutdown_.load(std::memory_order_acquire)) {
              /// ignore all unfinished messages and free by smarter pointer
              success_enter = false; /// no recheck needed
              break;
            } else if (killed_.load(std::memory_order_acquire)) {
              /// safe to access it because we check shutdown before
              tcp_.session_manager().remove_and_shutdown(
                  tcp_.epoll().session_count(),
                  encoder_.message_encoder().sid());
              /// ignore all unfinished messages and free by smarter pointer.
              success_enter = false; /// no recheck needed
              break;
            }

            /// do execute one msg
            auto &msg = msgs.front();
            if (Polarx::ClientMessages::SESS_CLOSE == msg.type) {
              /// send ok first
              encoder_.message_encoder().encode_ok();
              flush();

              /// check and remove myself
              if (!shutdown_.load(std::memory_order_acquire)) {
                tcp_.session_manager().remove_and_shutdown(
                    tcp_.epoll().session_count(),
                    encoder_.message_encoder().sid());
              }

              /// Ignore other msgs. No recheck needed.
              success_enter = false;
              break; /// Break the message consume.
            } else {
              bool run = false;
              int64_t run_start_time = 0;
              if (enable_perf_hist)
                run_start_time = Ctime::steady_ns();
              dispatch(std::move(msg), run);
              if (run_start_time != 0) {
                auto run_end_time = Ctime::steady_ns();
                auto run_time = run_end_time - run_start_time;
                g_run_hist.update(static_cast<double>(run_time) / 1e9);
              }
              if (run)
                detacher.set_run();
            }

            /// done, pop, and next
            msgs.pop();
          }
          /// fast break
          if (shutdown_.load(std::memory_order_acquire)) {
            /// ignore all unfinished messages and free by smarter pointer
            success_enter = false; /// no recheck needed
            break;
          }
        }
      } catch (...) {
        /// do detach on any error
        detacher.set_run();
        encoder_.message_encoder().encode_error(
            Polarx::Error::FATAL, ER_POLARX_RPC_ERROR_MSG,
            "Session unexpected killed.", "HY000");
        flush();
      }
    }
  }

  if (success_enter) {
    /// recheck without enter to prevent missing notify
    auto pending_task = false;
    {
      CautoMcsSpinLock lck(queue_lock_, mcs_spin_cnt);
      pending_task = !message_queue_.empty();
    }
    if (pending_task)
      goto _retry;
  }
}

void Csession::dispatch(msg_t &&msg, bool &run) {
  DBG_LOG(("Session %p run msg %u", this, msg.type));

  /// reset thd kill status
  auto thd = get_thd();
  if (thd != nullptr) {
    if (thd->killed == THD::KILL_QUERY || thd->killed == THD::KILL_TIMEOUT)
      thd->killed = THD::NOT_KILLED;
  }

  err_t err;
  switch (msg.type) {
  case ::Polarx::ClientMessages::EXEC_SQL: {
    auto &m = static_cast<const Polarx::Sql::StmtExecute &>(*msg.msg);

    /// check cascade error
    if (m.has_reset_error() && !m.reset_error() && last_error_)
      err = last_error_;
    else {
      run = true;
      err = sql_stmt_execute(m);
      /// note: cache miss is not an error
      if (err.error != ER_POLARX_RPC_CACHE_MISS)
        last_error_ = err;
    }
  } break;

  case ::Polarx::ClientMessages::EXEC_PLAN_READ: {
    auto &m = static_cast<const Polarx::ExecPlan::ExecPlan &>(*msg.msg);

    /// check cascade error
    if (m.has_reset_error() && !m.reset_error() && last_error_)
      err = last_error_;
    else {
      run = true;
      err = sql_plan_execute(m);
      /// note: cache miss is not an error
      if (err.error != ER_POLARX_RPC_CACHE_MISS)
        last_error_ = err;
    }
  } break;

  default:
    err = err_t(ER_POLARX_RPC_ERROR_MSG, "Message not supported.");
    break;
  }

  if (err) {
    encoder_.message_encoder().encode_error(
        err.get_protocol_severity(), err.error, err.message, err.sql_state);
    flush();
  }
}

err_t Csession::sql_stmt_execute(const Polarx::Sql::StmtExecute &msg) {
  ///
  /// dealing cache
  ///
  /// just for life cycle control, never touch it
  std::shared_ptr<std::string> stmt;
  /// use this to get real stmt
  const std::string *stmt_ptr = nullptr;
  if (msg.has_stmt() && msg.has_stmt_digest()) {
    /// copy, move and put into LRU
    auto digest(msg.stmt_digest());
    if (msg.stmt().size() <= request_cache_max_length) {
      /// only cache which less than max length
      stmt = std::make_shared<std::string>(msg.stmt());
      plugin_info.cache->set_sql(std::move(digest), std::move(stmt));
    }
    stmt_ptr = &msg.stmt(); /// use original in msg
  } else if (msg.has_stmt_digest()) {
    stmt = plugin_info.cache->get_sql(msg.stmt_digest());
    if (!stmt)
      /// not found?
      return err_t(ER_POLARX_RPC_CACHE_MISS, "do not exist query!");
    stmt_ptr = stmt.get();
  } else if (msg.has_stmt())
    stmt_ptr = &msg.stmt();
  else
    return err_t(ER_POLARX_RPC_ERROR_MSG, "Neither stmt nor digest exist.");

  /// switch db
  if (msg.has_schema_name()) {
    const auto &db_name = msg.schema_name();
    CcallbackCommandDelegate delegate;
    auto err = init_db(db_name.data(), db_name.length(), delegate);
    if (err)
      return err; /// invoke error
    if (delegate.get_error())
      return err; /// switch db error
  }

  auto thd = get_thd();
#ifdef MYSQL8
  /// lizard specific GCN timestamp
  if (!thd->in_active_multi_stmt_transaction())
    thd->reset_gcn_variables();
  if (msg.has_use_cts_transaction() && msg.use_cts_transaction())
    thd->variables.innodb_current_snapshot_gcn = true;
  if (msg.has_snapshot_seq())
    thd->variables.innodb_snapshot_gcn = msg.snapshot_seq();
  else if (msg.has_commit_seq())
    thd->variables.innodb_commit_gcn = msg.commit_seq();
#else
  /// 5.7 specific CTS timestamp
  if (!thd_in_active_multi_stmt_transaction(thd))
    thd->clear_transaction_seq();
  if (msg.has_use_cts_transaction() && msg.use_cts_transaction())
    thd->mark_cts_transaction();
  if (msg.has_snapshot_seq())
    thd->set_snapshot_seq(msg.snapshot_seq());
  else if (msg.has_commit_seq())
    thd->set_commit_seq(msg.commit_seq());
#endif

  /// token reset
  auto flow_control_enabled = false;
  if (msg.has_token()) {
    if (msg.token() > 0) {
      flow_control_.flow_reset(msg.token());
      flow_control_enabled = true;
    } else
      return err_t(ER_POLARX_RPC_ERROR_MSG, "Invalid Token");
  }

  /// build query
  qb_.clear();
  Sql_statement_builder builder(&qb_);
  try {
    if (msg.has_hint())
      builder.build(*stmt_ptr, msg.args(), msg.hint());
    else
      builder.build(*stmt_ptr, msg.args());
  } catch (const err_t &e) {
    return e;
  }

  CstmtCommandDelegate delegate(
      *this, encoder_, [this]() { return flush(); }, msg.compact_metadata());
  if (flow_control_enabled)
    delegate.set_flow_control(&flow_control_); /// enable flow control
  /// chunk?
  if (msg.has_chunk_result())
    delegate.set_chunk_result(msg.chunk_result());
  /// feedback?
  if (msg.has_feed_back())
    delegate.set_feedback(msg.feed_back());
  // sql_print_information("sid %lu run %s", sid_, qb_.get().c_str());
  const auto err = execute_sql(qb_.get().data(), qb_.get().length(), delegate);
  // sql_print_information("sid %lu run %s done err %d sqlerr %d", sid_,
  // qb_.get().c_str(), err.error, sql_err.error);
  if (err) {
    /// send warning
    send_warnings(*this, encoder_, true);
    flush();
  }
  return err;
}

err_t Csession::sql_plan_execute(const Polarx::ExecPlan::ExecPlan &msg) {
  ///
  /// dealing cache
  ///
  /// just for life cycle control, never touch it
  std::shared_ptr<Polarx::ExecPlan::AnyPlan> plan;
  /// use this to get real plan
  const Polarx::ExecPlan::AnyPlan *plan_ptr = nullptr;
  if (msg.has_plan() && msg.has_plan_digest()) {
    /// copy, move and put into LRU
    auto digest(msg.plan_digest());
    // TODO check plan store size?
    plan = std::make_shared<Polarx::ExecPlan::AnyPlan>(msg.plan());
    plugin_info.cache->set_plan(std::move(digest), std::move(plan));
    plan_ptr = &msg.plan(); /// use original in msg
  } else if (msg.has_plan_digest()) {
    plan = plugin_info.cache->get_plan(msg.plan_digest());
    if (!plan)
      /// not found?
      return err_t(ER_POLARX_RPC_CACHE_MISS, "do not exist plan!");
    plan_ptr = plan.get();
  } else if (msg.has_plan())
    plan_ptr = &msg.plan();
  else
    return err_t(ER_POLARX_RPC_ERROR_MSG, "Neither stmt nor digest exist.");

  auto thd = get_thd();
#ifdef MYSQL8
  /// lizard specific GCN timestamp
  if (!thd->in_active_multi_stmt_transaction())
    thd->reset_gcn_variables();
  if (msg.has_use_cts_transaction() && msg.use_cts_transaction())
    thd->variables.innodb_current_snapshot_gcn = true;
  if (msg.has_snapshot_seq())
    thd->variables.innodb_snapshot_gcn = msg.snapshot_seq();
  else if (msg.has_commit_seq())
    thd->variables.innodb_commit_gcn = msg.commit_seq();
#else
  /// 5.7 specific CTS timestamp
  if (!thd_in_active_multi_stmt_transaction(thd))
    thd->clear_transaction_seq();
  if (msg.has_use_cts_transaction() && msg.use_cts_transaction())
    thd->mark_cts_transaction();
  if (msg.has_snapshot_seq())
    thd->set_snapshot_seq(msg.snapshot_seq());
  else if (msg.has_commit_seq())
    thd->set_commit_seq(msg.commit_seq());
#endif

  /// token reset
  auto flow_control_enabled = false;
  if (msg.has_token()) {
    if (msg.token() > 0 || msg.token() == -1) {
      flow_control_.flow_reset(msg.token());
      flow_control_enabled = true;
    } else
      return err_t(ER_POLARX_RPC_ERROR_MSG, "Invalid Token");
  }

  CstmtCommandDelegate delegate(
      *this, encoder_, [this]() { return flush(); }, msg.compact_metadata());
  if (flow_control_enabled)
    delegate.set_flow_control(&flow_control_); /// enable flow control
  /// chunk?
  if (msg.has_chunk_result())
    delegate.set_chunk_result(msg.chunk_result());
  /// feedback?
  if (msg.has_feed_back())
    delegate.set_feedback(msg.feed_back());
  auto iret = rpc_executor::Executor::instance().execute(
      *plan_ptr, msg.parameters(), delegate, thd);
  if (iret != 0)
    return err_t(ER_POLARX_RPC_ERROR_MSG, "Executor error");
  return err_t::Success();
}

} // namespace polarx_rpc
