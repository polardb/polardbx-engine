//
// Created by zzy on 2022/8/3.
//

#include <cstring>
#include <memory>

#include "../global_defines.h"
#ifdef MYSQL8
#include "sql/sql_lex.h"
#include "sql/timestamp_service.h"
#else
#define MYSQL_SERVER
#include "global_timestamp_service.h"
#endif
#include "sql/mysqld.h"
#include "sql/sql_class.h"

#include "../coders/buffering_command_delegate.h"
#include "../coders/custom_command_delegates.h"
#include "../coders/notices.h"
#include "../coders/protocol_fwd.h"
#include "../polarx_rpc.h"
#include "../server/epoll_group_ctx.h"
#include "../server/tcp_connection.h"
#include "../sql_query/sql_statement_builder.h"
#include "../utility/time.h"

#include "session_manager.h"

namespace polarx_rpc {

void CsessionManager::execute(CtcpConnection &tcp, const uint64_t &sid,
                              msg_t &&msg,
                              std::map<uint64_t, bool> &notify_set) {
  if (sid & UINT64_C(0x8000000000000000)) {
    /// auto commit session
    return execute_locally(tcp, sid, std::forward<msg_t>(msg));
  }

  switch (msg.type) {
  case ::Polarx::ClientMessages::SESS_NEW: {
    /// check limits first
    auto now_sessions = g_session_count.load(std::memory_order_acquire);
    if (static_cast<ulong>(now_sessions) > max_connections * 9 / 10) {
      tcp.msg_enc().reset_sid(sid);
      tcp.msg_enc().encode_error(Polarx::Error::FATAL, ER_POLARX_RPC_ERROR_MSG,
                                 "max_connections limit exceed", "HY000");
      tcp.encoder().flush(tcp);
    } else {
      std::shared_ptr<Csession> s;
      auto err = new_session(tcp, sid, s);
      tcp.msg_enc().reset_sid(sid);
      if (UNLIKELY(static_cast<bool>(err)))
        tcp.msg_enc().encode_error(Polarx::Error::FATAL, err.error, err.message,
                                   err.sql_state);
      else {
        s->detach(); /// detach and clear TLS thd anyway
        tcp.msg_enc().encode_ok();
      }
      tcp.encoder().flush(tcp);
    }
  } break;

  case ::Polarx::ClientMessages::SESS_CLOSE:
  case ::Polarx::ClientMessages::EXEC_SQL:
  case ::Polarx::ClientMessages::EXEC_PLAN_READ: {
    /// normal session request
    auto session = get_session(sid);
    if (LIKELY(session)) {
      auto need_notify = false;
      if (LIKELY(
              session->push_message(std::forward<msg_t>(msg), need_notify))) {
        if (need_notify) {
          auto in_trx = session->get_thd()->in_active_multi_stmt_transaction();
          notify_set.emplace(std::make_pair(sid, in_trx));
        }
        /// although moved, type in msg still valid
        DBG_LOG(("SessionMgr %p got message %u and push to session %lu", this,
                 msg.type, sid));
      } else {
        /// Send the fatal message first before any killed message.
        tcp.msg_enc().reset_sid(sid);
        tcp.msg_enc().encode_error(
            Polarx::Error::FATAL, ER_POLARX_RPC_ERROR_MSG,
            "Session max_queued_messages limit exceed.", "HY000");
        tcp.encoder().flush(tcp);
        /// remove and shutdown session
        remove_and_shutdown(tcp.epoll().session_count(), sid);
      }
    } else {
      tcp.msg_enc().reset_sid(sid);
      tcp.msg_enc().encode_error(Polarx::Error::FATAL, ER_POLARX_RPC_ERROR_MSG,
                                 "Session not found.", "HY000");
      tcp.encoder().flush(tcp);
    }
  } break;

  case ::Polarx::ClientMessages::GET_TSO: {
    auto &tso_req = static_cast<const ::Polarx::ExecPlan::GetTSO &>(*msg.msg);

    err_t err;
    uint64_t ts = 0;
    auto err_no = 0;
#ifdef MYSQL8
    /// note when 80 should use session
    err = get_tso_mysql80(tcp, ts, tso_req.batch_count());
#else
    /// special for AliSQL 5.7
    auto &inst = GlobalTimeService::get_gts_instance();
    auto iret = inst.get_timestamp(ts, tso_req.batch_count());
    switch (iret) {
    case GTS_SUCCESS:
      err_no = GTS_PROTOCOL_SUCCESS;
      break;
    case GTS_NOT_INITED:
      err_no = GTS_PROTOCOL_NOT_INITED; /// 1
      break;
    default:
      err_no = GTS_PROTOCOL_LEASE_EXPIRE; /// 2
      break;
    }
#endif

    /// send it
    tcp.msg_enc().reset_sid(sid);
    if (err) {
      tcp.msg_enc().encode_error(err.get_protocol_severity(),
                                 ER_POLARX_RPC_ERROR_MSG, err.message,
                                 err.sql_state);
    } else {
      ::Polarx::ExecPlan::ResultTSO tso_msg;
      tso_msg.set_error_no(err_no);
      tso_msg.set_ts(ts);
      tcp.msg_enc()
          .encode_protobuf_message<::Polarx::ServerMessages::RESULTSET_TSO>(
              tso_msg);
    }
    tcp.encoder().flush(tcp);
  } break;

  case ::Polarx::ClientMessages::SESS_KILL: {
    auto &kill_msg =
        static_cast<const ::Polarx::Session::KillSession &>(*msg.msg);
    auto not_found = false;
    if (::Polarx::Session::KillSession_KillType_CONNECTION == kill_msg.type()) {
      /// Note: when kill, should release the session to ensure session freed
      if (LIKELY(remove_and_shutdown(tcp.epoll().session_count(), sid))) {
        /// send fatal msg()
        tcp.msg_enc().reset_sid(sid);
        tcp.msg_enc().encode_error(Polarx::Error::FATAL,
                                   ER_POLARX_RPC_ERROR_MSG,
                                   "Session is killing.", "70100");
        tcp.encoder().flush(tcp);
      } else
        not_found = true;
    } else {
      auto session = get_session(sid);
      if (LIKELY(session))
        session->remote_cancel(); /// no return message
      else
        not_found = true;
    }
    if (UNLIKELY(not_found)) {
      tcp.msg_enc().reset_sid(sid);
      tcp.msg_enc().encode_error(Polarx::Error::FATAL, ER_POLARX_RPC_ERROR_MSG,
                                 "Session not found.", "HY000");
      tcp.encoder().flush(tcp);
    }
  } break;

  case ::Polarx::ClientMessages::TOKEN_OFFER: {
    auto session = get_session(sid);
    if (LIKELY(session)) {
      session->flow_control().flow_offer(
          static_cast<const ::Polarx::Sql::TokenOffer &>(*msg.msg).token());
    } else {
      tcp.msg_enc().reset_sid(sid);
      tcp.msg_enc().encode_error(Polarx::Error::FATAL, ER_POLARX_RPC_ERROR_MSG,
                                 "Session not found.", "HY000");
      tcp.encoder().flush(tcp);
    }
  } break;

  default:
    tcp.msg_enc().reset_sid(sid);
    tcp.msg_enc().encode_error(Polarx::Error::FATAL, ER_POLARX_RPC_ERROR_MSG,
                               "Unexpected message for polarx rpc.", "HY000");
    tcp.encoder().flush(tcp);
  }
}

#ifdef MYSQL8

#define SYS_GTS_DB "mysql"
#define SYS_GTS_TABLE "gts_base"

err_t CsessionManager::get_tso_mysql80(CtcpConnection &tcp, uint64_t &ts,
                                       int32_t batch_count) {
  /// take one session from pool
  std::unique_ptr<reusable_session_t> s;
  tcp.epoll().get_extra_ctx().reusable_sessions.pop(s);
  if (!s) {
    /// generate new one
    s.reset(new reusable_session_t);
    auto err = s->session.init(0);
    if (UNLIKELY(err))
      return err;
    /// mark host with epoll group info
    char buf[0x100];
    ::snprintf(buf, sizeof(buf), "xG%02u_shared_session",
               tcp.epoll().group_id());
    err = s->session.switch_to_user(user_.c_str(), buf, nullptr, nullptr);
    if (UNLIKELY(err))
      return err;
  }

  /// attach
  auto thd = s->session.get_thd();
  THD *current_stack = thd;
  thd->thread_stack = reinterpret_cast<char *>(&current_stack);
  thd->store_globals();
  THD_STAGE_INFO(thd, stage_starting);
  thd->set_time();
  thd->lex->sql_command = SQLCOM_SELECT;

  /// get tso
  err_t err;
  TimestampService ts_service(s->session.get_thd(), SYS_GTS_DB, SYS_GTS_TABLE);
  if (ts_service.init()) {
    err = err_t(ER_POLARX_RPC_ERROR_MSG, "Failed to init GTS table.");
  } else {
    if (ts_service.get_timestamp(ts, batch_count))
      err = err_t(ER_POLARX_RPC_ERROR_MSG, "Failed to get tso.");
    ts_service.deinit();
  }

  /// mark sleep
  thd->reset_query();
  thd->set_command(COM_SLEEP);
  thd->proc_info = nullptr;
  thd->lex->sql_command = SQLCOM_END;

  /// detach
  s->session.detach();

  /// try recycle
  if (UNLIKELY(err ||
               Ctime::steady_ms() - s->start_time_ms > shared_session_lifetime))
    s.reset();
  else
    /// auto free if failed to push
    tcp.epoll().get_extra_ctx().reusable_sessions.push(std::move(s));

  return err;
}

#endif

void CsessionManager::execute_locally(CtcpConnection &tcp, const uint64_t &sid,
                                      msg_t &&msg) {
  /// reset sid first
  tcp.msg_enc().reset_sid(sid);

  /// check type
  if (UNLIKELY(msg.type != ::Polarx::ClientMessages::EXEC_SQL)) {
    tcp.msg_enc().encode_error(Polarx::Error::ERROR, ER_POLARX_RPC_ERROR_MSG,
                               "Not support for polarx rpc.", "HY000");
    tcp.encoder().flush(tcp);
    return;
  }

  /// take one session from pool
  std::unique_ptr<reusable_session_t> s;
  tcp.epoll().get_extra_ctx().reusable_sessions.pop(s);
  if (!s) {
    /// generate new one
    s.reset(new reusable_session_t);
    auto err = s->session.init(0);
    if (UNLIKELY(err)) {
      tcp.msg_enc().encode_error(err.get_protocol_severity(),
                                 ER_POLARX_RPC_ERROR_MSG, err.message,
                                 err.sql_state);
      tcp.encoder().flush(tcp);
      return;
    }
    /// mark host with epoll group info
    char buf[0x100];
    ::snprintf(buf, sizeof(buf), "xG%02u_shared_session",
               tcp.epoll().group_id());
    err = s->session.switch_to_user(user_.c_str(), buf, nullptr, nullptr);
    if (UNLIKELY(err)) {
      tcp.msg_enc().encode_error(err.get_protocol_severity(),
                                 ER_POLARX_RPC_ERROR_MSG, err.message,
                                 err.sql_state);
      tcp.encoder().flush(tcp);
      return;
    }
  }

  /// dealing it
  auto err = sql_stmt_execute_locally(
      tcp, *s, static_cast<const ::Polarx::Sql::StmtExecute &>(*msg.msg));
  /// try recycle
  if (UNLIKELY(err ||
               Ctime::steady_ms() - s->start_time_ms > shared_session_lifetime))
    s.reset();
  else
    /// auto free if failed to push
    tcp.epoll().get_extra_ctx().reusable_sessions.push(std::move(s));

  /// send err if needed
  if (UNLIKELY(err)) {
    tcp.msg_enc().encode_error(err.get_protocol_severity(),
                               ER_POLARX_RPC_ERROR_MSG, err.message,
                               err.sql_state);
    tcp.encoder().flush(tcp);
  }
}

err_t CsessionManager::sql_stmt_execute_locally(
    CtcpConnection &tcp, reusable_session_t &s,
    const Polarx::Sql::StmtExecute &msg) {
  /// reset first
  auto err = s.session.reset();
  if (UNLIKELY(err))
    return err;

  /// check options(exec locally not support cache/flow ctl/chunk/feedback)
  /// token is ignored
  if (!msg.has_stmt() || (msg.has_chunk_result() && msg.chunk_result()) ||
      (msg.has_feed_back() && msg.feed_back()))
    return err_t(ER_POLARX_RPC_ERROR_MSG,
                 "Exec locally not support cache/chunk/feedback and token will "
                 "be ignored.");

  /// switch db
  if (msg.has_schema_name()) {
    const auto &db_name = msg.schema_name();
    CcallbackCommandDelegate delegate;
    err = s.session.init_db(db_name.data(), db_name.length(), delegate);
    if (UNLIKELY(err))
      return err; /// invoke error
    if (UNLIKELY(delegate.get_error()))
      return err; /// switch db error
  }

  /// execute query
  s.qb.clear();
  Sql_statement_builder builder(&s.qb);
  try {
    if (msg.has_hint())
      builder.build(msg.stmt(), msg.args(), msg.hint());
    else
      builder.build(msg.stmt(), msg.args());
  } catch (const err_t &e) {
    return e;
  }

  CstmtCommandDelegate delegate(
      s.session, tcp.encoder(), [&tcp]() { return tcp.encoder().flush(tcp); },
      msg.compact_metadata());
  err = s.session.execute_sql(s.qb.get().data(), s.qb.get().length(), delegate);
  if (err) {
    /// send warning
    send_warnings(s.session, tcp.encoder(), true);
    tcp.encoder().flush(tcp);
  }

  /// reset after successful invoke but ignore result
  s.session.reset();

  /// mark sleep
  auto thd = s.session.get_thd();
  thd->reset_query();
  thd->set_command(COM_SLEEP);
  thd->proc_info = nullptr;
  thd->lex->sql_command = SQLCOM_END;

  /// detach
  s.session.detach();
  return err;
}

err_t CsessionManager::new_session(CtcpConnection &tcp, const uint64_t &sid,
                                   std::shared_ptr<Csession> &ptr) {
  auto session = std::make_shared<Csession>(tcp.epoll(), tcp, sid);
  auto err = session->init(port_);
  if (UNLIKELY(err))
    return err;
  err = session->switch_to_user(user_.c_str(), host_.c_str(), nullptr, nullptr);
  if (UNLIKELY(err))
    return err;
  /// concat host with epoll group id
  char buf[0x100];
  ::snprintf(buf, sizeof(buf), "xG%02u_%s", tcp.epoll().group_id(),
             host_.c_str());
  session->set_show_hostname(buf);
  /// finish init
  {
    CautoSpinRWLock lck(session_lock_, true, session_poll_rwlock_spin_cnt);
    auto ib = sessions_.insert(std::make_pair(sid, session));
    if (UNLIKELY(!ib.second))
      return err_t(ER_POLARX_RPC_ERROR_MSG, "Duplicate session ID.");
  }
  tcp.epoll().session_count().fetch_add(1, std::memory_order_release);
  ptr.swap(session);
  DBG_LOG(("SessionMgr %p new session %lu", this, sid));
  return err_t::Success();
}

} // namespace polarx_rpc
