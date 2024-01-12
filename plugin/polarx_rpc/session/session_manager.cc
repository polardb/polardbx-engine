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
#include "../server/epoll.h"
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
    /// reset sid first
    tcp.msg_enc().reset_sid(sid);

    /// auto commit session
    const auto err = execute_locally(tcp, std::forward<msg_t>(msg));
    if (UNLIKELY(err)) {
      tcp.msg_enc().encode_error(err.get_protocol_severity(),
                                 ER_POLARX_RPC_ERROR_MSG, err.message,
                                 err.sql_state);
      tcp.encoder().flush(tcp);
    }
    return;
  }

  switch (msg.type) {
    case ::PolarXRPC::ClientMessages::SESS_NEW: {
      /// check limits first
      auto now_sessions = g_session_count.load(std::memory_order_acquire);
      if (static_cast<ulong>(now_sessions) > max_connections * 9 / 10) {
        tcp.msg_enc().reset_sid(sid);
        tcp.msg_enc().encode_error(PolarXRPC::Error::FATAL,
                                   ER_POLARX_RPC_ERROR_MSG,
                                   "max_connections limit exceed", "HY000");
        tcp.encoder().flush(tcp);
      } else {
        std::shared_ptr<Csession> s;
        auto err = new_session(tcp, sid, s);
        tcp.msg_enc().reset_sid(sid);
        if (UNLIKELY(static_cast<bool>(err)))
          tcp.msg_enc().encode_error(PolarXRPC::Error::FATAL, err.error,
                                     err.message, err.sql_state);
        else {
          s->detach();  /// detach and clear TLS thd anyway
          tcp.msg_enc().encode_ok();
        }
        tcp.encoder().flush(tcp);
      }
    } break;

    case ::PolarXRPC::ClientMessages::FILE_OPERATION_GET_FILE_INFO:
    case ::PolarXRPC::ClientMessages::FILE_OPERATION_TRANSFER_FILE_DATA:
    case ::PolarXRPC::ClientMessages::FILE_OPERATION_FILE_MANAGE:
    case ::PolarXRPC::ClientMessages::AUTO_SP:
    case ::PolarXRPC::ClientMessages::SESS_CLOSE:
    case ::PolarXRPC::ClientMessages::EXEC_SQL:
    case ::PolarXRPC::ClientMessages::EXEC_PLAN_READ: {
      /// normal session request
      auto session = get_session(sid);
      if (LIKELY(session)) {
        auto need_notify = false;
        if (LIKELY(
                session->push_message(std::forward<msg_t>(msg), need_notify))) {
          if (need_notify) {
            auto in_trx =
                session->get_thd()->in_active_multi_stmt_transaction();
            notify_set.emplace(std::make_pair(sid, in_trx));
          }
          /// although moved, type in msg still valid
          DBG_LOG(("SessionMgr %p got message %u and push to session %lu", this,
                   msg.type, sid));
        } else {
          /// Send the fatal message first before any killed message.
          tcp.msg_enc().reset_sid(sid);
          tcp.msg_enc().encode_error(
              PolarXRPC::Error::FATAL, ER_POLARX_RPC_ERROR_MSG,
              "Session max_queued_messages limit exceed.", "HY000");
          tcp.encoder().flush(tcp);
          /// remove and shutdown session
          remove_and_shutdown(tcp.epoll().session_count(), sid, true);
        }
      } else {
        tcp.msg_enc().reset_sid(sid);
        tcp.msg_enc().encode_error(PolarXRPC::Error::FATAL,
                                   ER_POLARX_RPC_ERROR_MSG,
                                   "Session not found.", "HY000");
        tcp.encoder().flush(tcp);
      }
    } break;

    case ::PolarXRPC::ClientMessages::GET_TSO: {
      auto &tso_req =
          static_cast<const ::PolarXRPC::ExecPlan::GetTSO &>(*msg.msg);

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
          err_no = GTS_PROTOCOL_NOT_INITED;  /// 1
          break;
        default:
          err_no = GTS_PROTOCOL_LEASE_EXPIRE;  /// 2
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
        ::PolarXRPC::ExecPlan::ResultTSO tso_msg;
        tso_msg.set_error_no(err_no);
        tso_msg.set_ts(ts);
        tcp.msg_enc()
            .encode_protobuf_message<
                ::PolarXRPC::ServerMessages::RESULTSET_TSO>(tso_msg);
      }
      tcp.encoder().flush(tcp);
    } break;

    case ::PolarXRPC::ClientMessages::SESS_KILL: {
      auto &kill_msg =
          static_cast<const ::PolarXRPC::Session::KillSession &>(*msg.msg);
      auto not_found = false;
      if (::PolarXRPC::Session::KillSession_KillType_CONNECTION ==
          kill_msg.type()) {
        /// Note: when kill, should release the session to ensure session freed
        if (LIKELY(
                remove_and_shutdown(tcp.epoll().session_count(), sid, true))) {
          /// send fatal msg()
          tcp.msg_enc().reset_sid(sid);
          tcp.msg_enc().encode_error(PolarXRPC::Error::FATAL,
                                     ER_POLARX_RPC_ERROR_MSG,
                                     "Session is killing.", "70100");
          tcp.encoder().flush(tcp);
        } else
          not_found = true;
      } else {
        auto session = get_session(sid);
        if (LIKELY(session))
          session->remote_cancel();  /// no return message
        else
          not_found = true;
      }
      if (UNLIKELY(not_found)) {
        tcp.msg_enc().reset_sid(sid);
        tcp.msg_enc().encode_error(PolarXRPC::Error::FATAL,
                                   ER_POLARX_RPC_ERROR_MSG,
                                   "Session not found.", "HY000");
        tcp.encoder().flush(tcp);
      }
    } break;

    case ::PolarXRPC::ClientMessages::TOKEN_OFFER: {
      auto session = get_session(sid);
      if (LIKELY(session)) {
        const auto token =
            static_cast<const ::PolarXRPC::Sql::TokenOffer &>(*msg.msg).token();
        if (token >= 0) {
          auto flow_size = token * 1024;  /// unit is KB
          if (flow_size < 0 || flow_size < token)
            flow_size = INT32_MAX;  /// set to max when overflow
          session->flow_control().flow_offer(flow_size);
        }
      } else {
        tcp.msg_enc().reset_sid(sid);
        tcp.msg_enc().encode_error(PolarXRPC::Error::FATAL,
                                   ER_POLARX_RPC_ERROR_MSG,
                                   "Session not found.", "HY000");
        tcp.encoder().flush(tcp);
      }
    } break;

    default:
      tcp.msg_enc().reset_sid(sid);
      tcp.msg_enc().encode_error(PolarXRPC::Error::FATAL,
                                 ER_POLARX_RPC_ERROR_MSG,
                                 "Unexpected message for polarx rpc.", "HY000");
      tcp.encoder().flush(tcp);
  }
}

err_t CsessionManager::get_shared_session_and_attach(
    CmtEpoll &epoll, std::unique_ptr<reusable_session_t> &ptr) {
  /// take one session from pool
  std::unique_ptr<reusable_session_t> s;
  epoll.get_extra_ctx().reusable_sessions.pop(s);
  if (!s) {
    /// generate new one
    s.reset(new reusable_session_t);
    auto err = s->session.init(0);
    if (UNLIKELY(err)) return err;  /// auto free
    err = s->session.switch_to_user(user_.c_str(), "shared_session", nullptr,
                                    nullptr);
    if (UNLIKELY(err)) return err;  /// auto free
  }

  /// attach
  const auto err = s->session.attach();
  if (UNLIKELY(err)) return err;  /// auto free

  /// good attached session
  ptr.swap(s);
  return err_t::Success();
}

void CsessionManager::detach_and_reuse_shared_session(
    CmtEpoll &epoll, std::unique_ptr<reusable_session_t> &ptr,
    const err_t &err) {
  /// detach
  const auto detach_err = ptr->session.detach();
  if (UNLIKELY(!ptr->session.is_detach_and_tls_cleared()))
    sql_print_error("FATAL: polarx_rpc tso session not detach and cleared!");

  /// try recycle
  if (UNLIKELY(err || detach_err ||
               Ctime::steady_ms() - ptr->start_time_ms >
                   shared_session_lifetime))
    ptr.reset();
  else
    /// auto free if failed to push
    epoll.get_extra_ctx().reusable_sessions.push(std::move(ptr));
}

#ifdef MYSQL8

#define SYS_GTS_DB "mysql"
#define SYS_GTS_TABLE "gts_base"

err_t CsessionManager::get_tso_mysql80(CtcpConnection &tcp, uint64_t &ts,
                                       int32_t batch_count) {
  /// take one session from pool
  std::unique_ptr<reusable_session_t> s;
  auto err = get_shared_session_and_attach(tcp.epoll(), s);
  if (UNLIKELY(err)) return err;
  assert(s);

  /// mark start
  auto thd = s->session.get_thd();
  static constexpr char get_tso_query[] = "select get_tso()";
  CsessionBase::begin_query(thd, get_tso_query, sizeof(get_tso_query) - 1);

  /// get tso
  TimestampService ts_service(s->session.get_thd(), SYS_GTS_DB, SYS_GTS_TABLE);
  if (ts_service.init()) {
    err = err_t(ER_POLARX_RPC_ERROR_MSG, "Failed to init GTS table.");
  } else {
    if (ts_service.get_timestamp(ts, batch_count))
      err = err_t(ER_POLARX_RPC_ERROR_MSG, "Failed to get tso.");
    ts_service.deinit();
  }

  /// mark end
  CsessionBase::end_query(thd);

  /// detach and try recycle
  detach_and_reuse_shared_session(tcp.epoll(), s, err);
  return err;
}

#endif

err_t CsessionManager::execute_locally(CtcpConnection &tcp, msg_t &&msg) {
  /// check type
  if (UNLIKELY(msg.type != ::PolarXRPC::ClientMessages::EXEC_SQL))
    return err_t(ER_POLARX_RPC_ERROR_MSG, "Not support for polarx rpc.");

  /// take one session from pool
  std::unique_ptr<reusable_session_t> s;
  auto err = get_shared_session_and_attach(tcp.epoll(), s);
  if (UNLIKELY(err)) return err;
  assert(s);

  /// dealing it
  err = sql_stmt_execute_locally(
      tcp, *s, static_cast<const ::PolarXRPC::Sql::StmtExecute &>(*msg.msg));

  /// detach and try recycle
  detach_and_reuse_shared_session(tcp.epoll(), s, err);
  return err;
}

err_t CsessionManager::sql_stmt_execute_locally(
    CtcpConnection &tcp, reusable_session_t &s,
    const PolarXRPC::Sql::StmtExecute &msg) {
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
    const auto err = s.session.init_db(db_name.data(), db_name.length());
    if (UNLIKELY(err)) return err;  /// fatal error
  }

  /// execute query
  const auto thd = s.session.get_thd();
  const auto charset = thd->variables.character_set_results != nullptr
                           ? thd->variables.character_set_results
                           : &my_charset_utf8mb4_general_ci;
  s.qb.clear();
  Sql_statement_builder builder(&s.qb);
  try {
    if (msg.has_hint())
      builder.build(msg.stmt(), msg.args(), *charset, msg.hint());
    else
      builder.build(msg.stmt(), msg.args(), *charset);
  } catch (const err_t &e) {
    return e;
  }

  const auto caps =
      msg.has_capabilities() ? msg.capabilities() : DEFAULT_CAPABILITIES;
  CstmtCommandDelegate delegate(
      s.session, tcp.encoder(), [&tcp]() { return tcp.encoder().flush(tcp); },
      msg.compact_metadata(), caps);
  const auto err =
      s.session.execute_sql(s.qb.get().data(), s.qb.get().length(), delegate);
  if (err) {
    /// send warning
    send_warnings(s.session, tcp.encoder(), true);
    tcp.encoder().flush(tcp);
  }

  /// reset after successful invoke but ignore result
  s.session.reset();

  /// mark sleep
  CsessionBase::cleanup_and_mark_sleep(thd);

  return err;
}

err_t CsessionManager::new_session(CtcpConnection &tcp, const uint64_t &sid,
                                   std::shared_ptr<Csession> &ptr) {
  auto session = std::make_shared<Csession>(tcp.epoll(), tcp, sid);
  auto err = session->init(port_);
  if (UNLIKELY(err)) return err;
  err = session->switch_to_user(user_.c_str(), host_.c_str(), nullptr, nullptr);
  if (UNLIKELY(err)) return err;
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

}  // namespace polarx_rpc
