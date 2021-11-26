//
// Created by zzy on 2021/11/19.
//

#include <cassert>
#include <string>

#include "galaxy_parallel_handler.h"
#include "plugin/x/src/sql_statement_builder.h"
#include "plugin/x/src/xpl_error.h"
#include "plugin/x/src/xpl_log.h"
#include "plugin/x/src/xpl_resultset.h"
#include "sql/log.h"

namespace xpl {

bool Galaxy_parallel_handler::execute(ngs::Message_request &command) {
  init_once();  // Init only when invoked.

  auto sid = command.get_galaxy_request().sid;
  log_debug("GP sid:%lu type:%u", sid, command.get_message_type());

  if ((sid & gx::AUTO_COMMIT_GSESSION_ID_MASK) != 0)
    // This is auto commit optimized request. Just run by myself.
    return execute_locally(command);

  switch (command.get_message_type()) {
    case Mysqlx::ClientMessages::SESS_NEW: {
      std::shared_ptr<Galaxy_session_context> session;
      auto err = m_sessions.new_session(m_session, sid, session);
      if (err) {
        m_session.proto().build_header(gx::Protocol_type::GALAXYX, sid);
        m_session.proto().send_error(err);
      } else {
        assert(session);
        session->encoder().send_ok();  // Use session encoder to send.
      }
    } break;

    case Mysqlx::ClientMessages::EXPECT_OPEN:
    case Mysqlx::ClientMessages::EXPECT_CLOSE:
    case Mysqlx::ClientMessages::SESS_CLOSE:
    case Mysqlx::ClientMessages::GALAXY_STMT_EXECUTE: {
      auto session = m_sessions[sid];
      if (session) {
        auto need_notify = false;
        if (session->push_message(std::make_pair(command.get_message_type(),
                                                 command.move_out_message()),
                                  need_notify)) {
          if (need_notify) m_sessions.push_task(sid);
        } else {
          // Send the fatal message first before any killed message.
          m_session.proto().build_header(gx::Protocol_type::GALAXYX, sid);
          m_session.proto().send_error(
              ngs::Fatal(ER_XPLUGIN_ERROR_MSG,
                         "Session max_queued_messages limit exceed"));
          // Session queue depth limit exceed. Remove and kill it.
          m_sessions.remove_and_kill(sid);
        }
      } else {
        m_session.proto().build_header(gx::Protocol_type::GALAXYX, sid);
        m_session.proto().send_error(ngs::Fatal(
            ER_XPLUGIN_ERROR_MSG, "Session not found in galaxy parallel"));
      }
    } break;

    case Mysqlx::ClientMessages::GET_TSO: {
      // Redirect to sid.
      m_session.proto().build_header(gx::Protocol_type::GALAXYX, sid);
      auto err = m_tso_handler.execute(
          static_cast<const Mysqlx::GetTSO &>(*command.get_message()));
      if (err) m_session.proto().send_error(err);
    } break;

    case Mysqlx::ClientMessages::TOKEN_OFFER: {
      auto session = m_sessions[sid];
      if (session) {
        session->flow_control().flow_offer(
            static_cast<const Mysqlx::Sql::TokenOffer &>(*command.get_message())
                .token());
      } else {
        m_session.proto().build_header(gx::Protocol_type::GALAXYX, sid);
        m_session.proto().send_error(ngs::Fatal(
            ER_XPLUGIN_ERROR_MSG, "Session not found in galaxy parallel"));
      }
    } break;

    case Mysqlx::ClientMessages::SESS_KILL: {
      auto &kill_msg = static_cast<const Mysqlx::Session::KillSession &>(
          *command.get_message());
      auto session = m_sessions[kill_msg.x_session_id()];
      if (session) {
        if (kill_msg.type() == Mysqlx::Session::KillSession_KillType_CONNECTION)
          session->remote_kill();
        else
          session->remote_cancel();
        // No return message.
      } else {
        m_session.proto().build_header(gx::Protocol_type::GALAXYX, sid);
        m_session.proto().send_error(
            ngs::Fatal(ER_XPLUGIN_ERROR_MSG,
                       "Session to kill not found in galaxy parallel"));
      }
    } break;

    default: {
      auto err = ngs::Fatal(ER_UNKNOWN_COM_ERROR,
                            "Unexpected message for galaxy parallel");
      m_session.proto().build_header(gx::Protocol_type::GALAXYX, sid);
      m_session.proto().send_error(err);
    } break;
  }

  return true;
}

bool Galaxy_parallel_handler::execute_locally(ngs::Message_request &command) {
  auto sid = command.get_galaxy_request().sid;
  // Switch to target sid.
  m_session.proto().build_header(gx::Protocol_type::GALAXYX, sid);

  switch (command.get_message_type()) {
    case Mysqlx::ClientMessages::GALAXY_STMT_EXECUTE: {
      try {
        auto error = sql_stmt_execute_locally(
            static_cast<const Mysqlx::Sql::GalaxyStmtExecute &>(
                *command.get_message()));
        if (error) m_session.proto().send_error(error);
      } catch (ngs::Error_code &err) {
        m_session.proto().send_error(err);
      }
    } break;

    default: {
      auto err = ngs::Fatal(
          ER_UNKNOWN_COM_ERROR,
          "Only GALAXY_STMT_EXECUTE allowed for auto commit request");
      m_session.proto().send_error(err);
    } break;
  }
  return true;
}

ngs::Error_code Galaxy_parallel_handler::sql_stmt_execute_locally(
    const Mysqlx::Sql::GalaxyStmtExecute &msg) {
  log_debug("GP: sql_stmt_execute_locally");

  auto &da = m_session.data_context();
  // Reset before and after auto commit invoke to prevent any side effect.
  auto err = da.reset();
  if (err) return err;

  if (msg.has_db_name()) {
    auto &db_name = msg.db_name();
    Empty_resultset empty_rs;
    err = da.init_db(db_name.data(), db_name.length(), &empty_rs);
    if (err) return err;
  }

  m_qb.clear();
  Sql_statement_builder builder(&m_qb);
  try {
    builder.build(msg.stmt(), msg.args());
  } catch (const ngs::Error_code &error) {
    return error;
  }

  auto show_warnings = m_session.get_notice_configuration().is_notice_enabled(
      ngs::Notice_type::k_warning);
  auto &proto = m_session.proto();
  Streaming_resultset<Stmt_command_delegate> resultset(&m_session,
                                                       msg.compact_metadata());
  err = da.execute(m_qb.get().data(), m_qb.get().length(), &resultset);

  if (err) {
    if (show_warnings) notices::send_warnings(da, proto, true);
    return err;
  }

  // Reset after successful invoke but ignore result.
  da.reset();

  return ngs::Success();
}

void Galaxy_parallel_handler::init_once() {
  std::call_once(m_init, [this]() {
    // Run one to init Vio for parallel.
    auto bret = m_session.client().connection().prepare_for_parallel();
    sql_print_information("Galaxy parallel prepare Vio %s",
                          bret ? "success" : "fail");
  });
}

}  // namespace xpl
