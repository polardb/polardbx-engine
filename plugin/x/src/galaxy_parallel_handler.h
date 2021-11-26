//
// Created by zzy on 2021/11/19.
//

#pragma once

#include <mutex>

#include "plugin/x/ngs/include/ngs/error_code.h"
#include "plugin/x/ngs/include/ngs/interface/session_interface.h"
#include "plugin/x/ngs/include/ngs/protocol/message.h"
#include "plugin/x/src/galaxy_tso_handler.h"

#include "galaxy_session_pool_manager.h"

namespace xpl {

class Galaxy_parallel_handler {
 public:
  explicit Galaxy_parallel_handler(ngs::Session_interface &session)
      : m_session(session), m_tso_handler(&session) {}

  bool execute(ngs::Message_request &command);
  bool execute_locally(ngs::Message_request &command);
  ngs::Error_code sql_stmt_execute_locally(
      const Mysqlx::Sql::GalaxyStmtExecute &msg);

  inline void on_free() { m_sessions.on_free(); }

  void init_once();

 private:
  ngs::Session_interface &m_session;
  Galaxy_session_pool_manager m_sessions;

  // TSO.
  gx::Get_tso_handler m_tso_handler;

  std::once_flag m_init;

  // Query builder for local execute.
  xpl::Query_string_builder m_qb{1024};
};

}  // namespace xpl
