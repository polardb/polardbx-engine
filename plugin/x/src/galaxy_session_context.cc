//
// Created by zzy on 2021/11/23.
//

#include "mutex_lock.h"
#include "sql/sql_class.h"

#include "plugin/x/src/sql_statement_builder.h"
#include "plugin/x/src/xpl_log.h"
#include "plugin/x/src/xpl_resultset.h"

#include "galaxy_session_context.h"

namespace xpl {

void Galaxy_session_context::dispatch(uint8 msg_type, const ngs::Message &msg,
                                      bool &run) {
  auto error = m_expect_stack.pre_client_stmt(msg_type);
  if (error)
    m_encoder->send_result(error);
  else {
    switch (msg_type) {
      case Mysqlx::ClientMessages::GALAXY_STMT_EXECUTE: {
        run = true;
        error = sql_stmt_execute(
            static_cast<const Mysqlx::Sql::GalaxyStmtExecute &>(msg));
      } break;

      case Mysqlx::ClientMessages::EXPECT_OPEN: {
        error =
            m_expect_stack.open(static_cast<const Mysqlx::Expect::Open &>(msg));
        if (!error) m_encoder->send_ok();
      } break;

      case Mysqlx::ClientMessages::EXPECT_CLOSE: {
        error = m_expect_stack.close();
        if (!error) m_encoder->send_ok();
      } break;

      default:
        error = ngs::Error(ER_UNKNOWN_COM_ERROR, "Unexpected message received");
        break;
    }
    if (error) m_encoder->send_result(error);
    m_expect_stack.post_client_stmt(msg_type, error);
  }
}

ngs::Error_code Galaxy_session_context::sql_stmt_execute(
    const Mysqlx::Sql::GalaxyStmtExecute &msg) {
  log_debug("GP: dealing_stmt_exec");

  // reset thd kill status
  auto thd = m_sql.get_thd();
  if (thd != nullptr) {
    if (thd->killed == THD::KILL_QUERY || thd->killed == THD::KILL_TIMEOUT)
      thd->killed = THD::NOT_KILLED;
  }

  if (msg.has_db_name()) {
    auto &db_name = msg.db_name();
    Empty_resultset empty_rs;
    auto error = m_sql.init_db(db_name.data(), db_name.length(), &empty_rs);
    if (error) return error;
  }

  m_qb.clear();
  Sql_statement_builder builder(&m_qb);
  try {
    builder.build(msg.stmt(), msg.args());
  } catch (const ngs::Error_code &error) {
    return error;
  }

  Streaming_resultset<Stmt_command_delegate> rs(*this, msg.compact_metadata());
  log_debug("RunSQL: %s", m_qb.get().c_str());
  m_flow_control.flow_reset(msg.token());
  auto error = m_sql.execute(m_qb.get().data(), m_qb.get().length(), &rs);
  log_debug("RunSQL done: %s", m_qb.get().c_str());

  if (error) return error;
  return ngs::Success();
}

void Galaxy_session_context::remote_kill() {
  if (gx::Galaxy_system_variables::m_enable_galaxy_kill_log) {
    // Log first.
    char buf[0x100];
    snprintf(buf, sizeof(buf), "Galaxy session %p sid %zu kill pool %p.", this,
             m_sid, &m_pool);
    log_warning(ER_XPLUGIN_ERROR_MSG, buf);
  }

  // Store exit flag first.
  m_killed.store(true, std::memory_order_release);
  // Shutdown flow control.
  m_flow_control.exit();
  // Do kill.
  auto thd = m_sql.get_thd();
  if (thd != nullptr) {
    mysql_mutex_lock(&thd->LOCK_thd_data);
    // Lock held and awake with kill.
    if (thd->killed != THD::KILL_CONNECTION) thd->awake(THD::KILL_CONNECTION);
    mysql_mutex_unlock(&thd->LOCK_thd_data);
  }
}

void Galaxy_session_context::remote_cancel() {
  if (gx::Galaxy_system_variables::m_enable_galaxy_kill_log) {
    // Log first.
    char buf[0x100];
    snprintf(buf, sizeof(buf), "Galaxy session %p sid %zu cancel pool %p.",
             this, m_sid, &m_pool);
    log_warning(ER_XPLUGIN_ERROR_MSG, buf);
  }

  auto thd = m_sql.get_thd();
  if (thd != nullptr) {
    mysql_mutex_lock(&thd->LOCK_thd_data);
    // Lock held and awake with kill.
    if (thd->killed != THD::KILL_CONNECTION) thd->awake(THD::KILL_QUERY);
    mysql_mutex_unlock(&thd->LOCK_thd_data);
  }
}

}  // namespace xpl
