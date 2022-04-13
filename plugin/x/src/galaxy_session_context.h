//
// Created by zzy on 2021/11/22.
//

#pragma once

#include <atomic>
#include <memory>
#include <queue>
#include <utility>

#include "plugin/x/ngs/include/ngs/interface/client_interface.h"
#include "plugin/x/ngs/include/ngs/interface/notice_output_queue_interface.h"
#include "plugin/x/ngs/include/ngs/interface/protocol_encoder_interface.h"
#include "plugin/x/ngs/include/ngs/interface/server_interface.h"
#include "plugin/x/ngs/include/ngs/interface/session_interface.h"
#include "plugin/x/ngs/include/ngs/memory.h"
#include "plugin/x/ngs/include/ngs/protocol/message.h"
#include "plugin/x/ngs/include/ngs/protocol/page_pool.h"
#include "plugin/x/src/document_id_aggregator.h"
#include "plugin/x/src/expect/expect_stack.h"
#include "plugin/x/src/mq/notice_configuration.h"
#include "plugin/x/src/mq/notice_output_queue.h"
#include "plugin/x/src/query_string_builder.h"
#include "plugin/x/src/variables/galaxy_variables.h"
#include "plugin/x/src/xpl_log.h"

#include "galaxy_atomicex.h"
#include "galaxy_flow_control.h"
#include "sql_data_context.h"

namespace xpl {

class Galaxy_session_pool_manager;

class Galaxy_session_context {
 public:
  explicit Galaxy_session_context(ngs::Session_interface &session,
                                  gx::GSession_id sid,
                                  Galaxy_session_pool_manager &pool)
      : m_base_session(session),
        m_sid(sid),
        m_pool(pool),
        m_killed(false),
        m_document_id_aggregator(
            &session.client().server().get_document_id_generator()) {}

  ~Galaxy_session_context() {
    {
      // Log session destroy.
      char buf[0x100];
      snprintf(buf, sizeof(buf), "Galaxy session %p sid %zu deinit pool %p.",
               this, m_sid, &m_pool);
      log_info(ER_XPLUGIN_ERROR_MSG, buf);
    }

    m_sql.deinit();
  }

  static inline ngs::Error_code allocate(
      ngs::Session_interface &session, gx::GSession_id sid,
      Galaxy_session_pool_manager &pool,
      std::shared_ptr<Galaxy_session_context> &ptr) {
    // Use default new and delete for shared_ptr.
    auto ctx(std::make_shared<Galaxy_session_context>(session, sid, pool));
    auto error = ctx->init(session, sid);
    if (error) return error;
    ptr.swap(ctx);
    return error;
  }

  inline ngs::Session_interface &base_session() { return m_base_session; }

  inline ngs::Protocol_encoder_interface &encoder() { return *m_encoder; }

  inline Sql_data_context &data_context() { return m_sql; }

  inline gx::CspinLock &working_lock() { return m_working; }

  inline std::atomic_bool &killed() { return m_killed; }

  inline gx::CspinLock &queue_lock() { return m_queue_lock; }

  inline std::queue<
      std::pair<uint8, ngs::Memory_instrumented<ngs::Message>::Unique_ptr>>
      &queue() {
    return m_message_queue;
  }

  inline ngs::Notice_configuration_interface &get_notice_configuration() {
    return m_notice_configuration;
  }

  inline Notice_output_queue &notice_output_queue() const {
    return *m_notice_output_queue;
  }

  inline ngs::Document_id_aggregator_interface &get_document_id_aggregator() {
    return m_document_id_aggregator;
  }

  inline Galaxy_flow_control &flow_control() { return m_flow_control; }

  void dispatch(uint8 msg_type, const ngs::Message &msg, bool &run);

  inline bool push_message(
      std::pair<uint8, ngs::Memory_instrumented<ngs::Message>::Unique_ptr>
          &&msg,
      bool &need_notify) {
    gx::CautoSpinLock lck(m_queue_lock);
    if (m_message_queue.empty())
      need_notify = true;
    else if (m_message_queue.size() >=
             gx::Galaxy_system_variables::m_max_queued_messages)
      return false;
    m_message_queue.emplace(
        std::forward<std::pair<
            uint8, ngs::Memory_instrumented<ngs::Message>::Unique_ptr>>(msg));
    return true;
  }

  inline ngs::Error_code detach() { return m_sql.detach(); }

  // Take and free LOCK_thd_data internal, so be careful.
  void remote_kill();
  void remote_cancel();

 private:
  inline ngs::Error_code init(ngs::Session_interface &session,
                              gx::GSession_id sid) {
    auto &client = session.client();
    m_encoder =
        ngs::Memory_instrumented<ngs::Protocol_encoder_interface>::Unique_ptr(
            client.allocate_encoder(&m_memory_block_pool));
    m_encoder->build_header(gx::Protocol_type::GALAXYX, sid);
    m_encoder->set_flow_control(&m_flow_control);

    m_notice_output_queue =
        ngs::Memory_instrumented<Notice_output_queue>::Unique_ptr(
            ngs::allocate_object<Notice_output_queue>(m_encoder.get(),
                                                      &m_notice_configuration));

    auto error =
        m_sql.init(client.client_port(), client.connection().get_type());
    if (error) return error;
    m_flow_control.set_thd(m_sql.get_thd());
    auto user(session.data_context().get_authenticated_user_name());
    return m_sql.switch_to_user(user.c_str(), "galaxy", nullptr, nullptr);
  }

  ngs::Error_code sql_stmt_execute(const Mysqlx::Sql::GalaxyStmtExecute &msg);

  ngs::Session_interface &m_base_session;

  // Belongs.
  const gx::GSession_id m_sid;
  const Galaxy_session_pool_manager &m_pool;

  // Global working lock.
  gx::CspinLock m_working;

  // Kill flag and check in working state.
  std::atomic_bool m_killed;

  // Queued messages.
  gx::CspinLock m_queue_lock;
  std::queue<
      std::pair<uint8, ngs::Memory_instrumented<ngs::Message>::Unique_ptr>>
      m_message_queue;

  // Context and working content of session.
  ngs::Memory_block_pool m_memory_block_pool{{10, k_minimum_page_size}};
  ngs::Memory_instrumented<ngs::Protocol_encoder_interface>::Unique_ptr
      m_encoder;
  Sql_data_context m_sql;
  xpl::Query_string_builder m_qb{1024};
  Expectation_stack m_expect_stack;
  Notice_configuration m_notice_configuration;
  ngs::Memory_instrumented<Notice_output_queue>::Unique_ptr
      m_notice_output_queue;
  Document_id_aggregator m_document_id_aggregator;

  // Flow control.
  Galaxy_flow_control m_flow_control;

  // TODO: error control? kill flag?
};

}  // namespace xpl
