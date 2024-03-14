//
// Created by zzy on 2022/7/27.
//

#include <atomic>
#include <mutex>

#include "../global_defines.h"
#ifndef MYSQL8
#define MYSQL_SERVER
#endif
#include "m_string.h"
#include "mysql/service_command.h"
#include "sql/conn_handler/connection_handler_manager.h"
#include "sql/sql_class.h"
#include "sql/srv_session.h"

#include "../coders/buffering_command_delegate.h"
#include "../coders/command_delegate.h"
#include "../coders/custom_command_delegates.h"
#include "../coders/notices.h"
#include "../executor/executor.h"
#include "../helper/to_string.h"
#include "../polarx_rpc.h"
#include "../server/server_variables.h"
#include "../server/tcp_connection.h"
#include "../sql_query/raw_binary.h"
#include "../sql_query/sql_statement_builder.h"

#include "executor/physical_backfill.h"
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

static THD_event_functions polarx_rpc_monitor = {thd_wait_begin, thd_wait_end,
                                                 nullptr};

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

bool Csession::flush() {
  if (shutdown_.load(std::memory_order_acquire)) {
    encoder_.reset();  /// clear buffer even session was killed
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
    if (LIKELY(thd != nullptr)) thd->clear_polarx_rpc_monitor();
    if (run_) session_.detach();
    if (UNLIKELY(!session_.is_detach_and_tls_cleared()))
      sql_print_error("FATAL: polarx_rpc session not detach and cleared!");
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
            schedule_time_ = 0;  /// clear it anyway
          }
          if (msgs.empty()) break;
          while (!msgs.empty()) {
            /// exit anytime and use auto lock to keep safe
            if (shutdown_.load(std::memory_order_acquire)) {
              /// ignore all unfinished messages and free by smarter pointer
              success_enter = false;  /// no recheck needed
              break;
            } else if (killed_.load(std::memory_order_acquire)) {
              /// safe to access it because we check shutdown before
              tcp_.session_manager().remove_and_shutdown(
                  tcp_.epoll().session_count(),
                  encoder_.message_encoder().sid(), true);
              /// ignore all unfinished messages and free by smarter pointer.
              success_enter = false;  /// no recheck needed
              break;
            }

            /// do execute one msg
            auto &msg = msgs.front();
            if (PolarXRPC::ClientMessages::SESS_CLOSE == msg.type) {
              /// send ok first
              encoder_.message_encoder().encode_ok();
              flush();

              /// check and remove myself
              if (!shutdown_.load(std::memory_order_acquire)) {
                tcp_.session_manager().remove_and_shutdown(
                    tcp_.epoll().session_count(),
                    encoder_.message_encoder().sid(), false);
              }

              /// Ignore other msgs. No recheck needed.
              success_enter = false;
              break;  /// Break the message consume.
            } else {
              bool run = false;
              int64_t run_start_time = 0;
              if (enable_perf_hist) run_start_time = Ctime::steady_ns();
              dispatch(std::move(msg), run);
              if (run_start_time != 0) {
                auto run_end_time = Ctime::steady_ns();
                auto run_time = run_end_time - run_start_time;
                g_run_hist.update(static_cast<double>(run_time) / 1e9);
              }
              if (run) detacher.set_run();
            }

            /// done, pop, and next
            msgs.pop();
          }
          /// fast break
          if (shutdown_.load(std::memory_order_acquire)) {
            /// ignore all unfinished messages and free by smarter pointer
            success_enter = false;  /// no recheck needed
            break;
          }
        }
      } catch (...) {
        /// do detach on any error
        detacher.set_run();
        encoder_.message_encoder().encode_error(
            PolarXRPC::Error::FATAL, ER_POLARX_RPC_ERROR_MSG,
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
    if (pending_task) goto _retry;
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
    case ::PolarXRPC::ClientMessages::EXEC_SQL: {
      auto &m = static_cast<const PolarXRPC::Sql::StmtExecute &>(*msg.msg);

      /// check cascade error
      if (m.has_reset_error() && !m.reset_error() && last_error_)
        err = last_error_;
      else {
        run = true;
        err = sql_stmt_execute(m);
        /// note: cache miss is not an error
        if (err.error != ER_POLARX_RPC_CACHE_MISS) last_error_ = err;
      }
    } break;

    case ::PolarXRPC::ClientMessages::EXEC_PLAN_READ: {
      auto &m = static_cast<const PolarXRPC::ExecPlan::ExecPlan &>(*msg.msg);

      /// check cascade error
      if (m.has_reset_error() && !m.reset_error() && last_error_)
        err = last_error_;
      else {
        run = true;
        err = sql_plan_execute(m);
        /// note: cache miss is not an error
        if (err.error != ER_POLARX_RPC_CACHE_MISS) last_error_ = err;
      }
    } break;

    case ::PolarXRPC::ClientMessages::AUTO_SP: {
      auto &auto_sp_req =
          static_cast<const PolarXRPC::ExecPlan::AutoSp &>(*msg.msg);
      if (auto_sp_req.has_reset_error() && !auto_sp_req.reset_error() &&
          last_error_)
        err = last_error_;
      else {
        run = true;

        /// attach
        err = attach();

        if (LIKELY(!err)) {
          auto err_no = 0;
          LEX_CSTRING sp_name = {auto_sp_req.sp_name().data(),
                                 auto_sp_req.sp_name().length()};

          switch (auto_sp_req.op()) {
            case PolarXRPC::ExecPlan::AutoSp_Operation_SET:
              err_no = mysql_session_->set_savepoint(sp_name);
              break;
            case PolarXRPC::ExecPlan::AutoSp_Operation_RELEASE:
              err_no = mysql_session_->release_savepoint(sp_name);
              break;
            case PolarXRPC::ExecPlan::AutoSp_Operation_ROLLBACK:
              err_no = mysql_session_->rollback_savepoint(sp_name);
              break;
            default:
              err_no = 2;
              break;
          }

          if (err_no) {
            err =
                err_t(ER_POLARX_RPC_ERROR_MSG, "Handle auto savepoint failed.");
          } else {
            encoder_.message_encoder().encode_ok();
            flush();
          }
        }

        last_error_ = err;
      }
    } break;

    case ::PolarXRPC::ClientMessages::FILE_OPERATION_GET_FILE_INFO: {
      // thd io flag
      auto &file_info_msg =
          static_cast<const PolarXRPC::PhysicalBackfill::GetFileInfoOperator &>(
              *msg.msg);
      err = table_space_file_info(file_info_msg);
      if (!err) {
        flush();
      }
    } break;

    case ::PolarXRPC::ClientMessages::FILE_OPERATION_TRANSFER_FILE_DATA: {
      auto &transfer_info_msg = static_cast<
          const PolarXRPC::PhysicalBackfill::TransferFileDataOperator &>(
          *msg.msg);
      err = table_space_file_transfer(transfer_info_msg);
      if (!err) {
        flush();
      }
    } break;

    case ::PolarXRPC::ClientMessages::FILE_OPERATION_FILE_MANAGE: {
      auto &file_info_msg =
          static_cast<const PolarXRPC::PhysicalBackfill::FileManageOperator &>(
              *msg.msg);
      err = table_space_file_manage(file_info_msg, run);
      if (!err) {
        flush();
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

err_t Csession::sql_stmt_execute(const PolarXRPC::Sql::StmtExecute &msg) {
  /// check exit first
  if (plugin_info.exit.load(std::memory_order_acquire))
    return err_t(ER_SERVER_SHUTDOWN, "server shutdown", "HY000", err_t::FATAL);

  /// attach before any mysql internal operations
  auto err = attach();
  if (UNLIKELY(err)) return err;  /// success or fatal error

  /// apply GCN first to prevent any error
  auto thd = get_thd();
#ifdef MYSQL8
  /// lizard specific GCN timestamp
  if (!thd->in_active_multi_stmt_transaction()) thd->reset_gcn_variables();
  if (msg.has_use_cts_transaction() && msg.use_cts_transaction())
    thd->variables.innodb_current_snapshot_gcn = true;
  if (msg.has_snapshot_seq()) {
    thd->variables.innodb_snapshot_gcn = msg.snapshot_seq();
    thd->owned_vision_gcn.set(
        MYSQL_CSR_ASSIGNED, thd->variables.innodb_snapshot_gcn, MYSQL_SCN_NULL);
  }
  if (msg.has_commit_seq()) {
    thd->variables.innodb_commit_gcn = msg.commit_seq();
    thd->owned_commit_gcn.set(thd->variables.innodb_commit_gcn,
                              MYSQL_CSR_ASSIGNED);
  }
#else
  /// 5.7 specific CTS timestamp
  if (!thd_in_active_multi_stmt_transaction(thd)) thd->clear_transaction_seq();
  if (msg.has_use_cts_transaction() && msg.use_cts_transaction())
    thd->mark_cts_transaction();
  if (msg.has_mark_distributed() && msg.mark_distributed())
    thd->mark_distributed();
  if (msg.has_snapshot_seq()) thd->set_snapshot_seq(msg.snapshot_seq());
  if (msg.has_commit_seq()) thd->set_commit_seq(msg.commit_seq());
#endif

  /// switch DB
  if (msg.has_schema_name()) {
    const auto &db_name = msg.schema_name();
    err = init_db(db_name.data(), db_name.length());
    if (err) return err;  /// fatal error
  }

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
    stmt_ptr = &msg.stmt();  /// use original in msg
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

  /// token reset
  auto flow_control_enabled = false;
  if (msg.has_token()) {
    /// for size flow control, token can be 0
    if (msg.token() >= -1) {
      if (msg.token() >= 0) {
        auto flow_size = msg.token() * 1024;  /// unit is KB
        if (flow_size < 0 || flow_size < msg.token())
          flow_size = INT32_MAX;  /// set to max when overflow
        flow_control_.flow_reset(flow_size);
        flow_control_enabled = true;
      }
    } else
      return err_t(ER_POLARX_RPC_ERROR_MSG, "Invalid Token");
  }

  /// build query
  const auto charset = thd->variables.character_set_results != nullptr
                           ? thd->variables.character_set_results
                           : &my_charset_utf8mb4_general_ci;
  qb_.clear();
  Sql_statement_builder builder(&qb_);
  try {
    if (msg.has_hint())
      builder.build(*stmt_ptr, msg.args(), *charset, msg.hint());
    else
      builder.build(*stmt_ptr, msg.args(), *charset);
  } catch (const err_t &e) {
    return e;
  }

  const auto caps =
      msg.has_capabilities() ? msg.capabilities() : DEFAULT_CAPABILITIES;
  CstmtCommandDelegate delegate(
      *this, encoder_, [this]() { return flush(); }, msg.compact_metadata(),
      caps);
  if (flow_control_enabled)
    delegate.set_flow_control(&flow_control_);  /// enable flow control
  /// chunk?
  if (msg.has_chunk_result()) delegate.set_chunk_result(msg.chunk_result());
  /// feedback?
  if (msg.has_feed_back()) delegate.set_feedback(msg.feed_back());

  // sql_print_information("sid %lu run %s", sid_, qb_.get().c_str());
  err = execute_sql(qb_.get().data(), qb_.get().length(), delegate);
  // sql_print_information("sid %lu run %s done err %d sqlerr %d", sid_,
  // qb_.get().c_str(), err.error, sql_err.error);
  if (err) {
    /// send warning
    send_warnings(*this, encoder_, true);
    flush();
  }
  return err;
}

std::string plan_param_audit_str_builder(
    THD *thd,
    const ::google::protobuf::RepeatedPtrField<::PolarXRPC::Datatypes::Scalar>
        &params) {
  thread_local std::ostringstream oss;
  oss.str("");  /// clear it

  std::vector<std::string> tb_db_names;

  for (auto it = params.begin(); it != params.end(); ++it) {
    if (it->type() != PolarXRPC::Datatypes::Scalar_Type_V_IDENTIFIER)
      oss << ", ";

    switch (it->type()) {
      case PolarXRPC::Datatypes::Scalar_Type_V_SINT:
        oss << it->v_signed_int();
        break;

      case PolarXRPC::Datatypes::Scalar_Type_V_UINT:
        oss << it->v_unsigned_int();
        break;

      case PolarXRPC::Datatypes::Scalar_Type_V_NULL:
        oss << "null";
        break;

      case PolarXRPC::Datatypes::Scalar_Type_V_OCTETS: {
        RawBinary binary(it->v_octets().value());
        oss << binary.to_hex_string();
      } break;

      case PolarXRPC::Datatypes::Scalar_Type_V_DOUBLE:
        oss << std::setprecision(std::numeric_limits<double>::max_digits10)
            << it->v_double();
        break;

      case PolarXRPC::Datatypes::Scalar_Type_V_FLOAT:
        oss << std::setprecision(std::numeric_limits<float>::max_digits10)
            << it->v_float();
        break;

      case PolarXRPC::Datatypes::Scalar_Type_V_BOOL:
        oss << (it->v_bool() ? "true" : "false");
        break;

      case PolarXRPC::Datatypes::Scalar_Type_V_STRING: {
        const CHARSET_INFO *thd_charset = thd->variables.character_set_client;

        const auto &str = it->v_string().value();
        const std::size_t length_maximum = 2 * str.length() + 1 + 2;
        std::string value_escaped(length_maximum, '\0');

        std::size_t length_escaped = escape_string_for_mysql(
            nullptr == thd_charset ? &my_charset_utf8mb4_general_ci
                                   : thd_charset,
            &value_escaped[1], length_maximum, str.data(), str.length());
        value_escaped[0] = value_escaped[1 + length_escaped] = '\'';

        value_escaped.resize(length_escaped + 2);

        oss << value_escaped;
      } break;

      case PolarXRPC::Datatypes::Scalar_Type_V_PLACEHOLDER:
        oss << "\'placeholder:" << it->v_position() << '\'';
        break;

      case PolarXRPC::Datatypes::Scalar_Type_V_IDENTIFIER: {
        const CHARSET_INFO *thd_charset = thd->variables.character_set_client;

        const auto &str = it->has_v_identifier() ? it->v_identifier().value()
                                                 : it->v_string().value();
        const std::size_t length_maximum = 2 * str.length() + 1 + 2;
        std::string value_escaped(length_maximum, '\0');

        std::size_t length_escaped = escape_string_for_mysql(
            nullptr == thd_charset ? &my_charset_utf8mb4_general_ci
                                   : thd_charset,
            &value_escaped[1], length_maximum, str.data(), str.length());
        value_escaped[0] = value_escaped[1 + length_escaped] = '`';

        value_escaped.resize(length_escaped + 2);

        tb_db_names.emplace_back(value_escaped);
      } break;

      case PolarXRPC::Datatypes::Scalar_Type_V_RAW_STRING:
        oss << it->v_string().value();
        break;
    }
  }

  /// generate from str
  if (tb_db_names.size() >= 2) oss << " from ";
  auto first = true;
  for (size_t idx = 0; idx + 1 < tb_db_names.size(); idx += 2) {
    if (first)
      first = false;
    else
      oss << ", ";
    oss << tb_db_names[idx + 1] << '.' << tb_db_names[idx];
  }
  return oss.str();
}

err_t Csession::sql_plan_execute(const PolarXRPC::ExecPlan::ExecPlan &msg) {
  /// check exit first
  if (plugin_info.exit.load(std::memory_order_acquire))
    return err_t(ER_SERVER_SHUTDOWN, "server shutdown", "HY000", err_t::FATAL);

  /// attach before any mysql internal operations
  auto err = attach();
  if (UNLIKELY(err)) return err;

  /// apply GCN first to prevent any error
  auto thd = get_thd();
#ifdef MYSQL8
  /// lizard specific GCN timestamp
  if (!thd->in_active_multi_stmt_transaction()) thd->reset_gcn_variables();
  if (msg.has_use_cts_transaction() && msg.use_cts_transaction())
    thd->variables.innodb_current_snapshot_gcn = true;
  if (msg.has_snapshot_seq()) {
    thd->variables.innodb_snapshot_gcn = msg.snapshot_seq();
    thd->owned_vision_gcn.set(
        MYSQL_CSR_ASSIGNED, thd->variables.innodb_snapshot_gcn, MYSQL_SCN_NULL);
  }
  if (msg.has_commit_seq()) {
    thd->variables.innodb_commit_gcn = msg.commit_seq();
    thd->owned_commit_gcn.set(thd->variables.innodb_commit_gcn,
                              MYSQL_CSR_ASSIGNED);
  }
#else
  /// 5.7 specific CTS timestamp
  if (!thd_in_active_multi_stmt_transaction(thd)) thd->clear_transaction_seq();
  if (msg.has_use_cts_transaction() && msg.use_cts_transaction())
    thd->mark_cts_transaction();
  if (msg.has_mark_distributed() && msg.mark_distributed())
    thd->mark_distributed();
  if (msg.has_snapshot_seq()) thd->set_snapshot_seq(msg.snapshot_seq());
  if (msg.has_commit_seq()) thd->set_commit_seq(msg.commit_seq());
#endif

  ///
  /// dealing cache
  ///
  /// just for life cycle control, never touch it
  CrequestCache::plan_store_t plan_store;
  /// use this to get real plan
  const PolarXRPC::ExecPlan::AnyPlan *plan_ptr = nullptr;
  /// plan audit str
  std::string audit_str;
  if (msg.has_plan() && msg.has_plan_digest()) {
    /// copy, move and put into LRU
    auto digest(msg.plan_digest());
    audit_str = msg.audit_str();       /// copy one
    plan_store.audit_str = audit_str;  /// copy one
    plan_store.plan =
        std::make_shared<PolarXRPC::ExecPlan::AnyPlan>(msg.plan());
    // TODO check plan store size?
    plugin_info.cache->set_plan(std::move(digest), std::move(plan_store));
    plan_ptr = &msg.plan();  /// use original in msg
  } else if (msg.has_plan_digest()) {
    plan_store = std::move(plugin_info.cache->get_plan(msg.plan_digest()));
    if (!plan_store.plan)
      /// not found?
      return err_t(ER_POLARX_RPC_CACHE_MISS, "do not exist plan!");
    plan_ptr = plan_store.plan.get();
    audit_str = std::move(plan_store.audit_str);
  } else if (msg.has_plan()) {
    plan_ptr = &msg.plan();
    audit_str = msg.audit_str();
  } else
    return err_t(ER_POLARX_RPC_ERROR_MSG, "Neither stmt nor digest exist.");

  /// token reset
  auto flow_control_enabled = false;
  if (msg.has_token()) {
    /// for size flow control, token can be 0
    if (msg.token() >= -1) {
      if (msg.token() >= 0) {
        auto flow_size = msg.token() * 1024;  /// unit is KB
        if (flow_size < 0 || flow_size < msg.token())
          flow_size = INT32_MAX;  /// set to max when overflow
        flow_control_.flow_reset(flow_size);
        flow_control_enabled = true;
      }
    } else
      return err_t(ER_POLARX_RPC_ERROR_MSG, "Invalid Token");
  }

  const auto caps =
      msg.has_capabilities() ? msg.capabilities() : DEFAULT_CAPABILITIES;
  CstmtCommandDelegate delegate(
      *this, encoder_, [this]() { return flush(); }, msg.compact_metadata(),
      caps);
  if (flow_control_enabled)
    delegate.set_flow_control(&flow_control_);  /// enable flow control
  /// chunk?
  if (msg.has_chunk_result()) delegate.set_chunk_result(msg.chunk_result());
  /// feedback?
  if (msg.has_feed_back()) delegate.set_feedback(msg.feed_back());

  /// build audit string
  std::string full_audit;
  full_audit.reserve(1024);
  if (msg.has_trace_id()) {
    full_audit += msg.trace_id();
    full_audit += '\n';
  }
  if (msg.has_plan_digest()) {
    RawBinary bin(msg.plan_digest());
    full_audit += "/*xplan ";
    full_audit += bin.to_hex_string();
    full_audit += '\n';
  } else
    full_audit += "/*xplan\n";
  full_audit += audit_str;
  full_audit += "*/\nselect xplan";
  full_audit += plan_param_audit_str_builder(thd, msg.parameters());

  auto iret = rpc_executor::Executor::instance().execute(
      *plan_ptr, msg.parameters(), delegate, thd, full_audit.c_str(),
      full_audit.length());
  /// detach via auto detacher in session::run
  if (iret != 0) return err_t(ER_POLARX_RPC_ERROR_MSG, "Executor error");
  return err_t::Success();
}

err_t Csession::table_space_file_info(
    const PolarXRPC::PhysicalBackfill::GetFileInfoOperator &msg) {
  err_t ret = err_t::Success();
  PolarXRPC::PhysicalBackfill::GetFileInfoOperator out_msg;
  switch (msg.operator_type()) {
    case PolarXRPC::PhysicalBackfill::GetFileInfoOperator::
        CHECK_SRC_FILE_EXISTENCE:
    case PolarXRPC::PhysicalBackfill::GetFileInfoOperator::
        CHECK_TAR_FILE_EXISTENCE: {
      out_msg.set_operator_type(
          PolarXRPC::PhysicalBackfill::
              GetFileInfoOperator_Type_CHECK_SRC_FILE_EXISTENCE);
      ret = rpc_executor::Physical_backfill::instance().check_file_existence(
          msg, out_msg);
    }; break;
    default:
      return err_t(ER_POLARX_RPC_ERROR_MSG, "unsupported command");
  }
  if (ret) {
    return ret;
  } else {
    encoder_.message_encoder()
        .encode_protobuf_message<protocol::tags::GetFileInfoOK::server_id>(
            out_msg);
    return err_t::Success();
  }
}

err_t Csession::table_space_file_manage(
    const PolarXRPC::PhysicalBackfill::FileManageOperator &msg, bool &run) {
  err_t ret = err_t::Success();
  switch (msg.operator_type()) {
    case PolarXRPC::PhysicalBackfill::FileManageOperator::
        COPY_IBD_TO_TEMP_DIR_IN_SRC: {
      auto thd = get_thd();

      run = true;
      /// attach first
      auto err = attach();
      if (UNLIKELY(err)) return err;

      ret = rpc_executor::Physical_backfill::instance().clone_file(msg, thd);
    } break;
    case PolarXRPC::PhysicalBackfill::FileManageOperator::
        DELETE_IBD_FROM_TEMP_DIR_IN_SRC: {
      ret = rpc_executor::Physical_backfill::instance().delete_file(msg);
    } break;
    case PolarXRPC::PhysicalBackfill::FileManageOperator::FALLOCATE_IBD: {
      ret = rpc_executor::Physical_backfill::instance().pre_allocate(msg);
    } break;
    default:
      return err_t(ER_POLARX_RPC_ERROR_MSG, "unsupported command");
  }
  if (!ret) {
    PolarXRPC::PhysicalBackfill::FileManageOperatorResponse out_msg;
    out_msg.set_result(true);
    encoder_.message_encoder()
        .encode_protobuf_message<protocol::tags::FileManageOK::server_id>(
            out_msg);
    return err_t::Success();
  } else {
    return ret;
  }
}

err_t Csession::table_space_file_transfer(
    const PolarXRPC::PhysicalBackfill::TransferFileDataOperator &msg) {
  err_t ret = err_t::Success();
  PolarXRPC::PhysicalBackfill::TransferFileDataOperator out_msg;
  switch (msg.operator_type()) {
    case PolarXRPC::PhysicalBackfill::TransferFileDataOperator::
        GET_DATA_FROM_SRC_IBD: {
      out_msg.set_operator_type(
          PolarXRPC::PhysicalBackfill::TransferFileDataOperator::
              GET_DATA_FROM_SRC_IBD);
      ret =
          rpc_executor::Physical_backfill::instance().read_buffer(msg, out_msg);
    } break;
    case PolarXRPC::PhysicalBackfill::TransferFileDataOperator::
        PUT_DATA_TO_TAR_IBD: {
      out_msg.set_operator_type(
          PolarXRPC::PhysicalBackfill::TransferFileDataOperator::
              PUT_DATA_TO_TAR_IBD);
      ret = rpc_executor::Physical_backfill::instance().write_buffer(msg,
                                                                     out_msg);
    } break;
  }
  if (!ret) {
    encoder_.message_encoder()
        .encode_protobuf_message<protocol::tags::TransferFileDataOK::server_id>(
            out_msg);
    return err_t::Success();
  } else {
    return ret;
  }
}

}  // namespace polarx_rpc
