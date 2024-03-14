//
// Created by zzy on 2022/8/31.
//

#include "../global_defines.h"
#ifndef MYSQL8
#define MYSQL_SERVER
#endif
#include "mysql/psi/mysql_thread.h"
#ifdef MYSQL8
#include "my_psi_config.h"
#else
#include "mysql/psi/psi.h"
#endif
#include "mysql/service_command.h"
#include "mysql/service_srv_session.h"
#include "mysql/service_ssl_wrapper.h"
#include "sql/mysqld.h"
#include "sql/mysqld_thd_manager.h"
#include "sql/sql_class.h"
#include "sql/sql_db.h"
#include "sql/sql_lex.h"
#include "sql/srv_session.h"

#include "../coders/callback_command_delegate.h"
#include "../polarx_rpc.h"
#include "../server/server_variables.h"

#include "session_base.h"

namespace polarx_rpc {

CsessionBase::~CsessionBase() {
  if (mysql_session_ != nullptr) {
    /// de-init pfs
    const auto thd = get_thd();
    const auto psi = thd->get_psi();
#ifdef HAVE_PSI_THREAD_INTERFACE
    if (psi != nullptr) {
      PSI_THREAD_CALL(delete_thread)(psi);
      thd->set_psi(nullptr);
    }
#endif

    srv_session_close(mysql_session_);
    mysql_session_ = nullptr;
  }
  plugin_info.total_sessions.fetch_sub(1, std::memory_order_release);
}

err_t CsessionBase::init(uint16_t port) {
  if (mysql_session_ != nullptr)
    return err_t(ER_POLARX_RPC_ERROR_MSG, "Session inited");

  mysql_session_ =
      srv_session_open(&CsessionBase::default_completion_handler, this);
  if (nullptr == mysql_session_) {
    if (ER_SERVER_ISNT_AVAILABLE == last_sql_errno_)
      return err_t(ER_SERVER_ISNT_AVAILABLE, "Server API not ready");
    return {last_sql_errno_ != 0 ? static_cast<int>(last_sql_errno_)
                                 : ER_POLARX_RPC_ERROR_MSG,
            "Could not open session"};
  }

  if (srv_session_info_set_connection_type(mysql_session_, VIO_TYPE_TCPIP) != 0)
    return err_t(ER_POLARX_RPC_ERROR_MSG,
                 "Could not set session connection type");
  if (0 != srv_session_info_set_client_port(mysql_session_, port))
    return err_t(ER_POLARX_RPC_ERROR_MSG, "Could not set session client port");

  mysql_session_->set_safe(true);
  const auto thd = get_thd();
  flow_control_.set_thd(thd);

  /// init pfs with fake one thread per session
  assert(nullptr == thd->get_psi());
#ifdef HAVE_PSI_THREAD_INTERFACE
#ifdef MYSQL8PLUS
  const auto psi = PSI_THREAD_CALL(new_thread)(key_thread_one_connection, 0,
                                               thd, thd->thread_id());
#else
  const auto psi = PSI_THREAD_CALL(new_thread)(key_thread_one_connection, thd,
                                               thd->thread_id());
#endif
  thd->set_psi(psi);
  PSI_THREAD_CALL(set_thread_id)(psi, thd->thread_id());
  PSI_THREAD_CALL(set_thread_THD)(psi, thd);
  /// default attach to it
  PSI_THREAD_CALL(set_thread)(psi);
#endif
  return err_t::Success();
}

void CsessionBase::default_completion_handler(void *ctx, unsigned int sql_errno,
                                              const char *err_msg) {
  auto self = reinterpret_cast<CsessionBase *>(ctx);
  self->last_sql_errno_ = sql_errno;
  self->last_sql_error_ = err_msg ? err_msg : "";
}

err_t CsessionBase::switch_to_user(const char *username, const char *hostname,
                                   const char *address, const char *db) {
  MYSQL_SECURITY_CONTEXT scontext;

  if (thd_get_security_context(get_thd(), &scontext) != 0)
    return err_t(ER_ACCESS_DENIED_ERROR,
                 "Error getting security context for session");

  // security_context_lookup
  //   doesn't make a copy of username, hostname, address or db thus we need to
  //   make a copy of them and pass our pointers to security_context_lookup
  username_ = username ? username : "";
  hostname_ = hostname ? hostname : "";
  address_ = address ? address : "";
  db_ = db ? db : "";

  if (security_context_lookup(scontext, username_.c_str(), hostname_.c_str(),
                              address_.c_str(), db_.c_str()) != 0)
    return err_t::Error(ER_ACCESS_DENIED_ERROR,
                        "Unable to switch context to user %s", username);
  return err_t::Success();
}

void CsessionBase::set_show_hostname(const char *hostname) {
  show_hostname_ = hostname ? hostname : "";
  auto thd = get_thd();
  if (thd != nullptr) {
    auto sec = thd->security_context();
    if (sec != nullptr)
      sec->set_host_or_ip_ptr(show_hostname_.c_str(), show_hostname_.length());
  }
}

err_t CsessionBase::reset() {
  COM_DATA data;
  CcallbackCommandDelegate delegate;
  auto err = execute_server_command(COM_RESET_CONNECTION, data, delegate);
  return err ? err : delegate.get_error();
}

err_t CsessionBase::init_db(const char *db_name, std::size_t db_len) {
  auto thd = get_thd();
  if (nullptr == thd)
    return err_t(ER_QUERY_INTERRUPTED, "THD was not initialized", "70100",
                 err_t::FATAL);

  LEX_STRING tmp;
  thd->status_var.com_stat[SQLCOM_CHANGE_DB]++;
  thd->convert_string(&tmp, system_charset_info, db_name, db_len,
                      &my_charset_utf8mb4_general_ci);

  LEX_CSTRING cstr{tmp.str, tmp.length};
  if (0 == mysql_change_db(thd, cstr, false))
    return err_t::Success();
  else if (thd->get_stmt_da()->is_error()) {
    auto *stmt_da = thd->get_stmt_da();
    return err_t(static_cast<int>(stmt_da->mysql_errno()),
                 stmt_da->message_text(), stmt_da->returned_sqlstate(),
                 err_t::FATAL);
  } else
    return err_t(ER_UNKNOWN_ERROR, "USE db failed", "HY000", err_t::FATAL);
}

template <typename Result_type>
static bool get_security_context_value(MYSQL_THD thd, const char *option,
                                       Result_type *result) {
  MYSQL_SECURITY_CONTEXT scontext;

  if (thd_get_security_context(thd, &scontext)) return false;

  return false == security_context_get_option(scontext, option, result);
}

bool CsessionBase::is_acl_disabled() const {
  MYSQL_LEX_CSTRING value{"", 0};
  if (get_security_context_value(get_thd(), "priv_user", &value)) {
    return 0 != value.length && NULL != strstr(value.str, "skip-grants ");
  }
  return false;
}

bool CsessionBase::has_authenticated_user_a_super_priv() const {
  my_svc_bool value = 0;
  if (get_security_context_value(get_thd(), "privilege_super", &value))
    return value != 0;

  return false;
}

std::string CsessionBase::get_user_name() const {
  MYSQL_LEX_CSTRING result{"", 0};

  if (get_security_context_value(get_thd(), "user", &result)) return result.str;

  return "";
}

std::string CsessionBase::get_host_or_ip() const {
  MYSQL_LEX_CSTRING result{"", 0};

  if (get_security_context_value(get_thd(), "host_or_ip", &result))
    return result.str;

  return "";
}

std::string CsessionBase::get_authenticated_user_name() const {
  MYSQL_LEX_CSTRING result{"", 0};

  if (get_security_context_value(get_thd(), "priv_user", &result))
    return result.str;

  return "";
}

std::string CsessionBase::get_authenticated_user_host() const {
  MYSQL_LEX_CSTRING result{"", 0};

  if (get_security_context_value(get_thd(), "priv_host", &result))
    return result.str;

  return "";
}

/// hack code for no accounts for polarx
void CsessionBase::switch_to_sys_user() {
  username_ = "polarxsys";
  hostname_ = "127.0.0.1";

  MYSQL_SECURITY_CONTEXT scontext;
  thd_get_security_context(get_thd(), &scontext);

  scontext->set_user_ptr(username_.data(),
                         static_cast<int>(username_.length()));
  scontext->set_host_ptr(hostname_.data(),
                         static_cast<int>(hostname_.length()));
  scontext->set_ip_ptr(nullptr, 0);
  scontext->set_host_or_ip_ptr();

  scontext->set_master_access(0x1FFFFFFF);
#ifdef MYSQL8
  scontext->cache_current_db_access(0);
#else
  scontext->set_db_access(0);
#endif

  scontext->assign_priv_user(username_.data(),
                             static_cast<int>(username_.length()));
  scontext->assign_priv_host(hostname_.data(),
                             static_cast<int>(hostname_.length()));
  scontext->set_password_expired(false);
}

err_t CsessionBase::authenticate(const char *user, const char *host,
                                 const char *ip, const char *db,
                                 const std::string &passwd,
                                 Authentication_interface &account_verification,
                                 bool allow_expired_passwords) {
  auto error = switch_to_user(user, host, ip, db);
  if (error) return err_t::SQLError_access_denied();

  if (!is_acl_disabled()) {
    auto authenticated_user_name = get_authenticated_user_name();
    auto authenticated_user_host = get_authenticated_user_host();

    /// force switch to fake sys user
    switch_to_sys_user();

    if (!is_acl_disabled()) {
      error = account_verification.authenticate_account(
          authenticated_user_name, authenticated_user_host, passwd);
      if (error) return error;
    }

    /// switch to user
    error = switch_to_user(user, host, ip, db);
  }

  /// check db
  if (!error) {
    if (db && *db) {
      if (init_db(db, ::strlen(db)))
        return err_t(ER_NO_DB_ERROR, "Could not set database");
    }

    std::string user_name = get_user_name();
    std::string host_or_ip = get_host_or_ip();

    /*
      Instead of modifying the current security context in switch_user()
      method above, we must create a security_context to do the
      security_context_lookup() on newly created security_context then set
      that in the THD. Until that happens, we have to get the existing security
      context and set that again in the THD. The latter opertion is nedded as
      it may toggle the system_user flag in THD iff security_context has
      SYSTEM_USER privilege.
    */
    MYSQL_SECURITY_CONTEXT scontext;
    thd_get_security_context(get_thd(), &scontext);
    thd_set_security_context(get_thd(), scontext);

#ifdef HAVE_PSI_THREAD_INTERFACE
    PSI_THREAD_CALL(set_thread_account)
    (user_name.c_str(), static_cast<int>(user_name.length()),
     host_or_ip.c_str(), static_cast<int>(host_or_ip.length()));
#endif  // HAVE_PSI_THREAD_INTERFACE

    return err_t::Success();
  }
  return error;
}

err_t CsessionBase::execute_server_command(enum_server_command cmd,
                                           const COM_DATA &cmd_data,
                                           CcommandDelegate &delegate) {
  delegate.reset();
  if (command_service_run_command(mysql_session_, cmd, &cmd_data, nullptr,
                                  CcommandDelegate::callbacks(),
                                  delegate.representation(), &delegate) != 0) {
    /// if no error spec
    if (!delegate.get_error()) {
      /// check killed
#ifdef MYSQL8
      auto killed = get_thd()->killed.load(std::memory_order_acquire);
#else
      auto killed = get_thd()->killed;
#endif
      if (THD::KILL_CONNECTION == killed)
        return err_t(ER_QUERY_INTERRUPTED, "Query execution was interrupted",
                     "70100", err_t::FATAL);
      else if (THD::KILL_QUERY == killed)
        return err_t(ER_QUERY_INTERRUPTED, "Query execution was interrupted",
                     "70100");
      else
        return err_t(ER_POLARX_RPC_ERROR_MSG,
                     "Internal error executing command");
    } else if (THD::KILL_CONNECTION == get_thd()->killed) {
      auto err = delegate.get_error();
      return err_t(err.error, err.message, err.sql_state, err_t::FATAL);
    }
  }
  auto err = delegate.get_error();
  if (UNLIKELY(err && THD::KILL_CONNECTION == get_thd()->killed))
    return err_t(err.error, err.message, err.sql_state, err_t::FATAL);
  return err;
}

err_t CsessionBase::execute_sql(const char *sql, size_t sql_len,
                                CcommandDelegate &delegate) {
  COM_DATA data;
  data.com_query.query = sql;
  data.com_query.length = static_cast<unsigned int>(sql_len);
#ifndef MYSQL8
  data.com_query.version = 0;  /// only for MySQL57
#elif defined(MYSQL8PLUS)
  data.com_query.parameters = nullptr;
  data.com_query.parameter_count = 0;
#endif
  return execute_server_command(COM_QUERY, data, delegate);
}

err_t CsessionBase::attach() {
  if (nullptr == mysql_session_ ||
      srv_session_attach(mysql_session_, nullptr) != 0)
    return err_t::Fatal(ER_POLARX_RPC_ERROR_MSG,
                        "Internal error when attaching");

#ifdef HAVE_PSI_THREAD_INTERFACE
  /// set psi after successfully attach
  PSI_THREAD_CALL(set_thread)(get_thd()->get_psi());
#endif
  return err_t::Success();
}

err_t CsessionBase::detach() {
#ifdef HAVE_PSI_THREAD_INTERFACE
  /// detach psi before reset psi in srv_session_detach
  PSI_THREAD_CALL(set_thread)(nullptr);
#endif

  /// now detach
  auto err =
      nullptr == mysql_session_ || srv_session_detach(mysql_session_) != 0;
  /// Note: we should force clear thd, or stale thd may cause bad memory access
#ifdef MYSQL8
  current_thd = nullptr;
  THR_MALLOC = nullptr;
#else
  my_thread_set_THR_THD(nullptr);
  my_thread_set_THR_MALLOC(nullptr);
#endif
  return err ? err_t(ER_POLARX_RPC_ERROR_MSG, "Internal error when detaching")
             : err_t::Success();
}

void CsessionBase::remote_kill(bool log) {
  if (enable_kill_log && log) {
    std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
    if (plugin_info.plugin_info != nullptr)
      my_plugin_log_message(&plugin_info.plugin_info, MY_INFORMATION_LEVEL,
                            "Session %p sid %lu killing.", this, sid_);
  }

  /// store exit flag first
  killed_.store(true, std::memory_order_release);
  /// shutdown flow control
  flow_control_.exit();

  /// do kill
  auto thd = get_thd();
  if (thd != nullptr) {
    mysql_mutex_lock(&thd->LOCK_thd_data);
    // Lock held and awake with kill.
    if (thd->killed != THD::KILL_CONNECTION) thd->awake(THD::KILL_CONNECTION);
    mysql_mutex_unlock(&thd->LOCK_thd_data);
  }
}

void CsessionBase::remote_cancel() {
  if (enable_kill_log) {
    std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
    if (plugin_info.plugin_info != nullptr)
      my_plugin_log_message(&plugin_info.plugin_info, MY_INFORMATION_LEVEL,
                            "Session %p sid %lu canceling.", this, sid_);
  }

  auto thd = get_thd();
  if (thd != nullptr) {
    mysql_mutex_lock(&thd->LOCK_thd_data);
    // Lock held and awake with kill.
    if (thd->killed != THD::KILL_CONNECTION && thd->killed != THD::KILL_QUERY)
      thd->awake(THD::KILL_QUERY);
    mysql_mutex_unlock(&thd->LOCK_thd_data);
  }
}

bool CsessionBase::is_api_ready() {
#ifdef MYSQL8
  return 0 != srv_session_server_is_available() &&
         !connection_events_loop_aborted();
#else
  return 0 != srv_session_server_is_available() && !::abort_loop;
#endif
}

bool CsessionBase::is_detach_and_tls_cleared() {
  if (mysql_session_ != nullptr && mysql_session_->is_attached()) return false;
#ifdef MYSQL8
  return nullptr == current_thd && nullptr == THR_MALLOC;
#else
  return nullptr == my_thread_get_THR_THD() &&
         nullptr == my_thread_get_THR_MALLOC();
#endif
}

bool CsessionBase::is_thd_connection_killed() {
  auto thd = get_thd();
  return thd != nullptr && THD::KILL_CONNECTION == thd->killed;
}

static PSI_thread_key KEY_thread_polarx_rpc = PSI_NOT_INSTRUMENTED;

static PSI_thread_info all_threads[] = {
#ifdef MYSQL8PLUS
    {&KEY_thread_polarx_rpc, "polarx_rpc", "xrpc_worker",
     PSI_FLAG_USER | PSI_FLAG_AUTO_SEQNUM, 0, PSI_DOCUMENT_ME},
#else
    {&KEY_thread_polarx_rpc, "polarx_rpc", 0},
#endif
};

static void init_performance_schema() {
  const char *const category = "polarx_rpc";
  mysql_thread_register(category, all_threads, array_elements(all_threads));
}

static std::once_flag pfs_once;

static void init_session_once() {
  std::call_once(pfs_once, init_performance_schema);
}

void CsessionBase::create_session_thread(void *(*func)(void *), void *arg) {
  init_session_once();

  my_thread_attr_t connection_attrib;
  (void)my_thread_attr_init(&connection_attrib);

  /*
   check_stack_overrun() assumes that stack size is (at least)
   my_thread_stack_size. If it is smaller, we may segfault.
  */
  my_thread_attr_setstacksize(&connection_attrib, my_thread_stack_size);

  my_thread_handle thread;
  if (mysql_thread_create(KEY_thread_polarx_rpc, &thread, &connection_attrib,
                          func, arg))
    throw std::runtime_error("Could not create a thread");
}

void CsessionBase::init_thread_for_session() {
  {
    std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
    if (plugin_info.plugin_info != nullptr)
      srv_session_init_thread(plugin_info.plugin_info);
  }
#if defined(__APPLE__)
  pthread_setname_np("polarx_rpc");
#elif defined(HAVE_PTHREAD_SETNAME_NP)
  pthread_setname_np(pthread_self(), "polarx_rpc");
#endif
}

void CsessionBase::deinit_thread_for_session() {
  /// de-init pfs if exists
#ifdef HAVE_PSI_THREAD_INTERFACE
  PSI_THREAD_CALL(delete_current_thread)();
#endif

  ssl_wrapper_thread_cleanup();
  srv_session_deinit_thread();
}

void CsessionBase::begin_query(THD *thd, const char *query, uint len) {
  /// mark start query
  THD_STAGE_INFO(thd, stage_starting);
  thd->set_time();
  thd->lex->sql_command = SQLCOM_SELECT;

  /// clear rewrite query and set current query
#ifdef MYSQL8PLUS
  if (thd->rewritten_query().length()) {
    String empty;
    thd->swap_rewritten_query(empty);
  }
#else
  if (thd->rewritten_query.length()) thd->rewritten_query.mem_free();
#endif
  thd->set_query(query, len);
#ifdef MYSQL8PLUS
  thd->set_query_for_display(thd->query().str, thd->query().length);
#endif

  /// add global running
  Global_THD_manager::get_instance()->inc_thread_running();
}

void CsessionBase::cleanup_and_mark_sleep(THD *thd) {
  thd->reset_query();
  thd->set_command(COM_SLEEP);
#ifdef MYSQL8PLUS
  thd->set_proc_info(nullptr);
#else
  thd->proc_info = nullptr;
#endif
  thd->lex->sql_command = SQLCOM_END;

#ifdef MYSQL8PLUS
  thd->reset_query_for_display();
  if (thd->rewritten_query().length()) {
    String empty;
    thd->swap_rewritten_query(empty);
  }
#else
  if (thd->rewritten_query.length()) thd->rewritten_query.mem_free();
#endif

  /** Freeing the memroot will leave the THD::work_part_info invalid. */
  thd->work_part_info = nullptr;

  /// free mem_root
#ifdef MYSQL8
  /**
    If we've allocated a lot of memory (compared to the default preallocation
    size = 8192; note that we don't actually preallocate anymore), free
    it so that one big query won't cause us to hold on to a lot of RAM forever.
    If not, keep the last block so that the next query will hopefully be able to
    run without allocating memory from the OS.

    The factor 5 is pretty much arbitrary, but ends up allowing three
    allocations (1 + 1.5 + 1.5²) under the current allocation policy.
  */
  constexpr size_t kPreallocSz = 40960;
  if (thd->mem_root->allocated_size() < kPreallocSz)
    thd->mem_root->ClearForReuse();
  else
    thd->mem_root->Clear();
#else
  free_root(thd->mem_root, MYF(MY_KEEP_PREALLOC));
  thd->lex->query_tables = nullptr;
#endif
}

void CsessionBase::end_query(THD *thd) {
  /// record stat
  ++thd->status_var.questions;
  ++thd->status_var.com_stat[SQLCOM_SELECT];

  /// end select
  thd->set_row_count_func(-1);

#ifndef MYSQL8
  /// write audit log before exit
  thd->update_server_status();
  mysql_rds_audit_log.write_log(thd, thd->query().str, thd->query().length,
                                RDS_SQLCOM_NORMAL);
  log_slow_statement(thd);
#endif

  /// cleanup query
  cleanup_and_mark_sleep(thd);

  /// sub global running
  Global_THD_manager::get_instance()->dec_thread_running();
}

}  // namespace polarx_rpc
