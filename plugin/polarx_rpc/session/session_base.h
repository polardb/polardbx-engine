//
// Created by zzy on 2022/8/31.
//

#pragma once

#include <atomic>
#include <string>

#include "mysql/service_command.h"

#include "../coders/command_delegate.h"
#include "../common_define.h"
#include "../polarx_rpc.h"
#include "../secure/authentication_interface.h"
#include "../utility/error.h"

#include "flow_control.h"

namespace polarx_rpc {

class CsessionBase {
  NO_COPY_MOVE(CsessionBase)

 protected:
  const uint64_t sid_;

  /// kill flag and check in working state
  std::atomic_bool killed_;

  MYSQL_SESSION mysql_session_;

  unsigned int last_sql_errno_;
  std::string last_sql_error_;

  std::string username_;
  std::string hostname_;
  std::string address_;
  std::string db_;

  std::string show_hostname_;

  /// flow control
  CflowControl flow_control_;

  /// hack account
  void switch_to_sys_user();

  static void default_completion_handler(void *ctx, unsigned int sql_errno,
                                         const char *err_msg);

 public:
  explicit CsessionBase(uint64_t sid)
      : sid_(sid), killed_(false), mysql_session_(nullptr), last_sql_errno_(0) {
    plugin_info.total_sessions.fetch_add(1, std::memory_order_release);
  }

  virtual ~CsessionBase();

  err_t init(uint16_t port);

  inline MYSQL_THD get_thd() const {
    return srv_session_info_get_thd(mysql_session_);
  }

  inline CflowControl &flow_control() { return flow_control_; }

  err_t switch_to_user(const char *username, const char *hostname,
                       const char *address, const char *db);

  void set_show_hostname(const char *hostname);

  err_t reset();

  err_t init_db(const char *db_name, std::size_t db_len);

  bool is_acl_disabled() const;
  bool has_authenticated_user_a_super_priv() const;
  std::string get_user_name() const;
  std::string get_host_or_ip() const;
  std::string get_authenticated_user_name() const;
  std::string get_authenticated_user_host() const;

  err_t authenticate(const char *user, const char *host, const char *ip,
                     const char *db, const std::string &passwd,
                     Authentication_interface &account_verification,
                     bool allow_expired_passwords);

  err_t execute_server_command(enum_server_command cmd,
                               const COM_DATA &cmd_data,
                               CcommandDelegate &delegate);

  err_t execute_sql(const char *sql, size_t length, CcommandDelegate &delegate);

  err_t attach();
  err_t detach();

  /// These will take and free LOCK_thd_data internal, so be careful.
  void remote_kill(bool log);
  void remote_cancel();

  static bool is_api_ready();

  bool is_detach_and_tls_cleared();

  bool is_thd_connection_killed();

  static void create_session_thread(void *(*func)(void *), void *arg);
  static void init_thread_for_session();
  static void deinit_thread_for_session();

  static void begin_query(THD *thd, const char *query, uint len);
  static void cleanup_and_mark_sleep(THD *thd);
  static void end_query(THD *thd);
};

}  // namespace polarx_rpc
