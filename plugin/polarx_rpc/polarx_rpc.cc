//
// Created by zzy on 2022/7/5.
//

#include <cassert>
#include <mutex>

#include "m_ctype.h"
#include "sql/mysqld.h"

#include "polarx_rpc.h"
#include "server/server.h"
#include "server/server_variables.h"
#include "session/request_cache.h"
#include "session/session.h"

#include "global_defines.h"

polarx_rpc_info_t plugin_info;

static int polarx_rpc_init(MYSQL_PLUGIN info) {
  {
    std::lock_guard<std::mutex> lck(plugin_info.mutex);
    plugin_info.plugin_info = info;
  }

  /// plugin_info.plugin_info always valid in this function

  /// database init?
  if (opt_initialize) {
    my_plugin_log_message(&plugin_info.plugin_info, MY_WARNING_LEVEL,
                          "PolarX RPC disabled by database initialization.");
    return 0;
  }

  /// show log
  my_plugin_log_message(&plugin_info.plugin_info, MY_WARNING_LEVEL,
                        "polarx_rpc start up");

  /// construct
  assert(!plugin_info.server);
  try {
    plugin_info.server.reset(new polarx_rpc::Cserver);
    plugin_info.cache.reset(new polarx_rpc::CrequestCache(
        polarx_rpc::request_cache_number, polarx_rpc::request_cache_instances));
  } catch (std::exception &e) {
    my_plugin_log_message(&plugin_info.plugin_info, MY_ERROR_LEVEL,
                          "Startup failed with error \"%s\"", e.what());
    unireg_abort(MYSQLD_ABORT_EXIT);
    return 1;
  }

  return 0;
}

static int polarx_rpc_deinit(void *arg MY_ATTRIBUTE((unused))) {
  {
    std::lock_guard<std::mutex> lck(plugin_info.mutex);
    plugin_info.plugin_info = nullptr;
  }
  plugin_info.server.reset(); /// uninitialize
  /// cache never free
  return 0;
}

#ifndef MYSQL8
using SHOW_VAR = st_mysql_show_var;
#endif

void show_status_init(THD *thd, SHOW_VAR *var, char *buff) {
  var->type = SHOW_BOOL;
  var->value = buff;
  *reinterpret_cast<bool *>(buff) =
      plugin_info.inited.load(std::memory_order_acquire);
}

void show_status_tcp_connections(THD *thd, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  *reinterpret_cast<int64_t *>(buff) =
      plugin_info.tcp_connections.load(std::memory_order_acquire);
}

void show_status_tcp_closing(THD *thd, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  *reinterpret_cast<int64_t *>(buff) =
      plugin_info.tcp_closing.load(std::memory_order_acquire);
}

void show_status_worker_sessions(THD *thd, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  *reinterpret_cast<int64_t *>(buff) =
      polarx_rpc::g_session_count.load(std::memory_order_acquire);
}

void show_status_total_sessions(THD *thd, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  *reinterpret_cast<int64_t *>(buff) =
      plugin_info.total_sessions.load(std::memory_order_acquire);
}

void show_status_threads(THD *thd, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  *reinterpret_cast<int64_t *>(buff) =
      plugin_info.threads.load(std::memory_order_acquire);
}

void show_status_sql_hit(THD *thd, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  *reinterpret_cast<int64_t *>(buff) =
      plugin_info.sql_hit.load(std::memory_order_acquire);
}

void show_status_sql_miss(THD *thd, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  *reinterpret_cast<int64_t *>(buff) =
      plugin_info.sql_miss.load(std::memory_order_acquire);
}

void show_status_sql_evict(THD *thd, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  *reinterpret_cast<int64_t *>(buff) =
      plugin_info.sql_evict.load(std::memory_order_acquire);
}

void show_status_plan_hit(THD *thd, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  *reinterpret_cast<int64_t *>(buff) =
      plugin_info.plan_hit.load(std::memory_order_acquire);
}

void show_status_plan_miss(THD *thd, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  *reinterpret_cast<int64_t *>(buff) =
      plugin_info.plan_miss.load(std::memory_order_acquire);
}

void show_status_plan_evict(THD *thd, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  *reinterpret_cast<int64_t *>(buff) =
      plugin_info.plan_evict.load(std::memory_order_acquire);
}

typedef void (*show_status_func)(THD *, SHOW_VAR *, char *);

static char *func_ptr(show_status_func func) {
  union {
    char *ptr;
    show_status_func func;
  } ptr_cast;
  ptr_cast.func = func;
  return ptr_cast.ptr;
}

static SHOW_VAR polarx_rpc_status_variables[] = {
    {POLARX_RPC_PLUGIN_NAME "_inited", func_ptr(show_status_init), SHOW_FUNC,
     SHOW_SCOPE_GLOBAL},
    {POLARX_RPC_PLUGIN_NAME "_tcp_connections",
     func_ptr(show_status_tcp_connections), SHOW_FUNC, SHOW_SCOPE_GLOBAL},
    {POLARX_RPC_PLUGIN_NAME "_tcp_closing", func_ptr(show_status_tcp_closing),
     SHOW_FUNC, SHOW_SCOPE_GLOBAL},
    {POLARX_RPC_PLUGIN_NAME "_worker_sessions",
     func_ptr(show_status_worker_sessions), SHOW_FUNC, SHOW_SCOPE_GLOBAL},
    {POLARX_RPC_PLUGIN_NAME "_total_sessions",
     func_ptr(show_status_total_sessions), SHOW_FUNC, SHOW_SCOPE_GLOBAL},
    {POLARX_RPC_PLUGIN_NAME "_threads", func_ptr(show_status_threads),
     SHOW_FUNC, SHOW_SCOPE_GLOBAL},
    {POLARX_RPC_PLUGIN_NAME "_sql_hit", func_ptr(show_status_sql_hit),
     SHOW_FUNC, SHOW_SCOPE_GLOBAL},
    {POLARX_RPC_PLUGIN_NAME "_sql_miss", func_ptr(show_status_sql_miss),
     SHOW_FUNC, SHOW_SCOPE_GLOBAL},
    {POLARX_RPC_PLUGIN_NAME "_sql_evict", func_ptr(show_status_sql_evict),
     SHOW_FUNC, SHOW_SCOPE_GLOBAL},
    {POLARX_RPC_PLUGIN_NAME "_plan_hit", func_ptr(show_status_plan_hit),
     SHOW_FUNC, SHOW_SCOPE_GLOBAL},
    {POLARX_RPC_PLUGIN_NAME "_plan_miss", func_ptr(show_status_plan_miss),
     SHOW_FUNC, SHOW_SCOPE_GLOBAL},
    {POLARX_RPC_PLUGIN_NAME "_plan_evict", func_ptr(show_status_plan_evict),
     SHOW_FUNC, SHOW_SCOPE_GLOBAL},
    {NullS, NullS, SHOW_LONG, SHOW_SCOPE_GLOBAL}};

mysql_declare_plugin(polarx_rpc){
    MYSQL_DAEMON_PLUGIN,
    &plugin_info.daemon,
    POLARX_RPC_PLUGIN_NAME,
    "Alibaba Cloud PolarDB-X",
    "RPC framework for PolarDB-X",
    PLUGIN_LICENSE_PROPRIETARY,
    polarx_rpc_init,
#ifdef MYSQL8
    nullptr,
#endif
    polarx_rpc_deinit,
    0x0100,
    polarx_rpc_status_variables,
    ::polarx_rpc::polarx_rpc_system_variables,
    nullptr,
    0,
} mysql_declare_plugin_end;
