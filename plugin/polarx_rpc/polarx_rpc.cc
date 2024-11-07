/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/


//
// Created by zzy on 2022/7/5.
//

#include <cassert>
#include <mutex>

#include "m_ctype.h"
#include "mysql/plugin_audit.h"
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
    my_plugin_log_message(
        &plugin_info.plugin_info, MY_WARNING_LEVEL,
        "Plugin polarx_rpc disabled by database initialization.");
    return 0;
  }

  /// check reinitialize
  if (plugin_info.exit.load(std::memory_order_acquire)) {
    my_plugin_log_message(
        &plugin_info.plugin_info, MY_ERROR_LEVEL,
        "Plugin polarx_rpc already exit, and can't init again.");
    unireg_abort(MYSQLD_ABORT_EXIT);
    return 1;
  }

  /// show log
  my_plugin_log_message(&plugin_info.plugin_info, MY_WARNING_LEVEL,
                        "Plugin polarx_rpc start up.");

  /// construct
  assert(!plugin_info.server);
  try {
    /// init cache before server
    plugin_info.cache.reset(new polarx_rpc::CrequestCache(
        polarx_rpc::request_cache_number, polarx_rpc::request_cache_instances));
    plugin_info.server.reset(new polarx_rpc::Cserver);
  } catch (std::exception &e) {
    my_plugin_log_message(&plugin_info.plugin_info, MY_ERROR_LEVEL,
                          "Plugin polarx_rpc startup failed with error \"%s\"",
                          e.what());
    unireg_abort(MYSQLD_ABORT_EXIT);
    return 1;
  }

  return 0;
}

static int polarx_rpc_deinit(void *arg MY_ATTRIBUTE((unused))) {
  plugin_info.exit.store(true, std::memory_order_release);
  plugin_info.server.reset();  /// un-initialize
  /// listener and cache never free

  my_plugin_log_message(&plugin_info.plugin_info, MY_WARNING_LEVEL,
                        "Plugin polarx_rpc exit.");
  {
    std::lock_guard<std::mutex> lck(plugin_info.mutex);
    plugin_info.plugin_info = nullptr;
  }
  return 0;
}

void clear_xrpc_cache() {
  if (plugin_info.cache) plugin_info.cache->clear();
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

static int audit_event_notify(MYSQL_THD, mysql_event_class_t event_class,
                              const void *) {
  if (event_class == MYSQL_AUDIT_SERVER_SHUTDOWN_CLASS) {
    my_plugin_log_message(&plugin_info.plugin_info, MY_WARNING_LEVEL,
                          "Plugin polarx_rpc get shutdown notify.");

    /// start exit threads
    plugin_info.exit.store(true, std::memory_order_release);
    plugin_info.server.reset();  /// un-initialize
  }
  return 0;
}

/// cleanup callback
static struct st_mysql_audit polarx_rpc_plugin_descriptor = {
    MYSQL_AUDIT_INTERFACE_VERSION,  // interface version
    nullptr,                        // release_thd()
    audit_event_notify,             // event_notify()
    {0,                             // MYSQL_AUDIT_GENERAL_CLASS
     0,                             // MYSQL_AUDIT_CONNECTION_CLASS
     0,                             // MYSQL_AUDIT_PARSE_CLASS
     0,                             // MYSQL_AUDIT_AUTHORIZATION_CLASS
     0,                             // MYSQL_AUDIT_TABLE_ACCESS_CLASS
     0,                             // MYSQL_AUDIT_GLOBAL_VARIABLE_CLASS
     0,                             // MYSQL_AUDIT_SERVER_STARTUP_CLASS
     static_cast<unsigned long>(MYSQL_AUDIT_SERVER_SHUTDOWN_SHUTDOWN)}};

mysql_declare_plugin(polarx_rpc){
    MYSQL_AUDIT_PLUGIN,
    &polarx_rpc_plugin_descriptor,
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
