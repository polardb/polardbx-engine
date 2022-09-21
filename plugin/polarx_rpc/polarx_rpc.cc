//
// Created by zzy on 2022/7/5.
//

#include <cassert>

#include "m_ctype.h"
#include "sql/mysqld.h"

#include "polarx_rpc.h"
#include "server/server.h"
#include "server/server_variables.h"

polarx_rpc_info_t plugin_info;

static int polarx_rpc_init(MYSQL_PLUGIN info) {
  plugin_info.plugin_info = info;

  /// show log
  my_plugin_log_message(&plugin_info.plugin_info, MY_WARNING_LEVEL,
                        "polarx_rpc start up");

  /// construct
  assert(!plugin_info.server);
  try {
    plugin_info.server.reset(new polarx_rpc::Cserver);
  } catch (std::exception &e) {
    my_plugin_log_message(&plugin_info.plugin_info, MY_ERROR_LEVEL,
                          "Startup failed with error \"%s\"", e.what());
    // unireg_abort(MYSQLD_ABORT_EXIT);
    return 1;
  }

  return 0;
}

static int polarx_rpc_deinit(void *arg MY_ATTRIBUTE((unused))) {
  plugin_info.server.reset(); /// uninitialize
  return 0;
}

static struct SHOW_VAR polarx_rpc_status_variables[] = {
    {NullS, NullS, SHOW_LONG, SHOW_SCOPE_GLOBAL}};

mysql_declare_plugin(polarx_rpc){
    MYSQL_DAEMON_PLUGIN,
    &plugin_info,
    POLARX_RPC_PLUGIN_NAME,
    "Alibaba Cloud PolarDB-X",
    "RPC framework for PolarDB-X",
    PLUGIN_LICENSE_PROPRIETARY,
    polarx_rpc_init,
    nullptr,
    polarx_rpc_deinit,
    0x0100,
    polarx_rpc_status_variables,
    ::polarx_rpc::polarx_rpc_system_variables,
    nullptr,
    0,
} mysql_declare_plugin_end;
