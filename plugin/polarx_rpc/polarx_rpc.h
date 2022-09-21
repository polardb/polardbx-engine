//
// Created by zzy on 2022/7/5.
//

#pragma once

#include <memory>

#include <mysql.h>
#include <mysql/plugin.h>
#include <mysql/service_my_plugin_log.h>
#include "sql/log.h"

#define POLARX_RPC_PLUGIN_NAME "polarx_rpc"

namespace polarx_rpc {
class Cserver;
}

struct polarx_rpc_info_t final {
  MYSQL_PLUGIN plugin_info = nullptr;
  std::unique_ptr<polarx_rpc::Cserver> server;
};

extern polarx_rpc_info_t plugin_info;

#define POLARX_RPC_DBG 0
#if POLARX_RPC_DBG
#  define DBG_LOG(_x_) sql_print_information _x_
#else
#  define DBG_LOG(_x_)
#endif
