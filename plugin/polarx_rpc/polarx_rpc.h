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

#pragma once

#include <atomic>
#include <memory>
#include <mutex>

#include "global_defines.h"
#ifndef MYSQL8
#include <my_global.h>
#include "sql_plugin.h"
#endif
#include <mysql.h>
#include <mysql/plugin.h>
#include <mysql/service_my_plugin_log.h>
#include "sql/log.h"

#define POLARX_RPC_PLUGIN_NAME "polarx_rpc"

namespace polarx_rpc {
class Cserver;
class CrequestCache;
}  // namespace polarx_rpc

struct polarx_rpc_info_t final {
  /// server
  std::mutex mutex;
  MYSQL_PLUGIN plugin_info = nullptr;
  std::unique_ptr<polarx_rpc::Cserver> server;
  std::atomic<bool> exit = {false};

  /// cache
  std::unique_ptr<polarx_rpc::CrequestCache> cache;

  /// status
  std::atomic<bool> inited = {false};
  std::atomic<int64> tcp_connections = {0};
  std::atomic<int64> tcp_closing = {0};
  //// session count use polarx_rpc::g_session_count;
  std::atomic<int64> total_sessions = {0};  /// include internal session
  std::atomic<int64> threads = {0};  /// working threads(without watchdog)
  std::atomic<int64> sql_hit = {0};
  std::atomic<int64> sql_miss = {0};
  std::atomic<int64> sql_evict = {0};
  std::atomic<int64> plan_hit = {0};
  std::atomic<int64> plan_miss = {0};
  std::atomic<int64> plan_evict = {0};
};

extern polarx_rpc_info_t plugin_info;

void clear_xrpc_cache();

#define POLARX_RPC_DBG 0
#if POLARX_RPC_DBG
#define DBG_LOG(_x_) sql_print_information _x_
#else
#define DBG_LOG(_x_)
#endif

#define POLARX_RPC_PKT_DBG 0
