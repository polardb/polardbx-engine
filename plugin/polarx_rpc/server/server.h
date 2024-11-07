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
// Created by zzy on 2022/7/6.
//

#pragma once

#include <chrono>
#include <thread>

#include "../global_defines.h"
#ifdef MYSQL8
#include "sql/sys_vars_ext.h"
#else
extern int32 rpc_port;
extern bool new_rpc;
extern bool rpc_use_legacy_port;
extern bool xcluster_standalone;

#include "plugin/x/src/xpl_system_variables.h"
#endif

#include "../common_define.h"
#include "../polarx_rpc.h"
#include "../server/server_variables.h"
#include "../session/session_base.h"

#include "epoll.h"
#include "listener.h"
#include "server_variables.h"
#include "tcp_connection.h"

namespace polarx_rpc {

class Cserver final {
  NO_COPY_MOVE(Cserver)

  /// thread of watch dog to prevent deadlock on thread pool schedule
  static void watch_dog() {
    std::vector<std::shared_ptr<Csession>> killed_sessions;
    while (!plugin_info.exit.load(std::memory_order_acquire)) {
      /// check inited
      if (!plugin_info.inited.load(std::memory_order_acquire)) {
        if (CsessionBase::is_api_ready())
          plugin_info.inited.store(true, std::memory_order_release);

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        continue;  /// fast retry
      }

      /// check thread pool
      size_t inst_cnt;
      auto insts = CmtEpoll::get_instance(inst_cnt);
      for (size_t i = 0; i < inst_cnt; ++i) {
        auto &inst = *insts[i];
        if (inst.worker_stall_since_last_check())
          inst.force_scale_thread_pool();
        if (enable_tasker) inst.balance_tasker();
      }

      /// check available cores
      CmtEpoll::rebind_core();

      /// and do killed cleanup
      const auto start_ns = Ctime::steady_ns();
      killed_sessions.clear();
      for (size_t i = 0; i < inst_cnt; ++i) {
        auto &inst = *insts[i];
        inst.visit_tcp([&killed_sessions](CtcpConnection *tcp) {
          /// hold the lock, so just gather it
          tcp->session_manager().gather_killed_sessions(killed_sessions);
        });
      }
      const auto gather_ns = Ctime::steady_ns();
      /// kill them
      for (auto &s : killed_sessions) s->remote_kill(true);
      const auto killed_number = killed_sessions.size();
      killed_sessions.clear();
      const auto clean_ns = Ctime::steady_ns();
      if (killed_number > 0) {
        std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
        if (plugin_info.plugin_info != nullptr)
          my_plugin_log_message(&plugin_info.plugin_info, MY_WARNING_LEVEL,
                                "PolarX RPC free %lu killed session(s), gather "
                                "time %f ms, clean time %f ms.",
                                killed_number, (gather_ns - start_ns) / 1e6f,
                                (clean_ns - gather_ns) / 1e6f);
      }

      /// normal sleep and retry
      std::this_thread::sleep_for(std::chrono::milliseconds(
          epoll_group_thread_deadlock_check_interval));
    }

    std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
    if (plugin_info.plugin_info != nullptr)
      my_plugin_log_message(&plugin_info.plugin_info, MY_WARNING_LEVEL,
                            "Plugin polarx_rpc watch dog exit.");
  }

 public:
  Cserver() noexcept(false) {
#ifdef MYSQL8
    auto port = opt_rpc_port;
    if (port > 0 && port < 65536 && opt_enable_polarx_rpc) {
#else
    auto port = rpc_use_legacy_port ? polarxpl::Plugin_system_variables::xport
                                    : rpc_port;
    if (port > 0 && port < 65536 && new_rpc && !xcluster_standalone) {
#endif
      /// init server and bind port
      Clistener::start(static_cast<uint16_t>(port));
      /// init watch dog
      std::thread thread(&watch_dog);
      thread.detach();
    } else {
      std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
      if (plugin_info.plugin_info != nullptr)
        my_plugin_log_message(&plugin_info.plugin_info, MY_ERROR_LEVEL,
                              "PolarX RPC disabled.");
    }
  }

  ~Cserver() {
    /// wait all thread exit
    int64 threads;
    while ((threads = plugin_info.threads.load(std::memory_order_acquire)) >
           0) {
      {
        std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
        if (plugin_info.plugin_info != nullptr)
          my_plugin_log_message(
              &plugin_info.plugin_info, MY_WARNING_LEVEL,
              "Plugin polarx_rpc exiting still %ld threads running.", threads);
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
};

}  // namespace polarx_rpc
