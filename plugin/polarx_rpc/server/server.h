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

namespace polarx_rpc {

class Cserver final {
  NO_COPY_MOVE(Cserver);

  /// thread of watch dog to prevent deadlock on thread pool schedule
  static void watch_dog() {
    while (true) {
      /// check inited
      if (!plugin_info.inited.load(std::memory_order_acquire)) {
        if (CsessionBase::is_api_ready())
          plugin_info.inited.store(true, std::memory_order_release);

        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        continue; /// fast retry
      }

      /// check thread pool
      size_t inst_cnt;
      auto insts = CmtEpoll::get_instance(inst_cnt);
      for (size_t i = 0; i < inst_cnt; ++i) {
        auto &inst = *insts[i];
        if (inst.worker_stall_since_last_check())
          inst.force_scale_thread_pool();
        if (enable_tasker)
          inst.balance_tasker();
      }

      /// normal sleep and retry
      std::this_thread::sleep_for(std::chrono::milliseconds(
          epoll_group_thread_deadlock_check_interval));
    }
  }

public:
  Cserver() noexcept(false) {
#ifdef MYSQL8
    auto port = rpc_port;
    if (port > 0 && port < 65536 && new_rpc) {
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
};

} // namespace polarx_rpc
