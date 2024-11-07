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

#include <atomic>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>

#include "../global_defines.h"
#ifdef MYSQL8
#include "sql/hostname_cache.h"  // ip_to_hostname
#else
#include "sql/hostname.h"
#endif
#include "sql/mysqld.h"

#include "../common_define.h"
#include "../polarx_rpc.h"

#include "../session/session_base.h"

#include "epoll.h"
#include "server_variables.h"
#include "socket_operator.h"
#include "tcp_connection.h"

namespace polarx_rpc {

class Clistener final : public CepollCallback {
  NO_COPY_MOVE(Clistener)

 private:
  CmtEpoll &epoll_;
  int fd_;

  explicit Clistener(CmtEpoll &epoll) : epoll_(epoll), fd_(-1) {}

  void set_fd(int fd) final { fd_ = fd; }

  static inline bool resolve(const sockaddr_in &client_addr, std::string &ip,
                             std::string &host, uint16_t &port) {
    host.resize(INET6_ADDRSTRLEN);
    if (client_addr.sin_family == AF_INET) {
      ::inet_ntop(client_addr.sin_family, &client_addr.sin_addr,
                  const_cast<char *>(host.data()), host.size());
      port = ntohs(client_addr.sin_port);
    } else {
      ::inet_ntop(client_addr.sin_family,
                  &((const sockaddr_in6 &)client_addr).sin6_addr,
                  const_cast<char *>(host.data()), host.size());
      port = ntohs(((const sockaddr_in6 &)client_addr).sin6_port);
    }
    ip = host.c_str();
    host.resize(ip.size());  /// remove tail

    // turn IP to hostname for auth uses
    if (!opt_skip_name_resolve) {
      char *hostname = nullptr;
      uint connect_errors;
      int rc = ip_to_hostname(reinterpret_cast<sockaddr_storage *>(
                                  const_cast<sockaddr_in *>(&client_addr)),
                              host.data(), &hostname, &connect_errors);

      if (rc == RC_BLOCKED_HOST) {
        std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
        if (plugin_info.plugin_info != nullptr)
          my_plugin_log_message(&plugin_info.plugin_info, MY_WARNING_LEVEL,
                                "Resolve name blocked %s:%u.", host.c_str(),
                                port);
        return false;
      }
      if (hostname) {
        host = hostname;
        if (hostname != my_localhost) my_free(hostname);
      }
    }
    return true;
  }

  bool events(uint32_t events, int, int) final {
    auto retry = 0;
    int new_fd;
    while (true) {
      sockaddr_in accept_address;
      socklen_t accept_len = sizeof(accept_address);
      while (
          (new_fd = ::accept(fd_, reinterpret_cast<sockaddr *>(&accept_address),
                             &accept_len)) >= 0) {
        /// reset when success
        retry = 0;

        /// check api ready
        if (!CsessionBase::is_api_ready()) {
          /// api not ready, just reject
          Csocket::shutdown(new_fd);
          Csocket::close(new_fd);
          continue;
        }

        /// resolve host name
        std::string ip, host;
        uint16_t port;
        if (!resolve(accept_address, ip, host, port)) {
          /// failed to resolve host, just reject
          Csocket::shutdown(new_fd);
          Csocket::close(new_fd);
          continue;
        }

        /// once ready and connected, watch dog should work normally
        if (!plugin_info.inited.load(std::memory_order_acquire))
          plugin_info.inited.store(true, std::memory_order_release);

        /// new connection
        std::unique_ptr<CtcpConnection> tcp(new CtcpConnection(
            epoll_, g_tcp_id_generator.fetch_add(1, std::memory_order_relaxed),
            new_fd, std::move(ip), std::move(host), port));
        /// reference is inited to 1, and will add to 2 if successfully
        /// registered to epoll
        auto err = epoll_.add_fd(new_fd, EPOLLIN | EPOLLET, tcp.get());
        if (0 == err) {
          /// to avoid missing epoll_in
          tcp->pre_events();
          auto bret = tcp->events(EPOLLIN, 0, 1);
          (void)bret;
          assert(bret);  /// still keep the reference, so never fail
        } else {
          tcp->fin("failed to add to epoll");  /// close socket
          {
            std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
            if (plugin_info.plugin_info != nullptr)
              my_plugin_log_message(&plugin_info.plugin_info, MY_WARNING_LEVEL,
                                    "Failed to accept. %s",
                                    std::strerror(-err));
          }
        }
        auto after = tcp->sub_reference();
        if (after > 0) {
          assert(0 == err);  /// must be successfully registered
          tcp.release();     /// still reference, leak it to epoll
        }
        /// or free by unique_ptr
      }

      /// dealing accept error
      auto err = errno;
      if (LIKELY(EWOULDBLOCK == err || EAGAIN == err))
        break;  /// The socket is marked nonblocking and no connections are
                /// present to be accepted.
      else if (err != EINTR) {
        {
          std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
          if (plugin_info.plugin_info != nullptr)
            my_plugin_log_message(&plugin_info.plugin_info, MY_ERROR_LEVEL,
                                  "Fatal error when accept. %s",
                                  std::strerror(err));
        }
        throw std::runtime_error("Bad accept state.");
      }
      if (++retry >= 10) {
        std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
        if (plugin_info.plugin_info != nullptr)
          my_plugin_log_message(&plugin_info.plugin_info, MY_WARNING_LEVEL,
                                "Failed to accept with EINTR after retry %d.",
                                retry);
        break;
      }
    }
    return true;
  }

 public:
  static void start(uint16_t port) {
    /// check port first
    auto check_times = 0;
    int ierr;
    do {
      ierr = CmtEpoll::check_port(port);
      if (0 == ierr)
        break;  /// good
      else
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (++check_times < 3);
    if (ierr != 0) {
      {
        std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
        if (plugin_info.plugin_info != nullptr)
          my_plugin_log_message(&plugin_info.plugin_info, MY_ERROR_LEVEL,
                                "Failed to check port %u. %s", port,
                                std::strerror(-ierr));
      }
      throw std::runtime_error("Failed to check port.");
    }

    /// bind port
    size_t inst_cnt;
    auto insts = CmtEpoll::get_instance(inst_cnt);
    for (size_t i = 0; i < inst_cnt; ++i) {
      std::unique_ptr<Clistener> listener(new Clistener(*insts[i]));
      /// only set reuse port on other inst
      ierr = insts[i]->listen_port(port, listener.get(), inst_cnt > 1);
      if (0 == ierr)
        insts[i]->push_listener(std::move(listener));  /// push it to epoll
      else {
        {
          std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
          if (plugin_info.plugin_info != nullptr)
            my_plugin_log_message(&plugin_info.plugin_info, MY_ERROR_LEVEL,
                                  "Failed to listen on port %u. %s", port,
                                  std::strerror(-ierr));
        }
        throw std::runtime_error("Failed to listen.");
      }
    }
    std::lock_guard<std::mutex> plugin_lck(plugin_info.mutex);
    if (plugin_info.plugin_info != nullptr)
      my_plugin_log_message(&plugin_info.plugin_info, MY_WARNING_LEVEL,
                            "Listen on port %u.", port);
  }
};

}  // namespace polarx_rpc
