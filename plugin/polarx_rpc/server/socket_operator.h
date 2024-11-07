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

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

#include <arpa/inet.h>
#include <fcntl.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/sendfile.h>
#include <sys/socket.h>
#include <unistd.h>

#include "../common_define.h"

namespace polarx_rpc {

class Csocket final {
  NO_CONSTRUCTOR(Csocket)
  NO_COPY_MOVE(Csocket)

 public:
  /// 0 if success else -errno
  static int set_sndbuf(int sock, size_t buf_size) {
    int buf_len = buf_size;
    if (LIKELY(buf_len != 0))
      return 0 == ::setsockopt(sock, SOL_SOCKET, SO_SNDBUF, &buf_len,
                               sizeof(buf_len))
                 ? 0
                 : -errno;
    return 0;
  }

  /// 0 if success else -errno
  static int set_rcvbuf(int sock, size_t buf_size) {
    int buf_len = buf_size;
    if (LIKELY(buf_len != 0))
      return 0 == ::setsockopt(sock, SOL_SOCKET, SO_RCVBUF, &buf_len,
                               sizeof(buf_len))
                 ? 0
                 : -errno;
    return 0;
  }

  /// 0 if success else -errno
  static bool no_delay(int sock, bool no_delay = true) {
    int on = no_delay ? 1 : 0;
    return 0 == ::setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on))
               ? 0
               : -errno;
  }

  /// 0 if success else -errno
  static int reuse_addr(int sock, bool reuse = true) {
    int sock_op = reuse ? 1 : 0;
    return 0 == ::setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &sock_op,
                             sizeof(sock_op))
               ? 0
               : -errno;
  }

  /// sock if success else -errno
  static int udp() {
    int sock = ::socket(AF_INET, SOCK_DGRAM, 0);
    if (UNLIKELY(sock < 0)) return -errno;
    return sock;
  }

  /// sock if success else -errno
  static int bind_udp(const char *ip, uint16_t port) {
    int sock = udp();
    if (UNLIKELY(sock < 0)) return sock;

    ::sockaddr_in address;
    ::memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = ::inet_addr(ip);
    address.sin_port = htons(port);

    if (UNLIKELY(::bind(sock, (struct sockaddr *)&address, sizeof(address)) !=
                 0)) {
      auto err = errno;
      close(sock);
      return -err;
    }

    return sock;
  }

  /// sock if success else -errno
  static int bind_and_listen(const char *ip, uint16_t port,
                             int listen_queue_depth = 128) {
    ::sockaddr_in address;
    ::memset(&address, 0, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = ::inet_addr(ip);
    address.sin_port = htons(port);
    int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if (UNLIKELY(sock < 0)) return -errno;

    reuse_addr(sock);  /// ignore result
    if (UNLIKELY(::bind(sock, (struct sockaddr *)&address, sizeof(address)) !=
                 0)) {
      auto err = errno;
      close(sock);
      return -err;
    }

    if (UNLIKELY(::listen(sock, listen_queue_depth) != 0)) {
      auto err = errno;
      close(sock);
      return -err;
    }

    return sock;
  }

  /// sock if success else -errno
  static int accept(int sock) {
    return 0 == ::accept(sock, nullptr, nullptr) ? 0 : -errno;
  }

  /// sock if success else -errno
  static int connect(const char *ip, uint16_t port, uint16_t bind_port = 0) {
    int sock = ::socket(AF_INET, SOCK_STREAM, 0);
    if (UNLIKELY(sock < 0)) return -errno;

    if (UNLIKELY(bind_port != 0)) {
      ::sockaddr_in address;
      ::memset(&address, 0, sizeof(address));
      address.sin_family = AF_INET;
      address.sin_addr.s_addr = htonl(INADDR_ANY);
      address.sin_port = htons(bind_port);
      reuse_addr(sock);  /// ignore result
      if (::bind(sock, (struct sockaddr *)&address, sizeof(address)) != 0) {
        auto err = errno;
        close(sock);
        return -err;
      }
    }

    ::sockaddr_in addr;
    ::memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    ::inet_pton(AF_INET, ip, &addr.sin_addr);
    addr.sin_port = htons(port);
    if (UNLIKELY(::connect(sock, (sockaddr *)&addr, sizeof(addr)) != 0)) {
      auto err = errno;
      ::close(sock);
      return -err;
    }

    return sock;
  }

  static int shutdown(int sock) { return ::shutdown(sock, SHUT_RDWR); }

  static int close(int sock) { return ::close(sock); }

  static int send_udp(int sock, const ::sockaddr *addr, ::socklen_t addrlen,
                      const void *data, size_t datalen) {
    return ::sendto(sock, data, datalen, 0, addr, addrlen);
  }

  static int recvfrom(int sock, ::sockaddr *addr, ::socklen_t *addrlen,
                      void *data, size_t datalen) {
    return ::recvfrom(sock, data, datalen, 0, addr, addrlen);
  }

  static bool send_fixed(int sock, const void *data, size_t length) {
    auto ptr = reinterpret_cast<const uint8_t *>(data);
    while (length > 0) {
      auto iret = ::write(sock, ptr, length);
      if (UNLIKELY(iret <= 0))
        return false;
      else {
        ptr += iret;
        length -= iret;
      }
    }
    return true;
  }

  static bool sendfile_fixed(int sock, int file, int64_t offset, int length) {
    while (length > 0) {
      off_t off = offset;
      auto iret = ::sendfile(sock, file, &off, length);
      if (UNLIKELY(iret <= 0))
        return false;
      else {
        off += iret;
        length -= iret;
      }
    }
    return true;
  }

  static int recv(int sock, void *data, size_t length) {
    return ::read(sock, data, length);
  }

  static bool recv_fixed(int sock, void *data, size_t length) {
    auto ptr = reinterpret_cast<uint8_t *>(data);
    while (length > 0) {
      auto iret = ::read(sock, ptr, length);
      if (UNLIKELY(iret <= 0))
        return false;
      else {
        ptr += iret;
        length -= iret;
      }
    }
    return true;
  }
};

}  // namespace polarx_rpc
