/*
 * Copyright (c) 2017, 2019, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.
 *
 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have included with MySQL.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

#include <chrono>
#include <thread>

#include "mutex_lock.h"
#include "mysql/psi/mysql_socket.h"

#include "plugin/x/ngs/include/ngs/vio_wrapper.h"
#include "plugin/x/src/io/connection_type.h"
#include "plugin/x/src/variables/galaxy_variables.h"
#include "plugin/x/src/xpl_performance_schema.h"

namespace ngs {

Vio_wrapper::Vio_wrapper(Vio *vio, gx::Protocol_type ptype)
    : m_vio(vio), m_shutdown_mutex(KEY_mutex_x_vio_shutdown), m_ptype(ptype) {}

static inline int64_t stable_ms() {
  return std::chrono::duration_cast<std::chrono::milliseconds>(
             std::chrono::steady_clock::now().time_since_epoch())
      .count();
}

ssize_t Vio_wrapper::read(uchar *buffer, ssize_t bytes_to_send) {
#if USE_PPOLL_IN_VIO
  // Note: Problem with the original timeout(EAGAIN occurs even if not timeout).
  auto timeout = m_vio->read_timeout;
  if (timeout > 0) {
    auto start_ms = stable_ms();
    ssize_t result;
    do {
      // Use busy waiting.
      result = vio_read(m_vio, buffer, bytes_to_send);
    } while (result < 0 && (EINTR == errno || EAGAIN == errno) &&
             (std::this_thread::yield(), stable_ms() < start_ms + timeout));
    return result;
  }
#endif
  return vio_read(m_vio, buffer, bytes_to_send);
}

ssize_t Vio_wrapper::write(const uchar *buffer, ssize_t bytes_to_send) {
  MUTEX_LOCK(lock, m_shutdown_mutex);
#if USE_PPOLL_IN_VIO
  // Note: Problem with the original timeout(EAGAIN occurs even if not timeout).
  auto timeout = m_vio->write_timeout;
  if (timeout > 0) {
    auto start_ms = stable_ms();
    ssize_t result;
    do {
      // Use busy waiting.
      result = vio_write(m_vio, buffer, bytes_to_send);
    } while (result < 0 && (EINTR == errno || EAGAIN == errno) &&
             (std::this_thread::yield(), stable_ms() < start_ms + timeout));
    return result;
  }
#endif
  return vio_write(m_vio, buffer, bytes_to_send);
}

void Vio_wrapper::set_timeout_in_ms(const Direction direction,
                                    const uint64_t timeout_ms) {
  // VIO can set only timeouts with 1 second resolution.
  //
  // Thus if application needs second resolution ten following it
  // can do following:
  // vio_timeout(m_vio, static_cast<uint32_t>(direction), timeout_s);
  //
  // To get the millisecond resolution, we need to duplicate the logic
  // from "vio_timeout".

  bool old_mode = m_vio->write_timeout < 0 && m_vio->read_timeout < 0;

  int which = direction == Direction::k_write ? 1 : 0;

  if (which)
    m_vio->write_timeout = timeout_ms;
  else
    m_vio->read_timeout = timeout_ms;

  if (m_vio->timeout) m_vio->timeout(m_vio, which, old_mode);
}

void Vio_wrapper::set_state(const PSI_socket_state state) {
  MYSQL_SOCKET_SET_STATE(m_vio->mysql_socket, state);
}

void Vio_wrapper::set_thread_owner() {
  mysql_socket_set_thread_owner(m_vio->mysql_socket);

#ifdef USE_PPOLL_IN_VIO
  m_vio->thread_id = my_thread_self();
#endif
}

my_socket Vio_wrapper::get_fd() { return vio_fd(m_vio); }

xpl::Connection_type Vio_wrapper::get_type() {
  return xpl::Connection_type_helper::convert_type(vio_type(m_vio));
}

sockaddr_storage *Vio_wrapper::peer_addr(std::string &address, uint16 &port) {
  address.resize(256);
  char *buffer = &address[0];

  buffer[0] = 0;

  if (vio_peer_addr(m_vio, buffer, &port, address.capacity())) return nullptr;

  address.resize(strlen(buffer));

  return &m_vio->remote;
}

int Vio_wrapper::shutdown() {
  MUTEX_LOCK(lock, m_shutdown_mutex);
  return vio_shutdown(m_vio);
}

Vio_wrapper::~Vio_wrapper() {
  if (m_vio) vio_delete(m_vio);
}

bool Vio_wrapper::prepare_for_parallel() {
  auto any_fail = false;
  int buflen =
      static_cast<int>(gx::Galaxy_system_variables::m_socket_recv_buffer);
  if (mysql_socket_setsockopt(m_vio->mysql_socket, SOL_SOCKET, SO_RCVBUF,
                              &buflen, sizeof(buflen)) != 0)
    any_fail = true;
  buflen = static_cast<int>(gx::Galaxy_system_variables::m_socket_send_buffer);
  if (mysql_socket_setsockopt(m_vio->mysql_socket, SOL_SOCKET, SO_SNDBUF,
                              &buflen, sizeof(buflen)) != 0)
    any_fail = true;
  return !any_fail;
}

}  // namespace ngs
