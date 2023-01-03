/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */


/**
  Galaxy X-protocol

  Attention: galaxy_socket_events only is included into socket_events.cc
  to implement the override listen(...) function.
*/
namespace ngs {

bool Socket_events::listen(
    Socket_interface::Shared_ptr sock,
    std::function<void(Connection_acceptor_interface &)> callback,
    gx::Protocol_type ptype) {
  m_socket_events.push_back(allocate_object<Socket_data>());
  Socket_data *socket_event = m_socket_events.back();

  socket_event->callback = callback;
  socket_event->socket = sock;

  /** Galaxy X-protocol */
  socket_event->ptype = ptype;

  event_set(&socket_event->ev, static_cast<int>(sock->get_socket_fd()),
            EV_READ | EV_PERSIST, &Socket_events::socket_data_avaiable,
            socket_event);
  event_base_set(m_evbase, &socket_event->ev);

  return 0 == event_add(&socket_event->ev, NULL);
}

}  // namespace ngs

