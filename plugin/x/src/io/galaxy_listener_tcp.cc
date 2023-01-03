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


#include "plugin/x/src/io/galaxy_listener_tcp.h"

#include "plugin/x/ngs/include/ngs/galaxy_protocol.h"
#include "plugin/x/src/variables/galaxy_status.h"

namespace gx {

Galaxy_listener_tcp::~Galaxy_listener_tcp() {}

bool Galaxy_listener_tcp::setup_listener(On_connection on_connection) {
  if (!m_state.is(ngs::State_listener_initializing)) return false;

  m_tcp_socket = create_socket();

  if (m_tcp_socket.get() == nullptr) {
    close_listener();
    return false;
  }

  if (m_event.listen(m_tcp_socket, on_connection, gx::Protocol_type::GALAXYX)) {
    m_state.set(ngs::State_listener_prepared);
    return true;
  }

  m_last_error = "event dispatcher couldn't register galaxy socket";
  close_listener();

  m_tcp_socket.reset();

  return false;
}

void Galaxy_listener_tcp::report_properties(On_report_properties on_prop) {
  switch (m_state.get()) {
    case ngs::State_listener_prepared:
    case ngs::State_listener_running: {
      std::string p_port =
          Galaxy_status_variables::get_property(Galaxy_property_type::TCP_PORT);

      if (p_port == GALAXY_PROPERTY_UNDEFINED) {
        Galaxy_status_variables::set_property(Galaxy_property_type::TCP_PORT,
                                              std::to_string(m_port));
      }

      std::string p_address = Galaxy_status_variables::get_property(
          Galaxy_property_type::TCP_BIND_ADDRESS);

      if (p_address == GALAXY_PROPERTY_UNDEFINED) {
        Galaxy_status_variables::set_property(
            Galaxy_property_type::TCP_BIND_ADDRESS, m_bind_address);
      } else {
        p_address += ',' + m_bind_address;
        Galaxy_status_variables::m_properties
            [Galaxy_property_type::TCP_BIND_ADDRESS] = p_address;
      }
    }
    case ngs::State_listener_stopped:
      return;
    default:
      return;
  }
}

}  // namespace gx
