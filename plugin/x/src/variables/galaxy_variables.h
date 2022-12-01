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

#ifndef PLUGIN_X_SRC_VARIABLES_GALAXY_VARIABLES_H
#define PLUGIN_X_SRC_VARIABLES_GALAXY_VARIABLES_H

class THD;
struct SYS_VAR;

namespace gx {

class Galaxy_system_variables {
 public:
  static bool m_mtr;
  static int m_port;
  static unsigned int m_max_queued_messages;
  static unsigned int m_galaxy_worker_threads_per_tcp;
  static unsigned int m_galaxy_worker_threads_shrink_time;
  static bool m_enable_galaxy_session_pool_log;
  static bool m_enable_galaxy_kill_log;
  static unsigned int m_socket_recv_buffer;
  static unsigned int m_socket_send_buffer;

  static struct SYS_VAR *m_system_variables[];

  template <typename Copy_type>
  static void update_func(THD *thd, SYS_VAR *var, void *tgt, const void *save);
};

template <typename Copy_type>
void Galaxy_system_variables::update_func(THD *thd, SYS_VAR *, void *tgt,
                                          const void *save) {
  *static_cast<Copy_type *>(tgt) = *static_cast<const Copy_type *>(save);
}

}  // namespace gx

#endif
