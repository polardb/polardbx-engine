/*****************************************************************************

Copyright (c) 2013, 2023, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file sql/komodo0rpl0binlog0sender.cc

  Replication binlog sender.

  Created 2023-07-11 by Jiyang.zhang
 *******************************************************/

#include "sql/komodo0rpl0binlog0sender.h"
#include "sql/rpl_binlog_sender.h"

namespace komodo {
int Delay_binlog_sender::send_all_delay_events() {
  int error = 0;
  String tmp;

  if (!has_delayed_events()) {
    return 0;
  }

  tmp.copy(m_target->m_packet);
  tmp.length(m_target->m_packet.length());

  for (auto &ev : m_events) {
    if (ev.is_empty()) {
      continue;
    }

    m_target->m_packet.copy(ev.packet);
    m_target->m_packet.length(ev.packet.length());

    if (m_target->before_send_hook(ev.log_file.c_str(), ev.log_pos) ||
        unlikely(m_target->send_packet()) ||
        unlikely(m_target->after_send_hook(
            ev.log_file.c_str(), ev.in_exclude_group ? ev.log_pos : 0))) {
      error = 1;
      break;
    }
  }

  forget_delay_events();

  /** TODO: If error happens, also restore m_packet, we don't know if it's a
  good choice. <12-07-23, zanye.zjy> */
  m_target->m_packet.copy(tmp);
  m_target->m_packet.length(tmp.length());

  return error;
}

void Delay_binlog_sender::forget_delay_events() {
  for (auto &ev : m_events) {
    ev.reset();
  }
}
}  // namespace komodo
