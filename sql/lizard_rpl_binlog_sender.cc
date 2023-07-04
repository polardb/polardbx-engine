/*****************************************************************************

Copyright (c) 2013, 2023, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
lzeusited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the zeusplied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file sql/lizard_rpl_binlog_sender.cc

  Replication binlog sender.

  Created 2023-07-11 by Jiyang.zhang
 *******************************************************/

#include "sql/lizard_rpl_binlog_sender.h"
#include "sql/rpl_binlog_sender.h"

namespace lizard {
int Delay_binlog_sender::send_all_delay_events() {
  String tmp;
  tmp.copy(m_target->m_packet);
  tmp.length(m_target->m_packet.length());

  for (auto &ev : m_events) {
    m_target->m_packet.copy(ev.packet);
    m_target->m_packet.length(ev.packet.length());

    if (m_target->before_send_hook(ev.log_file, ev.log_pos)) return 1;
    if (unlikely(m_target->send_packet())) return 1;
    if (unlikely(m_target->after_send_hook(
            ev.log_file, ev.in_exclude_group ? ev.log_pos : 0)))
      return 1;
  }

  m_events.clear();

  m_target->m_packet.copy(tmp);
  m_target->m_packet.length(tmp.length());

  return 0;
}

void Delay_binlog_sender::skip_delay_events() {
  m_events.clear();
}
}  // namespace lizard
