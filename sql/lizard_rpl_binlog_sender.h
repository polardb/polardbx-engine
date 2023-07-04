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

/** @file sql/lizard_rpl_binlog_sender.h

  Replication binlog sender.

  Created 2023-07-11 by Jiyang.zhang
 *******************************************************/

#ifndef DEFINED_LIZARD_RPL_BINLOG_SENDER
#define DEFINED_LIZARD_RPL_BINLOG_SENDER

#include <vector>

#include "sql_string.h"

class Binlog_sender;

namespace lizard {
class Delay_binlog_sender {
 public:
  Delay_binlog_sender(Binlog_sender *target) : m_target(target) {
    m_events.reserve(2);
  }

  void push_event(String &_packet, const char *_log_file, my_off_t _log_pos,
                  bool _in_exclude_group) {
    m_events.emplace_back(_packet, _log_file, _log_pos, _in_exclude_group);
  }

  int send_all_delay_events();

  void skip_delay_events();

 private:
  class Event_packet_ctx {
   public:
    Event_packet_ctx(String &_packet, const char *_log_file, my_off_t _log_pos,
                     bool _in_exclude_group) {
      packet.copy(_packet);
      packet.length(_packet.length());
      log_file = _log_file;
      log_pos = _log_pos;
      in_exclude_group = _in_exclude_group;
    }
    String packet;
    const char *log_file;
    my_off_t log_pos;
    bool in_exclude_group;
  };

  Binlog_sender *m_target;
  std::vector<Event_packet_ctx> m_events;
};

}  // namespace lizard

#endif // DEFINED_LIZARD_RPL_BINLOG_SENDER
