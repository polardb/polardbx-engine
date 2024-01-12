
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

/** @file sql/lizard_rpl_rli.h

  Lizard relay log.

  Created 2023-07-12 by Jiyang.zhang
 *******************************************************/
#ifndef LIZARD_LIZARD_RPL_RLI_INCLUDED
#define LIZARD_LIZARD_RPL_RLI_INCLUDED

#include <cstddef>  // size_t...

class Log_event;
class Relay_log_info;

void start_sjg_and_enque_gaq(Relay_log_info *rli, Log_event *ev);

namespace lizard {
class Begin_events_before_gtid_manager {
 public:
  static bool is_b_events_before_gtid(Log_event *ev);

  Begin_events_before_gtid_manager(Relay_log_info *_rli) : rli(_rli) {}

  bool seen_b_events_before_gtid() const;

  void mark_as_curr_group(Log_event *ev);

  void reset_seen_status();

  size_t get_size() const;

 private:
  Relay_log_info *rli;
};

constexpr auto is_b_events_before_gtid =
    &Begin_events_before_gtid_manager::is_b_events_before_gtid;
}  // namespace lizard

#endif  // LIZARD_LIZARD_RPL_RLI_INCLUDED
