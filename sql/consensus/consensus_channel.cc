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

#include "sql/consensus/consensus_channel.h"
#include "sql/rpl_msr.h"

/** Special channel name for xpaxos channel. */
const char *Multisource_info::xpaxos_channel = "xpaxos_applier";

/** Whether channel is xpaxos replication according to channel name. */
bool Multisource_info::is_xpaxos_replication_channel_name(const char *channel) {
  if (!channel) return true;

  return !strcmp(channel, default_channel) || !strcmp(channel, xpaxos_channel);
}

/** Whether channel is xpaxos replication according to master info. */
bool Multisource_info::is_xpaxos_channel(const Master_info *mi) {
  return mi && mi->style() == Channel_style::XPaxos;
}
/** Whether channel is xpaxos replication according to relay log info . */
bool Multisource_info::is_xpaxos_channel(const Relay_log_info *rli) {
  return rli && rli->style() == Channel_style::XPaxos;
}
