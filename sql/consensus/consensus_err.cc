/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

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

#include "sql/consensus/consensus_err.h"

namespace xp {

void logger::log_event(std::string msg) {
  LogEvent()
      .type(LOG_TYPE_ERROR)
      .prio(m_level)
      .errcode(m_err)
      .subsys("XPaxos")
      .verbatim(msg.c_str());
}

logger::~logger() { log_event(m_oss.str()); }

fatal::~fatal() {
  log_event("[FATAL] " + m_oss.str());
  assert(0);
}

}  // namespace xp
