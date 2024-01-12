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

/** @file libbinlogevents/src/gcn_event.cpp

  Global Commit Number event.

  Created 2023-06-20 by Jianwei.zhao
 *******************************************************/

#include "gcn_event.h"
#include "binlog_event.h"
#include "event_reader.h"
#include "event_reader_macros.h"

#include "lizard_iface.h"

namespace binary_log {

Gcn_event::Gcn_event()
    : Binary_log_event(GCN_LOG_EVENT), flags(0), commit_gcn(MYSQL_GCN_NULL) {}

Gcn_event::Gcn_event(const char *buf, const Format_description_event *fde)
    : Binary_log_event(&buf, fde), flags(0), commit_gcn(MYSQL_GCN_NULL) {
  /*
     The layout of the buffer is as follows:
     +------------+
     |     1 bytes| flags
     +------------+
     |     8 bytes| commit_gcn
     +------------+
   */
  BAPI_ENTER("Gcn_event::Gcn_event(const char*, ...)");
  READER_TRY_INITIALIZATION;
  READER_ASSERT_POSITION(fde->common_header_len);

  READER_TRY_SET(flags, read<uint8_t>);

  // DBUG_ASSERT(flags != 0);

  if (flags & FLAG_HAVE_COMMITTED_GCN) {
    READER_TRY_SET(commit_gcn, read<uint64_t>);
  }

  READER_CATCH_ERROR;
  BAPI_VOID_RETURN;
}

}  // end namespace binary_log
