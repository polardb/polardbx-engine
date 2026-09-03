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

/** @file sql/komodo0binlog.h

  Binlog behaviours related to komodo lizard system.

  Created 2023-06-20 by Jianwei.zhao
 *******************************************************/

#ifndef komodo0binlog_h
#define komodo0binlog_h

#include <sys/types.h>
#include <memory>
#include "my_inttypes.h"

class THD;
class Binlog_event_writer;
class Gcn_log_event;
class binlog_cache_data;
class Consensus_log_event;
class Gtid_log_event;

namespace komodo {

// Wether to write gcn event.
extern bool opt_gcn_write_event;

bool assign_gcn_to_flush_group(THD *first_seen);

/**
  Like binlog_start_trans_and_stmt. The difference is:

  binlog_start_trans_and_stmt is not sure whether to open a statement or a
  transaction, while binlog_start_trans is to open a transaction
  deterministically.
*/
int binlog_start_trans(THD *thd);

}  // namespace komodo

#endif
