/* Copyright (c) 2008, 2018, Alibaba and/or its affiliates. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License, version 2.0,
  as published by the Free Software Foundation.

  This program is also distributed with certain software (including
  but not limited to OpenSSL) that is licensed under separate terms,
  as designated in a particular file or component or in included license
  documentation.  The authors of MySQL hereby grant you an additional
  permission to link the program and your derivative works with the
  separately licensed software that they have included with MySQL.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License, version 2.0, for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */
#ifndef BINLOG_EXT_INCLUDED
#define BINLOG_EXT_INCLUDED

#include <my_systime.h>
#include <atomic>
#include <vector>
#include "libbinlogevents/include/control_events.h"
#include "map_helpers.h"
#include "my_inttypes.h"
#include "sql/xa.h"

namespace lizard {
namespace xa {
/**
  Like binlog_start_trans_and_stmt. The difference is:

  binlog_start_trans_and_stmt is not sure whether to open a statement or a
  transaction, while binlog_start_trans is to open a transaction
  deterministically.
*/
int binlog_start_trans(THD *thd);
}
}  // namespace lizard

#endif
