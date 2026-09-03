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
#ifndef MYSQL_CONSENSUS_FIFO_CACHE_MANAGER_H
#define MYSQL_CONSENSUS_FIFO_CACHE_MANAGER_H

#include "sql/consensus/komodo0fifo0cache.h"

namespace komodo {

struct ConsensusLogEntry {
  uint64 term{};
  uint64 index{};
  size_t buf_size{};
  uchar *buffer{};
  bool outer{}; /* whether created by consensus module */
  uint flag{};  /* atomic flag marked */
  uint64 checksum{};
};

enum ConsensusLogCacheResultCode {
  CLC_SUCCESS = 0,
  CLC_ALREADY_SWAP_OUT = 1,
  CLC_OUT_OF_RANGE = 2,
  CLC_FULL = 3,
  CLC_INTERRUPT = 4,
  CLC_EMPTY = 5
};

typedef Fifo_cache Consensus_cache_manager;

}  // namespace komodo
#endif  // MYSQL_CONSENSUS_FIFO_CACHE_MANAGER_H
