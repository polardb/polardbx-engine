/*****************************************************************************

Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file sql/consensus/komodo0mon.h
  Komodo stauts.

 Created 2025-11-18 by Jiyang.zhang
 *******************************************************/
#ifndef komodo0mon_h
#define komodo0mon_h

#include <cstddef>
#include "mysql/status_var.h"

namespace komodo {

/*******************************************/
/** Fifo cache status */
/*******************************************/
struct Fifo_cache_status {
  /** Cache used size in bytes */
  size_t used_size{0};
  /** Oldest index in the cache */
  size_t first_index{0};
  /** Log count */
  size_t log_count{0};
  /** Read cache requests count. */
  size_t read_requests{0};
  /** Read cache miss count. */
  size_t read_miss{0};
  /** Consensus log is too big, so that malloc is used. */
  size_t malloc_total{0};
  /** Open buffer request total */
  size_t open_buffer{0};
  /** Evict because reserve memory failed when opening buffer. */
  size_t open_buffer_memory_evict{0};
  /** Evict because reserve slot failed when opening buffer. */
  size_t open_buffer_slot_evict{0};
  /** Try to evict. */
  size_t try_evict{0};
  /** Lock failed when trying evict. */
  size_t try_evict_lock_failed{0};
};
extern Fifo_cache_status fifo_cache_status;

/*******************************************/
/** Consensus flusher status */
/*******************************************/
struct Flusher_status {
  /** Total trx handled by Consensus_log_flusher. */
  size_t trx{0};
  /** Trx is too large so must be splited. */
  size_t split_trx{0};
  /** Event is too large so must be splited. */
  size_t split_event{0};
};
extern Flusher_status flusher_status;

/*******************************************/
/** Consensus status */
/*******************************************/
extern SHOW_VAR consensus_status[];
}

#endif
