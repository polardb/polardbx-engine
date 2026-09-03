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

/** @file sql/consensus/komodo0mon.cc
  Komodo stauts.

 Created 2025-11-18 by Jiyang.zhang
 *******************************************************/

#include "sql/komodo0mon.h"
#include "sql/sql_base.h"
#include "sql/komodo0xlog.h"

namespace komodo {

/*******************************************/
/** Fifo cache status */
/*******************************************/
Fifo_cache_status fifo_cache_status;

static SHOW_VAR consensus_fifo_cache_status[] = {
  {"used_size", (char *)&fifo_cache_status.used_size, SHOW_LONGLONG,
   SHOW_SCOPE_GLOBAL},
  {"first_index", (char *)&fifo_cache_status.first_index, SHOW_LONGLONG,
   SHOW_SCOPE_GLOBAL},
  {"log_count", (char *)&fifo_cache_status.log_count, SHOW_LONGLONG,
   SHOW_SCOPE_GLOBAL},
  {"read_requests", (char *)&fifo_cache_status.read_requests, SHOW_LONGLONG,
   SHOW_SCOPE_GLOBAL},
  {"read_miss", (char *)&fifo_cache_status.read_miss, SHOW_LONGLONG,
   SHOW_SCOPE_GLOBAL},
  {"malloc", (char *)&fifo_cache_status.malloc_total, SHOW_LONGLONG,
   SHOW_SCOPE_GLOBAL},
  {"open_buffer", (char *)&fifo_cache_status.open_buffer,
   SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},
  {"open_buffer_memory_evict",
   (char *)&fifo_cache_status.open_buffer_memory_evict, SHOW_LONGLONG,
   SHOW_SCOPE_GLOBAL},
  {"open_buffer_slot_evict",
   (char *)&fifo_cache_status.open_buffer_slot_evict, SHOW_LONGLONG,
   SHOW_SCOPE_GLOBAL},
  {"try_evict", (char *)&fifo_cache_status.try_evict, SHOW_LONGLONG,
   SHOW_SCOPE_GLOBAL},
  {"try_evict_lock_failed",
   (char *)&fifo_cache_status.try_evict_lock_failed, SHOW_LONGLONG,
   SHOW_SCOPE_GLOBAL},

  {NullS, NullS, SHOW_LONG, SHOW_SCOPE_GLOBAL}
};

static int show_consensus_fifo_status(THD *, SHOW_VAR *var, char *) {
  Fifo_cache *cache_mgr = komodo::xlog_engine_get_cache_manager();
  if (cache_mgr) {
    cache_mgr->export_status(&fifo_cache_status);
  }

  var->type = SHOW_ARRAY;
  var->value = (char *)&consensus_fifo_cache_status;
  var->scope = SHOW_SCOPE_GLOBAL;

  return 0;
}

/*******************************************/
/** Consensus flusher status */
/*******************************************/
Flusher_status flusher_status;

static SHOW_VAR consensus_flusher_status[] = {
  {"trx", (char *)&flusher_status.trx, SHOW_LONGLONG,
   SHOW_SCOPE_GLOBAL},
  {"split_trx", (char *)&flusher_status.split_trx, SHOW_LONGLONG,
   SHOW_SCOPE_GLOBAL},
  {"split_event", (char *)&flusher_status.split_event, SHOW_LONGLONG,
   SHOW_SCOPE_GLOBAL},

  {NullS, NullS, SHOW_LONG, SHOW_SCOPE_GLOBAL}
};

/*******************************************/
/** Consensus status */
/*******************************************/
SHOW_VAR consensus_status[] = {
  {"fifo_cache", (char *)&show_consensus_fifo_status, SHOW_FUNC,
   SHOW_SCOPE_GLOBAL},
  {"flusher", (char *)consensus_flusher_status, SHOW_ARRAY, SHOW_SCOPE_ALL},
  {NullS, NullS, SHOW_LONG, SHOW_SCOPE_GLOBAL}
};
}


