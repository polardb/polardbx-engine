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

/** @file include/lizard0xa0iface.h
  Lizard XA transaction structure for SQL server.

 Created 2023-02-26 by zanye.zjy
 *******************************************************/

#ifndef lizard0xa0iface_h
#define lizard0xa0iface_h

#include <cstdint>

#include "sql/handler.h"

namespace lizard {
namespace xa {

/** Transaction slot address. */
typedef uint64_t TSA;

enum Transaction_state {
  TRANS_STATE_COMMITTED = 0,
  TRANS_STATE_ROLLBACK = 1,
  TRANS_STATE_UNKNOWN = 2,
};

struct Transaction_info {
  Transaction_state state;
  uint64_t gcn;
};

/**
  1. start trx in innodb
  2. register innodb as a participants

  return true if error.
*/
bool start_and_register_rw_trx_for_xa(THD *thd);

/**
  Find transactions in the finalized state by GTRID.

  @params[in] in_gtrid          gtird
  @params[in] in_len            length
  @param[out] Transaction_info  Corresponding transaction info

  @retval     true if the corresponding transaction is found, false otherwise.
*/
bool trx_slot_get_trx_info_by_gtrid(const char *gtrid, unsigned len,
                                    Transaction_info *info);

/** trans state to message string. */
const char *trx_slot_trx_state_to_str(const enum Transaction_state state);

/**
  Alloc transaction slot in innodb

  return true if error
*/
bool trx_slot_assign_for_xa(THD *thd, TSA *tsa);

/**
  1. Update heartbeat timestamp to stop freezing purge system.
  2. Update flag to stop freezing operation.
*/
void hb_freezer_heartbeat();

/**
  Check if the purge system is freezing because heartbeat timeout.
*/
bool hb_freezer_is_freeze();
}  // namespace xa
}  // namespace lizard

#endif
