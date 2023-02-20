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

enum Transaction_state {
  TRANS_STATE_COMMITTED = 0,
  TRANS_STATE_ROLLBACK = 1,
};

struct Transaction_info {
  Transaction_state state;
  uint64_t gcn;
};

/**
  Find transactions in the finalized state by XID.

  @param[in]  xid          XID
  @param[out] txn_undo_hdr Corresponding txn undo header

  @retval     true if the corresponding transaction is found, false otherwise.
*/
bool get_transaction_info_by_xid(const XID *xid, Transaction_info *info);

/** trans state to message string. */
const char *transaction_state_to_str(const enum Transaction_state state);

}  // namespace xa
}  // namespace lizard

#endif
