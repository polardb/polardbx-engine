/* Copyright (c) 2008, 2024, Alibaba and/or its affiliates. All rights reserved.

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

#ifndef LIZARD_XA_TRX_INCUDED
#define LIZARD_XA_TRX_INCUDED

#include "sql/lizard_binlog.h"
#include "sql/lizard/lizard_service.h"
#include "sql/xa.h"

class THD;
class TC_LOG;
struct LEX;

namespace lizard {
namespace xa {

/**
  Apply for a readwrite transaction specially for external xa through allocating
  transaction slot from transaction slot storage engine.

    1. start trx in transaction slot storage engine.[ttse]
    2. register ttse as a participants
    3. alloc transaction slot in ttse
    4. register binlog as another participants if need

  @param[in]	Thread handler
  @param[in]	XID
  @param[out]	transaction slot address
  @param[out]	transaction identifier
*/
extern bool apply_trx_for_xa(THD *thd, const XID *xid, slot_ptr_t *slot_ptr,
                             trx_id_t *trx_id);

/**
  To prepare xa group for an external xa transaction, innodb must be
  registered as a participant. After one xa branch has been prepared
  (also including commited), the xa group would be closed. Then other
  xa branches can not participate in the xa group. We do the check
  here.
  1. start trx in transaction slot storage engine.[ttse]
  2. register ttse as a participant
  3. register xa group and check if it has been closed.
  @param[in]	Thread handler
  @param[in]	XID
  @return true if error, otherwise false.
 */
extern bool prepare_xa_group_and_check(THD *thd, const XID *xid);

/**
  Decide proposal GCN through pre_commit_gcn and sys GCN,
  and set THD::owned_commit_gcn.
*/
extern void ac_proposal_gcn(THD *thd);

/**
  Search XA transaction info by XID.
  @param[in]    xid     XID
  @param[out]   info    XA info
*/
extern void search_trx_info(xid_t *xid, MyXAInfo *info,
                            const slot_ptr_t slot_ptr_hint);
}  // namespace xa

/** init server_start_time_for_txn, so the TXN can be kept for a while after
restart instance. */
extern void init_server_start_time_for_txn();

}  // namespace lizard

#endif  // XA_TRX_INCUDED
