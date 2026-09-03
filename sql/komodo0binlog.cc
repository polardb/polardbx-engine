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

/** @file sql/komodo0binlog.cc

  Binlog behaviours related to komodo lizard system.

  Created 2023-06-20 by Jianwei.zhao
 *******************************************************/

#include "sql/komodo0binlog.h"
#include "sql/binlog.h"
#include "sql/xa/komodo0cmmt0policy.h"

namespace komodo {

bool opt_gcn_write_event = false;

bool assign_gcn_to_flush_group(THD *first_seen) {
  bool err = false;

  /**
    Some components, such as CDC, rely on the order of GCN and Binlog to meet
    certain conditions. For example, if the Binlog has the form:
    P1...C1...P2...C2, then it is assumed that the GCN of C1 must be less than
    C2. In regular TSO transactions, this is satisfied. This is because the
    timing is as follows:
    1.  XA PREPARE (P1)
    2.  GET GCN1
    3.1 XA COMMIT with GCN1 (C1)
    3.2 XA PREPARE (P2)
    4.  GET GCN2
    5.  XA COMMIT (C2)
    Step-3.1 and Step-3.2 can happen in any order, but Step-4 definitely occurs
    after Step-2.

    However, for Async Commit, the above conditions may not be met:
    1.  ac_prepare (P1 with PRE_GCN1 = 85)
    2.  decide PROPOSAL_GCN1 = max(PRE_GCN1 = 85, SYS_GCN = 90)
    3.1 ac_commit (C1 with GCN1 = 100), and SYS_GCN is not yet pushed up
    3.2 ac_prepare (P2 with PRE_GCN2 = 87)
    3.3 decide PROPOSAL_GCN2 = max(PRE_GCN2 = 87, SYS_GCN = 90)
    3.4 push up SYS_GCN = 100 because C1 (GCN1 = 100)
    4.  ac_commit (C2 with GCN2 = 87)
    Notes, 3.1-3.4 belong to the same BGC (Binlog Group Commit).
    The above situation obviously violates the preset assumptions because:
    GCN1 > GCN2

    Delaying the increase of SYS_GCN does not violate any distributed
    consistency guarantee, but in order to maintain the original assumption, the
    increase of SYS_GCN will be done when ac_commit deciding to ensure that the
    following conditions are met:
    GCN1 <= PROPOSAL_GCN2 <= GCN2

    More specifically, for Async Commit, the above process will become:
    1.  ac_prepare (P1 with PRE_GCN1 = 85)
    2.  decide PROPOSAL_GCN1 = max(PRE_GCN1 = 85, SYS_GCN = 90)
    3.1 ac_commit (C1 with GCN1 = 100), and SYS_GCN is also pushed up to 100
    3.2 ac_prepare (P2 with PRE_GCN2 = 87)
    3.3 decide PROPOSAL_GCN2 = max(PRE_GCN2 = 87, SYS_GCN = 100)
    4.  ac_commit (C2 with GCN2 >= 100)

  */
  for (THD *head = first_seen; head; head = head->next_to_commit) {
    commit_policy_decide(head, true);
  }
  return err;
}

int binlog_start_trans(THD *thd) {
  DBUG_TRACE;

  /*
    Initialize the cache manager if this was not done yet.
  */
  if (thd->binlog_setup_trx_data()) return 1;

  bool is_transactional = true;
  binlog_cache_mngr *cache_mngr = thd_get_cache_mngr(thd);
  binlog_cache_data *cache_data =
      cache_mngr->get_binlog_cache_data(is_transactional);

  register_binlog_handler(thd, thd->in_multi_stmt_transaction_mode());

  /*
    If the cache is empty log "BEGIN" at the beginning of every transaction.
    Here, a transaction is either a BEGIN..COMMIT/ROLLBACK block or a single
    statement in autocommit mode.
  */
  if (cache_data->is_binlog_empty()) {
    const char *query = nullptr;
    char buf[XID::ser_buf_size];
    char xa_start[sizeof("XA START") + 1 + sizeof(buf)];
    XID_STATE *xs = thd->get_transaction()->xid_state();
    int qlen = 0;

    if (is_transactional && xs->has_state(XID_STATE::XA_ACTIVE)) {
      /*
        XA-prepare logging case.
      */
      qlen = sprintf(xa_start, "XA START %s", xs->get_xid()->serialize(buf));
      query = xa_start;
    } else {
      /*
        Regular transaction case.
      */
      exec_binlog_error_action_abort(
          "Cannot use the function in non-XA mode. Something wrong must "
          "happen. Aborting the server.");
    }

    Query_log_event qinfo(thd, query, qlen, is_transactional, false, true, 0,
                          true);
    if (cache_data->write_event(&qinfo)) return 1;
  }

  return 0;
}

}  // namespace komodo
