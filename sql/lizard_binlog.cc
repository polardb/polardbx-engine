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

/** @file sql/lizard_binlog.h

  Binlog behaviours related to lizard system.

  Created 2023-06-20 by Jianwei.zhao
 *******************************************************/
#include "sql/binlog.h"
#include "sql/mysqld.h" //innodb_hton

#include "lizard_iface.h"
#include "sql/gcn_log_event.h"
#include "sql/lizard_binlog.h"
#include "sql/lizard0handler.h" // load_gcn...

bool Gcn_manager::assign_gcn_to_flush_group(THD *first_seen) {
  bool err = false;

  for (THD *head = first_seen; head; head = head->next_to_commit) {
    if (head->owned_commit_gcn.is_empty()) {
      head->owned_commit_gcn.set(innodb_hton->ext.load_gcn(),
                                 MYSQL_CSR_AUTOMATIC);
    }
  }
  return err;
}

namespace lizard {
namespace xa {
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
}  // namespace xa
}  // namespace lizard
