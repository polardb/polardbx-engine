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

/**
  @file

  Extends the binlog code for AliSQL
*/
#include "sql/binlog_ext.h"
#include <sstream>
#include "sql/dd/dd_minor_upgrade.h"
#include "sql/mysqld.h"
#include "sql/opt_costconstantcache.h"
#include "sql/rpl_info_factory.h"
#include "sql/transaction.h"
#include "sql/tztime.h"
#include "sql/xa_ext.h"

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
