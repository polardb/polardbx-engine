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
#include "raft0xa.h"
#include "sql/handler.h"
#include "sql/xa.h"
#include "sql/sys_vars.h"
#include "sql/sql_plugin.h"

static bool xarollback_handlerton(THD *, plugin_ref plugin, void *arg) {
  handlerton *hton = plugin_data<handlerton *>(plugin);
  if (hton->state == SHOW_OPTION_YES && hton->recover) {
    xa_status_code ret = hton->rollback_by_xid(hton, (XID *)arg);

    /*
      Consider XAER_NOTA as success since not every storage should be
      involved into XA transaction, therefore absence of transaction
      specified by xid in storage engine doesn't mean that a real error
      happened. To illustrate it, lets consider the corner case
      when no one storage engine is involved into XA transaction:
      XA START 'xid1';
      XA END 'xid1';
      XA PREPARE 'xid1';
      XA COMMIT 'xid1';
      For this use case, handing of the statement XA COMMIT leads to
      returning XAER_NOTA by ha_innodb::commit_by_xid because there isn't
      a real transaction managed by innodb. So, there is no XA transaction
      with specified xid in resource manager represented by InnoDB storage
      engine although such transaction exists in transaction manager
      represented by mysql server runtime.
    */
    if (ret != XA_OK && ret != XAER_NOTA) {
      my_error(ER_XAER_RMERR, MYF(0));
      return true;
    }
    return false;
  }
  return false;
}

int ha_rollback_xids(XID *xid) {
  DBUG_ENTER("ha_rollback_xids");
  plugin_foreach_without_binlog(nullptr, xarollback_handlerton,
                                MYSQL_STORAGE_ENGINE_PLUGIN, xid);
  DBUG_RETURN(0);
}
