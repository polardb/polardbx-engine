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

  The code for alisql
*/

#include "ha_innodb_ext.h"
#include <cstdio>
#include "mach0data.h"
#include "mtr0mtr.h"
#include "trx0sys.h"
#include "trx0types.h"

#include "lizard0xa.h"
#include "lizard0undo.h"
#include "ha_innodb.h"
#include "lizard0sys.h"

#include <sql_class.h>

/**
  Compare whether the xid in thd is the same as the xid in trx (and aslo in
  undo_ptr).

  @params[in]   thd   THD
  @params[in]   trx   trx_t

  @retval true if the xid in thd is the same as the xid in trx.
*/
bool xa_compare_xid_between_thd_and_trx(const THD *thd, const trx_t *trx) {
  XID xid_in_thd;
  thd_get_xid(thd, (MYSQL_XID *)(&xid_in_thd));

  ut_ad(trx_is_registered_for_2pc(trx));
  ut_a(!trx->internal);
  ut_a(trx->mysql_thd == thd);

  if (thd->get_transaction()->xid_state()->check_in_xa(false)) {
    ut_a(trx_is_started(trx));
    if (lizard::trx_is_txn_rseg_updated(trx)) {
      ut_a(!trx->rsegs.m_txn.xid.is_null());
      ut_a(trx->xid->eq(&trx->rsegs.m_txn.xid));
      ut_ad(trx->xid->eq(&xid_in_thd));
    }
  }

  return true;
}
