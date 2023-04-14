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

#include <cstdio>
#include "mach0data.h"
#include "mtr0mtr.h"
#include "trx0sys.h"
#include "trx0types.h"

#include "ha_innodb.h"
#include "lizard0ha_innodb.h"
#include "lizard0gcs.h"
#include "lizard0undo.h"
#include "lizard0xa.h"

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

  if (thd->get_transaction()->xid_state()->check_in_xa(false)) {
    if (lizard::trx_is_txn_rseg_updated(trx)) {
      ut_a(!trx->internal);
      ut_a(trx->mysql_thd == thd);
      ut_a(!trx->rsegs.m_txn.xid_for_hash.is_null());
      ut_a(trx->xid->eq(&trx->rsegs.m_txn.xid_for_hash));
      ut_ad(trx->xid->eq(&xid_in_thd));
    }
  }

  return true;
}

/**
  Copy server XA attributes into innobase.

  @param[in]      thd       connection handler.
*/
static void innobase_register_xa_attributes(THD *thd) {
  trx_t *&trx = thd_to_trx(thd);
  ut_ad(trx != nullptr);

  if (!trx_is_registered_for_2pc(trx)) {
    /** Note: Other session will compare trx group when assign readview. */
    trx_mutex_enter(trx);

    thd_get_xid(thd, (MYSQL_XID *)trx->xad.my_xid());
    trx->xad.build_group();

    ut_ad(!trx->xad.is_null());

    trx_mutex_exit(trx);
  }
}

uint64 innobase_load_gcn() { return lizard::gcs_load_gcn(); }

uint64 innobase_load_scn() { return lizard::gcs_load_scn(); }

/**
  Initialize innobase extension.

  param[in]  innobase_hton  handlerton of innobase.
*/
void innobase_init_ext(handlerton *innobase_hton) {
  innobase_hton->ext.register_xa_attributes = innobase_register_xa_attributes;
  innobase_hton->ext.load_gcn = innobase_load_gcn;
  innobase_hton->ext.load_scn = innobase_load_scn;
}
