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
#include "lizard0gcs.h"
#include "lizard0ha_innodb.h"
#include "lizard0undo.h"
#include "lizard0xa.h"

#include <sql_class.h>
#include "sql/xa/lizard_xa_trx.h"

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
  1. start trx in innodb
  2. register hton as a participants

  return true if error.
*/
static bool innobase_start_trx_for_xa(handlerton *hton, THD *thd, bool rw) {
  trx_t *trx = check_trx_exists(thd);

  /** check_trx_exists will create trx if no trx. */
  ut_ad(trx);

  trx_start_if_not_started(trx, rw, UT_LOCATION_HERE);

  innobase_register_trx_only_trans(hton, thd, trx);

  thd->get_ha_data(hton->slot)->ha_info[1].set_trx_read_write();

  return false;
}

bool innobase_assign_slot_for_xa(THD *thd, my_slot_ptr_t *slot_ptr_arg) {
  slot_ptr_t *slot_ptr = static_cast<my_slot_ptr_t *>(slot_ptr_arg);
  trx_t *trx = check_trx_exists(thd);

  /** check_trx_exists will create trx if no trx. */
  ut_ad(trx);

  /** The trx must have been started as rw mode. */
  if (!trx_is_registered_for_2pc(trx) || !trx_is_started(trx) ||
      trx->id == 0 || trx->read_only) {
    return true;
  }

  ut_ad(!trx->internal);

  /** Force to assign a TXN. */
  if (lizard::trx_assign_txn_undo(trx, slot_ptr) != DB_SUCCESS) {
    return true;
  }

  return false;
}

static bool innobase_search_trx_by_gtrid(const char *gtrid, unsigned len,
                                         lizard::xa::Transaction_info *info) {
  return lizard::xa::trx_search_by_gtrid(gtrid, len, info);
}
template <typename T>
static my_trx_id_t innobase_search_up_limit_tid(const T &lhs) {
  return static_cast<my_trx_id_t>(lizard::gcs_search_up_limit_tid<T>(lhs));
}

template my_trx_id_t innobase_search_up_limit_tid<lizard::Snapshot_scn_vision>(
    const lizard::Snapshot_scn_vision &lhs);
template my_trx_id_t innobase_search_up_limit_tid<lizard::Snapshot_gcn_vision>(
    const lizard::Snapshot_gcn_vision &lhs);

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

bool innobase_snapshot_scn_too_old(my_scn_t scn) {
  return scn < purge_sys->purged_scn.load();
}

bool innobase_snapshot_automatic_gcn_too_old(my_gcn_t gcn) {
  return gcn < purge_sys->purged_gcn.get();
}

bool innobase_snapshot_assigned_gcn_too_old(my_gcn_t gcn) {
  return gcn <= purge_sys->purged_gcn.get();
}

void innobase_set_gcn_if_bigger(my_gcn_t gcn_arg) {
  gcn_t gcn = static_cast<gcn_t>(gcn_arg);
  lizard::gcs_set_gcn_if_bigger(gcn);
}

int innobase_conver_timestamp_to_scn(THD *thd, my_utc_t utc_arg,
		my_scn_t *scn_arg) {
  utc_t utc = static_cast<utc_t>(utc_arg);
  scn_t *scn = static_cast<scn_t *>(scn_arg);
  return lizard::convert_timestamp_to_scn(thd, utc, scn);
}

/**
  Initialize innobase extension.

  param[in]  innobase_hton  handlerton of innobase.
*/
void innobase_init_ext(handlerton *hton) {
  hton->ext.register_xa_attributes = innobase_register_xa_attributes;
  hton->ext.load_gcn = innobase_load_gcn;
  hton->ext.load_scn = innobase_load_scn;

  hton->ext.snapshot_scn_too_old = innobase_snapshot_scn_too_old;
  hton->ext.snapshot_assigned_gcn_too_old =
      innobase_snapshot_assigned_gcn_too_old;
  hton->ext.snapshot_automatic_gcn_too_old =
      innobase_snapshot_automatic_gcn_too_old;
  hton->ext.set_gcn_if_bigger = innobase_set_gcn_if_bigger;
  hton->ext.start_trx_for_xa = innobase_start_trx_for_xa;
  hton->ext.assign_slot_for_xa = innobase_assign_slot_for_xa;
  hton->ext.search_trx_by_gtrid = innobase_search_trx_by_gtrid;
  hton->ext.convert_timestamp_to_scn = innobase_conver_timestamp_to_scn;
  hton->ext.search_up_limit_tid_for_scn =
      innobase_search_up_limit_tid<lizard::Snapshot_scn_vision>;
  hton->ext.search_up_limit_tid_for_gcn =
      innobase_search_up_limit_tid<lizard::Snapshot_gcn_vision>;
}
