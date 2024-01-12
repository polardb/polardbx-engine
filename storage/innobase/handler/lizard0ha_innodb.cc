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
#include "sql/package/proc_undo_purge.h"
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
  if (!trx_is_registered_for_2pc(trx) || !trx_is_started(trx) || trx->id == 0 ||
      trx->read_only) {
    return true;
  }

  ut_ad(!trx->internal);

  /** Force to assign a TXN. */
  if (lizard::trx_assign_txn_undo(trx, slot_ptr) != DB_SUCCESS) {
    return true;
  }

  return false;
}

static bool innobase_search_trx_by_xid(const XID *xid,
                                       lizard::xa::Transaction_info *info) {
  return lizard::xa::trx_search_by_xid(xid, info);
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

void innobase_get_undo_purge_status(im::Undo_purge_show_result *result) {
  lizard::Undo_retention::instance()->get_stat_data(
      &result->used_size, &result->file_size, &result->retained_time);
  purge_sys->blocked_stat.get(&result->blocked_cause, &result->blocked_utc);
  result->retention_time = lizard::Undo_retention::retention_time;
  result->reserved_size = lizard::Undo_retention::space_reserve;
  result->retention_size_limit = lizard::Undo_retention::space_limit;
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
  hton->ext.search_trx_by_xid = innobase_search_trx_by_xid;
  hton->ext.convert_timestamp_to_scn = innobase_conver_timestamp_to_scn;
  hton->ext.search_up_limit_tid_for_scn =
      innobase_search_up_limit_tid<lizard::Snapshot_scn_vision>;
  hton->ext.search_up_limit_tid_for_gcn =
      innobase_search_up_limit_tid<lizard::Snapshot_gcn_vision>;
  hton->ext.get_undo_purge_status = innobase_get_undo_purge_status;
}

enum_tx_isolation thd_get_trx_isolation(const THD *thd);

#include "ha_innodb.h"
#include "i_s.h"
#include "sync0sync.h"

/* for ha_innopart, Native InnoDB Partitioning. */
#include "ha_innopart.h"

#include "lizard0sample.h"
static dberr_t prebuilt_table_validity(row_prebuilt_t *prebuilt) {
  if (dict_table_is_discarded(prebuilt->table)) {
    return DB_TABLESPACE_DELETED;

  } else if (prebuilt->table->ibd_file_missing) {
    return DB_TABLESPACE_NOT_FOUND;

  } else if (!prebuilt->index_usable) {
    return DB_MISSING_HISTORY;

  } else if (prebuilt->index->is_corrupted()) {
    return DB_CORRUPTION;
  }

  return DB_SUCCESS;
}

int ha_innobase::lizard_sample_init(double sampling_percentage,
                                    int sampling_seed) {
  int err;

  ut_ad((table_share->primary_key == MAX_KEY) ==
        m_prebuilt->clust_index_was_generated);

  if (m_prebuilt->clust_index_was_generated) {
    err = change_active_index(MAX_KEY);
  } else {
    err = change_active_index(table_share->primary_key);
  }

  if (err != 0) {
    return (err);
  }

  return lizard_sample_init_low(sampling_percentage, sampling_seed);
}

int ha_innobase::lizard_sample_init_low(double sampling_percentage,
                                        int sampling_seed) {
  ut_a(sampling_percentage > 0.0);
  ut_a(sampling_percentage <= 100.0);

#ifndef POLARX_SAMPLE_TEST
  /* If in POLARX_SAMPLE_TEST, allow it. */
  if (dict_index_is_spatial(m_prebuilt->index) ||
      m_prebuilt->index->table->is_temporary()) {
    return HA_ERR_SAMPLE_WRONG_SEMANTIC;
  }
#endif

  dberr_t db_err;
  if ((db_err = prebuilt_table_validity(m_prebuilt)) != DB_SUCCESS) {
    return (convert_error_code_to_mysql(db_err, 0, ha_thd()));
  }

  /* Assign a trx_t. */
  update_thd();

  ut_a(m_prebuilt->trx);
  ut_a(m_prebuilt->index->is_clustered());
  ut_a(m_prebuilt->table->first_index() == m_prebuilt->index);
  ut_a(nullptr == m_sampler);

  m_sampler = lizard::create_sampler(sampling_seed, sampling_percentage,
                                     m_prebuilt->index);

  db_err = m_sampler->init(m_prebuilt->trx, m_prebuilt->index, m_prebuilt);

  if (db_err != DB_SUCCESS) {
    lizard_sample_end();
    return (convert_error_code_to_mysql(db_err, 0, ha_thd()));
  }

#ifdef POLARX_SAMPLE_TEST
  m_start_of_scan = true;
#endif
  return (0);
}

int ha_innobase::lizard_sample_next(uchar *buf) {
  dberr_t err = DB_SUCCESS;

  if (!srv_innodb_btree_sampling) {
    return HA_ERR_END_OF_FILE;
  }

#ifdef POLARX_SAMPLE_TEST
  if (m_start_of_scan && m_prebuilt->sql_stat_start &&
      !can_reuse_mysql_template()) {
    build_template(false);
  }
  m_start_of_scan = false;
#endif

  if ((err = prebuilt_table_validity(m_prebuilt)) == DB_SUCCESS) {
    ut_a(nullptr != m_sampler);

    /** Buffer rows one by one */
    err = m_sampler->next(buf);

    if (DB_END_OF_INDEX == err) {
      return HA_ERR_END_OF_FILE;
    }

    ut_ad(err == DB_SUCCESS);
  }

  return (convert_error_code_to_mysql(err, 0, ha_thd()));
}

int ha_innobase::lizard_sample_end() {
  if (m_sampler) {
    m_sampler->end();
    ut::delete_(m_sampler);
    m_sampler = nullptr;
  }

  return 0;
}

int ha_innopart::sample_init_in_part(uint part_id, bool scan) {
  int err;

  if (m_prebuilt->clust_index_was_generated) {
    err = change_active_index(part_id, MAX_KEY);
  } else {
    err = change_active_index(part_id, table_share->primary_key);
  }

  if (err) {
    return err;
  }

  ut_a(scan);
  // /* Don't use semi-consistent read in random row reads (by position).
  // This means we must disable semi_consistent_read if scan is false. */

  // if (!scan) {
  //   try_semi_consistent_read(false);
  // }

  return (ha_innobase::lizard_sample_init_low(m_sampling_percentage,
                                              m_sampling_seed));
}

int ha_innopart::sample_next_in_part(uint part_id, uchar *buf) {
  int error;

  DBUG_ENTER("ha_innopart::rnd_next_in_part");

  set_partition(part_id);

  error = ha_innobase::sample_next(nullptr, buf);

  update_partition(part_id);
  DBUG_RETURN(error);
}

int ha_innopart::sample_end_in_part(uint part_id, bool scan) {
  int error;

  ut_a(scan);
  if ((error = ha_innopart::rnd_end_in_part(part_id, scan))) {
    return error;
  }

  return ha_innobase::sample_end(nullptr);
}
