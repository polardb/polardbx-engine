/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */


#include "sql/table.h"
#include "trx0purge.h"
#include "row0mysql.h"
#include "ha_innodb.h"

#include "lizard0mysql.h"
#include "lizard0scn0hist.h"
#include "lizard0gcs.h"
#include "sql/sql_class.h"
#include "sql/lizard/lizard_snapshot.h"


namespace lizard {

/** Whether to enable use as of query (true by default) */
bool srv_force_normal_query_if_fbq = true;

/** The max tolerable lease time of a snapshot */
ulint srv_scn_valid_volumn = 30;

static utc_t utc_distance(utc_t x, utc_t y) {
  return x > y ? x - y : y - x;
}

/**
  Try to convert timestamp to scn for flashback query.

  @param[in/out]    prebuilt    row_prebuilt_t::m_scn_query could be modified

  @return           dberr_t     DB_SUCCESS if convert successfully or it's not
                                a flashback query.
                                ERROR: DB_AS_OF_INTERNAL,
  DB_SNAPSHOT_OUT_OF_RANGE, DB_AS_OF_TABLE_DEF_CHANGED
*/
static scn_t convert_timestamp_to_scn_low(utc_t user_utc, dberr_t *err) {
  scn_t fbq_scn;
  utc_t cur_utc;
  scn_transform_result_t trans_res;
  ut_ad(err);

  trans_res = try_scn_transform_by_utc(user_utc);

  cur_utc = ut_time_system_us() / 1000000;

  fbq_scn = SCN_NULL;

  if (user_utc > cur_utc) {
    /* Case 1: query the snapshot in the future */
    *err = DB_SNAPSHOT_OUT_OF_RANGE;
  } else if (trans_res.err != DB_SUCCESS) {
    /** Case 2: Not likely, might be a fatal error in row_mysql_handle_errors
    if we just return trans_res.err. So we turn to as DB_AS_OF_INTERNAL */
    *err = DB_AS_OF_INTERNAL;
  } else if (trans_res.state == SCN_TRANSFORM_STATE::NOT_FOUND) {

    /** TODO: Check if it's a next interval query <10-10-20, zanye.zjy> */

    /* case 3: can't find the appropriate records in innodb_flashback_snapshot */
    *err = DB_SNAPSHOT_OUT_OF_RANGE;
  } else if (utc_distance(trans_res.utc, user_utc)
               > srv_scn_valid_volumn * 60) {
    /** case 4: The corresponding utc in innodb_flashback_snapshot is quite
    different from user_utc */
    *err = DB_SNAPSHOT_OUT_OF_RANGE;
  } else {
    /* normal case */
    fbq_scn = trans_res.scn;
    *err = DB_SUCCESS;
  }

  return fbq_scn;
}

int convert_timestamp_to_scn(THD *thd, my_utc_t utc, my_scn_t *scn) {
  dberr_t err;

  if (!srv_force_normal_query_if_fbq) {
    *scn = SCN_MAX;
    return 0;
  }

  *scn = convert_timestamp_to_scn_low(utc, &err);

  return convert_error_code_to_mysql(err, 0, thd);
}

/**
  Try to convert flashback query in mysql to as-of query in innodb.

  @param[in/out]    prebuilt    row_prebuilt_t::m_scn_query could be modified

  @return           dberr_t     DB_SUCCESS if convert successfully or it's not
                                a flashback query.
                                ERROR: DB_AS_OF_INTERNAL, DB_SNAPSHOT_OUT_OF_RANGE,
                                DB_AS_OF_TABLE_DEF_CHANGED, DB_SNAPSHOT_TOO_OLD
*/
dberr_t convert_fbq_ctx_to_innobase(row_prebuilt_t *prebuilt) {
  TABLE *table;
  dict_index_t *clust_index;
  Snapshot_vision *snapshot_vision = nullptr;
  ut_ad(prebuilt);

  table = prebuilt->m_mysql_table;

  /* forbid as-of query */
  if (!srv_force_normal_query_if_fbq) return DB_SUCCESS;

  /* scn query context should never set twice */
  if (prebuilt->m_asof_query.is_assigned()) return DB_SUCCESS;

#if defined TURN_MVCC_SEARCH_TO_AS_OF

  if (!table || !table->table_snapshot.is_vision()) {
    ut_ad(prebuilt->trx->vision.is_active());

    /* temporary table is not allowed using as-of query */
    if (prebuilt->index->table->is_temporary()) return DB_SUCCESS;

    prebuilt->trx->vision.set_as_of_scn(prebuilt->trx->vision.snapshot_scn());

    return DB_SUCCESS;
  }
#endif

  /* If it's not a flash back query, just return */
  if (!table || !table->table_snapshot.is_vision()) return DB_SUCCESS;

  snapshot_vision = table->table_snapshot.vision();

  DBUG_EXECUTE_IF("simulate_gcn_def_changed_error", { goto simulate_error; });

  if (snapshot_vision->too_old()) {
    return DB_SNAPSHOT_TOO_OLD;
  }

#ifdef UNIV_DEBUG
simulate_error:
#endif

  /* Check if it's a temporary table */
  if (prebuilt->index && prebuilt->index->table &&
      prebuilt->index->table->is_temporary()) {
    /** Something wrong must happen. */
    return DB_AS_OF_INTERNAL;
  }

  /** Check if there is a DDL before the moment of as-of query. Only check
  clustered index */
  clust_index = prebuilt->index->table->first_index();
  if (!clust_index->is_usable_as_of(prebuilt->trx, snapshot_vision)) {
    return DB_AS_OF_TABLE_DEF_CHANGED;
  }

  /* set as as-of query */
  prebuilt->m_asof_query.assign_vision(snapshot_vision);

  return DB_SUCCESS;
}

/**
  Reset row_prebuilt_t::m_scn_query, query block level.

  @param[in/out]    prebuilt    row_prebuilt_t

  @return           dberr_t     DB_SUCCESS, or DB_SNAPSHOT_OUT_OF_RANGE.
*/
dberr_t reset_prebuilt_flashback_query_ctx(row_prebuilt_t *prebuilt) {
  dberr_t err = DB_SUCCESS;

  if (prebuilt->m_asof_query.too_old()) {
    err = DB_SNAPSHOT_TOO_OLD;
  }

  prebuilt->m_asof_query.release_vision();

  DBUG_EXECUTE_IF("required_scn_purged_before_reset",
                  err = DB_SNAPSHOT_TOO_OLD;);
  return err;
}

}  // namespace lizard
