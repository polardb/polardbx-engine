
#include "ha_innodb.h"
#include "row0mysql.h"
#include "sql/table.h"
#include "trx0purge.h"

#include "lizard0gcs.h"
#include "lizard0gcs0hist.h"
#include "lizard0mysql.h"
#include "sql/lizard/lizard_snapshot.h"
#include "sql/sql_class.h"

/* To get current session thread default THD */
THD *thd_get_current_thd();

namespace lizard {

/** Whether to enable use as of query (true by default) */
bool srv_flashback_query_enable = true;

static utc_t utc_distance(utc_t x, utc_t y) { return x > y ? x - y : y - x; }

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

    /* case 3: can't find the appropriate records in innodb_flashback_snapshot
     */
    *err = DB_SNAPSHOT_OUT_OF_RANGE;
  } else if (utc_distance(trans_res.utc, user_utc) >
             2 * SRV_SCN_HISTORY_INTERVAL_MAX) {
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

int convert_timestamp_to_scn(THD *thd, utc_t utc, scn_t *scn) {
  dberr_t err;

  if (!srv_flashback_query_enable) {
    *scn = SCN_MAX;
    return 0;
  }

  *scn = convert_timestamp_to_scn_low(utc, &err);

  return convert_error_code_to_mysql(err, 0, thd);
}

/**
  Get Snapshot_vison from prebuilt

  @param[in]        prebuilt    row_prebuilt_t

  @return           snapshot_vision if has,
                    nullptr if hasn't
*/
const lizard::Snapshot_vision *row_prebuilt_get_snapshot_vision(
    const row_prebuilt_t *prebuilt) {
  TABLE *mysql_table;
  mysql_table = prebuilt->m_mysql_table;
  /* If it's not a flash back query, just return */
  if (!mysql_table || !mysql_table->table_snapshot.is_vision()) return nullptr;

  return mysql_table->table_snapshot.vision();
}

/**
  Try to convert flashback query in mysql to as-of query in innodb.

  @param[in/out]    prebuilt    row_prebuilt_t::m_scn_query could be modified

  @return           dberr_t     DB_SUCCESS if convert successfully or it's not
                                a flashback query.
                                ERROR: DB_AS_OF_INTERNAL,
  DB_SNAPSHOT_OUT_OF_RANGE, DB_AS_OF_TABLE_DEF_CHANGED, DB_SNAPSHOT_TOO_OLD
*/
dberr_t row_prebuilt_bind_flashback_query(row_prebuilt_t *prebuilt) {
  const Snapshot_vision *snapshot_vision = nullptr;
  ut_ad(prebuilt);

  /* forbid as-of query */
  if (!srv_flashback_query_enable) return DB_SUCCESS;

  /* scn query context should never set twice */
  if (prebuilt->m_asof_query.is_assigned()) return DB_SUCCESS;

  /* If it's not a flash back query, just return */
  if ((snapshot_vision = row_prebuilt_get_snapshot_vision(prebuilt)) == nullptr) {
    return DB_SUCCESS;
  }

  if (snapshot_vision->get_flashback_area()) {
    ut_ad(prebuilt->table->is_2pp);
    generic_stats.flashback_area_query_cnt.inc();
  }

  if (snapshot_vision->too_old()) {
    return DB_SNAPSHOT_TOO_OLD;
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
dberr_t row_prebuilt_unbind_flashback_query(row_prebuilt_t *prebuilt) {
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
