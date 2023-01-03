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

/** @file include/lizard0scn0hist.cc
 Lizard INNODB_FLASHBACK_SNAPSHOT system table.

 Created 2020-09-22 by Jianwei.zhao
 *******************************************************/

#include "sql_thd_internal_api.h"

#include "lizard0scn0hist.h"
#include "lizard0undo.h"
#include "sql/dd/types/object_table.h"
#include "sql/dd/types/object_table_definition.h"

#include "lizard0scn.h"
#include "lizard0sys.h"
#include "pars0pars.h"
#include "que0que.h"
#include "srv0srv.h"
#include "trx0trx.h"
#include "row0sel.h"
#include "dict0dd.h"

#ifdef UNIV_PFS_THREAD
mysql_pfs_key_t scn_history_thread_key;
#endif

namespace lizard {

static bool scn_history_start_shutdown = false;

os_event_t scn_history_event = nullptr;

/** SCN rolling forward interval */
ulint srv_scn_history_interval = 3;

/** SCN rolling forward task */
bool srv_scn_history_task_enabled = true;

/** The max time of scn history record keeped */
ulint srv_scn_history_keep_days = 7;

/** Create INNODB_FLASHBACK_SNAPSHOT table */
dd::Object_table *create_innodb_scn_hist_table() {
  dd::Object_table *tbl = nullptr;
  dd::Object_table_definition *def = nullptr;

  tbl = dd::Object_table::create_object_table();
  ut_ad(tbl);

  tbl->set_hidden(false);
  def = tbl->target_table_definition();
  def->set_table_name(SCN_HISTORY_TABLE_NAME);
  def->add_field(0, "scn", "scn BIGINT UNSIGNED NOT NULL");
  def->add_field(1, "utc", "utc BIGINT UNSIGNED NOT NULL");
  def->add_field(2, "memo", "memo TEXT");
  /** SCN as primary key */
  def->add_index(0, "index_pk", "PRIMARY KEY(scn)");
  /** UTC index to speed lookup*/
  def->add_index(1, "index_utc", "index(utc)");

  return tbl;
}

/**
  Interpret the SCN and UTC from select node every record.

  @param[in]      node      SEL_NODE_T
  @param[in/out]  result    SCN_TRANFORM_RESULT_T

  @retval         true      Unused
*/
static ibool srv_fetch_scn_history_step(void *node_void,
                                        void *result_void) {
  sel_node_t *node = (sel_node_t *)node_void;
  scn_transform_result_t *result = (scn_transform_result_t *)(result_void);
  que_common_t *cnode;
  int i;
  ut_ad(result->state == SCN_TRANSFORM_STATE::NOT_FOUND);

  result->state = SCN_TRANSFORM_STATE::SUCCESS;

  /* this should loop exactly 2 times for scn and utc  */
  for (cnode = static_cast<que_common_t *>(node->select_list), i = 0;
       cnode != NULL;
       cnode = static_cast<que_common_t *>(que_node_get_next(cnode)), i++) {
    const byte *data;
    dfield_t *dfield = que_node_get_val(cnode);
    dtype_t *type = dfield_get_type(dfield);
    ulint len = dfield_get_len(dfield);

    data = static_cast<const byte *>(dfield_get_data(dfield));

    switch (i) {
      case 0:
        ut_a(dtype_get_mtype(type) == DATA_INT);
        ut_a(len == 8);

        result->scn = mach_read_from_8(data);
        break;
      case 1:
        ut_a(dtype_get_mtype(type) == DATA_INT);
        ut_a(len == 8);

        result->utc = mach_read_from_8(data);
        break;
      default:
        ut_error;
    }
  }

  return TRUE;
}

/**
  Try the best to transform UTC to SCN,
  it will search the first record which is more than the utc.
  Pls confirm the result state to judge whether the tranformation is success,
  and also it can report the error to client user if required through
  result.err.

  @param[in]      utc       Measure by second.
  @retval         result
*/
scn_transform_result_t try_scn_transform_by_utc(const ulint utc) {
  scn_transform_result_t result;
  trx_t *trx;
  pars_info_t *pinfo;

  trx = trx_allocate_for_background();

  trx->isolation_level = TRX_ISO_READ_UNCOMMITTED;

  if (srv_read_only_mode) {
    trx_start_internal_read_only(trx);
  } else {
    trx_start_internal(trx);
  }

  pinfo = pars_info_create();

  pars_info_add_ull_literal(pinfo, "utc", utc);

  pars_info_bind_function(pinfo, "fetch_scn_history_step",
                          srv_fetch_scn_history_step, &result);

  result.err = que_eval_sql(pinfo,
                            "PROCEDURE FETCH_SCN_HISTORY () IS\n"
                            "DECLARE FUNCTION fetch_scn_history_step;\n"
                            "DECLARE CURSOR scn_history_cur IS\n"
                            "  SELECT\n"
                            "  scn,\n"
                            "  utc \n"
                            "  FROM \"" SCN_HISTORY_TABLE_FULL_NAME
                            "\"\n"
                            "  WHERE\n"
                            "  utc >= :utc\n"
                            "  ORDER BY utc ASC;\n"

                            "BEGIN\n"

                            "OPEN scn_history_cur;\n"
                            "FETCH scn_history_cur INTO\n"
                            "  fetch_scn_history_step();\n"
                            "IF (SQL % NOTFOUND) THEN\n"
                            "  CLOSE scn_history_cur;\n"
                            "  RETURN;\n"
                            "END IF;\n"
                            "CLOSE scn_history_cur;\n"
                            "END;\n",
                            FALSE, trx);

  /* pinfo is freed by que_eval_sql() */

  trx_commit_for_mysql(trx);

  trx_free_for_background(trx);

  return result;
}

/**
  Rolling forward the SCN every SRV_SCN_HISTORY_INTERVAL.
*/
static dberr_t roll_forward_scn() {
  pars_info_t *pinfo;
  dberr_t ret;
  ulint keep;
  ulint utc;
  scn_t scn;
  trx_t *trx;

  lizard_sys_scn_mutex_enter();
  scn = lizard_sys->scn.new_commit_scn(GCN_NULL).first.scn;
  lizard_sys_scn_mutex_exit();

  utc = ut_time_system_us() / 1000000;

  keep = utc - (srv_scn_history_keep_days * 24 * 60 * 60);

  trx = trx_allocate_for_background();
  if (srv_read_only_mode) {
    trx_start_internal_read_only(trx);
  } else {
    trx_start_internal(trx);
  }

  pinfo = pars_info_create();

  pars_info_add_ull_literal(pinfo, "keep", keep);
  pars_info_add_ull_literal(pinfo, "scn", scn);
  pars_info_add_ull_literal(pinfo, "utc", utc);
  pars_info_add_str_literal(pinfo, "memo", "");

  ret = que_eval_sql(pinfo,
                     "PROCEDURE ROLL_FORWARD_SCN() IS\n"
                     "BEGIN\n"

                     "DELETE FROM \"" SCN_HISTORY_TABLE_FULL_NAME
                     "\"\n"
                     "WHERE\n"
                     "utc < :keep; \n"

                     "INSERT INTO \"" SCN_HISTORY_TABLE_FULL_NAME
                     "\"\n"
                     "VALUES\n"
                     "(\n"
                     ":scn,\n"
                     ":utc,\n"
                     ":memo\n"
                     ");\n"
                     "END;",
                     FALSE, trx);

  if (ret == DB_SUCCESS) {
    trx_commit_for_mysql(trx);
  } else {
    trx->op_info = "rollback of internal trx on rolling forward SCN";
    trx_rollback_to_savepoint(trx, NULL);
    trx->op_info = "";
    ut_a(trx->error_state == DB_SUCCESS);
  }

  trx_free_for_background(trx);

  return ret;
}

/** Start the background thread of scn rolling forward */
void srv_scn_history_thread(void) {
  dberr_t err;
  THD *thd = create_thd(false, true, true, 0);

  if (dict_sys->scn_hist == nullptr) {
    dict_sys->scn_hist = dd_table_open_on_name(
        thd, NULL, SCN_HISTORY_TABLE_FULL_NAME, false, DICT_ERR_IGNORE_NONE);
  }

  ut_a(dict_sys->scn_hist != nullptr);

  while (!scn_history_start_shutdown) {
    os_event_wait_time(scn_history_event, srv_scn_history_interval * 1000000);

    Undo_retention::instance()->refresh_stat_data();

    if (srv_scn_history_task_enabled)
      err = roll_forward_scn();
    else
      err = DB_SUCCESS;

    if (err != DB_SUCCESS) {
      ib::error(ER_ROLL_FORWARD_SCN)
          << "Cannot rolling forward scn and save into"
          << SCN_HISTORY_TABLE_FULL_NAME << ": " << ut_strerr(err);
    }

    if (scn_history_start_shutdown) break;

    os_event_reset(scn_history_event);
  }

  destroy_thd(thd);
}

/** Init the background thread attributes */
void srv_scn_history_thread_init() { scn_history_event = os_event_create(0); }

/** Deinit the background thread attributes */
void srv_scn_history_thread_deinit() {
  ut_a(!srv_read_only_mode);
  ut_ad(!srv_thread_is_active(srv_threads.m_scn_hist));

  os_event_destroy(scn_history_event);
  scn_history_event = nullptr;
  scn_history_start_shutdown = false;
}

/** Shutdown the background thread of scn rolling forward */
void srv_scn_history_shutdown() {
  scn_history_start_shutdown = true;
  os_event_set(scn_history_event);
  srv_threads.m_scn_hist.join();
}

}  // namespace lizard
