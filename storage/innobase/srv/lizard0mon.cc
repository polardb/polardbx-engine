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

/** @file include/lizard0mon.cc
  Lizard monitor metrics.

 Created 2020-06-03 by Jianwei.zhao
 *******************************************************/

#include "mysql/status_var.h"

#include "lizard0fil0types.h"
#include "lizard0gcs.h"
#include "lizard0mon.h"
#include "lizard0undo.h"
#include "lizard0undo0retent.h"

namespace lizard {

bool stat_enabled = false;

/** Global monitor metrics export status temporary address */
static generic_vars_t generic_vars;

/** GLobal monitor metrics */
generic_stats_t generic_stats;

static void export_lizard_status(void) {
  generic_vars.txn_undo_log_request = generic_stats.txn_undo_log_request;
  generic_vars.txn_undo_log_reuse = generic_stats.txn_undo_log_reuse;

  generic_vars.txn_undo_log_free_list_get =
      generic_stats.txn_undo_log_free_list_get;

  generic_vars.txn_undo_log_free_list_put =
      generic_stats.txn_undo_log_free_list_put;

  generic_vars.txn_undo_log_create = generic_stats.txn_undo_log_create;

  generic_vars.txn_undo_log_hash_element =
      generic_stats.txn_undo_log_hash_element;
  generic_vars.txn_undo_log_hash_hit = generic_stats.txn_undo_log_hash_hit;
  generic_vars.txn_undo_log_hash_miss = generic_stats.txn_undo_log_hash_miss;

  generic_vars.txn_undo_lost_page_miss_when_safe =
      generic_stats.txn_undo_lost_page_miss_when_safe;

  generic_vars.txn_undo_lost_magic_number_wrong =
      generic_stats.txn_undo_lost_magic_number_wrong;

  generic_vars.txn_undo_lost_ext_flag_wrong =
      generic_stats.txn_undo_lost_ext_flag_wrong;

  generic_vars.txn_undo_lost_trx_id_mismatch =
      generic_stats.txn_undo_lost_trx_id_mismatch;

  generic_vars.txn_undo_lookup_by_uba = generic_stats.txn_undo_lookup_by_uba;

  generic_vars.cleanout_cursor_restore_fail =
      generic_stats.cleanout_cursor_restore_fail;

  generic_vars.scan_cleanout_txn_clean = generic_stats.scan_cleanout_txn_clean;
  generic_vars.scan_cleanout_gpp_clean = generic_stats.scan_cleanout_gpp_clean;
  generic_vars.ddl_cleanout_clean = generic_stats.ddl_cleanout_clean;
  generic_vars.commit_cleanout_clean = generic_stats.commit_cleanout_clean;

  generic_vars.current_gcn = gcs_load_gcn();

  generic_vars.purged_gcn = gcs_get_purged_gcn();

  generic_vars.txn_undo_log_recycle = generic_stats.txn_undo_log_recycle;

  generic_vars.commit_snapshot_scn_search_hit =
      generic_stats.commit_snapshot_scn_search_hit;

  generic_vars.commit_snapshot_gcn_search_hit =
      generic_stats.commit_snapshot_gcn_search_hit;

  generic_vars.index_scan_guess_clust_hit =
      generic_stats.index_scan_guess_clust_hit;

  generic_vars.index_scan_guess_clust_miss =
      generic_stats.index_scan_guess_clust_miss;

  generic_vars.index_purge_guess_clust_hit =
      generic_stats.index_purge_guess_clust_hit;

  generic_vars.index_purge_guess_clust_miss =
      generic_stats.index_purge_guess_clust_miss;

  generic_vars.index_lock_guess_clust_hit =
      generic_stats.index_lock_guess_clust_hit;

  generic_vars.index_lock_guess_clust_miss =
      generic_stats.index_lock_guess_clust_miss;

  generic_vars.flashback_area_query_cnt = generic_stats.flashback_area_query_cnt;

  generic_vars.row_prev_vers_build_cnt = generic_stats.row_prev_vers_build_cnt;

  generic_vars.txn_read_guess_request = generic_stats.txn_read_guess_request;
  generic_vars.txn_read_guess_fail = generic_stats.txn_read_guess_fail;

  generic_vars.tcn_cache_hit = generic_stats.tcn_cache_hit;
  generic_vars.tcn_cache_miss = generic_stats.tcn_cache_miss;
}

static SHOW_VAR lizard_status_variables[] = {
    {"txn_undo_log_request", (char *)&generic_vars.txn_undo_log_request,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_reuse", (char *)&generic_vars.txn_undo_log_reuse, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_free_list_get",
     (char *)&generic_vars.txn_undo_log_free_list_get, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_free_list_put",
     (char *)&generic_vars.txn_undo_log_free_list_put, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_create", (char *)&generic_vars.txn_undo_log_create,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_hash_element",
     (char *)&generic_vars.txn_undo_log_hash_element, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_hash_hit", (char *)&generic_vars.txn_undo_log_hash_hit,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_hash_miss", (char *)&generic_vars.txn_undo_log_hash_miss,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"txn_undo_lost_page_miss_when_safe",
     (char *)&generic_vars.txn_undo_lost_page_miss_when_safe, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_lost_magic_number_wrong",
     (char *)&generic_vars.txn_undo_lost_magic_number_wrong, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_lost_ext_flag_wrong",
     (char *)&generic_vars.txn_undo_lost_ext_flag_wrong, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_lost_trx_id_mismatch",
     (char *)&generic_vars.txn_undo_lost_trx_id_mismatch, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_lookup_by_uba", (char *)&generic_vars.txn_undo_lookup_by_uba,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"cleanout_cursor_restore_fail",
     (char *)&generic_vars.cleanout_cursor_restore_fail, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"scan_cleanout_txn_clean", (char *)&generic_vars.scan_cleanout_txn_clean,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"scan_cleanout_gpp_clean", (char *)&generic_vars.scan_cleanout_gpp_clean,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"ddl_cleanout_clean", (char *)&generic_vars.ddl_cleanout_clean,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"commit_cleanout_clean", (char *)&generic_vars.commit_cleanout_clean,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"undo_retention_stats", Undo_retention::status, SHOW_CHAR,
     SHOW_SCOPE_GLOBAL},

    {"current_gcn", (char *)&generic_vars.current_gcn, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"purged_gcn", (char *)&generic_vars.purged_gcn, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"txn_undo_log_recycle", (char *)&generic_vars.txn_undo_log_recycle,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"commit_snapshot_scn_search_hit",
     (char *)&generic_vars.commit_snapshot_scn_search_hit, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"commit_snapshot_gcn_search_hit",
     (char *)&generic_vars.commit_snapshot_gcn_search_hit, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"index_scan_guess_clust_hit",
     (char *)&generic_vars.index_scan_guess_clust_hit, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"index_scan_guess_clust_miss",
     (char *)&generic_vars.index_scan_guess_clust_miss, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"index_purge_guess_clust_hit",
     (char *)&generic_vars.index_purge_guess_clust_hit, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"index_purge_guess_clust_miss",
     (char *)&generic_vars.index_purge_guess_clust_miss, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"index_lock_guess_clust_hit",
     (char *)&generic_vars.index_lock_guess_clust_hit, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"index_lock_guess_clust_miss",
     (char *)&generic_vars.index_lock_guess_clust_miss, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"flashback_area_query_cnt", (char *)&generic_vars.flashback_area_query_cnt,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"row_prev_vers_build_cnt", (char *)&generic_vars.row_prev_vers_build_cnt,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"txn_read_guess_request", (char *)&generic_vars.txn_read_guess_request,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"txn_read_guess_fail", (char *)&generic_vars.txn_read_guess_fail,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"tcn_cache_hit", (char *)&generic_vars.tcn_cache_hit, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"tcn_cache_miss", (char *)&generic_vars.tcn_cache_miss, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {NullS, NullS, SHOW_LONG, SHOW_SCOPE_GLOBAL}};

int show_generic_vars(THD *thd, SHOW_VAR *var, char *buff) {
  export_lizard_status();

  var->type = SHOW_ARRAY;
  var->value = (char *)&lizard_status_variables;
  var->scope = SHOW_SCOPE_GLOBAL;

  return 0;
}

}  // namespace lizard
