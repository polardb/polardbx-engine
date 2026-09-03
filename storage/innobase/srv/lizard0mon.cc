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

#include "lizard0gcs.h"
#include "lizard0mon.h"
#include "lizard0undo.h"
#include "lizard0undo0retent.h"

#include "dict0mem.h"

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
  generic_vars.pread_cleanout_clean = generic_stats.pread_cleanout_clean;
  generic_vars.commit_cleanout_clean = generic_stats.commit_cleanout_clean;

  generic_vars.current_gcn = gcs_load_gcn();

  generic_vars.purged_gcn = gcs_clean_purged_gcn();

  generic_vars.retention_gcn = Undo_manager::instance().get_retention_gcn();

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

  generic_vars.index_pcur_guess_restore_hit =
      generic_stats.index_pcur_guess_restore_hit;

  generic_vars.index_pcur_guess_restore_miss =
      generic_stats.index_pcur_guess_restore_miss;

  generic_vars.flashback_area_query_cnt = generic_stats.flashback_area_query_cnt;

  generic_vars.row_prev_vers_build_cnt = generic_stats.row_prev_vers_build_cnt;

  generic_vars.txn_read_guess_request = generic_stats.txn_read_guess_request;
  generic_vars.txn_read_guess_fail = generic_stats.txn_read_guess_fail;

  generic_vars.tcn_cache_hit = lizard::gcs->ccache.hit_counter();
  generic_vars.tcn_cache_miss = lizard::gcs->ccache.miss_counter();
  generic_vars.tcn_cache_write = lizard::gcs->ccache.write_counter();

  generic_vars.query_wait_count = generic_stats.query_wait_count;
  generic_vars.query_wait_time = generic_stats.query_wait_time;
  generic_vars.query_wait_timeout = generic_stats.query_wait_timeout;

  generic_vars.polarx_btree_search_mode_hit =
      generic_stats.polarx_btree_search_mode_hit;
  generic_vars.polarx_btree_search_mode_miss =
      generic_stats.polarx_btree_search_mode_miss;

  generic_vars.n_purge_threads = current_n_purge_threads;

  generic_vars.rowid_intersection_gpp_hit =
      generic_stats.rowid_intersection_gpp_hit;
  generic_vars.rowid_intersection_gpp_miss =
      generic_stats.rowid_intersection_gpp_miss;

  generic_vars.rowid_union_gpp_hit = generic_stats.rowid_union_gpp_hit;
  generic_vars.rowid_union_gpp_miss = generic_stats.rowid_union_gpp_miss;

  generic_vars.index_merge_gpp_hit = generic_stats.index_merge_gpp_hit;
  generic_vars.index_merge_gpp_miss = generic_stats.index_merge_gpp_miss;

  generic_vars.multi_range_read_gpp_hit = generic_stats.multi_range_read_gpp_hit;
  generic_vars.multi_range_read_gpp_miss = generic_stats.multi_range_read_gpp_miss;

  generic_vars.filesort_gpp_hit = generic_stats.filesort_gpp_hit;
  generic_vars.filesort_gpp_miss = generic_stats.filesort_gpp_miss;

  generic_vars.multi_update_gpp_hit = generic_stats.multi_update_gpp_hit;
  generic_vars.multi_update_gpp_miss = generic_stats.multi_update_gpp_miss;
  generic_vars.multi_delete_gpp_hit = generic_stats.multi_delete_gpp_hit;
  generic_vars.multi_delete_gpp_miss = generic_stats.multi_delete_gpp_miss;

  generic_vars.lob_prealloc_pages = generic_stats.lob_prealloc_pages;
  generic_vars.lob_prealloc_fail_no_space =
      generic_stats.lob_prealloc_fail_no_space;
  generic_vars.lob_prealloc_reject_too_large =
      generic_stats.lob_prealloc_reject_too_large;
  generic_vars.lob_prealloc_underestimate_pages =
      generic_stats.lob_prealloc_underestimate_pages;
  generic_vars.lob_prealloc_overestimate_pages =
      generic_stats.lob_prealloc_overestimate_pages;
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

    {"pread_cleanout_clean", (char *)&generic_vars.pread_cleanout_clean,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"commit_cleanout_clean", (char *)&generic_vars.commit_cleanout_clean,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"current_gcn", (char *)&generic_vars.current_gcn, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"purged_gcn", (char *)&generic_vars.purged_gcn, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"retention_gcn", (char *)&generic_vars.retention_gcn, SHOW_LONG,
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

    {"index_pcur_guess_restore_hit",
     (char *)&generic_vars.index_pcur_guess_restore_hit, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"index_pcur_guess_restore_miss",
     (char *)&generic_vars.index_pcur_guess_restore_miss, SHOW_LONG,
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

    {"tcn_cache_write", (char *)&generic_vars.tcn_cache_write, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"query_wait_count", (char *)&generic_vars.query_wait_count, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"query_wait_time", (char *)&generic_vars.query_wait_time, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"query_wait_timeout", (char *)&generic_vars.query_wait_timeout, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"polarx_btree_search_mode_hit",
     (char *)&generic_vars.polarx_btree_search_mode_hit, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"polarx_btree_search_mode_miss",
     (char *)&generic_vars.polarx_btree_search_mode_miss, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"n_purge_threads", (char *)&generic_vars.n_purge_threads, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"rowid_intersection_gpp_hit",
     (char *)&generic_vars.rowid_intersection_gpp_hit, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"rowid_intersection_gpp_miss",
     (char *)&generic_vars.rowid_intersection_gpp_miss, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"rowid_union_gpp_hit", (char *)&generic_vars.rowid_union_gpp_hit,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"rowid_union_gpp_miss", (char *)&generic_vars.rowid_union_gpp_miss,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"index_merge_gpp_hit", (char *)&generic_vars.index_merge_gpp_hit,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"index_merge_gpp_miss", (char *)&generic_vars.index_merge_gpp_miss,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"multi_range_read_gpp_hit", (char *)&generic_vars.multi_range_read_gpp_hit,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"multi_range_read_gpp_miss",
     (char *)&generic_vars.multi_range_read_gpp_miss, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {"filesort_gpp_hit", (char *)&generic_vars.filesort_gpp_hit,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"filesort_gpp_miss", (char *)&generic_vars.filesort_gpp_miss,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"multi_update_gpp_hit", (char *)&generic_vars.multi_update_gpp_hit,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"multi_update_gpp_miss", (char *)&generic_vars.multi_update_gpp_miss,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"multi_delete_gpp_hit", (char *)&generic_vars.multi_delete_gpp_hit,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},
    {"multi_delete_gpp_miss", (char *)&generic_vars.multi_delete_gpp_miss,
     SHOW_LONG, SHOW_SCOPE_GLOBAL},

    {"lob_prealloc_pages", (char *)&generic_vars.lob_prealloc_pages, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"lob_prealloc_fail_no_space",
     (char *)&generic_vars.lob_prealloc_fail_no_space, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"lob_prealloc_reject_too_large",
     (char *)&generic_vars.lob_prealloc_reject_too_large, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"lob_prealloc_underestimate_pages",
     (char *)&generic_vars.lob_prealloc_underestimate_pages, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},
    {"lob_prealloc_overestimate_pages",
     (char *)&generic_vars.lob_prealloc_overestimate_pages, SHOW_LONG,
     SHOW_SCOPE_GLOBAL},

    {NullS, NullS, SHOW_LONG, SHOW_SCOPE_GLOBAL}};

int show_generic_vars(THD *thd, SHOW_VAR *var, char *buff) {
  export_lizard_status();

  var->type = SHOW_ARRAY;
  var->value = (char *)&lizard_status_variables;
  var->scope = SHOW_SCOPE_GLOBAL;

  return 0;
}

/** Visitor.
@param[in]      table   table to visit */
void Gpp_index_stat_collector::operator()(dict_table_t *table) {
  for (dict_index_t *index : table->indexes) {
    if (index->n_s_gfields == 0) {
      continue;
    }

    ut_ad(index->is_gstored());

    m_stats.emplace_back(im::Index_gpp_stat{table->name.m_name,
                                            (const char *)index->name,
                                            index->gpp_hit, index->gpp_miss});
  }
}

/** Visitor.
@param[in]      table   table to visit */
void Gpp_index_stat_flusher::operator()(dict_table_t *table) {
  for (dict_index_t *index : table->indexes) {
    if (index->n_s_gfields == 0) {
      continue;
    }

    ut_ad(index->is_gstored());
    index->gpp_hit.reset();
    index->gpp_miss.reset();
  }
}

}  // namespace lizard
