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

/** @file include/lizard0mon.h
  Lizard monitor metrics.

 Created 2020-06-03 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0mon_h
#define lizard0mon_h

#include "univ.i"

#include "srv0srv.h"

class THD;
struct SHOW_VAR;

namespace lizard {

struct generic_vars_t {
  /** txn undo log segment request count */
  ulint txn_undo_log_request;

  /** txn undo log segment reuse count */
  ulint txn_undo_log_reuse;

  /** txn undo log segment get from rseg free list count */
  ulint txn_undo_log_free_list_get;

  /** txn undo log segment put into rseg free list count */
  ulint txn_undo_log_free_list_put;

  /** txn undo log segment create count */
  ulint txn_undo_log_create;

  /** txn undo log hash count */
  ulint txn_undo_log_hash_element;

  /** txn undo log hash hit count */
  ulint txn_undo_log_hash_hit;

  /** txn undo log hash miss count */
  ulint txn_undo_log_hash_miss;

  ulint txn_undo_lost_page_miss_when_safe;

  ulint txn_undo_lost_magic_number_wrong;

  ulint txn_undo_lost_ext_flag_wrong;

  ulint txn_undo_lost_trx_id_mismatch;

  ulint txn_undo_lookup_by_uba;

  ulint cleanout_cursor_restore_fail;

  ulint scan_cleanout_txn_clean;
  ulint scan_cleanout_gpp_clean;
  ulint ddl_cleanout_clean;
  ulint commit_cleanout_clean;

  /*Max commit gcn. */
  ulint current_gcn;

  /* Max purged gcn, snapshot gcn before that is too old to asof select. */
  ulint purged_gcn;

  /** txn undo log segment put into rseg cached list */
  ulint txn_undo_log_recycle;

  /** Search commit snapshot through scn */
  ulint commit_snapshot_scn_search_hit;
  /** Search commit snapshot through gcn */
  ulint commit_snapshot_gcn_search_hit;

  /** The count of successful clustered index record inferences during the scan.
   */
  ulint index_scan_guess_clust_hit;
  /** The count of failed clustered index record inferences during the scan. */
  ulint index_scan_guess_clust_miss;

  /** The count of successful clustered index record inferences during the
   * purge. */
  ulint index_purge_guess_clust_hit;
  /** The count of failed clustered index record inferences during the purge. */
  ulint index_purge_guess_clust_miss;

  /** The count of successful clustered index record inferences during locking.
   */
  ulint index_lock_guess_clust_hit;
  /** The count of failed clustered index record inferences during locking. */
  ulint index_lock_guess_clust_miss;

  /** Number of queries via flashback area. */
  ulint flashback_area_query_cnt;

  /** Number of previous version built. */
  ulint row_prev_vers_build_cnt;

  ulint txn_read_guess_request;
  ulint txn_read_guess_fail;

  ulint tcn_cache_hit;
  ulint tcn_cache_miss;
};

struct generic_stats_t {
  // typedef ib_counter_t<ulint, 64> ulint_ctr_64_t;
  typedef ib_counter_t<ulint, 1, single_indexer_t> ulint_ctr_1_t;

  /** txn undo log segment request count */
  ulint_ctr_1_t txn_undo_log_request;

  /** txn undo log segment reuse count */
  ulint_ctr_1_t txn_undo_log_reuse;

  /** txn undo log segment get from rseg free list count */
  ulint_ctr_1_t txn_undo_log_free_list_get;

  /** txn undo log segment put into rseg free list count */
  ulint_ctr_1_t txn_undo_log_free_list_put;

  /** txn undo log segment create count */
  ulint_ctr_1_t txn_undo_log_create;

  /** txn undo log hash count */
  ulint_ctr_1_t txn_undo_log_hash_element;

  /** txn undo log hash hit count */
  ulint_ctr_1_t txn_undo_log_hash_hit;

  /** txn undo log hash miss count */
  ulint_ctr_1_t txn_undo_log_hash_miss;

  /** txn undo lost when missing corresponding pages when cleanout safe mode */
  ulint_ctr_1_t txn_undo_lost_page_miss_when_safe;

  /** txn undo lost because magic number is wrong */
  ulint_ctr_1_t txn_undo_lost_magic_number_wrong;

  /** txn undo lost because ext flag is wrong */
  ulint_ctr_1_t txn_undo_lost_ext_flag_wrong;

  /** txn undo lost because trx_id is mismatch */
  ulint_ctr_1_t txn_undo_lost_trx_id_mismatch;

  /** lookup scn by uba */
  ulint_ctr_1_t txn_undo_lookup_by_uba;

  ulint_ctr_1_t cleanout_cursor_restore_fail;

  ulint_ctr_1_t scan_cleanout_txn_clean;
  ulint_ctr_1_t scan_cleanout_gpp_clean;
  ulint_ctr_1_t ddl_cleanout_clean;
  ulint_ctr_1_t commit_cleanout_clean;

  ulint_ctr_1_t txn_undo_log_recycle;

  /** Search commit snapshot through scn */
  ulint_ctr_1_t commit_snapshot_scn_search_hit;

  /** Search commit snapshot through gcn */
  ulint_ctr_1_t commit_snapshot_gcn_search_hit;

  /** The count of successful clustered index record inferences during the scan.
   */
  ulint_ctr_1_t index_scan_guess_clust_hit;
  /** The count of failed clustered index record inferences during the scan. */
  ulint_ctr_1_t index_scan_guess_clust_miss;

  /** The count of successful clustered index record inferences during the
   * purge. */
  ulint_ctr_1_t index_purge_guess_clust_hit;
  /** The count of failed clustered index record inferences during the purge. */
  ulint_ctr_1_t index_purge_guess_clust_miss;

  /** The count of successful clustered index record inferences during locking.
   */
  ulint_ctr_1_t index_lock_guess_clust_hit;
  /** The count of failed clustered index record inferences during locking. */
  ulint_ctr_1_t index_lock_guess_clust_miss;

  ulint_ctr_1_t flashback_area_query_cnt;

  ulint_ctr_1_t row_prev_vers_build_cnt;

  ulint_ctr_1_t txn_read_guess_request;
  ulint_ctr_1_t txn_read_guess_fail;

  ulint_ctr_1_t tcn_cache_hit;
  ulint_ctr_1_t tcn_cache_miss;
};

extern bool stat_enabled;

extern generic_stats_t generic_stats;

int show_generic_vars(THD *thd, SHOW_VAR *var, char *buff);

/** The count of successful / failed  clustered index record inferences during
 * the scan. */
inline void index_scan_guess_clust_stat(bool hit) {
  if (!stat_enabled) return;

  if (hit) {
    generic_stats.index_scan_guess_clust_hit.inc();
  } else {
    generic_stats.index_scan_guess_clust_miss.inc();
  }
}

/** The count of successful / failed  clustered index record inferences during
 * the purge. */
inline void index_purge_guess_clust_stat(bool hit) {
  if (!stat_enabled) return;

  if (hit) {
    generic_stats.index_purge_guess_clust_hit.inc();
  } else {
    generic_stats.index_purge_guess_clust_miss.inc();
  }
}

/** The count of successful / failed  clustered index record inferences during
 * locking. */
inline void index_lock_guess_clust_stat(bool hit) {
  if (!stat_enabled) return;

  if (hit) {
    generic_stats.index_lock_guess_clust_hit.inc();
  } else {
    generic_stats.index_lock_guess_clust_miss.inc();
  }
}

inline void tcn_cache_stat(bool hit) {
  if (!stat_enabled) return;

  if (hit) {
    generic_stats.tcn_cache_hit.inc();
  } else {
    generic_stats.tcn_cache_miss.inc();
  }
}

inline void scan_cleanout_txn_clean_stat(ulint value) {
  if (value > 0 && stat_enabled) {
    generic_stats.scan_cleanout_txn_clean.add(value);
  }
}

inline void scan_cleanout_gpp_clean_stat(ulint value) {
  if (value > 0 && stat_enabled) {
    generic_stats.scan_cleanout_gpp_clean.add(value);
  }
}

inline void ddl_cleanout_clean_stat(ulint value) {
  if (value > 0 && stat_enabled) {
    generic_stats.ddl_cleanout_clean.add(value);
  }
}
inline void commit_cleanout_clean_stat(ulint value) {
  if (value > 0 && stat_enabled) {
    generic_stats.commit_cleanout_clean.add(value);
  }
}

inline void cleanout_cursor_restore_fail_stat() {
  generic_stats.cleanout_cursor_restore_fail.inc();
}

}  // namespace lizard

#endif  // lizard0mon_h
