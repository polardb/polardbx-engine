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

#include <vector>

#include "univ.i"

#include "komodo0handler0types.h"

#include "sql/package/gpp_stat.h"

class THD;
struct SHOW_VAR;
struct dict_table_t;

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
  ulint pread_cleanout_clean;
  ulint commit_cleanout_clean;

  /*Max commit gcn. */
  ulint current_gcn;

  /* Max purged gcn, snapshot gcn before that is too old to asof select. */
  ulint purged_gcn;

  /* retention gcn */
  ulint retention_gcn;

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

  /** The count of successful pcur guess restore (page re-search hit). */
  ulint index_pcur_guess_restore_hit;
  /** The count of failed pcur guess restore (page re-search miss). */
  ulint index_pcur_guess_restore_miss;

  /** Number of queries via flashback area. */
  ulint flashback_area_query_cnt;

  /** Number of previous version built. */
  ulint row_prev_vers_build_cnt;

  ulint txn_read_guess_request;
  ulint txn_read_guess_fail;

  ulint tcn_cache_hit;
  ulint tcn_cache_miss;
  ulint tcn_cache_write;

  /** Global query wait counter. */
  ulint query_wait_count;
  ulint query_wait_time;
  ulint query_wait_timeout;

  /** Polarx btree search mode hit count. */
  ulint polarx_btree_search_mode_hit;
  /** Polarx btree search mode miss count. */
  ulint polarx_btree_search_mode_miss;
  /** Current number of working purge threads */
  ulint n_purge_threads;

  /** Count of gpp hit during the scan of rowid intersection. */
  ulint rowid_intersection_gpp_hit;
  /** Count of gpp miss during the scan of rowid intersection. */
  ulint rowid_intersection_gpp_miss;

  /** Count of gpp hit during the scan of rowid union. */
  ulint rowid_union_gpp_hit;
  /** Count of gpp miss during the scan of rowid union. */
  ulint rowid_union_gpp_miss;

  /** Count of gpp hit during the scan of sort union. */
  ulint index_merge_gpp_hit;
  /** Count of gpp miss during the scan of sort union. */
  ulint index_merge_gpp_miss;

  /** Count of gpp hit during the scan of multi-range read. */
  ulint multi_range_read_gpp_hit;
  /** Count of gpp miss during the scan of multi-range read. */
  ulint multi_range_read_gpp_miss;

  /** Count of gpp hit during the scan of filesort. */
  ulint filesort_gpp_hit;
  /** Count of gpp miss during the scan of filesort. */
  ulint filesort_gpp_miss;

  /** Count of gpp hit during multi-table update rowid lookup. */
  ulint multi_update_gpp_hit;
  /** Count of gpp miss during multi-table update rowid lookup. */
  ulint multi_update_gpp_miss;

  /** Count of gpp hit during multi-table delete rowid lookup. */
  ulint multi_delete_gpp_hit;
  /** Count of gpp miss during multi-table delete rowid lookup. */
  ulint multi_delete_gpp_miss;

  /** Number of LOB pages successfully preallocated by Lob_alloc_eager. */
  ulint lob_prealloc_pages;

  /** Number of LOB prealloc attempts aborted due to out-of-space. */
  ulint lob_prealloc_fail_no_space;

  /** Number of LOBs rejected from prealloc path because the required
      page count exceeds LOB_MAX_PREALLOC_PAGES. */
  ulint lob_prealloc_reject_too_large;

  /** Number of unexpected fallbacks into InsertContext::fill_lob_page
      (should stay 0 in well-behaved runs). */
  ulint lob_prealloc_underestimate_pages;

  /** Number of preallocated LOB pages freed by Lob_alloc_eager::clear()
      (should stay 0 in well-behaved runs; non-zero means leftover pages
      were returned to the segment instead of being consumed). */
  ulint lob_prealloc_overestimate_pages;
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
  ulint_ctr_1_t pread_cleanout_clean;
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

  /** The count of successful pcur guess restore (page re-search hit). */
  ulint_ctr_1_t index_pcur_guess_restore_hit;
  /** The count of failed pcur guess restore (page re-search miss). */
  ulint_ctr_1_t index_pcur_guess_restore_miss;

  ulint_ctr_1_t flashback_area_query_cnt;

  ulint_ctr_1_t row_prev_vers_build_cnt;

  ulint_ctr_1_t txn_read_guess_request;
  ulint_ctr_1_t txn_read_guess_fail;

  ulint_ctr_1_t query_wait_count;
  ulint_ctr_1_t query_wait_time;
  ulint_ctr_1_t query_wait_timeout;

  ulint_ctr_1_t polarx_btree_search_mode_hit;
  ulint_ctr_1_t polarx_btree_search_mode_miss;

  /* Count of gpp hit during the scan of rowid intersection. */
  ulint_ctr_1_t rowid_intersection_gpp_hit;
  /** Count of gpp miss during the scan of rowid intersection. */
  ulint_ctr_1_t rowid_intersection_gpp_miss;

  /* Count of gpp hit during the scan of rowid union. */
  ulint_ctr_1_t rowid_union_gpp_hit;
  /** Count of gpp miss during the scan of rowid union. */
  ulint_ctr_1_t rowid_union_gpp_miss;

  /* Count of gpp hit during the scan of sort union. */
  ulint_ctr_1_t index_merge_gpp_hit;
  /** Count of gpp miss during the scan of sort union. */
  ulint_ctr_1_t index_merge_gpp_miss;

  /* Count of gpp hit during the scan of multi-range read. */
  ulint_ctr_1_t multi_range_read_gpp_hit;
  /** Count of gpp miss during the scan of multi-range read. */
  ulint_ctr_1_t multi_range_read_gpp_miss;

  /* Count of gpp hit during the scan of filesort. */
  ulint_ctr_1_t filesort_gpp_hit;
  /** Count of gpp miss during the scan of filesort. */
  ulint_ctr_1_t filesort_gpp_miss;

  /* Count of gpp hit during multi-table update rowid lookup. */
  ulint_ctr_1_t multi_update_gpp_hit;
  /** Count of gpp miss during multi-table update rowid lookup. */
  ulint_ctr_1_t multi_update_gpp_miss;

  /* Count of gpp hit during multi-table delete rowid lookup. */
  ulint_ctr_1_t multi_delete_gpp_hit;
  /** Count of gpp miss during multi-table delete rowid lookup. */
  ulint_ctr_1_t multi_delete_gpp_miss;

  /** Number of LOB pages successfully preallocated by Lob_alloc_eager. */
  ulint_ctr_1_t lob_prealloc_pages;

  /** Number of LOB prealloc attempts aborted due to out-of-space. */
  ulint_ctr_1_t lob_prealloc_fail_no_space;

  /** Number of LOBs rejected from prealloc path because the required
      page count exceeds LOB_MAX_PREALLOC_PAGES. */
  ulint_ctr_1_t lob_prealloc_reject_too_large;

  /** Number of unexpected fallbacks into InsertContext::fill_lob_page
      (should stay 0 in well-behaved runs). */
  ulint_ctr_1_t lob_prealloc_underestimate_pages;

  /** Number of preallocated LOB pages freed by Lob_alloc_eager::clear()
      (should stay 0 in well-behaved runs; non-zero means leftover pages
      were returned to the segment instead of being consumed). */
  ulint_ctr_1_t lob_prealloc_overestimate_pages;
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

/** The count of successful / failed pcur guess restore during
 * restore_position. */
inline void index_pcur_guess_restore_stat(bool hit) {
  if (!stat_enabled) return;

  if (hit) {
    generic_stats.index_pcur_guess_restore_hit.inc();
  } else {
    generic_stats.index_pcur_guess_restore_miss.inc();
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

inline void pread_cleanout_clean_stat(ulint value) {
  if (value > 0 && stat_enabled) {
    generic_stats.pread_cleanout_clean.add(value);
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

inline void polarx_btree_search_mode_stat(bool hit) {
  if (hit) {
    generic_stats.polarx_btree_search_mode_hit.inc();
  } else {
    generic_stats.polarx_btree_search_mode_miss.inc();
  }
}

inline void rowid_intersection_gpp_stat(bool hit) {
  if (!stat_enabled) return;
  if (hit) {
    generic_stats.rowid_intersection_gpp_hit.inc();
  } else {
    generic_stats.rowid_intersection_gpp_miss.inc();
  }
}

inline void rowid_union_gpp_stat(bool hit) {
  if (!stat_enabled) return;
  if (hit) {
    generic_stats.rowid_union_gpp_hit.inc();
  } else {
    generic_stats.rowid_union_gpp_miss.inc();
  }
}

inline void index_merge_gpp_stat(bool hit) {
  if (!stat_enabled) return;
  if (hit) {
    generic_stats.index_merge_gpp_hit.inc();
  } else {
    generic_stats.index_merge_gpp_miss.inc();
  }
}
inline void multi_range_read_gpp_stat(bool hit) {
  if (!stat_enabled) return;
  if (hit) {
    generic_stats.multi_range_read_gpp_hit.inc();
  } else {
    generic_stats.multi_range_read_gpp_miss.inc();
  }
}

inline void filesort_gpp_stat(bool hit) {
  if (!stat_enabled) return;
  if (hit) {
    generic_stats.filesort_gpp_hit.inc();
  } else {
    generic_stats.filesort_gpp_miss.inc();
  }
}

inline void multi_update_gpp_stat(bool hit) {
  if (!stat_enabled) return;
  if (hit) {
    generic_stats.multi_update_gpp_hit.inc();
  } else {
    generic_stats.multi_update_gpp_miss.inc();
  }
}

inline void multi_delete_gpp_stat(bool hit) {
  if (!stat_enabled) return;
  if (hit) {
    generic_stats.multi_delete_gpp_hit.inc();
  } else {
    generic_stats.multi_delete_gpp_miss.inc();
  }
}

inline void index_pos_guess_clust_stat(komodo::guess_mode_t gmode, bool hit) {
  switch (gmode) {
    case komodo::GM_ROWID_INTERSECTION:
      rowid_intersection_gpp_stat(hit);
      break;
    case komodo::GM_ROWID_UNION:
      rowid_union_gpp_stat(hit);
      break;
    case komodo::GM_INDEX_MERGE:
      index_merge_gpp_stat(hit);
      break;
    case komodo::GM_MRR:
      multi_range_read_gpp_stat(hit);
      break;
    case komodo::GM_FILESORT:
      filesort_gpp_stat(hit);
      break;
    case komodo::GM_MULTI_UPDATE:
      multi_update_gpp_stat(hit);
      break;
    case komodo::GM_MULTI_DELETE:
      multi_delete_gpp_stat(hit);
      break;
    default:
      ut_ad(false);
      break;
  }
}

/** Collect gpp stats of each index. */
class Gpp_index_stat_collector {
 public:
  /** Constructor.
  @param[in,out]  stats   container to collect stats into */
  Gpp_index_stat_collector(std::vector<im::Index_gpp_stat> &stats)
      : m_stats(stats) {}

  Gpp_index_stat_collector() = delete;

  /** Visitor.
  @param[in]      table   table to visit */
  void operator()(dict_table_t *table);

 private:
  std::vector<im::Index_gpp_stat> &m_stats; /** Collected stats. */
};

/** Reset gpp stats of each index. */
class Gpp_index_stat_flusher {
 public:
  Gpp_index_stat_flusher() = default;

  /** Visitor.
  @param[in]      table   table to visit */
  void operator()(dict_table_t *table);
};

}  // namespace lizard

#endif  // lizard0mon_h
