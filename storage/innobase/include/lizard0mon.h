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

struct lizard_var_t {
  /** txn undo rollback segment free list length */
  ulint txn_undo_log_free_list_len;

  /** txn undo log cached for reuse */
  ulint txn_undo_log_cached;

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

  ulint txn_undo_lost_page_offset_overflow;

  ulint txn_undo_lost_magic_number_wrong;

  ulint txn_undo_lost_ext_flag_wrong;

  ulint txn_undo_lost_trx_id_mismatch;

  ulint txn_undo_lookup_by_uba;

  ulint cleanout_page_collect;

  ulint cleanout_record_clean;
};

struct lizard_stats_t {
  typedef ib_counter_t<ulint, 64> ulint_ctr_64_t;
  typedef ib_counter_t<lsn_t, 1, single_indexer_t> lsn_ctr_1_t;
  typedef ib_counter_t<ulint, 1, single_indexer_t> ulint_ctr_1_t;
  typedef ib_counter_t<lint, 1, single_indexer_t> lint_ctr_1_t;
  typedef ib_counter_t<int64_t, 1, single_indexer_t> int64_ctr_1_t;

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

  /** txn undo lost because uba.offset is out of undo page bound */
  ulint_ctr_1_t txn_undo_lost_page_offset_overflow;

  /** txn undo lost because magic number is wrong */
  ulint_ctr_1_t txn_undo_lost_magic_number_wrong;

  /** txn undo lost because ext flag is wrong */
  ulint_ctr_1_t txn_undo_lost_ext_flag_wrong;

  /** txn undo lost because trx_id is mismatch */
  ulint_ctr_1_t txn_undo_lost_trx_id_mismatch;

  /** lookup scn by uba */
  ulint_ctr_1_t txn_undo_lookup_by_uba;

  ulint_ctr_1_t cleanout_page_collect;

  ulint_ctr_1_t cleanout_record_clean;
};

namespace lizard {

extern lizard_stats_t lizard_stats;

int show_lizard_vars(THD *thd, SHOW_VAR *var, char *buff);

}  // namespace lizard

#define LIZARD_MONITOR_INC_TXN_CACHED(NUMBER)                             \
  do {                                                                    \
    if (lizard::lizard_sys != nullptr) {                                  \
      lizard::lizard_sys->txn_undo_log_cached.fetch_add((NUMBER));        \
    }                                                                     \
  } while (0)

#define LIZARD_MONITOR_DEC_TXN_CACHED(NUMBER)                             \
  do {                                                                    \
    if (lizard::lizard_sys != nullptr) {                                  \
      lizard::lizard_sys->txn_undo_log_cached.fetch_sub((NUMBER));        \
    }                                                                     \
  } while (0)

#endif  // lizard0mon_h

