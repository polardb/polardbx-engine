/*****************************************************************************

Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file trx/lizard0trx0history.cc
  Already committed trx history.

 Created 2025-07-03 by Jianwei.zhao
 *******************************************************/

#include "trx0purge.h"
#include "trx0rseg.h"

#include "lizard0cleanout0safe.h"
#include "lizard0gcs.h"
#include "lizard0trx0cycle.h"
#include "lizard0trx0erase.h"
#include "lizard0trx0history.h"
#include "lizard0txn0space.h"
#include "lizard0undo0retent.h"

#ifdef UNIV_PFS_MUTEX
/* cycle sys mutex PFS key */
mysql_pfs_key_t history_sys_mutex_key;
#endif

namespace lizard {

/** Initialize secondary list [free list / semi purge list] when booting. */
void trx_rseg_init_secondary_list(trx_rseg_t *rseg, trx_rsegf_t *rseg_hdr,
                                  pe_heap_t *hp, mtr_t *mtr) {
  if (rseg->is_txn) {
    /** Lizard: Initialize free list size */
    fil_addr_t node_addr;
    rseg->last_free_ommt = lizard::txn_slot_free_get_last_log(rseg, node_addr);
  } else {
    /** Lizard: Initialize erase heap and related members in rseg. */
    auto sp_list_len = flst_get_len(rseg_hdr + TRX_RSEG_SEMI_PURGE_LIST);
    trx_rseg_init_erase_heap(rseg, rseg_hdr, hp->erase_heap, sp_list_len, mtr);
  }

  /** Lizard: Init txn undo log hash table */
  trx_rseg_init_undo_hdr_hash(rseg->space_id, rseg_hdr, rseg, mtr);
}

void trx_rseg_init_history_len() {
  trx_sys->update_rseg_history_len.store(0);
  trx_sys->txn_rseg_history_len.store(0);
}

/** history length include upate undo and txn slot. */
ulint trx_rseg_history_len() {
  return trx_sys->update_rseg_history_len.load() +
         trx_sys->txn_rseg_history_len.load();
}

void trx_rseg_incre_history_len(trx_rseg_t *rseg, ulint len) {
  if (rseg->is_txn) {
    trx_sys->txn_rseg_history_len.fetch_add(len);
  } else {
    trx_sys->update_rseg_history_len.fetch_add(len);
  }
}

/** Allocate heap. */
void pe_heap_t::allocate_heap() {
  ut_ad(purge_heap == nullptr && erase_heap == nullptr);
  purge_heap = ut::new_withkey<purge_heap_t>(UT_NEW_THIS_FILE_PSI_KEY);
  erase_heap = ut::new_withkey<erase_heap_t>(UT_NEW_THIS_FILE_PSI_KEY);
}

/** Add into cycle or purge heap when scan rseg->history list.*/
void pe_heap_t::push_purge(trx_rseg_t *rseg, scn_t scn) {
  ut_ad(rseg->last_page_no != FIL_NULL);
  ut_ad(scn != SCN_NULL);

  /** txn slot heap is built by array structure when recycle. */
  if (rseg->is_txn) {
    ut_ad(rseg->last_page_no != FIL_NULL);
    return;
  }

  ut_ad(!rseg->is_txn);
  ut_a(!fsp_is_txn_tablespace_by_id(rseg->space_id));

  purge_rsegs_t elem(scn);
  elem.insert(rseg);

  /** Add into purge heap. */
  mutex_enter(&purge_sys->pq_mutex);
  purge_heap->push(std::move(elem));
  mutex_exit(&purge_sys->pq_mutex);

  return;
}

/** History memory structure. */
trx_history_t *history_sys = nullptr;

/** Entrance of cycle/purge/erase sys. */
void trx_history_sys_mem_create() {
  // 1. cycle sys.
  trx_cycle_sys_mem_create();
  trx_purge_sys_mem_create();
  trx_erase_sys_mem_create();

  history_sys = static_cast<trx_history_t *>(
      ut::zalloc_withkey(UT_NEW_THIS_FILE_PSI_KEY, sizeof(*history_sys)));

  new (&history_sys->rsegs_queue) std::vector<trx_rseg_t *>;

  new (&history_sys->hp) pe_heap_t();

  mutex_create(LATCH_ID_HISTORY_SYS, &history_sys->mutex);
}

/** Initialize cycle/purge/erase system. */
void trx_history_sys_initialize(pe_heap_t *hp) {
  ut_ad(hp == &history_sys->hp);

  trx_cycle_sys_initialize(1);

  trx_purge_sys_initialize(srv_threads.m_purge_workers_n, hp->purge_heap);

  trx_erase_sys_initialize(srv_threads.m_purge_workers_n, hp->erase_heap);

  hp->reset_heap();
}

/** Close cycle/purge/erase system. */
void trx_history_sys_close() {
  trx_cycle_sys_close();
  trx_purge_sys_close();
  trx_erase_sys_close();

  mutex_free(&history_sys->mutex);

  call_destructor(&history_sys->rsegs_queue);
  call_destructor(&history_sys->hp);

  ut::free(history_sys);
}

/** Calculate purge/erase status of undo tablespace.
 *
 * @param[in/out]	status array.
 * */
void trx_history_purge_status(komodo::purge_status_t &status) {
  status.history_length = trx_rseg_history_len();

  status.current_cn = {gcs_load_scn(), (utc_t)ut_time_system_us(),
                       gcs_load_gcn()};

  commit_order_t purged = gcs_clean_purged_position();
  status.purged_cn = {purged.scn, purged.us, purged.gcn};

  commit_order_t erased = gcs_clean_erased_position();
  status.erased_cn = {erased.scn, erased.us, erased.gcn};

  commit_order_t cycled = gcs_clean_cycled_position();

  status.cycled_cn = {cycled.scn, cycled.us, cycled.gcn};

  status.txn_history_length = trx_sys->txn_rseg_history_len.load();

  status.retention_gcn = Undo_manager::instance().get_retention_gcn();
}

/** Calculate rsegment status of undo tablespace.
 *
 * @param[in/out]	status array.
 * */
void trx_history_trunc_status(std::vector<komodo::trunc_status_t> &array) {
  mutex_enter(&undo::ddl_mutex);
  undo::spaces->s_lock();

  for (auto undo_space : undo::spaces->m_spaces) {
    komodo::trunc_status_t status;
    /** 1. undo tablespace name and file size. */
    status.undo_name = undo_space->space_name();
    status.file_pages = fil_space_get_size(undo_space->id());

    undo_space->rsegs()->s_lock();

    commit_order_t hist_ommt, sec_ommt, last;
    fil_addr_t addr;
    for (auto rseg : *undo_space->rsegs()) {
      komodo::rseg_stat_t stat;
      /** 2. Oldest log hdr in history list */
      last = trx_purge_get_last_log(rseg, addr, &stat);
      hist_ommt = ut_min(hist_ommt, last);
      /** 3. Oldest log hdr in semi-purge or free list */
      if (undo_space->is_txn()) {
        last = txn_slot_free_get_last_log(rseg, addr, &stat);
      } else {
        last = trx_erase_get_last_log(rseg, addr, &stat);
      }
      sec_ommt = ut_min(sec_ommt, last);
      status.aggregate(stat);
    }
    undo_space->rsegs()->s_unlock();

    status.oldest_history_cn = {hist_ommt.scn, hist_ommt.us, hist_ommt.gcn};
    status.oldest_secondary_cn = {sec_ommt.scn, sec_ommt.us, sec_ommt.gcn};
    /***/
    array.push_back(status);
  }

  undo::spaces->s_unlock();
  mutex_exit(&undo::ddl_mutex);
}


}  // namespace lizard
