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

/** @file trx/lizard0gcs0tid.cc
 Transaction number implementation.

 Created 2025-03-17 by Jianwei.zhao
 *******************************************************/

#include "mtr0log.h"
#include "mtr0mtr.h"
#include "sync0types.h"
#include "trx0undo.h"

#include "lizard0gcs.h"
#include "lizard0gcs0tid.h"

#ifdef UNIV_PFS_MUTEX
/* GCS tid list mutex PFS key */
mysql_pfs_key_t tid_list_mutex_key;
/* GCS tid persist mutex PFS key */
mysql_pfs_key_t tid_persist_mutex_key;

#endif

namespace lizard {

/** Tid list is partitioned by txn slot space_id. so max value == 4; */
uint32_t srv_gcs_tid_list_shards = 2;

/** Write tid value into tablespace.

    @param[in]	metadata   tid metadata
 */
void TID::TidPersister::write(const meta_t *meta) {
  ut_ad(meta->valid());
  mtr_t mtr;
  trx_sysf_t *sys_header;

  mtr_start(&mtr);

  sys_header = trx_sysf_get(&mtr);

  mlog_write_ull(sys_header + TRX_SYS_TRX_ID_STORE, meta->tid, &mtr);

  mtr_commit(&mtr);
}

/** Read tid value from tablespace and increase TID_NUMBER_MAGIN
    to promise unique.

    @param[in/out]	metadata   tid metadata

 */
void TID::TidPersister::read(meta_t *meta) {
  trx_sysf_t *sys_header;

  mtr_t mtr;
  mtr.start();

  sys_header = trx_sysf_get(&mtr);

  const trx_id_t max_trx_id =
      mach_read_from_8(sys_header + TRX_SYS_TRX_ID_STORE);

  meta->tid = max_trx_id + 2 * TRX_SYS_TRX_ID_WRITE_MARGIN;
  mtr.commit();
}

// Constructor
TID::TID()
    : m_next_tid(0),
      m_persister(),
      m_persisted_next_tid(0),
      m_shards(srv_gcs_tid_list_shards),
      m_boot_tid(0),
      m_inited(false) {
  m_lists = ut::new_arr_withkey<ut::Cacheline_padded<tid_list_t>>(
      UT_NEW_THIS_FILE_PSI_KEY, ut::Count{m_shards});

  for (size_t i = 0; i < m_shards; i++) {
    m_lists[i].init(this);
  }
  mutex_create(LATCH_ID_TID_PERSIST, &m_persist_mutex);
}

/** Assign min active tid according to list element. */
void TID::tid_list_t::start_active_tid() {
  ut_ad(lock_own());
  auto min_active_trx = UT_LIST_GET_LAST(m_list);

  trx_id_t min_tid =
      min_active_trx == nullptr ? tid_mgr->load_next_tid() : min_active_trx->id;

  m_min_active_tid.store(min_tid, std::memory_order_relaxed);
}

/** Remove trx from rw list after commit. */
void TID::tid_list_t::remove_list(trx_t *trx) {
  ut_ad(lock_own());
  ut_ad(trx->id > 0);
  ut_ad(UT_LIST_GET_LEN(m_list) > 0);

  UT_LIST_REMOVE(m_list, trx);

  auto min_active_trx = UT_LIST_GET_LAST(m_list);

  trx_id_t min_tid =
      min_active_trx == nullptr ? tid_mgr->load_next_tid() : min_active_trx->id;

  m_min_active_tid.store(min_tid, std::memory_order_relaxed);

  ut_ad(check_min_active_tid());
}

/** Check the trx list ordered by tid and min_active_tid was maintained
 * correctly.
 *
 * @retval	true	valid */
bool TID::tid_list_t::check_min_active_tid() const {
  trx_id_t next_tid;
  trx_t *trx = nullptr;
  trx_t *prev_trx = nullptr;
  ut_ad(lock_own());

  next_tid = tid_mgr->load_next_tid();
  if (UT_LIST_GET_LEN(m_list) == 0) {
    ut_a(m_min_active_tid.load(std::memory_order_relaxed) <= next_tid);
  } else {
    trx = UT_LIST_GET_FIRST(m_list);
    ut_a(trx);
    ut_a(m_min_active_tid.load(std::memory_order_relaxed) <= trx->id);
    ut_a(m_min_active_tid <= next_tid);

    /** trx in rw list are ordered by tid */
    prev_trx = trx;
    for (trx = UT_LIST_GET_NEXT(trx_list, trx); trx != nullptr;
         trx = UT_LIST_GET_NEXT(trx_list, trx)) {
      ut_a(m_min_active_tid.load(std::memory_order_relaxed) <= trx->id);
      ut_a(prev_trx->id > trx->id);
      prev_trx = trx;
    }
  }
  return true;
}

/** Destructor of TID */
TID::~TID() {
  for (size_t i = 0; i < m_shards; i++) {
    m_lists[i].destroy();
  }

  ut::delete_arr(m_lists);

  mutex_free(&m_persist_mutex);
  m_inited = false;
}

/** Assign the init value by reading from tablespace */
void TID::boot() {
  ut_ad(!m_inited);
  ut_ad(m_next_tid == 0);

  meta_t meta;
  m_persister.read(&meta);
  m_next_tid.store(meta.tid);
  m_persisted_next_tid.store(meta.tid);

  ut_a(m_next_tid > 0 && m_next_tid < TRX_ID_MAX);

  lizard_system(ER_LIZARD) << "GCS Read TID: " << m_persisted_next_tid.load();

  m_inited = true;
  return;
}

/** Acquire resurrected transaction slot address. */
static slot_addr_t trx_get_resurrected_txn_slot(trx_t *trx) {
  trx_undo_t *undo = nullptr;
  ut_ad(trx->is_recovered);
  ut_ad(trx_is_redo_rseg_updated(trx) || trx_is_txn_rseg_updated(trx));

  /** Insert undo maybe commit in memory. */
  if (trx->rsegs.m_redo.insert_undo != nullptr) {
    undo = trx->rsegs.m_redo.insert_undo;

  } else if (trx->rsegs.m_redo.update_undo != nullptr) {
    /** Update undo must be active or prepared. */
    ut_ad(!trx_state_eq(trx, TRX_STATE_COMMITTED_IN_MEMORY));
    undo = trx->rsegs.m_redo.update_undo;

  } else if (trx->rsegs.m_txn.txn_undo != nullptr) {
    /** txn undo must be active or prepared.
     * TXN_UNDO_SLOT still be not reused by MASTER_ADDRESS.
     * */
    ut_ad(!trx_state_eq(trx, TRX_STATE_COMMITTED_IN_MEMORY));
    undo = trx->rsegs.m_txn.txn_undo;

  } else {
    ut_a(0);
  }

  ut_a(undo);
  slot_addr_t slot_addr = undo->slot_addr;

  ut_ad(slot_addr.is_redo());
  return slot_addr;
}

/** Resurrect recovered trx which is active/prepared/committed_in_memory. */
void TID::resurrect_trx(trx_t *trx) {
  ut_ad(!trx->in_rw_trx_list);
  ut_ad(trx->id != 0);
  ut_ad(trx->tid_shard_no == -1);
  ut_ad(trx_is_redo_rseg_updated(trx) || trx_is_txn_rseg_updated(trx));

  /** Txn slot address of resurrected tansaction. */
  slot_addr_t slot_addr = trx_get_resurrected_txn_slot(trx);

  trx->tid_shard_no = map2shard(slot_addr.space_id);

  // add ordered trx into list.
  latch_and_execute_tid_list(trx->tid_shard_no, [&](tid_list_t *list) -> bool {
    list->add_list(trx);
    ut_d(trx->in_rw_trx_list = true);
    return false;
  });
}

/** Cleanup after commit. */
void TID::erase(trx_t *trx) {
  ut_ad(m_inited);
  ut_ad(trx->in_rw_trx_list);
  ut_ad(trx && trx->id > 0);
  ut_ad(trx->tid_shard_no != -1);

#ifdef UNIV_DEBUG
  trx_id_t old_min_active_tid = load_min_active_tid();
#endif

  // Remove from rw list.
  latch_and_execute_tid_list(trx->tid_shard_no, [&](tid_list_t *list) -> bool {
    list->remove_list(trx);
    ut_d(trx->in_rw_trx_list = false);
    return false;
  });

  trx->tid_shard_no = -1;

  ut_d(trx_id_t min_active_tid = load_min_active_tid());
  ut_ad(old_min_active_tid <= min_active_tid);
}

/**
 * Persist tid if next tid > persisted tid.
 *
 * @retval	true	if written */
bool TID::persist_tid() {
  bool written = false;
  mutex_enter(&m_persist_mutex);
  trx_id_t next_tid = m_next_tid.load();

  if (next_tid > m_persisted_next_tid.load()) {
    /** Recover must be completed. */
    ut_ad(!recv_sys || !recv_sys->cn_recover->is_need_recovery());

    m_persisted_next_tid.store(next_tid);

    meta_t meta;
    meta.tid = next_tid;
    m_persister.write(&meta);

    written = true;

    if (srv_gcs_print_process_debug) {
      lizard_info(ER_LIZARD) << "GCS persist TID: " << next_tid;
    }
  }
  mutex_exit(&m_persist_mutex);
  return written;
}

/**
  Push up next tid if bigger when recovery.

  @param[in]	meta
*/
void TID::recover_tid_if_bigger(const meta_t &meta) {
  ut_ad(meta.valid());

  /** meta come from allocated tid. */
  if (meta.tid + 1 > m_next_tid.load()) {
    m_next_tid.store(meta.tid + 1);
  }
}


}  // namespace lizard
