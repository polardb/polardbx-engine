/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
lzeusited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the zeusplied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file trx/lizard0gcs.cc
 Global Change System implementation.

 Created 2020-03-23 by Jianwei.zhao
 *******************************************************/

#include "fsp0fsp.h"
#include "mtr0mtr.h"
#include "srv0start.h"
#include "trx0sys.h"
#include "trx0trx.h"

#include "lizard0dbg.h"
#include "lizard0dict.h"
#include "lizard0fil.h"
#include "lizard0fsp.h"
#include "lizard0gcs.h"
#include "lizard0read0read.h"
#include "lizard0scn.h"

#include "lizard0ut.h"

#ifdef UNIV_PFS_MUTEX
/* GCS scn list mutex PFS key */
mysql_pfs_key_t scn_list_mutex_key;
/* GCS gcn order mutex PFS key */
mysql_pfs_key_t gcn_order_mutex_key;
/* GCS gcn persist mutex PFS key */
mysql_pfs_key_t gcn_persist_mutex_key;
#endif

namespace lizard {

#ifdef UNIV_DEBUG
/** simulate assigned gcn to run all testcases. */
bool srv_simulate_assigned_gcn_debug = false;
#endif

/** The global Global Change System memory structure */
gcs_t *gcs = nullptr;

/** Initialize GCS system memory structure. */
void gcs_init() {
  ut_ad(gcs == nullptr);

  gcs = static_cast<gcs_t *>(ut::zalloc(sizeof(*gcs)));

  new (&(gcs->persisters)) Persisters();

  /** Placement new SCN object  */
  new (&(gcs->scn)) SCN(gcs->persisters.scn_persister());
  /** Placement new GCN object  */
  new (&(gcs->gcn)) GCN(gcs->persisters.gcn_persister());

  new (&(gcs->csnapshot_mgr))
      CSnapshot_mgr(commit_snapshot_mem_key, srv_commit_snapshot_partition,
                    srv_commit_snapshot_capacity);

  /** Promise here didn't have any active trx */
  ut_ad(trx_sys == nullptr || UT_LIST_GET_LEN(trx_sys->rw_trx_list) == 0);

  /** Attention: it's monitor metrics, didn't promise accuration */
  gcs->txn_undo_log_free_list_len = 0;
  gcs->txn_undo_log_cached = 0;

  UT_LIST_INIT(gcs->serialisation_list_scn);

  mutex_create(LATCH_ID_SCN_LIST, &gcs->m_scn_list_mutex);
  mutex_create(LATCH_ID_GCN_ORDER, &gcs->m_gcn_order_mutex);
  mutex_create(LATCH_ID_GCN_PERSIST, &gcs->m_gcn_persist_mutex);

  gcs->min_active_trx_id.store(GCS_DATA_MTX_ID_NULL);

  gcs->m_persisted_gcn.store(0);

  gcs->mtx_inited = true;

  return;
}

/**
  Raise gcn metadata when parse gcn redo log record.

  @param[in]	gcn	Parsed gcn
*/
void CRecover::recover_gcn(gcn_t gcn) {
  ut_ad(is_need_recovery());
  ut_ad(gcn != GCN_NULL);

  m_metadata.set_gcn_if_bigger(gcn);
  return;
}

/**
  Recover the max parsed gcn to current gcn.
*/
void CRecover::apply_gcn() {
  gcn_t recovered_gcn = m_metadata.get_gcn();

  /** Not found any gcn redo log record. */
  if (recovered_gcn == GCN_NULL) {
    m_need_recovery = false;
    return;
  }

  ut_ad(is_need_recovery());

  ut_ad(gcs->gcn.load_gcn() == gcs->m_persisted_gcn.load());

  gcs->gcn.set_gcn_if_bigger(recovered_gcn);

  m_need_recovery = false;
  return;
}

commit_mark_t gcs_t::new_commit(trx_t *trx, mtr_t *mtr) {
  commit_mark_t cmmt = COMMIT_MARK_NULL;

  ut_ad(!recv_sys || !recv_sys->cn_recover->is_need_recovery());

  /** 1. generate scn number */
  scn_list_mutex_enter();

  DBUG_EXECUTE_IF(
      "crash_before_gcn_commit",
      if (trx->txn_desc.cmmt.gcn != GCN_NULL) { DBUG_SUICIDE(); });

  assert_trx_commit_mark_initial(trx);

  /** We don't want to call **ut_time_system_us** within the scope
  of the gcs mutex protection. So we just only set
  trx->txn_desc.scn.first here */
  trx->txn_desc.cmmt.scn = cmmt.scn = scn.new_scn();

  /** If a read only transaction (for example: start transaction read only),
  temporary table can be also modified. It doesn't matter if purge_sys purges
  them */

  /** Revision:
      Temp undo still need to purge/truncate, so delay it by adding into
      serialisation list */

  /** add to gcs->serialisation_list_scn */
  UT_LIST_ADD_LAST(gcs->serialisation_list_scn, trx);

  scn_list_mutex_exit();

  /** 2. generate gcn number. */
  std::pair<gcn_t, csr_t> ret =
      gcn.new_gcn(trx->txn_desc.cmmt.gcn, trx->txn_desc.cmmt.csr, mtr);

  trx->txn_desc.cmmt.gcn = cmmt.gcn = ret.first;
  trx->txn_desc.cmmt.csr = cmmt.csr = ret.second;

  /** 3. generate utc time. */
  trx->txn_desc.cmmt.us = cmmt.us = ut_time_system_us();

  /** 4. mark undo_ptr, only memory structure, didn't modify undo header UBA. */
  ut_ad(undo_ptr_is_active(trx->txn_desc.undo_ptr));

#ifdef UNIV_DEBUG
  if (srv_simulate_assigned_gcn_debug) {
    trx->txn_desc.cmmt.csr = cmmt.csr = CSR_ASSIGNED;
  }
#endif

  undo_ptr_set_commit(&trx->txn_desc.undo_ptr, trx->txn_desc.cmmt.csr);

  return cmmt;
}

/** Push a commit snapshot. */
void gcs_t::new_snapshot(const commit_snap_t &snap) {
  csnapshot_mgr.push(snap);
  return;
}

template <typename T>
trx_id_t gcs_t::search_up_limit_tid(const T &lhs) {
  return csnapshot_mgr.search_up_limit_tid(lhs);
}

template <typename T>
extern trx_id_t gcs_search_up_limit_tid(const T &lhs) {
  return gcs->search_up_limit_tid(lhs);
}

template trx_id_t gcs_t::search_up_limit_tid<Snapshot_gcn_vision>(
    const Snapshot_gcn_vision &lhs);
template trx_id_t gcs_t::search_up_limit_tid<Snapshot_scn_vision>(
    const Snapshot_scn_vision &lhs);

template trx_id_t gcs_search_up_limit_tid<Snapshot_gcn_vision>(
    const Snapshot_gcn_vision &lhs);

template trx_id_t gcs_search_up_limit_tid<Snapshot_scn_vision>(
    const Snapshot_scn_vision &lhs);
/**
  Persist gcn if current gcn > persisted gcn.

  @retval	true	if written
 */
bool gcs_persist_gcn() {
  ut_ad(gcs);
  return gcs->persist_gcn();
}

/**
  Persist gcn if current gcn > persisted gcn.

  @retval	true	if written
 */
bool gcs_t::persist_gcn() {
  bool written = false;
  gcn_t current_gcn;

  gcn_persist_mutex_enter();

  current_gcn = gcn.load_gcn();
  if (current_gcn > m_persisted_gcn.load()) {
    /** Recover must be completed. */
    ut_ad(!recv_sys || !recv_sys->cn_recover->is_need_recovery());

    m_persisted_gcn.store(current_gcn);

    PersistentGcsData meta;
    meta.set_gcn(m_persisted_gcn.load());
    persisters.gcn_persister()->write(&meta);

    written = true;
  }
  gcn_persist_mutex_exit();

  return written;
}

/** Erase trx in serialisation_list_scn, and update min_safe_scn
@param[in]      trx      trx to be removed */
void gcs_erase_lists(trx_t *trx) {
  assert_lizard_min_safe_scn_valid();

  scn_list_mutex_enter();

  lizard_ut_ad(UT_LIST_GET_LEN(gcs->serialisation_list_scn) > 0);

  lizard_ut_ad(trx != NULL && trx->state == TRX_STATE_COMMITTED_IN_MEMORY);

  auto oldest_trx = UT_LIST_GET_FIRST(gcs->serialisation_list_scn);
  UT_LIST_REMOVE(gcs->serialisation_list_scn, trx);

  /** Considering the case:
  serialisation_list_scn [100, 101], gcs->scn = 101
  if **101** erased, and then **100** erased, serialisation_list_scn []
  we also set min_safe_scn as 100 for safety */
  if (oldest_trx == trx) {
    lizard_ut_ad(gcs->min_safe_scn.load() < trx->txn_desc.cmmt.scn);
    gcs->min_safe_scn.store(oldest_trx->txn_desc.cmmt.scn);
  }

  scn_list_mutex_exit();

  assert_lizard_min_safe_scn_valid();
}

/** Close Global Change System structure. */
void gcs_close() {
  if (gcs != nullptr) {
    lizard_ut_ad(UT_LIST_GET_LEN(gcs->serialisation_list_scn) == 0);

    gcs->scn.~SCN();
    gcs->gcn.~GCN();

    gcs->persisters.~Persisters();

    gcs->csnapshot_mgr.~CSnapshot_mgr();

    gcs->mtx_inited = false;

    mutex_free(&gcs->m_scn_list_mutex);
    mutex_free(&gcs->m_gcn_order_mutex);
    mutex_free(&gcs->m_gcn_persist_mutex);

    ut::free(gcs);
    gcs = nullptr;
  }

  trx_vision_container_destroy();
}

/** Get the address of Global Change System header */
gcs_sysf_t *gcs_sysf_get(mtr_t *mtr) {
  buf_block_t *block = nullptr;
  gcs_sysf_t *hdr;
  ut_ad(mtr);

  block =
      buf_page_get(page_id_t(dict_lizard::s_lizard_space_id, GCS_DATA_PAGE_NO),
                   univ_page_size, RW_X_LATCH, UT_LOCATION_HERE, mtr);

  hdr = GCS_DATA + buf_block_get_frame(block);
  return hdr;
}

/** Init the elements of Global Change System */
void gcs_boot() {
  ut_ad(gcs);

  gcs->scn.boot();

  gcs->min_safe_scn = gcs->scn.load_scn();

  gcs->gcn.boot();

  gcs->m_persisted_gcn = gcs->gcn.load_gcn();

  ut_ad(gcs->m_persisted_gcn.load() != GCN_NULL &&
        gcs->m_persisted_gcn.load() >= GCN_INITIAL);

  /** Init vision system */
  trx_vision_container_init();
}

/** Create a new file segment special for Global Change System
@param[in]      mtr */
static void gcs_create_sysf(mtr_t *mtr) {
  gcs_sysf_t *hdr;
  buf_block_t *block;
  page_t *page;
  byte *ptr;
  ut_ad(mtr);

  mtr_x_lock_space(fil_space_get_lizard_space(), mtr);

  block = fseg_create(dict_lizard::s_lizard_space_id, 0,
                      GCS_DATA + GCS_DATA_FSEG_HEADER, mtr);

  ut_a(block->page.id.page_no() == GCS_DATA_PAGE_NO);
  page = buf_block_get_frame(block);

  hdr = gcs_sysf_get(mtr);

  /* The scan number is counting from SCN_RESERVERD_MAX */
  mach_write_to_8(hdr + GCS_DATA_SCN, SCN_RESERVERD_MAX);

  /** The global commit number initial value */
  mach_write_to_8(hdr + GCS_DATA_GCN, GCN_INITIAL);

  /** The max GCN number whose transaction has been purged */
  mach_write_to_8(hdr + GCS_DATA_PURGE_GCN, GCN_INITIAL);

  ptr = hdr + GCS_DATA_NOT_USED;

  memset(ptr, 0, UNIV_PAGE_SIZE - FIL_PAGE_DATA_END + page - ptr);

  mlog_log_string(hdr, UNIV_PAGE_SIZE - FIL_PAGE_DATA_END + page - hdr, mtr);
  lizard_info(ER_LIZARD) << "Initialize Global Change System";
}

/** Create Global Change System pages within system tablespace */
void gcs_create_sys_pages() {
  mtr_t mtr;

  mtr_start(&mtr);

  /* Create a new file segment special for Global Change System */
  gcs_create_sysf(&mtr);

  mtr_commit(&mtr);
}

/** Get current SCN number */
scn_t gcs_load_scn() {
  ut_a(gcs);

  return gcs->scn.load_scn();
}

/** Get max persisted GCN number */
gcn_t gcs_load_gcn() {
  ut_a(gcs);

  return gcs->gcn.load_gcn();
}

/**
  Modify the min active trx id

  @param[in]      the removed trx */
void gcs_mod_min_active_trx_id(trx_t *trx) {
  trx_t *min_active_trx = NULL;

  ut_ad(trx != NULL);
  ut_ad(gcs->mtx_inited);
  /* Must hold the trx sys mutex */
  ut_ad(trx_sys_mutex_own() || srv_is_being_started);

  /** Called after remove */
  // ut_ad(!trx->in_rw_trx_list);

  /** Read only didn't put into rw list even through it allocated new trx id
     for temporary table update. */
  ut_ad((trx->read_only ||
         (trx->rsegs.m_redo.rseg == NULL && trx->rsegs.m_txn.rseg == NULL)) ||
        gcs->min_active_trx_id.load() <= trx->id);

#ifdef UNIV_DEBUG
  /** Only myself modify mtx id, so delay to hold mtx mutex */
  trx_id_t old_min_active_id = gcs->min_active_trx_id.load();
#endif

  min_active_trx = UT_LIST_GET_LAST(trx_sys->rw_trx_list);

  trx_id_t min_id = min_active_trx == nullptr
                        ? trx_sys->next_trx_id_or_no.load()
                        : min_active_trx->id;

  gcs->min_active_trx_id.store(min_id);

  ut_ad(old_min_active_id <= gcs->min_active_trx_id.load());
}

/**
  Get the min active trx id

  @retval         the min active id in trx_sys. */
trx_id_t gcs_load_min_active_trx_id() {
  trx_id_t ret;
  ret = gcs->min_active_trx_id.load();
  return ret;
}

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
/** Check if min_safe_scn is valid
@return         true if min_safe_scn is valid */
void min_safe_scn_valid() {
  scn_t sys_scn;
  trx_t *trx = nullptr;
  trx_t *prev_trx = nullptr;

  ut_ad(!scn_list_mutex_own());

  scn_list_mutex_enter();

  sys_scn = gcs_load_scn();
  if (UT_LIST_GET_LEN(gcs->serialisation_list_scn) == 0) {
    ut_a(gcs->min_safe_scn <= sys_scn);
  } else {
    trx = UT_LIST_GET_FIRST(gcs->serialisation_list_scn);
    ut_a(trx);

    ut_a(gcs->min_safe_scn < trx->txn_desc.cmmt.scn);

    ut_a(gcs->min_safe_scn < sys_scn);

    /** trx in serialisation_list_scn are ordered by scn */
    prev_trx = trx;
    for (trx = UT_LIST_GET_NEXT(scn_list, trx); trx != nullptr;
         trx = UT_LIST_GET_NEXT(scn_list, trx)) {
      ut_a(prev_trx->txn_desc.cmmt.scn < trx->txn_desc.cmmt.scn);
      prev_trx = trx;
    }
  }

  scn_list_mutex_exit();
}
#endif /* UNIV_DEBUG || LIZARD_DEBUG */

/**
  In MySQL 8.0:
  * Hold trx_sys::mutex, generate trx->no, add trx to
  trx_sys->serialisation_list
  * Hold purge_sys::pq_mutex, add undo rseg to purge_queue. All undo records are
  ordered.
  * Erase above mutexs, commit in undo header
  * Hold trx_sys::mutex, erase serialisation_list, rw_trx_ids, rw_trx_list,
    the modifications from the committed trx can be seen.

  In Lizard:
  * Hold trx_sys::mutex, generate trx->txn_desc.scn
  * Hold SCN::mutex, add trx to trx_sys->serialisation_list_scn
  * Erase trx_sys::mutex, hold purge_sys::pq_mutex and rseg::mutex, and add
    undo rseg to purge_queue. So undo records in the same rseg are ordered, but
    undo records from different rsegs are not ordered in purge_heap.
  * Erase above mutexs, commit in undo header
  * Hold SCN::mutex, remove trx in trx_sys->serialisation_list_scn,
    and update min_safe_scn.

  To ensure purge_sys purge in order, min_safe_scn is used for purge sys.
  min_safe_scn is the current smallest scn of committing transactions (both
  prepared state and committed in memory state).

  This function might be used in the following scenarios:
  * purge sys should get a safe scn for purging
  * clone
  * PolarDB

  @retval         the min safe commited scn in current lizard sys
*/
scn_t gcs_load_min_safe_scn() {
  ut_ad(!scn_list_mutex_own());
  assert_lizard_min_safe_scn_valid();
  /* Get the oldest transaction from serialisation list. */
  return gcs->min_safe_scn.load();
}

void gcs_set_gcn_if_bigger(gcn_t gcn) { gcs->gcn.set_gcn_if_bigger(gcn); }

}  // namespace lizard
