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

/** @file trx/lizard0gcs.cc
 Global Change System implementation.

 Created 2020-03-23 by Jianwei.zhao
 *******************************************************/

#include "fsp0fsp.h"
#include "mtr0mtr.h"
#include "trx0sys.h"
#include "trx0trx.h"
#include "clone0clone.h"

#include "lizard0dict0dict.h"
#include "lizard0fil.h"
#include "lizard0gcs0cache.h"
#include "lizard0gcs0gcn.h"
#include "lizard0gcs.h"
#include "lizard0gcs0read.h"
#include "lizard0gcs0scn.h"
#include "lizard0trx0purge.h"

#include "lizard0ut.h"
#include "univ.i"

namespace lizard {

#ifdef UNIV_DEBUG
/** simulate assigned gcn to run all testcases. */
bool srv_simulate_assigned_gcn_debug = false;
#endif

/** Whether to print gcs process info. */
bool srv_gcs_print_process_debug = false;


/** The global Global Change System memory structure */
gcs_t *gcs = nullptr;

/** Initialize GCS system memory structure. */
void gcs_init() {
  ut_ad(gcs == nullptr);

  gcs = static_cast<gcs_t *>(ut::zalloc(sizeof(*gcs)));

  /** Placement new TID object  */
  new (&(gcs->tid)) TID();

  /** Placement new SCN object  */
  new (&(gcs->scn)) SCN();

  /** Placement new GCN object  */
  new (&(gcs->gcn)) GCN();

  /** Placement new UTC object  */
  new (&(gcs->utc)) UTC();

  /** Placement new mvcc object  */
  new (&(gcs->mvcc)) MVCC(MVCC_N_LISTS);

  /** Placement new csnapshot object */
  new (&(gcs->csnapshot))
      CSnapshot(commit_snapshot_mem_key, srv_commit_snapshot_partition,
                srv_commit_snapshot_capacity);

  /** Placement new cclean object */
  new (&(gcs->cclean)) CClean(GCS_DATA_PURGE_GCN, GCS_DATA_ERASE_GCN);

  /** Placement new ccache object */
  new (&(gcs->ccache)) Atomic_ccache(srv_tcn_cache_entries);

  return;
}

/**
  Raise gcn metadata when parse gcn redo log record.

  @param[in]	gcn	Parsed gcn */
void CRecover::recover_gcn(const gcn_meta_t &meta) {
  ut_ad(is_need_recovery());
  ut_a(meta.valid());

  if (srv_gcs_print_process_debug) {
    lizard_info(ER_LIZARD) << "GCS Recover GCN: " << meta.gcn;
  }

  m_gcn_meta.set_gcn_if_bigger(meta);
  m_gcn_recovered = true;
  return;
}
/**
  Raise scn metadata when parse scn redo log record.

  @param[in]	scn	Parsed scn */
void CRecover::recover_scn(const scn_meta_t &meta) {
  ut_ad(is_need_recovery());
  ut_a(meta.valid());

  if (srv_gcs_print_process_debug) {
    lizard_info(ER_LIZARD) << "GCS Recover SCN: " << meta.scn;
  }

  m_scn_meta.set_scn_if_bigger(meta);
  m_scn_recovered = true;
  return;
}
/**
  Raise tid metadata when parse undo log header redo.

  @param[in]	tid	Parsed tid */
void CRecover::recover_tid(const tid_meta_t &meta) {
  ut_ad(is_need_recovery());
  ut_ad(meta.valid());

  if (srv_gcs_print_process_debug) {
    lizard_info(ER_LIZARD) << "GCS Recover TID: " << meta.tid;
  }

  m_tid_meta.set_tid_if_bigger(meta);
  m_tid_recovered = true;
  return;
}

/** Recover the max parsed gcn to current gcn. */
void CRecover::apply_cmmt() {
  // Apply max used tid from undo hdr redo log.
  if (m_tid_recovered) {
    ut_ad(m_tid_meta.valid());
    ut_ad(m_need_recovery);
    ut_ad(gcs->tid.load_next_tid() == gcs->tid.persisted_next_tid());

    gcs->tid.recover_tid_if_bigger(m_tid_meta);

    lizard_system(ER_LIZARD) << "GCS Recover Max TID: " << m_tid_meta.tid;
  }

  /** Apply max scn from redo log record. */
  if (m_scn_recovered) {
    ut_ad(m_scn_meta.valid());
    ut_ad(is_need_recovery());
    ut_ad(gcs->scn.load_scn() == gcs->scn.persisted_scn());

    gcs->scn.recover_scn_if_bigger(m_scn_meta);

    lizard_system(ER_LIZARD) << "GCS Recover Max SCN: " << m_scn_meta.scn;
  }

  /** Apply max gcn from redo log record. */
  if (m_gcn_recovered) {
    ut_ad(is_need_recovery());
    ut_ad(m_gcn_meta.valid());
    ut_ad(gcs->gcn.load_gcn() == gcs->gcn.persisted_gcn());

    gcs->gcn.recover_gcn_if_bigger(m_gcn_meta);

    lizard_system(ER_LIZARD) << "GCS Recover Max GCN: " << m_gcn_meta.gcn;
  }

  m_need_recovery = false;
  return;
}

/** Prepare also generate commit number if has proposal, it only
 *  produce gcn, and left scn until real commit.
 *
 * @param[in/out]	trx
 * @param[in/out]	mtr
 *
 * @retval	proposal mark */
proposal_mark_t gcs_t::new_proposal(trx_t *trx, mtr_t *mtr) {
  ut_ad(trx->txn_desc.cmmt.scn == SCN_NULL);
  ut_ad(!trx->txn_desc.xa_pmmt().is_null());

  const proposal_mark_t xa_pmmt = trx->txn_desc.xa_pmmt();

  gcn_tuple_t gtuple = gcn.new_proposal_gcn(xa_pmmt, trx, mtr);

  ut_ad(!trx->txn_desc.is_slot_committed());

  return {gtuple.gcn, gtuple.csr};
}

/** Commit by generating all commit number.
 *
 * @param[in/out]	trx
 * @param[in/out]	mtr
 *
 * @retval	commit mark */
commit_mark_t gcs_t::new_commit(trx_t *trx, mtr_t *mtr) {
  ut_ad(!recv_sys || !recv_sys->cn_recover->is_need_recovery());
  gcn_tuple_t xa_gcn = trx->txn_desc.xa_gcn();

  DBUG_EXECUTE_IF(
      "crash_before_gcn_commit",
      if (xa_gcn.gcn != GCN_NULL) { DBUG_SUICIDE(); });

  ut_ad(trx->txn_desc.is_cmmt_initial());

  /** 1. generate scn number */
  /** We don't want to call **ut_time_system_us** within the scope
  of the gcs mutex protection. So we just only set
  trx->txn_desc.scn.first here */
  scn_t commit_scn = scn.new_scn(trx, mtr);

  /** If a read only transaction (for example: start transaction read only),
  temporary table can be also modified. It doesn't matter if purge_sys purges
  them */

  /** Revision:
      Temp undo still need to purge/truncate, so delay it by adding into
      serialisation list */

  /** add to gcs->serialisation_list_scn */

  /** 2. generate gcn number. */
  gcn_tuple_t commit_gcn = gcn.new_commit_gcn(xa_gcn, trx, mtr);

  /** 3. generate utc time. */
  utc_t commit_us = utc.new_us(trx);

  ut_ad(trx->txn_desc.is_cmmt_committed());
  ut_ad(!trx->txn_desc.is_slot_committed());

#ifdef UNIV_DEBUG
  if (srv_simulate_assigned_gcn_debug) {
    trx->txn_desc.cmmt.csr = commit_gcn.csr = CSR_ASSIGNED;
  }
#endif

  return {commit_scn, commit_us, commit_gcn.gcn, commit_gcn.csr};
}

/** Task a global commit system snapshot. */
commit_snap_t gcs_t::new_snapshot() {
  commit_snap_t snap;

  /** Step 1: Get least active tid. */
  snap.up_limit_tid = tid.load_min_active_tid();
  /** Step 2: Get utc. */
  snap.utc_sec = ut_time_system_us() / 1000000;
  /** Step 3. Generate new scn. */
  snap.scn = scn.new_scn();
  /** Step 4. Get gcn. */
  snap.gcn = gcn.load_gcn();

  csnapshot.push(snap);
  return snap;
}

/**
  Persist transaction id and commit number.

  @retval	true	if written */
bool gcs_persist_cmmt() {
  ut_ad(gcs);
  bool written_tid = gcs->tid.persist_tid();
  bool written_scn = gcs->scn.persist_scn();
  bool written_gcn = gcs->gcn.persist_gcn();

  return written_tid || written_scn || written_gcn;
}

/** Close Global Change System structure. */
void gcs_close() {
  if (gcs != nullptr) {
    gcs->tid.~TID();
    gcs->scn.~SCN();
    gcs->gcn.~GCN();
    gcs->utc.~UTC();

    gcs->mvcc.~MVCC();

    gcs->csnapshot.~CSnapshot();

    gcs->cclean.~CClean();

    gcs->ccache.~CCache();

    ut::free(gcs);
    gcs = nullptr;
  }
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

  gcs->tid.boot();

  gcs->scn.boot();

  gcs->gcn.boot();

  lizard_system(ER_LIZARD) << "Global Change System has started.";
}

/** Resurrect all recovered trx into gcs structures. */
void gcs_resurrect_trxs() {
  ut::vector<trx_t *> trxs;

  for (auto &shard : trx_sys->shards) {
    shard.active_rw_trxs.latch_and_execute(
        [&](const Trx_by_id_with_min &trx_by_id_with_min) {
          for (const auto &trx_track : trx_by_id_with_min.by_id()) {
            trxs.emplace_back(trx_track.second);
          }
        },
        UT_LOCATION_HERE);
  }

  std::sort(trxs.begin(), trxs.end(),
            [&](trx_t *a, trx_t *b) { return a->id < b->id; });

  for (trx_t *trx : trxs) {
    gcs->tid.resurrect_trx(trx);
  }

  gcs->tid.start_active_tid();
  lizard_system(ER_LIZARD) << "GCS Start Active TID: "
                           << gcs->tid.load_min_active_tid();

  /** Push up min safe scn */
  gcs->scn.start_active_scn();
  lizard_system(ER_LIZARD) << "GCS Start Active SCN: "
                           << gcs->scn.load_min_active_scn();
}

/** Determine if there are incomplete transactions in the system.
@return whether incomplete transactions need rollback */
bool gcs_need_rollback() {
  uint64_t n_trx = 0;
  uint64_t n_prepared_trx = 0;

  gcs->tid.latch_and_iterate_trx([&](trx_t *trx) -> bool {
    n_trx++;

    if (trx_state_eq(trx, TRX_STATE_PREPARED)) {
      n_prepared_trx++;
    }

    return false;
  });

  ut_ad(n_trx >= n_prepared_trx);

  n_trx -= n_prepared_trx;

  return n_trx > 0;
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

  /* The scn number is counting from SCN_INSTALL_VALUE */
  mach_write_to_8(hdr + GCS_DATA_SCN, SCN::SCN_INSTALL_VALUE);

  /** The global commit number initial value */
  mach_write_to_8(hdr + GCS_DATA_GCN, GCN::GCN_INSTALL_VALUE);

  /** The max GCN number whose transaction has been purged */
  mach_write_to_8(hdr + GCS_DATA_PURGE_GCN, GCN::GCN_INSTALL_VALUE);

  ptr = hdr + GCS_DATA_NOT_USED;

  memset(ptr, 0, UNIV_PAGE_SIZE - FIL_PAGE_DATA_END + page - ptr);

  mlog_log_string(hdr, UNIV_PAGE_SIZE - FIL_PAGE_DATA_END + page - hdr, mtr);

  lizard_system(ER_LIZARD) << "Global Change System has initialized.";
}

/** Create Global Change System pages within system tablespace */
void gcs_create_sys_pages() {
  mtr_t mtr;

  mtr_start(&mtr);

  /* Create a new file segment special for Global Change System */
  gcs_create_sysf(&mtr);

  mtr_commit(&mtr);
}

/**
  Assign vision for an active trancaction.

  @param[in]		the transaction to assign vision
*/
void gcs_vision_open(trx_t *trx) {
  Vision *vision = nullptr;
  ut_ad(trx_sys == nullptr || !trx_sys_mutex_own());

  vision = &trx->vision;
  gcs->mvcc.vision_open(vision, trx->id);

  /* Cache min active tid. */
  trx->min_active_tid.store(vision->up_limit_tid(), std::memory_order_relaxed);

  /** Verify commit snapshot module correctness. */
  if (srv_vision_use_commit_snapshot_debug) {
    komodo::Snapshot_scn_vision v(vision->snapshot_scn(), 0);
    vision->set_up_limit_tid(
        gcs_search_up_limit_tid<komodo::Snapshot_scn_vision>(&v));
  }
}

void gcs_shadow_view_open(trx_t *trx) {
  Shadow_view *shadow = nullptr;
  Vision *vision = nullptr;
  ut_ad(trx->id != TRX_ID_MAX);
  ut_ad(trx_sys == nullptr || !trx_sys_mutex_own());

  shadow = &trx->shadow_view;
  vision = shadow->vision();

  /** 1. Get SCN and block purge */
  gcs->mvcc.vision_open(vision, trx->id);

  /** 2. Get GCN */
  shadow->set_gcn(gcs_load_gcn());

  /** 3. Get up_limit_tid */
  komodo::Snapshot_automatic_gcn_vision tmp(shadow->gcn(),
                                            vision->snapshot_scn());
  shadow->set_up_limit_tid(
      gcs_search_up_limit_tid<komodo::Snapshot_gcn_vision>(&tmp));
}

/**
  Release the corresponding element.

  @param[in]		the element will be released
*/
void gcs_vision_release(Vision *vision) { gcs->mvcc.vision_release(vision); }

void gcs_shadow_view_release(Shadow_view *shadow) {
  Vision *vision = nullptr;
  vision = shadow->vision();
  ut_ad(vision != nullptr);

  gcs->mvcc.vision_release(vision);
  shadow->reset();
}

/**
  Get the earliest undestructed element.

  @param[out]		the earliest entry undestructed element
*/
void gcs_clone_oldest_vision(cycle_vision_t *cvision) {
  Vision *vision = nullptr;
  scn_t min_active_scn = 0;
  scn_t oldest_scn = 0;

  ut_ad(gcs->mvcc.inited());
  ut_ad(trx_sys == nullptr || !trx_sys_mutex_own());

  vision = cvision->vision();

  /** S1: acquire min active scn. */
#ifdef UNIV_DEBUG
  min_active_scn = gcs->scn.traverse_min_active_scn();
#else
  min_active_scn = gcs->scn.load_min_active_scn();
  if (cvision->limit().scn >= min_active_scn) {
    min_active_scn = gcs->scn.traverse_min_active_scn();
  }
#endif

  commit_order_t limit = {min_active_scn, (utc_t)ut_time_system_us(),
                         gcs_load_gcn()};
  cvision->set_limit(limit);

  /** S2: acquire oldest vision. */
  oldest_scn = gcs->mvcc.clone_oldest_vision();

  /** Difference with purge vision:
   *
   * 1. min safe is unnecessary since cycle didn't use min-heap.
   *
   * 2. gtid is protected by update undo. so ignore it.
   * */
  vision->set_snapshot_scn(oldest_scn);
}

/**
  Get the earliest undestructed element.

  @param[out]		the earliest entry undestructed element
*/
void gcs_clone_oldest_vision(purge_vision_t *pvision) {
  Vision *vision = nullptr;
  scn_t min_safe_scn = 0;
  scn_t oldest_scn = 0;
  scn_t gtid_oldest_trxscn = 0;
  ut_ad(gcs->mvcc.inited());
  ut_ad(trx_sys == nullptr || !trx_sys_mutex_own());

  vision = pvision->vision();

  /** S1: acquire min safe scn. */
#ifdef UNIV_DEBUG
  min_safe_scn = gcs_traverse_min_safe_scn();
#else
  min_safe_scn = gcs_load_min_safe_scn();
  /** More accurate if few transaction. */
  if (pvision->safe_scn() >= min_safe_scn) {
    min_safe_scn = gcs_traverse_min_safe_scn();
  }
#endif
  /** Save scn for min-heap pop safely. */
  pvision->set_safe_scn(min_safe_scn);

  /** S2: acquire oldest vision */
  oldest_scn = gcs->mvcc.clone_oldest_vision();

  /** S3: Block purging transaction till GTID is persisted. */
  auto &gtid_persistor = clone_sys->get_gtid_persistor();
  gtid_oldest_trxscn = gtid_persistor.get_oldest_trx_scn();

  /** Save oldest as vision snapshot scn. */
  oldest_scn = std::min(oldest_scn, gtid_oldest_trxscn);
  vision->set_snapshot_scn(std::min(min_safe_scn, oldest_scn));
}

/**
  Get total size of all active vision

  @retval       total size of all active vision
*/
ulint gcs_mvcc_size() {
  ut_ad(gcs->mvcc.inited());
  ut_ad(trx_sys == nullptr || !trx_sys_mutex_own());

  return gcs->mvcc.size();
}

/** Guess whether rec tid is within the range of tcn cache.
 *
 * @param[in]	txn_rec		txn record
 * @param[in]	up_limit_tid	up limit tid of vision
 *
 * @retval		guess result */
bool gcs_cache_guess(const txn_rec_t &txn_rec, const trx_id_t up_limit_tid) {
  trx_id_t rec_tid = 0;
  trx_id_t boot_tid = 0;
  trx_id_t low_tid = 0;
  ulint n_entry = 0;
  ulint window = 0;
  ut_ad(txn_rec.is_active());

  rec_tid = txn_rec.trx_id;
  boot_tid = gcs->tid.boot_tid();
  n_entry = gcs->ccache.n_entry();

  ut_ad(rec_tid > 0 && rec_tid < TRX_ID_MAX);
  ut_ad(boot_tid > 0 && boot_tid < TRX_ID_MAX);

  /** Global tcn cache is disabled (innodb_tcn_cache_level = none). */
  if (n_entry == 0) {
    return false;
  }

  /** Rec tid is less than boot tid, maybe out of range. Active recovered
   * transactions whose tids are less than boot tid also fall here even if
   * they are cached after committed, they give up the global tcn cache
   * acceleration. */
  if (rec_tid < boot_tid) {
    return false;
  }

  /** Rec tid maybe is newer transaction. */
  if (up_limit_tid > 0 && rec_tid >= up_limit_tid) {
    return true;
  }

  /** Calculate low tid by the tid window scaled from cache entry number. */
  window = (n_entry * srv_tcn_cache_guess_ratio) / 100;
  if (up_limit_tid > window) {
    low_tid = up_limit_tid - window;
  } else {
    low_tid = 1;
  }

  low_tid = std::max(low_tid, boot_tid);

  return (rec_tid >= low_tid && rec_tid < up_limit_tid);
}

}  // namespace lizard
