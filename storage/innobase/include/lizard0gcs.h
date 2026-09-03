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

/** @file include/lizard0gcs.h
 Global Change System implementation.

 Created 2020-03-23 by Jianwei.zhao
 *******************************************************/
#ifndef lizard0gcs_h
#define lizard0gcs_h

#include "univ.i"

#include "fsp0types.h"
#include "trx0sys.h"
#include "trx0types.h"

#include "lizard0cmmt0cache.h"
#include "lizard0gcs0clean.h"
#include "lizard0gcs0gcn.h"
#include "lizard0gcs0read.h"
#include "lizard0gcs0scn.h"
#include "lizard0gcs0snapshot.h"
#include "lizard0gcs0tid.h"
#include "lizard0gcs0utc.h"
#include "lizard0trx0cycle.h"
#include "lizard0txn0slot0types.h"

struct mtr_t;

/** Global Change System file segment header */
typedef byte gcs_sysf_t;

/** GCS system header */
/**-----------------------------------------------------------------------*/

/** The offset of GCS system header on the file segment page */
#define GCS_DATA FSEG_PAGE_DATA

/** The scn number which is stored here, it occupied 8 bytes */
#define GCS_DATA_SCN 0

/** The global commit number which is stored here. */
#define GCS_DATA_GCN (GCS_DATA_SCN + 8)

/** The purge scn number which is stored here, it occupied 8 bytes */
#define GCS_DATA_PURGE_SCN (GCS_DATA_GCN + 8)

/** The offset of file segment header */
#define GCS_DATA_FSEG_HEADER (GCS_DATA_PURGE_SCN + 8)

/**
   Revision history:
   ------------------------------------------------------
   1. Add purged_gcn
   2. Add erased_gcn
*/
#define GCS_DATA_PURGE_GCN (GCS_DATA_FSEG_HEADER + FSEG_HEADER_SIZE)

#define GCS_DATA_ERASE_GCN (GCS_DATA_PURGE_GCN + 8)

/** The start of not used */
#define GCS_DATA_NOT_USED (GCS_DATA_ERASE_GCN + 8)
/**-----------------------------------------------------------------------*/

/** The page number of GCS system header in lizard tablespace */
#define GCS_DATA_PAGE_NO 3

namespace lizard {

struct purge_vision_t;
struct cycle_vision_t;

class CRecover {
 public:
  CRecover()
      : m_need_recovery(false),
        m_tid_meta(),
        m_tid_recovered(false),
        m_scn_meta(),
        m_scn_recovered(false),
        m_gcn_meta(),
        m_gcn_recovered(false) {}

  virtual ~CRecover() {}
  /**
    Raise gcn metadata when parse gcn redo log record.

    @param[in]	gcn	Parsed gcn */
  void recover_gcn(const gcn_meta_t &meta);
  /**
    Raise scn metadata when parse scn redo log record.

    @param[in]	scn	Parsed scn */
  void recover_scn(const scn_meta_t &meta);
  /**
    Raise tid metadata when parse undo log header redo.

    @param[in]	tid	Parsed tid */
  void recover_tid(const tid_meta_t &meta);

  /** Recover the max parsed cmmt to current cmmt */
  void apply_cmmt();

  void need_recovery(bool value) { m_need_recovery = value; }

  bool is_need_recovery() { return m_need_recovery; }

 private:
  bool m_need_recovery;

  tid_meta_t m_tid_meta;
  bool m_tid_recovered;

  scn_meta_t m_scn_meta;
  bool m_scn_recovered;

  gcn_meta_t m_gcn_meta;
  bool m_gcn_recovered;
};

/** The memory structure of GCS system */
struct gcs_t {
  // Transaction id.
  TID tid;
  /** The scn which is order. */
  SCN scn;
  /** The gcn which come from TSO or Inner. */
  GCN gcn;
  /** Commit time. */
  UTC utc;

  /** Commit number vision. */
  MVCC mvcc;

  /** Commit number snapshot. */
  CSnapshot csnapshot;

  /** Commit number clean by purge/erase system. */
  CClean cclean;

  /** Commit number cache. */
  Atomic_ccache ccache;

  /** Prepare also generate commit number if has proposal, it only
   *  produce gcn, and left scn until real commit.
   *
   * @param[in/out]	trx
   * @param[in/out]	mtr
   *
   * @retval	proposal mark */
  proposal_mark_t new_proposal(trx_t *trx, mtr_t *mtr);

  /** Commit by generating all commit number.
   *
   * @param[in/out]	trx
   * @param[in/out]	mtr
   *
   * @retval	commit mark */
  commit_mark_t new_commit(trx_t *trx, mtr_t *mtr);

  /** Task a global commit system snapshot. */
  commit_snap_t new_snapshot();
};

/** GLobal GCS system */
extern gcs_t *gcs;

/** Initialize GCS system memory structure. */
extern void gcs_init();

/** Close GCS system structure. */
extern void gcs_close();

/** Boot GCS system */
extern void gcs_boot();

/** Get the address of GCS system header */
extern gcs_sysf_t *gcs_sysf_get(mtr_t *mtr);

/** Create GCS system pages within lizard tablespace */
extern void gcs_create_sys_pages();

/** Resurrect all recovered trx into gcs structures. */
extern void gcs_resurrect_trxs();

// see trx0sys.ic
extern bool gcs_need_rollback();

/**
  Persist transaction id and commit number.

  @retval	true	if written  */
extern bool gcs_persist_cmmt();

inline Tid_list_t *gcs_tid_list(size_t shard_no) {
  return gcs->tid.get_list(shard_no);
}

template <typename F>
inline bool gcs_iterate_rw_trx(F &&f) {
  return gcs->tid.latch_and_iterate_trx(std::forward<F>(f));
}

template <typename F>
inline bool gcs_iterate_rw_list(F &&f) {
  return gcs->tid.latch_and_iterate_trx_list(std::forward<F>(f));
}

/** How many prepared transaction. */
inline uint64_t gcs_n_prepared_trx() {
  uint64_t n_prepared_trx = 0;
  gcs->tid.latch_and_iterate_trx([&n_prepared_trx](trx_t *trx) -> bool {
    if (trx_state_eq(trx, TRX_STATE_PREPARED)) n_prepared_trx++;
    return false;
  });
  return n_prepared_trx;
}

/** How many rw transaction. */
inline uint64_t gcs_n_rw_trx() {
  uint64_t n_rw_trx = 0;
  gcs->tid.latch_and_iterate_trx([&n_rw_trx](trx_t *trx) -> bool {
    n_rw_trx++;
    return false;
  });
  return n_rw_trx;
}

/** Acquire a rw trx from rw list. */
inline trx_t *gcs_get_rw_trx() {
  trx_t *rw_trx = nullptr;
  bool exist = gcs->tid.latch_and_iterate_trx([&rw_trx](trx_t *trx) -> bool {
    rw_trx = trx;
    return true;
  });

  if (exist) {
    ut_ad(rw_trx);
  }

  return rw_trx;
}

/** Allocate a new tid. */
inline trx_id_t gcs_new_tid() { return gcs->tid.new_tid(); }

/** Start a new transaction. */
template <typename F>
inline trx_id_t gcs_new_tid(trx_t *trx, F &&f) {
  return gcs->tid.new_tid(trx, std::forward<F>(f));
}

inline bool gcs_validate_rw_list() { return gcs->tid.validate_trx_list(); }

/** Get current SCN number */
inline scn_t gcs_load_scn() { return gcs->scn.load_scn(); }

/** Get current persisted GCN number */
inline gcn_t gcs_load_gcn() { return gcs->gcn.load_gcn(); }

/** Get new trx id. */
inline trx_id_t gcs_load_next_tid() { return gcs->tid.load_next_tid(); }

/** Peek the min active tid according to txn address.*/
inline trx_id_t gcs_peek_min_active_tid(const undo_ptr_t &undo_ptr) {
  slot_addr_t slot_addr(undo_ptr);
  ut_ad(slot_addr.is_redo());

  return gcs->tid.peek_min_active_tid(slot_addr);
}

/**
  Get the min active trx id

  @retval         the min active id in trx_sys. */
inline trx_id_t gcs_load_min_active_tid() {
  return gcs->tid.load_min_active_tid();
}

/**
  In MySQL 8.0:
  * Hold trx_sys::mutex, generate trx->no, add trx to
  trx_sys->serialisation_list
  * Hold purge_sys::pq_mutex, add undo rseg to purge_queue. All undo records
  are ordered.
  * Erase above mutexs, commit in undo header
  * Hold trx_sys::mutex, erase serialisation_list, rw_trx_ids, rw_trx_list,
    the modifications from the committed trx can be seen.

  In Lizard:
  * Hold trx_sys::mutex, generate trx->txn_desc.scn
  * Hold SCN::mutex, add trx to trx_sys->serialisation_list_scn
  * Erase trx_sys::mutex, hold purge_sys::pq_mutex and rseg::mutex, and add
    undo rseg to purge_queue. So undo records in the same rseg are ordered,
  but undo records from different rsegs are not ordered in purge_heap.
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
inline scn_t gcs_load_min_safe_scn() {
  // assert_lizard_min_safe_scn_valid();
  /* Get the oldest transaction from serialisation list. */
  scn_t min_active_scn = gcs->scn.load_min_active_scn();
  ut_a(min_active_scn > 0);

  return min_active_scn - 1;
}

inline scn_t gcs_traverse_min_safe_scn() {
  /* Get the oldest transaction from serialisation list. */
  scn_t min_active_scn = gcs->scn.traverse_min_active_scn();
  ut_a(min_active_scn > 0);

  return min_active_scn - 1;
}

/** Erase trx in scn_list_scn, and update min_safe_scn
@param[in]      trx      trx to be removed */
inline void gcs_erase_scn(trx_t *trx) { gcs->scn.erase(trx); }

/** Erase trx in rw_list, and update min_active_tid
@param[in]      trx      trx to be removed */
inline void gcs_erase_tid(trx_t *trx) { gcs->tid.erase(trx); }

template <typename T>
inline trx_id_t gcs_search_up_limit_tid(const T *lhs) {
  return gcs->csnapshot.search_up_limit_tid(lhs);
}

inline void gcs_set_gcn_if_bigger(gcn_t gcn) {
  gcs->gcn.set_gcn_if_bigger(gcn);
}

inline void gcs_clean_advance_purge(const commit_order_t &ommt) {
  gcs->cclean.advance_purge(ommt);
}

inline void gcs_clean_advance_erase(const commit_order_t &ommt) {
  gcs->cclean.advance_erase(ommt);
}

inline void gcs_clean_init_purge() { gcs->cclean.init_purge(); }
inline void gcs_clean_init_erase() { gcs->cclean.init_erase(); }

inline commit_order_t gcs_clean_cycled_position() { return cycle_sys->last(); }

inline scn_t gcs_clean_purged_scn() { return gcs->cclean.purged_scn(); }
inline gcn_t gcs_clean_purged_gcn() { return gcs->cclean.purged_gcn(); }
inline commit_order_t gcs_clean_purged_position() {
  return gcs->cclean.purged_position();
}

inline scn_t gcs_clean_erased_scn() { return gcs->cclean.erased_scn(); }
inline gcn_t gcs_clean_erased_gcn() { return gcs->cclean.erased_gcn(); }
inline commit_order_t gcs_clean_erased_position() {
  return gcs->cclean.erased_position();
}

/** Search from cache and overwrite the txn rec
 *
 * @param[in/out]   txn rec
 * @param[in/out]   txn lookup status
 *
 * @retval          cache hit or not */
inline bool gcs_cache_find(txn_rec_t *txn_rec, txn_status_t *txn_status) {
  return gcs->ccache.find(txn_rec, txn_status);
}
/** Cache Commit Number if txn_rec was looked up.
 *
 * @param[in]	txn rec info that looked up
 * @param[in]	txn status */
inline void gcs_cache_insert(const txn_rec_t &txn_rec,
                             const txn_status_t &status) {
  gcs->ccache.insert(txn_rec, status);
}
/** Cache Commit Number after trx commit.
 *
 * @param[in]	Committed trx
 * @param[in]	Allocated commit number  */
inline void gcs_cache_insert(const trx_t *trx, bool serialised) {
  gcs->ccache.insert(trx, serialised);
}
/** Guess whether rec tid is within the range of tcn cache.
 *
 * @param[in]	txn_rec		txn record
 * @param[in]	up_limit_tid	up limit tid of vision
 *
 * @retval		guess result */
bool gcs_cache_guess(const txn_rec_t &txn_rec, const trx_id_t up_limit_tid);
/**
  Assign vision for an active trancaction.

  @param[in]		the transaction to assign vision
*/
extern void gcs_vision_open(trx_t *trx);

/**
  Assign shadow vision for a single shard read.

  @param[in]		the transaction to assign vision
*/
extern void gcs_shadow_view_open(trx_t *trx);

/**
  Release the corresponding element.

  @param[in]		the element will be released
*/
extern void gcs_vision_release(Vision *vision);

/**
  Release the shadow vision so the purge sys can advance

  @param[in]		the element will be released
*/
extern void gcs_shadow_view_release(Shadow_view *vision);

/**
  Get the earliest undestructed element.

  @param[out]		the earliest entry undestructed element
*/
extern void gcs_clone_oldest_vision(purge_vision_t *pvision);

/**
  Get the earliest undestructed element.

  @param[out]		the earliest entry undestructed element
*/
extern void gcs_clone_oldest_vision(cycle_vision_t *cvision);

/**
  Get total size of all active vision

  @retval       total size of all active vision
*/
extern ulint gcs_mvcc_size();

}  // namespace lizard

#endif  // lizard0gcs_h define
