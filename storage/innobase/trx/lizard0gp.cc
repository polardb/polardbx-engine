/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */


/** @file include/lizard0gp.h
  Lizard global query.

 Created 2020-12-30 by Jianwei.zhao
 *******************************************************/

#include <mysql/service_thd_wait.h>

#include "lizard0gp.h"
#include "lizard0row.h"
#include "lizard0undo.h"
#include "page0page.h"
#include "row0row.h"
#include "srv0conc.h"
#include "trx0trx.h"
#include "srv0srv.h"
#include "srv0start.h"

class THD;

#ifdef UNIV_PFS_MUTEX
/* Global query mutex PFS key */
mysql_pfs_key_t gp_sys_mutex_key;
/* Global query wait mutex PFS key */
mysql_pfs_key_t gp_sys_wait_mutex_key;
#endif

#ifdef UNIV_PFS_THREAD
mysql_pfs_key_t srv_gp_wait_timeout_thread_key;
#endif

namespace lizard {

/** Global query singlton system */
gp_sys_t *gp_sys = nullptr;

/**
  Initialize global query system
*/
void gp_sys_create() {
  ulint gp_sys_sz;

  gp_sys_sz = sizeof(*gp_sys) + srv_max_n_threads * sizeof(gp_slot_t);

  gp_sys = static_cast<gp_sys_t *>(ut_zalloc_nokey(gp_sys_sz));

  void *ptr = &gp_sys[1];

  gp_sys->waiting_threads = static_cast<gp_slot_t *>(ptr);

  mutex_create(LATCH_ID_GP_SYS, &gp_sys->mutex);

  mutex_create(LATCH_ID_GP_SYS_WAIT, &gp_sys->wait_mutex);
}

/**
  Close global query system
*/
void gp_sys_destroy() {
  if (gp_sys == nullptr) return;

  gp_slot_t *slot = gp_sys->waiting_threads;

  for (ulint i = 0; i < srv_max_n_threads; i++, ++slot) {
    if (slot->event != nullptr) {
      os_event_destroy(slot->event);
    }
  }
  mutex_destroy(&gp_sys->mutex);
  mutex_destroy(&gp_sys->wait_mutex);
  ut_free(gp_sys);
  gp_sys = nullptr;
}

/**----------------------- GP timeout ----------------------------*/

/**----------------------- GP timeout ----------------------------*/

/**----------------------- GP transaction ----------------------------*/
/**
  Search the transaction from trx_sys->rw_trx_list by trx_id;
  1) If NOT FOUND, take it as committed since the trx_id come from
     record, so it's impossible for TRX_STATE_NOT_STARTED,

  2) TRX_STATE_FORCED_ROLLBACK, TRX_STATE_ACTIVE, TRX_STATE_PREPARED,
     Those all will be found in list, only TRX_STATE_PREPARED will be
     blocked to commit by reference, since we will build the blocking
     relationship.
*/
std::pair<trx_t *, trx_state_t> trx_rw_is_prepared(trx_id_t trx_id) {
  trx_t *trx = nullptr;
  trx_state_t state;

  /** Already committed; */
  if (lizard_sys_get_min_active_trx_id() > trx_id)
    return std::make_pair(nullptr, TRX_STATE_COMMITTED_IN_MEMORY);

  trx_sys_mutex_enter();
  trx = trx_rw_is_active_low(trx_id, nullptr);

  /** Active transaction */
  if (trx != nullptr) {
    if (trx->state == TRX_STATE_PREPARED) {
      trx = trx_reference(trx, true);
      state = TRX_STATE_PREPARED;
    } else {
      ut_ad(trx->state == TRX_STATE_ACTIVE || trx->state == TRX_STATE_FORCED_ROLLBACK);
      trx = nullptr;
      state = TRX_STATE_ACTIVE;
    }
  } else {
    /** Not found */
    trx = nullptr;
    state = TRX_STATE_COMMITTED_IN_MEMORY;
  }

  trx_sys_mutex_exit();
  return std::make_pair(trx, state);
}

/**
  Build the blocking between global query and prepared transaction.
  Blocking trx has been blocked by reference.

  @param[in]      query_trx         global query trx
  @param[in]      blocking_trx      prepared trx
*/
static void gp_build_wait_state(trx_t *query_trx, trx_t *blocking_trx) {
  ut_ad(trx_is_referenced(blocking_trx));

  gp_mutex_enter();

  /** Step 1: Build query state */
  trx_mutex_enter(query_trx);

  /** Query state must be initial */
  assert_gp_state_initial(query_trx);

  trx_gp_state(query_trx)->build(blocking_trx);

  /** Wait state must be built in advance of allocating slot. */
  ut_ad(trx_gp_state(query_trx)->slot == nullptr);

  trx_mutex_exit(query_trx);

  /** Step 2: Build wait state */
  trx_mutex_enter(blocking_trx);
  trx_gp_wait(blocking_trx)->build(query_trx);
  trx_mutex_exit(blocking_trx);

  gp_mutex_exit();
}

/**
  If the query has been allocated a slot, signal it.
  @param[in]    trx     query trx
*/
static void gp_wait_signal_thread(trx_t *trx) {
  gp_slot_t *slot = nullptr;
  ut_ad(gp_mutex_own());
  ut_ad(trx_mutex_own(trx));

  if ((slot = trx_gp_state(trx)->slot) != nullptr) {
    os_event_set(slot->event);
  }
}

/**
  Reset the wait state and blocking relationship from
  query trx point, signal it if allocated slot.

  @param[in]      trx     global query
*/
static void gp_reset_wait_and_release_thread(trx_t *trx) {
  trx_t *blocking_trx = nullptr;
  gp_state_t *state = nullptr;
  gp_wait_t *wait = nullptr;
  ut_ad(gp_mutex_own());
  ut_ad(trx_mutex_own(trx));

  state = trx_gp_state(trx);
  blocking_trx = state->blocking_trx;
  state->release();

  ut_ad(trx_mutex_own(blocking_trx));
  wait = trx_gp_wait(blocking_trx);
  wait->release(trx);

  /** Signal by the slot */
  gp_wait_signal_thread(trx);
}

/**
  Cancal all blocked global query when commit
  @param[in]      trx       committing trx
*/
void gp_wait_cancel_all_when_commit(trx_t *trx) {
  gp_wait_t *gp_wait = trx_gp_wait(trx);

  /** Didn't has any blocked global query */
  if (gp_wait->n_blocked.load() == 0) return;

  gp_mutex_enter();

  trx_mutex_enter(trx);

  /** Copy all the blocked trxs */
  Trx_array dups;
  copy_to(gp_wait->blocked_trxs, dups);

  for (trx_t *blocked_trx : dups) {
    trx_mutex_enter(blocked_trx);
    /** Reset wait state and release blocked thread */
    gp_reset_wait_and_release_thread(blocked_trx);
    trx_mutex_exit(blocked_trx);
  }

  gp_mutex_exit();
  trx_mutex_exit(trx);
}

/**
  Check the target slot and wake up if timeout.

  @param[in]      slot
*/
void gp_wait_check_and_cancel(gp_slot_t *slot) {
  trx_t *trx;
  trx_t *blocking_trx;
  ut_ad(gp_wait_mutex_own());

  auto suspend_time = slot->suspend_time;

  ut_ad(slot->in_use);
  ut_ad(slot->suspend);

  ulong wait_time = ut_time_monotonic() - suspend_time;
  trx = slot->query_trx;

  /** Timeout */
  if (wait_time > slot->wait_timeout) {
    gp_mutex_enter();
    trx_mutex_enter(trx);

    gp_state_t *state = trx_gp_state(trx);
    blocking_trx = state->blocking_trx;
    if (state->waiting == true) {
      ut_ad(blocking_trx != nullptr);

      trx_mutex_enter(blocking_trx);
      gp_reset_wait_and_release_thread(trx);
      trx_mutex_exit(blocking_trx);
    }

    gp_mutex_exit();
    trx_mutex_exit(trx);
  }
}
/**----------------------- GP transaction ----------------------------*/

/**
  Acquire a available slot for global query thread.
  It should be successful.

  @param[in]    trx     Global query trx context
  @param[in]    timeout Wait timeout

  @retval       slot
*/
gp_slot_t *gp_reserve_slot(trx_t *trx, ulong wait_timeout) {
  gp_slot_t *slot;

  ut_ad(gp_wait_mutex_own());
  ut_ad(trx_mutex_own(trx));

  /** Global query should complete XA transaction */
  ut_ad(trx->gp_wait.validate_null());

  slot = gp_sys->waiting_threads;

  for (ulint i = 0; i < srv_max_n_threads; i++, ++slot) {
    if (!slot->in_use) {
      slot->in_use = true;
      slot->query_trx = trx;

      if (slot->event == nullptr) {
        slot->event = os_event_create(0);
      }
      os_event_reset(slot->event);
      slot->suspend = true;
      slot->suspend_time = ut_time_monotonic();
      slot->wait_timeout = wait_timeout;

      gp_sys->n_waiting++;

      /** GP state has been allocated */
      assert_gp_state_allocated(trx);

      /** Under the protect of gq trx mutex */
      trx->gp_state.slot = slot;
      return slot;
    }
  }
  ib::error(ER_IB_MSG_646)
      << "There appear to be " << srv_max_n_threads
      << " user"
         " threads currently waiting inside InnoDB, which is the upper"
         " limit. Cannot continue operation. Before aborting, we print"
         " a list of waiting threads.";
  /** TODO: print */

  ut_error;
}

/**
  Release the slot which is reserved by gloal query thread.

  @param[in]      slot
*/
void gp_release_slot(gp_slot_t *slot) {
  gp_wait_mutex_enter();

  ut_ad(slot->in_use);
  ut_ad(slot->suspend);
  ut_ad(slot->query_trx != nullptr);
  ut_ad(slot->query_trx->gp_state.slot == slot);
  /**
    Global query thread has been broadcast by:
     1) Background timeout thread
     2) Committed XA transaction
  */
  ut_ad(slot->query_trx->gp_state.blocking_trx == nullptr);
  /** Global query should complete XA transaction */
  ut_ad(slot->query_trx->gp_wait.validate_null());

  /** Clear the slot pointer */
  trx_mutex_enter(slot->query_trx);
  slot->query_trx->gp_state.slot = nullptr;
  /**
    Other state attributes which are protected by gp_sys->mutex
    has been cleared by timeout thread or XA transaction.
  */
  assert_gp_state_initial(slot->query_trx);
  trx_mutex_exit(slot->query_trx);

  slot->in_use = false;
  slot->suspend = false;
  slot->query_trx = nullptr;
  gp_sys->n_waiting--;

  gp_wait_mutex_exit();
}

/**
  Suspend global query thread
  @param[in]      trx   global query context
*/
void gp_wait_suspend_thread(trx_t *trx) {
  gp_slot_t *slot;
  long wait_timeout;
  gp_state_t *gp_state;

  wait_timeout = trx_gp_wait_timeout_get(trx);

  gp_wait_mutex_enter();
  trx_mutex_enter(trx);

  gp_state = &trx->gp_state;

  /** If XA commit has cancel the waiting */
  if (gp_state->waiting == false) {
    ut_a(gp_state->blocking_trx == nullptr);
    ut_a(gp_state->slot == nullptr);

    /** TODO: SET trx->error_state */
    gp_wait_mutex_exit();
    trx_mutex_exit(trx);

    trx->error_state = DB_SUCCESS;
    return;
  }
  /** GP state has been allocated */
  assert_gp_state_allocated(trx);

  /** Request wait slot */
  slot = gp_reserve_slot(trx, wait_timeout);

  gp_wait_mutex_exit();
  trx_mutex_exit(trx);

  bool was_declared_inside_innodb = trx->declared_to_be_inside_innodb;
  if (was_declared_inside_innodb) {
    srv_conc_force_exit_innodb(trx);
  }

  thd_wait_begin(trx->mysql_thd, THD_WAIT_SLEEP);

  os_event_wait(slot->event);

  thd_wait_end(trx->mysql_thd);

  if (was_declared_inside_innodb) {
    srv_conc_force_enter_innodb(trx);
  }

  const auto wait_time = ut_time_monotonic() - slot->suspend_time;

  /** Wakeup by timeout or XA commit */
  assert_gp_state_initial(trx);

  gp_release_slot(slot);

  if (wait_time > wait_timeout) {
    trx->error_state = DB_LOCK_WAIT_TIMEOUT;
  } else {
    trx->error_state = DB_SUCCESS;
  }
}

/**
  Global query vision judgement. Vision must include GCN number;

  @param[in]      trx     global query trx context
  @param[in]      rec     user record which should be read
  @param[in]      index   cluster index
  @param[in]      offset  rec_get_offsets(rec, index)
  @param[in]      pcur
  @param[in]      vision  consistent read view

  @retval         true    visible = true
  @retval         false
*/
bool gp_clust_rec_cons_read_sees(trx_t *trx, const rec_t *rec,
                                 dict_index_t *index, const ulint *offsets,
                                 btr_pcur_t *pcur, lizard::Vision *vision,
                                 dberr_t *error) {
  bool active;
#ifdef UNIV_DEBUG
  bool looped = false;
#endif
  ut_ad(index->is_clustered());
  ut_ad(page_rec_is_user_rec(rec));
  ut_ad(rec_offs_validate(rec, index, offsets));

  /* Temp-tables are not shared across connections and multiple
  transactions from different connections cannot simultaneously
  operate on same temp-table and so read of temp-table is
  always consistent read. */
  if (srv_read_only_mode || index->table->is_temporary()) {
    ut_ad(vision == 0 || !vision->is_active() || index->table->is_temporary());
    return (true);
  }

  ut_ad(vision->is_asof_gcn());

retry:
  trx_id_t trx_id = row_get_rec_trx_id(rec, index, offsets);

  /** Lizard: the following codes is a check */
  txn_rec_t txn_rec = {
      trx_id,
      row_get_rec_scn_id(rec, index, offsets),
      row_get_rec_undo_ptr(rec, index, offsets),
      row_get_rec_gcn(rec, index, offsets),
  };

  active = txn_rec_cleanout_state_by_misc(&txn_rec, pcur, rec, index, offsets);
  /** 1. Already committed; */
  if (!active) {
    ut_a(txn_rec.gcn != GCN_NULL);
    ut_a(txn_rec.scn != SCN_NULL);

    return (vision->modifications_visible(&txn_rec, index->table->name));
  } else {
    /**
      Here, maybe active or prepared, prepared state has to wait for a
      while until commit. */
    ut_ad(looped == false);
    ut_ad(txn_rec.gcn == GCN_NULL);

    /** Find the prepared trx to wait, others should judge visible directly */
    std::pair<trx_t *, trx_state_t> result = trx_rw_is_prepared(trx_id);

    switch (result.second) {
        /** 2.1. Still active, judge it for whether itself or not */
      case TRX_STATE_ACTIVE:
        return (vision->modifications_visible(&txn_rec, index->table->name));

        /** 2.2. Already commit, judge it again */
      case TRX_STATE_COMMITTED_IN_MEMORY:
        ut_ad(looped == false);
        ut_d(looped = true);
        goto retry;

        /** 2.3. Blocking trx is prepared, and its commit has been blocked */
      default:
        ut_ad(result.second == TRX_STATE_PREPARED);
        gp_build_wait_state(trx, result.first);
        /** Allow prepared trx to commit */
        trx_release_reference(result.first);
        *error = DB_GP_WAIT;
        return false;
    }
  }
}

static void gp_wait_check_slots_for_timeouts() {
  gp_slot_t *slot;
  int slot_cnt = 0;

  gp_wait_mutex_enter();

  slot = gp_sys->waiting_threads;
  for (ulint i = 0; i < srv_max_n_threads; i++, ++slot) {
    if (slot->in_use) {
      slot_cnt++;
      gp_wait_check_and_cancel(slot);
      if (slot_cnt >= gp_sys->n_waiting.load()) break;
    }
  }

  gp_wait_mutex_exit();
}

void gp_wait_timeout_thread() {
  ut_ad(!srv_read_only_mode);

  /** The last time we've checked for timeouts. */
  auto last_checked_for_timeouts_at = ut_time();
  do {
    auto now = ut_time();

    if (0.5 < ut_difftime(now, last_checked_for_timeouts_at)) {
      last_checked_for_timeouts_at = now;
      gp_wait_check_slots_for_timeouts();
    }

    os_thread_sleep(1000000);

  } while (srv_shutdown_state.load() == SRV_SHUTDOWN_NONE);
}

}  // namespace lizard

