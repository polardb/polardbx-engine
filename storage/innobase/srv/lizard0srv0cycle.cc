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

/** @file srv/lizard0srv0cycle.cc
  txn slot cycle service.

 Created 2025-07-14 by Jianwei.zhao
 *******************************************************/

#include "srv0srv.h"
#include "trx0sys.h"

#include "lizard0trx0cycle.h"

namespace lizard {

#ifdef UNIV_DEBUG
bool srv_cycle_run_debug = true;
#endif

/** Wait for the cycle coordinator thread to start up.*/
void srv_start_wait_for_cycle_to_start() {
  cycle_state_t state = cycle_sys->state;

  while (srv_shutdown_state.load() < SRV_SHUTDOWN_PURGE &&
         srv_force_recovery < SRV_FORCE_NO_BACKGROUND &&
         state == CYCLE_STATE_INIT) {
    switch (state = cycle_sys->state) {
      case CYCLE_STATE_RUN:
        break;

      case CYCLE_STATE_INIT:
        ib::info(ER_IB_MSG_1097);

        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        break;

      case CYCLE_STATE_EXIT:
        ut_error;
    }
  }
}

/** We reuse purge logic by shutdown state. */
bool srv_cycle_should_exit(ulint n_logs_cycled) {
  return srv_purge_should_exit(n_logs_cycled);
}

/** Try to wakeup thread if suspend. */
void srv_cycle_wakeup(void) {
  ut_ad(!srv_read_only_mode);

  if (srv_force_recovery < SRV_FORCE_NO_BACKGROUND) {
    srv_release_threads(SRV_CYCLE, 1);
  }
}

/**Suspend cycle coordinator thread.
 *
 * @param[in]	srv slot
 * @param[in]	rseg history len before call
 * @param[in]	threshold which break wait.
 * @param[in]	n logs has been cycled.
 * */
void srv_cycle_coordinator_suspend(srv_slot_t *slot, ulint rseg_history_len,
                                   ulint threshold, ulint n_logs_cycled) {
  ut_ad(!srv_read_only_mode);
  ut_a(slot->type == SRV_CYCLE);

  /** 1000ms wait time on the cycle event. */
  constexpr std::chrono::milliseconds SRV_TIMEOUT_200{200};
  /** 500ms wait time on the cycle event. */
  constexpr std::chrono::milliseconds SRV_TIMEOUT_100{100};
  /** max loop times */
  constexpr size_t MAX_TIMES = 5;

  int64_t sig_count = srv_suspend_thread(slot);

  ulint wait_time = 0;

  do {
    ulint ret = 0;
    ulint history_len = trx_sys->txn_rseg_history_len.load();

    if (n_logs_cycled == 0) {
      ret = os_event_wait_time_low(slot->event, SRV_TIMEOUT_200, sig_count);
      wait_time++;
    } else if (n_logs_cycled < threshold) {
      ret = os_event_wait_time_low(slot->event, SRV_TIMEOUT_100, sig_count);
      wait_time++;
    } else {
      /** if more than batch size*/
      ret = 0;
      break;
    }

    /**TODO: */
    if (ret != OS_SYNC_TIME_EXCEEDED) {
      break;
    }

    /** More than 5 times. */
    if (wait_time >= MAX_TIMES) break;

    if (history_len != rseg_history_len && history_len >= threshold) {
      break;
    }

  } while (srv_shutdown_state.load() < SRV_SHUTDOWN_PURGE);

  srv_wakeup_if_suspend(slot);
}

/** Entrance of cycle coordinator thread. */
void srv_cycle_coordinator_thread() {
  ulint n_logs_cycled = ULINT_UNDEFINED;
  srv_slot_t *slot;

  cycle_sys->state = CYCLE_STATE_RUN;

  slot = srv_reserve_slot(SRV_CYCLE);

  do {
    /** batch size for every rsegment. */
    ulint batch_size = srv_cycle_rseg_batch_size;

    /** Sum of all txn slot rsegment. */
    ulint threshold = TXN_RSEGS * batch_size;

#ifdef UNIV_DEBUG
    while (!srv_cycle_run_debug &&
           srv_shutdown_state.load() < SRV_SHUTDOWN_PURGE) {
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
#endif

    /** If cycled logs is less than batch, suspend */
    if (srv_shutdown_state.load() < SRV_SHUTDOWN_PURGE &&
        (n_logs_cycled < threshold)) {
      srv_cycle_coordinator_suspend(slot, trx_sys->txn_rseg_history_len.load(),
                                    threshold, n_logs_cycled);
    }

    if (srv_cycle_should_exit(n_logs_cycled)) {
      ut_a(!slot->suspended);
      break;
    }

    n_logs_cycled = 0;

    n_logs_cycled = cycle_sys->recycle_history(batch_size);
  } while (!srv_cycle_should_exit(n_logs_cycled));

  n_logs_cycled = ULINT_MAX;

  /* Ensure that all records are purged if it is not a fast shutdown.
  This covers the case where a record can be added after we exit the
  loop above. */
  while (srv_fast_shutdown == 0 && n_logs_cycled > 0) {
    n_logs_cycled = cycle_sys->recycle_history(srv_cycle_rseg_batch_size);
  }

  srv_free_slot(slot);

  cycle_sys->state = CYCLE_STATE_EXIT;
}

}  // namespace lizard
