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

/** @file include/lizard0gp.h
  Lizard global query.

 Created 2020-12-30 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0gp_h
#define lizard0gp_h

#include "lizard0gp0types.h"
#include "trx0trx.h"
#include "ut0mutex.h"

#ifdef UNIV_PFS_THREAD
extern mysql_pfs_key_t srv_gp_wait_timeout_thread_key;
#endif

/**
  Global query && XA prepared transaction logic:
  ---------------------------------------------

  1) Global query will take the GCN as the vision judgement;
  2) If XA transaction has finished XA PREPARE, it didn't allowed
     to execute GLOAL QUERY within current trx context.


  Blocking relationship:
  ---------------------------------------------

  1) Global query trx will state into trx->gp_state
  2) XA prepared trx will state into trx->gp_wait
  3) Wakeup [by timeout or commit] only cancel the blocking,
     GP should retry once again.

  Locking strategy:
  ---------------------------------------------

  0) Find prepared XA
     trx_sys_mutex_enter();
     search_prepare_trx_by_id();
     trx_reference();
     trx_sys_mutex_exit();

  1) Construct blocking relationship:
      gp_lock_enter();

       query_trx_mutex_enter();
         query_trx->gp_state.waiting = true;
         query_trx->gp_state.blocking_trx = prepare_trx;
       query_trx_mutex_exit();

       prepare_trx_mutex_enter();
         prepare_trx->gp_wait.enter_queue(query_trx);
       prepare_trx_mutex_exit();

       gp_lock_exit();

  2) Wakeup
      gp_lock_enter();

        for ()
          prepare_trx_mutex_enter();
          wakeup
          prepare_trx_mutex_exit();

      gp_lock_exit();


  3) Timeout
      gp_wait_mutex_enter();

      for ()
        gp_lock_enter();

          wakeup()

        gp_lock_enter();

      gp_wait_mutex_exit();


  4) Release slot
      gp_wait_mutex_enter();

      release_slot();

      gp_wait_mutex_exit();



  MVCC vision:
  ---------------------------------------------
  1) According to the txn GCN to determined:
    -- if trx_state < PREPARED;  visible = (rec_trx_id == trx_id);
    -- if trx_state == PREPARED; wait->loop;
    -- if trx_state == COMMITTED; Visible = (rec_gcn <= gcn);

  for vision = false
     goto build prev version;
*/

typedef ib_mutex_t GpMutex;

#ifdef UNIV_PFS_MUTEX
/* Global query mutex PFS key */
extern mysql_pfs_key_t gp_sys_mutex_key;
/* Global query wait mutex PFS key */
extern mysql_pfs_key_t gp_sys_wait_mutex_key;
#endif

/** Global query blocking system */
struct gp_sys_t {
  /** Global query system mutex */
  GpMutex mutex;

  /** Protect the waiting_thread and n_waiting */
  GpMutex wait_mutex;

  /** Array of waiting threads */
  gp_slot_t *waiting_threads;

  /** Counter of waiting threads */
  std::atomic<int> n_waiting{0};
};

namespace lizard {

extern gp_sys_t *gp_sys;

/** Create global query system */
void gp_sys_create();

/** Close global query system */
void gp_sys_destroy();

/**
  Suspend global query thread
  @param[in]      trx   global query context
*/
void gp_wait_suspend_thread(trx_t *trx);

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
                                 dberr_t *error);

/**
  Cancal all blocked global query when commit
  @param[in]      trx       committing trx
*/
void gp_wait_cancel_all_when_commit(trx_t *trx);

void gp_wait_timeout_thread();

}  // namespace lizard

inline std::chrono::seconds trx_gp_wait_timeout_get(const trx_t *t) {
  return thd_global_query_wait_timeout((t)->mysql_thd);
}

#define gp_mutex_own() (lizard::gp_sys->mutex.is_owned())

/* Acquire gp_sys->mutex */
#define gp_mutex_enter()         \
  do {                           \
    mutex_enter(&gp_sys->mutex); \
  } while (0)

#define gp_mutex_exit()         \
  do {                          \
    mutex_exit(&gp_sys->mutex); \
  } while (0)

#define gp_wait_mutex_own() (gp_sys->wait_mutex.is_owned())

/* Acquire gp_sys->mutex */
#define gp_wait_mutex_enter()         \
  do {                                \
    mutex_enter(&gp_sys->wait_mutex); \
  } while (0)

#define gp_wait_mutex_exit()         \
  do {                               \
    mutex_exit(&gp_sys->wait_mutex); \
  } while (0)

#define GP_STATE_NULL \
  { false, nullptr, nullptr }

/** Return the global query state */
inline gp_state_t *trx_gp_state(trx_t *trx) { return &trx->gp_state; }

/** Return the prepared transaction wait information */
inline gp_wait_t *trx_gp_wait(trx_t *trx) { return &trx->gp_wait; }

/**-------------------- DEBUG ----------------------*/
#if defined UNIV_DEBUG || defined LIZARD_DEBUG

#define assert_gp_state_initial(trx)             \
  do {                                           \
    ut_a(trx->gp_state.waiting == false &&       \
         trx->gp_state.blocking_trx == nullptr); \
  } while (0)

#define assert_gp_state_allocated(trx)           \
  do {                                           \
    ut_a(trx->gp_state.waiting != false &&       \
         trx->gp_state.blocking_trx != nullptr); \
  } while (0)
#else
#define assert_gp_state_initial(trx)
#define assert_gp_state_allocated(trx)
#endif  // defined UNIV_DEBUG || defined LIZARD_DEBUG

#endif
