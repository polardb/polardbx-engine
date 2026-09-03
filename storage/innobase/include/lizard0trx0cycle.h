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

/** @file include/lizard0trx0cycle.h
  Transactional slot cycle management.

 Created 2025-07-03 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0trx0cycle_h
#define lizard0trx0cycle_h

#include "mem0mem.h"
#include "trx0types.h"

#include "lizard0cmmt0types.h"
#include "lizard0gcs0read.h"

#ifdef UNIV_PFS_MUTEX
/* cycle sys mutex PFS key */
extern mysql_pfs_key_t cycle_pq_mutex_key;
#endif

#ifdef UNIV_PFS_RWLOCK
/* cycle sys latch PFS key */
extern mysql_pfs_key_t cycle_rw_latch_key;
#endif

struct txn_slot_ptr_t;

namespace undo {
class Tablespace;
}  // namespace undo

namespace lizard {

/** Whether to print cycle process info. */
extern bool srv_cycle_print_process_debug;

/** batch size for every txn slot rsegment . */
extern ulint srv_cycle_rseg_batch_size;

/** All txn slot rsegs. */
constexpr ulint TXN_RSEGS =
    FSP_IMPLICIT_TXN_TABLESPACES * FSP_MAX_ROLLBACK_SEGMENTS;

static_assert(TXN_RSEGS == 512, "All txn slot rsegs is 512");

enum cycle_state_t {
  CYCLE_STATE_INIT,  // cycle instance created.
  CYCLE_STATE_RUN,   // Cycle should be running.
  CYCLE_STATE_EXIT   // Cycle has been shutdown.
};

/** Vision for cycle system. */
struct cycle_vision_t {
 public:
  cycle_vision_t() : m_vision(), m_limit() {}

  scn_t snapshot_scn() const { return m_vision.snapshot_scn(); }

  Vision *vision() { return &m_vision; }

  void set_limit(commit_order_t value) { m_limit = value; }

  commit_order_t limit() const { return m_limit; }

 private:
  Vision m_vision;
  /** Cycled allowed end position which come from min active scn. */
  commit_order_t m_limit;
};


/** The control structure of cycle operation. */
struct trx_cycle_t {
 public:
  trx_cycle_t();

  virtual ~trx_cycle_t();

 public:
  /** Boot cycle system. */
  void boot();

  /** Try to recycle txn slot history.
   *
   * @retval	how many txn slot were recycled.
   * */
  ulint recycle_history(ulint batch_size);

  /** Get last */
  commit_order_t last() const {
    return {m_last_scn.load(), m_last_us, m_last_gcn};
  }

 private:
  /** Iterate next recycle round through clone oldest vision, return
   * until commit point
   *
   * @retval	commit order which is oldest vision or min safe scn.
   * */
  commit_order_t iterate();

  /** Save last. */
  void store_last(const commit_order_t &ommt) {
    m_last_scn.store(ommt.scn);
    m_last_us = ommt.us;
    m_last_gcn = ommt.gcn;
  }

  /** txn slot header can be recycled .
  * @param[in]		ommt      commit order
  * @retval	    true if blocking cycle */
  bool advice(const commit_order_t &ommt);
  /** Boot a space rsegs. 
   *
   * @param[in]		txn slot tablespace
   *
   * @retval	minimum last order or null
   * */
  commit_order_t boot_space(undo::Tablespace *space);

  /** Recycle all rsegs within space.
   *
   * @param[in]		txn slot tablespace
   * @param[in]		batch size every rseg.
   * @param[in/out]	how many txn slot recycled.
   *
   * @retval	minimum last order or null
   * */
  commit_order_t recycle_space(undo::Tablespace *space, ulint batch_size,
                               ulint *n_logs);
  /** try to recycle txn slot rsegment
   * @param[in]		txn slot roll segment
   * @param[in]		batch size every rseg.
   * @param[in/out]	how many logs was recycled
   *
   * @retval minimum last order or null
   * */
  commit_order_t recycle_rseg(trx_rseg_t *rseg, ulint batch_size,
                              ulint *n_logs);


 private:
  /** Oldest vision. */
  cycle_vision_t m_cvision;

  /** Same as priority queue. but here real-time calc */
  std::atomic<scn_t> m_last_scn;
  utc_t m_last_us;
  gcn_t m_last_gcn;

  rw_lock_t *m_rw_latch;

  ib_mutex_t m_pq_mutex;

  /** boot last scn when restart. */
  bool m_last_scn_booted;

 public:
  /** Label it to skip unnessary wakeup by commit. */
  volatile cycle_state_t state;
};

/** Global data structure of cycle system. */
extern trx_cycle_t *cycle_sys;

/** Allocate memory structure. */
extern void trx_cycle_sys_mem_create();

/** Initialize cycle system. */
extern void trx_cycle_sys_initialize(size_t n_threads);

extern void trx_cycle_sys_close();

/*-----------------------------------------------------------------------------*/
/**
  Add the txn undo log header into history for cycle.

  @param[in]      trx       transaction
  @param[in/out]  undo_ptr    txn undo log structure
  @param[in]      undo_page   txn undo log header page, x-latched
  @param[in]      mtr */
void trx_cycle_add_txn_slot_to_history(trx_t *trx, txn_slot_ptr_t *undo_ptr,
                                       page_t *undo_page, mtr_t *mtr);

}  // namespace lizard

#endif
