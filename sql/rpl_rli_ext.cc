/* Copyright (c) 2006, 2018, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   51 Franklin Street, Suite 500, Boston, MA 02110-1335 USA */

#include "sql/rpl_rli_ext.h"
#include "bl_consensus_log.h"
#include "sql/log_event.h"
#include "sql/rpl_rli.h"
#include "storage/innobase/include/ut0dbg.h"

bool opt_consensus_index_buf_enabled = false;

void update_consensus_apply_index(Relay_log_info *rli, Log_event *ev) {
  /**
   * 1. xpaxos_replication_channel
   * 2. end_group of current consensus index or not
   */
  if (!rli || !rli->info_thd->xpaxos_replication_channel) return;

  if (rli->is_parallel_exec()) {
    /**
     * if rli->m_consensus_index_buf is not inited
     *  mts will update consensus index in mts_checkpoint_routine
     */
    mts_advance_consensus_apply_index(rli, ev);
  } else {
    if (!ev || ev->future_event_relay_log_pos != ev->consensus_index_end_pos)
      return;

    if (ev->consensus_real_index > consensus_ptr->getAppliedIndex()) {
      consensus_ptr->updateAppliedIndex(ev->consensus_real_index);
      replica_read_manager.update_lsn(ev->consensus_real_index);
    }
  }
}

void mts_init_consensus_apply_index(Relay_log_info *rli,
                                    uint64 consensus_index) {
  /** rli->m_consensus_index_buf will be inited in mts and xpaxos_replication */
  if (!rli || !rli->m_consensus_index_buf) return;

  // assert(rli->is_parallel_exec());
  assert(rli->info_thd->xpaxos_replication_channel);

  xp::system(ER_XP_APPLIER) << "mts_init_consensus_apply_index "
                          << consensus_index;

  rli->m_consensus_index_buf->init_tail(consensus_index);
}

void mts_advance_consensus_apply_index(Relay_log_info *rli, Log_event *ev) {
  /** rli->m_consensus_index_buf will be inited in mts and xpaxos_replication */
  if (!rli || !rli->m_consensus_index_buf) return;

  /** current index group has not finished */
  if (!ev || ev->future_event_relay_log_pos != ev->consensus_index_end_pos)
    return;

  assert(rli->is_parallel_exec());
  assert(rli->info_thd->xpaxos_replication_channel);

  uint64 consensus_index = rli->m_consensus_index_buf->add_index_advance_tail(
      ev->consensus_real_index);

  if (consensus_index > consensus_ptr->getAppliedIndex()) {
    consensus_ptr->updateAppliedIndex(consensus_index);
    replica_read_manager.update_lsn(consensus_index);
  }
}

void mts_force_consensus_apply_index(Relay_log_info *rli,
                                     uint64 consensus_index) {
  /** rli->m_consensus_index_buf will be inited in mts and xpaxos_replication */
  if (rli && rli->m_consensus_index_buf) {
    assert(rli->is_parallel_exec());
    assert(rli->info_thd->xpaxos_replication_channel);
    rli->m_consensus_index_buf->force_advance_tail(consensus_index);
  }

  if (consensus_index > consensus_ptr->getAppliedIndex()) {
    consensus_ptr->updateAppliedIndex(consensus_index);
    replica_read_manager.update_lsn(consensus_index);
  }
}

Index_link_buf::Index_link_buf(uint64 capacity)
    : m_capacity(capacity), m_indexes(nullptr), m_tail(0), m_locked(false) {
  if (capacity == 0) return;

  m_indexes = (std::atomic<uint64> *)my_malloc(
      PSI_INSTRUMENT_ME, sizeof(std::atomic<uint64>) * capacity, MYF(0));
  ut_a(m_indexes != nullptr);

  for (size_t i = 0; i < capacity; ++i) {
    m_indexes[i].store(0);
  }
}

Index_link_buf::~Index_link_buf() {
  if (m_indexes) my_free(m_indexes);
}

bool Index_link_buf::lock(bool retry) {
  uint retry_times = 0;
  bool expected = false;
  if (m_locked.compare_exchange_strong(expected, true,
                                       std::memory_order_seq_cst))
    return true;

  if (retry) {
    expected = false;
    while (!m_locked.compare_exchange_strong(expected, true,
                                             std::memory_order_seq_cst)) {
      expected = false;
      retry_times++;
      usleep(1000);

      if (retry_times > 10) return false;
    }
    return true;
  }

  return false;
}

void Index_link_buf::unlock() {
  m_locked.store(false, std::memory_order_seq_cst);
}

inline void Index_link_buf::init_tail(uint64 index) { m_tail.store(index); }

inline uint64 Index_link_buf::get_slot_index(uint64 index) const {
  return index % m_capacity;
}

inline uint64 Index_link_buf::add_index_advance_tail(uint64 index) {
  uint64 old_tail = m_tail.load();
  uint64 ret = 0;
  ut_a(index <= old_tail + m_capacity);

  if (index <= old_tail) return index;

  auto slot_index = get_slot_index(index);
  auto &slot = m_indexes[slot_index];
  slot.store(index, std::memory_order_release);
  ret = advance_tail();

  // xp::info(ER_XP_APPLIER) << "add_index_advance_tail " << index
  //   << ", old tail " << old_tail
  //   << ", old count " << (index > old_tail ? index - old_tail : 0)
  //   << ", curr tail " << ret
  //   << ", curr count " << (index > ret ? index - ret : 0);
  return ret;
}

inline uint64 Index_link_buf::advance_tail() {
  if (!lock()) return 0;
  auto current_index = m_tail.load();

  while (true) {
    auto advance_index = current_index + 1;
    auto slot_index = get_slot_index(advance_index);
    auto &slot = m_indexes[slot_index];

    if (slot.load(std::memory_order_acquire) != advance_index) break;

    current_index = ++m_tail;
  }

  unlock();
  return current_index;
}

inline void Index_link_buf::force_advance_tail(uint64 index) {
  if (m_tail.load() >= index) return;

  if (!lock(true)) return;

  if (m_tail.load() >= index) {
    unlock();
    return;
  }

  // xp::info(ER_XP_APPLIER) << "force_advance_tail " << index
  //   << ", old tail " << m_tail.load()
  //   << ", old count " << (index > m_tail.load() ? index - m_tail.load() : 0);

  m_tail.store(index);
  unlock();
}
