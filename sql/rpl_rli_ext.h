/* Copyright (c) 2006, 2018, Oracle and/or its affiliates. Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All rights reserved.

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

#ifndef RPL_RLI_EXT_H
#define RPL_RLI_EXT_H

#include <atomic>

class Relay_log_info;
class Log_event;

extern bool opt_consensus_index_buf_enabled;

void update_consensus_apply_index(Relay_log_info *rli, Log_event *ev);
void mts_init_consensus_apply_index(Relay_log_info *rli,
                                    uint64 consensus_index);
void mts_advance_consensus_apply_index(Relay_log_info *rli, Log_event *ev);
void mts_force_consensus_apply_index(Relay_log_info *rli,
                                     uint64 consensus_index);

class Index_link_buf {
 public:
  /**
    Constructs the index buffer. Allocated memory for the slots.
    Initializes the tail pointer with 0.

    @param[in]	capacity	number of slots in the ring buffer
  */
  explicit Index_link_buf(uint64 capacity);

  /** Destructs the link buffer. Deallocates memory for the links. */
  ~Index_link_buf();

  /** Add a directed index. It is user's
  responsibility to ensure that there is space for the link. This is
  because it can be useful to ensure much earlier that there is space.
  In addition, advances the tail pointer in the buffer if possible.

  @param[in]	index	index	position in original unit

  @return tail position in original unit*/
  uint64 add_index_advance_tail(uint64 index);

  /** force advance the tail pointer in the buffer .

  @see force_advance_tail()

  @return tail position in original unit*/
  void force_advance_tail(uint64 index);

  /** init the tail pointer in the buffer .

  @see init_tail() */
  void init_tail(uint64 index);

 private:
  /** Translates position expressed in original unit to position
  in the m_indexes (which is a ring buffer).

  @param[in]	index	position in original unit

  @return slot index in the m_indexes */
  uint64 get_slot_index(uint64 index) const;

  /** Advances the tail pointer in the buffer .

  @see advance_tail()

  @return tail position in original unit */
  uint64 advance_tail();

  /** atomic<bool> lock

  @see lock()/unlock

  @return whether lock success */
  bool lock(bool retry = false);

  void unlock();

 private:
  /** Capacity of the buffer. */
  uint64 m_capacity;

  /** Pointer to the ring buffer (unaligned). */
  std::atomic<uint64> *m_indexes;

  /**
   * Tail pointer in the buffer (expressed in original unit).
   * Lastest setted index.
   */
  std::atomic<uint64> m_tail;

  /** atomic lock*/
  std::atomic<bool> m_locked;
};

#endif