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

/** @file sql/consensus/komodo0fifo0cache.h
  Komodo fifo cache.

 Created 2025-05-16 by Jiyang.zhang
 *******************************************************/
#ifndef komodo0fifo0cache_h
#define komodo0fifo0cache_h

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "my_inttypes.h"
#include "sql/binlog.h"
#include "sql/mysqld.h"
#include "sql/raii/sentry.h"
#include "sql/consensus/komodo0buffer.h"

namespace komodo {
class Fifo_cache_status;

#ifdef HAVE_PSI_INTERFACE
extern PSI_memory_key key_memory_FIFO_CACHE;

extern PSI_rwlock_key key_rwlock_FIFO_RESIZE;

extern PSI_mutex_key key_mutex_FIFO_CACHE;
#endif

class Fifo_cache {
 public:
  static constexpr uint64 INVALID_INDEX = 0;

 public:
  virtual ~Fifo_cache() {}

  /**
    Open a writable buffer in the FIFO cache for the given log index,
    and write the payload into it. No need to close buffer.

    @param[in] term         consensus term
    @param[in] index        consensus index
    @param[in] payload_len  consensus payload length
    @param[in] payload      consensus payload
    @param[in] outer        true if it is Consensus_cluster_info_log_event
    @param[in] flag         consensus log entry flag
  */
  virtual void open_and_write_buffer(uint64 term, uint64 index,
                                     size_t payload_len, const uchar *payload,
                                     bool outer, uint flag) = 0;

  /**
    Open a writable buffer in the FIFO cache for the given log index.
    Must explicitly close buffer.

    @param[in] term         consensus term
    @param[in] index        consensus index
    @param[in] payload_len  consensus payload length
    @param[in] outer        true if it is Consensus_cluster_info_log_event
    @param[in] flag         consensus log entry flag
    @param[in] malloc_if_not_enough
    If malloc_if_not_enough is true, the underlying pool may fallback to a
    malloc-backed buffer when preallocated space is insufficient.

    @param[in] mutex_held
    If mutex_held is true, the caller must already hold the cache mutex via
    lock(), and open_buffer will not re-lock or unlock it. This is intended for
    batch operations that need to avoid interleaving and potential deadlocks
    when allocating multiple buffers.

    @return opened buffer, never nullptr. If resources are insufficient,
            the oldest entries are evicted (and the pool may fallback to
            malloc when malloc_if_not_enough is true) until the open
            succeeds.
  */
  virtual Buffer *open_buffer(uint64 term, uint64 index, size_t payload_len,
                              bool outer, uint flag,
                              bool malloc_if_not_enough = false,
                              bool mutex_held = false) = 0;

  /**
    Close an opened buffer.

    On success, mark the slot as used and make it visible to readers.
    On error, unlock the slot and evict all slots whose index <= index.

    Note: opened buffers are not necessarily closed in open order; the
    batch write path (Buffer_array::append) closes the oldest one first.

    @param[in] index        consensus index
    @param[in] error        true to discard: unlock and evict slots <= index
  */
  virtual void close_buffer(uint64 index, bool error) = 0;

  /**
    Get the consensus log from the cache.
    @return CLC_ALREADY_SWAP_OUT if the the log content is not in the cache.
  */
  [[nodiscard]] virtual int get(uint64 index, uint64 *term,
                                std::string &log_content, bool *outer,
                                uint *flag) = 0;

  /**
    Evict the consensus log whose index >= low_index
  */
  virtual void evict_newest_until(uint64 low_index) = 0;

  /**
    Evict the consensus log whose index <= up_index
  */
  virtual void evict_oldest_until(uint64 up_index, bool error) = 0;

  /**
    Try to evict the consesus log whose index <= hint_index
    @param[in]  hint_index    Try to evict the consesus log whose
                              index <= hint_index
    @param[in]  hint_count    Try to evict hint_count consensus log
  */
  virtual void evict_oldest_try(uint64 hint_index, uint64 hint_count) = 0;

  /**
    Get total log size between [begin_index, end_index] which in cache.
    @param[in]  begin_index       Get total log size between
                                  [begin_index, end_index]
    @param[in]  max_packet_size   If beyond max_packet_size then exit early
  */
  virtual uint64 get_log_size(uint64 begin_index, uint64 end_index,
                              uint64 max_packet_size) = 0;

  /**
    Export cache_status.
  */
  virtual void export_status(Fifo_cache_status *cache_status) = 0;

  /**
    Resize the Fifo Cache to the size of new_size_in_bytes.
  */
  virtual void resize(const size_t new_size_in_bytes) = 0;

  /**
    Check if the cache has enough resources.
  */
  virtual bool has_space_for(const size_t index_count,
                             const size_t space_bytes) const = 0;

  /** Acquire/release the internal mutex protecting the cache. */
  virtual void lock() = 0;
  virtual void unlock() = 0;
};

extern Fifo_cache *create_fifo_cache(size_t max_fifo_cache_size_arg);

}  // namespace komodo

#endif
