/*****************************************************************************

Copyright (c) 2013, 2023, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file sql/komodo0flusher.h

  Komodo flusher in group commit

  Created 2025-05-26 by Yuan Ting
 *******************************************************/

#ifndef komodo0flusher_h
#define komodo0flusher_h

#include <sys/types.h>
#include <cstddef>
#include <memory>
#include "gcn_log_event.h"
#include "my_inttypes.h"
#include "paxos.h"
#include "sql/consensus_log_manager.h"
#include "sql/log_event.h"

class THD;
class Binlog_event_writer;
class Gcn_log_event;
class binlog_cache_data;
class Consensus_log_event;
class Gtid_log_event;

namespace komodo {
class Prealloc_binlog_event_data_allocator;
class Peekable_binlog_event_data_istream;

/**
 * @brief Base class for flushing transaction logs.
 *
 * This class provides a base interface for flushing transaction logs.
 */
class TLog_flusher {
 public:
  TLog_flusher(Consensus_log_manager *log_mgr) : m_log_mgr(log_mgr) {}

  virtual ~TLog_flusher() {}

  /**
   * Flushes a transaction log.
   *
   * This function writes the consensus log event, GCN (Global Commit Number)
   * event, GTID (Global Transaction ID) event, and the transaction content to
   * the binlog sequentially.
   *
   * @param thd         The current thread/session context.
   * @param cache_data  The binlog cache data containing the transaction.
   * @return            true if an error occurred during any write operation,
   * false otherwise.
   */
  virtual bool write(THD *, Gcn_log_event *, Gtid_log_event *,
                     binlog_cache_data *, Binlog_event_writer *) = 0;

 private:
  TLog_flusher(const TLog_flusher &) = delete;
  TLog_flusher &operator=(const TLog_flusher &) = delete;
  TLog_flusher(TLog_flusher &&) = delete;
  TLog_flusher &operator=(TLog_flusher &&) = delete;

 protected:
  Consensus_log_manager *m_log_mgr;
};

/**
 * @brief Flusher for binary logs.
 *
 * This class is responsible for flushing binary logs.
 */
class Binary_log_flusher : public TLog_flusher {
 public:
  Binary_log_flusher(Consensus_log_manager *log_mgr) : TLog_flusher(log_mgr) {}

  ~Binary_log_flusher() override = default;

  virtual bool write(THD *, Gcn_log_event *, Gtid_log_event *,
                     binlog_cache_data *, Binlog_event_writer *) override;
};

/**
 * @brief Flusher for consensus logs.
 *
 * This class is responsible for flushing consensus logs.
 */
class Consensus_log_flusher : public TLog_flusher {
 public:
  Consensus_log_flusher(Consensus_log_manager *log_mgr,
                        std::shared_ptr<alisql::Paxos> paxos,
                        Fifo_cache *cache_manager)
      : TLog_flusher(log_mgr), m_paxos(paxos), m_cache_manager(cache_manager) {}

  ~Consensus_log_flusher() override = default;

  virtual bool write(THD *, Gcn_log_event *, Gtid_log_event *,
                     binlog_cache_data *, Binlog_event_writer *) override;

 private:
  bool write_split(THD *, Gcn_log_event *, Gtid_log_event *,
                   binlog_cache_data *, Binlog_event_writer *,
                   ulonglong max_batch_size);

  bool write_no_split(THD *, Gcn_log_event *, Gtid_log_event *,
                      binlog_cache_data *, Binlog_event_writer *);

  bool write_consensus_log(THD *thd, uint flag, ulonglong length,
                           Binlog_event_writer *writer);

  bool read_batch(Peekable_binlog_event_data_istream *istream,
                  Prealloc_binlog_event_data_allocator *allocator,
                  bool *last_batch, ulonglong *events_count);

  bool write_batch(THD *thd, unsigned char *buffer, my_off_t length,
                   bool last_batch, ulonglong event_count,
                   Binlog_event_writer *writer);

  bool split_event_if_beyond_batch(THD *thd,
                                   Peekable_binlog_event_data_istream *istream,
                                   Binlog_event_writer *writer,
                                   bool *is_last_event, ulonglong batch_size);

  bool read_large_event(Peekable_binlog_event_data_istream *istream,
                        Default_binlog_event_allocator *allocator,
                        unsigned char **data, ulonglong *event_length,
                        bool *is_last_event);

 private:
  std::shared_ptr<alisql::Paxos> m_paxos;
  Fifo_cache *m_cache_manager;
};

}  // namespace komodo

#endif
