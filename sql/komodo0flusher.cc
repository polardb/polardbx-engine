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

/** @file sql/komodo0flusher.cc

  Komodo flusher in group commit

  Created 2025-05-26 by Yuan Ting
 *******************************************************/

#include "sql/komodo0flusher.h"
#include "paxos.h"
#include "raii/sentry.h"
#include "sql/binlog.h"
#include "sql/consensus_log_event.h"
#include "sql/gcn_log_event.h"

#include "sql/consensus/komodo0buffer.h"
#include "sql/consensus/komodo0fifo0cache.h"
#include "sql/komodo0assert.h"
#include "sql/komodo0binlog0reader.h"
#include "sql/komodo0mon.h"

namespace komodo {

class Split_batch_buffer : public Prealloc_binlog_event_data_allocator,
                           public Basic_ostream {
 public:
  Split_batch_buffer(ulonglong prealloc_size)
      : Prealloc_binlog_event_data_allocator(prealloc_size) {}

  virtual ~Split_batch_buffer() {}

  virtual bool write(const unsigned char *data, my_off_t length) override {
    if (length > free()) {
      return true;
    }

    unsigned char *ptr = allocate(length);
    memcpy(ptr, data, length);
    return false;
  }
};

static inline ulonglong cal_event_len_with_checksum(ulonglong buf_len,
                                                    bool has_checksum) {
  return buf_len + (has_checksum ? BINLOG_CHECKSUM_LEN : 0);
}

static inline ulonglong calc_payload_length(Gcn_log_event *gcn_event,
                                            Gtid_log_event *gtid_event,
                                            bool need_checksum) {
  ulonglong payload_len = 0;
  if (gcn_event) {
    payload_len += cal_event_len_with_checksum(gcn_event->get_event_length(),
                                               need_checksum);
  }

  payload_len += gtid_event->transaction_length;
  return payload_len;
}

static my_off_t cal_payload_length_by_cache_size(my_off_t cache_size,
                                                 bool is_checksum_enabled,
                                                 ulonglong event_counter) {
  return cache_size +
         (is_checksum_enabled ? event_counter * BINLOG_CHECKSUM_LEN : 0);
}

bool Binary_log_flusher::write(THD *thd, Gcn_log_event *gcn_event,
                               Gtid_log_event *gtid_event,
                               binlog_cache_data *cache_data,
                               Binlog_event_writer *writer) {
  thd->consensus_index = m_log_mgr->fetch_and_inc_current_index();

  Consensus_log_event cons_event(
      0, thd->consensus_term, thd->consensus_index,
      calc_payload_length(gcn_event, gtid_event,
                          writer->is_checksum_enabled()));

  return (cons_event.write(writer) || (gcn_event && gcn_event->write(writer)) ||
          gtid_event->write(writer) ||
          mysql_bin_log.write_cache(thd, cache_data, writer));
}

bool Consensus_log_flusher::write(THD *thd, Gcn_log_event *gcn_event,
                                  Gtid_log_event *gtid_event,
                                  binlog_cache_data *cache_data,
                                  Binlog_event_writer *writer) {
  ulonglong max_log_size = opt_consensus_max_log_size;
  ulonglong large_trx_split_size = opt_consensus_large_trx_split_size;

  bool need_split = cache_data->get_byte_position() > max_log_size;
  DBUG_EXECUTE_IF("force_large_trx", { need_split = true; });
  flusher_status.trx++;
  if (need_split) {
    return write_split(thd, gcn_event, gtid_event, cache_data, writer,
                       std::min(large_trx_split_size, max_log_size));
  } else {
    return write_no_split(thd, gcn_event, gtid_event, cache_data, writer);
  }
}

bool Consensus_log_flusher::write_consensus_log(THD *thd, uint flag,
                                                ulonglong length,
                                                Binlog_event_writer *writer) {
  thd->consensus_index = m_log_mgr->fetch_and_inc_current_index();

  Consensus_log_event event(flag, thd->consensus_term, thd->consensus_index,
                            length);
  if (!(flag & Consensus_log_event_flag::FLAG_LARGE_TRX)) {
    m_paxos->getLocalServer()->setLastNonCommitDepIndex(thd->consensus_index);
  }

  return event.write(writer);
}

bool Consensus_log_flusher::read_batch(
    Peekable_binlog_event_data_istream *istream,
    Prealloc_binlog_event_data_allocator *allocator, bool *last_batch,
    ulonglong *events_count) {
  ulonglong next_event_length;
  ulonglong event_length;
  unsigned char *data = nullptr;

  *last_batch = false;
read_next_event:
  if (istream->peek_next_event_length(&next_event_length)) {
    if (istream->get_read_error().get_type() == Binlog_read_error::READ_EOF) {
      /** EOF */
      *last_batch = true;
      return false;
    } else {
      /** Something wrong must happened */
      return true;
    }
  }

  /** Batch might too large */
  kut_a(next_event_length > 0);
  if (next_event_length > allocator->free() ||
      (*events_count > 0 &&
       DBUG_EVALUATE_IF("force_large_trx_single_ev", 1, 0))) {
    return false;
  }

  /** Read event data to prealloc buffer. */
  if (istream->read_event_data(&data, &event_length, allocator)) {
    kut_a(istream->get_read_error().get_type() != Binlog_read_error::READ_EOF);
    return true;
  }

  kut_a(event_length == next_event_length);
  (*events_count)++;

  goto read_next_event;
}

bool Consensus_log_flusher::read_large_event(
    Peekable_binlog_event_data_istream *istream,
    Default_binlog_event_allocator *allocator, unsigned char **data,
    ulonglong *event_length, bool *is_last_event) {
  kut_a(*is_last_event == false);
  ulonglong next_event_length;

  if (istream->read_event_data(data, event_length, allocator)) {
    kut_a(istream->get_read_error().get_type() != Binlog_read_error::READ_EOF);
    return true;
  }

  if (istream->peek_next_event_length(&next_event_length)) {
    if (istream->get_read_error().get_type() == Binlog_read_error::READ_EOF) {
      /** EOF */
      *is_last_event = true;
      return false;
    } else {
      /** Something wrong must happened */
      return true;
    }
  }

  return false;
}

bool Consensus_log_flusher::write_batch(THD *thd, unsigned char *buffer,
                                        my_off_t buffer_length, bool last_batch,
                                        ulonglong event_count,
                                        Binlog_event_writer *writer) {
  uint32 flag = 0;
  my_off_t payload_length;
  my_off_t write_pos;
  Buffer *fifo_buffer = nullptr;
  bool err = false;

  /** Might large event. */
  if (event_count == 0) {
    kut_a(buffer_length == 0);
    return false;
  }

  /** 1. Generate Consensus_log_event by its payload. */
  kut_a(event_count);
  thd->consensus_index = m_log_mgr->fetch_and_inc_current_index();
  flag = last_batch ? Consensus_log_event_flag::FLAG_LARGE_TRX_END
                    : Consensus_log_event_flag::FLAG_LARGE_TRX;
  payload_length = cal_payload_length_by_cache_size(
      buffer_length, writer->is_checksum_enabled(), event_count);
  Consensus_log_event cons_event(flag, thd->consensus_term,
                                 thd->consensus_index, payload_length);
  if (last_batch) {
    m_paxos->getLocalServer()->setLastNonCommitDepIndex(thd->consensus_index);
  }

  /** 2. Write Consensus_log_event and its payload to writer and fifo cahce. */
  write_pos = writer->get_end_log_pos();
  if ((err = cons_event.write(writer))) {
    return true;
  }

  kut_a(write_pos + Consensus_log_event::MAX_EVENT_LENGTH +
            (writer->is_checksum_enabled() ? BINLOG_CHECKSUM_LEN : 0) ==
        writer->get_end_log_pos());

  {
    fifo_buffer = m_cache_manager->open_buffer(
        thd->consensus_term, thd->consensus_index, payload_length, false, flag);
    raii::Sentry<> guard([this, &thd, &err, &writer] {
      m_cache_manager->close_buffer(thd->consensus_index, err);
      writer->deregister_fifo();
    });

    DBUG_EXECUTE_IF("crash_during_large_trx_binlog_flush2", {
      // force miss 1 byte and then crash
      err = writer->write(buffer, buffer_length - 1);
      mysql_bin_log.flush();
      DBUG_SUICIDE();
    });
    writer->register_fifo(fifo_buffer);
    if ((err = writer->write(buffer, buffer_length))) {
      return true;
    }
  }

  kut_a(write_pos + Consensus_log_event::MAX_EVENT_LENGTH +
            (writer->is_checksum_enabled() ? BINLOG_CHECKSUM_LEN : 0) +
            payload_length ==
        writer->get_end_log_pos());

  /** 3. Flush to binlog. */
  if ((err = mysql_bin_log.flush())) return true;

  /** 4. Try to Sync binlog. */
  if (!opt_consensus_disable_sync_for_large_trx) {
    if ((err = mysql_bin_log.sync_binlog_file(false).first)) return true;
  }
  m_log_mgr->set_sync_index_if_greater(thd->consensus_index);
  /* use lockless writeLogDone */
  m_paxos->getLocalServer()->writeLogDoneInternal(thd->consensus_index, true);

  DEBUG_SYNC(thd, "large_trx_sync_part");
  DBUG_EXECUTE_IF("crash_during_large_trx_binlog_flush", { DBUG_SUICIDE(); });
  DBUG_EXECUTE_IF("crash_during_large_trx_binlog_flush_slow",
                  { /* let follower get the log */
                    sleep(2);
                    DBUG_SUICIDE();
                  });

  xp::info(ER_XP_COMMIT) << "large trx split: batch_size " << payload_length
                         << ", consensus_index " << thd->consensus_index;
  return false;
}

bool Consensus_log_flusher::split_event_if_beyond_batch(
    THD *thd, Peekable_binlog_event_data_istream *istream,
    Binlog_event_writer *writer, bool *is_last_event, ulonglong batch_size) {
  ulonglong large_event_split_size =
      std::min(batch_size, opt_consensus_large_event_split_size);
  bool error = false;
  uint flag = 0;
  ulonglong data_length = 0;
  ulonglong event_length = 0;
  ulonglong empty_event_count = 0;
  unsigned char *data = nullptr;
  my_off_t write_pos = writer->get_end_log_pos();
  Buffer_array buffers(m_cache_manager);
  bool add_fifo_cache = false;
  Default_binlog_event_allocator allocator;
  bool cache_manager_locked = false;
  ulonglong event_start_index = m_log_mgr->get_current_index() + 1;

  /* 1. Read large event */
  read_large_event(istream, &allocator, &data, &data_length, is_last_event);
  event_length =
      cal_event_len_with_checksum(data_length, writer->is_checksum_enabled());

  /* 2. Write empty events for all splits except the last one */
  empty_event_count = (event_length - 1) / large_event_split_size;
  add_fifo_cache =
      m_cache_manager->has_space_for(empty_event_count + 1, event_length);

  /*
    Hold the cache manager mutex to reserve multiple buffers atomically and
    prevent deadlocks during split allocations.
  */
  if (add_fifo_cache) {
    m_cache_manager->lock();
    cache_manager_locked = true;
  }
  for (ulonglong i = 0; !error && i < empty_event_count; i++) {
    flag = (i == 0) ? Consensus_log_event_flag::FLAG_BLOB_START : 0;
    flag |= Consensus_log_event_flag::FLAG_BLOB |
            Consensus_log_event_flag::FLAG_LARGE_TRX;

    Consensus_empty_log_event empty_event;
    ulonglong empty_event_length = cal_event_len_with_checksum(
        empty_event.get_event_length(), writer->is_checksum_enabled());
    error = write_consensus_log(thd, flag, empty_event_length, writer) ||
            empty_event.write(writer);
    if (error) goto func_exit;

    if (add_fifo_cache) {
      buffers.add_buffer(m_cache_manager->open_buffer(
                             thd->consensus_term, thd->consensus_index,
                             large_event_split_size, false, flag, true, true),
                         thd->consensus_index);
    }

    kut_a(write_pos + Consensus_log_event::MAX_EVENT_LENGTH +
              (writer->is_checksum_enabled() ? BINLOG_CHECKSUM_LEN : 0) +
              empty_event_length ==
          writer->get_end_log_pos());
    write_pos = writer->get_end_log_pos();

    DBUG_EXECUTE_IF("crash_during_large_event_binlog_flush", {
      static int ccnt = 0;
      ccnt++;
      if (ccnt > 1) {
        DBUG_SUICIDE();
      }
    });
    DBUG_EXECUTE_IF("crash_during_large_event_binlog_flush_slow",
                    { /* let follower get the log */
                      static int ccnt = 0;
                      ccnt++;
                      if (ccnt > 1) {
                        sleep(2);
                        DBUG_SUICIDE();
                      }
                    });
  }

  /* 3. Write the complete event at once */
  flag = *is_last_event ? Consensus_log_event_flag::FLAG_LARGE_TRX_END
                        : Consensus_log_event_flag::FLAG_LARGE_TRX;
  flag |= Consensus_log_event_flag::FLAG_BLOB_END;

  error = write_consensus_log(thd, flag, event_length, writer);
  if (error) goto func_exit;

  if (add_fifo_cache) {
    buffers.add_buffer(
        m_cache_manager->open_buffer(
            thd->consensus_term, thd->consensus_index,
            event_length - large_event_split_size * empty_event_count, false,
            flag, true, true),
        thd->consensus_index);
    kut_a(cache_manager_locked);
    m_cache_manager->unlock();
    cache_manager_locked = false;

    raii::Sentry<> guard([this, &thd, &error, &writer, &buffers] {
      buffers.close_buffers(error);
      writer->deregister_fifo();
    });
    writer->register_fifo(&buffers);
    error = writer->write(data, data_length);
  } else {
    error = writer->write(data, data_length);
  }
  if (error) goto func_exit;

  kut_a(write_pos + Consensus_log_event::MAX_EVENT_LENGTH +
            (writer->is_checksum_enabled() ? BINLOG_CHECKSUM_LEN : 0) +
            event_length ==
        writer->get_end_log_pos());

  if (*is_last_event) {
    m_paxos->getLocalServer()->setLastNonCommitDepIndex(thd->consensus_index);
  }

  error = mysql_bin_log.flush();
  if (error) goto func_exit;

  if (!opt_consensus_disable_sync_for_large_trx) {
    error = mysql_bin_log.sync_binlog_file(false).first;
    if (error) goto func_exit;
  }

  m_log_mgr->set_sync_index_if_greater(thd->consensus_index);
  /* use lockless writeLogDone */
  m_paxos->getLocalServer()->writeLogDoneInternal(thd->consensus_index, true);

  /* 4. Clear cache */
  if (!add_fifo_cache) {
    m_cache_manager->evict_newest_until(0);
  }

  xp::info(ER_XP_COMMIT) << "large event split: original event length "
                         << event_length << ", start consensus_index "
                         << event_start_index << ", end consensus_index "
                         << thd->consensus_index << ", split batch size "
                         << large_event_split_size;

  flusher_status.split_event++;

func_exit:
  if (cache_manager_locked) m_cache_manager->unlock();
  allocator.deallocate(data);

  return error;
}

bool Consensus_log_flusher::write_split(THD *thd, Gcn_log_event *gcn_event,
                                        Gtid_log_event *gtid_event,
                                        binlog_cache_data *cache_data,
                                        Binlog_event_writer *writer,
                                        ulonglong max_batch_size) {
  Binlog_read_error m_read_error;
  bool is_last_batch = false;
  bool is_last_event = false;
  ulonglong events_count = 0;
  ulonglong start_trx_index = m_log_mgr->get_current_index() + 1;

  // Create input stream from binlog_cache_data
  IO_CACHE *binlog_cache = cache_data->get_cache()->get_io_cache();
  reinit_io_cache(binlog_cache, READ_CACHE, 0, 0, 0);
  Peekable_binlog_event_data_istream istream(&m_read_error, binlog_cache,
                                             UINT_MAX);

  Split_batch_buffer batch_buffer(max_batch_size);

  if ((gcn_event && gcn_event->write(&batch_buffer)) ||
      gtid_event->write(&batch_buffer)) {
    return true;
  }

  events_count = gcn_event ? 2 : 1;
  while (!is_last_batch && !is_last_event) {
    kut_a(!m_read_error.has_error());

    if (read_batch(&istream, &batch_buffer, &is_last_batch, &events_count) ||
        write_batch(thd, batch_buffer.data(), batch_buffer.size(),
                    is_last_batch, events_count, writer) ||
        (events_count == 0 &&
         split_event_if_beyond_batch(thd, &istream, writer, &is_last_event,
                                     max_batch_size))) {
      return true;
    }

    batch_buffer.clear();
    events_count = 0;
  }

  xp::info(ER_XP_COMMIT) << "large trx split finish: total_size "
                         << cache_data->get_byte_position()
                         << ", start consensus_index " << start_trx_index
                         << ", end consensus_index " << thd->consensus_index
                         << ", split batch size " << max_batch_size;

  flusher_status.split_trx++;

  return false;
}

bool Consensus_log_flusher::write_no_split(THD *thd, Gcn_log_event *gcn_event,
                                           Gtid_log_event *gtid_event,
                                           binlog_cache_data *cache_data,
                                           Binlog_event_writer *writer) {
  bool err = false;
  Buffer *fifo_buffer = nullptr;

  thd->consensus_index = m_log_mgr->fetch_and_inc_current_index();

  Consensus_log_event cons_event(
      0, thd->consensus_term, thd->consensus_index,
      calc_payload_length(gcn_event, gtid_event,
                          writer->is_checksum_enabled()));
  if ((err = cons_event.write(writer))) {
    return err;
  }
  m_paxos->getLocalServer()->setLastNonCommitDepIndex(thd->consensus_index);

  {
    fifo_buffer = m_cache_manager->open_buffer(
        thd->consensus_term, thd->consensus_index, cons_event.get_length(),
        false, cons_event.get_flag());
    raii::Sentry<> guard([this, &thd, &err, &writer] {
      m_cache_manager->close_buffer(thd->consensus_index, err);
      writer->deregister_fifo();
    });

    writer->register_fifo(fifo_buffer);
    err = (gcn_event && gcn_event->write(writer)) ||
          gtid_event->write(writer) ||
          mysql_bin_log.write_cache(thd, cache_data, writer);
  }

  return err;
}

}  // namespace komodo
