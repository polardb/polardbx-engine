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

#include "consensus_log_manager.h"
#include "raft/raft0thd.h"
#include "sql/binlog.h"
#include "sql/consensus_log_manager.h"
#include "sql/bl_consensus_log.h"
#include "consensus_recovery_manager.h"

#include "my_config.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>

#include "my_dbug.h"
#include "mysql/components/services/log_builtins.h"
#include "sql/sql_lex.h"
#include "sql/debug_sync.h"  // DEBUG_SYNC
#include "sql/log.h"
#include <string>

#include "replica_read_manager.h"
#include "mysql/psi/mysql_file.h"

static void correct_binlog_event_log_pos(char *buf, size_t buf_len, ulonglong offset)
{
  if (!opt_consensuslog_revise)
    return;
  // calc correct end_log_pos
  ulonglong end_log_pos= my_b_safe_tell(mysql_bin_log.get_binlog_file()->get_io_cache());
  end_log_pos+= offset + buf_len;
  // inc the length of consensus_log_event
  end_log_pos+= Consensus_log_event::MAX_EVENT_LENGTH;
  if (binlog_checksum_options != binary_log::BINLOG_CHECKSUM_ALG_OFF)
    end_log_pos+= 4; // checksum length

  // correct end_log_pos in ev->temp_buf
  int4store(buf + LOG_POS_OFFSET, end_log_pos);

  // update checksum field because we modify the event's content
  if (binlog_checksum_options != binary_log::BINLOG_CHECKSUM_ALG_OFF)
  {
    uint32_t crc= checksum_crc32(0L, NULL, 0);
    crc= checksum_crc32(crc, (const unsigned char *)buf, buf_len - BINLOG_CHECKSUM_LEN);
    int4store(buf + buf_len - BINLOG_CHECKSUM_LEN, crc);
  }
}

int large_event_flush(THD *thd, uchar *buffer, ulonglong total_size, Log_event *ev, ulonglong &total_batch_size)
{
  int error = 0;
  uint flag = 0;
  uint64 start_pos = 0 , end_pos = opt_consensus_large_event_split_size > ev->buf_len? ev->buf_len: opt_consensus_large_event_split_size;
  std::string log_content = consensus_log_manager.get_empty_log();
  while(start_pos < ev->buf_len)
  {
    uint64 blen = end_pos - start_pos;
    memcpy(buffer, ev->temp_buf + start_pos, blen);
    total_batch_size += blen;
    if (total_batch_size == total_size)
      flag = Consensus_log_event_flag::FLAG_LARGE_TRX_END;
    else
      flag = Consensus_log_event_flag::FLAG_LARGE_TRX;
    if (end_pos == ev->buf_len)
      flag |= Consensus_log_event_flag::FLAG_BLOB_END;
    else if (start_pos == 0)
    {
      consensus_log_manager.get_fifo_cache_manager()->set_lock_blob_index(consensus_log_manager.get_current_index());
      flag |= (Consensus_log_event_flag::FLAG_BLOB | Consensus_log_event_flag::FLAG_BLOB_START);
    }
    else
      flag |= Consensus_log_event_flag::FLAG_BLOB;
    thd->consensus_index = consensus_log_manager.get_current_index();
    raft::info(ER_RAFT_COMMIT) << "Large event batch_size " << blen
        << ", total_batch_size " << total_batch_size
        << ", original buf_size " << total_size
        << ", consensus_index " << thd->consensus_index;
    uint64 crc32 = opt_consensus_checksum? checksum_crc32(0, buffer, blen): 0;
    if (end_pos == ev->buf_len)
    {
      correct_binlog_event_log_pos(ev->temp_buf, ev->buf_len, 0);
      error = mysql_bin_log.write_consensus_log(flag, thd->consensus_term, ev->buf_len, crc32);
    } else {
      correct_binlog_event_log_pos((char *)log_content.data(), log_content.length(), 0);
      error = mysql_bin_log.write_consensus_log(flag, thd->consensus_term, log_content.length(), crc32);
    }
    if (!error)
    {
      consensus_log_manager.get_fifo_cache_manager()->add_log_to_cache(thd->consensus_term, thd->consensus_index, blen, buffer, FALSE, flag, crc32);
      if (end_pos == ev->buf_len)
      {
        error = mysql_bin_log.write_buf_to_log_file((uchar *)ev->temp_buf, ev->buf_len);
        consensus_log_manager.get_fifo_cache_manager()->set_lock_blob_index(0);
      }
      else
        error = mysql_bin_log.write_buf_to_log_file((uchar *)log_content.data(), log_content.length());
      if (!error)
      {
        error = mysql_bin_log.flush_and_sync(false);
        consensus_log_manager.set_sync_index_if_greater(thd->consensus_index);
        alisql_server->writeLogDoneInternal(thd->consensus_index, true);
      }
    }
    DBUG_EXECUTE_IF("crash_during_large_event_binlog_flush", {
        static int ccnt = 0; ccnt++; if (ccnt > 1) { DBUG_SUICIDE(); }
    });
    DBUG_EXECUTE_IF("crash_during_large_event_binlog_flush_slow", { /* let follower get the log */
        static int ccnt = 0; ccnt++; if (ccnt > 1) { sleep(2); DBUG_SUICIDE(); }
    });
    start_pos = end_pos;
    end_pos = end_pos + opt_consensus_large_event_split_size > ev->buf_len? ev->buf_len: end_pos + opt_consensus_large_event_split_size;
  }
  return error;
}

int large_trx_flush(THD *thd, uchar *buffer, ulonglong total_size)
{
  int error = 0;
  Log_event *ev = NULL;
  uint flag = 0;
  bool force_batch = false;
  uint64 crc32 = 0;
  Format_description_log_event fd_ev;
  IO_CACHE *consensus_log = consensus_log_manager.get_cache();
  ulonglong batch_size = 0 , total_batch_size = 0;
  // init consensus_log for read
  reinit_io_cache(consensus_log, READ_CACHE, 0, 0, 0);
  Binlog_read_error m_error;
  Write_cache_istream consensus_log_istream(consensus_log);
  Binlog_event_data_istream consensus_log_data_istream(&m_error, &consensus_log_istream, UINT_MAX);
  Binlog_event_object_istream<Binlog_event_data_istream> consensus_log_event_istream(&m_error, &consensus_log_data_istream);
  Default_binlog_event_allocator default_alloc;
  /*
   * the max size of buffer is opt_consensus_max_log_size
   * do not use more than this restriction
   */
  while(!error && (ev = consensus_log_event_istream.read_event_object(fd_ev, opt_source_verify_checksum, &default_alloc)) != NULL)
  {
    if (ev->buf_len > opt_consensus_max_log_size || DBUG_EVALUATE_IF("force_large_trx_single_ev", 1, 0))
      force_batch = true; /* current ev is large event */
    else
      force_batch = false;
    /*
     * When should we batch a consensus log?
     * 1. current buffer must not empty
     * 2. current buffer size is larger than opt_consensus_large_trx_split_size
     * 3. current buffer size is ok, but if we add next event, the buffer will overflow
     * 4. force do a batch: large event OR for debug
     */
    if (batch_size > 0 && (batch_size > opt_consensus_large_trx_split_size
        || batch_size + ev-> buf_len > opt_consensus_max_log_size || force_batch))
    {
      total_batch_size += batch_size;
      flag = Consensus_log_event_flag::FLAG_LARGE_TRX;
      assert(total_batch_size < total_size);
      thd->consensus_index = consensus_log_manager.get_current_index();
      raft::info(ER_RAFT_COMMIT) << "Large event batch_size " << batch_size
          << ", total_batch_size " << total_batch_size
          << ", original buf_size " << total_size
          << ", consensus_index " << thd->consensus_index;
      crc32 = opt_consensus_checksum? checksum_crc32(0, buffer, batch_size ): 0;
      error = mysql_bin_log.write_consensus_log(flag, thd->consensus_term, batch_size, crc32); /* inc current index inside */
      if (!error)
      {
        consensus_log_manager.get_fifo_cache_manager()->add_log_to_cache(thd->consensus_term, thd->consensus_index, batch_size, buffer, FALSE, flag, crc32);
        DBUG_EXECUTE_IF("crash_during_large_trx_binlog_flush2", {
            static int ccnt = 0; ccnt++;
            if (ccnt > 1)
            {
              // force miss 1 byte and then crash
              mysql_bin_log.write_buf_to_log_file(buffer, batch_size - 1);
              mysql_bin_log.flush_and_sync(true);
              DBUG_SUICIDE();
            }
        });
        error = mysql_bin_log.write_buf_to_log_file(buffer, batch_size);
        /* for large trx, sync directly after flush for performance */
        if (!error)
        {
          error = mysql_bin_log.flush_and_sync(false);
          consensus_log_manager.set_sync_index_if_greater(thd->consensus_index);
          /* use lockless writeLogDone */
          alisql_server->writeLogDoneInternal(thd->consensus_index, true);
        }
      }
      batch_size = 0;
      DEBUG_SYNC(thd, "large_trx_sync_part");
      DBUG_EXECUTE_IF("crash_during_large_trx_binlog_flush", {
          static int ccnt = 0; ccnt++; if (ccnt > 1) { DBUG_SUICIDE(); }
      });
      DBUG_EXECUTE_IF("crash_during_large_trx_binlog_flush_slow", { /* let follower get the log */
          static int ccnt = 0; ccnt++; if (ccnt > 1) { sleep(2); DBUG_SUICIDE(); }
      });
    }
    //TODO @yanhua, need use force_batch here
    if (ev->buf_len > opt_consensus_max_log_size)
    {
      /* current ev is large event */
      assert(batch_size == 0);
      error = large_event_flush(thd, buffer, total_size, ev, total_batch_size);
      delete ev;
    }
    else
    {
      correct_binlog_event_log_pos(ev->temp_buf, ev->buf_len, batch_size);
      memcpy(buffer + batch_size, ev->temp_buf, ev->buf_len);
      batch_size += ev->buf_len;
      delete ev;
    }
  }
  /* deal with remained buffer */
  if (batch_size > 0)
  {
    total_batch_size += batch_size;
    flag = Consensus_log_event_flag::FLAG_LARGE_TRX_END;
    thd->consensus_index = consensus_log_manager.get_current_index();
    raft::info(ER_RAFT_COMMIT) << "Large event batch_size " << batch_size
        << ", total_batch_size " << total_batch_size
        << ", original buf_size " << total_size
        << ", consensus_index " << thd->consensus_index;
    assert(total_batch_size == total_size && batch_size > 0);
    crc32 = opt_consensus_checksum? checksum_crc32(0, buffer, batch_size ): 0;
    error = mysql_bin_log.write_consensus_log(flag, thd->consensus_term, batch_size, crc32); /* inc current index inside */
    if (!error)
    {
      consensus_log_manager.get_fifo_cache_manager()->add_log_to_cache(thd->consensus_term, thd->consensus_index, batch_size, buffer, FALSE, flag, crc32);
      error = mysql_bin_log.write_buf_to_log_file(buffer, batch_size);
    }
  }
  /* set consensus cache_log back to write cache */
  reinit_io_cache(consensus_log, WRITE_CACHE, 0, 0, 1);
  return error;
}

bool MYSQL_BIN_LOG::open_for_normandy(
#ifdef HAVE_PSI_INTERFACE
  PSI_file_key log_file_key,
#endif
  const char *log_name,
  const char *new_name __attribute__((unused)))
{
  DBUG_ENTER("MYSQL_BIN_LOG::open");
  bool ret = false;
  my_off_t file_off = 0;

  write_error = 0;
  myf flags = MY_WME | MY_NABP | MY_WAIT_IF_FULL;
  // xpaxos threads have no THD and can't report the WAITING_FULL state
  // TODO: add THD to xpaxos threads
  if (is_relay_log && !is_raft_log) flags = flags | MY_REPORT_WAITING_IF_FULL;

  if (!(name = my_strdup(key_memory_MYSQL_LOG_name, log_name, MYF(MY_WME)))) {
    name = const_cast<char*>(log_name);  // for the error message
    goto err;
  }

  // if (init_and_set_log_file_name(name, new_name, new_index_number) ||
      // DBUG_EVALUATE_IF("fault_injection_init_name", 1, 0))
    // goto err;

  db[0] = 0;

  /* Keep the key for reopen */
  m_log_file_key = log_file_key;

  /*
    LOCK_sync guarantees that no thread is calling m_binlog_file to sync data
    to disk when another thread is opening the new file
    (FLUSH LOG or RESET MASTER).
  */
  if (!is_relay_log) mysql_mutex_lock(&LOCK_sync);
  // avoid open for new binlog assert fail
  m_binlog_file->close();
  ret = m_binlog_file->open(log_file_key, log_file_name, flags);
  if (ret) {
    LogErr(ERROR_LEVEL, ER_BINLOG_CANT_OPEN_FOR_LOGGING, name, errno);
    goto err;
  }
  // write from the end
  file_off = my_seek(m_binlog_file->get_io_cache()->file, 0L, MY_SEEK_END, MYF(MY_WME + MY_FAE));
  m_binlog_file->truncate(file_off);

  if (!is_relay_log) mysql_mutex_unlock(&LOCK_sync);

  atomic_log_state = LOG_OPENED;
  DBUG_RETURN(0);

err:
  if (binlog_error_action == ABORT_SERVER) {
    exec_binlog_error_action_abort(
        "Either disk is full, file system is read only or "
        "there was an encryption error while opening the binlog. "
        "Aborting the server.");
  } else
    LogErr(ERROR_LEVEL, ER_BINLOG_CANT_OPEN_FOR_LOGGING, name, errno);

  my_free(name);
  name = NULL;
  atomic_log_state = LOG_CLOSED;
  DBUG_RETURN(1);
}

int MYSQL_BIN_LOG::build_consensus_log_index() {
  LOG_INFO log_info;
  int error = 1;
  std::vector<std::string>  consensuslog_file_name_vector;
  mysql_mutex_lock(&LOCK_index);
  // find last log name according to the binlog index
  if (!my_b_inited(&index_file)) {
    mysql_mutex_unlock(&LOCK_index);
    raft::error(ER_RAFT_COMMIT) << "build consenus log index failed, can't init index file";
    return 1;  
  }
  if ((error = find_log_pos(&log_info, NullS, false))) {
    if (error != LOG_INFO_EOF ) {
      raft::error(ER_RAFT_COMMIT) << "find_log_pos() failed error: " << error;
      mysql_mutex_unlock(&LOCK_index);
      return error;
    }
  }
  if (error == 0) {
    do {
      consensuslog_file_name_vector.push_back(log_info.log_file_name);
    } while (!(error = find_next_log(&log_info, false/*need_lock_index=true*/)));
  }
  if (error != LOG_INFO_EOF) {
      raft::error(ER_RAFT_COMMIT) << "find_log_pos() failed error: " << error;
    mysql_mutex_unlock(&LOCK_index);
    return error;
  } else {
    error = 0;
  }
  mysql_mutex_unlock(&LOCK_index);
  if (error)
    return error;

  for (auto iter = consensuslog_file_name_vector.begin(); iter != consensuslog_file_name_vector.end(); ++iter) {
    // const char *errmsg = NULL;
    //IO_CACHE log;
    //File file = open_binlog_file(&log, iter->c_str(), &errmsg);
    //if (file < 0)
      //return 1;
    Binlog_file_reader binlog_file_reader(opt_source_verify_checksum);
    if (binlog_file_reader.open(iter->c_str())) 
      return 1;

    Format_description_log_event fd_ev;
    Format_description_log_event *fd_ev_p = &fd_ev;

    // my_b_seek(&log, BIN_LOG_HEADER_SIZE);
    binlog_file_reader.seek(BIN_LOG_HEADER_SIZE);
    binlog_file_reader.set_format_description_event(*fd_ev_p);
    Log_event *ev = NULL;
    Previous_consensus_index_log_event *prev_consensus_index_ev = NULL;
    bool find_prev_consensus_log = FALSE;

    // while (!find_prev_consensus_log && ((ev = Log_event::read_log_event(&log, 0, fd_ev_p, 1)) != NULL))
    while (!find_prev_consensus_log && (ev = binlog_file_reader.read_event_object()) != NULL) {
      switch (ev->get_type_code()) {
      case binary_log::FORMAT_DESCRIPTION_EVENT:
        if (fd_ev_p != &fd_ev)
          delete fd_ev_p;
        fd_ev_p = (Format_description_log_event*)ev;
        binlog_file_reader.set_format_description_event(*fd_ev_p);
        break;
      case binary_log::PREVIOUS_CONSENSUS_INDEX_LOG_EVENT:
        prev_consensus_index_ev = (Previous_consensus_index_log_event*)ev;
        consensus_log_manager.get_log_file_index()->add_to_index_list(prev_consensus_index_ev->get_index(), prev_consensus_index_ev->common_header->when.tv_sec, *iter);
        find_prev_consensus_log = TRUE;
        break;
      default:
        break;
      }
      if (ev != NULL && ev != fd_ev_p)
        delete ev, ev = NULL;
    }

    if (fd_ev_p != &fd_ev) {
      delete fd_ev_p;
      fd_ev_p = &fd_ev;
    }
    // mysql_file_close(file, MYF(MY_WME));
    // end_io_cache(&log);

    if (!find_prev_consensus_log) {
      raft::error(ER_RAFT_COMMIT) << "log file " << iter->c_str() << " do not contain prev_consensus_log_ev";
      return 1;
    }
  }

  return 0;
}

int MYSQL_BIN_LOG::init_last_index_of_term(uint64 term)
{
  LOG_INFO log_info;
  bool found = FALSE;
  std::vector<std::string>  consensuslog_file_name_vector;
  get_consensus_log_file_list(consensuslog_file_name_vector);

  for (auto iter = consensuslog_file_name_vector.rbegin(); !found && iter != consensuslog_file_name_vector.rend(); ++iter)
  {
    uint64 current_term = 0;
    uint64 current_index = 0;
    uint64 current_flag = 0;
    // const char *errmsg = NULL;
    // IO_CACHE log;
    // File file = open_binlog_file(&log, iter->c_str(), &errmsg);
    // if (file < 0)
      //return 1;

    Binlog_file_reader binlog_file_reader(opt_source_verify_checksum);
    if (binlog_file_reader.open(iter->c_str()))
      return 1;

    Format_description_log_event fd_ev;
    Format_description_log_event *fd_ev_p = &fd_ev;

    // my_b_seek(&log, BIN_LOG_HEADER_SIZE);
    binlog_file_reader.seek(BIN_LOG_HEADER_SIZE);
    binlog_file_reader.set_format_description_event(*fd_ev_p);
    Log_event *ev = NULL;
    Consensus_log_event *consensus_log_ev = NULL;
    bool skip = FALSE;
   
    // while (!skip && ((ev = Log_event::read_log_event(&log, 0, fd_ev_p, 1)) != NULL))
    while (!skip && (ev = binlog_file_reader.read_event_object()) != NULL)
    {
      switch (ev->get_type_code())
      {
      case binary_log::FORMAT_DESCRIPTION_EVENT:
        if (fd_ev_p != &fd_ev)
          delete fd_ev_p;
        fd_ev_p = (Format_description_log_event*)ev;
        binlog_file_reader.set_format_description_event(*fd_ev_p);
        break;
      case binary_log::CONSENSUS_LOG_EVENT:
        consensus_log_ev = (Consensus_log_event*)ev;
        current_term = consensus_log_ev->get_term();
        current_index = consensus_log_ev->get_index();
        current_flag = consensus_log_ev->get_flag();
        // if find larger term, skip current file to previous one
        if (current_term > term)
        {
          skip = TRUE;
        }
        if (current_term  <= term && !(current_flag & Consensus_log_event_flag::FLAG_LARGE_TRX))
        {
          consensus_log_manager.get_recovery_manager()->set_last_leader_term_index(current_index);
          found = TRUE;
        }
        break;
      default:
        break;
      }
      if (ev != NULL && ev != fd_ev_p)
        delete ev, ev = NULL;
    }

    if (fd_ev_p != &fd_ev)
    {
      delete fd_ev_p;
      fd_ev_p = &fd_ev;
    }
    // mysql_file_close(file, MYF(MY_WME));
    // end_io_cache(&log);

    if (!found)
    {
      raft::warn(ER_RAFT_COMMIT) << "log file " << iter->c_str()
        << " cannot found last log term index, term is " << term;
    }
  }
  raft::info(ER_RAFT_COMMIT) << "last log term is " << term << ", last log term index is " << consensus_log_manager.get_recovery_manager()->get_last_leader_term_index();
  return !found;
}



int MYSQL_BIN_LOG::get_consensus_log_file_list(std::vector<std::string> & consensuslog_file_name_vector)
{
  consensus_log_manager.get_log_file_index()->get_log_file_list(consensuslog_file_name_vector);
  return 0;
}

int MYSQL_BIN_LOG::find_log_by_consensus_index(uint64 consensus_index, std::string & file_name)
{
  return consensus_log_manager.get_log_file_index()->get_log_file_from_index(consensus_index, file_name);
}

uint64 MYSQL_BIN_LOG::get_trx_end_index(uint64 firstIndex)
{
  std::string file_name;
  // use another io_cache , so do not need lock LOCK_log
  if (find_log_by_consensus_index(firstIndex, file_name))
  {
    raft::error(ER_RAFT_COMMIT) << "get_trx_end_index cannot find consensus index log " << firstIndex;
    return 0;
  }

  // const char *errmsg = NULL;
  // IO_CACHE log;

  // File file = open_binlog_file(&log, file_name.c_str(), &errmsg);
  // if (file < 0) {
    //return 0;
  //}
  Binlog_file_reader binlog_file_reader(opt_source_verify_checksum);
  if (binlog_file_reader.open(file_name.c_str()))
    return 0; // ??????

  Format_description_log_event fd_ev;
  Format_description_log_event *fd_ev_p = &fd_ev;

  // my_b_seek(&log, BIN_LOG_HEADER_SIZE);
  binlog_file_reader.seek(BIN_LOG_HEADER_SIZE);
  binlog_file_reader.set_format_description_event(*fd_ev_p);
  Log_event *ev = NULL;
  Consensus_log_event *consensus_log_ev = NULL;
  bool stop_scan = FALSE;
  uint64 currentIndex = 0;
  uint64 currentFlag = 0;
  // while (!stop_scan && (ev = Log_event::read_log_event(&log, 0, fd_ev_p, 1)) != NULL)
  while (!stop_scan && (ev = binlog_file_reader.read_event_object()) != NULL)
  {
    switch (ev->get_type_code())
    {
    case binary_log::CONSENSUS_LOG_EVENT:
      consensus_log_ev = (Consensus_log_event*)ev;
      currentIndex = consensus_log_ev->get_index();
      currentFlag = consensus_log_ev->get_flag();
      if (firstIndex <= currentIndex && !(currentFlag & Consensus_log_event_flag::FLAG_LARGE_TRX))
        stop_scan = true;
      break;
    case binary_log::FORMAT_DESCRIPTION_EVENT:
      if (fd_ev_p != &fd_ev)
        delete fd_ev_p;
      fd_ev_p = (Format_description_log_event*)ev;
      binlog_file_reader.set_format_description_event(*fd_ev_p);
      break;
    default:
      break;
    }
    if (ev != NULL && ev != fd_ev_p)
      delete ev;
  }

  if (fd_ev_p != &fd_ev)
  {
    delete fd_ev_p;
    fd_ev_p = &fd_ev;
  }

  // mysql_file_close(file, MYF(MY_WME));
  // end_io_cache(&log);

  return stop_scan? currentIndex: 0;
}

// int MYSQL_BIN_LOG::fetch_binlog_by_offset(IO_CACHE *log, uint64 start_pos, uint64 end_pos, Consensus_cluster_info_log_event *rci_ev, std::string& log_content)
int MYSQL_BIN_LOG::fetch_binlog_by_offset(Binlog_file_reader &binlog_file_reader, uint64 start_pos, uint64 end_pos, Consensus_cluster_info_log_event *rci_ev, std::string& log_content)
{
  if (start_pos == end_pos)
  {
    log_content.assign("");
    return 0;
  }
  if (rci_ev == NULL)
  {
    unsigned int buf_size = end_pos - start_pos;
    uchar* buffer = (uchar*)my_malloc(key_memory_thd_main_mem_root, buf_size, MYF(MY_WME));
    // my_b_seek(log, start_pos);
    binlog_file_reader.seek(start_pos);
    // my_b_read(log, buffer, buf_size);
    // binlog_file_reader.read_event_data(&buffer, &(buf_size));
    my_b_read(binlog_file_reader.get_io_cache(), buffer, buf_size);
    log_content.assign((char *)buffer, buf_size);
    my_free(buffer);
  }
  else
  {
    log_content.assign(rci_ev->get_info(), (size_t)rci_ev->get_info_length());
  }
  return 0;
}

int MYSQL_BIN_LOG::read_log_by_consensus_index(const char* file_name, uint64 consensus_index, uint64 *consensus_term, std::string& log_content, bool *outer, uint *flag, uint64 *checksum, bool need_content)
{
  // const char *errmsg = NULL;
  // IO_CACHE log;

  // File file = open_binlog_file(&log, file_name, &errmsg);
  // if (file < 0) {
    //return 1;
  //}
  Binlog_file_reader binlog_file_reader(opt_source_verify_checksum);
  if (binlog_file_reader.open(file_name))
    return 1;
  Format_description_log_event fd_ev;
  Format_description_log_event *fd_ev_p = &fd_ev;

  // my_b_seek(&log, BIN_LOG_HEADER_SIZE);
  binlog_file_reader.seek(BIN_LOG_HEADER_SIZE);
  binlog_file_reader.set_format_description_event(*fd_ev_p);
  Log_event *ev = NULL;
  Consensus_cluster_info_log_event *rci_ev = NULL;
  Consensus_log_event *consensus_log_ev = NULL;
  bool found = FALSE;
  bool stop_scan = FALSE;
  bool in_transaction = FALSE;
  uint64 start_pos = my_b_tell(binlog_file_reader.get_io_cache());
  uint64 end_pos = start_pos;
  uint64 consensus_log_length = 0;
  uint64 cindex, cterm, cflag, ccrc32;
  std::vector<uint64> blob_index_list;
  std::vector<uint64> blob_term_list;
  std::vector<uint64> blob_flag_list;
  std::vector<uint64> blob_crc32_list;
  // while (!stop_scan && (ev = Log_event::read_log_event(&log, 0, fd_ev_p, 1)) != NULL) 
  while (!stop_scan && (ev = binlog_file_reader.read_event_object()) != NULL)
  {
    switch (ev->get_type_code())
    {
    case binary_log::CONSENSUS_LOG_EVENT:
      consensus_log_ev = (Consensus_log_event*)ev;
      cindex = consensus_log_ev->get_index();
      cterm = consensus_log_ev->get_term();
      cflag = consensus_log_ev->get_flag();
      ccrc32 = consensus_log_ev->get_reserve();
      consensus_log_length = consensus_log_ev->get_length();
      end_pos = start_pos = my_b_tell(binlog_file_reader.get_io_cache());
      if (consensus_index == cindex)
      {
        found = TRUE;
        *consensus_term = cterm;
        *flag = cflag;
        *checksum = ccrc32;
      }
      else if (!found && consensus_log_ev->get_index() > consensus_index)
      {
        raft::info(ER_RAFT_COMMIT) << "directly read log error, log size is error";
        abort();
      }
      break;
    case binary_log::FORMAT_DESCRIPTION_EVENT:
      if (fd_ev_p != &fd_ev)
        delete fd_ev_p;
      fd_ev_p = (Format_description_log_event*)ev;
      binlog_file_reader.set_format_description_event(*fd_ev_p);
      break;
    default:
      if (!ev->is_control_event())
      {
        end_pos = my_b_tell(binlog_file_reader.get_io_cache());
        if (ev->get_type_code() ==  binary_log::CONSENSUS_CLUSTER_INFO_EVENT && found)
        {
          rci_ev = (Consensus_cluster_info_log_event*)ev;
        }
        if (end_pos > start_pos && end_pos - start_pos == consensus_log_length)
        {
          if (need_content && (cflag & Consensus_log_event_flag::FLAG_BLOB))
          {
            blob_index_list.push_back(cindex);
            blob_term_list.push_back(cterm);
            blob_flag_list.push_back(cflag);
            blob_crc32_list.push_back(ccrc32);
          }
          else if (need_content && (cflag & Consensus_log_event_flag::FLAG_BLOB_END))
          {
            blob_index_list.push_back(cindex);
            blob_term_list.push_back(cterm);
            blob_flag_list.push_back(cflag);
            blob_crc32_list.push_back(ccrc32);
            if (found)
            {
              assert(consensus_index >= blob_index_list[0] && consensus_index <= cindex);
              /* It means the required index is between a blob event */
              uint64 split_len = opt_consensus_large_event_split_size;
              uint64 blob_start_pos = start_pos, blob_end_pos = start_pos + split_len;
              for (size_t i=0; i<blob_index_list.size(); ++i)
              {
                if (blob_index_list[i] == consensus_index)
                {
                  // fetch_binlog_by_offset(&log, blob_start_pos, blob_end_pos, NULL, log_content);
                  fetch_binlog_by_offset(binlog_file_reader, blob_start_pos, blob_end_pos, NULL, log_content);
                  *outer = false;
                  end_pos = start_pos = my_b_tell(binlog_file_reader.get_io_cache());
                  stop_scan = TRUE;
                  break;
                }
                blob_start_pos = blob_end_pos;
                blob_end_pos = blob_end_pos + split_len > end_pos? end_pos: blob_end_pos + split_len;
              }
            }
            blob_index_list.clear();
            blob_term_list.clear();
            blob_flag_list.clear();
            blob_crc32_list.clear();
          }
          else
          {
            if (found)
            {
              if (need_content || rci_ev != NULL)
                // fetch_binlog_by_offset(&log, start_pos, end_pos, rci_ev, log_content);
                fetch_binlog_by_offset(binlog_file_reader, start_pos, end_pos, rci_ev, log_content);
              *outer = (rci_ev != NULL);
              end_pos = start_pos = my_b_tell(binlog_file_reader.get_io_cache());
              stop_scan = TRUE;
              rci_ev = NULL;
            }
          }
        }
      }
      break;
    }
    if (ev != NULL && ev != fd_ev_p)
      delete ev;
  }

  // if scan to end of file
  if (end_pos > start_pos && !in_transaction)
  {
    if (need_content || rci_ev != NULL)
      // fetch_binlog_by_offset(&log, start_pos, end_pos, rci_ev, log_content);
      fetch_binlog_by_offset(binlog_file_reader, start_pos, end_pos, rci_ev, log_content);
    raft::info(ER_RAFT_COMMIT) << "directly read last log size " << end_pos - start_pos;
    end_pos = start_pos = my_b_tell(binlog_file_reader.get_io_cache());
  }

  if (fd_ev_p != &fd_ev)
  {
    delete fd_ev_p;
    fd_ev_p = &fd_ev;
  }

  raft::info(ER_RAFT_COMMIT) << "directly read log reached consensus index " << consensus_index;

  if (!found)
    raft::error(ER_RAFT_COMMIT) << "read log by consensus index failed";

  // mysql_file_close(file, MYF(MY_WME));
  // end_io_cache(&log);

  return (int)!found;
}

int MYSQL_BIN_LOG::prefetch_logs_of_file(THD *thd, uint64 channel_id, const char* file_name, uint64 start_index)
{
  // const char *errmsg = NULL;
  // IO_CACHE log;
  LOG_INFO linfo;

  strncpy(linfo.log_file_name, file_name, FN_REFLEN - 1);
  mysql_mutex_lock(&thd->LOCK_thd_data);
  thd->current_linfo = &linfo;
  mysql_mutex_unlock(&thd->LOCK_thd_data);

  // File file = open_binlog_file(&log, file_name, &errmsg);
  // if (file < 0) {
    // thd->current_linfo = 0;
    // return 1;
  // }
  Binlog_file_reader binlog_file_reader(opt_source_verify_checksum);
  if (binlog_file_reader.open(file_name)) {
    mysql_mutex_lock(&thd->LOCK_thd_data);
    thd->current_linfo = 0;
    mysql_mutex_unlock(&thd->LOCK_thd_data);
    return 1;
  }
  Format_description_log_event fd_ev;
  Format_description_log_event *fd_ev_p = &fd_ev;

  // my_b_seek(&log, BIN_LOG_HEADER_SIZE);
  binlog_file_reader.seek(BIN_LOG_HEADER_SIZE);
  binlog_file_reader.set_format_description_event(*fd_ev_p);
  Log_event *ev = NULL;
  Consensus_cluster_info_log_event *rci_ev = NULL;
  Consensus_log_event *consensus_log_ev = NULL;
  uint64 start_pos = my_b_tell(binlog_file_reader.get_io_cache());
  uint64 end_pos = start_pos;

  uint64 current_index = 0;
  uint64 current_term = 0;
  uint32 consensus_log_length = 0;
  uint current_flag = 0;
  uint64 current_crc32 = 0;
  bool stop_prefetch = FALSE;
  std::string log_content;
  std::vector<uint64> blob_index_list;
  std::vector<uint64> blob_term_list;
  std::vector<uint64> blob_flag_list;
  std::vector<uint64> blob_crc32_list;
  ConsensusPreFetchManager *prefetch_mgr = consensus_log_manager.get_prefetch_manager();
  ConsensusPreFetchChannel *prefetch_channel = prefetch_mgr->get_prefetch_channel(channel_id);
  prefetch_channel->set_prefetching(TRUE);
  if (prefetch_channel->get_channel_id() == 0)
    prefetch_channel->clear_large_trx_table();
  // while (!stop_prefetch && (ev = Log_event::read_log_event(&log, 0, fd_ev_p, 1)) != NULL)
  while (!stop_prefetch && (ev = binlog_file_reader.read_event_object()) != NULL)
  {
    switch (ev->get_type_code())
    {
    case binary_log::CONSENSUS_LOG_EVENT:
      consensus_log_ev = (Consensus_log_event*)ev;
      current_index = consensus_log_ev->get_index();
      current_term = consensus_log_ev->get_term();
      consensus_log_length = consensus_log_ev->get_length();
      current_flag = consensus_log_ev->get_flag();
      current_crc32 = consensus_log_ev->get_reserve();
      end_pos = start_pos = my_b_tell(binlog_file_reader.get_io_cache());
      if (opt_consensus_prefetch_fast_fetch)
      {
        /*
         * jump to next consensus_log_event:
         * 1. not large trx, not blob
         * 2. current_index + window_size < start_index
         */
        if ((!(current_flag & (Consensus_log_event_flag::FLAG_LARGE_TRX
                          | Consensus_log_event_flag::FLAG_LARGE_TRX_END
                          | Consensus_log_event_flag::FLAG_BLOB
                          | Consensus_log_event_flag::FLAG_BLOB_START
                          | Consensus_log_event_flag::FLAG_BLOB_END)))
            && (current_index + prefetch_channel->get_window_size() < start_index))
        {
          // my_b_seek(&log, start_pos + consensus_log_length);
          binlog_file_reader.seek(start_pos + consensus_log_length);
        }
        /*
         * fetch data directly:
         * 1. not large trx, not blob
         * 2. not configure change
         * 3. current_index + window_size >= start_index
         */
        if ((!(current_flag & (Consensus_log_event_flag::FLAG_LARGE_TRX
                          | Consensus_log_event_flag::FLAG_LARGE_TRX_END
                          | Consensus_log_event_flag::FLAG_BLOB
                          | Consensus_log_event_flag::FLAG_BLOB_START
                          | Consensus_log_event_flag::FLAG_BLOB_END
                          | Consensus_log_event_flag::FLAG_CONFIG_CHANGE)))
            && (current_index + prefetch_channel->get_window_size() >= start_index))
        {
          uchar* buffer = (uchar*)my_malloc(key_memory_thd_main_mem_root, consensus_log_length, MYF(MY_WME));
          // my_b_read(&log, buffer, consensus_log_length);
          // binlog_file_reader.read_event_data(&buffer, &consensus_log_length);
          my_b_read(binlog_file_reader.get_io_cache(), buffer, consensus_log_length);
          int result = 0;
          while ((result = prefetch_channel->add_log_to_prefetch_cache(current_term,
            current_index, consensus_log_length,
            buffer, false, current_flag, current_crc32)) == FULL)
          {
            // wait condition already executed in add log to prefetch cache
          }
          if (result == INTERRUPT || current_index == consensus_log_manager.get_sync_index())
            stop_prefetch = TRUE;
          my_free(buffer);
          end_pos = my_b_tell(binlog_file_reader.get_io_cache());
          assert(end_pos - start_pos == consensus_log_length);
        }
      }
      break;
    case binary_log::FORMAT_DESCRIPTION_EVENT:
      if (fd_ev_p != &fd_ev)
        delete fd_ev_p;
      fd_ev_p = (Format_description_log_event*)ev;
      binlog_file_reader.set_format_description_event(*fd_ev_p);
      break;
    default:
      if (!ev->is_control_event())
      {
        end_pos = my_b_tell(binlog_file_reader.get_io_cache());
        if (ev->get_type_code() == binary_log::CONSENSUS_CLUSTER_INFO_EVENT)
        {
          rci_ev = (Consensus_cluster_info_log_event*)ev;
        }
        if (end_pos > start_pos && end_pos - start_pos == consensus_log_length)
        {
          if (prefetch_channel->get_channel_id() == 0 && (current_flag
              & (Consensus_log_event_flag::FLAG_LARGE_TRX | Consensus_log_event_flag::FLAG_LARGE_TRX_END)))
          {
            prefetch_channel->add_log_to_large_trx_table(current_term, current_index, (rci_ev != NULL), current_flag);
          }
          if (current_flag & Consensus_log_event_flag::FLAG_BLOB)
          {
            blob_index_list.push_back(current_index);
            blob_term_list.push_back(current_term);
            blob_flag_list.push_back(current_flag);
            blob_crc32_list.push_back(current_crc32);
          }
          else if (current_flag & Consensus_log_event_flag::FLAG_BLOB_END)
          {
            blob_index_list.push_back(current_index);
            blob_term_list.push_back(current_term);
            blob_flag_list.push_back(current_flag);
            blob_crc32_list.push_back(current_crc32);
            uint64 split_len = opt_consensus_large_event_split_size;
            uint64 blob_start_pos = start_pos, blob_end_pos = start_pos + split_len;
            for (size_t i=0; i<blob_index_list.size(); ++i)
            {
              if (blob_index_list[i] + prefetch_channel->get_window_size() >= start_index)
              {
                // fetch_binlog_by_offset(&log, blob_start_pos, blob_end_pos, NULL, log_content);
                fetch_binlog_by_offset(binlog_file_reader, blob_start_pos, blob_end_pos, NULL, log_content);
                int result = 0;
                while ((result = prefetch_channel->add_log_to_prefetch_cache(blob_term_list[i],
                  blob_index_list[i], log_content.size(),
                  (uchar*)const_cast<char*>(log_content.c_str()), false, blob_flag_list[i], blob_crc32_list[i])) == FULL)
                {
                  // wait condition already executed in add log to prefetch cache
                }
                if (result == INTERRUPT)
                {
                  stop_prefetch = TRUE;
                  break; // break iterate blob_index_list
                }
              }
              blob_start_pos = blob_end_pos;
              blob_end_pos = blob_end_pos + split_len > end_pos? end_pos: blob_end_pos + split_len;
            }
            blob_index_list.clear();
            blob_term_list.clear();
            blob_flag_list.clear();
            blob_crc32_list.clear();
          }
          else
          {
            if (current_index + prefetch_channel->get_window_size() >= start_index)
            {
              // fetch_binlog_by_offset(&log, start_pos, end_pos, rci_ev, log_content);
              fetch_binlog_by_offset(binlog_file_reader, start_pos, end_pos, rci_ev, log_content); 
              int result = 0;
              while ((result = prefetch_channel->add_log_to_prefetch_cache(current_term,
                current_index, log_content.size(),
                (uchar*)const_cast<char*>(log_content.c_str()), (rci_ev != NULL), current_flag, current_crc32)) == FULL)
              {
                // wait condition already executed in add log to prefetch cache
              }
              if (result == INTERRUPT || current_index == consensus_log_manager.get_sync_index())
              {
                stop_prefetch = TRUE; // because truncate log happened, stop prefetch and retry
              }
            }
          }
          rci_ev = NULL;
        }
      }
      break;
    }
    if (ev != NULL && ev != fd_ev_p)
      delete ev, ev = NULL;
  }

  if (fd_ev_p != &fd_ev)
  {
    delete fd_ev_p;
    fd_ev_p = &fd_ev;
  }
  prefetch_channel->set_prefetching(FALSE);
  prefetch_channel->dec_ref_count();
  prefetch_channel->clear_prefetch_request();
  // mysql_file_close(file, MYF(MY_WME));
  // end_io_cache(&log);
  mysql_mutex_lock(&thd->LOCK_thd_data);
  thd->current_linfo = 0;
  mysql_mutex_unlock(&thd->LOCK_thd_data);

  raft::info(ER_RAFT_COMMIT) << "channel_id " << channel_id
    << " prefetch log reached consensus index " << (uint64)current_index;

  return 0;
}


/*
   There are 3 condition to determine the right position
   1. beginning of the index
   2. ending of the previous index
   3. beginning of the binlog file
*/
int MYSQL_BIN_LOG::find_pos_by_consensus_index(const char* file_name, uint64 consensus_index, uint64 *pos)
{
  // const char *errmsg = NULL;
  // IO_CACHE log;

  // File file = open_binlog_file(&log, file_name, &errmsg);
  // if (file < 0)
  //{
    //return 1;
  //}
  Binlog_file_reader binlog_file_reader(opt_source_verify_checksum);
  if (binlog_file_reader.open(file_name))
    return 1;
  Format_description_log_event fd_ev;
  Format_description_log_event *fd_ev_p = &fd_ev;

  // my_b_seek(&log, BIN_LOG_HEADER_SIZE);
  binlog_file_reader.seek(BIN_LOG_HEADER_SIZE);
  binlog_file_reader.set_format_description_event(*fd_ev_p);
  Log_event *ev = NULL;
  Consensus_log_event *consensus_log_ev = NULL;
  Previous_consensus_index_log_event *consensus_prev_ev = NULL;
  bool found = FALSE;
  bool first_log_in_file = FALSE;

  // while (!found && (ev = Log_event::read_log_event(&log, 0, fd_ev_p, 1)) != NULL)
  while (!found && (ev = binlog_file_reader.read_event_object()) != NULL)
  {
    switch (ev->get_type_code())
    {
    case binary_log::CONSENSUS_LOG_EVENT:
      consensus_log_ev = (Consensus_log_event*)ev;
      if (consensus_index == consensus_log_ev->get_index())
        found = TRUE;
      if (consensus_index == consensus_log_ev->get_index() + 1)
      {
        found = TRUE;
        *pos = my_b_tell(binlog_file_reader.get_io_cache()) + consensus_log_ev->get_length();
      }
      break;
    case binary_log::PREVIOUS_CONSENSUS_INDEX_LOG_EVENT:
      consensus_prev_ev = (Previous_consensus_index_log_event*)ev;
      if (consensus_index == consensus_prev_ev->get_index())
        first_log_in_file = TRUE;
      break;
    case binary_log::PREVIOUS_GTIDS_LOG_EVENT:
      if (first_log_in_file)
      {
        *pos = my_b_tell(binlog_file_reader.get_io_cache());
        found = TRUE;
      }
      break;
    case binary_log::FORMAT_DESCRIPTION_EVENT:
      if (fd_ev_p != &fd_ev)
        delete fd_ev_p;
      fd_ev_p = (Format_description_log_event*)ev;
      binlog_file_reader.set_format_description_event(*fd_ev_p);
      break;
    default:
      break;
    }
    if (ev != NULL && ev != fd_ev_p)
      delete ev, ev = NULL;
  }

  if (fd_ev_p != &fd_ev)
  {
    delete fd_ev_p;
    fd_ev_p = &fd_ev;
  }
  // mysql_file_close(file, MYF(MY_WME));
  // end_io_cache(&log);

  return !found;
}


int MYSQL_BIN_LOG::truncate_logs_from_index(std::vector<std::string> & files_list, std::string last_file)
{
  LOG_INFO log_info;
  mysql_mutex_lock(&LOCK_index);
  if (find_log_pos(&log_info, last_file.c_str(), false/*need_lock_index=false*/))
  {
    raft::error(ER_RAFT_COMMIT) << "MYSQL_BIN_LOG::truncate_logs was called with file " << last_file.c_str()
          << " not listed in the index.";
    goto err;
  }


  if (open_crash_safe_index_file())
  {
    raft::error(ER_RAFT_COMMIT) << "MYSQL_BIN_LOG::remove_logs_from_index failed to "
      "open the crash safe index file.";
    goto err;
  }

  for (std::vector<std::string>::iterator it = files_list.begin(); it != files_list.end(); it++)
  {
    std::string record = (*it) + "\n";
    if (mysql_file_write(crash_safe_index_file.file, (uchar*)const_cast<char*>(record.c_str()), record.length(), MYF(MY_WME | MY_NABP)))
      goto err;
  }

  if (close_crash_safe_index_file())
  {
    raft::error(ER_RAFT_COMMIT) << "MYSQL_BIN_LOG::remove_logs_from_index failed to "
      "close the crash safe index file.";
    goto err;
  }
  DBUG_EXECUTE_IF("fault_injection_copy_part_file", DBUG_SUICIDE(););

  if (move_crash_safe_index_file_to_index_file(false/*need_lock_index=false*/))
  {
    raft::error(ER_RAFT_COMMIT) << "MYSQL_BIN_LOG::remove_logs_from_index failed to "
      "move crash safe index file to index file.";
    goto err;
  }
// #ifdef HAVE_REPLICATION
  // now update offsets in index file for running threads
  adjust_linfo_offsets(log_info.index_file_start_offset);
// #endif
  mysql_mutex_unlock(&LOCK_index);
  return 0;

err:
  raft::error(ER_RAFT_COMMIT) << "truncate log from index failed";
  mysql_mutex_unlock(&LOCK_index);
  return 1;
}


int MYSQL_BIN_LOG::truncate_files_after(std::string & file_name)
{
  int error = 0;
  bool found = false;
  std::vector<std::string> consensus_log_file_name_vector;
  std::vector<std::string> delete_vector;
  std::vector<std::string> exist_vector;
  mysql_mutex_assert_owner(&LOCK_log);
  get_consensus_log_file_list(consensus_log_file_name_vector);

  if (file_name == *(consensus_log_file_name_vector.rbegin()))
    return 0;
  for (std::vector<std::string>::iterator iter = consensus_log_file_name_vector.begin();
    iter != consensus_log_file_name_vector.end(); iter++)
  {
    if (!found)
      exist_vector.push_back(*iter);
    else
      delete_vector.push_back(*iter);

    if (*iter == file_name)
        found = TRUE;
  }

  // truncate consensus log file index
  consensus_log_manager.get_log_file_index()->truncate_after(file_name);

  // modify index file
  truncate_logs_from_index(exist_vector, file_name);

  // delete file
  for (std::vector<std::string>::iterator iter = delete_vector.begin();
    iter != delete_vector.end(); iter++)
  {
    if (mysql_file_delete(key_file_binlog, (*iter).c_str(), MYF(0)))
    {
      assert(0);
      error = 1;
      break;
    }
  }

  if (error)
    raft::error(ER_RAFT_COMMIT) << "truncate_files_after failed";
  return error;
}


int MYSQL_BIN_LOG::truncate_single_file_by_consensus_index(const char* file_name, uint64 consensus_index)
{
  assert(consensus_index != 0);
  uint64 offset = 0;
  File file;
  mysql_mutex_assert_owner(&LOCK_log);
  if (find_pos_by_consensus_index(file_name, consensus_index, &offset))
  {
    raft::error(ER_RAFT_COMMIT) << "Failed to find pos by consensus index " << consensus_index << " when truncate ";
    return -1;
  }

  if ((file = mysql_file_open(key_file_binlog, file_name,
    O_RDWR | O_BINARY, MYF(MY_WME))) < 0)
  {
    raft::error(ER_RAFT_COMMIT) << "Failed to open the binlog file when truncate.";
    return -1;
  }
  if (my_chsize(file, offset, 0, MYF(MY_WME)))
  {
    raft::error(ER_RAFT_COMMIT) << "Failed to resize binlog file when truncate.";
    mysql_file_close(file, MYF(MY_WME));
    return -1;
  }
  else
  {
    raft::info(ER_RAFT_COMMIT) << "Truncate binlog file " << file_name
      << ", Binlog trimmed to " << offset << " bytes.";
    mysql_file_close(file, MYF(MY_WME));
  }
  return 0;
}

int MYSQL_BIN_LOG::consensus_truncate_log(uint64 consensus_index, Relay_log_info *rli)
{
  std::string file_name;
  mysql_mutex_lock(&LOCK_sync);

  // truncate must not cross binlog file.
  if (find_log_by_consensus_index(consensus_index, file_name) ||
    truncate_files_after(file_name) ||
    truncate_single_file_by_consensus_index(file_name.c_str(), consensus_index))
  {
    raft::error(ER_RAFT_COMMIT) << "Truncate cannot find consensus index log " << consensus_index;
    abort();
  }
  else
  {
    consensus_log_manager.set_sync_index(consensus_index - 1);
    consensus_log_manager.set_current_index(consensus_index);
    // move this to truncate_single_file_by_consensus_index
    uint64 offset = 0;
    if (find_pos_by_consensus_index(file_name.c_str(), consensus_index, &offset))
    {
      raft::error(ER_RAFT_COMMIT) << "Failed to find pos by consensus index " << consensus_index << " when truncate ";
      goto err;
    }
    if (m_binlog_file->truncate(offset)) {
      raft::error(ER_RAFT_COMMIT) << "Failed to truncate the binlog file " << offset;
      goto err;
    }

    atomic_binlog_end_pos = offset;

    if (rli) {
      rli->notify_relay_log_truncated();
    }
  }
  mysql_mutex_unlock(&LOCK_sync);
  return 0;
err:
  mysql_mutex_unlock(&LOCK_sync);
  return 1;
}

int MYSQL_BIN_LOG::consensus_get_log_position(uint64 consensus_index, char* log_name, uint64 *pos)
{
  std::string file_name;
  int ret = 0;
  // use another io_cache , so do not need lock LOCK_log
  if (find_log_by_consensus_index(consensus_index, file_name) ||
    find_pos_by_consensus_index(file_name.c_str(), consensus_index, pos))
  {
    raft::error(ER_RAFT_COMMIT) << "Get log position cannot find consensus index log " << consensus_index;
    ret = 1;
  }
  strncpy(log_name, file_name.c_str(), FN_REFLEN);
  return ret;
}

int MYSQL_BIN_LOG::consensus_get_log_entry(uint64 consensus_index, uint64 *consensus_term, std::string& log_content, bool *outer, uint *flag, uint64 *checksum, bool need_content)
{
  std::string file_name;
  int ret = 0;
  // use another io_cache , so do not need lock LOCK_log
  if (find_log_by_consensus_index(consensus_index, file_name) ||
    read_log_by_consensus_index(file_name.c_str(), consensus_index, consensus_term, log_content, outer, flag, checksum, need_content))
  {
    raft::error(ER_RAFT_COMMIT) << "Get log entry cannot find consensus index log " << consensus_index;
    ret = 1;
  }

  return ret;
}

int MYSQL_BIN_LOG::consensus_prefetch_log_entries(THD *thd, uint64 channel_id, uint64 consensus_index)
{
  std::string file_name;
  int ret = 0;
  // use another io_cache , so do not need lock LOCK_log
  if (find_log_by_consensus_index(consensus_index, file_name) ||
    prefetch_logs_of_file(thd, channel_id, file_name.c_str(), consensus_index))
  {
    raft::error(ER_RAFT_COMMIT) << "Prefetch cannot find consensus index log " << consensus_index;
    ret = 1;
  }

  return ret;
}

static void store_gtid_for_xpaxos(const char *buf, Relay_log_info *rli) {
  Log_event_type event_type = (Log_event_type)buf[EVENT_TYPE_OFFSET];
  Format_description_log_event fd_ev;
  fd_ev.footer()->checksum_alg =
      static_cast<enum_binlog_checksum_alg>(binlog_checksum_options);

  if (event_type == binary_log::GCN_LOG_EVENT) {
    buf = buf + Gcn_log_event::get_event_length(fd_ev.footer()->checksum_alg);
    event_type = (Log_event_type)buf[EVENT_TYPE_OFFSET];
  }

  if (event_type == binary_log::GTID_LOG_EVENT) {
    Gtid_log_event gtid_ev(buf, &fd_ev);
    rli->get_sid_lock()->wrlock();
    rli->add_logged_gtid(rli->get_sid_map()->add_sid(*gtid_ev.get_sid()), gtid_ev.get_gno());
    rli->get_sid_lock()->unlock();
  }
}

static void revise_one_event(uchar *event_ptr, size_t event_len, size_t log_pos)
{
  /* PolarDB-X Engine: fix timestamp for non-leader local event */
  if (consensus_log_manager.get_status() != Consensus_Log_System_Status::BINLOG_WORKING)
  {
    uint32 tt = uint4korr(event_ptr);
    if (!Log_event::is_local_event_type(static_cast<Log_event_type>(event_ptr[EVENT_TYPE_OFFSET])))
    {
      // cache the last timestamp
      consensus_log_manager.set_event_timestamp(tt);
    }
    else
    {
      // set control event timestamp to the lastest non-control one
      uint32 last_tt = consensus_log_manager.get_event_timestamp();
      if (last_tt)
        int4store(event_ptr, last_tt);
    }
  }

  /* PolarDB-X Engine: reset each binlog event's log_pos (end_log_pos) to the correct value */
  int4store(event_ptr + LOG_POS_OFFSET, log_pos);

  /* PolarDB-X Engine: recalculate the checksum if necessary */
  if (binlog_checksum_options != binary_log::BINLOG_CHECKSUM_ALG_OFF)
  {
    ha_checksum crc= checksum_crc32(0L, NULL, 0);
    crc= checksum_crc32(crc, event_ptr, event_len - BINLOG_CHECKSUM_LEN);
    int4store(event_ptr + event_len - BINLOG_CHECKSUM_LEN, crc);
  }
}

static int init_consensus_event_timestamp(uchar *buf, size_t len)
{
  if (opt_consensuslog_revise &&
      consensus_log_manager.get_status() != Consensus_Log_System_Status::BINLOG_WORKING &&
      consensus_log_manager.get_event_timestamp() == 0)
  {
    /* get event timestamp from current buffer */
    size_t event_len = 0;
    uchar *header = buf;
    while ((size_t)(header - buf) < len)
    {
      event_len = uint4korr(header + EVENT_LEN_OFFSET);
      if (!Log_event::is_local_event_type(static_cast<Log_event_type>(header[EVENT_TYPE_OFFSET])))
      {
        consensus_log_manager.set_event_timestamp(uint4korr(header));
        break;
      }
      header += event_len;
    }
    if ((size_t)(header - buf) > len)
    {
      raft::error(ER_RAFT_COMMIT) << "Found invalid event during init_consensus_event_timestamp.";
      return 1; // report error to abort
    }
  }
  return 0;
}

static int revise_entry_and_write(MYSQL_BIN_LOG::Binlog_ofile *binlog_file, uchar *buf, size_t len)
{
  /* revise end_pos & timestamp in consensus log */
  if (!opt_consensuslog_revise)
    return binlog_file->write(buf, len);
  size_t event_len = 0;
  uchar *header = buf;
  size_t offset = binlog_file->position();
  while ((size_t)(header - buf) < len)
  {
    event_len = uint4korr(header + EVENT_LEN_OFFSET);
    revise_one_event(header, event_len, offset + header - buf + event_len);
    header += event_len;
  }
  if ((size_t)(header - buf) != len)
  {
    raft::error(ER_RAFT_COMMIT) << "Found invalid event during revise_entry_and_write.";
    return 1; // report error to abort
  }
  return binlog_file->write(buf, len);
}

int MYSQL_BIN_LOG::append_consensus_log(ConsensusLogEntry &log,
                                       uint64* index, bool* rotate_var, Relay_log_info *rli, bool with_check)
{
  int error = 0;
  uint64 bytes = 0;
  uchar *real_buffer = NULL;
  size_t real_buf_size = 0;
  my_off_t end_pos = 0;
  mysql_mutex_lock(&LOCK_log);
  if (with_check)
  {
    mysql_mutex_lock(consensus_log_manager.get_term_lock());
    if (consensus_log->getCurrentTerm() != log.term)
    {
      mysql_mutex_unlock(consensus_log_manager.get_term_lock());
      mysql_mutex_unlock(&LOCK_log);
      /* set index to 0 to mark it fail */
      *index = 0;
      /* return 0 do not let it abort */
      return 0;
    }
    mysql_mutex_unlock(consensus_log_manager.get_term_lock());
  }
  // cluster info should consider real binlog format size
  if (log.outer)
  {
    Consensus_cluster_info_log_event ev(log.buf_size, (char*)(log.buffer));
    ev.common_footer->checksum_alg = static_cast<enum_binlog_checksum_alg>
      (binlog_checksum_options);
    error = ev.write(consensus_log_manager.get_log_file());
    real_buf_size = consensus_log_manager.serialize_cache(&real_buffer);
  }
  else if (log.flag & Consensus_log_event_flag::FLAG_BLOB)
  {
    if (log.flag & Consensus_log_event_flag::FLAG_BLOB_START)
    {
      /* some log may truncate after leader crash recovery, so if FLAG_BLOB_START, clear the cache */
      reinit_io_cache(consensus_log_manager.get_cache(), WRITE_CACHE, 0, 0, 1);
    }
    /* save empty log as a replace to binlog */
    std::string empty_log = consensus_log_manager.get_empty_log();
    real_buffer = (uchar*)my_malloc(key_memory_thd_main_mem_root, empty_log.length(), MYF(MY_WME));
    memcpy(real_buffer, empty_log.data(), empty_log.length());
    real_buf_size = empty_log.length();
    /* save real data to cache */
    raft::info(ER_RAFT_COMMIT) << "Large event: cache the current log, size " << log.buf_size;
    my_b_write(consensus_log_manager.get_cache(), log.buffer, log.buf_size);
  }
  else if (log.flag & Consensus_log_event_flag::FLAG_BLOB_END)
  {
    DBUG_EXECUTE_IF("crash_during_large_event_receive", {DBUG_SUICIDE();});
    DBUG_EXECUTE_IF("crash_during_large_event_receive_slow", {sleep(2); DBUG_SUICIDE();});
    /* save real data to cache, now cache has the integrated blob event */
    raft::info(ER_RAFT_COMMIT) << "Large event: cache the current log, size " << log.buf_size;
    my_b_write(consensus_log_manager.get_cache(), log.buffer, log.buf_size);
    real_buf_size = consensus_log_manager.serialize_cache(&real_buffer);
    raft::info(ER_RAFT_COMMIT) << "Large event: cache the whole log, size " << real_buf_size;
  }
  else
  {
    real_buffer = log.buffer;
    real_buf_size = log.buf_size;
  }

  *index = consensus_log_manager.get_current_index();
  if (*index != log.index && log.index != 0)  // leader write empty log entry with index 0
  {
    raft::error(ER_RAFT_COMMIT) << "Consensus Index Mismatch, system current index is " << *index
                           << ", but the log index is " << log.index;
    abort();
  }
  consensus_log_manager.get_fifo_cache_manager()->add_log_to_cache(log.term, *index, log.buf_size, log.buffer, log.outer, log.flag, log.checksum);

  if (!error)
    error = init_consensus_event_timestamp(real_buffer, real_buf_size);

  if (!error)
    error = write_consensus_log(log.flag, log.term, real_buf_size, log.checksum);

  if (!error)
    error =  revise_entry_and_write(m_binlog_file, real_buffer, real_buf_size);

  if (!error) {
    store_gtid_for_xpaxos((const char*)real_buffer, rli);
  }

  bytes += real_buf_size;

  if (log.outer || (log.flag & (Consensus_log_event_flag::FLAG_BLOB | Consensus_log_event_flag::FLAG_BLOB_END)))
  {
    my_free(real_buffer);
  }

  bytes_written += bytes;

  if (!error)
    error = flush_and_sync(FALSE);

  if (error)
    goto err;

  consensus_log_manager.set_sync_index_if_greater(*index);
  // signal_update();
  end_pos = my_b_safe_tell(m_binlog_file->get_io_cache());
  update_binlog_end_pos(m_binlog_file->get_binlog_name(), end_pos);
  if (end_pos >= (my_off_t)max_size)
    *rotate_var = true;

  if (opt_cluster_log_type_instance) {
    consensus_ptr->updateAppliedIndex(*index);
    replica_read_manager.update_lsn(*index);
  }

err:  
  if (error)
  {
    char err_buff[MYSQL_ERRMSG_SIZE] = "Append log error Hence aborting the server.";
    exec_binlog_error_action_abort(err_buff);
  }
  mysql_mutex_unlock(&LOCK_log);
  return error;
}

int MYSQL_BIN_LOG::append_multi_consensus_logs(std::vector<ConsensusLogEntry> &logs, uint64* max_index, bool* rotate_var, Relay_log_info *rli)
{
  int error = 0;
  my_off_t end_pos = 0;
  mysql_mutex_lock(&LOCK_log);

  for (auto iter = logs.begin(); iter != logs.end(); iter++)
  {
    uint64 bytes = 0;
    size_t real_buf_size = 0;
    uchar *real_buffer = NULL;
    *max_index = consensus_log_manager.get_current_index();
    if (*max_index != iter->index)
    {
      raft::error(ER_RAFT_COMMIT) << "Consensus Index Mismatch, system current index is " << *max_index
                             << ", but the log index is " << iter->index;
      abort();
    }
    consensus_log_manager.get_fifo_cache_manager()->add_log_to_cache(iter->term, *max_index, iter->buf_size, iter->buffer, iter->outer, iter->flag, iter->checksum);

    // cluster info should consider real binlog format size
    if (iter->outer)
    {
      Consensus_cluster_info_log_event ev(iter->buf_size, (char*)(iter->buffer));
      ev.common_footer->checksum_alg = static_cast<enum_binlog_checksum_alg>
        (binlog_checksum_options);
      error = ev.write(consensus_log_manager.get_log_file());
      real_buf_size = consensus_log_manager.serialize_cache(&real_buffer);
    }
    else if (iter->flag & Consensus_log_event_flag::FLAG_BLOB)
    {
      if (iter->flag & Consensus_log_event_flag::FLAG_BLOB_START)
      {
        /* some log may truncate after leader crash recovery, so if FLAG_BLOB_START, clear the cache */
        reinit_io_cache(consensus_log_manager.get_cache(), WRITE_CACHE, 0, 0, 1);
      }
      /* save empty log as a replace to binlog */
      std::string empty_log = consensus_log_manager.get_empty_log();
      real_buffer = (uchar*)my_malloc(key_memory_thd_main_mem_root, empty_log.length(), MYF(MY_WME));
      memcpy(real_buffer, empty_log.data(), empty_log.length());
      real_buf_size = empty_log.length();
      /* save real data to cache */
      raft::info(ER_RAFT_COMMIT) << "Large event: cache the current log, size " << iter->buf_size;
      my_b_write(consensus_log_manager.get_cache(), iter->buffer, iter->buf_size);
    }
    else if (iter->flag & Consensus_log_event_flag::FLAG_BLOB_END)
    {
      DBUG_EXECUTE_IF("crash_during_large_event_receive", {DBUG_SUICIDE();});
      DBUG_EXECUTE_IF("crash_during_large_event_receive_slow", {sleep(2); DBUG_SUICIDE();});
      /* save real data to cache, now cache has the integrated blob event */
      raft::info(ER_RAFT_COMMIT) << "Large event: cache the current log, size " << iter->buf_size;
      my_b_write(consensus_log_manager.get_cache(), iter->buffer, iter->buf_size);
      /* read the total cache */
      real_buf_size = consensus_log_manager.serialize_cache(&real_buffer);
      raft::info(ER_RAFT_COMMIT) << "Large event: cache the whole log, size " << real_buf_size;
    }
    else
    {
      real_buffer = iter->buffer;
      real_buf_size = iter->buf_size;
    }

    if (!error)
      error = init_consensus_event_timestamp(real_buffer, real_buf_size);
    if (!error)
      error = write_consensus_log(iter->flag, iter->term,  real_buf_size, iter->checksum);
    if (!error)
      error = revise_entry_and_write(m_binlog_file, real_buffer,  real_buf_size);

    if (!error && rli != NULL) {
      store_gtid_for_xpaxos((const char*)real_buffer, rli);
    }

    bytes = real_buf_size;

    if (iter->outer || (iter->flag & (Consensus_log_event_flag::FLAG_BLOB | Consensus_log_event_flag::FLAG_BLOB_END)))
    {
      my_free(real_buffer);
    }

    if (error)
      break;
    bytes_written += bytes;
  }

  if (!error)
    error = flush_and_sync(FALSE);

  if (error)
    goto err;

  consensus_log_manager.set_sync_index_if_greater(*max_index);
  // signal_update();
  end_pos = my_b_safe_tell(m_binlog_file->get_io_cache());
  update_binlog_end_pos(m_binlog_file->get_binlog_name(), end_pos);
  if (end_pos >= (my_off_t)max_size)
    *rotate_var = true;

  if (opt_cluster_log_type_instance) {
    consensus_ptr->updateAppliedIndex(*max_index);
    replica_read_manager.update_lsn(*max_index);
  }

err:
  if (error)
  {
    char err_buff[MYSQL_ERRMSG_SIZE] = "Append multi logs error Hence aborting the server.";
    exec_binlog_error_action_abort(err_buff);
  }
  mysql_mutex_unlock(&LOCK_log);
  return error;
}


int MYSQL_BIN_LOG::rotate_consensus_log()
{
  int error = 0;
  bool need_real_rotate = false;
  
  mysql_mutex_lock(&LOCK_rotate);
  need_real_rotate = !rotating;
  if (!rotating)
    rotating = TRUE;
  mysql_mutex_unlock(&LOCK_rotate);
  if (need_real_rotate)
  {
    mysql_mutex_lock(&LOCK_log);
    bool check_purge = false;
    error = rotate(false, &check_purge);
    mysql_mutex_unlock(&LOCK_log);

    mysql_mutex_lock(&LOCK_rotate);
    rotating = FALSE;
    mysql_mutex_unlock(&LOCK_rotate);
  }
  return error;
}

void MYSQL_BIN_LOG::consensus_before_commit(THD *thd) {
  if (opt_initialize)
    return;
  if (thd->commit_error != THD::CE_NONE ||
      ((consensus_ptr->waitCommitIndexUpdate(thd->consensus_index - 1, thd->consensus_term) < thd->consensus_index) &&
      (thd->consensus_index > consensus_log_manager.get_consensus_info()->get_start_apply_index())) )
  {
    // TODO: need write apply index to consensus info table???
    raft::warn(ER_RAFT_COMMIT) << "Failed to commit ,because previous error or shutdown or leadership changed, system apply index:" 
      << consensus_log_manager.get_consensus_info()->get_start_apply_index()
      << " , thd consensus term:" << thd->consensus_term
      << ", consensus index:" << thd->consensus_index;

    if (thd->commit_error == THD::CE_NONE) {
      raft::warn(ER_RAFT_COMMIT) <<
          "'There are some dirty binlogs, restert to deal with them";
      flush_error_log_messages();
      abort();
    }

    thd->mark_transaction_to_rollback(true);
    thd->commit_error = THD::CE_COMMIT_ERROR;
    thd->get_transaction()->m_flags.commit_low = false;
    // define error code
    // if code is not shutdown or log too large, it must be leadership change
    if (consensus_ptr->isShutdown())
      thd->consensus_error = THD::CSS_SHUTDOWN;
    if (thd->consensus_error == THD::CSS_NONE)
      thd->consensus_error = THD::CSS_LEADERSHIP_CHANGE;

    if (thd->consensus_error == THD::CSS_LEADERSHIP_CHANGE)
      my_error(ER_CONSENSUS_LEADERSHIP_CHANGE, MYF(0));
    else if (thd->consensus_error == THD::CSS_LOG_TOO_LARGE)
      my_error(ER_CONSENSUS_LOG_TOO_LARGE, MYF(0));
    else if (thd->consensus_error == THD::CSS_SHUTDOWN)
      my_error(ER_SERVER_SHUTDOWN, MYF(0));
    else
      my_error(ER_CONSENSUS_OTHER_ERROR, MYF(0));
  }
}

void binlog_commit_pos_watcher(bool *is_running)
{
  std::string log_name;
  uint64_t commitIndex = 0, pos = 0;
  uint retry = 0;
  Format_description_log_event fd_ev, *fd_ev_p = &fd_ev;
  Log_event *ev = NULL;
  Consensus_log_event *consensus_log_ev = NULL;
  // const char *errmsg = NULL;
  bool skip = false; // skip flag if flush log

  while (*is_running)
  {
    /*
      Note that you cannot flush log if it still has pending xid,
      which means commit position locates in last binlog file.
    */
    log_name = consensus_log_manager.get_log_file_index()->get_last_log_file_name();
    Binlog_file_reader binlog_file_reader(opt_source_verify_checksum);
    if (binlog_file_reader.open(log_name.c_str())) {
      raft::error(ER_RAFT_COMMIT) << "Thread binlog_commit_pos_watcher fails to open the binlog file " << log_name.c_str();
      goto err;
    }
    skip = false;

    while (*is_running && !skip && (ev = binlog_file_reader.read_event_object()) != NULL)
    {
      switch (ev->get_type_code())
      {
      case binary_log::PREVIOUS_CONSENSUS_INDEX_LOG_EVENT:
      {
        Previous_consensus_index_log_event *consensus_prev_ev = (Previous_consensus_index_log_event*)ev;
        uint64_t prev_index = consensus_prev_ev->get_index() - 1;
        /*
          1. open a new binlog file
          2. reopen the same binlog file because truncateLog happens
        */
        if (prev_index >= consensus_log_manager.get_commit_pos_index())
        {
          pos = binlog_file_reader.position();
          consensus_log_manager.update_commit_pos(log_name, pos, prev_index);
          retry = 0; // reset retry times after a success update_commit_pos
        }

        // push to a commitIndex larger than the reported one
        while(*is_running && ((commitIndex = consensus_ptr->getCommitIndex())
            <= consensus_log_manager.get_commit_pos_index()))
        {
          my_sleep(opt_commit_pos_watcher_interval);
          // check whether 'flush log' happens
          if (log_name != consensus_log_manager.get_log_file_index()->get_last_log_file_name())
          {
            skip = true;
            break;
          }
        }
        break;
      }
      case binary_log::CONSENSUS_LOG_EVENT:
        // PolarDB-X Engine makes sure the corresponding logEntry exists if index is commitIndex
        consensus_log_ev = (Consensus_log_event*)ev;
        if (commitIndex <= consensus_log_ev->get_index())
        {
          if (commitIndex < consensus_log_ev->get_index())
          {
            raft::error(ER_RAFT_COMMIT) << "Thread binlog_commit_pos_watcher reports a unsafe commit position.";  // for defence
            assert(0); // abort on debug mode
          }
          pos = binlog_file_reader.position() + consensus_log_ev->get_length();
          consensus_log_manager.update_commit_pos(log_name, pos, consensus_log_ev->get_index());
          retry = 0; // reset retry times after a success update_commit_pos
        }

        // push to a commitIndex larger than the reported one
        while(*is_running && ((commitIndex = consensus_ptr->getCommitIndex())
            <= consensus_log_manager.get_commit_pos_index()))
        {
          my_sleep(opt_commit_pos_watcher_interval);
          // check whether 'flush log' happens
          if (log_name != consensus_log_manager.get_log_file_index()->get_last_log_file_name())
          {
            skip = true;
            break;
          }
        }
        break;
      case binary_log::FORMAT_DESCRIPTION_EVENT:
        if (fd_ev_p != &fd_ev)
          delete fd_ev_p;
        fd_ev_p = (Format_description_log_event*)ev;
        break;
      default:
        break;
      }
      if (ev != NULL && ev != fd_ev_p)
        delete ev, ev = NULL;
    } // shutdown or EOF
err:
    if (fd_ev_p != &fd_ev)
    {
      delete fd_ev_p;
      fd_ev_p = &fd_ev;
    }

    /* It is safe in truncate_log case and the error is not READ_EOF */
    if (binlog_file_reader.has_fatal_error())
    {
      // avoid sleep too long (maximum 60 * intervals) and reduce log output
      if (retry < 60)
      {
        retry++;
        raft::warn(ER_RAFT_COMMIT) << "Fail to find commit position. "
          "It could be caused by a binlog truncation or a failed read_log_event. "
          "Just wait and reopen the file.";
      }
      for (uint c = 0; c < retry && *is_running; ++c)
        my_sleep(opt_commit_pos_watcher_interval);
    }
  }
}


bool MYSQL_BIN_LOG::write_consensus_log(uint flag, uint64 term, uint64 length, uint64 checksum) {
  DBUG_ENTER("MYSQL_BIN_LOG::write_consensus_log");

  Consensus_log_event rev(flag, term, consensus_log_manager.get_current_index(), length);
  rev.set_reserve(checksum);
  if (!(rev.get_flag() & Consensus_log_event_flag::FLAG_LARGE_TRX))
    alisql_server->setLastNonCommitDepIndex(rev.get_index());
  if (opt_consensuslog_revise && is_raft_log && is_relay_log)
    rev.consensus_extra_time = consensus_log_manager.get_event_timestamp();
  if (write_event_to_binlog(&rev)) return true;
  if (!opt_initialize)
    consensus_log_manager.incr_current_index();
  DBUG_RETURN(false);
}

/**
Open a already existed binlog file for normandy

- Open the log file and the index file.
- When calling this when the file is in use, you must have a locks
on LOCK_log and LOCK_index.

@retval
0	ok
@retval
1	error
*/

bool MYSQL_BIN_LOG::open_exist_binlog(const char *log_name,
  const char *new_name,
  ulong max_size_arg,
  bool null_created_arg __attribute__((unused)),
  bool need_lock_index,
  bool need_sid_lock,
  Format_description_log_event *extra_description_event __attribute__((unused)))
{
  LOG_INFO log_info;
  int error = 1;

  // lock_index must be acquired *before* sid_lock.
  assert(need_sid_lock || !need_lock_index);
  DBUG_ENTER("MYSQL_BIN_LOG::open_binlog_for_normandy(const char *, ...)");
  DBUG_PRINT("enter", ("base filename: %s", log_name));

  mysql_mutex_assert_owner(get_log_lock());

  // find last log name according to the binlog index 
  if (!my_b_inited(&index_file))
  {
    cleanup();
    return 1;
  }
  if ((error = find_log_pos(&log_info, NullS, true)))
  {
    if (error != LOG_INFO_EOF)
    {
      raft::error(ER_RAFT_COMMIT) << "find_log_pos() failed error: " << error;
      return error;
    }
  }

  if (error == 0)
  {
    do
    {
      strmake(log_file_name, log_info.log_file_name, sizeof(log_file_name) - 1);
    } while (!(error = find_next_log(&log_info, true/*need_lock_index=true*/)));
  }

  if (error != LOG_INFO_EOF)
  {
    raft::error(ER_RAFT_COMMIT) << "find_log_pos() failed error: " << error;
    return error;
  }

  // #ifdef HAVE_REPLICATION
  DBUG_EXECUTE_IF("crash_create_non_critical_before_update_index", DBUG_SUICIDE(););
  // #endif

  write_error = 0;

  /* open the main log file */
  if (open_for_normandy(
  #ifdef HAVE_PSI_INTERFACE
	  m_key_file_log,
  #endif
	  log_name, new_name))
  {
	  DBUG_RETURN(1);                            /* all warnings issued */
  }

  max_size = max_size_arg;

  /* This must be before goto err. */
  #ifndef DBUG_OFF
  binary_log_debug::debug_pretend_version_50034_in_binlog =
	  DBUG_EVALUATE_IF("pretend_version_50034_in_binlog", true, false);
  #endif
  
  if (is_relay_log)
  {
    /* relay-log */
    if (relay_log_checksum_alg == binary_log::BINLOG_CHECKSUM_ALG_UNDEF)
    {
      /*
      PolarDB-X Engine do not send fd event to Follower, so just use binlog_checksum_options.
      The binlog_checksum_options of Leader and Follower must be set to a same value.
      */
      relay_log_checksum_alg= static_cast<enum_binlog_checksum_alg>
                            (binlog_checksum_options);
    }
  }

  atomic_log_state = LOG_OPENED;

  /*
    At every rotate memorize the last transaction counter state to use it as
    offset at logging the transaction logical timestamps.
  */
  m_dependency_tracker.rotate();

  update_binlog_end_pos();
  DBUG_RETURN(0);
}

uint64 MYSQL_BIN_LOG::wait_xid_disappear() {
  DBUG_EXECUTE_IF("semi_sync_3-way_deadlock",
                DEBUG_SYNC(current_thd, "before_rotate_binlog"););
  mysql_mutex_lock(&LOCK_xids);

  while (get_prep_xids() > 0)
  {
    DEBUG_SYNC(current_thd, "before_rotate_binlog_file");
    mysql_cond_wait(&m_prep_xids_cond, &LOCK_xids);
  }
  uint64 sync_index = consensus_log_manager.get_sync_index();
  mysql_mutex_unlock(&LOCK_xids);
  return sync_index;
}

/**
  Write the consensus log cache to the binary log.

  The cache will be reset as a READ_CACHE to be able to read the
  contents from it.

  The data will be post-processed: see class Binlog_event_writer for
  details.

  @param cache Events will be read from this IO_CACHE.
  @param writer Events will be written to this Binlog_event_writer.

  @retval true IO error.
  @retval false Success.

  @see MYSQL_BIN_LOG::write_cache
*/
bool MYSQL_BIN_LOG::write_buf_to_log_file(uchar *buffer, size_t buf_size) {
  // bool ret = my_b_safe_write(&log_file, buffer, buf_size);
  bool ret = m_binlog_file->write(buffer, buf_size);
  return ret;
}

int flush_consensus_log(THD *thd, binlog_cache_data *,
                        Binlog_event_writer *, bool &mark_as_rollback,
                        my_off_t &bytes_in_cache) {
  int error = 0;
  ulonglong buf_size;
  uchar *buffer = NULL;
  uint flag = 0;
  bool is_large_trx = false;
  // alloc the buffer
  // NOTE: already write_cache in MYSQL_BIN_LOG::write_transaction
  // error = mysql_bin_log.write_cache(thd, binlog_cache, writer);
  buf_size = my_b_tell(consensus_log_manager.get_cache());
  // determine whether log is too large
  if (buf_size > opt_consensus_max_log_size) is_large_trx = TRUE;
  // group update do not support large trx
  DBUG_EXECUTE_IF("simulate_trx_cache_error", {
    if (thd->consensus_index != 0) mark_as_rollback = true;
  });
  DBUG_EXECUTE_IF("simulate_trx_cache_error_slow", {
    if (thd->consensus_index != 0) {
      consensus_ptr->leaderTransfer(2);
      sleep(5);
      mark_as_rollback = true;
    }
  });
  if (mark_as_rollback || (!opt_consensus_large_trx && is_large_trx)) {
    raft::warn(ER_RAFT_COMMIT) <<
        "Failed to flush log ,because consensus log is too large.";
    thd->mark_transaction_to_rollback(true);
    thd->commit_error = THD::CE_COMMIT_ERROR;
    thd->get_transaction()->m_flags.commit_low = false;
    thd->consensus_error = THD::CSS_LOG_TOO_LARGE;
    bytes_in_cache = 0;
    // clear the cache
    reinit_io_cache(consensus_log_manager.get_cache(), WRITE_CACHE, 0, 0, 1);
    // rollback logical clock
    mysql_bin_log.m_dependency_tracker.step_down();
    goto end;
  }

  DBUG_EXECUTE_IF("force_large_trx", { is_large_trx = true; });
  if (!is_large_trx) {
    uint64 crc32 = 0;
    // firstly write consensus log event
    if (!error) {
      buf_size = consensus_log_manager.serialize_cache(&buffer);
      thd->consensus_index = consensus_log_manager.get_current_index();
      crc32 = opt_consensus_checksum ? checksum_crc32(0, buffer, buf_size) : 0;
      error = mysql_bin_log.write_consensus_log(flag, thd->consensus_term,
                                                buf_size, crc32);
    }

    // secondly write consensus_log_body
    if (!error) {
      int ret =
          consensus_log_manager.get_fifo_cache_manager()->add_log_to_cache(
              thd->consensus_term, thd->consensus_index, buf_size, buffer,
              FALSE, flag, crc32, true);
      error = mysql_bin_log.write_buf_to_log_file(buffer, buf_size);
      // ret == 1 means fifo do not use the buffer
      if (ret == 1 && buffer) my_free(buffer);
      buffer = NULL; /* fifo cache reuse the buffer */
    }
  } else {
    if (!error) {
      size_t buffer_maxsize =
          opt_consensus_max_log_size > opt_consensus_large_event_split_size
              ? opt_consensus_max_log_size
              : opt_consensus_large_event_split_size;
      buffer = (uchar *)my_malloc(key_memory_thd_main_mem_root, buffer_maxsize,
                                  MYF(MY_WME));
      error = large_trx_flush(thd, buffer, buf_size);
    }
  }
end:
  if (buffer) {
    my_free(buffer);
  }

  return error;
}