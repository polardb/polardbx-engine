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

#include "sql/binlog.h"
#include "sql/consensus_log_manager.h"
#include "sql/bl_consensus_log.h"
#include "consensus_recovery_manager.h"

bool MYSQL_BIN_LOG::open_exist_binlog(const char *, const char *, ulong, bool,
                                      bool, bool,
                                      Format_description_log_event *) {
  return false;
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
      raft::error(ER_RAFT_0) << "Thread binlog_commit_pos_watcher fails to open the binlog file " << log_name.c_str();
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
            raft::error(ER_RAFT_0) << "Thread binlog_commit_pos_watcher reports a unsafe commit position.";  // for defence
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
        raft::warn(ER_RAFT_0) << "Fail to find commit position. "
          << "It could be caused by a binlog truncation or a failed read_log_event. "
          << "Just wait and reopen the file.";
      }
      for (uint c = 0; c < retry && *is_running; ++c)
        my_sleep(opt_commit_pos_watcher_interval);
    }
  }
}
