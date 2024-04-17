/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "consensus_admin.h"

#include "consensus_recovery_manager.h"
#include "sql/appliedindex_checker.h"
#include "sql/auth/auth_acls.h"
#include "sql/binlog.h"
#include "sql/bl_consensus_log.h"
#include "sql/consensus_log_manager.h"
#include "sql/current_thd.h"
#include "sql/mysqld_thd_manager.h"
#include "sql/rpl_mi.h"
#include "sql/rpl_msr.h"
#include "sql/rpl_replica.h"
#include "sql/rpl_rli.h"

#include "my_config.h"

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>

#include "my_dbug.h"
#include "sql/debug_sync.h"  // DEBUG_SYNC
#include "sql/log.h"
#include "sql/sql_lex.h"
//#include "sql/binlog_ext.h"
#include "mysql/psi/mysql_file.h"
#include "replica_read_manager.h"
#include "sql/protocol.h"

using std::max;

/* wait no commit index when exec event in xpaxos_channel */
bool opt_disable_wait_commitindex = false;

/* Helper function for SHOW BINLOG/RELAYLOG EVENTS */

bool show_consensuslog_events(THD *thd, MYSQL_BIN_LOG *binary_log,
                              unsigned long long consensus_index) {
  Protocol *protocol = thd->get_protocol();
  List<Item> field_list;
  const char *errmsg = 0;
  bool ret = true;
  Binlog_file_reader binlog_file_reader(opt_source_verify_checksum);
  int old_max_allowed_packet = thd->variables.max_allowed_packet;
  LOG_INFO linfo;

  DBUG_ENTER("show_consensuslog_events");

  assert(thd->lex->sql_command == SQLCOM_SHOW_CONSENSUSLOG_EVENTS);

  Format_description_log_event *description_event =
      new Format_description_log_event(); /* MySQL 4.0 by default ********* now
                                             is 4 ???? */

  if (binary_log->is_open()) {
    Query_expression *unit = thd->lex->unit;
    ha_rows event_count, limit_start, limit_end;
    uint64 pos = BIN_LOG_HEADER_SIZE;  // user-friendly
    char search_file_name[FN_REFLEN], *name;
    mysql_mutex_t *log_lock = binary_log->get_log_lock();
    Log_event *ev;

    unit->set_limit(thd, thd->lex->current_query_block());
    limit_start = unit->offset_limit_cnt;
    limit_end = unit->select_limit_cnt;
    // find right position
    if (binary_log->consensus_get_log_position(consensus_index,
                                               search_file_name, &pos)) {
      errmsg = "Could not find target log";
      goto err;
    }

    name = search_file_name;

    linfo.index_file_offset = 0;

    if (binary_log->find_log_pos(&linfo, name, true /*need_lock_index=true*/)) {
      errmsg = "Could not find target log";
      goto err;
    }

    mysql_mutex_lock(&thd->LOCK_thd_data);
    thd->current_linfo = &linfo;
    mysql_mutex_unlock(&thd->LOCK_thd_data);

    if (binlog_file_reader.open(linfo.log_file_name)) goto err;

    my_off_t end_pos;
    /*
    Acquire LOCK_log only for the duration to calculate the
    log's end position. LOCK_log should be acquired even while
    we are checking whether the log is active log or not.
    */
    mysql_mutex_lock(log_lock);
    if (binary_log->is_active(linfo.log_file_name)) {
      LOG_INFO li;
      binary_log->get_current_log(&li, false /*LOCK_log is already acquired*/);
      end_pos = li.pos;
    } else {
      end_pos = my_b_filelength(binlog_file_reader.get_io_cache());
    }
    mysql_mutex_unlock(log_lock);

    /*
    to account binlog event header size
    */
    thd->variables.max_allowed_packet += MAX_LOG_EVENT_HEADER;

    DEBUG_SYNC(thd, "after_show_binlog_event_found_file");
    /*
    open_binlog_file() sought to position 4.
    Read the first event in case it's a Format_description_log_event, to
    know the format. If there's no such event, we are 3.23 or 4.x. This
    code, like before, can't read 3.23 binlogs.
    This code will fail on a mixed relay log (one which has Format_desc then
    Rotate then Format_desc).
    */
    // ev = Log_event::read_log_event(&log, (mysql_mutex_t*)0,
    // description_event, opt_source_verify_checksum);
    ev = binlog_file_reader.read_event_object();
    if (ev) {
      if (ev->get_type_code() == binary_log::FORMAT_DESCRIPTION_EVENT) {
        delete description_event;
        description_event = (Format_description_log_event *)ev;
      } else
        delete ev;
    }

    binlog_file_reader.seek(pos);

    if (!description_event->is_valid()) {
      errmsg = "Invalid Format_description event; could be out of memory";
      goto err;
    }

    for (event_count = 0; (ev = binlog_file_reader.read_event_object());) {
      DEBUG_SYNC(thd, "wait_in_show_binlog_events_loop");
      if (ev->get_type_code() == binary_log::FORMAT_DESCRIPTION_EVENT)
        description_event->common_footer->checksum_alg =
            ev->common_footer->checksum_alg;
      if (event_count >= limit_start &&
          ev->net_send(protocol, linfo.log_file_name, pos)) {
        errmsg = "Net error";
        delete ev;
        goto err;
      }

      pos = my_b_tell(binlog_file_reader.get_io_cache());
      delete ev;

      if (++event_count >= limit_end || pos >= end_pos) break;
    }

    if (event_count < limit_end && binlog_file_reader.has_fatal_error()) {
      errmsg = "Wrong offset or I/O error";
      goto err;
    }
  }
  // Check that linfo is still on the function scope.
  DEBUG_SYNC(thd, "after_show_consensuslog_events");

  ret = false;

err:
  delete description_event;
#if 0
  if (file >= 0)
  {
    end_io_cache(&log);
    mysql_file_close(file, MYF(MY_WME));
  }
#endif
  if (errmsg) {
    my_error(ER_ERROR_WHEN_EXECUTING_COMMAND, MYF(0),
             "SHOW CONSENSUSLOG EVENTS", errmsg);
  } else
    my_eof(thd);

  mysql_mutex_lock(&thd->LOCK_thd_data);
  thd->current_linfo = 0;
  mysql_mutex_unlock(&thd->LOCK_thd_data);
  thd->variables.max_allowed_packet = old_max_allowed_packet;
  DBUG_RETURN(ret);
}

/**
Execute a SHOW CONSENSUSLOG EVENTS statement.

@param thd Pointer to THD object for the client thread executing the
statement.

@retval FALSE success
@retval TRUE failure
*/
bool mysql_show_consensuslog_events(THD *thd,
                                    unsigned long long consensus_index) {
  mem_root_deque<Item *> field_list(thd->mem_root);
  DBUG_ENTER("mysql_show_consensuslog_events");

  assert(thd->lex->sql_command == SQLCOM_SHOW_CONSENSUSLOG_EVENTS);

  Log_event::init_show_field_list(&field_list);
  if (thd->send_result_metadata(field_list,
                                Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
    DBUG_RETURN(true);

  /*
  Wait for handlers to insert any pending information
  into the binlog.  For e.g. ndb which updates the binlog asynchronously
  this is needed so that the uses sees all its own commands in the binlog
  */
  ha_binlog_wait(thd);
  GUARDED_READ_CONSENSUS_LOG();
  bool res = show_consensuslog_events(thd, consensus_log, consensus_index);
  DBUG_RETURN(res);
}

/**
  Execute a SHOW CONSENSUS LOGS statement.

  @param thd Pointer to THD object for the client thread executing the
  statement.

  @retval FALSE success
  @retval TRUE failure
*/
bool show_consensus_logs(THD *thd) {
  IO_CACHE *index_file;
  LOG_INFO cur;
  File file;
  char fname[FN_REFLEN];
  mem_root_deque<Item *> field_list(thd->mem_root);
  size_t length;
  size_t cur_dir_len;
  Protocol *protocol = thd->get_protocol();
  DBUG_ENTER("show_consensus_logs");

  GUARDED_READ_CONSENSUS_LOG();
  if (!consensus_log->is_open()) {
    my_error(ER_NO_BINARY_LOGGING, MYF(0));
    DBUG_RETURN(true);
  }

  field_list.push_back(new Item_empty_string("Log_name", 255));
  field_list.push_back(
      new Item_return_int("File_size", 20, MYSQL_TYPE_LONGLONG));
  field_list.push_back(
      new Item_return_int("Start_log_index", 20, MYSQL_TYPE_LONGLONG));

  if (thd->send_result_metadata(field_list,
                                Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF)) {
    DBUG_RETURN(true);
  }


  mysql_mutex_lock(consensus_log->get_log_lock());
  DEBUG_SYNC(thd, "show_binlogs_after_lock_log_before_lock_index");
  auto index_guard = create_lock_guard(
    [&] { consensus_log->lock_index(); },
    [&] { consensus_log->unlock_index(); }
  );

  index_file = consensus_log->get_index_file();
  consensus_log->raw_get_current_log(&cur);           // dont take mutex

  mysql_mutex_unlock(consensus_log->get_log_lock());  // lockdep, OK

  cur_dir_len = dirname_length(cur.log_file_name);

  reinit_io_cache(index_file, READ_CACHE, (my_off_t)0, 0, 0);

  /* The file ends with EOF or empty line */
  while ((length = my_b_gets(index_file, fname, sizeof(fname))) > 1) {
    size_t dir_len;
    ulonglong file_length = 0;  // Length if open fails
    fname[--length] = '\0';     // remove the newline

    protocol->start_row();
    dir_len = dirname_length(fname);
    length -= dir_len;
    protocol->store_string(fname + dir_len, length, &my_charset_bin);

    if (!(strncmp(fname + dir_len, cur.log_file_name + cur_dir_len, length)))
      file_length = cur.pos; /* The active log, use the active position */
    else {
      /* this is an old log, open it and find the size */
      if ((file = mysql_file_open(key_file_binlog, fname,
                                  O_RDONLY | O_SHARE | O_BINARY, MYF(0))) >=
          0) {
        file_length = (ulonglong)mysql_file_seek(file, 0L, MY_SEEK_END, MYF(0));
        mysql_file_close(file, MYF(0));
      }
    }
    protocol->store(file_length);

    ulonglong start_index =
        consensus_log_manager.get_log_file_index()->get_start_index_of_file(
            std::string(fname));
    protocol->store(start_index);
    if (protocol->end_row()) {
      DBUG_PRINT(
          "info",
          ("stopping dump thread because protocol->write failed at line %d",
           __LINE__));
      goto err;
    }
  }
  if (index_file->error == -1) goto err;
  index_guard.unlock();
  consensus_guard.unlock();
  my_eof(thd);
  DBUG_RETURN(false);

err:
  DBUG_RETURN(true);
}

static bool invalid_on_consensus_limited(enum_sql_command cmd,
                                         bool no_write_to_binlog) {
  /* cases for setting no_write_to_binlog, according to sql_yacc.yy */
  /* alter/optimize may execute slowly, just check it ahead. */
  if (cmd == SQLCOM_ALTER_TABLE || cmd == SQLCOM_OPTIMIZE)
    return no_write_to_binlog ? false : true;

  /* update(2017.12.12): allow prepare statement */
  /* update(2018.01.22): disallow change master */
  /* update(2018.04.10): allow follower install/uninstall/show plugins */
  /* update(2018.11.21): allow follower check table */
  return (cmd > SQLCOM_SELECT && cmd <= SQLCOM_DROP_INDEX) ||
         (cmd == SQLCOM_LOAD) || (cmd == SQLCOM_GRANT) ||
         (cmd == SQLCOM_CHANGE_MASTER) ||
         (cmd >= SQLCOM_CREATE_DB && cmd < SQLCOM_CHECK) ||
         (cmd >= SQLCOM_ASSIGN_TO_KEYCACHE && cmd < SQLCOM_FLUSH) ||
         (cmd >= SQLCOM_DELETE_MULTI && cmd <= SQLCOM_UPDATE_MULTI) ||
         (cmd >= SQLCOM_CREATE_USER && cmd <= SQLCOM_REVOKE_ALL) ||
         (cmd >= SQLCOM_CREATE_PROCEDURE && cmd <= SQLCOM_SHOW_STATUS_FUNC) ||
         (cmd >= SQLCOM_CREATE_VIEW && cmd <= SQLCOM_DROP_TRIGGER) ||
         (cmd == SQLCOM_ALTER_TABLESPACE) ||
         (cmd == SQLCOM_BINLOG_BASE64_EVENT) ||
         (cmd >= SQLCOM_CREATE_SERVER && cmd <= SQLCOM_DROP_EVENT) ||
         (cmd >= SQLCOM_GET_DIAGNOSTICS && cmd <= SQLCOM_SHOW_CREATE_USER) ||
         (cmd == SQLCOM_ALTER_INSTANCE);
}

static bool invalid_on_logger_limited(enum_sql_command cmd) {
  return (cmd == SQLCOM_START_XPAXOS_REPLICATION) ||
         (cmd == SQLCOM_STOP_XPAXOS_REPLICATION) ||
         (cmd == SQLCOM_CHANGE_MASTER);
}

static bool invalid_on_consensus_force_recovery_limited(enum_sql_command cmd) {
  return (cmd == SQLCOM_START_XPAXOS_REPLICATION) ||
         (cmd == SQLCOM_STOP_XPAXOS_REPLICATION);
}

int consensus_command_limit(THD *thd) {
  /* rw check: leader read-write, others read-only */
  bool reject_query = false;
  bool is_leader = false;
  DBUG_ENTER("consensus_command_limit");
  // dd_upgrade execute inner sql before consensus module startup
  if (!opt_initialize && consensus_ptr != NULL) {
    if (consensus_ptr->getState() == alisql::Paxos::LEADER) {
      is_leader = true;
      if (consensus_ptr->getTerm() != consensus_log_manager.get_current_term())
        reject_query = true;
    } else {
      is_leader = false;
      if (invalid_on_consensus_limited(thd->lex->sql_command,
                                       thd->lex->no_write_to_binlog))
        reject_query = true;
    }
  }
  if (reject_query && !thd->slave_thread) {
    if ((thd->security_context()->master_access() & SUPER_ACL) == 0) {
      // normal account
      if (is_leader)
        my_error(ER_CONSENSUS_LEADER_NOT_ALLOWED, MYF(0));
      else
        my_error(ER_CONSENSUS_FOLLOWER_NOT_ALLOWED, MYF(0));
      DBUG_RETURN(1);
    } else if (thd->variables.opt_force_revise == false &&
               (thd->variables.option_bits &
                OPTION_BIN_LOG))  // arg is only used for super account
    {
      // super account
      if (!opt_cluster_log_type_instance) {
        // log type node allow super user to write data with sql-log-bin off
        if (is_leader &&
            invalid_on_consensus_limited(thd->lex->sql_command,
                                         thd->lex->no_write_to_binlog)) {
          my_error(ER_CONSENSUS_LEADER_NOT_ALLOWED, MYF(0));
          DBUG_RETURN(1);
        }
        if (!is_leader) {
          my_error(ER_CONSENSUS_FOLLOWER_NOT_ALLOWED, MYF(0));
          DBUG_RETURN(1);
        }
      }
    }
  }

  /* logger node check: logger is not allowed to do configure change unless
   * force_revise is set to TRUE */
  if (opt_cluster_log_type_instance &&
      invalid_on_logger_limited(thd->lex->sql_command) &&
      thd->variables.opt_force_revise == false) {
    my_error(ER_CONSENSUS_LOG_TYPE_NODE, MYF(0));
    DBUG_RETURN(1);
  }

  /* Consensus related commands are not allowed in consensus_force_recovery mode
   */
  if (opt_consensus_force_recovery &&
      invalid_on_consensus_force_recovery_limited(thd->lex->sql_command)) {
    my_error(ER_CONSENSUS_SERVER_NOT_READY, MYF(0));
    DBUG_RETURN(1);
  }

  DBUG_RETURN(0);
}

class Kill_all_conn : public Do_THD_Impl {
 public:
  Kill_all_conn() {}

  void operator()(THD *thd_to_kill) override {
    mysql_mutex_lock(&thd_to_kill->LOCK_thd_data);

    /* kill all connections */
    if (thd_to_kill->security_context()->has_account_assigned() &&
        thd_to_kill->killed != THD::KILL_CONNECTION &&
        !thd_to_kill->slave_thread)
      thd_to_kill->awake(THD::KILL_CONNECTION);

    mysql_mutex_unlock(&thd_to_kill->LOCK_thd_data);
  }
};

void killall_threads() {
  Kill_all_conn kill_all_conn;
  Global_THD_manager *thd_manager = Global_THD_manager::get_instance();
  thd_manager->do_for_all_thd(&kill_all_conn);
}

class Kill_all_dump_conn : public Do_THD_Impl {
 public:
  Kill_all_dump_conn() {}

  void operator()(THD *thd_to_kill) override {
    mysql_mutex_lock(&thd_to_kill->LOCK_thd_data);

    /* Kill all binlog dump connections */
    if (thd_to_kill->security_context()->has_account_assigned() &&
        (thd_to_kill->get_command() == COM_BINLOG_DUMP ||
         thd_to_kill->get_command() == COM_BINLOG_DUMP_GTID) &&
        thd_to_kill->killed != THD::KILL_CONNECTION &&
        !thd_to_kill->slave_thread)
      thd_to_kill->awake(THD::KILL_CONNECTION);

    mysql_mutex_unlock(&thd_to_kill->LOCK_thd_data);
  }
};

void killall_dump_threads() {
  Kill_all_dump_conn Kill_all_dump_conn;
  Global_THD_manager *thd_manager = Global_THD_manager::get_instance();
  thd_manager->do_for_all_thd(&Kill_all_dump_conn);
}

int start_consensus_apply_threads() {
  DBUG_ENTER("start_consensus_apply_threads");
  Master_info *mi = NULL;
  int thread_mask = SLAVE_SQL;
  int error = 0;
  channel_map.wrlock();
  if (!opt_skip_replica_start || !opt_initialize) {
    for (mi_map::iterator it = channel_map.begin(); it != channel_map.end();
         it++) {
      mi = it->second;

      // Todo: mi must be itself
      /* If server id is not set, start_slave_thread() will say it */
      if (mi &&
          channel_map.is_xpaxos_replication_channel_name(mi->get_channel())) {
        /* same as in start_slave() cache the global var values into rli's
         * members */
        mi->rli->opt_replica_parallel_workers =
            opt_mts_replica_parallel_workers;
        mi->rli->checkpoint_group = opt_mta_checkpoint_group;
        if (mts_parallel_option == MTS_PARALLEL_TYPE_DB_NAME)
          mi->rli->channel_mts_submode = MTS_PARALLEL_TYPE_DB_NAME;
        else
          mi->rli->channel_mts_submode = MTS_PARALLEL_TYPE_LOGICAL_CLOCK;
        /* wait intergrity consensus log  */
        while (mi->rli->get_consensus_apply_index() >
               consensus_log_manager.get_sync_index())
          my_sleep(2000000);
        if (start_slave_threads(true /*need_lock_slave=true*/,
                                false /*wait_for_start=false*/, mi,
                                thread_mask)) {
          /*
          Creation of slave threads for subsequent channels are stopped
          if a failure occurs in this iteration.
          @todo:have an option if the user wants to continue
          the replication for other channels.
          */
          xp::error(ER_XP_0) << "Failed to create slave threads";
          error = 1;
        }
      }
    }
  }
  channel_map.unlock();
  DBUG_RETURN(error);
}

int check_exec_consensus_log_end_condition(Relay_log_info *rli,
                                           bool is_xpaxos_replication) {
  DBUG_ENTER("check_exec_consensus_log_end_condition");
  if (is_xpaxos_replication && !opt_disable_wait_commitindex) {
    uint64 tmpCommitIndex = 0;
    // when the followe case, continue read, return 0
    //   commitIndex > applyIndex
    //   commitIndex == applyIndex && apply_current_pos < apply_end_pos
    // any the followe case, wait and retry
    //   commitIndex < applyIndex
    //   commitIndex == applyIndex && apply_current_pos >= apply_end_pos
    while (((tmpCommitIndex = consensus_ptr->checkCommitIndex(
                 consensus_log_manager.get_real_apply_index() - 1,
                 consensus_log_manager.get_current_term())) <
            consensus_log_manager.get_real_apply_index()) ||
           (tmpCommitIndex == consensus_log_manager.get_real_apply_index() &&
            consensus_log_manager.get_apply_index_current_pos() >=
                consensus_log_manager.get_apply_index_end_pos())) {
      if (consensus_ptr->isShutdown()) {
        xp::info(ER_XP_APPLIER)
            << "Apply thread is terminated because of shutdown";
        DBUG_RETURN(1);
      }

      if (sql_slave_killed(rli->info_thd, rli)) {
        xp::system(ER_XP_APPLIER)
            << "Apply thread is terminated because of slave_killed";
        DBUG_RETURN(1);
      }

      if (rli->is_time_for_mta_checkpoint() ) {
        if (mta_checkpoint_routine(rli, false)) {
          DBUG_RETURN(1);
        } else {
          // mta_checkpoint_routine could change thd status to waiting for handler
          // commit, this is a hack to solve this problem.
          THD_STAGE_INFO(current_thd, stage_reading_event_from_the_relay_log);
        }
      }

      // determine whether exit
      uint64 stop_term = consensus_log_manager.get_stop_term();
      long time_diff = (long)(time(0) - rli->last_master_timestamp);
      if (stop_term == UINT64_MAX) {
        my_sleep(opt_consensus_check_commit_index_interval);
        continue;
      } else if (consensus_log_manager.get_apply_term() >= stop_term ||
                 opt_consensus_leader_stop_apply ||
                 (opt_consensus_leader_stop_apply_time &&
                  (time_diff < (long)opt_consensus_leader_stop_apply_time))) {
        xp::system(ER_XP_APPLIER)
            << "Apply thread stop, opt_consensus_leader_stop_apply: "
            << (opt_consensus_leader_stop_apply ? "true" : "false")
            << ", seconds_behind_master: " << time_diff
            << ", opt_consensus_leader_stop_apply_time: "
            << opt_consensus_leader_stop_apply_time;
        opt_consensus_leader_stop_apply = false;
        mysql_mutex_lock(consensus_log_manager.get_apply_thread_lock());
        mysql_cond_broadcast(consensus_log_manager.get_catchup_cond());
        consensus_log_manager.set_apply_catchup(true);
        rli->sql_thread_kill_accepted = true;
        rli->force_apply_queue_before_stop = true;
        mysql_mutex_unlock(consensus_log_manager.get_apply_thread_lock());
        xp::system(ER_XP_APPLIER)
            << "Apply thread catchup commit index, consensus index: "
            << rli->get_consensus_apply_index()
            << ", current term: " << consensus_log_manager.get_current_term()
            << ", apply term: " << consensus_log_manager.get_apply_term()
            << ", stop term: " << consensus_log_manager.get_stop_term();
        DBUG_RETURN(1);
      } else if (consensus_ptr->getCommitIndex() >
                     consensus_log_manager.get_real_apply_index() ||
                 (consensus_ptr->getCommitIndex() ==
                      consensus_log_manager.get_real_apply_index() &&
                  consensus_log_manager.get_apply_index_current_pos() <
                      consensus_log_manager.get_apply_index_end_pos()) ||
                 (consensus_ptr->getCommitIndex() == 1 &&
                  consensus_log_manager.get_real_apply_index() == 1)) {
        // not reach commit index, continue to read log
        break;
      } else {
        // reach commit index, continue to wait exit condition
        my_sleep(opt_consensus_check_commit_index_interval);
        continue;
      }
    }
  }

  DBUG_RETURN(0);
}

void update_consensus_apply_pos(Relay_log_info *rli, Log_event *ev,
                                bool is_xpaxos_replication) {
  MY_UNUSED(rli);
  if (is_xpaxos_replication) {
    // update apply index
    /* for large trx, use the first one */
    if (ev->get_type_code() == binary_log::CONSENSUS_LOG_EVENT) {
      Consensus_log_event *r_ev = (Consensus_log_event *)ev;
      uint64 consensus_index = r_ev->get_index();
      uint64 consensus_term = r_ev->get_term();
      uint64 consensus_index_end_pos =
          r_ev->future_event_relay_log_pos + r_ev->get_length();

      if (r_ev->get_flag() & Consensus_log_event_flag::FLAG_LARGE_TRX) {
        if (!consensus_log_manager.get_in_large_trx_applying()) {
          consensus_log_manager.set_apply_index(consensus_index);
          consensus_log_manager.set_apply_term(consensus_term);
          consensus_log_manager.set_apply_ev_sequence(0);
          consensus_log_manager.set_in_large_trx_applying(true);
        }
      } else if (r_ev->get_flag() &
                 Consensus_log_event_flag::FLAG_LARGE_TRX_END) {
        consensus_log_manager.set_in_large_trx_applying(false);
      } else {
        /* normal case */
        consensus_log_manager.set_apply_index(consensus_index);
        consensus_log_manager.set_apply_term(consensus_term);
        consensus_log_manager.set_apply_ev_sequence(0);
      }
      consensus_log_manager.set_real_apply_index(consensus_index);
      consensus_log_manager.set_apply_index_end_pos(consensus_index_end_pos);

      // xp::info(ER_XP_APPLIER) << "update_consensus_apply_pos: " <<
      // consensus_index
      //                       << ", consensus_term: " << consensus_term
      //                       << ", consensus_index: " << consensus_index
      //                       << ", consensus_index_end_pos: " <<
      //                       consensus_index_end_pos
      //                       << ", consensus_index_current_pos: " <<
      //                       ev->future_event_relay_log_pos;
    }

    consensus_log_manager.set_apply_index_current_pos(
        ev->future_event_relay_log_pos);
    ev->consensus_index = consensus_log_manager.get_apply_index();
    ev->consensus_real_index = consensus_log_manager.get_real_apply_index();
    ev->consensus_index_end_pos =
        consensus_log_manager.get_apply_index_end_pos();
    ev->consensus_sequence = consensus_log_manager.get_apply_ev_sequence();
    consensus_log_manager.incr_apply_ev_sequence();
  }
}

int calculate_consensus_apply_start_pos(Relay_log_info *rli,
                                        bool is_xpaxos_channel) {
  uint64 recover_status = 0;
  ulonglong start_apply_index = 0;
  uint64 recover_term = 0;
  std::string recover_log_content;
  bool recover_outer = false;
  uint recover_flag = 0;
  uint64 recover_checksum = 0;
  uint64 rli_appliedindex = 0;
  uint64 log_pos = 0;
  char log_name[FN_REFLEN];

  if (is_xpaxos_channel) {
    recover_status =
        consensus_log_manager.get_consensus_info()->get_recover_status();
    assert(consensus_log_manager.get_status() == RELAY_LOG_WORKING);
    start_apply_index =
        consensus_log_manager.get_consensus_info()->get_start_apply_index();
    xp::system(ER_XP_APPLIER)
        << "Apply thread start, recover status = " << recover_status
        << ", start apply index = " << start_apply_index
        << ", rli consensus index " << rli->get_consensus_apply_index()
        << ", consensus_ptr CommitIndex " << consensus_ptr->getCommitIndex();

    // follower recover, do nothing, leader recover should set apply start point
    if (recover_status == BINLOG_WORKING) {
      // leader degrade
      if (start_apply_index == 0) {
        // crash when the role is leader
        // care whether truncate log
        uint64 recover_index = consensus_log_manager.get_recovery_manager()
                                   ->get_last_leader_term_index();
        uint64 next_index =
            consensus_log_manager.get_next_trx_index(recover_index);
        if (consensus_log_manager.get_log_position(next_index, false, log_name,
                                                   &log_pos)) {
          xp::error(ER_XP_APPLIER)
              << "Apply thread cannot find start index " << next_index;
          abort();
        }
        xp::system(ER_XP_APPLIER)
            << "Apply thread group relay log file name = '" << log_name
            << "', pos = " << log_pos << "', next_index = " << next_index
            << ", rli apply index = " << recover_index;
        rli->set_group_relay_log_name(log_name);
        rli->set_group_relay_log_pos(log_pos);

        if (consensus_log_manager.get_log_directly(
                recover_index, &recover_term, recover_log_content,
                &recover_outer, &recover_flag, &recover_checksum, false)) {
          xp::error(ER_XP_APPLIER)
              << "Apply thread cannot find term by index " << recover_index;
          abort();
        }
        rli->set_consensus_apply_index(recover_index);
        consensus_log_manager.set_apply_term(recover_term);
        rli->flush_info(true);
      } else {
        // role already degraded to follower ,but log status is still binlog
        // working
        uint64 start_index =
            max(start_apply_index, rli->get_consensus_apply_index());
        uint64 next_index =
            consensus_log_manager.get_next_trx_index(start_index);
        if (consensus_log_manager.get_log_position(next_index, false, log_name,
                                                   &log_pos)) {
          xp::error(ER_XP_APPLIER)
              << "Apply thread cannot find start index " << next_index;
          abort();
        }
        xp::system(ER_XP_APPLIER)
            << "Apply thread group relay log file name = '" << log_name
            << "', pos = " << log_pos << "', next_index = " << next_index
            << ", rli apply index = " << start_index;
        rli->set_group_relay_log_name(log_name);
        rli->set_group_relay_log_pos(log_pos);

        if (consensus_log_manager.get_log_directly(
                start_index, &recover_term, recover_log_content, &recover_outer,
                &recover_flag, &recover_checksum, false)) {
          xp::error(ER_XP_APPLIER)
              << "Apply thread cannot find term by index " << start_index;
          abort();
        }
        rli->set_consensus_apply_index(start_index);
        consensus_log_manager.set_apply_term(recover_term);
        rli->flush_info(true);
      }
    } else {
      if (start_apply_index == 0) {
        // node first initial
        if (consensus_log_manager.get_recovery_manager()
                    ->get_last_leader_term_index() == 0 &&
            rli->get_consensus_apply_index() == 0) {
          // start replay from first position
          uint64 start_index = 0, start_term = 1;
          uint64 next_index = 1;
          if (consensus_log_manager.get_log_position(next_index, false,
                                                     log_name, &log_pos)) {
            xp::error(ER_XP_APPLIER)
                << "Apply thread cannot find start index " << next_index;
            abort();
          }
          xp::system(ER_XP_APPLIER)
              << "Apply thread group relay log file name = '" << log_name
              << "', pos = " << log_pos << "', next_index = " << next_index
              << ", rli apply index = " << start_index;
          rli->set_group_relay_log_name(log_name);
          rli->set_group_relay_log_pos(log_pos);
          rli->set_consensus_apply_index(start_index);
          consensus_log_manager.set_apply_term(start_term);
          rli->flush_info(true);
        } else {
          // because backup restore will reorganize the log , so should use
          // index to set the replay pos
          uint64 start_index = rli->get_consensus_apply_index();
          uint64 next_index =
              consensus_log_manager.get_next_trx_index(start_index);
          if (consensus_log_manager.get_log_position(next_index, false,
                                                     log_name, &log_pos)) {
            xp::error(ER_XP_APPLIER)
                << "Apply thread cannot find start index " << next_index;
            abort();
          }
          xp::system(ER_XP_APPLIER)
              << "Apply thread group relay log file name = '" << log_name
              << "', pos = " << log_pos << "', next_index = " << next_index
              << ", rli apply index = " << start_index;

          rli->set_group_relay_log_name(log_name);
          rli->set_group_relay_log_pos(log_pos);
          rli->flush_info(true);
        }
      } else {
        // these code will not reached anymore
        // start_apply_index != 0 && recover_status == RELAYLOG_WORKING is
        // impossible
        uint64 start_index =
            max(start_apply_index, rli->get_consensus_apply_index());
        uint64 next_index =
            consensus_log_manager.get_next_trx_index(start_index);
        if (consensus_log_manager.get_log_position(next_index, false, log_name,
                                                   &log_pos)) {
          xp::error(ER_XP_APPLIER)
              << "Apply thread cannot find start index " << next_index;
          abort();
        }
        xp::system(ER_XP_APPLIER)
            << "Apply thread group relay log file name = '" << log_name
            << "', pos = " << log_pos << ", rli apply index = " << start_index;
        rli->set_group_relay_log_name(log_name);
        rli->set_group_relay_log_pos(log_pos);

        if (consensus_log_manager.get_log_directly(
                start_index, &recover_term, recover_log_content, &recover_outer,
                &recover_flag, &recover_checksum, false)) {
          xp::error(ER_XP_APPLIER)
              << "Apply thread cannot find start index " << next_index;
          abort();
        }
        rli->set_consensus_apply_index(start_index);
        consensus_log_manager.set_apply_term(recover_term);
        rli->flush_info(true);
      }
    }

    // deal with appliedindex
    rli_appliedindex = rli->get_consensus_apply_index();
    rli_appliedindex = opt_appliedindex_force_delay >= rli_appliedindex
                           ? 0
                           : rli_appliedindex - opt_appliedindex_force_delay;
    consensus_ptr->updateAppliedIndex(rli_appliedindex);
    replica_read_manager.update_lsn(rli_appliedindex);

    /*
     * Wait until new leader empty log is committed,
     * which means truncate log is finished and we have correct hotlog.
     */
    /*
    while (consensus_ptr->getCommitIndex() <=
    consensus_log_manager.get_consensus_info()->get_start_apply_index())
      my_sleep(5000);
    */
    // set consensus info to relay-working
    consensus_log_manager.get_consensus_info()->set_start_apply_index(0);
    consensus_log_manager.get_consensus_info()->set_last_leader_term(0);
    consensus_log_manager.get_consensus_info()->set_recover_status(
        RELAY_LOG_WORKING);
    if (consensus_log_manager.get_consensus_info()->flush_info(true, false)) {
      rli->report(ERROR_LEVEL, ER_SLAVE_FATAL_ERROR,
                  "Error flush consensus info set recover status");
      return -1;
    }
  }

  return 0;
}
