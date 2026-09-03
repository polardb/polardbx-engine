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

/** @file sql/komodo0xlog0engine.cc

  Binlog behaviours related to komodo lizard system.

  Created 2023-06-20 by Jianwei.zhao
 *******************************************************/

#include "my_inttypes.h"

#include "consensus_admin.h"
#include "rpl_handler.h"
#include "rpl_msr.h"
#include "rpl_replica.h"
#include "rpl_rli.h"
#include "sql/binlog.h"
#include "sql/consensus_recovery_manager.h"
#include "sql/events.h"
#include "sql/komodo0consensus0xlog.h"
#include "sql/komodo0flusher.h"
#include "sql/komodo0xlog0engine.h"
#include "sql/komodo0xlog0recovery.h"
#include "sql/sql_lex.h"
#include "sql/xa/komodo0cmmt0policy.h"

#include "auth/auth_acls.h"
#include "komodo0xlog0meta.h"

#include "extra/IS/src/algorithm/autonomy.h"
#include "extra/IS/src/algorithm/paxos.h"

#include "sql/consensus/komodo0fifo0cache.h"

namespace komodo {

XLog_engine::XLog_engine(MYSQL_BIN_LOG *binlog) {
  if (m_log_manager.init(opt_consensus_start_index))
    unireg_abort(MYSQLD_ABORT_EXIT);
  m_log_manager.set_binlog(binlog);
}

XLog_engine::~XLog_engine() { m_log_manager.cleanup(); }

Autonomy_xlog_engine::Autonomy_xlog_engine(MYSQL_BIN_LOG *binlog)
    : XLog_engine(binlog),
      m_flusher(std::make_unique<Binary_log_flusher>(&m_log_manager)) {
  m_autonomy = std::make_shared<alisql::Autonomy>();
}

/**
 * Starts the recovery process for the autonomy xlog engine.
 *
 * @return Always returns 0.
 */
int Autonomy_xlog_engine::start_in_recovery(const std::function<int()> &) {
  return 0;
}

/**
 * Starts the autonomy xlog engine after recovery.
 *
 * @return Always returns 0.
 */
int Autonomy_xlog_engine::start_after_recovery() {
  if (opt_bin_log) {
    /*
      Configures what object is used by the current log to store processed
      gtid(s). This is necessary in the MYSQL_BIN_LOG::MYSQL_BIN_LOG to
      correctly compute the set of previous gtids.
    */
    assert(!mysql_bin_log.is_relay_log);
    auto log_guard = create_lock_guard(
        [&] { mysql_mutex_lock(mysql_bin_log.get_log_lock()); },
        [&] { mysql_mutex_unlock(mysql_bin_log.get_log_lock()); });
    if (mysql_bin_log.open_binlog(opt_bin_logname, nullptr, max_binlog_size,
                                  false, true /*need_lock_index=true*/,
                                  true /*need_sid_lock=true*/, nullptr)) {
      return 1;
    }
  }
  return 0;
}

/**
 * Starts apply threads for the autonomy xlog engine.
 */
void Autonomy_xlog_engine::start_apply_threads_after_recovery() {
  ReplicaInitializer replica_initializer(opt_initialize, opt_skip_replica_start,
                                         rpl_channel_filters,
                                         &opt_replica_skip_errors);
}

/**
 * Revises the GTID set from the binlog during server startup.
 */
int Autonomy_xlog_engine::init_gtid_set_after_recovery() {
  if (opt_bin_log && !opt_initialize) {
    /*
      Initialize GLOBAL.GTID_EXECUTED and GLOBAL.GTID_PURGED from
      gtid_executed table and binlog files during server startup.
    */
    Gtid_set *executed_gtids =
        const_cast<Gtid_set *>(gtid_state->get_executed_gtids());
    Gtid_set *lost_gtids = const_cast<Gtid_set *>(gtid_state->get_lost_gtids());
    Gtid_set *gtids_only_in_table =
        const_cast<Gtid_set *>(gtid_state->get_gtids_only_in_table());
    Gtid_set *previous_gtids_logged =
        const_cast<Gtid_set *>(gtid_state->get_previous_gtids_logged());

    Gtid_set purged_gtids_from_binlog(global_sid_map, global_sid_lock);
    Gtid_set gtids_in_binlog(global_sid_map, global_sid_lock);
    Gtid_set gtids_in_binlog_not_in_table(global_sid_map, global_sid_lock);

    if (mysql_bin_log.init_gtid_sets(
            &gtids_in_binlog, &purged_gtids_from_binlog,
            opt_source_verify_checksum, true /*true=need lock*/,
            nullptr /*trx_parser*/, nullptr /*partial_trx*/,
            true /*is_server_starting*/))
      return 1;

    global_sid_lock->wrlock();

    purged_gtids_from_binlog.dbug_print("purged_gtids_from_binlog");
    gtids_in_binlog.dbug_print("gtids_in_binlog");
    komodo::log_gtid_set(&purged_gtids_from_binlog, false,
                         "purged_gtids_from_binlog");
    komodo::log_gtid_set(&gtids_in_binlog, false, "gtids_in_binlog");

    if (!gtids_in_binlog.is_empty() &&
        !gtids_in_binlog.is_subset(executed_gtids)) {
      gtids_in_binlog_not_in_table.add_gtid_set(&gtids_in_binlog);
      if (!executed_gtids->is_empty())
        gtids_in_binlog_not_in_table.remove_gtid_set(executed_gtids);

      komodo::log_gtid_set(&gtids_in_binlog_not_in_table, false,
                           "gtids_in_binlog_not_in_table");

      /*
        Save unsaved GTIDs into gtid_executed table, in the following
        four cases:
          1. the upgrade case.
          2. the case that a slave is provisioned from a backup of
             the master and the slave is cleaned by RESET MASTER
             and RESET SLAVE before this.
          3. the case that no binlog rotation happened from the
             last RESET MASTER on the server before it crashes.
          4. The set of GTIDs of the last binlog is not saved into the
             gtid_executed table if server crashes, so we save it into
             gtid_executed table and executed_gtids during recovery
             from the crash.
      */
      if (gtid_state->save(&gtids_in_binlog_not_in_table) == -1) {
        global_sid_lock->unlock();
        return 1;
      }
      executed_gtids->add_gtid_set(&gtids_in_binlog_not_in_table);
    }

    komodo::log_gtid_set(&gtids_in_binlog, false, "Gtid_in_binlog");
    komodo::log_gtid_set(&gtids_in_binlog_not_in_table, false,
                         "Gtid_in_binlog_not_in_table");

    /* gtids_only_in_table= executed_gtids - gtids_in_binlog */
    if (gtids_only_in_table->add_gtid_set(executed_gtids) != RETURN_STATUS_OK) {
      global_sid_lock->unlock();
      return 1;
    }
    gtids_only_in_table->remove_gtid_set(&gtids_in_binlog);
    /*
      lost_gtids = executed_gtids -
                   (gtids_in_binlog - purged_gtids_from_binlog)
                 = gtids_only_in_table + purged_gtids_from_binlog;
    */
    assert(lost_gtids->is_empty());
    if (lost_gtids->add_gtid_set(gtids_only_in_table) != RETURN_STATUS_OK ||
        lost_gtids->add_gtid_set(&purged_gtids_from_binlog) !=
            RETURN_STATUS_OK) {
      global_sid_lock->unlock();
      return 1;
    }

    /* Prepare previous_gtids_logged for next binlog */
    if (previous_gtids_logged->add_gtid_set(&gtids_in_binlog) !=
        RETURN_STATUS_OK) {
      global_sid_lock->unlock();
      return 1;
    }

    /*
      Write the previous set of gtids at this point because during
      the creation of the binary log this is not done as we cannot
      move the init_gtid_sets() to a place before opening the binary
      log. This requires some investigation.

      /Alfranio
    */
    Previous_gtids_log_event prev_gtids_ev(&gtids_in_binlog);

    global_sid_lock->unlock();

    (prev_gtids_ev.common_footer)->checksum_alg =
        static_cast<enum_binlog_checksum_alg>(binlog_checksum_options);

    if (mysql_bin_log.write_event_to_binlog_and_sync(&prev_gtids_ev))
      return 1;

    // run auto purge member function. It will evaluate auto purge controls
    // and configuration, calculate which log files are to be purged, and
    // if any file is to be purged, it will purge it. This is all encapsulated
    // insiude the auto purge member function.
    //
    // Note on the DBUG_EVALUATE_IF usage below:
    // - when compiling it out: the condition evaluates to true, thus
    //   mysql_bin_log.auto_purge_at_server_startup() runs
    // - when "expire_logs_always_at_start" is set: evaluates to false,
    //   thus mysql_bin_log.purge_logs_before_date() runs
    // - when "expire_logs_always_at_start" is not set: evaluates to true,
    //   this mysql_bin_log.auto_purge_at_server_startup() runs
    if (DBUG_EVALUATE_IF("expire_logs_always_at_start", false, true))
      mysql_bin_log.auto_purge_at_server_startup();
    else if (expire_logs_days > 0 || binlog_expire_logs_seconds > 0)
      mysql_bin_log.purge_logs_before_date(time(nullptr), true);

    (void)RUN_HOOK(server_state, after_engine_recovery, (nullptr));
  }
  return 0;
}

/**
 * rotate binlog
 *
 * @return 0 if succ, 1 otherwise.
 */
int Autonomy_xlog_engine::rotate_consensus_log() {
  bool dont_care = false;
  return mysql_bin_log.rotate(true, &dont_care);
}

/**
 * Gets the protocol for the autonomy xlog engine.
 *
 * @return Pointer to the protocol.
 */
alisql::Protocol *Autonomy_xlog_engine::get_protocol() { return m_autonomy.get(); }

/**
 * Constructor for Consensus_xlog_engine.
 * Initializes consensus-related components and managers.
 *
 * @param meta Pointer to XLog_meta.
 * @param recovery Pointer to Consensus_xlog_recovery.
 * @param binlog Pointer to the MySQL binlog object.
 * @param is_logger Boolean indicating if this is a logger node.
 */
Consensus_xlog_engine::Consensus_xlog_engine(XLog_meta *meta,
                                             Consensus_xlog_recovery *recovery,
                                             MYSQL_BIN_LOG *binlog,
                                             const bool is_logger)
    : XLog_engine(binlog),
      m_meta(meta),
      m_recovery(recovery),
      m_is_logger(is_logger) {
  m_log_manager.set_consensus_logger(is_logger);

  binlog->is_xpaxos_log = true;

  m_log_manager.set_consensus_info(m_meta->get_consensus_info());
  m_log_manager.set_recovery_manager(m_recovery->get_recovery_manager());

  m_prefetch_manager.init(opt_consensus_prefetch_cache_size);
  m_log_manager.set_prefetch_manager(&m_prefetch_manager);

  m_cache_manager = create_fifo_cache(opt_consensus_log_cache_size);
  m_log_manager.set_fifo_cache_manager(m_cache_manager);

  m_log_manager.set_applied_index_checker(&m_applied_index_checker);

  m_state_change_manager.init();

  create_paxos();

  m_flusher = std::make_unique<Consensus_log_flusher>(&m_log_manager, m_paxos,
                                                      m_cache_manager);
}

/**
 * Destructor for Consensus_xlog_engine.
 * Cleans up consensus-related components and managers.
 */
Consensus_xlog_engine::~Consensus_xlog_engine() {
  /* set apply_catchup to true to skip wait_replay_log_finished */
  m_log_manager.set_apply_catchup(true);
  m_state_change_manager.cleanup();
  m_log_manager.cleanup();
  m_prefetch_manager.cleanup();
  delete m_cache_manager;
}

/**
 * Waits for the commit index to reach the required value during recovery.
 */
void Consensus_xlog_engine::wait_commit_index_in_recovery() {
  uint64 max_consensus_index = 0;

  xp::system(ER_XP_RECOVERY)
      << "wait_commit_index_in_recovery begin"
      << ", current commitIndex " << m_paxos->getCommitIndex();

  while (m_paxos->getCommitIndex() <
         (max_consensus_index =
              m_recovery->get_recovery_manager()
                  ->get_max_consensus_index_from_pending_recovering_trxs())) {
    my_sleep(500);
  }

  xp::system(ER_XP_RECOVERY)
      << "Found max consensus index from pending_recovering_trxs"
      << ", current commitIndex " << m_paxos->getCommitIndex()
      << ", max_consensus_index " << max_consensus_index;

  /*
    If the node was leader when shutdown, it should wait for binlog truncation
    completed to get the corrent binlog end position before applying the lost
    binlog events on recovery.
  */
  auto recover_status = m_meta->get_consensus_info()->get_recover_status();
  auto start_apply_index =
      m_meta->get_consensus_info()->get_start_apply_index();

  if (recover_status == BINLOG_WORKING && start_apply_index == 0) {
    while (m_paxos->getCommitIndex() <
           m_recovery->get_recovery_manager()->get_last_leader_term_index()) {
      my_sleep(500);
    }
  }

  xp::system(ER_XP_RECOVERY)
      << "wait_commit_index_in_recovery end"
      << ", recover_status " << recover_status << ", start_apply_index "
      << start_apply_index << ", current commitIndex "
      << m_paxos->getCommitIndex() << ", last_leader_term_index "
      << m_recovery->get_recovery_manager()->get_last_leader_term_index();
}

/**
 * Starts the consensus xlog engine in recovery mode.
 *
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::start_in_recovery(const std::function<int()> &create_pid_file) {
  if (m_log_manager.check_empty_binlog()
      || m_log_manager.create_rpl_info()
      || m_log_manager.dump_consensus_info()
      || create_pid_file()
      || m_log_manager.check_force_change_meta(m_meta->force_learner()))
    return 1;

  m_xlog_handler->setMockStartIndex(m_log_manager.get_mock_start_index(opt_consensus_start_index));

  const bool is_learner =
      m_meta->get_consensus_info()->get_cluster_info().empty() ||
      m_meta->get_consensus_info()->get_cluster_info().find('@') ==
          std::string::npos;

  if (start_paxos(is_learner) ||
      m_log_manager.start_consensus_commit_pos_watcher())
    return 1;

  wait_commit_index_in_recovery();

  if (handle_pending_recovering_trx()) return 1;

  return 0;
}

/**
 * Starts the consensus xlog engine after recovery.
 *
 * @return Always returns 0.
 */
int Consensus_xlog_engine::start_after_recovery() {
  return 0;
}

/**
 * Sets privilege checks user for a relay log info object.
 *
 * @param rpl_info The relay log info object.
 */
static void do_set_privilege_checks_user(Relay_log_info *rpl_info) {
  if (strlen(rpl_info->consensus_privilege_checks_username) > 0 ||
      strlen(rpl_info->consensus_privilege_checks_hostname) > 0) {
    xp::info(ER_XP_RECOVERY)
        << "do set_privilege_checks_user for"
        << " channel: " << rpl_info->get_channel()
        << ", username: " << rpl_info->consensus_privilege_checks_username
        << ", hostname: " << rpl_info->consensus_privilege_checks_hostname;
    Relay_log_info::enum_priv_checks_status status =
        rpl_info->set_privilege_checks_user(
            rpl_info->consensus_privilege_checks_username,
            rpl_info->consensus_privilege_checks_hostname);
    if (!!status) {
      rpl_info->set_privilege_checks_user_corrupted(true);
      rpl_info->report_privilege_check_error(
          WARNING_LEVEL, status, false, rpl_info->get_channel(),
          rpl_info->consensus_privilege_checks_username,
          rpl_info->consensus_privilege_checks_hostname);
    }
    rpl_info->consensus_privilege_checks_username[0] = '\0';
    rpl_info->consensus_privilege_checks_hostname[0] = '\0';
  }
}

/**
 * Sets privilege checks user for all configured channels.
 */
static void rpl_set_privilege_checks_user() {
  Master_info *mi = NULL;
  channel_map.rdlock();
  for (mi_map::iterator it = channel_map.begin(); it != channel_map.end();
        it++) {
    mi = it->second;
    if (Master_info::is_configured(mi) && mi->rli->inited) {
      xp::info(ER_XP_RECOVERY)
          << "set_privilege_checks_user for channel '" << mi->get_channel() << "'";
      do_set_privilege_checks_user(mi->rli);
    }
  }
  channel_map.unlock();
}

/**
 * Starts apply threads for the consensus xlog engine.
 */
void Consensus_xlog_engine::start_apply_threads_after_recovery() {
  rpl_set_privilege_checks_user();

  if (!m_is_logger) start_consensus_apply_threads();
}

/**
 * Handles pending recovering transactions.
 *
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::handle_pending_recovering_trx() {
  if (!m_is_logger) {
    m_log_manager.get_recovery_manager()->recover_remaining_pending_recovering_trxs();
    m_log_manager.get_recovery_manager()->clear();
  } else {
    assert(m_log_manager.get_recovery_manager()->is_pending_recovering_trx_empty());
  }
  return 0;
}

/**
 * Revises the GTID set from the binlog for consensus xlog engine.
 * (No operation for consensus engine.)
 */
int Consensus_xlog_engine::init_gtid_set_after_recovery() {
  /*
    Initialize GLOBAL.GTID_EXECUTED and GLOBAL.GTID_PURGED from
    gtid_executed table and binlog files during server startup.
  */
  Gtid_set *executed_gtids =
      const_cast<Gtid_set *>(gtid_state->get_executed_gtids());
  Gtid_set *lost_gtids = const_cast<Gtid_set *>(gtid_state->get_lost_gtids());
  Gtid_set *gtids_only_in_table =
      const_cast<Gtid_set *>(gtid_state->get_gtids_only_in_table());

  Gtid_set purged_gtids_from_binlog(global_sid_map, global_sid_lock);
  Gtid_set gtids_in_binlog(global_sid_map, global_sid_lock);

  if (mysql_bin_log.init_gtid_sets(
          &gtids_in_binlog, &purged_gtids_from_binlog,
          opt_source_verify_checksum, true /*true=need lock*/,
          nullptr /*trx_parser*/, nullptr /*partial_trx*/,
          true /*is_server_starting*/))
    return 1;

  global_sid_lock->wrlock();

  komodo::log_gtid_set(&purged_gtids_from_binlog, false, "purged_gtids_from_binlog_1");
  komodo::log_gtid_set(&gtids_in_binlog, false, "gtids_in_binlog_1");
  komodo::log_gtid_set(executed_gtids, false, "executed_gtids");
  komodo::log_gtid_set(lost_gtids, false, "lost_gtids_0");
  komodo::log_gtid_set(gtids_only_in_table, false, "gtids_only_in_table_0");

  /* gtids_only_in_table= executed_gtids - gtids_in_binlog */
  if (gtids_only_in_table->add_gtid_set(executed_gtids) != RETURN_STATUS_OK) {
    global_sid_lock->unlock();
    return 1;
  }
  gtids_only_in_table->remove_gtid_set(&gtids_in_binlog);

  komodo::log_gtid_set(gtids_only_in_table, false, "gtids_only_in_table_1");
  /*
    lost_gtids = executed_gtids -
                  (gtids_in_binlog - purged_gtids_from_binlog)
                = gtids_only_in_table + purged_gtids_from_binlog;
  */
  assert(lost_gtids->is_empty());
  if (lost_gtids->add_gtid_set(gtids_only_in_table) != RETURN_STATUS_OK ||
      lost_gtids->add_gtid_set(&purged_gtids_from_binlog) !=
          RETURN_STATUS_OK) {
    global_sid_lock->unlock();
    return 1;
  }
  komodo::log_gtid_set(lost_gtids, false, "lost_gtids_1");

  global_sid_lock->unlock();

  //Consensus startup as follower role default,
  //use Relay_log_info instead of Binlog, so we close binlog here
  if (opt_bin_log && !opt_initialize) {
    mysql_bin_log.close(LOG_CLOSE_INDEX | LOG_CLOSE_TO_BE_OPENED, true, true);
  }
  return 0;
}

/**
 * Shuts down the consensus xlog engine and Paxos protocol.
 */
void Consensus_xlog_engine::shutdown() {
  m_log_manager.stop_consensus_commit_pos_watcher();
  assert(m_paxos);
  m_paxos->shutdown();
}

/**
 * Adds a log file entry to the log file index.
 *
 * @param timestamp The timestamp of the log file.
 * @param log_file_name The name of the log file.
 */
void Consensus_xlog_engine::add_into_log_file_index(const uint64_t timestamp,
                                                    const char *log_file_name) {
  std::string file_name(log_file_name);
  m_log_manager.get_log_file_index()->add_to_index_list(
      m_log_manager.get_current_index(), timestamp, file_name);
}

/**
 * Truncates log file index entries before the specified log file.
 *
 * @param log_file_name The name of the log file.
 */
void Consensus_xlog_engine::truncate_before_from_log_file_index(
    const char *log_file_name) {
  std::string file_name(log_file_name);
  m_log_manager.get_log_file_index()->truncate_before(file_name);
  xp::system(ER_XP_0) << "binlog will be purged before " << file_name;
}

/**
 * Gets the safety index for paxos log purging.
 *
 * @return The safety index.
 */
uint64_t Consensus_xlog_engine::get_safety_index_for_purge() {
  assert(m_paxos);
  return m_paxos->getSafetyIndexForPurge();
}

/**
 * Updates the apply thread running status in Paxos.
 *
 * @param is_running True if the apply thread is running, false otherwise.
 */
void Consensus_xlog_engine::update_apply_thread_running(bool is_running) {
  assert(m_paxos);
  return m_paxos->updateApplyThreadRunning(is_running);
}

/**
 * Updates the apply delay seconds in Paxos.
 *
 * @param sec The delay in seconds.
 */
void Consensus_xlog_engine::update_apply_delay_seconds(uint64_t sec) {
  assert(m_paxos);
  return m_paxos->updateApplyDelaySeconds(sec);
}

/**
 * Replicates an empty log entry in Paxos.
 */
void Consensus_xlog_engine::replicate_empty_log() {
  m_paxos->replicateEmptyLog();
}

/**
 * Gets the current term from Paxos.
 *
 * @return The current term.
 */
uint64_t Consensus_xlog_engine::get_current_paxos_term() {
  assert(m_paxos);
  alisql::LogEntry entry;
  // use fake buf_size, tell consenesus layer, the entry is not noop
  Consensus_xlog_handler::packLogEntry((uchar *)const_cast<char *>("any"), 3, 0, 0,
                               Consensus_xlog_handler::UNCERTAIN, entry);
  m_paxos->replicateLog(entry);
  return entry.term();
}

/**
 * check is leader but not single node
 */
bool Consensus_xlog_engine::is_leader_not_single() const {
  return m_paxos->isLeaderNotSingle();
}

/**
 * Prepares the applied index checker with the current commit queue.
 *
 * @param final_queue The commit queue.
 */
void Consensus_xlog_engine::applied_index_check_prepare(THD *final_queue) {
  // protected by LOCK_sync
  if (opt_enable_appliedindex_checker) {
    uint64 maxi = 0, mini = UINT64_MAX, size = 0;
    for (THD *head = final_queue; head; head = head->next_to_commit) {
      if (head->consensus_index == 0) continue;
      maxi = std::max(head->consensus_index, maxi);
      mini = std::min(head->consensus_index, mini);
      size++;
    }
    if (maxi > 0) {
      intervalType inv{maxi, mini, size};
      m_applied_index_checker.prepare(inv);
    }
  }
}

/**
 * Creates and initializes the Paxos protocol.
 */
void Consensus_xlog_engine::create_paxos() {
  m_xlog_handler = std::make_shared<Consensus_xlog_handler>(
      *m_meta, m_log_manager, m_switchover_manager);
  m_paxos = std::make_shared<alisql::Paxos >(
      opt_consensus_election_timeout, m_xlog_handler);

  m_paxos->setStateChangeCb([&](alisql::Paxos::StateType state, uint64_t term,
                                uint64_t commit_index) {
    m_state_change_manager.add_state_change_request(state, term, commit_index);
  });
  m_paxos->setMaxPacketSize(opt_consensus_max_packet_size);
  m_paxos->setPipeliningTimeout(opt_consensus_pipelining_timeout);
  m_paxos->setLargeBatchRatio(opt_consensus_large_batch_ratio);
  m_paxos->setMaxDelayIndex(opt_consensus_max_delay_index);
  m_paxos->setMinDelayIndex(opt_consensus_min_delay_index);
  m_paxos->setSyncFollowerMetaInterval(
      opt_consensus_sync_follower_meta_interval);
  m_paxos->setConsensusAsync(opt_weak_consensus_mode);
  m_paxos->setReplicateWithCacheLog(opt_consensus_replicate_with_cache_log);
  m_paxos->setCompactOldMode(opt_consensus_old_compact_mode);
  // todo keep IS log level sync with MySQL log
  m_paxos->setAlertLogLevel(
      alisql::Paxos::AlertLogLevel(opt_consensus_log_level + 3));
  m_paxos->setForceSyncEpochDiff(opt_consensus_force_sync_epoch_diff);
  m_paxos->setConfigureChangeTimeout(opt_consensus_configure_change_timeout);
  m_paxos->setMaxDelayIndex4NewMember(opt_consensus_new_follower_threshold);
  m_paxos->setMaxDelaySeconds4NewLeader(
      opt_consensus_new_leader_max_apply_delay_seconds);
  m_paxos->setEnableDynamicEasyIndex(opt_consensus_dynamic_easyindex);
  m_paxos->setEnableLearnerPipelining(opt_consensus_learner_pipelining);
  m_paxos->setEnableLearnerHeartbeat(opt_consensus_learner_heartbeat);
  m_paxos->setWeakReadRefreshTimeout(opt_consensus_weak_read_refresh_timeout);
  m_paxos->setEnableAutoResetMatchIndex(opt_consensus_auto_reset_match_index);
  m_paxos->setEnableAutoLeaderTransfer(opt_consensus_auto_leader_transfer);
  m_paxos->setServerIp(opt_consensus_server_ip);
  m_paxos->setServerPort(opt_consensus_server_port);
  m_paxos->setAutoLeaderTransferCheckSeconds(
      opt_consensus_auto_leader_transfer_check_seconds);
  m_paxos->setThreadHook([]() { my_thread_init(); }, my_thread_end);
  m_paxos->setLogInstance(m_is_logger);
  m_paxos->setMaxPipelineEntrySize(opt_consensus_max_pipeline_entry_size);
}

/**
 * Initializes and starts the Paxos protocol for consensus.
 *
 * @param is_learner If true, starts the node as a learner; otherwise, starts as
 * a normal node.
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::start_paxos(const bool is_learner) {
  if (!is_learner) {
    std::vector<std::string> cluster_str_config;
    // startup as normal node
    if (m_paxos->init(cluster_str_config, 0, ::server_id, NULL,
                      opt_consensus_io_thread_cnt,
                      opt_consensus_worker_thread_cnt, nullptr,
                      opt_consensus_easy_pool_size,
                      opt_consensus_heartbeat_thread_cnt))
      return 1;
  } else {
    std::string empty_str;
    // startup as learner node, config string arg pass empty
    if (m_paxos->initAsLearner(
            empty_str, ::server_id, NULL, opt_consensus_io_thread_cnt,
            opt_consensus_worker_thread_cnt, nullptr,
            opt_consensus_easy_pool_size, opt_consensus_heartbeat_thread_cnt))
      return 1;
  }
  m_paxos->setAsLogType(m_is_logger);
  m_paxos->initAutoPurgeLog(false, false, NULL);  // disable autoPurge
  m_paxos->applyConf(opt_consensus_election_timeout,
                     opt_consensus_heartbeat_interval,
                     opt_consensus_vote_backoff_timeout,
                     opt_consensus_send_timeout,
                     opt_consensus_connect_timeout,
                     opt_consensus_force_promote);

  if (opt_cluster_force_single_mode)  // use nuclear weapon
    m_paxos->forceSingleLeader();

  m_paxos->waitCommitIndexUpdate(0);
  return 0;
}

/**
 * Checks if a command should be blocked based on consensus state and privileges.
 *
 * @param thd The thread handler.
 * @return True if the command should be blocked, false otherwise.
 */
bool Consensus_xlog_engine::need_block_command(THD *thd) {
  assert(m_paxos);

  /* rw check: leader read-write, others read-only */
  bool reject_query = false;
  bool is_leader = false;
  DBUG_ENTER("need_block_command");
  // dd_upgrade execute inner sql before consensus module startup

  if (m_paxos->getState() == alisql::Paxos::LEADER) {
    is_leader = true;
    if (m_paxos->getTerm() != m_log_manager.get_current_term())
      reject_query = true;
  } else {
    if (invalid_on_consensus_limited(thd->lex->sql_command,
                                     thd->lex->no_write_to_binlog))
      reject_query = true;
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
      if (!m_is_logger) {
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
  if (m_is_logger && invalid_on_logger_limited(thd->lex->sql_command) &&
      thd->variables.opt_force_revise == false) {
    my_error(ER_CONSENSUS_LOG_TYPE_NODE, MYF(0));
    DBUG_RETURN(1);
  }

  DBUG_RETURN(0);
}

/**
 * Updates the applied index during commit.
 *
 * @param index The index to update.
 */
void Consensus_xlog_engine::update_applied_index_during_commit(uint64_t index) {
  assert(m_paxos);
  if (opt_enable_appliedindex_checker) {
    m_applied_index_checker.commit(index);
  } else {
    update_applied_index(m_paxos->getCommitIndex());
  }
}


/**
 * Updates the logger's applied index.
 *
 * @param index The index to update.
 */
void Consensus_xlog_engine::update_logger_applied_index(uint64_t index) {
  assert(m_paxos);
  if (m_is_logger) {
    m_paxos->updateAppliedIndex(index);
    replica_read_manager.update_lsn(index);
  }
}

void print_log_for_wait_commit(
    const THD *thd, const Consensus_info *consensus_info,
    const std::shared_ptr<alisql::Paxos> paxos) {
   xp::warn(ER_XP_COMMIT)	
        << "Failed to commit from shutdown or leadership changed"
        << ", system current term:" << consensus_info->get_current_term()	
        << ", system apply index:" << consensus_info->get_start_apply_index()	
        << ", thd consensus term:" << thd->consensus_term	
        << ", thd consensus index:" << thd->consensus_index	
        << ", isShutdown:" << paxos->isShutdown()	
        << ", sql_command:" << thd->lex->sql_command;
}

/**
 * Waits for the commit index to be updated for the given thread.
 *
 * @param thd The thread handler.
 */
void Consensus_xlog_engine::wait_commit(THD *thd) {
  assert(m_paxos);

  auto *consensus_info = m_meta->get_consensus_info();
  if (thd->commit_error == THD::CE_NONE &&
      thd->consensus_error == THD::CSS_NONE &&
      (DBUG_EVALUATE_IF("force_in_wait_commit", true, false) ||
       (m_paxos->waitCommitIndexUpdate(thd->consensus_index - 1,
                                       thd->consensus_term) <
            thd->consensus_index &&
        thd->consensus_index > consensus_info->get_start_apply_index()))) {

    print_log_for_wait_commit(thd, consensus_info, m_paxos);

    // skip commit
    thd->get_transaction()->m_flags.commit_low = false;
    if (m_paxos->isShutdown())
      thd->consensus_error = THD::CSS_SHUTDOWN;
    else
      thd->consensus_error = THD::CSS_LEADERSHIP_CHANGED;
    thd->commit_error = THD::CE_COMMIT_ERROR;

    exec_binlog_error_action_exit(
        "Waiting for consensus majority commit failed, restart to deal with "
        "it");
  }
}

void print_log_for_rollback_with_flush_nothing(const THD *thd) {
  xp::warn(ER_XP_COMMIT)	      
          << "xa commit/rollback fail, restart to deal with it"	          
          << ", commit_error: " << thd->commit_error	          
          << ", consensus_error: " << thd->consensus_error	          
          << ", sql_command: " << thd->lex->sql_command << ", xid "	          
          << *thd->get_transaction()->xid_state()->get_xid();	
}

/**
 * Rolls back a transaction and flushes nothing if a consensus error occurs.
 *
 * @param thd The thread handler.
 * @param is_my_thd True if this is the current thread.
 */
void Consensus_xlog_engine::rollback_with_flush_nothing(THD *thd,
                                                        bool is_my_thd) {
  /*
    If xa commit error, we can not simply roll back the trx of this THD.
    Because the trx is prepared by 'xa prepare' and its consensus index
    may reach a majority in the cluster. It must not be rolled back.
    Currently, we crash the server to handle the consistency by recovery.
  */
  if (thd->consensus_error != THD::CSS_NONE) {
    if (thd->get_transaction()->xid_state()->has_state(
            XID_STATE::XA_PREPARED)) {
      print_log_for_rollback_with_flush_nothing(thd);
      flush_error_log_messages();
      exec_binlog_error_action_exit(
          "xa commit/rollback fail, restart to deal with it");
    }

    if (thd->get_transaction()->m_flags.commit_low)
      trx_coordinator::rollback_in_engines(
          thd, thd->get_transaction()->m_flags.real_commit);
    gtid_state->update_on_rollback(thd);
    reset_binlog_cache_mngr(thd);
    if (thd->get_stmt_da()->is_ok())
      thd->get_stmt_da()->reset_diagnostics_area();
    if (is_my_thd)
      my_error(THD::Consensus_error_code[thd->consensus_error], MYF(0));
  }
}

/**
 * Waits for the commit index to be updated to the specified value.
 * checkCommitIndex to make sure only send committed log
 *
 * @param thd                  The current thread handler.
 * @param log_pos              The log position to wait for.
 * @param index                The target commit index.
 * @param heartbeat_period     The period for sending heartbeat events.
 * @param packet               The packet used for communication.
 * @param flush_net            Callback to flush the network buffer.
 * @param send_heartbeat_event Callback to send a heartbeat event with a given log position.
 * @return                     0 on success, 1 on failure.
 */
bool Consensus_xlog_engine::wait_commit_index_update(
    const THD *thd, my_off_t log_pos, uint64_t index,
    std::chrono::nanoseconds heartbeat_period, String &packet,
    const std::function<int()> &flush_net,
    const std::function<int(my_off_t)> &send_heartbeat_event) {
  assert(m_paxos);

#ifndef NDEBUG
  ulong hb_info_counter = 0;
#endif
  longlong sec_cnt = 0;
  bool hb_send = false;
  String tmp;
  /* heartbeat period is measured by second */
  uint ratio =
      std::max(1000000ULL / opt_consensus_check_commit_index_interval, 1ULL);
  // TODO@yanhua, need use strong check like
  // check_exec_consensus_log_end_condition
  while (m_paxos->checkCommitIndex(m_log_manager.get_current_term()) < index) {
    if (unlikely(flush_net())) return 1;

    my_sleep(opt_consensus_check_commit_index_interval);
    if (unlikely(thd->killed)) return 1;
    // send heartbeat event
    if (heartbeat_period.count() > 0) {
      sec_cnt++;
      if (heartbeat_period.count() == (sec_cnt / ratio)) {
        sec_cnt = 0;
#ifndef NDEBUG
        if (hb_info_counter < 3) {
          xp::info(ER_XP_APPLIER) << "master sends heartbeat message";
          hb_info_counter++;
          if (hb_info_counter == 3)
            xp::info(ER_XP_APPLIER) << "the rest of heartbeat info skipped ...";
        }
#endif
        if (!hb_send) {
          /* Save a copy of the buffer content. */
          tmp.copy(packet);
          tmp.length(packet.length());
          hb_send = true;
        }

        if (send_heartbeat_event(log_pos)) return 1;
      }
    }
  }
  if (hb_send) {
    /* Restore the copy back. */
    packet.copy(tmp);
    packet.length(tmp.length());
  }
  assert(index <= m_paxos->getCommitIndex());
#ifndef NDEBUG
  xp::info(ER_XP_APPLIER) << "master sends consensus index " << index;
#endif

  return 0;
}

/**
 * Refreshes the learner metadata in Paxos.
 *
 * @param config The configuration for learners.
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::refresh_learner_meta(
    std::vector<std::string> &config) {
  assert(m_paxos);
  return m_paxos->changeLearners(alisql::Paxos::CCSyncLearnerAll, config);
}

/**
 * Adds a learner node to the cluster.
 *
 * @param config The configuration for the learner.
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::add_learner(std::vector<std::string> &config) {
  assert(m_paxos);
  return m_paxos->changeLearners(alisql::Paxos::CCOpType::CCAddNode, config);
}

/**
 * Drops a learner node from the cluster.
 *
 * @param config The configuration for the learner.
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::drop_learner(std::vector<std::string> &config) {
  assert(m_paxos);
  return m_paxos->changeLearners(alisql::Paxos::CCOpType::CCDelNode, config);
}

/**
 * Forces the cluster to have a single leader.
 *
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::force_single_leader() {
  assert(m_paxos);
  return m_paxos->forceSingleLeader();
}

/**
 * Forces the cluster to have a single learner.
 *
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::force_single_learner() {
  assert(m_paxos);
  return m_paxos->forceSingleLearner();
}

/**
 * Forces the purge of logs up to the specified index.
 *
 * @param local Whether to purge locally.
 * @param index The index up to which logs should be purged.
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::force_purge_log(bool local, uint64_t index) {
  assert(m_paxos);
  return m_paxos->forcePurgeLog(local, index);
}

/**
 * Forces fixing the match index for a target node.
 *
 * @param target_id The target node ID.
 * @param index The index to set.
 */
void Consensus_xlog_engine::force_fix_match_index(uint64_t target_id,
                                                  uint64_t index) {
  assert(m_paxos);
  m_paxos->forceFixMatchIndex(target_id, index);
}

/**
 * Initiates a leader change to the specified node.
 *
 * @param node_id The ID of the node to become leader.
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::change_leader(uint64_t node_id) {
  assert(m_paxos);

  int err = m_paxos->leaderTransferPrecheck(node_id);

  DBUG_EXECUTE_IF("force_leadertransferprecheck_succ",
    err = alisql::PaxosErrorCode::PE_DEFAULT;);

  if (err == alisql::PaxosErrorCode::PE_DEFAULT) {
    xp::system(ER_XP_0) << "begin leaderTransfer to server " << node_id;
    m_switchover_manager.set_limit_new_trx();
    m_paxos->resetLocalHaInfo();

    auto wait_time_ms = m_switchover_manager.wait_old_trx_finish();
    m_paxos->addBeforeRequestVote(wait_time_ms);

    err = m_paxos->leaderTransfer(node_id);
  }
  return err;
}

/**
 * Adds a follower node to the cluster.
 *
 * @param config The configuration for the follower.
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::add_follower(std::string &config) {
  assert(m_paxos);
  return m_paxos->changeMember(alisql::Paxos::CCOpType::CCAddLearnerAutoChange,
                               config);
}

/**
 * Upgrades a learner node to a follower.
 *
 * @param config The configuration for the learner.
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::upgrade_learner(std::string &config) {
  assert(m_paxos);
  return m_paxos->changeMember(alisql::Paxos::CCOpType::CCAddNode, config);
}

/**
 * Configures a cluster member.
 *
 * @param server_id The server ID.
 * @param force_sync Whether to force synchronization.
 * @param election_weight The election weight.
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::configure_member(uint64_t server_id,
                                             bool force_sync,
                                             uint election_weight) {
  assert(m_paxos);
  return m_paxos->configureMember(server_id, force_sync, election_weight);
}

/**
 * Configures multiple cluster members.
 *
 * @param server_ids The server IDs.
 * @param force_syncs The force sync flags.
 * @param election_weights The election weights.
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::configure_members(
    std::vector<uint64_t> &server_ids, std::vector<bool> &force_syncs,
    std::vector<uint> &election_weights) {
  assert(m_paxos);
  return m_paxos->configureMembers(server_ids, force_syncs, election_weights);
}

/**
 * Configures a learner node.
 *
 * @param server_id The server ID.
 * @param source_id The source ID.
 * @param apply_mode The apply mode flag.
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::configure_learner(uint64_t server_id,
                                              uint64_t source_id,
                                              bool apply_mode) {
  assert(m_paxos);
  return m_paxos->configureLearner(server_id, source_id, apply_mode);
}

/**
 * Downgrades a member node to a learner.
 *
 * @param addr The address of the node.
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::downgrade_member(const std::string &addr) {
  assert(m_paxos);
  return m_paxos->downgradeMember(addr);
}

/**
 * Gets the size of the cluster.
 *
 * @return The number of nodes in the cluster.
 */
uint64_t Consensus_xlog_engine::get_cluster_size() {
  assert(m_paxos);
  return m_paxos->getClusterSize();
}

/**
 * Sets the cluster ID.
 *
 * @param id The cluster ID.
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::set_cluster_id(uint64_t id) {
  assert(m_paxos);
  return m_paxos->setClusterId(id);
}

/**
 * Resets the event scheduler to its configured state.
 */
void Consensus_xlog_engine::reset_event_scheduler() {
  opt_configured_event_scheduler = Events::opt_event_scheduler;
  Events::opt_event_scheduler = Events::EVENTS_OFF;
}

/**
 * Retrieves cluster information and populates the results vector.
 *
 * @param results The vector to store global result pointers.
 * @param mem_root The memory root for allocation.
 */
void Consensus_xlog_engine::get_cluster_global_info(
    std::vector<im::Consensus_show_global_result *> &results,
    MEM_ROOT *mem_root) {
  assert(m_paxos);

  alisql::Paxos::MemberInfoType member_info;
  std::vector<alisql::Paxos::ClusterInfoType> cluster_info;

  m_paxos->getMemberInfo(&member_info);
  /* if node is not LEADER, global information is empty */
  if (member_info.role != alisql::Paxos::StateType::LEADER) {
    return;
  }
  m_paxos->getClusterInfo(cluster_info);

  for (auto &ci : cluster_info) {
    im::Consensus_show_global_result *result =
        new (mem_root) im::Consensus_show_global_result();
    result->id = ci.serverId;
    result->ip_port.str =
        strmake_root(mem_root, ci.ipPort.c_str(), ci.ipPort.length());
    result->ip_port.length = ci.ipPort.length();
    result->match_index = ci.matchIndex;
    result->next_index = ci.nextIndex;
    const char *res = NULL;
    switch (ci.role) {
      case alisql::Paxos::StateType::FOLLOWER:
        res = "Follower";
        break;
      case alisql::Paxos::StateType::CANDIDATE:
        res = "Candidate";
        break;
      case alisql::Paxos::StateType::LEADER:
        res = "Leader";
        break;
      case alisql::Paxos::StateType::LEARNER:
        res = "Learner";
        break;
      default:
        res = "No Role";
        break;
    }
    result->role.str = res;
    result->role.length = strlen(res);
    switch (ci.hasVoted) {
      case 1:
        res = "Yes";
        break;
      case 0:
        res = "No";
        break;
    }
    result->has_voted.str = res;
    result->has_voted.length = strlen(res);
    if (ci.forceSync)
      res = "Yes";
    else
      res = "No";
    result->force_sync.str = res;
    result->force_sync.length = strlen(res);
    result->election_weight = ci.electionWeight;
    result->applied_index = ci.appliedIndex;
    result->learner_source = ci.learnerSource;
    if (ci.pipelining)
      res = "Yes";
    else
      res = "No";
    result->pipelining.str = res;
    result->pipelining.length = strlen(res);
    if (ci.useApplied)
      res = "Yes";
    else
      res = "No";
    result->send_applied.str = res;
    result->send_applied.length = strlen(res);

    res = ci.logInstance ? "Log" : "Normal";
    result->instance_type.str = res;
    result->instance_type.length = strlen(res);

    res = ci.disableElection ? "Yes" : "No";
    result->disable_election.str = res;
    result->disable_election.length = strlen(res);
    result->server_ip.str =
        strmake_root(mem_root, ci.serverIp.c_str(), ci.serverIp.length());
    result->server_ip.length = ci.serverIp.length();
    result->server_port = ci.serverPort;

    results.push_back(result);
  }
}

/**
 * Retrieves local node information and populates the result structure.
 *
 * @param result The structure to store local result.
 * @param mem_root The memory root for allocation.
 */
void Consensus_xlog_engine::get_local_info(
    im::Consensus_show_local_result &result, MEM_ROOT *mem_root) {
  assert(m_paxos);

  const bool apply_thread_running = m_paxos->getApplyThreadRunning();
  const bool is_relay_working =
      (m_log_manager.get_status() ==
       Consensus_Log_State::RELAY_LOG_WORKING);

  alisql::Paxos::MemberInfoType mi;
  m_paxos->getMemberInfo(&mi);

  const char *res = nullptr;
  result.id = mi.serverId;
  result.current_term = mi.currentTerm;
  result.current_leader.str = strmake_root(
      mem_root, mi.currentLeaderAddr.c_str(), mi.currentLeaderAddr.length());
  result.current_leader.length = mi.currentLeaderAddr.length();
  result.commit_index = mi.commitIndex;
  result.last_log_term = mi.lastLogTerm;
  result.last_log_index = mi.lastLogIndex;
  switch (mi.role) {
    case alisql::Paxos::StateType::FOLLOWER:
      res = "Follower";
      break;
    case alisql::Paxos::StateType::CANDIDATE:
      res = "Candidate";
      break;
    case alisql::Paxos::StateType::LEADER:
      /* not a stable leader if server_ready_for_rw is no and
       * consensus_auto_leader_transfer is on */
      if (opt_consensus_auto_leader_transfer &&
          (m_is_logger || is_relay_working))
        res = "Prepared";
      else
        res = "Leader";
      break;
    case alisql::Paxos::StateType::LEARNER:
      res = "Learner";
      break;
    default:
      res = "No Role";
      break;
  }
  result.role.str = res;
  result.role.length = strlen(res);
  result.vote_for = mi.votedFor;
  result.applied_index = mi.lastAppliedIndex;

  if (m_is_logger || is_relay_working) {
    res = "No";
  } else {
    res = "Yes";
  }
  result.server_ready_for_rw.str = res;
  result.server_ready_for_rw.length = strlen(res);
  if (m_is_logger)
    res = "Log";
  else
    res = "Normal";
  result.instance_type.str = res;
  result.instance_type.length = strlen(res);

  res = opt_consensus_disable_election ? "Yes" : "No";
  result.disable_election.str = res;
  result.disable_election.length = strlen(res);

  res = apply_thread_running ? "Yes" : "No";
  result.apply_running.str = res;
  result.apply_running.length = strlen(res);

  result.leader_ip.str =
      strmake_root(mem_root, mi.leaderIp.c_str(), mi.leaderIp.length());
  result.leader_ip.length = mi.leaderIp.length();
  result.leader_port = mi.leaderPort;

  return;
}

/**
 * Fills the cluster info table with current cluster information.
 *
 * @param thd The thread handler.
 * @param table The table to fill.
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::fill_cluster_info(THD *thd, TABLE *table) {
  std::vector<im::Consensus_show_global_result *> results;
  get_cluster_global_info(results, thd->mem_root);

  if (results.empty()) {
    return schema_table_store_record(thd, table);
  }

  for (auto &res : results) {
    int field_num = 0;
    table->field[field_num++]->store((longlong)res->id, true);
    table->field[field_num++]->store(res->ip_port.str, res->ip_port.length,
                                     system_charset_info);
    table->field[field_num++]->store((longlong)res->match_index, true);
    table->field[field_num++]->store((longlong)res->next_index, true);
    table->field[field_num++]->store(res->role.str, res->role.length,
                                     system_charset_info);
    table->field[field_num++]->store(res->has_voted.str, res->has_voted.length,
                                     system_charset_info);
    table->field[field_num++]->store(
        res->force_sync.str, res->force_sync.length, system_charset_info);
    table->field[field_num++]->store((longlong)res->election_weight, true);
    table->field[field_num++]->store((longlong)res->learner_source, true);
    table->field[field_num++]->store((longlong)res->applied_index, true);
    table->field[field_num++]->store(
        res->pipelining.str, res->pipelining.length, system_charset_info);
    table->field[field_num++]->store(
        res->send_applied.str, res->send_applied.length, system_charset_info);
    table->field[field_num++]->store(
        res->instance_type.str, res->instance_type.length, system_charset_info);
    table->field[field_num++]->store(res->disable_election.str,
                                     res->disable_election.length,
                                     system_charset_info);
    table->field[field_num++]->store(res->server_ip.str, res->server_ip.length,
                                     system_charset_info);
    table->field[field_num++]->store((longlong)res->server_port, true);
    if (schema_table_store_record(thd, table)) return 1;
  }
  return 0;
}

/**
 * Fills the local info table with current node information.
 *
 * @param thd The thread handler.
 * @param table The table to fill.
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::fill_local_info(THD *thd, TABLE *table) {
  im::Consensus_show_local_result result;
  get_local_info(result, thd->mem_root);

  int field_num = 0;
  table->field[field_num++]->store((longlong)result.id, true);
  table->field[field_num++]->store((longlong)result.current_term, true);
  table->field[field_num++]->store(result.current_leader.str,
                                   result.current_leader.length,
                                   system_charset_info);
  table->field[field_num++]->store((longlong)result.commit_index, true);
  table->field[field_num++]->store((longlong)result.last_log_term, true);
  table->field[field_num++]->store((longlong)result.last_log_index, true);
  table->field[field_num++]->store(result.role.str, result.role.length,
                                   system_charset_info);
  table->field[field_num++]->store((longlong)result.vote_for, true);
  table->field[field_num++]->store((longlong)result.applied_index, true);
  table->field[field_num++]->store(result.server_ready_for_rw.str,
                                   result.server_ready_for_rw.length,
                                   system_charset_info);
  table->field[field_num++]->store(result.instance_type.str,
                                   result.instance_type.length,
                                   system_charset_info);
  table->field[field_num++]->store(result.disable_election.str,
                                   result.disable_election.length,
                                   system_charset_info);
  table->field[field_num++]->store(result.apply_running.str,
                                   result.apply_running.length,
                                   system_charset_info);
  table->field[field_num++]->store(
      result.leader_ip.str, result.leader_ip.length, system_charset_info);
  table->field[field_num++]->store((longlong)result.leader_port, true);

  if (schema_table_store_record(thd, table)) return 1;

  return 0;
}

/**
 * Fills the learner source table with learner and source information.
 *
 * @param thd The thread handler.
 * @param table The table to fill.
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::fill_learner_source(THD *thd, TABLE *table) {
  assert(m_paxos);

  uint64 learner_id = 0;
  uint64 source_id = 0;
  std::string learner_ip_port;
  std::string source_ip_port;
  uint64 source_last_index;
  uint64 source_commit_index;
  uint64 learner_match_index = 0;
  uint64 learner_next_index = 0;
  uint64 learner_applied_index = 0;
  uint64 learner_source = 0;
  size_t node_num = 1;
  uint learner_source_count = 0;
  std::vector<alisql::Paxos::ClusterInfoType> cluster_info;
  alisql::Paxos::MemberInfoType member_info;

  m_paxos->getMemberInfo(&member_info);
  m_paxos->getClusterInfo(cluster_info);

  source_id = member_info.serverId;
  node_num = cluster_info.size();

  for (uint i = 0; i < node_num; i++) {
    learner_source = cluster_info[i].learnerSource;
    if (cluster_info[i].serverId == source_id)
      source_ip_port = cluster_info[i].ipPort;
    if (learner_source != source_id)
      continue;
    else
      learner_source_count++;

    learner_id = cluster_info[i].serverId;
    learner_ip_port = cluster_info[i].ipPort;
    source_last_index = member_info.lastLogIndex;
    source_commit_index = member_info.commitIndex;
    learner_match_index = cluster_info[i].matchIndex;
    learner_next_index = cluster_info[i].nextIndex;
    learner_applied_index = cluster_info[i].appliedIndex;

    int field_num = 0;
    table->field[field_num++]->store((longlong)learner_id, true);
    table->field[field_num++]->store(
        learner_ip_port.c_str(), learner_ip_port.length(), system_charset_info);
    table->field[field_num++]->store((longlong)source_id, true);
    table->field[field_num++]->store(
        source_ip_port.c_str(), source_ip_port.length(), system_charset_info);
    table->field[field_num++]->store((longlong)source_last_index, true);
    table->field[field_num++]->store((longlong)source_commit_index, true);
    table->field[field_num++]->store((longlong)learner_match_index, true);
    table->field[field_num++]->store((longlong)learner_next_index, true);
    table->field[field_num++]->store((longlong)learner_applied_index, true);
    if (schema_table_store_record(thd, table)) return 1;
  }
  if (learner_source_count == 0) {
    if (schema_table_store_record(thd, table)) return 1;
  }

  return 0;
}

/**
 * Fills the cluster health info table with health information.
 *
 * @param thd The thread handler.
 * @param table The table to fill.
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::fill_cluster_health_info(THD *thd, TABLE *table) {
  assert(m_paxos);

  std::vector<alisql::Paxos::HealthInfoType> health_info;
  m_paxos->getClusterHealthInfo(health_info);

  for (auto e : health_info) {
    int field_num = 0;
    std::string role;
    std::string connected;
    std::string running;
    uint64_t applyDelaySeconds;
    table->field[field_num++]->store((longlong)e.serverId, true);
    table->field[field_num++]->store(e.addr.c_str(), e.addr.length(),
                                     system_charset_info);
    switch (e.role) {
      case alisql::Paxos::StateType::FOLLOWER:
        role = "Follower";
        break;
      case alisql::Paxos::StateType::CANDIDATE:
        role = "Candidate";
        break;
      case alisql::Paxos::StateType::LEADER:
        role = "Leader";
        break;
      case alisql::Paxos::StateType::LEARNER:
        role = "Learner";
        break;
      default:
        role = "No Role";
        break;
    }
    table->field[field_num++]->store(role.c_str(), role.length(),
                                     system_charset_info);
    if (e.connected) {
      connected = "YES";
    } else {
      connected = "NO";
    }
    table->field[field_num++]->store(connected.c_str(), connected.length(),
                                     system_charset_info);
    table->field[field_num++]->store((longlong)e.logDelayNum, true);
    table->field[field_num++]->store((longlong)e.applyDelayNum, true);

    running = e.applyThreadRunning ? "Yes" : "No";
    table->field[field_num++]->store(running.c_str(), running.length(),
                                     system_charset_info);
    applyDelaySeconds = (e.applyDelayNum == 0) ? 0 : e.applyDelaySeconds;
    table->field[field_num++]->store((longlong)applyDelaySeconds, true);

    if (schema_table_store_record(thd, table)) return 1;
  }
  return 0;
}

/**
 * Fills the cluster status table with status information.
 *
 * @param thd The thread handler.
 * @param table The table to fill.
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::fill_cluster_status(THD *thd, TABLE *table) {
  assert(m_paxos);
  auto &stats = m_paxos->getStats();
  auto &log_stats = m_paxos->getLogStats();

  uint64 id = server_id;
  uint64 count_msg_append_log = 0;
  uint64 count_msg_request_vote = 0;
  uint64 count_heartbeat = 0;
  uint64 count_on_msg_append_log = 0;
  uint64 count_on_msg_request_vote = 0;
  uint64 count_on_heartbeat = 0;
  uint64 count_replicate_log = 0;
  uint64 count_log_meta_get_in_cache = 0;
  uint64 count_log_meta_get_total = 0;
  count_msg_append_log = stats.countMsgAppendLog;
  count_msg_request_vote = stats.countMsgRequestVote;
  count_heartbeat = stats.countHeartbeat;
  count_on_msg_append_log = stats.countOnMsgAppendLog;
  count_on_msg_request_vote = stats.countOnMsgRequestVote;
  count_on_heartbeat = stats.countOnHeartbeat;
  count_replicate_log = stats.countReplicateLog;
  count_log_meta_get_in_cache = log_stats.countMetaGetInCache;
  count_log_meta_get_total = log_stats.countMetaGetTotal;

  int field_num = 0;
  table->field[field_num++]->store((longlong)id, true);
  table->field[field_num++]->store((longlong)count_msg_append_log, true);
  table->field[field_num++]->store((longlong)count_msg_request_vote, true);
  table->field[field_num++]->store((longlong)count_heartbeat, true);
  table->field[field_num++]->store((longlong)count_on_msg_append_log, true);
  table->field[field_num++]->store((longlong)count_on_msg_request_vote, true);
  table->field[field_num++]->store((longlong)count_on_heartbeat, true);
  table->field[field_num++]->store((longlong)count_replicate_log, true);
  table->field[field_num++]->store((longlong)count_log_meta_get_in_cache, true);
  table->field[field_num++]->store((longlong)count_log_meta_get_total, true);
  if (schema_table_store_record(thd, table)) return 1;

  return 0;
}


/**
 * Fills the cluster membership changes table with membership change history.
 *
 * @param thd The thread handler.
 * @param table The table to fill.
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::fill_cluster_membership_changes(THD *thd,
                                                           TABLE *table) {
  assert(m_paxos);

  std::deque<alisql::Paxos::MembershipChangeType> mch =
      m_paxos->getMembershipChangeHistory();

  for (auto &e : mch) {
    int field_num = 0;
    table->field[field_num++]->store(e.time.c_str(), e.time.length(),
                                     system_charset_info);
    std::string command;
    e.address = "'" + e.address + "'";
    if (e.cctype == alisql::Protocol::CCOpType::CCMemberOp) {
      switch (e.optype) {
        case alisql::Protocol::CCOpType::CCAddNode:
          command = "change consensus_learner " + e.address +
                    " to consensus_follower";
          break;
        case alisql::Protocol::CCOpType::CCDelNode:
          command = "drop consensus_follower " + e.address;
          break;
        case alisql::Protocol::CCOpType::CCDowngradeNode:
          command = "change consensus_follower " + e.address +
                    " to consensus_learner";
          break;
        case alisql::Protocol::CCOpType::CCConfigureNode:
          command =
              "change consensus_node " + e.address + " consensus_force_sync " +
              (e.forceSync ? "true" : "false") + " consensus_election_weight " +
              std::to_string(e.electionWeight);
          break;
        case alisql::Protocol::CCOpType::CCLeaderTransfer:
          command = "change consensus_leader to " + e.address;
          break;
        default:
          break;
      }
    } else {  // alisql::::CCOpType::CCLearnerOp
      switch (e.optype) {
        case alisql::Protocol::CCOpType::CCAddNode:
        case alisql::Protocol::CCOpType::CCAddLearnerAutoChange:
          command = "add consensus_learner " + e.address;
          break;
        case alisql::Protocol::CCOpType::CCDelNode:
          command = "drop consensus_learner " + e.address;
          break;
        case alisql::Protocol::CCOpType::CCConfigureNode:
          if (e.learnerSource.size())
            e.learnerSource = "'" + e.learnerSource + "'";
          command = "change consensus_node " + e.address +
                    " consensus_learner_source " + e.learnerSource +
                    " consensus_use_applyindex " +
                    (e.sendByAppliedIndex ? "true" : "false");
          break;
        case alisql::Protocol::CCOpType::CCSyncLearnerAll:
          command = "change consensus_learner for consensus_meta";
          break;
        default:
          break;
      }
    }
    command += ";";
    table->field[field_num++]->store(command.c_str(), command.length(),
                                     system_charset_info);
    if (schema_table_store_record(thd, table)) return 1;
  }

  return 0;
}

std::string get_ha_type_string(alisql::Paxos::HAType type) {
  switch (type) {
    case alisql::Paxos::HA_TYPE_SWITCHOVER:
      return "Switchover";
    case alisql::Paxos::HA_TYPE_FAILOVER:
      return "Failover";
    default:
      return "Invalid";
  }
}

/**
 * Fills the cluster ha history.
 *
 * @param thd The thread handler.
 * @param table The table to fill.
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::fill_cluster_ha_history(THD *thd,
                                                           TABLE *table) {
  assert(m_paxos);

  const std::deque<alisql::Paxos::HaInfo> &ha_history =
      m_paxos->getHaHistory();
  for (auto &e : ha_history) {
    int field_num = 0;
    /** HA finish time stamp. */
    table->field[field_num++]->store(e.finishTimestamp.c_str(),
                                     e.finishTimestamp.length(),
                                     system_charset_info);
    /** Current term. */
    table->field[field_num++]->store((longlong)e.currentTerm, true);
    /** HA type. */
    std::string ha_type = get_ha_type_string(e.haType);
    table->field[field_num++]->store(ha_type.c_str(), ha_type.length(),
                                     system_charset_info);
    /** HA source address. */
    table->field[field_num++]->store(
        e.sourceAddress.c_str(), e.sourceAddress.length(), system_charset_info);

    /** HA target address. */
    table->field[field_num++]->store(
        e.targetAddress.c_str(), e.targetAddress.length(), system_charset_info);

    /** HA local consensus duration. */
    table->field[field_num++]->store((longlong)e.localConsensusDuration, true);

    /** HA local server duration. */
    table->field[field_num++]->store((longlong)e.localServerDuration, true);

    /** HA total duration. */
    table->field[field_num++]->store((longlong)e.totalDuration, true);

    /** HA success. */
    table->field[field_num++]->store(e.haSucess ? "Y" : "N", 1,
                                     system_charset_info);
    if (schema_table_store_record(thd, table)) return 1;
  }
  return 0;
}

/**
 * Forces the purge of the fifo cache up to the specified index.
 *
 * @param index The index up to which the cache should be purged.
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::force_purge_cache(uint64_t index) {
  assert(m_paxos);
  m_cache_manager->evict_oldest_until(index, false);
  return 0;
}

/**
 * Purges the log up to the specified index.
 *
 * @param index The index up to which the log should be purged.
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::purge_log(uint64_t index) {
  assert(m_paxos);
  return m_log_manager.purge_log(index);
}

/**
 * Drops the prefetch channel with the specified ID.
 *
 * @param channel_id The ID of the prefetch channel.
 * @return 0 on success, 1 on failure.
 */
int Consensus_xlog_engine::drop_prefetch_channel(uint64_t channel_id) {
  assert(m_paxos);
  return m_prefetch_manager.drop_prefetch_channel(channel_id);
}

/**
 * Gets the server address from the configuration.
 *
 * @param item The item containing the address.
 * @param ret The server Addr.
 */
void Consensus_xlog_engine::get_config_server_addr(Item *item,
                                                   std::string &ret) {
  assert(m_paxos);
  ret = m_paxos->getConfig()->getServer(item->val_int())->strAddr;
}

/**
 * Gets the server ID from the configuration.
 *
 * @param item The item containing the server ID.
 * @param ret The server ID.
 */
void Consensus_xlog_engine::get_config_server_id(Item *item, uint64_t &ret) {
  assert(m_paxos);
  ret = m_paxos->getConfig()->getServerIdFromAddr(
      std::string(item->val_str(nullptr)->ptr()));
}

/**
 * Gets the protocol (Paxos).
 *
 * @return Pointer to the protocol.
 */
alisql::Protocol *Consensus_xlog_engine::get_protocol() { return m_paxos.get(); }

/**
 * Marks log synchronization as done for a group of threads.
 *
 * @param head The head of the thread group.
 */
void Consensus_xlog_engine::sync_log_done(const THD *head) {
  uint64_t group_max_log_index = 0;
  uint64_t queue_count = 0;
  // set last index and write log done out of order is supported
  for (; head; head = head->next_to_commit) {
    // find last sync log index
    if (head->consensus_index > group_max_log_index) {
      group_max_log_index = head->consensus_index;
    }
    queue_count++;
  }

  if (group_max_log_index > 0) {
    m_log_manager.set_sync_index_if_greater(group_max_log_index);
    m_paxos->getLocalServer()->syncLogDone(group_max_log_index);
    m_cache_manager->evict_oldest_try(m_paxos->getMinMatchIndex(),
                                      (queue_count << 1));
  }
}

/**
 * Sets the last non-commit dependent log index.
 *
 * @param logIndex The log index to set.
 */
void Consensus_xlog_engine::set_last_non_commit_dep_index(uint64_t logIndex) {
  m_paxos->getLocalServer()->setLastNonCommitDepIndex(logIndex);
}

/**
 * Marks log writing as done internally.
 *
 * @param logIndex The log index.
 * @param forceSend Whether to force sending.
 */
void Consensus_xlog_engine::write_log_done_internal(uint64_t logIndex,
                                                    bool forceSend) {
  m_paxos->getLocalServer()->writeLogDoneInternal(logIndex, forceSend);
}

/**
 * Marks cache log writing as done.
 */
void Consensus_xlog_engine::write_cache_log_done() {
  m_paxos->getLocalServer()->writeCacheLogDone();
}

/**
 * Sets the consensus message compression option.
 */
void Consensus_xlog_engine::set_consensus_msg_compress_option() {
  // format: ip1:port1 type threshold checksum; ip2:port2 type threshold
  // checksum...
  char* option = opt_consensus_msg_compress_option;
  if (option == NULL) return;
  std::size_t current, previous = 0;
  std::string fcstr(option);
  std::vector<std::string> splits;
  current = fcstr.find(';');
  while (current != std::string::npos) {
    splits.push_back(fcstr.substr(previous, current - previous));
    previous = current + 1;
    current = fcstr.find(';', previous);
  }
  splits.push_back(fcstr.substr(previous, current - previous));
  m_paxos->resetMsgCompressOption();
  for (auto &kv : splits) {
    if (kv.empty()) continue;
    char addr[256];
    uint32 type, threshold, checksum;
    if (std::sscanf(kv.c_str(), "%s %u %u %u", addr, &type, &threshold,
                    &checksum) == 4) {
      std::string address(addr);
      if (address == "*:*") address = "";
      if (m_paxos->setMsgCompressOption(type, threshold, (bool)checksum,
                                              address) == 0)
        xp::warn(ER_XP_0) << "Set consensus server "
                          << (address == "" ? "all" : address.c_str())
                          << " msg compress option succeed: type(" << type
                          << "), threshold(" << threshold << "), checksum("
                          << checksum << ")";
      else
        xp::warn(ER_XP_0) << "Set consensus server "
                          << (address == "" ? "all" : address.c_str())
                          << "msg compress option failed, wrong address!";
    }
  }
}

/**
 * Sets the consensus flow control option.
 */
void Consensus_xlog_engine::set_consensus_flow_control() {
  // format: ip1:port1 fc1;ip2:port2 fc2...
  char* option = opt_consensus_msg_compress_option;
  if(option == NULL) return;
  std::size_t current, previous = 0;
  std::string fcstr(option);
  std::vector<std::string> splits;
  current = fcstr.find(';');
  while (current != std::string::npos) {
    splits.push_back(fcstr.substr(previous, current - previous));
    previous = current + 1;
    current = fcstr.find(';', previous);
  }
  splits.push_back(fcstr.substr(previous, current - previous));
  if (splits.size() > 0) m_paxos->reset_flow_control();
  for (auto &kv : splits) {
    char addr[300];
    uint64 serverid;
    int64 fc;
    if (std::sscanf(kv.c_str(), "%s %ld", addr, &fc) == 2) {
      serverid = m_paxos->getServerIdFromAddr(addr);
      xp::warn(ER_XP_0) << "Add consensus server " << serverid
                        << " flow control " << fc;
      m_paxos->set_flow_control(serverid, fc);
    }
  }
}

/**
 * Sets the consensus asynchronous mode.
 */
void Consensus_xlog_engine::set_consensus_async() {
  replica_exec_mode_options = opt_weak_consensus_mode ? 2 : 0;
  m_paxos->setConsensusAsync(opt_weak_consensus_mode);
}

/**
 * Disables election in consensus if not in weak mode.
 */
void Consensus_xlog_engine::set_consensus_disable_election() {
  if (!m_paxos->getConsensusAsync())
    xp::warn(ER_XP_0) << "Change consensus_disable_election to "
      << opt_consensus_disable_election
      << " while cluster is in weak mode.";
  m_paxos->setDisableElection(opt_consensus_disable_election);
}

/**
 * Saves privilege checks user information for replication.
 *
 * @param rpl_info The relay log info object.
 * @param username The username.
 * @param hostname The hostname.
 * @return True on success, false otherwise.
 */
bool Consensus_xlog_engine::rpl_save_privilege_checks_user(
    Relay_log_info *rpl_info, const char *username, const char *hostname) {
  if (username != nullptr
      && hostname != nullptr
      && (strlen(username) > 0 || strlen(hostname) > 0)) {
    if (strlen(username) > 0)
      memcpy(rpl_info->consensus_privilege_checks_username, username, strlen(username) + 1);
    if (strlen(hostname) > 0)
      memcpy(rpl_info->consensus_privilege_checks_hostname, hostname, strlen(hostname) + 1);
    xp::info(ER_XP_RECOVERY)
        << "save set_privilege_checks_user for"
        << " channel: " << rpl_info->get_channel() 
        << ", username: " << username
        << ", hostname: " << hostname;

    return true;
  }
  return false;
}

/**
 * rotate binlog or relay log
 *
 * @return 0 if succ, 1 otherwise.
 */
int Consensus_xlog_engine::rotate_consensus_log() {
  auto consensus_guard = create_lock_guard(
      [&] { m_log_manager.rdlock_consensus_status(); },
      [&] { m_log_manager.unlock_consensus_status(); });
  if (m_log_manager.get_status() ==
      Consensus_Log_State::BINLOG_WORKING) {
    bool dont_care = false;
    return mysql_bin_log.rotate(true, &dont_care);

  } else {
    Relay_log_info *rli_info = m_log_manager.get_relay_log_info();
    auto data_guard = create_lock_guard(
      [&] { mysql_mutex_lock(&rli_info->mi->data_lock); },
      [&] { mysql_mutex_unlock(&rli_info->mi->data_lock); });

    return rotate_relay_log(rli_info->mi);
  }
  return 0;
}

void print_log_for_check_term_during_flush(
    const uint64_t term,
    const std::shared_ptr<Consensus_xlog_handler> xlog_handler,
    Consensus_log_manager *log_manager,
    const Consensus_switchover_manager *switchover_manager) {
xp::warn(ER_XP_COMMIT) << "Failed to flush, because leadership changing"	
                              "replicate log or check term failed"	
                           << ", consensus_term: "	
                           << xlog_handler->getCurrentTerm()	
                           << ", term: " << term << ", current term: "	
                           << log_manager->get_current_term()	
                           << ", m_switchover_state: "	
                           << switchover_manager->get_switchover_state();	
}
/**
 * Checks the term during log flush.
 *
 * @param term The term to check.
 * @return 0 if term is valid, 1 otherwise.
 */
int Consensus_xlog_engine::check_term_during_flush(const uint64_t term) {
  if (term == 0
      || m_xlog_handler->getCurrentTerm() != term
      || term != m_log_manager.get_current_term()
      || DBUG_EVALUATE_IF("force_after_leader_transfer", true, false)) {
    print_log_for_check_term_during_flush(term, m_xlog_handler, &m_log_manager, &m_switchover_manager);
    return 1;
  }
  return 0;
}

void print_log_for_check_term_before_flush(
    const THD *thd,
    const Consensus_switchover_manager *switchover_manager) {
    xp::warn(ER_XP_COMMIT)
        << "Failed to ordered_commit, because leadership changing"	
        << ", m_switchover_state: "	
        << switchover_manager->get_switchover_state()	
        << ", sql_command: " << thd->lex->sql_command << ", xid "	
        << *thd->get_transaction()->xid_state()->get_xid()	
        << ", status "	
        << thd->get_transaction()->xid_state()->state_name();	
}

/**
 * Checks the term before log flush.
 *
 * @param thd The thread handler.
 * @return 0 if term is valid, 1 otherwise.
 */
int Consensus_xlog_engine::check_term_before_flush(THD *thd) {
  if (m_log_manager.get_status() !=
          Consensus_Log_State::BINLOG_WORKING ||
      m_is_logger) {
    thd->commit_error = THD::CE_COMMIT_ERROR;
    thd->consensus_error = THD::CSS_SERVER_NOT_READY;
    rollback_with_flush_nothing(thd, true);
    return 1;
  }

  if (DBUG_EVALUATE_IF("force_in_leader_transfer", true, false) ||
      (opt_consensus_disable_commit_before_change_leader &&
       m_switchover_manager.is_in_limit_all())) {
    XID empty_xid;
    thd->commit_error = THD::CE_COMMIT_ERROR;
    thd->consensus_error = THD::CSS_LEADERSHIP_CHANGING;
    print_log_for_check_term_before_flush(thd, &m_switchover_manager);
    rollback_with_flush_nothing(thd, true);
    return 1;
  }
  return 0;
}

uint64_t Consensus_xlog_engine::get_handler_current_term() {
  auto term_guard = create_lock_guard(
      [&] { mysql_mutex_lock(m_log_manager.get_term_lock()); },
      [&] { mysql_mutex_unlock(m_log_manager.get_term_lock()); });

  assert(m_xlog_handler);
  return m_xlog_handler->getCurrentTerm();
}

uint64_t Consensus_xlog_engine::get_mock_start_index() {
  assert(m_xlog_handler);
  return m_xlog_handler->getMockStartIndex();
}

void Consensus_xlog_engine::add_log_to_cache(uint64 term, uint64 index,
                                             size_t buf_size, uchar *buffer,
                                             bool outer, uint flag) {
  m_cache_manager->open_and_write_buffer(term, index, buf_size, buffer, outer,
                                         flag);

  m_log_manager.set_cache_index(index);
}

}  // namespace komodo
