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

/** @file sql/komodo0xlog.cc

  Binlog behaviours related to komodo lizard system.

  Created 2023-06-20 by Jianwei.zhao
 *******************************************************/

#include "sql/komodo0xlog.h"
#include "komodo0xlog0recovery.h"
#include "sql/komodo0xlog0engine.h"
#include "sql/komodo0xlog0meta.h"
#include "sql/sql_class.h"

namespace komodo {
/**
 * Get the instance of XLog.
 *
 * @return The instance of XLog.
 */
XLog &XLog::instance() {
  static XLog m_instance;
  return m_instance;
}

/**
 * Create XLog based on a set of configurations.
 *
 * @param[in] initialize
 * @param[in] force_recover
 * @param[in] enable_consensus
 * @param[in] bin_log
 * @param[in] learner_node
 * @param[in] logger_node
 * @param[in] binlog_arg
 * @return true if error occurs
 */
bool XLog::create(const bool initialize, const bool force_recover,
                  const bool enable_consensus, const bool bin_log,
                  const bool learner_node, const bool logger_node,
                  MYSQL_BIN_LOG *binlog_arg) {
  /** 1. Do the necessary check to determine if we really need to enable
   * consensus module. */
  bool should_enable_consensus = false;

  if (!initialize && !force_recover && enable_consensus) {
    //* has binlog
    if (!bin_log) {
      xp::error(ER_XP_0) << "PolarDB-X Engine log_bin must be set to ON when "
                            "@@enable_consensus=ON";
      LogErr(WARNING_LEVEL, ER_NEED_LOG_BIN, "--log-bin");
      return true;
    }
    should_enable_consensus = true;
  }

  /** 2. After the check, create the necessary components. */
  m_xlog_meta = new XLog_meta(learner_node, initialize, should_enable_consensus);

  if (should_enable_consensus) {
    auto xlog_recovery = new Consensus_xlog_recovery(logger_node);
    m_xlog_engine = new Consensus_xlog_engine(m_xlog_meta, xlog_recovery,
                                              binlog_arg, logger_node);
    m_xlog_recovery = xlog_recovery;
  } else {
    m_xlog_recovery = new binlog::Binlog_recovery();
    m_xlog_engine = new Autonomy_xlog_engine(binlog_arg);
  }

  return false;
}

/**
 * Start recovery process for the xlog engine.
 *
 * @return 0 if successful, 1 otherwise.
 */
int XLog::start_in_recovery(const std::function<int()> &create_pid_file) {
  return m_xlog_engine->start_in_recovery(create_pid_file);
}

/**
 * Start after recovery process for the xlog engine.
 *
 * @return 0 if successful, 1 otherwise.
 */
int XLog::start_after_recovery() {
  return m_xlog_engine->start_after_recovery();
}

/**
 * Start apply threads for the xlog engine.
 */
void XLog::start_apply_threads_after_recovery() {
  m_xlog_engine->start_apply_threads_after_recovery();
}

/**
 * Revises the GTID set from the binlog.
 */
int XLog::init_gtid_set_after_recovery() {
  return m_xlog_engine->init_gtid_set_after_recovery();
}

/**
 * Shuts down the xlog engine.
 */
void XLog::shutdown() {
  if (m_xlog_engine) m_xlog_engine->shutdown();
}

/**
 * Destroys the components of xlog engine.
 */
void XLog::destroy() {
  if (m_xlog_engine) delete m_xlog_engine;
  if (m_xlog_meta) delete m_xlog_meta;
  if (m_xlog_recovery) delete m_xlog_recovery;
}

/*---------------------- Local server related --------------------*/
/**
 * Sets the last non-commit dependency index.
 * @param logIndex The index of the log.
 */
void xlog_set_last_non_commit_dep_index(uint64_t logIndex) {
  XLog::instance().get_xlog_engine()->set_last_non_commit_dep_index(logIndex);
}

/**
 * Marks the log writing as done internally.
 *
 * @param logIndex The index of the log.
 * @param forceSend Whether to force send the log.
 */
void xlog_write_log_done_internal(uint64_t logIndex, bool forceSend) {
  XLog::instance().get_xlog_engine()->write_log_done_internal(logIndex,
                                                              forceSend);
}

/**
 * Writes the cache log done.
 */
void xlog_write_cache_log_done() {
  XLog::instance().get_xlog_engine()->write_cache_log_done();
}

/*---------------- Consensus log manager related ---------------- */

/**
 * Marks the log synchronization as done for a group of threads.
 *
 * @param head The head of the thread.
 */
void xlog_sync_log_done(const THD *head) {
  XLog::instance().get_xlog_engine()->sync_log_done(head);
}

/**
 * Adds log file entry to the log file index.
 *
 * @param timestamp The timestamp of the log file.
 * @param log_file_name The name of the log file.
 */
void xlog_add_into_log_file_index(const uint64_t timestamp,
                                  const char *log_file_name) {
  komodo::XLog::instance().get_xlog_engine()->add_into_log_file_index(
      timestamp, log_file_name);
}
/**
 * Truncate log file index before the given log file name in log manager.
 *
 * @param log_file_name The name of the log file.
 */
void xlog_log_manager_truncate_before_from_log_file_index(const char *log_file_name) {
  komodo::XLog::instance()
      .get_xlog_engine()
      ->truncate_before_from_log_file_index(log_file_name);
}

/**
 * Adds internal pending recovering trx.
 *
 * @param ht The handle to the transaction.
 * @param xa_trx The XA transaction.
 * @param xa_spec The XA specification.
 */
void xlog_recovery_add_internal_pending_recovering_trx(
    handlerton &ht, const XA_recover_txn *xa_trx,
    const komodo::XA_specification &xa_spec) {
  komodo::XLog::instance()
      .get_xlog_recovery()
      ->add_internal_pending_recovering_trx(ht, xa_trx, xa_spec);
}

/**
 * Adds external pending recovering trx.
 *
 * @param ht The handle to the transaction.
 * @param current_state The current state of the transaction.
 * @param next_state The next state of the transaction.
 * @param xa_trx The XA transaction.
 * @param xa_spec The XA specification.
 */
void xlog_recovery_add_external_pending_recovering_trx(
    handlerton &ht, enum_ha_recover_xa_state current_state,
    enum_ha_recover_xa_state next_state, const XA_recover_txn *xa_trx,
    const komodo::XA_specification &xa_spec) {
  komodo::XLog::instance()
      .get_xlog_recovery()
      ->add_external_pending_recovering_trx(ht, current_state, next_state,
                                            xa_trx, xa_spec);
}

/**
 * Set last leader term index in recovery manager.
 *
 * @param index The index of the last leader term.
 */
void xlog_recovery_set_last_leader_term_index(const uint64_t index) {
  komodo::XLog::instance().get_xlog_recovery()->set_last_leader_term_index(
      index);
}

/**
 * Get last leader term index from recovery manager.
 *
 * @return last leader term index
 */
uint64_t xlog_recovery_get_last_leader_term_index() {
  return komodo::XLog::instance()
      .get_xlog_recovery()
      ->get_last_leader_term_index();
}

/**
 * Get current index from log manager.
 *
 * @return current index
 */
uint64_t xlog_log_manager_get_current_index() {
  return komodo::XLog::instance()
      .get_xlog_engine()
      ->m_log_manager.get_current_index();
}

/**
 * Get sync index from log manager.
 *
 * @return sync index
 */
uint64_t xlog_engine_get_sync_index() {
  return komodo::XLog::instance()
      .get_xlog_engine()
      ->m_log_manager.get_sync_index();
}

/**
 * Get event timestamp from consensus_log_manager.
 * @return event timestamp
 */
uint64_t xlog_log_manager_get_event_timestamp() {
  return komodo::XLog::instance()
      .get_xlog_engine()
      ->m_log_manager.get_event_timestamp();
}
/**
 * Get term lock from log manager.
 *
 * @return term lock
 */
mysql_mutex_t *xlog_log_manager_get_term_lock() {
  return komodo::XLog::instance()
      .get_xlog_engine()
      ->m_log_manager.get_term_lock();
}
/**
 * Get IO cache from log manager.
 *
 * @return IO cache
 */
IO_CACHE *xlog_log_manager_get_io_cache() {
  return komodo::XLog::instance().get_xlog_engine()->m_log_manager.get_cache();
}

/**
 * Sets the recovery reader.
 *
 * @param binlog_file_reader The binlog file reader.
 */
void xlog_recovery_set_reader(
    Binlog_file_reader *binlog_file_reader) {
  komodo::XLog::instance().get_xlog_recovery()->set_reader(binlog_file_reader);
}

/**
 * Gets the recovery object.
 *
 * @return The recovery object.
 */
binlog::Binlog_recovery *xlog_recovery_get_recovery() {
  return komodo::XLog::instance().get_xlog_recovery();
}
/**
 * Check term with current term in log manager during flush.
 *
 * @param term The term to check.
 * @return 0 if term is valid, 1 otherwise.
 */
int xlog_engine_check_term_during_flush(const uint64_t term) {
  return komodo::XLog::instance().get_xlog_engine()->check_term_during_flush(
      term);
}
/**
 * Check term before flush.
 *
 * @param thd The thread to check.
 * @return 0 if term is valid, 1 otherwise.
 */
int xlog_engine_check_term_before_flush(THD *thd) {
  return komodo::XLog::instance().get_xlog_engine()->check_term_before_flush(
      thd);
}

/**
 * Gets the current term.
 *
 * @return The current term.
 */
uint64_t xlog_engine_get_handler_current_term() {
  return komodo::XLog::instance().get_xlog_engine()->get_handler_current_term();
}

/**
 * Gets the mock_start_index.
 *
 * @return The mock_start_index.
 */
uint64_t xlog_engine_get_mock_start_index() {
  return komodo::XLog::instance().get_xlog_engine()->get_mock_start_index();
}

/**
 * Lock consensus status in log manager.
 *
 * @return void
 */
void xlog_log_manager_rdlock_consensus_status() {
  komodo::XLog::instance()
      .get_xlog_engine()
      ->m_log_manager.rdlock_consensus_status();
}
/**
 * Unlock consensus status in log manager.
 *
 * @return void
 */
void xlog_log_manager_unlock_consensus_status() {
  komodo::XLog::instance()
      .get_xlog_engine()
      ->m_log_manager.unlock_consensus_status();
}

/**
 * Resets the event scheduler to its configured state.
 *
 * @return void
 */
void xlog_engine_reset_event_scheduler() {
  komodo::XLog::instance().get_xlog_engine()->reset_event_scheduler();
}

/**
 * Check leader role.
 *
 * @return true if leader, false otherwise.
 */
bool xlog_engine_leader_role() {
  alisql::Protocol *protocol =
  komodo::XLog::instance().get_xlog_engine()->get_protocol();
  return protocol->getState() == alisql::Protocol::StateType::LEADER;
}
/**
 * Check if binlog is working.
 *
 * @return true if binlog is working, false otherwise.
 */
bool xlog_engine_binlog_working() {
  return komodo::XLog::instance()
             .get_xlog_engine()
             ->m_log_manager.get_status() ==
         Consensus_Log_State::BINLOG_WORKING;
}

/**
 * Get log file index from log manager.
 *
 * @return log file index
 */
Consensus_logindex_manager *xlog_engine_get_log_file_index() {
  return komodo::XLog::instance()
      .get_xlog_engine()
      ->m_log_manager.get_log_file_index();
}

/**
 * Get prefetch manager from log manager.
 *
 * @return prefetch manager
 */
Consensus_prefetch_manager *xlog_engine_get_prefetch_manager() {
  return komodo::XLog::instance().get_xlog_engine()->get_prefetch_manager();
}

/**
 * Get cache manager from log manager.
 *
 * @return cache manager
 */
Consensus_cache_manager *xlog_engine_get_cache_manager() {
  return komodo::XLog::instance().get_xlog_engine()->get_cache_manager();
}

/**
 * Get log manager from log manager.
 *
 * @return log manager
 */
Consensus_log_manager &xlog_engine_get_log_manager() {
  return komodo::XLog::instance().get_xlog_engine()->m_log_manager;
}

/**
 * Get switchover manager from log manager.
 *
 * @return switchover manager
 */
Base_switchover_manager *xlog_engine_get_switchover_manager() {
  return komodo::XLog::instance().get_xlog_engine()->get_switchover_manager();
}

/**
 * Get protocol from log manager.
 *
 * @return protocol
 */
alisql::Protocol *xlog_engine_get_protocol() {
  return komodo::XLog::instance().get_xlog_engine()->get_protocol();
}

void xlog_engine_add_log_to_cache(uint64 term, uint64 index, size_t buf_size,
                                  uchar *buffer, bool outer, uint flag) {
  komodo::XLog::instance().get_xlog_engine()->add_log_to_cache(
      term, index, buf_size, buffer, outer, flag);
}

/**
 * Save set privilege checks user.
 *
 * @param rpl_info The relay log info.
 * @param username The username.
 * @param hostname The hostname.
 * @return True on success, false otherwise.
 */
bool xlog_engine_rpl_save_privilege_checks_user(Relay_log_info *rpl_info,
                                                     const char *username,
                                                     const char *hostname) {
  return komodo::XLog::instance()
      .get_xlog_engine()
      ->rpl_save_privilege_checks_user(rpl_info, username, hostname);
}

/**
 * Rotates the consensus log.
 *
 * @return 0 if successful, 1 otherwise.
 */
int xlog_engine_rotate_consensus_log() {
  return komodo::XLog::instance().get_xlog_engine()->rotate_consensus_log();
}

/**
 * Calls a function on the xlog engine.
 *
 * @param func The function to call.
 * @return void
 */
void xlog_engine_call_function(void (XLog_engine::*func)()) {
    (komodo::XLog::instance().get_xlog_engine()->*func)();
}

/**
 * Tries to initialize the consensus cluster info.
 */
void xlog_meta_try_init_consensus_info() {
  komodo::XLog::instance().get_xlog_meta()->try_init_consensus_info();
}

/**
 * Gets the consensus info.
 *
 * @return The consensus info.
 */
Consensus_info *xlog_meta_get_consensus_info() {
  return komodo::XLog::instance().get_xlog_meta()->get_consensus_info();
}

/*----------------------- Consensus ptr related -------------------- */
/**
 * Creates the xlog engine.
 *
 * @param initialize Whether to initialize the xlog engine.
 * @param force_recover Whether to force recover the xlog engine.
 * @param enable_consensus Whether to enable consensus.
 * @param bin_log Whether to enable binlog.
 * @param learner_node Whether to enable learner node.
 * @param logger_node Whether to enable logger node.
 * @param binlog_arg The binlog argument.
 * @return true if the xlog engine is created, false otherwise.
 */
bool xlog_create(const bool initialize, const bool force_recover,
                 const bool enable_consensus, const bool bin_log,
                 const bool learner_node, const bool logger_node,
                 MYSQL_BIN_LOG *binlog_arg) {
  return komodo::XLog::instance().create(initialize, force_recover,
                                         enable_consensus, bin_log,
                                         learner_node, logger_node, binlog_arg);
}

/**
 * Starts the xlog engine in recovery mode.
 *
 * @return true if the xlog engine is started in recovery mode, false otherwise.
 */
bool xlog_start_in_recovery(const std::function<int()> &create_pid_file) {
  return komodo::XLog::instance().start_in_recovery(create_pid_file);
}

/**
 * Starts the xlog engine after recovery.
 *
 * @return true if the xlog engine is started after recovery, false otherwise.
 */
bool xlog_start_after_recovery() {
  return komodo::XLog::instance().start_after_recovery();
}

/**
 * Starts the apply threads for the xlog engine.
 */
void xlog_start_apply_threads_after_recovery() {
  komodo::XLog::instance().start_apply_threads_after_recovery();
}

/**
 * Init the GTID set from the binlog after recovery
 */
int xlog_init_gtid_set_after_recovery() {
  return komodo::XLog::instance().init_gtid_set_after_recovery();
}

/**
 * Destroys the xlog engine.
 */
void xlog_destroy() { komodo::XLog::instance().destroy(); }

/**
 * Shuts down the xlog engine.
 */
void xlog_shutdown() { komodo::XLog::instance().shutdown(); }

/**
 * Gets the safety index for purge.
 *
 * @return The safety index for purge.
 */
uint64_t xlog_get_safety_index_for_purge() {
  return komodo::XLog::instance()
      .get_xlog_engine()
      ->get_safety_index_for_purge();
}

/**
 * Updates the apply thread running.
 *
 * @param is_running Whether the apply thread is running.
 */
void xlog_update_apply_thread_running(bool is_running) {
  komodo::XLog::instance().get_xlog_engine()->update_apply_thread_running(
      is_running);
}

/**
 * Updates the apply delay seconds.
 *
 * @param sec The delay seconds.
 */
void xlog_update_apply_delay_seconds(uint64_t sec) {
  komodo::XLog::instance().get_xlog_engine()->update_apply_delay_seconds(sec);
}

/**
 * Replicates the empty log.
 */
void xlog_replicate_empty_log() {
  if (komodo::XLog::instance().get_xlog_engine()) {
    komodo::XLog::instance().get_xlog_engine()->replicate_empty_log();
  }
}


/**
 * check is leader but not single node
 */
bool xlog_is_leader_not_single() {
  if (komodo::XLog::instance().get_xlog_engine()) {
    return komodo::XLog::instance().get_xlog_engine()->is_leader_not_single();
  }
  return false;
}

/**
 * Gets the current paxos term.
 *
 * @return The current paxos term.
 */
uint64_t xlog_engine_get_paxos_term() {
  return komodo::XLog::instance().get_xlog_engine()->get_current_paxos_term();
}

/**
 * Prepares the applied index checker with the current commit queue.
 *
 * @param final_queue The commit queue.
 */
void xlog_engine_applied_index_check_prepare(THD *final_queue) {
  komodo::XLog::instance().get_xlog_engine()->applied_index_check_prepare(
      final_queue);
}

/**
 * Checks if the command needs to be blocked.
 *
 * @param thd The thread.
 * @return true if the command needs to be blocked, false otherwise.
 */
bool xlog_need_block_command(THD *thd) {
  return komodo::XLog::instance().get_xlog_engine() &&
         komodo::XLog::instance().get_xlog_engine()->need_block_command(thd);
}

/**
 * Updates the applied index during finish commit.
 *
 * @param index The index.
 */
void xlog_update_applied_index_during_finish_commit(uint64_t index) {
  return komodo::XLog::instance()
      .get_xlog_engine()
      ->update_applied_index_during_commit(index);
}

/**
 * Updates the logger applied index.
 *
 * @param index The index.
 */
void xlog_update_logger_applied_index(uint64_t index) {
  return komodo::XLog::instance()
      .get_xlog_engine()
      ->update_logger_applied_index(index);
}

/**
 * Gets the status of the group queue for the applied index checker.
 *
 * @return The status of the group queue.
 */
const char *xlog_engine_applied_index_checker_get_group_queue_status() {
  return komodo::XLog::instance()
      .get_xlog_engine()
      ->applied_index_checker_get_group_queue_status();
}
/**
 * Get sequence stage1 lock from log manager.
 *
 * @return sequence stage1 lock
 */
mysql_mutex_t *xlog_log_manager_get_sequence_stage1_lock() {
  return komodo::XLog::instance()
      .get_xlog_engine()
      ->m_log_manager.get_sequence_stage1_lock();
}

/**
 * Checks if the paxos commit is ready.
 *
 * @param thd The thread to check.
 * @param is_my_thd whether is myself thd.
 * @return 0 if the paxos commit is ready, 1 otherwise.
 */
int xlog_engine_check_wait_paxos_commit(THD *thd, const bool is_my_thd) {
  if (thd->consensus_error == THD::CSS_NONE) {
    /* Wait until the logs are received by more than half of the nodes */
    komodo::XLog::instance().get_xlog_engine()->wait_commit(thd);
    ut_a(thd->consensus_error == THD::CSS_NONE);
    return 0;
  } else {
    komodo::XLog::instance().get_xlog_engine()->rollback_with_flush_nothing(
        thd, is_my_thd);
    return 1;
  }
}

/**
 * Waits for the commit index update.
 *
 * @param thd The thread.
 * @param log_pos The log position.
 * @param index The index.
 * @param heartbeat_period The heartbeat period.
 * @param packet The packet.
 * @param flush_net The flush net.
 * @param send_heartbeat_event The send heartbeat event.
 * @return 0 if the commit index is updated, 1 otherwise.
 */
int xlog_wait_commit_index_update(
    const THD *thd, my_off_t log_pos, uint64_t index,
    std::chrono::nanoseconds heartbeat_period, String &packet,
    const std::function<int()> &flush_net,
    const std::function<int(my_off_t)> &send_heartbeat_event) {
  return komodo::XLog::instance().get_xlog_engine()->wait_commit_index_update(
      thd, log_pos, index, heartbeat_period, packet, flush_net,
      send_heartbeat_event);
}

/**
 * Flushes the logs.
 *
 * @param thd The thread.
 * @param gtid_event The gtid event.
 * @param cache_data The cache data.
 * @param writer The writer.
 * @return true if error occurred, false otherwise.
 */
bool xlog_flush_logs(THD *thd, Gtid_log_event *gtid_event,
                     binlog_cache_data *cache_data,
                     Binlog_event_writer *writer) {
  TLog_flusher *flusher =
      komodo::XLog::instance().get_xlog_engine()->get_tlog_flusher();
  if (opt_gcn_write_event) {
    Gcn_log_event gcn_event(thd);
    return flusher->write(thd, &gcn_event, gtid_event, cache_data, writer);
  } else {
    return flusher->write(thd, nullptr, gtid_event, cache_data, writer);
  }
}

/**
 * Forces the single leader.
 *
 * @return 0 if the single leader is forced, 1 otherwise.
 */
int xlog_force_single_leader() {
  return komodo::XLog::instance().get_xlog_engine()->force_single_leader();
}

/**
 * Forces the single learner.
 *
 * @return 0 on success, 1 otherwise.
 */
int xlog_force_single_learner() {
  return komodo::XLog::instance().get_xlog_engine()->force_single_learner();
}

/**
 * Forces the purge log.
 *
 * @param local Whether the purge log is local.
 * @param index The index.
 * @return 0 on success, 1 otherwise.
 */
int xlog_force_purge_log(bool local, uint64_t index) {
  return komodo::XLog::instance().get_xlog_engine()->force_purge_log(local,
                                                                     index);
}

/**
 * Forces the fix match index.
 *
 * @param target_id The target id.
 * @param index The index.
 */
void xlog_force_fix_match_index(uint64_t target_id, uint64_t index) {
  komodo::XLog::instance().get_xlog_engine()->force_fix_match_index(
      target_id, index);
}

/**
 * Changes the leader.
 *
 * @param node_id The node id.
 * @return 0 on success, 1 otherwise.
 */
int xlog_change_leader(uint64_t node_id) {
  return komodo::XLog::instance().get_xlog_engine()->change_leader(node_id);
}

/**
 * Adds a follower.
 *
 * @param config The config.
 * @return 0 on success, 1 otherwise.
 */
int xlog_add_follower(std::string &config) {
  return komodo::XLog::instance().get_xlog_engine()->add_follower(config);
}

/**
 * Upgrades the learner.
 *
 * @param config The config.
 * @return 0 on success, 1 otherwise.
 */
int xlog_upgrade_learner(std::string &config) {
  return komodo::XLog::instance().get_xlog_engine()->upgrade_learner(config);
}

/**
 * Refreshes the learner meta.
 *
 * @param config The config.
 * @return 0 on success, 1 otherwise.
 */
int xlog_refresh_learner_meta(std::vector<std::string> &config) {
  return komodo::XLog::instance().get_xlog_engine()->refresh_learner_meta(
      config);
}

/**
 * Adds a learner.
 *
 * @param config The config.
 * @return 0 on success, 1 otherwise.
 */
int xlog_add_learner(std::vector<std::string> &config) {
  return komodo::XLog::instance().get_xlog_engine()->add_learner(config);
}

/**
 * Drops a learner.
 *
 * @param config The config.
 * @return 0 on success, 1 otherwise.
 */
int xlog_drop_learner(std::vector<std::string> &config) {
  return komodo::XLog::instance().get_xlog_engine()->drop_learner(config);
}

/**
 * Configures a member.
 *
 * @param server_id The server id.
 * @param force_sync Whether to force sync.
 * @param election_weight The election weight.
 * @return 0 on success, 1 otherwise.
 */
int xlog_configure_member(uint64_t server_id, bool force_sync,
                           uint election_weight) {
  return komodo::XLog::instance().get_xlog_engine()->configure_member(
      server_id, force_sync, election_weight);
}

/**
 * Configures members.
 *
 * @param server_ids The server ids.
 * @param force_syncs The force syncs.
 * @param election_weights The election weights.
 * @return 0 on success, 1 otherwise.
 */
int xlog_configure_members(std::vector<uint64_t> &server_ids,
                            std::vector<bool> &force_syncs,
                            std::vector<uint> &election_weights) {
  return komodo::XLog::instance().get_xlog_engine()->configure_members(
      server_ids, force_syncs, election_weights);
}

/**
 * Configures a learner.
 *
 * @param server_id The server id.
 * @param source_id The source id.
 * @param apply_mode The apply mode.
 * @return 0 on success, 1 otherwise.
 */
int xlog_configure_learner(uint64_t server_id, uint64_t source_id,
                            bool apply_mode) {
  return komodo::XLog::instance().get_xlog_engine()->configure_learner(
      server_id, source_id, apply_mode);
}

/**
 * Downgrades a member.
 *
 * @param addr The address.
 * @return 0 on success, 1 otherwise.
 */
int xlog_downgrade_member(const std::string &addr) {
  return komodo::XLog::instance().get_xlog_engine()->downgrade_member(addr);
}

/**
 * Sets the cluster id.
 *
 * @param id The id.
 * @return 0 on success, 1 otherwise.
 */
int xlog_set_cluster_id(uint64_t id) {
  return komodo::XLog::instance().get_xlog_engine()->set_cluster_id(id);
}

/**
 * Gets the size of the cluster.
 *
 * @return The size of the cluster.
 */
uint64_t xlog_get_cluster_size() {
  return komodo::XLog::instance().get_xlog_engine()->get_cluster_size();
}

/**
 * Gets the cluster global info.
 *
 * @param results The results.
 * @param mem_root The memory root.
 */
void xlog_get_cluster_global_info(
    std::vector<im::Consensus_show_global_result *> &results,
    MEM_ROOT *mem_root) {
  komodo::XLog::instance().get_xlog_engine()->get_cluster_global_info(results,
                                                               mem_root);
}

/**
 * Gets the local info.
 *
 * @param result The result.
 * @param mem_root The memory root.
 */
void xlog_get_local_info(im::Consensus_show_local_result &result,
                         MEM_ROOT *mem_root) {
  komodo::XLog::instance().get_xlog_engine()->get_local_info(result, mem_root);
}

/**
 * Fills the learner source.
 *
 * @param thd The thread.
 * @param table The table.
 * @return 0 on success, 1 otherwise.
 */
int xlog_fill_learner_source(THD *thd, TABLE *table) {
  return komodo::XLog::instance().get_xlog_engine()->fill_learner_source(thd,
                                                                         table);
}

/**
 * Fills the cluster health info.
 *
 * @param thd The thread.
 * @param table The table.
 * @return 0 on success, 1 otherwise.
 */
int xlog_fill_cluster_health_info(THD *thd, TABLE *table) {
  return komodo::XLog::instance().get_xlog_engine()->fill_cluster_health_info(
      thd, table);
}

/**
 * Fills the cluster info.
 *
 * @param thd The thread.
 * @param table The table.
 * @return 0 on success, 1 otherwise.
 */
int xlog_fill_cluster_info(THD *thd, TABLE *table) {
  return komodo::XLog::instance().get_xlog_engine()->fill_cluster_info(thd,
                                                                       table);
}

/**
 * Fills the local info.
 *
 * @param thd The thread.
 * @param table The table.
 * @return 0 on success, 1 otherwise.
 */
int xlog_fill_local_info(THD *thd, TABLE *table) {
  return komodo::XLog::instance().get_xlog_engine()->fill_local_info(thd,
                                                                     table);
}

/**
 * Fills the cluster status.
 *
 * @param thd The thread.
 * @param table The table.
 * @return 0 on success, 1 otherwise.
 */
int xlog_fill_cluster_status(THD *thd, TABLE *table) {
  return komodo::XLog::instance().get_xlog_engine()->fill_cluster_status(thd,
                                                                         table);
}

/**
 * Fills the cluster membership changes.
 *
 * @param thd The thread.
 * @param table The table.
 * @return 0 on success, 1 otherwise.
 */
int xlog_fill_cluster_membership_changes(THD *thd, TABLE *table) {
  return komodo::XLog::instance()
      .get_xlog_engine()
      ->fill_cluster_membership_changes(thd, table);
}

/**
 * Fills the ha history
 *
 * @param thd The thread.
 * @param table The table.
 * @return 0 on success, 1 otherwise.
 */
int xlog_fill_cluster_ha_history(THD *thd, TABLE *table) {
  return komodo::XLog::instance()
      .get_xlog_engine()
      ->fill_cluster_ha_history(thd, table);
}

/**
 * Purges the log.
 *
 * @param index The index.
 * @return 0 on success, 1 otherwise.
 */
int xlog_purge_log(uint64_t index) {
  return komodo::XLog::instance().get_xlog_engine()->purge_log(index);
}

/**
 * Forces the purge cache.
 *
 * @param index The index.
 * @return 0 on success, 1 otherwise.
 */
int xlog_force_purge_cache(uint64_t index) {
  return komodo::XLog::instance().get_xlog_engine()->force_purge_cache(index);
}

/**
 * Drops the prefetch channel.
 *
 * @param channel_id The channel id.
 * @return 0 on success, 1 otherwise.
 */
int xlog_drop_prefetch_channel(uint64_t channel_id) {
  return komodo::XLog::instance().get_xlog_engine()->drop_prefetch_channel(
      channel_id);
}

/**
 * Gets the paxos commit index.
 *
 * @return The paxos commit index.
 */
uint64_t xlog_get_paxos_commit_index() {
  return komodo::xlog_engine_get_protocol()->getCommitIndex();
}

/**
 * Gets the paxos term.
 *
 * @return The paxos term.
 */
uint64_t xlog_get_paxos_term() {
  return komodo::xlog_engine_get_protocol()->getTerm();
}

/**
 * Gets the paxos applied index.
 *
 * @return The paxos applied index.
 */
uint64_t xlog_get_paxos_applied_index() {
  return komodo::xlog_engine_get_protocol()->getAppliedIndex();
}

/**
 * Updates the paxos applied index.
 *
 * @param index The index.
 */
void xlog_update_paxos_applied_index(uint64_t index) {
  return komodo::xlog_engine_get_protocol()->updateAppliedIndex(index);
}

/**
 * Checks if the paxos is shutdown.
 *
 * @return true if the paxos is shutdown, false otherwise.
 */
bool xlog_is_paxos_shutdown() {
  return komodo::xlog_engine_get_protocol()->isShutdown();
}

/**
 * Checks the paxos commit index.
 *
 * @param term The term.
 * @return The paxos commit index.
 */
uint64_t xlog_check_paxos_commit_index(uint64_t term) {
  return komodo::xlog_engine_get_protocol()->checkCommitIndex(term);
}

/**
 * Gets the config server address.
 *
 * @param item The item.
 * @param addr The config server address.
 */
void xlog_get_config_server_addr(Item *item, std::string &addr) {
  komodo::XLog::instance().get_xlog_engine()->get_config_server_addr(item,
                                                                     addr);
}

/**
 * Gets the config server id.
 *
 * @param item The item.
 * @param server_id The server id.
 */
void xlog_get_config_server_id(Item *item, uint64_t &server_id) {
  komodo::XLog::instance().get_xlog_engine()->get_config_server_id(item,
                                                                   server_id);
}

/**
 * Checks if the consensus is enabled.
 *
 * @return true if the consensus is enabled, false otherwise.
 */
bool xlog_enable_consensus() {
  return dynamic_cast<Consensus_xlog_engine *>(
             komodo::XLog::instance().get_xlog_engine()) != nullptr;
}
}  // namespace komodo
