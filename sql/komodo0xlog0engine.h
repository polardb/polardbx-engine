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

/** @file sql/komodo0xlog0engine.h

  xlog behaviours related to komodo lizard system.

  Created 2023-06-20 by Jianhua.sjh
 *******************************************************/
#ifndef komodo0xlog0engine_h
#define komodo0xlog0engine_h

#include "sql/applied_index_checker.h"
#include "sql/consensus/consensus_common.h"
#include "sql/consensus_log_manager.h"
#include "sql/komodo0state0change0manager.h"
#include "sql/komodo0consensus0xlog.h"
#include "sql/serverless/komodo0switchover0manager.h"
#include "sql/consensus/komodo0fifo0cache.h"
#include "sql/komodo0flusher.h"

class THD;
class Binlog_event_writer;
class Gcn_log_event;
class binlog_cache_data;
class Consensus_log_event;
class Gtid_log_event;

namespace alisql {
class Autonomy;
class Protocol;
class Paxos;
class Server;
}  // namespace alisql

namespace komodo {
class Consensus_xlog_recovery;
class Consensus_xlog_handler;
class XLog_meta;
}  // namespace komodo

namespace komodo {

typedef Fifo_cache Consensus_cache_manager;

/**
 * @class XLog_engine
 * @brief Abstract base class for XLog engines.
 *
 * Provides the interface for log engine operations, including recovery,
 * consensus protocol, log management, and schema-related operations.
 * Derived classes implement specific behaviors for consensus and autonomy modes.
 */
class XLog_engine {
 public:
  explicit XLog_engine(MYSQL_BIN_LOG *binlog_arg);
  virtual ~XLog_engine();
  virtual int start_in_recovery(const std::function<int()> &) = 0;
  virtual int start_after_recovery() = 0;
  virtual void start_apply_threads_after_recovery() = 0;
  virtual int init_gtid_set_after_recovery() = 0;
  virtual void reset_event_scheduler() = 0;
  virtual void shutdown() = 0;

  /*--------------------- System Variable Related --------------------*/
  /* Only for xlog engine */
  virtual void set_consensus_msg_compress_option() = 0;
  virtual void set_consensus_flow_control() = 0;
  virtual void set_consensus_async() = 0;
  virtual void set_consensus_disable_election() = 0;

  /*--------------------- Protocol Related --------------------------*/
  virtual alisql::Protocol *get_protocol() = 0;
  virtual void set_last_non_commit_dep_index(uint64_t) = 0;
  virtual void write_log_done_internal(uint64_t, bool) = 0;
  virtual void write_cache_log_done() = 0;
  virtual uint64_t get_safety_index_for_purge() = 0;
  virtual void update_apply_thread_running(bool) = 0;
  virtual void update_apply_delay_seconds(uint64_t) = 0;
  virtual void replicate_empty_log() = 0;
  virtual uint64_t get_current_paxos_term() = 0;
  virtual bool is_leader_not_single() const = 0;

  /*------------------------ Binlog Related --------------------------*/
  virtual uint64_t get_event_timestamp() = 0;
  virtual void add_into_log_file_index(const uint64_t, const char *) = 0;
  virtual void truncate_before_from_log_file_index(const char *) = 0;
  virtual bool rpl_save_privilege_checks_user(Relay_log_info *,
                                                  const char *,
                                                  const char *) = 0;
  virtual int rotate_consensus_log() = 0;

  virtual void applied_index_checker_reset() = 0;
  virtual const char *applied_index_checker_get_group_queue_status() = 0;
  virtual void applied_index_check_prepare(THD *) = 0;
  virtual void update_applied_index_during_commit(uint64_t) = 0;
  virtual void update_logger_applied_index(uint64_t) = 0;
  virtual void sync_log_done(const THD *) = 0;
  virtual void wait_commit(THD *) = 0;
  virtual void rollback_with_flush_nothing(THD *, bool) = 0;
  virtual bool wait_commit_index_update(
      const THD *, my_off_t, uint64_t, std::chrono::nanoseconds, String &,
      const std::function<int()> &, const std::function<int(my_off_t)> &) = 0;
  virtual int check_term_during_flush(const uint64_t term) = 0;
  virtual int check_term_before_flush(THD *thd) = 0;
  virtual uint64_t get_handler_current_term() = 0;
  virtual uint64_t get_mock_start_index() = 0;
  virtual void add_log_to_cache(uint64, uint64, size_t, uchar *, bool,
                                uint) = 0;
  virtual TLog_flusher *get_tlog_flusher() = 0;

  /*---------------------- Command Check -----------------------------*/
  virtual bool need_block_command(THD *) = 0;

  /*-------------------- Consensus Proc Related ---------------------*/
  virtual int force_single_leader() = 0;
  virtual int force_single_learner() = 0;
  virtual int force_purge_log(bool, uint64_t) = 0;
  virtual void force_fix_match_index(uint64_t, uint64_t) = 0;
  virtual int change_leader(uint64_t) = 0;
  virtual int add_follower(std::string &) = 0;
  virtual int upgrade_learner(std::string &) = 0;
  virtual int refresh_learner_meta(std::vector<std::string> &) = 0;
  virtual int add_learner(std::vector<std::string> &) = 0;
  virtual int drop_learner(std::vector<std::string> &) = 0;
  virtual int configure_member(uint64_t, bool, uint) = 0;
  virtual int configure_members(std::vector<uint64_t> &, std::vector<bool> &,
                                 std::vector<uint> &) = 0;
  virtual int configure_learner(uint64_t, uint64_t, bool) = 0;
  virtual int downgrade_member(const std::string &) = 0;
  virtual int force_purge_cache(uint64_t) = 0;
  virtual int purge_log(uint64_t) = 0;
  virtual int drop_prefetch_channel(uint64_t) = 0;
  virtual int set_cluster_id(uint64_t) = 0;

  virtual uint64_t get_cluster_size() = 0;
  virtual void get_cluster_global_info(
      std::vector<im::Consensus_show_global_result *> &, MEM_ROOT *) = 0;
  virtual void get_local_info(im::Consensus_show_local_result &,
                              MEM_ROOT *) = 0;
  virtual void get_config_server_addr(Item *, std::string &) = 0;
  virtual void get_config_server_id(Item *, uint64_t &) = 0;

  /*----------------- Consensus Schema Related ------------------------*/
  virtual int fill_learner_source(THD *, TABLE *) = 0;
  virtual int fill_cluster_health_info(THD *, TABLE *) = 0;
  virtual int fill_cluster_info(THD *, TABLE *) = 0;
  virtual int fill_local_info(THD *, TABLE *) = 0;
  virtual int fill_cluster_status(THD *, TABLE *) = 0;
  virtual int fill_cluster_membership_changes(THD *, TABLE *) = 0;
  virtual int fill_cluster_ha_history(THD *, TABLE *) = 0;

  /*------------------- Getter Function ---------------------------------*/
  virtual Consensus_prefetch_manager *get_prefetch_manager() = 0;
  virtual Consensus_cache_manager *get_cache_manager() = 0;
  virtual Base_switchover_manager *get_switchover_manager() = 0;

 public:
  /* Consensus log manager responsible for managing consensus logs, binlogs, and
   * related operations. */
  Consensus_log_manager m_log_manager;

 private:
  XLog_engine(const XLog_engine &) = delete;
  XLog_engine &operator=(const XLog_engine &) = delete;
  XLog_engine(XLog_engine &&) = delete;
  XLog_engine &operator=(XLog_engine &&) = delete;
};

/**
 * @class Consensus_xlog_engine
 * @brief XLog engine implementation for consensus protocol.
 *
 * Implements the XLog engine interface for consensus-based replication,
 * including Paxos protocol integration, log management, and cluster operations.
 */
class Consensus_xlog_engine final : public XLog_engine {
 public:
  Consensus_xlog_engine(XLog_meta *meta, Consensus_xlog_recovery *recovery,
                        MYSQL_BIN_LOG *binlog, const bool is_logger);

  virtual ~Consensus_xlog_engine();

  int start_in_recovery(const std::function<int()> &) override;
  int start_after_recovery() override;
  void start_apply_threads_after_recovery() override;
  int init_gtid_set_after_recovery() override;
  void reset_event_scheduler() override;
  void shutdown() override;

  /*--------------------- System Variable Related --------------------*/
  /* Only for xlog engine */
  void set_consensus_msg_compress_option() override;
  void set_consensus_flow_control() override;
  void set_consensus_async() override;
  void set_consensus_disable_election() override;

  /*--------------------- Protocol Related --------------------------*/
  alisql::Protocol *get_protocol() override;
  void set_last_non_commit_dep_index(uint64_t logIndex) override;
  void write_log_done_internal(uint64_t logIndex, bool forceSend) override;
  void write_cache_log_done() override;
  uint64_t get_safety_index_for_purge() override;
  void update_apply_thread_running(bool is_running) override;
  void update_apply_delay_seconds(uint64_t sec) override;
  void replicate_empty_log() override;
  uint64_t get_current_paxos_term() override;
  bool is_leader_not_single() const override;

  /*------------------------ Binlog Related --------------------------*/
  uint64_t get_event_timestamp() override {
    return m_log_manager.get_event_timestamp();
  }
  void add_into_log_file_index(const uint64_t timestamp,
                               const char *log_file_name) override;
  void truncate_before_from_log_file_index(const char *log_file_name) override;
  bool rpl_save_privilege_checks_user(Relay_log_info *rpl_info,
                                          const char *username,
                                          const char *hostname) override;
  int rotate_consensus_log() override;
  void applied_index_checker_reset() override {
    m_applied_index_checker.reset();
  }
  const char *applied_index_checker_get_group_queue_status() override {
    return m_applied_index_checker.get_group_queue_status();
  }
  void applied_index_check_prepare(THD *final_queue) override;
  void update_applied_index_during_commit(uint64_t index) override;
  void update_logger_applied_index(uint64_t index) override;
  void sync_log_done(const THD *head) override;
  void wait_commit(THD *thd) override;
  void rollback_with_flush_nothing(THD *thd, bool is_my_thd) override;
  bool wait_commit_index_update(
      const THD *thd, my_off_t log_pos, uint64_t index,
      std::chrono::nanoseconds heartbeat_period, String &packet,
      const std::function<int()> &flush_net,
      const std::function<int(my_off_t)> &send_heartbeat_event) override;
  int check_term_during_flush(const uint64_t term) override;
  int check_term_before_flush(THD *thd) override;
  uint64_t get_handler_current_term() override;
  uint64_t get_mock_start_index() override;
  void add_log_to_cache(uint64 term, uint64 index, size_t buf_size,
                        uchar *buffer, bool outer, uint flag) override;
  virtual TLog_flusher *get_tlog_flusher() override { return m_flusher.get(); }

  /*---------------------- Command Check ------------------------------*/
  bool need_block_command(THD *thd) override;

  /*------------- Consensus Proc Related ----------------------------*/
  int force_single_leader() override;
  int force_single_learner() override;
  int force_purge_log(bool local, uint64_t index) override;
  void force_fix_match_index(uint64_t target_id, uint64_t index) override;
  int change_leader(uint64_t node_id) override;
  int add_follower(std::string &config) override;
  int upgrade_learner(std::string &config) override;
  int refresh_learner_meta(std::vector<std::string> &config) override;
  int add_learner(std::vector<std::string> &config) override;
  int drop_learner(std::vector<std::string> &config) override;
  int configure_member(uint64_t server_id, bool force_sync,
                        uint election_weight) override;
  int configure_members(std::vector<uint64_t> &server_ids,
                         std::vector<bool> &force_syncs,
                         std::vector<uint> &election_weights) override;
  int configure_learner(uint64_t server_id, uint64_t source_id,
                         bool apply_mode) override;
  int downgrade_member(const std::string &addr) override;
  int set_cluster_id(uint64_t id) override;
  uint64_t get_cluster_size() override;
  void get_cluster_global_info(
      std::vector<im::Consensus_show_global_result *> &results,
      MEM_ROOT *mem_root) override;
  void get_local_info(im::Consensus_show_local_result &result,
                      MEM_ROOT *mem_root) override;
  int force_purge_cache(uint64_t index) override;
  int purge_log(uint64_t index) override;
  int drop_prefetch_channel(uint64_t channel_id) override;

  void get_config_server_addr(Item *item, std::string &ret) override;
  void get_config_server_id(Item *item, uint64_t &ret) override;

  /*----------------- Consensus Schema Related ----------------------*/
  int fill_learner_source(THD *thd, TABLE *table) override;
  int fill_cluster_health_info(THD *thd, TABLE *table) override;
  int fill_cluster_info(THD *thd, TABLE *table) override;
  int fill_local_info(THD *thd, TABLE *table) override;
  int fill_cluster_status(THD *thd, TABLE *table) override;
  int fill_cluster_membership_changes(THD *thd, TABLE *table) override;
  int fill_cluster_ha_history(THD *thd, TABLE *table) override;

  /*------------------- Getter Function ---------------------------------*/
  Consensus_prefetch_manager *get_prefetch_manager() override {
    return &m_prefetch_manager;
  }
  Consensus_cache_manager *get_cache_manager() override {
    return m_cache_manager;
  }
  Base_switchover_manager *get_switchover_manager() override {
    return &m_switchover_manager;
  }

 private:
  void create_paxos();
  int start_paxos(const bool is_learner);
  void wait_commit_index_in_recovery();
  int handle_pending_recovering_trx();

  /* Handler for consensus xlog operations, manages log application and protocol
   * interaction. */
  std::shared_ptr<Consensus_xlog_handler> m_xlog_handler;

  /* Paxos protocol instance for consensus operations. */
  std::shared_ptr<alisql::Paxos> m_paxos;

  /* Applied index checker for tracking and verifying applied log indexes. */
  Applied_index_checker m_applied_index_checker;

  /* Manager for prefetching consensus logs to improve performance. */
  Consensus_prefetch_manager m_prefetch_manager;

  /* Manager for FIFO cache of consensus logs.*/
  Consensus_cache_manager *m_cache_manager;

  /* Log state manager for handling consensus protocol role transitions. */
  State_change_manager m_state_change_manager;

  /* Switchover manager for handling role transitions and failover. */
  Consensus_switchover_manager m_switchover_manager;

  /* Metadata for the XLog engine, including cluster and recovery information.
   */
  XLog_meta *m_meta;

  /* Recovery manager for consensus xlog recovery operations. */
  Consensus_xlog_recovery *m_recovery;

  /* Indicates if this node is a logger node (true) or not (false).*/
  bool m_is_logger;

  std::unique_ptr<Consensus_log_flusher> m_flusher;
};

/**
 * @class Autonomy_xlog_engine
 * @brief XLog engine implementation for autonomy mode (binlog).
 *
 * Implements the XLog engine interface for autonomy (binlog) mode,
 * providing basic log management and binlog operations.
 */
class Autonomy_xlog_engine final : public XLog_engine {
 public:
  explicit Autonomy_xlog_engine(MYSQL_BIN_LOG *binlog);
  virtual ~Autonomy_xlog_engine() = default;
  int start_in_recovery(const std::function<int()> &) override;
  int start_after_recovery() override;
  void start_apply_threads_after_recovery() override;
  int init_gtid_set_after_recovery() override;
  void reset_event_scheduler() override {}
  void shutdown() override { m_log_manager.stop_consensus_commit_pos_watcher(); }

  /*--------------------- System Variable Related --------------------*/
  void set_consensus_msg_compress_option() override {}
  void set_consensus_flow_control() override {}
  void set_consensus_async() override {}
  void set_consensus_disable_election() override {}

  /*--------------------- Protocol Related --------------------------*/
  alisql::Protocol *get_protocol() override;
  void set_last_non_commit_dep_index(uint64_t) override {}
  void write_log_done_internal(uint64_t, bool) override {}
  void write_cache_log_done() override {}
  uint64_t get_safety_index_for_purge() override { return 0; }
  void update_apply_thread_running(bool) override {}
  void update_apply_delay_seconds(uint64_t) override {}
  void replicate_empty_log() override {}
  uint64_t get_current_paxos_term() override { return 0; }
  bool is_leader_not_single() const override { return false; }

  /*------------------------ Binlog Related --------------------------*/
  uint64_t get_event_timestamp() override { return 0; }
  void add_into_log_file_index(const uint64_t, const char *) override {}
  void truncate_before_from_log_file_index(const char *) override {}
  bool rpl_save_privilege_checks_user(Relay_log_info *, const char *,
                                          const char *) override {
    return false;
  }
  int rotate_consensus_log() override;
  void applied_index_checker_reset() override {}
  const char *applied_index_checker_get_group_queue_status() override {
    return nullptr;
  }
  void applied_index_check_prepare(THD *) override {}
  void update_applied_index_during_commit(uint64_t) override {}
  void update_logger_applied_index(uint64_t) override {}
  void sync_log_done(const THD *) override {}
  void wait_commit(THD *) override {}
  void rollback_with_flush_nothing(THD *, bool) override {}
  bool wait_commit_index_update(const THD *, my_off_t, uint64_t,
                                std::chrono::nanoseconds, String &,
                                const std::function<int()> &,
                                const std::function<int(my_off_t)> &) override {
    return false;
  }
  int check_term_during_flush(const uint64_t) override { return 0; }
  int check_term_before_flush(THD *) override { return 0; }
  uint64_t get_handler_current_term() override { return 0; }
  uint64_t get_mock_start_index() override { return 0; }
  void add_log_to_cache(uint64, uint64, size_t, uchar *, bool, uint) override {
    return;
  }
  virtual TLog_flusher *get_tlog_flusher() override { return m_flusher.get(); }

  /*---------------------- Command Check -----------------------------*/
  bool need_block_command(THD *) override { return false; }

  /*-------------------- Consensus Proc Related ---------------------*/
  int force_single_leader() override { return 0; }
  int force_single_learner() override { return 0; }
  int force_purge_log(bool, uint64_t) override { return 0; }
  void force_fix_match_index(uint64_t, uint64_t) override {}
  int change_leader(uint64_t) override { return 0; }
  int add_follower(std::string &) override { return 0; }
  int upgrade_learner(std::string &) override { return 0; }
  int refresh_learner_meta(std::vector<std::string> &) override {
    return 0;
  }
  int add_learner(std::vector<std::string> &) override { return 0; }
  int drop_learner(std::vector<std::string> &) override { return 0; }
  int configure_member(uint64_t, bool, uint) override { return 0; }
  int configure_members(std::vector<uint64_t> &, std::vector<bool> &,
                         std::vector<uint> &) override {
    return 0;
  }
  int configure_learner(uint64_t, uint64_t, bool) override { return 0; }
  int downgrade_member(const std::string &) override { return 0; }
  int force_purge_cache(uint64_t) override { return 0; }
  int purge_log(uint64_t) override { return 0; }
  int drop_prefetch_channel(uint64_t) override { return 0; }
  int set_cluster_id(uint64_t) override { return 0; }
  uint64_t get_cluster_size() override { return 0; }
  void get_cluster_global_info(std::vector<im::Consensus_show_global_result *> &,
                        MEM_ROOT *) override {}
  void get_local_info(im::Consensus_show_local_result &, MEM_ROOT *) override {}
  void get_config_server_addr(Item *, std::string &) override {}
  void get_config_server_id(Item *, uint64_t &) override {}

  /*----------------- Consensus Schema Related ------------------------*/
  int fill_learner_source(THD *, TABLE *) override { return 0; }
  int fill_cluster_health_info(THD *, TABLE *) override { return 0; }
  int fill_cluster_info(THD *, TABLE *) override { return 0; }
  int fill_local_info(THD *, TABLE *) override { return 0; }
  int fill_cluster_status(THD *, TABLE *) override { return 0; }
  int fill_cluster_membership_changes(THD *, TABLE *) override { return 0; }
  int fill_cluster_ha_history(THD *, TABLE *) override { return 0; }

  /*------------------- Getter Function ---------------------------------*/
  Consensus_prefetch_manager *get_prefetch_manager() override {
    return nullptr;
  }
  Consensus_cache_manager *get_cache_manager() override {
    return nullptr;
  }
  Base_switchover_manager *get_switchover_manager() override {
    return &m_switchover_manager;
  }

 private:
  /* Pointer to the autonomy protocol instance for non-consensus (binlog)
   * mode.*/
  std::shared_ptr<alisql::Autonomy> m_autonomy;

  /* Switchover manager for handling role transitions and failover. */
  Autonomy_switchover_manager m_switchover_manager;

  std::unique_ptr<Binary_log_flusher> m_flusher;
};

}  // namespace komodo

#endif
