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

/** @file sql/komodo0xlog.h

  xlog behaviours related to komodo lizard system.

  Created 2023-06-20 by Jianhua.sjh
 *******************************************************/

#include "scope_guard.h"
#include "sql/binlog/recovery.h"
#include "sql/consensus/consensus_common.h"
#include "sql/consensus_log_manager.h"

#ifndef komodo0xlog_h
#define komodo0xlog_h

class THD;
class MYSQL_BIN_LOG;

namespace alisql {
class Protocol;
}

namespace komodo {
class XLog_meta;
class XLog_engine;

class Consensus_logindex_manager;
class Consensus_prefetch_manager;
class Consensus_log_manager;
class Base_switchover_manager;

class TLog_flusher;

class XLog {
 public:
  bool create(const bool initialize, const bool force_recover,
              const bool enable_consensus, const bool bin_log,
              const bool learner_node, const bool logger_node,
              MYSQL_BIN_LOG *binlog_arg);
  int start_in_recovery(const std::function<int()> &);
  int start_after_recovery();
  void start_apply_threads_after_recovery();
  int init_gtid_set_after_recovery();
  void shutdown();
  void destroy();
  static XLog &instance();

  XLog_meta *get_xlog_meta() { return m_xlog_meta; }
  XLog_engine *get_xlog_engine() { return m_xlog_engine; }
  binlog::Binlog_recovery *get_xlog_recovery() { return m_xlog_recovery; }

 private:
  // Disable direct instantiation. Use the method init() instead.
  XLog() = default;
  XLog(const XLog &) = delete;
  XLog &operator=(const XLog &) = delete;
  XLog(XLog &&) = delete;
  XLog &operator=(XLog &&) = delete;

 private:
  XLog_meta *m_xlog_meta{nullptr};
  XLog_engine *m_xlog_engine{nullptr};
  binlog::Binlog_recovery *m_xlog_recovery{nullptr};
};

/*---------------------- XLog Meta Related ---------------------- */
void xlog_meta_try_init_consensus_info();
Consensus_info *xlog_meta_get_consensus_info();

/*-------------------- XLog Engine Related --- ------------------ */
void xlog_sync_log_done(const THD *head);
void xlog_set_last_non_commit_dep_index(uint64_t logIndex);
void xlog_write_log_done_internal(uint64_t logIndex, bool forceSend);
void xlog_write_cache_log_done();

/*---------------- Consensus log manager related ---------------- */
uint64_t xlog_log_manager_get_event_timestamp();
void xlog_add_into_log_file_index(const uint64_t timestamp,
                                  const char *log_file_name);
void xlog_log_manager_truncate_before_from_log_file_index(
    const char *log_file_name);
void xlog_recovery_set_last_leader_term_index(const uint64_t index);
uint64_t xlog_recovery_get_last_leader_term_index();
uint64_t xlog_log_manager_get_current_index();
uint64_t xlog_engine_get_sync_index();
IO_CACHE *xlog_log_manager_get_io_cache();
mysql_mutex_t *xlog_log_manager_get_term_lock();
mysql_mutex_t *xlog_log_manager_get_sequence_stage1_lock();
int xlog_engine_check_term_during_flush(const uint64_t term);
int xlog_engine_check_term_before_flush(THD *thd);
uint64_t xlog_engine_get_handler_current_term();
uint64_t xlog_engine_get_mock_start_index();
void xlog_engine_add_log_to_cache(uint64 term, uint64 index, size_t buf_size,
                                  uchar *buffer, bool outer, uint flag);
void xlog_log_manager_rdlock_consensus_status();
void xlog_log_manager_unlock_consensus_status();
bool xlog_engine_leader_role();
bool xlog_engine_binlog_working();
Consensus_logindex_manager *xlog_engine_get_log_file_index();
Consensus_prefetch_manager *xlog_engine_get_prefetch_manager();
Consensus_cache_manager *xlog_engine_get_cache_manager();
Consensus_log_manager &xlog_engine_get_log_manager();
Base_switchover_manager *xlog_engine_get_switchover_manager();
alisql::Protocol *xlog_engine_get_protocol();
bool xlog_engine_rpl_save_privilege_checks_user(Relay_log_info *rpl_info,
                                                     const char *username,
                                                     const char *hostname);
void xlog_engine_reset_event_scheduler();
void xlog_engine_applied_index_check_prepare(THD *final_queue);
int xlog_engine_check_wait_paxos_commit(THD *thd, const bool is_my_thd);
const char *xlog_engine_applied_index_checker_get_group_queue_status();
int xlog_engine_rotate_consensus_log();
void xlog_engine_call_function(void (XLog_engine::*func)());
void xlog_engine_set_cache_mgr_function(void (Consensus_cache_manager::*func)(uint64_t), uint64_t num);

/*--------------------- XLog Recovery Related --------------------- */
void xlog_recovery_set_last_leader_term_index(const uint64_t index);
uint64_t xlog_recovery_get_last_leader_term_index();

void xlog_recovery_set_reader(Binlog_file_reader *binlog_file_reader);
binlog::Binlog_recovery *xlog_recovery_get_recovery();
void xlog_recovery_add_internal_pending_recovering_trx(
    handlerton &ht, const XA_recover_txn *xa_trx,
    const komodo::XA_specification &xa_spec);
void xlog_recovery_add_external_pending_recovering_trx(
    handlerton &ht, enum_ha_recover_xa_state current_state,
    enum_ha_recover_xa_state next_state, const XA_recover_txn *xa_trx,
    const komodo::XA_specification &xa_spec);

/*----------------------- XLog Related ---------------------------- */
bool xlog_create(const bool initialize, const bool force_recover,
                 const bool enable_consensus, const bool bin_log,
                 const bool learner_node, const bool logger_node,
                 MYSQL_BIN_LOG *binlog_arg);
bool xlog_start_in_recovery(const std::function<int()> &);
bool xlog_start_after_recovery();
void xlog_start_apply_threads_after_recovery();
int xlog_init_gtid_set_after_recovery();
void xlog_destroy();
void xlog_shutdown();

void xlog_set_last_non_commit_dep_index(uint64_t logIndex);
void xlog_write_log_done_internal(uint64_t logIndex, bool forceSend);
void xlog_write_cache_log_done();

void xlog_add_into_log_file_index(const uint64_t timestamp,
                                  const char *log_file_name);

uint64_t xlog_get_safety_index_for_purge();
void xlog_update_apply_thread_running(bool is_running);
void xlog_update_apply_delay_seconds(uint64_t sec);
void xlog_replicate_empty_log();
bool xlog_is_leader_not_single();
uint64_t xlog_engine_get_paxos_term();
bool xlog_need_block_command(THD *thd);
void xlog_update_applied_index_during_finish_commit(uint64_t index);
void xlog_update_logger_applied_index(uint64_t index);
int xlog_wait_commit_index_update(
    const THD *thd, my_off_t log_pos, uint64_t index,
    std::chrono::nanoseconds heartbeat_period, String &packet,
    const std::function<int()> &flush_net,
    const std::function<int(my_off_t)> &send_heartbeat_event);
bool xlog_flush_logs(THD *thd, Gtid_log_event *gtid_event,
                     binlog_cache_data *cache_data,
                     Binlog_event_writer *writer);

/*---------------------- Conensus Proc Related ------------------- */
/** All functions below are about Consensus Proc. Acturally these procs should
 * not be exposed in autonomy mode. However, we have to do so because of the
 * restriction of Proc Implementation. */
int xlog_force_single_leader();
int xlog_force_single_learner();
int xlog_force_purge_log(bool local, uint64_t index);
void xlog_force_fix_match_index(uint64_t target_id, uint64_t index);
int xlog_change_leader(uint64_t node_id);
int xlog_add_follower(std::string &config);
int xlog_upgrade_learner(std::string &config);
int xlog_refresh_learner_meta(std::vector<std::string> &info_vector);
int xlog_add_learner(std::vector<std::string> &config);
int xlog_drop_learner(std::vector<std::string> &config);
int xlog_configure_member(uint64_t server_id, bool force_sync,
                           uint election_weight);
int xlog_configure_members(std::vector<uint64_t> &server_ids,
                            std::vector<bool> &force_syncs,
                            std::vector<uint> &election_weights);
int xlog_configure_learner(uint64_t server_id, uint64_t source_id,
                            bool apply_mode);
int xlog_downgrade_member(const std::string &addr);
int xlog_purge_log(uint64_t index);
int xlog_force_purge_cache(uint64_t index);
int xlog_drop_prefetch_channel(uint64_t channel_id);
void xlog_get_config_server_addr(Item *item, std::string &addr);
void xlog_get_config_server_id(Item *item, uint64_t &id);

int xlog_set_cluster_id(uint64_t id);
uint64_t xlog_get_cluster_size();
void xlog_get_cluster_global_info(
    std::vector<im::Consensus_show_global_result *> &results,
    MEM_ROOT *mem_root);
void xlog_get_local_info(im::Consensus_show_local_result &result,
                         MEM_ROOT *mem_root);

/*----------------------- Conensus Schema Related -------------------- */
/** All functions below are about Consensus Schema. Acturally, these tables
 * should not be exposed in autonomy mode. */
int xlog_fill_learner_source(THD *thd, TABLE *table);
int xlog_fill_cluster_health_info(THD *thd, TABLE *table);
int xlog_fill_cluster_info(THD *thd, TABLE *table);
int xlog_fill_local_info(THD *thd, TABLE *table);
int xlog_fill_cluster_status(THD *thd, TABLE *table);
int xlog_fill_cluster_membership_changes(THD *thd, TABLE *table);
int xlog_fill_cluster_ha_history(THD *thd, TABLE *table);

/*------------------------- Other Things ------------------------------ */
uint64_t xlog_get_paxos_commit_index();
uint64_t xlog_get_paxos_term();
uint64_t xlog_get_paxos_applied_index();
void xlog_update_paxos_applied_index(uint64_t index);
bool xlog_is_paxos_shutdown();
uint64_t xlog_check_paxos_commit_index(uint64_t term);
bool xlog_enable_consensus();

}  // namespace komodo

#define GUARDED_READ_CONSENSUS_LOG()                                \
  auto consensus_guard = create_lock_guard(                         \
      [&] { komodo::xlog_log_manager_rdlock_consensus_status(); },  \
      [&] { komodo::xlog_log_manager_unlock_consensus_status(); }); \
  MYSQL_BIN_LOG *consensus_log =                                    \
      komodo::xlog_engine_get_log_manager().get_status() ==         \
              komodo::Consensus_Log_State::BINLOG_WORKING   \
          ? &mysql_bin_log                                          \
          : &komodo::xlog_engine_get_log_manager()                  \
                 .get_relay_log_info()                              \
                 ->relay_log;

#endif
