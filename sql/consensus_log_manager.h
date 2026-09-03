/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

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

#ifndef CONSENSUS_LOG_MANAGER_INCLUDE
#define CONSENSUS_LOG_MANAGER_INCLUDE

#include <atomic>
#include <iterator>
#include <map>
#include <queue>
#include <vector>
#include "basic_ostream.h"
#include "binlog.h"
#include "consensus_cache_manager.h"
#include "consensus_info.h"
#include "consensus_logindex_manager.h"
#include "consensus_prefetch_manager.h"
#include "my_macros.h"
#include "sql/applied_index_checker.h"
#include "sql/rpl_rli.h"
#include <include/scope_guard.h>
#include "sql/consensus/komodo0fifo0cache.h"


#define CACHE_BUFFER_SIZE (IO_SIZE * 16)

#define CLUSTER_INFO_EXTRA_LENGTH \
  27  // LOG_EVENT_HEADER_LEN + POST_HEADER_LENGTH + BINLOG_CHECKSUM_LEN

class MYSQL_BIN_LOG;

namespace komodo {

class Consensus_recovery_manager;

enum Consensus_Log_State { RELAY_LOG_WORKING = 0, BINLOG_WORKING = 1 };

class ConsensusCommitPos {
 public:
  ConsensusCommitPos() : fname(""), pos(0), index(0) {}

 private:
  std::string fname;
  uint64_t pos;
  uint64_t index;

  friend class Consensus_log_manager;
};

class Consensus_log_manager {
 public:
  Consensus_log_manager();
  ~Consensus_log_manager();
  int init(uint64 fake_current_index = 0);
  int create_rpl_info();
  int check_force_change_meta(const bool force_learner);
  int cleanup();

  // class args
  Relay_log_info *get_relay_log_info() { return rli_info; }
  IO_CACHE *get_cache();
  MYSQL_BIN_LOG::Binlog_ofile *get_log_file() { return cache_log.get(); }
  Consensus_Log_State get_status() { return status; }
  uint64 get_current_index() { return current_index; }
  uint64 get_apply_index() { return apply_index; }
  uint64 get_real_apply_index() { return real_apply_index; }
  uint64 get_apply_index_end_pos() { return apply_index_end_pos; }
  uint64 get_apply_index_current_pos() { return apply_index_current_pos; }
  uint64 get_apply_term() { return apply_term; }
  uint64 get_apply_ev_sequence() { return apply_ev_seq; }
  uint64 get_stop_term() { return stop_term; }
  uint64 get_apply_ev_finish_count() { return apply_ev_finish_count; }
  bool is_trx_apply_finished() { return apply_ev_finish_count == apply_ev_seq; }
  bool get_in_large_trx_applying() { return in_large_trx_applying; }
  bool get_in_large_trx_appending() { return in_large_trx_appending; }
  bool get_in_large_event_appending() { return in_large_event_appending; }
  bool get_enable_rotate() { return enable_rotate; }

  Consensus_logindex_manager *get_log_file_index() { return log_file_index; }
  uint64_t get_mock_start_index(const uint64_t consensus_start_index) const {
    return std::max(log_file_index->get_first_index(), consensus_start_index);
  }
  mysql_mutex_t *get_sequence_stage1_lock() {
    return &LOCK_consensuslog_sequence_stage1;
  }
  mysql_mutex_t *get_term_lock() { return &LOCK_consensuslog_term; }
  mysql_mutex_t *get_apply_thread_lock() {
    return &LOCK_consensuslog_apply_thread;
  }
  mysql_cond_t *get_catchup_cond() { return &COND_consensuslog_catchup; }

  uint64 get_current_term() { return current_term; }

  std::string get_empty_log();

  void set_binlog(MYSQL_BIN_LOG *binlog_arg) { binlog = binlog_arg; }
  void set_relay_log_info(Relay_log_info *rli_info_arg) {
    rli_info = rli_info_arg;
  }
  void set_consensus_system_status(Consensus_Log_State status_arg) {
    status = status_arg;
  }
  void set_current_index(uint64 current_index_arg) {
    current_index = current_index_arg;
  }
  void set_apply_index(uint64 apply_index_arg) {
    apply_index = apply_index_arg;
  }
  void set_real_apply_index(uint64 real_apply_index_arg) {
    real_apply_index = real_apply_index_arg;
  }
  void set_apply_index_end_pos(uint64 apply_index_end_pos_arg) {
    apply_index_end_pos = apply_index_end_pos_arg;
  }
  void set_apply_index_current_pos(uint64 apply_index_current_pos_arg) {
    apply_index_current_pos = apply_index_current_pos_arg;
  }
  void set_apply_term(uint64 apply_term_arg) { apply_term = apply_term_arg; }
  void set_apply_ev_sequence(uint64 apply_ev_seq_arg) {
    apply_ev_seq = apply_ev_seq_arg;
    apply_ev_finish_count = apply_ev_seq_arg;
  }
  void set_apply_catchup(uint apply_catchup_arg) {
    apply_catchup = apply_catchup_arg;
  }
  void set_in_large_trx_applying(bool in_large_trx_arg) {
    in_large_trx_applying = in_large_trx_arg;
  }
  void set_in_large_trx_appending(bool in_large_trx_arg) {
    in_large_trx_appending = in_large_trx_arg;
  }
  void set_in_large_event_appending(bool in_large_event_arg) {
    in_large_event_appending = in_large_event_arg;
  }

  void set_current_term(uint64 current_term_arg) {
    current_term = current_term_arg;
  }

  uint64 fetch_and_inc_current_index() { return current_index++; }
  void incr_apply_ev_sequence() { apply_ev_seq++; }
  void incr_apply_ev_finish_count() { apply_ev_finish_count++; }
  uint64 get_cache_index();
  void set_cache_index(uint64 cache_index_arg);
  uint64 get_sync_index(bool serious = false);
  uint64 get_final_sync_index();
  uint64 get_final_sync_index_no_lock();

  void set_sync_index(uint64 sync_index_arg);
  void set_sync_index_if_greater(uint64 sync_index_arg);
  void set_enable_rotate(bool arg) { enable_rotate = arg; }
  Consensus_info *get_consensus_info() { return consensus_info; }

  void set_consensus_info(Consensus_info *ptr) {
    consensus_info = ptr;
  }
  Consensus_recovery_manager *get_recovery_manager() {
    return recovery_manager;
  }
  void set_recovery_manager(Consensus_recovery_manager *ptr) {
    recovery_manager = ptr;
  }
  Consensus_cache_manager *get_fifo_cache() {
    return cache_manager;
  }
  void set_fifo_cache_manager(Consensus_cache_manager *arg) {
    cache_manager = arg;
  }

  void set_prefetch_manager(Consensus_prefetch_manager *arg) {
    prefetch_manager = arg;
  }

  void set_applied_index_checker(Applied_index_checker *checker) {
    m_applied_index_checker = checker;
  }

  // for concurrency
  inline void rdlock_consensus_status() { mysql_rwlock_rdlock(&LOCK_consensuslog_status); }
  inline void unlock_consensus_status() { mysql_rwlock_unlock(&LOCK_consensuslog_status); }

  int set_start_apply_index_if_need(uint64 consensus_index);
  int set_start_apply_term_if_need(uint64 consensus_term);

  // for log operation
  int write_log_entry(ConsensusLogEntry &log, uint64 *consensus_index,
                      bool with_check = false);
  int write_log_entries(std::vector<ConsensusLogEntry> &logs,
                        uint64 *max_index);
  int get_log_entry(uint64 channel_id, uint64 consensus_index,
                    uint64 *consensus_term, std::string &log_content,
                    bool *outer, uint *flag, bool fast_fail);
  int get_log_directly(uint64 consensus_index, uint64 *consensus_term,
                       std::string &log_content, bool *outer, uint *flag,
                       bool need_content = true);
  uint64_t get_left_log_size(uint64 start_log_index, uint64 max_packet_size);
  int prefetch_log_directly(THD *thd, uint64 channel_id,
                            uint64 consensus_index);
  int get_log_position(uint64 consensus_index, bool need_lock, char *log_name,
                       uint64 *pos);
  uint64 get_next_trx_index(uint64 consensus_index, bool enable_retry = true);
  uint32 serialize_cache(uchar **buffer);
  int truncate_log(uint64 consensus_index);
  int purge_log(uint64 consensus_index);

  uint64 get_exist_log_length();

  uint get_atomic_logging_flag() { return atomic_logging_flag; }
  void set_new_atomic_logging_flag() {
    atomic_logging_flag = 3 - atomic_logging_flag;
  }

  void force_update_applied_index(const uint64_t index);

  int wait_leader_degraded(uint64 term, uint64 index);
  int wait_follower_upgraded(uint64 term, uint64 index);
  int wait_follower_change_term(uint64 term);

  int start_consensus_commit_pos_watcher();
  void stop_consensus_commit_pos_watcher();
  void update_commit_pos(const std::string &log_name, uint64_t pos,
                         uint64_t index);
  uint64_t get_commit_pos_index() { return commit_pos.index; }
  uint64_t get_commit_pos_position() { return commit_pos.pos; }
  void get_commit_pos(char *const fname_ptr, uint64_t *pos_ptr,
                      uint64_t *index_ptr);
  bool is_state_machine_ready();
  void set_event_timestamp(uint32 t) { ev_tt_.store(t); }
  uint32 get_event_timestamp() { return ev_tt_.load(); }

  int check_empty_binlog();
  int dump_consensus_info();

  /** TODO: refactor later */
  void set_consensus_logger(bool is_logger) { m_is_logger = is_logger; }

 private:
  void wait_replay_log_finished();
  void wait_apply_threads_start();
  void wait_apply_threads_stop();
  int dump_cluster_info_to_file(std::string meta_file_name,
                                std::string output_str);

 private:
  bool inited;
  PSI_rwlock_key key_LOCK_consensuslog_status;
  PSI_mutex_key key_LOCK_consensuslog_sequence_stage1;
  PSI_mutex_key key_LOCK_consensuslog_term;
  PSI_mutex_key key_LOCK_consensuslog_apply_thread;
//  PSI_mutex_key key_LOCK_log_state_change;
  PSI_thread_key consensus_thread_key;
  PSI_cond_key key_COND_consensuslog_catchup;
//  PSI_cond_key key_COND_log_state_change;
  mysql_rwlock_t LOCK_consensuslog_status;  // used to protect the status of
                                            // consensus log module
  mysql_mutex_t
      LOCK_consensuslog_sequence_stage1;  // used to get term in replicatelog
  mysql_mutex_t LOCK_consensuslog_term;   // use to protect setTerm
  mysql_mutex_t LOCK_consensuslog_apply_thread;
  mysql_cond_t COND_consensuslog_catchup;  // used to point out whether apply
                                           // thread arrived commit index

  std::atomic<uint64>
      current_index;  // last log index in the log system, protected by LOCK_log
  std::atomic<uint64> cache_index;  // used to tell last cache log entry
  std::atomic<uint64> sync_index;   // used to tell last log entry
  std::atomic<uint64> stop_term;  // used to mark sql thread coordinator stop condition
  uint64 apply_index;  // used to record sql thread coordinator apply index
  uint64 real_apply_index;     // for large trx
  uint64 apply_index_end_pos;  // used to record sql thread
                               // coordinator apply index end pos
  uint64 apply_index_current_pos;  // used to record sql thread coordinator apply
                                   // index current pos
  uint64 apply_term;  // used to record sql thread coordinator apply term
  uint64 apply_ev_seq;  // used to record sql thread coordinator
                        // apply event sequence in one index
  uint64 apply_ev_finish_count;
  std::atomic<uint64> current_term;  // record the current system term, changed
                                     // by stageChange callback
  std::atomic<bool> in_large_trx_applying;
  std::atomic<bool>
      enable_rotate;  // do not rotate if last log is in middle of large trx
  std::atomic<bool> in_large_trx_appending;
  std::atomic<bool> in_large_event_appending;

  Consensus_logindex_manager *log_file_index;              // consensus log file index

  std::map<uint64, uint64>
      consensus_pos_index;  // <consensusIndex, pos> used to search log

  std::unique_ptr<MYSQL_BIN_LOG::Binlog_ofile>
      cache_log;  // used to cache a ConsensusLogEntry, and communicate with
                  // algorithm layer
  Consensus_Log_State
      status;  // leader: binlog system is working,
               // follower or candidator: relaylog system is working

  std::atomic<bool> already_set_start_index;  // set at first downgrade, used to
                                              // set correct start apply index
  std::atomic<bool> already_set_start_term;   // set at first downgrade, used to
                                              // set correct start apply term
  std::atomic<uint> apply_catchup;  // determine whether apply thread catchup
  uint atomic_logging_flag;  // used to mark atomic send logs, 0,1,2 , 0 stands
                             // for not atomic, 1 and 2 stands for different
                             // group

  std::string empty_log_event_content;

  // bool log_state_change_is_running;
  // std::deque<ConsensusStateChange> log_state_change_queue;
  // my_thread_handle log_state_change_thread_handle;
  // mysql_cond_t COND_log_state_change;
  // mysql_mutex_t LOCK_log_state_change;

  /*
    commit position watcher part
  */
  bool consensus_commit_pos_watcher_is_running{false};
  my_thread_handle consensus_commit_pos_watcher_thread_handle;
  mysql_mutex_t LOCK_consensus_commit_pos;
  ConsensusCommitPos commit_pos;

  std::atomic<uint32>
      ev_tt_;  // store last log event timestamp received from leader
  // std::atomic<uint32> m_switchover_state{CSS_LIMIT_NONE};

  MYSQL_BIN_LOG *binlog;  // point to the MySQL binlog object
  Relay_log_info
      *rli_info;  // point to the MySQL relay log info object, include relay_log

 private:
  Consensus_info *consensus_info{nullptr};                 // consensus system info
  Consensus_recovery_manager *recovery_manager{nullptr};   // recovery module
  Consensus_prefetch_manager *prefetch_manager{nullptr};     // prefetch module
  Consensus_cache_manager *cache_manager{nullptr};        // fifo cache module
  Applied_index_checker *m_applied_index_checker{nullptr};

  bool m_is_logger{false};
};

static inline uchar *get_uchar_str(std::string &str) {
  return (uchar *)const_cast<char *>(str.c_str());
}

void *run_consensus_state_change(void *arg);
void *run_consensus_commit_pos_watcher(void *arg);
}  // namespace komodo
#endif
