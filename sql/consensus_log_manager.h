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

#include "binlog.h"
#include "consensus_info.h"
#include "mysql/components/services/bits/mysql_mutex_bits.h"
#include "rpl_rli.h"
#include "basic_ostream.h"
#include "consensus_fifo_cache_manager.h"
#include "consensus_log_index.h"

#ifdef HAVE_PSI_INTERFACE
extern PSI_rwlock_key key_rwlock_ConsensusLog_status_lock;
extern PSI_mutex_key key_CONSENSUSLOG_LOCK_commit_pos;
extern PSI_mutex_key key_CONSENSUSLOG_LOCK_ConsensusLog_sequence_stage1_lock;
extern PSI_mutex_key key_CONSENSUSLOG_LOCK_ConsensusLog_term_lock;
extern PSI_mutex_key key_CONSENSUSLOG_LOCK_ConsensusLog_apply_lock;
extern PSI_mutex_key key_CONSENSUSLOG_LOCK_ConsensusLog_apply_thread_lock;
extern PSI_mutex_key key_CONSENSUSLOG_LOCK_Consensus_stage_change;
extern PSI_cond_key key_COND_ConsensusLog_catchup;
#endif

enum Consensus_Log_System_Status { RELAY_LOG_WORKING = 0, BINLOG_WORKING = 1 };

enum Consensus_log_event_flag {
  FLAG_GU1 = 1,
  FLAG_GU2 = 1 << 1, /* FLAG_GU2 = 3 - FLAG_GU1 */
  FLAG_LARGE_TRX = 1 << 2,
  FLAG_LARGE_TRX_END = 1 << 3,
  FLAG_CONFIG_CHANGE = 1 << 4,
  FLAG_BLOB = 1 << 5,
  FLAG_BLOB_END = 1 << 6,
  FLAG_BLOB_START = 1 << 7 /* we should mark the start for SDK */
};

class Consensus_log_manager {
 public:
  Consensus_log_manager() = default;
  ~Consensus_log_manager() = default;

  Consensus_info *get_consensus_info();
  mysql_mutex_t *get_term_lock();

  std::string get_empty_log();

  uint64 get_cache_index();
  uint64 get_sync_index(bool serious = false);
  uint64 get_final_sync_index();

  int set_start_apply_index_if_need(uint64 consensus_index);
  int set_start_apply_term_if_need(uint64 consensus_term);

  int write_log_entry(ConsensusLogEntry &log, uint64 *consensus_index,
                      bool with_check = false);
  int write_log_entries(std::vector<ConsensusLogEntry> &logs,
                        uint64 *max_index);
  int get_log_entry(uint64 channel_id, uint64 consensus_index,
                    uint64 *consensus_term, std::string &log_content,
                    bool *outer, uint *flag, uint64 *checksum, bool fast_fail);
  uint64_t get_left_log_size(uint64 start_log_index, uint64 max_packet_size);
  int truncate_log(uint64 consensus_index);

  int purge_log(uint64 consensus_index);

  uint64 get_exist_log_length();
  bool is_state_machine_ready();

  void set_cache_index(uint64 cache_index_arg);

 private:
  Consensus_info *consensus_info;

  mysql_mutex_t LOCK_consensus_log_term;
};

extern Consensus_log_manager consensus_log_manager;

#endif
