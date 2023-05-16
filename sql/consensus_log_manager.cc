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


#include "consensus_log_manager.h"
#include "events.h"
#include "rpl_info_factory.h"
#include "rpl_msr.h"
#include "rpl_replica.h"
#include "sql/rpl_mi.h"

#ifdef HAVE_PSI_INTERFACE
PSI_rwlock_key key_rwlock_ConsensusLog_status_lock;
PSI_mutex_key key_CONSENSUSLOG_LOCK_commit_pos;
PSI_mutex_key key_CONSENSUSLOG_LOCK_ConsensusLog_sequence_stage1_lock;
PSI_mutex_key key_CONSENSUSLOG_LOCK_ConsensusLog_term_lock;
PSI_mutex_key key_CONSENSUSLOG_LOCK_ConsensusLog_apply_lock;
PSI_mutex_key key_CONSENSUSLOG_LOCK_ConsensusLog_apply_thread_lock;
PSI_mutex_key key_CONSENSUSLOG_LOCK_Consensus_stage_change;
PSI_cond_key key_COND_ConsensusLog_catchup;
#endif

Consensus_log_manager consensus_log_manager;

Consensus_info *Consensus_log_manager::get_consensus_info() {
  return consensus_info;
}

mysql_mutex_t *Consensus_log_manager::get_term_lock() {
  return &LOCK_consensus_log_term;
}

std::string Consensus_log_manager::get_empty_log() { return ""; }

uint64_t Consensus_log_manager::get_cache_index() { return 0; }

uint64_t Consensus_log_manager::get_sync_index(bool) { return 0; }

uint64_t Consensus_log_manager::get_final_sync_index() { return 0; }

int Consensus_log_manager::set_start_apply_index_if_need(
    uint64) {
  return 0;
}

int Consensus_log_manager::set_start_apply_term_if_need(uint64) {
  return 0;
}

int Consensus_log_manager::write_log_entry(ConsensusLogEntry &,
                                           uint64 *,
                                           bool) {
  return 0;
}

int Consensus_log_manager::write_log_entries(
    std::vector<ConsensusLogEntry> &, uint64 *) {
  return 0;
}

int Consensus_log_manager::get_log_entry(uint64,
                                         uint64,
                                         uint64 *,
                                         string &, bool *,
                                         uint *, uint64 *,
                                         bool) {
  return 0;
}

uint64_t Consensus_log_manager::get_left_log_size(uint64,
                                                  uint64) {
  return 0;
}

int Consensus_log_manager::truncate_log(uint64) { return 0; }

int Consensus_log_manager::purge_log(uint64) { return 0; }

uint64 Consensus_log_manager::get_exist_log_length() { return 0; }

bool Consensus_log_manager::is_state_machine_ready() { return false; }

void Consensus_log_manager::set_cache_index(uint64) {}
