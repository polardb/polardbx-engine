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
/** @file include/komodo0consensus0xlog.cc
 Komodo consensus xlog interface.

 Created 2025-06-04 by komodo
 *******************************************************/

#include "komodo0consensus0xlog.h"
#include <cstring>

namespace komodo {
int Consensus_xlog_handler::getEntry(uint64_t logIndex, alisql::LogEntry &entry,
                                     bool fastFail, uint64_t serverId) {
  uint64 term = 0;
  std::string log_content;
  OP_TYPE optype = NORMAL;
  bool outerlog = false;
  uint flag = 0;
  uint64 checksum = 0;
  if (logIndex < m_mock_start_index) {
    term = 0;
    log_content = "";
    optype = MOCK;
  } else {
    if (m_xlog_mgr.get_log_entry(serverId, logIndex, &term, log_content,
                                 &outerlog, &flag, fastFail))
      return 1;
    if (outerlog == true)
      optype = CONFIGCHANGE;
    else {
      if (flag & Consensus_log_event_flag::FLAG_LARGE_TRX)
        optype = LARGETRX;
      else if (flag & Consensus_log_event_flag::FLAG_LARGE_TRX_END)
        optype = LARGETRXEND;
      else
        optype = NORMAL;
    }
  }
  entry.set_term(term);
  entry.set_index(logIndex);
  entry.set_optype(optype);
  entry.set_value(std::move(log_content));
  entry.set_info(flag);
  entry.set_checksum(checksum);
  return 0;
}

uint64_t Consensus_xlog_handler::getFirstIndexOfLastFileTerm(const uint64_t term) {
  return mysql_bin_log.get_first_index_of_last_file_term(term);
}

uint64_t Consensus_xlog_handler::getLastIndexOfTerm(const uint64_t term) {
  return mysql_bin_log.get_last_index_of_term(term);
}
int Consensus_xlog_handler::getEntry(uint64_t logIndex, alisql::LogEntry &entry,
                                     bool fastFail) {
  // if not set serverid, use 0 default
  return getEntry(logIndex, entry, fastFail, 0);
}

bool Consensus_xlog_handler::getLeftSize(uint64_t startLogIndex,
                                         uint64_t maxPacketSize) {
  if (m_xlog_mgr.get_left_log_size(startLogIndex, maxPacketSize) >
      maxPacketSize)
    return true;
  else
    return false;
}

int Consensus_xlog_handler::getEmptyEntry(alisql::LogEntry &entry) {
  std::string log_content = m_xlog_mgr.get_empty_log();
  entry.set_value(log_content);
  entry.set_optype(NORMAL);
  entry.set_info(0);
  entry.set_checksum(0);
  return 0;
}

uint64_t Consensus_xlog_handler::getLastLogIndex() {
  return (this->m_xlog_mgr.get_sync_index());
}

uint64_t Consensus_xlog_handler::getLastCachedLogIndex() {
  return (this->m_xlog_mgr.get_cache_index());
}

uint64_t Consensus_xlog_handler::getSafeLastLogIndex() {
  return (this->m_xlog_mgr.get_final_sync_index());
}

uint64_t Consensus_xlog_handler::getSafeLastLogIndexNoLock() {
  return (this->m_xlog_mgr.get_final_sync_index_no_lock());
}

uint64_t Consensus_xlog_handler::getWaitMilliseconds4OldTrxFinish() {
  return this->m_switchover_mgr.get_wait_milliseconds_for_old_trx_finish();
}

void Consensus_xlog_handler::forceUpdateAppliedIndex(const uint64_t index) {
  this->m_xlog_mgr.force_update_applied_index(index);
}

uint64_t Consensus_xlog_handler::waitOldTrxFinish() {
  return this->m_switchover_mgr.wait_old_trx_finish();
}

uint64_t Consensus_xlog_handler::waitOldXaFinish() {
  return this->m_switchover_mgr.wait_old_xa_finish();
}

std::pair<uint64_t, uint64_t> Consensus_xlog_handler::waitOldBgcFinish(alisql::Paxos *paxos) {
  return this->m_switchover_mgr.wait_old_bgc_finish(
      m_xlog_mgr.get_final_sync_index(), paxos);
}

uint64_t Consensus_xlog_handler::appendWithCheck(
    const alisql::LogEntry &entry) {
  uint64 index = 0;
  if (entry.optype() == UNCERTAIN) {
    // optype UNCERTAIN means group commit replicate log send a fake log
    // do not need to write actually
    index = 0;
  } else {
    uint flag = entry.info();
    if (entry.optype() == CONFIGCHANGE)
      flag |= Consensus_log_event_flag::FLAG_CONFIG_CHANGE;
    ConsensusLogEntry log_entry = {
        entry.term(),
        entry.index(),
        entry.value().length(),
        (uchar *)const_cast<char *>(entry.value().c_str()),
        entry.optype() == CONFIGCHANGE,
        flag,
        entry.checksum()};
    if (m_xlog_mgr.write_log_entry(log_entry, &index, true /*check term*/)) {
      _exit(2);  // Using _exit(), since exit() is not async signal safe
    }
  }
  return index;
}

uint64_t Consensus_xlog_handler::append(const alisql::LogEntry &entry) {
  uint64 index = 0;
  if (entry.optype() == UNCERTAIN) {
    // optype UNCERTAIN means group commit replicate log send a fake log
    // do not need to write actually
    index = 0;
  } else {
    uint flag = entry.info();
    if (entry.optype() == CONFIGCHANGE)
      flag |= Consensus_log_event_flag::FLAG_CONFIG_CHANGE;
    ConsensusLogEntry log_entry = {
        entry.term(),
        entry.index(),
        entry.value().length(),
        (uchar *)const_cast<char *>(entry.value().c_str()),
        entry.optype() == CONFIGCHANGE,
        flag,
        entry.checksum()};
    if (m_xlog_mgr.write_log_entry(log_entry, &index)) {
      _exit(2);  // Using _exit(), since exit() is not async signal safe
    }
  }
  return index;
}

uint64_t Consensus_xlog_handler::append(
    const ::google::protobuf::RepeatedPtrField<alisql::LogEntry> &entries) {
  std::vector<ConsensusLogEntry> log_vector;
  for (const auto &entry : entries) {
    uint flag = entry.info();
    if (entry.optype() == CONFIGCHANGE)
      flag |= Consensus_log_event_flag::FLAG_CONFIG_CHANGE;
    ConsensusLogEntry log = {entry.term(),
                             entry.index(),
                             entry.value().length(),
                             (uchar *)const_cast<char *>(entry.value().c_str()),
                             entry.optype() == CONFIGCHANGE,
                             flag,
                             entry.checksum()};
    log_vector.push_back(log);
  }
  uint64 max_index = 0;

  if (m_xlog_mgr.write_log_entries(log_vector, &max_index)) {
    _exit(2);  // Using _exit(), since exit() is not async signal safe
  }

  assert(max_index != 0);
  return max_index;
}

void Consensus_xlog_handler::truncateBackward(uint64_t firstIndex) {
  m_xlog_mgr.truncate_log(firstIndex);
}

void Consensus_xlog_handler::truncateForward(uint64_t lastIndex) {
  m_xlog_mgr.purge_log(lastIndex);
}

int Consensus_xlog_handler::getMetaData(const std::string &key,
                                        uint64_t *value) {
  if (m_xlog_meta.get_consensus_info()->consensus_init_info()) return 1;

  if (key == "@keyVoteFor_@")
    *value = m_xlog_meta.get_consensus_info()->get_vote_for();
  else if (key == "@keyCurrentTerm_@")
    *value = m_xlog_meta.get_consensus_info()->get_current_term();
  else if (key == "@keyLastLeaderTerm_@")
    *value = m_xlog_meta.get_consensus_info()->get_last_leader_term();
  else if (key == "@keyLastLeaderLogIndex_@")
    *value = m_xlog_meta.get_consensus_info()->get_start_apply_index();
  else if (key == "@keyScanIndex_@")
    *value = m_xlog_meta.get_consensus_info()->get_cluster_recover_index();
  else if (key == "@keyClusterId_@")
    *value = m_xlog_meta.get_consensus_info()->get_cluster_id();
  else
    assert(0);
  return 0;
}

int Consensus_xlog_handler::getMetaData(const std::string &key,
                                        std::string &value) {
  if (m_xlog_meta.get_consensus_info()->consensus_init_info()) return 1;

  if (key == "@keyMemberConfigure_@")
    value = m_xlog_meta.get_consensus_info()->get_cluster_info();
  else if (key == "@keyLearnerConfigure_@")
    value = m_xlog_meta.get_consensus_info()->get_cluster_learner_info();
  else
    assert(0);
  return 0;
}

int Consensus_xlog_handler::setMetaData(const std::string &key,
                                        const uint64_t value) {
  if (key == "@keyVoteFor_@")
    m_xlog_meta.get_consensus_info()->set_vote_for(value);
  else if (key == "@keyCurrentTerm_@")
    m_xlog_meta.get_consensus_info()->set_current_term(value);
  else if (key == "@keyLastLeaderTerm_@")
    m_xlog_mgr.set_start_apply_term_if_need(value);
  else if (key == "@keyLastLeaderLogIndex_@")
    m_xlog_mgr.set_start_apply_index_if_need(value);
  else if (key == "@keyScanIndex_@")
    m_xlog_meta.get_consensus_info()->set_cluster_recover_index(value);
  else if (key == "@keyClusterId_@")
    m_xlog_meta.get_consensus_info()->set_cluster_id(value);
  else
    assert(0);

  if (m_xlog_meta.get_consensus_info()->flush_info(true, true)) {
    _exit(2);  // Using _exit(), since exit() is not async signal safe
  }
  return 0;
}

int Consensus_xlog_handler::setMetaData(const std::string &key,
                                        const std::string &value) {
  if (key == "@keyMemberConfigure_@")
    m_xlog_meta.get_consensus_info()->set_cluster_info(value);
  else if (key == "@keyLearnerConfigure_@")
    m_xlog_meta.get_consensus_info()->set_cluster_learner_info(value);
  else
    assert(0);

  if (m_xlog_meta.get_consensus_info()->flush_info(true, true)) {
    _exit(2);  // Using _exit(), since exit() is not async signal safe
  }
  return 0;
}

void Consensus_xlog_handler::setTerm(uint64_t term) {
  mysql_mutex_lock(m_xlog_mgr.get_term_lock());
  this->currentTerm_ = term;
  mysql_mutex_unlock(m_xlog_mgr.get_term_lock());
}

uint64_t Consensus_xlog_handler::getLength() {
  return m_xlog_mgr.get_exist_log_length();
}

void Consensus_xlog_handler::packLogEntry(uchar *buffer, size_t buf_size,
                                          uint64 term, uint64 index,
                                          OP_TYPE entry_type,
                                          alisql::LogEntry &log_entry) {
  std::string log_content((char *)buffer, buf_size);
  log_entry.set_value(log_content);
  log_entry.set_term(term);
  log_entry.set_index(index);
  log_entry.set_optype(entry_type);
}

bool Consensus_xlog_handler::isStateMachineHealthy() {
  return m_xlog_mgr.is_state_machine_ready();
}

}  // namespace komodo
