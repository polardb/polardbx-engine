/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  consensus_recovery_manager.cc,v 1.0 08/22/2016 09:16:32 AM
 *droopy.hw(droopy.hw@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file consensus_recovery_manager.cc
 * @author droopy.hw(droopy.hw@alibaba-inc.com)
 * @date 08/22/2016 09:16:32 PM
 * @version 1.0
 * @brief The implement of the CONSENSUS recovery
 *
 **/

#include "consensus_recovery_manager.h"

#include "log.h"
#include "log_event.h"
#include "mysql/thread_pool_priv.h"
#include "mysqld.h"
#include "raft/raft0err.h"
#include "raft/raft0xa.h"
#include "sql_parse.h"

#ifdef HAVE_PSI_INTERFACE
PSI_mutex_key key_CONSENSUSLOG_LOCK_ConsensusLog_recover_hash_lock;
#endif

int ConsensusRecoveryManager::init() {
  key_LOCK_consensus_log_recover_hash =
      key_CONSENSUSLOG_LOCK_ConsensusLog_recover_hash_lock;
  mysql_mutex_init(key_LOCK_consensus_log_recover_hash,
                   &LOCK_consensus_log_recover_hash, MY_MUTEX_INIT_FAST);
  inited = TRUE;
  last_leader_term_index = 0;
  return 0;
}

int ConsensusRecoveryManager::cleanup() {
  if (inited) {
    clear_all_map();
    mysql_mutex_destroy(&LOCK_consensus_log_recover_hash);
  }
  return 0;
}

void ConsensusRecoveryManager::add_trx_to_total_commit_map(
    uint64 consensus_index, uint64 xid, ulonglong gcn) {
  mysql_mutex_lock(&LOCK_consensus_log_recover_hash);
  total_commit_trx_map[xid] = consensus_index;
  total_xid_gcn_map[xid] = gcn;

  raft::system(ER_RAFT_RECOVERY)
      << "XID = [" << xid << "] GCN = [" << gcn << "] Index = ["
      << consensus_index
      << " was added at ConsensusRecoveryManager::add_trx_to_total_commit_map";
  mysql_mutex_unlock(&LOCK_consensus_log_recover_hash);
}

// int ConsensusRecoveryManager::collect_commit_trx_to_hash(HASH *xid_hash,
// MEM_ROOT* mem_root)
int ConsensusRecoveryManager::collect_commit_trx_to_hash(
    malloc_unordered_map<my_xid, my_commit_gcn> &commit_list,
    MEM_ROOT *mem_root __attribute__((unused))) {
  mysql_mutex_lock(&LOCK_consensus_log_recover_hash);
  for (auto &iter : total_commit_trx_map) {
    // uint64 xid = iter->first;
    //  uchar *x = (uchar *)memdup_root(mem_root, (uchar*)&xid,
    // sizeof(xid));

    // if (!x || my_hash_insert(xid_hash, x))
    my_xid xid = iter.first;
    ulonglong gcn = total_xid_gcn_map.at(xid);

    if (!commit_list.insert({xid, gcn}).second) {
      mysql_mutex_unlock(&LOCK_consensus_log_recover_hash);
      return 1;
    }
    raft::system(ER_RAFT_RECOVERY)
        << "XID = [" << xid << "] GCN = [" << gcn << "]"
        << " was added into commit list "
           "ConsensusRecoveryManager::collect_commit_trx_to_hash";
  }
  mysql_mutex_unlock(&LOCK_consensus_log_recover_hash);
  return 0;
}

void ConsensusRecoveryManager::add_xid_to_commit_map(XID *xid) {
  mysql_mutex_lock(&LOCK_consensus_log_recover_hash);
  auto iter1 = total_commit_trx_map.find(xid->get_my_xid());
  if (iter1 != total_commit_trx_map.end()) {
    uint64 consensus_index = iter1->second;
    raft::system(ER_RAFT_RECOVERY)
        << "XID = [" << xid->get_my_xid() << "] Index = [" << consensus_index
        << "]"
        << " was added into commit map at "
           "ConsensusRecoveryManager::add_xid_to_commit_map";
    consensus_commit_xid_map[consensus_index] = xid;
  } else {
    raft::error(ER_RAFT_RECOVERY)
        << "XID = [" << xid->get_my_xid() << "] "
        << "was not found at ConsensusRecoveryManager::add_xid_to_commit_map";
  }
  mysql_mutex_unlock(&LOCK_consensus_log_recover_hash);
}

void ConsensusRecoveryManager::clear_total_xid_map() {
  mysql_mutex_lock(&LOCK_consensus_log_recover_hash);
  total_commit_trx_map.clear();
  mysql_mutex_unlock(&LOCK_consensus_log_recover_hash);
}

void ConsensusRecoveryManager::clear_xid_gcn_map() {
  mysql_mutex_lock(&LOCK_consensus_log_recover_hash);
  total_xid_gcn_map.clear();
  mysql_mutex_unlock(&LOCK_consensus_log_recover_hash);
}

void ConsensusRecoveryManager::clear_all_map() {
  mysql_mutex_lock(&LOCK_consensus_log_recover_hash);
  total_commit_trx_map.clear();
  for (auto &iter : consensus_commit_xid_map) {
    my_free(iter.second);
  }
  consensus_commit_xid_map.clear();
  total_xid_gcn_map.clear();
  mysql_mutex_unlock(&LOCK_consensus_log_recover_hash);
}

uint64
ConsensusRecoveryManager::get_max_consensus_index_from_recover_trx_hash() {
  uint64 max_consensus_index;
  mysql_mutex_lock(&LOCK_consensus_log_recover_hash);
  auto iter = consensus_commit_xid_map.rbegin();
  if (iter == consensus_commit_xid_map.rend())
    max_consensus_index = 0;
  else
    max_consensus_index = iter->first;
  mysql_mutex_unlock(&LOCK_consensus_log_recover_hash);

  raft::system(ER_RAFT_RECOVERY)
      << "Found max consensus index = [" << max_consensus_index << "]"
      << "at "
         "ConsensusRecoveryManager::get_max_consensus_index_from_recover_trx_"
         "hash";
  return max_consensus_index;
}

int ConsensusRecoveryManager::truncate_commit_xid_map(uint64 consensus_index) {
  mysql_mutex_lock(&LOCK_consensus_log_recover_hash);
  for (auto iter = consensus_commit_xid_map.begin();
       iter != consensus_commit_xid_map.end();) {
    if (iter->first >= consensus_index) {
      raft::system(ER_RAFT_RECOVERY)
          << "XID = [" << iter->second->get_my_xid() << "] Index = ["
          << iter->first << "]"
          << " is rollback since index large than " << consensus_index
          << " at ConsensusRecoveryManager::truncate_commit_xid_map";

      ha_rollback_xids(iter->second);
      my_free(iter->second);
      consensus_commit_xid_map.erase(iter++);
    } else {
      iter++;
    }
  }
  mysql_mutex_unlock(&LOCK_consensus_log_recover_hash);
  return 0;
}

bool ConsensusRecoveryManager::is_recovering_trx_empty() {
  bool empty = FALSE;
  mysql_mutex_lock(&LOCK_consensus_log_recover_hash);
  empty = consensus_commit_xid_map.empty();
  mysql_mutex_unlock(&LOCK_consensus_log_recover_hash);
  return empty;
}

// truncate the continuous xids of the prepared xids
int ConsensusRecoveryManager::truncate_not_confirmed_xids_from_map(
    uint64 max_index_in_binlog_file) {
  auto iter = consensus_commit_xid_map.rbegin();
  uint64 index = max_index_in_binlog_file;

  while (iter != consensus_commit_xid_map.rend() && iter->first == index) {
    iter++;
    index--;
  }

  index++;
  if (index <= max_index_in_binlog_file) {
    raft::system(ER_RAFT_RECOVERY)
        << "Last leader term index to = [" << index - 1 << "] but max index = ["
        << max_index_in_binlog_file << "] in binlog file at "
        << "ConsensusRecoveryManager::truncate_not_confirmed_xids_from_map ";
  } else {
    raft::error(ER_RAFT_RECOVERY)
        << "Last leader term index to = [" << index - 1 << "] but max index = ["
        << max_index_in_binlog_file << "] in binlog file at "
        << "ConsensusRecoveryManager::truncate_not_confirmed_xids_from_map ";
  }

  return 0;
}
