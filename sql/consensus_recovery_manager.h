/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  consensus_recovery_manager.h,v 1.0 08/22/2016 12:37:45 PM
 *droopy.hw(droopy.hw@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file consensus_recovery_manager.h
 * @author droopy.hw(droopy.hw@alibaba-inc.com)
 * @date 08/22/2016 12:37:45 PM
 * @version 1.0
 * @brief the CONSENSUS recovery interface
 *
 **/

#ifndef CONSENSUS_RECOVERY_MANAGER_INCLUDE
#define CONSENSUS_RECOVERY_MANAGER_INCLUDE

// #include "my_global.h"
#include "binlog.h"
#include "hash.h"
#include "raft/raft0xa.h"
#include "xa.h"

#include <atomic>
#include <map>
#include <vector>


#ifdef HAVE_PSI_INTERFACE
extern PSI_mutex_key key_CONSENSUSLOG_LOCK_ConsensusLog_recover_hash_lock;
#endif

class ConsensusRecoveryManager {
 public:
  ConsensusRecoveryManager()
      : inited(false),
        key_LOCK_consensus_log_recover_hash(),
        LOCK_consensuslog_recover_hash(),
        last_leader_term_index(0),
        total_commit_trx_map(),
        consensus_commit_xid_map() {}
  ~ConsensusRecoveryManager() = default;

  int init();
  int cleanup();

  std::map<uint64, XID *> &get_commit_index_map() {
    return consensus_commit_xid_map;
  }
  uint64 get_last_leader_term_index() const { return last_leader_term_index; }

  void set_last_leader_term_index(uint64 last_leader_term_index_arg) {
    last_leader_term_index = last_leader_term_index_arg;
  }

  // for recover
  void add_trx_to_total_commit_map(uint64 consensus_index, uint64 xid,
                                   ulonglong gcn);

  int collect_commit_trx_to_hash(
      mem_root_unordered_map<my_xid, my_commit_gcn> &commit_list,
      MEM_ROOT *mem_root);

  void add_xid_to_commit_map(XID *xid);
  void clear_total_xid_map();
  void clear_all_map();
  uint64 get_max_consensus_index_from_recover_trx_hash();
  int truncate_commit_xid_map(uint64 consensus_index);
  bool is_recovering_trx_empty();
  // truncate the continuous prepared xids from the map
  int truncate_not_confirmed_xids_from_map(uint64 max_index_in_binlog_file);

 private:
  bool inited;
  PSI_mutex_key key_LOCK_consensus_log_recover_hash;
  // used to protect commit hash map
  mysql_mutex_t LOCK_consensuslog_recover_hash;
  uint64 last_leader_term_index;
  //<xid, consensusIndex> for save relation between index and my_xid when
  // recovering
  std::map<uint64, uint64> total_commit_trx_map;
  // <consensusIndex, xid> used to commit trx when recovering
  std::map<uint64, XID *> consensus_commit_xid_map;

 public:
  std::map<uint64, ulonglong> &get_xid_gcn_map() { return total_xid_gcn_map; }
  void clear_xid_gcn_map();
  void clear_xid_gcn_and_gtid_xid_map();

 private:
  //<xid, gcn> for save relation between my_xid and gcn
  // when recovering
  std::map<uint64, ulonglong> total_xid_gcn_map;
};

#endif
