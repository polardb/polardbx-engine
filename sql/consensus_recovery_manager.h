/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef CONSENSUS_RECOVERY_MANAGER_INCLUDE
#define CONSENSUS_RECOVERY_MANAGER_INCLUDE

// #include "my_global.h"
#include "hash.h"
#include "binlog.h"
#include "xa.h"

#include <map>
#include <vector>
#include <atomic>

class ConsensusRecoveryManager {
 public:
  ConsensusRecoveryManager():inited(false) {};
  ~ConsensusRecoveryManager() {};

  int init();
  int cleanup();


  std::map<uint64, XID*>& get_commit_index_map() { return consensus_commit_xid_map; }
  uint64 get_last_leader_term_index() { return last_leader_term_index; }

  void set_last_leader_term_index(uint64 last_leader_term_index_arg)
    { last_leader_term_index = last_leader_term_index_arg; }

  // for recover
  void add_trx_to_total_commit_map(uint64 consensus_index, uint64 xid, ulonglong gcn, Gtid gtid);
  int collect_commit_trx_to_hash(
      memroot_unordered_map<my_xid, my_commit_gcn> &commit_list,
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
  PSI_mutex_key key_LOCK_consensuslog_recover_hash;
  mysql_mutex_t
      LOCK_consensuslog_recover_hash;  // used to protect commit hash map
  uint64 last_leader_term_index;
  std::map<uint64, uint64>
      total_commit_trx_map;  //<xid, consensusIndex> for save relation between
                             // index and my_xid when recovering
  std::map<uint64, XID *>
      consensus_commit_xid_map;  // <consensusIndex, xid> used to commit trx
                                 // when recovering

 public:
  std::map<uint64, Gtid>&  get_total_gtid_map() { return total_xid_gtid_map; }
  std::map<uint64, ulonglong>& get_xid_gcn_map() { return total_xid_gcn_map; }
  void clear_xid_gcn_and_gtid_xid_map();

 private:
  std::map<uint64, Gtid>                    total_xid_gtid_map;   // <xid, gtid> used to get gtid 
  std::map<uint64, ulonglong>               total_xid_gcn_map;    //<xid, gcn> for save relation between my_xid and gcn when recovering

};

#endif
