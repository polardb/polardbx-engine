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

#include "consensus_recovery_manager.h"

#include "mysqld.h"
#include "log_event.h"
#include "sql_parse.h"
#include "mysql/thread_pool_priv.h"
#include "log.h"
#include "binlog_ext.h"


int ConsensusRecoveryManager::init()
{
  key_LOCK_consensuslog_recover_hash = key_CONSENSUSLOG_LOCK_ConsensusLog_recover_hash_lock;
  mysql_mutex_init(key_LOCK_consensuslog_recover_hash, &LOCK_consensuslog_recover_hash, MY_MUTEX_INIT_FAST);
  inited = TRUE;
  last_leader_term_index = 0;
  return 0;
}

int ConsensusRecoveryManager::cleanup()
{
  if (inited)
  {
    clear_all_map();
    mysql_mutex_destroy(&LOCK_consensuslog_recover_hash);
  }
  return 0;
}

void ConsensusRecoveryManager::add_trx_to_total_commit_map(uint64 consensus_index, uint64 xid, ulonglong gcn, Gtid gtid)
{
  mysql_mutex_lock(&LOCK_consensuslog_recover_hash);
  total_commit_trx_map[xid] = consensus_index;
  total_xid_gcn_map[xid] = gcn;
  total_xid_gtid_map[xid] = gtid;
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
}

// int ConsensusRecoveryManager::collect_commit_trx_to_hash(HASH *xid_hash, MEM_ROOT* mem_root)
int ConsensusRecoveryManager::collect_commit_trx_to_hash(memroot_unordered_map<my_xid, my_commit_gcn> &commit_list, MEM_ROOT* mem_root __attribute__((unused)))
{
  mysql_mutex_lock(&LOCK_consensuslog_recover_hash);
  for (auto iter = total_commit_trx_map.begin();
    iter != total_commit_trx_map.end(); iter++)
  {
    //uint64 xid = iter->first;
    // uchar *x = (uchar *)memdup_root(mem_root, (uchar*)&xid,
      //sizeof(xid));

    // if (!x || my_hash_insert(xid_hash, x))
    my_xid xid = iter->first;
    ulonglong gcn = total_xid_gcn_map.at(xid);

    if (!commit_list.insert({xid, gcn}).second)
    {
      mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
      return 1;
    }
  }
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
  return 0;
}

void ConsensusRecoveryManager::add_xid_to_commit_map(XID* xid)
{
  mysql_mutex_lock(&LOCK_consensuslog_recover_hash);
  auto iter1 = total_commit_trx_map.find(xid->get_my_xid());
  if (iter1 != total_commit_trx_map.end())
  {
    uint64 consensus_index = iter1->second;
    sql_print_information("ConsensusRecoveryManager::add_xid_to_commit_map, xid: %llu, consensus index: %llu.", xid->get_my_xid(), consensus_index);
    consensus_commit_xid_map[consensus_index] = xid;
  }
  else
  {
    sql_print_error("ConsensusRecoveryManager::add_xid_to_commit_map, can not found xid=%lld.", xid->get_my_xid());
  }
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
}


void ConsensusRecoveryManager::clear_total_xid_map()
{
  mysql_mutex_lock(&LOCK_consensuslog_recover_hash);
  total_commit_trx_map.clear();
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
}

void ConsensusRecoveryManager::clear_xid_gcn_and_gtid_xid_map()
{
  mysql_mutex_lock(&LOCK_consensuslog_recover_hash);
  total_commit_trx_map.clear();
  total_xid_gcn_map.clear();
  total_xid_gtid_map.clear();
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
}

void ConsensusRecoveryManager::clear_all_map()
{
  mysql_mutex_lock(&LOCK_consensuslog_recover_hash);
  total_commit_trx_map.clear();
  for (auto iter = consensus_commit_xid_map.begin(); iter != consensus_commit_xid_map.end(); iter++)
  {
    my_free(iter->second);
  }
  consensus_commit_xid_map.clear();
  total_xid_gcn_map.clear();
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
}


uint64 ConsensusRecoveryManager::get_max_consensus_index_from_recover_trx_hash()
{
  uint64 max_consensus_index = 0;
  mysql_mutex_lock(&LOCK_consensuslog_recover_hash);
  auto iter = consensus_commit_xid_map.rbegin();
  if (iter == consensus_commit_xid_map.rend())
    max_consensus_index = 0;
  else
    max_consensus_index = iter->first;
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
  return max_consensus_index;
}

int ConsensusRecoveryManager::truncate_commit_xid_map(uint64 consensus_index)
{
  mysql_mutex_lock(&LOCK_consensuslog_recover_hash); 
  for (auto  iter = consensus_commit_xid_map.begin();
    iter != consensus_commit_xid_map.end(); )
  {
    if (iter->first >= consensus_index)
    {
      ha_rollback_xids(iter->second);
      my_free(iter->second);
      consensus_commit_xid_map.erase(iter++);
    }
    else
    {
      iter++;
    }
  }
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
  return 0;
}


bool ConsensusRecoveryManager::is_recovering_trx_empty()
{
  bool empty = FALSE;
  mysql_mutex_lock(&LOCK_consensuslog_recover_hash);
  empty = consensus_commit_xid_map.empty();
  mysql_mutex_unlock(&LOCK_consensuslog_recover_hash);
  return empty;
}

// truncate the continuous xids of the prepared xids
int ConsensusRecoveryManager::truncate_not_confirmed_xids_from_map(uint64 max_index_in_binlog_file)
{
  auto iter = consensus_commit_xid_map.rbegin();
  uint64 index = max_index_in_binlog_file;

  while (iter != consensus_commit_xid_map.rend() && iter->first == index)
  {
    iter++;
    index--;
  }

  index++;
  if (index <= max_index_in_binlog_file)
  {
    sql_print_information("Truncate prepared trx from index %llu", index);
    truncate_commit_xid_map(index);
    set_last_leader_term_index(index - 1);

    if (opt_print_gtid_info_during_recovery == DETAIL_INFO) {
      sql_print_error("truncate_not_confirmed_xids_from_map "
                      "set_last_leader_term_index to %llu", index - 1);
    }
  }

  return 0;
}
