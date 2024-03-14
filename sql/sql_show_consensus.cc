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

#include "sql/sql_show.h"

#include "my_config.h"

#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <algorithm>
#include <atomic>
#include <functional>
#include <memory>
#include <new>
#include <string>

#include "keycache.h"                                // dflt_key_cache
#include "mutex_lock.h"                              // MUTEX_LOCK
#include "mysql/components/services/log_builtins.h"  // LogErr
#include "mysql/plugin.h"                            // st_mysql_plugin
#include "scope_guard.h"                             // Scope_guard
#include "sql/auth/auth_acls.h"                      // DB_ACLS
#include "sql/dd/cache/dictionary_client.h"  // dd::cache::Dictionary_client
#include "sql/dd/dd_schema.h"                // dd::Schema_MDL_locker
#include "sql/dd/dd_table.h"                 // is_encrypted
#include "sql/dd/types/column.h"             // dd::Column
#include "sql/dd/types/table.h"              // dd::Table
#include "sql/debug_sync.h"                  // DEBUG_SYNC
#include "sql/derror.h"                      // ER_THD
#include "sql/error_handler.h"               // Internal_error_handler
#include "sql/field.h"                       // Field
#include "sql/filesort.h"                    // filesort_free_buffers
#include "sql/item.h"                        // Item_empty_string
#include "sql/log.h"                         // query_logger
#include "sql/mysqld.h"                      // lower_case_table_names
#include "sql/mysqld_thd_manager.h"          // Global_THD_manager
#include "sql/opt_trace.h"                   // fill_optimizer_trace_info
#include "sql/partition_info.h"              // partition_info
#include "sql/protocol.h"                    // Protocol
#include "sql/sp_head.h"                     // sp_head
#include "sql/sql_base.h"                    // close_thread_tables
#include "sql/sql_class.h"                   // THD
#include "sql/sql_db.h"                      // get_default_db_collation
#include "sql/sql_executor.h"                // QEP_TAB
#include "sql/sql_lex.h"                     // LEX
#include "sql/sql_optimizer.h"               // JOIN
#include "sql/sql_parse.h"                   // command_name
#include "sql/sql_partition.h"               // HA_USE_AUTO_PARTITION
#include "sql/sql_plugin.h"                  // PLUGIN_IS_DELETED, LOCK_plugin
#include "sql/sql_profile.h"                 // query_profile_statistics_info
#include "sql/sql_table.h"                   // primary_key_name
#include "sql/sql_tmp_table.h"               // create_ondisk_from_heap
#include "sql/sql_trigger.h"                 // acquire_shared_mdl_for_trigger
#include "sql/strfunc.h"                     // lex_string_strmake
#include "sql/sys_vars_consensus.h"
#include "sql/table_trigger_dispatcher.h"    // Table_trigger_dispatcher
#include "sql/temp_table_param.h"            // Temp_table_param
#include "sql/trigger.h"                     // Trigger
#include "sql/tztime.h"                      // my_tz_SYSTEM

//#include "sql/ppi/ppi_table_iostat.h"
//#include "sql/sql_implicit_common.h"
//#include "sql/sql_statistics_common.h"
//#include "sql/proxy/proxy_current_connection.h"

#include "bl_consensus_log.h"

extern char *opt_cluster_info;

int fill_consensus_commit_pos(THD *thd, Table_ref *tables, Item *) {
  DBUG_ENTER("fill_consensus_commit_pos");
  CHARSET_INFO *cs = system_charset_info;
  TABLE *table = tables->table;
  char fname[FN_REFLEN];
  uint64_t pos, index;
  consensus_log_manager.get_commit_pos(fname, &pos, &index);
  table->field[0]->store(fname, strlen(fname), cs);
  table->field[1]->store((ulonglong)pos);
  table->field[2]->store((ulonglong)index);
  if (schema_table_store_record(thd, table)) DBUG_RETURN(1);
  DBUG_RETURN(0);
}

int fill_alisql_cluster_global(THD *thd, Table_ref *tables, Item *) {
  DBUG_ENTER("fill_alisql_cluster_global");
  int res = 0;
  if (opt_consensus_force_recovery) DBUG_RETURN(res);
  TABLE *table = tables->table;
  // get from consensus alg layer
  uint64 id = server_id;
  std::string ip_port;
  uint64 match_index = 0;
  uint64 next_index = 0;
  std::string role;
  std::string has_voted;
  std::string force_sync;
  uint election_weight = 0;
  uint64 applied_index = 0;
  uint64 learner_source = 0;
  std::string pipelining;
  std::string use_applied;
  size_t node_num = 1;
  alisql::Paxos::MemberInfoType mi;
  std::vector<alisql::Paxos::ClusterInfoType> cis;
  consensus_ptr->getMemberInfo(&mi);
  // if node is not LEADER, global information is empty
  if (mi.role != alisql::Paxos::StateType::LEADER) {
    if (schema_table_store_record(thd, table))
      DBUG_RETURN(1);
    else
      DBUG_RETURN(res);
  }
  consensus_ptr->getClusterInfo(cis);
  node_num = cis.size();

  for (uint i = 0; i < node_num; i++) {
    id = cis[i].serverId;
    ip_port = cis[i].ipPort;
    match_index = cis[i].matchIndex;
    next_index = cis[i].nextIndex;
    switch (cis[i].role) {
      case alisql::Paxos::StateType::FOLLOWER:
        role = "Follower";
        break;
      case alisql::Paxos::StateType::CANDIDATE:
        role = "Candidate";
        break;
      case alisql::Paxos::StateType::LEADER:
        role = "Leader";
        break;
      case alisql::Paxos::StateType::LEARNER:
        role = "Learner";
        break;
      default:
        role = "No Role";
        break;
    }
    switch (cis[i].hasVoted) {
      case 1:
        has_voted = "Yes";
        break;
      case 0:
        has_voted = "No";
        break;
    }
    force_sync = cis[i].forceSync ? "Yes" : "No";
    election_weight = cis[i].electionWeight;
    applied_index = cis[i].appliedIndex;
    learner_source = cis[i].learnerSource;
    if (cis[i].pipelining) {
      pipelining = "Yes";
    } else {
      pipelining = "No";
    }
    if (cis[i].useApplied) {
      use_applied = "Yes";
    } else {
      use_applied = "No";
    }
    int field_num = 0;
    table->field[field_num++]->store((longlong)id, true);
    table->field[field_num++]->store(ip_port.c_str(), ip_port.length(),
                                     system_charset_info);
    table->field[field_num++]->store((longlong)match_index, true);
    table->field[field_num++]->store((longlong)next_index, true);
    table->field[field_num++]->store(role.c_str(), role.length(),
                                     system_charset_info);
    table->field[field_num++]->store(has_voted.c_str(), has_voted.length(),
                                     system_charset_info);
    table->field[field_num++]->store(force_sync.c_str(), force_sync.length(),
                                     system_charset_info);
    table->field[field_num++]->store((longlong)election_weight, true);
    table->field[field_num++]->store((longlong)learner_source, true);
    table->field[field_num++]->store((longlong)applied_index, true);
    table->field[field_num++]->store(pipelining.c_str(), pipelining.length(),
                                     system_charset_info);
    table->field[field_num++]->store(use_applied.c_str(), use_applied.length(),
                                     system_charset_info);
    if (schema_table_store_record(thd, table)) DBUG_RETURN(1);
  }

  DBUG_RETURN(res);
}

int fill_alisql_cluster_local(THD *thd, Table_ref *tables, Item *) {
  DBUG_ENTER("fill_alisql_cluster_local");
  int res = 0;
  if (opt_consensus_force_recovery) DBUG_RETURN(res);
  TABLE *table = tables->table;
  // get from consensus alg layer
  uint64 id = server_id;
  uint64 current_term = 0;
  std::string current_leader_ip_port;
  uint64 commit_index = 0, last_apply_index = 0;
  std::string role;
  uint64 voted_for = 0;

  // get from local log manager
  uint64 last_log_term = 0;
  uint64 last_log_index = 0;
  Consensus_Log_System_Status rw_status = consensus_log_manager.get_status();
  std::string rw_status_str;
  std::string instance_type;
  alisql::Paxos::MemberInfoType mi;
  consensus_ptr->getMemberInfo(&mi);
  id = mi.serverId;
  current_term = mi.currentTerm;
  current_leader_ip_port = mi.currentLeaderAddr;
  commit_index = mi.commitIndex;
  last_apply_index = mi.lastAppliedIndex;
  last_log_term = mi.lastLogTerm;
  last_log_index = mi.lastLogIndex;
  switch (mi.role) {
    case alisql::Paxos::StateType::FOLLOWER:
      role = "Follower";
      break;
    case alisql::Paxos::StateType::CANDIDATE:
      role = "Candidate";
      break;
    case alisql::Paxos::StateType::LEADER:
      role = "Leader";
      break;
    case alisql::Paxos::StateType::LEARNER:
      role = "Learner";
      break;
    default:
      role = "No Role";
      break;
  }
  /* not a stable leader if server_ready_for_rw is no and
   * consensus_auto_leader_transfer is on */
  if (role == "Leader" && opt_consensus_auto_leader_transfer &&
      (opt_cluster_log_type_instance || rw_status == RELAY_LOG_WORKING))
    role = "Prepared";

  voted_for = mi.votedFor;

  if (opt_cluster_log_type_instance) {
    rw_status_str = "No";
  } else {
    switch (rw_status) {
      case BINLOG_WORKING:
        rw_status_str = "Yes";
        break;
      case RELAY_LOG_WORKING:
        rw_status_str = "No";
        break;
    }
  }

  instance_type = opt_cluster_log_type_instance ? "Log" : "Normal";

  int field_num = 0;
  table->field[field_num++]->store((longlong)id, true);
  table->field[field_num++]->store((longlong)current_term, true);
  table->field[field_num++]->store(current_leader_ip_port.c_str(),
                                   current_leader_ip_port.length(),
                                   system_charset_info);
  table->field[field_num++]->store((longlong)commit_index, true);
  table->field[field_num++]->store((longlong)last_log_term, true);
  table->field[field_num++]->store((longlong)last_log_index, true);
  table->field[field_num++]->store(role.c_str(), role.length(),
                                   system_charset_info);
  table->field[field_num++]->store((longlong)voted_for, true);
  table->field[field_num++]->store((longlong)last_apply_index, true);
  table->field[field_num++]->store(rw_status_str.c_str(),
                                   rw_status_str.length(), system_charset_info);
  table->field[field_num++]->store(instance_type.c_str(),
                                   instance_type.length(), system_charset_info);

  if (schema_table_store_record(thd, table)) DBUG_RETURN(1);

  DBUG_RETURN(res);
}

int fill_alisql_cluster_health(THD *thd, Table_ref *tables, Item *) {
  DBUG_ENTER("fill_alisql_cluster_health");
  if (opt_consensus_force_recovery) DBUG_RETURN(0);
  TABLE *table = tables->table;

  std::vector<alisql::Paxos::HealthInfoType> hi;
  if (consensus_ptr->getClusterHealthInfo(hi)) DBUG_RETURN(0);

  for (auto e : hi) {
    int field_num = 0;
    std::string role;
    std::string connected;
    table->field[field_num++]->store((longlong)e.serverId, true);
    table->field[field_num++]->store(e.addr.c_str(), e.addr.length(),
                                     system_charset_info);
    switch (e.role) {
      case alisql::Paxos::StateType::FOLLOWER:
        role = "Follower";
        break;
      case alisql::Paxos::StateType::CANDIDATE:
        role = "Candidate";
        break;
      case alisql::Paxos::StateType::LEADER:
        role = "Leader";
        break;
      case alisql::Paxos::StateType::LEARNER:
        role = "Learner";
        break;
      default:
        role = "No Role";
        break;
    }
    table->field[field_num++]->store(role.c_str(), role.length(),
                                     system_charset_info);
    if (e.connected) {
      connected = "YES";
    } else {
      connected = "NO";
    }
    table->field[field_num++]->store(connected.c_str(), connected.length(),
                                     system_charset_info);
    table->field[field_num++]->store((longlong)e.logDelayNum, true);
    table->field[field_num++]->store((longlong)e.applyDelayNum, true);

    if (schema_table_store_record(thd, table)) DBUG_RETURN(1);
  }

  DBUG_RETURN(0);
}

int fill_alisql_cluster_learner_source(THD *thd, Table_ref *tables, Item *) {
  DBUG_ENTER("fill_alisql_cluster_learner_source");
  int res = 0;
  TABLE *table = tables->table;

  // get from consensus alg layer
  uint64 learner_id = 0;
  uint64 source_id = 0;
  std::string learner_ip_port;
  std::string source_ip_port;
  uint64 source_last_index;
  uint64 source_commit_index;
  uint64 learner_match_index = 0;
  uint64 learner_next_index = 0;

  uint64 learner_applied_index = 0;
  uint64 learner_source = 0;

  size_t node_num = 1;

  uint learner_source_count = 0;

  alisql::Paxos::MemberInfoType mi;
  std::vector<alisql::Paxos::ClusterInfoType> cis;

  consensus_ptr->getMemberInfo(&mi);
  consensus_ptr->getClusterInfo(cis);
  node_num = cis.size();

  source_id = mi.serverId;

  for (uint i = 0; i < node_num; i++) {
    learner_source = cis[i].learnerSource;
    if (cis[i].serverId == source_id) source_ip_port = cis[i].ipPort;
    if (learner_source != source_id)
      continue;
    else
      learner_source_count++;

    learner_id = cis[i].serverId;
    learner_ip_port = cis[i].ipPort;
    source_last_index = mi.lastLogIndex;
    source_commit_index = mi.commitIndex;
    learner_match_index = cis[i].matchIndex;
    learner_next_index = cis[i].nextIndex;
    learner_applied_index = cis[i].appliedIndex;

    int field_num = 0;
    table->field[field_num++]->store((longlong)learner_id, true);
    table->field[field_num++]->store(
        learner_ip_port.c_str(), learner_ip_port.length(), system_charset_info);
    table->field[field_num++]->store((longlong)source_id, true);
    table->field[field_num++]->store(
        source_ip_port.c_str(), source_ip_port.length(), system_charset_info);
    table->field[field_num++]->store((longlong)source_last_index, true);
    table->field[field_num++]->store((longlong)source_commit_index, true);
    table->field[field_num++]->store((longlong)learner_match_index, true);
    table->field[field_num++]->store((longlong)learner_next_index, true);
    table->field[field_num++]->store((longlong)learner_applied_index, true);
    if (schema_table_store_record(thd, table)) DBUG_RETURN(1);
  }
  if (learner_source_count == 0) {
    if (schema_table_store_record(thd, table)) DBUG_RETURN(1);
  }

  DBUG_RETURN(res);
}

int fill_alisql_cluster_prefetch_channel(THD *thd, Table_ref *tables, Item *) {
  DBUG_ENTER("fill_alisql_cluster_prefetch_channel");
  int res = 0;
  TABLE *table = tables->table;

  // get from consensus alg layer
  ConsensusPreFetchManager *prefetch_mgr =
      consensus_log_manager.get_prefetch_manager();
  prefetch_mgr->lock_prefetch_channels_hash(true);
  for (auto iter = prefetch_mgr->get_channels_hash()->begin();
       iter != prefetch_mgr->get_channels_hash()->end(); ++iter) {
    std::string stop_flag_str =
        iter->second->get_stop_preftch_request() ? "YES" : "NO";
    int field_num = 0;
    table->field[field_num++]->store((longlong)iter->second->get_channel_id(),
                                     true);
    table->field[field_num++]->store(
        (longlong)iter->second->get_first_index_in_cache(), true);
    table->field[field_num++]->store(
        (longlong)iter->second->get_last_index_in_cache(), true);
    table->field[field_num++]->store(
        (longlong)iter->second->get_prefetch_cache_size(), true);
    table->field[field_num++]->store(
        (longlong)iter->second->get_current_request(), true);
    table->field[field_num++]->store(
        stop_flag_str.c_str(), stop_flag_str.length(), system_charset_info);

    if (schema_table_store_record(thd, table)) DBUG_RETURN(1);
  }

  prefetch_mgr->unlock_prefetch_channels_hash();
  DBUG_RETURN(res);
}

int fill_alisql_cluster_consensus_status(THD *thd, Table_ref *tables, Item *) {
  DBUG_ENTER("fill_alisql_cluster_consensus_status");
  int res = 0;
  TABLE *table = tables->table;

  // get from consensus alg layer
  uint64 id = server_id;
  uint64 count_msg_append_log = 0;
  uint64 count_msg_request_vote = 0;
  uint64 count_heartbeat = 0;
  uint64 count_on_msg_append_log = 0;
  uint64 count_on_msg_request_vote = 0;
  uint64 count_on_heartbeat = 0;
  uint64 count_replicate_log = 0;
  uint64 count_log_meta_get_in_cache = 0;
  uint64 count_log_meta_get_total = 0;
  const alisql::Paxos::StatsType &stats = consensus_ptr->getStats();
  const alisql::PaxosLog::StatsType &log_stats = consensus_ptr->getLogStats();
  count_msg_append_log = stats.countMsgAppendLog;
  count_msg_request_vote = stats.countMsgRequestVote;
  count_heartbeat = stats.countHeartbeat;
  count_on_msg_append_log = stats.countOnMsgAppendLog;
  count_on_msg_request_vote = stats.countOnMsgRequestVote;
  count_on_heartbeat = stats.countOnHeartbeat;
  count_replicate_log = stats.countReplicateLog;
  count_log_meta_get_in_cache = log_stats.countMetaGetInCache;
  count_log_meta_get_total = log_stats.countMetaGetTotal;

  int field_num = 0;
  table->field[field_num++]->store((longlong)id, true);
  table->field[field_num++]->store((longlong)count_msg_append_log, true);
  table->field[field_num++]->store((longlong)count_msg_request_vote, true);
  table->field[field_num++]->store((longlong)count_heartbeat, true);
  table->field[field_num++]->store((longlong)count_on_msg_append_log, true);
  table->field[field_num++]->store((longlong)count_on_msg_request_vote, true);
  table->field[field_num++]->store((longlong)count_on_heartbeat, true);
  table->field[field_num++]->store((longlong)count_replicate_log, true);
  table->field[field_num++]->store((longlong)count_log_meta_get_in_cache, true);
  table->field[field_num++]->store((longlong)count_log_meta_get_total, true);
  if (schema_table_store_record(thd, table)) DBUG_RETURN(1);

  DBUG_RETURN(res);
}

int fill_alisql_cluster_consensus_membership_change(THD *thd, Table_ref *tables,
                                                    Item *) {
  DBUG_ENTER("fill_alisql_cluster_consensus_membership_change");
  TABLE *table = tables->table;

  std::vector<alisql::Paxos::MembershipChangeType> mch =
      consensus_ptr->getMembershipChangeHistory();
  for (auto &e : mch) {
    int field_num = 0;
    table->field[field_num++]->store(e.time.c_str(), e.time.length(),
                                     system_charset_info);
    std::string command;
    e.address = "'" + e.address + "'";
    if (e.cctype == alisql::Consensus::CCOpType::CCMemberOp) {
      switch (e.optype) {
        case alisql::Consensus::CCOpType::CCAddNode:
          command = "change consensus_learner " + e.address +
                    " to consensus_follower";
          break;
        case alisql::Consensus::CCOpType::CCDelNode:
          command = "drop consensus_follower " + e.address;
          break;
        case alisql::Consensus::CCOpType::CCDowngradeNode:
          command = "change consensus_follower " + e.address +
                    " to consensus_learner";
          break;
        case alisql::Consensus::CCOpType::CCConfigureNode:
          command =
              "change consensus_node " + e.address + " consensus_force_sync " +
              (e.forceSync ? "true" : "false") + " consensus_election_weight " +
              std::to_string(e.electionWeight);
          break;
        case alisql::Consensus::CCOpType::CCLeaderTransfer:
          command = "change consensus_leader to " + e.address;
          break;
        default:
          break;
      }
    } else {  // alisql::Consensus::CCOpType::CCLearnerOp
      switch (e.optype) {
        case alisql::Consensus::CCOpType::CCAddNode:
        case alisql::Consensus::CCOpType::CCAddLearnerAutoChange:
          command = "add consensus_learner " + e.address;
          break;
        case alisql::Consensus::CCOpType::CCDelNode:
          command = "drop consensus_learner " + e.address;
          break;
        case alisql::Consensus::CCOpType::CCConfigureNode:
          if (e.learnerSource.size())
            e.learnerSource = "'" + e.learnerSource + "'";
          command = "change consensus_node " + e.address +
                    " consensus_learner_source " + e.learnerSource +
                    " consensus_use_applyindex " +
                    (e.sendByAppliedIndex ? "true" : "false");
          break;
        case alisql::Consensus::CCOpType::CCSyncLearnerAll:
          command = "change consensus_learner for consensus_meta";
          break;
        default:
          break;
      }
    }
    command += ";";
    table->field[field_num++]->store(command.c_str(), command.length(),
                                     system_charset_info);
    if (schema_table_store_record(thd, table)) DBUG_RETURN(1);
  }

  DBUG_RETURN(0);
}

ST_FIELD_INFO consensus_commit_pos_info[] = {
    {"LOGNAME", FN_REFLEN, MYSQL_TYPE_STRING, 0, 0, "", 0},
    {"POSITION", 21, MYSQL_TYPE_LONGLONG, 0, MY_I_S_UNSIGNED, "", 0},
    {"INDEX", 21, MYSQL_TYPE_LONGLONG, 0, MY_I_S_UNSIGNED, "", 0},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, "", 0}};

ST_FIELD_INFO alisql_cluster_global_fields_info[] = {
    {"SERVER_ID", 10, MYSQL_TYPE_LONG, 0, 0, 0, 0},
    {"IP_PORT", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"MATCH_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0,
     0},
    {"NEXT_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0,
     0},
    {"ROLE", 10, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"HAS_VOTED", 3, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"FORCE_SYNC", 3, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"ELECTION_WEIGHT", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     0, 0},
    {"LEARNER_SOURCE", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     0, 0},
    {"APPLIED_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0,
     0},
    {"PIPELINING", 3, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"SEND_APPLIED", 3, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, 0}};

ST_FIELD_INFO alisql_cluster_local_fields_info[] = {
    {"SERVER_ID", 10, MYSQL_TYPE_LONG, 0, 0, 0, 0},
    {"CURRENT_TERM", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0,
     0},
    {"CURRENT_LEADER", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"COMMIT_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0,
     0},
    {"LAST_LOG_TERM", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0,
     0},
    {"LAST_LOG_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     0, 0},
    {"ROLE", 10, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"VOTED_FOR", 10, MYSQL_TYPE_LONG, 0, 0, 0, 0},
    {"LAST_APPLY_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     0, 0},
    {"SERVER_READY_FOR_RW", 3, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"INSTANCE_TYPE", 10, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, 0}};

ST_FIELD_INFO alisql_cluster_health_fields_info[] = {
    {"SERVER_ID", 10, MYSQL_TYPE_LONG, 0, 0, 0, 0},
    {"IP_PORT", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"ROLE", 10, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"CONNECTED", 3, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"LOG_DELAY_NUM", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0,
     0},
    {"APPLY_DELAY_NUM", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     0, 0},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, 0}};

ST_FIELD_INFO alisql_cluster_learner_source_fields_info[] = {
    {"LEARNER_SERVER_ID", 10, MYSQL_TYPE_LONG, 0, 0, 0, 0},
    {"LEARNER_IP_PORT", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"SOURCE_SERVER_ID", 10, MYSQL_TYPE_LONG, 0, 0, 0, 0},
    {"SOURCE_IP_PORT", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"SOURCE_LAST_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     0, 0, 0},
    {"SOURCE_COMMIT_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     0, 0, 0},
    {"LEARNER_MATCH_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     0, 0, 0},
    {"LEARNER_NEXT_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     0, 0, 0},
    {"LEARNER_APPLIED_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG,
     0, 0, 0, 0},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, 0}};

ST_FIELD_INFO alisql_cluster_prefetch_channel_info[] = {
    {"CHANNEL_ID", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0,
     0},
    {"FIRST_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0,
     0},
    {"LAST_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0,
     0},
    {"CACHE_SIZE", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0,
     0},
    {"CURRENT_REQUEST", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"STOP_FLAG", 10, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, 0}};

ST_FIELD_INFO alisql_cluster_consensus_status_fields_info[] = {
    {"SERVER_ID", 10, MYSQL_TYPE_LONG, 0, 0, 0, 0},
    {"COUNT_MSG_APPEND_LOG", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG,
     0, 0, 0, 0},
    {"COUNT_MSG_REQUEST_VOTE", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG,
     0, 0, 0, 0},
    {"COUNT_HEARTBEAT", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     0, 0},
    {"COUNT_ON_MSG_APPEND_LOG", MY_INT64_NUM_DECIMAL_DIGITS,
     MYSQL_TYPE_LONGLONG, 0, 0, 0, 0},
    {"COUNT_ON_MSG_REQUEST_VOTE", MY_INT64_NUM_DECIMAL_DIGITS,
     MYSQL_TYPE_LONGLONG, 0, 0, 0, 0},
    {"COUNT_ON_HEARTBEAT", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     0, 0, 0},
    {"COUNT_REPLICATE_LOG", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     0, 0, 0},
    {"COUNT_LOG_META_GET_IN_CACHE", MY_INT64_NUM_DECIMAL_DIGITS,
     MYSQL_TYPE_LONGLONG, 0, 0, 0, 0},
    {"COUNT_LOG_META_GET_TOTAL", MY_INT64_NUM_DECIMAL_DIGITS,
     MYSQL_TYPE_LONGLONG, 0, 0, 0, 0},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, 0}};

ST_FIELD_INFO alisql_cluster_consensus_membership_change_fields_info[] = {
    {"TIME", 32, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"COMMAND", 512, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, 0}};
