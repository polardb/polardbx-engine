/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  consensus_log_index.cc,v 1.0 08/22/2016 09:16:32 AM
 *droopy.hw(droopy.hw@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file consensus_log_index.cc
 * @author droopy.hw(droopy.hw@alibaba-inc.com)
 * @date 08/22/2016 09:16:32 PM
 * @version 1.0
 * @brief The implement of the CONSENSUS log index
 *
 **/

#include "consensus_log_index.h"
#include "mysql/psi/mysql_mutex.h"
#include "thr_mutex.h"

#ifdef HAVE_PSI_INTERFACE
PSI_mutex_key key_CONSENSUSLOG_LOCK_ConsensusLog_index;
#endif

int ConsensusLogIndex::init() {
  key_LOCK_consensuslog_index = key_CONSENSUSLOG_LOCK_ConsensusLog_index;
  mysql_mutex_init(key_LOCK_consensuslog_index, &LOCK_consensuslog_index,
                   MY_MUTEX_INIT_FAST);
  inited = true;
  return 0;
}

int ConsensusLogIndex::cleanup() {
  if (inited) {
    mysql_mutex_destroy(&LOCK_consensuslog_index);
  }
  return 0;
}

int ConsensusLogIndex::add_to_index_list(uint64 consensus_index,
                                         ulong timestamp,
                                         std::string &log_name) {
  ConsensusLogIndexEntry entry = {consensus_index, timestamp, log_name};
  mysql_mutex_lock(&LOCK_consensuslog_index);
  index_list.push_back(entry);
  mysql_mutex_unlock(&LOCK_consensuslog_index);
  return 0;
}

int ConsensusLogIndex::truncate_before(std::string &log_name) {
  mysql_mutex_lock(&LOCK_consensuslog_index);
  for (auto iter = index_list.begin(); iter != index_list.end(); ++iter) {
    if (log_name == iter->file_name) {
      index_list.erase(index_list.begin(), iter);
      break;
    }
  }
  mysql_mutex_unlock(&LOCK_consensuslog_index);
  return 0;
}

int ConsensusLogIndex::truncate_after(std::string &log_name) {
  mysql_mutex_lock(&LOCK_consensuslog_index);
  for (auto iter = index_list.begin(); iter != index_list.end(); ++iter) {
    if (log_name == iter->file_name) {
      index_list.erase(iter + 1, index_list.end());
      break;
    }
  }
  mysql_mutex_unlock(&LOCK_consensuslog_index);
  return 0;
}

int ConsensusLogIndex::get_log_file_from_index(uint64 consensus_index,
                                               std::string &log_name) {
  bool found = false;
  mysql_mutex_lock(&LOCK_consensuslog_index);
  for (auto riter = index_list.rbegin(); riter != index_list.rend(); ++riter) {
    if (consensus_index >= riter->index) {
      found = true;
      log_name = riter->file_name;
      break;
    }
  }
  mysql_mutex_unlock(&LOCK_consensuslog_index);
  return (!found);
}

int ConsensusLogIndex::get_log_file_by_timestamp(ulong timestamp,
                                                 std::string &log_name) {
  bool found = false;
  mysql_mutex_lock(&LOCK_consensuslog_index);
  for (auto riter = index_list.rbegin(); riter != index_list.rend(); ++riter) {
    if (timestamp >= riter->timestamp) {
      found = true;
      log_name = riter->file_name;
      break;
    }
  }
  mysql_mutex_unlock(&LOCK_consensuslog_index);
  return (!found);
}

int ConsensusLogIndex::get_log_file_list(std::vector<std::string> &file_list) {
  mysql_mutex_lock(&LOCK_consensuslog_index);
  for (auto iter = index_list.begin(); iter != index_list.end(); ++iter) {
    file_list.push_back(iter->file_name);
  }
  mysql_mutex_unlock(&LOCK_consensuslog_index);
  return 0;
}

uint64 ConsensusLogIndex::get_first_index() {
  uint64 first_index = 0;
  mysql_mutex_lock(&LOCK_consensuslog_index);
  auto iter = index_list.begin();
  if (iter != index_list.end()) first_index = iter->index;
  mysql_mutex_unlock(&LOCK_consensuslog_index);
  return first_index;
}

uint64 ConsensusLogIndex::get_start_index_of_file(const std::string &log_name) {
  uint64 index = 0;
  mysql_mutex_lock(&LOCK_consensuslog_index);
  for (auto riter = index_list.rbegin(); riter != index_list.rend(); ++riter) {
    if (log_name == riter->file_name) {
      index = riter->index;
      break;
    }
  }
  mysql_mutex_unlock(&LOCK_consensuslog_index);
  return index;
}

std::string ConsensusLogIndex::get_last_log_file_name() {
  std::string ret;
  mysql_mutex_lock(&LOCK_consensuslog_index);
  ret = index_list.back().file_name;
  mysql_mutex_unlock(&LOCK_consensuslog_index);
  return ret;
}
