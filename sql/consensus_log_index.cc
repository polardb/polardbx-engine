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

#include "consensus_log_index.h"

int ConsensusLogIndex::init() {
  key_LOCK_consensuslog_index = key_CONSENSUSLOG_LOCK_ConsensusLog_index;
  mysql_mutex_init(key_LOCK_consensuslog_index, &LOCK_consensuslog_index, MY_MUTEX_INIT_FAST);
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

uint64 ConsensusLogIndex::get_start_index_of_file(const std::string & log_name)
{
  uint64 index = 0;
  mysql_mutex_lock(&LOCK_consensuslog_index);
  for (auto riter = index_list.rbegin(); riter != index_list.rend(); ++riter)
  {
    if (log_name == riter->file_name)
    {
      index = riter->index;
      break;
    }
  }
  mysql_mutex_unlock(&LOCK_consensuslog_index);
  return index;

}


std::string ConsensusLogIndex::get_last_log_file_name()
{
  std::string ret;
  mysql_mutex_lock(&LOCK_consensuslog_index);
  ret = index_list.back().file_name;
  mysql_mutex_unlock(&LOCK_consensuslog_index);
  return ret;
}
