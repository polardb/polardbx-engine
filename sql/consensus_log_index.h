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

#ifndef CONSENSUS_LOG_INDEX_INCLUDE
#define CONSENSUS_LOG_INDEX_INCLUDE

// #include "my_global.h"
#include "mysqld.h"
#include <deque>
#include <vector>
#include <string>


struct ConsensusLogIndexEntry {
  uint64          index;
  ulong           timestamp;
  std::string     file_name;
};


class ConsensusLogIndex {
public:
  ConsensusLogIndex():inited(false) {};
  ~ConsensusLogIndex() {};

  int init();
  int cleanup();

  int add_to_index_list(uint64 consensus_index, ulong timastamp, std::string & log_name);
  int truncate_before(std::string & log_name);  // retain log_name
  int truncate_after(std::string & log_name);   // retain log_name

  int get_log_file_from_index(uint64 consensus_index, std::string &log_name);
  int get_log_file_by_timestamp(ulong timestamp, std::string &log_name);
  int get_log_file_list(std::vector<std::string> &file_list);
  uint64 get_start_index_of_file(const std::string & log_name);
  std::string get_last_log_file_name();

  uint64 get_first_index();

private:
  bool                                                    inited;
  PSI_mutex_key                                           key_LOCK_consensuslog_index;
  mysql_mutex_t                                           LOCK_consensuslog_index;
  std::deque<ConsensusLogIndexEntry>                      index_list;  // hold all the binlogs' index
};


#endif
