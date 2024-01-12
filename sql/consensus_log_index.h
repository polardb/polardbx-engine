/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  consensus_log_index.h,v 1.0 08/22/2016 12:37:45 PM
 *droopy.hw(droopy.hw@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file consensus_log_manager.h
 * @author droopy.hw(droopy.hw@alibaba-inc.com)
 * @date 08/22/2016 12:37:45 PM
 * @version 1.0
 * @brief the CONSENSUS log index
 *
 **/

#ifndef CONSENSUS_LOG_INDEX_INCLUDE
#define CONSENSUS_LOG_INDEX_INCLUDE

// #include "my_global.h"
#include <deque>
#include <string>
#include <vector>
#include "mysqld.h"

#ifdef HAVE_PSI_INTERFACE
extern PSI_mutex_key key_CONSENSUSLOG_LOCK_ConsensusLog_index;
#endif

struct ConsensusLogIndexEntry {
  uint64 index;
  ulong timestamp;
  std::string file_name;
};

class ConsensusLogIndex {
 public:
  ConsensusLogIndex() : inited(false) {}
  ~ConsensusLogIndex() = default;

  int init();
  int cleanup();

  int add_to_index_list(uint64 consensus_index, ulong timastamp,
                        std::string &log_name);
  int truncate_before(std::string &log_name);  // retain log_name
  int truncate_after(std::string &log_name);   // retain log_name

  int get_log_file_from_index(uint64 consensus_index, std::string &log_name);
  int get_log_file_by_timestamp(ulong timestamp, std::string &log_name);
  int get_log_file_list(std::vector<std::string> &file_list);
  uint64 get_start_index_of_file(const std::string &log_name);
  std::string get_last_log_file_name();

  uint64 get_first_index();

 private:
  bool inited;
  PSI_mutex_key key_LOCK_consensuslog_index;
  mysql_mutex_t LOCK_consensuslog_index;
  std::deque<ConsensusLogIndexEntry> index_list;  // hold all the binlogs' index
};

#endif
