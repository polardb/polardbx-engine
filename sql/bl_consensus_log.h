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

#ifndef BL_CONSENSUS_LOG_INCLUDE
#define BL_CONSENSUS_LOG_INCLUDE

#include "consensus_log_manager.h"
// #include "my_global.h"

#ifdef NORMANDY_CLUSTER
#include "../cluster/consensus/paxos.h"
#include "../cluster/consensus/paxos_log.h"
#include "../cluster/consensus/paxos_server.h"

#include <memory>

struct ConsensusStateChange
{
  alisql::Paxos::StateType state; 
  uint64 term;
  uint64 index;
};

class BLConsensusLog : public alisql::PaxosLog{
public :

  enum Consensus_Log_Op_Type
  {
    NORMAL = 0,
    NOOP = 1,
    CONFIGCHANGE = 7,
    MOCK = 8,
    LARGETRX = 11,
    LARGETRXEND = 12,
    UNCERTAIN = 10
  };

  BLConsensusLog();
  virtual ~BLConsensusLog();
  void init(uint64 fake_start_index_arg, ConsensusLogManager *consensus_log_manager_arg);

  virtual int getEntry(uint64_t logIndex, alisql::LogEntry &entry, bool fastFail, uint64_t serverId);
  virtual int getEntry(uint64_t logIndex, alisql::LogEntry &entry, bool fastFail);
  virtual const alisql::LogEntry *getEntry(uint64_t logIndex __attribute__((unused)), bool fastfail __attribute__((unused)) = false) { return NULL; }
  virtual uint64_t getLeftSize(uint64_t startLogIndex);
  virtual bool getLeftSize(uint64_t startLogIndexi, uint64_t maxPacketSize);
  virtual int getEmptyEntry(alisql::LogEntry &entry);
  virtual uint64_t getLastLogIndex();
  virtual uint64_t getLastCachedLogIndex();
  virtual uint64_t getSafeLastLogIndex();
  virtual uint64_t appendWithCheck(const alisql::LogEntry &entry);
  virtual uint64_t append(const alisql::LogEntry &entry);
  virtual uint64_t append(const ::google::protobuf::RepeatedPtrField<alisql::LogEntry> &entries);
  virtual void truncateBackward(uint64_t firstIndex);
  virtual void truncateForward(uint64_t lastIndex);
  virtual int getMetaData(const std::string &key, uint64_t *value);
  virtual int getMetaData(const std::string &key, std::string &value);
  virtual int setMetaData(const std::string &key, const uint64_t value);
  virtual int setMetaData(const std::string &key, const std::string &value);
  virtual void setTerm(uint64_t term); 
  virtual uint64_t getLength();
  virtual bool isStateMachineHealthy();

  uint64 getCurrentTerm() { return currentTerm_; }
  static void packLogEntry(uchar* buffer, size_t buf_size, uint64 term, uint64 index, Consensus_Log_Op_Type entry_type, alisql::LogEntry& log_entry);

private:
  uint64          mock_start_index;  // before this index, all log entry should be mocked
  ConsensusLogManager  *consensusLogManager_;  // ConsensusLog Operation detail

};

extern std::shared_ptr<BLConsensusLog> consensus_log;
extern std::shared_ptr<alisql::AliSQLServer> alisql_server;
extern alisql::Paxos *consensus_ptr;

#endif

#endif

