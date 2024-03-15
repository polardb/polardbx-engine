/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

#ifndef BL_CONSENSUS_LOG_INCLUDE
#define BL_CONSENSUS_LOG_INCLUDE

#include <memory>

#include "consensus_fifo_cache_manager.h"
#include "consensus_log_manager.h"

#include "paxos.h"
#include "paxos_log.h"

namespace alisql {
class AliSQLServer;
class Paxos;
}  // namespace alisql

struct ConsensusStateChange {
  alisql::Paxos::StateType state{alisql::Paxos::NOROLE};
  uint64 term{0};
  uint64 index{0};
};

class BLConsensusLog : public alisql::PaxosLog {
 public:
  enum Consensus_Log_Op_Type {
    NORMAL = 0,
    NOOP = 1,
    CONFIGCHANGE = 7,
    MOCK = 8,
    LARGETRX = 11,
    LARGETRXEND = 12,
    UNCERTAIN = 10
  };

  BLConsensusLog();
  ~BLConsensusLog() override;
  void init(uint64 fake_start_index_arg,
            ConsensusLogManager *consensus_log_manager_arg);

  int getEntry(uint64_t logIndex, alisql::LogEntry &entry, bool fastFail,
               uint64_t serverId) override;
  int getEntry(uint64_t logIndex, alisql::LogEntry &entry,
               bool fastFail) override;
  const alisql::LogEntry *getEntry(uint64_t, bool) override { return nullptr; }
  uint64_t getLeftSize(uint64_t startLogIndex) override;
  bool getLeftSize(uint64_t startLogIndex, uint64_t maxPacketSize) override;
  int getEmptyEntry(alisql::LogEntry &entry) override;
  uint64_t getLastLogIndex() override;
  uint64_t getLastCachedLogIndex() override;
  uint64_t getSafeLastLogIndex() override;
  uint64_t appendWithCheck(const alisql::LogEntry &entry) override;
  uint64_t append(const alisql::LogEntry &entry) override;
  uint64_t append(const ::google::protobuf::RepeatedPtrField<alisql::LogEntry>
                      &entries) override;
  void truncateBackward(uint64_t firstIndex) override;
  void truncateForward(uint64_t lastIndex) override;
  int getMetaData(const std::string &key, uint64_t *value) override;
  int getMetaData(const std::string &key, std::string &value) override;
  int setMetaData(const std::string &key, uint64_t value) override;
  int setMetaData(const std::string &key, const std::string &value) override;
  void setTerm(uint64_t term) override;
  uint64_t getLength() override;
  bool isStateMachineHealthy() override;

  uint64 getCurrentTerm() const { return currentTerm_; }
  static void packLogEntry(uchar *buffer, size_t buf_size, uint64 term,
                           uint64 index, Consensus_Log_Op_Type entry_type,
                           alisql::LogEntry &log_entry);

 private:
  uint64 mock_start_index;  // before this index, all log entry should be mocked
  ConsensusLogManager *consensusLogManager_;  // ConsensusLog Operation detail
};

extern std::shared_ptr<BLConsensusLog> consensus_log;
extern std::shared_ptr<alisql::AliSQLServer> alisql_server;
extern alisql::Paxos *consensus_ptr;

#endif