/*****************************************************************************

Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

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
/** @file include/komodo0consensus0xlog.h
 Komodo consensus xlog interface.

 Created 2025-06-04 by komodo
 *******************************************************/

#ifndef komodo0consensus0xlog_h
#define komodo0consensus0xlog_h

#include <memory>

#include "consensus_cache_manager.h"
#include "consensus_log_manager.h"

#include "paxos_log.h"
#include "sql/komodo0xlog0meta.h"
#include "sql/serverless/komodo0switchover0manager.h"

namespace komodo {

enum Consensus_log_event_flag {
  FLAG_GU1 = 1,
  FLAG_GU2 = 1 << 1, /* FLAG_GU2 = 3 - FLAG_GU1 */
  FLAG_LARGE_TRX = 1 << 2,
  FLAG_LARGE_TRX_END = 1 << 3,
  FLAG_CONFIG_CHANGE = 1 << 4,
  FLAG_BLOB = 1 << 5,
  FLAG_BLOB_END = 1 << 6,
  FLAG_BLOB_START = 1 << 7 /* we should mark the start for SDK */
};

/** Interface of consensus log which is handled by paxos. */
class Consensus_xlog_handler : public alisql::PaxosLogHandler {
 public:
  enum OP_TYPE {
    NORMAL = 0,
    NOOP = 1,
    CONFIGCHANGE = 7,
    MOCK = 8,
    LARGETRX = 11,
    LARGETRXEND = 12,
    UNCERTAIN = 10
  };

 public:
  // Constructor
  explicit Consensus_xlog_handler(komodo::XLog_meta &xlog_meta,
                                  Consensus_log_manager &xlog_mgr,
                                  Consensus_switchover_manager &switchover_mgr)
      : m_mock_start_index(
            xlog_mgr.get_mock_start_index(opt_consensus_start_index)),
        m_xlog_meta(xlog_meta),
        m_xlog_mgr(xlog_mgr),
        m_switchover_mgr(switchover_mgr) {}

  ~Consensus_xlog_handler() = default;

  int getEntry(uint64_t logIndex, alisql::LogEntry &entry, bool fastFail,
               uint64_t serverId) override;
  int getEntry(uint64_t logIndex, alisql::LogEntry &entry,
               bool fastFail) override;
  uint64_t getFirstIndexOfLastFileTerm(const uint64_t term) override;
  uint64_t getLastIndexOfTerm(const uint64_t term) override;
  const alisql::LogEntry *getEntry(uint64_t, bool) override { return nullptr; }
  bool getLeftSize(uint64_t startLogIndex, uint64_t maxPacketSize) override;
  int getEmptyEntry(alisql::LogEntry &entry) override;
  uint64_t getLastLogIndex() override;
  uint64_t getLastCachedLogIndex() override;
  uint64_t getSafeLastLogIndex() override;
  uint64_t getSafeLastLogIndexNoLock() override;
  uint64_t getWaitMilliseconds4OldTrxFinish() override;
  void forceUpdateAppliedIndex(const uint64_t index) override;

  uint64_t waitOldTrxFinish() override;
  uint64_t waitOldXaFinish() override;
  std::pair<uint64_t, uint64_t> waitOldBgcFinish(alisql::Paxos *paxos) override;
  void setLimitNone() override { m_switchover_mgr.set_limit_none(); }
  void setLimitNewTrx() override { m_switchover_mgr.set_limit_new_trx(); }
  void setLimitXaFinish() override { m_switchover_mgr.set_limit_xa_finish(); }
  void setLimitAll() override { m_switchover_mgr.set_limit_all(); }
  bool isInLeaderTransfer() override {
    return m_switchover_mgr.is_in_switchover();
  }
  bool isInLimitAll() override { return m_switchover_mgr.is_in_limit_all(); }
  uint64_t getLeaderTransferState() override {
    return m_switchover_mgr.get_switchover_state();
  }
  uint64_t getLimitNewTrxState() override {
    return m_switchover_mgr.get_limit_new_trx_state();
  }
  uint64_t getLimitNoneState() override {
    return m_switchover_mgr.get_limit_none_state();
  }

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
  uint64_t getMockStartIndex() override { return m_mock_start_index; }
  void setMockStartIndex(uint64_t index) override { m_mock_start_index = index; }

  uint64_t getCurrentTerm() const { return currentTerm_; }
  static void packLogEntry(uchar *buffer, size_t buf_size, uint64_t term,
                           uint64_t index, OP_TYPE entry_type,
                           alisql::LogEntry &log_entry);

 private:
  // before this index, all log entry should be mocked
  uint64_t m_mock_start_index;

  // consensus log meta info handler.
  komodo::XLog_meta &m_xlog_meta;

  // ConsensusLog Operation detail
  Consensus_log_manager &m_xlog_mgr;

  // ConsensusLog Operation detail
  Consensus_switchover_manager &m_switchover_mgr;
};

}  // namespace komodo
// extern std::shared_ptr<alisql::AliSQLServer> alisql_server;

#endif
