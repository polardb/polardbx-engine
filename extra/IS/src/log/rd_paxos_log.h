/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  rd_paxos_log.h,v 1.0 08/15/2016 02:46:50 PM
 *hangfeng.fj(hangfeng.fj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file rd_paxos_log.h
 * @author hangfeng.fj(hangfeng.fj@alibaba-inc.com)
 * @date 08/15/2016 02:15:50 PM
 * @version 1.0
 * @brief
 *
 **/

#ifndef cluster_rd_paxos_log_INC
#define cluster_rd_paxos_log_INC

#include <atomic>
#include <map>
#include <mutex>
#include <string>
#include "paxos.h"
#include "paxos_log.h"

namespace rocksdb {
class DB;
}

namespace alisql {

/**
 * @class RDPaxosLog
 *
 * @brief class for RocksDB Paxos Log
 *
 **/
class RDPaxosLog : public PaxosLog {
 public:
  RDPaxosLog(const std::string &dataDir, bool compress, size_t writeBufferSize,
             bool sync = true);
  virtual ~RDPaxosLog();

  virtual int getEntry(uint64_t logIndex, LogEntry &entry,
                       bool fastfail = false);
  virtual int getEmptyEntry(LogEntry &entry);
  virtual uint64_t getLastLogIndex();
  virtual uint64_t append(const LogEntry &entry);
  virtual void truncateBackward(uint64_t firstIndex);
  virtual void truncateForward(uint64_t lastIndex);
  virtual int getMetaData(const std::string &key, std::string &value);
  virtual int setMetaData(const std::string &key, const std::string &value);
  virtual int getMetaData(const std::string &key, uint64_t *value);
  virtual int setMetaData(const std::string &key, const uint64_t value);
  virtual uint64_t append(
      const ::google::protobuf::RepeatedPtrField<LogEntry> &entries);
  virtual uint64_t getLastLogTerm();

  uint64_t getFirstLogIndex();
  void appendEmptyEntry();
  virtual uint64_t getLength();
  bool readEntry(uint64_t index, LogEntry *logEntry);
  uint64_t appendEntry(const LogEntry &logEntry);
  static uint64_t stringToInt(const std::string &s);
  static std::string intToString(uint64_t num);
  static void intToString(uint64_t num, std::string &key);
  void encodeLogEntry(const LogEntry &logEntry, std::string *buf);
  void decodeLogEntry(const std::string &buf, LogEntry *logEntry);

  void set_debug_async() { async_ = true; }

  void setPurgeLogFilter(std::function<bool(const LogEntry &le)> cb) {
    purgeLogFilter_ = cb;
  }
  void debugSetLastLogIndex(uint i) {
    std::lock_guard<std::mutex> lg(lock_);
    firstLogIndex_ = lastLogIndex_ = i;
  }

 private:
  void initLastLogTerm();
  rocksdb::DB *db_;
  uint64_t lastLogIndex_;
  uint64_t lastLogTerm_;
  /* log index we purge from */
  std::atomic<uint64_t> firstLogIndex_;
  bool async_;
  bool sync_;

  std::function<bool(const LogEntry &le)>
      purgeLogFilter_; /* act as a filter for purging log */
};                     /* end of class RDPaxosLog */

};                     /* end of namespace alisql */

#endif                 // #ifndef cluster_rd_paxos_log_INC
