/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  paxos_log.h,v 1.0 08/17/2016 11:20:41 AM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file paxos_log.h
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 08/17/2016 11:20:41 AM
 * @version 1.0
 * @brief the interface of paxos log.
 *
 **/

#ifndef cluster_paxos_log_INC
#define cluster_paxos_log_INC

#include <atomic>
#include <deque>
#include <functional>
#include <mutex>
#include <string>
#include "log_meta_cache.h"
#include "paxos.pb.h"

namespace alisql {

enum LogOperation {
  kNormal = 0,
  kPut = 3,
  kDel = 4,
  kCas = 5,
  kTairSet = 6,
  kConfigureChange = 7,
  kMock = 8,
  kCommitDep = 11,
  kCommitDepEnd = 12,
  kNop = 10
};

/**
 * @class PaxosLog
 *
 * @brief
 *
 **/
class PaxosLog {
 public:
  PaxosLog() : currentTerm_(0) {}
  virtual ~PaxosLog() = default;
  PaxosLog(const PaxosLog &other) = delete;  // copy constructor
  const PaxosLog &operator=(const PaxosLog &other) =
      delete;                                // assignment operator

  virtual int getEntry(uint64_t logIndex, LogEntry &entry, bool fastfail,
                       uint64_t /* serverId */) {
    return getEntry(logIndex, entry, fastfail);
  }
  virtual int getEntry(uint64_t logIndex, LogEntry &entry,
                       bool fastfail = false) = 0;
  virtual const LogEntry *getEntry(uint64_t /* logIndex */,
                                   bool /* fastfail */ = false) {
    return nullptr;
  }
  virtual int getEmptyEntry(LogEntry &entry) = 0;
  virtual uint64_t getLeftSize(uint64_t startLogIndex) {
    const LogEntry *entry;
    uint64_t lastLogIndex = getLastLogIndex();
    uint64_t size = 0;
    for (uint64_t i = startLogIndex; i <= lastLogIndex; ++i) {
      if (nullptr != (entry = getEntry(i, true)))
        size += entry->ByteSizeLong();
      else
        break;
    }
    return size;
  }
  virtual bool getLeftSize(uint64_t startLogIndex, uint64_t maxPacketSize) {
    const LogEntry *entry;
    uint64_t lastLogIndex = getLastLogIndex();
    uint64_t size = 0;
    for (uint64_t i = startLogIndex; i <= lastLogIndex; ++i) {
      if (nullptr != (entry = getEntry(i, true)))
        size += entry->ByteSizeLong();
      else
        break;

      if (size >= maxPacketSize) return true;
    }
    return false;
  }
  virtual uint64_t getLastLogTerm() {
    LogEntry entry;
    getEntry(getLastLogIndex(), entry, false);
    return entry.term();
  }
  virtual uint64_t getLastLogIndex() = 0;
  virtual uint64_t getLastCachedLogIndex() { return getLastLogIndex(); }
  // get the actual sync index before transferring leader
  // should not be called if paxos mutex is held, there is deadlock risk in
  // AliSQLServer
  virtual uint64_t getSafeLastLogIndex() { return getLastLogIndex(); }
  virtual uint64_t getLength() = 0;
  virtual uint64_t append(const LogEntry &entry) = 0;
  virtual uint64_t appendWithCheck(const LogEntry &entry) {
    return append(entry);
  }
  /* Truncate the log entry after (include!) firstIndex. */
  virtual void truncateBackward(uint64_t firstIndex) = 0;
  /* Truncate the log entry before (exclude!) lastIndex */
  virtual void truncateForward(uint64_t lastIndex) = 0;
  virtual void setPurgeLogFilter(
      std::function<bool(const LogEntry &le)> /* cb */) {}
  virtual int getMetaData(const std::string &key, uint64_t *value) = 0;
  virtual int setMetaData(const std::string &key, uint64_t value) = 0;
  virtual int getMetaData(const std::string &key, std::string &value) {
    uint64_t ivalue;
    int ret = getMetaData(key, &ivalue);
    if (ret == 0)
      value = std::to_string(ivalue);
    else
      value = "";
    return ret;
  }
  virtual int setMetaData(const std::string &key, const std::string &value) {
    if (value.empty())
      return -1;
    else
      return setMetaData(key, std::stoull(value));
  }
  virtual uint64_t append(
      const ::google::protobuf::RepeatedPtrField<LogEntry> &entries) = 0;
  virtual void setTerm(uint64_t term) {
    lock_.lock();
    /* We may reset term now 20170321 */
    // assert(term >= currentTerm_);
    currentTerm_ = term;
    lock_.unlock();
  }

  typedef struct Stats {
    std::atomic<uint64_t> countMetaGetTotal;
    std::atomic<uint64_t> countMetaGetInCache;
    Stats() : countMetaGetTotal(0), countMetaGetInCache(0) {}
  } StatsType;
  const StatsType &getStats() { return stats_; }

  void initMetaCache() { metaCache_.init(); }
  void resetMetaCache() { metaCache_.reset(); }
  void putLogMeta(uint64_t index, uint64_t term, uint64_t optype,
                  uint64_t info) {
    metaCache_.putLogMeta(index, term, optype, info);
  }
  int getLogMeta(uint64_t index, uint64_t *term, uint64_t *optype,
                 uint64_t *info) {
    int ret = 0;
    if (metaCache_.getLogMeta(index, term, optype, info) == false) {
      LogEntry le;
      ret = getEntry(index, le, false);
      *term = le.term();
      *optype = le.optype();
      *info = le.info();
      assert(index == le.index());
    } else {
      ++stats_.countMetaGetInCache;
    }
    ++stats_.countMetaGetTotal;
    return ret;
  }
  virtual bool isStateMachineHealthy() { return true; }
  virtual bool entriesPreCheck(
      const ::google::protobuf::RepeatedPtrField<LogEntry> &) {
    return false;
  }

 protected:
  uint64_t currentTerm_;
  std::mutex lock_;

  StatsType stats_;

 private:
  LogMetaCache metaCache_;
}; /* end of class PaxosLog */

}  // namespace alisql

#endif  // #ifndef cluster_paxos_log_INC
