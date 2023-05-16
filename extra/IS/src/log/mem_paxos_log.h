/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  mem_paxos_log.h,v 1.0 12/22/2016 2:22:37 PM
 *jarry.zj(jarry.zj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file mem_paxos_log.h
 * @author jarry.zj(jarry.zj@alibaba-inc.com)
 * @date 12/22/2016 2:22:37 PM
 * @version 1.0
 * @brief memory based Paxos Log
 *
 **/

#ifndef CONSENSUS_INCLUDE_MEM_PAXOS_LOG_H_
#define CONSENSUS_INCLUDE_MEM_PAXOS_LOG_H_

#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>
#include "paxos.pb.h"
#include "paxos_log.h"

namespace alisql {

/* TODO add sync to disk option */

/**
 * @class MemPaxosLog
 * @brief class for memory based Paxos log
 * MemPaxosLog is used in learner node
 *
 */
class MemPaxosLog : public PaxosLog {
 public:
  /**
   * init a MemPaxosLog
   * @lastLogIndex
   * @cacheSize maximum size of log kept in memory (non-strict constraint). if
   * full, block the append oepration.
   */
  MemPaxosLog(uint64_t lastLogIndex = 0, uint64_t readTimeout = 0,
              uint64_t cacheSize = 1000);
  virtual ~MemPaxosLog();

  /**
   * getEntry by logIndex (do not delete entry from the queue)
   * non-blocking
   */
  virtual int getEntry(uint64_t logIndex, LogEntry &entry,
                       bool fastfail = false);
  /**
   * getEntry from the queue (pop the first entry)
   * blocking
   */
  virtual int getEntry(LogEntry &entry);
  virtual int getEmptyEntry(LogEntry &entry);
  virtual uint64_t getLastLogIndex();
  virtual uint64_t getLength();
  virtual uint64_t append(const LogEntry &entry);
  virtual uint64_t append(
      const ::google::protobuf::RepeatedPtrField<LogEntry> &entries);
  virtual void truncateBackward(uint64_t firstIndex);
  virtual void truncateForward(uint64_t lastIndex);
  virtual int getMetaData(const std::string &key, uint64_t *value);
  virtual int setMetaData(const std::string &key, const uint64_t value);

  void appendEmptyEntry();
  void setAppendTimeout(uint64_t t) { appendTimeout_ = t; }
  void resetLastLogIndex(uint lli);

 protected:
  void shutdown();
  std::condition_variable isFullCond_;
  std::condition_variable isEmptyCond_;

  uint64_t lastLogIndex_;
  uint64_t cacheSize_;
  uint64_t readTimeout_;
  uint64_t appendTimeout_;
  std::deque<LogEntry *> log_;
};

} /* namespace alisql */

#endif /* CONSENSUS_INCLUDE_MEM_PAXOS_LOG_H_ */
