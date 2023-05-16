/************************************************************************
 *
 * Copyright (c) 2017 Alibaba.com, Inc. All Rights Reserved
 * $Id:  paxos_log_cache.h,v 1.0 01/16/2017 05:44:32 PM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file paxos_log_cache.h
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 01/16/2017 05:44:32 PM
 * @version 1.0
 * @brief
 *
 **/

#ifndef paxos_log_cache_INC
#define paxos_log_cache_INC

#include <deque>
#include "paxos.pb.h"

namespace alisql {

class PaxosLogCacheNode {
 public:
  uint64_t beginIndex;
  uint64_t endIndex;
  ::google::protobuf::RepeatedPtrField<LogEntry> entries;

  PaxosLogCacheNode(uint64_t bi, uint64_t ei,
                    ::google::protobuf::RepeatedPtrField<LogEntry> &ets)
      : beginIndex(bi), endIndex(ei) {
    entries.CopyFrom(ets);
  }

  int getByteSize() {
    int ret = 0;
#ifdef DEBUG_LOG_CACHE
    for (auto it = entries.begin(); it != entries.end(); ++it) {
      ret += it->ByteSize();
    }
#endif
    return ret;
  }
};

/**
 * @class PaxosLogCache
 *
 * @brief
 *
 **/
class PaxosLogCache {
 public:
  PaxosLogCache() { byteSize_ = 0; }
  virtual ~PaxosLogCache() {}

  /* May be merge node to exist one. */
  void put(uint64_t beginIndex, uint64_t endIndex,
           ::google::protobuf::RepeatedPtrField<LogEntry> &entries);
  void put(PaxosLogCacheNode *newNode);
  PaxosLogCacheNode *get(uint64_t beginIndex);
  void clear();
  int getByteSize() { return byteSize_; }
  void setCommitIndex(uint64_t index) {
    commitIndex = index > commitIndex ? index : commitIndex;
  }
  uint64_t getCommitIndex() { return commitIndex; }

  PaxosLogCacheNode *debugGet(uint64_t i);
  int debugGetByteSize();

 protected:
  std::deque<PaxosLogCacheNode *> logCache_;
  int byteSize_;
  /*
   * max msg->commitIndex in cache
   * used for configureChange
   * (TODO) follower commitIndex_ is a bit smaller
   */
  uint64_t commitIndex;

 private:
  PaxosLogCache(const PaxosLogCache &other);  // copy constructor
  const PaxosLogCache &operator=(
      const PaxosLogCache &other);            // assignment operator

};                                            /* end of class PaxosLogCache */

}  // namespace alisql

#endif  // #ifndef paxos_log_cache_INC
