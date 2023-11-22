/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  witness.h,v 1.0 12/20/2016 07:15:45 PM
 *jarry.zj(jarry.zj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file witness.h
 * @author jarry.zj(jarry.zj@alibaba-inc.com)
 * @date 12/20/2016 07:15:45 PM
 * @version 1.0
 * @brief Witness node in paxos protocol
 *
 **/

#ifndef ALICONSENSUS_INCLUDE_WITNESS_H_
#define ALICONSENSUS_INCLUDE_WITNESS_H_

#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include "consensus.h"
#include "mem_paxos_log.h"
#include "service.h"
#include "thread_timer.h"

extern easy_log_level_t easy_log_level;
#define PRINT_TIME()                                  \
  do {                                                \
    struct timeval tv;                                \
    if (easy_log_level >= EASY_LOG_INFO) {            \
      gettimeofday(&tv, NULL);                        \
      printf("TS:%ld.%06ld ", tv.tv_sec, tv.tv_usec); \
      std::cout << std::flush;                        \
    }                                                 \
  } while (0)
namespace alisql {

class Witness : public Consensus {
 public:
  Witness(std::string addr, std::shared_ptr<MemPaxosLog> log,
          uint64_t clusterId = 0, uint64_t ioThreadCnt = 4,
          uint64_t workThreadCnt = 4);
  virtual ~Witness();

  /* Callback for witness */
  virtual int onAppendLog(PaxosMsg *msg, PaxosMsg *rsp);

  /* functions do not need to implement */
  virtual int onRequestVote(PaxosMsg *msg, PaxosMsg *rsp);
  virtual int onRequestVoteResponce(PaxosMsg *msg) { return 0; }
  virtual int onAppendLogSendFail(PaxosMsg *msg, uint64_t *newId = nullptr) {
    return 0;
  }
  virtual int onAppendLogResponce(PaxosMsg *msg) { return 0; }
  virtual int requestVote(bool force) { return 0; }
  virtual uint64_t replicateLog(LogEntry &entry) { return 0; }
  virtual int appendLog(const bool needLock) { return 0; }
  virtual int onLeaderCommand(PaxosMsg *msg, PaxosMsg *rsp);
  virtual int onLeaderCommandResponce(PaxosMsg *msg) { return 0; };
  virtual int onClusterIdNotMatch(PaxosMsg *msg) { return 0; };
  virtual int onMsgPreCheck(PaxosMsg *msg, PaxosMsg *rsp) { return 0; }
  virtual int onMsgPreCheckFailed(PaxosMsg *msg) { return 0; }

  void updateAppliedIndex(uint64_t index);
  uint64_t getCommitIndex() { return commitIndex_; }
  void setCommitIndex(uint64_t ci) { commitIndex_ = ci; }
  uint64_t getLastLogIndex() { return log_->getLastLogIndex(); }
  uint64_t getCurrentLeader() { return leaderId_; }
  std::shared_ptr<MemPaxosLog> getLog() { return log_; }
  virtual uint64_t getClusterId() { return clusterId_.load(); }
  virtual int setClusterId(uint64_t ci) {
    clusterId_.store(ci);
    return 0;
  }

  virtual void setLeaderChangeCb(std::function<void(uint64_t leaderId)> cb);
  bool isShutdown() { return shutdown_.load(); }
  void resetLastLogIndex(uint64_t lli);

  static bool debugWitnessTest;

 protected:
  std::shared_ptr<MemPaxosLog> log_; /* MemPaxosLog is required */
  std::shared_ptr<Service> srv_;

  /* timeout unit is ms */
  uint64_t currentTerm_;
  std::atomic<uint64_t> commitIndex_;
  std::atomic<uint64_t> appliedIndex_;
  uint64_t leaderId_;
  uint64_t serverId;
  std::atomic<uint64_t> clusterId_;
  std::atomic<bool> shutdown_;

  mutable std::mutex lock_;

  /* user-defined async callback functions */
  std::function<void(uint64_t leaderId)> leaderChangeHandler_;

 private:
  Witness(const Witness &other);
  const Witness &operator=(const Witness &other);
};

}  // namespace alisql

#endif /* ALICONSENSUS_INCLUDE_WITNESS_H_ */
