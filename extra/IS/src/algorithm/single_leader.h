/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  single_leader.h,v 1.0 05/08/2017 3:42:01 PM
 *jarry.zj(jarry.zj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file single_leader.h
 * @author jarry.zj(jarry.zj@alibaba-inc.com)
 * @date 05/08/2017 3:42:01 PM
 * @version 1.0
 * @brief
 *
 **/

#ifndef CONSENSUS_INCLUDE_SINGLE_LEADER_H_
#define CONSENSUS_INCLUDE_SINGLE_LEADER_H_

#include <atomic>
#include <memory>
#include "consensus.h"
#include "paxos.pb.h"

namespace alisql {

/*
 * singleLeader
 *
 * no heartbeat, send msg to the only learner, no changemember
 */

class MemPaxosLog;
class LearnerServer;

class SingleLeader : public Consensus {
 public:
  SingleLeader(uint64_t clusterId);

  virtual ~SingleLeader() { shutdown(); }
  void init(int port, std::string learnerAddr, uint64_t lastLogIndex = 0,
            uint64_t lastTerm = 0, uint64_t cacheSize = 1000,
            uint64_t ioThreadCnt = 4, uint64_t workThreadCnt = 4);
  virtual uint64_t replicateLog(LogEntry &entry);
  void appendLogToLearner(bool needLock = false);
  uint64_t appendLogFillForEach(PaxosMsg *msg, LearnerServer *server);
  virtual int onAppendLogResponce(PaxosMsg *msg);
  std::shared_ptr<MemPaxosLog> getLog() { return log_; }
  virtual uint64_t getClusterId() { return clusterId_.load(); }
  uint64_t getLearnerMatchIndex();
  uint64_t getLastLogIndex();

  /* functions not use */
  virtual int onAppendLogSendFail(PaxosMsg *msg, uint64_t *newId) { return 0; }
  virtual int onAppendLog(PaxosMsg *msg, PaxosMsg *rsp) { return 0; };
  virtual int onRequestVote(PaxosMsg *msg, PaxosMsg *rsp) { return 0; }
  virtual int onRequestVoteResponce(PaxosMsg *msg) { return 0; }
  virtual int onLeaderCommand(PaxosMsg *msg, PaxosMsg *rsp) { return 0; }
  virtual int onLeaderCommandResponce(PaxosMsg *msg) { return 0; }
  virtual int requestVote(bool force) { return 0; }
  virtual int appendLog(const bool needLock) {
    return 0;
  } /* only appendLogToLearner */
  virtual int onClusterIdNotMatch(PaxosMsg *msg) { return 0; }
  virtual int onMsgPreCheck(PaxosMsg *msg, PaxosMsg *rsp) { return 0; }
  virtual int onMsgPreCheckFailed(PaxosMsg *msg) { return 0; }
  virtual int setClusterId(uint64_t ci) { return 0; }
  virtual bool isShutdown() { return false; }

 protected:
  void stop();
  void shutdown();

  std::shared_ptr<Service> srv_;
  std::shared_ptr<LearnerServer> learnerServer_;
  std::shared_ptr<MemPaxosLog> log_;
  std::atomic<uint64_t> currentTerm_;
  std::atomic<uint64_t> commitIndex_;
  const uint64_t heartbeatTimeout_;
  std::atomic<uint64_t> clusterId_;
  mutable std::mutex lock_;
  uint64_t mockLastIndex_;
  uint64_t mockLastTerm_;
  const static uint64_t maxSystemPacketSize_;

  friend class LearnerServer;
};

}  // namespace alisql

#endif /* CONSENSUS_INCLUDE_SINGLE_LEADER_H_ */
