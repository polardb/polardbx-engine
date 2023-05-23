/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  consensus.h,v 1.0 07/27/2016 11:42:30 AM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file consensus.h
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 07/27/2016 11:42:30 AM
 * @version 1.0
 * @brief the interface class of Consensus
 *
 **/

#ifndef cluster_consensus_INC
#define cluster_consensus_INC

#include "paxos.pb.h"
#include "service.h"

namespace alisql {

/**
 * @class Consensus
 *
 * @brief interface of consensus algorithm module
 *
 **/
class Consensus {
 public:
  enum MsgType {
    RequestVote = 0,
    RequestVoteResponce,
    AppendLog,
    AppendLogResponce,
    LeaderCommand,
    LeaderCommandResponce,
    ClusterIdNotMatch,
    OptimisticHeartbeat,
    PreCheckFailedResponce
  };

  typedef enum CCOpType {
    CCNoOp = 0,
    CCMemberOp = 1,
    CCLearnerOp = 2,
    CCAddNode = 3,
    CCDelNode = 4,
    CCConfigureNode = 5,
    CCDowngradeNode = 6,
    CCSyncLearnerAll = 7,
    CCAddLearnerAutoChange = 8,
    CCLeaderTransfer = 9
  } CCOpTypeT;

  Consensus() {}
  virtual ~Consensus() {}

  virtual int onRequestVote(PaxosMsg *msg, PaxosMsg *rsp) = 0;
  virtual int onAppendLog(PaxosMsg *msg, PaxosMsg *rsp) = 0;
  virtual int onRequestVoteResponce(PaxosMsg *msg) = 0;
  virtual int onAppendLogSendFail(PaxosMsg *msg, uint64_t *newId) = 0;
  virtual int onAppendLogResponce(PaxosMsg *msg) = 0;
  virtual int requestVote(bool force) = 0;
  virtual uint64_t replicateLog(LogEntry &entry) = 0;
  virtual int appendLog(const bool needLock) = 0;
  virtual int onLeaderCommand(PaxosMsg *msg, PaxosMsg *rsp) = 0;
  virtual int onLeaderCommandResponce(PaxosMsg *msg) = 0;
  virtual int onClusterIdNotMatch(PaxosMsg *msg) = 0;
  virtual int onMsgPreCheck(PaxosMsg *msg, PaxosMsg *rsp) = 0;
  virtual int onMsgPreCheckFailed(PaxosMsg *msg) = 0;
  virtual uint64_t getClusterId() = 0;
  virtual int setClusterId(uint64_t ci) = 0;
  virtual bool isShutdown() = 0;

 protected:
 private:
  Consensus(const Consensus &other);                   // copy constructor
  const Consensus &operator=(const Consensus &other);  // assignment operator

}; /* end of class Consensus */

}  // namespace alisql

#endif  // #ifndef cluster_consensus_INC
