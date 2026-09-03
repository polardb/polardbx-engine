/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  protocol.h,v 1.0 07/27/2016 11:42:30 AM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file protocol.h
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 07/27/2016 11:42:30 AM
 * @version 1.0
 * @brief the interface class of Protocol
 *
 **/

#ifndef ALISQL_PROTOCOL_H
#define ALISQL_PROTOCOL_H

// #include "paxos.pb.h"
#include "service.h"

namespace alisql {
/**
 * @class Protocol
 *
 * @brief interface of consensus algorithm module
 *
 **/
class Protocol {
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

  typedef enum AlertLogLevel {
    LOG_ERROR = EASY_LOG_ERROR,
    LOG_WARN = EASY_LOG_WARN,
    LOG_INFO = EASY_LOG_INFO,
    LOG_DEBUG = EASY_LOG_DEBUG,
    LOG_TRACE = EASY_LOG_TRACE,
  } AlertLogLevelType;

    typedef enum State {
    FOLLOWER,
    CANDIDATE,
    LEADER,
    LEARNER,
    NOROLE,
  } StateType;

  Protocol() {}
  virtual ~Protocol() {}

  virtual int onRequestVote(PaxosMsg *msg, PaxosMsg *rsp) = 0;
  virtual int onAppendLog(PaxosMsg *msg, PaxosMsg *rsp) = 0;
  virtual int onRequestVoteResponce(PaxosMsg *msg) = 0;
  virtual int onAppendLogSendFail(PaxosMsg *msg, uint64_t *newId) = 0;
  virtual int onAppendLogResponce(PaxosMsg *msg) = 0;
  virtual int requestVote(bool force) = 0;
  virtual uint64_t replicateLog(LogEntry &entry) = 0;
  virtual uint64_t replicateEmptyLog() = 0;
  virtual int appendLog(const bool needLock) = 0;
  virtual int onLeaderCommand(PaxosMsg *msg, PaxosMsg *rsp) = 0;
  virtual int onLeaderCommandResponce(PaxosMsg *msg) = 0;
  virtual int onClusterIdNotMatch(PaxosMsg *msg) = 0;
  virtual int onMsgPreCheck(PaxosMsg *msg, PaxosMsg *rsp) = 0;
  virtual int onMsgPreCheckFailed(PaxosMsg *msg) = 0;
  virtual uint64_t getClusterId() = 0;
  virtual int setClusterId(uint64_t ci) = 0;
  virtual bool isShutdown() = 0;
  virtual uint64_t getCommitIndex() { return 0; }
  virtual uint64_t getTerm() { return 0; }
  virtual uint64_t getAppliedIndex() { return 0; }
  virtual void updateAppliedIndex(uint64_t) { return; }
  virtual uint64_t checkCommitIndex(uint64_t) { return 0; }

  /*----------------------------------------------------------------*/
  /* sys_vars_consensus related */
  /*----------------------------------------------------------------*/
  virtual void setEnableDynamicEasyIndex(bool) { return;}
  virtual void setReplicateWithCacheLog(bool)  { return;}
  virtual void setCompactOldMode(bool) { return;}
  virtual void setForceSyncEpochDiff(uint64_t) { return;}
  virtual void setMaxDelayIndex4NewMember(uint64_t) { return;}
  virtual void setMaxDelaySeconds4NewLeader(uint64_t) { return;}
  virtual void setMaxPipelineEntrySize(uint64_t) { return;}
  virtual void setEnableLearnerPipelining(bool) { return;}
  virtual void setConfigureChangeTimeout(uint64_t) { return;}
  virtual void setWeakReadRefreshTimeout(int64_t) { return;}
  /* Per-parameter dynamic configuration update methods (called from
     ON_UPDATE callbacks after releasing LOCK_global_system_variables) */
  virtual void applyElectionTimeout(uint64_t) { return;}
  virtual void applyHeartbeatInterval(uint64_t) { return;}
  virtual void applyVoteBackoffTimeout(uint64_t) { return;}
  virtual void applySendTimeout(uint64_t) { return;}
  virtual void applyConnectTimeout(uint64_t) { return;}
  virtual void applyForcePromote(bool) { return;}
  virtual void setMaxPacketSize(uint64_t) { return;}
  virtual void setPipeliningTimeout(uint64_t) { return;}
  virtual void setLargeBatchRatio(uint64_t) { return;}
  virtual void setMaxDelayIndex(uint64_t) { return;}
  virtual void setMinDelayIndex(uint64_t) { return;}
  virtual void setOptimisticHeartbeat(bool) { return;}
  virtual void setDisableElection(bool) { return;}
  virtual void setSyncFollowerMetaInterval(uint64_t) { return;}
  virtual void setEnableAutoResetMatchIndex(bool) { return;}
  virtual void setEnableLearnerHeartbeat(bool) { return;}
  virtual void setEnableAutoLeaderTransfer(bool) { return;}
  virtual void setAutoLeaderTransferCheckSeconds(uint64_t) { return;}
  virtual void setForceLeaderTransfer(bool) { return;}
  virtual void setAlertLogLevel(alisql::Protocol::AlertLogLevelType) { return;}
  virtual int forcePromote() {  return 0; }
  virtual void setServerIp(const char *) { return;}
  virtual void setServerPort(uint64_t ) { return;}
  virtual bool getApplyThreadRunning() { return false; }
  virtual uint64_t getClusterChangeVersion() const {  return 0; }
  virtual void HaHistoryAdd(const bool) { return; }
  virtual StateType getState() { return StateType::NOROLE; }

  /*----------------------------------------------------------------*/
  /* sys_vars_consensus related */
  /*----------------------------------------------------------------*/

 protected:
 private:
  Protocol(const Protocol &other);                   // copy constructor
  const Protocol &operator=(const Protocol &other);  // assignment operator

}; /* end of class Protocol */

}  // namespace alisql

#endif  // #ifndef ALISQL_PROTOCOL_H
