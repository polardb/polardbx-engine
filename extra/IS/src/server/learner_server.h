/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  learner_server.h,v 1.0 05/08/2017 3:37:56 PM
 *jarry.zj(jarry.zj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file learner_server.h
 * @author jarry.zj(jarry.zj@alibaba-inc.com)
 * @date 05/08/2017 3:37:56 PM
 * @version 1.0
 * @brief
 *
 **/

#ifndef CONSENSUS_INCLUDE_LEARNER_SERVER_H_
#define CONSENSUS_INCLUDE_LEARNER_SERVER_H_

#include "paxos_server.h"

namespace alisql {

class SingleLeader;

class LearnerServer : public RemoteServer {
 public:
  LearnerServer(uint64_t serverId);
  virtual ~LearnerServer() {}
  virtual void stop(void *);
  virtual void sendMsg(void *);
  virtual void connect(void *);
  virtual void disconnect(void *);
  virtual uint64_t getMatchIndex() const { return matchIndex; }
  virtual uint64_t getAppliedIndex() const { return appliedIndex.load(); }
  virtual uint64_t getLastLogIndex();
  virtual void onConnectCb();

  /* functions not used */
  virtual void fillInfo(void *) {}
  virtual void beginRequestVote(void *) {}
  virtual void beginLeadership(void *) {}
  virtual void stepDown(void *) {}
  virtual uint64_t getLastAckEpoch() const { return 0; }
  virtual void setLastAckEpoch(uint64_t epoch) {}
  virtual bool haveVote() const { return true; }
  /* TODO:
   virtual bool isCaughtUp() const;
   */

  SingleLeader *singleLeader;
  uint64_t connectTimeout;
};
}  // namespace alisql

#endif /* CONSENSUS_INCLUDE_LEARNER_SERVER_H_ */
