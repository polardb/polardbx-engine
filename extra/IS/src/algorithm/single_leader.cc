/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  single_leader.cc,v 1.0 04/12/2017 3:40:23 PM
 *jarry.zj(jarry.zj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file single_leader.cc
 * @author jarry.zj(jarry.zj@alibaba-inc.com)
 * @date 04/12/2017 3:40:23 PM
 * @version 1.0
 * @brief
 *
 **/
#include "single_leader.h"
#include "learner_server.h"
#include "mem_paxos_log.h"
#include "service.h"

namespace alisql {

SingleLeader::SingleLeader(uint64_t clusterId)
    : commitIndex_(0),
      heartbeatTimeout_(1000),
      clusterId_(clusterId),
      mockLastIndex_(0),
      mockLastTerm_(0) {}

void SingleLeader::init(int port, std::string learnerAddr,
                        uint64_t lastLogIndex, uint64_t lastTerm,
                        uint64_t cacheSize, uint64_t ioThreadCnt,
                        uint64_t workThreadCnt) {
  mockLastTerm_ = lastTerm;
  mockLastIndex_ = lastLogIndex;
  easy_warn_log("single_leader : init mockLastIndex_ %llu, mockLastTerm_ %llu",
                mockLastIndex_, mockLastTerm_);
  log_ = std::make_shared<MemPaxosLog>(lastLogIndex, 0, cacheSize);
  log_->setAppendTimeout(0); /* required, making log->append a blocking api */
  srv_ = std::shared_ptr<Service>(new Service(this));
  srv_->init(ioThreadCnt, workThreadCnt, heartbeatTimeout_);
  srv_->start(port);

  learnerServer_ = std::make_shared<LearnerServer>(100);
  // do not need to set ptrR->heartbeatTimer
  learnerServer_->strAddr = learnerAddr;
  learnerServer_->srv = srv_;
  learnerServer_->appliedIndex = 0;
  learnerServer_->addr.port = 0;
  learnerServer_->connectTimeout = heartbeatTimeout_ / 4;
  learnerServer_->singleLeader = this;
  learnerServer_->connect(NULL);
}

uint64_t SingleLeader::replicateLog(LogEntry &entry) {
  /* write log */
  uint64_t logIndex = log_->append(entry);
  easy_warn_log(
      "single_leader : replicateLog write done logTerm(%ld), logIndex(%ld)\n",
      entry.term(), logIndex);
  std::lock_guard<std::mutex> lg(lock_);
  commitIndex_.store(logIndex);
  currentTerm_.store(entry.term());
  appendLogToLearner();
  return logIndex;
}

void SingleLeader::appendLogToLearner(bool needLock) {
  if (needLock) lock_.lock();
  PaxosMsg msg;
  msg.set_msgtype(AppendLog);
  msg.set_leaderid(1);
  msg.set_commitindex(commitIndex_.load());
  msg.set_term(currentTerm_.load());
  msg.set_clusterid(clusterId_);
  // TODO call sendMsg async
  learnerServer_->sendMsg(&msg);
  if (needLock) lock_.unlock();
}

uint64_t SingleLeader::appendLogFillForEach(PaxosMsg *msg,
                                            LearnerServer *server) {
  uint64_t prevLogTerm;
  uint64_t prevLogIndex = server->nextIndex - 1;
  uint64_t lastLogIndex = log_->getLastLogIndex();
  uint64_t size = 0;
  if (prevLogIndex > lastLogIndex) {
    easy_warn_log(
        "single_leader: server %d 's prevLogIndex %d larger than lastLogIndex "
        "%d. Just ignore.\n",
        server->serverId, prevLogIndex, lastLogIndex);
    /* clear the mempaxoslog, release the cachesize */
    log_->truncateForward(lastLogIndex);
    return size;
  }

  if (prevLogIndex > 0) {
    if (prevLogIndex < mockLastIndex_) {
      easy_warn_log(
          "single_leader: Fail to AppendLog to server %d, prevLogIndex(%ld) is "
          "less than mockLastIndex_(%ld).\n",
          msg->serverid(), prevLogIndex, mockLastIndex_);
      return size;
    } else if (prevLogIndex == mockLastIndex_) {
      prevLogTerm = mockLastTerm_;
    } else {
      LogEntry entry;
      int ret = log_->getEntry(prevLogIndex, entry, true);
      if (ret != 0) {
        easy_warn_log(
            "single_leader: getEntry fail for prevLogIndex(%ld) in Fill "
            "AppendLog to server %d, miss log entries.\n",
            prevLogIndex, msg->serverid());
        return size;
      } else {
        prevLogTerm = entry.term();
      }
    }
  } else {
    // prevLogIndex == 0
    prevLogTerm = 0;
  }
  msg->set_prevlogindex(prevLogIndex);
  msg->set_prevlogterm(prevLogTerm);
  msg->set_nocache(true);
  /* since cachesize(mempaxoslog) is limited, no maxPacketSize here */
  if (lastLogIndex >= server->nextIndex) {
    LogEntry entry;
    ::google::protobuf::RepeatedPtrField<LogEntry> *entries;
    entries = msg->mutable_entries();
    for (uint64_t i = server->nextIndex; i <= lastLogIndex; ++i) {
      if (0 != log_->getEntry(i, entry, true)) {
        easy_warn_log(
            "single_leader: getEntry fail for entries(i:%ld) in Fill AppendLog "
            "to server %d, miss log entries\n",
            i, msg->serverid());
        easy_warn_log("single_leader: send log index [%ld-%ld].\n",
                      server->nextIndex.load(), i - 1);
        return size;
      }
      assert(entry.index() == i);
      *(entries->Add()) = entry;
      size += entry.ByteSize();
      if (size > maxSystemPacketSize_) break;
    }
    easy_warn_log("single_leader: send log index [%ld-%ld].\n",
                  server->nextIndex.load(), lastLogIndex);
  }
  return size;
}

int SingleLeader::onAppendLogResponce(PaxosMsg *msg) {
  assert(msg->msgtype() == AppendLogResponce);
  std::lock_guard<std::mutex> lg(lock_);
  /* check the msg comes from the learner we managed */
  if (msg->serverid() != learnerServer_->serverId) {
    easy_error_log(
        "single_leader: onAppendLogResponce receive a msg msgId(%llu) from "
        "server %llu which is a wrong learner.\n",
        msg->msgid(), msg->serverid());
    return -1;
  }
  learnerServer_->waitForReply = 0;
  learnerServer_->appliedIndex =
      msg->appliedindex(); /* we do not care about appliedIndex here, just
                              update */
  uint64_t oldMatchIndex = learnerServer_->matchIndex;
  uint64_t oldNextIndex = learnerServer_->nextIndex;
  if (msg->issuccess()) {
    if (msg->lastlogindex() < learnerServer_->nextIndex) {
      easy_warn_log(
          "single_leader: onAppendLogResponce this response of AppendLog to "
          "server %d may be a resend msg that we have already received, msg "
          "index(%ld) is smaller than nextIndex(%ld)\n",
          msg->serverid(), msg->lastlogindex(),
          learnerServer_->nextIndex.load());
    } else {
      if (msg->lastlogindex() > learnerServer_->matchIndex)
        learnerServer_->matchIndex = msg->lastlogindex();
      if (learnerServer_->nextIndex < learnerServer_->matchIndex + 1)
        learnerServer_->nextIndex = learnerServer_->matchIndex + 1;
    }
    easy_warn_log(
        "single_leader: msgId(%llu) AppendLog to server %d success, "
        "matchIndex(old:%llu,new:%llu) and nextIndex(old:%llu,new:%llu) have "
        "changed\n",
        msg->msgid(), msg->serverid(), oldMatchIndex,
        learnerServer_->matchIndex.load(), oldNextIndex,
        learnerServer_->nextIndex.load());
    /* clear the mempaxoslog before learnerServer's matchIndex */
    log_->truncateForward(learnerServer_->matchIndex);
    /* send the remaining log */
    appendLogToLearner();
  } else {
    /* fail to append log */
    learnerServer_->matchIndex = msg->lastlogindex();
    learnerServer_->nextIndex = learnerServer_->matchIndex + 1;
    if (oldNextIndex != learnerServer_->nextIndex ||
        oldMatchIndex != learnerServer_->matchIndex) {
      /* clear the mempaxoslog before learnerServer's matchIndex, release the
       * cachesize */
      log_->truncateForward(learnerServer_->matchIndex);
      /* send the remaining log */
      appendLogToLearner();
      easy_error_log(
          "single_leader: Learner(%d) change its local log position! We reset "
          "server(learner)'s matchIndex(old:%llu,new:%llu) and "
          "nextIndex(old:%llu,new:%llu).",
          msg->serverid(), oldMatchIndex, learnerServer_->matchIndex.load(),
          oldNextIndex, learnerServer_->nextIndex.load());
    }
  }

  return 0;
}

uint64_t SingleLeader::getLearnerMatchIndex() {
  return learnerServer_->matchIndex;
}

uint64_t SingleLeader::getLastLogIndex() { return log_->getLastLogIndex(); }

void SingleLeader::stop() { srv_->stop(); }

void SingleLeader::shutdown() {
  learnerServer_->stop(NULL);
  srv_->shutdown();
}

const uint64_t SingleLeader::maxSystemPacketSize_ = 50000000;

}  // namespace alisql
