/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  witness.cc,v 1.0 12/20/2016 19:15:45 PM
 *jarry.zj(jarry.zj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file witness.cc
 * @author jarry.zj(jarry.zj@alibaba-inc.com)
 * @date 12/20/2016 19:15:45 PM
 * @version 1.0
 * @brief The implement of witness node in paxos protocol
 *
 **/

#include "witness.h"

namespace alisql {

Witness::Witness(std::string addr, std::shared_ptr<MemPaxosLog> log,
                 uint64_t clusterId, uint64_t ioThreadCnt,
                 uint64_t workThreadCnt)
    : log_(log),
      currentTerm_(0),
      commitIndex_(0),
      appliedIndex_(0),
      leaderId_(0),
      serverId(0),
      clusterId_(clusterId),
      shutdown_(false),
      leaderChangeHandler_(nullptr) {
  /* init commitIndex */
  commitIndex_ = log_->getLastLogIndex();

  /* Init Service */
  srv_ = std::shared_ptr<Service>(new Service(this));
  srv_->init(ioThreadCnt, workThreadCnt);
  std::string strPort = addr.substr(addr.find_last_of(':') + 1);
  int error = 0;
  if ((error = srv_->start(std::stoull(strPort)))) {
    easy_error_log("Fail to start libeasy service, error(%d).", error);
    abort();
  }
  if (clusterId == 0) {
    easy_warn_log("ClusterId is initialized to 0.\n");
  }
}

Witness::~Witness() {
  lock_.lock();
  shutdown_.store(true);
  lock_.unlock();
  srv_->shutdown();
}

int Witness::onAppendLog(PaxosMsg *msg, PaxosMsg *rsp) {
  assert(msg->msgtype() == AppendLog);
  std::lock_guard<std::mutex> lg(lock_);
  if (shutdown_.load()) return -1;

  uint64_t lastLogIndex = log_->getLastLogIndex();
  uint64_t prevLogIndex = msg->prevlogindex();

  easy_warn_log(
      "Server %d : msgId(%llu) onAppendLog start, receive logs from "
      "leader(%d), msg.term(%d) \n",
      serverId, msg->msgid(), msg->leaderid(), msg->term());

  /* Leader should give a serverId, update the local */
  if (serverId == 0) {
    easy_warn_log("Get serverId from leader %d\n", msg->serverid());
    serverId = msg->serverid();
  } else if (serverId != msg->serverid()) {
    /* serverId not match */
    easy_warn_log(
        "Server %d, receive logs but serverId not match. local %d, msg %d. "
        "Just update. \n",
        serverId, serverId, msg->serverid());
    serverId = msg->serverid();
  }

  rsp->set_msgid(msg->msgid());
  rsp->set_msgtype(AppendLogResponce);
  rsp->set_serverid(serverId);
  rsp->set_issuccess(false);
  rsp->set_lastlogindex(lastLogIndex);
  rsp->set_ignorecheck(false);
  rsp->set_appliedindex(appliedIndex_.load());

  /* check leaderId, leader may change */
  if (msg->term() >= currentTerm_ && leaderId_ != msg->leaderid()) {
    /* leader changed */
    easy_warn_log(
        "Server %d : Leader change. old leader-term(%d, %d), new "
        "leader-term(%d, %d). \n",
        serverId, leaderId_, currentTerm_, msg->leaderid(), msg->term());
    leaderId_ = msg->leaderid();
    if (leaderChangeHandler_) {
      srv_->sendAsyncEvent(leaderChangeHandler_, leaderId_);
    }
  }

  /* check term */
  if (msg->term() != currentTerm_) {
    // witness just accepts the msg term
    easy_warn_log(
        "Server %d : msgId(%llu) New Term in onAppendLog !! server %d 's "
        "term(%d) is different from me(%d).\n",
        serverId, msg->msgid(), leaderId_, msg->term(), currentTerm_);
    currentTerm_ = msg->term();
  }

  rsp->set_term(currentTerm_);

  if (!msg->has_prevlogindex()) {
    rsp->set_ignorecheck(true);
    easy_warn_log(
        "Server %d : msgId(%llu) receive logs without prevlogindex. from "
        "server %ld, localTerm(%ld),msg.term(%d) lli:%ld\n",
        serverId, msg->msgid(), msg->leaderid(), currentTerm_, msg->term(),
        lastLogIndex);
    return 0;
  }

  if (prevLogIndex > lastLogIndex) {
    easy_warn_log(
        "Server %d : msgId(%llu) receive log's prevlogindex(%ld, term:%ld) is "
        "bigger than lastLogIndex(%ld, term:%ld) reject.\n",
        serverId, msg->msgid(), msg->prevlogindex(), msg->prevlogterm(),
        lastLogIndex, currentTerm_);
    /* lastLogIndex will be used as a hint */
    return -1;
  }
  easy_warn_log(
      "Server %d : msgId(%llu) receive log's prevlogindex(%ld, term:%ld)",
      serverId, msg->msgid(), msg->prevlogindex(), msg->prevlogterm());
  { /* Check the term. For witness, it can be skipped. */
    LogEntry entry;
    /* entry may not exists since log_ can be a cache */
    int ret = log_->getEntry(prevLogIndex, entry, false);
    easy_warn_log("Server %d : getEntry ret %d \n", serverId, ret);
    if (ret == 0 && entry.term() != msg->prevlogterm()) {
      /* log not match */
      easy_warn_log("Server %d : entry.term %d msg->prevlogterm %d\n", serverId,
                    entry.term(), msg->prevlogterm());
      return 0;
    }
  }
  rsp->set_issuccess(true);

  /* Logically, we never truncate or overwrite log in a witness node
   * since it may has been consumed by another thread
   */
  if (msg->entries_size() > 0) {
    if (lastLogIndex == prevLogIndex &&
        !debugWitnessTest) /* debug: force to use single append */
    {
      /* append directly */
      log_->append(msg->entries());
    } else /* log_->getLastLogIndex() > prevLogIndex, the '<' case has been
              handled above */
    {
      uint64_t msgLastIndex = prevLogIndex + msg->entries_size();
      if (msgLastIndex > lastLogIndex) {
        int skipcount = lastLogIndex - prevLogIndex;
        easy_warn_log(
            "Server %d : lastLogIndex %ld is larger than prevLogIndex %ld, we "
            "skip %ld logs.\n",
            serverId, lastLogIndex, prevLogIndex, skipcount);
        auto entries = msg->entries();
        for (auto it = entries.begin(); it != entries.end(); ++it) {
          if (skipcount > 0) {
            --skipcount;
            continue;
          }
          uint64_t retIndex = log_->append(*it);
          if (retIndex != it->index()) /* append timeout happens */
            break;
        }
      }
    }
  }
  /* update lastlogindex */
  rsp->set_lastlogindex(log_->getLastLogIndex());
  /* update commitIndex */
  if (msg->commitindex() > commitIndex_) {
    /* use leader commitIndex. */
    easy_warn_log(
        "Server %d : commitIndex change from %ld to %ld, now lastLogIndex is "
        "%ld\n",
        serverId, commitIndex_.load(), msg->commitindex(), log_->getLastLogIndex());
    commitIndex_ = msg->commitindex();
  }
  easy_warn_log("Server %d : msgId(%llu) onAppendLog end, is_success %d\n",
                serverId, msg->msgid(), rsp->issuccess());
  return 0;
}

void Witness::updateAppliedIndex(uint64_t index) { appliedIndex_.store(index); }

void Witness::setLeaderChangeCb(std::function<void(uint64_t leaderId)> cb) {
  std::lock_guard<std::mutex> lg(lock_);
  leaderChangeHandler_ = cb;
}

int Witness::onRequestVote(PaxosMsg *msg, PaxosMsg *rsp) {
  std::lock_guard<std::mutex> lg(lock_);
  if (shutdown_.load()) return -1;

  rsp->set_msgid(msg->msgid());
  rsp->set_msgtype(RequestVoteResponce);
  rsp->set_serverid(serverId);
  rsp->set_term(msg->term());
  rsp->set_votegranted(0);
  easy_system_log(
      "Server %d : Receive an RequestVote from server %d, term(%llu) when I'm "
      "WITNESS!! Just reject!!\n",
      serverId, msg->candidateid(), msg->term());
  return 0;
}

int Witness::onLeaderCommand(PaxosMsg *msg, PaxosMsg *rsp) {
  std::lock_guard<std::mutex> lg(lock_);
  if (shutdown_.load()) return -1;

  easy_system_log(
      "Server %d : Receive an onLeaderCommand when I'm WITNESS!! Just "
      "reject!!\n",
      serverId);
  rsp->set_msgid(msg->msgid());
  rsp->set_msgtype(LeaderCommandResponce);
  rsp->set_serverid(serverId);
  rsp->set_term(currentTerm_);
  rsp->set_lctype(msg->lctype());
  rsp->set_issuccess(false);
  return 0;
}

void Witness::resetLastLogIndex(uint64_t lli) {
  std::lock_guard<std::mutex> lg(lock_);
  easy_system_log("Server %d : resetLastLogIndex to %llu\n", serverId, lli);
  log_->resetLastLogIndex(lli);
  commitIndex_ = log_->getLastLogIndex();
}

bool Witness::debugWitnessTest = false;

} /* namespace alisql */
