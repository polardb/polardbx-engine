/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  paxos.cc,v 1.0 07/31/2016 04:16:32 PM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file paxos.cc
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 07/31/2016 04:16:32 PM
 * @version 1.0
 * @brief The implement of the PAXOS algorithm
 *
 **/

#include "paxos.h"
#include <sys/time.h>

namespace alisql {

Paxos::Paxos(uint64_t electionTimeout, std::shared_ptr<PaxosLog> log,
             uint64_t purgeLogTimeout)
    : debugMaxSendLogIndex(0),
      config_(new StableConfiguration()) /*TODO:*/
      ,
      log_(log),
      clusterId_(0),
      myServerId_(0),
      shutdown_(false),
      maxPacketSize_(1000000),
      maxDelayIndex_(10000),
      minDelayIndex_(100),
      largeBatchRatio_(5),
      pipeliningTimeout_(3),
      electionTimeout_(electionTimeout),
      heartbeatTimeout_(electionTimeout / 5),
      purgeLogTimeout_(purgeLogTimeout),
      currentTerm_(1),
      leaderStepDowning_(false),
      commitIndex_(0),
      leaderId_(0),
      leaderAddr_(""),
      votedFor_(0),
      forceRequestMode_(false),
      currentEpoch_(0),
      forceSyncEpochDiff_(0),
      state_(FOLLOWER),
      subState_(SubNone),
      weightElecting_(false),
      leaderForceSyncStatus_(true),
      consensusAsync_(false),
      replicateWithCacheLog_(false),
      optimisticHeartbeat_(false)
      //,changeStateWorkers_(0)
      ,
      autoPurge_(false),
      useAppliedIndex_(true),
      minMatchIndex_(0),
      appliedIndex_(0),
      followerMetaNo_(0),
      lastSyncMetaNo_(0),
      syncMetaInterval_(1),
      maxDelayIndex4NewMember_(100),
      maxMergeReportTimeout_(2000),
      nextEpochCheckStatemachine_(0),
      compactOldMode_(true),
      enableLogCache_(false),
      enableDynamicEasyIndex_(false),
      enableLearnerPipelining_(false),
      enableAutoResetMatchIndex_(false),
      enableLearnerAutoResetMatchIndex_(false),
      stateChangeCb_(nullptr),
      checksumCb_(nullptr),
      checksum_mode_(false),
      port_(0) {}

Paxos::~Paxos() {
  if (!shutdown_.load()) shutdown();
}

void Paxos::shutdown() {
  /* We should stop all ThreadTimer before close ThreadTimerService in
   * Service::shutdown */
  lock_.lock();
  if (state_ == LEADER) {
    log_->setMetaData(keyLastLeaderTerm, currentTerm_);
    log_->setMetaData(keyLastLeaderLogIndex, commitIndex_);
  }
  shutdown_.store(true);
  if (ccMgr_.prepared) {
    ccMgr_.aborted = 1;
    ccMgr_.cond.notify_all();
  }
  ccMgr_.autoChangeAddr = "";
  ccMgr_.condChangeDone.notify_all();
  lock_.unlock();
  cond_.notify_all();
  electionTimer_.reset();
  epochTimer_.reset();
  purgeLogTimer_.reset();
  purgeLogQueue_.stop();
  changeStateQueue_.stop();
  appendLogQueue_.stop();
  commitDepQueue_.stop();
  if (config_.get() != nullptr) {
    config_->forEach(&Server::stop, NULL);
    config_->forEachLearners(&Server::stop, NULL);
  }
  srv_->shutdown();
  /* When Service::shutdown return, there is not backend worker left, so we can
   * release config_ now. */
  config_.reset();

  if (threadHook != nullptr) {
    delete threadHook;
    threadHook = nullptr;
  }
}

void Paxos::stop() {
  /* We should stop all ThreadTimer before close ThreadTimerService in
   * Service::shutdown */
  electionTimer_->stop();
  epochTimer_->stop();
  purgeLogTimer_->stop();
  config_.reset();
  srv_->stop();
}

void Paxos::changeState_(enum State newState) {
  /* We call sendAsyncEvent every time if the term change or state change. */
  /*
  if (state_ == newState)
    return;
  */
  easy_system_log("Server %d : Paxos state change from %s to %s !!\n",
                 localServer_->serverId, stateString[state_],
                 stateString[newState]);
  /* only leader run purge log timer */
  if (state_ == LEADER) {
    purgeLogTimer_->stop();
  }

  if (newState != CANDIDATE) {
    forceRequestMode_ = false;
  }

  state_.store(newState);
  leaderForceSyncStatus_.store(true);
  if (newState == LEADER) {
    if (autoPurge_ == true) {
      purgeLogTimer_->restart();
    }
  } else {
    subState_.store(SubNone);
    weightElecting_ = false;
  }

  if (newState == LEADER) {
    leaderId_.store(localServer_->serverId);
    leaderAddr_ = localServer_->strAddr;
    option.extraStore->setRemote(option.extraStore->getLocal());
  }

  log_->resetMetaCache();

  if (stateChangeCb_) {
    if (changeStateQueue_.push(
            new ChangeStateArgType(state_, currentTerm_, commitIndex_, this)))
      srv_->sendAsyncEvent(&SingleProcessQueue<ChangeStateArgType>::process,
                           &changeStateQueue_, Paxos::execStateChangeCb);
  }

  cond_.notify_all();
}

void Paxos::membershipChangeHistoryUpdate_(const MembershipChangeType &mc) {
  if (membershipChangeHistory_.size() >= 10)
    membershipChangeHistory_.erase(membershipChangeHistory_.begin());
  membershipChangeHistory_.push_back(mc);
}

int Paxos::applyConfigureChangeNoLock_(uint64_t logIndex) {
  LogEntry entry;
  uint64_t index = 0;
  if (logIndex == 0)  // for defensive
    return 0;
  if (log_->getEntry(logIndex, entry, false)) {
    easy_error_log(
        "Server %d: fail to get log entry when apply configure change, index "
        "%llu",
        localServer_->serverId, logIndex);
    return 0;
  }
  assert(entry.optype() == kConfigureChange);

  ConfigureChangeValue val;
  val.ParseFromString(std::move(entry.value()));

  MembershipChangeType mc;
  mc.cctype = (CCOpTypeT)val.cctype();
  mc.optype = (CCOpTypeT)val.optype();
  if (val.addrs().size()) mc.address = *(val.addrs().begin());
  if (val.cctype() == CCMemberOp) {
    // for membership change
    const std::string &addr = *(val.addrs().begin());
    if (val.optype() == CCAddNode) {
      assert(val.addrs_size() == 1);
      if (state_ != LEARNER)
        config_->addMember(addr, this);
      else if (addr == localServer_->strAddr) {
        /* learner change to follower */
        std::vector<std::string> strConfig;
        for (auto &it : val.allservers()) {
          strConfig.push_back(it);
          if (it == addr) index = strConfig.size();
        }
        assert(index != 0);
        /* the allservers already include the new add server(local server). */
        // strConfig.push_back(addr);

        /* The learner's localServer may be AliSQLServer, we should pass it to
         * installConfig. */
        std::dynamic_pointer_cast<StableConfiguration>(config_)->installConfig(
            strConfig, index, this, localServer_);
        config_->forEach(&Server::connect, (void *)NULL);
        changeState_(FOLLOWER);
        electionTimer_->start();

        /* Init learners */
        std::vector<std::string> strConfigL;
        for (auto &addr : val.alllearners()) {
          if (addr != localServer_->strAddr)
            strConfigL.push_back(std::move(addr));
        }
        config_->delAllLearners();
        config_->addLearners(strConfigL, this, true);

        log_->setMetaData(Paxos::keyLearnerConfigure,
                          config_->learnersToString());
        log_->setMetaData(Paxos::keyMemberConfigure,
                          config_->membersToString(localServer_->strAddr));

        /* Print Log */
        std::string logBuf;
        for (auto &addr : strConfig) {
          logBuf += addr;
          logBuf += " ";
        }
        std::string logBufL;
        for (auto &addr : strConfigL) {
          logBufL += addr;
          logBufL += " ";
        }
        easy_system_log(
            "Server %d : Init follower from learner, new members(%s) new "
            "learners(%s)\n",
            localServer_->serverId, logBuf.c_str(), logBufL.c_str());
      } else {
        std::vector<std::string> strConfig;
        strConfig.push_back(std::move(addr));
        config_->delLearners(strConfig, this);
      }
      if (ccMgr_.autoChangeAddr == addr) {
        ccMgr_.autoChangeAddr = "";
        ccMgr_.autoChangeRet = 0;
        ccMgr_.condChangeDone.notify_all();
      }
    } else if (val.optype() == CCDelNode) {
      if (state_ != LEARNER) {
        if (addr != localServer_->strAddr)
          config_->delMember(addr, this);
        else {
          /* This node is removed from the cluster, shutdown myself */
          easy_system_log(
              "Server %d : This node is removed from the cluster, shutdown "
              "myself currentTerm(%llu) lli(%llu) ccIndex(%llu)!!\n",
              localServer_->serverId, currentTerm_.load(),
              log_->getLastLogIndex(), logIndex);
          localServer_->serverId += 1000;
          stop();
        }
      }
    } else if (val.optype() == CCDowngradeNode) {
      std::vector<std::string> strConfig;
      strConfig.push_back(addr);
      if (state_ != LEARNER) {
        if (addr != localServer_->strAddr) {
          config_->delMember(addr, this);
          config_->addLearners(strConfig, this);
          // if (state_ == LEADER)
          //   config_->forEachLearners(&Server::connect, (void *)NULL);
        } else {
          auto oldId = localServer_->serverId;
          std::shared_ptr<LocalServer> localServer =
              std::dynamic_pointer_cast<LocalServer>(config_->getServer(oldId));
          assert(localServer != nullptr);
          std::dynamic_pointer_cast<StableConfiguration>(config_)
              ->installConfig(strConfig, 1, this, localServer);
          config_->addLearners(strConfig, this);
          log_->setMetaData(Paxos::keyLearnerConfigure,
                            config_->learnersToString());
          log_->setMetaData(Paxos::keyMemberConfigure,
                            config_->membersToString());
          /* We set the init serverId to 100 for tmp, this serverId will change
           * when we  */
          localServer_->serverId = 100;
          this->state_.store(LEARNER);
          this->electionTimer_->stop();
          easy_system_log(
              "Server %d : This server is downgrade from follower(%llu) to "
              "learner(%llu)!!",
              localServer_->serverId, oldId, localServer_->serverId);
        }
      } else {
        config_->addLearners(strConfig, this);
      }
    } else if (val.optype() == CCConfigureNode) {
      mc.forceSync = val.forcesync();
      mc.electionWeight = val.electionweight();
      if (state_ != LEARNER) {
        auto server = config_->getServer(val.serverid());
        if (server == nullptr || addr != server->strAddr) {
          easy_error_log(
              "Server %d : Can't find the target server(id:%llu, addr:%s) in "
              "the configure!! Current member configure:%s\n",
              localServer_->serverId, val.serverid(), addr.c_str(),
              config_->membersToString(localServer_->strAddr).c_str());
        } else {
          config_->configureMember(val.serverid(), val.forcesync(),
                                   val.electionweight(), this);
          if (val.serverid() == localServer_->serverId)
            electionTimer_->setRandWeight(val.electionweight());
        }
      }
    }
  } else if (val.cctype() == CCLearnerOp) {
    if (val.optype() == CCAddNode || val.optype() == CCAddLearnerAutoChange) {
      std::vector<std::string> strConfig, strLearners;
      for (auto &addr : val.addrs()) strConfig.push_back(std::move(addr));
      for (auto &addr : val.alllearners())
        strLearners.push_back(std::move(addr));
      if (StableConfiguration::isServerInVector(localServer_->strAddr,
                                                strConfig)) {
        config_->delAllLearners();
        config_->addLearners(strLearners, this, true);
      }
      config_->addLearners(strConfig, this);
      /* The old learner will skip the connect call. */
      if (state_ == LEADER)
        config_->forEachLearners(&Server::connect, (void *)NULL);
      if (val.optype() == CCAddLearnerAutoChange) {
        // strConfig.size is 1 in this case
        ccMgr_.autoChangeAddr = strConfig[0];
      }
    } else if (val.optype() == CCDelNode) {
      std::vector<std::string> strConfig;
      for (auto &addr : val.addrs()) strConfig.push_back(std::move(addr));
      config_->delLearners(strConfig, this);
      /* autoChange case, wakeup addFollower if deleted */
      auto findret =
          std::find(strConfig.begin(), strConfig.end(), ccMgr_.autoChangeAddr);
      if (ccMgr_.autoChangeAddr != "" && findret != strConfig.end()) {
        ccMgr_.autoChangeAddr = "";
        ccMgr_.autoChangeRet = -2;
        ccMgr_.condChangeDone.notify_all();
      }
    } else if (val.optype() == CCConfigureNode) {
      auto server = config_->getServer(val.serverid());
      auto source = config_->getServer(val.learnersource());
      const std::string &addr = *(val.addrs().begin());
      mc.learnerSource = (source ? source->strAddr : "");
      mc.sendByAppliedIndex = val.applymode();

      if (server == nullptr || addr != server->strAddr) {
        easy_error_log(
            "Server %d : Can't find the target server(id:%llu, addr:%s) in the "
            "configure!! Current learner configure:%s\n",
            localServer_->serverId, val.serverid(), addr.c_str(),
            config_->learnersToString().c_str());
      } else {
        if (server->learnerSource == localServer_->serverId) {
          server->stepDown(nullptr);
        }
        config_->configureLearner(val.serverid(), val.learnersource(), this);
        server->sendByAppliedIndex = val.applymode();
        /* We should also init learner if we're leader and learner source is 0.
         */
        if (server->learnerSource == localServer_->serverId ||
            (state_ == LEADER && server->learnerSource == 0)) {
          easy_system_log("Server %d : a new learner %d is sourced from me!!\n",
                         localServer_->serverId, server->serverId);
          server->beginLeadership(nullptr);
          server->connect(nullptr);
        }
      }
    } else if (val.optype() == CCSyncLearnerAll) {
      std::vector<std::string> strConfig;
      std::string strServers;
      for (auto &addr : val.alllearners()) {
        strServers += addr;
        strServers += ";";
        strConfig.push_back(std::move(addr));
      }

      if (strServers.size() > 0) strServers.resize(strServers.size() - 1);

      auto strLearners = config_->learnersToString();
      if (strServers != strLearners) {
        easy_error_log(
            "Server %d : Error: local learner meta error local:%s "
            "leader:%s!!\n",
            localServer_->serverId, strLearners.c_str(), strServers.c_str());
        easy_system_log(
            "Server %d : SyncLearnerAll: update local learner config from %s "
            "to %s\n",
            localServer_->serverId, strLearners.c_str(), strServers.c_str());
        config_->delAllLearners();
        config_->addLearners(strConfig, this, true);

        log_->setMetaData(Paxos::keyLearnerConfigure,
                          config_->learnersToString());
      } else {
        easy_system_log(
            "Server %d : SyncLearnerAll: local learner is match with leader "
            "%s\n",
            localServer_->serverId, strServers.c_str());
      }
    }
  } else {
    assert(0);
  }

  membershipChangeHistoryUpdate_(mc);

  uint64_t itmp;
  log_->getMetaData(std::string(keyScanIndex), &itmp);
  if (itmp <= logIndex) log_->setMetaData(keyScanIndex, 0);

  if (ccMgr_.needNotify == 1) {
    ccMgr_.applied = 1;
    ccMgr_.aborted = 0;
    ccMgr_.cond.notify_all();
  }

  easy_system_log(
      "Server %d : applyConfigureChange_ done! logIndex(%llu) currentTerm(%ld) "
      "val.cctype(%d) val.optype(%d)\n",
      localServer_->serverId, logIndex, currentTerm_.load(), val.cctype(),
      val.optype());
  return 0;
}

int Paxos::leaderTransfer_(uint64_t targetId) {
  if (state_ != LEADER) return PaxosErrorCode::PE_NOTLEADR;
  auto server = config_->getServer(targetId);
  if (nullptr == server || targetId == 0) return PaxosErrorCode::PE_NOTFOUND;
  if (subState_ == SubLeaderTransfer) {
    easy_system_log(
        "Server %d : leaderTransfer to server(%ld), Now we're in another "
        "leader transfer, skip this action!",
        localServer_->serverId, targetId);
    return PaxosErrorCode::PE_CONFLICTS;
  }
  if (cdrMgr_.inRecovery) {
    easy_system_log(
        "Server %d : leaderTransfer to server(%ld), Now we're in commit "
        "dependency recovery, skip this action!",
        localServer_->serverId, targetId);
    return PaxosErrorCode::PE_CONFLICTS;
  }
  if (targetId == localServer_->serverId) return PaxosErrorCode::PE_NONE;
  if (std::dynamic_pointer_cast<RemoteServer>(server)
          ->isLearner)  // server == nullptr already checked
  {
    easy_system_log(
        "Server %d : leaderTransfer to server(%ld), it is a learner, skip this "
        "action!",
        localServer_->serverId, targetId);
    return PaxosErrorCode::PE_NOTFOLLOWER;
  }

  ++(stats_.countLeaderTransfer);

  easy_system_log(
      "Server %d : leaderTransfer to server(%ld), currentTerm(%ld), lli(%ld)\n",
      localServer_->serverId, targetId, currentTerm_.load(),
      log_->getLastLogIndex());
  /* Stop new replicateLog */
  subState_.store(SubLeaderTransfer);
  MembershipChangeType mc;
  mc.cctype = CCMemberOp;
  mc.optype = CCLeaderTransfer;
  mc.address = server->strAddr;
  membershipChangeHistoryUpdate_(mc);

  auto term = currentTerm_.load();
  auto lli = log_->getLastLogIndex();

  lock_.unlock();
  auto slli = log_->getSafeLastLogIndex();
  // sleep for 500ms to let log sync to disk
  if (lli < slli) msleep(500);
  int ret = leaderTransferSend_(targetId, term, slli, 5);
  lock_.lock();
  return ret;
}

int Paxos::leaderTransfer(uint64_t targetId) {
  std::lock_guard<std::mutex> lg(lock_);
  return leaderTransfer_(targetId);
}

int Paxos::leaderTransfer(const std::string &addr) {
  std::lock_guard<std::mutex> lg(lock_);
  uint64_t targetId = config_->getServerIdFromAddr(addr);
  return leaderTransfer_(targetId);
}

int Paxos::leaderTransferSend_(uint64_t targetId, uint64_t term,
                               uint64_t logIndex, uint64_t leftCnt) {
  std::lock_guard<std::mutex> lg(lock_);

  --leftCnt;

  if (checkLeaderTransfer(targetId, term, logIndex, leftCnt) > 0) {
    std::shared_ptr<RemoteServer> server =
        std::dynamic_pointer_cast<RemoteServer>(config_->getServer(targetId));
    if (server == nullptr) {
      subState_ = SubNone;
      easy_system_log(
          "Server %d : try transfer leader to id(%d), which is not in the "
          "configuration!!",
          localServer_->serverId, targetId);
      return PaxosErrorCode::PE_NOTFOUND;
    }
    if (commitIndex_ == logIndex && commitIndex_ == server->matchIndex)
      leaderCommand(LeaderTransfer, server);
    else {
      easy_system_log(
          "Server %d : skip send cmd LeaderTransfer because the pos is not "
          "catch up. commitIndex(%llu), lli(%llu), li(%llu), target matchIndex(%llu)",
          localServer_->serverId, commitIndex_.load(), log_->getLastLogIndex(), logIndex,
          server->matchIndex.load());
    }
    // TODO we also need to call leaderCommand in tryUpdateCommitIndex_

    /* do not conflict with heartbeat timeout */
    new ThreadTimer(srv_->getThreadTimerService(), srv_,
                    getLeaderTransferInterval_(), ThreadTimer::Oneshot,
                    &Paxos::leaderTransferSend_, this, targetId, term, logIndex,
                    leftCnt);
  }

  return PaxosErrorCode::PE_NONE;
}

int Paxos::checkLeaderTransfer(uint64_t targetId, uint64_t term,
                               uint64_t &logIndex, uint64_t leftCnt) {
  uint64_t lastLogIndex = log_->getLastLogIndex();
  if (state_ == LEADER && subState_ == SubLeaderTransfer &&
      term == currentTerm_) {
    if (lastLogIndex > logIndex) {
      easy_system_log(
          "Server %d : checkLeaderTransfer: In transfer to server %ld local "
          "lli:%llu is bigger than target lli:%llu, we update target lli to "
          "current lli.\n",
          localServer_->serverId, targetId, lastLogIndex, logIndex);
      logIndex = lastLogIndex;
    }

    if (leftCnt > 0) {
      easy_warn_log(
          "Server %d : checkLeaderTransfer: LeaderTransfer to server %ld not "
          "complete, left check time %llu",
          localServer_->serverId, targetId, leftCnt);
      return 1;
    } else {
      subState_.store(SubNone);
      weightElecting_ = false;
      easy_error_log(
          "Server %d : checkLeaderTransfer: LeaderTransfer to server %ld fail "
          "because of timeout currentTerm(%ld), lli(%ld)\n",
          localServer_->serverId, targetId, term, logIndex);
      return -1;
    }
  } else if (state_ == FOLLOWER && currentTerm_ > term &&
             lastLogIndex > logIndex && leaderId_ == targetId) {
    easy_system_log(
        "Server %d : checkLeaderTransfer: LeaderTransfer success target(id:%ld "
        "t:%ld lli:%ld) current(t:%ld lli:%ld)\n",
        localServer_->serverId, targetId, term, logIndex, currentTerm_.load(),
        lastLogIndex);
    return 0;
  } else {
    subState_.store(SubNone);
    weightElecting_ = false;
    easy_error_log(
        "Server %d : checkLeaderTransfer: Nonleader election may happened "
        "during the leadertransfer, please check the status! target(id:%ld "
        "t:%ld lli:%ld) current(id:%ld t:%ld lli:%ld)\n",
        localServer_->serverId, targetId, term, logIndex, leaderId_.load(),
        currentTerm_.load(), lastLogIndex);
    return -1;
  }
  return 0;
}

int Paxos::checkConfigure_(
    CCOpTypeT cctype, CCOpTypeT type, std::vector<std::string> &strConfig,
    const std::vector<Configuration::ServerRef> &servers) {
  for (auto it = strConfig.begin(); it != strConfig.end();) {
    if (type == CCAddNode || type == CCAddLearnerAutoChange) {
      bool dup = false;
      for (auto &server : servers) {
        if (server && server->strAddr == *it) {
          it = strConfig.erase(it);
          dup = true;
          break;
        }
      }
      /* In add learner case we should also check servers */
      if (!dup && cctype == CCLearnerOp) {
        auto &members = config_->getServers();
        for (auto &server : members) {
          if (server && server->strAddr == *it) {
            it = strConfig.erase(it);
            dup = true;
            break;
          }
        }
      }
      if (!dup) ++it;
    } else if (type == CCDelNode) {
      bool found = false;
      uint64_t i = 0;
      for (auto &server : servers) {
        if (server && server->strAddr == *it) {
          found = true;
          break;
        }
        ++i;
      }
      if (found)
        ++it;
      else
        it = strConfig.erase(it);
    }
  }
  return 0;
}

inline void Paxos::prepareConfigureChangeEntry_(const LogEntry &entry,
                                                PaxosMsg *msg, bool fromCache) {
  if (ccMgr_.prepared == 0) {
    ccMgr_.prepared = 1;
    ccMgr_.preparedIndex = entry.index();
  } else {
    //    uint64_t leaderCommitIndex = fromCache?
    //    logRecvCache_.getCommitIndex(): msg->commitindex();
    // potantial bug here
    //    assert(ccMgr_.preparedIndex <= leaderCommitIndex ||
    //    ccMgr_.preparedIndex <= commitIndex_);
    applyConfigureChangeNoLock_(ccMgr_.preparedIndex);
    ccMgr_.prepared = 1;
    ccMgr_.preparedIndex = entry.index();
  }
  log_->setMetaData(keyScanIndex, ccMgr_.preparedIndex);
}

int Paxos::sendConfigureAndWait_(const ConfigureChangeValue &val,
                                 std::unique_lock<std::mutex> &ul) {
  int ret = 0;
  std::string buf;
  val.SerializeToString(&buf);

  LogEntry entry;
  entry.set_optype(kConfigureChange);
  entry.set_value(buf);

  // Step 3: send log entry, and wait for applied
  uint64_t index;
  if (ccMgr_.prepared == 0) {
    log_->setMetaData(keyScanIndex, log_->getLastLogIndex());
    if ((index = replicateLog_(entry, false)) > 0) {
      if (entry.index() > commitIndex_) {
        ccMgr_.prepared = 1;
        ccMgr_.preparedIndex = entry.index();
        ccMgr_.needNotify = 1;
        if (ccMgr_.waitTimeout.load() == 0) {
          while (ccMgr_.applied == 0 && ccMgr_.aborted == 0)
            ccMgr_.cond.wait(ul);
        } else {
          bool waitRet = ccMgr_.cond.wait_for(
              ul, std::chrono::milliseconds(ccMgr_.waitTimeout.load()),
              [this]() {
                return (ccMgr_.applied != 0 || ccMgr_.aborted != 0);
              });
          if (waitRet == false) {
            ccMgr_.needNotify = 0;
            easy_error_log(
                "Server %d : configureChange wait timeout, "
                "preparedIndex(%d).\n",
                localServer_->serverId, ccMgr_.preparedIndex);
            return PaxosErrorCode::PE_TIMEOUT;
          }
        }
        if (ccMgr_.aborted == 1) {
          easy_error_log(
              "Server %d : configureChange aborted, preparedIndex(%d).\n",
              localServer_->serverId, ccMgr_.preparedIndex);
          ret = PaxosErrorCode::PE_DEFAULT;
        } else {
          if (state_ == FOLLOWER && ccMgr_.preparedIndex != entry.index()) {
            /*
             * Refering to fucntion prepareConfigureChangeEntry_,
             * leader changed during configureChange,
             * just return timeout as a result to let the client check and retry
             */
            ccMgr_.aborted = ccMgr_.applied = ccMgr_.needNotify = 0;
            easy_error_log(
                "Server %d : configureChange timeout after leader transfer, "
                "old preparedIndex(%d), current preparedIndex(%d).\n",
                localServer_->serverId, entry.index(), ccMgr_.preparedIndex);
            return PaxosErrorCode::PE_TIMEOUT;
          }
          assert(ccMgr_.preparedIndex == entry.index());
          // success, ret is 0
        }
      } else {
        /* one node case */
        applyConfigureChangeNoLock_(entry.index());
        if (ccMgr_.needNotify != 1) ccMgr_.clear();
      }
    } else {
      log_->setMetaData(keyScanIndex, 0);
      ret = PaxosErrorCode::PE_REPLICATEFAIL;
    }
  } else
    ret = PaxosErrorCode::PE_CONFLICTS;
  return ret;
}

void Paxos::setConfigureChangeTimeout(uint64_t t) {
  ccMgr_.waitTimeout.store(t);
}

int Paxos::configureChange_(
    CCOpTypeT cctype, CCOpTypeT optype, std::vector<std::string> &strConfigArg,
    const std::vector<Configuration::ServerRef> &servers) {
  if (cctype != CCMemberOp && cctype != CCLearnerOp)
    return PaxosErrorCode::PE_INVALIDARGUMENT;
  if (optype != CCAddNode && optype != CCDelNode &&
      optype != CCSyncLearnerAll && optype != CCAddLearnerAutoChange)
    return PaxosErrorCode::PE_INVALIDARGUMENT;

  int ret = PaxosErrorCode::PE_NONE;
  std::vector<std::string> strConfig = strConfigArg;

  std::unique_lock<std::mutex> ul(lock_);

  if (optype == CCAddLearnerAutoChange) {
    if (strConfig.size() > 1) {
      easy_error_log(
          "Server %d : Learner auto change to follower only support one "
          "learner at a time.\n",
          localServer_->serverId);
      return PaxosErrorCode::PE_INVALIDARGUMENT;
    }
    if (ccMgr_.autoChangeAddr != "") {
      easy_error_log(
          "Server %d : Previous learner auto change to follower not finish.\n",
          localServer_->serverId);
      return PaxosErrorCode::PE_DEFAULT;
    }
  }

  if (optype == CCAddNode || optype == CCAddLearnerAutoChange ||
      optype == CCDelNode) {
    /* Step 1: remove dup servers */
    checkConfigure_(cctype, optype, strConfig, servers);
    if (strConfig.size() == 0) {
      easy_error_log(
          "Server %d : New add member already exist or delete member not "
          "found!!\n",
          localServer_->serverId);
      if (optype == CCDelNode)
        return PaxosErrorCode::PE_NOTFOUND;
      else
        return PaxosErrorCode::PE_EXISTS;
    }
  }

  /* Step 2: build log entry  */
  std::string logBuf;
  ConfigureChangeValue val;
  for (auto &addr : strConfig) {
    val.add_addrs(addr);
    logBuf += addr;
    logBuf += " ";
  }
  easy_system_log(
      "Server %d : configureChange begin: cctype(%d) optype(%d) term(%llu) "
      "lli(%llu) addrs(%s)\n",
      localServer_->serverId, cctype, optype, currentTerm_.load(),
      log_->getLastLogIndex(), logBuf.c_str());
  val.set_cctype(cctype);
  val.set_optype(optype);
  if (cctype == CCMemberOp && optype == CCAddNode) {
    auto &newServerAddr = *(strConfig.begin());
    bool addNew = false;
    auto newServer = config_->getLearnerByAddr(newServerAddr);
    if (newServer == nullptr) {
      easy_error_log(
          "Server %d : Try to add member from learner %s which is not "
          "exist!!\n",
          localServer_->serverId, newServerAddr.c_str());
      return PaxosErrorCode::PE_NOTFOUND;
    } else if (newServer->getMatchIndex() + maxDelayIndex4NewMember_ <
               log_->getLastLogIndex()) {
      easy_error_log(
          "Server %d : Try to add member from learner %d, which is delay too "
          "much, matchIndex(%llu), lli(%llu)!!\n",
          localServer_->serverId, newServer->serverId,
          newServer->getMatchIndex(), log_->getLastLogIndex());
      return PaxosErrorCode::PE_DELAY;
    }
    for (auto &server : servers) {
      if (server)
        val.add_allservers(StableConfiguration::memberToString(server));
      else {
        if (!addNew) {
          val.add_allservers(newServerAddr);
          addNew = true;
        } else
          val.add_allservers("0");
      }
    }
    if (!addNew) val.add_allservers(newServerAddr);
    for (auto &learner : config_->getLearners()) {
      if (learner && newServerAddr != learner->strAddr)
        val.add_alllearners(StableConfiguration::learnerToString(learner));
      else
        val.add_alllearners("0");
    }
  } else if (cctype == CCLearnerOp &&
             (optype == CCAddNode || optype == CCAddLearnerAutoChange ||
              optype == CCSyncLearnerAll)) {
    for (auto &learner : config_->getLearners()) {
      if (learner)
        val.add_alllearners(StableConfiguration::learnerToString(learner));
      else
        val.add_alllearners("0");
    }
  }

  ret = sendConfigureAndWait_(val, ul);
  easy_system_log(
      "Server %d : configureChange return: cctype(%d) optype(%d) addrs(%s) "
      "return(%d) success(%d) preparedIndex(%llu) lli(%llu)\n",
      localServer_->serverId, cctype, optype, logBuf.c_str(), ret,
      ccMgr_.applied, ccMgr_.preparedIndex, log_->getLastLogIndex());
  /*
   * In some cases we cannot clear ccMgr flags:
   * 1. an old configureChange exist and sendConfigureAndWait_ return error
   * directly
   * 2. replicateLog fail because of some reasons (leader change...)
   * 3. configureChange timeout (needNotify is set to 0 in sendConfigureAndWait_
   * )
   */
  if (ret != PaxosErrorCode::PE_REPLICATEFAIL &&
      ret != PaxosErrorCode::PE_CONFLICTS && ret != PaxosErrorCode::PE_TIMEOUT)
    ccMgr_.clear();
  return ret;
}

int Paxos::changeLearners(CCOpTypeT type, std::vector<std::string> &strConfig) {
  return configureChange_(CCLearnerOp, type, strConfig, config_->getLearners());
}

int Paxos::changeMember(CCOpTypeT type, std::string &strAddr) {
  std::vector<std::string> tmpConfig{strAddr};
  if (type == CCAddLearnerAutoChange) {
    /* addFollower procedure: add learner -> wait recv log -> change to follower
     */
    int ret = configureChange_(CCLearnerOp, CCAddLearnerAutoChange, tmpConfig,
                               config_->getLearners());
    if (ret != 0) {
      easy_error_log(
          "Server %d : addFollower configChange stage 1 fail, error code %d.\n",
          localServer_->serverId, ret);
      return ret;
    }
    /* wait learner become follower */
    std::unique_lock<std::mutex> ul(lock_);
    if (ccMgr_.waitTimeout.load() == 0)
      ccMgr_.condChangeDone.wait(
          ul, [this]() { return ccMgr_.autoChangeAddr == ""; });
    else {
      ret = ccMgr_.condChangeDone.wait_for(
          ul, std::chrono::milliseconds(ccMgr_.waitTimeout.load()),
          [this]() { return ccMgr_.autoChangeAddr == ""; });
      if (!ret) {
        easy_error_log("Server %d : addFollower wait timeout (%d ms).\n",
                       localServer_->serverId, ccMgr_.waitTimeout.load());
        return PaxosErrorCode::PE_TIMEOUT;
      }
    }
    return ccMgr_.autoChangeRet;
  } else {
    return configureChange_(CCMemberOp, type, tmpConfig, config_->getServers());
  }
}

int Paxos::autoChangeLearnerAction() {
  std::vector<std::string> tmpConfig;
  {
    std::lock_guard<std::mutex> lg(lock_);
    if (ccMgr_.autoChangeAddr == "") return PaxosErrorCode::PE_DEFAULT;
    tmpConfig.push_back(ccMgr_.autoChangeAddr);
  }
  int ret =
      configureChange_(CCMemberOp, CCAddNode, tmpConfig, config_->getServers());
  if (ccMgr_.autoChangeAddr != "") {
    ccMgr_.autoChangeAddr = "";
    ccMgr_.autoChangeRet = ret;
    ccMgr_.condChangeDone.notify_all();
  }
  return ret;
}

int Paxos::configureLearner_(uint64_t serverId, uint64_t source, bool applyMode,
                             std::unique_lock<std::mutex> &ul) {
  auto server = config_->getServer(serverId);
  int ret = PaxosErrorCode::PE_NONE;

  if (!server) {
    easy_error_log("Server %d : can't find server %llu in configureLearner\n",
                   localServer_->serverId, serverId);
    return PaxosErrorCode::PE_NOTFOUND;
  }
  if (server->learnerSource == source &&
      server->sendByAppliedIndex == applyMode) {
    easy_warn_log(
        "Server %d : nothing changed in configureLearner server %llu, "
        "learnerSource:%llu\n",
        localServer_->serverId, serverId, source);
    return PaxosErrorCode::PE_NONE;
  }

  easy_system_log(
      "Server %d : configureLearner: change learnerSource from %llu to %llu\n",
      localServer_->serverId, server->learnerSource, source);

  ConfigureChangeValue val;
  val.set_cctype(CCLearnerOp);
  val.set_optype(CCConfigureNode);
  /* For check. */
  val.add_addrs(server->strAddr);
  val.set_serverid(serverId);
  val.set_learnersource(source);
  val.set_applymode(applyMode);

  ret = sendConfigureAndWait_(val, ul);
  easy_system_log(
      "Server %d : configureLearner return: serverid(%d) return(%d) "
      "success(%d) preparedIndex(%llu) lli(%llu)\n",
      localServer_->serverId, serverId, ret, ccMgr_.applied,
      ccMgr_.preparedIndex, log_->getLastLogIndex());
  if (ret != PaxosErrorCode::PE_REPLICATEFAIL &&
      ret != PaxosErrorCode::PE_CONFLICTS && ret != PaxosErrorCode::PE_TIMEOUT)
    ccMgr_.clear();
  return ret;
}

int Paxos::configureLearner(uint64_t serverId, uint64_t source,
                            bool applyMode) {
  std::unique_lock<std::mutex> ul(lock_);
  return configureLearner_(serverId, source, applyMode, ul);
}

int Paxos::configureLearner(const std::string &addr,
                            const std::string &sourceAddr, bool applyMode) {
  std::unique_lock<std::mutex> ul(lock_);
  uint64_t serverId = config_->getServerIdFromAddr(addr);
  uint64_t source = config_->getServerIdFromAddr(sourceAddr);
  if (serverId < 100 || source == 0) return PaxosErrorCode::PE_NOTFOUND;
  /* We make a trick here: If you want to clear the current learner source, just
   * source the address to itself */
  if (serverId == source) source = 0;
  return configureLearner_(serverId, source, applyMode, ul);
}

int Paxos::configureMember_(uint64_t serverId, bool forceSync,
                            uint electionWeight,
                            std::unique_lock<std::mutex> &ul) {
  if (electionWeight > 9) {
    easy_error_log(
        "Server %d : Fail to change electionWeight. Max electionWeight is 9.",
        localServer_->serverId);
    return PaxosErrorCode::PE_INVALIDARGUMENT;
  }
  auto server = config_->getServer(serverId);
  int ret = PaxosErrorCode::PE_NONE;

  if (!server) {
    easy_error_log("Server %d : can't find server %llu in configureMember\n",
                   localServer_->serverId, serverId);
    return PaxosErrorCode::PE_NOTFOUND;
  }

  if (serverId >= 100) {
    easy_error_log(
        "Server %d : can't configure learner %llu in configureMember\n",
        localServer_->serverId, serverId);
    return PaxosErrorCode::PE_WEIGHTLEARNER;
  }

  if (server->forceSync == forceSync &&
      server->electionWeight == electionWeight) {
    easy_warn_log(
        "Server %d : nothing changed in configureMember server %llu, "
        "forceSync:%u electionWeight:%u\n",
        localServer_->serverId, serverId, forceSync, electionWeight);
    return PaxosErrorCode::PE_NONE;
  }

  ConfigureChangeValue val;
  val.set_cctype(CCMemberOp);
  val.set_optype(CCConfigureNode);
  /* For check. */
  val.add_addrs(server->strAddr);
  val.set_serverid(serverId);
  val.set_forcesync(forceSync);
  val.set_electionweight(electionWeight);

  ret = sendConfigureAndWait_(val, ul);
  easy_system_log(
      "Server %d : configureMember return: serverid(%d) return(%d) success(%d) "
      "preparedIndex(%llu) lli(%llu)\n",
      localServer_->serverId, serverId, ret, ccMgr_.applied,
      ccMgr_.preparedIndex, log_->getLastLogIndex());
  if (ret != PaxosErrorCode::PE_REPLICATEFAIL &&
      ret != PaxosErrorCode::PE_CONFLICTS && ret != PaxosErrorCode::PE_TIMEOUT)
    ccMgr_.clear();
  return ret;
}

int Paxos::configureMember(uint64_t serverId, bool forceSync,
                           uint electionWeight) {
  std::unique_lock<std::mutex> ul(lock_);
  return configureMember_(serverId, forceSync, electionWeight, ul);
}

int Paxos::configureMember(const std::string &addr, bool forceSync,
                           uint electionWeight) {
  std::unique_lock<std::mutex> ul(lock_);
  uint64_t serverId = config_->getServerIdFromAddr(addr);
  return configureMember_(serverId, forceSync, electionWeight, ul);
}

int Paxos::downgradeMember_(uint64_t serverId,
                            std::unique_lock<std::mutex> &ul) {
  auto server = config_->getServer(serverId);
  int ret = 0;

  if (serverId >= 100) {
    easy_error_log(
        "Server %d : try to downgrade server %d which is already a learner!!\n",
        localServer_->serverId, serverId);
    return PaxosErrorCode::PE_DOWNGRADLEARNER;
  }

  if (!server) {
    easy_error_log("Server %d : can't find server %llu in configureMember!!\n",
                   localServer_->serverId, serverId);
    return PaxosErrorCode::PE_NOTFOUND;
  }

  if (localServer_->serverId == serverId && state_ == LEADER) {
    easy_error_log("Server %d : can't downgrade leader(%llu) to learner!!\n",
                   localServer_->serverId, serverId);
    return PaxosErrorCode::PE_DOWNGRADELEADER;
  }

  ConfigureChangeValue val;
  val.set_cctype(CCMemberOp);
  val.set_optype(CCDowngradeNode);
  /* For check. */
  val.add_addrs(server->strAddr);

  ret = sendConfigureAndWait_(val, ul);
  easy_system_log(
      "Server %d : downgradeMember return: serverid(%d) return(%d) success(%d) "
      "preparedIndex(%llu) lli(%llu)\n",
      localServer_->serverId, serverId, ret, ccMgr_.applied,
      ccMgr_.preparedIndex, log_->getLastLogIndex());
  if (ret != PaxosErrorCode::PE_REPLICATEFAIL &&
      ret != PaxosErrorCode::PE_CONFLICTS && ret != PaxosErrorCode::PE_TIMEOUT)
    ccMgr_.clear();
  return ret;
}

int Paxos::downgradeMember(uint64_t serverId) {
  std::unique_lock<std::mutex> ul(lock_);
  return downgradeMember_(serverId, ul);
}

int Paxos::downgradeMember(const std::string &addr) {
  std::unique_lock<std::mutex> ul(lock_);
  uint64_t serverId = config_->getServerIdFromAddr(addr);
  return downgradeMember_(serverId, ul);
}

void Paxos::becameLeader_() {
  if (state_ != LEADER) {
    /* Deal with commit dependency case before set to LEADER */
    LogEntry tmpEntry;
    uint64_t tmpIndex = log_->getLastLogIndex();
    if (log_->getEntry(tmpIndex, tmpEntry, false)) {
      easy_error_log(
          "Server %d: fail to get log entry when become leader to find out "
          "commit dependency, index %llu",
          localServer_->serverId, tmpIndex);
      exit(1);
    }
    if (tmpEntry.optype() == kCommitDep) {
      cdrMgr_.inRecovery = true;
      cdrMgr_.lastLogIndex = tmpIndex;
      cdrMgr_.lastNonCommitDepIndex = 0;
      easy_system_log(
          "Server %d : Last log optype is kCommitDep, will reset the log, "
          "lastLogIndex(%llu). tmpEntry(term:%llu,index:%llu)\n",
          localServer_->serverId, tmpIndex, tmpEntry.term(), tmpEntry.index());
    }

    nextEpochCheckStatemachine_ =
        getNextEpochCheckStatemachine_(currentEpoch_.load());

    /* Deal with the election weight things. */
    if (config_->needWeightElection(localServer_->electionWeight)) {
      easy_system_log(
          "Server %d : Try weight election for this election term(%llu)!!\n",
          localServer_->serverId, currentTerm_.load());
      subState_.store(SubLeaderTransfer);
      weightElecting_ = true;
      new ThreadTimer(srv_->getThreadTimerService(), srv_, electionTimeout_,
                      ThreadTimer::Oneshot, &Paxos::electionWeightAction, this,
                      currentTerm_.load(), currentEpoch_.fetch_add(1));
    }
    /* become leader. */
    changeState_(LEADER);

    /* We change timer form election to heartbeat type. */
    electionTimer_->stop();
    config_->forEach(&Server::beginLeadership, NULL);
    config_->forEachLearners(&Server::beginLeadership, NULL);
    /* we start epochTimer_ when we became candidate to detect all node when we
     * became leader. */
    /* epochTimer_ will stop when we became follower. */
    // epochTimer_->restart();

    if (!cdrMgr_.inRecovery) {
      /* Send an empty log entry to implicitly commit old entries */
      LogEntry entry1;
      log_->getEmptyEntry(entry1);
      replicateLog_(entry1, false);
    } else {
      /* in commit dependency recovery */
      if (commitDepQueue_.push(new commitDepArgType(cdrMgr_.lastLogIndex,
                                                    currentTerm_.load(), this)))
        srv_->sendAsyncEvent(&SingleProcessQueue<commitDepArgType>::process,
                             &commitDepQueue_, Paxos::commitDepResetLog);
    }

    uint64_t lastLogIndex = log_->getLastLogIndex();
    LogEntry entry;
    log_->getEntry(lastLogIndex, entry, false);  // ignore error
    uint64_t lastLogTerm = entry.term();

    easy_system_log(
        "Server %d : become Leader (currentTerm %ld, lli:%ld, llt:%ld)!!\n",
        localServer_->serverId, currentTerm_.load(), lastLogIndex, lastLogTerm);
  }
}

bool Paxos::cdrIsValid(commitDepArgType *arg) {
  std::lock_guard<std::mutex> lg(lock_);
  if (cdrMgr_.inRecovery && currentTerm_ == arg->term)
    return true;
  else
    return false;
}

void Paxos::cdrClear(commitDepArgType *arg) {
  std::lock_guard<std::mutex> lg(lock_);
  if (currentTerm_ == arg->term) cdrMgr_.clear();
}

void Paxos::commitDepResetLog(commitDepArgType *arg) {
  if (!arg->paxos->cdrIsValid(arg)) return;
  std::shared_ptr<PaxosLog> log = arg->paxos->getLog();
  std::shared_ptr<LocalServer> localServer = arg->paxos->getLocalServer();
  easy_system_log("Server %d : start reset log because of commit dependency.\n",
                 localServer->serverId);
  LogEntry tmpEntry;
  uint64_t tmpIndex = arg->lastLogIndex;
  while (--tmpIndex > 0) {
    if (log->getEntry(tmpIndex, tmpEntry, false)) {
      easy_error_log(
          "Server %d: fail to get log entry when reset commit dependency, "
          "index %llu",
          localServer->serverId, tmpIndex);
      exit(1);
    }
    if (tmpEntry.optype() != kCommitDep) break;
  }
  easy_system_log("Server %d : commitDepResetLog reset from index %ld to %ld. tmpEntry(term:%llu,index:%llu)\n",
                 localServer->serverId, arg->lastLogIndex, tmpIndex + 1, tmpEntry.term(), tmpEntry.index());
  arg->paxos->truncateBackward_(tmpIndex + 1);
  if (arg->paxos->debugResetLogSlow) sleep(1);
  tmpEntry.Clear();
  log->getEmptyEntry(tmpEntry);
  tmpEntry.set_term(arg->term);
  while (log->getLastLogIndex() < arg->lastLogIndex) {
    if (debugResetLogSlow) sleep(1);
    tmpEntry.set_index(0);
    tmpEntry.set_checksum(0);
    /* do not use writeLog, lastSyncedIndex is larger than logindex  */
    /* no lock protection: X-Cluster log module ensure that you cannot append a
     * log if you are a follower */
    uint64_t reti = localServer->appendLog(tmpEntry);
    /* avoid dead loop */
    if (reti == 0) {
      /* fail term check */
      easy_error_log(
          "Server %d : fail to do log reset for index %llu, which means I am "
          "not the real leader.\n",
          log->getLastLogIndex() + 1);
      break;
    }
  }
  arg->paxos->cdrClear(arg);
  easy_system_log("Server %d : finish commitDepResetLog.\n",
                 localServer->serverId);
  /* still Send an extra empty log entry to implicitly commit old entries */
  arg->paxos->replicateLog_(tmpEntry, false);
}

uint64_t Paxos::replicateLog_(LogEntry &entry, const bool needLock) {
  uint64_t term = currentTerm_.load();
  auto state = state_.load();
  auto subState = subState_.load();
  if (leaderStepDowning_.load() || state != LEADER ||
      (subState == SubLeaderTransfer && needLock) ||
      term != currentTerm_.load()) {
    if (state != LEADER) {
      easy_error_log(
          "Server %d : replicateLog fail because we're not leader!\n",
          localServer_->serverId);
    } else if (subState == SubLeaderTransfer) {
      easy_error_log(
          "Server %d : replicateLog fail because we're in LeaderTransfer!\n",
          localServer_->serverId);
    } else {
      easy_error_log(
          "Server %d : replicateLog fail because we're in LeaderTransfer!\n",
          localServer_->serverId);
    }

    return 0;
  }
  if (cdrMgr_.inRecovery) {
    entry.set_term(0);
    easy_error_log(
        "Server %d : replicateLog fail because we're in commit dependency "
        "recovery!\n",
        localServer_->serverId);
    return 0;
  }

  entry.set_term(term);
  /*
  if (needLock)
    lock_.unlock();
    */

  ++(stats_.countReplicateLog);

  easy_info_log("Server %d : replicateLog write start logTerm(%ld)\n",
                localServer_->serverId, term);

  /* Traditional Path: write checksum before wirteLog, which is different from
   * X-Cluster */
  /* if checksum not 0, use the checksum from outside (for ut now) */
  if (checksumCb_ && checksum_mode_ && entry.checksum() == 0) {
    const unsigned char *buf =
        reinterpret_cast<const unsigned char *>(entry.value().c_str());
    entry.set_checksum((uint64_t)checksumCb_(0, buf, entry.value().size()));
  }

  auto logIndex = localServer_->writeLog(entry);
  entry.set_index(logIndex);
  if (entry.optype() != kCommitDep && logIndex > 0)
    cdrMgr_.setLastNonCommitDepIndex(logIndex);

  easy_info_log(
      "Server %d : replicateLog write done logTerm(%llu), logIndex(%llu), "
      "optype(%ld), lastNonCommitDepIndex(%llu)\n",
      localServer_->serverId, term, logIndex, entry.optype(),
      cdrMgr_.lastNonCommitDepIndex.load());
  /* TODO
   * if we use sendAsyncEvent to append log here,too much workers will be used
   * to appendLog and be blocked by mutex. So we use sendAsyncEvent only after
   * we have the Group appnedLog function !!
   */
  if (logIndex > 0)
    // appendLog(false);
    // srv_->sendAsyncEvent(AEAppendLog, NULL);
    // srv_->sendAsyncEvent(&Paxos::appendLog, this, true);
    if (appendLogQueue_.push(new (Paxos *)(this)))
      srv_->sendAsyncEvent(&SingleProcessQueue<Paxos *>::mergeableSameProcess,
                           &appendLogQueue_, Paxos::appendLogCb);

  if (!shutdown_.load() && config_->getServerNumLockFree() == 1) {
    if (needLock)
      tryUpdateCommitIndex();
    else
      tryUpdateCommitIndex_();
  }

  return logIndex;
}

int Paxos::requestVote(bool force) {
  std::lock_guard<std::mutex> lg(lock_);
  if (shutdown_.load()) return -1;
  if (state_ == LEADER) {
    /* TODO: should stepdown ? */
    /* only connect to learner when i am leader */
    if (debugWitnessTest) {
      PaxosMsg msg;
      msg.set_term(currentTerm_);
      msg.set_msgtype(RequestVote);
      msg.set_candidateid(localServer_->serverId);
      msg.set_addr(localServer_->strAddr);
      msg.set_force((uint64_t)force);
      uint64_t lastLogIndex;
      msg.set_lastlogindex(lastLogIndex = log_->getLastLogIndex());
      LogEntry entry;
      log_->getEntry(lastLogIndex, entry, false);
      msg.set_lastlogterm(entry.term());
      msg.set_myserverid(getMyServerId());
      config_->forEachLearners(&Server::sendMsg, (void *)&msg);
    }
    return -1;
  }
  if (state_ == LEARNER) {
    easy_system_log("Server %d : Skip requestVote because I am learner.",
                   localServer_->serverId);
    return -1;
  }

  if (localServer_->electionWeight == 0) {
    easy_system_log(
        "Server %d : Skip requestVote because electionWeight is 0 "
        "currentTerm(%ld)\n",
        localServer_->serverId, currentTerm_.load());
    return -1;
  }

  /* For debug */
  if (debugDisableElection) {
    easy_system_log(
        "Server %d : Skip requestVote because of debugDisableElection "
        "currentTerm(%ld)\n",
        localServer_->serverId, currentTerm_.load());
    return -2;
  }

  uint64_t lastLogIndex = log_->getLastLogIndex();
  LogEntry entry;
  if (log_->getEntry(lastLogIndex, entry, false)) {
    easy_error_log(
        "Server %d: fail to get log entry when request vote, index %llu",
        localServer_->serverId, lastLogIndex);
  } else {
    ++currentTerm_;
    log_->setTerm(currentTerm_);
    log_->setMetaData(keyCurrentTerm, currentTerm_);
    leaderId_.store(0);
    leaderAddr_ = std::string("");
    option.extraStore->setRemote("");
    config_->forEach(&Server::beginRequestVote, NULL);
    forceRequestMode_ = force;
    changeState_(CANDIDATE);
    easy_warn_log(
        "Server %d : Epoch task currentEpoch(%llu) during requestVote\n",
        localServer_->serverId, currentEpoch_.load());
    currentEpoch_.fetch_add(1);
    epochTimer_->restart();
    votedFor_ = localServer_->serverId;
    log_->setMetaData(keyVoteFor, votedFor_);
    easy_system_log("Server %d : Start new requestVote: new term(%ld)\n",
                   localServer_->serverId, currentTerm_.load());

    PaxosMsg msg;
    msg.set_term(currentTerm_);
    msg.set_msgtype(RequestVote);
    msg.set_candidateid(localServer_->serverId);
    msg.set_addr(localServer_->strAddr);
    msg.set_force((uint64_t)force);
    msg.set_lastlogindex(lastLogIndex);
    msg.set_lastlogterm(entry.term());
    msg.set_myserverid(getMyServerId());

    config_->forEach(&Server::sendMsg, (void *)&msg);
  }

  electionTimer_->restart(electionTimeout_, true);

  if (config_->getServerNum() == 1) {
    /* Only me in the cluster, became leader immediately. */
    becameLeader_();
  }
  return 0;
}

int Paxos::onRequestVote(PaxosMsg *msg, PaxosMsg *rsp) {
  ++(stats_.countOnMsgRequestVote);

  rsp->set_msgid(msg->msgid());
  rsp->set_msgtype(RequestVoteResponce);
  std::lock_guard<std::mutex> lg(lock_);
  if (shutdown_.load()) return -1;
  rsp->set_serverid(localServer_->serverId);

  if (msg->myserverid() == getMyServerId()) {
    rsp->set_term(currentTerm_);
    rsp->set_votegranted(0);
    easy_error_log(
        "Server %d : reject RequestVote because this server has same mysql server_id(%llu)"
        ", server(id:%llu, addr:%s).\n",
        localServer_->serverId, msg->myserverid(), msg->candidateid(), msg->addr().c_str());
    return 0;
  }

  if (state_ == LEARNER) {
    rsp->set_term(msg->term());
    rsp->set_votegranted(0);
    easy_system_log(
        "Server %d : Receive a RequestVote from server %d, term(%llu) when I'm "
        "LEARNER!! Just reject!!\n",
        localServer_->serverId, msg->candidateid(), msg->term());
    return 0;
  }

  auto server = std::dynamic_pointer_cast<RemoteServer>(
      config_->getServer(msg->candidateid()));
  if (server == nullptr || server->strAddr != msg->addr()) {
    rsp->set_term(currentTerm_);
    rsp->set_votegranted(0);
    easy_system_log(
        "Server %d : reject RequestVote because this server is not in the "
        "current configure, server(id:%llu, addr:%s).\n",
        localServer_->serverId, msg->candidateid(), msg->addr().c_str());
    return 0;
  }

  if (msg->term() < currentTerm_) {
    rsp->set_term(currentTerm_);
    rsp->set_votegranted(0);
    easy_system_log(
        "Server %d : Receive an old RequestVote from server %d msg term(%d) "
        "current term(%d) reject!!\n",
        localServer_->serverId, msg->serverid(), msg->term(),
        currentTerm_.load());
    return 0;
  }

  uint64_t lastLogIndex = log_->getLastLogIndex();
  LogEntry entry;
  if (log_->getEntry(lastLogIndex, entry, false)) {
    rsp->set_term(currentTerm_);
    rsp->set_votegranted(0);
    easy_error_log(
        "Server %d: fail to get log entry when on request vote, index %llu",
        localServer_->serverId, lastLogIndex);
    return 0;
  }
  uint64_t lastLogTerm = entry.term();

  bool logCheck = (msg->lastlogterm() > lastLogTerm ||
                   (msg->lastlogterm() == lastLogTerm &&
                    msg->lastlogindex() >= lastLogIndex));

  easy_system_log(
      "Server %d : leaderStickiness check: msg::force(%d) state_:%d "
      "electionTimer_::Stage:%d leaderId_:%llu .\n",
      localServer_->serverId, msg->force(), state_.load(),
      electionTimer_->getCurrentStage(), leaderId_.load());
  // if (state_ == LEADER || (state_ == FOLLOWER &&
  // electionTimer_->getCurrentStage() == 0 && !msg->force()))
  if (!msg->force() &&
      (state_ == LEADER ||
       (state_ == FOLLOWER &&
        (electionTimer_->getCurrentStage() == 0 && leaderId_ != 0) &&
        !Paxos::debugDisableElection))) {
    rsp->set_term(currentTerm_);
    rsp->set_votegranted(0);
    easy_system_log(
        "Server %d : reject RequestVote because of leaderStickiness, "
        "local(lli:%ld, llt:%ld); msg(candidateid: %d, term: %ld lli:%ld, "
        "llt:%ld) .\n",
        localServer_->serverId, lastLogIndex, lastLogTerm, msg->candidateid(),
        msg->term(), msg->lastlogindex(), msg->lastlogterm());

    if (state_ == LEADER) rsp->set_force(1);
    return 0;
  }

  if (msg->term() > currentTerm_) {
    /* Enter New Term */
    easy_system_log(
        "Server %d : New Term in onRequestVote !! server %d 's term(%d) is "
        "bigger than me(%d).\n",
        localServer_->serverId, msg->candidateid(), msg->term(),
        currentTerm_.load());
    newTerm(msg->term());

    // TODO handle leader case. need stepDown ?
    if (state_ == LEADER)
      ;
  }

  rsp->set_term(currentTerm_);
  rsp->set_votegranted(logCheck && votedFor_ == 0);
  if (rsp->votegranted()) {
    votedFor_ = msg->candidateid();
    log_->setMetaData(keyVoteFor, votedFor_);
    electionTimer_->restart();
  }
  easy_system_log(
      "Server %d : isVote: %d, local(lli:%llu, llt:%d); msg(candidateid: %d, "
      "term: %d lli:%llu, llt:%d) .\n",
      localServer_->serverId, rsp->votegranted(), lastLogIndex, lastLogTerm,
      msg->candidateid(), msg->term(), msg->lastlogindex(), msg->lastlogterm());
  return 0;
}

int Paxos::onClusterIdNotMatch(PaxosMsg *msg) {
  assert(msg->msgtype() == ClusterIdNotMatch);
  std::lock_guard<std::mutex> lg(lock_);
  if (shutdown_.load()) return -1;
  auto server = std::dynamic_pointer_cast<RemoteServer>(
      config_->getServer(msg->serverid()));
  if (server == nullptr) {
    easy_error_log(
        "Server %d : onClusterIdNotMatch receive a msg msgId(%llu) from server "
        "%llu which has been deleted already!\n",
        localServer_->serverId, msg->msgid(), msg->serverid());
    return -1;
  }

  server->disconnect(nullptr);
  easy_error_log(
      "Server %d : server %llu has different cluster id(%llu), local cluster "
      "id(%llu). we should remove this server from the current "
      "onfiguration!!\n",
      localServer_->serverId, msg->serverid(), msg->newclusterid(),
      clusterId_.load());

  return 0;
}

int Paxos::onMsgPreCheck(PaxosMsg *msg, PaxosMsg *rsp) {
  if (msg->clusterid() != clusterId_.load()) {
    easy_error_log(
        "Server %d: Recieve a msg from cluster(%llu), current cluster(%llu), "
        "msg type(%d), these nodes belong to different clusters.\n",
        localServer_->serverId, msg->clusterid(), clusterId_.load(),
        msg->msgtype());
    rsp->set_msgtype(Consensus::ClusterIdNotMatch);
    rsp->set_serverid(msg->serverid());
    rsp->set_term(msg->term());
    rsp->set_msgid(msg->msgid());
    rsp->set_clusterid(msg->clusterid());
    rsp->set_newclusterid(clusterId_.load());
    return 1;
  } else if (msg->entries_size() && log_->entriesPreCheck(msg->entries())) {
    easy_error_log(
        "Server %d: msgId(%llu) onMsgPreCheck, entries pre-check is failed, "
        "msg type(%d), skip AppendLog.\n",
        localServer_->serverId, msg->msgid(), msg->msgtype());
    rsp->set_msgtype(Consensus::PreCheckFailedResponce);
    rsp->set_serverid(msg->serverid());
    rsp->set_term(msg->term());
    rsp->set_msgid(msg->msgid());
    return 1;
  }
  return 0;
}

int Paxos::onMsgPreCheckFailed(PaxosMsg *msg) {
  assert(msg->msgtype() == PreCheckFailedResponce ||
         msg->msgtype() == ClusterIdNotMatch);

  // ClusterIdNotMatch
  if (msg->msgtype() == ClusterIdNotMatch) return onClusterIdNotMatch(msg);

  // PreCheckFailedResponce
  std::lock_guard<std::mutex> lg(lock_);
  if (shutdown_.load()) return -1;

  auto server = std::dynamic_pointer_cast<RemoteServer>(
      config_->getServer(msg->serverid()));
  if (server != nullptr) server->disconnect(nullptr);

  easy_error_log(
      "Server %d: msgId(%llu) onMsgPreCheckFailed, entries pre-check is "
      "failed, msg type(%d).",
      localServer_->serverId, msg->msgid(), msg->msgtype());
  return 0;
}

int Paxos::onRequestVoteResponce(PaxosMsg *msg) {
  assert(msg->msgtype() == RequestVoteResponce);
  std::lock_guard<std::mutex> lg(lock_);
  if (shutdown_.load()) return -1;
  auto server = std::dynamic_pointer_cast<RemoteServer>(
      config_->getServer(msg->serverid()));
  if (server == nullptr) {
    easy_error_log(
        "Server %d : onRequestVoteResponce receive a msg msgId(%llu) from "
        "server %llu which has been deleted already!\n",
        localServer_->serverId, msg->msgid(), msg->serverid());
    return -2;
  }

  if (static_cast<bool>(server) ==
      false)  // for unittest consensus.Paxos_requestVote1
    return 0;

  server->setLastAckEpoch(currentEpoch_);

  if (msg->term() > currentTerm_) {
    easy_system_log(
        "Server %d : New Term in onRequestVoteResponce !! server %d 's "
        "term(%d) is bigger than me(%d).\n",
        localServer_->serverId, msg->serverid(), msg->term(),
        currentTerm_.load());
    newTerm(msg->term());
  } else if (msg->term() < currentTerm_) {
    easy_system_log(
        "Server %d : Receive an old RequestVoteResponce from server %d msg "
        "term(%d) current term(%d) skip!!\n",
        localServer_->serverId, msg->serverid(), msg->term(),
        currentTerm_.load());
    if (msg->force()) {
      /* We reset term, when we're reject because leaderStickiness. */
      if (msg->term() >= log_->getLastLogTerm()) {
        easy_system_log(
            "Server %d : Downgrade term from %llu to %llu when "
            "onRequestVoteResponce, because there are leaderStickiness "
            "leader(%ld) exist!!\n",
            localServer_->serverId, currentTerm_.load(), msg->term(),
            msg->serverid());
        newTerm(msg->term());
      }
    }
  } else if (msg->votegranted()) {
    assert(msg->term() == currentTerm_);
    server->hasVote = true;

    easy_system_log(
        "Server %d : server %d (term:%ld) vote me to became leader.\n",
        localServer_->serverId, msg->serverid(), msg->term());

    if (config_->quorumAll(&Server::haveVote)) {
      becameLeader_();
    }
  } else
    easy_system_log("Server %d : server %d refuse to let me became leader.\n",
                   localServer_->serverId, msg->serverid());

  return 0;
}

int Paxos::appendLog(const bool needLock) {
  if (shutdown_.load()) return -1;

  if (needLock) lock_.lock();
  if (state_ != LEADER) {
    if (needLock) lock_.unlock();
    return -1;
  }

  LogEntry entry;

  PaxosMsg msg;
  msg.set_term(currentTerm_);
  msg.set_msgtype(AppendLog);

  msg.set_leaderid(localServer_->serverId);
  msg.set_myserverid(getMyServerId());
  msg.set_commitindex(commitIndex_);

  /*
   * Some fields of msg are filled by appendLogFillForEach,
   * called by RemoteServer::sendMsg.
   */

  config_->forEach(&Server::sendMsg, (void *)&msg);

  if (needLock) lock_.unlock();
  return 0;
}

int Paxos::appendLogToLearner(std::shared_ptr<RemoteServer> server,
                              bool needLock) {
  if (needLock) lock_.lock();

  /* XXX Now we support learner source to another learner */
  if (state_ != LEADER && state_ != FOLLOWER && state_ != LEARNER) {
    if (needLock) lock_.unlock();
    return -1;
  }

  LogEntry entry;

  PaxosMsg msg;
  msg.set_term(currentTerm_);
  msg.set_msgtype(AppendLog);

  msg.set_leaderid(localServer_->serverId);
  msg.set_myserverid(getMyServerId());
  msg.set_commitindex(commitIndex_);

  if (server == nullptr)
    config_->forEachLearners(&Server::sendMsg, (void *)&msg);
  else {
    server->sendMsg((void *)&msg);
  }

  if (needLock) lock_.unlock();

  return 0;
}

int Paxos::appendLogToServer(std::weak_ptr<RemoteServer> wserver, bool needLock,
                             bool force) {
  std::shared_ptr<RemoteServer> server;

  if (!(server = wserver.lock())) return -1;

  return appendLogToServerByPtr(server, needLock, force);
}

int Paxos::appendLogToServerByPtr(std::shared_ptr<RemoteServer> server,
                                  bool needLock, bool force) {
  bool lockless4force = false;

  if (!force) {
    if (needLock) lock_.lock();
    uint64_t lastLogIndex = replicateWithCacheLog_.load()
                                ? log_->getLastCachedLogIndex()
                                : log_->getLastLogIndex();
    if ((server->nextIndex > lastLogIndex) ||
        (server->isLearner && server->nextIndex > commitIndex_)) {
      if (needLock) lock_.unlock();
      return -1;
    }
  } else {
    if (state_ != LEADER) return -1;
    assert(needLock);
    if (!lock_.try_lock()) {
      lockless4force = true;
    }
  }

  LogEntry entry;

  PaxosMsg msg;
  if (lockless4force) {
    uint64_t savedTerm = currentTerm_.load();
    if (leaderStepDowning_.load() || state_.load() != LEADER ||
        savedTerm != currentTerm_.load())
      return -1;
    msg.set_term(savedTerm);
  } else {
    msg.set_term(currentTerm_);
  }
  msg.set_msgtype(AppendLog);

  msg.set_leaderid(localServer_->serverId);
  msg.set_myserverid(getMyServerId());
  msg.set_commitindex(commitIndex_);

  /*
   * Some fields of msg are filled by appendLogFillForEach,
   * called by RemoteServer::sendMsg.
   */

  /* TODO is force necessary ! */
  if (force) {
    if (server->waitForReply) {
      easy_warn_log(
          "Server %d : server %d do not response in the last heartbeat period, "
          "force to send heartbeat msg.\n",
          localServer_->serverId, server->serverId);
      server->waitForReply = 0;
    }
  }
  server->sendMsgFunc(lockless4force, force, (void *)&msg);

  if (needLock && !lockless4force) lock_.unlock();
  return 0;
}

// try to deal with heartbeat optimistically(without mutex),
// return true if we successfully processed this heartbeat,
// return false otherwise
bool Paxos::onHeartbeatOptimistically_(PaxosMsg *msg, PaxosMsg *rsp) {
  // next 2 load is not safe without mutex, but we just assume they remain
  // unchanged
  StateType state = state_.load();
  uint64_t currentTerm = currentTerm_.load();

  // state not right or different term, must process this heartbeat in a
  // traditional way(with mutex)
  if (state != FOLLOWER || msg->term() != currentTerm) return false;

  easy_error_log(
      "msgId(%llu) received from leader(%d), term(%d), it is heartbeat and "
      "deal it optimistically!\n",
      msg->msgid(), msg->leaderid(), msg->term());

  electionTimer_->restart();

  rsp->set_msgtype(AppendLogResponce);
  rsp->set_msgid(msg->msgid());
  // if `msg->serverid()` does not match local server id, leader will fail to
  // process this responce
  rsp->set_serverid(msg->serverid());
  rsp->set_issuccess(false);
  rsp->set_ignorecheck(true);
  rsp->set_term(currentTerm);
  rsp->set_appliedindex(0);

  return true;
}

int Paxos::onAppendLog(PaxosMsg *msg, PaxosMsg *rsp) {
  ++(stats_.countOnMsgAppendLog);
  assert(msg->msgtype() == AppendLog);

  std::unique_lock<std::mutex> lg(lock_, std::defer_lock);
  if (msg->entries_size() == 0 && optimisticHeartbeat_.load() == true) {
    if (lg.try_lock() == false) {
      if (onHeartbeatOptimistically_(msg, rsp) == true) {
        return 0;
      }
      lg.lock();
    }
    // lock is already held if we reach here
  } else {
    lg.lock();
  }
  if (shutdown_.load()) return -1;

  uint64_t lastLogIndex = log_->getLastLogIndex();
  uint64_t prevLogIndex = msg->prevlogindex();
  bool newTermFlag = false;

  if (1 == config_->getServerNumLockFree() && state_.load() != LEARNER) {
    easy_error_log(
        "Server %d : reject onAppendLog because this server is not in the "
        "current configure, server %llu\n",
        localServer_->serverId, msg->leaderid());
    rsp->set_msgid(msg->msgid());
    rsp->set_msgtype(AppendLogResponce);
    rsp->set_serverid(msg->serverid());
    rsp->set_issuccess(false);
    rsp->set_lastlogindex(lastLogIndex);
    rsp->set_ignorecheck(true);
    rsp->set_term(currentTerm_);
    rsp->set_appliedindex(0);
    return 0;
  }

  rsp->set_msgid(msg->msgid());
  rsp->set_msgtype(AppendLogResponce);
  rsp->set_serverid(msg->serverid());
  /* when add node and the node does not complete the initialization */
  if (NULL == localServer_) {
    int i = 0;
    while (NULL == localServer_) {
      /* avoid loop indefinately */
      if (i > 60) break;
      easy_warn_log("Local server has not be initialized, sleep 1 second!\n");
      sleep(1);
      i++;
    }
  }
  assert(localServer_ != NULL);
  if (localServer_->serverId != msg->serverid()) {
    if (state_ != LEARNER) {
      easy_error_log(
          "Server %d : the server id in the msg(%llu) is not match with local "
          "server id for a follower, this may happen during the configure "
          "change or hit a bug!!\n",
          localServer_->serverId, msg->serverid());
      if (compactOldMode_ && msg->serverid() == leaderId_) {
        rsp->set_serverid(localServer_->serverId);
        easy_warn_log(
            "Server %d : receive a msg from old version leader, in compact "
            "mode we use %llu instead of %llu as server id \n",
            localServer_->serverId, localServer_->serverId, msg->serverid());
      }
    } else {
      easy_error_log(
          "Server %d : the server id in the msg(%llu) is not match with local "
          "server id for a learner, we change the local server id to %llu!!\n",
          localServer_->serverId, msg->serverid(), msg->serverid());
      localServer_->serverId = msg->serverid();
    }
  }

  if (state_ == LEARNER && msg->myserverid() == getMyServerId()) {
    easy_error_log(
        "Server %d : reject AppendLog because this server has same mysql server_id(%llu)"
        ", server(id:%llu, addr:%s).\n",
        localServer_->serverId, msg->myserverid(), msg->candidateid(), msg->addr().c_str());
    return -1;
  }


  easy_warn_log(
      "Server %d : msgId(%llu) onAppendLog start, receive logs from "
      "leader(%d), msg.term(%d), msg.commitindex(%llu), msg.prevlogindex(%llu), entries_size(%llu), lli(%llu)\n",
      localServer_->serverId, msg->msgid(), msg->leaderid(), msg->term(),
      msg->commitindex(), msg->prevlogindex(), msg->entries_size(),
      lastLogIndex);

  /*
   * XXX  About msg->lastlogindex
   * when appendLog is success: msg->lastlogindex means the last log index in
   * the msg (prevLogIndex + numEntries) when appendLog is unsuccess:
   * msg->lastlogindex means the last log index in the follower's local log_
   * when appendLog is success but in cached mode: msg->lastlogindex means the
   * last log index in the follower's local log_(not include cache)
   */
  rsp->set_issuccess(false);
  rsp->set_lastlogindex(lastLogIndex);
  rsp->set_ignorecheck(false);
  rsp->set_appliedindex(appliedIndex_.load());

  /* in some case we should step down */
  if (msg->term() > currentTerm_) {
    easy_warn_log(
        "Server %d : New Term in onAppendLog !! server %d 's term(%d) is "
        "bigger than me(%d).\n",
        localServer_->serverId, msg->leaderid(), msg->term(),
        currentTerm_.load());
    if (state_.load() != LEADER) {
      newTerm(msg->term());
      newTermFlag = true;
    } else {
      rsp->set_term(currentTerm_);
      return -1;
    }
  } else if (msg->term() < currentTerm_) {
    if (!forceRequestMode_ && leaderId_.load() == 0 &&
        msg->term() >= log_->getLastLogTerm() && state_ == CANDIDATE) {
      easy_system_log(
          "Server %d : Downgrade term from %llu to %llu when onAppendLog, "
          "because there are leaderStickiness leader(%ld) exist!!\n",
          localServer_->serverId, currentTerm_.load(), msg->term(),
          msg->serverid());
      newTerm(msg->term());
    } else if (state_ == LEARNER && (enableLearnerAutoResetMatchIndex_ ||
                                     msg->term() >= log_->getLastLogTerm())) {
      easy_system_log(
          "Server %d : Downgrade term from %llu to %llu when onAppendLog, "
          "because I am learner!!\n",
          localServer_->serverId, currentTerm_.load(), msg->term());
      newTerm(msg->term());
    } else {
      easy_warn_log(
          "Server %d : msgId(%llu) receive logs from old leader(%ld) current "
          "leader(%ld). localTerm(%ld),msg.term(%d) \n",
          localServer_->serverId, msg->msgid(), msg->leaderid(),
          leaderId_.load(), currentTerm_.load(), msg->term());
      rsp->set_term(currentTerm_);
      return -1;
    }
  } else if (state_ != FOLLOWER && state_ != LEARNER) {
    changeState_(FOLLOWER);
  }
  rsp->set_term(currentTerm_);

  if (leaderId_ == 0) {
    leaderId_.store(msg->leaderid());
    leaderAddr_ = "";
    option.extraStore->setRemote("");
    rsp->set_force(1);
  } else if (leaderId_ != msg->leaderid()) {
    /* TODO is this possible? */
    easy_warn_log(
        "Server %d : receive logs from different leader. old(%d),new(%d), "
        "term(%ld),msg.term(%d) \n",
        localServer_->serverId, leaderId_.load(), msg->leaderid(),
        currentTerm_.load(), msg->term());
    leaderId_.store(msg->leaderid());
    leaderAddr_ = "";
    option.extraStore->setRemote("");
    rsp->set_force(1);
  }

  if (msg->has_addr()) {
    leaderAddr_ = msg->addr();
    if (msg->has_extra()) option.extraStore->setRemote(msg->extra());
  }
  if (leaderAddr_ == "") rsp->set_force(1);

  if (state_ != LEARNER) electionTimer_->restart();

  if (!msg->has_prevlogterm()) {
    rsp->set_ignorecheck(true);
    easy_warn_log(
        "Server %d : msgId(%llu) receive logs without prevlogterm. from server "
        "%ld, localTerm(%ld),msg.term(%d) lli:%ld\n",
        localServer_->serverId, msg->msgid(), msg->leaderid(),
        currentTerm_.load(), msg->term(), lastLogIndex);
    return 0;
  }

  if (prevLogIndex > lastLogIndex) {
    uint64_t msgEntrieSize = msg->entries_size();
    uint64_t msgLastIndex;
    uint64_t beginTerm;
    uint64_t beginIndex;
    if (msgEntrieSize != 0) {
      msgLastIndex = prevLogIndex + msg->entries_size();
      beginTerm = msg->entries().begin()->term();
      beginIndex = msg->entries().begin()->index();
    }

    /* Now we allow hole in the log. we put the uncontinue log in the cache */
    if (enableLogCache_ && !msg->nocache() && msgEntrieSize != 0 &&
        beginTerm == (msg->entries().end() - 1)->term() &&
        beginTerm == currentTerm_) {
      logRecvCache_.put(beginIndex, msgLastIndex, *(msg->mutable_entries()));
      logRecvCache_.setCommitIndex(msg->commitindex());
      rsp->set_issuccess(true);
      rsp->set_lastlogindex(log_->getLastLogIndex());
      easy_warn_log(
          "Server %d : receive uncontinue log local lastLogIndex(%ld, "
          "term:%ld); msgId(%llu) msg prevlogindex(%ld, term:%ld) has %llu "
          "entries firstIndex(%llu) lastIndex(%llu); put it in cache.\n",
          localServer_->serverId, lastLogIndex, currentTerm_.load(),
          msg->msgid(), msg->prevlogindex(), msg->prevlogterm(), msgEntrieSize,
          beginIndex, msgLastIndex);
    } else {
      /*
       * This is possible. It happened when the new leader send the first
       * appendlog msg. We return a hint to let leader know our last log index.
       */
      easy_warn_log(
          "Server %d : msgId(%llu) receive log's prevlogindex(%ld, term:%ld) "
          "is bigger than lastLogIndex(%ld, term:%ld) reject.\n",
          localServer_->serverId, msg->msgid(), msg->prevlogindex(),
          msg->prevlogterm(), lastLogIndex, currentTerm_.load());
      rsp->set_lastlogindex(log_->getLastLogIndex());

      /* We clear cache here. */
      // logRecvCache_.clear();
      return -1;
    }
  } else {
    LogEntry prevLogEntry;
    int error;
    if ((error = log_->getEntry(prevLogIndex, prevLogEntry, false)) ||
        (prevLogEntry.term() != msg->prevlogterm() &&
         prevLogEntry.optype() !=
             kMock))  // TODO should check start index here.
    {
      if (error) {
        easy_warn_log(
            "Server %d: fail to get log entry on append log, index %llu",
            localServer_->serverId, prevLogIndex);
      } else {
        /* log is not match, reject it. the leader will send the correct log
         * again. */
        easy_warn_log(
            "Server %d : msgId(%llu) msg's prevlogterm(%llu) is not match with "
            "local log's prevlogterm(%llu) with index(%llu) reject!",
            localServer_->serverId, msg->msgid(), msg->prevlogterm(),
            prevLogEntry.term(), prevLogIndex);
      }
      if (state_ == FOLLOWER) {
        /*
         * In some rare case, leader thinks it is a learner but this node is
         * still in follower state, because it has not received the downgrade
         * configure change logEntry. Just set the role field in response msg
         * and let leader be aware of this situation.
         */
        rsp->set_role(state_);
      }
      return 0;
    }
    /*
    else
      easy_warn_log("Server %d : msgId(%llu) msg's prevlogterm(%llu) is match
    with local log's prevlogterm(%llu) pass.", localServer_->serverId,
    msg->msgid(), msg->prevlogterm(), prevLogEntry.term());
      */

    // checksum test
    if (checksum_mode_) {
      for (auto &entry : msg->entries()) {
        if (log_checksum_test(entry)) {
          easy_error_log("Server %d: msgId(%llu) log index %llu checksum fail(expect %llu).",
                         localServer_->serverId, msg->msgid(), entry.index(), entry.checksum());
          return -1;
        }
      }
    }

    rsp->set_issuccess(true);
    enableLogCache_ = true;

    // assert(msg->entries_size() <= 1);
    if (msg->entries_size() > 0) {
      easy_warn_log(
          "Server %d : msgId(%llu) receive log has %ld entries, plt:%ld, "
          "pli:%ld, commitIndex:%ld, lli:%ld, pli2:%ld\n",
          localServer_->serverId, msg->msgid(), msg->entries_size(),
          msg->prevlogterm(), msg->prevlogindex(), msg->commitindex(),
          log_->getLastLogIndex(), prevLogIndex);
      bool appendDone = false;
      if (msg->entries_size() != 0 && log_->getLastLogIndex() == prevLogIndex) {
        /* no need truncate */
        uint64_t lli = log_->append(msg->entries());
        if (lli == 0) {
          rsp->set_lastlogindex(log_->getLastLogIndex());
          rsp->set_issuccess(false);
          rsp->set_msgerror(PaxosMsg::APPEND);
          easy_warn_log(
              "Server %d : msgId(%llu) write log entries from index %llu, size "
              "%lu failed\n",
              localServer_->serverId, msg->msgid(), prevLogIndex + 1,
              msg->entries_size());
          return 0;
        }
        appendDone = true;
        rsp->set_lastlogindex(log_->getLastLogIndex());
        assert(lli == (prevLogIndex + msg->entries_size()));
        assert(lli == log_->getLastLogIndex());
        if (true)  //(state_ != LEARNER)
        {
          for (auto &entry : msg->entries())
            if (entry.optype() == kConfigureChange) {
              prepareConfigureChangeEntry_(entry, msg);
            }
        }
      } else {
        uint64_t msgLastIndex = prevLogIndex + msg->entries_size();
        uint64_t lastLogIndex = log_->getLastLogIndex();
        if (!newTermFlag && lastLogIndex >= msgLastIndex) {
          /* In some continues log entries, if the first entry and the last
           * entry have the same term, all these entries have the same term. */
          uint64_t beginTerm = msg->entries().begin()->term();
          if (beginTerm == (msg->entries().end() - 1)->term() &&
              beginTerm == currentTerm_ &&
              prevLogEntry.term() == currentTerm_) {
            easy_warn_log(
                "Server %d : ignore %ld entries, plt:%ld, pli:%ld, "
                "commitIndex:%ld lliInMsg:%llu lli:%llu\n",
                localServer_->serverId, msg->entries_size(), msg->prevlogterm(),
                msg->prevlogindex(), msg->commitindex(), msgLastIndex,
                lastLogIndex);
            appendDone = true;
            rsp->set_lastlogindex(msgLastIndex);
          }
        }
      }

      if (!appendDone) {
        uint64_t index = prevLogIndex;
        rsp->set_lastlogindex(prevLogIndex + msg->entries_size());
        int dupcnt = 0;
        for (auto it = msg->entries().begin(); it != msg->entries().end();
             ++it) {
          ++index;
          const LogEntry &entry = *it;

          easy_warn_log(
              "Server %d : parse entries index:%ld, entry.term:%ld, "
              "entry.index:%ld, lli:%ld\n",
              localServer_->serverId, index, entry.term(), entry.index(),
              log_->getLastLogIndex());
          assert(entry.index() == index);

          if (log_->getLastLogIndex() >= index) {
            /* need truncate */
            LogEntry en;
            if (!log_->getEntry(index, en, false) &&
                (en.term() == entry.term() || en.optype() == kMock)) {
              /* The duplicate log entry, that has already received. */
              dupcnt++;
              easy_warn_log(
                  "Server %d : duplicate log entry, ignore, entry.term:%ld, "
                  "entry.index:%ld, optype(%ld)\n",
                  localServer_->serverId, entry.term(), entry.index(), en.optype());
              continue;
            }
            easy_warn_log("Server %d : need trunc log entry, entry(term:%ld, index:%ld, optype:%ld)\n",
                localServer_->serverId, en.term(), en.index(), en.optype());

            // commit index might be bigger than last log index if we set
            // learner auto reset match index on
            if (enableLearnerAutoResetMatchIndex_ && commitIndex_ >= index &&
                index > 1)
              commitIndex_ = index - 1;
            /* Truncate the log start from the index. */
            truncateBackward_(index);
            ++(stats_.countTruncateBackward);
            easy_system_log(
                "Server %d : truncate paxos log from(include) %ld in appendLog "
                "msg, lli:%ld\n",
                localServer_->serverId, index, log_->getLastLogIndex());
            break;
          } else
            break;
        }
        int msgEntrieSize = msg->entries_size();
        msg->mutable_entries()->DeleteSubrange(0, dupcnt);
        assert(msg->entries_size() == (msgEntrieSize - dupcnt));
        easy_warn_log(
            "Server %d : Duplicate entrys count %d, remaining entries count %d",
            localServer_->serverId, dupcnt, msg->entries_size());
        if (msg->entries_size() > 0) {
          assert((log_->getLastLogIndex() + 1) == msg->entries(0).index());
          if (log_->append(msg->entries()) == 0) {
            rsp->set_lastlogindex(log_->getLastLogIndex());
            rsp->set_issuccess(false);
            rsp->set_msgerror(PaxosMsg::APPEND);
            easy_warn_log(
                "Server %d : msgId(%llu) write log entries from index %llu, "
                "size %lu failed\n",
                localServer_->serverId, msg->msgid(), msg->entries(0).index(),
                msg->entries_size());
            return 0;
          }
          assert(log_->getLastLogIndex() ==
                 msg->entries(msg->entries_size() - 1).index());
        }
        // deal with ConfigureChange
        for (auto &entry : msg->entries())
          if (entry.optype() == kConfigureChange) {
            prepareConfigureChangeEntry_(entry, msg);
          }
      }
      PaxosLogCacheNode *node = logRecvCache_.get(log_->getLastLogIndex() + 1);
      if (node != NULL) {
        if (log_->append(node->entries) == 0) {
          rsp->set_lastlogindex(log_->getLastLogIndex());
          rsp->set_issuccess(false);
          rsp->set_msgerror(PaxosMsg::APPEND);
          easy_warn_log(
              "Server %d : msgId(%llu) write cached log entries from index "
              "%llu, size %lu failed\n",
              localServer_->serverId, msg->msgid(), log_->getLastLogIndex() + 1,
              node->entries.size());
          delete node;
          return 0;
        }
        rsp->set_lastlogindex(log_->getLastLogIndex());
        for (auto &entry : node->entries)
          if (entry.optype() == kConfigureChange) {
            prepareConfigureChangeEntry_(entry, msg, true);
          }
        easy_warn_log(
            "Server %d : Get log from cache, beginIndex(%llu) endIndex(%llu) "
            "term(%llu)\n",
            localServer_->serverId, node->beginIndex, node->endIndex,
            node->entries.begin()->term());
      }
      delete node;
    } else
      ++(stats_.countOnHeartbeat);

    // rsp->set_lastlogindex(log_->getLastLogIndex());

    /* Update commitIndex. */
    if (msg->commitindex() > commitIndex_ && !debugSkipUpdateCommitIndex) {
      if (ccMgr_.prepared && ccMgr_.preparedIndex <= msg->commitindex() &&
          ccMgr_.preparedIndex > commitIndex_) {
        // srv_->sendAsyncEvent(&Paxos::applyConfigureChange_, this,
        // ccMgr_.preparedIndex);
        applyConfigureChangeNoLock_(ccMgr_.preparedIndex);
        if (ccMgr_.needNotify != 1) ccMgr_.clear();
      }
      easy_warn_log("Server %d : Follower commitIndex change from %ld to %ld\n",
                    localServer_->serverId, commitIndex_.load(), msg->commitindex());
      commitIndex_ = msg->commitindex();
      assert(commitIndex_ <= log_->getLastLogIndex());

      /* already hold the lock_ by the caller. */
      cond_.notify_all();

      /* X-Paxos support learner get log from follower. */
      appendLogToLearner();
      /*
         if (srv_->cs)
         srv_->cs->set(entry.ikey(), entry.value());
         */
    }
  }

  if (tryFillFollowerMeta_(rsp->mutable_cientries()))
    easy_warn_log("Server %d : msgId(%llu) tryFillFollowerMeta\n",
                  localServer_->serverId);

  easy_warn_log("Server %d : msgId(%llu) onAppendLog end, is_success %d, current commitIndex_ %llu\n",
                localServer_->serverId, msg->msgid(), rsp->issuccess(), commitIndex_.load());
  return 0;
}

int Paxos::onAppendLogResponce(PaxosMsg *msg) {
  assert(msg->msgtype() == AppendLogResponce);
  /* XXX Now we support learner source to another learner */
  if (state_ != LEADER && state_ != FOLLOWER && state_ != LEARNER) return -1;

  easy_info_log(
      "Server %d : onAppendLogResponce msgId(%llu)"
      "server %d, lastlogindex(%ld)\n",
      localServer_->serverId, msg->msgid(), msg->serverid(), msg->lastlogindex());

  /* update extra storage for Followers */
  if (msg->has_extra()) option.extraStore->setRemote(msg->extra());

  std::lock_guard<std::mutex> lg(lock_);
  if (shutdown_.load()) return -1;
  auto server = std::dynamic_pointer_cast<RemoteServer>(
      config_->getServer(msg->serverid()));
  auto wserver = std::weak_ptr<RemoteServer>(server);
  // easy_warn_log("Server %d : onAppendLogResponce receive a msg msgId(%llu)
  // from server %llu\n", localServer_->serverId, msg->msgid(),
  // msg->serverid());
  if (server == nullptr) {
    easy_warn_log(
        "Server %d : onAppendLogResponce receive a msg msgId(%llu) from server "
        "%llu which has been deleted already!\n",
        localServer_->serverId, msg->msgid(), msg->serverid());
    return -2;
  }

  if (state_ == FOLLOWER &&
      (server->learnerSource != localServer_->serverId || !server->isLearner)) {
    easy_warn_log(
        "Server %d : onAppendLogResponce receive a msg msgId(%llu) from server "
        "%llu learnerSource:%llu who's learnerSource not match or already not "
        "a learner!\n",
        localServer_->serverId, msg->msgid(), msg->serverid(),
        server->learnerSource);
    return -3;
  }

  // assert(server->waitForReply == 1);
  if (msg->msgid() >= server->guardId)
    server->waitForReply = 0;
  else
    easy_warn_log(
        "Server %d : onAppendLogResponce skip reset waitForReply, msgid %llu "
        "guardid %llu",
        localServer_->serverId, msg->msgid(), server->guardId.load());
  if (msg->has_force() && msg->force() == 1) server->needAddr = true;

  if (msg->term() > currentTerm_) {
    easy_warn_log(
        "Server %d : New Term in onAppendLogResponce msgId(%llu) !! server %d "
        "'s term(%d) is bigger than me(%d).\n",
        localServer_->serverId, msg->msgid(), msg->serverid(), msg->term(),
        currentTerm_.load());
    if (state_.load() != LEADER) {
      newTerm(msg->term());
    } else {
      if (server->matchIndex.load() != 0) {
        /* The follower(server) now is a naughty server, do not use pipelinng */
        easy_warn_log(
            "Server %d : msgId(%llu) server %d became a naughty server, reset "
            "match index from %lu to 0\n",
            localServer_->serverId, msg->msgid(), msg->serverid(),
            server->matchIndex.load());
        server->resetMatchIndex(0);
      }
    }
  } else if (msg->term() < currentTerm_) {
    easy_warn_log(
        "Server %d : Receive prev term's AppendLogResponce msgId(%llu) "
        "(term:%ld) from server(%ld), currentTerm(%ld) just ignore!!\n",
        localServer_->serverId, msg->msgid(), msg->term(), msg->serverid(),
        currentTerm_.load());
  } else {
    assert(msg->term() == currentTerm_);
    /* Inc epoch for RemoteServer's. we reset RemoteServer's heartbeat in
     * sendMsgFunc. */
    server->setLastAckEpoch(currentEpoch_);
    if (server->appliedIndex < msg->appliedindex())
      server->appliedIndex = msg->appliedindex();
    /*
     * XXX  About msg->lastlogindex
     * when appendLog is success: msg->lastlogindex means the last log index in the msg (prevLogIndex + numEntries)
     * when appendLog is unsuccess: msg->lastlogindex means the last log index in the follower's local log_
     * when appendLog is success but in cached mode: msg->lastlogindex means the last log index in the follower's local log_(not include cache)
     */
    if (msg->issuccess()) {
      if (server->nextIndex != msg->lastlogindex() + 1 ||
          server->matchIndex != 0) {
        uint64_t oldMatchIndex = server->matchIndex;
        uint64_t oldNextIndex = server->nextIndex;

        if (msg->lastlogindex() < server->nextIndex &&
            server->matchIndex == 0) {
          easy_warn_log(
              "Server %d : onAppendLogResponce this response of AppendLog to "
              "server %d may be a resend msg that we have already received, "
              "msg index(%ld) is smaller than nextIndex(%ld)\n",
              localServer_->serverId, msg->serverid(), msg->lastlogindex(),
              server->nextIndex.load());
        } else {
          server->hasMatched = true;
          if (msg->lastlogindex() > server->matchIndex) {
            server->matchIndex = msg->lastlogindex();
            /* trigger auto change to follower, use maxDelayIndex4NewMember_/2
             * to have more chance to success */
            if (ccMgr_.autoChangeAddr == server->strAddr && state_ == LEADER &&
                server->isLearner &&
                (log_->getLastLogIndex() <=
                 (maxDelayIndex4NewMember_ / 2 + server->matchIndex.load()))) {
              srv_->sendAsyncEvent(&Paxos::autoChangeLearnerAction, this);
            }
          }
          if (server->nextIndex < server->matchIndex + 1)
            server->nextIndex = server->matchIndex + 1;
          /*
           * try to update commitIndex here,
           * only if matchIndex is greater than commitIndex
           */
          if (server->matchIndex > commitIndex_) tryUpdateCommitIndex_();
        }

        easy_warn_log(
            "Server %d : msgId(%llu) AppendLog to server %d success, "
            "matchIndex(old:%llu,new:%llu) and nextIndex(old:%llu,new:%llu) "
            "have changed\n",
            localServer_->serverId, msg->msgid(), msg->serverid(),
            oldMatchIndex, server->matchIndex.load(), oldNextIndex,
            server->nextIndex.load());
      } else if (server->matchIndex == 0) {
        // previously this server might be considered a naughty server, and its
        // match index is set to 0, now we set it right, so when no log is
        // replicated, match index will still be right
        server->matchIndex = msg->lastlogindex();
        easy_warn_log(
            "Server %d : msgId(%llu) AppendLog to server %d success, this is a "
            "heartbeat responce, set match index from 0 to %llu. "
            "nextIndex(%llu) msg(lli:%llu term:%llu)\n",
            localServer_->serverId, msg->msgid(), msg->serverid(),
            msg->lastlogindex(), server->nextIndex.load(), msg->lastlogindex(),
            msg->term());
      } else {
        /*
         * We receive a heartbeat responce, before we commit any logEntry in
         * this term. There must be some bug, or misorder msg.
         */
        easy_warn_log(
            "Server %d : msgId(%llu) AppendLog to server %d success, skip "
            "because this is a heartbeat responce. nextIndex(%llu) "
            "msg(lli:%llu term:%llu)\n",
            localServer_->serverId, msg->msgid(), msg->serverid(),
            server->nextIndex.load(), msg->lastlogindex(), msg->term());
      }

      /* Update meta for learner source */
      if (msg->cientries_size() > 0) {
        config_->mergeFollowerMeta(msg->cientries());
        easy_warn_log(
            "Server %d : msgId(%llu) mergeFollowerMeta from server %d\n",
            localServer_->serverId, msg->msgid(), msg->serverid());
      }
      // we should deal with matchIndex == 0 case.

      /*
       * Case when the follower is not uptodate,
       * There is no need to wait for next heart beat to send the next log
       * entry.
       */
      // if (server->nextIndex <= log_->getLastLogIndex())
      //   appendLogToServer(server.get(), false);
      // srv_->sendAsyncEvent(&Paxos::appendLogToServer, this, (RemoteServer
      // *)server.get(), true, false);
      appendLogToServer(std::move(wserver), false, false);
      if (getState() == Paxos::FOLLOWER || getState() == Paxos::LEARNER) {
        updateFollowerMetaNo();
        easy_warn_log("Server %d : updateFollowerMetaNo\n",
                      localServer_->serverId);
      }
    } else if (msg->has_ignorecheck() && msg->ignorecheck()) {
      easy_warn_log(
          "Server %d : msgId(%llu) AppendLog to server %d without check\n",
          localServer_->serverId, msg->msgid(), msg->serverid());
      if (server->isLearner) appendLogToServer(std::move(wserver), false);
    } else {
      uint64_t oldNextIndex = server->nextIndex;

      /* We also need to reset matchindex if leader thinks it is a learner but
       * the node is still in follower state */
      bool learnerStateNotMatch =
          server->isLearner && msg->has_role() && msg->role() == FOLLOWER;
      if (learnerStateNotMatch)
        server->resetMatchIndex(0);  // to trigger decrement nextIndex case
      if (!server->isLearner || learnerStateNotMatch) {
        uint64_t term = 0, optype = 0, info = 0;
        if (consensusAsync_.load() || enableAutoResetMatchIndex_ ||
            (!log_->getLogMeta(server->matchIndex.load() + 1, &term, &optype,
                               &info) &&
             (info & (1 << 5 | 1 << 6)))) {
          if (!server->hasMatched && msg->lastlogindex() < server->matchIndex) {
            /*  log is lost, it might happen due to follower does not sync the
             *log after they receive it and there has been a crash recovery
             *  1. sync log is not set
             *  2. FLAG_BLOB | FLAG_BLOB_END, haven't got the chance to flush
             **/
            easy_error_log(
                "Server %d : follower(%d) might lost some logs. "
                "matchIndex(%llu) is greater than follower's lli(%llu), we "
                "reset matchIndex to 0!",
                localServer_->serverId, msg->serverid(),
                server->matchIndex.load(), msg->lastlogindex());
            server->resetMatchIndex(0);
          }
        }

        if (server->matchIndex > 0) {
          server->nextIndex = server->matchIndex + 1;
        } else {
          /* send correct log for this RemoteServer. */
          if (server->nextIndex > 1) {
            --(server->nextIndex);
          }

          /* XXX if the follower lost many logs, decrement nextIndex once. */
          // if (server->nextIndex > msg->prevlogindex() + 1)
          if (server->nextIndex > msg->lastlogindex() + 1) {
            server->nextIndex = msg->lastlogindex() + 1;
          }
        }

        if (!msg->has_msgerror() || msg->msgerror() != PaxosMsg::APPEND)
          appendLogToServer(std::move(wserver), false);

        easy_warn_log(
            "Server %d : msgId(%llu) AppendLog to server %d failed, msg error "
            "%d, "
            "lastlogindex(%ld) is not match with the local nextIndex(%ld), "
            "set local nextIndex to %ld, matchIndex(%llu).\n",
            localServer_->serverId, msg->msgid(), msg->serverid(),
            msg->has_msgerror() ? msg->msgerror() : 0, msg->lastlogindex(),
            oldNextIndex, server->nextIndex.load(), server->matchIndex.load());
      } else {
        if (enableLearnerAutoResetMatchIndex_) {
          server->resetMatchIndex(0);

          // decrease next index until log match, treat learner like follower
          if (server->nextIndex > 1) {
            --server->nextIndex;
          }

          if (server->nextIndex > msg->lastlogindex() + 1) {
            server->nextIndex = msg->lastlogindex() + 1;
          }

          easy_warn_log(
              "Server %d : Learner(%d) match index reset to 0, nextIndex set "
              "to %llu.",
              localServer_->serverId, msg->serverid(),
              server->nextIndex.load());

          appendLogToLearner(wserver.lock());
        } else {
          uint64_t oldNextIndex = server->nextIndex;
          uint64_t oldMatchIndex = server->matchIndex;
          server->resetMatchIndex(msg->lastlogindex());
          server->nextIndex = msg->lastlogindex() + 1;
          if (oldNextIndex != server->nextIndex ||
              oldMatchIndex != server->matchIndex) {
            easy_warn_log(
                "Server %d : Learner(%d) change its local log position! We "
                "reset server(learner)'s matchIndex(old:%llu,new:%llu) and "
                "nextIndex(old:%llu,new:%llu).",
                localServer_->serverId, msg->serverid(), oldMatchIndex,
                server->matchIndex.load(), oldNextIndex,
                server->nextIndex.load());

            appendLogToLearner(wserver.lock());
          } else {
            easy_warn_log(
                "Server %d : Learner(%d) change its local log position or "
                "term, which is not correct !! current matchIndex(%llu) "
                "nextIndex(%llu).",
                localServer_->serverId, msg->serverid(),
                server->matchIndex.load(), server->nextIndex.load());
          }
        }
      }
      /* Resend the correct log entry. */
      /* TODO Need async */
    }
  }

  /* send correct log if needed */
  return 0;
}

int Paxos::onAppendLogSendFail(PaxosMsg *msg, uint64_t *newId) {
  /* No need to resend the msg if this server is not leader or the prev term
   * msg. */
  if (state_ != LEADER || msg->term() != currentTerm_) return -1;

  if (msg->msgtype() != AppendLog) return -2;

  /* No need to resend the heartbeat msg. */
  if (msg->entries_size() == 0 && msg->has_compressedentries() == false)
    return -3;

  lock_.lock();
  auto server = std::dynamic_pointer_cast<RemoteServer>(
      config_->getServer(msg->serverid()));
  auto wserver = std::weak_ptr<RemoteServer>(server);
  if (server == nullptr) {
    easy_warn_log(
        "Server %d : onAppendLogSendFail try resend msgId(%llu) to server %llu "
        "which has been deleted already!\n",
        localServer_->serverId, msg->msgid(), msg->serverid());
    lock_.unlock();
    return -4;
  }

  if (server->lostConnect.load() || server->disablePipelining.load()) {
    lock_.unlock();
    return -5;
  }

  lock_.unlock();

  if (newId) *newId = server->msgId.fetch_add(1);

  /* TODO reset timer for the correspond RemoteServer. */
  return 0;
}

int Paxos::onLeaderCommand(PaxosMsg *msg, PaxosMsg *rsp) {
  ++(stats_.countOnLeaderCommand);
  lock_.lock();
  if (shutdown_.load()) return -1;

  rsp->set_msgid(msg->msgid());
  rsp->set_msgtype(LeaderCommandResponce);
  rsp->set_serverid(localServer_->serverId);
  rsp->set_term(currentTerm_);

  if (msg->lctype() == LeaderTransfer) {
    /* Update commitIndex. */
    if (msg->commitindex() > commitIndex_) {
      if (ccMgr_.prepared && ccMgr_.preparedIndex <= msg->commitindex() &&
          ccMgr_.preparedIndex > commitIndex_) {
        applyConfigureChangeNoLock_(ccMgr_.preparedIndex);
        if (ccMgr_.needNotify != 1) ccMgr_.clear();
      }
      easy_warn_log(
          "Server %d : Follower commitIndex change from %ld to %ld during "
          "onLeaderCommand\n",
          localServer_->serverId, commitIndex_.load(), msg->commitindex());
      commitIndex_ = msg->commitindex();
      assert(commitIndex_ <= log_->getLastLogIndex());
      /* notify waitCommitIndexUpdate */
      cond_.notify_all();
    }
    if (msg->lastlogindex() == log_->getLastLogIndex() &&
        msg->lastlogindex() == commitIndex_) {
      rsp->set_issuccess(true);
      lock_.unlock();
      requestVote();
    } else {
      rsp->set_issuccess(false);
      lock_.unlock();
    }
  } else if (msg->lctype() == PurgeLog) {
    /* check for purge log */
    easy_warn_log("Server %d : prepare to purge log, minMatchIndex %ld \n",
                  localServer_->serverId, msg->minmatchindex());
    purgeLogQueue_.push(new purgeLogArgType(msg->minmatchindex(), this));
    srv_->sendAsyncEvent(&SingleProcessQueue<purgeLogArgType>::process,
                         &purgeLogQueue_, Paxos::doPurgeLog);
    rsp->set_issuccess(true);
    lock_.unlock();
  }

  easy_warn_log(
      "Server %d : msgId(%llu) receive leaderCommand from server(%ld), "
      "currentTerm(%ld), lli(%ld), issuccess(%d)\n",
      localServer_->serverId, msg->msgid(), msg->serverid(),
      currentTerm_.load(), log_->getLastLogIndex(), rsp->issuccess());

  return 0;
}
int Paxos::leaderCommand(LcTypeT type, std::shared_ptr<RemoteServer> server) {
  /* We call from internal now. */
  // std::lock_guard<std::mutex> lg(lock_);

  /* just ensure only leader do leaderCommand */
  if (state_ != LEADER) {
    return -1;
  }

  PaxosMsg msg;
  msg.set_term(currentTerm_);
  msg.set_msgtype(LeaderCommand);
  msg.set_serverid(localServer_->serverId);
  msg.set_lctype(type);

  ++(stats_.countLeaderCommand);

  if (type == LeaderTransfer) {
    easy_system_log(
        "Server %d : leaderCommand(LeaderTransfer) to server(%ld), "
        "currentTerm(%ld), lli(%llu)\n",
        localServer_->serverId, server->serverId, currentTerm_.load(),
        log_->getLastLogIndex());
    assert(commitIndex_ == log_->getLastLogIndex() &&
           commitIndex_ == server->matchIndex);
    msg.set_lastlogindex(log_->getLastLogIndex());
    msg.set_commitindex(commitIndex_);
  } else if (type == PurgeLog) {
    easy_warn_log("Server %d : leaderCommand(PurgeLog) to all followers\n",
                  localServer_->serverId);
    /* broadcast minMatchIndex for purging log */
    msg.set_minmatchindex(minMatchIndex_);
  }

  if (server != nullptr)
    server->sendMsg((void *)&msg);
  else
    config_->forEach(&Server::sendMsg, (void *)&msg);

  if (debugWitnessTest)
    config_->forEachLearners(&Server::sendMsg, (void *)&msg);

  return 0;
}
int Paxos::onLeaderCommandResponce(PaxosMsg *msg) {
  easy_warn_log(
      "Server %d : msgId(%llu) receive leaderCommandResponce from server(%ld), "
      "currentTerm(%ld), lli(%llu)\n",
      localServer_->serverId, msg->msgid(), msg->serverid(),
      currentTerm_.load(), log_->getLastLogIndex());
  return 0;
}

int Paxos::forceSingleLeader() {
  std::lock_guard<std::mutex> lg(lock_);
  if (state_.load() == LEARNER) {
    easy_error_log("Server %d : Execute forceSingleLeader for this learner!!",
                   localServer_->serverId);

    localServer_->serverId = 1;
    changeState_(FOLLOWER);
    config_->delAllLearners();
  } else {
    easy_error_log("Server %d : Execute forceSingleLeader for this server!!",
                   localServer_->serverId);

    config_->delAllRemoteServer(localServer_->strAddr, this);
  }

  log_->setMetaData(Paxos::keyLearnerConfigure, config_->learnersToString());
  log_->setMetaData(Paxos::keyMemberConfigure,
                    config_->membersToString(localServer_->strAddr));

  srv_->sendAsyncEvent(&Paxos::requestVote, this, true);

  return 0;
}

int Paxos::forceSingleLearner() {
  std::lock_guard<std::mutex> lg(lock_);
  if (state_.load() == LEARNER) {
    easy_error_log(
        "Server %d : Execute forceSingleLearner failed because this is already "
        "a learner!!",
        localServer_->serverId);
    return 1;
  } else {
    easy_error_log("Server %d : Execute forceSingleLearner for this server!!",
                   localServer_->serverId);

    config_->delAllLearners();
    config_->delAllRemoteServer(localServer_->strAddr, this);
  }

  log_->setMetaData(Paxos::keyLearnerConfigure, config_->membersToString());
  log_->setMetaData(Paxos::keyMemberConfigure, localServer_->strAddr);

  changeState_(LEARNER);
  electionTimer_->stop();
  localServer_->serverId += 100;

  return 0;
}

int Paxos::forcePromote() {
  /* send requestVote request immediately to try to become a leader */
  std::lock_guard<std::mutex> lg(lock_);
  srv_->sendAsyncEvent(&Paxos::requestVote, this, true);
  return 0;
}

uint64_t Paxos::waitCommitIndexUpdate(uint64_t baseIndex, uint64_t term) {
  std::unique_lock<std::mutex> ul(lock_);

  if (term != 0 && currentTerm_ != term) return 0;
  /* TODO maybe we can signal cond_ only when it is need by a min heap of
   * baseIndex. */
  while (commitIndex_ <= baseIndex && (term == 0 || currentTerm_ == term) &&
         !shutdown_.load() &&
         (state_ != LEADER || !consensusAsync_.load() ||
          localServer_->lastSyncedIndex.load() <= baseIndex))
    cond_.wait(ul);

  if (term != 0 && currentTerm_ != term) return 0;

  return (state_ == LEADER && consensusAsync_.load())
             ? localServer_->lastSyncedIndex.load()
             : commitIndex_.load();
}

uint64_t Paxos::checkCommitIndex(uint64_t baseIndex, uint64_t term) {
  uint64_t ret = 0;
  /* should call the blocking interface (waitCommitIndexUpdate)
     if term is 0 */
  if (term == 0) return 0;
  /* double check term to make sure we have a valid commitIndex */
  if (currentTerm_ != term) return 0;
  ret = getCommitIndex();
  /* double check term & check shutdown */
  if (currentTerm_ != term || shutdown_.load()) return 0;
  return ret;
}

void Paxos::newTerm(uint64_t newTerm) {
  if (state_ == LEADER) {
    leaderStepDowning_.store(true);
    easy_system_log(
        "Server %d : new term(old:%ld,new:%ld), This is a Leader Step Down!!\n",
        localServer_->serverId, currentTerm_.load(), newTerm);
    log_->setMetaData(keyLastLeaderTerm, currentTerm_);
    log_->setMetaData(keyLastLeaderLogIndex, commitIndex_);
    if (ccMgr_.autoChangeAddr != "") {
      ccMgr_.autoChangeAddr = "";
      ccMgr_.autoChangeRet = -1;
      ccMgr_.condChangeDone.notify_all();
    }
  } else
    easy_system_log("Server %d : new term(old:%ld,new:%ld) !!\n",
                   localServer_->serverId, currentTerm_.load(), newTerm);
  currentTerm_.store(newTerm);
  log_->setTerm(currentTerm_);
  log_->setMetaData(keyCurrentTerm, currentTerm_);
  leaderId_.store(0);
  leaderAddr_ = std::string("");
  option.extraStore->setRemote("");
  votedFor_ = 0;
  log_->setMetaData(keyVoteFor, votedFor_);
  if (state_ != LEARNER) {
    changeState_(FOLLOWER);
    // electionTimer_->restart(electionTimeout_, true);
  } else {
    changeState_(LEARNER);
  }
  leaderStepDowning_.store(false);

  logRecvCache_.clear();

  /* TODO only step down when we are Leader */
  config_->forEach(&Server::stepDown, NULL);
  config_->forEachLearners(&Server::stepDown, NULL);
  config_->forEachLearners(&Server::disconnect, NULL);
  epochTimer_->stop();
}

uint64_t Paxos::appendLogFillForEachAsync(PaxosMsg *msg, RemoteServer *server,
                                          LogFillModeT mode) {
  std::lock_guard<std::mutex> lg(lock_);

  if (currentTerm_ != msg->term()) {
    easy_warn_log(
        "Server %d : skip sendMsg async, because term has already changed "
        "target(%llu), now(%llu)\n",
        localServer_->serverId, msg->term(), currentTerm_.load());
    return 0;
  }

  return appendLogFillForEach(msg, server, mode);
}

uint64_t Paxos::appendLogFillForEach(PaxosMsg *msg, RemoteServer *server,
                                     LogFillModeT mode) {
  /* There is no need to lock in this function. */
  uint64_t prevLogTerm;
  uint64_t nextIndex = server->nextIndex;
  uint64_t prevLogIndex = nextIndex - 1;
  uint64_t lastLogIndex = replicateWithCacheLog_.load()
                              ? log_->getLastCachedLogIndex()
                              : log_->getLastLogIndex();
  uint64_t size = 0;

  easy_info_log(
      "Server %d : server %d 's appendLogFillForEach nextIndex %llu, prevLogIndex %llu, "
      "lastLogIndex %llu, msg->msgid(%llu), msg->entries_size(%llu), msg->commitindex(%llu) \n",
      localServer_->serverId, server->serverId, nextIndex, prevLogIndex,
      lastLogIndex, msg->msgid(), msg->entries_size(), msg->commitindex());

  if (cdrMgr_.inRecovery) {
    easy_warn_log(
        "Server %d : fill nothing to msg during commit dependency recovery.\n",
        localServer_->serverId);
    return size; /* size is 0 */
  }
  if (prevLogIndex > lastLogIndex) {
    easy_warn_log(
        "Server %d : server %d 's prevLogIndex %ld larger than lastLogIndex "
        "%ld. Just ignore.\n",
        localServer_->serverId, server->serverId, prevLogIndex, lastLogIndex);
    return size; /* size is 0 */
  }
  assert(prevLogIndex <= lastLogIndex);

  if (prevLogIndex > 0) {
    LogEntry entry;
    if (0 != log_->getEntry(prevLogIndex, entry, true, server->serverId)) {
      easy_warn_log(
          "Server %d :getEntry fail for prevLogIndex(%ld) in Fill AppendLog to "
          "server %d\n",
          localServer_->serverId, prevLogIndex, msg->serverid());
      return size;
    }
    prevLogTerm = entry.term();
  } else
    prevLogTerm = 0;

  if (server->needAddr) {
    msg->set_addr(localServer_->strAddr);
    msg->set_extra(option.extraStore->getLocal());
    server->needAddr = false;
  }
  msg->set_prevlogindex(prevLogIndex);
  msg->set_prevlogterm(prevLogTerm);
  msg->set_nocache(true);

  /* We reuse msg here, so there may some entries exist. (need to send to other
   * follower) */
  if (msg->entries_size() != 0) msg->mutable_entries()->Clear();
  /* We reuse msg here, since there may exists a compression. (need to send to
   * other follower) */
  if (msg->has_compressedentries()) msg->clear_compressedentries();

  /* try to use appliedIndex instead of commitIndex for learner */
  uint64_t lastSendLogIndex = lastLogIndex;
  if (server->isLearner) {
    if (!server->sendByAppliedIndex)
      lastSendLogIndex = commitIndex_;
    else
      lastSendLogIndex = appliedIndex_.load();
  }

  /* For debug */
  if (debugMaxSendLogIndex != 0) {
    lastSendLogIndex = (lastSendLogIndex > debugMaxSendLogIndex)
                           ? debugMaxSendLogIndex
                           : lastSendLogIndex;
  }

  uint64_t maxPacketSize = maxPacketSize_;
  if (mode == LargeBatchMode) maxPacketSize *= largeBatchRatio_;
  if (lastSendLogIndex >= nextIndex) {
    LogEntry entry;
    ::google::protobuf::RepeatedPtrField<LogEntry> *entries;
    entries = msg->mutable_entries();

    uint64_t lastIndex = 0;
    uint64_t lastInfo = 0;
    for (uint64_t i = nextIndex; i <= lastSendLogIndex; ++i) {
      if (0 != log_->getEntry(i, entry, true, server->serverId)) {
        easy_warn_log(
            "Server %d :getEntry fail for entries(i:%ld) in Fill AppendLog to "
            "server %d\n",
            localServer_->serverId, i, msg->serverid());
        break;
      }
      log_->putLogMeta(entry.index(), entry.term(), entry.optype(),
                       entry.info());
      assert(entry.index() == i);
      if (entry.optype() == kMock) {
        easy_error_log(
            "Server %d : read mock log(index:%llu) when send to server %d, the "
            "configure of mock index may error or may hit bug!!",
            localServer_->serverId, i, server->serverId);
        break;
      }

      /*
       *  Restriction from X-Cluster, possible info values:
       *  1. FLAG_GU1 = 0x01, needGroup
       *  2. FLAG_GU2 = 0x02, needGroup
       *  3. FLAG_LARGE_TRX = 0x04, do not care
       *  4. FLAG_LARGE_TRX_END = 0x08, do not care
       */
      bool needGroup = false;
      if ((lastInfo == 1 || lastInfo == 2) && lastInfo == entry.info())
        needGroup = true;
      if (entry.has_info())
        lastInfo = entry.info();
      else
        lastInfo = 0;

      auto entrySize = entry.ByteSize();
      if (size + entrySize >= maxPacketSize && size != 0 && !needGroup) break;

      if (size + entrySize >= maxSystemPacketSize_) {
        if (size != 0) {
          easy_warn_log(
              "Server %d : truncate the sending msg, because it may exceed "
              "system max packet size (current size:%llu, add size:%llu)",
              localServer_->serverId, size, entrySize);
          break;
        } else {
          easy_warn_log(
              "Server %d : force send a msg, it may exceed system max packet "
              "size (current size:%llu, add size:%llu)",
              localServer_->serverId, size, entrySize);
        }
      }

      *(entries->Add()) = entry;
      lastIndex = i;
      size += entrySize;
      /* packet size may exceed maxPacketSize a little bit. */
      if (mode == EmptyMode) {
        /* XXX in EmptyMode we send 1 entty */
        break;
      }
    }

    /*
     * We enable pipelining in two cases:
     * 1. in a new term, a server has not matched once.
     * 2. the server is learner (because the learner may change its local log
     * pos in one term.)
     */
    if ((!server->isLearner || enableLearnerPipelining_) &&
        server->matchIndex != 0 && mode == NormalMode && lastIndex != 0) {
      server->nextIndex = lastIndex + 1;
      msg->set_nocache(false);
      easy_warn_log(
          "Server %d : update server %d 's nextIndex(old:%llu,new:%llu)\n",
          localServer_->serverId, server->serverId, nextIndex,
          server->nextIndex.load());
    }
  }

  msg->set_commitindex(
      std::min(msg->commitindex(), prevLogIndex + msg->entries_size()));

  easy_info_log("Server %d : server %d 's appendLogFillForEach end nextIndex %llu, "
      "prevLogIndex %llu, lastLogIndex %llu, msg->msgid(%llu), "
      "msg->entries_size(%llu), msg->commitindex(%llu)\n",
      localServer_->serverId, server->serverId, nextIndex, prevLogIndex,
      lastLogIndex, msg->msgid(), msg->entries_size(), msg->commitindex());
  return size;
}

int Paxos::tryUpdateCommitIndex() {
  std::lock_guard<std::mutex> lg(lock_);

  int ret = tryUpdateCommitIndex_();

  // if (ret == 0)
  // appendLog(false);

  return ret;
}

int Paxos::tryUpdateCommitIndex_() {
  if (state_ != LEADER) return -1;
  if (shutdown_.load()) return -1;

  uint64_t newCommitIndex = config_->quorumMin(&Server::getMatchIndex);
  uint64_t forceCommitIndex = config_->forceMin(&Server::getMatchIndex);
  if (forceCommitIndex < newCommitIndex && leaderForceSyncStatus_.load())
    newCommitIndex = forceCommitIndex;

  easy_info_log("Server %d : tryUpdateCommitIndex_ newCommitIndex %llu, "
                "forceCommitIndex %llu, commitIndex %llu, LastLogIndex %llu, "
                "lastNonCommitDepIndex %llu.\n",
      localServer_->serverId, newCommitIndex, forceCommitIndex,
      commitIndex_.load(), log_->getLastLogIndex(),
      cdrMgr_.lastNonCommitDepIndex.load());

  if (commitIndex_ >= newCommitIndex) return -1;

  // in case leader does not write log to disk, unlikely to happen
  if (newCommitIndex > log_->getLastLogIndex()) return -1;

  // if async mode, skip log check
  if (!consensusAsync_) {
    uint64_t term = 0, optype = 0, info = 0;
    if (log_->getLogMeta(newCommitIndex, &term, &optype, &info)) return -1;

    /* XXX leader don't commit for other term. */
    if (term != currentTerm_) return -1;

    /* commit dependency case */
    if (optype == kCommitDep) {
      easy_warn_log(
          "Server %d : index %ld is kCommitDep, check lastNonCommitDepIndex "
          "%llu.\n",
          localServer_->serverId, newCommitIndex,
          cdrMgr_.lastNonCommitDepIndex.load());
      if (cdrMgr_.lastNonCommitDepIndex > newCommitIndex)
        return -1;
      else
        newCommitIndex = cdrMgr_.lastNonCommitDepIndex;
    }
  }

  if (commitIndex_ >= newCommitIndex) return -1;

  if (ccMgr_.prepared && ccMgr_.preparedIndex <= newCommitIndex &&
      ccMgr_.preparedIndex > commitIndex_) {
    applyConfigureChangeNoLock_(ccMgr_.preparedIndex);
    /*
     * Case: we prepare a change when we're follower, and we apply when we're
     * leader. In this case, we should clear ccMgr info.
     */
    if (ccMgr_.needNotify != 1) ccMgr_.clear();
  }

  easy_warn_log("Server %d : Leader commitIndex change from %ld to %ld\n",
                localServer_->serverId, commitIndex_.load(), newCommitIndex);
  commitIndex_ = newCommitIndex;

  /* already hold the lock_ by the caller. */
  cond_.notify_all();

  appendLogToLearner();
  return 0;
}

/* TODO should read from config file or cmd line */
int Paxos::init(const std::vector<std::string> &strConfig /*start 0*/,
                uint64_t current /*start 1*/, uint64_t myServerId,
                ClientService *cs,
                uint64_t ioThreadCnt, uint64_t workThreadCnt,
                std::shared_ptr<LocalServer> localServer,
                bool memory_usage_count, uint64_t heartbeatThreadCnt) {
  // set new seed for auto leader transfer
  srand(time(0));

  bool needSetMeta = false;
  /* Init persistent variables */
  uint64_t itmp;
  if (!log_->getMetaData(std::string(keyClusterId), &itmp)) {
    clusterId_.store(itmp);
  }

  myServerId_.store(myServerId);

  if (!log_->getMetaData(std::string(keyCurrentTerm), &itmp)) {
    currentTerm_ = itmp;
    log_->setTerm(currentTerm_);
  }

  if (!log_->getMetaData(std::string(keyVoteFor), &itmp)) votedFor_ = itmp;

  /* Init members and learners */
  std::string config;
  log_->getMetaData(std::string(keyMemberConfigure), config);

  uint64_t metaCurrent = 0;
  std::vector<std::string> strMembers =
      StableConfiguration::stringToVector(config, metaCurrent);
  // TODO check strConfig and strMembers is equal

  const std::vector<std::string> *pConfig = NULL;
  uint64_t index;
  if (strConfig.size() == 0) {
    if (metaCurrent == 0) {
      easy_error_log(
          "Paxos::init: Can't find metaCurrent in MemberConfigure when init a "
          "follower node, there may have some error in meta, or this may be a "
          "learner!!");
      assert(0);
      return -1;
    }
    pConfig = &strMembers;
    index = metaCurrent;
  } else {
    pConfig = &strConfig;
    index = current;
    /* We init from the arg(not the meta), so we should set the meta after init
     * the configure! */
    needSetMeta = true;
  }

  config.clear();
  log_->getMetaData(std::string(keyLearnerConfigure), config);
  std::vector<std::string> strLearners =
      StableConfiguration::stringToVector(config, metaCurrent);

  /* Search logs and init ccMgr if any configurechange has not been applied. */
  uint64_t startScanIndex = 0;
  uint64_t lastLogIndex = log_->getLastLogIndex();
  log_->getMetaData(keyScanIndex, &startScanIndex);

  if (startScanIndex != 0) {
    if (startScanIndex > lastLogIndex) {
      /* We have not write the configure change logentry into the log, skip the
       * scan. */
      log_->setMetaData(keyScanIndex, 0);
    } else {
      easy_system_log(
          "Server %d : Start scan log on startup from %llu to %llu for "
          "uncommit configure change log entries.\n",
          index, startScanIndex, lastLogIndex);
      for (uint64_t i = startScanIndex; i <= lastLogIndex; ++i) {
        LogEntry entry;
        if (log_->getEntry(i, entry, false)) {
          easy_error_log("Fail to get log on startup, index %llu", i);
          exit(1);
        }
        if (entry.optype() == kConfigureChange) {
          if (ccMgr_.prepared == 0) {
            ccMgr_.prepared = 1;
            ccMgr_.preparedIndex = entry.index();
          } else {
            easy_error_log(
                "Server %d : Scan log on startup find more than 1 uncommit "
                "configure change entries!!\n",
                index);
          }
        }
      }
    }

    /* Check if the start scan index have already been clean up or hit some bug.
     */
    log_->getMetaData(keyScanIndex, &startScanIndex);
    if (startScanIndex != 0) {
      easy_error_log(
          "Server %d : startScanIndex(%llu) does not been clean up after scan "
          "log, may hit a bug!! We clean it now!!\n",
          index);
      log_->setMetaData(keyScanIndex, 0);
    }
  }

  log_->initMetaCache();

  /* Init Service */
  srv_ = std::make_shared<Service>(this);
  if (cs) srv_->cs = cs;

  srv_->init(ioThreadCnt, workThreadCnt, heartbeatTimeout_, memory_usage_count,
             heartbeatThreadCnt, threadHook);

  std::string curConfig = (*pConfig)[index - 1];
  /* Host format: [ipv6]:port, ipv4:port, we find the last ':' */
  auto pos = curConfig.find_last_of(':');
  host_ = curConfig.substr(0, pos);
  port_ = std::stoull(curConfig.substr(pos + 1));
  int error = 0;
  if ((error = srv_->start(port_))) {
    easy_error_log("Fail to start libeasy service, error(%d).", error);
    abort();
  }

  electionTimer_ = std::make_shared<ThreadTimer>(
      srv_->getThreadTimerService(), srv_, electionTimeout_, ThreadTimer::Stage,
      &Paxos::startElectionCallback, this);
  electionTimer_->start();
  epochTimer_ = std::make_shared<ThreadTimer>(
      srv_->getThreadTimerService(), srv_, electionTimeout_,
      ThreadTimer::Repeatable, &Paxos::epochTimerCallback, this);
  purgeLogTimer_ = std::make_shared<ThreadTimer>(
      srv_->getThreadTimerService(), srv_, purgeLogTimeout_,
      ThreadTimer::Repeatable, &Paxos::purgeLogCallback, this);

  /*
   * TODO: we should 1. start libeasy  2. installConfig  3. requestVote
   */

  /* Init Configuration */
  std::dynamic_pointer_cast<StableConfiguration>(config_)->installConfig(
      (*pConfig), index, this, localServer);
  config_->forEach(&Server::connect, (void *)NULL);
  config_->addLearners(strLearners, this, true);

  if (needSetMeta) {
    log_->setMetaData(Paxos::keyLearnerConfigure, config_->learnersToString());
    log_->setMetaData(Paxos::keyMemberConfigure,
                      config_->membersToString(localServer_->strAddr));
  }

  return 0;
}

int Paxos::initAsLearner(std::string &strConfig, uint64_t myServerId,
                         ClientService *cs,
                         uint64_t ioThreadCnt, uint64_t workThreadCnt,
                         std::shared_ptr<LocalServer> localServer,
                         bool memory_usage_count, uint64_t heartbeatThreadCnt) {
  // set new seed for auto leader transfer
  srand(time(0));

  bool needSetMeta = false;
  state_ = LEARNER;

  easy_warn_log("Start init node as a learner.");

  /* Init persistent variables */
  uint64_t itmp;
  if (!log_->getMetaData(std::string(keyClusterId), &itmp)) {
    clusterId_.store(itmp);
  }

  myServerId_.store(myServerId);

  if (!log_->getMetaData(std::string(keyCurrentTerm), &itmp)) {
    currentTerm_ = itmp;
    log_->setTerm(currentTerm_);
  }

  if (!log_->getMetaData(std::string(keyVoteFor), &itmp)) votedFor_ = itmp;

  std::vector<std::string> strLearners;
  std::string strMember;
  std::string config;
  if (strConfig.size() == 0) {
    /* Init members and learners */
    /**
     * Old learner node format:
     *   keyMemberConfigure: ""
     *   keyLearnerConfigure: "<local ip:port>"
     * New learner node format:
     *   keyMemberConfigure: "<local ip:port>"
     *   keyLearnerConfigure: "all learners in the cluster"
     **/
    log_->getMetaData(std::string(keyMemberConfigure), config);

    if (config.size() > 0) {
      /* new format */
      uint64_t metaCurrent;
      strMember = config;

      config.clear();
      log_->getMetaData(std::string(keyLearnerConfigure), config);
      strLearners = StableConfiguration::stringToVector(config, metaCurrent);

    } else {
      /* old format */
      log_->getMetaData(std::string(keyLearnerConfigure), strMember);
      needSetMeta = true;
    }
  } else {
    strMember = strConfig;
    needSetMeta = true;
  }

  log_->initMetaCache();

  /* Init Service */
  srv_ = std::shared_ptr<Service>(new Service(this));
  if (cs) srv_->cs = cs;

  srv_->init(ioThreadCnt, workThreadCnt, heartbeatTimeout_, memory_usage_count,
             heartbeatThreadCnt, threadHook);
  electionTimer_ = std::make_shared<ThreadTimer>(
      srv_->getThreadTimerService(), srv_, electionTimeout_, ThreadTimer::Stage,
      &Paxos::startElectionCallback, this);
  epochTimer_ = std::make_shared<ThreadTimer>(
      srv_->getThreadTimerService(), srv_, electionTimeout_,
      ThreadTimer::Repeatable, &Paxos::epochTimerCallback, this);
  purgeLogTimer_ = std::make_shared<ThreadTimer>(
      srv_->getThreadTimerService(), srv_, purgeLogTimeout_,
      ThreadTimer::Repeatable, &Paxos::purgeLogCallback, this);

  const std::string &curConfig = strMember;
  /* Host format: [ipv6]:port, ipv4:port, we find the last ':' */
  auto pos = curConfig.find_last_of(':');
  host_ = curConfig.substr(0, pos);
  port_ = std::stoull(curConfig.substr(pos + 1));
  int error = 0;
  if ((error = srv_->start(port_))) {
    easy_error_log("Fail to start libeasy service on port(%u), error(%d).", port_, error);
    abort();
  }

  /*
   * TODO: we should 1. start libeasy  2. installConfig  3. requestVote
   */

  /* Init Configuration */
  std::vector<std::string> tmpConfig;
  tmpConfig.push_back(strMember);
  std::dynamic_pointer_cast<StableConfiguration>(config_)->installConfig(
      tmpConfig, 1, this, localServer);
  localServer_->serverId += 100;

  /* Learner has all other learner's info now. */
  // config_->forEachLearners(&Server::connect, (void *)NULL);
  config_->addLearners(strLearners, this, true);

  if (needSetMeta) {
    log_->setMetaData(Paxos::keyLearnerConfigure, config_->learnersToString());
    log_->setMetaData(Paxos::keyMemberConfigure, config_->membersToString());
  }
  return 0;
}

void Paxos::msleep(uint64_t t) {
  struct timeval sleeptime;
  if (t == 0) return;
  sleeptime.tv_sec = t / 1000;
  sleeptime.tv_usec = (t - (sleeptime.tv_sec * 1000)) * 1000;
  select(0, 0, 0, 0, &sleeptime);
}

void Paxos::startElectionCallback() {
  easy_warn_log("Server %d : Enter startElectionCallback\n",
                localServer_->serverId);
  requestVote(false);
}

void Paxos::heartbeatCallback(std::weak_ptr<RemoteServer> wserver) {
  std::shared_ptr<RemoteServer> server;
  if (!(server = wserver.lock())) return;

  Paxos *paxos = server->paxos;

  easy_warn_log("Server %d : send heartbeat msg to server %ld\n",
                paxos->getLocalServer()->serverId, server->serverId);
  paxos->appendLogToServer(wserver, true, true);
}

uint64_t Paxos::getLeaderTransferInterval_() {
  return (electionTimeout_ / 5) + 100;
}

uint64_t Paxos::getNextEpochCheckStatemachine_(uint64_t epoch) {
  if (option.enableAutoLeaderTransfer_)
    return epoch +
           std::max((uint64_t)5, (option.autoLeaderTransferCheckSeconds_ *
                                  1000 / electionTimeout_));
  else
    return UINT64_MAX;
}

// paxos mutex protected
uint64_t Paxos::leaderTransferIfNecessary_(uint64_t epoch) {
  bool run = false;
  std::string reason;
  uint64_t target;
  if (!option.enableAutoLeaderTransfer_.load() || state_ != LEADER ||
      subState_ == SubLeaderTransfer) {
    return 0;
  }

  if (localServer_->logType) {
    run = true;
    reason = "instance is log node";
  } else if (nextEpochCheckStatemachine_ != UINT64_MAX) {
    if (log_->isStateMachineHealthy()) {
      nextEpochCheckStatemachine_ = UINT64_MAX;
    } else if (epoch >= nextEpochCheckStatemachine_) {
      run = true;
      reason = "state machine not healthy";
      nextEpochCheckStatemachine_ = getNextEpochCheckStatemachine_(epoch);
    }
  }

  if (!run) {
    return 0;
  }

  run = false;

  auto servers = config_->getServers();
  std::vector<uint64_t> choices;
  for (auto &e : servers) {
    if (e == nullptr || e->serverId == localServer_->serverId) continue;
    std::shared_ptr<RemoteServer> server =
        std::dynamic_pointer_cast<RemoteServer>(e);
    if (server->electionWeight >= localServer_->electionWeight &&
        server->getLastAckEpoch() >= epoch) {
      run = true;
      choices.push_back(server->serverId);
    }
  }

  if (!run) {
    return 0;
  }

  target = choices[rand() % choices.size()];
  easy_system_log(
      "Server %d: try to do an auto leader transfer, reason: %s, target: %llu",
      localServer_->serverId, reason.c_str(), target);

  return target;
}

void Paxos::epochTimerCallback() {
  std::unique_lock<std::mutex> ul(lock_);
  if (state_ != LEADER && state_ != CANDIDATE) {
    epochTimer_->stop();
    return;
  }

  if (state_ == CANDIDATE) {
    /* When we're candidate we only calculate the epoch. */
    easy_warn_log("Server %d : Epoch task currentEpoch(%llu)\n",
                  localServer_->serverId, currentEpoch_.load());
    currentEpoch_.fetch_add(1);
    return;
  }
  uint64_t forceMinEpoch = config_->forceMin(&Server::getLastAckEpoch);
  uint64_t quorumEpoch = config_->quorumMin(&Server::getLastAckEpoch);

  easy_warn_log(
      "Server %d : Epoch task currentEpoch(%llu) quorumEpoch(%llu) "
      "forceMinEpoch(%llu)\n",
      localServer_->serverId, currentEpoch_.load(), quorumEpoch, forceMinEpoch);

  if (currentEpoch_.load() > (forceMinEpoch + forceSyncEpochDiff_)) {
    if (leaderForceSyncStatus_ == true) {
      leaderForceSyncStatus_.store(false);
      easy_warn_log(
          "Server %d : lost connect with force sync server, disable force sync "
          "now!\n",
          localServer_->serverId);
    }
  } else {
    if (leaderForceSyncStatus_ == false) {
      leaderForceSyncStatus_.store(true);
      easy_warn_log(
          "Server %d : reconnect with all force sync server, enable force sync "
          "now!\n",
          localServer_->serverId);
    }
  }

  if (currentEpoch_.load() > quorumEpoch) {
    /* Lost connect with major followers, we should step down. */
    easy_error_log(
        "Server %d : lost connect with major followers, stepdown myself\n",
        localServer_->serverId);
    if (debugDisableStepDown) {
      easy_warn_log(
          "Server %d : Skip step down because of debugDisableStepDown "
          "currentTerm(%ld)\n",
          localServer_->serverId, currentTerm_.load());
      return;
    }
    newTerm(currentTerm_ + 1);
    electionTimer_->start();
  } else {
    assert(currentEpoch_.load() == quorumEpoch);
    uint64_t prevEpoch = currentEpoch_.fetch_add(1);
    uint64_t target = leaderTransferIfNecessary_(prevEpoch);
    if (target) {
      subState_.store(SubLeaderTransfer);
      weightElecting_ = true;
      ul.unlock();
      /* try time should not exceed one epoch */
      uint64_t times = std::max(
          (electionTimeout_ / getLeaderTransferInterval_() + 1), (uint64_t)3);
      leaderTransferSend_(target, currentTerm_.load(), log_->getLastLogIndex(),
                          times);
    }
  }
}

int Paxos::initAutoPurgeLog(bool autoPurge, bool useAppliedIndex,
                            std::function<bool(const LogEntry &le)> handler) {
  autoPurge_ = autoPurge;
  if (!autoPurge_) purgeLogTimer_->stop();
  useAppliedIndex_ = useAppliedIndex;
  if (autoPurge && !useAppliedIndex) {
    easy_warn_log(
        "Server %d : use commitIndex instead of appliedIndex when auto purging "
        "log.",
        localServer_->serverId);
  }
  log_->setPurgeLogFilter(handler);
  return 0;
}

void Paxos::purgeLogCallback() {
  /* purge log without a forceIndex */
  forcePurgeLog(false /* local */);
}

void Paxos::doPurgeLog(purgeLogArgType *arg) {
  uint64_t purgeIndex;
  if (arg->paxos->useAppliedIndex_)
    purgeIndex = arg->index < arg->paxos->getAppliedIndex()
                     ? arg->index
                     : arg->paxos->getAppliedIndex();
  else
    purgeIndex = arg->index < arg->paxos->getCommitIndex()
                     ? arg->index
                     : arg->paxos->getCommitIndex();
  easy_warn_log("Server %d : doPurgeLog purge index %ld\n",
                arg->paxos->localServer_->serverId, purgeIndex);
  arg->paxos->getLog()->truncateForward(purgeIndex);
}

void Paxos::updateAppliedIndex(uint64_t index) { appliedIndex_.store(index); }

uint64_t Paxos::collectMinMatchIndex(std::vector<ClusterInfoType> &cis,
                                     bool local, uint64_t forceIndex) {
  uint64_t ret = forceIndex;
  /*
   * minMatchIndex Protection
   * 1. non local (only leader): all nodes matchIndex
   * 2. local & Leader: all nodes matchIndex
   * 3. local & not leader: all learner source from me
   */
  for (auto ci : cis) {
    if (ci.serverId == localServer_->serverId) continue;
    if (local == false || state_ == LEADER ||
        (ci.role == LEARNER && ci.learnerSource == localServer_->serverId))
      ret = ci.matchIndex < ret ? ci.matchIndex : ret;
  }
  uint64_t lastLogIndex = log_->getLastLogIndex();
  if (ret > lastLogIndex) ret = lastLogIndex;
  return ret;
}

int Paxos::forcePurgeLog(bool local, uint64_t forceIndex) {
  if (local == false && state_ != LEADER) {
    return -1;
  }
  /* update minMatchIndex_ */
  /* appendlog should take purge log information if minMatchIndex_ is not 0 */
  std::vector<Paxos::ClusterInfoType> cis;
  getClusterInfo(cis);
  if (cis.size() == 0) {
    return 0;
  }
  minMatchIndex_ = collectMinMatchIndex(cis, local, forceIndex);
  easy_warn_log(
      "Server %d : Prepare to purge log to %s, update minMatchIndex %ld\n",
      localServer_->serverId, local ? "local" : "cluster", minMatchIndex_);
  /* leader */
  purgeLogQueue_.push(new purgeLogArgType(minMatchIndex_, this));
  srv_->sendAsyncEvent(&SingleProcessQueue<purgeLogArgType>::process,
                       &purgeLogQueue_, Paxos::doPurgeLog);
  if (local == false) {
    /* follower */
    std::lock_guard<std::mutex> lg(lock_);
    return leaderCommand(PurgeLog, NULL);
  } else {
    return 0;
  }
}

void Paxos::electionWeightAction(uint64_t term, uint64_t baseEpoch) {
  easy_system_log("Server %d : electionWeightAction start, term:%llu epoch:%llu",
                 localServer_->serverId, term, baseEpoch);
  std::lock_guard<std::mutex> lg(lock_);
  if (term != currentTerm_.load() || state_.load() != LEADER) {
    subState_.store(SubNone);
    weightElecting_ = false;
    easy_error_log(
        "Server %d : electionWeightAction fail, action term(%llu), "
        "currentTerm(%llu), current state(%s)\n",
        localServer_->serverId, term, currentTerm_.load(), stateString[state_]);
    return;
  }

  uint64_t targetId = config_->getMaxWeightServerId(baseEpoch, localServer_);

  if (targetId != localServer_->serverId && targetId != 0) {
    auto term = currentTerm_.load();
    auto lli = log_->getLastLogIndex();

    easy_system_log(
        "Server %d : electionWeightAction try to transfer leader to server "
        "%llu, term(%llu)\n",
        localServer_->serverId, targetId, term);

    uint64_t retryTimes = 5;
    lock_.unlock();
    leaderTransferSend_(targetId, term, lli, retryTimes);
    lock_.lock();
  } else {
    subState_.store(SubNone);
    weightElecting_ = false;
    easy_system_log(
        "Server %d : electionWeightAction skip transfer leader because %s.\n",
        localServer_->serverId,
        targetId == 0 ? "no available server"
                      : "I am the max weight available server");
  }
}

void Paxos::resetNextIndexForServer(std::shared_ptr<RemoteServer> server) {
  std::lock_guard<std::mutex> lg(lock_);
  auto lastLogIndex = getLastLogIndex();
  /* make sure the first appendLog msg when reconnect have payload to
   * truncateForward. */
  if (lastLogIndex > 1) lastLogIndex -= 1;

  if (server->matchIndex.load() != 0)
    server->nextIndex.store(server->matchIndex.load() + 1);
  else if (server->isLearner && server->sendByAppliedIndex)
    server->nextIndex.store(appliedIndex_.load() + 1);
  else
    server->nextIndex.store(lastLogIndex);
}

bool Paxos::tryFillFollowerMeta_(
    ::google::protobuf::RepeatedPtrField<::alisql::ClusterInfoEntry>
        *ciEntries) {
  uint64_t localFollowerMetaNo = followerMetaNo_.fetch_add(0);
  if (localFollowerMetaNo > lastSyncMetaNo_ + syncMetaInterval_) {
    lastSyncMetaNo_ = localFollowerMetaNo;
    config_->forEachLearners(&Server::fillFollowerMeta, (void *)ciEntries);
  }
  return ciEntries->size() != 0;
}

void Paxos::setAlertLogLevel(AlertLogLevelType level) {
  easy_log_level = static_cast<easy_log_level_t>(level);
}

int Paxos::getClusterInfo(std::vector<ClusterInfoType> &cis) {
  cis.clear();

  config_->forEach(&Server::fillInfo, (void *)&cis);
  config_->forEachLearners(&Server::fillInfo, (void *)&cis);

  return 0;
}

int Paxos::getClusterHealthInfo(std::vector<HealthInfoType> &healthInfo) {
  std::lock_guard<std::mutex> lg(lock_);
  if (state_ != LEADER) return 1;

  uint64_t lastLogIndex = getLastLogIndex();
  uint64_t appliedIndex = appliedIndex_;
  std::vector<ClusterInfoType> cis;
  getClusterInfo(cis);

  for (auto &e : cis) {
    HealthInfoType hi;
    hi.serverId = e.serverId;
    hi.addr = e.ipPort;
    hi.role = e.role;
    if (e.serverId != localServer_->serverId) {
      std::shared_ptr<RemoteServer> server =
          std::dynamic_pointer_cast<RemoteServer>(
              config_->getServer(e.serverId));
      if (server) {
        hi.connected = !(server->lostConnect || server->netError);
      } else {
        hi.connected = false;
      }
    } else {
      hi.connected = true;
    }
    hi.logDelayNum =
        lastLogIndex > e.matchIndex ? lastLogIndex - e.matchIndex : 0;
    hi.applyDelayNum =
        appliedIndex > e.appliedIndex ? appliedIndex - e.appliedIndex : 0;
    healthInfo.push_back(hi);
  }

  return 0;
}

void Paxos::printClusterInfo(const std::vector<ClusterInfoType> &cis) {
  for (auto &ci : cis) {
    std::cout << "serverId:" << ci.serverId << " ipPort:" << ci.ipPort
              << " matchIndex:" << ci.matchIndex
              << " nextIndex:" << ci.nextIndex << " role:" << ci.role
              << " hasVoted:" << ci.hasVoted << " forceSync:" << ci.forceSync
              << " electionWeight:" << ci.electionWeight
              << " learnerSource:" << ci.learnerSource
              << " appliedIndex:" << ci.appliedIndex
              << " pipelining:" << ci.pipelining << std::endl
              << std::flush;
  }
}

void Paxos::getMemberInfo(MemberInfoType *mi) {
  mi->serverId = localServer_->serverId;
  mi->currentTerm = currentTerm_;
  mi->currentLeader = leaderId_;
  mi->commitIndex = commitIndex_;

  uint64_t lastLogIndex = log_->getLastLogIndex();
  LogEntry entry;
  uint64_t lastLogTerm = 0;
  if (log_->getEntry(lastLogIndex, entry, false) == 0)
    lastLogTerm = entry.term();

  mi->lastLogTerm = lastLogTerm;
  mi->lastLogIndex = lastLogIndex;
  if (weightElecting_.load() || leaderStepDowning_.load())
    mi->role = NOROLE;
  else
    mi->role = state_;
  mi->votedFor = votedFor_;
  mi->lastAppliedIndex = appliedIndex_.load();
  mi->currentLeaderAddr = leaderAddr_;
}

uint64_t Paxos::getServerIdFromAddr(const std::string &strAddr) {
  std::unique_lock<std::mutex> ul(lock_);
  return config_->getServerIdFromAddr(strAddr);
}

// override default compress option for address, if address is empty, override
// for all return 0 on success, 1 on failure
int Paxos::setMsgCompressOption(int type, size_t threshold, bool checksum,
                                const std::string &strAddr) {
  std::unique_lock<std::mutex> ul(lock_);
  MsgCompressOption option((MsgCompressionType)type, threshold, checksum);

  if (shutdown_.load() || config_ == nullptr) {
    easy_error_log("set MsgCompressOption fail, Paxos is stopped.\n");
    return 1;
  }

  if (strAddr == "") {
    config_->forEach(&Server::setMsgCompressOption, &option);
    config_->forEachLearners(&Server::setMsgCompressOption, &option);
  } else {
    uint64_t id = config_->getServerIdFromAddr(strAddr);
    Configuration::ServerRef server;
    if (id == 0 || ((server = config_->getServer(id)) == nullptr)) {
      easy_error_log(
          "Server %d : can't find server %s in setMsgCompressOption\n",
          localServer_->serverId, strAddr.c_str());
      return 1;
    }
    server->setMsgCompressOption(&option);
  }

  easy_warn_log(
      "set MsgCompressOption type(%d) threshold(%u) checksum(%d) to server(%s) "
      "succeed.\n",
      type, threshold, checksum, strAddr == "" ? "all" : strAddr.c_str());
  return 0;
}

int Paxos::resetMsgCompressOption() {
  return setMsgCompressOption(0 /* type */, 0 /* threshold */, 0 /* checksum */,
                              "");
}

int Paxos::setClusterId(uint64_t ci) {
  int ret = log_->setMetaData(std::string(keyClusterId), ci);
  if (ret == 0) clusterId_.store(ci);
  return ret;
}

void Paxos::setLearnerConnTimeout(uint64_t t) {
  if (t < (heartbeatTimeout_ / 4)) t = heartbeatTimeout_ / 4;
  easy_warn_log("Server %d : Learner connection timeout set to %llu.",
                localServer_->serverId, t);
  localServer_->learnerConnTimeout = t;
}

void Paxos::setSendPacketTimeout(uint64_t t) {
  if (t < heartbeatTimeout_) t = heartbeatTimeout_;
  easy_warn_log("Server %d : Send packet timeout set to %llu.",
                localServer_->serverId, t);
  srv_->setSendPacketTimeout(t);
}

void Paxos::forceFixMatchIndex(uint64_t targetId, uint64_t newIndex) {
  std::unique_lock<std::mutex> ul(lock_);
  if (state_ != LEADER || targetId == 0 || targetId == localServer_->serverId)
    return;
  std::shared_ptr<RemoteServer> server =
      std::dynamic_pointer_cast<RemoteServer>(config_->getServer(targetId));
  if (!server) {
    easy_warn_log("Server %d : can't find server %llu in forceFixMatchIndex\n",
                  localServer_->serverId, targetId);
    return;
  }
  easy_error_log(
      "Server %d : force fix server %d's matchIndex(old: %llu, new: %llu). "
      "Dangerous Operation!",
      localServer_->serverId, targetId, server->matchIndex.load(), newIndex);
  server->resetMatchIndex(newIndex);
}

void Paxos::forceFixMatchIndex(const std::string &addr, uint64_t newIndex) {
  std::unique_lock<std::mutex> ul(lock_);
  uint64_t targetId = config_->getServerIdFromAddr(addr);
  if (state_ != LEADER || targetId == 0 || targetId == localServer_->serverId)
    return;
  std::shared_ptr<RemoteServer> server =
      std::dynamic_pointer_cast<RemoteServer>(config_->getServer(targetId));
  if (!server) {
    easy_warn_log("Server %d : can't find server %llu in forceFixMatchIndex\n",
                  localServer_->serverId, targetId);
    return;
  }
  easy_error_log(
      "Server %d : force fix server %d's matchIndex(old: %llu, new: %llu). "
      "Dangerous Operation!",
      localServer_->serverId, targetId, server->matchIndex.load(), newIndex);
  server->resetMatchIndex(newIndex);
}

int Paxos::log_checksum_test(const LogEntry &le) {
  if (checksumCb_ && checksum_mode_ && le.checksum() != 0) {
    const unsigned char *buf =
        reinterpret_cast<const unsigned char *>(le.value().c_str());
    uint64_t cs = checksumCb_(0, buf, le.value().size());
    if (cs == le.checksum())
      return 0;
    else
      return -1;
  }
  return 0;
}

void Paxos::reset_flow_control() {
  std::unique_lock<std::mutex> ul(lock_);
  config_->reset_flow_control();
}

void Paxos::set_flow_control(uint64_t serverId, int64_t fc) {
  /*
   * flow control mode:
   * >0   TODO
   * 0    no flow control
   * -1   slow send log (send by heartbeat)
   * <-1  never send log
   */
  std::unique_lock<std::mutex> ul(lock_);
  config_->set_flow_control(serverId, fc);
}

void Paxos::truncateBackward_(uint64_t firstIndex) {
  if (ccMgr_.prepared && ccMgr_.preparedIndex >= firstIndex) {
    /*
     * 1. set aborted to 1 to notify client the configureChange is failed.
     * 2. reset preparedIndex to prevent apply configureChange before ccMgr_ is
     * cleared.
     * 3. prepared flag is kept to 1 to prevent concurrent configureChange until
     *    client is notified and ccMgr_ is cleared.
     */
    ccMgr_.aborted = 1;
    ccMgr_.preparedIndex = 0;
    ccMgr_.cond.notify_all();

    // if scan index < firstIndex, we do not reset, it will be reset when
    // applied; if scan index >= firstIndex, then the prepared configure change
    // will not be committed, we need to reset to 0.
    log_->setMetaData(keyScanIndex, 0);
  }
  log_->truncateBackward(firstIndex);
}

const std::string Paxos::keyCurrentTerm = "@keyCurrentTerm_@";
const std::string Paxos::keyVoteFor = "@keyVoteFor_@";
const std::string Paxos::keyLastLeaderTerm = "@keyLastLeaderTerm_@";
const std::string Paxos::keyLastLeaderLogIndex = "@keyLastLeaderLogIndex_@";
const std::string Paxos::keyMemberConfigure = "@keyMemberConfigure_@";
const std::string Paxos::keyLearnerConfigure = "@keyLearnerConfigure_@";
const std::string Paxos::keyScanIndex = "@keyScanIndex_@";
const std::string Paxos::keyClusterId = "@keyClusterId_@";
const uint64_t Paxos::maxSystemPacketSize_ = 50000000;

bool Paxos::debugDisableElection = false;
bool Paxos::debugDisableStepDown = false;
bool Paxos::debugWitnessTest = false;
bool Paxos::debugResetLogSlow = false;
bool Paxos::debugSkipUpdateCommitIndex = false;

}  // namespace alisql
