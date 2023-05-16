/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  configuration.cc,v 1.0 07/30/2016 04:52:09 PM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file configuration.cc
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 07/30/2016 04:52:09 PM
 * @version 1.0
 * @brief the implement of the configuration
 *
 **/

#include <assert.h>
#include <algorithm>
#include "paxos.h"
#include "paxos_configuration.h"

namespace alisql {

void StableConfiguration::forEach(const SideEffect &sideEffect, void *ptr) {
  for (auto &it : servers)
    if (it) sideEffect(*it, ptr);
}

void StableConfiguration::forEachLearners(const SideEffect &sideEffect,
                                          void *ptr) {
  for (auto &it : learners)
    if (it) sideEffect(*it, ptr);
}

bool StableConfiguration::quorumAll(const Predicate &predicate) const {
  if (servers.empty()) return true;
  uint64_t count = 0;
  for (auto &it : servers)
    if (it)
      if (predicate(*it)) ++count;
  return (count >= getServerNum() / 2 + 1);
}

uint64_t StableConfiguration::quorumMin(const GetValue &getValue) const {
  if (servers.empty()) return 0;
  std::vector<uint64_t> values;
  for (auto &it : servers)
    if (it) values.push_back(getValue(*it));
  std::sort(values.begin(), values.end());
  return values.at((values.size() - 1) / 2);
}

uint64_t StableConfiguration::forceMin(const GetValue &getValue) const {
  if (servers.empty()) return 0;
  uint64_t value = UINT64_MAX;
  for (auto &it : servers) {
    if (it && it->forceSync) {
      auto tmp = getValue(*it);
      if (tmp < value) {
        value = tmp;
      }
    }
  }
  return value;
}

uint64_t StableConfiguration::allMin(const GetValue &getValue) const {
  if (servers.empty()) return 0;
  uint64_t value = UINT64_MAX;
  for (auto &it : servers) {
    if (it) {
      auto tmp = getValue(*it);
      if (tmp < value) {
        value = tmp;
      }
    }
  }
  return value;
}

Configuration::ServerRef StableConfiguration::getServer(uint64_t serverId) {
  if (serverId == 0) return nullptr;

  if (serverId < 100) {
    serverId -= 1;
    // servers' id start from 1
    if (servers.size() > serverId)
      return servers[serverId];
    else
      return nullptr;
  } else  // getLearners
  {
    // learners' id start from 0
    /*
    serverId -= 100;
    if (learners.size() > serverId)
      return learners[serverId];
    else
      return nullptr;
      */
    for (auto &learner : learners) {
      if (learner && learner->serverId == serverId) return learner;
    }
    return nullptr;
  }
}

Configuration::ServerRef StableConfiguration::getLearnerByAddr(
    const std::string &addr) {
  for (auto &learner : learners) {
    if (learner && getAddr(learner->strAddr) == addr) return learner;
  }
  return nullptr;
}

uint64_t StableConfiguration::getServerIdFromAddr(const std::string &addr) {
  for (auto &server : servers) {
    if (server && getAddr(server->strAddr) == addr) return server->serverId;
  }
  for (auto &learner : learners) {
    if (learner && getAddr(learner->strAddr) == addr) return learner->serverId;
  }
  return 0;
}

void StableConfiguration::installConfig(
    const std::vector<std::string> &strConfig, uint64_t current, Paxos *paxos,
    std::shared_ptr<LocalServer> localServer) {
  uint64_t i = 0;
  std::shared_ptr<LocalServer> ptrL;
  std::shared_ptr<RemoteServer> ptrR;

  forEach(&Server::stop, NULL);
  servers.clear();
  serversNum.store(0);
  servers.resize(strConfig.size());

  for (auto &str : strConfig) {
    ++i;
    if (str == "0") {
      /* empty server */
      servers[i - 1] = nullptr;
      continue;
    }
    serversNum++;
    if (i != current) {
      servers[i - 1] = (ptrR = std::make_shared<RemoteServer>(i));
      initServerFromString(ptrR, str);
      ptrR->srv = paxos->getService();
      ptrR->paxos = paxos;
      ptrR->heartbeatTimer = std::unique_ptr<ThreadTimer>(
          new ThreadTimer(paxos->getService()->getThreadTimerService(),
                          paxos->getService(), paxos->getHeartbeatTimeout(),
                          ThreadTimer::Repeatable, Paxos::heartbeatCallback,
                          std::move(std::weak_ptr<RemoteServer>(ptrR))));
    } else {
      std::shared_ptr<LocalServer> tmpServer;
      tmpServer = localServer ? localServer : std::make_shared<LocalServer>(i);
      tmpServer->serverId = i;

      servers[i - 1] = (ptrL = tmpServer);
      initServerFromString(ptrL, str);
      paxos->setLocalServer(ptrL);
      ptrL->paxos = paxos;
    }
  }
  assert(i == strConfig.size());
}

std::string StableConfiguration::memberToString(ServerRef server) {
  std::string ret("");

  ret += server->strAddr;
  ret += "#";
  ret += server->electionWeight + '0';
  ret += server->forceSync ? "F" : "N";

  return ret;
}

std::string StableConfiguration::learnerToString(ServerRef server) {
  std::string ret("");

  ret += server->strAddr;
  ret += "$";
  ret += server->learnerSource / 10 + '0';
  ret += server->learnerSource % 10 + '0';

  return ret;
}

std::string StableConfiguration::configToString(std::vector<ServerRef> &servers,
                                                const std::string &localAddr,
                                                bool forceMember) {
  std::string ret("");
  uint64_t localIndex = 0, i = 1;

  for (auto &server : servers) {
    if (server != nullptr) {
      if (localAddr != "" || forceMember) {
        ret += memberToString(server);
      } else {
        ret += learnerToString(server);
      }
      ret += ";";
      if (localAddr != "" && server->strAddr == localAddr) localIndex = i;
    } else {
      ret += "0;";
    }
    ++i;
  }

  if (i != 1) {
    if (localAddr != "") {
      assert(localIndex != 0);

      ret[ret.size() - 1] = '@';
      ret += std::to_string(localIndex);
    } else {
      ret.resize(ret.size() - 1);
    }
  }

  return ret;
}

Configuration::ServerRef StableConfiguration::addServer(
    std::vector<ServerRef> &servers, ServerRef newServer, bool useAppend) {
  uint64_t id = 0;
  for (auto &it : servers) {
    ++id;
    if (!useAppend && it == nullptr) {
      it = newServer;
      newServer->serverId = id;
      return newServer;
    }
  }

  ++id;
  servers.push_back(newServer);
  newServer->serverId = id;
  return newServer;
}

void StableConfiguration::initServerFromString(ServerRef server,
                                               std::string str,
                                               bool isLearner) {
  auto size = str.size();
  uint64_t pos = 0;
  if (str.at(size - 3) == '#') {
    server->forceSync = (str.at(size - 1) == 'S');
    server->electionWeight = str.at(size - 2) - '0';
    str.resize(size - 3);
  } else if ((pos = str.find('$', 0)) != std::string::npos) {
    uint64_t tmpSource = str.at(pos + 1) - '0';
    tmpSource *= 10;
    tmpSource += str.at(pos + 2) - '0';
    str.resize(pos);

    server->learnerSource = tmpSource;
    server->forceSync = 0;
    server->electionWeight = 5;
  } else {
    server->forceSync = 0;
    if (isLearner)
      server->electionWeight = 0;
    else
      server->electionWeight = 5;
  }
  server->strAddr = str;
}

void StableConfiguration::initServerDefault(ServerRef serverArg) {
  auto server = std::dynamic_pointer_cast<RemoteServer>(serverArg);
  server->isLearner = false;
  server->forceSync = false;
  server->electionWeight = 5;
  server->appliedIndex = 0;
}

std::vector<std::string> StableConfiguration::stringToVector(
    const std::string &str, uint64_t &currentIndex) {
  /* Return obj has already been optimized in C++11. */
  /* 127.0.0.1:10001;127.0.0.1:10002;127.0.0.1:10003@1 */
  /* 127.0.0.1:10001#9N;127.0.0.1:10002#5N;127.0.0.1:10003#0N@1 */
  std::vector<std::string> ret;
  uint64_t start = 0, stop = 0;
  do {
    stop = str.find(';', start);
    if (stop == std::string::npos) break;
    /* If learner source is larger than 109, skip this ';', it represents 11. */
    if (stop > 1 && str[stop - 1] == '$') stop = str.find(';', stop + 1);
    if (stop == std::string::npos) break;
    ret.push_back(str.substr(start, stop - start));
    start = stop + 1;
  } while (stop != std::string::npos);

  stop = str.find('@', start);
  /* If learner source is larger than 159, skip this '@', it represents 16. */
  if (stop == std::string::npos || (stop > 1 && str[stop - 1] == '$')) {
    if (str.size() != 0) ret.push_back(str.substr(start, stop - start));
  } else {
    ret.push_back(str.substr(start, stop - start));
    start = stop + 1;

    currentIndex = std::stoull(str.substr(start));
  }

  return ret;
}

uint64_t StableConfiguration::getServerNum() const {
  uint64_t cnt = 0;
  for (auto &it : servers)
    if (it) ++cnt;
  return cnt;
}

uint64_t StableConfiguration::getLearnerNum() const {
  uint64_t cnt = 0;
  for (auto &it : learners)
    if (it) ++cnt;
  return cnt;
}

bool StableConfiguration::needWeightElection(uint64_t localWeight) {
  for (auto &it : servers) {
    if (it && it->electionWeight > localWeight) return true;
  }
  return false;
}

uint64_t StableConfiguration::getMaxWeightServerId(uint64_t baseEpoch,
                                                   ServerRef localServer) {
  uint64_t ret = localServer->serverId;
  uint64_t priv = localServer->electionWeight;
  for (auto &it : servers) {
    if (it == nullptr) continue;
    auto lastAckEpoch = it->getLastAckEpoch();
    if (it->electionWeight > priv && lastAckEpoch > baseEpoch) {
      ret = it->serverId;
      priv = it->electionWeight;
    }
  }
  return ret;
}

int StableConfiguration::addMember(const std::string &strAddr, Paxos *paxos) {
  std::string logBuf = "";
  for (auto &server : servers) {
    if (server) {
      logBuf += std::to_string(server->serverId);
      logBuf += ":";
      logBuf += server->strAddr;
      logBuf += " ";
    }
  }
  easy_warn_log(
      "Server %d : StableConfiguration::addMember: current servers(%s), add "
      "server(%s)\n",
      paxos->getLocalServer()->serverId, logBuf.c_str(), strAddr.c_str());
  for (auto it = learners.begin(); it != learners.end(); ++it)
    if (*it && (*it)->strAddr == strAddr) {
      auto server = std::dynamic_pointer_cast<RemoteServer>(*it);
      serversNum.fetch_add(1);
      addServer(servers, server);
      *it = nullptr;
      // servers.push_back(server);
      // learners.erase(it);
      // server->serverId= servers.size();
      initServerDefault(server);
      if (paxos->getState() == Paxos::LEADER) {
        server->beginLeadership((void *)1);
      } else
        server->stepDown(NULL);
      server->connect(NULL);
      logBuf = "";
      for (auto &server : servers) {
        if (server) {
          logBuf += std::to_string(server->serverId);
          logBuf += ":";
          logBuf += server->strAddr;
          logBuf += " ";
        }
      }
      while (learners.size() > 0 && learners[learners.size() - 1] == nullptr)
        learners.resize(learners.size() - 1);
      paxos->getLog()->setMetaData(Paxos::keyLearnerConfigure,
                                   learnersToString());
      paxos->getLog()->setMetaData(
          Paxos::keyMemberConfigure,
          membersToString(paxos->getLocalServer()->strAddr));
      easy_warn_log(
          "Server %d : StableConfiguration::addMember: success current "
          "servers(%s)\n",
          paxos->getLocalServer()->serverId, logBuf.c_str());
      return 0;
    }
  easy_warn_log(
      "Server %d : StableConfiguration::addMember: fail current servers(%s)\n",
      paxos->getLocalServer()->serverId, logBuf.c_str());
  return -1;
}

int StableConfiguration::delMember(const std::string &strAddr, Paxos *paxos) {
  int ret = -1;
  for (auto it = servers.begin(); it != servers.end(); ++it) {
    if (*it == nullptr) continue;
    if ((*it)->strAddr == strAddr) {
      auto serverId = (*it)->serverId;
      (*it)->stop(NULL);
      serversNum.fetch_sub(1);
      // servers.erase(it);
      *it = nullptr;
      ret = 0;
      /* recycle the last nullptr in the servers */
      if (serverId == servers.size() && servers.size() > 0) {
        while (servers[servers.size() - 1] == nullptr)
          servers.resize(servers.size() - 1);
      }
      if (paxos)
        paxos->getLog()->setMetaData(
            Paxos::keyMemberConfigure,
            membersToString(paxos->getLocalServer()->strAddr));
      break;
    }
  }
  return ret;
}

int StableConfiguration::configureLearner(const uint64_t serverId,
                                          uint64_t source, Paxos *paxos) {
  auto server = this->getServer(serverId);
  if (!server) {
    easy_warn_log(
        "Server %d : StableConfiguration::configureLearner: server %d not "
        "found, just skip.",
        paxos->getLocalServer()->serverId, serverId);
    return 0;
  }

  server->learnerSource = source;

  if (paxos)
    paxos->getLog()->setMetaData(Paxos::keyLearnerConfigure,
                                 learnersToString());

  return 0;
}

int StableConfiguration::configureMember(const uint64_t serverId,
                                         bool forceSync, uint electionWeight,
                                         Paxos *paxos) {
  auto server = this->getServer(serverId);
  if (!server) {
    easy_warn_log(
        "Server %d : StableConfiguration::configureMember: server %d not "
        "found, just skip.",
        paxos->getLocalServer()->serverId, serverId);
    return 0;
  }
  server->forceSync = forceSync;
  server->electionWeight = electionWeight;

  if (paxos)
    paxos->getLog()->setMetaData(
        Paxos::keyMemberConfigure,
        membersToString(paxos->getLocalServer()->strAddr));

  return 0;
}

void StableConfiguration::addLearners(const std::vector<std::string> &strConfig,
                                      Paxos *paxos, bool replaceAll) {
  if (strConfig.size() == 0) return;

  std::shared_ptr<RemoteServer> ptrR;
  uint64_t i = 0;
  /*
  if (learners.size() == 0)
    i= 100;
  else
    i= learners.back()->serverId + 1;
    */
  for (auto &str : strConfig) {
    ++i;
    if (str == "0") {
      /* empty server */
      if (learners.size() >= i)
        learners[i - 1] = nullptr;
      else {
        assert(learners.size() == i - 1);
        learners.push_back(nullptr);
      }
      continue;
    }
    // learners.push_back(ptrR= std::make_shared<RemoteServer>(i++));
    ptrR = std::make_shared<RemoteServer>(0);
    addServer(learners, ptrR, replaceAll);
    ptrR->serverId += 99;
    ptrR->srv = paxos->getService();
    ptrR->paxos = paxos;
    ptrR->isLearner = true;
    initServerFromString(ptrR, str);
    // ptrR->strAddr= str;
    // ptrR->forceSync= false;
    // ptrR->electionWeight= 0;
    ptrR->appliedIndex = 0;
    /* The Learner will become follower finally, so we create heartbeatTimer
     * here. */
    ptrR->heartbeatTimer = std::unique_ptr<ThreadTimer>(
        new ThreadTimer(paxos->getService()->getThreadTimerService(),
                        paxos->getService(), paxos->getHeartbeatTimeout(),
                        ThreadTimer::Repeatable, Paxos::heartbeatCallback,
                        std::move(std::weak_ptr<RemoteServer>(ptrR))));
    if ((paxos->getState() == Paxos::LEADER && ptrR->learnerSource == 0) ||
        ptrR->learnerSource == paxos->getLocalServer()->serverId)
      ptrR->beginLeadership((void *)1);
  }
  paxos->getLog()->setMetaData(Paxos::keyLearnerConfigure, learnersToString());
}

void StableConfiguration::delLearners(const std::vector<std::string> &strConfig,
                                      Paxos *paxos) {
  for (auto &str : strConfig) {
    for (auto it = learners.begin(); it != learners.end(); ++it)
      if (*it && (*it)->strAddr == str) {
        /* TODO: clear for RemoteServer. */
        (*it)->stop(NULL);
        *it = nullptr;
        // learners.erase(it);
        break;
      }
  }

  while (learners.size() > 0 && learners[learners.size() - 1] == nullptr)
    learners.resize(learners.size() - 1);

  if (paxos)
    paxos->getLog()->setMetaData(Paxos::keyLearnerConfigure,
                                 learnersToString());
}

void StableConfiguration::delAllLearners() {
  for (auto it = learners.begin(); it != learners.end(); ++it)
    if (*it) (*it)->stop(NULL);

  learners.clear();
}

void StableConfiguration::delAllRemoteServer(const std::string &localStrAddr,
                                             Paxos *paxos) {
  // uint64_t id= 1;
  for (auto it = servers.begin(); it != servers.end(); ++it) {
    if (*it && (*it)->strAddr != localStrAddr) {
      (*it)->stop(NULL);
      serversNum.fetch_sub(1);
      *it = nullptr;
      // servers.erase(it);
    }
  }

  assert(serversNum.load() == 1);

  if (paxos)
    paxos->getLog()->setMetaData(
        Paxos::keyMemberConfigure,
        membersToString(paxos->getLocalServer()->strAddr));

  if (servers.size() > 0) {
    while (servers[servers.size() - 1] == nullptr)
      servers.resize(servers.size() - 1);
  }
}

void StableConfiguration::mergeFollowerMeta(
    const ::google::protobuf::RepeatedPtrField<::alisql::ClusterInfoEntry>
        &ciEntries) {
  for (auto cit = ciEntries.begin(); cit != ciEntries.end(); ++cit) {
    auto learner =
        std::dynamic_pointer_cast<RemoteServer>(getServer(cit->serverid()));
    if (learner == nullptr) {
      easy_warn_log(
          "StableConfiguration::mergeFollowerMeta: try to find server %d, but "
          "not in current configure!!\n",
          cit->serverid());
      continue;
    }
    if (learner->serverId == cit->serverid() &&
        learner->learnerSource == cit->learnersource()) {
      learner->matchIndex.store(cit->matchindex());
      learner->nextIndex.store(cit->nextindex());
      learner->appliedIndex.store(cit->appliedindex());
      learner->lastMergeTP = learner->now();
    }
  }
}

std::string StableConfiguration::getAddr(const std::string &addr) {
  std::string ret = addr;
  uint64_t pos = std::string::npos;

  pos = ret.find('$', 0);
  if (pos == std::string::npos) pos = ret.find('#', 0);

  if (pos != std::string::npos) ret.resize(pos);

  return ret;
}

bool StableConfiguration::isServerInVector(
    const std::string &server, const std::vector<std::string> &strConfig) {
  std::string addr = getAddr(server);
  for (auto &str : strConfig) {
    if (addr == getAddr(str)) return true;
  }
  return false;
}

void StableConfiguration::reset_flow_control() {
  for (auto &server : servers)
    if (server) server->flowControl = 0;
  for (auto &learner : learners)
    if (learner) learner->flowControl = 0;
}

void StableConfiguration::set_flow_control(uint64_t serverId, int64_t fc) {
  for (auto &server : servers) {
    if (server && server->serverId == serverId) {
      server->flowControl = fc;
      return;
    }
  }
  for (auto &learner : learners) {
    if (learner && learner->serverId == serverId) {
      learner->flowControl = fc;
      return;
    }
  }
}

}  // namespace alisql
