/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  paxos_configuration.h,v 1.0 07/30/2016 04:23:32 PM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file paxos_configuration.h
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 07/30/2016 04:23:32 PM
 * @version 1.0
 * @brief the configuration interface and implement of the paxos.
 *
 **/

#ifndef cluster_paxos_configuration_INC
#define cluster_paxos_configuration_INC

#include <memory>
#include <vector>
#include "paxos_server.h"

namespace alisql {

class Paxos;

/**
 * @class Configuration
 *
 * @brief interface of paxos configuration
 *
 **/
class Configuration {
 public:
  Configuration() {}
  virtual ~Configuration() {}
  typedef std::shared_ptr<Server> ServerRef;
  typedef std::function<bool(Server &)> Predicate;
  typedef std::function<uint64_t(Server &)> GetValue;
  typedef std::function<void(Server &, void *)> SideEffect;

  virtual void forEach(const SideEffect &sideEffect, void *ptr) = 0;
  virtual void forEachLearners(const SideEffect &sideEffect, void *ptr) = 0;
  virtual bool quorumAll(const Predicate &predicate) const = 0;
  virtual uint64_t quorumMin(const GetValue &getValue) const = 0;
  virtual uint64_t forceMin(const GetValue &getValue) const = 0;
  virtual uint64_t allMin(const GetValue &getValue) const = 0;

  virtual ServerRef getServer(uint64_t serverId) = 0;
  virtual ServerRef getLearnerByAddr(const std::string &addr) = 0;
  virtual uint64_t getServerIdFromAddr(const std::string &addr) = 0;
  virtual const std::vector<ServerRef> &getServers() = 0;
  virtual const std::vector<ServerRef> &getLearners() = 0;
  virtual uint64_t getServerNum() const = 0;
  virtual uint64_t getServerNumLockFree() const = 0;
  virtual uint64_t getLearnerNum() const = 0;
  virtual bool needWeightElection(uint64_t localWeight) = 0;
  virtual uint64_t getMaxWeightServerId(uint64_t baseEpoch,
                                        ServerRef localServer) = 0;
  virtual int addMember(const std::string &strAddr, Paxos *paxos) = 0;
  virtual int delMember(const std::string &strAddr, Paxos *paxos) = 0;
  virtual int configureLearner(uint64_t serverId, uint64_t source,
                               Paxos *paxos) = 0;
  virtual int configureMember(const uint64_t serverId, bool forceSync,
                              uint electionWeight, Paxos *paxos) = 0;
  virtual void addLearners(const std::vector<std::string> &strConfig,
                           Paxos *paxos, bool replaceAll = false) = 0;
  virtual void delLearners(const std::vector<std::string> &strConfig,
                           Paxos *paxos) = 0;
  virtual void delAllLearners() = 0;
  virtual void delAllRemoteServer(const std::string &localStrAddr,
                                  Paxos *paxos) = 0;
  virtual void mergeFollowerMeta(
      const ::google::protobuf::RepeatedPtrField<::alisql::ClusterInfoEntry>
          &) = 0;

  virtual std::string membersToString(const std::string &localAddr) = 0;
  virtual std::string membersToString() = 0;
  virtual std::string learnersToString() = 0;

  /* append log flow control */
  virtual void reset_flow_control() = 0;
  virtual void set_flow_control(uint64_t serverId, int64_t fc) = 0;

 private:
  Configuration(const Configuration &other);  // copy constructor
  const Configuration &operator=(
      const Configuration &other);            // assignment operator

};                                            /* end of class Configuration */

/**
 * @class StableConfiguration
 *
 * @brief implement of a stable configuration
 *
 **/
class StableConfiguration : public Configuration {
 public:
  StableConfiguration() : serversNum(0) {}
  ~StableConfiguration() override {}

  void forEach(const SideEffect &sideEffect, void *ptr) override;
  void forEachLearners(const SideEffect &sideEffect, void *ptr) override;
  bool quorumAll(const Predicate &predicate) const override;
  uint64_t quorumMin(const GetValue &getValue) const override;
  uint64_t forceMin(const GetValue &getValue) const override;
  uint64_t allMin(const GetValue &getValue) const override;

  void installConfig(const std::vector<std::string> &strConfig,
                     uint64_t current, Paxos *paxos,
                     std::shared_ptr<LocalServer> localServer);
  static std::vector<std::string> stringToVector(const std::string &str,
                                                 uint64_t &currentIndex);
  static std::string memberToString(ServerRef server);
  static std::string learnerToString(ServerRef server);
  static std::string configToString(std::vector<ServerRef> &servers,
                                    const std::string &localAddr,
                                    bool forceMember = false);
  static ServerRef addServer(std::vector<ServerRef> &servers,
                             ServerRef newServer, bool useAppend = false);

  ServerRef getServer(uint64_t serverId) override;
  ServerRef getLearnerByAddr(const std::string &addr) override;
  uint64_t getServerIdFromAddr(const std::string &addr) override;
  const std::vector<ServerRef> &getServers() override { return servers; }
  const std::vector<ServerRef> &getLearners() override { return learners; }
  uint64_t getServerNum() const override;
  uint64_t getServerNumLockFree() const override { return serversNum.load(); }
  uint64_t getLearnerNum() const override;
  bool needWeightElection(uint64_t localWeight) override;
  uint64_t getMaxWeightServerId(uint64_t baseEpoch,
                                ServerRef localServer) override;
  int addMember(const std::string &strAddr, Paxos *paxos) override;
  int delMember(const std::string &strAddr, Paxos *paxos) override;
  int configureLearner(uint64_t serverId, uint64_t source,
                       Paxos *paxos) override;
  int configureMember(const uint64_t serverId, bool forceSync,
                      uint electionWeight, Paxos *paxos) override;
  void addLearners(const std::vector<std::string> &strConfig, Paxos *paxos,
                   bool replaceAll = false) override;
  void delLearners(const std::vector<std::string> &strConfig,
                   Paxos *paxos) override;
  void delAllLearners() override;
  void delAllRemoteServer(const std::string &localStrAddr,
                          Paxos *paxos) override;
  void mergeFollowerMeta(
      const ::google::protobuf::RepeatedPtrField<::alisql::ClusterInfoEntry>
          &ciEntries) override;

  std::string membersToString(const std::string &localAddr) override {
    return StableConfiguration::configToString(servers, localAddr);
  }
  std::string membersToString() override {
    return StableConfiguration::configToString(servers, std::string(""), true);
  }
  std::string learnersToString() override {
    return StableConfiguration::configToString(learners, std::string(""));
  }
  static void initServerFromString(ServerRef server, std::string str,
                                   bool isLearner = false);
  static void initServerDefault(ServerRef server);
  static std::string getAddr(const std::string &addr);
  static bool isServerInVector(const std::string &server,
                               const std::vector<std::string> &strConfig);

  /* append log flow control */
  void reset_flow_control() override;
  void set_flow_control(uint64_t serverId, int64_t fc) override;

  std::atomic<uint64_t> serversNum;
  std::vector<ServerRef> servers;
  /*  TODO We may use map instead of vector here for learners */
  std::vector<ServerRef> learners;

 private:
  StableConfiguration(const StableConfiguration &other);  // copy constructor
  const StableConfiguration &operator=(
      const StableConfiguration &other);                  // assignment operator

}; /* end of class StableConfiguration */

}  // namespace alisql

#endif  // #ifndef cluster_paxos_configuration_INC
