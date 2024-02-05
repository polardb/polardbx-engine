/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  consensus-t.cc,v 1.0 07/30/2016 02:41:21 PM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file consensus-t.cc
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 07/30/2016 02:41:21 PM
 * @version 1.0
 * @brief unit test for alisql::Consensus
 *
 **/

#include <gtest/gtest.h>
#include <atomic>
#include <memory>
#include <thread>
#include "client_service.h"
#include "consensus.h"
#include "easyNet.h"
#include "files.h"
#include "paxos.h"
#include "paxos.pb.h"
#include "paxos_configuration.h"
#include "paxos_log.h"
#include "paxos_server.h"
#include "service.h"

#include "file_paxos_log.h"
#include "rd_paxos_log.h"

using namespace alisql;

TEST(consensus, helloworld) { EXPECT_TRUE(1); }

TEST(consensus, StableConfiguration) {
  std::shared_ptr<StableConfiguration> config =
      std::make_shared<StableConfiguration>();
  EXPECT_TRUE(static_cast<bool>(config));

  std::shared_ptr<RemoteServer> ptr;
  std::shared_ptr<LocalServer> lptr;
  config->servers.push_back(lptr = std::make_shared<LocalServer>(1));
  lptr->strAddr = std::string("127.0.0.1:10001");
  config->servers.push_back(ptr = std::make_shared<RemoteServer>(2));
  ptr->matchIndex = 5;
  ptr->strAddr = std::string("127.0.0.1:10002");
  config->servers.push_back(ptr = std::make_shared<RemoteServer>(3));
  ptr->strAddr = std::string("127.0.0.1:10003");
  ptr->matchIndex = 6;

  uint64_t qmin = config->quorumMin(&Server::getMatchIndex);
  EXPECT_EQ(qmin, 5);

  config->delMember(std::string("127.0.0.1:10002"), nullptr);
  EXPECT_EQ(config->getServerNum(), 2);
  EXPECT_EQ(config->servers[0]->serverId, 1);
  EXPECT_EQ(config->servers[0]->strAddr, std::string("127.0.0.1:10001"));
  EXPECT_EQ(config->servers[2]->serverId, 3);
  EXPECT_EQ(config->servers[2]->strAddr, std::string("127.0.0.1:10003"));
  // EXPECT_EQ(config->membersToString(lptr->strAddr),
  // std::string("127.0.0.1:10001#5N;127.0.0.1:10003#5N@1"));
  EXPECT_EQ(config->membersToString(lptr->strAddr),
            std::string("127.0.0.1:10001#5N;0;127.0.0.1:10003#5N@1"));
  config->configureMember(1, true, 9, nullptr);
  // EXPECT_EQ(config->membersToString(lptr->strAddr),
  // std::string("127.0.0.1:10001#9F;127.0.0.1:10003#5N@1"));
  EXPECT_EQ(config->membersToString(lptr->strAddr),
            std::string("127.0.0.1:10001#9F;0;127.0.0.1:10003#5N@1"));

  std::string strConfig = "127.0.0.1:10001;127.0.0.1:10002;127.0.0.1:10003@1";
  uint64_t index = 0;
  std::vector<std::string> v =
      StableConfiguration::stringToVector(strConfig, index);
  EXPECT_EQ(v[0], "127.0.0.1:10001");
  EXPECT_EQ(v[1], "127.0.0.1:10002");
  EXPECT_EQ(v[2], "127.0.0.1:10003");
  EXPECT_EQ(index, 1);

  // for Learner string
  strConfig = "127.0.0.1:10001;127.0.0.1:10002";
  v = StableConfiguration::stringToVector(strConfig, index);
  EXPECT_EQ(v[0], "127.0.0.1:10001");
  EXPECT_EQ(v[1], "127.0.0.1:10002");

  EXPECT_EQ(std::string("127.0.0.1:10001"),
            StableConfiguration::getAddr(std::string("127.0.0.1:10001#5N")));
  EXPECT_EQ(std::string("127.0.0.1:10001"),
            StableConfiguration::getAddr(std::string("127.0.0.1:10001$:2")));

  strConfig = "127.0.0.1:11004$00;127.0.0.1:11005$00;0;127.0.0.1:11006$00";
  v = StableConfiguration::stringToVector(strConfig, index);
  EXPECT_TRUE(
      StableConfiguration::isServerInVector(std::string("127.0.0.1:11005"), v));
  EXPECT_TRUE(StableConfiguration::isServerInVector(
      std::string("127.0.0.1:11005$02"), v));
  EXPECT_TRUE(StableConfiguration::isServerInVector(
      std::string("127.0.0.1:11005#5N"), v));
  EXPECT_FALSE(
      StableConfiguration::isServerInVector(std::string("127.0.0.1:11001"), v));
  EXPECT_FALSE(StableConfiguration::isServerInVector(
      std::string("127.0.0.1:11001#5N"), v));

  // test learnerSource larger than 99 and 109 and 159
  strConfig =
      "127.0.0.1:11004$:1;127.0.0.1:11005$;2;0;127.0.0.1:11006$00;127.0.0.1:"
      "11007$@3";
  index = 0;
  v = StableConfiguration::stringToVector(strConfig, index);
  EXPECT_TRUE(index == 0);
  EXPECT_TRUE(v.size() == 5);
  EXPECT_TRUE(
      StableConfiguration::isServerInVector(std::string("127.0.0.1:11005"), v));
  EXPECT_TRUE(StableConfiguration::isServerInVector(
      std::string("127.0.0.1:11005$;2"), v));
  EXPECT_TRUE(StableConfiguration::isServerInVector(
      std::string("127.0.0.1:11007$@3"), v));

  strConfig =
      "127.0.0.1:11004$00;127.0.0.1:11005$00;0;127.0.0.1:11006$00;127.0.0.1:"
      "11007$;0";
  index = 0;
  v = StableConfiguration::stringToVector(strConfig, index);
  EXPECT_TRUE(index == 0);
  EXPECT_EQ(v.size(), 5);
  EXPECT_TRUE(
      StableConfiguration::isServerInVector(std::string("127.0.0.1:11007"), v));
  EXPECT_TRUE(StableConfiguration::isServerInVector(
      std::string("127.0.0.1:11007$;0"), v));
  EXPECT_TRUE(
      StableConfiguration::isServerInVector(std::string("127.0.0.1:11004"), v));
  EXPECT_TRUE(StableConfiguration::isServerInVector(
      std::string("127.0.0.1:11004$00"), v));
  EXPECT_EQ(v[0], "127.0.0.1:11004$00");
  EXPECT_EQ(v[4], "127.0.0.1:11007$;0");

  strConfig =
      "0;127.0.0.1:11004$00;127.0.0.1:11005$00;0;127.0.0.1:11006$00;127.0.0.1:"
      "11007$;0";
  index = 0;
  v = StableConfiguration::stringToVector(strConfig, index);
  EXPECT_TRUE(index == 0);
  EXPECT_EQ(v.size(), 6);
  EXPECT_TRUE(
      StableConfiguration::isServerInVector(std::string("127.0.0.1:11007"), v));
  EXPECT_TRUE(StableConfiguration::isServerInVector(
      std::string("127.0.0.1:11007$;0"), v));
  EXPECT_TRUE(
      StableConfiguration::isServerInVector(std::string("127.0.0.1:11004"), v));
  EXPECT_TRUE(StableConfiguration::isServerInVector(
      std::string("127.0.0.1:11004$00"), v));
  EXPECT_EQ(v[0], "0");
  EXPECT_EQ(v[5], "127.0.0.1:11007$;0");
}

TEST(consensus, StableConfiguration_uncontinue_serverid) {
  std::string strConfig;
  uint64_t index = 0;
  std::shared_ptr<StableConfiguration> config =
      std::make_shared<StableConfiguration>();
  EXPECT_TRUE(static_cast<bool>(config));

  std::shared_ptr<RemoteServer> ptr;
  std::shared_ptr<LocalServer> lptr;
  config->servers.push_back(lptr = std::make_shared<LocalServer>(1));
  lptr->strAddr = std::string("127.0.0.1:10001");
  config->servers.push_back(nullptr);
  config->servers.push_back(ptr = std::make_shared<RemoteServer>(3));
  ptr->matchIndex = 5;
  ptr->strAddr = std::string("127.0.0.1:10002");
  config->servers.push_back(ptr = std::make_shared<RemoteServer>(4));
  ptr->strAddr = std::string("127.0.0.1:10003");
  ptr->matchIndex = 6;

  uint64_t qmin = config->quorumMin(&Server::getMatchIndex);
  EXPECT_EQ(qmin, 5);

  config->delMember(std::string("127.0.0.1:10002"), nullptr);
  EXPECT_EQ(config->getServerNum(), 2);
  EXPECT_EQ(config->servers[0]->serverId, 1);
  EXPECT_EQ(config->servers[0]->strAddr, std::string("127.0.0.1:10001"));
  EXPECT_EQ(config->servers[3]->serverId, 4);
  EXPECT_EQ(config->servers[3]->strAddr, std::string("127.0.0.1:10003"));
  // EXPECT_EQ(config->membersToString(lptr->strAddr),
  // std::string("127.0.0.1:10001#5N;127.0.0.1:10003#5N@1"));
  EXPECT_EQ(config->membersToString(lptr->strAddr),
            std::string("127.0.0.1:10001#5N;0;0;127.0.0.1:10003#5N@1"));
  /*
  config->configureMember(1, true, 9, NULL);
  EXPECT_EQ(config->membersToString(lptr->strAddr),
  std::string("127.0.0.1:10001#9F;127.0.0.1:10003#5N@1"));


  strConfig= "127.0.0.1:10001;127.0.0.1:10002;127.0.0.1:10003@1";
  std::vector<std::string> v= StableConfiguration::stringToVector(strConfig,
  index); EXPECT_EQ(v[0], "127.0.0.1:10001"); EXPECT_EQ(v[1],
  "127.0.0.1:10002"); EXPECT_EQ(v[2], "127.0.0.1:10003"); EXPECT_EQ(index, 1);
  */

  // for Learner string
  strConfig = "127.0.0.1:10001;0;127.0.0.1:10002";
  auto v1 = StableConfiguration::stringToVector(strConfig, index);
  EXPECT_EQ(v1[0], "127.0.0.1:10001");
  EXPECT_EQ(v1[1], "0");
  EXPECT_EQ(v1[2], "127.0.0.1:10002");
}

TEST(consensus, PaxosLog) {
  // TODO need a factory.
  // PaxosLog *rlog= new PaxosLog();
  PaxosLog *rlog =
      (PaxosLog *)new RDPaxosLog("paxosLogTestDir", true, 4 * 1024 * 1024);
  LogEntry entry;

  EXPECT_EQ(rlog->getLastLogIndex(), 0);
  rlog->getEntry(0, entry);
  EXPECT_EQ(entry.term(), 0);
  EXPECT_EQ(entry.index(), 0);

  entry.set_term(1);
  entry.set_index(1);
  entry.set_optype(1);
  entry.set_ikey("aa");
  entry.set_value("bb");
  rlog->append(entry);

  entry.Clear();
  rlog->getEntry(1, entry);
  EXPECT_EQ(rlog->getLastLogIndex(), 1);
  EXPECT_EQ(entry.term(), 1);
  EXPECT_EQ(entry.index(), 1);
  EXPECT_EQ(entry.optype(), 1);
  EXPECT_EQ(entry.ikey(), "aa");
  EXPECT_EQ(entry.value(), "bb");

  entry.set_term(1);
  entry.set_index(2);
  entry.set_optype(1);
  entry.set_ikey("aa1");
  entry.set_value("bb1");
  rlog->append(entry);
  entry.set_term(1);
  entry.set_index(3);
  entry.set_optype(1);
  entry.set_ikey("aa2");
  entry.set_value("bb2");
  rlog->append(entry);

  entry.Clear();
  rlog->getEntry(3, entry);
  EXPECT_EQ(entry.term(), 1);
  EXPECT_EQ(entry.index(), 3);
  EXPECT_EQ(entry.optype(), 1);
  EXPECT_EQ(entry.ikey(), "aa2");
  EXPECT_EQ(entry.value(), "bb2");

  rlog->truncateBackward(2);
  EXPECT_EQ(rlog->getLastLogIndex(), 1);

  entry.Clear();
  rlog->getEntry(1, entry);
  EXPECT_EQ(rlog->getLastLogIndex(), 1);
  EXPECT_EQ(entry.term(), 1);
  EXPECT_EQ(entry.index(), 1);
  EXPECT_EQ(entry.optype(), 1);
  EXPECT_EQ(entry.ikey(), "aa");
  EXPECT_EQ(entry.value(), "bb");

  rlog->truncateBackward(1);
  EXPECT_EQ(rlog->getLastLogIndex(), 0);

  // Patch entries append API
  ::google::protobuf::RepeatedPtrField<LogEntry> entries;
  entry.set_term(1);
  entry.set_index(1);
  entry.set_optype(1);
  entry.set_ikey("aa");
  entry.set_value("bb");
  *(entries.Add()) = entry;

  entry.set_term(2);
  entry.set_index(2);
  entry.set_optype(1);
  entry.set_ikey("bb");
  entry.set_value("cc");
  *(entries.Add()) = entry;

  EXPECT_EQ(2, rlog->append(entries));

  rlog->getEntry(1, entry);
  EXPECT_EQ(rlog->getLastLogIndex(), 2);
  EXPECT_EQ(entry.term(), 1);
  EXPECT_EQ(entry.index(), 1);
  EXPECT_EQ(entry.optype(), 1);
  EXPECT_EQ(entry.ikey(), "aa");
  EXPECT_EQ(entry.value(), "bb");
  rlog->getEntry(2, entry);
  EXPECT_EQ(rlog->getLastLogIndex(), 2);
  EXPECT_EQ(entry.term(), 2);
  EXPECT_EQ(entry.index(), 2);
  EXPECT_EQ(entry.optype(), 1);
  EXPECT_EQ(entry.ikey(), "bb");
  EXPECT_EQ(entry.value(), "cc");

  delete rlog;
  deleteDir("paxosLogTestDir");
}

TEST(consensus, FilePaxosLog) {
  // PaxosLog *rlog= new PaxosLog();
  PaxosLog *rlog =
      (PaxosLog *)new FilePaxosLog("filelog", FilePaxosLog::LTSync);
  LogEntry entry;

  EXPECT_EQ(rlog->getLastLogIndex(), 0);
  rlog->getEntry(0, entry);
  EXPECT_EQ(entry.term(), 0);
  EXPECT_EQ(entry.index(), 0);

  entry.set_term(1);
  entry.set_index(1);
  entry.set_optype(1);
  entry.set_ikey("aa");
  entry.set_value("bb");
  rlog->append(entry);

  entry.Clear();
  rlog->getEntry(1, entry);
  EXPECT_EQ(rlog->getLastLogIndex(), 1);
  EXPECT_EQ(entry.term(), 1);
  EXPECT_EQ(entry.index(), 1);
  EXPECT_EQ(entry.optype(), 1);
  EXPECT_EQ(entry.ikey(), "aa");
  EXPECT_EQ(entry.value(), "bb");

  entry.set_term(1);
  entry.set_index(2);
  entry.set_optype(1);
  entry.set_ikey("aa1");
  entry.set_value("bb1");
  rlog->append(entry);
  entry.set_term(1);
  entry.set_index(3);
  entry.set_optype(1);
  entry.set_ikey("aa2");
  entry.set_value("bb2");
  rlog->append(entry);

  entry.Clear();
  rlog->getEntry(3, entry);
  EXPECT_EQ(entry.term(), 1);
  EXPECT_EQ(entry.index(), 3);
  EXPECT_EQ(entry.optype(), 1);
  EXPECT_EQ(entry.ikey(), "aa2");
  EXPECT_EQ(entry.value(), "bb2");

  rlog->truncateBackward(2);
  EXPECT_EQ(rlog->getLastLogIndex(), 1);

  entry.Clear();
  rlog->getEntry(1, entry);
  EXPECT_EQ(rlog->getLastLogIndex(), 1);
  EXPECT_EQ(entry.term(), 1);
  EXPECT_EQ(entry.index(), 1);
  EXPECT_EQ(entry.optype(), 1);
  EXPECT_EQ(entry.ikey(), "aa");
  EXPECT_EQ(entry.value(), "bb");

  rlog->truncateBackward(1);
  EXPECT_EQ(rlog->getLastLogIndex(), 0);

  // Patch entries append API
  ::google::protobuf::RepeatedPtrField<LogEntry> entries;
  entry.set_term(1);
  entry.set_index(1);
  entry.set_optype(1);
  entry.set_ikey("aa");
  entry.set_value("bb");
  *(entries.Add()) = entry;

  entry.set_term(2);
  entry.set_index(2);
  entry.set_optype(1);
  entry.set_ikey("bb");
  entry.set_value("cc");
  *(entries.Add()) = entry;

  EXPECT_EQ(2, rlog->append(entries));

  rlog->getEntry(1, entry);
  EXPECT_EQ(rlog->getLastLogIndex(), 2);
  EXPECT_EQ(entry.term(), 1);
  EXPECT_EQ(entry.index(), 1);
  EXPECT_EQ(entry.optype(), 1);
  EXPECT_EQ(entry.ikey(), "aa");
  EXPECT_EQ(entry.value(), "bb");
  rlog->getEntry(2, entry);
  EXPECT_EQ(rlog->getLastLogIndex(), 2);
  EXPECT_EQ(entry.term(), 2);
  EXPECT_EQ(entry.index(), 2);
  EXPECT_EQ(entry.optype(), 1);
  EXPECT_EQ(entry.ikey(), "bb");
  EXPECT_EQ(entry.value(), "cc");

  delete rlog;
  deleteDir("filelog");
}

TEST(consensus, pbTest) {
  PaxosMsg msg;
  msg.set_clusterid(0);
  msg.set_msgid(0);
  msg.set_serverid(0);
  msg.set_msgtype(1);
  msg.set_term(2);
  msg.set_leaderid(3);

  EXPECT_TRUE(msg.IsInitialized());

  auto length = uint32_t(msg.ByteSize());

  std::string buf;
  msg.SerializeToString(&buf);

  EXPECT_GT(buf.length(), 1);

  PaxosMsg rcv;
  rcv.ParseFromString(buf);

  EXPECT_EQ(rcv.msgtype(), 1);
  EXPECT_EQ(rcv.term(), 2);
  EXPECT_TRUE(rcv.has_leaderid());
  EXPECT_EQ(rcv.leaderid(), 3);
  EXPECT_FALSE(rcv.has_prevlogindex());
}

TEST(consensus, RemoteServerSendMsg) {
  auto srv = std::make_shared<Service>(nullptr);
  EXPECT_TRUE(static_cast<bool>(srv));
  srv->init();
  srv->start(11001);

  auto ptr = std::make_shared<RemoteServer>(1);
  ptr->matchIndex = 5;
  ptr->strAddr = std::string("127.0.0.1:11001");
  ptr->srv = srv;

  PaxosMsg msg;
  msg.set_clusterid(0);
  msg.set_msgid(0);
  msg.set_serverid(0);
  msg.set_msgtype((int)Consensus::LeaderCommand);
  msg.set_term(2);
  msg.set_leaderid(3);
  EXPECT_TRUE(msg.IsInitialized());
  msg.clear_msgid();

  ptr->connect(nullptr);
  sleep(1);
  // todo need to decouple service and consensus
  // ptr->sendMsg(&msg);
  sleep(1);
  ptr.reset();

  srv->shutdown();
}

TEST(consensus, Paxos_requestVote3) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog;
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir1", true, 4 * 1024 * 1024);
  auto *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir2", true, 4 * 1024 * 1024);
  auto *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir3", true, 4 * 1024 * 1024);
  auto *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3);

  sleep(3);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_ikey("bbb");
  le.set_value("ccc");

  paxos1->replicateLog(le);

  // Test Learner
  std::string strTmp("127.0.0.1:11004");
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir4", true, 4 * 1024 * 1024);
  auto *learner1 = new Paxos(timeout, rlog);
  learner1->initAsLearner(strTmp, 11);
  sleep(1);

  strConfig.clear();
  strConfig.push_back(strTmp);
  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  sleep(2);
  EXPECT_EQ(learner1->getLastLogIndex(), paxos1->getLastLogIndex());
  le.clear_term();
  paxos1->replicateLog(le);
  sleep(2);
  EXPECT_EQ(learner1->getLastLogIndex(), paxos1->getLastLogIndex());

  // Transfer Leader to Server 2 success case
  sleep(1);
  paxos1->leaderTransfer(2);
  sleep(1);
  EXPECT_EQ(paxos2->getState(), Paxos::LEADER);

  // Test if the learners can still recieve msgs from new leader.
  le.clear_term();
  paxos2->replicateLog(le);
  sleep(1);
  EXPECT_EQ(learner1->getLastLogIndex(), paxos2->getLastLogIndex());
  le.clear_term();
  paxos2->replicateLog(le);
  sleep(1);
  EXPECT_EQ(learner1->getLastLogIndex(), paxos2->getLastLogIndex());

  // Remove unexist Learner and add already exist.
  strConfig.clear();
  strConfig.emplace_back("127.0.0.1:11010");
  EXPECT_EQ(paxos2->changeLearners(Paxos::CCDelNode, strConfig),
            PaxosErrorCode::PE_NOTFOUND);
  strConfig.clear();
  strConfig.push_back(strTmp);
  ;
  EXPECT_EQ(paxos2->changeLearners(Paxos::CCAddNode, strConfig),
            PaxosErrorCode::PE_EXISTS);

  // Remove Learner
  strConfig.clear();
  strConfig.push_back(strTmp);
  paxos2->changeLearners(Paxos::CCDelNode, strConfig);
  sleep(2);
  le.clear_term();
  paxos2->replicateLog(le);
  sleep(1);
  EXPECT_LT(learner1->getLastLogIndex(), paxos2->getLastLogIndex());

  // add member
  // // create 2 learners
  std::string strTmp2("127.0.0.1:11005");
  std::string strTmp3("127.0.0.1:11006");
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir5", true, 4 * 1024 * 1024);
  auto *learner2 = new Paxos(timeout, rlog);
  learner2->initAsLearner(strTmp2, 12);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir6", true, 4 * 1024 * 1024);
  auto *learner3 = new Paxos(timeout, rlog);
  learner3->initAsLearner(strTmp3, 13);
  sleep(1);
  // // add 2 learners
  strConfig.clear();
  strConfig.push_back(strTmp2);
  strConfig.push_back(strTmp3);
  paxos2->changeLearners(Paxos::CCAddNode, strConfig);
  sleep(1);
  auto config =
      std::dynamic_pointer_cast<StableConfiguration>(paxos2->getConfig());
  // auto& servers= config->getServers();
  auto &learners = config->getLearners();
  EXPECT_EQ(config->getServerNum(), 3);
  EXPECT_EQ(learners.size(), 2);
  EXPECT_EQ(learner2->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(learner3->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(config->membersToString(("127.0.0.1:11002")),
            std::string(
                "127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#5N@2"));
  EXPECT_EQ(config->learnersToString(),
            std::string("127.0.0.1:11005$00;127.0.0.1:11006$00"));
  // // change learners to followers one by one
  paxos2->changeMember(Paxos::CCAddNode, strTmp2);
  sleep(1);
  le.clear_term();
  paxos2->replicateLog(le);
  sleep(1);
  EXPECT_EQ(learner2->getState(), Paxos::FOLLOWER);
  EXPECT_EQ(config->getServerNum(), 4);
  EXPECT_EQ(learners.size(), 2);
  EXPECT_EQ(config->getLearnerNum(), 1);
  paxos2->changeMember(Paxos::CCAddNode, strTmp3);
  sleep(1);
  paxos2->configureMember(5, true, 9);
  le.clear_term();
  paxos2->replicateLog(le);
  sleep(1);
  EXPECT_EQ(learner3->getState(), Paxos::FOLLOWER);
  EXPECT_EQ(config->getServerNum(), 5);
  EXPECT_EQ(learners.size(), 0);
  EXPECT_EQ(config->membersToString(("127.0.0.1:11002")),
            std::string("127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#"
                        "5N;127.0.0.1:11005#5N;127.0.0.1:11006#9F@2"));
  EXPECT_EQ(config->learnersToString(), std::string(""));
  EXPECT_EQ(learner3->getConfig()->membersToString(
                learner3->getLocalServer()->strAddr),
            std::string("127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#"
                        "5N;127.0.0.1:11005#5N;127.0.0.1:11006#9F@5"));
  // // check 2 new followers
  EXPECT_EQ(learner2->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(learner3->getLastLogIndex(), paxos2->getLastLogIndex());
  std::vector<Paxos::ClusterInfoType> cis;
  paxos2->getClusterInfo(cis);
  for (auto &ci : cis) {
    std::cout << "serverId:" << ci.serverId << " ipPort:" << ci.ipPort
              << " matchIndex:" << ci.matchIndex
              << " nextIndex:" << ci.nextIndex << " role:" << ci.role
              << " hasVoted:" << ci.hasVoted << " forceSync:" << ci.forceSync
              << " electionWeight: " << ci.electionWeight << std::endl
              << std::flush;
  }
  EXPECT_EQ(cis[3].ipPort, strTmp2);
  EXPECT_EQ(cis[3].forceSync, false);
  EXPECT_EQ(cis[3].electionWeight, 5);
  EXPECT_EQ(cis[4].ipPort, strTmp3);
  EXPECT_EQ(cis[4].forceSync, true);
  EXPECT_EQ(cis[4].electionWeight, 9);
  // // delete one follower
  paxos2->changeMember(Paxos::CCDelNode, strTmp2);
  sleep(1);
  le.clear_term();
  paxos2->replicateLog(le);
  sleep(1);
  EXPECT_EQ(config->getServerNum(), 4);
  EXPECT_EQ(learners.size(), 0);
  EXPECT_EQ(config->learnersToString(), std::string(""));
  EXPECT_LT(learner2->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(learner3->getLastLogIndex(), paxos2->getLastLogIndex());
  paxos2->getClusterInfo(cis);
  for (auto &ci : cis) {
    std::cout << "serverId:" << ci.serverId << " ipPort:" << ci.ipPort
              << " matchIndex:" << ci.matchIndex
              << " nextIndex:" << ci.nextIndex << " role:" << ci.role
              << " hasVoted:" << ci.hasVoted << " forceSync:" << ci.forceSync
              << " electionWeight: " << ci.electionWeight << std::endl
              << std::flush;
  }
  EXPECT_EQ(cis[3].ipPort, strTmp3);

  // Transfer Leader to Server 3 fail case
  delete paxos3;
  sleep(1);
  paxos2->leaderTransfer(3);
  sleep(1);
  EXPECT_EQ(paxos2->getState(), Paxos::LEADER);
  EXPECT_EQ(paxos2->getSubState(), Paxos::SubLeaderTransfer);
  sleep(2);
  EXPECT_EQ(paxos2->getState(), Paxos::LEADER);
  EXPECT_EQ(paxos2->getSubState(), Paxos::SubNone);

  delete paxos1;
  delete learner2;
  delete learner3;

  // Leader step-down.
  sleep(3);
  if (paxos2->getState() != Paxos::FOLLOWER) sleep(1);
  EXPECT_EQ(paxos2->getState(), Paxos::FOLLOWER);
  config.reset();
  delete paxos2;
  delete learner1;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
  deleteDir("paxosLogTestDir4");
  deleteDir("paxosLogTestDir5");
  deleteDir("paxosLogTestDir6");
}

TEST(consensus, Paxos_requestVoteAuto) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11011");
  strConfig.emplace_back("127.0.0.1:11012");
  strConfig.emplace_back("127.0.0.1:11013");

  std::shared_ptr<PaxosLog> rlog1 =
      std::make_shared<RDPaxosLog>("paxosLogTestDir11", true, 4 * 1024 * 1024);
  std::shared_ptr<PaxosLog> rlog2 =
      std::make_shared<RDPaxosLog>("paxosLogTestDir12", true, 4 * 1024 * 1024);
  std::shared_ptr<PaxosLog> rlog3 =
      std::make_shared<RDPaxosLog>("paxosLogTestDir13", true, 4 * 1024 * 1024);

  auto *paxos1 = new Paxos(500, rlog1);
  paxos1->init(strConfig, 1, 1);
  auto *paxos2 = new Paxos(500, rlog2);
  paxos2->init(strConfig, 2, 2);
  auto *paxos3 = new Paxos(500, rlog3);
  paxos3->init(strConfig, 3, 3);

  sleep(3);

  EXPECT_TRUE(paxos1->getState() == Paxos::LEADER ||
              paxos2->getState() == Paxos::LEADER ||
              paxos3->getState() == Paxos::LEADER);

  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir11");
  deleteDir("paxosLogTestDir12");
  deleteDir("paxosLogTestDir13");
}

void applyThread(Paxos *paxos) {
  paxos->waitCommitIndexUpdate(0);
  EXPECT_GE(paxos->getCommitIndex(), 1);
  uint64_t i = 0;
  std::shared_ptr<PaxosLog> log = paxos->getLog();
  for (i = 1; i <= paxos->getCommitIndex(); ++i) {
    LogEntry entry;
    log->getEntry(i, entry);
    std::cout << "LogIndex " << i << ": key:" << entry.ikey()
              << ", value:" << entry.value() << std::endl;
  }
  std::cout << "ApplyThread: commitIndex has been updated to "
            << paxos->getCommitIndex() << " ." << std::endl
            << std::flush;

  paxos->waitCommitIndexUpdate(10, 3);
  EXPECT_LE(paxos->getCommitIndex(), 10);
  EXPECT_NE(paxos->getTerm(), 3);
  std::cout << "ApplyThread: return because state changed." << std::endl
            << std::flush;
}

void stateChangeCb(Paxos::StateType state, uint64_t term,
                   uint64_t commitIndex) {
  static uint64_t cnt = 0;
  std::cout << "State change to " << state << " in stateChangeCb term:" << term
            << " commitIndex:" << commitIndex << std::endl;
  if (cnt == 0) {
    EXPECT_EQ(state, Paxos::CANDIDATE);
    EXPECT_EQ(term, 2);
    EXPECT_EQ(commitIndex, 0);
  } else if (cnt == 1) {
    EXPECT_EQ(state, Paxos::LEADER);
    EXPECT_EQ(term, 2);
    EXPECT_EQ(commitIndex, 0);
  } else if (cnt == 2) {
    EXPECT_EQ(state, Paxos::FOLLOWER);
    EXPECT_EQ(term, 3);
    EXPECT_EQ(commitIndex, 4);
  } else if (cnt == 3) {
    EXPECT_EQ(state, Paxos::FOLLOWER);
    EXPECT_EQ(term, 4);
    EXPECT_EQ(commitIndex, 4);
  }

  ++cnt;
}

void printPaxosStats(Paxos *paxos) {
  const Paxos::StatsType &stats = paxos->getStats();

  std::cout << "countMsgAppendLog:" << stats.countMsgAppendLog
            << " countMsgRequestVote:" << stats.countMsgRequestVote
            << " countOnMsgAppendLog:" << stats.countOnMsgAppendLog
            << " countHeartbeat:" << stats.countHeartbeat
            << " countOnMsgRequestVote:" << stats.countOnMsgRequestVote
            << " countOnHeartbeat:" << stats.countOnHeartbeat
            << " countReplicateLog:" << stats.countReplicateLog << std::endl
            << std::flush;
}

TEST(consensus, Paxos_replicateLog) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  uint64_t timeout = 12500;
  std::shared_ptr<PaxosLog> rlog;
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir21", true, 4 * 1024 * 1024);
  auto *cs1 = new ClientService();
  auto *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1, cs1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir22", true, 4 * 1024 * 1024);
  auto *cs2 = new ClientService();
  auto *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2, cs2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir23", true, 4 * 1024 * 1024);
  auto *cs3 = new ClientService();
  auto *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3, cs3);

  // Need one reconnect, because the other Paxos is not inited.
  sleep(5);
  paxos1->setStateChangeCb(stateChangeCb);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  le.set_ikey("bbb");
  // le.set_value("ccc");

  const Paxos::StatsType &stats = paxos1->getStats();
  // std::cout<< "countMsgAppendLog:"<<stats.countMsgAppendLog << "
  // countHeartbeat:"<< stats.countHeartbeat<< std::endl<< std::flush;
  printPaxosStats(paxos1);

  std::thread th1(applyThread, paxos1);
  paxos1->replicateLog(le);
  // std::cout<< "countMsgAppendLog:"<<stats.countMsgAppendLog <<"
  // countHeartbeat:"<< stats.countHeartbeat<<  std::endl<< std::flush;
  printPaxosStats(paxos1);
  sleep(1);
  paxos1->replicateLog(le);
  // std::cout<< "countMsgAppendLog:"<<stats.countMsgAppendLog <<"
  // countHeartbeat:"<< stats.countHeartbeat<<  std::endl<< std::flush;
  printPaxosStats(paxos1);
  sleep(1);
  paxos1->replicateLog(le);
  // std::cout<< "countMsgAppendLog:"<<stats.countMsgAppendLog <<"
  // countHeartbeat:"<< stats.countHeartbeat<<  std::endl<< std::flush;
  printPaxosStats(paxos1);
  EXPECT_EQ(stats.countHeartbeat, 0);
  sleep(3);
  // std::cout<< "countMsgAppendLog:"<<stats.countMsgAppendLog << "
  // countHeartbeat:"<< stats.countHeartbeat<< std::endl<< std::flush;
  printPaxosStats(paxos1);
  EXPECT_EQ(stats.countHeartbeat, 2);

  printPaxosStats(paxos2);

  paxos2->requestVote();
  sleep(1);
  paxos3->requestVote();
  sleep(1);

  th1.join();

  delete paxos1;
  delete paxos2;
  delete paxos3;
  delete cs1;
  delete cs2;
  delete cs3;

  deleteDir("paxosLogTestDir21");
  deleteDir("paxosLogTestDir22");
  deleteDir("paxosLogTestDir23");
}

std::atomic<uint64_t> failCnt(0);
class FailRDPaxosLog : public RDPaxosLog {
 public:
  FailRDPaxosLog(const std::string &dataDir, bool compress,
                 size_t writeBufferSize, bool sync = true)
      : RDPaxosLog(dataDir, compress, writeBufferSize, sync){};
  int getEntry(uint64_t logIndex, LogEntry &entry, bool fastfail) override {
    if (failCnt > 0) {
      --failCnt;
      std::cout << "getEntry will return fail." << std::endl << std::flush;
      return 1;
    } else {
      return RDPaxosLog::getEntry(logIndex, entry);
    }
  }
};
TEST(consensus, Paxos_AliSQLServer) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  std::string strTmp3("127.0.0.1:11003");

  uint64_t timeout = 12500;
  std::shared_ptr<AliSQLServer> aserver;
  std::shared_ptr<AliSQLServer> aserver1 = std::make_shared<AliSQLServer>(1);

  std::shared_ptr<PaxosLog> rlog, rlog1;
  std::shared_ptr<FailRDPaxosLog> rdlog;
  rlog1 = rlog = rdlog = std::make_shared<FailRDPaxosLog>(
      "paxosLogTestDir1", true, 4 * 1024 * 1024);
  rdlog->set_debug_async();
  auto *cs1 = new ClientService();
  auto *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1, cs1, 2, 2,
               aserver = std::make_shared<AliSQLServer>(1));
  rlog = rdlog = std::make_shared<FailRDPaxosLog>("paxosLogTestDir2", true,
                                                  4 * 1024 * 1024);
  auto *cs2 = new ClientService();
  auto *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2, cs2);
  rlog = rdlog = std::make_shared<FailRDPaxosLog>("paxosLogTestDir3", true,
                                                  4 * 1024 * 1024);
  rdlog->set_debug_async();
  auto *paxos3 = new Paxos(timeout, rlog);
  paxos3->initAsLearner(strTmp3, 3, nullptr, 2, 2, aserver1);

  sleep(5);
  paxos1->requestVote();
  sleep(2);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  le.set_ikey("bbb");
  le.set_value("ccc");

  const Paxos::StatsType &stats = paxos1->getStats();
  std::cout << "countMsgAppendLog:" << stats.countMsgAppendLog
            << " countHeartbeat:" << stats.countHeartbeat << std::endl
            << std::flush;

  paxos1->replicateLog(le);
  // reject the old term replicateLog call.
  le.set_term(1);
  EXPECT_EQ(0, paxos1->replicateLog(le));
  le.set_term(0);

  std::cout << "countMsgAppendLog:" << stats.countMsgAppendLog
            << " countHeartbeat:" << stats.countHeartbeat << std::endl
            << std::flush;
  sleep(1);
  std::cout << "commitIndex: " << paxos1->getCommitIndex() << std::endl
            << std::flush;
  EXPECT_EQ(paxos1->getCommitIndex(), 1);
  std::cout << "lastLogIndex: " << rlog1->getLastLogIndex() << std::endl
            << std::flush;
  EXPECT_GE(rlog1->getLastLogIndex(), 1);
  aserver->writeLogDone(rlog1->getLastLogIndex());
  sleep(1);

  EXPECT_EQ(paxos1->getCommitIndex(), rlog1->getLastLogIndex());
  std::cout << "commitIndex: " << paxos1->getCommitIndex() << std::endl
            << std::flush;

  std::vector<Paxos::ClusterInfoType> cis;
  paxos1->getClusterInfo(cis);

  for (auto &ci : cis) {
    std::cout << "serverId:" << ci.serverId << " ipPort:" << ci.ipPort
              << " matchIndex:" << ci.matchIndex
              << " nextIndex:" << ci.nextIndex << " role:" << ci.role
              << " hasVoted:" << ci.hasVoted << " forceSync:" << ci.forceSync
              << " electionWeight: " << ci.electionWeight << std::endl
              << std::flush;
  }

  Paxos::MemberInfoType mi;
  paxos1->getMemberInfo(&mi);
  std::cout << "serverId:" << mi.serverId << " currentTerm:" << mi.currentTerm
            << " currentLeader:" << mi.currentLeader
            << " commitIndex:" << mi.commitIndex
            << " lastLogTerm:" << mi.lastLogTerm
            << " lastLogIndex:" << mi.lastLogIndex << " role:" << mi.role
            << " votedFor:" << mi.votedFor
            << " lastAppliedIndex:" << mi.lastAppliedIndex
            << " currentLeaderAddr:" << mi.currentLeaderAddr << std::endl
            << std::flush;

  paxos2->getMemberInfo(&mi);
  std::cout << "serverId:" << mi.serverId << " currentTerm:" << mi.currentTerm
            << " currentLeader:" << mi.currentLeader
            << " commitIndex:" << mi.commitIndex
            << " lastLogTerm:" << mi.lastLogTerm
            << " lastLogIndex:" << mi.lastLogIndex << " role:" << mi.role
            << " votedFor:" << mi.votedFor
            << " lastAppliedIndex:" << mi.lastAppliedIndex
            << " currentLeaderAddr:" << mi.currentLeaderAddr << std::endl
            << std::flush;

  strConfig.clear();
  strConfig.push_back(strTmp3);
  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  sleep(1);

  EXPECT_EQ(0, paxos1->changeMember(Paxos::CCAddNode, strTmp3));
  sleep(1);
  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);

  EXPECT_EQ(0, paxos1->downgradeMember(3));
  sleep(1);
  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);

  paxos3->forceSingleLeader();
  sleep(1);
  le.clear_term();
  paxos3->replicateLog(le);
  aserver1->writeLogDone(paxos3->getLastLogIndex());
  sleep(1);
  EXPECT_EQ(paxos3->getCommitIndex(), paxos3->getLastLogIndex());
  sleep(1);

  failCnt.store(1, std::memory_order_relaxed);
  paxos1->replicateLog(le);

  sleep(1);

  /*
  sleep(1);
  paxos1->replicateLog(le);
  std::cout<< "countMsgAppendLog:"<<stats.countMsgAppendLog <<"
  countHeartbeat:"<< stats.countHeartbeat<<  std::endl<< std::flush; sleep(1);
  paxos1->replicateLog(le);
  std::cout<< "countMsgAppendLog:"<<stats.countMsgAppendLog <<"
  countHeartbeat:"<< stats.countHeartbeat<<  std::endl<< std::flush;
  EXPECT_EQ(stats.countHeartbeat, 0);
  sleep(3);
  std::cout<< "countMsgAppendLog:"<<stats.countMsgAppendLog << "
  countHeartbeat:"<< stats.countHeartbeat<< std::endl<< std::flush;
  EXPECT_EQ(stats.countHeartbeat, 2);

  */

  delete paxos1;
  delete paxos2;
  delete paxos3;
  delete cs1;
  delete cs2;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

void stateChangeCb2_1(Paxos *p, Paxos::StateType state, uint64_t term,
                      uint64_t commitIndex) {
  static uint64_t cnt = 0;
  std::cout << "Server(" << p->getLocalServer()->strAddr << ") state change to "
            << state << " in stateChangeCb term:" << term
            << " commitIndex:" << commitIndex << std::endl;
  if (cnt == 0) {
    EXPECT_EQ(state, Paxos::CANDIDATE);
    EXPECT_EQ(term, 2);
    EXPECT_EQ(commitIndex, 0);
    EXPECT_EQ(p->getLocalServer()->strAddr, "127.0.0.1:11001");
  } else if (cnt == 1) {
    EXPECT_EQ(state, Paxos::LEADER);
    EXPECT_EQ(term, 2);
    EXPECT_EQ(commitIndex, 0);
    EXPECT_EQ(p->getLocalServer()->strAddr, "127.0.0.1:11001");
  } else if (cnt == 2) {
    EXPECT_EQ(state, Paxos::FOLLOWER);
    EXPECT_EQ(term, 3);
    EXPECT_EQ(commitIndex, 4);
    EXPECT_EQ(p->getLocalServer()->strAddr, "127.0.0.1:11001");
  } else if (cnt == 3) {
    EXPECT_EQ(state, Paxos::FOLLOWER);
    EXPECT_EQ(term, 4);
    EXPECT_EQ(commitIndex, 4);
    EXPECT_EQ(p->getLocalServer()->strAddr, "127.0.0.1:11001");
  }

  ++cnt;
}

void stateChangeCb2_2(Paxos *p, Paxos::StateType state, uint64_t term,
                      uint64_t commitIndex) {
  std::cout << "Callback with 4 arguments" << std::endl;
  EXPECT_EQ(p->getLocalServer()->strAddr, "127.0.0.1:11002");
}

void stateChangeCb2_3_lessarg(Paxos::StateType state, uint64_t term,
                              uint64_t commitIndex) {
  std::cout << "Callback with 3 arguments" << std::endl;
}

// callback1: Paxos_replicateLog
TEST(consensus, callback2) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  uint64_t timeout = 12500;
  std::shared_ptr<PaxosLog> rlog;
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir21", true, 4 * 1024 * 1024);
  auto *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir22", true, 4 * 1024 * 1024);
  auto *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir23", true, 4 * 1024 * 1024);
  auto *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3);

  // Need one reconnect, because the other Paxos is not inited.
  sleep(5);
  paxos1->setStateChangeCb(stateChangeCb2_1);
  paxos2->setStateChangeCb(stateChangeCb2_2);
  paxos3->setStateChangeCb(stateChangeCb2_3_lessarg);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  le.set_ikey("bbb");
  // le.set_value("ccc");
  le.set_value(
      "0123456789 123456789 123456789 123456789 123456789 123456789 123456789 "
      "123456789 123456789 12345");

  const Paxos::StatsType &stats = paxos1->getStats();

  std::thread th1(applyThread, paxos1);
  paxos1->replicateLog(le);
  sleep(1);
  paxos1->replicateLog(le);
  sleep(1);
  paxos1->replicateLog(le);
  EXPECT_EQ(stats.countHeartbeat, 0);
  sleep(3);
  EXPECT_EQ(stats.countHeartbeat, 2);

  printPaxosStats(paxos2);

  paxos2->requestVote();
  sleep(1);
  paxos3->requestVote();
  sleep(1);

  th1.join();

  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir21");
  deleteDir("paxosLogTestDir22");
  deleteDir("paxosLogTestDir23");
}

TEST(consensus, timeout_set) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1;
  rlog1 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir1", true, 4 * 1024 * 1024);
  auto *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir2", true, 4 * 1024 * 1024);
  auto *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir3", true, 4 * 1024 * 1024);
  auto *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3);

  sleep(3);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  std::string strTmp("127.0.0.1:11004");
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir4", true, 4 * 1024 * 1024);
  auto *learner1 = new Paxos(timeout, rlog);
  learner1->initAsLearner(strTmp, 11);
  sleep(1);

  strConfig.clear();
  strConfig.push_back(strTmp);
  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  sleep(2);

  LogEntry le;
  for (int i = 0; i < 10; ++i) {
    le.Clear();
    le.set_optype(1);
    paxos1->replicateLog(le);
  }
  paxos1->setSendPacketTimeout(10000);
  for (int i = 0; i < 10; ++i) {
    le.Clear();
    le.set_optype(1);
    paxos1->replicateLog(le);
  }
  paxos1->setLearnerConnTimeout(1000);
  for (int i = 0; i < 10; ++i) {
    le.Clear();
    le.set_optype(1);
    paxos1->replicateLog(le);
  }

  sleep(2);
  EXPECT_EQ(learner1->getLastLogIndex(), paxos1->getLastLogIndex());
  EXPECT_EQ(paxos2->getLastLogIndex(), paxos1->getLastLogIndex());
  delete learner1;
  delete paxos1;
  delete paxos2;
  delete paxos3;
  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
  deleteDir("paxosLogTestDir4");
}

TEST(consensus, fix_match_index) {
  std::vector<std::string> strConfig = {"127.0.0.1:11001", "127.0.0.1:11002",
                                        "127.0.0.1:11003"};
  uint64_t timeout = 10000;
  std::shared_ptr<PaxosLog> rlog;
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir1", true, 4 * 1024 * 1024);
  auto *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir2", true, 4 * 1024 * 1024);
  auto *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir3", true, 4 * 1024 * 1024);
  auto *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3);

  sleep(1);
  paxos1->requestVote();
  sleep(2);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  /* replicate a log */
  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  le.set_value("12345");
  paxos1->replicateLog(le);

  std::atomic<bool> shutdown(false);
  auto worker_main = [&shutdown](Paxos *p, uint64_t time) {
    while (shutdown.load() == false) {
      LogEntry le;
      le.set_index(0);
      le.set_optype(1);
      le.set_value("12345");
      if (p->getState() == Paxos::LEADER) p->replicateLog(le);
      usleep(time * 1000);
    }
  };
  std::thread writer1(worker_main, paxos1, 100);
  std::thread writer2(worker_main, paxos2, 100);
  std::thread writer3(worker_main, paxos3, 100);

  paxos2->forceFixMatchIndex(2, paxos3->getLastLogIndex() - 2);

  std::thread op1([paxos1, paxos2, paxos3, &shutdown]() {
    sleep(1);
    while (shutdown.load() == false) {
      paxos1->forceFixMatchIndex(2, paxos2->getLastLogIndex() - 2);
      sleep(2);
      paxos1->forceFixMatchIndex(3, paxos3->getLastLogIndex() - 2);
      sleep(2);
    }
  });
  /* how long to test */
  sleep(10);

  shutdown.store(true);
  writer1.join();
  writer2.join();
  writer3.join();
  op1.join();
  paxos1->replicateLog(le);
  paxos2->replicateLog(le);
  paxos3->replicateLog(le);
  sleep(2);
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(paxos2->getLastLogIndex(), paxos3->getLastLogIndex());

  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(consensus, checksum) {
  std::vector<std::string> strConfig = {"127.0.0.1:11001", "127.0.0.1:11002",
                                        "127.0.0.1:11003"};
  uint64_t timeout = 10000;
  std::shared_ptr<PaxosLog> rlog;
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir1", true, 4 * 1024 * 1024);
  auto *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir2", true, 4 * 1024 * 1024);
  auto *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir3", true, 4 * 1024 * 1024);
  auto *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3);

  sleep(1);
  paxos1->requestVote();
  sleep(2);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  /* replicate a log */
  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  le.set_value("12345");
  paxos1->replicateLog(le);
  sleep(1);
  std::function<uint32_t(uint32_t, const unsigned char *, size_t)>
      checksumFunc = [](uint32_t base, const unsigned char *buf,
                        size_t len) -> uint32_t { return base + len; };
  paxos1->setChecksumCb(checksumFunc);
  paxos2->setChecksumCb(checksumFunc);
  paxos3->setChecksumCb(checksumFunc);
  paxos1->setChecksumMode(true);
  paxos2->setChecksumMode(true);
  paxos3->setChecksumMode(true);
  std::cout << "enable checksum..." << std::endl;
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(paxos2->getLastLogIndex(), paxos3->getLastLogIndex());
  le.set_index(0);
  le.set_value("12345");
  le.set_checksum(0);
  paxos1->replicateLog(le);
  sleep(1);
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(paxos2->getLastLogIndex(), paxos3->getLastLogIndex());
  le.set_index(0);
  le.set_value("12345");
  le.set_checksum(6);  // 5 is correct
  paxos1->replicateLog(le);
  for (int i = 0; i < 10; ++i) {
    le.set_index(0);
    le.set_value("12345");
    le.set_checksum(0);
    paxos1->replicateLog(le);
  }
  sleep(1);
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos2->getLastLogIndex() + 11);
  EXPECT_EQ(paxos2->getLastLogIndex(), paxos3->getLastLogIndex());
  std::cout << "disable checksum..." << std::endl;
  paxos1->setChecksumMode(false);
  paxos2->setChecksumMode(false);
  paxos3->setChecksumMode(false);
  sleep(5);
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(paxos2->getLastLogIndex(), paxos3->getLastLogIndex());

  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(consensus, update_commitindex_in_leader_transfer) {
  std::vector<std::string> strConfig = {"127.0.0.1:11001", "127.0.0.1:11002",
                                        "127.0.0.1:11003"};
  uint64_t timeout = 10000;
  std::shared_ptr<PaxosLog> rlog;
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir1", true, 4 * 1024 * 1024);
  auto *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir2", true, 4 * 1024 * 1024);
  auto *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir3", true, 4 * 1024 * 1024);
  auto *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3);

  sleep(1);
  paxos1->requestVote();
  sleep(2);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  /* replicate a log */
  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  le.set_value("12345");
  paxos1->replicateLog(le);

  for (int i = 0; i < 10; ++i) paxos1->replicateLog(le);
  while (paxos1->getLastLogIndex() != paxos2->getLastLogIndex()) sleep(1);
  paxos2->debugSkipUpdateCommitIndex = true;

  strConfig.clear();
  strConfig.push_back("127.0.0.1:11004");
  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  for (int i = 0; i < 10; ++i) paxos1->replicateLog(le);
  while (paxos1->getLastLogIndex() != paxos2->getLastLogIndex()) sleep(1);
  EXPECT_EQ(paxos1->getCommitIndex(), paxos2->getLastLogIndex());
  EXPECT_NE(paxos1->getCommitIndex(), paxos2->getCommitIndex());
  paxos1->leaderTransfer(2);
  sleep(2);
  EXPECT_EQ(paxos2->getState(), Paxos::LEADER);
  paxos2->debugSkipUpdateCommitIndex = false;
  while (paxos1->getLastLogIndex() != paxos2->getLastLogIndex()) sleep(1);
  sleep(5);
  EXPECT_EQ(paxos1->getCommitIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(paxos1->getCommitIndex(), paxos2->getCommitIndex());

  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(consensus, flow_control) {
  std::vector<std::string> strConfig = {"127.0.0.1:11001", "127.0.0.1:11002",
                                        "127.0.0.1:11003"};
  uint64_t timeout = 5000;  // 1s heartbeat
  std::shared_ptr<PaxosLog> rlog;
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir1", true, 4 * 1024 * 1024);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir2", true, 4 * 1024 * 1024);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir3", true, 4 * 1024 * 1024);
  Paxos *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3);

  sleep(1);
  paxos1->requestVote();
  sleep(2);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  /* replicate a log */
  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  le.set_value("12345");
  paxos1->replicateLog(le);

  for (int i = 0; i < 4; ++i) paxos1->replicateLog(le);
  sleep(2);
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos2->getLastLogIndex());

  uint64_t mps = paxos1->getMaxPacketSize();
  paxos1->setMaxPacketSize(1);
  paxos1->set_flow_control(2, -1);
  paxos1->set_flow_control(3, -2);
  for (int i = 0; i < 4; ++i) paxos1->replicateLog(le);
  sleep(2);
  EXPECT_NE(paxos1->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_NE(paxos1->getLastLogIndex(), paxos3->getLastLogIndex());
  sleep(6);
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_NE(paxos1->getLastLogIndex(), paxos3->getLastLogIndex());

  paxos1->setMaxPacketSize(mps);
  paxos1->set_flow_control(3, 0);
  for (int i = 0; i < 4; ++i) paxos1->replicateLog(le);
  sleep(2);
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos3->getLastLogIndex());

  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(consensus, memory_usage_count) {
  std::vector<std::string> strConfig = {"127.0.0.1:11001", "127.0.0.1:11002",
                                        "127.0.0.1:11003"};
  uint64_t timeout = 5000;  // 1s heartbeat
  std::shared_ptr<PaxosLog> rlog, rlog1;
  rlog1 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir1", true, 4 * 1024 * 1024);
  Paxos *paxos1 = new Paxos(timeout, rlog1);
  paxos1->init(strConfig, 1, 1, NULL, 4, 4, NULL, true);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir2", true, 4 * 1024 * 1024);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2, NULL, 4, 4, NULL, true);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir3", true, 4 * 1024 * 1024);
  Paxos *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3, NULL, 4, 4, NULL, true);

  sleep(1);
  paxos1->requestVote();
  sleep(2);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  /* replicate a log */
  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  le.set_value("12345");
  paxos1->replicateLog(le);

  for (int i = 0; i < 4; ++i) paxos1->replicateLog(le);
  sleep(2);
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos2->getLastLogIndex());
  // memory usage not equal to 0
  EXPECT_NE(easy_pool_alloc_byte, 0);
  std::cout << "easy_pool_alloc_byte: " << easy_pool_alloc_byte << std::endl;

  delete paxos1;
  delete paxos2;
  delete paxos3;

  // memory usage go back to 0
  EXPECT_EQ(easy_pool_alloc_byte, 0);
  std::cout << "easy_pool_alloc_byte: " << easy_pool_alloc_byte << std::endl;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(consensus, paxos_force_promote) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  uint64_t timeout = 10000;
  std::shared_ptr<PaxosLog> rlog;
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir1", true, 4 * 1024 * 1024);
  Paxos *paxos1 = new Paxos(timeout, rlog, 3000);
  paxos1->init(strConfig, 1, 1);
  paxos1->initAutoPurgeLog(false, false, NULL);  // disable autoPurge
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir2", true, 4 * 1024 * 1024);
  Paxos *paxos2 = new Paxos(timeout, rlog, 3000);
  paxos2->init(strConfig, 2, 2);
  paxos2->initAutoPurgeLog(false, false, NULL);  // disable autoPurge
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir3", true, 4 * 1024 * 1024);
  Paxos *paxos3 = new Paxos(timeout, rlog, 3000);
  paxos3->init(strConfig, 3, 3);

  std::string learnerAddr = "127.0.0.1:11004";
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir4", true, 4 * 1024 * 1024);
  Paxos *paxos4 = new Paxos(timeout, rlog, 3000);
  paxos4->initAsLearner(learnerAddr, 14);

  sleep(3);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_value("aaa");
  paxos1->replicateLog(le);

  sleep(1);

  std::vector<std::string> vec;
  vec.push_back(learnerAddr);
  paxos1->changeLearners(Paxos::CCAddNode, vec);
  sleep(1);
  EXPECT_EQ(paxos1->getConfig()->learnersToString(),
            std::string("127.0.0.1:11004$00"));

  /* learner forcePromote: do nothing */
  paxos4->forcePromote();
  sleep(1);
  EXPECT_EQ(paxos4->getState(), Paxos::LEARNER);

  /* follower forcePromote: become new leader */
  paxos2->forcePromote();
  sleep(1);
  EXPECT_EQ(paxos2->getState(), Paxos::LEADER);
  EXPECT_EQ(paxos1->getState(), Paxos::FOLLOWER);
  delete paxos1;
  delete paxos2;
  delete paxos3;
  delete paxos4;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
  deleteDir("paxosLogTestDir4");
}

TEST(consensus, extra_info) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");
  uint64_t timeout = 5000;
  std::shared_ptr<PaxosLog> rlog;
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir1", true, 4 * 1024 * 1024);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir2", true, 4 * 1024 * 1024);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir3", true, 4 * 1024 * 1024);
  Paxos *paxos3 = new Paxos(timeout, rlog);

  class MockExtraStore : public ExtraStore {
   public:
    MockExtraStore(uint p) : port(p), isLeader(0) {}
    virtual std::string getRemote() { return data; }
    virtual void setRemote(const std::string &d) { data = d; }
    /* local info to send to others */
    virtual std::string getLocal() {
      return isLeader ? std::to_string(port) : "";
    }

    uint getRemotePort() { return data != "" ? stoul(data) : 0; }
    void setIsLeader(bool b) { isLeader = b; }

   private:
    uint port;
    std::string data;
    bool isLeader; /* maintained by rds server level */
  };

  paxos1->init(strConfig, 1, 1);
  paxos2->init(strConfig, 2, 2);
  paxos3->init(strConfig, 3, 3);
  paxos1->setExtraStore(
      std::make_shared<MockExtraStore>(paxos1->getPort() - 8000));
  paxos2->setExtraStore(
      std::make_shared<MockExtraStore>(paxos2->getPort() - 8000));
  paxos3->setExtraStore(
      std::make_shared<MockExtraStore>(paxos3->getPort() - 8000));

  sleep(3);
  paxos1->requestVote(1);
  sleep(1);
  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  std::dynamic_pointer_cast<MockExtraStore>(paxos1->getExtraStore())
      ->setIsLeader(1);
  std::dynamic_pointer_cast<MockExtraStore>(paxos2->getExtraStore())
      ->setIsLeader(0);
  std::dynamic_pointer_cast<MockExtraStore>(paxos3->getExtraStore())
      ->setIsLeader(0);
  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_value("aaa");
  paxos1->replicateLog(le);
  sleep(1);

  /* every follower will have port-8000 from leader paxos1 */
  EXPECT_EQ(std::dynamic_pointer_cast<MockExtraStore>(paxos1->getExtraStore())
                ->getRemotePort(),
            0);
  EXPECT_EQ(std::dynamic_pointer_cast<MockExtraStore>(paxos2->getExtraStore())
                ->getRemotePort(),
            paxos1->getPort() - 8000);
  EXPECT_EQ(std::dynamic_pointer_cast<MockExtraStore>(paxos3->getExtraStore())
                ->getRemotePort(),
            paxos1->getPort() - 8000);

  paxos2->requestVote(1);
  sleep(1);
  EXPECT_EQ(paxos2->getState(), Paxos::LEADER);
  std::dynamic_pointer_cast<MockExtraStore>(paxos1->getExtraStore())
      ->setIsLeader(0);
  std::dynamic_pointer_cast<MockExtraStore>(paxos2->getExtraStore())
      ->setIsLeader(1);
  std::dynamic_pointer_cast<MockExtraStore>(paxos3->getExtraStore())
      ->setIsLeader(0);
  paxos2->replicateLog(le);
  sleep(1);

  /* every follower will have port-8000 from new leader paxos2 */
  EXPECT_EQ(std::dynamic_pointer_cast<MockExtraStore>(paxos1->getExtraStore())
                ->getRemotePort(),
            paxos2->getPort() - 8000);
  EXPECT_EQ(std::dynamic_pointer_cast<MockExtraStore>(paxos2->getExtraStore())
                ->getRemotePort(),
            0);
  EXPECT_EQ(std::dynamic_pointer_cast<MockExtraStore>(paxos3->getExtraStore())
                ->getRemotePort(),
            paxos2->getPort() - 8000);

  delete paxos1;
  delete paxos2;
  delete paxos3;
  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}
