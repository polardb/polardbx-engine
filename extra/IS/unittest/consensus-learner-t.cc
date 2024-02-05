/************************************************************************
 *
 * Copyright (c) 2017 Alibaba.com, Inc. All Rights Reserved
 * $Id:  consensus1-t.cc,v 1.0 03/21/2017 09:19:25 PM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file consensus-learner-t.cc
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 03/21/2017 09:19:25 PM
 * @version 1.0
 * @brief
 *
 **/

#include <gtest/gtest.h>
#include <atomic>
#include <thread>
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

TEST(consensus, learnerSource) {
  std::vector<std::string> strConfig;
  LogEntry le;
  le.set_term(1);
  le.set_index(1);
  le.set_optype(kMock);
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");
  std::string strTmp3("127.0.0.1:11004");
  std::string strTmp4("127.0.0.1:11005");
  std::string strTmp5("127.0.0.1:11006");

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1;
  rlog1 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir11", true, 4 * 1024 * 1024);
  // We init log with mock log in index 1 to make error happend when we send log
  // entry 1 to learner!
  rlog->append(le);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir12", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir13", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3);

  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir14", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos4 = new Paxos(timeout, rlog);
  paxos4->initAsLearner(strTmp3, 4);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir15", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos5 = new Paxos(timeout, rlog);
  paxos5->initAsLearner(strTmp4, 5);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir16", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos6 = new Paxos(timeout, rlog);
  paxos6->initAsLearner(strTmp5, 6);

  const Paxos::StatsType &stats = paxos2->getStats();

  sleep(3);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  strConfig.clear();
  strConfig.push_back(strTmp3);
  strConfig.push_back(strTmp4);

  paxos1->changeLearners(Paxos::CCAddNode, strConfig);

  sleep(1);

  EXPECT_EQ(paxos1->getConfig()->membersToString(("127.0.0.1:11002")),
            std::string(
                "127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#5N@2"));
  EXPECT_EQ(paxos1->getConfig()->learnersToString(),
            std::string("127.0.0.1:11004$00;127.0.0.1:11005$00"));

  /* extra safety test */
  EXPECT_EQ(paxos1->configureMember(100, true, 9), PE_WEIGHTLEARNER);
  EXPECT_STREQ(pxserror(paxos1->configureMember("127.0.0.1:11004", true, 9)),
               "Configure forcesync or weight to a learner is not allowed.");

  paxos1->configureLearner(100, 2);

  sleep(1);
  EXPECT_EQ(paxos1->getConfig()->membersToString(("127.0.0.1:11002")),
            std::string(
                "127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#5N@2"));
  EXPECT_EQ(paxos1->getConfig()->learnersToString(),
            std::string("127.0.0.1:11004$02;127.0.0.1:11005$00"));
  EXPECT_EQ(paxos2->getConfig()->learnersToString(),
            std::string("127.0.0.1:11004$02;127.0.0.1:11005$00"));
  EXPECT_EQ(paxos2->getConfig()->getServer(100)->learnerSource, 2);
  /* test getServerIdFromAddr */
  EXPECT_EQ(paxos1->getServerIdFromAddr("127.0.0.1:11001"), 1);
  EXPECT_EQ(paxos1->getServerIdFromAddr("127.0.0.1:11002"), 2);
  EXPECT_EQ(paxos1->getServerIdFromAddr("127.0.0.1:11003"), 3);
  EXPECT_EQ(paxos1->getServerIdFromAddr("127.0.0.1:11004"), 100);
  EXPECT_EQ(paxos1->getServerIdFromAddr("127.0.0.1:11005"), 101);
  paxos2->clearStats();

  le.set_index(0);
  le.set_optype(0);
  le.set_value("aaa");
  paxos1->replicateLog(le);

  sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);

  sleep(2);
  std::cout << "countMsgAppendLog:" << stats.countMsgAppendLog
            << " countMsgRequestVote:" << stats.countMsgRequestVote
            << " countOnMsgAppendLog:" << stats.countOnMsgAppendLog
            << " countHeartbeat:" << stats.countHeartbeat
            << " countOnMsgRequestVote:" << stats.countOnMsgRequestVote
            << " countOnHeartbeat:" << stats.countOnHeartbeat
            << " countReplicateLog:" << stats.countReplicateLog << std::endl
            << std::flush;
  EXPECT_EQ(stats.countMsgAppendLog, 2);

  EXPECT_EQ(paxos4->getCurrentLeader(), 2);
  EXPECT_EQ(paxos5->getCurrentLeader(), 1);

  EXPECT_EQ(paxos4->getLastLogIndex(), paxos1->getLastLogIndex());
  EXPECT_EQ(paxos5->getLastLogIndex(), paxos1->getLastLogIndex());

  std::vector<Paxos::ClusterInfoType> cis;
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(cis[3].learnerSource, 2);
  EXPECT_EQ(cis[4].learnerSource, 0);
  EXPECT_EQ(cis[0].learnerSource, 0);

  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);
  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_TRUE(cis[3].matchIndex <= cis[1].matchIndex);
  EXPECT_TRUE(cis[3].matchIndex >= cis[1].matchIndex - 2);
  EXPECT_TRUE(cis[3].nextIndex <= cis[1].nextIndex);
  EXPECT_TRUE(cis[3].nextIndex >= cis[1].nextIndex - 2);

  paxos1->configureLearner(100, 3);
  sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);
  sleep(2);

  EXPECT_EQ(paxos4->getCurrentLeader(), 3);
  EXPECT_EQ(paxos4->getLastLogIndex(), paxos1->getLastLogIndex());

  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_TRUE(cis[3].matchIndex <= cis[1].matchIndex);
  EXPECT_TRUE(cis[3].matchIndex >= cis[1].matchIndex - 2);
  EXPECT_TRUE(cis[3].nextIndex <= cis[1].nextIndex);
  EXPECT_TRUE(cis[3].nextIndex >= cis[1].nextIndex - 2);

  paxos1->configureLearner(100, 0);
  sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);

  EXPECT_EQ(paxos4->getCurrentLeader(), 1);
  EXPECT_EQ(paxos4->getLastLogIndex(), paxos1->getLastLogIndex());

  // Already fixed! Error may happy when server 1 is leader, and we change
  // learnerSource from 1 to 0.
  paxos1->configureLearner(100, 1);
  sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);

  EXPECT_EQ(paxos4->getCurrentLeader(), 1);
  EXPECT_EQ(paxos4->getLastLogIndex(), paxos1->getLastLogIndex());

  paxos1->configureLearner(100, 0);
  sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);

  EXPECT_EQ(paxos4->getCurrentLeader(), 1);
  EXPECT_EQ(paxos4->getLastLogIndex(), paxos1->getLastLogIndex());

  // set the learnerSource to another learner
  paxos1->configureLearner(100, 101);
  sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);

  EXPECT_EQ(paxos4->getCurrentLeader(), 101);
  EXPECT_EQ(paxos4->getLastLogIndex(), paxos1->getLastLogIndex());

  uint64_t tmpInt = 0;
  std::cout << "Cluster Info for server " << paxos1->getLocalServer()->serverId
            << std::endl
            << std::flush;
  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  tmpInt = cis[0].matchIndex;
  std::cout << "Cluster Info for server " << paxos5->getLocalServer()->serverId
            << std::endl
            << std::flush;
  cis.clear();
  paxos5->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(tmpInt, cis[1].matchIndex);

  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);

  std::cout << "Cluster Info for server " << paxos1->getLocalServer()->serverId
            << std::endl
            << std::flush;
  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_LE(tmpInt, cis[3].matchIndex);
  std::cout << "Cluster Info for server " << paxos5->getLocalServer()->serverId
            << std::endl
            << std::flush;
  cis.clear();
  paxos5->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);

  // check if the new added member has the forceSync electionWeight and
  // learnerSource info.
  paxos1->configureMember(2, false, 8);
  sleep(1);
  paxos1->configureLearner(100, 2);
  sleep(1);

  strConfig.clear();
  strConfig.push_back(strTmp5);

  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  paxos1->changeMember(Paxos::CCAddNode, strTmp5);

  sleep(5);
  EXPECT_EQ(paxos6->getCurrentLeader(), 1);
  EXPECT_EQ(paxos6->getLastLogIndex(), paxos1->getLastLogIndex());

  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  cis.clear();
  paxos6->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(cis[1].electionWeight, 8);
  EXPECT_EQ(cis[4].learnerSource, 2);

  delete paxos6;
  delete paxos5;
  delete paxos4;
  delete paxos3;
  delete paxos2;
  delete paxos1;

  deleteDir("paxosLogTestDir11");
  deleteDir("paxosLogTestDir12");
  deleteDir("paxosLogTestDir13");
  deleteDir("paxosLogTestDir14");
  deleteDir("paxosLogTestDir15");
  deleteDir("paxosLogTestDir16");
}

TEST(consensus, follower2learner) {
  std::vector<std::string> strConfig;
  LogEntry le;
  le.set_term(1);
  le.set_index(1);
  le.set_optype(kMock);
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");
  std::string strTmp3("127.0.0.1:11004");
  std::string strTmp4("127.0.0.1:11005");

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1, rlog4;
  rlog1 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir11", true, 4 * 1024 * 1024);
  // We init log with mock log in index 1 to make error happend when we send log
  // entry 1 to learner!
  rlog->append(le);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir12", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir13", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3);

  rlog4 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir14", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos4 = new Paxos(timeout, rlog);
  paxos4->initAsLearner(strTmp3, 4);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir15", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos5 = new Paxos(timeout, rlog);
  paxos5->initAsLearner(strTmp4, 5);

  le.set_optype(0);

  // const Paxos::StatsType &stats= paxos2->getStats();

  sleep(3);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  strConfig.clear();
  strConfig.push_back(strTmp3);
  strConfig.push_back(strTmp4);

  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  sleep(1);
  EXPECT_EQ(0, paxos1->changeMember(Paxos::CCAddNode, strTmp3));
  sleep(1);
  EXPECT_EQ(0, paxos1->changeMember(Paxos::CCAddNode, strTmp4));
  sleep(1);
  auto config1 =
      std::dynamic_pointer_cast<StableConfiguration>(paxos1->getConfig());
  EXPECT_EQ(config1->membersToString(("127.0.0.1:11001")),
            std::string("127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#"
                        "5N;127.0.0.1:11004#5N;127.0.0.1:11005#5N@1"));
  std::vector<Paxos::ClusterInfoType> cis;
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);

  EXPECT_EQ(0, paxos1->downgradeMember(4));
  sleep(2);
  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  cis.clear();
  paxos2->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  cis.clear();
  paxos4->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(Paxos::LEARNER, paxos4->getState());
  std::string tmpStr;
  rlog4->getMetaData(std::string(Paxos::keyMemberConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("127.0.0.1:11004#5N"));
  rlog4->getMetaData(std::string(Paxos::keyLearnerConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("127.0.0.1:11004$00"));

  sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);

  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  sleep(1);

  EXPECT_EQ(0, paxos1->changeMember(Paxos::CCAddNode, strTmp3));
  sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);
  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  sleep(1);

  EXPECT_EQ(0, paxos1->downgradeMember(4));
  sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);
  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(100, cis[4].serverId);
  EXPECT_EQ("127.0.0.1:11004", cis[4].ipPort);
  sleep(1);

  std::cout << "strTmp3:" << strTmp3 << std::endl << std::flush;
  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(0, paxos1->changeMember(Paxos::CCAddNode, strTmp3));
  sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);
  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  sleep(1);

  // can't downgrade a learner
  EXPECT_EQ(PaxosErrorCode::PE_DOWNGRADLEARNER, paxos1->downgradeMember(100));
  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(5, cis.size());

  // when a node change from follower => learner => follower, this node can't
  // create connect to other node. That means can't became leader.
  sleep(1);
  paxos1->leaderTransfer(4);
  sleep(6);
  EXPECT_EQ(paxos4->getState(), Paxos::LEADER);

  sleep(1);
  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);

  delete paxos5;
  delete paxos4;
  delete paxos3;
  delete paxos2;
  delete paxos1;

  deleteDir("paxosLogTestDir11");
  deleteDir("paxosLogTestDir12");
  deleteDir("paxosLogTestDir13");
  deleteDir("paxosLogTestDir14");
  deleteDir("paxosLogTestDir15");
}

TEST(consensus, downgrade_follower_donnot_recieve_msg) {
  std::vector<std::string> strConfig;
  LogEntry le;
  le.set_term(1);
  le.set_index(1);
  le.set_optype(kMock);
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1;
  rlog1 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir11", true, 4 * 1024 * 1024);
  // We init log with mock log in index 1 to make error happend when we send log
  // entry 1 to learner!
  rlog->append(le);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir12", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir13", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3);

  le.set_optype(0);

  // const Paxos::StatsType &stats= paxos2->getStats();

  sleep(3);
  paxos1->requestVote();
  for (uint i = 0; i < 30; ++i) {
    if (paxos1->getState() != Paxos::LEADER)
      sleep(1);
    else
      break;
  }
  ASSERT_TRUE(paxos1->getState() == Paxos::LEADER);

  std::vector<Paxos::ClusterInfoType> cis;
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);

  ASSERT_TRUE(0 == paxos1->downgradeMember(2));
  auto s100 = paxos1->getConfig()->getServer(100);
  ASSERT_TRUE(s100 != 0);
  s100->disconnect(nullptr);
  paxos2->requestVote(false);

  sleep(1);
  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(cis[2].serverId, 100);
  EXPECT_EQ(cis[1].serverId, 3);
  le.clear_term();
  paxos1->replicateLog(le);

  sleep(2);
  EXPECT_EQ(paxos2->getCurrentLeader(), 1);
  EXPECT_EQ(paxos2->getState(), Paxos::LEARNER);
  EXPECT_EQ(paxos2->getLastLogIndex(), paxos1->getLastLogIndex());

  delete paxos3;
  delete paxos2;
  delete paxos1;

  deleteDir("paxosLogTestDir11");
  deleteDir("paxosLogTestDir12");
  deleteDir("paxosLogTestDir13");
}

TEST(consensus, restart_learner) {
  std::vector<std::string> strConfig;
  std::string tmpStr;
  LogEntry le;
  le.set_term(1);
  le.set_index(1);
  le.set_optype(kMock);
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");
  std::string strTmp3("127.0.0.1:11004");
  std::string strTmp4("127.0.0.1:11005");
  std::vector<Paxos::ClusterInfoType> cis;

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1, rlog4;
  rlog1 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir11", true, 4 * 1024 * 1024);
  // We init log with mock log in index 1 to make error happend when we send log
  // entry 1 to learner!
  rlog->append(le);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir12", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir13", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3);

  rlog4 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir14", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos4 = new Paxos(timeout, rlog);
  paxos4->initAsLearner(strTmp3, 4);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir15", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos5 = new Paxos(timeout, rlog);
  paxos5->initAsLearner(strTmp4, 5);

  le.set_optype(0);
  sleep(3);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  strConfig.clear();
  strConfig.push_back(strTmp3);

  paxos1->changeLearners(Paxos::CCAddNode, strConfig);

  sleep(1);

  EXPECT_EQ(paxos1->getConfig()->membersToString(("127.0.0.1:11002")),
            std::string(
                "127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#5N@2"));
  // EXPECT_EQ(paxos1->getConfig()->learnersToString(),
  // std::string("127.0.0.1:11004$00;127.0.0.1:11005$00"));
  EXPECT_EQ(paxos1->getConfig()->learnersToString(),
            std::string("127.0.0.1:11004$00"));

  sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);

  cis.clear();
  paxos4->getClusterInfo(cis);
  sleep(1);
  Paxos::printClusterInfo(cis);

  std::cout << "11004 restart" << std::endl << std::flush;
  delete paxos4;
  sleep(2);
  paxos4 = new Paxos(timeout, rlog4);
  std::string emptryString("");
  paxos4->initAsLearner(emptryString, 14);
  rlog4->getMetaData(std::string(Paxos::keyLearnerConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("127.0.0.1:11004$00"));
  rlog4->getMetaData(std::string(Paxos::keyMemberConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("127.0.0.1:11004#5N"));
  cis.clear();
  paxos4->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  sleep(2);

  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);
  EXPECT_EQ(paxos4->getLastLogIndex(), paxos1->getLastLogIndex());

  strConfig.clear();
  strConfig.push_back(strTmp4);
  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  sleep(1);

  std::cout << "11004 restart" << std::endl << std::flush;
  delete paxos4;
  sleep(1);
  paxos4 = new Paxos(timeout, rlog4);
  paxos4->initAsLearner(emptryString, 15);
  sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);
  sleep(2);
  rlog4->getMetaData(std::string(Paxos::keyLearnerConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("127.0.0.1:11004$00;127.0.0.1:11005$00"));
  rlog4->getMetaData(std::string(Paxos::keyMemberConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("127.0.0.1:11004#5N"));
  cis.clear();
  paxos4->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);

  delete paxos4;
  delete paxos5;
  delete paxos3;
  delete paxos2;
  delete paxos1;

  deleteDir("paxosLogTestDir11");
  deleteDir("paxosLogTestDir12");
  deleteDir("paxosLogTestDir13");
  deleteDir("paxosLogTestDir14");
  deleteDir("paxosLogTestDir15");
}

TEST(consensus, restart_follower) {
  std::vector<std::string> strConfig;
  std::string tmpStr;
  LogEntry le;
  le.set_term(1);
  le.set_index(1);
  le.set_optype(kMock);
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");
  std::vector<Paxos::ClusterInfoType> cis;

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1, rlog2;
  rlog1 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir11", true, 4 * 1024 * 1024);
  // We init log with mock log in index 1 to make error happend when we send log
  // entry 1 to learner!
  rlog->append(le);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog2 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir12", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir13", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3);

  le.set_optype(0);
  sleep(3);
  paxos3->requestVote();
  sleep(1);

  EXPECT_EQ(paxos3->getState(), Paxos::LEADER);

  EXPECT_EQ(paxos3->getConfig()->membersToString(("127.0.0.1:11002")),
            std::string(
                "127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#5N@2"));
  // EXPECT_EQ(paxos3->getConfig()->learnersToString(),
  // std::string("127.0.0.1:11004$00;127.0.0.1:11005$00"));
  EXPECT_EQ(paxos3->getConfig()->learnersToString(), std::string(""));

  sleep(1);
  le.clear_term();
  paxos3->replicateLog(le);

  cis.clear();
  paxos3->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);

  ASSERT_TRUE(0 == paxos3->downgradeMember(1));
  sleep(1);
  EXPECT_EQ(paxos1->getState(), Paxos::LEARNER);
  cis.clear();
  paxos3->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);

  std::cout << "11002 restart" << std::endl << std::flush;
  delete paxos2;
  sleep(2);
  paxos2 = new Paxos(timeout, rlog2);
  strConfig.clear();
  paxos2->init(strConfig, 0, 2);
  sleep(2);
  rlog2->getMetaData(std::string(Paxos::keyLearnerConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("127.0.0.1:11001$00"));
  rlog2->getMetaData(std::string(Paxos::keyMemberConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("0;127.0.0.1:11002#5N;127.0.0.1:11003#5N@2"));
  cis.clear();
  paxos2->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  sleep(2);

  le.clear_term();
  paxos3->replicateLog(le);
  sleep(1);

  delete paxos3;
  delete paxos2;
  delete paxos1;

  deleteDir("paxosLogTestDir11");
  deleteDir("paxosLogTestDir12");
  deleteDir("paxosLogTestDir13");
}

TEST(consensus, configureChange_affect_learner_meta) {
  std::vector<std::string> strConfig;
  LogEntry le;
  le.set_term(1);
  le.set_index(1);
  le.set_optype(kMock);
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");
  std::string strTmp3("127.0.0.1:11004");
  std::string strTmp4("127.0.0.1:11005");
  std::string strTmp6("127.0.0.1:11006");

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1, rlog2, rlog4, rlog5, rlog6;
  rlog1 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir11", true, 4 * 1024 * 1024);
  // We init log with mock log in index 1 to make error happend when we send log
  // entry 1 to learner!
  rlog->append(le);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog2 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir12", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir13", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3);

  rlog4 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir14", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos4 = new Paxos(timeout, rlog);
  paxos4->initAsLearner(strTmp3, 4);
  rlog5 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir15", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos5 = new Paxos(timeout, rlog);
  paxos5->initAsLearner(strTmp4, 5);
  rlog6 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir16", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos6 = new Paxos(timeout, rlog);
  paxos6->initAsLearner(strTmp6, 6);

  le.set_optype(0);

  // const Paxos::StatsType &stats= paxos2->getStats();

  sleep(3);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  strConfig.clear();
  strConfig.push_back(strTmp3);
  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  sleep(1);

  strConfig.clear();
  strConfig.push_back(strTmp4);
  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  sleep(1);

  auto config1 =
      std::dynamic_pointer_cast<StableConfiguration>(paxos1->getConfig());
  EXPECT_EQ(config1->membersToString(("127.0.0.1:11001")),
            std::string(
                "127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#5N@1"));
  EXPECT_EQ(config1->learnersToString(),
            std::string("127.0.0.1:11004$00;127.0.0.1:11005$00"));
  std::vector<Paxos::ClusterInfoType> cis;
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  std::string tmpStr;
  rlog4->getMetaData(std::string(Paxos::keyMemberConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("127.0.0.1:11004#5N"));
  rlog4->getMetaData(std::string(Paxos::keyLearnerConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("127.0.0.1:11004$00;127.0.0.1:11005$00"));

  // follower -> learner
  EXPECT_EQ(0, paxos1->downgradeMember(2));
  sleep(2);
  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);

  std::cout << "s1:" << std::endl << std::flush;
  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  std::cout << "s2:" << std::endl << std::flush;
  cis.clear();
  paxos2->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  cis.clear();
  std::cout << "s4:" << std::endl << std::flush;
  paxos4->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(Paxos::LEARNER, paxos4->getState());
  rlog4->getMetaData(std::string(Paxos::keyMemberConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("127.0.0.1:11004#5N"));
  rlog2->getMetaData(std::string(Paxos::keyLearnerConfigure), tmpStr);
  EXPECT_EQ(
      tmpStr,
      std::string("127.0.0.1:11004$00;127.0.0.1:11005$00;127.0.0.1:11002$00"));
  rlog4->getMetaData(std::string(Paxos::keyLearnerConfigure), tmpStr);
  EXPECT_EQ(
      tmpStr,
      std::string("127.0.0.1:11004$00;127.0.0.1:11005$00;127.0.0.1:11002$00"));
  rlog5->getMetaData(std::string(Paxos::keyLearnerConfigure), tmpStr);
  EXPECT_EQ(
      tmpStr,
      std::string("127.0.0.1:11004$00;127.0.0.1:11005$00;127.0.0.1:11002$00"));

  // learner -> follower
  EXPECT_EQ(0, paxos1->changeMember(Paxos::CCAddNode, strTmp3));
  sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);

  std::cout << "s1:" << std::endl << std::flush;
  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  std::cout << "s2:" << std::endl << std::flush;
  cis.clear();
  paxos2->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  cis.clear();
  std::cout << "s4:" << std::endl << std::flush;
  paxos4->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(Paxos::FOLLOWER, paxos4->getState());
  rlog4->getMetaData(std::string(Paxos::keyMemberConfigure), tmpStr);
  EXPECT_EQ(tmpStr,
            std::string(
                "127.0.0.1:11001#5N;127.0.0.1:11004#5N;127.0.0.1:11003#5N@2"));
  rlog2->getMetaData(std::string(Paxos::keyLearnerConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("0;127.0.0.1:11005$00;127.0.0.1:11002$00"));
  rlog4->getMetaData(std::string(Paxos::keyLearnerConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("0;127.0.0.1:11005$00;127.0.0.1:11002$00"));
  rlog5->getMetaData(std::string(Paxos::keyLearnerConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("0;127.0.0.1:11005$00;127.0.0.1:11002$00"));

  // follower -> learner
  EXPECT_EQ(0, paxos1->downgradeMember(3));
  sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);

  std::cout << "s1:" << std::endl << std::flush;
  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  std::cout << "s2:" << std::endl << std::flush;
  cis.clear();
  paxos2->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  cis.clear();
  std::cout << "s4:" << std::endl << std::flush;
  paxos4->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(Paxos::FOLLOWER, paxos4->getState());
  rlog4->getMetaData(std::string(Paxos::keyMemberConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("127.0.0.1:11001#5N;127.0.0.1:11004#5N@2"));
  rlog2->getMetaData(std::string(Paxos::keyLearnerConfigure), tmpStr);
  EXPECT_EQ(
      tmpStr,
      std::string("127.0.0.1:11003$00;127.0.0.1:11005$00;127.0.0.1:11002$00"));
  rlog4->getMetaData(std::string(Paxos::keyLearnerConfigure), tmpStr);
  EXPECT_EQ(
      tmpStr,
      std::string("127.0.0.1:11003$00;127.0.0.1:11005$00;127.0.0.1:11002$00"));
  rlog5->getMetaData(std::string(Paxos::keyLearnerConfigure), tmpStr);
  EXPECT_EQ(
      tmpStr,
      std::string("127.0.0.1:11003$00;127.0.0.1:11005$00;127.0.0.1:11002$00"));

  // configureLearner
  paxos1->configureLearner(100, 102);
  for (int i = 0; i < 7; ++i) {
    // add 103-109
    strConfig.clear();
    strConfig.push_back("127.0.0.1:1200" + std::to_string(i));
    paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  }
  // add 110
  strConfig.clear();
  strConfig.push_back(strTmp6);
  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  for (int i = 0; i < 7; ++i) {
    // delete 103-109
    strConfig.clear();
    strConfig.push_back("127.0.0.1:1200" + std::to_string(i));
    paxos1->changeLearners(Paxos::CCDelNode, strConfig);
  }
  paxos1->configureLearner(101, 110);
  sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);

  std::cout << "s1:" << std::endl << std::flush;
  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  std::cout << "s2:" << std::endl << std::flush;
  cis.clear();
  paxos2->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  cis.clear();
  std::cout << "s4:" << std::endl << std::flush;
  paxos4->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(Paxos::FOLLOWER, paxos4->getState());
  rlog4->getMetaData(std::string(Paxos::keyMemberConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("127.0.0.1:11001#5N;127.0.0.1:11004#5N@2"));
  rlog2->getMetaData(std::string(Paxos::keyLearnerConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("127.0.0.1:11003$:2;127.0.0.1:11005$;0;127.0.0."
                                "1:11002$00;0;0;0;0;0;0;0;127.0.0.1:11006$00"));
  rlog4->getMetaData(std::string(Paxos::keyLearnerConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("127.0.0.1:11003$:2;127.0.0.1:11005$;0;127.0.0."
                                "1:11002$00;0;0;0;0;0;0;0;127.0.0.1:11006$00"));
  rlog5->getMetaData(std::string(Paxos::keyLearnerConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("127.0.0.1:11003$:2;127.0.0.1:11005$;0;127.0.0."
                                "1:11002$00;0;0;0;0;0;0;0;127.0.0.1:11006$00"));

  // restart learner
  std::cout << "11005 restart" << std::endl << std::flush;
  delete paxos5;
  sleep(2);
  paxos5 = new Paxos(timeout, rlog5);
  std::string emptryString("");
  paxos5->initAsLearner(emptryString, 15);
  rlog5->getMetaData(std::string(Paxos::keyLearnerConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("127.0.0.1:11003$:2;127.0.0.1:11005$;0;127.0.0."
                                "1:11002$00;0;0;0;0;0;0;0;127.0.0.1:11006$00"));
  rlog5->getMetaData(std::string(Paxos::keyMemberConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("127.0.0.1:11005#5N"));
  cis.clear();
  std::cout << "s5:" << std::endl << std::flush;
  paxos5->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  sleep(2);

  delete paxos6;
  delete paxos5;
  delete paxos4;
  delete paxos3;
  delete paxos2;
  delete paxos1;

  deleteDir("paxosLogTestDir11");
  deleteDir("paxosLogTestDir12");
  deleteDir("paxosLogTestDir13");
  deleteDir("paxosLogTestDir14");
  deleteDir("paxosLogTestDir15");
  deleteDir("paxosLogTestDir16");
}

TEST(consensus, learner_old_meta_new_version) {
  std::vector<std::string> strConfig;
  std::string tmpStr;
  LogEntry le;
  le.set_term(1);
  le.set_index(1);
  le.set_optype(kMock);
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");
  std::string strTmp3("127.0.0.1:11004");
  std::vector<Paxos::ClusterInfoType> cis;

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1, rlog4;
  rlog1 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir11", true, 4 * 1024 * 1024);
  // We init log with mock log in index 1 to make error happend when we send log
  // entry 1 to learner!
  rlog->append(le);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir12", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir13", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3);

  rlog4 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir14", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos4 = new Paxos(timeout, rlog);
  paxos4->initAsLearner(strTmp3, 4);

  le.set_optype(0);
  sleep(3);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  strConfig.clear();
  strConfig.push_back(strTmp3);

  paxos1->changeLearners(Paxos::CCAddNode, strConfig);

  sleep(1);

  EXPECT_EQ(paxos1->getConfig()->membersToString(("127.0.0.1:11002")),
            std::string(
                "127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#5N@2"));
  EXPECT_EQ(paxos1->getConfig()->learnersToString(),
            std::string("127.0.0.1:11004$00"));

  sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);

  cis.clear();
  paxos4->getClusterInfo(cis);
  sleep(1);
  Paxos::printClusterInfo(cis);

  std::cout << "11004 restart" << std::endl << std::flush;
  delete paxos4;
  sleep(2);

  // change meta to old format
  rlog4->getMetaData(std::string(Paxos::keyMemberConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("127.0.0.1:11004#5N"));
  rlog4->getMetaData(std::string(Paxos::keyLearnerConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("127.0.0.1:11004$00"));
  rlog4->setMetaData(std::string(Paxos::keyMemberConfigure), std::string(""));

  paxos4 = new Paxos(timeout, rlog4);
  std::string emptryString("");
  paxos4->initAsLearner(emptryString, 14);
  sleep(2);
  rlog4->getMetaData(std::string(Paxos::keyLearnerConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string(""));
  rlog4->getMetaData(std::string(Paxos::keyMemberConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("127.0.0.1:11004#5N"));
  cis.clear();
  paxos4->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  sleep(2);

  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);
  EXPECT_EQ(paxos4->getLastLogIndex(), paxos1->getLastLogIndex());

  delete paxos4;
  delete paxos3;
  delete paxos2;
  delete paxos1;

  deleteDir("paxosLogTestDir11");
  deleteDir("paxosLogTestDir12");
  deleteDir("paxosLogTestDir13");
  deleteDir("paxosLogTestDir14");
}

TEST(consensus, SyncLearnerAll) {
  std::vector<std::string> strConfig;
  LogEntry le;
  le.set_term(1);
  le.set_index(1);
  le.set_optype(kMock);
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");
  std::string strTmp3("127.0.0.1:11004");
  std::string strTmp4("127.0.0.1:11005");
  std::string strTmp5("127.0.0.1:11006");

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1, rlog6;
  rlog1 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir11", true, 4 * 1024 * 1024);
  // We init log with mock log in index 1 to make error happend when we send log
  // entry 1 to learner!
  rlog->append(le);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir12", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir13", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3);

  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir14", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos4 = new Paxos(timeout, rlog);
  paxos4->initAsLearner(strTmp3, 4);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir15", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos5 = new Paxos(timeout, rlog);
  paxos5->initAsLearner(strTmp4, 5);
  rlog6 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir16", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos6 = new Paxos(timeout, rlog);
  paxos6->initAsLearner(strTmp5, 6);

  const Paxos::StatsType &stats = paxos2->getStats();

  sleep(3);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  strConfig.clear();
  strConfig.push_back(strTmp3);
  strConfig.push_back(strTmp4);

  paxos1->changeLearners(Paxos::CCAddNode, strConfig);

  sleep(1);

  std::vector<Paxos::ClusterInfoType> cis;
  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(paxos1->getConfig()->membersToString(("127.0.0.1:11002")),
            std::string(
                "127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#5N@2"));
  EXPECT_EQ(paxos1->getConfig()->learnersToString(),
            std::string("127.0.0.1:11004$00;127.0.0.1:11005$00"));

  le.set_index(0);
  le.set_optype(0);
  le.set_value("aaa");
  paxos1->replicateLog(le);
  sleep(1);

  auto lli = paxos1->getLastLogIndex();
  le.set_optype(kMock);
  for (uint i = 2; i <= lli - 1; ++i) {
    le.set_index(2);
    rlog6->append(le);
  }
  le.set_optype(0);
  EXPECT_EQ(lli - 1, rlog6->getLastLogIndex());

  strConfig.clear();
  strConfig.push_back(strTmp5);
  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  sleep(2);
  EXPECT_EQ(
      paxos6->getConfig()->learnersToString(),
      std::string("127.0.0.1:11004$00;127.0.0.1:11005$00;127.0.0.1:11006$00"));

  /* for old added learner to sync meta */
  std::vector<std::string> tmp;
  paxos1->changeLearners(Paxos::CCSyncLearnerAll, tmp);
  sleep(2);
  EXPECT_EQ(
      paxos6->getConfig()->learnersToString(),
      std::string("127.0.0.1:11004$00;127.0.0.1:11005$00;127.0.0.1:11006$00"));

  /*
  paxos1->configureLearner(100, 2);

  sleep(1);
  EXPECT_EQ(paxos1->getConfig()->membersToString(("127.0.0.1:11002")),
  std::string("127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#5N@2"));
  EXPECT_EQ(paxos1->getConfig()->learnersToString(),
  std::string("127.0.0.1:11004$02;127.0.0.1:11005$00"));
  EXPECT_EQ(paxos2->getConfig()->learnersToString(),
  std::string("127.0.0.1:11004$02;127.0.0.1:11005$00"));
  EXPECT_EQ(paxos2->getConfig()->getServer(100)->learnerSource, 2);
  paxos2->clearStats();

  le.set_index(0);
  le.set_optype(0);
  le.set_value("aaa");
  paxos1->replicateLog(le);

  sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);

  sleep(2);
  std::cout<< "countMsgAppendLog:"<<stats.countMsgAppendLog<< "
  countMsgRequestVote:"<<stats.countMsgRequestVote<< " countOnMsgAppendLog:"<<
  stats.countOnMsgAppendLog<< " countHeartbeat:"<< stats.countHeartbeat << "
  countOnMsgRequestVote:"<<stats.countOnMsgRequestVote<< "
  countOnHeartbeat:"<<stats.countOnHeartbeat<< "
  countReplicateLog:"<<stats.countReplicateLog<< std::endl<< std::flush;
  EXPECT_EQ(stats.countMsgAppendLog, 2);

  EXPECT_EQ(paxos4->getCurrentLeader(), 2);
  EXPECT_EQ(paxos5->getCurrentLeader(), 1);

  EXPECT_EQ(paxos4->getLastLogIndex(), paxos1->getLastLogIndex());
  EXPECT_EQ(paxos5->getLastLogIndex(), paxos1->getLastLogIndex());

  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(cis[3].learnerSource, 2);
  EXPECT_EQ(cis[4].learnerSource, 0);
  EXPECT_EQ(cis[0].learnerSource, 0);

  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);
  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_TRUE(cis[3].matchIndex <= cis[1].matchIndex);
  EXPECT_TRUE(cis[3].matchIndex >= cis[1].matchIndex - 2);
  EXPECT_TRUE(cis[3].nextIndex <= cis[1].nextIndex);
  EXPECT_TRUE(cis[3].nextIndex >= cis[1].nextIndex - 2);

  paxos1->configureLearner(100, 3);
  sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);
  sleep(2);

  EXPECT_EQ(paxos4->getCurrentLeader(), 3);
  EXPECT_EQ(paxos4->getLastLogIndex(), paxos1->getLastLogIndex());

  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_TRUE(cis[3].matchIndex <= cis[1].matchIndex);
  EXPECT_TRUE(cis[3].matchIndex >= cis[1].matchIndex - 2);
  EXPECT_TRUE(cis[3].nextIndex <= cis[1].nextIndex);
  EXPECT_TRUE(cis[3].nextIndex >= cis[1].nextIndex - 2);

  paxos1->configureLearner(100, 0);
  sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);

  EXPECT_EQ(paxos4->getCurrentLeader(), 1);
  EXPECT_EQ(paxos4->getLastLogIndex(), paxos1->getLastLogIndex());

  // Already fixed! Error may happy when server 1 is leader, and we change
  learnerSource from 1 to 0. paxos1->configureLearner(100, 1); sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);

  EXPECT_EQ(paxos4->getCurrentLeader(), 1);
  EXPECT_EQ(paxos4->getLastLogIndex(), paxos1->getLastLogIndex());

  paxos1->configureLearner(100, 0);
  sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);

  EXPECT_EQ(paxos4->getCurrentLeader(), 1);
  EXPECT_EQ(paxos4->getLastLogIndex(), paxos1->getLastLogIndex());

  // set the learnerSource to another learner
  paxos1->configureLearner(100, 101);
  sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);

  EXPECT_EQ(paxos4->getCurrentLeader(), 101);
  EXPECT_EQ(paxos4->getLastLogIndex(), paxos1->getLastLogIndex());

  uint64_t tmpInt= 0;
  std::cout<<"Cluster Info for server
  "<<paxos1->getLocalServer()->serverId<<std::endl<<std::flush; cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  tmpInt= cis[0].matchIndex;
  std::cout<<"Cluster Info for server
  "<<paxos5->getLocalServer()->serverId<<std::endl<<std::flush; cis.clear();
  paxos5->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(tmpInt, cis[1].matchIndex);

  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);

  std::cout<<"Cluster Info for server
  "<<paxos1->getLocalServer()->serverId<<std::endl<<std::flush; cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_LE(tmpInt, cis[3].matchIndex);
  std::cout<<"Cluster Info for server
  "<<paxos5->getLocalServer()->serverId<<std::endl<<std::flush; cis.clear();
  paxos5->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);


  // check if the new added member has the forceSync electionWeight and
  learnerSource info. paxos1->configureMember(2, false, 8); sleep(1);
  paxos1->configureLearner(100, 2);
  sleep(1);

  strConfig.clear();
  strConfig.push_back(strTmp5);

  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  paxos1->changeMember(Paxos::CCAddNode, strTmp5);

  sleep(1);
  EXPECT_EQ(paxos6->getCurrentLeader(), 1);
  EXPECT_EQ(paxos6->getLastLogIndex(), paxos1->getLastLogIndex());

  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  cis.clear();
  paxos6->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(cis[1].electionWeight, 8);
  EXPECT_EQ(cis[4].learnerSource, 2);
  */

  delete paxos6;
  delete paxos5;
  delete paxos4;
  delete paxos3;
  delete paxos2;
  delete paxos1;

  deleteDir("paxosLogTestDir11");
  deleteDir("paxosLogTestDir12");
  deleteDir("paxosLogTestDir13");
  deleteDir("paxosLogTestDir14");
  deleteDir("paxosLogTestDir15");
  deleteDir("paxosLogTestDir16");
}

TEST(consensus, cctimeout) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");
  std::string strTmp3("127.0.0.1:11004");

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1;
  rlog1 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir11", true, 4 * 1024 * 1024);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir12", true, 4 * 1024 * 1024);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir13", true, 4 * 1024 * 1024);
  Paxos *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3);

  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir14", true, 4 * 1024 * 1024);
  Paxos *paxos4 = new Paxos(timeout, rlog);
  paxos4->initAsLearner(strTmp3, 4);

  sleep(3);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  strConfig.clear();
  strConfig.push_back(strTmp3);

  paxos1->setConfigureChangeTimeout(1000);
  EXPECT_EQ(paxos1->changeLearners(Paxos::CCAddNode, strConfig), 0);
  delete paxos2;
  delete paxos3;

  easy_error_log("start changeMember");
  EXPECT_EQ(paxos1->changeMember(Paxos::CCAddNode, strTmp3),
            PaxosErrorCode::PE_TIMEOUT);
  easy_error_log("changeMember timeout");

  delete paxos1;
  delete paxos4;
  deleteDir("paxosLogTestDir11");
  deleteDir("paxosLogTestDir12");
  deleteDir("paxosLogTestDir13");
  deleteDir("paxosLogTestDir14");
}

static void msleep(uint64_t t) {
  struct timeval sleeptime;
  if (t == 0) return;
  sleeptime.tv_sec = t / 1000;
  sleeptime.tv_usec = (t - (sleeptime.tv_sec * 1000)) * 1000;
  select(0, 0, 0, 0, &sleeptime);
}
TEST(consensus, learnerSource4) {
  std::vector<std::string> strConfig;
  LogEntry le;
  le.set_term(1);
  le.set_index(1);
  le.set_optype(kMock);
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");
  std::string strTmp3("127.0.0.1:11004");
  std::string strTmp4("127.0.0.1:11005");
  std::string strTmp5("127.0.0.1:11006");
  std::string strTmp6("127.0.0.1:11007");
  std::string strTmp7("127.0.0.1:11008");

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1;
  rlog1 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir11", true, 4 * 1024 * 1024);
  // We init log with mock log in index 1 to make error happend when we send log
  // entry 1 to learner!
  rlog->append(le);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir12", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir13", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3);

  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir14", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos4 = new Paxos(timeout, rlog);
  paxos4->initAsLearner(strTmp3, 4);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir15", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos5 = new Paxos(timeout, rlog);
  paxos5->initAsLearner(strTmp4, 5);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir16", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos6 = new Paxos(timeout, rlog);
  paxos6->initAsLearner(strTmp5, 6);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir17", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos7 = new Paxos(timeout, rlog);
  paxos7->initAsLearner(strTmp6, 7);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir18", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos8 = new Paxos(timeout, rlog);
  paxos8->initAsLearner(strTmp7, 8);

  le.set_optype(0);

  paxos2->setMaxMergeReportTimeout(500);
  paxos4->setMaxMergeReportTimeout(500);
  paxos5->setMaxMergeReportTimeout(500);
  paxos6->setMaxMergeReportTimeout(500);
  paxos7->setMaxMergeReportTimeout(500);
  paxos8->setMaxMergeReportTimeout(500);

  const Paxos::StatsType &stats = paxos2->getStats();

  sleep(3);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  strConfig.clear();
  strConfig.push_back(strTmp3);
  strConfig.push_back(strTmp4);
  strConfig.push_back(strTmp5);
  strConfig.push_back(strTmp6);
  strConfig.push_back(strTmp7);

  paxos1->changeLearners(Paxos::CCAddNode, strConfig);

  sleep(1);

  EXPECT_EQ(paxos1->getConfig()->membersToString(("127.0.0.1:11002")),
            std::string(
                "127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#5N@2"));
  EXPECT_EQ(paxos1->getConfig()->learnersToString(),
            std::string("127.0.0.1:11004$00;127.0.0.1:11005$00;127.0.0.1:11006$"
                        "00;127.0.0.1:11007$00;127.0.0.1:11008$00"));

  paxos1->configureLearner(100, 2);
  sleep(1);
  paxos1->configureLearner(101, 100);
  sleep(1);
  paxos1->configureLearner(102, 101);
  sleep(1);
  paxos1->configureLearner(103, 102);
  sleep(1);
  paxos1->configureLearner(104, 103);

  sleep(2);
  EXPECT_EQ(paxos1->getConfig()->membersToString(("127.0.0.1:11002")),
            std::string(
                "127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#5N@2"));
  EXPECT_EQ(paxos1->getConfig()->learnersToString(),
            std::string("127.0.0.1:11004$02;127.0.0.1:11005$:0;127.0.0.1:11006$"
                        ":1;127.0.0.1:11007$:2;127.0.0.1:11008$:3"));
  EXPECT_EQ(paxos2->getConfig()->learnersToString(),
            std::string("127.0.0.1:11004$02;127.0.0.1:11005$:0;127.0.0.1:11006$"
                        ":1;127.0.0.1:11007$:2;127.0.0.1:11008$:3"));
  EXPECT_EQ(paxos2->getConfig()->getServer(100)->learnerSource, 2);
  EXPECT_EQ(paxos2->getConfig()->getServer(101)->learnerSource, 100);
  EXPECT_EQ(paxos2->getConfig()->getServer(102)->learnerSource, 101);
  EXPECT_EQ(paxos2->getConfig()->getServer(103)->learnerSource, 102);
  EXPECT_EQ(paxos2->getConfig()->getServer(104)->learnerSource, 103);
  paxos2->clearStats();

  std::vector<Paxos::ClusterInfoType> cis;
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(cis[3].learnerSource, 2);
  EXPECT_EQ(cis[4].learnerSource, 100);
  EXPECT_EQ(cis[0].learnerSource, 0);

  auto index = paxos1->getLastLogIndex();
  le.set_index(0);
  le.set_optype(0);
  le.set_value("aaa");
  paxos1->replicateLog(le);

  for (uint64_t i = 0; i < 14; ++i) {
    le.clear_term();
    paxos1->replicateLog(le);
    msleep(200);
  }

  sleep(2);

  std::cout << "paxos1:" << std::endl << std::flush;
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  std::cout << "paxos2:" << std::endl << std::flush;
  paxos2->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  std::cout << "paxos4(100):" << std::endl << std::flush;
  paxos4->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  std::cout << "paxos5(101):" << std::endl << std::flush;
  paxos5->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  std::cout << "paxos6(102):" << std::endl << std::flush;
  paxos6->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  std::cout << "paxos7(103):" << std::endl << std::flush;
  paxos7->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);

  EXPECT_EQ(index + 15, paxos1->getLastLogIndex());
  EXPECT_EQ(index + 15, paxos4->getLastLogIndex());
  EXPECT_EQ(index + 15, paxos5->getLastLogIndex());

  // change learnersource
  paxos1->configureLearner(103, 2);
  sleep(1);
  EXPECT_EQ(paxos2->getConfig()->getServer(103)->learnerSource, 2);

  index = paxos1->getLastLogIndex();
  le.set_index(0);
  le.set_optype(0);
  le.set_value("aaa");
  paxos1->replicateLog(le);

  for (uint64_t i = 0; i < 14; ++i) {
    le.clear_term();
    paxos1->replicateLog(le);
    msleep(200);
  }

  sleep(2);

  std::cout << "paxos1:" << std::endl << std::flush;
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  std::cout << "paxos2:" << std::endl << std::flush;
  paxos2->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  std::cout << "paxos4(100):" << std::endl << std::flush;
  paxos4->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  std::cout << "paxos5(101):" << std::endl << std::flush;
  paxos5->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  std::cout << "paxos6(102):" << std::endl << std::flush;
  paxos6->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  std::cout << "paxos7(103):" << std::endl << std::flush;
  paxos7->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);

  EXPECT_EQ(index + 15, paxos1->getLastLogIndex());
  EXPECT_EQ(index + 15, paxos4->getLastLogIndex());
  EXPECT_EQ(index + 15, paxos5->getLastLogIndex());

  delete paxos8;
  delete paxos7;
  delete paxos6;
  delete paxos5;
  delete paxos4;
  delete paxos3;
  delete paxos2;
  delete paxos1;

  deleteDir("paxosLogTestDir11");
  deleteDir("paxosLogTestDir12");
  deleteDir("paxosLogTestDir13");
  deleteDir("paxosLogTestDir14");
  deleteDir("paxosLogTestDir15");
  deleteDir("paxosLogTestDir16");
  deleteDir("paxosLogTestDir17");
  deleteDir("paxosLogTestDir18");
}

/**
 * bug: concurrent configureChange may clear other's flag,
 *      cause later configureChange hang
 */
TEST(consensus, concurrent_cc) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1;
  rlog1 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir11", true, 4 * 1024 * 1024);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir12", true, 4 * 1024 * 1024);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir13", true, 4 * 1024 * 1024);
  Paxos *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3);

  sleep(3);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  std::thread op1([paxos1]() {
    std::vector<std::string> strConfig;
    strConfig.push_back("127.0.0.1:11004");
    paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  });
  std::thread op2([paxos1]() {
    std::vector<std::string> strConfig;
    strConfig.push_back("127.0.0.1:11005");
    paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  });
  std::thread op3([paxos1]() {
    std::vector<std::string> strConfig;
    strConfig.push_back("127.0.0.1:11006");
    paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  });
  op1.join();
  op2.join();
  op3.join();

  paxos1->setConfigureChangeTimeout(5 * 1000);  // 5s
  delete paxos2;
  delete paxos3;
  std::thread op4([paxos1]() {
    std::vector<std::string> strConfig;
    strConfig.push_back("127.0.0.1:11007");
    paxos1->changeLearners(Paxos::CCAddNode, strConfig);  // timeout
  });
  std::thread op5([paxos1]() {
    std::vector<std::string> strConfig;
    strConfig.push_back("127.0.0.1:11008");
    paxos1->changeLearners(Paxos::CCAddNode, strConfig);  // timeout
  });
  op4.join();
  op5.join();

  delete paxos1;
  deleteDir("paxosLogTestDir11");
  deleteDir("paxosLogTestDir12");
  deleteDir("paxosLogTestDir13");
}

TEST(consensus, learner_downgrade_term) {
  std::vector<std::string> strConfig;
  LogEntry le;
  le.set_term(1);
  le.set_index(1);
  le.set_optype(kMock);
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");
  std::string strTmp3("127.0.0.1:11004");
  std::vector<Paxos::ClusterInfoType> cis;

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1, rlog4;
  rlog1 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir11", true, 4 * 1024 * 1024);
  // We init log with mock log in index 1 to make error happend when we send log
  // entry 1 to learner!
  rlog->append(le);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir12", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir13", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3);

  rlog4 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir14", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos4 = new Paxos(timeout, rlog);
  paxos4->initAsLearner(strTmp3, 4);

  le.set_optype(0);
  sleep(3);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  strConfig.clear();
  strConfig.push_back(strTmp3);

  paxos1->changeLearners(Paxos::CCAddNode, strConfig);

  sleep(1);

  EXPECT_EQ(paxos1->getConfig()->membersToString(("127.0.0.1:11002")),
            std::string(
                "127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#5N@2"));
  EXPECT_EQ(paxos1->getConfig()->learnersToString(),
            std::string("127.0.0.1:11004$00"));

  sleep(1);
  le.clear_term();
  paxos1->replicateLog(le);

  cis.clear();
  paxos4->getClusterInfo(cis);
  sleep(1);
  Paxos::printClusterInfo(cis);

  std::cout << "11004 restart" << std::endl << std::flush;
  delete paxos4;
  sleep(2);

  // force to give learner a larger term
  uint64_t cterm1 = 0, cterm2 = 0;
  rlog4->getMetaData(std::string(Paxos::keyCurrentTerm), &cterm1);
  rlog1->getMetaData(std::string(Paxos::keyCurrentTerm), &cterm2);
  EXPECT_EQ(cterm1, cterm2);
  rlog4->setMetaData(std::string(Paxos::keyCurrentTerm), cterm1 + 10);

  paxos4 = new Paxos(timeout, rlog4);
  std::string emptryString("");
  paxos4->initAsLearner(emptryString, 4);
  sleep(2);
  rlog4->getMetaData(std::string(Paxos::keyCurrentTerm), &cterm1);
  // check term downgrade successfully
  EXPECT_EQ(cterm1, cterm2);

  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);
  // check log replicate is normal
  EXPECT_EQ(paxos4->getLastLogIndex(), paxos1->getLastLogIndex());

  delete paxos4;
  delete paxos3;
  delete paxos2;
  delete paxos1;

  deleteDir("paxosLogTestDir11");
  deleteDir("paxosLogTestDir12");
  deleteDir("paxosLogTestDir13");
  deleteDir("paxosLogTestDir14");
}

TEST(consensus, learner_heartbeat) {
  std::vector<std::string> strConfig;
  LogEntry le;
  le.set_term(1);
  le.set_index(1);
  le.set_optype(kMock);
  strConfig.emplace_back("127.0.0.1:11001");
  std::string strTmp3("127.0.0.1:11004");
  std::vector<Paxos::ClusterInfoType> cis;

  const uint64_t timeout = 5000;
  std::shared_ptr<PaxosLog> rlog1, rlog4;
  rlog1 =
      std::make_shared<RDPaxosLog>("paxosLogTestDir11", true, 4 * 1024 * 1024);
  // We init log with mock log in index 1 to make error happend when we send log
  // entry 1 to learner!
  rlog1->append(le);
  Paxos *paxos1 = new Paxos(timeout, rlog1);
  paxos1->init(strConfig, 1, 1);

  rlog4 =
      std::make_shared<RDPaxosLog>("paxosLogTestDir14", true, 4 * 1024 * 1024);
  rlog4->append(le);
  Paxos *paxos4 = new Paxos(timeout, rlog4);
  paxos4->initAsLearner(strTmp3, 4);

  le.set_optype(0);
  sleep(3);
  paxos1->requestVote();
  sleep(1);
  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  strConfig.clear();
  strConfig.push_back(strTmp3);
  paxos1->changeLearners(Paxos::CCAddNode, strConfig);

  /* now we have 1 leader and 1 learner*/
  const Paxos::StatsType &stats = paxos1->getStats();
  uint64_t tmp = 0;
  sleep(2);
  EXPECT_EQ(stats.countHeartbeat, 0);
  paxos1->changeMember(Paxos::CCAddNode, strConfig[0]);
  while (rlog1->getLastLogIndex() != rlog4->getLastLogIndex()) {
  }
  tmp = stats.countHeartbeat;
  sleep(2);
  EXPECT_GT(stats.countHeartbeat, tmp);
  paxos1->downgradeMember(strConfig[0]);
  while (rlog1->getLastLogIndex() != rlog4->getLastLogIndex()) {
  }
  tmp = stats.countHeartbeat;
  sleep(2);
  EXPECT_EQ(stats.countHeartbeat, tmp);
  paxos1->changeLearners(Paxos::CCDelNode, strConfig);
  paxos1->setEnableLearnerHeartbeat(true);
  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  while (rlog1->getLastLogIndex() != rlog4->getLastLogIndex()) {
  }
  tmp = stats.countHeartbeat;
  sleep(2);
  EXPECT_GT(stats.countHeartbeat, tmp);
  paxos1->changeMember(Paxos::CCAddNode, strConfig[0]);
  while (rlog1->getLastLogIndex() != rlog4->getLastLogIndex()) {
  }
  tmp = stats.countHeartbeat;
  sleep(2);
  EXPECT_GT(stats.countHeartbeat, tmp);
  paxos1->downgradeMember(strConfig[0]);
  while (rlog1->getLastLogIndex() != rlog4->getLastLogIndex()) {
  }
  tmp = stats.countHeartbeat;
  sleep(2);
  EXPECT_GT(stats.countHeartbeat, tmp);

  delete paxos4;
  delete paxos1;

  deleteDir("paxosLogTestDir11");
  deleteDir("paxosLogTestDir14");
}

/* change learnerSource use the address as argument */
TEST(consensus, learnersource_addr) {
  std::vector<std::string> strConfig;
  LogEntry le;
  le.set_term(1);
  le.set_index(1);
  le.set_optype(kMock);
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");
  std::string strTmp3("127.0.0.1:11004");
  std::vector<Paxos::ClusterInfoType> cis;

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1, rlog4;
  rlog1 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir11", true, 4 * 1024 * 1024);
  // We init log with mock log in index 1 to make error happend when we send log
  // entry 1 to learner!
  rlog->append(le);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir12", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir13", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3);

  rlog4 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir14", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos4 = new Paxos(timeout, rlog);
  paxos4->initAsLearner(strTmp3, 4);

  le.set_optype(0);
  sleep(3);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  strConfig.clear();
  strConfig.push_back(strTmp3);

  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  sleep(1);
  EXPECT_EQ(paxos1->getConfig()->membersToString(("127.0.0.1:11002")),
            std::string(
                "127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#5N@2"));
  EXPECT_EQ(paxos1->getConfig()->learnersToString(),
            std::string("127.0.0.1:11004$00"));

  // cannot configure follower
  EXPECT_EQ(paxos1->configureLearner("127.0.0.1:11002", "127.0.0.1:11002"),
            PaxosErrorCode::PE_NOTFOUND);
  // cannot configure not-exist node
  EXPECT_EQ(paxos1->configureLearner("127.0.0.1:11005", "127.0.0.1:11002"),
            PaxosErrorCode::PE_NOTFOUND);
  EXPECT_EQ(paxos1->configureLearner("127.0.0.1:11004", "127.0.0.1:11005"),
            PaxosErrorCode::PE_NOTFOUND);
  // success case
  EXPECT_EQ(paxos1->configureLearner("127.0.0.1:11004", "127.0.0.1:11003"),
            PaxosErrorCode::PE_NONE);
  sleep(1);
  EXPECT_EQ(paxos1->getConfig()->learnersToString(),
            std::string("127.0.0.1:11004$03"));
  // reset to 0
  EXPECT_EQ(paxos1->configureLearner("127.0.0.1:11004", "127.0.0.1:11004"),
            PaxosErrorCode::PE_NONE);
  sleep(1);
  EXPECT_EQ(paxos1->getConfig()->learnersToString(),
            std::string("127.0.0.1:11004$00"));

  delete paxos4;
  delete paxos3;
  delete paxos2;
  delete paxos1;

  deleteDir("paxosLogTestDir11");
  deleteDir("paxosLogTestDir12");
  deleteDir("paxosLogTestDir13");
  deleteDir("paxosLogTestDir14");
}

TEST(consensus, enable_learner_auto_reset_match_index) {
  std::vector<std::string> strConfig;
  LogEntry le;
  le.set_term(1);
  le.set_index(1);
  le.set_optype(kNormal);
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");
  std::string strTmp3("127.0.0.1:11004");
  std::vector<Paxos::ClusterInfoType> cis;

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1, rlog2, rlog4;
  rlog1 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir11", true, 4 * 1024 * 1024);
  // We init log with mock log in index 1 to make error happend when we send log
  // entry 1 to learner!
  rlog->append(le);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog2 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir12", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir13", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3);

  rlog4 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir14", true, 4 * 1024 * 1024);
  rlog->append(le);
  Paxos *paxos4 = new Paxos(timeout, rlog);
  paxos4->initAsLearner(strTmp3, 4);

  le.set_optype(0);
  sleep(3);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  strConfig.clear();
  strConfig.push_back(strTmp3);

  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  sleep(1);
  EXPECT_EQ(paxos1->getConfig()->membersToString(("127.0.0.1:11002")),
            std::string(
                "127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#5N@2"));
  EXPECT_EQ(paxos1->getConfig()->learnersToString(),
            std::string("127.0.0.1:11004$00"));

  EXPECT_EQ(paxos4->getLastLogIndex(), 3);

  std::cout << "shutdown learner" << std::endl;
  delete paxos4;
  sleep(1);

  paxos2->requestVote(true);
  sleep(1);

  EXPECT_EQ(paxos2->getState(), Paxos::LEADER);
  EXPECT_EQ(paxos2->getLastLogIndex(), 4);

  // new leader's index 4 term should be 3
  {
    LogEntry le;
    rlog2->getEntry(4, le);
    EXPECT_EQ(le.term(), 3);
  }

  // learner's index 4 term should be 2
  {
    LogEntry le;
    le.set_term(2);
    le.set_optype(kNormal);
    rlog4->append(le);
    rlog4->getEntry(4, le);
    EXPECT_EQ(le.term(), 2);
  }
  {
    LogEntry le;
    le.set_term(2);
    le.set_optype(kNormal);
    rlog4->append(le);
    rlog4->getEntry(5, le);
    EXPECT_EQ(le.term(), 2);
  }

  paxos2->changeLearners(Paxos::CCDelNode, strConfig);
  sleep(1);
  EXPECT_EQ(paxos2->getConfig()->learnersToString(), std::string(""));

  paxos2->changeLearners(Paxos::CCAddNode, strConfig);
  sleep(1);
  EXPECT_EQ(paxos2->getConfig()->learnersToString(),
            std::string("127.0.0.1:11004$00"));

  EXPECT_EQ(paxos2->getLastLogIndex(), 6);

  std::cout << "restart learner" << std::endl;
  paxos4 = new Paxos(timeout, rlog4);
  paxos4->initAsLearner(strTmp3, 4);

  sleep(3);

  EXPECT_EQ(paxos2->getEnableLearnerAutoResetMatchIndex(), false);
  // log can not be replicated
  {
    LogEntry le;
    rlog4->getEntry(4, le);
    EXPECT_EQ(le.term(), 2);
  }

  paxos2->setEnableLearnerAutoResetMatchIndex(true);
  EXPECT_EQ(paxos2->getEnableLearnerAutoResetMatchIndex(), true);

  {
    LogEntry le;
    le.set_term(3);
    le.set_optype(kNormal);
    paxos2->replicateLog(le);
  }

  sleep(5);

  // log can be replicated
  EXPECT_EQ(paxos4->getLastLogIndex(), 7);
  {
    LogEntry le;
    rlog4->getEntry(4, le);
    EXPECT_EQ(le.term(), 3);
  }

  delete paxos4;
  delete paxos3;
  delete paxos2;
  delete paxos1;

  deleteDir("paxosLogTestDir11");
  deleteDir("paxosLogTestDir12");
  deleteDir("paxosLogTestDir13");
  deleteDir("paxosLogTestDir14");
}

// test force single learner
// after a node becomes a single learner, it can be added back to a new cluster,
// and its log position will be automatically matched.
TEST(consensus, force_single_learner) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");
  std::string strTmp3("127.0.0.1:11004");
  std::vector<Paxos::ClusterInfoType> cis;

  const uint64_t timeout = 5000;
  std::shared_ptr<PaxosLog> rlog, rlog1, rlog2, rlog4;
  rlog1 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir11", true, 4 * 1024 * 1024);
  auto *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog2 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir12", true, 4 * 1024 * 1024);
  auto localServer = std::make_shared<alisql::LocalServer>(0);
  auto *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2, 0, 4, 4, localServer);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir13", true, 4 * 1024 * 1024);
  auto *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3);

  rlog4 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir14", true, 4 * 1024 * 1024);
  auto *paxos4 = new Paxos(timeout, rlog);
  paxos4->initAsLearner(strTmp3, 4);

  paxos1->setEnableLearnerAutoResetMatchIndex(true);
  paxos2->setEnableLearnerAutoResetMatchIndex(true);
  paxos3->setEnableLearnerAutoResetMatchIndex(true);
  paxos4->setEnableLearnerAutoResetMatchIndex(true);

  // get standard learner meta info
  EXPECT_EQ(paxos4->getConfig()->membersToString(),
            std::string("127.0.0.1:11004#5N"));
  EXPECT_EQ(paxos4->getConfig()->learnersToString(), std::string(""));

  sleep(1);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  strConfig.clear();
  strConfig.push_back(strTmp3);

  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  sleep(3);
  EXPECT_EQ(paxos1->getConfig()->membersToString(("127.0.0.1:11001")),
            std::string(
                "127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#5N@1"));
  EXPECT_EQ(paxos1->getConfig()->learnersToString(),
            std::string("127.0.0.1:11004$00"));

  EXPECT_EQ(paxos2->getCommitIndex(), 2);
  EXPECT_EQ(paxos3->getCommitIndex(), 2);
  EXPECT_EQ(paxos4->getCommitIndex(), 2);

  // does not work for learner
  EXPECT_EQ(paxos4->forceSingleLearner(), 1);
  EXPECT_EQ(paxos4->getConfig()->membersToString(),
            std::string("127.0.0.1:11004#5N"));
  EXPECT_EQ(paxos4->getConfig()->learnersToString(),
            std::string("127.0.0.1:11004$00"));

  // make learner single leader
  paxos4->forceSingleLeader();
  paxos4->requestVote();
  LogEntry le;
  le.set_optype(0);
  le.set_ikey("a");
  paxos4->replicateLog(le);
  EXPECT_EQ(paxos4->getCommitIndex(), 4);
  {
    LogEntry le;
    rlog4->getEntry(3, le);
    EXPECT_EQ(le.term(), 3);
  }

  le.set_ikey("b");
  paxos1->replicateLog(le);
  sleep(3);
  EXPECT_EQ(paxos1->getCommitIndex(), 3);
  EXPECT_EQ(paxos2->getCommitIndex(), 3);
  EXPECT_EQ(paxos3->getCommitIndex(), 3);
  {
    LogEntry le;
    rlog1->getEntry(3, le);
    EXPECT_EQ(le.term(), 2);
  }
  {
    LogEntry le;
    rlog2->getEntry(3, le);
    EXPECT_EQ(le.term(), 2);
  }

  // generate bigger term log to test learner auto reset match index
  paxos2->requestVote();
  sleep(1);
  EXPECT_EQ(paxos2->getState(), Paxos::LEADER);
  paxos1->requestVote();
  sleep(3);
  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  EXPECT_EQ(paxos1->getCommitIndex(), 5);
  EXPECT_EQ(paxos2->getCommitIndex(), 5);
  EXPECT_EQ(paxos3->getCommitIndex(), 5);

  // change follower to single learner
  EXPECT_EQ(paxos2->getConfig()->membersToString(("127.0.0.1:11002")),
            std::string(
                "127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#5N@2"));
  EXPECT_EQ(paxos2->getConfig()->learnersToString(),
            std::string("127.0.0.1:11004$00"));
  EXPECT_EQ(paxos2->forceSingleLearner(), 0);
  EXPECT_EQ(paxos2->getConfig()->membersToString(),
            std::string("0;127.0.0.1:11002#5N"));
  EXPECT_EQ(paxos2->getConfig()->learnersToString(), std::string(""));

  // change leader to single learner
  EXPECT_EQ(paxos1->getConfig()->membersToString(("127.0.0.1:11001")),
            std::string(
                "127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#5N@1"));
  EXPECT_EQ(paxos1->getConfig()->learnersToString(),
            std::string("127.0.0.1:11004$00"));
  EXPECT_EQ(paxos1->forceSingleLearner(), 0);
  EXPECT_EQ(paxos1->getConfig()->membersToString(),
            std::string("127.0.0.1:11001#5N"));
  EXPECT_EQ(paxos1->getConfig()->learnersToString(), std::string(""));

  // add paxos 1 as learner
  strConfig.clear();
  strConfig.emplace_back("127.0.0.1:11001");
  paxos4->changeLearners(Paxos::CCAddNode, strConfig);
  sleep(3);
  EXPECT_EQ(paxos4->getCommitIndex(), 5);
  EXPECT_EQ(paxos1->getCommitIndex(), 5);
  EXPECT_EQ(paxos4->getConfig()->membersToString(("127.0.0.1:11004")),
            std::string("127.0.0.1:11004#5N@1"));
  EXPECT_EQ(paxos4->getConfig()->learnersToString(),
            std::string("127.0.0.1:11001$00"));
  EXPECT_EQ(paxos1->getConfig()->membersToString(),
            std::string("127.0.0.1:11001#5N"));
  EXPECT_EQ(paxos1->getConfig()->learnersToString(),
            std::string("127.0.0.1:11001$00"));

  le.set_optype(0);
  paxos4->replicateLog(le);
  sleep(3);
  EXPECT_EQ(paxos4->getCommitIndex(), 6);
  EXPECT_EQ(paxos1->getCommitIndex(), 6);

  // add paxos 2 as learner
  strConfig.clear();
  strConfig.emplace_back("127.0.0.1:11002");
  paxos4->changeLearners(Paxos::CCAddNode, strConfig);
  sleep(3);
  EXPECT_EQ(paxos4->getCommitIndex(), 7);
  EXPECT_EQ(paxos2->getCommitIndex(), 7);
  EXPECT_EQ(paxos4->getConfig()->membersToString(("127.0.0.1:11004")),
            std::string("127.0.0.1:11004#5N@1"));
  EXPECT_EQ(paxos4->getConfig()->learnersToString(),
            std::string("127.0.0.1:11001$00;127.0.0.1:11002$00"));
  EXPECT_EQ(paxos2->getConfig()->membersToString(),
            std::string("0;127.0.0.1:11002#5N"));
  EXPECT_EQ(paxos2->getConfig()->learnersToString(),
            std::string("127.0.0.1:11001$00;127.0.0.1:11002$00"));

  le.set_optype(0);
  paxos4->replicateLog(le);
  sleep(3);
  EXPECT_EQ(paxos4->getCommitIndex(), 8);
  EXPECT_EQ(paxos2->getCommitIndex(), 8);

  {
    // upgrade paxos 1 to follower
    std::string s("127.0.0.1:11001");
    paxos4->changeMember(Paxos::CCAddNode, s);
    sleep(3);
    EXPECT_EQ(paxos4->getCommitIndex(), 9);
    EXPECT_EQ(paxos1->getCommitIndex(), 9);
    EXPECT_EQ(paxos4->getConfig()->membersToString(("127.0.0.1:11004")),
              std::string("127.0.0.1:11004#5N;127.0.0.1:11001#5N@1"));
    EXPECT_EQ(paxos4->getConfig()->learnersToString(),
              std::string("0;127.0.0.1:11002$00"));
    EXPECT_EQ(paxos1->getConfig()->membersToString(("127.0.0.1:11001")),
              std::string("127.0.0.1:11004#5N;127.0.0.1:11001#5N@2"));
    EXPECT_EQ(paxos1->getConfig()->learnersToString(),
              std::string("0;127.0.0.1:11002$00"));
  }

  le.set_optype(0);
  paxos4->replicateLog(le);
  sleep(6);
  EXPECT_EQ(paxos4->getCommitIndex(), 10);
  EXPECT_EQ(paxos2->getCommitIndex(), 10);

  {
    // upgrade paxos 2 to follower
    std::string s("127.0.0.1:11002");
    paxos4->changeMember(Paxos::CCAddNode, s);
    sleep(6);
    EXPECT_EQ(paxos4->getCommitIndex(), 11);
    EXPECT_EQ(paxos2->getCommitIndex(), 11);
    EXPECT_EQ(
        paxos4->getConfig()->membersToString(("127.0.0.1:11004")),
        std::string(
            "127.0.0.1:11004#5N;127.0.0.1:11001#5N;127.0.0.1:11002#5N@1"));
    EXPECT_EQ(paxos4->getConfig()->learnersToString(), std::string(""));
    EXPECT_EQ(
        paxos1->getConfig()->membersToString(("127.0.0.1:11002")),
        std::string(
            "127.0.0.1:11004#5N;127.0.0.1:11001#5N;127.0.0.1:11002#5N@3"));
    EXPECT_EQ(paxos1->getConfig()->learnersToString(), std::string(""));
  }

  le.set_optype(0);
  paxos4->replicateLog(le);
  sleep(6);
  EXPECT_EQ(paxos4->getCommitIndex(), 12);
  EXPECT_EQ(paxos2->getCommitIndex(), 12);

  // since this function will break the protocol,
  // logs might be different on every node
  {
    LogEntry le;
    rlog1->getEntry(3, le);
    EXPECT_TRUE(le.term() == 3 || le.term() == 2);
  }
  {
    LogEntry le;
    rlog2->getEntry(3, le);
    EXPECT_TRUE(le.term() == 3 || le.term() == 2);
  }

  {
    std::vector<Paxos::ClusterInfoType> cis;
    paxos4->getClusterInfo(cis);
    EXPECT_EQ(cis[2].matchIndex, 12);
  }

  {
    paxos2->requestVote(true);
    sleep(1);
    EXPECT_EQ(paxos2->getState(), Paxos::LEADER);
    LogEntry le;
    le.set_optype(0);
    le.set_term(paxos2->getTerm());
    localServer->writeLog(le);
    sleep(2);
    std::vector<Paxos::ClusterInfoType> cis;
    paxos2->getClusterInfo(cis);
    EXPECT_EQ(paxos2->getCommitIndex(), 14);
    // if bug exists, match index will be 13
    EXPECT_EQ(cis[2].matchIndex, 14);
  }

  delete paxos4;
  delete paxos3;
  delete paxos2;
  delete paxos1;

  deleteDir("paxosLogTestDir11");
  deleteDir("paxosLogTestDir12");
  deleteDir("paxosLogTestDir13");
  deleteDir("paxosLogTestDir14");
}
