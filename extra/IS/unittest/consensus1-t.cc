/************************************************************************
 *
 * Copyright (c) 2017 Alibaba.com, Inc. All Rights Reserved
 * $Id:  consensus1-t.cc,v 1.0 03/21/2017 09:19:25 PM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file consensus1-t.cc
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

TEST(consensus, paxos_one_node) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");

  uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog;
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir1");
  Paxos *paxos1 = new Paxos(timeout, rlog, 3000);
  paxos1->setAlertLogLevel(Paxos::LOG_ERROR);
  paxos1->setAlertLogLevel();
  paxos1->init(strConfig, 1, 1);

  sleep(1);
  paxos1->requestVote();
  sleep(3);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_value("aaa");
  paxos1->replicateLog(le);

  le.set_value("bbb");
  paxos1->replicateLog(le);

  sleep(1);

  EXPECT_EQ(paxos1->getLastLogIndex(), paxos1->getCommitIndex());
  EXPECT_EQ(paxos1->getConfig()->getServerNum(), 1);
  EXPECT_EQ(paxos1->getConfig()->getServerNumLockFree(), 1);

  // add node in one node cluster.
  std::string strTmp1("127.0.0.1:11002");
  std::string strTmp2("127.0.0.1:11003");
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir2");
  Paxos *learner1 = new Paxos(timeout, rlog);
  learner1->initAsLearner(strTmp1, 11);
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir3");
  Paxos *learner2 = new Paxos(timeout, rlog);
  learner2->initAsLearner(strTmp2, 12);
  sleep(1);

  strConfig.clear();
  strConfig.push_back(strTmp1);
  strConfig.push_back(strTmp2);
  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  sleep(1);

  auto config =
      std::dynamic_pointer_cast<StableConfiguration>(paxos1->getConfig());
  EXPECT_EQ(config->learnersToString(),
            std::string("127.0.0.1:11002$00;127.0.0.1:11003$00"));

  paxos1->changeMember(Paxos::CCAddNode, strTmp1);
  sleep(1);
  paxos1->changeMember(Paxos::CCAddNode, strTmp2);
  sleep(1);

  EXPECT_EQ(paxos1->getConfig()->getServerNum(), 3);
  EXPECT_EQ(paxos1->getConfig()->getServerNumLockFree(), 3);
  EXPECT_EQ(config->membersToString(("127.0.0.1:11001")),
            std::string(
                "127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#5N@1"));

  paxos1->replicateLog(le);
  sleep(3);
  EXPECT_EQ(learner1->getState(), Paxos::FOLLOWER);
  EXPECT_EQ(learner2->getState(), Paxos::FOLLOWER);
  EXPECT_EQ(paxos1->getCommitIndex(), paxos1->getLastLogIndex());
  EXPECT_EQ(learner1->getCommitIndex(), paxos1->getLastLogIndex());
  EXPECT_EQ(learner2->getCommitIndex(), paxos1->getLastLogIndex());

  auto config2 =
      std::dynamic_pointer_cast<StableConfiguration>(learner1->getConfig());
  EXPECT_EQ(config2->learnersToString(), std::string(""));
  EXPECT_EQ(config2->membersToString(("127.0.0.1:11002")),
            std::string(
                "127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#5N@2"));

  config.reset();
  config2.reset();
  delete paxos1;
  delete learner1;
  delete learner2;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(consensus, paxos_leaderStickiness_and_downgradeTerm) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog;
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir61");
  Paxos *paxos1 = new Paxos(timeout, rlog, 3000);
  paxos1->init(strConfig, 1, 1);
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir62");
  Paxos *paxos2 = new Paxos(timeout, rlog, 3000);
  paxos2->init(strConfig, 2, 2);
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir63");
  Paxos *paxos3 = new Paxos(timeout, rlog, 3000);
  paxos3->init(strConfig, 3, 3);

  sleep(1);
  paxos1->requestVote();
  sleep(3);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_value("aaa");
  paxos1->replicateLog(le);

  le.set_value("bbb");
  paxos1->replicateLog(le);

  sleep(3);
  // paxos2 requestVote fail because of leaderStickiness
  paxos2->requestVote(false);
  sleep(3);
  paxos1->replicateLog(le);
  sleep(1);
  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  EXPECT_EQ(paxos2->getState(), Paxos::FOLLOWER);
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos2->getLastLogIndex());

  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir61");
  deleteDir("paxosLogTestDir62");
  deleteDir("paxosLogTestDir63");
}

TEST(consensus, paxos_force_one_node) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog;
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir61");
  Paxos *paxos1 = new Paxos(timeout, rlog, 3000);
  paxos1->init(strConfig, 1, 1);
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir62");
  Paxos *paxos2 = new Paxos(timeout, rlog, 3000);
  paxos2->init(strConfig, 2, 2);
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir63");
  Paxos *paxos3 = new Paxos(timeout, rlog, 3000);
  paxos3->init(strConfig, 3, 3);

  sleep(1);
  paxos1->requestVote();
  sleep(3);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_value("aaa");
  paxos1->replicateLog(le);

  sleep(1);
  EXPECT_EQ(paxos1->getCommitIndex(), 2);

  delete paxos1;
  delete paxos2;
  std::cout << "delete paxos1/paxos2" << std::endl << std::flush;

  sleep(3);
  EXPECT_EQ(paxos3->getState(), Paxos::CANDIDATE);
  paxos3->forceSingleLeader();
  sleep(1);
  EXPECT_EQ(paxos3->getState(), Paxos::LEADER);
  le.clear_term();
  paxos3->replicateLog(le);
  EXPECT_EQ(paxos3->getCommitIndex(), 4);

  delete paxos3;

  deleteDir("paxosLogTestDir61");
  deleteDir("paxosLogTestDir62");
  deleteDir("paxosLogTestDir63");
}

TEST(consensus, paxos_force_one_node_learner) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");
  std::string strTmp3("127.0.0.1:11004");

  uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog;
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir61");
  Paxos *paxos1 = new Paxos(timeout, rlog, 3000);
  paxos1->init(strConfig, 1, 1);
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir62");
  Paxos *paxos2 = new Paxos(timeout, rlog, 3000);
  paxos2->init(strConfig, 2, 2);
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir63");
  Paxos *paxos3 = new Paxos(timeout, rlog, 3000);
  paxos3->init(strConfig, 3, 3);

  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir64");
  Paxos *paxos4 = new Paxos(timeout, rlog);
  paxos4->initAsLearner(strTmp3, 4);

  sleep(1);
  paxos1->requestVote();
  sleep(3);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  strConfig.clear();
  strConfig.push_back(strTmp3);

  paxos1->changeLearners(Paxos::CCAddNode, strConfig);

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_value("aaa");
  paxos1->replicateLog(le);

  sleep(1);
  EXPECT_EQ(paxos1->getCommitIndex(), 3);

  std::vector<Paxos::ClusterInfoType> cis;
  paxos4->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  std::cout << "================" << std::endl << std::flush;
  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);

  sleep(3);
  EXPECT_EQ(paxos4->getState(), Paxos::LEARNER);
  paxos4->forceSingleLeader();
  sleep(1);
  EXPECT_EQ(paxos4->getState(), Paxos::LEADER);
  le.clear_term();
  paxos4->replicateLog(le);
  sleep(1);
  EXPECT_EQ(paxos4->getCommitIndex(), 5);

  cis.clear();
  paxos4->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(1, cis.size());
  std::cout << "================" << std::endl << std::flush;
  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(4, cis.size());

  // check old cluster
  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);
  EXPECT_EQ(paxos1->getCommitIndex(), 4);

  // check new cluster
  le.clear_term();
  paxos4->replicateLog(le);
  sleep(1);
  EXPECT_EQ(paxos4->getCommitIndex(), 6);

  cis.clear();
  paxos4->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  std::cout << "================" << std::endl << std::flush;
  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);

  delete paxos1;
  delete paxos2;
  delete paxos3;
  delete paxos4;

  deleteDir("paxosLogTestDir61");
  deleteDir("paxosLogTestDir62");
  deleteDir("paxosLogTestDir63");
  deleteDir("paxosLogTestDir64");
}

TEST(consensus, largeBatchMode) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");
  std::string strTmp4("127.0.0.1:11004");

  uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog;
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir61");
  Paxos *paxos1 = new Paxos(timeout, rlog, 3000);
  paxos1->init(strConfig, 1, 1);
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir62");
  Paxos *paxos2 = new Paxos(timeout, rlog, 3000);
  paxos2->init(strConfig, 2, 2);
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir63");
  Paxos *paxos3 = new Paxos(timeout, rlog, 3000);
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir64");
  Paxos *paxos4 = new Paxos(timeout, rlog, 3000);

  sleep(1);
  paxos1->requestVote();
  sleep(3);
  paxos1->setMaxPacketSize(10);
  // disable pipeline
  paxos1->setMaxDelayIndex(1);
  paxos1->setMinDelayIndex(1);
  paxos1->setLargeBatchRatio(5);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_value("aaa");
  for (uint i = 0; i < 20; ++i) {
    le.clear_term();
    paxos1->replicateLog(le);
  }

  paxos3->init(strConfig, 3, 3);
  paxos4->initAsLearner(strTmp4, 4);
  strConfig.clear();
  strConfig.push_back(strTmp4);
  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  sleep(2);

  const Paxos::StatsType &stats = paxos3->getStats();
  std::cout << "countMsgAppendLog:" << stats.countMsgAppendLog
            << " countMsgRequestVote:" << stats.countMsgRequestVote
            << " countOnMsgAppendLog:" << stats.countOnMsgAppendLog
            << " countHeartbeat:" << stats.countHeartbeat
            << " countOnMsgRequestVote:" << stats.countOnMsgRequestVote
            << " countOnHeartbeat:" << stats.countOnHeartbeat
            << " countReplicateLog:" << stats.countReplicateLog << std::endl
            << std::flush;
  const Paxos::StatsType &stats1 = paxos4->getStats();
  std::cout << "countMsgAppendLog:" << stats1.countMsgAppendLog
            << " countMsgRequestVote:" << stats1.countMsgRequestVote
            << " countOnMsgAppendLog:" << stats1.countOnMsgAppendLog
            << " countHeartbeat:" << stats1.countHeartbeat
            << " countOnMsgRequestVote:" << stats1.countOnMsgRequestVote
            << " countOnHeartbeat:" << stats1.countOnHeartbeat
            << " countReplicateLog:" << stats1.countReplicateLog << std::endl
            << std::flush;

  EXPECT_EQ(stats.countOnMsgAppendLog - stats.countOnHeartbeat - 1, 7);
  EXPECT_EQ(stats1.countOnMsgAppendLog - stats1.countOnHeartbeat, 7);

  delete paxos4;
  delete paxos3;
  delete paxos2;
  delete paxos1;

  deleteDir("paxosLogTestDir61");
  deleteDir("paxosLogTestDir62");
  deleteDir("paxosLogTestDir63");
  deleteDir("paxosLogTestDir64");
}

TEST(consensus, pipelining) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");
  std::string strTmp4("127.0.0.1:11004");

  uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog;
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir61");
  Paxos *paxos1 = new Paxos(timeout, rlog, 3000);
  paxos1->init(strConfig, 1, 1);
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir62");
  Paxos *paxos2 = new Paxos(timeout, rlog, 3000);
  paxos2->init(strConfig, 2, 2);
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir63");
  Paxos *paxos3 = new Paxos(timeout, rlog, 3000);
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir64");
  Paxos *paxos4 = new Paxos(timeout, rlog, 3000);

  sleep(1);
  paxos1->requestVote();
  sleep(3);
  paxos1->setMaxPacketSize(10);
  // disable pipeline
  paxos1->setMaxDelayIndex(1000);
  paxos1->setMinDelayIndex(1);
  paxos1->setLargeBatchRatio(5);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  paxos1->setConsensusAsync(true);

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_value("aaa");
  for (uint i = 0; i < 20; ++i) {
    le.clear_term();
    paxos1->replicateLog(le);
  }

  paxos3->init(strConfig, 3, 3);
  paxos4->initAsLearner(strTmp4, 4);
  strConfig.clear();
  strConfig.push_back(strTmp4);
  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  sleep(2);

  const Paxos::StatsType &stats = paxos3->getStats();
  std::cout << "countMsgAppendLog:" << stats.countMsgAppendLog
            << " countMsgRequestVote:" << stats.countMsgRequestVote
            << " countOnMsgAppendLog:" << stats.countOnMsgAppendLog
            << " countHeartbeat:" << stats.countHeartbeat
            << " countOnMsgRequestVote:" << stats.countOnMsgRequestVote
            << " countOnHeartbeat:" << stats.countOnHeartbeat
            << " countReplicateLog:" << stats.countReplicateLog << std::endl
            << std::flush;
  const Paxos::StatsType &stats1 = paxos4->getStats();
  std::cout << "countMsgAppendLog:" << stats1.countMsgAppendLog
            << " countMsgRequestVote:" << stats1.countMsgRequestVote
            << " countOnMsgAppendLog:" << stats1.countOnMsgAppendLog
            << " countHeartbeat:" << stats1.countHeartbeat
            << " countOnMsgRequestVote:" << stats1.countOnMsgRequestVote
            << " countOnHeartbeat:" << stats1.countOnHeartbeat
            << " countReplicateLog:" << stats1.countReplicateLog << std::endl
            << std::flush;

  EXPECT_EQ(stats.countOnMsgAppendLog - stats.countOnHeartbeat - 1, 22);
  EXPECT_EQ(stats1.countOnMsgAppendLog - stats1.countOnHeartbeat, 7);

  delete paxos4;
  delete paxos3;
  delete paxos2;
  delete paxos1;

  deleteDir("paxosLogTestDir61");
  deleteDir("paxosLogTestDir62");
  deleteDir("paxosLogTestDir63");
  deleteDir("paxosLogTestDir64");
}

TEST(consensus, disable_pipelining_large_entry) {
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

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_value("aaa");
  for (uint64_t i = 0; i < 10; ++i) {
    le.clear_term();
    paxos1->replicateLog(le);
  }
  sleep(1);
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos3->getLastLogIndex());

  /* pipelining is on */
  std::vector<Paxos::ClusterInfoType> cis;
  paxos1->getClusterInfo(cis);
  EXPECT_EQ(cis[1].pipelining, true);
  EXPECT_EQ(cis[2].pipelining, true);

  std::string data(3ULL * 1024 * 1024, 'a');
  le.set_value(data);
  for (uint64_t i = 0; i < 10; ++i) {
    le.clear_term();
    paxos1->replicateLog(le);
  }
  sleep(1);
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos3->getLastLogIndex());
  /* pipelining is off */
  paxos1->getClusterInfo(cis);
  EXPECT_EQ(cis[1].pipelining, false);
  EXPECT_EQ(cis[2].pipelining, false);

  le.set_value("aaa");
  for (uint64_t i = 0; i < 1; ++i) {
    le.clear_term();
    paxos1->replicateLog(le);
  }
  sleep(1);
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos3->getLastLogIndex());
  /* pipelining is on again */
  paxos1->getClusterInfo(cis);
  EXPECT_EQ(cis[1].pipelining, true);
  EXPECT_EQ(cis[2].pipelining, true);

  delete paxos1;
  delete paxos2;
  delete paxos3;
  deleteDir("paxosLogTestDir11");
  deleteDir("paxosLogTestDir12");
  deleteDir("paxosLogTestDir13");
}
