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

TEST(consensus, Paxos_add_node2) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  // strConfig.emplace_back("127.0.0.1:11003");
  std::string strTmp3("127.0.0.1:11003");

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1;
  rlog1 = rlog =
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
  paxos3->initAsLearner(strTmp3, 3);

  sleep(3);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  strConfig.clear();
  strConfig.push_back(strTmp3);

  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  paxos1->changeMember(Paxos::CCAddNode, strTmp3);

  sleep(1);
  delete paxos1;

  sleep(7);
  EXPECT_TRUE(paxos2->getState() == Paxos::LEADER ||
              paxos3->getState() == Paxos::LEADER);
  sleep(2);
  EXPECT_TRUE(paxos2->getState() == Paxos::LEADER ||
              paxos3->getState() == Paxos::LEADER);
  sleep(2);
  EXPECT_TRUE(paxos2->getState() == Paxos::LEADER ||
              paxos3->getState() == Paxos::LEADER);

  delete paxos3;
  delete paxos2;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(consensus, Paxos_add_node3) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");
  std::string strTmp3("127.0.0.1:11004");
  std::string strTmp4("127.0.0.1:11005");

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

  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  paxos1->changeMember(Paxos::CCAddNode, strTmp3);

  // sleep(1);
  delete paxos1;

  sleep(7);
  EXPECT_TRUE(paxos2->getState() == Paxos::LEADER ||
              paxos3->getState() == Paxos::LEADER ||
              paxos4->getState() == Paxos::LEADER);
  sleep(2);
  EXPECT_TRUE(paxos2->getState() == Paxos::LEADER ||
              paxos3->getState() == Paxos::LEADER ||
              paxos4->getState() == Paxos::LEADER);
  sleep(2);
  EXPECT_TRUE(paxos2->getState() == Paxos::LEADER ||
              paxos3->getState() == Paxos::LEADER ||
              paxos4->getState() == Paxos::LEADER);

  Paxos *leader;
  if (paxos2->getState() == Paxos::LEADER)
    leader = paxos2;
  else if (paxos3->getState() == Paxos::LEADER)
    leader = paxos3;
  else
    leader = paxos4;
  EXPECT_EQ(leader->getState(), Paxos::LEADER);

  // add a learner with different cluster id
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir15", true, 4 * 1024 * 1024);
  rlog->setMetaData(Paxos::keyClusterId, 1);
  Paxos *paxos5 = new Paxos(timeout, rlog);
  paxos5->initAsLearner(strTmp4, 5);

  strConfig.clear();
  strConfig.push_back(strTmp4);

  leader->changeLearners(Paxos::CCAddNode, strConfig);

  sleep(2);
  EXPECT_EQ(leader->getConfig()->learnersToString(),
            std::string("127.0.0.1:11005$00"));
  EXPECT_EQ(paxos5->getLog()->getLastLogIndex(), 0);

  delete paxos5;
  delete paxos4;
  delete paxos3;
  delete paxos2;

  deleteDir("paxosLogTestDir11");
  deleteDir("paxosLogTestDir12");
  deleteDir("paxosLogTestDir13");
  deleteDir("paxosLogTestDir14");
  deleteDir("paxosLogTestDir15");
}

TEST(consensus, Paxos_readd_node) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  std::string strTmp3("127.0.0.1:11003");

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1;
  rlog1 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir1", true, 4 * 1024 * 1024);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir2", true, 4 * 1024 * 1024);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir3", true, 4 * 1024 * 1024);

  sleep(3);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  auto config =
      std::dynamic_pointer_cast<StableConfiguration>(paxos2->getConfig());
  auto net = paxos1->getService()->getEasyNet();

  // 1st add 127.0.0.1:11003
  Paxos *paxos3 = new Paxos(timeout, rlog);
  paxos3->initAsLearner(strTmp3, 3);
  strConfig.clear();
  strConfig.push_back(strTmp3);
  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  sleep(1);
  EXPECT_EQ(config->learnersToString(), strTmp3 + "$00");
  EXPECT_EQ(net->getConnCnt(), 2);

  paxos1->changeLearners(Paxos::CCDelNode, strConfig);
  sleep(1);
  EXPECT_EQ(config->learnersToString(), std::string(""));
  EXPECT_EQ(net->getConnCnt(), 1);

  delete paxos3;
  paxos3 = nullptr;

  // 2nd add 127.0.0.1:11003
  paxos3 = new Paxos(timeout, rlog);
  paxos3->initAsLearner(strTmp3, 3);
  strConfig.clear();
  strConfig.push_back(strTmp3);
  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  sleep(1);
  EXPECT_EQ(config->learnersToString(), strTmp3 + "$00");
  EXPECT_EQ(net->getConnCnt(), 2);

  paxos1->changeLearners(Paxos::CCDelNode, strConfig);
  sleep(1);
  EXPECT_EQ(config->learnersToString(), std::string(""));
  EXPECT_EQ(net->getConnCnt(), 1);

  delete paxos3;
  paxos3 = nullptr;

  // 3rd add 127.0.0.1:11003
  paxos3 = new Paxos(timeout, rlog);
  paxos3->initAsLearner(strTmp3, 3);
  strConfig.clear();
  strConfig.push_back(strTmp3);
  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  sleep(1);
  EXPECT_EQ(config->learnersToString(), strTmp3 + "$00");
  EXPECT_EQ(net->getConnCnt(), 2);

  paxos1->changeLearners(Paxos::CCDelNode, strConfig);
  sleep(1);
  EXPECT_EQ(config->learnersToString(), std::string(""));
  EXPECT_EQ(net->getConnCnt(), 1);

  delete paxos3;
  paxos3 = nullptr;

  delete paxos1;
  delete paxos2;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(consensus, Paxos_add_delay_node) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  std::string strTmp1("127.0.0.1:11001");
  std::string strTmp3("127.0.0.1:11003");
  std::string strTmp4("127.0.0.1:11004");

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1;
  rlog1 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir1", true, 4 * 1024 * 1024);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir2", true, 4 * 1024 * 1024);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir3", true, 4 * 1024 * 1024);

  sleep(3);
  paxos1->requestVote();
  sleep(1);

  // 1st add 127.0.0.1:11003
  strConfig.clear();
  strConfig.push_back(strTmp3);
  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  sleep(1);

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_value("aaa");
  for (uint i = 0; i < 20; ++i) {
    le.clear_term();
    paxos1->replicateLog(le);
  }

  strConfig.clear();
  strConfig.push_back(strTmp1);
  auto ret = paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  EXPECT_EQ(PaxosErrorCode::PE_EXISTS, ret);

  ret = paxos1->changeMember(Paxos::CCAddNode, strTmp4);
  EXPECT_EQ(PaxosErrorCode::PE_NOTFOUND, ret);

  paxos1->setMaxDelayIndex4NewMember(10);
  ret = paxos1->changeMember(Paxos::CCAddNode, strTmp3);
  EXPECT_EQ(PaxosErrorCode::PE_DELAY, ret);

  paxos1->setMaxDelayIndex4NewMember(50);
  ret = paxos1->changeMember(Paxos::CCAddNode, strTmp3);
  EXPECT_EQ(0, ret);

  delete paxos1;
  delete paxos2;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(consensus, Paxos_add_follower) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  std::string strTmp3("127.0.0.1:11003");

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1;
  rlog1 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir1", true, 4 * 1024 * 1024);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  paxos1->setMaxDelayIndex4NewMember(2);
  paxos1->init(strConfig, 1, 1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir2", true, 4 * 1024 * 1024);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  paxos2->setMaxDelayIndex4NewMember(2);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir3", true, 4 * 1024 * 1024);
  Paxos *paxos3 = new Paxos(timeout, rlog);
  paxos3->setMaxDelayIndex4NewMember(2);
  paxos3->initAsLearner(strTmp3, 3);

  sleep(3);
  paxos1->requestVote();
  sleep(1);
  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  le.set_value("log1");
  paxos1->replicateLog(le);
  le.set_index(0);
  le.set_optype(1);
  le.set_value("log2");
  paxos1->replicateLog(le);

  /* basic case (sync) */
  EXPECT_EQ(paxos1->changeMember(Paxos::CCAddLearnerAutoChange, strTmp3), 0);
  std::vector<Paxos::ClusterInfoType> cis;
  paxos1->getClusterInfo(cis);
  paxos1->printClusterInfo(cis);
  EXPECT_EQ(cis.size(), 3);
  sleep(2);
  EXPECT_EQ(paxos3->getState(), Paxos::FOLLOWER);

  /* run paxos after addFollower */
  strTmp3 = "127.0.0.1:11004";
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir4", true, 4 * 1024 * 1024);
  Paxos *paxos4 = new Paxos(timeout, rlog);
  paxos4->setMaxDelayIndex4NewMember(2);

  std::thread th1(
      [paxos4, &strTmp3](int sleeptime) {
        sleep(sleeptime);
        paxos4->initAsLearner(strTmp3, 4);
      },
      5);

  EXPECT_EQ(paxos1->changeMember(Paxos::CCAddLearnerAutoChange, strTmp3), 0);
  paxos1->getClusterInfo(cis);
  paxos1->printClusterInfo(cis);
  EXPECT_EQ(cis.size(), 4);

  th1.join();
  sleep(2);
  EXPECT_EQ(paxos4->getState(), Paxos::FOLLOWER);

  delete paxos1;
  delete paxos3;
  delete paxos4;
  delete paxos2;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
  deleteDir("paxosLogTestDir4");
}

TEST(consensus, Paxos_add_follower2) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  std::string strTmp3("127.0.0.1:11003");

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1;
  rlog1 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir1", true, 4 * 1024 * 1024);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  paxos1->setMaxDelayIndex4NewMember(2);
  paxos1->init(strConfig, 1, 1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir2", true, 4 * 1024 * 1024);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  paxos2->setMaxDelayIndex4NewMember(2);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir3", true, 4 * 1024 * 1024);
  Paxos *paxos3 = new Paxos(timeout, rlog);
  paxos3->setMaxDelayIndex4NewMember(2);
  paxos3->initAsLearner(strTmp3, 3);

  sleep(3);
  paxos1->requestVote();
  sleep(1);
  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  le.set_value("log1");
  paxos1->replicateLog(le);
  le.set_index(0);
  le.set_optype(1);
  le.set_value("log2");
  paxos1->replicateLog(le);

  /* basic case (async, timeout=5000) */
  paxos1->setConfigureChangeTimeout(5000);
  EXPECT_EQ(paxos1->changeMember(Paxos::CCAddLearnerAutoChange, strTmp3), 0);
  EXPECT_EQ(paxos3->getState(), Paxos::LEARNER);
  sleep(2);
  std::vector<Paxos::ClusterInfoType> cis;
  paxos1->getClusterInfo(cis);
  paxos1->printClusterInfo(cis);
  EXPECT_EQ(cis.size(), 3);
  sleep(2);
  EXPECT_EQ(paxos3->getState(), Paxos::FOLLOWER);

  /* leader transfer during addFollower */
  strTmp3 = "127.0.0.1:11004";
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir4", true, 4 * 1024 * 1024);
  Paxos *paxos4 = new Paxos(timeout, rlog);
  paxos4->setMaxDelayIndex4NewMember(2);

  std::thread th1([paxos1]() {
    sleep(1);
    paxos1->leaderTransfer(2);
  });

  std::thread th2([paxos4, &strTmp3]() {
    sleep(3);
    paxos4->initAsLearner(strTmp3, 4);
  });

  paxos1->setConfigureChangeTimeout(0);
  EXPECT_EQ(paxos1->changeMember(Paxos::CCAddLearnerAutoChange, strTmp3), -1);
  paxos2->getClusterInfo(cis);
  paxos2->printClusterInfo(cis);
  EXPECT_EQ(cis.size(), 4);

  th1.join();
  th2.join();
  sleep(4);
  EXPECT_EQ(paxos4->getState(), Paxos::FOLLOWER);

  delete paxos1;
  delete paxos2;
  delete paxos3;
  delete paxos4;
  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
  deleteDir("paxosLogTestDir4");
}

TEST(consensus, Paxos_add_follower3) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  std::string strTmp3("127.0.0.1:11003");

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1;
  rlog1 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir1", true, 4 * 1024 * 1024);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  paxos1->setMaxDelayIndex4NewMember(2);
  paxos1->init(strConfig, 1, 1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir2", true, 4 * 1024 * 1024);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  paxos2->setMaxDelayIndex4NewMember(2);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir3", true, 4 * 1024 * 1024);
  Paxos *paxos3 = new Paxos(timeout, rlog);
  paxos3->setMaxDelayIndex4NewMember(2);

  sleep(3);
  paxos1->requestVote();
  sleep(1);
  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  le.set_value("log1");
  paxos1->replicateLog(le);
  le.set_index(0);
  le.set_optype(1);
  le.set_value("log2");
  paxos1->replicateLog(le);

  /* basic case (async, trigger timeout retcode) */
  paxos1->setConfigureChangeTimeout(100);
  EXPECT_EQ(paxos1->changeMember(Paxos::CCAddLearnerAutoChange, strTmp3),
            PaxosErrorCode::PE_TIMEOUT);
  /* retry is ok */
  paxos1->changeMember(Paxos::CCAddLearnerAutoChange, strTmp3);
  paxos3->initAsLearner(strTmp3, 3);
  paxos1->changeMember(Paxos::CCAddLearnerAutoChange, strTmp3);

  sleep(2);
  std::vector<Paxos::ClusterInfoType> cis;
  paxos1->getClusterInfo(cis);
  paxos1->printClusterInfo(cis);
  EXPECT_EQ(cis.size(), 3);
  sleep(2);
  EXPECT_EQ(paxos3->getState(), Paxos::FOLLOWER);

  /* learner is deleted during addFollower */
  strTmp3 = "127.0.0.1:11004";
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir4", true, 4 * 1024 * 1024);
  Paxos *paxos4 = new Paxos(timeout, rlog);
  paxos4->setMaxDelayIndex4NewMember(2);

  std::thread th1([paxos1, &strTmp3]() {
    sleep(1);
    std::vector<std::string> strConfig{strTmp3};
    paxos1->changeLearners(Paxos::CCDelNode, strConfig);
  });

  std::thread th2([paxos4, &strTmp3]() {
    sleep(3);
    paxos4->initAsLearner(strTmp3, 4);
  });

  paxos1->setConfigureChangeTimeout(0);
  EXPECT_EQ(paxos1->changeMember(Paxos::CCAddLearnerAutoChange, strTmp3), -2);
  paxos1->getClusterInfo(cis);
  paxos1->printClusterInfo(cis);
  EXPECT_EQ(cis.size(), 3); /* still 3 not 4 */

  th1.join();
  th2.join();
  sleep(2);

  delete paxos1;
  delete paxos2;
  delete paxos3;
  delete paxos4;
  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
  deleteDir("paxosLogTestDir4");
}
