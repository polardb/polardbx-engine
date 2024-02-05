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

TEST(consensus, Paxos_delete_node) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

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
  paxos3->init(strConfig, 3, 3);

  sleep(3);
  paxos2->requestVote();
  sleep(1);

  EXPECT_EQ(paxos2->getState(), Paxos::LEADER);

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_ikey("bbb");
  le.set_value("ccc");

  paxos2->replicateLog(le);

  // add member
  // // create 2 learners
  std::string strTmp2("127.0.0.1:11005");
  std::string strTmp3("127.0.0.1:11006");
  // there is no 127.0.0.1:11007 instance actually !
  std::string strTmp4("127.0.0.1:11007");
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir5", true, 4 * 1024 * 1024);
  Paxos *learner2 = new Paxos(timeout, rlog);
  learner2->initAsLearner(strTmp2, 12);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir6", true, 4 * 1024 * 1024);
  Paxos *learner3 = new Paxos(timeout, rlog);
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
  auto &servers = config->getServers();
  auto &learners = config->getLearners();
  EXPECT_EQ(config->getServerNum(), 3);
  EXPECT_EQ(config->getLearnerNum(), 2);
  EXPECT_EQ(learner2->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(learner3->getLastLogIndex(), paxos2->getLastLogIndex());

  // // add another learner
  strConfig.clear();
  strConfig.push_back(strTmp4);
  paxos2->changeLearners(Paxos::CCAddNode, strConfig);
  sleep(1);
  EXPECT_EQ(config->getServerNum(), 3);
  EXPECT_EQ(config->getLearnerNum(), 3);
  EXPECT_EQ(learner2->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(learner3->getLastLogIndex(), paxos2->getLastLogIndex());

  std::vector<Paxos::ClusterInfoType> cis;
  paxos2->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  /*
  for (auto& ci : cis)
  {
    std::cout<< "serverId:"<< ci.serverId<< " ipPort:"<< ci.ipPort<< "
  matchIndex:"<< ci.matchIndex<< " nextIndex:"<< ci.nextIndex<< " role:"<<
  ci.role<< " hasVoted:"<< ci.hasVoted  << " forceSync:" << ci.forceSync << "
  electionWeight: " << ci.electionWeight << std::endl<< std::flush;
  }
  */

  sleep(1);
  std::string tmpStr;
  tmpStr.clear();
  rlog1->getMetaData(std::string(Paxos::keyLearnerConfigure), tmpStr);
  EXPECT_EQ(
      tmpStr,
      std::string("127.0.0.1:11005$00;127.0.0.1:11006$00;127.0.0.1:11007$00"));

  // // change learners to followers one by one 1
  paxos2->changeMember(Paxos::CCAddNode, strTmp2);
  sleep(1);
  le.clear_term();
  paxos2->replicateLog(le);
  sleep(1);
  EXPECT_EQ(learner2->getState(), Paxos::FOLLOWER);
  EXPECT_EQ(config->getServerNum(), 4);
  EXPECT_EQ(config->getLearnerNum(), 2);

  // restart paxos1
  sleep(1);
  EXPECT_EQ(paxos1->getCommitIndex(), paxos2->getCommitIndex());
  tmpStr.clear();
  rlog1->getMetaData(std::string(Paxos::keyMemberConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0."
                                "1:11003#5N;127.0.0.1:11005#5N@1"));
  tmpStr.clear();
  rlog1->getMetaData(std::string(Paxos::keyLearnerConfigure), tmpStr);
  EXPECT_EQ(tmpStr, std::string("0;127.0.0.1:11006$00;127.0.0.1:11007$00"));
  delete paxos1;
  paxos1 = new Paxos(timeout, rlog1);
  strConfig.clear();
  paxos1->init(strConfig, 1, 1);
  sleep(1);
  auto config1 =
      std::dynamic_pointer_cast<StableConfiguration>(paxos1->getConfig());
  EXPECT_EQ(config1->membersToString(("127.0.0.1:11001")),
            std::string("127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#"
                        "5N;127.0.0.1:11005#5N@1"));

  // // change learners to followers one by one 2
  paxos2->changeMember(Paxos::CCAddNode, strTmp3);
  sleep(1);
  le.clear_term();
  paxos2->replicateLog(le);
  sleep(1);
  EXPECT_EQ(learner3->getState(), Paxos::FOLLOWER);
  EXPECT_EQ(config->getServerNum(), 5);
  EXPECT_EQ(config->getLearnerNum(), 1);

  // // check 2 new followers
  EXPECT_EQ(learner2->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(learner3->getLastLogIndex(), paxos2->getLastLogIndex());
  cis.clear();
  paxos2->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(cis[3].ipPort, strTmp2);
  EXPECT_EQ(cis[4].ipPort, strTmp3);
  // // delete one follower
  paxos2->changeMember(Paxos::CCDelNode, strTmp2);
  sleep(1);
  le.clear_term();
  paxos2->replicateLog(le);
  sleep(1);
  // EXPECT_EQ(config->getServerNum(), 4);
  // EXPECT_EQ(config->getLearnerNum(), 0);
  EXPECT_LT(learner2->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(learner3->getLastLogIndex(), paxos2->getLastLogIndex());
  paxos2->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(cis[3].ipPort, strTmp3);

  // add learner with mock log
  auto rlog7 =
      std::make_shared<RDPaxosLog>("paxosLogTestDir7", true, 4 * 1024 * 1024);
  le.set_term(1);
  le.set_index(0);
  le.set_optype(kMock);
  le.set_ikey("bbb");
  le.set_value("ccc");
  rlog7->debugSetLastLogIndex(2);
  rlog7->append(le);
  EXPECT_EQ(rlog7->getLastLogIndex(), 3);

  LogEntry le1;
  rlog7->getEntry(3, le1, false);
  EXPECT_EQ(le1.index(), 3);
  EXPECT_EQ(le1.term(), 1);

  std::cout << "add learner 127.0.0.1:11008" << std::endl;
  Paxos *learner4 = new Paxos(timeout, rlog7);
  std::string strTmp5("127.0.0.1:11008");
  learner4->initAsLearner(strTmp5, 15);
  strConfig.clear();
  strConfig.push_back(strTmp5);
  paxos2->changeLearners(Paxos::CCAddNode, strConfig);

  sleep(1);
  EXPECT_EQ(learner4->getLastLogIndex(), paxos2->getLastLogIndex());

  // add learner with error log(without mock)
  auto rlog8 =
      std::make_shared<RDPaxosLog>("paxosLogTestDir8", true, 4 * 1024 * 1024);
  le.set_term(1);
  le.set_index(0);
  le.set_optype(0);
  le.set_ikey("bbb");
  le.set_value("ccc");
  rlog8->debugSetLastLogIndex(2);
  rlog8->append(le);
  EXPECT_EQ(rlog8->getLastLogIndex(), 3);

  rlog8->getEntry(3, le1, false);
  EXPECT_EQ(le1.index(), 3);
  EXPECT_EQ(le1.term(), 1);

  std::cout << "add learner 127.0.0.1:11009" << std::endl;
  Paxos *learner5 = new Paxos(timeout, rlog8);
  std::string strTmp6("127.0.0.1:11009");
  learner5->initAsLearner(strTmp6, 16);
  strConfig.clear();
  strConfig.push_back(strTmp6);
  paxos2->changeLearners(Paxos::CCAddNode, strConfig);

  sleep(1);
  le.clear_term();
  paxos2->replicateLog(le);

  sleep(1);
  cis.clear();
  paxos2->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(learner5->getLastLogIndex(), 3);

  // test appliedIndex
  paxos1->updateAppliedIndex(5);
  sleep(1);
  paxos2->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(cis[0].appliedIndex, 5);

  EXPECT_EQ(learner4->getLastLogIndex(), paxos2->getLastLogIndex());

  // delete server 1
  strTmp2 = "127.0.0.1:11001";
  paxos2->changeMember(Paxos::CCDelNode, strTmp2);
  sleep(1);
  le.clear_term();
  paxos2->replicateLog(le);
  sleep(1);
  paxos2->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(paxos2->getCurrentLeader(), paxos2->getLocalServer()->serverId);

  delete paxos1;
  delete learner2;
  delete learner3;
  delete learner4;
  delete learner5;
  config.reset();
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
  deleteDir("paxosLogTestDir5");
  deleteDir("paxosLogTestDir6");
  deleteDir("paxosLogTestDir7");
  deleteDir("paxosLogTestDir8");
}
