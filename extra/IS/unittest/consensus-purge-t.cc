/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  consensus-purge-t.cc,v 1.0 Sep 3, 2019 1:34:10 PM
 *jarry.zj(jarry.zj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file consensus-purge-t.cc
 * @author jarry.zj(jarry.zj@alibaba-inc.com)
 * @date Sep 3, 2019 1:34:10 PM
 * @version 1.0
 * @brief
 *
 **/
#include <gtest/gtest.h>
#include "files.h"
#include "paxos.h"
#include "rd_paxos_log.h"

using namespace alisql;

static void msleep(uint64_t t) {
  struct timeval sleeptime;
  if (t == 0) return;
  sleeptime.tv_sec = t / 1000;
  sleeptime.tv_usec = (t - (sleeptime.tv_sec * 1000)) * 1000;
  select(0, 0, 0, 0, &sleeptime);
}

TEST(consensus, paxos_purgeLog_normal) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  uint64_t timeout = 12500;
  std::shared_ptr<PaxosLog> rlog;
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir31", true, 4 * 1024 * 1024);
  Paxos *paxos1 = new Paxos(timeout, rlog, 3000);
  paxos1->init(strConfig, 1, 1);
  paxos1->initAutoPurgeLog(true);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir32", true, 4 * 1024 * 1024);
  Paxos *paxos2 = new Paxos(timeout, rlog, 3000);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir33", true, 4 * 1024 * 1024);
  Paxos *paxos3 = new Paxos(timeout, rlog, 3000);
  paxos3->init(strConfig, 3, 3);

  sleep(3);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_value("aaa");
  paxos1->replicateLog(le);

  le.set_value("bbb");
  paxos1->replicateLog(le);
  // wait replicate
  sleep(3);
  paxos1->updateAppliedIndex(paxos1->getCommitIndex() - 1);  // purge paxos1
  // wait purge log
  sleep(3);
  EXPECT_EQ(paxos1->getLog()->getLength(), 2);
  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir31");
  deleteDir("paxosLogTestDir32");
  deleteDir("paxosLogTestDir33");
}

TEST(consensus, paxos_purgeLog_nopurge_forcepurge) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  uint64_t timeout = 12500;
  std::shared_ptr<PaxosLog> rlog;
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir41", true, 4 * 1024 * 1024);
  Paxos *paxos1 = new Paxos(timeout, rlog, 3000);
  paxos1->init(strConfig, 1, 1);
  paxos1->initAutoPurgeLog(false, true, NULL);  // disable autoPurge
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir42", true, 4 * 1024 * 1024);
  Paxos *paxos2 = new Paxos(timeout, rlog, 3000);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir43", true, 4 * 1024 * 1024);
  Paxos *paxos3 = new Paxos(timeout, rlog, 3000);
  paxos3->init(strConfig, 3, 3);

  sleep(3);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_value("aaa");
  paxos1->replicateLog(le);

  le.set_value("bbb");
  paxos1->replicateLog(le);
  // wait replicate
  sleep(3);
  paxos1->updateAppliedIndex(paxos1->getCommitIndex());  // purge paxos1
  // wait purge log
  sleep(3);
  EXPECT_NE(paxos1->getLog()->getLength(), 1);
  // test force purge log here
  paxos1->forcePurgeLog(false, 1000);
  sleep(3);
  EXPECT_EQ(paxos1->getLog()->getLength(), 1);
  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir41");
  deleteDir("paxosLogTestDir42");
  deleteDir("paxosLogTestDir43");
}

TEST(consensus, paxos_purgeLog_disable_appliedIndex) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  uint64_t timeout = 12500;
  std::shared_ptr<PaxosLog> rlog;
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir51", true, 4 * 1024 * 1024);
  Paxos *paxos1 = new Paxos(timeout, rlog, 3000);
  paxos1->init(strConfig, 1, 1);
  paxos1->initAutoPurgeLog(true, false,
                           NULL);  // disable appliedIndex use commitIndex
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir52", true, 4 * 1024 * 1024);
  Paxos *paxos2 = new Paxos(timeout, rlog, 3000);
  paxos2->init(strConfig, 2, 2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir53", true, 4 * 1024 * 1024);
  Paxos *paxos3 = new Paxos(timeout, rlog, 3000);
  paxos3->init(strConfig, 3, 3);

  sleep(3);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_value("aaa");
  paxos1->replicateLog(le);

  le.set_value("bbb");
  paxos1->replicateLog(le);
  // wait replicate
  sleep(3);
  // wait purge log
  sleep(3);
  EXPECT_EQ(paxos1->getLog()->getLength(), 1);
  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir51");
  deleteDir("paxosLogTestDir52");
  deleteDir("paxosLogTestDir53");
}

TEST(consensus, paxos_purgeLog_local) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  uint64_t timeout = 10000;
  std::shared_ptr<PaxosLog> rlog;
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir41", true, 4 * 1024 * 1024);
  Paxos *paxos1 = new Paxos(timeout, rlog, 3000);
  paxos1->init(strConfig, 1, 1);
  paxos1->initAutoPurgeLog(false, false, NULL);  // disable autoPurge
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir42", true, 4 * 1024 * 1024);
  Paxos *paxos2 = new Paxos(timeout, rlog, 3000);
  paxos2->init(strConfig, 2, 2);
  paxos2->initAutoPurgeLog(false, false, NULL);  // disable autoPurge
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir43", true, 4 * 1024 * 1024);
  Paxos *paxos3 = new Paxos(timeout, rlog, 3000);
  paxos3->init(strConfig, 3, 3);

  std::string learnerAddr = "127.0.0.1:11004";
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir44", true, 4 * 1024 * 1024);
  Paxos *paxos4 = new Paxos(timeout, rlog, 3000);
  paxos4->initAsLearner(learnerAddr, 4);

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
  paxos1->configureLearner(100, 1);
  sleep(1);
  EXPECT_EQ(paxos1->getConfig()->learnersToString(),
            std::string("127.0.0.1:11004$01"));
  uint64_t len = paxos1->getLog()->getLength();
  paxos1->forceFixMatchIndex(100, 2);
  paxos1->forcePurgeLog(true /* local */);
  sleep(1);
  EXPECT_EQ(paxos1->getLog()->getLength(), len - 2);

  /* test follower purge local */
  paxos1->configureLearner(100, 2);
  paxos1->replicateLog(le);
  sleep(1);
  while (paxos2->getCommitIndex() != paxos1->getCommitIndex()) {
  }
  sleep(1);
  paxos2->forcePurgeLog(true);
  msleep(1000);
  EXPECT_EQ(paxos2->getLog()->getLength(), 1);
  paxos1->replicateLog(le);
  sleep(1);
  EXPECT_EQ(paxos2->getLog()->getLength(), 2);
  while (paxos2->getCommitIndex() != paxos1->getCommitIndex()) {
  }
  sleep(1);
  paxos2->forcePurgeLog(true);
  msleep(1000);
  EXPECT_EQ(paxos2->getLog()->getLength(), 1);
  delete paxos4;
  paxos1->replicateLog(le);
  sleep(1);
  EXPECT_EQ(paxos2->getLog()->getLength(), 2);
  while (paxos2->getCommitIndex() != paxos1->getCommitIndex()) {
  }
  sleep(1);
  paxos2->forcePurgeLog(true);
  msleep(1000);
  EXPECT_EQ(paxos2->getLog()->getLength(),
            2);  // still 2 because learner is shutdown

  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir41");
  deleteDir("paxosLogTestDir42");
  deleteDir("paxosLogTestDir43");
  deleteDir("paxosLogTestDir44");
}
