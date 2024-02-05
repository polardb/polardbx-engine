/************************************************************************
 *
 * Copyright (c) 2019 Alibaba.com, Inc. All Rights Reserved
 * $Id:  consensus-leader-transfer-t.cc,v 1.0 07/31/2019 16:23:00 PM
 *aili.xp(aili.xp@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file consensus-leader-transfer-t.cc
 * @author aili.xp(aili.xp@alibaba-inc.com)
 * @date 07/31/2019 16:23:00 PM
 * @version 1.0
 * @brief unit test for leader transfer
 *
 **/

#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

#include "file_paxos_log.h"
#include "files.h"
#include "paxos.h"

using namespace alisql;

TEST(LeaderTransfer, UnnecessaryLog) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  const uint64_t timeout = 10000;
  std::string dir1 = std::string("paxosLogTestDir71");
  std::shared_ptr<PaxosLog> rlog1 =
      std::make_shared<FilePaxosLog>(dir1, FilePaxosLog::LogType::LTMem);
  Paxos *paxos1 = new Paxos(timeout, rlog1);
  paxos1->init(strConfig, 1, 1);
  std::string dir2 = std::string("paxosLogTestDir72");
  std::shared_ptr<PaxosLog> rlog2 =
      std::make_shared<FilePaxosLog>(dir2, FilePaxosLog::LogType::LTMem);
  Paxos *paxos2 = new Paxos(timeout, rlog2);
  paxos2->init(strConfig, 2, 2);
  std::string dir3 = std::string("paxosLogTestDir73");
  std::shared_ptr<PaxosLog> rlog3 =
      std::make_shared<FilePaxosLog>(dir3, FilePaxosLog::LogType::LTMem);
  Paxos *paxos3 = new Paxos(timeout, rlog3);
  paxos3->init(strConfig, 3, 3);

  sleep(1);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  uint64_t term = paxos1->getTerm();

  sleep(1);

  EXPECT_EQ(paxos1->getLastLogIndex(), 1);
  EXPECT_EQ(paxos1->getCommitIndex(), 1);

  paxos1->setMaxPacketSize(10);
  for (int i = 0; i < 20; ++i) {
    LogEntry le;
    le.set_index(0);
    le.set_term(paxos1->getTerm());
    le.set_info(0);
    le.set_optype(0);
    paxos1->getLog()->append(le);
  }

  paxos1->leaderTransfer(2);

  // wait for leader transfer taking effect
  sleep(12);

  EXPECT_EQ(paxos2->getState(), Paxos::LEADER);
  EXPECT_EQ(paxos2->getTerm(), term + 1);  // only one election should happen
  EXPECT_EQ(paxos2->getCommitIndex(), 22);
  EXPECT_EQ(paxos2->getLastLogIndex(), 22);

  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir71");
  deleteDir("paxosLogTestDir72");
  deleteDir("paxosLogTestDir73");
}

class PaxosLogTmp : public FilePaxosLog {
 public:
  PaxosLogTmp(const std::string &dataDir, FilePaxosLog::LogTypeT type)
      : FilePaxosLog(dataDir, type) {}
  virtual ~PaxosLogTmp() {}

  virtual bool isStateMachineHealthy() { return healthy; }
  bool healthy = true;
};

TEST(LeaderTransfer, AutoLeaderTransfer) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  const uint64_t timeout = 1000;
  std::string dir1 = std::string("paxosLogTestDir71");
  std::shared_ptr<PaxosLog> rlog1 =
      std::make_shared<PaxosLogTmp>(dir1, FilePaxosLog::LogType::LTMem);
  Paxos *paxos1 = new Paxos(timeout, rlog1);
  paxos1->init(strConfig, 1, 1);
  std::string dir2 = std::string("paxosLogTestDir72");
  std::shared_ptr<PaxosLog> rlog2 =
      std::make_shared<PaxosLogTmp>(dir2, FilePaxosLog::LogType::LTMem);
  Paxos *paxos2 = new Paxos(timeout, rlog2);
  paxos2->init(strConfig, 2, 2);
  std::string dir3 = std::string("paxosLogTestDir73");
  std::shared_ptr<PaxosLog> rlog3 =
      std::make_shared<PaxosLogTmp>(dir3, FilePaxosLog::LogType::LTMem);
  Paxos *paxos3 = new Paxos(timeout, rlog3);
  paxos3->init(strConfig, 3, 3);

  sleep(1);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  paxos1->setEnableAutoLeaderTransfer(true);
  paxos1->setAutoLeaderTransferCheckSeconds(5);
  paxos2->setEnableAutoLeaderTransfer(true);
  paxos2->setAutoLeaderTransferCheckSeconds(5);
  paxos3->setEnableAutoLeaderTransfer(true);
  paxos3->setAutoLeaderTransferCheckSeconds(5);
  uint64_t term = paxos1->getTerm();

  // case 1: log type node
  paxos1->setAsLogType(true);

  sleep(2);

  ASSERT_EQ(paxos1->getTerm(), term + 1);

  paxos1->setAsLogType(false);

  // case 2: state machine not healthy

  ((PaxosLogTmp *)rlog1.get())->healthy = false;

  paxos2->leaderTransfer(1);
  paxos3->leaderTransfer(1);

  sleep(2);

  ASSERT_EQ(paxos1->getState(), Paxos::LEADER);
  ASSERT_EQ(paxos1->getTerm(), term + 2);

  sleep(5);

  ASSERT_NE(paxos1->getState(), Paxos::LEADER);
  ASSERT_EQ(paxos1->getTerm(), term + 3);

  // case 3: test whether target will be random
  ((PaxosLogTmp *)rlog1.get())->healthy = false;
  ((PaxosLogTmp *)rlog2.get())->healthy = false;
  ((PaxosLogTmp *)rlog3.get())->healthy = false;

  paxos1->leaderTransfer(2);

  int l1 = 0, l2 = 0, l3 = 0;
  for (int i = 0; i < 10; ++i) {
    sleep(6);
    if (paxos1->getState() == Paxos::LEADER) l1++;
    if (paxos2->getState() == Paxos::LEADER) l2++;
    if (paxos3->getState() == Paxos::LEADER) l3++;
  }

  ASSERT_NE(l1, 0);
  ASSERT_NE(l2, 0);
  ASSERT_NE(l3, 0);

  std::cout << "l1: " << l1 << std::endl;
  std::cout << "l2: " << l2 << std::endl;
  std::cout << "l3: " << l3 << std::endl;

  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir71");
  deleteDir("paxosLogTestDir72");
  deleteDir("paxosLogTestDir73");
}
