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

namespace alisql {
// Wait until the paxos instance has replicated log entry with specified log
// index, using pooling with a timeout default to 30 seconds.
static bool waitUntilLogReplicatedWithIndex(Paxos *paxos, uint index,
                                            uint timeout = 30) {
  for (uint i = 0; i < timeout; ++i) {
    if (paxos->getLastLogIndex() == index)
      return true;
    else
      sleep(1);
  }
  return false;
}
}  // namespace alisql

TEST(consensus, paxos_consensus_async) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog2;
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir61");
  Paxos *paxos1 = new Paxos(timeout, rlog, 3000);
  paxos1->init(strConfig, 1, 1);
  rlog2 = rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir62");
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

  sleep(2);
  le.clear_term();
  paxos1->replicateLog(le);

  ASSERT_TRUE(waitUntilLogReplicatedWithIndex(paxos1, 3));
  ASSERT_TRUE(waitUntilLogReplicatedWithIndex(paxos2, 3));
  ASSERT_TRUE(waitUntilLogReplicatedWithIndex(paxos3, 3));

  // leader commit no need majority case:
  paxos1->debugMaxSendLogIndex = 3;
  le.clear_term();
  paxos1->replicateLog(le);
  ASSERT_TRUE(waitUntilLogReplicatedWithIndex(paxos1, 4));
  ASSERT_TRUE(waitUntilLogReplicatedWithIndex(paxos2, 3));
  ASSERT_TRUE(waitUntilLogReplicatedWithIndex(paxos3, 3));
  EXPECT_EQ(paxos1->getCommitIndex(), 3);

  std::atomic<int> i(0);
  std::thread th([&i, &paxos1]() {
    paxos1->waitCommitIndexUpdate(3);
    i.store(1);
  });
  sleep(2);
  EXPECT_EQ(i.load(), 0);

  paxos1->setConsensusAsync(1);
  sleep(2);
  EXPECT_EQ(i.load(), 1);
  EXPECT_EQ(paxos1->getCommitIndex(), 4);

  // follower lost log case:
  paxos1->setConsensusAsync(0);
  paxos1->debugMaxSendLogIndex = 0;
  ASSERT_TRUE(waitUntilLogReplicatedWithIndex(paxos1, 4));
  ASSERT_TRUE(waitUntilLogReplicatedWithIndex(paxos2, 4));
  ASSERT_TRUE(waitUntilLogReplicatedWithIndex(paxos3, 4));

  delete paxos2;
  rlog2->truncateBackward(4);
  paxos2 = new Paxos(timeout, rlog2, 3000);
  paxos2->init(strConfig, 2, 2);
  sleep(1);
  ASSERT_TRUE(waitUntilLogReplicatedWithIndex(paxos2, 3));

  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);
  EXPECT_EQ(paxos1->getCommitIndex(), 5);
  ASSERT_TRUE(waitUntilLogReplicatedWithIndex(paxos2, 3));

  paxos1->setConsensusAsync(1);
  ASSERT_TRUE(waitUntilLogReplicatedWithIndex(paxos2, 5));

  th.join();

  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir61");
  deleteDir("paxosLogTestDir62");
  deleteDir("paxosLogTestDir63");
}

TEST(consensus, paxos_consensus_async_follower_still_sync) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");
  strConfig.emplace_back("127.0.0.1:11004");
  strConfig.emplace_back("127.0.0.1:11005");

  uint64_t timeout = 10000;
  std::shared_ptr<PaxosLog> rlog, rlog2;
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir61");
  Paxos *paxos1 = new Paxos(timeout, rlog, 3000);
  paxos1->init(strConfig, 1, 1);
  rlog2 = rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir62");
  Paxos *paxos2 = new Paxos(timeout, rlog, 3000);
  paxos2->init(strConfig, 2, 2);
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir63");
  Paxos *paxos3 = new Paxos(timeout, rlog, 3000);
  paxos3->init(strConfig, 3, 3);
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir64");
  Paxos *paxos4 = new Paxos(timeout, rlog, 3000);
  paxos4->init(strConfig, 4, 4);
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir65");
  Paxos *paxos5 = new Paxos(timeout, rlog, 3000);
  paxos5->init(strConfig, 5, 5);

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
  le.clear_term();
  paxos1->replicateLog(le);

  ASSERT_TRUE(waitUntilLogReplicatedWithIndex(paxos1, 3));
  ASSERT_TRUE(waitUntilLogReplicatedWithIndex(paxos2, 3));
  ASSERT_TRUE(waitUntilLogReplicatedWithIndex(paxos3, 3));
  ASSERT_TRUE(waitUntilLogReplicatedWithIndex(paxos4, 3));
  ASSERT_TRUE(waitUntilLogReplicatedWithIndex(paxos5, 3));

  paxos1->setConsensusAsync(1);
  delete paxos3;
  delete paxos4;
  delete paxos5;

  le.clear_term();
  paxos1->replicateLog(le);

  ASSERT_TRUE(waitUntilLogReplicatedWithIndex(paxos1, 4));
  ASSERT_TRUE(waitUntilLogReplicatedWithIndex(paxos2, 4));

  // check leader async
  EXPECT_EQ(paxos1->getCommitIndex(), 4);
  std::atomic<int> i1(0);
  std::thread th1([&i1, &paxos1]() {
    paxos1->waitCommitIndexUpdate(3);
    i1.store(1);
  });
  sleep(1);
  EXPECT_EQ(i1.load(), 1);
  // check follower async
  EXPECT_EQ(paxos2->getCommitIndex(), 3);
  std::atomic<int> i2(0);
  std::thread th2([&i2, &paxos2]() {
    paxos2->waitCommitIndexUpdate(3);
    i2.store(1);
  });
  sleep(1);
  EXPECT_EQ(i2.load(), 0);

  // check leader configure change sync
  std::vector<Paxos::ClusterInfoType> cis;
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(cis[1].electionWeight, 5);
  std::thread th3([&paxos1]() { paxos1->configureMember(2, false, 8); });
  sleep(2);
  cis.clear();
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);
  EXPECT_EQ(cis[1].electionWeight, 5);
  EXPECT_EQ(paxos1->getCommitIndex(), 5);
  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  delete paxos1;
  delete paxos2;

  th1.join();
  th2.join();
  th3.join();

  deleteDir("paxosLogTestDir61");
  deleteDir("paxosLogTestDir62");
  deleteDir("paxosLogTestDir63");
  deleteDir("paxosLogTestDir64");
  deleteDir("paxosLogTestDir65");
}
