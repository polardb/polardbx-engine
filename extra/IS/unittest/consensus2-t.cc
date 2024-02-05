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

TEST(consensus, paxos_leader_state_order) {
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

  while (paxos1->getCurrentLeader() == 0)
    ;
  Paxos::StateType state = paxos1->getState();
  EXPECT_TRUE(state == Paxos::LEADER || state == Paxos::FOLLOWER);
  while (paxos2->getCurrentLeader() == 0)
    ;
  state = paxos2->getState();
  EXPECT_TRUE(state == Paxos::LEADER || state == Paxos::FOLLOWER);
  while (paxos3->getCurrentLeader() == 0)
    ;
  state = paxos3->getState();
  EXPECT_TRUE(state == Paxos::LEADER || state == Paxos::FOLLOWER);

  sleep(1);
  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir61");
  deleteDir("paxosLogTestDir62");
  deleteDir("paxosLogTestDir63");
}

TEST(consensus, paxos_group_send) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  // strConfig.emplace_back("127.0.0.1:11003");

  uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1;
  rlog1 = rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir1");
  Paxos *paxos1 = new Paxos(timeout, rlog, 3000);
  paxos1->init(strConfig, 1, 1);
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir2");
  Paxos *paxos2 = new Paxos(timeout, rlog, 3000);
  paxos2->init(strConfig, 2, 2);
  // rlog= std::make_shared<FilePaxosLog>("paxosLogTestDir3");
  // Paxos *paxos3= new Paxos(timeout, rlog, 3000);
  // paxos3->init(strConfig, 3, 3);

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_value("AAAAAAAAAA");

  sleep(2);
  paxos1->setMaxPacketSize(1);
  paxos1->requestVote();
  for (uint i = 0; i < 30; ++i) {
    if (paxos1->getState() != Paxos::LEADER)
      sleep(1);
    else
      break;
  }
  ASSERT_TRUE(paxos1->getState() == Paxos::LEADER);

  paxos1->replicateLog(le);
  for (uint i = 0; i < 30; ++i) {
    if (paxos2->getLastLogIndex() != 2)
      sleep(1);
    else
      break;
  }
  ASSERT_TRUE(paxos2->getLastLogIndex() == 2);

  const Paxos::StatsType &stats = paxos2->getStats();
  uint64_t last_countOnMsgAppendLog = stats.countOnMsgAppendLog;
  uint64_t last_countOnHeartbeat = stats.countOnHeartbeat;
  std::cout << "countMsgAppendLog:" << stats.countMsgAppendLog
            << " countMsgRequestVote:" << stats.countMsgRequestVote
            << " countOnMsgAppendLog:" << stats.countOnMsgAppendLog
            << " countHeartbeat:" << stats.countHeartbeat
            << " countOnMsgRequestVote:" << stats.countOnMsgRequestVote
            << " countOnHeartbeat:" << stats.countOnHeartbeat
            << " countReplicateLog:" << stats.countReplicateLog << std::endl
            << std::flush;
  ::google::protobuf::RepeatedPtrField<LogEntry> entries;
  le.set_index(3);
  *(entries.Add()) = le;
  le.set_index(4);
  *(entries.Add()) = le;
  rlog1->append(entries);
  sleep(1);
  EXPECT_EQ(paxos2->getLastLogIndex(), 4);
  std::cout << "countMsgAppendLog:" << stats.countMsgAppendLog
            << " countMsgRequestVote:" << stats.countMsgRequestVote
            << " countOnMsgAppendLog:" << stats.countOnMsgAppendLog
            << " countHeartbeat:" << stats.countHeartbeat
            << " countOnMsgRequestVote:" << stats.countOnMsgRequestVote
            << " countOnHeartbeat:" << stats.countOnHeartbeat
            << " countReplicateLog:" << stats.countReplicateLog << std::endl
            << std::flush;
  EXPECT_EQ(stats.countOnMsgAppendLog -
                (stats.countOnHeartbeat - last_countOnHeartbeat),
            last_countOnMsgAppendLog + 2);

  entries.Clear();
  le.set_index(5);
  le.set_info(1);
  *(entries.Add()) = le;
  le.set_index(6);
  le.set_info(1);
  *(entries.Add()) = le;
  rlog1->append(entries);
  sleep(1);
  EXPECT_EQ(paxos2->getLastLogIndex(), 6);
  std::cout << "countMsgAppendLog:" << stats.countMsgAppendLog
            << " countMsgRequestVote:" << stats.countMsgRequestVote
            << " countOnMsgAppendLog:" << stats.countOnMsgAppendLog
            << " countHeartbeat:" << stats.countHeartbeat
            << " countOnMsgRequestVote:" << stats.countOnMsgRequestVote
            << " countOnHeartbeat:" << stats.countOnHeartbeat
            << " countReplicateLog:" << stats.countReplicateLog << std::endl
            << std::flush;
  EXPECT_EQ(stats.countOnMsgAppendLog -
                (stats.countOnHeartbeat - last_countOnHeartbeat),
            last_countOnMsgAppendLog + 3);

  entries.Clear();
  le.set_index(7);
  le.set_info(2);
  *(entries.Add()) = le;
  le.set_index(8);
  le.set_info(2);
  *(entries.Add()) = le;
  le.set_index(9);
  le.set_info(1);
  *(entries.Add()) = le;
  le.set_index(10);
  le.set_info(1);
  *(entries.Add()) = le;
  rlog1->append(entries);
  sleep(1);
  EXPECT_EQ(paxos2->getLastLogIndex(), 10);
  std::cout << "countMsgAppendLog:" << stats.countMsgAppendLog
            << " countMsgRequestVote:" << stats.countMsgRequestVote
            << " countOnMsgAppendLog:" << stats.countOnMsgAppendLog
            << " countHeartbeat:" << stats.countHeartbeat
            << " countOnMsgRequestVote:" << stats.countOnMsgRequestVote
            << " countOnHeartbeat:" << stats.countOnHeartbeat
            << " countReplicateLog:" << stats.countReplicateLog << std::endl
            << std::flush;
  EXPECT_EQ(stats.countOnMsgAppendLog -
                (stats.countOnHeartbeat - last_countOnHeartbeat),
            last_countOnMsgAppendLog + 5);

  entries.Clear();
  le.set_index(11);
  le.set_info(0);
  *(entries.Add()) = le;
  le.set_index(12);
  le.set_info(0);
  *(entries.Add()) = le;
  rlog1->append(entries);
  sleep(1);
  EXPECT_EQ(paxos2->getLastLogIndex(), 12);
  std::cout << "countMsgAppendLog:" << stats.countMsgAppendLog
            << " countMsgRequestVote:" << stats.countMsgRequestVote
            << " countOnMsgAppendLog:" << stats.countOnMsgAppendLog
            << " countHeartbeat:" << stats.countHeartbeat
            << " countOnMsgRequestVote:" << stats.countOnMsgRequestVote
            << " countOnHeartbeat:" << stats.countOnHeartbeat
            << " countReplicateLog:" << stats.countReplicateLog << std::endl
            << std::flush;
  EXPECT_EQ(stats.countOnMsgAppendLog -
                (stats.countOnHeartbeat - last_countOnHeartbeat),
            last_countOnMsgAppendLog + 7);

  delete paxos1;
  delete paxos2;
  // delete paxos3;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  // deleteDir("paxosLogTestDir3");
}
