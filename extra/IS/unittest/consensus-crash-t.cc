/************************************************************************
 *
 * Copyright (c) 2017 Alibaba.com, Inc. All Rights Reserved
 * $Id:  consensus-crash-t.cc,v 1.0 02/22/2017 02:11:11 PM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file consensus-crash-t.cc
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 02/22/2017 02:11:11 PM
 * @version 1.0
 * @brief
 *
 **/

#include <gtest/gtest.h>
#include <atomic>
#include <thread>
#include "easyNet.h"
#include "paxos.h"
#include "paxos.pb.h"
#include "paxos_configuration.h"
#include "paxos_log.h"
#include "paxos_server.h"
#include "service.h"

#include "file_paxos_log.h"
#include "files.h"
#include "rd_paxos_log.h"

using namespace alisql;

TEST(consensus, follower_has_more_log) {
  /*
   * paxos1 has more log in term 0 (index:1-3), paxos2/paxos3 only have index:1
   * in term 0; But paxos2 became leader in term 1. This case used to check if
   * the paxos1 can truncate the log(2-3). We limit MaxPacketSize to 1 to let
   * leader send on log for each time.
   */
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_ikey("bbb");
  le.set_value("ccc");

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1, rlog2, rlog3;
  rlog1 = rlog =
      std::make_shared<FilePaxosLog>("paxosLogTestDir1", FilePaxosLog::LTSync);
  EXPECT_EQ(rlog1->getLastLogIndex(), 0);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  rlog2 = rlog =
      std::make_shared<FilePaxosLog>("paxosLogTestDir2", FilePaxosLog::LTSync);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  rlog3 = rlog =
      std::make_shared<FilePaxosLog>("paxosLogTestDir3", FilePaxosLog::LTSync);
  Paxos *paxos3 = new Paxos(timeout, rlog);

  le.set_term(0);
  le.set_index(1);
  rlog1->append(le);
  rlog2->append(le);
  rlog3->append(le);
  le.set_index(2);
  rlog1->append(le);
  le.set_index(3);
  rlog1->append(le);

  paxos2->init(strConfig, 2, 2);
  paxos3->init(strConfig, 3, 3);

  sleep(2);
  paxos2->setMaxPacketSize(1);
  paxos2->requestVote();
  sleep(1);
  const Paxos::StatsType &stats = paxos1->getStats();
  EXPECT_EQ(stats.countTruncateBackward, 0);
  paxos1->init(strConfig, 1, 1);
  paxos1->requestVote();
  sleep(1);
  EXPECT_EQ(stats.countTruncateBackward, 1);
  paxos2->replicateLog(le);
  paxos2->replicateLog(le);

  // check rlog1's log
  sleep(1);
  // EXPECT_EQ(rlog1->getLastLogIndex(), 3);
  LogEntry le1;
  rlog1->getEntry(2, le1);
  EXPECT_EQ(le1.term(), 2);
  EXPECT_EQ(le1.index(), 2);
  rlog1->getEntry(3, le1);
  EXPECT_EQ(le1.term(), 2);
  EXPECT_EQ(le1.index(), 3);
  rlog1->getEntry(4, le1);
  EXPECT_EQ(le1.term(), 2);
  EXPECT_EQ(le1.index(), 4);

  EXPECT_EQ(paxos2->getState(), Paxos::LEADER);

  auto s3 = paxos2->getConfig()->getServer(3);
  s3->disconnect(nullptr);

  // paxos2->replicateLog(le);
  sleep(4);

  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(consensus, leader_has_more_log) {
  /*
   * paxos1 has more log in term 0 (index:1-3), paxos2/paxos3 only have index:1
   * in term 0; But paxos1 became leader in term 1. We limit MaxPacketSize to 1
   * to let leader send on log for each time. We should not push
   * matchIndex/commitIndex when we not send this term's log entry to the
   * follower.
   */
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_ikey("bbb");
  le.set_value("ccc");

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1, rlog2, rlog3;
  rlog1 = rlog =
      std::make_shared<FilePaxosLog>("paxosLogTestDir1", FilePaxosLog::LTSync);
  EXPECT_EQ(rlog1->getLastLogIndex(), 0);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  rlog2 = rlog =
      std::make_shared<FilePaxosLog>("paxosLogTestDir2", FilePaxosLog::LTSync);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  rlog3 = rlog =
      std::make_shared<FilePaxosLog>("paxosLogTestDir3", FilePaxosLog::LTSync);
  Paxos *paxos3 = new Paxos(timeout, rlog);

  le.set_term(0);
  le.set_index(1);
  rlog1->append(le);
  rlog2->append(le);
  rlog3->append(le);
  le.set_index(2);
  rlog1->append(le);
  le.set_index(3);
  rlog1->append(le);

  paxos1->init(strConfig, 1, 1);
  paxos2->init(strConfig, 2, 2);
  paxos3->init(strConfig, 3, 3);

  sleep(2);
  paxos1->setMaxPacketSize(1);
  paxos1->debugMaxSendLogIndex = 2;
  paxos1->requestVote();
  sleep(2);
  EXPECT_EQ(paxos1->getCommitIndex(), 0);

  paxos1->debugMaxSendLogIndex = 0;
  sleep(2);
  EXPECT_EQ(paxos1->getCommitIndex(), 4);

  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(consensus, follower_has_different_log) {
  //
  // paxos1 has more log in term 0 (index:1-3), paxos2/paxos3 only have index:1
  // in term 0, 2-3 in term 1; This case used to check if the paxos2 can send
  // 2-3 to paxos1, and the paxos1 can truncate the log(2-3). We limit
  // MaxPacketSize to 1 to let leader send on log for each time.
  //
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_ikey("bbb");
  le.set_value("ccc");

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1, rlog2, rlog3;
  rlog1 = rlog =
      std::make_shared<FilePaxosLog>("paxosLogTestDir1", FilePaxosLog::LTSync);
  EXPECT_EQ(rlog1->getLastLogIndex(), 0);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  rlog2 = rlog =
      std::make_shared<FilePaxosLog>("paxosLogTestDir2", FilePaxosLog::LTSync);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  rlog3 = rlog =
      std::make_shared<FilePaxosLog>("paxosLogTestDir3", FilePaxosLog::LTSync);
  Paxos *paxos3 = new Paxos(timeout, rlog);

  le.set_term(0);
  le.set_index(1);
  rlog1->append(le);
  rlog2->append(le);
  rlog3->append(le);
  le.set_index(2);
  rlog1->append(le);
  le.set_index(3);
  rlog1->append(le);

  le.set_term(1);
  le.set_index(2);
  rlog2->append(le);
  le.set_index(3);
  rlog2->append(le);

  paxos2->init(strConfig, 2, 2);
  paxos3->init(strConfig, 3, 3);

  sleep(2);
  paxos2->setMaxPacketSize(1);
  paxos2->requestVote();
  sleep(1);
  const Paxos::StatsType &stats = paxos1->getStats();
  EXPECT_EQ(stats.countTruncateBackward, 0);
  paxos1->init(strConfig, 1, 1);
  paxos1->requestVote();
  sleep(1);
  EXPECT_EQ(stats.countTruncateBackward, 1);
  paxos2->replicateLog(le);
  paxos2->replicateLog(le);

  // check rlog1's log
  sleep(1);
  // EXPECT_EQ(rlog1->getLastLogIndex(), 3);
  LogEntry le1;
  rlog1->getEntry(2, le1);
  EXPECT_EQ(le1.term(), 1);
  EXPECT_EQ(le1.index(), 2);
  rlog1->getEntry(3, le1);
  EXPECT_EQ(le1.term(), 1);
  EXPECT_EQ(le1.index(), 3);
  rlog1->getEntry(4, le1);
  EXPECT_EQ(le1.term(), 2);
  EXPECT_EQ(le1.index(), 4);

  EXPECT_EQ(paxos2->getState(), Paxos::LEADER);

  auto s3 = paxos2->getConfig()->getServer(3);
  s3->disconnect(nullptr);

  // paxos2->replicateLog(le);
  sleep(4);

  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(consensus, follower_lose_log) {
  /*
   * leader has sent log to follower, and follower's match index has proceeded,
   * but follower fails to write the log to disk due to crash.
   */
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1, rlog2, rlog3;
  rlog1 = rlog =
      std::make_shared<FilePaxosLog>("paxosLogTestDir1", FilePaxosLog::LTMem);
  EXPECT_EQ(rlog1->getLastLogIndex(), 0);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  rlog2 = rlog =
      std::make_shared<FilePaxosLog>("paxosLogTestDir2", FilePaxosLog::LTMem);
  Paxos *paxos2 = new Paxos(timeout, rlog);
  rlog3 = rlog =
      std::make_shared<FilePaxosLog>("paxosLogTestDir3", FilePaxosLog::LTMem);
  Paxos *paxos3 = new Paxos(timeout, rlog);
  paxos1->setEnableAutoResetMatchIndex(true);

  paxos1->init(strConfig, 1, 1);
  paxos2->init(strConfig, 2, 2);
  paxos3->init(strConfig, 3, 3);

  sleep(2);

  paxos1->requestVote(true);

  sleep(2);
  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  LogEntry le;
  le.set_optype(0);
  le.set_ikey("bbb");
  le.set_value("ccc");

  paxos1->replicateLog(le);
  paxos1->replicateLog(le);

  sleep(2);
  EXPECT_EQ(paxos1->getCommitIndex(), 3);
  EXPECT_EQ(rlog2->getLastLogIndex(), 3);

  // fake follower's crash
  rlog2->truncateBackward(2);
  EXPECT_EQ(rlog2->getLastLogIndex(), 1);
  delete paxos2;
  paxos2 = new Paxos(timeout, rlog2);
  paxos2->init(strConfig, 2, 2);

  sleep(1);

  paxos1->replicateLog(le);

  sleep(2);
  EXPECT_EQ(paxos1->getCommitIndex(), 4);
  EXPECT_EQ(rlog2->getLastLogIndex(), 4);

  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(consensus, leader_truncate_configure_change) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  const uint64_t timeout = 5000;
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
  paxos1->requestVote();
  sleep(1);
  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  paxos1->setMaxPacketSize(1);

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  paxos1->replicateLog(le);
  paxos1->replicateLog(le);
  paxos1->debugMaxSendLogIndex = 3;

  sleep(2);
  EXPECT_EQ(paxos1->getLastLogIndex(), 3);
  EXPECT_EQ(paxos1->getCommitIndex(), 3);

  std::thread op([paxos1]() {
    std::vector<std::string> strConfig;
    strConfig.push_back("127.0.0.1:11005");
    paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  });

  sleep(2);
  EXPECT_EQ(paxos1->getLastLogIndex(), 4);
  EXPECT_EQ(paxos2->getLastLogIndex(), 3);
  EXPECT_EQ(paxos3->getLastLogIndex(), 3);

  paxos2->requestVote(1);
  sleep(1);
  EXPECT_EQ(paxos2->getState(), Paxos::LEADER);
  paxos1->debugMaxSendLogIndex = 0;

  paxos2->replicateLog(le);
  sleep(1);
  EXPECT_EQ(paxos1->getLastLogIndex(), 5);
  EXPECT_EQ(paxos2->getLastLogIndex(), 5);
  EXPECT_EQ(paxos3->getLastLogIndex(), 5);

  op.join();  // changeLearners is aborted

  paxos1->requestVote();
  sleep(1);
  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  // changeLearners is ok now
  std::vector<std::string> config;
  config.push_back("127.0.0.1:11005");
  EXPECT_EQ(paxos1->changeLearners(Paxos::CCAddNode, config),
            PaxosErrorCode::PE_NONE);
  EXPECT_EQ(paxos1->changeLearners(Paxos::CCDelNode, config),
            PaxosErrorCode::PE_NONE);

  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}
