/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  consensus-trx.cc,v 1.0 Nov 30, 2017 5:05:02 PM
 *jarry.zj(jarry.zj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file consensus-trx.cc
 * @author jarry.zj(jarry.zj@alibaba-inc.com)
 * @date Nov 30, 2017 5:05:02 PM
 * @version 1.0
 * @brief unit test for consensus with commit dependency (large trx)
 *
 **/

#include <gtest/gtest.h>
#include <atomic>
#include <string>
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

/*
 * testcase list:
 *  followers should keep consistency in all cases
 *  normal case: large trx commitindex update atomically
 *  leader crash during a large trx (cannot transfer leader during a large trx )
 *  write new logs during a large trx recovery
 *  debug very slow case:
 *    just very slow, keep heartbeat
 *    leader transfer during a large trx recovery (weight election should also
 * not work) crash during a large trx recovery another leader requestvote during
 * a large trx recovery
 */

TEST(trx, basic) {
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
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  LogEntry le;
  uint64_t cindex;  // currentIndex
  le.set_index(0);
  le.set_optype(kNormal);
  le.set_value("first");
  paxos1->replicateLog(le);
  cindex = le.index();
  sleep(1);
  EXPECT_EQ(paxos1->getCommitIndex(), cindex);

  le.Clear();
  le.set_optype(kNormal);
  le.set_value("second");
  paxos1->replicateLog(le);
  for (int i = 0; i < 9; ++i) {
    le.Clear();
    le.set_optype(kCommitDep);
    le.set_value("part-" + std::to_string(i));
    paxos1->replicateLog(le);
    usleep(100000);
  }
  sleep(1);
  EXPECT_EQ(paxos1->getCommitIndex(), cindex + 1);  // still the old one
  le.Clear();
  le.set_optype(kCommitDepEnd);
  le.set_value("part-end");
  paxos1->replicateLog(le);
  sleep(1);
  EXPECT_EQ(paxos1->getCommitIndex(), cindex + 11);
  EXPECT_EQ(paxos1->getCommitIndex(), paxos1->getLastLogIndex());
  le.Clear();
  le.set_optype(kNormal);
  le.set_value("final");
  paxos1->replicateLog(le);
  sleep(1);
  EXPECT_EQ(paxos1->getCommitIndex(), paxos1->getLastLogIndex());
  EXPECT_EQ(paxos1->getCommitIndex(), paxos2->getCommitIndex());

  delete paxos1;
  delete paxos2;
  delete paxos3;
  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(trx, leader_crash_during_trx) {
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
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  LogEntry le;
  uint64_t cindex = paxos1->getLastLogIndex();  // currentIndex
  for (int i = 0; i < 10; ++i) {
    le.Clear();
    le.set_optype(kCommitDep);
    le.set_value("part-" + std::to_string(i));
    paxos1->replicateLog(le);
    usleep(100000);
  }
  sleep(1);
  EXPECT_EQ(paxos1->getCommitIndex(), cindex);  // still the old one
  cindex = paxos1->getLastLogIndex();
  delete paxos1;
  while (paxos2->getState() != Paxos::LEADER &&
         paxos3->getState() != Paxos::LEADER) {
  }
  Paxos *leader;
  if (paxos2->getState() == Paxos::LEADER) leader = paxos2;
  if (paxos3->getState() == Paxos::LEADER) leader = paxos3;
  le.Clear();
  le.set_optype(kNormal);
  le.set_value("normal case");
  leader->replicateLog(le);  // may fail
  EXPECT_EQ(le.term(), 0);
  sleep(1);
  EXPECT_EQ(leader->getCommitIndex(), cindex + 1);
  for (int i = 0; i < 11; ++i) {
    // check all kCommitDep is replaced
    leader->getLog()->getEntry(cindex - i, le, false);
    EXPECT_NE(le.optype(), kCommitDep);
  }
  le.Clear();
  le.set_optype(kNormal);
  le.set_value("final");
  leader->replicateLog(le);
  sleep(1);
  EXPECT_EQ(paxos2->getCommitIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(paxos2->getCommitIndex(), paxos3->getCommitIndex());

  delete paxos2;
  delete paxos3;
  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(trx, trx_recovery_slow) {
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
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  LogEntry le;
  uint64_t cindex = paxos1->getLastLogIndex();  // currentIndex
  for (int i = 0; i < 3; ++i) {
    le.Clear();
    le.set_optype(kCommitDep);
    le.set_value("part-" + std::to_string(i));
    paxos1->replicateLog(le);
    usleep(100000);
  }
  sleep(1);
  EXPECT_EQ(paxos1->getCommitIndex(), cindex);  // still the old one
  cindex = paxos1->getLastLogIndex();
  paxos2->debugResetLogSlow = true;
  paxos3->debugResetLogSlow = true;
  delete paxos1;
  while (paxos2->getState() != Paxos::LEADER &&
         paxos3->getState() != Paxos::LEADER) {
  }
  Paxos *leader, *other;
  if (paxos2->getState() == Paxos::LEADER) {
    leader = paxos2;
    other = paxos3;
  }
  if (paxos3->getState() == Paxos::LEADER) {
    leader = paxos3;
    other = paxos2;
  }
  std::atomic<bool> foo(false);
  other->setStateChangeCb([&foo](Paxos::StateType state, uint64_t term,
                                 uint64_t index) { foo = true; });
  le.Clear();
  le.set_optype(kNormal);
  le.set_value("normal case");
  leader->replicateLog(le);  // should fail
  EXPECT_EQ(le.term(), 0);
  sleep(6);
  for (int i = 0; i < 5; ++i) {
    leader->replicateLog(le);  // success
    EXPECT_NE(le.term(), 0);
  }
  sleep(2);
  /* recovery is slow but still send heartbeat */
  /* ensure other never statechange (requestVote) */
  EXPECT_EQ(foo.load(), false);
  EXPECT_EQ(paxos2->getCommitIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(paxos2->getCommitIndex(), paxos3->getCommitIndex());

  easy_warn_log("Restart paxos1.........\n");
  /* now I bring paxos1 back */
  strConfig.clear();
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");
  paxos1 = new Paxos(timeout, rlog1);
  paxos1->init(strConfig, 1, 1);
  sleep(2);
  EXPECT_EQ(paxos2->getCommitIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(paxos2->getCommitIndex(), paxos3->getCommitIndex());
  EXPECT_EQ(paxos2->getCommitIndex(), paxos1->getCommitIndex());
  delete paxos1;
  delete paxos2;
  delete paxos3;
  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(trx, trx_recovery_slow2) {
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
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  LogEntry le;
  uint64_t cindex = paxos1->getLastLogIndex();  // currentIndex
  for (int i = 0; i < 3; ++i) {
    le.Clear();
    le.set_optype(kCommitDep);
    le.set_value("part-" + std::to_string(i));
    paxos1->replicateLog(le);
    usleep(100000);
  }
  sleep(1);
  EXPECT_EQ(paxos1->getCommitIndex(), cindex);  // still the old one
  cindex = paxos1->getLastLogIndex();
  paxos2->debugResetLogSlow = true;
  paxos3->debugResetLogSlow = true;
  delete paxos1;
  while (paxos2->getState() != Paxos::LEADER &&
         paxos3->getState() != Paxos::LEADER) {
  }
  Paxos *leader, *other;
  if (paxos2->getState() == Paxos::LEADER) {
    leader = paxos2;
    other = paxos3;
  }
  if (paxos3->getState() == Paxos::LEADER) {
    leader = paxos3;
    other = paxos2;
  }
  easy_warn_log("Restart paxos1.........\n");
  /* now I bring paxos1 back, during trx recovery */
  strConfig.clear();
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");
  paxos1 = new Paxos(timeout, rlog1);
  paxos1->init(strConfig, 1, 1);

  le.Clear();
  le.set_optype(kNormal);
  le.set_value("normal case");
  leader->replicateLog(le);  // should fail
  EXPECT_EQ(le.term(), 0);
  sleep(6);
  for (int i = 0; i < 5; ++i) {
    leader->replicateLog(le);  // success
    EXPECT_NE(le.term(), 0);
  }
  sleep(2);
  EXPECT_EQ(paxos2->getCommitIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(paxos2->getCommitIndex(), paxos3->getCommitIndex());
  EXPECT_EQ(paxos2->getCommitIndex(), paxos1->getCommitIndex());
  delete paxos1;
  delete paxos2;
  delete paxos3;
  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(trx, trx_recovery_slow_leader_transfer) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  const uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog, rlog1, rlog2;
  rlog1 = rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir1", true, 4 * 1024 * 1024);
  Paxos *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1);
  rlog2 = rlog =
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

  LogEntry le;
  uint64_t cindex = paxos1->getLastLogIndex();  // currentIndex
  for (int i = 0; i < 3; ++i) {
    le.Clear();
    le.set_optype(kCommitDep);
    le.set_value("part-" + std::to_string(i));
    paxos1->replicateLog(le);
    usleep(100000);
  }
  sleep(1);
  EXPECT_EQ(paxos1->getCommitIndex(), cindex);  // still the old one
  cindex = paxos1->getLastLogIndex();
  paxos2->debugResetLogSlow = true;
  paxos3->debugResetLogSlow = true;
  delete paxos1;
  while (paxos2->getState() != Paxos::LEADER &&
         paxos3->getState() != Paxos::LEADER) {
  }
  Paxos *leader, *other;
  if (paxos2->getState() == Paxos::LEADER) {
    leader = paxos2;
    other = paxos3;
  }
  if (paxos3->getState() == Paxos::LEADER) {
    leader = paxos3;
    other = paxos2;
  }
  easy_warn_log("Restart paxos1.........\n");
  /* now I bring paxos1 back, during trx recovery */
  strConfig.clear();
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");
  paxos1 = new Paxos(timeout, rlog1);
  paxos1->init(strConfig, 1, 1);

  le.Clear();
  le.set_optype(kNormal);
  le.set_value("normal case");
  leader->replicateLog(le);  // may fail
  EXPECT_EQ(le.term(), 0);
  EXPECT_EQ(leader->leaderTransfer(other->getLocalServer()->serverId),
            PaxosErrorCode::PE_CONFLICTS); /* leader transfer is disabled */
  /* now crash leader */
  delete leader;
  sleep(6);
  EXPECT_EQ(paxos1->getCommitIndex(), paxos1->getLastLogIndex());
  EXPECT_EQ(paxos1->getCommitIndex(), other->getCommitIndex());

  delete paxos1;
  delete other;
  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(trx, trx_recovery_slow_crash) {
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
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  LogEntry le;
  uint64_t cindex = paxos1->getLastLogIndex();  // currentIndex
  for (int i = 0; i < 10; ++i) {
    le.Clear();
    le.set_optype(kCommitDep);
    le.set_value("part-" + std::to_string(i));
    paxos1->replicateLog(le);
    usleep(100000);
  }
  sleep(1);
  EXPECT_EQ(paxos1->getCommitIndex(), cindex);  // still the old one
  cindex = paxos1->getLastLogIndex();
  paxos2->debugResetLogSlow = true;
  paxos3->debugResetLogSlow = true;
  delete paxos1;
  while (paxos2->getState() != Paxos::LEADER &&
         paxos3->getState() != Paxos::LEADER) {
  }
  Paxos *leader, *other;
  if (paxos2->getState() == Paxos::LEADER) {
    leader = paxos2;
    other = paxos3;
  }
  if (paxos3->getState() == Paxos::LEADER) {
    leader = paxos3;
    other = paxos2;
  }
  other->debugResetLogSlow = false;
  sleep(1);
  delete leader;
  other->forceSingleLeader();
  sleep(6);
  EXPECT_EQ(other->getLastLogIndex(), other->getCommitIndex());
  EXPECT_TRUE(other->getLastLogIndex() > cindex);

  delete other;
  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(trx, trx_recovery_slow_recv_requestVote) {
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
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  LogEntry le;
  uint64_t cindex = paxos1->getLastLogIndex();  // currentIndex
  for (int i = 0; i < 10; ++i) {
    le.Clear();
    le.set_optype(kCommitDep);
    le.set_value("part-" + std::to_string(i));
    paxos1->replicateLog(le);
    usleep(100000);
  }
  sleep(1);
  EXPECT_EQ(paxos1->getCommitIndex(), cindex);  // still the old one
  cindex = paxos1->getLastLogIndex();
  paxos2->debugResetLogSlow = true;
  paxos3->debugResetLogSlow = true;
  delete paxos1;
  while (paxos2->getState() != Paxos::LEADER &&
         paxos3->getState() != Paxos::LEADER) {
  }
  Paxos *leader, *other;
  if (paxos2->getState() == Paxos::LEADER) {
    leader = paxos2;
    other = paxos3;
  }
  if (paxos3->getState() == Paxos::LEADER) {
    leader = paxos3;
    other = paxos2;
  }
  other->debugResetLogSlow = false;
  //  other->requestVote(false); /* will trigger stickness */
  if (leader == paxos2)
    leader->leaderTransfer(3);
  else
    leader->leaderTransfer(2);
  sleep(1);
  EXPECT_EQ(other->getLastLogIndex(), other->getCommitIndex());
  EXPECT_EQ(other->getLastLogIndex(), leader->getLastLogIndex());

  delete paxos2;
  delete paxos3;
  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}
