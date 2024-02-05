/************************************************************************
 *
 * Copyright (c) 2019 Alibaba.com, Inc. All Rights Reserved
 * $Id:  consensus-bug-t.cc,v 1.0 Mar 06, 2019 5:22:02 PM
 *jarry.zj(jarry.zj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file consensus-bug-t.cc
 * @author jarry.zj(jarry.zj@alibaba-inc.com)
 * @date Mar 06, 2019 5:22:02 PM
 * @version 1.0
 * @brief unit test for consensus complicated bugs (maybe still unsolved)
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

static void msleep(uint64_t t) {
  struct timeval sleeptime;
  if (t == 0) return;
  sleeptime.tv_sec = t / 1000;
  sleeptime.tv_usec = (t - (sleeptime.tv_sec * 1000)) * 1000;
  select(0, 0, 0, 0, &sleeptime);
}

/*
 * corner case1: change follower to learner
 * Ref: https://yuque.antfin-inc.com/db_core_team/internal_docs/kfg8ch#84659740
 * It is a real case from Alibaba production environment.
 * Has been fixed in X-Paxos
 */
TEST(bug, cc_corner_case1) {
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

  EXPECT_EQ(paxos1->changeLearners(Paxos::CCAddNode, strConfig), 0);

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_value("aaa");
  for (uint64_t i = 0; i < 10; ++i) {
    le.clear_term();
    paxos1->replicateLog(le);
    msleep(200);
  }
  sleep(1);
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos3->getLastLogIndex());
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos4->getLastLogIndex());
  /* Now, all nodes are normal */

  /*paxos1 write a new log and nobody receive */
  paxos1->set_flow_control(2, -2);
  paxos1->set_flow_control(3, -2);
  paxos1->set_flow_control(100, -2);
  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos2->getLastLogIndex() + 1);
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos3->getLastLogIndex() + 1);
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos4->getLastLogIndex() + 1);

  /* paxos1 crash, paxos2 become new leader */
  easy_warn_log("Now stop paxos1...");
  delete paxos1;
  paxos2->requestVote();
  sleep(1);
  EXPECT_EQ(paxos2->getState(), Paxos::LEADER);
  sleep(1);
  /* equal but last one is different */
  EXPECT_EQ(paxos2->getLastLogIndex(), rlog1->getLastLogIndex());

  /* change paxos1 to learner, change paxos3 to follower */
  paxos2->downgradeMember(1);
  paxos2->changeMember(Paxos::CCAddNode, strTmp3);
  le.clear_term();
  paxos2->replicateLog(le);
  sleep(1);

  /* restart paxos1 */
  easy_warn_log("Now restart paxos1...");
  paxos1 = new Paxos(timeout, rlog1);
  strConfig.clear();
  paxos1->init(strConfig, 0, 1);
  sleep(5);

  /* paxos3 and paxos4 are normal */
  EXPECT_EQ(paxos2->getLastLogIndex(), paxos3->getLastLogIndex());
  EXPECT_EQ(paxos2->getLastLogIndex(), paxos4->getLastLogIndex());

  /* If bug still exists */
  //  EXPECT_EQ(paxos2->getLastLogIndex(), paxos1->getLastLogIndex() + 3);
  /* If bug fixed */
  EXPECT_EQ(paxos2->getLastLogIndex(), paxos1->getLastLogIndex());

  delete paxos1;
  delete paxos2;
  delete paxos3;
  delete paxos4;
  deleteDir("paxosLogTestDir11");
  deleteDir("paxosLogTestDir12");
  deleteDir("paxosLogTestDir13");
  deleteDir("paxosLogTestDir14");
}

/*
 * corner case2: change learner to follower
 * Ref: https://yuque.antfin-inc.com/db_core_team/internal_docs/kfg8ch#60d98465
 * We are not going to solve this problem now
 * because it hardly ever happens in real world.
 */
TEST(bug, cc_corner_case2) {
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

  EXPECT_EQ(paxos1->changeLearners(Paxos::CCAddNode, strConfig), 0);

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_value("aaa");
  for (uint64_t i = 0; i < 10; ++i) {
    le.clear_term();
    paxos1->replicateLog(le);
    msleep(200);
  }
  sleep(1);
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos3->getLastLogIndex());
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos4->getLastLogIndex());
  /* Now, all nodes are normal */

  /* paxos1 change paxos4 to follower, but paxos4 do not receive this log */
  paxos1->set_flow_control(100, -2);
  paxos1->changeMember(Paxos::CCAddNode, strTmp3);
  while (paxos1->getLastLogIndex() != paxos2->getCommitIndex()) sleep(1);
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos3->getLastLogIndex());
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos2->getCommitIndex());
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos4->getLastLogIndex() + 1);

  /* paxos1 die */
  delete paxos1;
  paxos2->requestVote();
  sleep(10); /* try to elect a leader */

  /* If bug still exists, no leader will be elected */
  EXPECT_FALSE(paxos2->getState() == Paxos::LEADER);
  EXPECT_FALSE(paxos3->getState() == Paxos::LEADER);
  EXPECT_FALSE(paxos4->getState() == Paxos::LEADER);
  /* If bug fixed, leader exists */
  //  EXPECT_TRUE(paxos2->getState() == Paxos::LEADER || paxos3->getState() ==
  //  Paxos::LEADER || paxos4->getState() == Paxos::LEADER);

  delete paxos2;
  delete paxos3;
  delete paxos4;
  deleteDir("paxosLogTestDir11");
  deleteDir("paxosLogTestDir12");
  deleteDir("paxosLogTestDir13");
  deleteDir("paxosLogTestDir14");
}

/* check server-id */
class rdsRDPaxosLog : public RDPaxosLog {
 public:
  uint32_t rds_server_id;

  rdsRDPaxosLog(const std::string &dataDir)
      : rds_server_id(0), RDPaxosLog(dataDir, true, 4 * 1024 * 1024) {}
  int getEntry(uint64_t logIndex, LogEntry &entry, bool fastfail) override {
    int ret = RDPaxosLog::getEntry(logIndex, entry, fastfail);
    // add rds fields to entry
    RDSFields opaque;
    opaque.set_rdsserverid(rds_server_id);
    entry.set_opaque(opaque.SerializeAsString());
    return ret;
  }
  bool entriesPreCheck(
      const ::google::protobuf::RepeatedPtrField<LogEntry> &entries) override {
    if (entries.size() > 0) {
      RDSFields opaque;
      opaque.ParseFromString(entries.Get(0).opaque());
      // check rds_server_id
      if (opaque.has_rdsserverid() && opaque.rdsserverid() != rds_server_id)
        return 1;
    }
    return 0;
  }
};

/* incorrect opaque causes pre-check fail */
TEST(bug, pre_check) {
  std::vector<std::string> strConfig;
  strConfig.push_back("127.0.0.1:11001");
  std::string strTmp3("127.0.0.1:11004");

  const uint64_t timeout = 2000;
  std::shared_ptr<rdsRDPaxosLog> rlog1, rlog2;
  rlog1 = std::make_shared<rdsRDPaxosLog>("paxosLogTestDir11");
  Paxos *paxos1 = new Paxos(timeout, rlog1);
  paxos1->init(strConfig, 1, 1);

  rlog2 = std::make_shared<rdsRDPaxosLog>("paxosLogTestDir14");
  Paxos *paxos4 = new Paxos(timeout, rlog2);
  paxos4->initAsLearner(strTmp3, 4);

  sleep(3);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  strConfig.clear();
  strConfig.push_back(strTmp3);

  EXPECT_EQ(paxos1->changeLearners(Paxos::CCAddNode, strConfig), 0);

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_value("aaa");

  /* ---- main testcase: rds_server_id pre-check ---- */
  for (uint64_t i = 0; i < 2; ++i) {
    le.clear_term();
    paxos1->replicateLog(le);
  }
  sleep(1);
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos4->getLastLogIndex());
  rlog2->rds_server_id = 1;  // use a different server-id
  easy_warn_log("Set rds_server_id to 1");
  for (uint64_t i = 0; i < 2; ++i) {
    le.clear_term();
    paxos1->replicateLog(le);
  }
  sleep(1);
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos4->getLastLogIndex() + 2);
  for (uint64_t i = 0; i < 2; ++i) {
    le.clear_term();
    paxos1->replicateLog(le);
  }
  sleep(1);
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos4->getLastLogIndex() + 4);

  rlog2->rds_server_id = 0;  // reset to same server-id
  easy_warn_log("Set rds_server_id to 0");
  for (uint64_t i = 0; i < 2; ++i) {
    le.clear_term();
    paxos1->replicateLog(le);
  }
  sleep(1);
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos4->getLastLogIndex());
  /* ---- end of testcase ---- */

  delete paxos1;
  delete paxos4;
  deleteDir("paxosLogTestDir11");
  deleteDir("paxosLogTestDir14");
}

TEST(bug, follower_does_not_proceed_match_index) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  const uint64_t timeout = 5000;
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

  sleep(1);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_value("aaa");
  paxos1->replicateLog(le);
  sleep(1);
  EXPECT_EQ(paxos1->getCommitIndex(), 2);
  EXPECT_EQ(paxos2->getLastLogIndex(), 2);

  // follower 2 match index should be 2
  {
    std::vector<Paxos::ClusterInfoType> cis;
    paxos1->getClusterInfo(cis);
    EXPECT_EQ(cis[1].matchIndex, 2);
  }

  // let follower 2 increase its term, then on leader its match index will be
  // reset to 0
  easy_warn_log("now fake election");
  paxos2->fakeRequestVote();

  // follower 2 match index will be set to 0
  sleep(2);
  {
    std::vector<Paxos::ClusterInfoType> cis;
    paxos1->getClusterInfo(cis);
    EXPECT_EQ(cis[1].matchIndex, 0);
  }

  paxos2->unfakeRequestVote();

  // follower 2 match index will be set right
  sleep(2);
  {
    std::vector<Paxos::ClusterInfoType> cis;
    paxos1->getClusterInfo(cis);
    EXPECT_EQ(cis[1].matchIndex, 2);
  }

  paxos1->replicateLog(le);
  sleep(1);
  EXPECT_EQ(paxos1->getCommitIndex(), 3);
  EXPECT_EQ(paxos2->getLastLogIndex(), 3);

  delete paxos1;
  delete paxos2;
  delete paxos3;
  deleteDir("paxosLogTestDir11");
  deleteDir("paxosLogTestDir12");
  deleteDir("paxosLogTestDir13");
}
