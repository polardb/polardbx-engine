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

TEST(consensus, Paxos_forceSync_replica) {
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
  paxos2->requestVote();
  sleep(1);

  EXPECT_EQ(paxos2->getState(), Paxos::LEADER);

  std::string tmpStr;
  rlog2->getMetaData(std::string(Paxos::keyMemberConfigure), tmpStr);
  EXPECT_EQ(tmpStr,
            std::string(
                "127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#5N@2"));

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_ikey("bbb");
  le.set_value("ccc");

  le.clear_term();
  paxos2->replicateLog(le);
  sleep(1);
  EXPECT_EQ(paxos2->getCommitIndex(), le.index());

  // configureMember
  paxos2->configureMember(3, true, 6);
  sleep(1);

  tmpStr.clear();
  rlog2->getMetaData(std::string(Paxos::keyMemberConfigure), tmpStr);
  EXPECT_EQ(tmpStr,
            std::string(
                "127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#6F@2"));

  le.clear_term();
  paxos2->replicateLog(le);
  sleep(1);
  EXPECT_EQ(paxos2->getCommitIndex(), le.index());

  delete paxos3;

  le.clear_term();
  paxos2->replicateLog(le);
  sleep(1);
  EXPECT_EQ(paxos2->getCommitIndex(), le.index() - 1);
  EXPECT_EQ(paxos2->getForceSyncStatus(), 1);
  sleep(4);
  // Force sync is disabled after election timeout.
  EXPECT_EQ(paxos2->getCommitIndex(), le.index());
  EXPECT_EQ(paxos2->getForceSyncStatus(), 0);

  // check force sync after change leader
  int ret = 0;
  ret = paxos2->leaderTransfer(1);
  std::cout << "Start transfer leader to server 1, ret:" << ret << std::endl;
  sleep(1);
  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  EXPECT_EQ(paxos1->getCommitIndex(), paxos1->getLastLogIndex() - 1);
  EXPECT_EQ(paxos1->getForceSyncStatus(), 1);
  sleep(4);
  // Force sync is disabled after election timeout.
  EXPECT_EQ(paxos1->getCommitIndex(), paxos1->getLastLogIndex());
  EXPECT_EQ(paxos1->getForceSyncStatus(), 0);

  // check force sync after change leader
  paxos2->setForceSyncEpochDiff(1000);
  ret = paxos1->leaderTransfer(2);
  std::cout << "Start transfer leader to server 2, ret:" << ret << std::endl;
  sleep(1);
  EXPECT_EQ(paxos2->getState(), Paxos::LEADER);
  EXPECT_EQ(paxos2->getCommitIndex(), paxos1->getLastLogIndex() - 1);
  EXPECT_EQ(paxos2->getForceSyncStatus(), 1);
  sleep(4);
  // Force sync is not disabled after election timeout.
  EXPECT_EQ(paxos2->getCommitIndex(), paxos1->getLastLogIndex() - 1);
  EXPECT_EQ(paxos2->getForceSyncStatus(), 1);

  paxos2->setForceSyncEpochDiff(0);
  sleep(4);
  // Force sync is disabled after election timeout.
  EXPECT_EQ(paxos2->getCommitIndex(), paxos1->getLastLogIndex());
  EXPECT_EQ(paxos2->getForceSyncStatus(), 0);

  EXPECT_EQ(paxos1->leaderTransfer(10), PaxosErrorCode::PE_NOTLEADR);
  // checkout change leader to a non-exist node
  EXPECT_EQ(paxos2->leaderTransfer(10), PaxosErrorCode::PE_NOTFOUND);

  delete paxos1;
  delete paxos2;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(consensus, Paxos_weight_election) {
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
  paxos2->requestVote();
  sleep(1);

  EXPECT_EQ(paxos2->getState(), Paxos::LEADER);

  std::string tmpStr;
  rlog2->getMetaData(std::string(Paxos::keyMemberConfigure), tmpStr);
  EXPECT_EQ(tmpStr,
            std::string(
                "127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#5N@2"));

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_ikey("bbb");
  le.set_value("ccc");

  le.clear_term();
  paxos2->replicateLog(le);
  sleep(1);
  EXPECT_EQ(paxos2->getCommitIndex(), le.index());

  // configureMember
  paxos2->configureMember(1, false, 9);
  sleep(1);
  // bug: crash after configureMember but server not found
  paxos2->configureMember(10, false, 5);

  tmpStr.clear();
  rlog2->getMetaData(std::string(Paxos::keyMemberConfigure), tmpStr);
  EXPECT_EQ(tmpStr,
            std::string(
                "127.0.0.1:11001#9N;127.0.0.1:11002#5N;127.0.0.1:11003#5N@2"));

  paxos2->leaderTransfer(3);
  sleep(1);
  EXPECT_EQ(paxos3->getState(), Paxos::LEADER);
  Paxos::MemberInfoType mi;
  paxos3->getMemberInfo(&mi);
  EXPECT_EQ(mi.role, Paxos::NOROLE);
  sleep(5);
  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  paxos3->getMemberInfo(&mi);
  EXPECT_EQ(mi.role, Paxos::FOLLOWER);

  // electionWeight == 0 case
  std::cout << "Start configureMember 2" << std::endl;
  paxos1->configureMember(1, false, 5);
  paxos1->configureMember(3, false, 0);
  sleep(1);
  rlog1->getMetaData(std::string(Paxos::keyMemberConfigure), tmpStr);
  EXPECT_EQ(tmpStr,
            std::string(
                "127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#0N@1"));
  std::vector<Paxos::ClusterInfoType> cis;
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);

  paxos1->leaderTransfer(3);
  sleep(3);
  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  paxos3->requestVote();
  sleep(1);
  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  delete paxos3;
  delete paxos1;
  delete paxos2;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(consensus, Paxos_weight_election_live) {
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
  paxos2->requestVote();
  sleep(1);

  EXPECT_EQ(paxos2->getState(), Paxos::LEADER);

  std::string tmpStr;
  rlog2->getMetaData(std::string(Paxos::keyMemberConfigure), tmpStr);
  EXPECT_EQ(tmpStr,
            std::string(
                "127.0.0.1:11001#5N;127.0.0.1:11002#5N;127.0.0.1:11003#5N@2"));

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_ikey("bbb");
  le.set_value("ccc");

  le.clear_term();
  paxos2->replicateLog(le);
  sleep(1);
  EXPECT_EQ(paxos2->getCommitIndex(), le.index());

  // configureMember
  paxos2->configureMember(2, false, 9);
  sleep(1);
  paxos2->configureMember(1, false, 1);
  sleep(1);

  tmpStr.clear();
  rlog2->getMetaData(std::string(Paxos::keyMemberConfigure), tmpStr);
  EXPECT_EQ(tmpStr,
            std::string(
                "127.0.0.1:11001#1N;127.0.0.1:11002#9N;127.0.0.1:11003#5N@2"));
  std::vector<Paxos::ClusterInfoType> cis;
  paxos1->getClusterInfo(cis);
  Paxos::printClusterInfo(cis);

  delete paxos2;
  sleep(2);
  paxos1->requestVote();
  sleep(1);
  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  sleep(4);
  EXPECT_EQ(paxos3->getState(), Paxos::LEADER);

  delete paxos3;
  delete paxos1;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}
