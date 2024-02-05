/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  witness-t.cc,v 1.0 12/21/2016 11:59:02 AM
 *jarry.zj(jarry.zj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file witness-t.cc
 * @author jarry.zj(jarry.zj@alibaba-inc.com)
 * @date 12/21/2016 11:59:02 AM
 * @version 1.0
 * @brief
 *
 **/

#include <gtest/gtest.h>
#include <unistd.h>
#include <atomic>
#include <thread>
#include "easyNet.h"
#include "file_paxos_log.h"
#include "files.h"
#include "learner_client.h"
#include "mem_paxos_log.h"
#include "paxos.h"
#include "paxos.pb.h"
#include "paxos_configuration.h"
#include "paxos_log.h"
#include "paxos_server.h"
#include "rd_paxos_log.h"
#include "service.h"
#include "single_leader.h"
#include "witness.h"

using namespace alisql;

TEST(witness, Paxos_replicateLog) {
  std::vector<std::string> strConfig = {"127.0.0.1:11001", "127.0.0.1:11002",
                                        "127.0.0.1:11003"};
  uint64_t timeout = 12500;
  std::shared_ptr<PaxosLog> rlog;
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir1", true, 4 * 1024 * 1024);
  ClientService *cs1 = new ClientService();
  Paxos *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1, cs1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir2", true, 4 * 1024 * 1024);
  ClientService *cs2 = new ClientService();
  Paxos *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2, cs2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir3", true, 4 * 1024 * 1024);
  ClientService *cs3 = new ClientService();
  Paxos *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3, cs3);

  std::vector<std::string> witnesslist = {"127.0.0.1:11004"};
  sleep(3);
  paxos1->requestVote();
  sleep(1);
  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  paxos1->changeLearners(Paxos::CCAddNode, witnesslist);

  /* start witness node */
  std::shared_ptr<MemPaxosLog> mlog = std::make_shared<MemPaxosLog>();
  Witness *witness1 = new Witness("127.0.0.1:11004", mlog, 0);

  /* replicate a log */
  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  le.set_ikey("bbb");
  le.set_value("12345");

  paxos1->replicateLog(le);
  sleep(2);

  int lasti = witness1->getLog()->getLastLogIndex();
  std::cout << "Witness lastLogIndex " << lasti << std::endl;
  LogEntry ret;
  witness1->getLog()->getEntry(lasti, ret);
  EXPECT_STREQ("12345", ret.value().c_str());

  /* test send RequestVote & LeaderCommand to learner (should not crash) */
  paxos1->debugWitnessTest = true;
  paxos2->debugWitnessTest = true;
  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  paxos1->leaderTransfer(2);
  sleep(6);
  EXPECT_EQ(paxos2->getState(), Paxos::LEADER);
  paxos2->requestVote();
  sleep(2);
  paxos1->debugWitnessTest = false;
  paxos2->debugWitnessTest = false;
  paxos2->leaderTransfer(3);
  sleep(6);
  EXPECT_EQ(paxos3->getState(), Paxos::LEADER);
  paxos3->leaderTransfer(1);
  sleep(6);
  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  paxos1->replicateLog(le);
  sleep(2);
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(paxos2->getLastLogIndex(), paxos3->getLastLogIndex());
  EXPECT_EQ(paxos3->getLastLogIndex(), witness1->getLastLogIndex());

  delete witness1;
  delete paxos1;
  delete paxos2;
  delete paxos3;
  delete cs1;
  delete cs2;
  delete cs3;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(witness, wrong_clusterid) {
  std::vector<std::string> strConfig = {"127.0.0.1:11001", "127.0.0.1:11002",
                                        "127.0.0.1:11003"};
  uint64_t timeout = 12500;
  std::shared_ptr<PaxosLog> rlog;
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir1", true, 4 * 1024 * 1024);
  ClientService *cs1 = new ClientService();
  Paxos *paxos1 = new Paxos(timeout, rlog);
  paxos1->init(strConfig, 1, 1, cs1);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir2", true, 4 * 1024 * 1024);
  ClientService *cs2 = new ClientService();
  Paxos *paxos2 = new Paxos(timeout, rlog);
  paxos2->init(strConfig, 2, 2, cs2);
  rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir3", true, 4 * 1024 * 1024);
  ClientService *cs3 = new ClientService();
  Paxos *paxos3 = new Paxos(timeout, rlog);
  paxos3->init(strConfig, 3, 3, cs3);

  std::vector<std::string> witnesslist = {"127.0.0.1:11004"};
  paxos1->requestVote();

  // EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  while (1) {
    sleep(1);
    if (paxos1->getCurrentLeader() != 0) {
      paxos1->changeLearners(Paxos::CCAddNode, witnesslist);
      paxos2->changeLearners(Paxos::CCAddNode, witnesslist);
      paxos3->changeLearners(Paxos::CCAddNode, witnesslist);
      break;
    }
  }

  /* start witness node */
  std::shared_ptr<MemPaxosLog> mlog = std::make_shared<MemPaxosLog>();
  /* give a wrong clusterid 1 (the correct is 0) */
  Witness *witness1 = new Witness("127.0.0.1:11004", mlog, 1);

  /* replicate a log */
  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  le.set_ikey("bbb");
  le.set_value("12345");

  paxos1->replicateLog(le);
  paxos2->replicateLog(le);
  paxos3->replicateLog(le);
  sleep(5);

  int lasti = witness1->getLog()->getLastLogIndex();
  EXPECT_EQ(lasti, 0);

  delete witness1;
  delete paxos1;
  delete paxos2;
  delete paxos3;
  delete cs1;
  delete cs2;
  delete cs3;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(witness, single_leader_witness) {
  SingleLeader *sl = new SingleLeader(1);
  sl->init(11001, "127.0.0.1:11002");
  std::shared_ptr<MemPaxosLog> mlog = std::make_shared<MemPaxosLog>();
  Witness *witness1 = new Witness("127.0.0.1:11002", mlog, 1);
  LogEntry le;
  le.set_term(1);
  le.set_optype(1);
  le.set_value("12345");
  sl->replicateLog(le);
  sleep(1);
  LogEntry le2;
  EXPECT_EQ(witness1->getLastLogIndex(), 1);
  witness1->getLog()->getEntry(le2);
  EXPECT_EQ(le.value(), le2.value());

  delete witness1;
  delete sl;
}

TEST(witness, single_leader_learner) {
  std::string addr = "127.0.0.1:11002";
  SingleLeader *sl = new SingleLeader(1);
  sl->init(11001, addr);
  auto rlog =
      std::make_shared<RDPaxosLog>("paxosLogTestDir1", true, 4 * 1024 * 1024);
  rlog->setMetaData(Paxos::keyClusterId, 1);
  Paxos *paxos1 = new Paxos(5000, rlog);
  paxos1->initAsLearner(addr, 11);
  LogEntry le;
  le.set_term(1);
  le.set_optype(1);
  le.set_value("12345");
  sl->replicateLog(le);
  sleep(1);
  LogEntry le2;
  EXPECT_EQ(paxos1->getLastLogIndex(), 1);
  paxos1->getLog()->getEntry(1, le2);
  EXPECT_EQ(le.value(), le2.value());

  delete paxos1;
  delete sl;

  deleteDir("paxosLogTestDir1");
}

TEST(witness, client_mode) {
  std::vector<std::string> strConfig = {"127.0.0.1:11001", "127.0.0.1:11002",
                                        "127.0.0.1:11003"};
  uint64_t timeout = 12500;
  std::shared_ptr<PaxosLog> rlog;
  rlog =
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

  std::vector<std::string> witnesslist = {"127.0.0.1:11004"};
  sleep(1);
  paxos1->requestVote();
  sleep(2);

  int loop = 0;
  while (++loop < 10) {
    sleep(1);
    if (paxos1->getState() != Paxos::LEADER) {
      paxos1->requestVote();
      break;
    }
  }

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  paxos1->changeLearners(Paxos::CCAddNode, witnesslist);
  /* start witness node */
  std::shared_ptr<MemPaxosLog> mlog =
      std::make_shared<MemPaxosLog>(5 /*lastLogIndex*/);
  Witness *witness1 = new Witness("127.0.0.1:11004", mlog);

  /* replicate a log */
  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  le.set_value("12345");

  paxos1->replicateLog(le);
  sleep(2);

  EXPECT_EQ(mlog->getLastLogIndex(), 5);
  EXPECT_EQ(mlog->getLength(), 0);

  loop = 0;
  while (++loop < 10) {
    usleep(5000);
    paxos1->replicateLog(le);
  }
  sleep(2);
  EXPECT_EQ(mlog->getLastLogIndex(), paxos1->getLastLogIndex());
  EXPECT_EQ(mlog->getLength(), paxos1->getLastLogIndex() - 5);

  /* test client with index too small */
  paxos1->initAutoPurgeLog(false /* no auto */, false /*use commitIndex*/);
  paxos1->forcePurgeLog(false, 4); /* clear index 1,2,3 */
  sleep(1);
  EXPECT_EQ(paxos1->getLog()->getLength(), paxos1->getLastLogIndex() - 3);
  std::vector<std::string> witnesslist2 = {"127.0.0.1:11005"};
  paxos1->changeLearners(Paxos::CCAddNode, witnesslist2);
  std::shared_ptr<MemPaxosLog> mlog2 =
      std::make_shared<MemPaxosLog>(2 /*lastLogIndex*/);
  Witness *witness2 = new Witness("127.0.0.1:11005", mlog2);
  paxos1->replicateLog(le);
  sleep(1);
  EXPECT_EQ(mlog2->getLastLogIndex(), 2);
  EXPECT_EQ(mlog2->getLength(), 0);

  delete witness1;
  delete witness2;
  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(witness, cache_timeout) {
  std::vector<std::string> strConfig = {"127.0.0.1:11001", "127.0.0.1:11002",
                                        "127.0.0.1:11003"};
  uint64_t timeout = 12500;
  std::shared_ptr<PaxosLog> rlog;
  rlog =
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

  std::vector<std::string> witnesslist = {"127.0.0.1:11004"};
  sleep(1);
  paxos1->requestVote();
  sleep(2);

  int loop = 0;
  while (++loop < 10) {
    sleep(1);
    if (paxos1->getState() != Paxos::LEADER) {
      paxos1->requestVote();
      break;
    }
  }
  /* replicate a log */
  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  le.set_value("12345");

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  paxos1->changeLearners(Paxos::CCAddNode, witnesslist);
  /* start witness node */
  std::shared_ptr<MemPaxosLog> mlog =
      std::make_shared<MemPaxosLog>(4, 1000, 4 /* cachesize */);
  Witness *witness1 = new Witness("127.0.0.1:11004", mlog);

  loop = 0;
  while (++loop < 10) {
    usleep(2000 * 1000);
    paxos1->replicateLog(le);
  }

  sleep(2);
  EXPECT_EQ(mlog->getLastLogIndex(), 8);
  EXPECT_EQ(mlog->getLength(), 4);

  // clear memlog
  LogEntry tmp;
  while (1) {
    mlog->getEntry(tmp);
    if (tmp.index() == paxos1->getLastLogIndex()) break;
  }
  EXPECT_EQ(mlog->getLastLogIndex(), paxos1->getLastLogIndex());
  EXPECT_EQ(mlog->getLength(), 0);

  witness1->debugWitnessTest = true;
  easy_warn_log("Set debugWitnessTest to true (force single append).\n");
  loop = 0;
  while (++loop < 10) {
    paxos1->replicateLog(le); /* no sleep to let task merge */
  }
  sleep(1);
  mlog->getEntry(tmp);
  sleep(1);
  EXPECT_EQ(mlog->getLength(), 4);

  delete witness1;
  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(witness, send_by_appliedIndex) {
  std::vector<std::string> strConfig = {"127.0.0.1:11001", "127.0.0.1:11002",
                                        "127.0.0.1:11003"};
  uint64_t timeout = 12500;
  std::shared_ptr<PaxosLog> rlog;
  rlog =
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

  std::vector<std::string> witnesslist = {"127.0.0.1:11004"};
  sleep(3);
  paxos1->requestVote();
  sleep(1);
  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  paxos1->changeLearners(Paxos::CCAddNode, witnesslist);
  paxos1->configureLearner(100, 2, true);

  /* start witness node */
  std::shared_ptr<MemPaxosLog> mlog = std::make_shared<MemPaxosLog>();
  Witness *witness1 = new Witness("127.0.0.1:11004", mlog);

  /* replicate a log */
  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  le.set_ikey("bbb");
  le.set_value("12345");

  for (int i = 0; i < 4; ++i) {
    paxos1->replicateLog(le);
    usleep(10000);
  }
  paxos2->updateAppliedIndex(6);
  sleep(1);
  for (int i = 0; i < 4; ++i) {
    paxos1->replicateLog(le);
    usleep(10000);
  }
  sleep(1);
  EXPECT_EQ(witness1->getLog()->getLastLogIndex(), 6);
  paxos2->updateAppliedIndex(12);
  for (int i = 0; i < 4; ++i) {
    paxos1->replicateLog(le);
    usleep(10000);
  }
  sleep(1);
  EXPECT_EQ(witness1->getLog()->getLastLogIndex(), 12);
  paxos1->configureLearner(100, 2, false);
  for (int i = 0; i < 4; ++i) {
    paxos1->replicateLog(le);
    usleep(10000);
  }
  sleep(1);
  EXPECT_EQ(witness1->getLog()->getLastLogIndex(), paxos2->getCommitIndex());

  delete witness1;
  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(learnerClient, basic) {
  std::vector<std::string> strConfig = {"127.0.0.1:11001", "127.0.0.1:11002",
                                        "127.0.0.1:11003"};
  uint64_t timeout = 12500;
  std::shared_ptr<PaxosLog> rlog;
  rlog =
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

  std::vector<std::string> witnesslist = {"127.0.0.1:11004"};
  sleep(3);
  paxos1->requestVote();
  sleep(1);
  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  paxos1->changeLearners(Paxos::CCAddNode, witnesslist);

  uint64_t lli = paxos1->getLastLogIndex();
  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  le.set_value("12345");
  for (int i = 0; i < 15; ++i) {
    le.set_value("data" + std::to_string(i));
    paxos1->replicateLog(le);
  }

  LearnerClient *client1 = new LearnerClient();
  client1->open("127.0.0.1:11004", 0, lli + 5, 0, 1000);
  sleep(1);
  for (int i = 5; i < 15; ++i) {
    LogEntry tmp;
    client1->read(tmp);
    EXPECT_EQ(tmp.value(), "data" + std::to_string(i));
  }

  delete client1;
  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}

TEST(learnerClient, blob) {
  std::vector<std::string> strConfig = {"127.0.0.1:11001", "127.0.0.1:11002",
                                        "127.0.0.1:11003"};
  uint64_t timeout = 12500;
  std::shared_ptr<PaxosLog> rlog;
  rlog =
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

  std::vector<std::string> witnesslist = {"127.0.0.1:11004", "127.0.0.1:11005",
                                          "127.0.0.1:11006"};
  sleep(3);
  paxos1->requestVote();
  sleep(1);
  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  paxos1->changeLearners(Paxos::CCAddNode, witnesslist);

  uint64_t lli = paxos1->getLastLogIndex();
  std::cout << "lli: " << lli << std::endl;
  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  for (int i = 0; i < 15; ++i) {
    le.clear_info();
    /* BLOB: 4 5 6 7 8 9 10 */
    if (i == 4)
      le.set_info(Logentry_info_flag::FLAG_BLOB_START |
                  Logentry_info_flag::FLAG_BLOB);
    if (i > 4 && i < 10) le.set_info(Logentry_info_flag::FLAG_BLOB);
    if (i == 10) le.set_info(Logentry_info_flag::FLAG_BLOB_END);
    le.set_value("data" + std::to_string(i));
    paxos1->replicateLog(le);
  }

  LearnerClient *client1 = new LearnerClient();
  client1->open("127.0.0.1:11004", 0, lli + 11, 0, 1000);
  sleep(1);
  for (int i = 11; i < 15; ++i) {
    LogEntry tmp;
    client1->read(tmp);
    EXPECT_EQ(tmp.value(), "data" + std::to_string(i));
  }

  LearnerClient *client2 = new LearnerClient();
  client2->open("127.0.0.1:11005", 0, lli + 4, 0, 1000);
  sleep(1);
  for (int i = 4; i < 15; ++i) {
    LogEntry tmp;
    client2->read(tmp);
    if (i >= 4 && i < 10)
      EXPECT_EQ(tmp.value(), "");
    else if (i == 10)
      EXPECT_EQ(tmp.value(), "data4data5data6data7data8data9data10");
    else
      EXPECT_EQ(tmp.value(), "data" + std::to_string(i));
  }

  LearnerClient *client3 = new LearnerClient();
  client3->open("127.0.0.1:11006", 0, lli + 8, 0, 1000);
  LogEntry tmp;
  while (client3->read(tmp) == LearnerClient::RET_WAIT_BLOB) {
    paxos1->replicateLog(le);
  }
  for (int i = 5; i < 15; ++i) {
    client3->read(tmp);
    if (i >= 5 && i < 10)
      EXPECT_EQ(tmp.value(), "");
    else if (i == 10)
      EXPECT_EQ(tmp.value(), "data4data5data6data7data8data9data10");
    else
      EXPECT_EQ(tmp.value(), "data" + std::to_string(i));
  }

  delete client1;
  delete client2;
  delete client3;
  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}
