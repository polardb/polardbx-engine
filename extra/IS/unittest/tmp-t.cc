/************************************************************************
 *
 * Copyright (c) 2017 Alibaba.com, Inc. All Rights Reserved
 * $Id:  tmp-t.cc,v 1.0 02/16/2017 10:29:00 AM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file tmp-t.cc
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 02/16/2017 10:29:00 AM
 * @version 1.0
 * @brief
 *
 **/

#include <gtest/gtest.h>
#include <atomic>
#include <iostream>
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

TEST(consensus, PB) {
  std::cout << "start" << std::endl << std::flush;
  {
    PaxosMsg pmsg;
    PaxosMsg *msg = &pmsg;

    ::google::protobuf::RepeatedPtrField<LogEntry> *entries;
    entries = msg->mutable_entries();

    LogEntry entry;
    entry.set_index(1);
    entry.set_value("aaaaaaaaaaaaaaa");
    *(entries->Add()) = entry;
    entry.set_index(2);
    entry.set_value("bbbbbbbbbbbbbbbbbbb");
    *(entries->Add()) = entry;
  }
  std::cout << "stop" << std::endl << std::flush;
}

TEST(consensus, Cpp11Time) {
  std::cout << "start" << std::endl << std::flush;
  std::chrono::steady_clock::time_point tp1, tp2;

  tp1 = std::chrono::steady_clock::now();
  sleep(1);
  /*
  tp2= std::chrono::steady_clock::now();

  auto tt= tp2 - tp1;
  auto tt1= std::chrono::duration_cast<std::chrono::microseconds>(tt);
  uint64_t diff= tt1.count();
  */
  uint64_t diff = RemoteServer::diffMS(tp1);

  std::cout << "diff is:" << diff << std::endl << std::flush;

  std::cout << "stop" << std::endl << std::flush;
}

TEST(consensus, PB_bin) {
  std::cout << "start" << std::endl << std::flush;

  TestMsg1 msg1;
  msg1.set_id(0x5e5e5e);

  TestMsg1 msg2;
  msg2.set_id(0xe5e5e5);
  msg2.set_c1(0xdddddd);

  TestMsg2 msg3;
  msg3.set_id(0xe5e5e5);
  msg3.set_c1(0xdddddd);

  TestMsg2 msg4;
  msg4.set_id(0xe5e5e5);
  msg4.set_c1(0xdddddd);
  msg4.add_c2(0xeeeeee);

  std::string buf1;
  msg1.SerializeToString(&buf1);
  std::string buf2;
  msg2.SerializeToString(&buf2);
  std::string buf3;
  msg3.SerializeToString(&buf3);
  std::string buf4;
  msg4.SerializeToString(&buf4);

  TestMsg2 msg5;
  msg5.ParseFromString(buf2);

  TestMsg1 msg6;
  msg6.ParseFromString(buf4);

  // uint64_t index= atoi("3572172862");
  uint64_t index = strtoull("3572172862", NULL, 10);

  std::cout << "index:" << index << std::endl << std::flush;

  std::cout << "stop" << std::endl << std::flush;
}

std::atomic<int> ttsCalled(0);
TEST(Service, ThreadTimer_weight_repeat) {
  auto tts = new ThreadTimerService();
  ttsCalled.store(0);

  ThreadTimer *tt3 = new ThreadTimer(tts, 10.0, ThreadTimer::Stage, [] {
    std::cout << "foo3" << std::endl << std::flush;
    ttsCalled.fetch_add(1);
  });
  tt3->setRandWeight(5);
  tt3->start();

  std::cout << "Start sleep in ThreadTimerTest" << std::endl << std::flush;
  sleep(9);
  std::cout << "Stop sleep in ThreadTimerTest" << std::endl << std::flush;
  EXPECT_EQ(tt3->getCurrentStage(), 0);
  tt3->restart();
  std::cout << "Start sleep 2 in ThreadTimerTest" << std::endl << std::flush;
  sleep(9);
  std::cout << "Stop sleep 2 in ThreadTimerTest" << std::endl << std::flush;
  EXPECT_EQ(ttsCalled.load(), 0);
  EXPECT_EQ(tt3->getCurrentStage(), 0);
  sleep(4);
  EXPECT_EQ(ttsCalled.load(), 0);
  EXPECT_EQ(tt3->getCurrentStage(), 1);
  sleep(5);
  EXPECT_EQ(ttsCalled.load(), 1);
  EXPECT_EQ(tt3->getCurrentStage(), 0);
  tt3->stop();

  delete tt3;
  delete tts;
}

TEST(consensus, paxos_learner_restart) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");

  uint64_t timeout = 2000;
  std::shared_ptr<PaxosLog> rlog;
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir1");
  Paxos *paxos1 = new Paxos(timeout, rlog, 3000);
  paxos1->init(strConfig, 1, 1);
  std::string strTmp1("127.0.0.1:11002");
  std::string strTmp2("127.0.0.1:11003");
  rlog = std::make_shared<FilePaxosLog>("paxosLogTestDir3");
  Paxos *learner2 = new Paxos(timeout, rlog);
  learner2->initAsLearner(strTmp2, 12);

  sleep(1);
  paxos1->requestVote();
  sleep(3);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);

  LogEntry le;
  le.set_index(0);
  le.set_optype(0);
  le.set_value("aaa");
  paxos1->replicateLog(le);

  le.set_value("bbb");
  paxos1->replicateLog(le);

  sleep(1);

  EXPECT_EQ(paxos1->getLastLogIndex(), paxos1->getCommitIndex());
  EXPECT_EQ(paxos1->getConfig()->getServerNum(), 1);
  EXPECT_EQ(paxos1->getConfig()->getServerNumLockFree(), 1);

  // add node in one node cluster.

  strConfig.clear();
  strConfig.push_back(strTmp1);
  strConfig.push_back(strTmp2);
  strConfig.push_back(std::string("127.0.0.1:11004"));
  strConfig.push_back(std::string("127.0.0.1:11005"));
  strConfig.push_back(std::string("127.0.0.1:11006"));
  strConfig.push_back(std::string("127.0.0.1:11007"));
  strConfig.push_back(std::string("127.0.0.1:11008"));
  paxos1->changeLearners(Paxos::CCAddNode, strConfig);
  sleep(1);

  auto config =
      std::dynamic_pointer_cast<StableConfiguration>(learner2->getConfig());
  EXPECT_EQ(
      config->learnersToString(),
      std::string(
          "127.0.0.1:11002$00;127.0.0.1:11003$00;127.0.0.1:11004$00;127.0.0.1:"
          "11005$00;127.0.0.1:11006$00;127.0.0.1:11007$00;127.0.0.1:11008$00"));

  sleep(1);
  strConfig.clear();
  strConfig.push_back(std::string("127.0.0.1:11005"));
  paxos1->changeLearners(Paxos::CCDelNode, strConfig);
  sleep(1);
  EXPECT_EQ(
      config->learnersToString(),
      std::string("127.0.0.1:11002$00;127.0.0.1:11003$00;127.0.0.1:11004$00;0;"
                  "127.0.0.1:11006$00;127.0.0.1:11007$00;127.0.0.1:11008$00"));

  sleep(1);
  strConfig.clear();
  strConfig.push_back(std::string("127.0.0.1:11006"));
  paxos1->changeLearners(Paxos::CCDelNode, strConfig);
  sleep(1);
  EXPECT_EQ(config->learnersToString(),
            std::string("127.0.0.1:11002$00;127.0.0.1:11003$00;127.0.0.1:11004$"
                        "00;0;0;127.0.0.1:11007$00;127.0.0.1:11008$00"));

  sleep(1);
  strConfig.clear();
  strConfig.push_back(std::string("127.0.0.1:11004"));
  paxos1->changeLearners(Paxos::CCDelNode, strConfig);
  sleep(1);
  EXPECT_EQ(config->learnersToString(),
            std::string("127.0.0.1:11002$00;127.0.0.1:11003$00;0;0;0;127.0.0.1:"
                        "11007$00;127.0.0.1:11008$00"));

  delete learner2;
  sleep(1);
  learner2 = new Paxos(timeout, rlog);
  learner2->initAsLearner(strTmp2, 12);

  le.clear_term();
  paxos1->replicateLog(le);
  sleep(1);

  EXPECT_EQ(config->learnersToString(),
            std::string("127.0.0.1:11002$00;127.0.0.1:11003$00;0;0;0;127.0.0.1:"
                        "11007$00;127.0.0.1:11008$00"));

  config.reset();
  delete learner2;
  delete paxos1;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir3");
}

TEST(consensus, pb_large) {
  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  le.set_term(1);
  le.set_value(std::string(400 * 1024 * 1024, 'a'));
  std::string buf;
  le.SerializeToString(&buf);
  std::cout << buf.length() << std::endl;
  LogEntry le2;
  bool ret = le2.ParseFromString(buf);
#if GOOGLE_PROTOBUF_VERSION < 3006001
  EXPECT_EQ(ret, false);
#else
  EXPECT_EQ(ret, true);
#endif
  std::cout << le2.value().length() << std::endl;
  LogEntry le3;
  ret = MyParseFromArray(le3, buf.c_str(), buf.length());
  EXPECT_EQ(ret, true);
  std::cout << le3.value().length() << std::endl;
  EXPECT_EQ(le3.value().length(), le.value().length());
}

TEST(consensus, stability) {
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

  sleep(1);
  paxos1->requestVote();
  sleep(2);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  /* replicate a log */
  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  le.set_value("12345");
  paxos1->replicateLog(le);

  std::atomic<bool> shutdown(false);
  auto worker_main = [&shutdown](Paxos *p, uint64_t time) {
    while (shutdown.load() == false) {
      LogEntry le;
      le.set_index(0);
      le.set_optype(1);
      le.set_value("12345");
      if (p->getState() == Paxos::LEADER) p->replicateLog(le);
      usleep(time * 1000);
    }
  };
  std::thread writer1(worker_main, paxos1, 100);
  std::thread writer2(worker_main, paxos2, 100);
  std::thread writer3(worker_main, paxos3, 100);

  /* leader transfer */
  std::thread op1([paxos1, paxos2, paxos3, &shutdown]() {
    while (shutdown.load() == false) {
      sleep(2);
      paxos1->leaderTransfer(2);
      paxos2->leaderTransfer(3);
      paxos3->leaderTransfer(1);
    }
  });
  /* config change (sometimes config change will cause leaderTransfer check
   * fail) */
  std::thread op2([paxos1, paxos2, paxos3, &shutdown]() {
    std::vector<std::string> strConfig;
    strConfig.emplace_back("127.0.0.1:12000");
    while (shutdown.load() == false) {
      sleep(3);
      paxos1->changeLearners(Paxos::CCAddNode, strConfig);
      paxos2->changeLearners(Paxos::CCAddNode, strConfig);
      paxos3->changeLearners(Paxos::CCAddNode, strConfig);
      sleep(3);
      paxos1->changeLearners(Paxos::CCDelNode, strConfig);
      paxos2->changeLearners(Paxos::CCDelNode, strConfig);
      paxos3->changeLearners(Paxos::CCDelNode, strConfig);
      sleep(3);
    }
  });
  /* set cluster id (randomly kick a node out) */
  std::thread op3([paxos1, paxos2, paxos3, &shutdown]() {
    while (shutdown.load() == false) {
      sleep(1);
      switch (rand() % 3) {
        case 0:
          paxos1->setClusterId(1);
          sleep(1);
          paxos1->setClusterId(0);
          break;
        case 1:
          paxos2->setClusterId(1);
          sleep(1);
          paxos2->setClusterId(0);
          break;
        case 2:
          paxos3->setClusterId(1);
          sleep(1);
          paxos3->setClusterId(0);
          break;
      }
    }
  });
  /* how long to test */
  sleep(20);

  shutdown.store(true);
  writer1.join();
  writer2.join();
  writer3.join();
  op1.join();
  op2.join();
  op3.join();
  paxos1->replicateLog(le);
  paxos2->replicateLog(le);
  paxos3->replicateLog(le);
  sleep(2);
  EXPECT_EQ(paxos1->getLastLogIndex(), paxos2->getLastLogIndex());
  EXPECT_EQ(paxos2->getLastLogIndex(), paxos3->getLastLogIndex());

  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}
