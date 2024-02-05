/************************************************************************
 *
 * Copyright (c) 2017 Alibaba.com, Inc. All Rights Reserved
 * $Id:  misc-t.cc,v 1.0 01/11/2017 10:59:25 AM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file misc-t.cc
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 01/11/2017 10:59:25 AM
 * @version 1.0
 * @brief
 *
 **/
#include <gtest/gtest.h>
#include <thread>
#include "files.h"
#include "paxos.h"
#include "rd_paxos_log.h"

#include "single_process_queue.h"

using namespace alisql;

uint64_t globalProcessId = 0;
void processCb(uint64_t *i) {
  std::cout << "Start process id:" << *i << std::endl;
  sleep(1);
  globalProcessId = *i;
}

TEST(misc, SingleProcessQueue) {
  SingleProcessQueue<uint64_t> testQueue;

  testQueue.push(new uint64_t(20));
  testQueue.push(new uint64_t(40));
  // testQueue.process(&processCb);
  std::thread th1(&SingleProcessQueue<uint64_t>::process, &testQueue,
                  processCb);

  testQueue.push(new uint64_t(10));
  std::thread th2(&SingleProcessQueue<uint64_t>::process, &testQueue,
                  processCb);

  th1.join();
  th2.join();

  EXPECT_EQ(globalProcessId, 10);
}

#if 0
/* temporary disable this test because it accesses protected/private class members */
TEST(misc, minMatchIndex)
{
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  uint64_t timeout= 12500;
  std::shared_ptr<PaxosLog> rlog;
  rlog= std::make_shared<RDPaxosLog>("paxosLogTestDir1", true, 4 * 1024 * 1024);
  Paxos *paxos1= new Paxos(timeout, rlog, 3000);
  paxos1->init(strConfig, 1, 1);
  sleep(1);
  paxos1->requestVote();
  sleep(2);
  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  /* replicate a log */
  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  le.set_value("12345");
  while(paxos1->getLastLogIndex() < 5)
    paxos1->replicateLog(le);
  sleep(2);
  std::vector<Paxos::ClusterInfoType> cis;
  uint64_t ret;
  Paxos::ClusterInfoType ci;
  ci.serverId = 1;
  ci.role = Paxos::LEADER;
  ci.matchIndex = 5;
  cis.push_back(ci); // 0
  ci.serverId = 2;
  ci.role = Paxos::FOLLOWER;
  ci.matchIndex = 4;
  cis.push_back(ci); // 1
  ci.serverId = 100;
  ci.role = Paxos::LEARNER;
  ci.matchIndex = 3;
  ci.learnerSource = 0;
  cis.push_back(ci); // 2
  ci.serverId = 101;
  ci.role = Paxos::LEARNER;
  ci.matchIndex = 2;
  ci.learnerSource = 0;
  cis.push_back(ci); // 3

  cis[0].matchIndex= 0;
  ret = paxos1->collectMinMatchIndex(cis, false, UINT64_MAX);
  EXPECT_EQ(ret, 2);
  ret = paxos1->collectMinMatchIndex(cis, true, UINT64_MAX);
  EXPECT_EQ(ret, 2);
  cis[0].matchIndex= 5;

  paxos1->state_ = Paxos::FOLLOWER;
  paxos1->localServer_->serverId = 2;
  cis[1].matchIndex= 0;
  ret = paxos1->collectMinMatchIndex(cis, true, UINT64_MAX);
  EXPECT_EQ(ret, 5);
  cis[3].learnerSource= 2;
  ret = paxos1->collectMinMatchIndex(cis, true, UINT64_MAX);
  EXPECT_EQ(ret, 2);
  cis[1].matchIndex= 5;

  paxos1->state_ = Paxos::LEARNER;
  paxos1->localServer_->serverId = 100;
  ret = paxos1->collectMinMatchIndex(cis, true, UINT64_MAX);
  EXPECT_EQ(ret, 5);

  cis[3].learnerSource = 100;
  ret = paxos1->collectMinMatchIndex(cis, true, UINT64_MAX);
  EXPECT_EQ(ret, 2);

  delete paxos1;
  deleteDir("paxosLogTestDir1");
}
#endif
