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

TEST(consensus, may_read_mock_log) {
  /*
   * paxos1 has more log in term 1 (index:1-10), but 1-3 is mock log.
   * paxos2/paxos3 have index:1-5 in term 1;
   * paxos1 became leader in term 2, maxDelayIndex is 2, paxos2/paxos3 became
   * disable pipeling mode We should make sure leader will not read the mock log
   * in this case.
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

  for (int i = 1; i <= 3; ++i) {
    le.set_term(0);
    le.set_index(i);
    le.set_optype(kMock);
    rlog1->append(le);

    le.set_term(1);
    le.set_index(i);
    le.set_optype(1);
    rlog2->append(le);
    rlog3->append(le);
  }
  for (int i = 4; i <= 5; ++i) {
    le.set_term(1);
    le.set_index(i);
    le.set_optype(1);
    rlog1->append(le);
    rlog2->append(le);
    rlog3->append(le);
  }
  for (int i = 6; i <= 10; ++i) {
    le.set_term(1);
    le.set_index(i);
    le.set_optype(1);
    rlog1->append(le);
  }

  paxos1->init(strConfig, 1, 1);
  paxos2->init(strConfig, 2, 2);
  paxos3->init(strConfig, 3, 3);

  sleep(2);
  paxos1->setMaxPacketSize(2);
  paxos1->setMaxDelayIndex(2);
  paxos1->requestVote();
  sleep(1);

  // check rlog1's log
  sleep(1);

  EXPECT_EQ(paxos1->getLastLogIndex(), 11);
  EXPECT_EQ(paxos2->getLastLogIndex(), 11);
  EXPECT_EQ(paxos3->getLastLogIndex(), 11);

  LogEntry rle;
  rlog2->getEntry(1, rle, false);
  EXPECT_EQ(rle.term(), 1);
  EXPECT_NE(rle.optype(), kMock);
  rlog2->getEntry(2, rle, false);
  EXPECT_EQ(rle.term(), 1);
  EXPECT_NE(rle.optype(), kMock);
  rlog3->getEntry(3, rle, false);
  EXPECT_EQ(rle.term(), 1);
  EXPECT_NE(rle.optype(), kMock);

  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir1");
  deleteDir("paxosLogTestDir2");
  deleteDir("paxosLogTestDir3");
}
