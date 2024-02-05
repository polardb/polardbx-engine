/************************************************************************
 *
 * Copyright (c) 2019 Alibaba.com, Inc. All Rights Reserved
 * $Id:  consensus-replicate-with-cache-log-t.cc,v 1.0 03/14/2019 11:06:00 AM
 *aili.xp(aili.xp@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file consensus-replicate-with-cache-log-t.cc
 * @author aili.xp(aili.xp@alibaba-inc.com)
 * @date 03/14/2019 11:06:00 AM
 * @version 1.0
 * @brief unit test for alisql::Consensus
 *
 **/

#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>

#include "file_paxos_log.h"
#include "files.h"
#include "paxos.h"
#include "paxos.pb.h"

using namespace alisql;

class FilePaxosLogTest : public FilePaxosLog {
 public:
  FilePaxosLogTest(const std::string &dataDir, LogTypeT type = LTMem)
      : FilePaxosLog(dataDir, type) {}
  void setLastCachedLogIndex(uint64_t ci) { cache_index = ci; }
  virtual uint64_t getLastCachedLogIndex() { return cache_index; }
  uint64_t cache_index = 0;
};

TEST(consensus, replicate_with_cache_log) {
  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  const uint64_t timeout = 2000;
  std::string dir1 = std::string("paxosLogTestDir") + strConfig[0];
  std::shared_ptr<PaxosLog> rlog1 =
      std::make_shared<FilePaxosLogTest>(dir1, FilePaxosLog::LogType::LTMem);
  Paxos *paxos1 = new Paxos(timeout, rlog1);
  paxos1->init(strConfig, 1, 1);
  std::string dir2 = std::string("paxosLogTestDir") + strConfig[1];
  std::shared_ptr<PaxosLog> rlog2 =
      std::make_shared<FilePaxosLogTest>(dir2, FilePaxosLog::LogType::LTMem);
  Paxos *paxos2 = new Paxos(timeout, rlog2);
  paxos2->init(strConfig, 2, 2);
  std::string dir3 = std::string("paxosLogTestDir") + strConfig[2];
  std::shared_ptr<PaxosLog> rlog3 =
      std::make_shared<FilePaxosLogTest>(dir3, FilePaxosLog::LogType::LTMem);
  Paxos *paxos3 = new Paxos(timeout, rlog3);
  paxos3->init(strConfig, 3, 3);

  sleep(3);
  paxos1->requestVote();
  sleep(1);

  EXPECT_EQ(paxos1->getState(), Paxos::LEADER);
  uint64_t base = rlog1->getLastLogIndex();

  paxos1->setReplicateWithCacheLog(true);

  LogEntry le;
  le.set_optype(0);
  le.set_ikey("a");

  paxos1->replicateLog(le);
  EXPECT_EQ(rlog1->getLastLogIndex(), base + 1);
  sleep(2);
  EXPECT_TRUE(rlog2->getLastLogIndex() == base &&
              rlog3->getLastLogIndex() == base);

  ((FilePaxosLogTest *)rlog1.get())
      ->setLastCachedLogIndex(rlog1->getLastLogIndex());
  paxos1->appendLog(false);
  sleep(2);
  EXPECT_TRUE(rlog2->getLastLogIndex() == base + 1 &&
              rlog3->getLastLogIndex() == base + 1);

  paxos1->setReplicateWithCacheLog(false);

  LogEntry le2;
  le2.set_optype(0);
  le2.set_ikey("a");
  paxos1->replicateLog(le2);
  sleep(2);
  EXPECT_TRUE(rlog2->getLastLogIndex() == base + 2 &&
              rlog3->getLastLogIndex() == base + 2);

  delete paxos3;
  delete paxos2;
  delete paxos1;

  deleteDir(dir1.c_str());
  deleteDir(dir2.c_str());
  deleteDir(dir3.c_str());
}
