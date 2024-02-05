/************************************************************************
 *
 * Copyright (c) 2019 Alibaba.com, Inc. All Rights Reserved
 * $Id:  consensus-leader-degrade-t.cc,v 1.0 07/15/2019 14:03:00 PM
 *aili.xp(aili.xp@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file consensus-leader-degrade-t.cc
 * @author aili.xp(aili.xp@alibaba-inc.com)
 * @date 07/15/2019 14:03:00 PM
 * @version 1.0
 * @brief unit test for leader degradation due to large io
 *
 **/

#include <gtest/gtest.h>
#include <unistd.h>
#include <atomic>
#include <iostream>
#include <string>
#include <vector>

#include "file_paxos_log.h"
#include "files.h"
#include "paxos.h"

using namespace alisql;

class TestFilePaxosLog : public FilePaxosLog {
 public:
  TestFilePaxosLog(const std::string &dataDir)
      : FilePaxosLog(dataDir), delay(0) {}

  virtual int getEntry(uint64_t logIndex, LogEntry &entry,
                       bool fastFail = false) {
    if (!fastFail && delay) sleep(delay.load());
    return FilePaxosLog::getEntry(logIndex, entry, fastFail);
  }

  void setDelay(int delay) { this->delay = delay; }

 private:
  std::atomic<int> delay;
};

// OptimisticHeartbeat make sure that leader will not degrade if follower
// response to heartbeat too slow
TEST(LeaderDegrade, OptimisticHeartbeat) {
  extern easy_log_level_t easy_log_level;
  easy_log_level = EASY_LOG_INFO;

  std::vector<std::string> strConfig;
  strConfig.emplace_back("127.0.0.1:11001");
  strConfig.emplace_back("127.0.0.1:11002");
  strConfig.emplace_back("127.0.0.1:11003");

  uint64_t timeout = 5000;
  std::shared_ptr<PaxosLog> rlog;
  rlog = std::make_shared<TestFilePaxosLog>("paxosLogTestDir61");
  Paxos *paxos1 = new Paxos(timeout, rlog, 10000);
  paxos1->init(strConfig, 1, 1, NULL, 4, 4, NULL, false, 1);
  rlog = std::make_shared<TestFilePaxosLog>("paxosLogTestDir62");
  Paxos *paxos2 = new Paxos(timeout, rlog, 10000);
  paxos2->init(strConfig, 2, 2, NULL, 4, 4, NULL, false, 1);
  rlog = std::make_shared<TestFilePaxosLog>("paxosLogTestDir63");
  Paxos *paxos3 = new Paxos(timeout, rlog, 10000);
  paxos3->init(strConfig, 3, 3, NULL, 4, 4, NULL, false, 1);

  Paxos *leader = NULL, *follower1 = NULL, *follower2 = NULL;
  Paxos *paxosList[3];
  paxosList[0] = paxos1;
  paxosList[1] = paxos2;
  paxosList[2] = paxos3;
  while (leader == NULL) {
    sleep(3);
    for (int i = 0; i < 3; ++i) {
      if (paxosList[i]->getState() == Paxos::LEADER) {
        leader = paxosList[i];
        follower1 = paxosList[(i + 1) % 3];
        follower2 = paxosList[(i + 2) % 3];
        std::cout << "Leader is " << i << std::endl;
        break;
      }
    }

    if (leader == NULL) std::cout << "====> Election Fail! " << std::endl;
  }

  LogEntry le;
  le.set_index(0);
  le.set_optype(1);
  le.set_info(1);
  le.set_value(std::string(2 * 1024 * 1024, '0'));
  leader->replicateLog(le);

  sleep(1);
  ((TestFilePaxosLog *)follower1->getLog().get())->setDelay(15);
  ((TestFilePaxosLog *)follower2->getLog().get())->setDelay(15);

  le.set_index(0);
  leader->replicateLog(le);

  // wait at least 2 epoch timer callback
  sleep(11);

  ASSERT_TRUE(leader->getState() != Paxos::LEADER);

  ((TestFilePaxosLog *)follower1->getLog().get())->setDelay(0);
  ((TestFilePaxosLog *)follower2->getLog().get())->setDelay(0);

  sleep(5);

  std::cout << "Second Round!!!" << std::endl;

  leader = NULL;
  while (leader == NULL) {
    sleep(3);
    for (int i = 0; i < 3; ++i) {
      if (paxosList[i]->getState() == Paxos::LEADER) {
        leader = paxosList[i];
        follower1 = paxosList[(i + 1) % 3];
        follower2 = paxosList[(i + 2) % 3];
        std::cout << "Leader is " << i << std::endl;
        break;
      }
    }

    if (leader == NULL) std::cout << "====> Election Fail! " << std::endl;
  }

  sleep(5);

  follower1->setOptimisticHeartbeat(true);
  follower2->setOptimisticHeartbeat(true);
  ((TestFilePaxosLog *)follower1->getLog().get())->setDelay(15);
  ((TestFilePaxosLog *)follower2->getLog().get())->setDelay(15);

  le.set_index(0);
  leader->replicateLog(le);

  // wait at least 2 epoch timer callback
  sleep(11);

  ASSERT_TRUE(leader->getState() == Paxos::LEADER);

  ((TestFilePaxosLog *)follower1->getLog().get())->setDelay(0);
  ((TestFilePaxosLog *)follower2->getLog().get())->setDelay(0);

  sleep(5);

  delete paxos1;
  delete paxos2;
  delete paxos3;

  deleteDir("paxosLogTestDir61");
  deleteDir("paxosLogTestDir62");
  deleteDir("paxosLogTestDir63");
}
