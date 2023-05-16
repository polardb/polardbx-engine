/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  paxos_log-t.cc,v 1.0 01/11/2017 11:28:24 AM
 *jarry.zj(jarry.zj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file paxos_log-t.cc
 * @author jarry.zj(jarry.zj@alibaba-inc.com)
 * @date 01/11/2017 11:28:24 AM
 * @version 1.0
 * @brief
 *
 **/

#include <gtest/gtest.h>
#include "files.h"
#include "mem_paxos_log.h"
#include "paxos.pb.h"
#include "paxos_log.h"
#include "rd_paxos_log.h"

using namespace alisql;

TEST(PaxosLog, basic) {
  std::string dir = "paxosLogTestBasic";
  RDPaxosLog *log = new RDPaxosLog(dir, true, 4 * 1024 * 1024);
  EXPECT_EQ(0, log->getLastLogIndex());
  EXPECT_EQ(1, log->getLength());
  log->appendEmptyEntry();
  log->appendEmptyEntry();
  log->appendEmptyEntry();
  log->appendEmptyEntry();
  log->appendEmptyEntry();
  log->appendEmptyEntry();
  EXPECT_EQ(6, log->getLastLogIndex());
  EXPECT_EQ(7, log->getLength());
  EXPECT_EQ(0, log->getFirstLogIndex());

  log->truncateForward(4);
  log->appendEmptyEntry();
  EXPECT_EQ(4, log->getFirstLogIndex());
  EXPECT_EQ(7, log->getLastLogIndex());
  EXPECT_EQ(4, log->getLength());
  LogEntry le;
  EXPECT_NE(0, log->getEntry(3, le));
  EXPECT_EQ(0, log->getEntry(4, le));

  delete log;
  deleteDir(dir.c_str());
}

TEST(PaxosLog, truncate_speed) {
  std::string dir = "paxosLogTestBasic";
  RDPaxosLog *log = new RDPaxosLog(dir, true, 4 * 1024 * 1024);

  EXPECT_EQ(0, log->getLastLogIndex());
  EXPECT_EQ(1, log->getLength());
  // 40w, 76800 bytes each log
  LogEntry le;
  le.set_index(0);
  le.set_term(1);
  le.set_optype(1);
  constexpr int DATA1 = 76800;
  constexpr int DATA2 = 400000;
  constexpr int DATA3 = 100;
  char buf[DATA1 + 1];
  int i;
  for (i = 0; i < DATA1; i++) {
    buf[i] = rand() % 256;
  }
  buf[DATA1] = '\0';
  for (i = 0; i < (DATA2 + 100); ++i) {
    // shuffle
    for (int j = 0; j < DATA3; j++) {
      int a = rand() % DATA1;
      int b = rand() % DATA1;
      int tmp = buf[a];
      buf[a] = buf[b];
      buf[b] = tmp;
    }
    le.set_value(buf);
    log->appendEntry(le);
  }
  easy_warn_log("log last index %d\n", log->getLastLogIndex());
  log->truncateForward(DATA2);
  easy_warn_log("finish truncate last log index %d, first index %d\n",
                log->getLastLogIndex(), log->getFirstLogIndex());
  delete log;
  deleteDir(dir.c_str());
}

TEST(PaxosLog, memlog) {
  std::string dir = "paxosLogTestMemlog";
  MemPaxosLog *log = new MemPaxosLog();
  log->appendEmptyEntry();
  log->appendEmptyEntry();
  log->appendEmptyEntry();
  EXPECT_EQ(3, log->getLength());
  LogEntry le;
  log->getEntry(le);
  EXPECT_EQ(2, log->getLength());
  log->getEntry(le);
  EXPECT_EQ(1, log->getLength());

  delete log;
  deleteDir(dir.c_str());
}
