/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  service-t.cc,v 1.0 08/01/2016 11:14:06 AM yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file service-t.cc
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 08/01/2016 11:14:06 AM
 * @version 1.0
 * @brief unit test for RDRaftLog
 **/

#include <unistd.h>
#include <gtest/gtest.h>
#include "rd_raft_log.h"
#include "files.h"
#include "rocksdb/options.h"

using namespace alisql;

TEST(RDRaftLog, MetaData)
{
  const char* raftLogTestDir= "raftLogTestDir";
  deleteDir(raftLogTestDir);
  rocksdb::Options options;
  RaftLog *rlog= new RDRaftLog(raftLogTestDir, true, 4 * 1024 * 1024);

  int ret= rlog->setMetaData("VoteFor", 1);
  EXPECT_EQ(ret, 0);

  uint64_t voteFor= 0;
  ret= rlog->getMetaData("VoteFor", &voteFor);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(voteFor, 1);

  delete rlog;
}

TEST(RDRaftLog, RaftLog)
{
  const char* raftLogTestDir = "raftLogTestDir";
  deleteDir(raftLogTestDir);
  RaftLog *rlog= new RDRaftLog(raftLogTestDir, true, 4 * 1024 * 1024);
  LogEntry entry;

  EXPECT_EQ(rlog->getLastLogIndex(), 0);
  rlog->getEntry(0, entry);
  EXPECT_EQ(entry.term(), 0);
  EXPECT_EQ(entry.index(), 0);

  entry.set_term(1);
  entry.set_index(1);
  entry.set_optype(1);
  entry.set_key("aa");
  entry.set_value("bb");
  rlog->append(entry);

  entry.Clear();
  rlog->getEntry(1, entry);
  EXPECT_EQ(rlog->getLastLogIndex(), 1);
  EXPECT_EQ(entry.term(), 1);
  EXPECT_EQ(entry.index(), 1);
  EXPECT_EQ(entry.optype(), 1);
  EXPECT_EQ(entry.key(), "aa");
  EXPECT_EQ(entry.value(), "bb");

  entry.set_term(1);
  entry.set_index(2);
  entry.set_optype(1);
  entry.set_key("aa1");
  entry.set_value("bb1");
  rlog->append(entry);
  entry.set_term(1);
  entry.set_index(3);
  entry.set_optype(1);
  entry.set_key("aa2");
  entry.set_value("bb2");
  rlog->append(entry);

  entry.Clear();
  rlog->getEntry(3, entry);
  EXPECT_EQ(entry.term(), 1);
  EXPECT_EQ(entry.index(), 3);
  EXPECT_EQ(entry.optype(), 1);
  EXPECT_EQ(entry.key(), "aa2");
  EXPECT_EQ(entry.value(), "bb2");

  rlog->truncateBackward(2);

  entry.Clear();
  rlog->getEntry(1, entry);
  EXPECT_EQ(rlog->getLastLogIndex(), 1);
  EXPECT_EQ(entry.term(), 1);
  EXPECT_EQ(entry.index(), 1);
  EXPECT_EQ(entry.optype(), 1);
  EXPECT_EQ(entry.key(), "aa");
  EXPECT_EQ(entry.value(), "bb");

  delete rlog;
}


