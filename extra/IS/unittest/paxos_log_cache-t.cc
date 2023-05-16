/************************************************************************
 *
 * Copyright (c) 2017 Alibaba.com, Inc. All Rights Reserved
 * $Id:  paxos_log_cache-t.cc,v 1.0 01/17/2017 06:57:45 PM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file paxos_log_cache-t.cc
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 01/17/2017 06:57:45 PM
 * @version 1.0
 * @brief
 *
 **/

#include <gtest/gtest.h>

#include "paxos_log_cache.h"

using namespace alisql;

::google::protobuf::RepeatedPtrField<LogEntry> makeEntries(uint64_t bi,
                                                           uint64_t ei) {
  ::google::protobuf::RepeatedPtrField<LogEntry> les;

  LogEntry le;
  le.set_term(1);
  le.set_optype(1);

  for (uint64_t i = bi; i <= ei; ++i) {
    le.set_index(i);
    *(les.Add()) = le;
  }

  return les;
}

TEST(PaxosLogCacheNode, t1) {
  PaxosLogCache cache;
  PaxosLogCacheNode *node;

  auto t1 = makeEntries(2, 4);
  cache.put(2, 4, t1);
  EXPECT_EQ(cache.getByteSize(), cache.debugGetByteSize());

  auto t2 = makeEntries(7, 9);
  cache.put(7, 9, t2);
  EXPECT_EQ(cache.getByteSize(), cache.debugGetByteSize());

  node = cache.debugGet(0);
  EXPECT_EQ(node->beginIndex, 2);
  EXPECT_EQ(node->endIndex, 4);
  EXPECT_EQ(node->entries.size(), 3);
  EXPECT_EQ(node->entries.begin()->index(), 2);
  EXPECT_EQ(cache.getByteSize(), cache.debugGetByteSize());

  node = cache.debugGet(1);
  EXPECT_EQ(node->beginIndex, 7);
  EXPECT_EQ(node->endIndex, 9);
  EXPECT_EQ(node->entries.size(), 3);
  EXPECT_EQ(node->entries.begin()->index(), 7);
  EXPECT_EQ(cache.getByteSize(), cache.debugGetByteSize());

  auto t3 = makeEntries(6, 8);
  cache.put(6, 8, t3);
  EXPECT_EQ(cache.getByteSize(), cache.debugGetByteSize());

  node = cache.debugGet(1);
  EXPECT_EQ(node->beginIndex, 6);
  EXPECT_EQ(node->endIndex, 9);
  EXPECT_EQ(node->entries.size(), 4);
  EXPECT_EQ(node->entries.begin()->index(), 6);
  EXPECT_EQ(cache.getByteSize(), cache.debugGetByteSize());

  node = cache.get(1);
  EXPECT_EQ(node == NULL, true);
  EXPECT_EQ(cache.getByteSize(), cache.debugGetByteSize());

  node = cache.get(3);
  EXPECT_EQ(node->beginIndex, 3);
  EXPECT_EQ(node->endIndex, 4);
  EXPECT_EQ(node->entries.size(), 2);
  EXPECT_EQ(node->entries.begin()->index(), 3);
  EXPECT_EQ(cache.getByteSize(), cache.debugGetByteSize());

  auto t5 = makeEntries(13, 15);
  cache.put(13, 15, t5);
  EXPECT_EQ(cache.getByteSize(), cache.debugGetByteSize());

  node = cache.get(12);
  EXPECT_EQ(node == NULL, true);
  EXPECT_EQ(cache.getByteSize(), cache.debugGetByteSize());

  node = cache.debugGet(0);
  EXPECT_EQ(node->beginIndex, 13);
  EXPECT_EQ(node->endIndex, 15);
  EXPECT_EQ(node->entries.size(), 3);
  EXPECT_EQ(node->entries.begin()->index(), 13);
  EXPECT_EQ(cache.getByteSize(), cache.debugGetByteSize());

  auto t4 = makeEntries(17, 19);
  cache.put(17, 19, t4);
  EXPECT_EQ(cache.getByteSize(), cache.debugGetByteSize());

  node = cache.get(17);
  EXPECT_EQ(node->beginIndex, 17);
  EXPECT_EQ(node->endIndex, 19);
  EXPECT_EQ(node->entries.size(), 3);
  EXPECT_EQ(node->entries.begin()->index(), 17);
  EXPECT_EQ(cache.getByteSize(), cache.debugGetByteSize());

  node = cache.debugGet(0);
  EXPECT_EQ(node == NULL, true);
  EXPECT_EQ(cache.getByteSize(), cache.debugGetByteSize());

  auto t6 = makeEntries(21, 21);
  cache.put(21, 21, t6);
  EXPECT_EQ(cache.getByteSize(), cache.debugGetByteSize());
  auto t7 = makeEntries(22, 22);
  cache.put(22, 22, t7);
  EXPECT_EQ(cache.getByteSize(), cache.debugGetByteSize());

  node = cache.debugGet(0);
  EXPECT_EQ(node->beginIndex, 21);
  EXPECT_EQ(node->endIndex, 22);
  EXPECT_EQ(node->entries.size(), 2);
  EXPECT_EQ(node->entries.begin()->index(), 21);
  EXPECT_EQ(cache.getByteSize(), cache.debugGetByteSize());

  auto t8 = makeEntries(20, 20);
  cache.put(20, 20, t8);
  EXPECT_EQ(cache.getByteSize(), cache.debugGetByteSize());

  node = cache.debugGet(0);
  EXPECT_EQ(node->beginIndex, 20);
  EXPECT_EQ(node->endIndex, 22);
  EXPECT_EQ(node->entries.size(), 3);
  EXPECT_EQ(node->entries.begin()->index(), 20);
  EXPECT_EQ(cache.getByteSize(), cache.debugGetByteSize());
  // print 0 if WITH_DEBUG_LOG_CACHE is OFF
  std::cout << "Cache ByteSize: " << cache.getByteSize() << std::endl;

  node = cache.get(1000);  // will return null and clear all cache
  EXPECT_TRUE(node == NULL);
  EXPECT_EQ(cache.getByteSize(), 0);

  auto t9 = makeEntries(2, 4);
  cache.put(2, 4, t9);
  EXPECT_EQ(cache.getByteSize(), cache.debugGetByteSize());

  cache.clear();
  EXPECT_EQ(cache.getByteSize(), 0);
}
