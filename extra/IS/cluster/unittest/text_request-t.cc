/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id: text_request-t.cc,v 1.0 08/27/2016 11:14:06 AM  hangfeng.fj( hangfeng.fj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file text_request-t.cc
 * @author hangfeng.fj( hangfeng.fj@alibaba-inc.com)
 * @date 08/27/2016 11:14:06 AM
 * @version 1.0
 * @brief unit test for TextResponse
 **/

#include <unistd.h>
#include <gtest/gtest.h>
#include "../memcached/text_request.h"

using namespace alisql;

#define MEMCACHE_TEST(n, d) \
  char data##n[] = d; \
  TextRequest t##n; \
  t##n.init(data##n, sizeof(data##n) - 1); \
  t##n.parse();

TEST(TextRequest, newObj)
{
  TextRequest *textRequest= new TextRequest();
  EXPECT_TRUE(textRequest);
  delete textRequest;
}

TEST(TextRequest, parse)
{
  MEMCACHE_TEST(1, "set key 0 0 1 \r\n");
  EXPECT_EQ(t1.getCommand(), TextCommand::SET);
}

TEST(TextRequest, parseStorage)
{
  MEMCACHE_TEST(1, "set key 0 0 1 \r\na\r\n");
  EXPECT_EQ(t1.getCommand(), TextCommand::SET);
  EXPECT_EQ(std::get<1>(t1.getKey()), 3);
  EXPECT_EQ(t1.getFlags(), 0);
  EXPECT_EQ(t1.getExptime(), 0);
  EXPECT_EQ(t1.getNoreply(), false);
  EXPECT_EQ(t1.getValid(), true);
  EXPECT_EQ(std::get<1>(t1.getData()), 1);
}

TEST(TextRequest, parseTairStorage)
{
  MEMCACHE_TEST(1, "tair_set key 0 0 1 0 0\r\na\r\n");
  EXPECT_EQ(t1.getCommand(), TextCommand::TAIR_SET);
  EXPECT_EQ(std::get<1>(t1.getKey()), 3);
  EXPECT_EQ(t1.getFlags(), 0);
  EXPECT_EQ(t1.getExptime(), 0);
  EXPECT_EQ(t1.getNoreply(), false);
  EXPECT_EQ(t1.getNameSpace(), 0);
  EXPECT_EQ(t1.getVersion(), 0);
  EXPECT_EQ(t1.getValid(), true);
  EXPECT_EQ(std::get<1>(t1.getData()), 1);
}

TEST(TextRequest, parseGet)
{
  MEMCACHE_TEST(1, "get key\r\n");
  EXPECT_EQ(t1.getCommand(), TextCommand::GET);
  EXPECT_EQ(t1.getValid(), true);
}


TEST(TextRequest, parseDelete)
{
  MEMCACHE_TEST(1, "delete key\r\n");
  EXPECT_EQ(t1.getCommand(), TextCommand::DELETE);
  EXPECT_EQ(t1.getValid(), true);
}

TEST(TextRequest, parseCas)
{
  MEMCACHE_TEST(1, "cas key 0 0 1 61 \r\na\r\n");
  EXPECT_EQ(t1.getCommand(), TextCommand::CAS);
  EXPECT_EQ(std::get<1>(t1.getKey()), 3);
  EXPECT_EQ(t1.getFlags(), 0);
  EXPECT_EQ(t1.getExptime(), 0);
  EXPECT_EQ(t1.getNoreply(), false);
  EXPECT_EQ(t1.getValid(), true);
  EXPECT_EQ(std::get<1>(t1.getData()), 1);
  EXPECT_EQ(t1.getCasUnique(), 61);
}
