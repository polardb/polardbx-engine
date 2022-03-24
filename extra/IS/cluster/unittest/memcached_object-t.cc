/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id: memcached_object-t.cc,v 1.0 08/27/2016 11:14:06 AM  hangfeng.fj( hangfeng.fj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file memcached_object-t.cc
 * @author hangfeng.fj( hangfeng.fj@alibaba-inc.com)
 * @date 08/27/2016 11:14:06 AM
 * @version 1.0
 * @brief unit test for MemcachedObject
 **/

#include <unistd.h>
#include <gtest/gtest.h>
#include "../memcached/memcached_object.h"
#include "rd_raft_log.h"

using namespace alisql;

TEST(MemcachedObject, newObj)
{
  MemcachedObject *object= new MemcachedObject();
  EXPECT_TRUE(object);
  delete object;
}

TEST(MemcachedObject, dumpObject)
{
  uint64_t seqNum= 16;
  std::string data("abcde");
  MemcachedObject object(static_cast<uint8_t>(kPut), 1, 2, data);

  std::string buffer;
  object.dumpObject(&buffer);

  MemcachedObject::serializeSeqNum(buffer, seqNum);

  MemcachedObject newObject;
  newObject.loadObject(buffer);
  
  EXPECT_EQ(newObject.getSeqNum(), seqNum);
  EXPECT_EQ(newObject.getOP(), object.getOP());
  EXPECT_EQ(newObject.getFlags(), object.getFlags());
  EXPECT_EQ(newObject.getExptime(), object.getExptime());
  EXPECT_STREQ(newObject.getData().data(), object.getData().data());
}

