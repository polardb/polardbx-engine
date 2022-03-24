/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id: text_response-t.cc,v 1.0 08/27/2016 11:14:06 AM  hangfeng.fj( hangfeng.fj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file text_response-t.cc
 * @author hangfeng.fj( hangfeng.fj@alibaba-inc.com)
 * @date 08/27/2016 11:14:06 AM
 * @version 1.0
 * @brief unit test for TextResponse
 *
 **/

#include <unistd.h>
#include <gtest/gtest.h>
#include "../memcached/text_response.h"
#include "../memcached/memcached_object.h"
#include "rd_raft_log.h"

using namespace alisql;

TEST(TextResponse, newObj)
{
  TextResponse *textResponse= new TextResponse();
  EXPECT_TRUE(textResponse);
  delete textResponse;
}

TEST(TextResponse, setResult)
{
  TextResponse textResponse;
  EXPECT_EQ(textResponse.getResult().length(), 0);
  const char STATUS_NOT_FOUND[] = "Not found";
  textResponse.setResult(STATUS_NOT_FOUND);
  EXPECT_EQ(textResponse.getResult(), STATUS_NOT_FOUND);
  EXPECT_EQ(textResponse.getResult().length(), sizeof(STATUS_NOT_FOUND) - 1);
}

TEST(TextResponse, serializeToArray)
{
  std::string data("abcde");
  MemcachedObject object(static_cast<uint8_t>(kPut), 1, 2, data);

  std::string buffer;
  object.dumpObject(&buffer);

  TextResponse textResponse;
  std::string key("key");

  textResponse.setValueResult(key, buffer, false);

  std::string result("VALUE key 1 5\r\nabcde\r\nEND\r\n");

  EXPECT_STREQ(textResponse.getResult().data(), result.data());
}
