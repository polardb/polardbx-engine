/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  service-t.cc,v 1.0 08/01/2016 11:14:06 AM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file client-service-t.cc
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 08/01/2016 11:14:06 AM
 * @version 1.0
 * @brief unit test for alisql::Service
 *
 **/

#include <gtest/gtest.h>
#include <unistd.h>
#include <iostream>
#include "client_service.h"

using namespace alisql;

TEST(ClientService, t1) {
  ClientService *cs = new ClientService();
  EXPECT_TRUE(cs);

  const std::string &str = cs->get("aa");
  std::cout << str << std::endl;
  EXPECT_EQ(str, "");

  cs->set("aaa", "bbb");
  std::cout << cs->get("aaa") << std::endl;
}
