/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  easyNet-t.cc,v 1.0 07/31/2016 11:42:21 PM
 *yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file easyNet-t.cc
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 07/31/2016 11:42:21 PM
 * @version 1.0
 * @brief unit test for alisql::EasyNet
 *
 **/

#include <gtest/gtest.h>
#include <unistd.h>
#include "easyNet.h"

using namespace alisql;

TEST(EasyNet, staticTest) {
  EasyNet *en = new EasyNet();
  EXPECT_TRUE(en);
  EasyNet::reciveProcess(NULL);
  delete en;
}

TEST(EasyNet, listen) {
  EasyNet *en = new EasyNet();
  EXPECT_TRUE(en);

  en->setWorkPool(NULL);

  EXPECT_TRUE(en->init() == 0);
  EXPECT_TRUE(en->start(11000) == 0);

  EXPECT_EQ(en->getReciveCnt(), 0);

  en->shutdown();
  delete en;
}

TEST(EasyNet, send) {
  EasyNet *en = new EasyNet();
  EXPECT_TRUE(en);

  en->setWorkPool(NULL);

  EXPECT_TRUE(en->init() == 0);
  EXPECT_TRUE(en->start(11001) == 0);
  easy_addr_t addr = en->createConnection(std::string("127.0.0.1:11001"), NULL);

  EXPECT_TRUE(addr.port != 0);

  /*
  const char *buf= "aaaaaaaaa";
  en->sendPacket(addr, buf, strlen(buf));
  sleep(1);
  EXPECT_EQ(en->getReciveCnt(), 2);

  const std::string sbuf("bbbbbbb");
  en->sendPacket(addr, sbuf);
  sleep(1);
  EXPECT_EQ(en->getReciveCnt(), 2);
  */

  en->shutdown();
  delete en;
}
