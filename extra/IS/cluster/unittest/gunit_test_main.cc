/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  gunit_test_main.cc,v 1.0 07/30/2016 02:48:07 PM yingqiang.zyq(yingqiang.zyq@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file gunit_test_main.cc
 * @author yingqiang.zyq(yingqiang.zyq@alibaba-inc.com)
 * @date 07/30/2016 02:48:07 PM
 * @version 1.0
 * @brief 
 *
 **/

#include <gtest/gtest.h>

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  //::testing::InitGoogleMock(&argc, argv);

  return RUN_ALL_TESTS();
}
