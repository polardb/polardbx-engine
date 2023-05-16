/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  pxserror-t.cc,v 1.0 May 8, 2019 10:58:29 AM
 *jarry.zj(jarry.zj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file pxserror-t.cc
 * @author jarry.zj(jarry.zj@alibaba-inc.com)
 * @date May 8, 2019 10:58:29 AM
 * @version 1.0
 * @brief
 *
 **/
#include <gtest/gtest.h>
#include "paxos_error.h"

using namespace alisql;

TEST(PaxosError, basic) {
  EXPECT_STREQ(pxserror(), "Some error happens, please check the error log.");
  EXPECT_STREQ(pxserror(PaxosErrorCode::PE_TEST), "For test.");
  EXPECT_STREQ(pxserror(-1), "Some error happens, please check the error log.");
  EXPECT_STREQ(pxserror(PaxosErrorCode::PE_TOTAL), "Unknown error.");
  EXPECT_STREQ(pxserror(PaxosErrorCode::PE_NONE), "Success.");
}
