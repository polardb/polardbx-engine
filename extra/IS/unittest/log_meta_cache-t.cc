/************************************************************************
 *
 * Copyright (c) 2019 Alibaba.com, Inc. All Rights Reserved
 * $Id:  log_meta_cache-t.cc,v 1.0 07/14/2019 13:35:00 PM
 *aili.xp(aili.xp@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file log_meta_cache-t.cc
 * @author aili.xp(aili.xp@alibaba-inc.com)
 * @date 07/14/2019 13:35:00 PM
 * @version 1.0
 * @brief unit test for LogMetaCache
 *
 **/

#include <gtest/gtest.h>

#include "log_meta_cache.h"

using namespace alisql;

TEST(LogMetaCache, Basic) {
  LogMetaCache cache;
  cache.init();

  uint64_t index = 1, term = 2, optype = 3, info = 4;
  ASSERT_FALSE(cache.getLogMeta(0, &term, &optype, &info));

  ASSERT_TRUE(cache.putLogMeta(index, term, optype, info));
  term = 0, optype = 0, info = 0;
  ASSERT_TRUE(cache.getLogMeta(index, &term, &optype, &info));
  ASSERT_TRUE(term == 2 && optype == 3 && info == 4);

  term = 3, optype = 4, info = 5;
  ASSERT_TRUE(cache.putLogMeta(index + 1, term, optype, info));
  term = 0, optype = 0, info = 0;
  ASSERT_TRUE(cache.getLogMeta(index + 1, &term, &optype, &info));
  ASSERT_TRUE(term == 3 && optype == 4 && info == 5);

  ASSERT_FALSE(cache.getLogMeta(index + 2, &term, &optype, &info));

  term = 2, optype = 3, info = 4;
  ASSERT_TRUE(cache.putLogMeta(index + 3, term, optype, info));
  term = 0, optype = 0, info = 0;
  ASSERT_TRUE(cache.getLogMeta(index + 3, &term, &optype, &info));
  ASSERT_TRUE(term == 2 && optype == 3 && info == 4);
  // index and index+1 is cleared by index+3
  ASSERT_FALSE(cache.getLogMeta(index, &term, &optype, &info));
  ASSERT_FALSE(cache.getLogMeta(index + 1, &term, &optype, &info));
}

TEST(LogMetaCache, Full) {
  LogMetaCache cache;
  cache.init();

  uint64_t index = 1, term = 2, optype = 3, info = 4;
  for (int i = 0; i < 8193; ++i)
    ASSERT_TRUE(cache.putLogMeta(index + i, term + i, optype + i, info + i));

  ASSERT_FALSE(cache.getLogMeta(index, &term, &optype, &info));

  for (int i = index + 1; i < 8193; ++i) {
    ASSERT_TRUE(cache.getLogMeta(index + i, &term, &optype, &info));
    ASSERT_TRUE(term == 2u + i && optype == 3u + i && info == 4u + i);
  }
}
