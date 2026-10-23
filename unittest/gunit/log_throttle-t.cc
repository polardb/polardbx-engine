/* Copyright (c) 2013, 2022, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include <gtest/gtest.h>
#include <sys/types.h>

#include "my_systime.h"
#include "mysql/psi/mysql_mutex.h"
#include "sql/log.h"
#include "unittest/gunit/test_utils.h"

namespace log_throttle_unittest {

using my_testing::Server_initializer;

int summary_count = 0;
char last_query[32];
bool last_aggregate = false;
ulonglong last_lock_usec = 0;
ulonglong last_exec_usec = 0;

static bool slow_logger(THD *, const char *query, size_t, bool aggregate,
                        ulonglong lock_usec, ulonglong exec_usec) {
  summary_count++;
  last_aggregate = aggregate;
  last_lock_usec = lock_usec;
  last_exec_usec = exec_usec;
  strcpy(last_query, query);
  return false;
}

class LogThrottleTest : public ::testing::Test {
 protected:
  void SetUp() override {
    initializer.SetUp();
    mysql_mutex_init(0, &m_mutex, MY_MUTEX_INIT_FAST);
    summary_count = 0;
    last_aggregate = false;
    last_lock_usec = last_exec_usec = 0;
  }

  void TearDown() override {
    initializer.TearDown();
    mysql_mutex_destroy(&m_mutex);
  }

  THD *thd() { return initializer.thd(); }

  Server_initializer initializer;
  mysql_mutex_t m_mutex;
};

// Slow_log_throttles test cases starts from here.

/*
  Test basic functionality - throttling, eligibility, printing of summary of
                             Slow_log_throttle.
*/
TEST_F(LogThrottleTest, SlowLogBasic) {
  ulong threshold = 2;
  ulong window = 1000000;
  Slow_log_throttle throttle(&threshold, &m_mutex, window, slow_logger, "%lu");

  // Should not be throttled
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_FALSE(throttle.log(thd(), true));

  // Should not be throttled, not eligible
  EXPECT_FALSE(throttle.log(thd(), false));

  // Flush and check that summary was not printed
  EXPECT_FALSE(throttle.flush(thd()));
  EXPECT_EQ(0, summary_count);

  // Should be throttled
  EXPECT_TRUE(throttle.log(thd(), true));

  // Flush and check that summary was printed
  EXPECT_TRUE(throttle.flush(thd()));
  EXPECT_EQ(1, summary_count);

  // Flush and check that summary was not printed again
  EXPECT_FALSE(throttle.flush(thd()));
  EXPECT_EQ(1, summary_count);

  // Get another summary printed
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.flush(thd()));
  EXPECT_EQ(2, summary_count);
}

// Test changes to the threshold level of slow logger
TEST_F(LogThrottleTest, SlowLogThresholdChange) {
  ulong threshold = 2;
  ulong window = 1000000;
  Slow_log_throttle throttle(&threshold, &m_mutex, window, slow_logger, "%lu");

  // Reach threshold
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_FALSE(throttle.log(thd(), true));

  // Flush and check that summary was not printed
  EXPECT_FALSE(throttle.flush(thd()));
  EXPECT_EQ(0, summary_count);

  // Reduce threshold, flush and check that summary was printed
  threshold = 1;
  EXPECT_TRUE(throttle.flush(thd()));
  EXPECT_EQ(1, summary_count);

  // Increase threshold and reach it
  threshold = 3;
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_FALSE(throttle.log(thd(), true));

  // Flush and check that summary was not printed
  EXPECT_FALSE(throttle.flush(thd()));
  EXPECT_EQ(1, summary_count);
}

// Test number of suppressed messages written by slow logger
TEST_F(LogThrottleTest, SlowLogSuppressCount) {
  ulong threshold = 1;
  ulong window = 1000000;
  Slow_log_throttle throttle(&threshold, &m_mutex, window, slow_logger, "%lu");

  // Suppress 3 events
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.flush(thd()));
  EXPECT_EQ(1, summary_count);
  EXPECT_STREQ("3", last_query);

  // Suppress 2 events
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_FALSE(throttle.log(thd(), false));
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_FALSE(throttle.log(thd(), false));
  EXPECT_TRUE(throttle.flush(thd()));
  EXPECT_EQ(2, summary_count);
  EXPECT_STREQ("2", last_query);
}
// End of Slow_log_throttles test cases.

// The legacy Slow_log_throttle tests above remain unchanged.
class ScopedShardedThrottle : public Sharded_slow_log_throttle {
 public:
  explicit ScopedShardedThrottle(ulong window = 60000000)
      : Sharded_slow_log_throttle(window, slow_logger, "%llu") {
    EXPECT_FALSE(init(0));
  }
  ~ScopedShardedThrottle() { cleanup(); }
};

TEST_F(LogThrottleTest, ShardedQuotaIsGlobal) {
  ScopedShardedThrottle throttle;
  throttle.update_rate(thd(), 3);
  int admitted = 0;
  // New THDs receive distinct connection IDs and exercise different shards.
  for (int i = 0; i < 32; ++i) {
    THD other;
    other.set_new_thread_id();
    other.start_utime = my_micro_time();
    if (!throttle.log(&other, true)) ++admitted;
  }
  EXPECT_EQ(3, admitted);
  EXPECT_TRUE(throttle.flush(thd()));
  EXPECT_EQ(1, summary_count);
  EXPECT_STREQ("29", last_query);
  EXPECT_FALSE(throttle.flush(thd()));
}

TEST_F(LogThrottleTest, ShardedFlushDoesNotRefillQuota) {
  ScopedShardedThrottle throttle;
  throttle.update_rate(thd(), 1);
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.flush(thd()));
  EXPECT_STREQ("1", last_query);
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.flush(thd()));
  EXPECT_STREQ("1", last_query);
  EXPECT_EQ(2, summary_count);
}

TEST_F(LogThrottleTest, ShardedRateChangeReportsActualSuppression) {
  ScopedShardedThrottle throttle;
  throttle.update_rate(thd(), 1);
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.update_rate(thd(), 100));
  EXPECT_STREQ("2", last_query);
  EXPECT_EQ(1, summary_count);
  EXPECT_FALSE(throttle.flush(thd()));
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_FALSE(throttle.update_rate(thd(), 0));
  EXPECT_EQ(1, summary_count);
  for (int i = 0; i < 10; ++i) EXPECT_FALSE(throttle.log(thd(), true));
}

TEST_F(LogThrottleTest, ShardedUnchangedRateDoesNotRefillQuota) {
  ScopedShardedThrottle throttle;
  throttle.update_rate(thd(), 1);
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_FALSE(throttle.update_rate(thd(), 1));
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.update_rate(thd(), 1));
  EXPECT_STREQ("1", last_query);
  EXPECT_TRUE(throttle.log(thd(), true));
}

TEST_F(LogThrottleTest, ShardedIneligibleQueriesDoNotConsumeQuota) {
  ScopedShardedThrottle throttle;
  throttle.update_rate(thd(), 1);
  for (int i = 0; i < 32; ++i) EXPECT_FALSE(throttle.log(thd(), false));
  EXPECT_FALSE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_FALSE(throttle.log(thd(), false));
  EXPECT_TRUE(throttle.flush(thd()));
  EXPECT_STREQ("1", last_query);
}

TEST_F(LogThrottleTest, ShardedSummaryUsesAggregateCallback) {
  ScopedShardedThrottle throttle;
  throttle.update_rate(thd(), 1);
  EXPECT_FALSE(throttle.log(thd(), true));
  thd()->start_utime = my_micro_time() - 1000;
  thd()->pop_lock_usec(7);
  const ulonglong saved_start = thd()->start_utime;
  Security_context *saved_sctx = thd()->security_context();
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.flush(thd()));
  EXPECT_TRUE(last_aggregate);
  EXPECT_EQ(7ULL, last_lock_usec);
  EXPECT_GE(last_exec_usec, 1000ULL);
  EXPECT_EQ(saved_start, thd()->start_utime);
  EXPECT_EQ(7ULL, thd()->get_lock_usec());
  EXPECT_EQ(saved_sctx, thd()->security_context());

  // A later summary must contain only the new statement's lock time.
  thd()->start_utime = my_micro_time();
  thd()->pop_lock_usec(3);
  EXPECT_TRUE(throttle.log(thd(), true));
  EXPECT_TRUE(throttle.flush(thd()));
  EXPECT_EQ(3ULL, last_lock_usec);
  EXPECT_EQ(2, summary_count);
  EXPECT_FALSE(throttle.flush(thd()));
  EXPECT_EQ(2, summary_count);
}

}  // namespace log_throttle_unittest
