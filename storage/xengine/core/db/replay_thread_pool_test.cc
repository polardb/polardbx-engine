/*
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <assert.h>
#include <iostream>
#include <memory>

#include "db/db_impl.h"
#include "db/db_test_util.h"
#include "db/replay_task.h"
#include "db/replay_thread_pool.h"
#include "xengine/status.h"
#include "util/testutil.h"

using namespace xengine;
using namespace common;

#define private public

namespace xengine {
namespace db {

class MockReplayTask : public ReplayTask {
 public:
  using ReplayTask::ReplayTask;
  virtual ~MockReplayTask() = default;
  virtual void run() override {};
};

class ReplayThreadPoolTest : public testing::Test {
public:
  ReplayThreadPoolTest() = default;
  ~ReplayThreadPoolTest() = default;
};

TEST_F(ReplayThreadPoolTest, test_run_thread_pool) {
  ReplayThreadPool error_thread_pool(5, nullptr);
  ASSERT_EQ(Status::kInvalidArgument, error_thread_pool.init());
  DBOptions options;
  DBImpl *db = new DBImpl(options, "test_db");
  ReplayThreadPool thread_pool(5, db);
  ASSERT_EQ(Status::kNotInit, thread_pool.set_error());
  ASSERT_OK(thread_pool.init());
  ASSERT_EQ(Status::kInitTwice, thread_pool.init());
  ASSERT_EQ(5, thread_pool.get_replay_thread_count());
  for (uint64_t i = 0; i < 20; ++i) {
    MockReplayTask *task = new MockReplayTask(nullptr, i, false, 100, db, &thread_pool, nullptr);
    ASSERT_OK(thread_pool.submit_task(task));
  }
  ASSERT_EQ(0, thread_pool.get_stopped_thread_num());
  ASSERT_EQ(20, thread_pool.get_submit_task_num());
  uint64_t replayed_task_num = 0;
  ASSERT_EQ(Status::kNotSupported, thread_pool.wait_for_all_threads_stopped());
  ASSERT_OK(thread_pool.stop());
  sleep(1);
  ASSERT_EQ(5, thread_pool.get_stopped_thread_num());
  ASSERT_EQ(true, thread_pool.all_threads_stopped());
  ASSERT_OK(thread_pool.destroy(&replayed_task_num));
  ASSERT_EQ(20, replayed_task_num);
  delete db;
}

TEST_F(ReplayThreadPoolTest, test_run_thread_pool_error) {
  DBOptions options;
  DBImpl *db = new DBImpl(options, "test_db");
  ReplayThreadPool thread_pool(5, db);
  ASSERT_OK(thread_pool.init());
  for (uint64_t i = 0; i < 10; ++i) {
    MockReplayTask *task = new MockReplayTask(nullptr, i, false, 100, db, &thread_pool, nullptr);
    ASSERT_OK(thread_pool.submit_task(task));
  }
  thread_pool.set_error();
  for (uint64_t i = 0; i < 10; ++i) {
    MockReplayTask *task = new MockReplayTask(nullptr, i, false, 100, db, &thread_pool, nullptr);
    ASSERT_EQ(Status::kCorruption, thread_pool.submit_task(task));
  }

  ASSERT_EQ(10, thread_pool.get_submit_task_num());
  sleep(1);
  uint64_t replayed_task_num = 0;
  ASSERT_OK(thread_pool.stop());
  ASSERT_EQ(Status::kErrorUnexpected, thread_pool.destroy(&replayed_task_num));
  ASSERT_NE(20, replayed_task_num);
  delete db;
}

TEST_F(ReplayThreadPoolTest, test_suspend_during_recovery) {
  DBOptions options;
  DBImpl *db = new DBImpl(options, "test_db");
  ReplayThreadPool thread_pool(5, db);
  ASSERT_OK(thread_pool.init());
  ASSERT_EQ(Status::kNotSupported, thread_pool.wait_for_all_threads_suspend());
  for (uint64_t i = 0; i < 20; ++i) {
    MockReplayTask *task = new MockReplayTask(nullptr, i, false, 100, db, &thread_pool, nullptr);
    ASSERT_OK(thread_pool.submit_task(task));
    if (i == 5) {
      ASSERT_OK(thread_pool.set_during_switching_memtable());
    }
  }
  ASSERT_EQ(20, thread_pool.get_submit_task_num());
  ASSERT_NE(thread_pool.get_submit_task_num(), thread_pool.get_executed_task_num());
  ASSERT_OK(thread_pool.wait_for_all_threads_suspend());
  ASSERT_EQ(5, thread_pool.get_suspend_thread_num());
  ASSERT_EQ(0, thread_pool.get_stopped_thread_num());
  sleep(2);
  ASSERT_OK(thread_pool.finish_switching_memtable());
  sleep(1);
  ASSERT_EQ(0, thread_pool.get_suspend_thread_num());
  ASSERT_EQ(0, thread_pool.get_stopped_thread_num());
  uint64_t replayed_task_num = 0;
  ASSERT_OK(thread_pool.stop());
  ASSERT_OK(thread_pool.destroy(&replayed_task_num));
  ASSERT_EQ(20, replayed_task_num);
  ASSERT_EQ(Status::kNotInit, thread_pool.wait_for_all_threads_suspend());
  ASSERT_EQ(Status::kNotInit, thread_pool.wait_for_all_threads_stopped());
  ASSERT_OK(thread_pool.init());
  ASSERT_OK(thread_pool.stop());
  ASSERT_OK(thread_pool.set_during_switching_memtable());
  ASSERT_OK(thread_pool.wait_for_all_threads_suspend());
  ASSERT_OK(thread_pool.finish_switching_memtable());
  ASSERT_OK(thread_pool.destroy(nullptr));
  delete db;
}

TEST_F(ReplayThreadPoolTest, test_suspend_during_recovery_with_error) {
  DBOptions options;
  DBImpl *db = new DBImpl(options, "test_db");
  ReplayThreadPool thread_pool(5, db);
  ASSERT_OK(thread_pool.init());
  for (uint64_t i = 0; i < 20; ++i) {
    MockReplayTask *task = new MockReplayTask(nullptr, i, false, 100, db, &thread_pool, nullptr);
    thread_pool.submit_task(task);
    if (i == 5) {
      ASSERT_OK(thread_pool.set_during_switching_memtable());
      thread_pool.set_error();
    }
  }
  ASSERT_EQ(6, thread_pool.get_submit_task_num());
  ASSERT_EQ(Status::kCorruption, thread_pool.wait_for_all_threads_suspend());
  sleep(2);
  uint64_t replayed_task_num = 0;
  ASSERT_OK(thread_pool.stop());
  sleep(1);
  ASSERT_EQ(5, thread_pool.get_stopped_thread_num());
  ASSERT_EQ(Status::kErrorUnexpected, thread_pool.destroy(&replayed_task_num));
  ASSERT_NE(20, replayed_task_num);
  delete db;
}

/*
TEST_F(ReplayThreadPoolTest, test_task_queue_full) { // TODO
  DBOptions options;
  DBImpl *db = new DBImpl(options, "test_db");
  ReplayThreadPool thread_pool(2, db, 10);
  ASSERT_OK(thread_pool.init());
  ASSERT_OK(thread_pool.set_during_switching_memtable());
  for (uint64_t i = 0; i < 10; ++i) {
    MockReplayTask *task = new MockReplayTask(nullptr, i, false, 100, db, &thread_pool, nullptr);
    ASSERT_OK(thread_pool.submit_task(task));
  }
  MockReplayTask *new_task = new MockReplayTask(nullptr, 10, false, 100, db, &thread_pool, nullptr);
  ASSERT_EQ(Status::kOverLimit, thread_pool.submit_task(new_task));
  ASSERT_EQ(10, thread_pool.get_submit_task_num());
  ASSERT_OK(thread_pool.finish_switching_memtable());
  sleep(1);
  MockReplayTask *new_task1 = new MockReplayTask(nullptr, 10, false, 100, db, &thread_pool, nullptr);
  ASSERT_OK(thread_pool.submit_task(new_task1));
  ASSERT_EQ(11, thread_pool.get_submit_task_num());
  uint64_t replayed_task_num = 0;
  ASSERT_EQ(0, thread_pool.get_stopped_thread_num());
  ASSERT_OK(thread_pool.stop());
  ASSERT_OK(thread_pool.destroy(&replayed_task_num));
  ASSERT_EQ(11, replayed_task_num);
  delete db;
}*/

} // namespace db
} // namespace xengine

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
	xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}

