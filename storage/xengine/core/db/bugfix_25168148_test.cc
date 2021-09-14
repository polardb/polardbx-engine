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

#include "db/db_impl.h"
#include "db/replay_task.h"
#include "db/replay_thread_pool.h"
#include "db/db_test_util.h"
#include "env/mock_env.h"
#include "port/port.h"
#include "util/sync_point.h"
#include "xengine/utilities/transaction.h"
#include "xengine/utilities/transaction_db.h"

using namespace xengine;
using namespace common;
using namespace util;

namespace xengine
{
namespace db
{
class WriteCheckpointFailedTest : public DBTestBase {
public:
  WriteCheckpointFailedTest() : DBTestBase("bugfix_25168148_test")
  {
  }
};

TEST_F(WriteCheckpointFailedTest, version_set_write_checkpoint_failed)
{
  Options options;
  options.create_if_missing = true;
  options.env = env_;
  options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  options.parallel_wal_recovery = false;
  
  CreateAndReopenWithCF({"yuanfeng"}, options);
  /**Insert data*/
  for (int i = 0; i < 20; ++i) {
    std::string key = "zds" + std::to_string(i);
    std::string value = "ppl" + std::to_string(i);
    ASSERT_OK(Put(1, key, value));
  }

  /**check insert*/
  for (int i = 0; i < 20; ++i) {
    std::string key = "zds" + std::to_string(i);
    std::string value = "ppl" + std::to_string(i);
    ASSERT_EQ(value, Get(1, key));
  }

  /**flush data to disk*/
  Flush(1);

  /**recovery and check data*/
  ReopenWithColumnFamilies({"default", "yuanfeng"}, options);
  for (int i = 0; i < 20; ++i) {
    std::string key = "zds" + std::to_string(i);
    std::string value = "ppl" + std::to_string(i);
    ASSERT_EQ(value, Get(1, key));
  }

  /**write checkpoint*/
  ASSERT_EQ(Status::kOk, write_checkpoint());

  /**recovery and check data*/
  ReopenWithColumnFamilies({"default", "yuanfeng"}, options);
  for (int i = 0; i < 20; ++i) {
    std::string key = "zds" + std::to_string(i);
    std::string value = "ppl" + std::to_string(i);
    ASSERT_EQ(value, Get(1, key));
  }

  /**Insert another data, Flush, and write checkpoint*/
  for (int i = 0; i < 20; ++i) {
    std::string key = "ppl" + std::to_string(i);
    std::string value = "zds" + std::to_string(i);
    ASSERT_OK(Put(1, key, value));
  }
  Flush(1);
  /**inject write checkpoint error*/
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::do_checkpoint::inject_error", [&](void *arg) {
      dbfull()->TEST_inject_version_set_write_checkpoint();
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_EQ(Status::kErrorUnexpected, write_checkpoint());
  /**Insert data and flush*/
  for (int i = 0; i < 20; ++i) {
    std::string key = "zdsppl" + std::to_string(i);
    std::string value = "zdsppl" + std::to_string(i);
    ASSERT_OK(Put(1, key, value));
  }
  Flush(1);

  /**recovery and check*/
  ReopenWithColumnFamilies({"default", "yuanfeng"}, options);
  for (int i = 0; i < 20; ++i) {
    std::string key = "zds" + std::to_string(i);
    std::string value = "ppl" + std::to_string(i);
    ASSERT_EQ(value, Get(1, key));
  }
  for (int i = 0; i < 20; ++i) {
    std::string key = "ppl" + std::to_string(i);
    std::string value = "zds" + std::to_string(i);
    ASSERT_EQ(value, Get(1, key));
  }
  for (int i = 0; i < 20; ++i) {
    std::string key = "zdsppl" + std::to_string(i);
    std::string value = "zdsppl" + std::to_string(i);
    ASSERT_EQ(value, Get(1, key));
  }

  /**success write checkpoint, and recovery check data*/
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(Status::kOk, write_checkpoint());
  ReopenWithColumnFamilies({"default", "yuanfeng"}, options);
  for (int i = 0; i < 20; ++i) {
    std::string key = "zds" + std::to_string(i);
    std::string value = "ppl" + std::to_string(i);
    ASSERT_EQ(value, Get(1, key));
  }
  for (int i = 0; i < 20; ++i) {
    std::string key = "ppl" + std::to_string(i);
    std::string value = "zds" + std::to_string(i);
    ASSERT_EQ(value, Get(1, key));
  }
  for (int i = 0; i < 20; ++i) {
    std::string key = "zdsppl" + std::to_string(i);
    std::string value = "zdsppl" + std::to_string(i);
    ASSERT_EQ(value, Get(1, key));
  }
}

} //namespace db
} //namespace xengine

int main(int argc, char **argv)
{
  port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
