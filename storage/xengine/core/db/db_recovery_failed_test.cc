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
//fix bug#31726304

#include "db/db_impl.h"
#include "db/db_test_util.h"
#include "env/mock_env.h"
#include "port/port.h"
#include "storage/data_file.h"
#include "util/sync_point.h"
#include "table/extent_table_builder.h"
#include "xengine/utilities/transaction.h"
#include "xengine/utilities/transaction_db.h"

using namespace xengine;
using namespace common;
using namespace util;
using namespace storage;
using namespace table;

#define TEST_CASE_NAME "db_recovery_failed_test"
namespace xengine
{
namespace db
{
class DbRecoveryFailedTest : public DBTestBase {
public:
  DbRecoveryFailedTest() : DBTestBase(TEST_CASE_NAME), db_name_(), options_()
  {
  }

  void SetUp() override
  {
    db_name_ = test::TmpDir() + TEST_CASE_NAME;
    options_.create_if_missing = true;
    options_.env = env_;
    options_.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
    options_.parallel_wal_recovery = false;
  }

  void TearDown() override
  {
    DestroyAndReopen(options_);
  }

protected:
  void insert_data(int64_t index_id)
  {
    for (int i = 0; i < 20; ++i) {
      std::string key = std::to_string(i);
      std::string value = std::to_string(index_id) + std::to_string(i);
      ASSERT_OK(Put(index_id, key, value));
    }
  }

  void check_data(int64_t index_id)
  {
    for (int i = 0; i < 20; ++i) {
      std::string key = std::to_string(i);
      std::string value = std::to_string(index_id) + std::to_string(i);
      ASSERT_EQ(value, Get(index_id, key));
    }
  }

protected:
  std::string db_name_;
  Options options_;
};

TEST_F(DbRecoveryFailedTest, fallocate_failed)
{
  CreateAndReopenWithCF({"zds", "ppl", "summer"}, options_);
  //insert and check data in zds
  insert_data(1);
  check_data(1);

  //flush data in zds normal
  Flush(1);
  check_data(1);

  //insert and check data in ppl
  insert_data(2);
  check_data(2);

  //insert and check data in summer
  insert_data(3);
  check_data(3);

  //inject fallocate error
  SyncPoint::GetInstance()->SetCallBack(
      "DataFile::create::inject_fallocated_failed", [&](void *arg) {
      DataFile *data_file = reinterpret_cast<DataFile *>(arg);
      data_file->TEST_inject_fallocate_failed();
      });

  //ignore flush data
  SyncPoint::GetInstance()->SetCallBack(
      "ExtentTableBuilder::ignore_flush_data", [&](void *arg) {
      table::ExtentBasedTableBuilder *builder = reinterpret_cast<table::ExtentBasedTableBuilder*>(arg);
      builder->TEST_inject_ignore_flush_data();
      });
  SyncPoint::GetInstance()->EnableProcessing();

  //flush data in summer
  Flush(3);

  //recovery
  ReopenWithColumnFamilies({"default", "zds", "ppl", "summer"}, options_);

  //check data
  check_data(1);
  check_data(2);
}

TEST_F(DbRecoveryFailedTest, double_write_header_failed)
{
  SyncPoint::GetInstance()->DisableProcessing();

  CreateAndReopenWithCF({"zds", "ppl", "summer"}, options_);
  //insert and check data in zds
  insert_data(1);
  check_data(1);

  //flush data in zds normal
  Flush(1);
  check_data(1);

  //insert and check data in ppl
  insert_data(2);
  check_data(2);

  //insert and check data in summer
  insert_data(3);
  check_data(3);

  //inject double write header failed
  SyncPoint::GetInstance()->SetCallBack(
      "DataFile::create::inject_double_write_header_failed", [&](void *arg) {
      DataFile *data_file = reinterpret_cast<DataFile *>(arg);
      data_file->TEST_inject_double_write_header_failed();
      });

  //ignore flush data
  SyncPoint::GetInstance()->SetCallBack(
      "ExtentTableBuilder::ignore_flush_data", [&](void *arg) {
      table::ExtentBasedTableBuilder *builder = reinterpret_cast<table::ExtentBasedTableBuilder*>(arg);
      builder->TEST_inject_ignore_flush_data();
      });
  SyncPoint::GetInstance()->EnableProcessing();

  //flush data in summer
  Flush(3);

  //recovery
  ReopenWithColumnFamilies({"default", "zds", "ppl", "summer"}, options_);

  //check data
  check_data(1);
  check_data(2);
}

} // namespace db
} // namespace xengine

int main(int argc, char **argv)
{
  port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
