/*
   Copyright (c) 2000, 2016, Alibaba and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

   Author: dongsheng.zds@alibaba-inc.com
*/

// Aone: https://work.aone.alibaba-inc.com/issue/32848059

#include "db/db_impl.h"
#include "db/db_test_util.h"
#include "env/mock_env.h"
#include "port/port.h"
#include "util/sync_point.h"

using namespace xengine;
using namespace common;
using namespace util;

namespace xengine {
namespace db {
class AllocateExtentFailedTest : public DBTestBase {
 public:
  AllocateExtentFailedTest() : DBTestBase("AllocateExtentFailedTest") {}
};

TEST_F(AllocateExtentFailedTest, recycle_io_info_hang) {
  int ret = Status::kOk;
  Options options;
  options.create_if_missing = true;
  options.env = env_;
  options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  options.parallel_wal_recovery = false;

  CreateAndReopenWithCF({"yuanfeng", "summer"}, options);
  /**Insert data first round*/
  for (int i = 0; i < 20; i += 2) {
    std::string key = "zds" + std::to_string(i);
    std::string value = "ppl" + std::to_string(i);
    ASSERT_OK(Put(1, key, value));
  }

  /**flush data to disk, generate extent1*/
  Flush(1);

  /**Insert data second round, overlap with first round*/
  for (int i = 1; i < 20; i += 2) {
    std::string key = "zds" + std::to_string(i);
    std::string value = "ppl" + std::to_string(i);
    ASSERT_OK(Put(1, key, value));
  }

  /**flush data to disk, generate extent2 which overlap with extent1*/
  Flush(1);

  db::SuperVersion *sv =
      reinterpret_cast<DBImpl *>(db_)->GetAndRefSuperVersion(1);

  /**inject recycle io info hang*/
  SyncPoint::GetInstance()->SetCallBack(
      "ExtentSpaceManager::recycle::inject_recycle_io_info_hang",
      [&](void *arg) { sleep(10); });
  SyncPoint::GetInstance()->SetCallBack(
      "StorageManager::TEST_inject_async_recycle_hang",
      [&](void *arg) { sleep(0); });
  SyncPoint::GetInstance()->EnableProcessing();

  /**compact all data to level2, and generate extent3, extent1 and extent2 can
   * been recycle, but will  hang at recycle io info*/
  ret = CompactRange(1, TaskType::MANUAL_FULL_AMOUNT_TASK).code();
  ASSERT_EQ(Status::kOk, ret);

  /**sleep wait compaction finish*/
  sleep(2);

  /**insert data, and switch memtable, but not wait flush finish. allocate
   * extent will hang until extent1 been recycled from table space*/
  SyncPoint::GetInstance()->SetCallBack(
      "ExtentSpaceManager::allocate::inject_allocate_hang",
      [&](void *arg) { sleep(3); });
  /**Insert data third round, which not overlap with extent1 and extent2*/
  for (int i = 20; i < 30; ++i) {
    std::string key = "zds" + std::to_string(i);
    std::string value = "ppl" + std::to_string(i);
    ASSERT_OK(Put(1, key, value));
  }

  /**flush, which will allocate extent1 failed*/
  Flush(1, false /*wait*/);

  /**release meta snapshot, recycle extent1 and extent2*/
  reinterpret_cast<DBImpl *>(db_)->ReturnAndCleanupSuperVersion(1, sv);

  /**wait for background flush*/
  sleep(10);
}

}  // namespace db
}  // namespace xengine

int main(int argc, char **argv) {
  port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
