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

//Aone: https://work.aone.alibaba-inc.com/issue/33610218

#include "db/db_test_util.h"
#include "env/mock_env.h"
#include "port/port.h"
#include "util/sync_point.h"


using namespace xengine;
using namespace common;
using namespace util;

namespace xengine
{
namespace db
{
class ShrinkJobTest : public DBTestBase {
public:
  ShrinkJobTest() : DBTestBase("shrink_job_test")
  {
  }
};

TEST_F(ShrinkJobTest, shrink_failed)
{
  Options options;
  options.create_if_missing = true;
  options.env = env_;
  options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  options.parallel_wal_recovery = false;
  options.shrink_allocate_interval = 0;
  options.max_free_extent_percent = 1;
  options.total_max_shrink_extent_count = 512;
  options.max_shrink_extent_count = 512;

  CreateAndReopenWithCF({"yuanfeng", "pinglan"}, options);
  /**Insert data*/
  /**generate one extent, [0, 1]*/
  for (int i = 0; i < 20; ++i) {
    std::string key = "zds" + std::to_string(i);
    std::string value = "ppl" + std::to_string(i);
    ASSERT_OK(Put(1, key, value));
  }
  Flush(1);

  /**genearate another extent, [0, 2]*/
  for (int i = 21; i < 40; ++i) {
    std::string key = "zds" + std::to_string(i);
    std::string value = "ppl" + std::to_string(i);
    ASSERT_OK(Put(1, key, value));
  }
  Flush(1);

  /**genearate another extent, [0, 3]*/
  for (int i = 41; i < 60; ++i) {
    std::string key = "zds" + std::to_string(i);
    std::string value = "ppl" + std::to_string(i);
    ASSERT_OK(Put(1, key, value));
  }
  Flush(1);

  /**Intro level0 compaction, merge [0, 1],[0, 2],[0,3] to [0, 4]*/
  CompactRange(1, INTRA_COMPACTION_TASK);
  sleep(5); //wait async compaction end

  /**inject schedule shrink extent space hang*/
  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::shrink_extent_spaces_schedule_hang",
      [&](void *arg) { sleep(5); });
  SyncPoint::GetInstance()->EnableProcessing();
  /**schedule shrink job*/
  schedule_shrink();

  /**Insert data, occupy the free extent*/
  for (int i = 41; i < 60; ++i) {
    std::string key = "zds" + std::to_string(i);
    std::string value = "ppl" + std::to_string(i);
    ASSERT_OK(Put(1, key, value));
  }
  Flush(1);

  sleep(10); //wait aync shrink

  std::vector<storage::DataFileStatistics> data_file_stats;
  test_get_data_file_stats(1, data_file_stats);
  ASSERT_EQ(1, data_file_stats.size());
  ASSERT_EQ(5, data_file_stats[0].total_extent_count_);
  ASSERT_EQ(2, data_file_stats[0].free_extent_count_);

  /**retry to schedule shrink, expect success to shrink*/
  SyncPoint::GetInstance()->DisableProcessing();
  data_file_stats.clear();
  schedule_shrink();
  sleep(10); //wait async shrink
  test_get_data_file_stats(1, data_file_stats);
  ASSERT_EQ(1, data_file_stats.size());
  ASSERT_EQ(3, data_file_stats[0].total_extent_count_);
  ASSERT_EQ(0, data_file_stats[0].free_extent_count_);
}

TEST_F(ShrinkJobTest, shrink_success)
{
  Options options;
  options.create_if_missing = true;
  options.env = env_;
  options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  options.parallel_wal_recovery = false;
  options.shrink_allocate_interval = 0;
  options.max_free_extent_percent = 1;

  CreateAndReopenWithCF({"yuanfeng", "pinglan"}, options);
  /**Insert data, generate one extent, [1, 1]*/
  for (int i = 0; i < 20; ++i) {
    std::string key = "zds" + std::to_string(i);
    std::string value = "ppl" + std::to_string(i);
    ASSERT_OK(Put(1, key, value));
  }
  Flush(1);

  /**Insert data, generate another extent, [1, 2]*/
  for (int i = 21; i < 40; ++i) {
    std::string key = "zds" + std::to_string(i);
    std::string value = "ppl" + std::to_string(i);
    ASSERT_OK(Put(1, key, value));
  }
  Flush(1);

  /**Insert data, generate another extent, [1, 3]*/
  for (int i = 41; i < 60; ++i) {
    std::string key = "zds" + std::to_string(i);
    std::string value = "ppl" + std::to_string(i);
    ASSERT_OK(Put(1, key, value));
  }
  Flush(1);

  /**Intro level0 compaction, merge [1, 1],[1, 2],[1, 3] to [1, 4]*/
  CompactRange(1, INTRA_COMPACTION_TASK);
  sleep(5); //wait async compaction end

  std::vector<storage::DataFileStatistics> data_file_stats;
  test_get_data_file_stats(1, data_file_stats);
  ASSERT_EQ(1, data_file_stats.size());
  ASSERT_EQ(5, data_file_stats[0].total_extent_count_);
  ASSERT_EQ(3, data_file_stats[0].free_extent_count_);

  /**schedule shrink*/
  schedule_shrink();
  sleep(5);

  /**check shrink result*/
  data_file_stats.clear();
  test_get_data_file_stats(1, data_file_stats);
  ASSERT_EQ(1, data_file_stats.size());
  ASSERT_EQ(2, data_file_stats[0].total_extent_count_);
  ASSERT_EQ(0, data_file_stats[0].free_extent_count_);
}

} // namespace 
} // namespace xengine

int main(int argc, char **argv)
{
  port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
