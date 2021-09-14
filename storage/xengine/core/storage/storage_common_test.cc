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
#include "storage_common.h"
#include "util/testharness.h"

using namespace xengine::common;

namespace xengine
{
namespace storage
{
TEST(StorageCommon, ExtentStats)
{
  ExtentStats total_extent_stats;
  ExtentStats extent_stats_1(100, 10, 1, 200);

  //merge
  total_extent_stats.merge(extent_stats_1);
  ASSERT_EQ(100, total_extent_stats.data_size_);
  ASSERT_EQ(10, total_extent_stats.num_entries_);
  ASSERT_EQ(1, total_extent_stats.num_deletes_);
  ASSERT_EQ(200, total_extent_stats.disk_size_);

  //operator==
  ExtentStats extent_stats_2;
  extent_stats_2 = extent_stats_1;
  ASSERT_EQ(true, extent_stats_1 == extent_stats_2);
  extent_stats_2.data_size_ = 1;
  ASSERT_EQ(false, extent_stats_1 == extent_stats_2);

  //reset
  total_extent_stats.reset();
  ASSERT_EQ(0, total_extent_stats.data_size_);
  ASSERT_EQ(0, total_extent_stats.num_entries_);
  ASSERT_EQ(0, total_extent_stats.num_deletes_);
  ASSERT_EQ(0, total_extent_stats.disk_size_);
}
}
}

int main(int argc, char **argv)
{
  std::string log_path = xengine::util::test::TmpDir() + "/storage_common_test.log";
  xengine::util::test::init_logger(log_path.c_str(), xengine::logger::DEBUG_LEVEL);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
