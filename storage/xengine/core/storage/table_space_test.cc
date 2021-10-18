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
#include <cstdio>
#include <cstdlib>
#include "util/filename.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "table_space.h"

static const std::string test_dir = xengine::util::test::TmpDir() + "/table_space_test";
namespace xengine
{
using namespace common;
using namespace util;
namespace storage
{
class TableSpaceTest : public testing::Test
{
public:
  TableSpaceTest() : env_(util::Env::Default()),
                     env_options_(),
                     table_space_(nullptr)
  {
  }
  ~TableSpaceTest()
  {
  }

  void build_table_space_args(CreateTableSpaceArgs &args)
  {
    args.table_space_id_ = 1;
    args.db_paths_.push_back(DbPath(test_dir, 0));
  }
protected:
  virtual void SetUp()
  {
    table_space_ = new TableSpace(env_, env_options_);
  }
  virtual void TearDown()
  {
    if (nullptr != table_space_) {
      delete table_space_;
      table_space_ = nullptr;
    }
  }

protected:
  util::Env *env_;
  util::EnvOptions env_options_;
  TableSpace *table_space_;
  std::atomic<int64_t> file_number_;
  std::atomic<int64_t> unique_id_;
};

TEST_F(TableSpaceTest, create)
{
  int ret = Status::kOk;
  CreateTableSpaceArgs args;

  //invalid args
  ret = table_space_->create(args);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  //success to create
  build_table_space_args(args);
  ret = table_space_->create(args);
  ASSERT_EQ(Status::kOk, ret);

  //dumplicate create
  ret = table_space_->create(args);
  ASSERT_EQ(Status::kInitTwice, ret);
}

TEST_F(TableSpaceTest, allocate_and_recycle)
{
  int ret = Status::kOk;
  CreateTableSpaceArgs args;
  ExtentIOInfo extent;
  ExtentId extent_id;

  //not init
  ret = table_space_->allocate(HOT_EXTENT_SPACE, extent);
  ASSERT_EQ(Status::kNotInit, ret);

  build_table_space_args(args);
  ret = table_space_->create(args);
  ASSERT_EQ(Status::kOk, ret);
  //the extent space not exist
  ret = table_space_->allocate(WARM_EXTENT_SPACE, extent);
  ASSERT_EQ(Status::kErrorUnexpected, ret);

  //success to allocate
  ret = Status::kOk;
  for (int64_t i = 0; Status::kOk == ret && i < 2; ++i) {
    for (int64_t j = 1; Status::kOk == ret && j < 5120; ++j) {
      ret = table_space_->allocate(HOT_EXTENT_SPACE, extent);
      ASSERT_EQ(Status::kOk, ret);
      ASSERT_EQ(i, extent.get_extent_id().file_number);
      ASSERT_EQ(j, extent.get_extent_id().offset);
    }
  }

  //success to recycle
  ret = Status::kOk;
  for (int64_t i = 0; Status::kOk == ret && i < 2; ++i) {
    for (int64_t j = 1; Status::kOk == ret && j < 5120; ++j) {
      extent_id.file_number = i;
      extent_id.offset = j;
      ret = table_space_->recycle(HOT_EXTENT_SPACE, extent_id);
      ASSERT_EQ(Status::kOk, ret);
    }
  }

  //success to allocate the recycled extent
  ret = Status::kOk;
  for (int64_t i = 0; Status::kOk == ret && i < 3; ++i) {
    for (int64_t j = 1; Status::kOk == ret && j < 5120; ++j) {
      ret = table_space_->allocate(HOT_EXTENT_SPACE, extent);
      ASSERT_EQ(Status::kOk, ret);
      ASSERT_EQ(i, extent.get_extent_id().file_number);
      ASSERT_EQ(j, extent.get_extent_id().offset);
    }
  }
}

TEST_F(TableSpaceTest, recycle)
{
  int ret = Status::kOk;
  CreateTableSpaceArgs args;
  ExtentId extent_id;

  //not init
  ret = table_space_->recycle(HOT_EXTENT_SPACE, extent_id);
  ASSERT_EQ(Status::kNotInit, ret);
  
  build_table_space_args(args);
  ret = table_space_->create(args);
  ASSERT_EQ(Status::kOk, ret);
  //the extent space not exist
  ret = table_space_->recycle(WARM_EXTENT_SPACE, extent_id);
  ASSERT_EQ(Status::kErrorUnexpected, ret);


}

} //namespace storage
} //namespace xengine


int main(int argc, char **argv)
{
  xengine::util::test::remove_dir(test_dir.c_str());
  xengine::util::Env::Default()->CreateDir(test_dir);
  std::string log_path = xengine::util::test::TmpDir() + "/table_space_test.log";
  xengine::logger::Logger::get_log().init(log_path.c_str(), xengine::logger::DEBUG_LEVEL);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
