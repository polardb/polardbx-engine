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
#include "extent_space.h"

static const std::string test_dir = xengine::util::test::TmpDir() + "/extent_space_test";
namespace xengine
{
using namespace common;
using namespace util;
namespace storage
{
class ExtentSpaceTest : public testing::Test
{
public:
  ExtentSpaceTest() : env_(util::Env::Default()),
                      env_options_(),
                      extent_space_(nullptr)
  {
  }
  ~ExtentSpaceTest()
  {
  }

  void build_extent_space_args(CreateExtentSpaceArgs &args)
  {
    args.table_space_id_ = 1;
    args.extent_space_type_ = HOT_EXTENT_SPACE;
    args.db_path_.path = test_dir; 
  }

protected:
  virtual void SetUp()
  {
    extent_space_ = new ExtentSpace(env_, env_options_);
  }
  virtual void TearDown()
  {
    if (nullptr != extent_space_) {
      delete extent_space_;
      extent_space_ = nullptr;
    }
  }

protected:
  util::Env *env_;
  util::EnvOptions env_options_;
  ExtentSpace *extent_space_;
  CreateExtentSpaceArgs args_;
};

TEST_F(ExtentSpaceTest, create)
{
  int ret = Status::kOk;
  CreateExtentSpaceArgs args;

  //invalid args
  args.table_space_id_ = -1;
  ret = extent_space_->create(args);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  //success to create
  build_extent_space_args(args);
  ret = extent_space_->create(args);
  ASSERT_EQ(Status::kOk, ret);

  //dumplicate create
  ret = extent_space_->create(args);
  ASSERT_EQ(Status::kInitTwice, ret);
}

TEST_F(ExtentSpaceTest, allocate_and_recycle)
{
  int ret = Status::kOk;
  CreateExtentSpaceArgs args;
  ExtentIOInfo io_info;
  ExtentId extent_id;

  //not init
  ret = extent_space_->allocate(io_info);
  ASSERT_EQ(Status::kNotInit, ret);

  //success to allocate
  build_extent_space_args(args);
  ret = extent_space_->create(args);
  ASSERT_EQ(Status::kOk, ret);
  for (int64_t i = 1; Status::kOk == ret && i < 5120; ++i) {
    ret = extent_space_->allocate(io_info);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_EQ(0, io_info.get_extent_id().file_number);
    ASSERT_EQ(i, io_info.get_extent_id().offset);
  }

  for (int64_t i = 1; Status::kOk == ret && i < 5120; ++i) {
    ret = extent_space_->allocate(io_info);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_EQ(1, io_info.get_extent_id().file_number);
    ASSERT_EQ(i, io_info.get_extent_id().offset);
  }

  //recycle extent, which datafile not exist
  extent_id.file_number = 2;
  extent_id.offset = 1;
  ret = extent_space_->recycle(extent_id);
  ASSERT_EQ(Status::kErrorUnexpected, ret);

  //recycle extent, which offset is exceed the datafile
  extent_id.file_number = 1;
  extent_id.offset = 5122;
  ret = extent_space_->recycle(extent_id);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  //success to recycle extent
  extent_id.file_number = 1;
  extent_id.offset = 1;
  ret = extent_space_->recycle(extent_id);
  ASSERT_EQ(Status::kOk, ret);
  ExtentIOInfo new_io_info;
  ret = extent_space_->allocate(new_io_info);
  ASSERT_EQ(1, new_io_info.get_extent_id().file_number);
  ASSERT_EQ(1, new_io_info.get_extent_id().offset);

  for (int32_t i = 1; Status::kOk == ret && i <= 256; ++i) {
    extent_id.file_number = 0;
    extent_id.offset = i;
    ret = extent_space_->recycle(extent_id);
    ASSERT_EQ(Status::kOk, ret);
  }

  for (int32_t i = 1; Status::kOk == ret && i <= 256; ++i) {
    extent_id.file_number = 1;
    extent_id.offset = i;
    ret = extent_space_->recycle(extent_id);
    ASSERT_EQ(Status::kOk, ret);
  }

  for (int32_t i = 1; Status::kOk == ret && i <=256; ++i) {
    ret = extent_space_->allocate(io_info);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_EQ(0, io_info.get_extent_id().file_number);
    ASSERT_EQ(i, io_info.get_extent_id().offset);
  }

  for (int32_t i = 1; Status::kOk == ret && i <=256; ++i) {
    ret = extent_space_->allocate(io_info);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_EQ(1, io_info.get_extent_id().file_number);
    ASSERT_EQ(i, io_info.get_extent_id().offset);
  }

  for (int32_t i = 1; Status::kOk == ret && i <=512; ++i) {
    ret = extent_space_->allocate(io_info);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_EQ(2, io_info.get_extent_id().file_number);
    ASSERT_EQ(i, io_info.get_extent_id().offset);
  }
}

TEST_F(ExtentSpaceTest, recycle)
{
  int ret = Status::kOk;
  CreateExtentSpaceArgs args;
  ExtentId extent_id;

  //not init
  ret = extent_space_->recycle(extent_id);
  ASSERT_EQ(Status::kNotInit, ret);
}
} //namespace storage
} //namespace xengine

int main(int argc, char **argv)
{
  xengine::util::test::remove_dir(test_dir.c_str());
  xengine::util::Env::Default()->CreateDir(test_dir);
  std::string log_path = xengine::util::test::TmpDir() + "/extent_space_test.log";
  xengine::logger::Logger::get_log().init(log_path.c_str(), xengine::logger::DEBUG_LEVEL);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
