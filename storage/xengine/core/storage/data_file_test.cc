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
#include "data_file.h"

static const std::string test_dir = xengine::util::test::TmpDir() + "/data_file_test";
namespace xengine
{
using namespace common;
using namespace util;
namespace storage
{
class DataFileTest : public testing::Test
{
public:
  DataFileTest() : env_(util::Env::Default()),
                   env_options_(),
                   data_file_(nullptr),
                   tmp_data_file_(nullptr)
  {
  }

  ~DataFileTest()
  {
  }

  void build_data_file_args(int64_t table_space_id, int64_t file_number, CreateDataFileArgs &args)
  {
    args.table_space_id_ = table_space_id;
    args.extent_space_type_ = HOT_EXTENT_SPACE;
    args.data_file_path_ = util::MakeTableFileName(test_dir, file_number);
    args.file_number_ = file_number;
  }

protected:
  virtual void SetUp()
  {
    data_file_ = new DataFile(env_, env_options_);
    tmp_data_file_ = new DataFile(env_, env_options_);
  }
  virtual void TearDown()
  {
    if (nullptr != data_file_) {
      delete data_file_;
      data_file_ = nullptr;
    }
    if (nullptr != tmp_data_file_) {
      delete tmp_data_file_;
      tmp_data_file_ = nullptr;
    }
  }
protected:
  util::Env *env_;
  util::EnvOptions env_options_;
  std::atomic<int64_t> unique_id_;
  DataFile *data_file_;
  DataFile *tmp_data_file_;
};

TEST_F(DataFileTest, create)
{
  int ret = Status::kOk;
  CreateDataFileArgs args;

  //invalid args
  args.table_space_id_ = -1;
  ret = data_file_->create(args);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  //success to create
  build_data_file_args(1, 1, args);
  ret = data_file_->create(args);
  ASSERT_EQ(Status::kOk, ret);
  //dumplicate to create
  ret = data_file_->create(args);
  ASSERT_EQ(Status::kInitTwice, ret);
  //file has exist
  ret = tmp_data_file_->create(args);
  ASSERT_EQ(Status::kErrorUnexpected, ret);
}

TEST_F(DataFileTest, allocate)
{
  int ret = Status::kOk;
  ExtentIOInfo io_info;

  //not init
  ret = data_file_->allocate(io_info);
  ASSERT_EQ(Status::kNotInit, ret);

  //test normal allocate
  CreateDataFileArgs args;
  const int64_t file_number = 2;
  build_data_file_args(1, file_number, args);
  ret = data_file_->create(args);
  ASSERT_EQ(Status::kOk, ret);
  for (int32_t i = 1; Status::kOk == ret && i < 512; ++i) {
    ret = data_file_->allocate(io_info);
    ASSERT_EQ(file_number, io_info.extent_id_.file_number);
    ASSERT_EQ(i, io_info.extent_id_.offset);
  }

  //test exceed datafile size
  for (int32_t i = 512; Status::kOk == ret && i < 5120; ++i) {
    ret = data_file_->allocate(io_info);
    ASSERT_EQ(Status::kOk, ret);
    ASSERT_EQ(file_number, io_info.get_extent_id().file_number);
    ASSERT_EQ(i, io_info.get_extent_id().offset);
  }
  ret = data_file_->allocate(io_info);
  ASSERT_EQ(Status::kNoSpace, ret);
}

TEST_F(DataFileTest, recycle)
{
  int ret = Status::kOk;
  ExtentId extent_id(0, 0);
  ExtentIOInfo io_info;

  //test not init
  ret = data_file_->recycle(extent_id);
  ASSERT_EQ(Status::kNotInit, ret);
  
  //invalid extent id
  CreateDataFileArgs args;
  const int64_t file_number = 3;
  build_data_file_args(1, file_number, args);
  ret = data_file_->create(args);
  ASSERT_EQ(Status::kOk, ret);
  for (int32_t i = 1; Status::kOk == ret && i < 512; ++i) {
    ret = data_file_->allocate(io_info);
    ASSERT_EQ(file_number, io_info.extent_id_.file_number);
    ASSERT_EQ(i, io_info.extent_id_.offset);
  }
  extent_id.file_number = file_number + 1;
  extent_id.offset = 1;
  ret = data_file_->recycle(extent_id);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  extent_id.file_number = file_number;
  extent_id.offset = 512 + 64;
  ret = data_file_->recycle(extent_id);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  //test the allocate order, from low offset to high offset
  for (int32_t i = 511; Status::kOk == ret && i >= 1; --i) {
    extent_id.file_number = file_number;
    extent_id.offset = i;
    ret = data_file_->recycle(extent_id);
    ASSERT_EQ(Status::kOk, ret);
  }


  for (int32_t i = 1; Status::kOk == ret && i < 512; ++i) {
    ret = data_file_->allocate(io_info);
    ASSERT_EQ(file_number, io_info.extent_id_.file_number);
    ASSERT_EQ(i, io_info.extent_id_.offset);
  }

  //test double recycle
  extent_id.file_number = file_number;
  extent_id.offset = 1;
  ret = data_file_->recycle(extent_id);
  ASSERT_EQ(Status::kOk, ret);
  ret = data_file_->recycle(extent_id);
  ASSERT_EQ(Status::kErrorUnexpected, ret);
}

TEST_F(DataFileTest, open)
{
  int ret = Status::kOk;

  ExtentId extent_id;
  ExtentIOInfo io_info;
  CreateDataFileArgs args;
  const int64_t table_space_id = 1;
  const int64_t file_number = 4;
  build_data_file_args(table_space_id, file_number, args);
  ret = data_file_->create(args);
  ASSERT_EQ(Status::kOk, ret);
  ret = data_file_->allocate(io_info);
  ASSERT_EQ(Status::kOk, ret);
  int64_t total_extent_count = data_file_->get_total_extent_count();
  
  DataFile open_data_file(env_, env_options_);
  ret = open_data_file.open(args.data_file_path_);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(table_space_id, open_data_file.get_table_space_id());
  ASSERT_EQ(file_number, open_data_file.get_file_number());
  ASSERT_EQ(total_extent_count, open_data_file.get_total_extent_count());

}

} //namespace storage
} //namespace xengine

int main(int argc, char **argv)
{
  xengine::util::test::remove_dir(test_dir.c_str());
  xengine::util::Env::Default()->CreateDir(test_dir);
  std::string log_path = xengine::util::test::TmpDir() + "/data_file_test.log";
  xengine::logger::Logger::get_log().init(log_path.c_str(), xengine::logger::DEBUG_LEVEL);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
