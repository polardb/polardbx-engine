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

#include "storage/extent_space_manager.h"
#include "db/version_set.h"
#include "storage/io_extent.h"
#include "util/sync_point.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "xengine/cache.h"
#include "xengine/db.h"
#include "xengine/options.h"
#include "xengine/write_buffer_manager.h"

using namespace xengine;
using namespace common;
using namespace util;
using namespace cache;
using namespace db;

namespace xengine {
namespace storage {

class ExtentSpaceManagerTest : public testing::Test {
 public:
  ExtentSpaceManagerTest()
      : env_(Env::Default()),
        dbname_("./unittest_tmp/"),
        table_cache_(NewLRUCache(50000, 16)),
        write_buffer_manager_(db_options_.db_write_buffer_size),
        next_file_number_(2) {
    options.db_paths.emplace_back(dbname_, 0);
    spacemanager = new ExtentSpaceManager(options, next_file_number_);
    DestroyDB(dbname_, Options(options, cf_options));
    assert(mkdir(dbname_.c_str(), 0755) == 0);
    assert(spacemanager->allocate(write_extent) == Status::OK());
  };

  ~ExtentSpaceManagerTest() {
    // clean up
    DestroyDB(dbname_, Options(options, cf_options));
  }

  virtual void TestBody() {};

  Env *env_;
  std::string dbname_;
  EnvOptions env_options_;
  ImmutableDBOptions db_options_;
  MutableCFOptions mutable_cf_options_;
  std::shared_ptr<Cache> table_cache_;
  WriteController write_controller_;
  WriteBufferManager write_buffer_manager_;
  std::unique_ptr<VersionSet> versions_;
  DBOptions options;
  ColumnFamilyOptions cf_options;

  ExtentSpaceManager *spacemanager;
  WritableExtent write_extent;
  RandomAccessExtent read_extent;
  FileNumber next_file_number_;
};

class ExtentSpaceTest : public testing::Test {
 public:
  ExtentSpaceTest() : managertest() {
    space = managertest.spacemanager->free_space_map_.begin()->second.get();
  }
  static void alloc(void *v) {
    ExtentSpace *space = static_cast<ExtentSpace *>(v);
    ExtentId extent_id;
    space->allocate(extent_id);
  }
  struct ARG {
    ExtentSpace *space;
    int32_t offset;
    ARG(ExtentSpace *s, int32_t e) : space(s), offset(e) {}
  };
  static void free(void *v) {
    ARG *arg = static_cast<ARG *>(v);
    ExtentId extent_id = ExtentId(2, arg->offset);
    arg->space->free(extent_id);
  }

  void test_private() {
    bool value;
    space->get_bit_value(0, value);
    ASSERT_TRUE(value);
    space->get_bit_value(2, value);
    ASSERT_FALSE(value);
    space->set_bit_value(2, true);
    space->get_bit_value(2, value);
    ASSERT_TRUE(value);
  }

  void set_private_before_serialize() {
    space->header_.header_size_ = 2017;
    space->header_.format_version_ = 2017;
    space->header_.start_extent_ = 2017;
    space->header_.extent_size_ = 2017;
    space->header_.data_block_size_ = 2017;
    space->header_.file_number_ = 2017;
    space->header_.used_extent_number_ = 2017;
    space->header_.total_extent_number_ = 2017;
    space->header_.create_timestamp_ = 2017;
    space->header_.modified_timestamp_ = 2017;
    strcpy(space->header_.filename_, "header_test.sst");
    space->header_.magic_number_ = 2017;
    for (int i = 2; i < 100; ++i) {
      space->extents_bit_map_.set(i);
    }
  }

  void set_private_after_serialize() {
    space->header_.header_size_ = 0;
    space->header_.format_version_ = 0;
    space->header_.start_extent_ = 0;
    space->header_.extent_size_ = 0;
    space->header_.data_block_size_ = 0;
    space->header_.file_number_ = 0;
    space->header_.used_extent_number_ = 0;
    space->header_.total_extent_number_ = 0;
    space->header_.create_timestamp_ = 0;
    space->header_.modified_timestamp_ = 0;
    memset(space->header_.filename_, 0, MAX_FILE_PATH_SIZE);
    space->header_.magic_number_ = 0;
    space->extents_bit_map_.reset();
  }

  ExtentSpaceManagerTest managertest;
  ExtentSpace *space;
};

TEST_F(ExtentSpaceTest, Header) {
  set_private_before_serialize();
  ASSERT_EQ(space->sync(), Status::OK());
  set_private_after_serialize();
  ASSERT_EQ(space->load(false), Status::OK());
  ASSERT_EQ(space->get_space_header().header_size_, 2017);
  ASSERT_EQ(space->get_space_header().format_version_, 2017);
  ASSERT_EQ(space->get_space_header().start_extent_, 2017);
  ASSERT_EQ(space->get_space_header().extent_size_, 2017);
  ASSERT_EQ(space->get_space_header().data_block_size_, 2017);
  ASSERT_EQ(space->get_space_header().file_number_, 2017);
  ASSERT_EQ(space->get_space_header().used_extent_number_, 2017);
  ASSERT_EQ(space->get_space_header().total_extent_number_, 2017);
  ASSERT_EQ(space->get_space_header().create_timestamp_, 2017);
  ASSERT_FALSE(strcmp(space->get_space_header().filename_, "header_test.sst"));
  ASSERT_EQ(space->get_space_header().magic_number_, 2017);
  ASSERT_EQ(space->get_used_number(), 100);

  // clean up
  std::string fname = MakeTableFileName("./unittest_tmp/", 3);
  ASSERT_EQ(unlink(fname.c_str()), 0);
}

TEST_F(ExtentSpaceTest, OneThread) {
  ASSERT_EQ(space->get_used_number(), 2);
  ASSERT_EQ(space->get_available_number(), MAX_EXTENT_NUM - 2);
  ExtentId extentid = ExtentId(2, 0);
  ASSERT_EQ(
      space->free(extentid).ToString(),
      "IO error: ./unittest_tmp//000002.sst: space header can't be freed");
  extentid = ExtentId(2, MAX_EXTENT_NUM);
  ASSERT_EQ(
      space->free(extentid).ToString(),
      "IO error: ./unittest_tmp//000002.sst: Given extent id is out of range");
  extentid = ExtentId(2, 2);
  ASSERT_EQ(space->free(extentid).ToString(),
            "IO error: ./unittest_tmp//000002.sst: Extent is already freed");
  extentid = ExtentId(2, -1);
  ASSERT_FALSE(space->is_extent_free(extentid));
  extentid = ExtentId(2, MAX_EXTENT_NUM);
  ASSERT_FALSE(space->is_extent_free(extentid));

  // clean up
  Env::Default()->SleepForMicroseconds(50000);
  std::string fname =
      MakeTableFileName("./unittest_tmp/", 3);  // create by asynchrous thread
  ASSERT_EQ(unlink(fname.c_str()), 0);
  test_private();
}

TEST_F(ExtentSpaceTest, reference) {
  ASSERT_EQ(space->reference(0).ToString(),
            "IO error: ./unittest_tmp//000002.sst: space header can't be used");
  ASSERT_EQ(
      space->reference(MAX_EXTENT_NUM).ToString(),
      "IO error: ./unittest_tmp//000002.sst: Given extent id is out of range");
  ASSERT_EQ(
      space->reference(1).ToString(),
      "IO error: ./unittest_tmp//000002.sst: Extent is already referenced");
  Status s = space->reference(2);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(space->get_used_number(), 3);
}

TEST_F(ExtentSpaceTest, RunMany) {
  for (int32_t i = 0; i < MAX_EXTENT_NUM - 2; i++) {
    managertest.env_->Schedule(&alloc, (void *)space);
  }
  Env::Default()->SleepForMicroseconds(50000);
  ASSERT_EQ(space->get_available_number(), 0);
  ASSERT_TRUE(space->is_used_up());
  ExtentId extent_id;
  ASSERT_EQ(space->allocate(extent_id).ToString(),
            "Operation failed. Try again.: ./unittest_tmp//000002.sst: No "
            "space is left");
  ARG **args = new ARG *[MAX_EXTENT_NUM - 1];
  for (int32_t i = 0; i < MAX_EXTENT_NUM - 1; ++i) {
    args[i] = new ARG(space, i + 1);
    managertest.env_->Schedule(&free, (void *)args[i]);
  }
  Env::Default()->SleepForMicroseconds(50000);
  ASSERT_EQ(space->get_used_number(), 1);

  // clean up
  std::string fname =
      MakeTableFileName("./unittest_tmp/", 3);  // create by asynchrous thread
  ASSERT_EQ(unlink(fname.c_str()), 0);
  delete[] args;
}

TEST_F(ExtentSpaceManagerTest, CreateSpace) {
  for (int i = 1; i < 510; ++i) {
    ASSERT_EQ(spacemanager->allocate(write_extent), Status::OK());
  }
  // async create one space
  Env::Default()->SleepForMicroseconds(500000);
  ASSERT_EQ(spacemanager->get_full_list().size() +
                spacemanager->get_free_list().size(),
            2);
  // test free before create
  SyncPoint::GetInstance()->LoadDependency(
      {{"ExtentSpaceManager::recycle:Before",
        "ExtentSpaceManager::allocate:Before"},
       {"ExtentSpaceManager::allocate:Before",
        "ExtentSpaceManager::recycle:After"},
       {"ExtentSpaceManager::create_extent_space:Create",
        "ExtentSpaceManager::allocate:Wait"},
       {"ExtentSpaceManager::allocate:Wait",
        "ExtentSpaceManager::create_extent_space:After"}, });
  SyncPoint::GetInstance()->ClearTrace();
  SyncPoint::GetInstance()->EnableProcessing();
  std::thread free_thread{[this]() { spacemanager->recycle(write_extent); }};
  WritableExtent write_extent_tmp;
  ASSERT_EQ(spacemanager->allocate(write_extent_tmp), Status::OK());
  free_thread.join();

  ASSERT_EQ(spacemanager->allocate(write_extent_tmp), Status::OK());
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ(spacemanager->allocate(write_extent_tmp), Status::OK());
  // ASSERT_EQ(spacemanager->get_free_list().size(), 1);
  // clean up
  std::string fname = MakeTableFileName(
      dbname_.c_str(), write_extent_tmp.get_extent_id().file_number);
  ASSERT_EQ(unlink(fname.c_str()), 0);
  fname = MakeTableFileName(dbname_.c_str(), 4);
  ASSERT_EQ(unlink(fname.c_str()), 0);
}

TEST_F(ExtentSpaceManagerTest, RecycleSpace) {
  std::vector<ExtentId> extents;
  for (int i = 1; i < 510; ++i) {
    ASSERT_TRUE(spacemanager->allocate(write_extent).ok());
    extents.emplace_back(write_extent.get_extent_id());
  }
  for (int i = 0; i < 511; ++i) {
    ASSERT_TRUE(spacemanager->allocate(write_extent).ok());
    extents.emplace_back(write_extent.get_extent_id());
  }
  for (int i = 0; i < 511; ++i) {
    ASSERT_TRUE(spacemanager->allocate(write_extent).ok());
    extents.emplace_back(write_extent.get_extent_id());
  }
  // async create space
  Env::Default()->SleepForMicroseconds(500000);
  ASSERT_EQ(spacemanager->get_full_list().size() +
                spacemanager->get_free_list().size(),
            4);
  for (auto extent : extents) {
    ASSERT_TRUE(spacemanager->recycle(extent).ok());
  }
  ASSERT_EQ(spacemanager->get_full_list().size() +
                spacemanager->get_free_list().size(),
            3);
}

TEST_F(ExtentSpaceManagerTest, sync_open) {
  Status s;
  for (int i = 1; i < 510; ++i) {
    s = spacemanager->allocate(write_extent);
    ASSERT_TRUE(s.ok());
  }
  Env::Default()->SleepForMicroseconds(500000);
  ASSERT_EQ(spacemanager->get_full_list().size() +
                spacemanager->get_free_list().size(),
            2);
  s = spacemanager->sync();
  ASSERT_TRUE(s.ok());
  delete spacemanager;
  spacemanager = nullptr;
  spacemanager = new ExtentSpaceManager(options, next_file_number_);
  ASSERT_TRUE(spacemanager != nullptr);
  std::string fname = MakeTableFileName(dbname_.c_str(), 2);
  s = spacemanager->open_extent_space(fname, 2);
  ASSERT_TRUE(s.ok());
  fname = MakeTableFileName(dbname_.c_str(), 3);
  s = spacemanager->open_extent_space(fname, 3);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(spacemanager->get_full_list().size() +
                spacemanager->get_free_list().size(),
            2);
}

}  // storage
}  // xengine

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
	xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
