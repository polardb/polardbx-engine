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

#include "storage/io_extent.h"
#include "db/version_set.h"
#include "storage/extent_space_manager.h"
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

class ExtentTest : public testing::Test {
 public:
  ExtentTest()
      : env_(Env::Default()),
        dbname_("./unittest_tmp/"),
        table_cache_(NewLRUCache(50000, 16)),
        write_buffer_manager_(db_options_.db_write_buffer_size),
        next_file_number_(2) {
    options.db_paths.emplace_back(dbname_, 0);
    spacemanager = new ExtentSpaceManager(options, next_file_number_);
    // new VersionSet(dbname_, &db_options_, env_options_, table_cache_.get(),
    //&write_buffer_manager_, &write_controller_));
    DestroyDB(dbname_, Options(options, cf_options));
    assert(mkdir(dbname_.c_str(), 0755) == 0);
    assert(spacemanager->allocate(write_extent) == Status::OK());
  };

  ~ExtentTest() {
    // clean up
    DestroyDB(dbname_, Options(options, cf_options));
  }

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

TEST_F(ExtentTest, Write) {
  ASSERT_EQ(write_extent.get_extent_id().file_number, 2);
  ASSERT_EQ(write_extent.get_extent_id().offset, 1);
  // all interface is ok
  ASSERT_EQ(write_extent.Truncate(0), Status::OK());
  ASSERT_EQ(write_extent.Close(), Status::OK());
  ASSERT_EQ(write_extent.Flush(), Status::OK());
  ASSERT_EQ(write_extent.Sync(), Status::OK());
  ASSERT_EQ(write_extent.Fsync(), Status::OK());
  ASSERT_TRUE(write_extent.IsSyncThreadSafe());
  ASSERT_FALSE(write_extent.UseOSBuffer());
  ASSERT_EQ(write_extent.GetRequiredBufferAlignment(), 4 * 1024);
  ASSERT_TRUE(write_extent.UseDirectIO());
  ASSERT_EQ(write_extent.InvalidateCache(0, 0), Status::OK());
  // write a aligned buffer
  void *buf = nullptr;
  posix_memalign(&(buf), 4 * 1024, 1024);
  ASSERT_TRUE(buf);
  strcpy(static_cast<char *>(buf), "extent_write_test");
  Slice test(static_cast<char *>(buf), 512);
  ASSERT_EQ(write_extent.Append(test), Status::OK());
  // unaligned length
  Slice test1(static_cast<char *>(buf));
  ASSERT_EQ(write_extent.Append(test1).ToString(),
            "IO error: Unaligned buffer for direct IO.");
  // unaligned address
  Slice test2(static_cast<char *>(buf) + 1, 512);
  ASSERT_EQ(write_extent.Append(test2).ToString(),
            "IO error: Unaligned buffer for direct IO.");
  // space is not enough
  void *buf1 = nullptr;
  posix_memalign(&(buf1), 4 * 1024, 2 * 1024 * 1024);
  memset(static_cast<char *>(buf1), 0, 2 * 1024 * 1024);
  Slice test3(static_cast<char *>(buf1), 2 * 1024 * 1024 - 512);
  ASSERT_EQ(write_extent.Append(test3), Status::OK());
  ASSERT_EQ(write_extent.Append(test).ToString(),
            "IO error: Not enough space left in extent for write.");
  ASSERT_EQ(write_extent.GetFileSize(), 2 * 1024 * 1024);
  free(buf);
  free(buf1);
  std::string fname = MakeTableFileName(dbname_.c_str(), 3);
  assert(unlink(fname.c_str()) == 0);
}

TEST_F(ExtentTest, Read) {
  // write data
  void *buf = nullptr;
  posix_memalign(&(buf), 4 * 1024, 1024);
  ASSERT_TRUE(buf);
  strcpy(static_cast<char *>(buf), "extent_write_test");
  Slice test(static_cast<char *>(buf), 512);
  ASSERT_EQ(write_extent.Append(test), Status::OK());
  ASSERT_EQ(write_extent.get_offset(), 2 * 1024 * 1024);
  ASSERT_EQ(write_extent.GetFileSize(), 512);
  // std::cout<<write_extent.get_extent_id().file_number<<" : "
  //<<write_extent.get_extent_id().offset<<std::endl;
  // read data
  std::string fname = MakeTableFileName(
      dbname_.c_str(), write_extent.get_extent_id().file_number);
  ASSERT_EQ(spacemanager->get_random_access_extent(write_extent.get_extent_id(),
                                                   read_extent),
            Status::OK());
  // read aligned
  void *out = nullptr;
  posix_memalign(&(out), 4 * 1024, 1024);
  ASSERT_TRUE(out);
  Slice result;
  ASSERT_EQ(read_extent.Read(0, 512, &result, static_cast<char *>(out)),
            Status::OK());
  // std::cout<<result.data()<<std::endl;
  ASSERT_EQ(memcmp(result.data(), "extent_write_test", 17), 0);
  // read unaligned
  ASSERT_EQ(read_extent.Read(0, 17, &result, static_cast<char *>(out)),
            Status::OK());
  ASSERT_EQ(memcmp(result.data(), "extent_write_test", 17), 0);
  free(buf);
  free(out);
  fname = MakeTableFileName(dbname_.c_str(), 3);
  assert(unlink(fname.c_str()) == 0);
}
}
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
	xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
