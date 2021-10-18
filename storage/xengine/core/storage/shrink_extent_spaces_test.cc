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
#include "storage/shrink_extent_spaces.h"
#include "db/db_test_util.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "options/options_helper.h"
//#include "level0_meta.h"

using namespace xengine;
using namespace db;
using namespace common;
using namespace util;
using namespace monitor;

namespace xengine {
namespace storage {

class ShrinkExtentSpacesJobTest : public DBTestBase {
public:
  ShrinkExtentSpacesJobTest() : DBTestBase("/shrink_extent_spaces_test") {}

  std::unique_ptr<ShrinkExtentSpacesJob> shrink_job_;

  void wait_for_l0_compaction(ColumnFamilyHandle *handle) {
    // ColumnFamilyData* cfd = ((ColumnFamilyHandleImpl*)(handle))->cfd();
    // while (((SnapshotImpl*)(cfd->get_storage_manager()->get_current_version()))->level0_->size() > 0) {
      //dbfull()->CompactRange(CompactRangeOptions(), handle, nullptr, nullptr);
      //Env::Default()->SleepForMicroseconds(1000);
    //} 
  }

  void test_get_extent_ids() {
    Options options = CurrentOptions();
    options.level0_layer_num_compaction_trigger = 8192;
    options.level0_file_num_compaction_trigger = 8192;
    options.level1_extents_major_compaction_trigger = 8192;
    options.flush_delete_percent = 0;
    CreateAndReopenWithCF({"tb", "tm"}, options);
    auto cfh0 = reinterpret_cast<ColumnFamilyHandleImpl*>(get_column_family_handle(0));
    //auto space_manager = dbfull()->storage_logger_->get_extent_space_manager();
    DBImpl *db_impl = dynamic_cast<DBImpl*>(db_);
    InstrumentedMutex *db_mutex = &(db_impl->mutex_);
    {
    shrink_job_.reset(new ShrinkExtentSpacesJob(2, db_mutex, dbfull()->versions_->get_global_ctx()));
    ASSERT_TRUE(nullptr != shrink_job_);
    //ASSERT_TRUE(!shrink_job_->get_column_family_extent_ids(cfh0->cfd()));
    ASSERT_EQ(Status::kNotSupported, shrink_job_->prepare());
    ASSERT_EQ(shrink_job_->extentid_2_info_map_.size(), 0);
    shrink_job_.reset();
    }
    WriteOptions writeOpt = WriteOptions();
    ASSERT_OK(dbfull()->Put(writeOpt, cfh0, "k1", "v1"));
    ASSERT_OK(Flush(0));
    ASSERT_OK(dbfull()->Put(writeOpt, cfh0, "k2", "v2"));
    ASSERT_OK(Flush(0));
    ASSERT_OK(db_->Delete(writeOpt, cfh0, "k1"));
    Flush(0); 
    dbfull()->CompactRange(CompactRangeOptions(), cfh0, nullptr, nullptr, 13);
    dbfull()->TEST_WaitForCompact();
    {
      shrink_job_.reset(new ShrinkExtentSpacesJob(2, db_mutex, dbfull()->versions_->get_global_ctx()));
      ASSERT_TRUE(nullptr != shrink_job_);
      ASSERT_EQ(shrink_job_->prepare(), Status::kNotSupported);
      shrink_job_.reset();
    }
    auto cfh2 = reinterpret_cast<ColumnFamilyHandleImpl*>(get_column_family_handle(2));
    for (int i = 1; i <= 1024; i++) {
      std::string key = "k" + std::to_string(i);
      std::string value = "v" + std::to_string(i);
      ASSERT_OK(dbfull()->Put(writeOpt, cfh0, key, value));
      ASSERT_OK(Flush(0));
      ASSERT_OK(dbfull()->Put(writeOpt, cfh2, key, value));
      ASSERT_OK(Flush(2));
    }
    for (int i = 1; i <= 1024; i++) {
      std::string key = "k" + std::to_string(i);
      ASSERT_OK(dbfull()->Delete(writeOpt, cfh2, key));
      ASSERT_OK(Flush(2));
    }
    dbfull()->CompactRange(CompactRangeOptions(), cfh2, nullptr, nullptr, 13);
    dbfull()->TEST_WaitForCompact();
    {
      shrink_job_.reset(new ShrinkExtentSpacesJob(2, db_mutex, dbfull()->versions_->get_global_ctx()));
      ASSERT_TRUE(nullptr != shrink_job_);
      ASSERT_EQ(shrink_job_->prepare(), Status::kOk);
      ASSERT_EQ(shrink_job_->extentid_2_info_map_.size(), 1025);
      ASSERT_EQ(shrink_job_->shrink_extent_spaces(), Status::kOk);
      ASSERT_EQ(shrink_job_->check_consistency(), Status::kOk);
      shrink_job_.reset();
    }
  }
};

TEST_F(ShrinkExtentSpacesJobTest, do_shrink) {
  test_get_extent_ids(); 

  ASSERT_EQ("v1", Get(0, "k1"));
  ASSERT_EQ("v256", Get(0, "k256"));
  ASSERT_EQ("v512", Get(0, "k512"));
  ASSERT_EQ("v1024", Get(0, "k1024"));

  // restart and check the data
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "tb", "tm"}, CurrentOptions()));
  ASSERT_EQ("v1", Get(0, "k1"));
  ASSERT_EQ("v256", Get(0, "k256"));
  ASSERT_EQ("v512", Get(0, "k512"));
  ASSERT_EQ("v1024", Get(0, "k1024"));
}

}
}

int main(int argc, char** argv) {
  //port::InstallStackTraceHandler();
  port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  std::string log_path = test::TmpDir() + "/shrink_extent_space.log";
  xengine::logger::Logger::get_log().init(log_path.c_str(), xengine::logger::DEBUG_LEVEL);
  return RUN_ALL_TESTS();
}
