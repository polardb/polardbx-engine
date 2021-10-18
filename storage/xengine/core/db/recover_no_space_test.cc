// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_test_util.h"
#include "util/sync_point.h"

using namespace xengine;
using namespace common;
using namespace util;
using namespace table;
using namespace cache;
using namespace memtable;
using namespace monitor;
using namespace storage;

namespace xengine {
namespace db {

class RecoverNoSpaceTest : public DBTestBase {
 public:
  RecoverNoSpaceTest() : DBTestBase("/recover_no_space_test") {}
  std::string &db_name() { return dbname_; }
};

TEST_F(RecoverNoSpaceTest, FailInRecover) {
  Options options;
  options.create_if_missing = true;
  options.write_buffer_size = 10000;
  SyncPoint::GetInstance()->SetCallBack(
      "VersionSet::LogAndApply:AfterSyncManifest", [&](void* arg) {
        fprintf(stdout, "Flush in recovery\n");
        Status* s = static_cast<Status *>(arg);
        *s = Status::IOError();
      });
  CreateAndReopenWithCF({"tb"}, options);
  Put(1, "k1", std::string(10000, 'x'));  // Fill memtable
  Put(1, "k2", std::string(10000, 'y'));  // Trigger flush
  std::string current_file_before;
  Status s =
  ReadFileToString(options.env, CurrentFileName(db_name()), &current_file_before);
  SyncPoint::GetInstance()->EnableProcessing();
  // io error
  ASSERT_EQ(TryReopenWithColumnFamilies({"default", "tb"}, CurrentOptions()).code(), Status::kOk);
  std::string current_file_after;
  s =
  ReadFileToString(options.env, CurrentFileName(db_name()), &current_file_after); 
  // current not change
  ASSERT_EQ(current_file_before, current_file_after);
  fprintf(stdout, "%s%s", current_file_before.c_str(), current_file_after.c_str());
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(TryReopenWithColumnFamilies({"default", "tb"}, CurrentOptions()));
  s =
  ReadFileToString(options.env, CurrentFileName(db_name()), &current_file_after); 
  fprintf(stdout, "%s", current_file_after.c_str());
}

}
}  // namespace xengine

int main(int argc, char** argv) {
  port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
	xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
