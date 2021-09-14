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

#include <stdint.h>
#include "xengine/backup_check.h"

#include "table/extent_table_factory.h"
#include "table/table_builder.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "storage/extent_space_manager.h"

using namespace xengine;
using namespace common;
using namespace db;
using namespace table;
using namespace util;
using namespace storage;

namespace xengine {
namespace tools {

const uint32_t optLength = 100;

namespace {
static std::string MakeKey(int i) {
  char buf[100];
  snprintf(buf, sizeof(buf), "k_%04d", i);
  InternalKey key(std::string(buf), 0, ValueType::kTypeValue);
  return key.Encode().ToString();
}

static std::string MakeValue(int i) {
  char buf[100];
  snprintf(buf, sizeof(buf), "v_%04d", i);
  InternalKey key(std::string(buf), 0, ValueType::kTypeValue);
  return key.Encode().ToString();
}

void createSST(MiniTables& mtables,
               const BlockBasedTableOptions& table_options) {
  std::shared_ptr<TableFactory> tf;
  tf.reset(new ExtentBasedTableFactory(table_options));

  Options opts;
  const ImmutableCFOptions imoptions(opts);
  InternalKeyComparator ikc(opts.comparator);
  unique_ptr<TableBuilder> tb;

  opts.table_factory = tf;
  std::vector<std::unique_ptr<IntTblPropCollectorFactory> >
      int_tbl_prop_collector_factories;
  
  std::string column_family_name;
  int unknown_level = -1;
  tb.reset(opts.table_factory->NewTableBuilderExt(
      TableBuilderOptions(imoptions, ikc, &int_tbl_prop_collector_factories,
                          CompressionType::kNoCompression, CompressionOptions(),
                          nullptr /* compression_dict */,
                          false /* skip_filters */, column_family_name,
                          unknown_level),
      TablePropertiesCollectorFactory::Context::kUnknownColumnFamily,
      &mtables));

  ASSERT_TRUE(tb.get() != nullptr);

  // Populate slightly more than 1K keys
  uint32_t num_keys = 1024;
  for (uint32_t i = 0; i < num_keys; i++) {
    tb->Add(MakeKey(i), MakeValue(i));
  }
  tb->Finish();
}

}  // namespace

// Test for  
class BackupCheckToolTest : public testing::Test {
 public:
  std::string dbname_;
  ExtentSpaceManager *spacemanager;
  BlockBasedTableOptions table_options_;
  DBOptions options;
  FileNumber next_file_number_;
  MiniTables mtables;

  BackupCheckToolTest() : next_file_number_(2)
  {
    dbname_ = test::TmpDir(Env::Default()) + "/backup_check_test";
    options.db_paths.emplace_back(dbname_, 0);
    spacemanager = new ExtentSpaceManager(options, next_file_number_);
    mtables.space_manager = spacemanager;
  }

  void test_check_extent(tools::BackupCheckTool &tool) {
    // check right
    ExtentId eid = ExtentId(2, 1);
    tool.data_dir = dbname_;
    bool result = true;
    tool.check_one_extent(eid, MakeKey(0), Slice(), result);  
    ASSERT_TRUE(result);

    // check wrong 
    tool.check_one_extent(eid, Slice(), Slice(), result);  
    ASSERT_TRUE(!result);
  }

  ~BackupCheckToolTest() {}
};

TEST_F(BackupCheckToolTest, check) {
  createSST(mtables, table_options_);

  char* usage[3];
  for (int i = 0; i < 3; i++) {
    usage[i] = new char[optLength];
  }
 
  // wang args
  snprintf(usage[0], optLength, "./backup_check");
  tools::BackupCheckTool tool;
  ASSERT_TRUE(tool.run(1, usage));

  test_check_extent(tool);

  for (int i = 0; i < 3; i++) {
    delete[] usage[i];
  }
}

}  // namespace tools
}  // namespace xengine

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
	xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
