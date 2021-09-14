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

#include "util/testharness.h"
#include "util/testutil.h"
#include "cache/lru_cache.h"
#include "db/column_family.h"
#include "db/internal_stats.h"
#include "storage/storage_manager.h"
#include "storage/storage_logger.h"
#include "storage/extent_space_manager.h"
#include "table/merging_iterator.h"
#include "xengine/cache.h"
#include "xengine/options.h"

namespace xengine
{
namespace storage
{
class StorageManagerTest : public testing::Test
{
public:
  StorageManagerTest();
  virtual ~StorageManagerTest();
  virtual void SetUp();
  virtual void TearDown();
  void build_extent_meta(int32_t index_id, int64_t start_key, int64_t end_key, int64_t smallest_seq, int64_t largest_seq, ExtentId extent_id, ExtentMeta &extent_meta);

public:
  static const size_t DEFAULT_TABLE_CACHE_SIZE = 1 * 1024 * 1024; //1MB
  static const int DEFAULT_TABLE_CACHE_NUMSHARDBITS = 6;
public:
  //const common::DBOptions db_options_;
  //const common::ColumnFamilyOptions cf_options_;
  //common::Options options_;
  db::ColumnFamilyData *sub_table_;
  cache::Cache *table_cache_;
  StorageLogger *storage_logger_;
  ExtentSpaceManager *extent_space_manager_;
  StorageManager *storage_manager_;
};
StorageManagerTest::StorageManagerTest()
    : sub_table_(nullptr),
      table_cache_(nullptr),
      storage_logger_(nullptr),
      extent_space_manager_(nullptr),
      storage_manager_(nullptr)
{
}

StorageManagerTest::~StorageManagerTest()
{
}

void StorageManagerTest::SetUp()
{
  common::Options options;
  db::FileNumber file_number(0);
  sub_table_ = new db::ColumnFamilyData(options);
  //table_cache_ = new db::TableCache(DEFAULT_TABLE_CACHE_SIZE, DEFAULT_TABLE_CACHE_NUMSHARDBITS);
  //table_cache_ = NewLRUCache(DEFAULT_TABLE_CACHE_SIZE, DEFAULT_TABLE_CACHE_NUMSHARDBITS);
  table_cache_ = new cache::LRUCache(0, DEFAULT_TABLE_CACHE_SIZE, DEFAULT_TABLE_CACHE_NUMSHARDBITS, false, 0.0, 0.0, false);
  extent_space_manager_ = new ExtentSpaceManager(common::DBOptions(options), file_number);
  storage_manager_ = new StorageManager(common::DBOptions(options), common::ColumnFamilyOptions(options), sub_table_);
}

void StorageManagerTest::TearDown()
{
  sub_table_->Unref();
  delete sub_table_;
  delete table_cache_;
  delete extent_space_manager_;
  delete storage_manager_;
}

void StorageManagerTest::build_extent_meta(int32_t index_id, int64_t start_key, int64_t end_key, int64_t smallest_seq, int64_t largest_seq, ExtentId extent_id, ExtentMeta &extent_meta)
{
  char user_key_buf[16];
  memset(user_key_buf, 0, 16);
  sprintf(user_key_buf, "%d", index_id);
  sprintf(user_key_buf + 4, "%ld", start_key);
  Slice start_user_key(user_key_buf, 4 + sizeof(start_key));
  db::InternalKey smallest_key(start_user_key, smallest_seq, db::kTypeValue);
  memset(user_key_buf, 0, 16);
  sprintf(user_key_buf, "%d", index_id);
  sprintf(user_key_buf + 4, "%ld", end_key);
  Slice end_user_key(user_key_buf, 4 + sizeof(end_key));
  db::InternalKey largest_key(end_user_key, largest_seq, db::kTypeValue);
  
  extent_meta.smallest_key_ = smallest_key;
  extent_meta.largest_key_ = largest_key;
  extent_meta.extent_id_ = extent_id;
  extent_meta.smallest_seqno_ = smallest_seq;
  extent_meta.largest_seqno_ = largest_seq;
  extent_meta.refs_ = 1;
  extent_meta.data_size_ = 100;
  extent_meta.index_size_ = 10;
  extent_meta.num_data_blocks_ = 16;
  extent_meta.num_entries_ = end_key - start_key;
}

TEST_F(StorageManagerTest, init)
{
  int ret = Status::kOk;
  //invalid argument
  ret = storage_manager_->init(nullptr, table_cache_, 1);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = storage_manager_->init(extent_space_manager_, nullptr, 1);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = storage_manager_->init(extent_space_manager_, table_cache_, -1);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  //success init
  ret = storage_manager_->init(extent_space_manager_, table_cache_, 1);
  ASSERT_EQ(Status::kOk, ret);
}

TEST_F(StorageManagerTest, apply)
{
  int ret = Status::kOk;
  ChangeInfo change_info;

  //not init
  ret = storage_manager_->apply(change_info, false);
  ASSERT_EQ(Status::kNotInit, ret);

  //invalid argument
  ret = storage_manager_->init(extent_space_manager_, table_cache_, 1);
  ASSERT_EQ(Status::kOk, ret);
  ret = storage_manager_->apply(change_info, false);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  /*---case 1: apply flush task change info---*/
  ExtentMeta extent_meta_1;
  ExtentId extent_id_1(0, 1);
  build_extent_meta(1, 1, 100, 1, 100, extent_id_1, extent_meta_1);
  extent_space_manager_->write_meta(&extent_meta_1);
  ExtentMeta extent_meta_2;
  ExtentId extent_id_2(0, 2);
  build_extent_meta(1, 101, 200, 101, 200, extent_id_2, extent_meta_2);
  extent_space_manager_->write_meta(&extent_meta_2);
  ret = change_info.add_extent(0, extent_id_1, false, &extent_meta_1);
  ASSERT_EQ(Status::kOk, ret);
  ret = change_info.add_extent(0, extent_id_2, false, &extent_meta_2);
  ASSERT_EQ(Status::kOk, ret);
  change_info.task_type_ = db::FLUSH_TASK;
  change_info.largest_seq_ = 200;
  ret = storage_manager_->apply(change_info, false);
  ASSERT_EQ(Status::kOk, ret);

  /*---case 2: apply intro minor task change info---*/
  //step 1: build another level 0 layer base on "case 1"
  change_info.clear();
  ExtentMeta extent_meta_3;
  ExtentId extent_id_3(0, 3);
  build_extent_meta(1, 201, 300, 201, 300, extent_id_3, extent_meta_3);
  extent_space_manager_->write_meta(&extent_meta_3);
  ExtentMeta extent_meta_4;
  ExtentId extent_id_4(0, 4);
  build_extent_meta(1, 301, 400, 301, 400, extent_id_4, extent_meta_4);
  extent_space_manager_->write_meta(&extent_meta_4);
  ret = change_info.add_extent(0, extent_id_3, false, &extent_meta_3);
  ASSERT_EQ(Status::kOk, ret);
  ret = change_info.add_extent(0, extent_id_4, false, &extent_meta_4);
  ASSERT_EQ(Status::kOk, ret);
  change_info.task_type_ = db::FLUSH_TASK;
  change_info.largest_seq_ = 400;
  ret = storage_manager_->apply(change_info, false);
  ASSERT_EQ(Status::kOk, ret);
  //step 2: build intro minor change info
  change_info.clear();
  ExtentMeta extent_meta_5;
  ExtentId extent_id_5(0, 5);
  build_extent_meta(1, 401, 500, 401, 500, extent_id_5, extent_meta_5);
  extent_space_manager_->write_meta(&extent_meta_5);
  change_info.delete_extent(0, extent_id_1, extent_meta_1.largest_key_.Encode(), false);
  change_info.delete_extent(0, extent_id_2, extent_meta_2.largest_key_.Encode(), false);
  change_info.delete_extent(0, extent_id_3, extent_meta_3.largest_key_.Encode(), false);
  change_info.delete_extent(0, extent_id_4, extent_meta_4.largest_key_.Encode(), false);
  ret = change_info.add_extent(0, extent_id_5, false, &extent_meta_5);
  ASSERT_EQ(Status::kOk, ret);
  change_info.task_type_ = db::INTRA_COMPACTION_TASK;
  change_info.largest_seq_ = 99;
  ret = storage_manager_->apply(change_info, false);
  ASSERT_EQ(Status::kOk, ret);
}

TEST_F(StorageManagerTest, get)
{
  int ret = Status::kOk;
  util::Arena arena;
  common::Slice start_key;
  db::SnapshotImpl snapshot;
  snapshot.extent_layer_versions_[0] = nullptr;
  snapshot.extent_layer_versions_[1] = nullptr;
  snapshot.extent_layer_versions_[2] = nullptr;
  std::function<int(const ExtentMeta *extent_meta, int32_t level, bool &found)> save_value = [&](const ExtentMeta *extent_meta, int32_t level, bool &found) {
    int ret = Status::kOk;
    if (nullptr == extent_meta || level < 0) {
      ret = Status::kInvalidArgument;
    }
    return ret;
  };
  
  /*---case 1: not init---*/
  ret = storage_manager_->get(start_key, snapshot, save_value);
  ASSERT_EQ(Status::kNotInit, ret);

  /*---case 2: invalid argument---*/
  ret = storage_manager_->init(extent_space_manager_, table_cache_, 1);
  ASSERT_EQ(Status::kOk, ret);
  ret = storage_manager_->get(start_key, snapshot, save_value);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  /*---case 3: extent layer version is nullptr---*/
  ExtentMeta extent_meta_1;
  ExtentId extent_id_1(0, 1);
  build_extent_meta(1, 100, 200, 100, 200, extent_id_1, extent_meta_1);
  ret = storage_manager_->get(extent_meta_1.largest_key_.Encode(), snapshot, save_value);
  ASSERT_EQ(Status::kErrorUnexpected, ret);

  /*---case 4: normal---*/
  ChangeInfo change_info;
  ExtentMeta extent_meta_2;
  ExtentId extent_id_2(0, 2);
  build_extent_meta(1, 201, 300, 201, 300, extent_id_2, extent_meta_2);
  extent_space_manager_->write_meta(&extent_meta_2);
  ExtentMeta extent_meta_3;
  ExtentId extent_id_3(0, 3);
  build_extent_meta(1, 301, 400, 301, 400, extent_id_3, extent_meta_3);
  extent_space_manager_->write_meta(&extent_meta_3);
  ret = change_info.add_extent(0, extent_id_2, false, &extent_meta_2);
  ASSERT_EQ(Status::kOk, ret);
  ret = change_info.add_extent(0, extent_id_3, false, &extent_meta_3);
  ASSERT_EQ(Status::kOk, ret);
  change_info.task_type_ = db::FLUSH_TASK;
  change_info.largest_seq_ = 400;
  ret = storage_manager_->apply(change_info, false);
  ASSERT_EQ(Status::kOk, ret);
  ret = storage_manager_->get(extent_meta_2.smallest_key_.Encode(), *(storage_manager_->get_current_version()), save_value);
  ASSERT_EQ(Status::kNotFound, ret);
}

TEST_F(StorageManagerTest, add_iterators)
{
  int ret = Status::kOk;
  util::Arena arena;
  ReadOptions read_options;
  Options options;
  ImmutableCFOptions imm_cf_options(options);
  util::EnvOptions env_options;
  util::Env *env = util::Env::Default();
  db::SnapshotImpl snapshot;
  db::TableCache *table_cache = new db::TableCache(imm_cf_options, env_options, table_cache_, extent_space_manager_);
  db::InternalStats *internal_stats = new db::InternalStats(3, env, sub_table_);
  table::MergeIteratorBuilder merge_iter_builder(&sub_table_->internal_comparator(), extent_space_manager_, &arena, false);
  db::RangeDelAggregator range_del_aggregator(sub_table_->internal_comparator(), 100, true);


  /*---case 1: not init---*/
  ret = storage_manager_->add_iterators(table_cache, internal_stats, read_options, &merge_iter_builder, &range_del_aggregator, &snapshot);
  ASSERT_EQ(Status::kNotInit, ret);

  /*---case 2: invalid argument---*/
  ret = storage_manager_->init(extent_space_manager_, table_cache_, 1);
  ASSERT_EQ(Status::kOk, ret);
  ret = storage_manager_->add_iterators(nullptr, internal_stats, read_options, &merge_iter_builder, &range_del_aggregator, &snapshot);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = storage_manager_->add_iterators(table_cache, internal_stats, read_options, nullptr, &range_del_aggregator, &snapshot);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = storage_manager_->add_iterators(table_cache, internal_stats, read_options, &merge_iter_builder, nullptr, &snapshot);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = storage_manager_->add_iterators(table_cache, internal_stats, read_options, &merge_iter_builder, &range_del_aggregator, nullptr);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  /*---case 3: normal---*/
  ChangeInfo change_info;
  ExtentMeta extent_meta_1;
  ExtentId extent_id_1(0, 1);
  build_extent_meta(1, 100, 200, 100, 200, extent_id_1, extent_meta_1);
  extent_space_manager_->write_meta(&extent_meta_1);
  ExtentMeta extent_meta_2;
  ExtentId extent_id_2(0, 2);
  build_extent_meta(1, 201, 300, 201, 300, extent_id_2, extent_meta_2);
  extent_space_manager_->write_meta(&extent_meta_2);
  ret = change_info.add_extent(0, extent_id_1, false, &extent_meta_1);
  ASSERT_EQ(Status::kOk, ret);
  ret = change_info.add_extent(0, extent_id_2, false, &extent_meta_2);
  ASSERT_EQ(Status::kOk, ret);
  change_info.task_type_ = db::FLUSH_TASK;
  change_info.largest_seq_ = 300;
  ret = storage_manager_->apply(change_info, false);
  ASSERT_EQ(Status::kOk, ret);
  ret = storage_manager_->add_iterators(table_cache, internal_stats, read_options, &merge_iter_builder, &range_del_aggregator, storage_manager_->get_current_version());
  ASSERT_EQ(Status::kOk, ret);
}

TEST_F(StorageManagerTest, serialize_and_deserialize)
{
  int ret = Status::kOk;
  char *buf = nullptr;
  int64_t buf_length = 0;
  int64_t pos = 0;

  /*---case 1: not init---*/
  ret = storage_manager_->serialize(buf, buf_length, pos);
  ASSERT_EQ(Status::kNotInit, ret);
  ret = storage_manager_->deserialize(buf, buf_length, pos);
  ASSERT_EQ(Status::kNotInit, ret);

  /*---case 2: invalid argument---*/
  ret = storage_manager_->init(extent_space_manager_, table_cache_, 1);
  ASSERT_EQ(Status::kOk, ret);
  ret = storage_manager_->serialize(buf, buf_length, pos);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = storage_manager_->deserialize(buf, buf_length, pos);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  /*---case 3: normal---*/
  //step 1: build level0
  ChangeInfo change_info;
  ExtentMeta extent_meta_1;
  ExtentId extent_id_1(0, 1);
  build_extent_meta(1, 100, 200, 100, 200, extent_id_1, extent_meta_1);
  extent_space_manager_->write_meta(&extent_meta_1);
  ExtentMeta extent_meta_2;
  ExtentId extent_id_2(0, 2);
  build_extent_meta(1, 201, 300, 201, 300, extent_id_2, extent_meta_2);
  extent_space_manager_->write_meta(&extent_meta_2);
  ret = change_info.add_extent(0, extent_id_1, false, &extent_meta_1);
  ASSERT_EQ(Status::kOk, ret);
  ret = change_info.add_extent(0, extent_id_2, false, &extent_meta_2);
  ASSERT_EQ(Status::kOk, ret);
  change_info.task_type_ = db::FLUSH_TASK;
  change_info.largest_seq_ = 300;
  ret = storage_manager_->apply(change_info, false);
  ASSERT_EQ(Status::kOk, ret);
  //step 2: build level1
  change_info.clear();
  ExtentMeta extent_meta_3;
  ExtentId extent_id_3(0, 3);
  build_extent_meta(1, 301, 400, 301, 400, extent_id_3, extent_meta_3);
  extent_space_manager_->write_meta(&extent_meta_3);
  ExtentMeta extent_meta_4;
  ExtentId extent_id_4(0, 4);
  build_extent_meta(1, 401, 500, 401, 500, extent_id_4, extent_meta_4);
  extent_space_manager_->write_meta(&extent_meta_4);
  ret = change_info.add_extent(1, extent_id_3, false);
  ASSERT_EQ(Status::kOk, ret);
  ret = change_info.add_extent(1, extent_id_4, false);
  ASSERT_EQ(Status::kOk, ret);
  change_info.task_type_ = db::SPLIT_TASK;
  ret = storage_manager_->apply(change_info, false);
  ASSERT_EQ(Status::kOk, ret);
  //step 3: build level2
  change_info.clear();
  ExtentMeta extent_meta_5;
  ExtentId extent_id_5(0, 5);
  build_extent_meta(1, 501, 600, 501, 600, extent_id_5, extent_meta_5);
  extent_space_manager_->write_meta(&extent_meta_5);
  ExtentMeta extent_meta_6;
  ExtentId extent_id_6(0, 6);
  build_extent_meta(1, 601, 700, 601, 700, extent_id_6, extent_meta_6);
  extent_space_manager_->write_meta(&extent_meta_6);
  ret = change_info.add_extent(2, extent_id_5, false);
  ASSERT_EQ(Status::kOk, ret);
  ret = change_info.add_extent(2, extent_id_6, false);
  ASSERT_EQ(Status::kOk, ret);
  change_info.task_type_ = db::MAJOR_SELF_COMPACTION_TASK;
  ret = storage_manager_->apply(change_info, false);
  ASSERT_EQ(Status::kOk, ret);
  //step 4: serialize and deserialize 
  buf_length = storage_manager_->get_serialize_size();
  buf = new char[buf_length];
  ret = storage_manager_->serialize(buf, buf_length, pos);
  ASSERT_EQ(Status::kOk, ret);
  Options options;
  StorageManager *storage_manager_tmp = new StorageManager(DBOptions(options), ColumnFamilyOptions(options), sub_table_);
  ret = storage_manager_tmp->init(extent_space_manager_, table_cache_, 1);
  ASSERT_EQ(Status::kOk, ret);
  pos = 0;
  ret = storage_manager_tmp->deserialize(buf, buf_length, pos);
  ASSERT_EQ(Status::kOk, ret);
  //step 5: check result
  const db::Snapshot *snapshot = storage_manager_tmp->get_current_version();
  ExtentLayerVersion *level0_version = snapshot->get_level_version(0);
  ASSERT_EQ(1, level0_version->get_extent_layer_size());
  ASSERT_EQ(2, level0_version->get_total_extent_size());
  ExtentLayer *extent_layer = level0_version->get_extent_layer(0);
  ASSERT_EQ(extent_id_1.id(), extent_layer->extent_meta_arr_.at(0)->extent_id_.id());
  ASSERT_EQ(extent_id_2.id(), extent_layer->extent_meta_arr_.at(1)->extent_id_.id());
}
}
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  std::string log_path = test::TmpDir() + "/storage_manager_test.log";
  logger::Logger::get_log().init(log_path.c_str(), logger::WARN_LEVEL);
  return RUN_ALL_TESTS();
}
