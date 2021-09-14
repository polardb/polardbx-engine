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
#include "db/dbformat.h"
#include "xengine/comparator.h"
#include "multi_version_extent_meta_layer.h"

namespace xengine
{
namespace storage
{
void build_extent_meta(int32_t index_id,
											 int64_t start_key,
											 int64_t end_key,
											 int64_t smallest_seq,
											 int64_t largest_seq,
											 ExtentId extent_id,
											 ExtentMeta &extent_meta)
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

TEST(ExtentLayerTest, add_extent)
{
	int ret = Status::kOk;
  const util::Comparator *byte_wise_comparator = BytewiseComparator();
  db::InternalKeyComparator internal_key_comparator(byte_wise_comparator);
	ExtentMeta extent_meta_1;
	ExtentId extent_id_1(0, 1);
	build_extent_meta(1, 100, 200, 100, 200, extent_id_1, extent_meta_1);
  ExtentMeta extent_meta_2;
  ExtentId extent_id_2(0, 2);
  build_extent_meta(1, 201, 300, 201, 300, extent_id_2, extent_meta_2);
  ExtentMeta extent_meta_3;
  ExtentId extent_id_3(0, 3);
  build_extent_meta(1, 301, 400, 301, 400, extent_id_3, extent_meta_3);

	/*---case 1: invalid argument---*/
	ExtentLayer extent_layer_1(&internal_key_comparator);
	ExtentMeta *extent_meta = nullptr;
	ret = extent_layer_1.add_extent(extent_meta, true);
	ASSERT_EQ(Status::kInvalidArgument, ret);

  /*---case 2: normal case sorted---*/
  ExtentLayer extent_layer_2(&internal_key_comparator);
  ret = extent_layer_2.add_extent(&extent_meta_3, true);
  ASSERT_EQ(Status::kOk, ret);
  ret = extent_layer_2.add_extent(&extent_meta_1, true);
  ASSERT_EQ(Status::kOk, ret);
  ret = extent_layer_2.add_extent(&extent_meta_2, true);
  ASSERT_EQ(Status::kOk, ret);
  //check result
  ASSERT_EQ(3, extent_layer_2.extent_meta_arr_.size());
  ASSERT_EQ(extent_id_1.id(), extent_layer_2.extent_meta_arr_.at(0)->extent_id_.id());
  ASSERT_EQ(extent_id_2.id(), extent_layer_2.extent_meta_arr_.at(1)->extent_id_.id());
  ASSERT_EQ(extent_id_3.id(), extent_layer_2.extent_meta_arr_.at(2)->extent_id_.id());

  /*---case 3: normal case not sorted---*/
  ExtentLayer extent_layer_3(&internal_key_comparator);
  ret = extent_layer_3.add_extent(&extent_meta_3, false);
  ASSERT_EQ(Status::kOk, ret);
  ret = extent_layer_3.add_extent(&extent_meta_1, false);
  ASSERT_EQ(Status::kOk, ret);
  ret = extent_layer_3.add_extent(&extent_meta_2, false);
  //check result
  ASSERT_EQ(3, extent_layer_3.extent_meta_arr_.size());
  ASSERT_EQ(extent_id_3.id(), extent_layer_3.extent_meta_arr_.at(0)->extent_id_.id());
  ASSERT_EQ(extent_id_1.id(), extent_layer_3.extent_meta_arr_.at(1)->extent_id_.id());
  ASSERT_EQ(extent_id_2.id(), extent_layer_3.extent_meta_arr_.at(2)->extent_id_.id());
}

TEST(ExtentLayerTest, remove_extent)
{
  int ret = Status::kOk;
  const util::Comparator *byte_wise_comparator = BytewiseComparator();
  db::InternalKeyComparator internal_key_comparator(byte_wise_comparator);
	ExtentMeta extent_meta_1;
	ExtentId extent_id_1(0, 1);
	build_extent_meta(1, 100, 200, 100, 200, extent_id_1, extent_meta_1);
  ExtentMeta extent_meta_2;
  ExtentId extent_id_2(0, 2);
  build_extent_meta(1, 201, 300, 201, 300, extent_id_2, extent_meta_2);
  ExtentMeta extent_meta_3;
  ExtentId extent_id_3(0, 3);
  build_extent_meta(1, 301, 400, 301, 400, extent_id_3, extent_meta_3);

  /*---case 1: invalid argument---*/
  ExtentLayer extent_layer_1(&internal_key_comparator);
  ExtentMeta *extent_meta = nullptr;
  ret = extent_layer_1.remove_extent(extent_meta);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  /*---case 2: remove not exist extent meta---*/
  ExtentLayer extent_layer_2(&internal_key_comparator);
  ret = extent_layer_2.remove_extent(&extent_meta_1);
  ASSERT_EQ(Status::kEntryNotExist, ret);

  /*---case 3: normal remove---*/
  ExtentLayer extent_layer_3(&internal_key_comparator);
  ret = extent_layer_3.add_extent(&extent_meta_1, true);
  ASSERT_EQ(Status::kOk, ret);
  ret = extent_layer_3.add_extent(&extent_meta_2, true);
  ASSERT_EQ(Status::kOk, ret);
  ret = extent_layer_3.add_extent(&extent_meta_3, true);
  ASSERT_EQ(Status::kOk, ret);
  ret = extent_layer_3.remove_extent(&extent_meta_1);
  ASSERT_EQ(Status::kOk, ret);
  ret = extent_layer_3.remove_extent(&extent_meta_2);
  ASSERT_EQ(Status::kOk, ret);
  ret = extent_layer_3.remove_extent(&extent_meta_3);
  ASSERT_EQ(Status::kOk, ret);
}

TEST(ExtentLayerTest, get_all_extent_ids)
{
  int ret = Status::kOk;
  const util::Comparator *byte_wise_comparator = BytewiseComparator();
  db::InternalKeyComparator internal_key_comparator(byte_wise_comparator);
	ExtentMeta extent_meta_1;
	ExtentId extent_id_1(0, 1);
	build_extent_meta(1, 100, 200, 100, 200, extent_id_1, extent_meta_1);
  ExtentMeta extent_meta_2;
  ExtentId extent_id_2(0, 2);
  build_extent_meta(1, 201, 300, 201, 300, extent_id_2, extent_meta_2);
  ExtentMeta extent_meta_3;
  ExtentId extent_id_3(0, 3);
  build_extent_meta(1, 301, 400, 301, 400, extent_id_3, extent_meta_3);

  /*---case 1: zero extent---*/
  util::autovector<ExtentId> extent_ids_1;
  ExtentLayer extent_layer_1(&internal_key_comparator);
  ret = extent_layer_1.get_all_extent_ids(extent_ids_1);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(0, extent_ids_1.size());

  /*---case 2: normal ---*/
  util::autovector<ExtentId> extent_ids_2;
  ExtentLayer extent_layer_2(&internal_key_comparator);
  ret = extent_layer_2.add_extent(&extent_meta_1, true);
  ASSERT_EQ(Status::kOk, ret);
  ret = extent_layer_2.add_extent(&extent_meta_2, true);
  ASSERT_EQ(Status::kOk, ret);
  ret = extent_layer_2.add_extent(&extent_meta_3, true);
  ASSERT_EQ(Status::kOk, ret);
  ret = extent_layer_2.get_all_extent_ids(extent_ids_2);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(3, extent_ids_2.size());
  ASSERT_EQ(extent_id_1.id(), extent_ids_2.at(0).id());
  ASSERT_EQ(extent_id_2.id(), extent_ids_2.at(1).id());
  ASSERT_EQ(extent_id_3.id(), extent_ids_2.at(2).id());
}

TEST(ExtentLayerVersion, add_layer)
{
  int ret = Status::kOk;
  const util::Comparator *byte_wise_comparator = BytewiseComparator();
  db::InternalKeyComparator internal_key_comparator(byte_wise_comparator);
  ExtentMeta extent_meta_1;
  ExtentId extent_id_1(0, 1);
  build_extent_meta(1, 100, 200, 100, 200, extent_id_1, extent_meta_1);
  ExtentMeta extent_meta_2;
  ExtentId extent_id_2(0, 2);
  build_extent_meta(1, 201, 300, 201, 300, extent_id_2, extent_meta_2);
  ExtentMeta extent_meta_3;
  ExtentId extent_id_3(0, 3);
  build_extent_meta(1, 301, 400, 301, 400, extent_id_3, extent_meta_3);
  ExtentMeta extent_meta_4;
  ExtentId extent_id_4(0, 4);
  build_extent_meta(1, 401, 500, 401, 500, extent_id_4, extent_meta_4);
  ExtentMeta extent_meta_5;
  ExtentId extent_id_5(0, 5);
  build_extent_meta(1, 501, 600, 501, 600, extent_id_5, extent_meta_5);
  ExtentMeta extent_meta_6;
  ExtentId extent_id_6(0, 6);
  build_extent_meta(1, 601, 700, 601, 700, extent_id_6, extent_meta_6);

  /*---case 1: invalid argument---*/
  ExtentLayer *extent_layer_1 = nullptr;
  ExtentLayerVersion extent_layer_version_1(0, 0);
  ret = extent_layer_version_1.add_layer(extent_layer_1);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  /*---case 2: normal---*/
  ExtentLayerVersion extent_layer_version_2(0, 0);
  ExtentLayer extent_layer_2(&internal_key_comparator);
  ret = extent_layer_2.add_extent(&extent_meta_1, true);
  ASSERT_EQ(Status::kOk, ret);
  ret = extent_layer_2.add_extent(&extent_meta_2, true);
  ASSERT_EQ(Status::kOk, ret);
  ret = extent_layer_version_2.add_layer(&extent_layer_2);
  ASSERT_EQ(Status::kOk, ret);
  ExtentLayer extent_layer_3(&internal_key_comparator);
  ret = extent_layer_3.add_extent(&extent_meta_3, true);
  ASSERT_EQ(Status::kOk, ret);
  ret = extent_layer_3.add_extent(&extent_meta_4, true);
  ASSERT_EQ(Status::kOk, ret);
  ret = extent_layer_version_2.add_layer(&extent_layer_3);
  ASSERT_EQ(Status::kOk, ret);
  ExtentLayer extent_layer_4(&internal_key_comparator);
  ret = extent_layer_4.add_extent(&extent_meta_5, true);
  ASSERT_EQ(Status::kOk, ret);
  ret = extent_layer_4.add_extent(&extent_meta_6, true);
  ASSERT_EQ(Status::kOk, ret);
  ret = extent_layer_version_2.add_layer(&extent_layer_4);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(3, extent_layer_version_2.get_extent_layer_size());
  ASSERT_EQ(6, extent_layer_version_2.get_total_extent_size());
  ASSERT_EQ(&extent_layer_4, extent_layer_version_2.get_extent_layer(0));
  ASSERT_EQ(&extent_layer_3, extent_layer_version_2.get_extent_layer(1));
  ASSERT_EQ(&extent_layer_2, extent_layer_version_2.get_extent_layer(2));
}

TEST(ExtentLayerVersion, get)
{
  int ret = Status::kOk;
  const util::Comparator *byte_wise_comparator = BytewiseComparator();
  db::InternalKeyComparator internal_key_comparator(byte_wise_comparator);
  ExtentMeta extent_meta_1;
  ExtentId extent_id_1(0, 1);
  build_extent_meta(1, 100, 200, 100, 200, extent_id_1, extent_meta_1);
  ExtentMeta extent_meta_2;
  ExtentId extent_id_2(0, 2);
  build_extent_meta(1, 201, 300, 201, 300, extent_id_2, extent_meta_2);
  ExtentMeta extent_meta_3;
  ExtentId extent_id_3(0, 3);
  build_extent_meta(1, 301, 400, 301, 400, extent_id_3, extent_meta_3);

  /*---case 1: normal case ---*/
  ExtentLayerVersion extent_layer_version(0, 0);
  ExtentLayer extent_layer(&internal_key_comparator);
  ret = extent_layer.add_extent(&extent_meta_1, true);
  ASSERT_EQ(Status::kOk, ret);
  ret = extent_layer.add_extent(&extent_meta_2, true);
  ASSERT_EQ(Status::kOk, ret);
  ret = extent_layer.add_extent(&extent_meta_3, true);
  ASSERT_EQ(Status::kOk, ret);
  ret = extent_layer_version.add_layer(&extent_layer) ;
  ASSERT_EQ(Status::kOk, ret);
  std::function<int(const ExtentMeta *extent_meta, int32_t level, bool &found)> save_value = [&](const ExtentMeta *extent_meta, int32_t level, bool &found) {
    int ret = Status::kOk;
    if (nullptr == extent_meta || level < 0) {
      ret = Status::kInvalidArgument;
    }
    return ret;
  };
  bool found = false;
  ret = extent_layer_version.get(extent_meta_2.largest_key_.Encode(), save_value, found);
  ASSERT_EQ(Status::kOk, ret);
}

} // namespace storage
} // namespace xengine

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  std::string log_path = test::TmpDir() + "/multi_version_extent_meta_layer_test.log";
  xengine::logger::Logger::get_log().init(log_path.c_str(), xengine::logger::WARN_LEVEL);
  return RUN_ALL_TESTS();
}
