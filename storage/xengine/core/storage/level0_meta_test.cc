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

#include "level0_meta.h"
#include "util/ring_buffer.h"
#include "xengine/comparator.h"
#include "db/dbformat.h"
#include "util/testharness.h"

using namespace xengine;
using namespace common;
using namespace util;
using namespace db;

namespace xengine {
namespace storage {

class Level0VersionTest : public testing::Test {};

TEST_F(Level0VersionTest, add_layer) {
  std::mutex mutex;
  std::unordered_map<int64_t, ExtentMeta> extent_meta_array;
  common::DBOptions db_options;
  std::unordered_map<int32_t, std::vector<db::IntTblPropCollector *>>
      table_properties_collectors;
  InternalKeyComparator cmp(BytewiseComparator());
  Level0Version *v1 = new Level0Version();

  ExtentId extent(1, 1);
  RingBuffer<ExtentMeta *> extents(10, 2);
  ExtentMeta meta;
  meta.extent_id_ = extent;
  meta.largest_key_.DecodeFrom(Slice("0100000000", 10));
  extent_meta_array[extent.id()] = meta;

  extents.put(&extent_meta_array[extent.id()]);
  Level0Layer *l1 = new Level0Layer(0, extents.dump(), 1, &cmp);
  v1->add_layer(l1);
  v1->ref();
  ASSERT_EQ(v1->size(), 1);
  ASSERT_EQ(extents.size(), 1);
  v1->unref(nullptr);
  ASSERT_EQ(extents.size(), 1);

  // not free
  Level0Version *v2 = new Level0Version();
  Level0Version *v3 = new Level0Version();
  extents.remove();
  extents.put(&extent_meta_array[extent.id()]);
  ASSERT_EQ(extents.size(), 1);
  Level0Layer *l2 = new Level0Layer(1, extents.dump(), 1, &cmp);
  v2->add_layer(l2);
  v2->ref();
  v3->add_layer(l2);
  v3->ref();

  v2->unref(nullptr);
  ASSERT_EQ(extents.size(), 1);
  v3->unref(nullptr);
  ASSERT_EQ(extents.size(), 1);
}

TEST_F(Level0VersionTest, search) {
  std::mutex mutex;
  common::DBOptions db_options;
  std::unordered_map<int32_t, std::vector<db::IntTblPropCollector *>>
      table_properties_collectors;
  std::unordered_map<int64_t, ExtentMeta> extent_meta_array;
  InternalKeyComparator cmp(BytewiseComparator());
  Level0Version *v1 = new Level0Version();
  Level0Version *v2 = new Level0Version();

  RingBuffer<ExtentMeta *> extents(10, 2);
  ExtentId extent = ExtentId(1, 1);
  ExtentMeta meta;
  meta.extent_id_ = extent;
  meta.smallest_key_.DecodeFrom(Slice("0100000000", 10));
  meta.largest_key_.DecodeFrom(Slice("0300000000", 10));
  extent_meta_array[extent.id()] = meta;
  extents.put(&extent_meta_array[extent.id()]);
  extent = ExtentId(1, 2);
  meta.extent_id_ = extent;
  meta.smallest_key_.DecodeFrom(Slice("0400000000", 10));
  meta.largest_key_.DecodeFrom(Slice("1000000000", 10));
  extent_meta_array[extent.id()] = meta;
  extents.put(&extent_meta_array[extent.id()]);

  Level0Layer *l0 = new Level0Layer(0, extents.dump(), 2, &cmp);
  extents.remove(2);

  extent = ExtentId(1, 3);
  meta.extent_id_ = extent;
  meta.smallest_key_.DecodeFrom(Slice("0000000000", 10));
  meta.largest_key_.DecodeFrom(Slice("0200000000", 10));
  extent_meta_array[extent.id()] = meta;
  extents.put(&extent_meta_array[extent.id()]);

  extent = ExtentId(1, 4);
  meta.extent_id_ = extent;
  meta.smallest_key_.DecodeFrom(Slice("0300000000", 10));
  meta.largest_key_.DecodeFrom(Slice("0500000000", 10));
  extent_meta_array[extent.id()] = meta;
  extents.put(&extent_meta_array[extent.id()]);

  extent = ExtentId(1, 5);
  meta.extent_id_ = extent;
  meta.smallest_key_.DecodeFrom(Slice("0700000000", 10));
  meta.largest_key_.DecodeFrom(Slice("0900000000", 10));
  extent_meta_array[extent.id()] = meta;
  extents.put(&extent_meta_array[extent.id()]);

  Level0Layer *l1 = new Level0Layer(1, extents.dump(), 3, &cmp);

  extents.remove(3);
  extent = ExtentId(1, 6);
  meta.extent_id_ = extent;
  meta.smallest_key_.DecodeFrom(Slice("0100000000", 10));
  meta.largest_key_.DecodeFrom(Slice("0800000000", 10));
  extent_meta_array[extent.id()] = meta;
  extents.put(&extent_meta_array[extent.id()]);

  Level0Layer *l2 = new Level0Layer(2, extents.dump(), 1, &cmp);
  ASSERT_EQ(extents.size(), 1);

  v1->ref();
  v1->add_layer(l0);
  v1->add_layer(l1);
  ASSERT_EQ(v1->size(), 2);

  v2->ref();
  v2->add_layer(l1);
  v2->add_layer(l2);
  ASSERT_EQ(v1->size(), 2);

  // simple search
  std::vector<MetaEntry> chosen;
  int32_t sorted_run = 0;
  int ret = v1->search(Slice("1000000000", 10), chosen, sorted_run);
  ASSERT_EQ(ret, Status::kOk);
  ASSERT_EQ(chosen[0].value_.extent_id_.offset, 2);
  chosen.clear();
  sorted_run = 0;
  ret = v2->search(Slice("1000000000", 10), chosen, sorted_run);
  ASSERT_EQ(ret, Status::kOk);
  ASSERT_EQ(chosen.size(), 0);

  // clear
  v1->unref(nullptr);
  ASSERT_EQ(extents.size(), 1);
  v2->unref(nullptr);
  ASSERT_EQ(extents.size(), 1);
}

}  // storage
}  // xengine

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
	xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
