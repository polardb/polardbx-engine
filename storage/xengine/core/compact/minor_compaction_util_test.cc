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
#include <gflags/gflags.h>
#include "compact/minor_compaction.h"
#include "db/builder.h"
#include "db/column_family.h"
#include "db/db_impl.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "options/options_helper.h"
#include "storage/extent_space_manager.h"
#include "storage/io_extent.h"
#include "storage/storage_manager.h"
#include "storage/storage_manager_mock.h"
#include "table/block.h"
#include "table/block_builder.h"
#include "table/extent_table_factory.h"
#include "table/merging_iterator.h"
#include "table/table_reader.h"
#include "util/arena.h"
#include "util/autovector.h"
#include "util/serialization.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "xengine/db.h"
#include "xengine/env.h"
#include "xengine/options.h"
#include "xengine/table.h"
#include "xengine/types.h"
#include "xengine/write_batch.h"
#include "xengine/xengine_constants.h"

using namespace xengine;
using namespace storage;
using namespace table;
using namespace db;
using namespace cache;
using namespace common;
using namespace memtable;
using namespace util;

class MinorCompactionUtilTest : public testing::Test {
 public:
  MinorCompactionUtilTest()
    : cmp_(BytewiseComparator()) {}

  Slice create_internal_key(memory::ArenaAllocator& arena, const int64_t key,
      const int64_t sequence) {
    const int64_t key_size = 20;
    char buf[128];
    memset(buf, 0, sizeof(buf));
    snprintf(buf, key_size, "%010ld", key);
    InternalKey ikey(Slice(buf, strlen(buf)), sequence, kTypeValue);
    Slice slice = ikey.Encode();
    return slice.deep_copy(arena);
  }

  MinorTask* create_minor_task(memory::ArenaAllocator& arena, const int64_t key_start,
      const int64_t key_end, const int64_t sequence) {
    CompactionContext context;
    context.internal_comparator_ = &cmp_;

    MinorTask* minor = new MinorTask();
    minor->set_compaction_context(context);

    storage::Range range;
    range.start_key_ = create_internal_key(arena, key_start, sequence);
    range.end_key_ = create_internal_key(arena, key_end, sequence);
    minor->set_range(range);

    return minor;
  }

  BlockInfo* create_block_info(memory::ArenaAllocator& arena, const int64_t key_start,
      const int64_t key_end, const int64_t sequence) {
    BlockInfo* block = new BlockInfo();
    std::string last_key;
    db::BlockStats stats;
    table::BlockBuilder builder(16);
    for (int64_t i = key_start; i < key_end; i++) {
      Slice key = create_internal_key(arena, i, sequence);
      Slice value;
      builder.Add(key, value);

      // record stats 
      stats.data_size_ += key.size() + value.size();
      stats.key_size_ += key.size(); 
      stats.value_size_ += value.size();
      stats.rows_++;
      if (stats.rows_ == 1) {
        stats.first_key_ = std::string(key.data(), key.size());
        stats.smallest_seqno_ = sequence;
      }
      last_key = std::string(key.data(), key.size());
      stats.entry_put_ += 1;

      stats.largest_seqno_ = sequence;
      //stats.actual_disk_size_
      //stats.entry_single_deletes_
      //stats.entry_others_
    }

    block->raw_block_ = builder.Finish();
    block->first_key_ = Slice(stats.first_key_).deep_copy(arena);
    block->last_key_ = Slice(last_key).deep_copy(arena);

    assert(!block->first_key_.empty());
    assert(!block->last_key_.empty());
    int64_t size = stats.get_serialize_size();
    char* tmp_str = (char*)base_malloc(size, memory::ModId::kMinorCompaction);
    int64_t end_pos = 0;
    stats.serialize(tmp_str, size, end_pos);
    block->block_stats_content_ = Slice(tmp_str, size);
    assert(!block->block_stats_content_.empty());
    
    return block;
  }

  MinorCompaction* build_minor_compaction() {
    CompactionContext context;
    context.internal_comparator_ = &cmp_;

    Options options;
    context.cf_options_ = new ImmutableCFOptions(options);
    //context.cf_options_->info_log = nullptr;
    MinorCompaction* compaction = new MinorCompaction(context, cf_desc_);
    return compaction;
  }

  bool check_clip(BlockInfo* in_block, int key_start, int key_end, int64_t seq) {
    memory::ArenaAllocator arena;
    BlockContents contents{in_block->raw_block_, false,
      common::CompressionType::kNoCompression};
    Block block_reader(std::move(contents), kDisableGlobalSequenceNumber);
    InternalKeyComparator icmp(BytewiseComparator());

    BlockIter *iter = new BlockIter;
    block_reader.NewIterator(&icmp, iter);

    int key_cnt = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      Slice ikey = create_internal_key(arena, key_start + key_cnt, seq);
      fprintf (stderr, "key_id:%d iter(%s) ikey(%s)\n", key_start + key_cnt,
          iter->key().ToString(true).c_str(),
          ikey.ToString(true).c_str());
      if (ikey.compare(iter->key()) != 0) {
        fprintf (stderr, "[not equal] key_id:%d iter(%s) ikey(%s)\n",
            key_start + key_cnt,
            iter->key().ToString(true).c_str(),
            ikey.ToString(true).c_str());
        return false;
      }
      key_cnt++;
    }
    delete iter;
    if (key_cnt != key_end - key_start) {
      return false;
    }
    return true;
  }

 private:
  InternalKeyComparator cmp_;
  ColumnFamilyDesc cf_desc_;
  CompactionContext context_;
};

/*
TEST_F(MinorCompactionUtilTest, list_push_back) {
  InternalKeyComparator icmp(BytewiseComparator());
  MinorTaskList task_list;
  memory::ArenaAllocator arena;
  for (int i = 0; i < 5; i++) {
    MinorTask* minor = create_minor_task(arena, 1000 + i * 100, 1100 + i * 100, 10 + i);
    task_list.push_back(minor);
  }

  ASSERT_EQ(task_list.size(), 5);
  for (int i = 0; i < 5; i++) {
    MinorTask* minor = task_list.pop_front(); 
    ASSERT_EQ(icmp.Compare(minor->range().start_key_,
          create_internal_key(arena, 1000 + i * 100, 10 + i)), 0);
    delete minor;
  }
  ASSERT_EQ(task_list.size(), 0);
}

TEST_F(MinorCompactionUtilTest, list_push_front_by_order) {
  InternalKeyComparator icmp(BytewiseComparator());
  MinorTaskList task_list;
  memory::ArenaAllocator arena;
  for (int i = 5; i < 10; i++) {
    MinorTask* minor = create_minor_task(arena, 1000 + i * 100, 1100 + i * 100, 10 + i);
    task_list.push_back(minor);
  }

  for (int i = 0; i < 5; i++) {
    MinorTask* minor = create_minor_task(arena, 1000 + i * 100, 1100 + i * 100, 10 + i);
    task_list.push_front_by_order(minor);
  }

  ASSERT_EQ(task_list.size(), 10);
  for (int i = 0; i < 10; i++) {
    MinorTask* minor = task_list.pop_front(); 
    ASSERT_EQ(icmp.Compare(minor->range().start_key_,
          create_internal_key(arena, 1000 + i * 100, 10 + i)), 0);
    //fprintf (stderr, "%d: [%s]\n", i,
    //    util::to_cstring(minor->range()));
    delete minor;
  }
  ASSERT_EQ(task_list.size(), 0);
}
*/

TEST_F(MinorCompactionUtilTest, test_clip_block) {
  memory::ArenaAllocator arena;
  BlockInfo* in_block = create_block_info(arena, 1000, 1050, 10);
  {
    BlockContents contents{in_block->raw_block_, false,
      common::CompressionType::kNoCompression};
    Block block_reader(std::move(contents), kDisableGlobalSequenceNumber);
    InternalKeyComparator icmp(BytewiseComparator());

    BlockIter *iter = new BlockIter;
    block_reader.NewIterator(&icmp, iter);
    int i = 0;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      fprintf (stderr, "key_id:%d iter(%s)\n", 1000 + i,
          iter->key().ToString(true).c_str());
      i++;
    }
    delete iter;
  }

  BlockInfo out_block;
  //Note we reserve buffer and free it here
  util::WritableBuffer buf;
  buf.reserve(in_block->raw_block_.size());

  storage::Range valid_range;
  valid_range.start_key_ = create_internal_key(arena, 1002, 10);
  valid_range.end_key_ = create_internal_key(arena, 1008, 10);
  MinorCompaction* compaction = build_minor_compaction();
  compaction->TEST_clip_block(in_block, valid_range, &out_block, &buf);

  ASSERT_TRUE(check_clip(&out_block, 1002, 1008, 10));

  delete in_block;
  delete compaction;
}

TEST_F(MinorCompactionUtilTest, test_clip_block1) {
  memory::ArenaAllocator arena;
  BlockInfo* in_block = create_block_info(arena, 1000, 1050, 10);

  BlockInfo out_block;
  //Note we reserve buffer and free it here
  util::WritableBuffer buf;
  buf.reserve(in_block->raw_block_.size());

  storage::Range valid_range;
  //valid_range.start_key_ = create_internal_key(arena, 1002, 10);
  valid_range.end_key_ = create_internal_key(arena, 1008, 10);
  MinorCompaction* compaction = build_minor_compaction();
  compaction->TEST_clip_block(in_block, valid_range, &out_block, &buf);

  ASSERT_TRUE(check_clip(&out_block, 1000, 1008, 10));

  delete in_block;
  delete compaction;
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
	xengine::util::test::init_logger(__FILE__);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  return RUN_ALL_TESTS();
}

