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
#include "table/sstable_scan_iterator.h"
#include "table/internal_iterator_test_base.h"
#include "db/db_test_util.h"

namespace xengine
{
using namespace storage;
using namespace db;
using namespace monitor;
using namespace common;
using namespace util;
using namespace util::test;

namespace table
{

class SSTableScanIteratorTest : public InternalIteratorTestBase
{
public:
  SSTableScanIteratorTest() : arena_(util::Arena::kMinBlockSize, 0, memory::ModId::kDbIter)
  {
    args_.db_path_ = test::TmpDir() + "/sstable_scan_iterator";
    xengine::monitor::QueryPerfContext::opt_enable_count_ = true;
  }
  virtual ~SSTableScanIteratorTest() {}

private:
  struct DataGenParam
  {
    // the number of rows in a block
    int64_t block_size_;
    // the number of blocks in an extent
    int64_t extent_size_;
    // the number of extents
    int64_t extent_count_;
  };

  void generate_data(const DataGenParam &param);
  void scan_test(const DataGenParam &param);
  void init_scan_iter(SSTableScanIterator *&scan_iter);
  void destroy_scan_iter(SSTableScanIterator *&scan_iter);

  void forward_scan_test(const DataGenParam &param);
  void backward_scan_test(const DataGenParam &param);

  void do_forward_whole_scan_test(const int64_t start_key, const int64_t expected_cnt);
  void do_backward_whole_scan_test(const int64_t start_key, const int64_t expected_cnt);
  void do_forward_scan_test(const int64_t start_key,
                            const int64_t end_key,
                            const int64_t start_value,
                            const int64_t expected_cnt);
  void do_backward_scan_test(const int64_t start_key,
                             const int64_t start_value,
                             const int64_t expected_cnt);

private:
  TestArgs args_;
  util::Arena arena_;
  IterKey saved_key_;
  IterKey saved_end_key_;
  std::unordered_map<int64_t /*key*/, ExtentId> key_source_map_;
  static const int64_t FIRST_KEY = 2;
  PinnedIteratorsManager pinned_iters_mgr_;
};

void SSTableScanIteratorTest::generate_data(const DataGenParam &param)
{
  key_source_map_.clear();
  args_.build_default_options();
  init(args_);
  int64_t row_id = 0;
  bool flush_block = false;
  for (int64_t extent_id = 0; extent_id < param.extent_count_; extent_id++) {
    open_extent_builder();
    for (int64_t block_id = 0; block_id < param.extent_size_; block_id++) {
      //// build key-source map
      //for (int64_t i = 0; i < param.block_size_ - 1; i++) {
        //key_source_map_[FIRST_KEY + row_id + i] =  ExtentId(1, extent_id + 1);
      //}
      //// append block
      //if (block_id == param.extent_size_ - 1) {
        //append_block(FIRST_KEY + row_id, FIRST_KEY + row_id + param.block_size_ - 1, true [> flush extent <]);
        //// if flush-extent is true, the last key will be in the next extent
        //key_source_map_[FIRST_KEY + row_id + param.block_size_ - 1] = ExtentId(1, extent_id + 2);
      //} else {
        //append_block(FIRST_KEY + row_id, FIRST_KEY + row_id + param.block_size_ - 1, false [> flush extent <]);
        //key_source_map_[FIRST_KEY + row_id + param.block_size_ - 1] = ExtentId(1, extent_id + 1);
      //}
      //row_id += param.block_size_;
      // build key-source map
      for (int64_t i = 0; i < param.block_size_; i++) {
        key_source_map_[FIRST_KEY + row_id + i] =  ExtentId(1, extent_id + 1);
      }
      flush_block = !(block_id == param.extent_size_ - 1);
      append_block(FIRST_KEY + row_id, FIRST_KEY + row_id + param.block_size_ - 1, flush_block);
      row_id += param.block_size_;
    }
    close_extent_builder();
  }
}

void SSTableScanIteratorTest::scan_test(const DataGenParam &param)
{
  const int64_t total_cnt = param.extent_count_ * param.extent_size_ * param.block_size_;
  do_forward_whole_scan_test(FIRST_KEY, total_cnt);
  do_backward_whole_scan_test(total_cnt + FIRST_KEY - 1, total_cnt);
  forward_scan_test(param);
  backward_scan_test(param);
}

void SSTableScanIteratorTest::init_scan_iter(SSTableScanIterator *&scan_iter)
{
  scan_iter = new SSTableScanIterator();
  ReadOptions read_options;

  ExtentLayerVersion *extent_layer_version = nullptr;
  ExtentLayer *extent_layer = nullptr;
  //extent_layer_version = storage_manager_->get_current_version()->get_level_version(level_);
  extent_layer_version = storage_manager_->get_current_version()->get_extent_layer_version(level_);
  ASSERT_TRUE(nullptr != extent_layer_version);
  extent_layer = extent_layer_version->get_extent_layer(0);
  ASSERT_TRUE(nullptr != extent_layer);

  ScanParam param;
  param.table_cache_ = table_cache_.get();
  param.read_options_ = &read_options;
  param.env_options_ = &context_->env_options_;
  param.icomparator_ = &internal_comparator_;
  param.file_read_hist_ = nullptr;
  param.for_compaction_ = false;
  param.skip_filters_ = false;
  param.layer_position_ = LayerPosition(level_, 0);
  param.range_del_agg_ = nullptr;
  param.subtable_id_ = 1;
  param.internal_stats_ = nullptr;
  param.extent_layer_ = extent_layer;
  param.arena_ = &arena_;
  param.scan_add_blocks_limit_ = 100;

  ASSERT_EQ(scan_iter->init(param), Status::kOk);
  scan_iter->SetPinnedItersMgr(&pinned_iters_mgr_);
}

void SSTableScanIteratorTest::destroy_scan_iter(SSTableScanIterator *&scan_iter)
{
  if (nullptr != scan_iter) {
    delete scan_iter;
    scan_iter = nullptr;
  }
}

void SSTableScanIteratorTest::forward_scan_test(const DataGenParam &param)
{
  const int64_t total_cnt = param.extent_count_ * param.extent_size_ * param.block_size_;
  // 1. scan from the key smaller than the start key
  do_forward_scan_test(FIRST_KEY - 1,
                       FIRST_KEY + total_cnt,
                       FIRST_KEY,
                       total_cnt);
  // 2. scan from then start key
  do_forward_scan_test(FIRST_KEY,
                       FIRST_KEY + total_cnt,
                       FIRST_KEY,
                       total_cnt);
  // 3. scan from the key which is the first row of a block
  if (param.extent_size_ > 1) {
    do_forward_scan_test(FIRST_KEY + param.block_size_ - 1,
                         FIRST_KEY + total_cnt,
                         FIRST_KEY + param.block_size_ - 1,
                         total_cnt - param.block_size_ + 1);
  }
  // 4. scan from the key which is the last row of a block
  if (param.extent_size_ > 1) {
    do_forward_scan_test(FIRST_KEY + param.block_size_ * 2 - 2,
                         FIRST_KEY + total_cnt,
                         FIRST_KEY + param.block_size_ * 2 - 2,
                         total_cnt - param.block_size_ * 2 + 2);
  }
  // 5. scan from the key which is the first row of an extent
  if (param.extent_count_ > 1) {
    do_forward_scan_test(FIRST_KEY + param.block_size_ * param.extent_size_ - 1,
                         FIRST_KEY + total_cnt,
                         FIRST_KEY + param.block_size_ * param.extent_size_ - 1,
                         total_cnt - param.block_size_ * param.extent_size_ + 1);
  }
  // 6. scan from the key which is the last row of an extent
  if (param.extent_count_ > 1) {
    do_forward_scan_test(FIRST_KEY + param.block_size_ * param.extent_size_ * 2 - 2,
                         FIRST_KEY + total_cnt,
                         FIRST_KEY + param.block_size_ * param.extent_size_ * 2 - 2,
                         total_cnt - param.block_size_ * param.extent_size_ * 2 + 2);
  }
  // 7. scan from then last key
  do_forward_scan_test(FIRST_KEY + total_cnt - 1,
                       FIRST_KEY + total_cnt,
                       FIRST_KEY + total_cnt - 1,
                       1);
  // 8. scan from the key larger than the last key
  do_forward_scan_test(FIRST_KEY + total_cnt,
                       FIRST_KEY + total_cnt,
                       0,
                       0);
}

void SSTableScanIteratorTest::backward_scan_test(const DataGenParam &param)
{
  const int64_t total_cnt = param.extent_count_ * param.extent_size_ * param.block_size_;
  const int64_t last_key = FIRST_KEY + total_cnt - 1;
  // 1. scan from the key smaller than the first key
  do_backward_scan_test(FIRST_KEY - 1,
                        0,
                        0);
  // 2. scan from then first key
  do_backward_scan_test(FIRST_KEY,
                        0,
                        0);
  // 3. scan from the key which is the first row of a block
  if (param.extent_size_ > 1) {
    do_backward_scan_test(last_key - param.block_size_,
                          last_key - param.block_size_ - 1,
                          total_cnt - param.block_size_ - 1);
  }
  // 4. scan from the key which is the last row of a block
  if (param.extent_size_ > 1) {
    do_backward_scan_test(last_key - param.block_size_ - 1,
                          last_key - param.block_size_ - 2,
                          total_cnt - param.block_size_ - 2);
  }
  // 5. scan from the key which is the first row of an extent
  if (param.extent_count_ > 1) {
    do_backward_scan_test(last_key - param.block_size_ * param.extent_size_,
                          last_key - param.block_size_ * param.extent_size_ - 1,
                          total_cnt - param.block_size_ * param.extent_size_ - 1);
  }
  // 6. scan from the key which is the last row of an extent
  if (param.extent_count_ > 1) {
    do_backward_scan_test(last_key - param.block_size_ * param.extent_size_ - 1,
                          last_key - param.block_size_ * param.extent_size_ - 2,
                          total_cnt - param.block_size_ * param.extent_size_ - 2);
  }
  // 7. scan from then last key
  do_backward_scan_test(last_key,
                        last_key - 1,
                        total_cnt - 1);
  // 8. scan from the key larger than the last key
  do_backward_scan_test(last_key + 1,
                        last_key,
                        total_cnt);
}

void SSTableScanIteratorTest::do_forward_whole_scan_test(const int64_t start_key, const int64_t expected_cnt)
{
  SSTableScanIterator *scan_iter = nullptr;
  char key_buf[key_size_];
  char row_buf[row_size_];
  init_scan_iter(scan_iter);
  scan_iter->SeekToFirst();
  int64_t scan_cnt = 0;
  while (scan_iter->Valid()) {
    make_value(row_buf, row_size_, start_key + scan_cnt);
    //XENGINE_LOG(WARN, "test", K(scan_iter->value()), K(Slice(row_buf, row_size_)));
    ASSERT_TRUE(Slice(row_buf, row_size_) == scan_iter->value());
    scan_iter->Next();
    scan_cnt++;
  }
  ASSERT_EQ(expected_cnt, scan_cnt);
  destroy_scan_iter(scan_iter);
  // make sure cache handles have been released
  ASSERT_EQ(0, args_.table_options_.block_cache->GetPinnedUsage());
  ASSERT_EQ(0, clock_cache_->GetPinnedUsage());
}

void SSTableScanIteratorTest::do_backward_whole_scan_test(const int64_t start_key, const int64_t expected_cnt)
{
  SSTableScanIterator *scan_iter = nullptr;
  char key_buf[key_size_];
  char row_buf[row_size_];
  init_scan_iter(scan_iter);
  scan_iter->SeekToLast();
  int64_t scan_cnt = 0;
  pinned_iters_mgr_.StartPinning();
  while (scan_iter->Valid()) {
    make_value(row_buf, row_size_, start_key - scan_cnt);
    //XENGINE_LOG(WARN, "test", K(scan_iter->value()), K(Slice(row_buf, row_size_)));
    ASSERT_TRUE(Slice(row_buf, row_size_) == scan_iter->value());
    scan_iter->Prev();
    scan_cnt++;
  }
  pinned_iters_mgr_.ReleasePinnedData();

  ASSERT_EQ(expected_cnt, scan_cnt) << "start key: " << start_key;
  destroy_scan_iter(scan_iter);

  // make sure cache handles have been released
  ASSERT_EQ(0, args_.table_options_.block_cache->GetPinnedUsage());
  ASSERT_EQ(0, clock_cache_->GetPinnedUsage());
}

void SSTableScanIteratorTest::do_forward_scan_test(const int64_t start_key,
                                                   const int64_t end_key,
                                                   const int64_t start_value,
                                                   const int64_t expected_cnt)
{
  SSTableScanIterator *scan_iter = nullptr;
  char key_buf[key_size_];
  char row_buf[row_size_];
  memset(row_buf, 0, row_size_);
  saved_key_.Clear();
  saved_end_key_.Clear();

  init_scan_iter(scan_iter);

  make_key(key_buf, key_size_, end_key);
  saved_end_key_.SetInternalKey(Slice(key_buf, strlen(key_buf)), (SequenceNumber)(0ULL) /* include end key*/);
  scan_iter->set_end_key(saved_end_key_.GetInternalKey(), true);

  make_key(key_buf, key_size_, start_key);
  saved_key_.SetInternalKey(Slice(key_buf, strlen(key_buf)), kMaxSequenceNumber);
  scan_iter->Seek(Slice(saved_key_.GetInternalKey()));

  int64_t scan_cnt = 0;
  while (scan_iter->Valid()) {
    make_value(row_buf, row_size_, start_value + scan_cnt);
    //XENGINE_LOG(WARN, "test", K(scan_iter->value()), K(Slice(row_buf, row_size_)), K(scan_iter->get_source()));
    ASSERT_EQ(scan_iter->get_source() & 0xFFFFFFFF, key_source_map_[start_value + scan_cnt].offset) 
        << "start_key: " << start_key << ", end key: " << end_key << ", start value" << start_value << ", scan_cnt" << scan_cnt;
    ASSERT_TRUE(Slice(row_buf, row_size_) == scan_iter->value());
    scan_iter->Next();
    scan_cnt++;
  }

  ASSERT_EQ(expected_cnt, scan_cnt) << "start key: " << start_key << ", end key: " << end_key;

  destroy_scan_iter(scan_iter);
  // make sure cache handles have been released
  ASSERT_EQ(0, args_.table_options_.block_cache->GetPinnedUsage());
  ASSERT_EQ(0, clock_cache_->GetPinnedUsage());
}

void SSTableScanIteratorTest::do_backward_scan_test(const int64_t start_key,
                                                    const int64_t start_value,
                                                    const int64_t expected_cnt)
{
  SSTableScanIterator *scan_iter = nullptr;
  char key_buf[key_size_];
  char row_buf[row_size_];
  memset(row_buf, 0, row_size_);
  saved_key_.Clear();

  init_scan_iter(scan_iter);

  make_key(key_buf, key_size_, start_key);
  saved_key_.SetInternalKey(Slice(key_buf, strlen(key_buf)), kMaxSequenceNumber);
  scan_iter->Seek(Slice(saved_key_.GetInternalKey()));

  pinned_iters_mgr_.StartPinning();

  // find details in rocksdb-range-access.txt, grep HA_READ_BEFORE_KEY
  if (scan_iter->Valid()) {
    scan_iter->Prev();
  } else {
    scan_iter->SeekToLast();
  }

  int64_t scan_cnt = 0;
  while (scan_iter->Valid()) {
    make_value(row_buf, row_size_, start_value - scan_cnt);
    //XENGINE_LOG(WARN, "test", K(scan_iter->value()), K(Slice(row_buf, row_size_)));
    ASSERT_TRUE(Slice(row_buf, row_size_) == scan_iter->value());
    scan_cnt++;
    scan_iter->Prev();
  }

  pinned_iters_mgr_.ReleasePinnedData();

  ASSERT_EQ(expected_cnt, scan_cnt) << "start key: " << start_key;
  destroy_scan_iter(scan_iter);

  // make sure cache handles have been released
  ASSERT_EQ(0, args_.table_options_.block_cache->GetPinnedUsage());
  ASSERT_EQ(0, clock_cache_->GetPinnedUsage());
}

TEST_F(SSTableScanIteratorTest, single_block)
{
  DataGenParam param;
  param.block_size_ = 3;
  param.extent_size_ = 1;
  param.extent_count_ = 1;
  generate_data(param);
  scan_test(param);
}

TEST_F(SSTableScanIteratorTest, multi_block)
{
  DataGenParam param;
  param.block_size_ = 3;
  param.extent_size_ = 4;
  param.extent_count_ = 1;
  generate_data(param);
  scan_test(param);
}

TEST_F(SSTableScanIteratorTest, multi_block_2)
{
  DataGenParam param;
  param.block_size_ = 3;
  param.extent_size_ = 64;
  param.extent_count_ = 1;
  generate_data(param);
  scan_test(param);
}

TEST_F(SSTableScanIteratorTest, multi_extent)
{
  DataGenParam param;
  param.block_size_ = 3;
  param.extent_size_ = 4;
  param.extent_count_ = 3;
  generate_data(param);
  scan_test(param);
}

TEST_F(SSTableScanIteratorTest, multi_extent_2)
{
  DataGenParam param;
  param.block_size_ = 3;
  param.extent_size_ = 64;
  param.extent_count_ = 32;
  generate_data(param);
  scan_test(param);
}

TEST_F(SSTableScanIteratorTest, add_block_limit)
{
  DataGenParam param;
  param.block_size_ = 3;
  param.extent_size_ = 64;
  param.extent_count_ = 32;
  generate_data(param);

  const int64_t total_cnt = param.extent_count_ * param.extent_size_ * param.block_size_;
  const int64_t last_key = FIRST_KEY + total_cnt - 1;

  int64_t before = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  do_forward_scan_test(FIRST_KEY,
                       FIRST_KEY + total_cnt,
                       FIRST_KEY,
                       total_cnt);
  int64_t after = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  // ExtentBasedTable::SCAN_ADD_BLOCKS_LIMIT is 100
  ASSERT_EQ(100, after - before);

  before = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  do_backward_scan_test(last_key + 1,
                        last_key,
                        total_cnt);
  after = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  // ExtentBasedTable::SCAN_ADD_BLOCKS_LIMIT is 100
  ASSERT_EQ(100, after - before);

}

// this case might fail if the strategy of io merge changes
TEST_F(SSTableScanIteratorTest, io_merge_forward)
{
  /*
   * /------------ extent 1 -------------\ /------------ extent 2 -----------\ /------------ extent 3 -------------\
   * |  io  | cache |  io  | cache |  io  |  io  |  io  | cache |  io  |  io  | cache | cache |  io  |  io  | cache |
   *   2-3    4-6     7-9    10-12   13-16  17-18  19-21  22-24   25-27  28-31  32-33   34-36   37-39  40-42  43-45
  */

  DataGenParam param;
  param.block_size_ = 3;
  param.extent_size_ = 5;
  param.extent_count_ = 3;
  generate_data(param);

  int64_t before = 0;
  int64_t after = 0;
  // add blocks to cache
  before = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  do_forward_scan_test(4, 5, 4, 2);
  after = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  ASSERT_EQ(1, after - before);

  before = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  do_forward_scan_test(10, 11, 10, 2);
  after = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  ASSERT_EQ(1, after - before);

  before = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  do_forward_scan_test(22, 23, 22, 2);
  after = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  ASSERT_EQ(1, after - before);

  before = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  do_forward_scan_test(32, 32, 32, 1);
  after = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  ASSERT_EQ(1, after - before);

  before = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  do_forward_scan_test(34, 35, 34, 2);
  after = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  ASSERT_EQ(1, after - before);

  before = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  do_forward_scan_test(43, 44, 43, 2);
  after = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  ASSERT_EQ(1, after - before);

  //
  int64_t before_aio = 0;
  int64_t after_aio = 0;
  before = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  before_aio = TestGetGlobalCount(CountPoint::AIO_REQUEST_SUBMIT);
  do_forward_scan_test(2, 44, 2, 43);
  after = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  after_aio = TestGetGlobalCount(CountPoint::AIO_REQUEST_SUBMIT);
  ASSERT_EQ(9, after - before);
  ASSERT_EQ(6, after_aio - before_aio);
}

// this case might fail if the strategy of io merge changes
TEST_F(SSTableScanIteratorTest, io_merge_backward)
{
  /*
   * /------------ extent 1 -------------\ /------------ extent 2 -----------\ /------------ extent 3 -------------\
   * |  io  | cache |  io  | cache |  io  |  io  |  io  | cache |  io  |  io  | cache | cache |  io  |  io  | cache |
   *   2-3    4-6     7-9    10-12   13-16  17-18  19-21  22-24   25-27  28-31  32-33   34-36   37-39  40-42  43-45
  */

  DataGenParam param;
  param.block_size_ = 3;
  param.extent_size_ = 5;
  param.extent_count_ = 3;
  generate_data(param);

  int64_t before = 0;
  int64_t after = 0;
  // add blocks to cache
  before = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  do_forward_scan_test(4, 5, 4, 2);
  after = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  ASSERT_EQ(1, after - before);

  before = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  do_forward_scan_test(10, 11, 10, 2);
  after = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  ASSERT_EQ(1, after - before);

  before = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  do_forward_scan_test(22, 23, 22, 2);
  after = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  ASSERT_EQ(1, after - before);

  before = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  do_forward_scan_test(32, 32, 32, 1);
  after = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  ASSERT_EQ(1, after - before);

  before = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  do_forward_scan_test(34, 35, 34, 2);
  after = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  ASSERT_EQ(1, after - before);

  before = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  do_forward_scan_test(43, 44, 43, 2);
  after = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  ASSERT_EQ(1, after - before);

  //
  int64_t before_aio = 0;
  int64_t after_aio = 0;
  before = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  before_aio = TestGetGlobalCount(CountPoint::AIO_REQUEST_SUBMIT);
  do_backward_scan_test(44, 43, 42);
  after = TestGetGlobalCount(CountPoint::BLOCK_CACHE_DATA_ADD);
  after_aio = TestGetGlobalCount(CountPoint::AIO_REQUEST_SUBMIT);
  ASSERT_EQ(9, after - before);
  ASSERT_EQ(9, after_aio - before_aio);
}
} // namespace table
} // namespace xengine

int main(int argc, char** argv) {
  //port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
	xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
