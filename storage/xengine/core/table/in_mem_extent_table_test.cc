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
#include "db/table_properties_collector.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "options/cf_options.h"
#include "storage/extent_space_manager.h"
#include "storage/io_extent.h"
#include "storage/storage_logger.h"
#include "table/extent_table_reader.h"
#include "table/get_context.h"
#include "table/table_builder.h"
#include "util/aio_wrapper.h"
#include "util/testharness.h"

using namespace xengine;
using namespace storage;
using namespace util;
using namespace common;
using namespace db;

static const std::string test_dir = xengine::util::test::TmpDir() + "/in_mem_extent_table_test";

namespace xengine {
namespace table {

static std::string GetFromFile(TableReader* table_reader,
                               const std::string& key, ReadOptions& ro,
                               const Comparator* comparator) {
  PinnableSlice value;
  GetContext get_context(comparator, nullptr, nullptr,
                         GetContext::kNotFound, Slice(key), &value, nullptr,
                         nullptr, nullptr, nullptr);
  LookupKey lk{key, kMaxSequenceNumber};
  table_reader->Get(ro, lk.internal_key(), &get_context);
  return std::string(value.data(), value.size());
}

static void get_data_block(TableReader* table_reader, const Slice& key,
                           unique_ptr<char[]>& block, size_t& size) {
  auto reader = dynamic_cast<ExtentBasedTable*>(table_reader);
  BlockIter iiter_on_stack;
  ExtentBasedTable::IndexReader* index_reader = nullptr;
  memory::ArenaAllocator alloc;
  auto iiter = reader->create_index_iterator(ReadOptions(), &iiter_on_stack,
                                             index_reader, alloc);
  std::unique_ptr<InternalIterator> iiter_unique_ptr;
  if (iiter != &iiter_on_stack) {
    iiter_unique_ptr = std::unique_ptr<InternalIterator>(iiter);
  }
  EXPECT_TRUE(iiter->status().ok());
  iiter->Seek(key);
  EXPECT_TRUE(iiter->Valid());

  BlockHandle handle;
  Slice input = iiter->value();
  Status s = handle.DecodeFrom(&input);
  EXPECT_TRUE(s.ok());

  size = handle.size() + kBlockTrailerSize;
  block.reset(new char[size]);
  Slice data_block(block.get(), size);
  int ret = reader->get_data_block(handle, data_block, true);
  EXPECT_EQ(ret, Status::kOk);
  if (block.get() != data_block.data()) {
    memcpy(block.get(), data_block.data(), size);
  }
}

TEST(InMemExtent, sim) {
  // dirty preparation
  storage::ChangeInfo change_info;
  MiniTables mtables;
  mtables.change_info_ = &change_info;
  unique_ptr<TableBuilder> builder;
  std::vector<std::unique_ptr<IntTblPropCollectorFactory>>
      int_tbl_prop_collector_factories;
  std::string column_family_name;
  storage::LayerPosition output_layer_position;
  BlockBasedTableOptions table_options;
  Options options;
  options.table_factory.reset(NewExtentBasedTableFactory(table_options));
  const ImmutableCFOptions ioptions(options);
  const InternalKeyComparator internal_comparator(options.comparator);
  EnvOptions soptions;
  ReadOptions ro;
  Env* env = Env::Default();
  std::string dbname = test_dir;
  Status s;
  EXPECT_TRUE(s.ok()) << s.ToString();
  unique_ptr<ExtentSpaceManager> space_manager;
  FileNumber file_number(2000);
  options.db_paths.emplace_back(dbname, 0);
  space_manager.reset(new ExtentSpaceManager(env, soptions, options));
  StorageLogger *storage_logger = new StorageLogger();
  ImmutableDBOptions doption;
  VersionSet *vs = nullptr;
  vs = new VersionSet(dbname, &doption, soptions, nullptr, nullptr, nullptr);

  storage_logger->init(env, dbname, soptions, doption, vs, space_manager.get(), 1 * 1024 * 1024 * 1024);
  space_manager->init(storage_logger);
  int ret = Status::kOk;
  ret = space_manager->create_table_space(0);
  ASSERT_EQ(Status::kOk, ret);
  mtables.space_manager = space_manager.get();
  mtables.table_space_id_ = 0;
  storage_logger->begin(storage::XengineEvent::FLUSH);
  builder.reset(ioptions.table_factory->NewTableBuilderExt(
      TableBuilderOptions(
          ioptions, internal_comparator, &int_tbl_prop_collector_factories,
          options.compression, CompressionOptions(),
          nullptr /* compression_dict */, false /* skip_filters */,
          column_family_name, output_layer_position),
      TablePropertiesCollectorFactory::Context::kUnknownColumnFamily,
      &mtables));

  // create an table/extent with one record
  InternalKey key("key", 0, kTypeValue);
  std::string value("val");
  builder->Add(key.Encode().ToString(), value);
  s = builder->Finish();
  EXPECT_TRUE(s.ok()) << s.ToString();

  ExtentId eid(mtables.metas[0].fd.GetNumber());

  unique_ptr<char[]> block1;
  size_t size1;
  {
    // method 1: read it normally
//    unique_ptr<RandomAccessExtent> extent(new storage::RandomAccessExtent());
    RandomAccessExtent *extent = MOD_NEW_OBJECT(memory::ModId::kDefaultMod, RandomAccessExtent);
    s = space_manager->get_random_access_extent(eid, *extent);
    EXPECT_TRUE(s.ok()) << s.ToString();

//    unique_ptr<RandomAccessFileReader> file_reader(
//        new RandomAccessFileReader(extent));
    RandomAccessFileReader *file_reader = MOD_NEW_OBJECT(memory::ModId::kDefaultMod, RandomAccessFileReader, extent);
//    unique_ptr<TableReader> table_reader;
    TableReader *table_reader = nullptr;
    s = ioptions.table_factory->NewTableReader(
        TableReaderOptions(ioptions, soptions, internal_comparator),
        file_reader, MAX_EXTENT_SIZE, table_reader);
    EXPECT_TRUE(s.ok()) << s.ToString();

    // verify
    std::string v =
        GetFromFile(table_reader, "key", ro, options.comparator);
    ASSERT_EQ(v, value);
    v = GetFromFile(table_reader, "ke", ro, options.comparator);
    ASSERT_EQ(v.size(), 0);  // not exist
    v = GetFromFile(table_reader, "keyx", ro, options.comparator);
    ASSERT_EQ(v.size(), 0);  // not exist

    get_data_block(table_reader, key.Encode(), block1, size1);
  }

  unique_ptr<char[]> block2;
  size_t size2;
  {
    // method 2: read it using the new added mem interface
    const int size = MAX_EXTENT_SIZE;

//    unique_ptr<AsyncRandomAccessExtent> extent(new AsyncRandomAccessExtent());
    AsyncRandomAccessExtent *extent = MOD_NEW_OBJECT(memory::ModId::kDefaultMod, AsyncRandomAccessExtent);
    s = space_manager->get_random_access_extent(eid, *extent);
    EXPECT_TRUE(s.ok()) << s.ToString();

    // async read it
    // TODO: to be removed
    //AsyncExtentBuffer arb;
    //arb.reserve(PAGE_SIZE, size);
    //extent->set_read_buffer(&arb);
    extent->prefetch();

    // the mem interface
//    unique_ptr<RandomAccessFileReader> in_mem_file_reader(
//        new RandomAccessFileReader(std::move(extent)));
    RandomAccessFileReader *in_mem_file_reader = MOD_NEW_OBJECT(memory::ModId::kDefaultMod, RandomAccessFileReader, extent);
//    unique_ptr<TableReader> in_mem_table_reader;
    TableReader *in_mem_table_reader = nullptr;
    s = ioptions.table_factory->NewTableReader(
        TableReaderOptions(ioptions, soptions, internal_comparator),
        in_mem_file_reader, MAX_EXTENT_SIZE, in_mem_table_reader);
    EXPECT_TRUE(s.ok()) << s.ToString();

    // verify
    std::string v =
        GetFromFile(in_mem_table_reader, "key", ro, options.comparator);
    ASSERT_EQ(v, value);
    v = GetFromFile(in_mem_table_reader, "ke", ro, options.comparator);
    ASSERT_EQ(v.size(), 0);  // not exist
    v = GetFromFile(in_mem_table_reader, "keyx", ro, options.comparator);
    ASSERT_EQ(v.size(), 0);  // not exist

    get_data_block(in_mem_table_reader, key.Encode(), block2, size2);
  }
  ASSERT_EQ(size1, size2);
  ASSERT_EQ(memcmp(block1.get(), block2.get(), size1), 0);
  delete vs;
  delete storage_logger;
}

}  // table
}  // xengine

int main(int argc, char** argv) {
   std::string log_path = xengine::util::test::TmpDir() + "/in_mem_extent_table_test.log";
   xengine::logger::Logger::get_log().init(log_path.c_str(), xengine::logger::DEBUG_LEVEL);
   xengine::util::test::remove_dir(test_dir.c_str());
   xengine::util::Env::Default()->CreateDir(test_dir);
  ::testing::InitGoogleTest(&argc, argv);
//	xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
