// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <inttypes.h>
#include <stdio.h>

#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "db/write_controller.h"
#include "memtable/stl_wrappers.h"
#include "monitoring/statistics.h"
#include "port/port.h"
#include "storage/extent_space_manager.h"
#include "storage/io_extent.h"
#include "table/block.h"
#include "table/block_based_table_builder.h"
#include "table/block_based_table_factory.h"
#include "table/block_based_table_reader.h"
#include "table/block_builder.h"
#include "table/extent_table_builder.h"
#include "table/extent_table_factory.h"
#include "table/extent_table_reader.h"
#include "table/format.h"
#include "table/get_context.h"
#include "table/internal_iterator.h"
#include "table/meta_blocks.h"
#include "table/plain_table_factory.h"
#include "table/scoped_arena_iterator.h"
#include "table/sst_file_writer_collectors.h"
#include "util/compression.h"
#include "util/crc32c.h"
#include "util/filename.h"
#include "util/random.h"
#include "util/string_util.h"
#include "util/sync_point.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "util/xxhash.h"
#include "utilities/merge_operators.h"
#include "xengine/cache.h"
#include "xengine/db.h"
#include "xengine/env.h"
#include "xengine/iterator.h"
#include "xengine/memtablerep.h"
#include "xengine/perf_context.h"
#include "xengine/slice_transform.h"
#include "xengine/statistics.h"
#include "xengine/write_buffer_manager.h"

using namespace xengine;
using namespace common;
using namespace util;
using namespace db;
using namespace cache;
using namespace monitor;

namespace xengine {
namespace table {

extern const uint64_t kLegacyBlockBasedTableMagicNumber;
extern const uint64_t kLegacyPlainTableMagicNumber;
extern const uint64_t kBlockBasedTableMagicNumber;
extern const uint64_t kPlainTableMagicNumber;

namespace {

// DummyPropertiesCollector used to test BlockBasedTableProperties
class DummyPropertiesCollector : public TablePropertiesCollector {
 public:
  const char* Name() const { return ""; }

  Status Finish(UserCollectedProperties* properties) { return Status::OK(); }

  Status Add(const Slice& user_key, const Slice& value) { return Status::OK(); }

  virtual UserCollectedProperties GetReadableProperties() const {
    return UserCollectedProperties{};
  }
};

class DummyPropertiesCollectorFactory1
    : public TablePropertiesCollectorFactory {
 public:
  virtual TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context context) {
    return new DummyPropertiesCollector();
  }
  const char* Name() const { return "DummyPropertiesCollector1"; }
};

class DummyPropertiesCollectorFactory2
    : public TablePropertiesCollectorFactory {
 public:
  virtual TablePropertiesCollector* CreateTablePropertiesCollector(
      TablePropertiesCollectorFactory::Context context) {
    return new DummyPropertiesCollector();
  }
  const char* Name() const { return "DummyPropertiesCollector2"; }
};

// Return reverse of "key".
// Used to test non-lexicographic comparators.
std::string Reverse(const Slice& key) {
  auto rev = key.ToString();
  std::reverse(rev.begin(), rev.end());
  return rev;
}

class ReverseKeyComparator : public Comparator {
 public:
  virtual const char* Name() const override {
    return "rocksdb.ReverseBytewiseComparator";
  }

  virtual int Compare(const Slice& a, const Slice& b) const override {
    return BytewiseComparator()->Compare(Reverse(a), Reverse(b));
  }

  virtual void FindShortestSeparator(std::string* start,
                                     const Slice& limit) const override {
    std::string s = Reverse(*start);
    std::string l = Reverse(limit);
    BytewiseComparator()->FindShortestSeparator(&s, l);
    *start = Reverse(s);
  }

  virtual void FindShortSuccessor(std::string* key) const override {
    std::string s = Reverse(*key);
    BytewiseComparator()->FindShortSuccessor(&s);
    *key = Reverse(s);
  }
};

ReverseKeyComparator reverse_key_comparator;

void Increment(const Comparator* cmp, std::string* key) {
  if (cmp == BytewiseComparator()) {
    key->push_back('\0');
  } else {
    assert(cmp == &reverse_key_comparator);
    std::string rev = Reverse(*key);
    rev.push_back('\0');
    *key = Reverse(rev);
  }
}

}  // namespace

// Helper class for tests to unify the interface between
// BlockBuilder/TableBuilder and Block/Table.
class Constructor {
 public:
  explicit Constructor(const Comparator* cmp)
      : data_(stl_wrappers::LessOfComparator(cmp)) {}
  virtual ~Constructor() {}

  void Add(const std::string& key, const Slice& value) {
    data_[key] = value.ToString();
  }

  virtual void DumpSink(std::string test_name) {}

  // Use the options to generate a TableBuilder.
  virtual unique_ptr<TableBuilder> GenBuilder(
      const Options& options, const ImmutableCFOptions& ioptions,
      const BlockBasedTableOptions& table_options,
      const InternalKeyComparator& internal_comparator) {
    return {nullptr};
  }

  virtual TableReader* GenReader(
      const ImmutableCFOptions& ioptions,
      const InternalKeyComparator& internal_comparator) {
    return nullptr;
  }

  // Finish constructing the data structure with all the keys that have
  // been added so far.  Returns the keys in sorted order in "*keys"
  // and stores the key/value pairs in "*kvmap"
  void Finish(const Options& options, const ImmutableCFOptions& ioptions,
              const BlockBasedTableOptions& table_options,
              const InternalKeyComparator& internal_comparator,
              std::vector<std::string>* keys, stl_wrappers::KVMap* kvmap) {
    last_internal_key_ = &internal_comparator;
    *kvmap = data_;
    keys->clear();
    for (const auto& kv : data_) {
      keys->push_back(kv.first);
    }
    data_.clear();
    Status s = FinishImpl(options, ioptions, table_options, internal_comparator,
                          *kvmap);
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  // Construct the data structure from the data in "data"
  virtual Status FinishImpl(const Options& options,
                            const ImmutableCFOptions& ioptions,
                            const BlockBasedTableOptions& table_options,
                            const InternalKeyComparator& internal_comparator,
                            const stl_wrappers::KVMap& data) = 0;

  virtual InternalIterator* NewIterator() const = 0;

  virtual const stl_wrappers::KVMap& data() { return data_; }

  virtual bool IsArenaMode() const { return false; }

  virtual DB* db() const { return nullptr; }  // Overridden in DBConstructor

  virtual bool AnywayDeleteIterator() const { return false; }

 protected:
  const InternalKeyComparator* last_internal_key_;

 private:
  stl_wrappers::KVMap data_;
};

class BlockConstructor : public Constructor {
 public:
  explicit BlockConstructor(const Comparator* cmp)
      : Constructor(cmp), comparator_(cmp), block_(nullptr) {}
  ~BlockConstructor() { delete block_; }
  virtual Status FinishImpl(const Options& options,
                            const ImmutableCFOptions& ioptions,
                            const BlockBasedTableOptions& table_options,
                            const InternalKeyComparator& internal_comparator,
                            const stl_wrappers::KVMap& kv_map) override {
    delete block_;
    block_ = nullptr;
    BlockBuilder builder(table_options.block_restart_interval);

    for (const auto kv : kv_map) {
      builder.Add(kv.first, kv.second);
    }
    // Open the block
    data_ = builder.Finish().ToString();
    BlockContents contents;
    contents.data = data_;
    contents.cachable = false;
    block_ = new Block(std::move(contents), kDisableGlobalSequenceNumber);
    return Status::OK();
  }
  virtual InternalIterator* NewIterator() const override {
    return block_->NewIterator(comparator_);
  }

 private:
  const Comparator* comparator_;
  std::string data_;
  Block* block_;

  BlockConstructor();
};

// A helper class that converts internal format keys into user keys
class KeyConvertingIterator : public InternalIterator {
 public:
  explicit KeyConvertingIterator(InternalIterator* iter,
                                 bool arena_mode = false)
      : iter_(iter), arena_mode_(arena_mode) {}
  virtual ~KeyConvertingIterator() {
    if (arena_mode_) {
      iter_->~InternalIterator();
    } else {
      delete iter_;
    }
  }
  virtual bool Valid() const override { return iter_->Valid(); }
  virtual void Seek(const Slice& target) override {
    ParsedInternalKey ikey(target, kMaxSequenceNumber, kTypeValue);
    std::string encoded;
    AppendInternalKey(&encoded, ikey);
    iter_->Seek(encoded);
  }
  virtual void SeekForPrev(const Slice& target) override {
    ParsedInternalKey ikey(target, kMaxSequenceNumber, kTypeValue);
    std::string encoded;
    AppendInternalKey(&encoded, ikey);
    iter_->SeekForPrev(encoded);
  }
  virtual void SeekToFirst() override { iter_->SeekToFirst(); }
  virtual void SeekToLast() override { iter_->SeekToLast(); }
  virtual void Next() override { iter_->Next(); }
  virtual void Prev() override { iter_->Prev(); }

  virtual Slice key() const override {
    assert(Valid());
    ParsedInternalKey parsed_key;
    if (!ParseInternalKey(iter_->key(), &parsed_key)) {
      status_ = Status::Corruption("malformed internal key");
      return Slice("corrupted key");
    }
    return parsed_key.user_key;
  }

  virtual Slice value() const override { return iter_->value(); }
  virtual Status status() const override {
    return status_.ok() ? iter_->status() : status_;
  }

 private:
  mutable Status status_;
  InternalIterator* iter_;
  bool arena_mode_;

  // No copying allowed
  KeyConvertingIterator(const KeyConvertingIterator&);
  void operator=(const KeyConvertingIterator&);
};

class ExtentTableConstructor : public Constructor {
 public:
  explicit ExtentTableConstructor(const Comparator* cmp,
                                  bool convert_to_internal_key = false,
                                  std::string test_name = "")
      : Constructor(cmp),
        convert_to_internal_key_(convert_to_internal_key),
        cf_name_("cftest"),
        test_name_(test_name),
        env_(Env::Default()),
        table_cache_(NewLRUCache(50000, 16)),
        write_buffer_manager_(db_options_.db_write_buffer_size),
        versions_(dbname_, &db_options_, soptions_, table_cache_.get(),
                  &write_buffer_manager_, &write_controller_) {
    Status status = env_->GetTestDirectory(&dbname_);
    assert(status.ok());

    options_.db_paths.emplace_back(dbname_, 0);
    space_manager_.reset(new storage::ExtentSpaceManager(
        options_, versions_.get_file_number_generator()));

    db_paths_.emplace_back(dbname_, 0);
    mtables_.io_priority = Env::IO_LOW;
    mtables_.space_manager = space_manager_.get();
  }

  ~ExtentTableConstructor() { Reset(); }

  virtual unique_ptr<TableBuilder> GenBuilder(
      const Options& options, const ImmutableCFOptions& ioptions,
      const BlockBasedTableOptions& table_options,
      const InternalKeyComparator& internal_comparator) override {
    Reset();
    soptions_.use_mmap_reads = ioptions.allow_mmap_reads;

    unique_ptr<TableBuilder> builder;

    std::vector<std::unique_ptr<IntTblPropCollectorFactory>>
        int_tbl_prop_collector_factories;

    builder.reset(ioptions.table_factory->NewTableBuilderExt(
        TableBuilderOptions(ioptions, internal_comparator,
                            &int_tbl_prop_collector_factories,
                            options.compression, CompressionOptions(),
                            nullptr /* compression_dict */,
                            false /* skip_filters */, cf_name_, -1),
        TablePropertiesCollectorFactory::Context::kUnknownColumnFamily,
        &mtables_));
    return builder;
  }

  virtual TableReader* GenReader(
      const ImmutableCFOptions& ioptions,
      const InternalKeyComparator& internal_comparator) override {
    std::unique_ptr<storage::RandomAccessExtent> extent(
        new storage::RandomAccessExtent());
    assert(extent);

    storage::ExtentId eid(mtables_.metas[0].fd.GetNumber());
    Status s = space_manager_->get_random_access_extent(eid, *extent);
    EXPECT_TRUE(s.ok()) << s.ToString();
    file_reader_.reset(new RandomAccessFileReader(std::move(extent)));

    s = ioptions.table_factory->NewTableReader(
        TableReaderOptions(ioptions, soptions_, internal_comparator),
        std::move(file_reader_), storage::MAX_EXTENT_SIZE, &table_reader_);
    return table_reader_.get();
  }

  virtual Status FinishImpl(const Options& options,
                            const ImmutableCFOptions& ioptions,
                            const BlockBasedTableOptions& table_options,
                            const InternalKeyComparator& internal_comparator,
                            const stl_wrappers::KVMap& kv_map) override {
    Reset();
    soptions_.use_mmap_reads = ioptions.allow_mmap_reads;
    unique_ptr<TableBuilder> builder;
    std::vector<std::unique_ptr<IntTblPropCollectorFactory>>
        int_tbl_prop_collector_factories;
    std::string column_family_name;
    int unknown_level = -1;
    builder.reset(ioptions.table_factory->NewTableBuilderExt(
        TableBuilderOptions(
            ioptions, internal_comparator, &int_tbl_prop_collector_factories,
            options.compression, CompressionOptions(),
            nullptr /* compression_dict */, false /* skip_filters */,
            column_family_name, unknown_level),
        TablePropertiesCollectorFactory::Context::kUnknownColumnFamily,
        &mtables_));

    for (const auto kv : kv_map) {
      if (convert_to_internal_key_) {
        ParsedInternalKey ikey(kv.first, kMaxSequenceNumber, kTypeValue);
        std::string encoded;
        AppendInternalKey(&encoded, ikey);
        builder->Add(encoded, kv.second);
      } else {
        builder->Add(kv.first, kv.second);
      }
      EXPECT_TRUE(builder->status().ok());
    }
    Status s = builder->Finish();

    EXPECT_TRUE(s.ok()) << s.ToString();

    // Open the table
    uniq_id_ = cur_uniq_id_++;

    std::unique_ptr<storage::RandomAccessExtent> extent(
        new storage::RandomAccessExtent());
    assert(extent);

    storage::ExtentId eid(mtables_.metas[0].fd.GetNumber());
    s = space_manager_->get_random_access_extent(eid, *extent);
    EXPECT_TRUE(s.ok()) << s.ToString();

    file_reader_.reset(new RandomAccessFileReader(std::move(extent)));

    return ioptions.table_factory->NewTableReader(
        TableReaderOptions(ioptions, soptions_, internal_comparator),
        std::move(file_reader_), storage::MAX_EXTENT_SIZE, &table_reader_);
  }

  virtual InternalIterator* NewIterator() const override {
    ReadOptions ro;
    InternalIterator* iter = table_reader_->NewIterator(ro);
    if (convert_to_internal_key_) {
      return new KeyConvertingIterator(iter);
    } else {
      return iter;
    }
  }

  uint64_t ApproximateOffsetOf(const Slice& key) const {
    if (convert_to_internal_key_) {
      InternalKey ikey(key, kMaxSequenceNumber, kTypeValue);
      const Slice skey = ikey.Encode();
      return table_reader_->ApproximateOffsetOf(skey);
    }
    return table_reader_->ApproximateOffsetOf(key);
  }

  virtual Status Reopen(const ImmutableCFOptions& ioptions) {
    std::unique_ptr<storage::RandomAccessExtent> extent(
        new storage::RandomAccessExtent());
    assert(extent);
    storage::ExtentId eid(mtables_.metas[0].fd.GetNumber());
    Status s = space_manager_->get_random_access_extent(eid, *extent);
    EXPECT_TRUE(s.ok()) << s.ToString();
    file_reader_.reset(new RandomAccessFileReader(std::move(extent)));

    return ioptions.table_factory->NewTableReader(
        TableReaderOptions(ioptions, soptions_, *last_internal_key_),
        std::move(file_reader_), storage::MAX_EXTENT_SIZE, &table_reader_);
  }

  virtual TableReader* GetTableReader() { return table_reader_.get(); }

  virtual bool AnywayDeleteIterator() const override {
    return convert_to_internal_key_;
  }

  void ResetTableReader() { table_reader_.reset(); }

  bool ConvertToInternalKey() { return convert_to_internal_key_; }

  void set_sst_file_name() {
    assert(!mtables_.metas.empty());
    auto& meta = mtables_.metas.back();
    sst_file_name_ =
        TableFileName(db_paths_, meta.fd.GetNumber(), meta.fd.GetPathId());
  }

 private:
  void Reset() {
    uniq_id_ = 0;
    table_reader_.reset();
    file_reader_.reset();
    mtables_.metas.clear();
    mtables_.props.clear();
  }

  uint64_t uniq_id_;
  unique_ptr<RandomAccessFileReader> file_reader_;
  unique_ptr<TableReader> table_reader_;
  bool convert_to_internal_key_;
  MiniTables mtables_;

  std::string cf_name_;
  std::string test_name_;

  ExtentTableConstructor();

  static uint64_t cur_uniq_id_;
  EnvOptions soptions_;
  Env* env_;
  std::string dbname_;
  ImmutableDBOptions db_options_;
  MutableCFOptions mutable_cf_options_;
  std::shared_ptr<Cache> table_cache_;
  WriteController write_controller_;
  WriteBufferManager write_buffer_manager_;
  DBOptions options_;
  ColumnFamilyOptions cf_options_;
  VersionSet versions_;

  unique_ptr<storage::ExtentSpaceManager> space_manager_;
  std::vector<DbPath> db_paths_;
  std::string sst_file_name_;
};
uint64_t ExtentTableConstructor::cur_uniq_id_ = 1;

class MemTableConstructor : public Constructor {
 public:
  explicit MemTableConstructor(const Comparator* cmp, WriteBufferManager* wb)
      : Constructor(cmp),
        internal_comparator_(cmp),
        write_buffer_manager_(wb),
        table_factory_(new memtable::SkipListFactory) {
    options_.memtable_factory = table_factory_;
    ImmutableCFOptions ioptions(options_);
    memtable_ =
        new MemTable(internal_comparator_, ioptions, MutableCFOptions(options_),
                     wb, kMaxSequenceNumber);
    memtable_->Ref();
  }
  ~MemTableConstructor() { delete memtable_->Unref(); }
  virtual Status FinishImpl(const Options&, const ImmutableCFOptions& ioptions,
                            const BlockBasedTableOptions& table_options,
                            const InternalKeyComparator& internal_comparator,
                            const stl_wrappers::KVMap& kv_map) override {
    delete memtable_->Unref();
    ImmutableCFOptions mem_ioptions(ioptions);
    memtable_ = new MemTable(internal_comparator_, mem_ioptions,
                             MutableCFOptions(options_), write_buffer_manager_,
                             kMaxSequenceNumber);
    memtable_->Ref();
    int seq = 1;
    for (const auto kv : kv_map) {
      memtable_->Add(seq, kTypeValue, kv.first, kv.second);
      seq++;
    }
    return Status::OK();
  }
  virtual InternalIterator* NewIterator() const override {
    return new KeyConvertingIterator(
        memtable_->NewIterator(ReadOptions(), &arena_), true);
  }

  virtual bool AnywayDeleteIterator() const override { return true; }

  virtual bool IsArenaMode() const override { return true; }

 private:
  mutable Arena arena_;
  InternalKeyComparator internal_comparator_;
  Options options_;
  WriteBufferManager* write_buffer_manager_;
  MemTable* memtable_;
  std::shared_ptr<memtable::SkipListFactory> table_factory_;
};

class InternalIteratorFromIterator : public InternalIterator {
 public:
  explicit InternalIteratorFromIterator(Iterator* it) : it_(it) {}
  virtual bool Valid() const override { return it_->Valid(); }
  virtual void Seek(const Slice& target) override { it_->Seek(target); }
  virtual void SeekForPrev(const Slice& target) override {
    it_->SeekForPrev(target);
  }
  virtual void SeekToFirst() override { it_->SeekToFirst(); }
  virtual void SeekToLast() override { it_->SeekToLast(); }
  virtual void Next() override { it_->Next(); }
  virtual void Prev() override { it_->Prev(); }
  Slice key() const override { return it_->key(); }
  Slice value() const override { return it_->value(); }
  virtual Status status() const override { return it_->status(); }

 private:
  unique_ptr<Iterator> it_;
};

class DBConstructor : public Constructor {
 public:
  explicit DBConstructor(const Comparator* cmp)
      : Constructor(cmp), comparator_(cmp) {
    db_ = nullptr;
    NewDB();
  }
  ~DBConstructor() { delete db_; }
  virtual Status FinishImpl(const Options& options,
                            const ImmutableCFOptions& ioptions,
                            const BlockBasedTableOptions& table_options,
                            const InternalKeyComparator& internal_comparator,
                            const stl_wrappers::KVMap& kv_map) override {
    delete db_;
    db_ = nullptr;
    NewDB();
    for (const auto kv : kv_map) {
      WriteBatch batch;
      batch.Put(kv.first, kv.second);
      EXPECT_TRUE(db_->Write(WriteOptions(), &batch).ok());
    }
    return Status::OK();
  }

  virtual InternalIterator* NewIterator() const override {
    return new InternalIteratorFromIterator(db_->NewIterator(ReadOptions()));
  }

  virtual DB* db() const override { return db_; }

 private:
  void NewDB() {
    std::string name = test::TmpDir() + "/table_testdb";

    Options options;
    options.comparator = comparator_;
    Status status = DestroyDB(name, options);
    ASSERT_TRUE(status.ok()) << status.ToString();

    options.create_if_missing = true;
    options.error_if_exists = true;
    options.write_buffer_size = 10000;  // Something small to force merging
    status = DB::Open(options, name, &db_);
    ASSERT_TRUE(status.ok()) << status.ToString();
  }

  const Comparator* comparator_;
  DB* db_;
};

enum TestType { EXTENT_BASED_TABLE_TEST, BLOCK_TEST, MEMTABLE_TEST, DB_TEST };

struct TestArgs {
  TestType type;
  bool reverse_compare;
  int restart_interval;
  CompressionType compression;
  uint32_t format_version;
  bool use_mmap;
};

static std::vector<TestArgs> GenerateArgList() {
  std::vector<TestArgs> test_args;
  std::vector<TestType> test_types = {EXTENT_BASED_TABLE_TEST, BLOCK_TEST,
                                      MEMTABLE_TEST, DB_TEST};
  std::vector<bool> reverse_compare_types = {false, true};
  std::vector<int> restart_intervals = {16, 1, 1024};

  // Only add compression if it is supported
  std::vector<std::pair<CompressionType, bool>> compression_types;
  compression_types.emplace_back(kNoCompression, false);
  if (Snappy_Supported()) {
    compression_types.emplace_back(kSnappyCompression, false);
  }
  if (Zlib_Supported()) {
    compression_types.emplace_back(kZlibCompression, false);
    compression_types.emplace_back(kZlibCompression, true);
  }
  if (BZip2_Supported()) {
    compression_types.emplace_back(kBZip2Compression, false);
    compression_types.emplace_back(kBZip2Compression, true);
  }
  if (LZ4_Supported()) {
    compression_types.emplace_back(kLZ4Compression, false);
    compression_types.emplace_back(kLZ4Compression, true);
    compression_types.emplace_back(kLZ4HCCompression, false);
    compression_types.emplace_back(kLZ4HCCompression, true);
  }
  if (XPRESS_Supported()) {
    compression_types.emplace_back(kXpressCompression, false);
    compression_types.emplace_back(kXpressCompression, true);
  }
  if (ZSTD_Supported()) {
    compression_types.emplace_back(kZSTD, false);
    compression_types.emplace_back(kZSTD, true);
  }

  for (auto test_type : test_types) {
    for (auto reverse_compare : reverse_compare_types) {
      for (auto restart_interval : restart_intervals) {
        for (auto compression_type : compression_types) {
          TestArgs one_arg;
          one_arg.type = test_type;
          one_arg.reverse_compare = reverse_compare;
          one_arg.restart_interval = restart_interval;
          one_arg.compression = compression_type.first;
          one_arg.format_version = 3;
          one_arg.use_mmap = false;
          test_args.push_back(one_arg);
        }
      }
    }
  }
  return test_args;
}

// In order to make all tests run for plain table format, including
// those operating on empty keys, create a new prefix transformer which
// return fixed prefix if the slice is not shorter than the prefix length,
// and the full slice if it is shorter.
class FixedOrLessPrefixTransform : public SliceTransform {
 private:
  const size_t prefix_len_;

 public:
  explicit FixedOrLessPrefixTransform(size_t prefix_len)
      : prefix_len_(prefix_len) {}

  virtual const char* Name() const override { return "rocksdb.FixedPrefix"; }

  virtual Slice Transform(const Slice& src) const override {
    assert(InDomain(src));
    if (src.size() < prefix_len_) {
      return src;
    }
    return Slice(src.data(), prefix_len_);
  }

  virtual bool InDomain(const Slice& src) const override { return true; }

  virtual bool InRange(const Slice& dst) const override {
    return (dst.size() <= prefix_len_);
  }
};

class HarnessTest : public testing::Test {
 public:
  HarnessTest()
      : ioptions_(options_),
        constructor_(nullptr),
        write_buffer_(options_.db_write_buffer_size) {}

  void Init(const TestArgs& args) {
    delete constructor_;
    constructor_ = nullptr;
    options_ = Options();
    options_.compression = args.compression;
    // Use shorter block size for tests to exercise block boundary
    // conditions more.
    if (args.reverse_compare) {
      options_.comparator = &reverse_key_comparator;
    }

    internal_comparator_.reset(
        new test::PlainInternalKeyComparator(options_.comparator));

    support_prev_ = true;
    only_support_prefix_seek_ = false;
    options_.allow_mmap_reads = args.use_mmap;
    switch (args.type) {
      case EXTENT_BASED_TABLE_TEST:
        table_options_.flush_block_policy_factory.reset(
            new FlushBlockBySizePolicyFactory());
        table_options_.block_size = 256;
        table_options_.block_restart_interval = args.restart_interval;
        table_options_.index_block_restart_interval = args.restart_interval;
        table_options_.format_version = args.format_version;
        options_.table_factory.reset(
            new ExtentBasedTableFactory(table_options_));
        constructor_ = new ExtentTableConstructor(
            options_.comparator, true /* convert_to_internal_key_ */);
        internal_comparator_.reset(
            new InternalKeyComparator(options_.comparator));
        break;
      case BLOCK_TEST:
        table_options_.block_size = 256;
        options_.table_factory.reset(
            new ExtentBasedTableFactory(table_options_));
        constructor_ = new BlockConstructor(options_.comparator);
        break;
      case MEMTABLE_TEST:
        table_options_.block_size = 256;
        options_.table_factory.reset(
            new ExtentBasedTableFactory(table_options_));
        constructor_ =
            new MemTableConstructor(options_.comparator, &write_buffer_);
        break;
      case DB_TEST:
        table_options_.block_size = 256;
        options_.table_factory.reset(
            new ExtentBasedTableFactory(table_options_));
        constructor_ = new DBConstructor(options_.comparator);
        break;
    }
    ioptions_ = ImmutableCFOptions(options_);
  }

  ~HarnessTest() { delete constructor_; }

  void Add(const std::string& key, const std::string& value) {
    constructor_->Add(key, value);
  }

  void Test(Random* rnd) {
    std::vector<std::string> keys;
    stl_wrappers::KVMap data;
    constructor_->Finish(options_, ioptions_, table_options_,
                         *internal_comparator_, &keys, &data);

    TestForwardScan(keys, data);
    if (support_prev_) {
      TestBackwardScan(keys, data);
    }
    TestRandomAccess(rnd, keys, data);
  }

  void TestForwardScan(const std::vector<std::string>& keys,
                       const stl_wrappers::KVMap& data) {
    InternalIterator* iter = constructor_->NewIterator();
    ASSERT_TRUE(!iter->Valid());
    iter->SeekToFirst();
    for (stl_wrappers::KVMap::const_iterator model_iter = data.begin();
         model_iter != data.end(); ++model_iter) {
      ASSERT_EQ(ToString(data, model_iter), ToString(iter));
      iter->Next();
    }
    ASSERT_TRUE(!iter->Valid());
    if (constructor_->IsArenaMode() && !constructor_->AnywayDeleteIterator()) {
      iter->~InternalIterator();
    } else {
      delete iter;
    }
  }

  void TestBackwardScan(const std::vector<std::string>& keys,
                        const stl_wrappers::KVMap& data) {
    InternalIterator* iter = constructor_->NewIterator();
    ASSERT_TRUE(!iter->Valid());
    iter->SeekToLast();
    for (stl_wrappers::KVMap::const_reverse_iterator model_iter = data.rbegin();
         model_iter != data.rend(); ++model_iter) {
      ASSERT_EQ(ToString(data, model_iter), ToString(iter));
      iter->Prev();
    }
    ASSERT_TRUE(!iter->Valid());
    if (constructor_->IsArenaMode() && !constructor_->AnywayDeleteIterator()) {
      iter->~InternalIterator();
    } else {
      delete iter;
    }
  }

  void TestRandomAccess(Random* rnd, const std::vector<std::string>& keys,
                        const stl_wrappers::KVMap& data) {
    static const bool kVerbose = false;
    InternalIterator* iter = constructor_->NewIterator();
    ASSERT_TRUE(!iter->Valid());
    stl_wrappers::KVMap::const_iterator model_iter = data.begin();
    if (kVerbose) fprintf(stderr, "---\n");
    for (int i = 0; i < 200; i++) {
      const int toss = rnd->Uniform(support_prev_ ? 5 : 3);
      switch (toss) {
        case 0: {
          if (iter->Valid()) {
            if (kVerbose) fprintf(stderr, "Next\n");
            iter->Next();
            ++model_iter;
            ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          }
          break;
        }

        case 1: {
          if (kVerbose) fprintf(stderr, "SeekToFirst\n");
          iter->SeekToFirst();
          model_iter = data.begin();
          ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          break;
        }

        case 2: {
          std::string key = PickRandomKey(rnd, keys);
          model_iter = data.lower_bound(key);
          if (kVerbose)
            fprintf(stderr, "Seek '%s'\n", EscapeString(key).c_str());
          iter->Seek(Slice(key));
          ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          break;
        }

        case 3: {
          if (iter->Valid()) {
            if (kVerbose) fprintf(stderr, "Prev\n");
            iter->Prev();
            if (model_iter == data.begin()) {
              model_iter = data.end();  // Wrap around to invalid value
            } else {
              --model_iter;
            }
            ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          }
          break;
        }

        case 4: {
          if (kVerbose) fprintf(stderr, "SeekToLast\n");
          iter->SeekToLast();
          if (keys.empty()) {
            model_iter = data.end();
          } else {
            std::string last = data.rbegin()->first;
            model_iter = data.lower_bound(last);
          }
          ASSERT_EQ(ToString(data, model_iter), ToString(iter));
          break;
        }
      }
    }
    if (constructor_->IsArenaMode() && !constructor_->AnywayDeleteIterator()) {
      iter->~InternalIterator();
    } else {
      delete iter;
    }
  }

  std::string ToString(const stl_wrappers::KVMap& data,
                       const stl_wrappers::KVMap::const_iterator& it) {
    if (it == data.end()) {
      return "END";
    } else {
      return "'" + it->first + "->" + it->second + "'";
    }
  }

  std::string ToString(const stl_wrappers::KVMap& data,
                       const stl_wrappers::KVMap::const_reverse_iterator& it) {
    if (it == data.rend()) {
      return "END";
    } else {
      return "'" + it->first + "->" + it->second + "'";
    }
  }

  std::string ToString(const InternalIterator* it) {
    if (!it->Valid()) {
      return "END";
    } else {
      return "'" + it->key().ToString() + "->" + it->value().ToString() + "'";
    }
  }

  std::string PickRandomKey(Random* rnd, const std::vector<std::string>& keys) {
    if (keys.empty()) {
      return "foo";
    } else {
      const int index = rnd->Uniform(static_cast<int>(keys.size()));
      std::string result = keys[index];
      switch (rnd->Uniform(support_prev_ ? 3 : 1)) {
        case 0:
          // Return an existing key
          break;
        case 1: {
          // Attempt to return something smaller than an existing key
          if (result.size() > 0 && result[result.size() - 1] > '\0' &&
              (!only_support_prefix_seek_ ||
               options_.prefix_extractor->Transform(result).size() <
                   result.size())) {
            result[result.size() - 1]--;
          }
          break;
        }
        case 2: {
          // Return something larger than an existing key
          Increment(options_.comparator, &result);
          break;
        }
      }
      return result;
    }
  }

  // Returns nullptr if not running against a DB
  DB* db() const { return constructor_->db(); }

 private:
  Options options_ = Options();
  ImmutableCFOptions ioptions_;
  BlockBasedTableOptions table_options_ = BlockBasedTableOptions();
  Constructor* constructor_;
  WriteBufferManager write_buffer_;
  bool support_prev_;
  bool only_support_prefix_seek_;
  shared_ptr<InternalKeyComparator> internal_comparator_;
};

static bool Between(uint64_t val, uint64_t low, uint64_t high) {
  bool result = (val >= low) && (val <= high);
  if (!result) {
    fprintf(stderr, "Value %llu is not in range [%llu, %llu]\n",
            (unsigned long long)(val), (unsigned long long)(low),
            (unsigned long long)(high));
  }
  return result;
}

// Tests against all kinds of tables
class TableTest : public testing::Test {
 public:
  const InternalKeyComparator& GetPlainInternalComparator(
      const Comparator* comp) {
    if (!plain_internal_comparator) {
      plain_internal_comparator.reset(
          new test::PlainInternalKeyComparator(comp));
    }
    return *plain_internal_comparator;
  }
  void IndexTest(BlockBasedTableOptions table_options);

 private:
  std::unique_ptr<InternalKeyComparator> plain_internal_comparator;
};

void GenerateOptimizedTable(ExtentTableConstructor& c, const Options& options,
                            const BlockBasedTableOptions& table_options,
                            const ImmutableCFOptions& ioptions,
                            InternalKeyComparator icmp) {
  unique_ptr<TableBuilder> table =
      c.GenBuilder(options, ioptions, table_options, icmp);
  ASSERT_TRUE(table->SupportAddBlock());

  std::string mock_value(1000, 'v');
  table->Add(InternalKey("111111111", 0, kTypeValue).Encode(),
             mock_value + "111");
  table->Add(InternalKey("222222222", 0, kTypeValue).Encode(),
             mock_value + "222");
  table->Add(InternalKey("333333333", 0, kTypeValue).Encode(),
             mock_value + "333");
  table->Add(InternalKey("444444444", 0, kTypeValue).Encode(),
             mock_value + "444");
  table->Add(InternalKey("555555555", 0, kTypeValue).Encode(),
             mock_value + "555");
  table->Add(InternalKey("666666666", 0, kTypeValue).Encode(),
             mock_value + "666");
  table->Add(InternalKey("777777777", 0, kTypeValue).Encode(),
             mock_value + "777");
  table->Add(InternalKey("888888888", 0, kTypeValue).Encode(),
             mock_value + "888");
  table->Add(InternalKey("999999999", 0, kTypeValue).Encode(),
             mock_value + "999");

  BlockBuilder block_builder(16);
  block_builder.Add(InternalKey("AAAAAAAAA", 0, kTypeValue).Encode(),
                    mock_value + "AAA");
  block_builder.Add(InternalKey("BBBBBBBBB", 0, kTypeValue).Encode(),
                    mock_value + "BBB");
  block_builder.Add(InternalKey("CCCCCCCCC", 0, kTypeValue).Encode(),
                    mock_value + "CCC");
  block_builder.Add(InternalKey("DDDDDDDDD", 0, kTypeValue).Encode(),
                    mock_value + "DDD");
  block_builder.Add(InternalKey("EEEEEEEEE", 0, kTypeValue).Encode(),
                    mock_value + "EEE");
  block_builder.Add(InternalKey("FFFFFFFFF", 0, kTypeValue).Encode(),
                    mock_value + "FFF");
  block_builder.Add(InternalKey("GGGGGGGGG", 0, kTypeValue).Encode(),
                    mock_value + "GGG");
  block_builder.Add(InternalKey("HHHHHHHHH", 0, kTypeValue).Encode(),
                    mock_value + "HHH");
  block_builder.Add(InternalKey("IIIIIIIII", 0, kTypeValue).Encode(),
                    mock_value + "III");

  Slice raw_block_encoding = block_builder.Finish();
  char trailer[kBlockTrailerSize];
  trailer[0] = kNoCompression;
  char* trailer_without_type = trailer + 1;
  switch (table_options.checksum) {
    case kNoChecksum:
      // we don't support no checksum yet
      assert(false);
    // intentional fallthrough in release binary
    case kCRC32c: {
      auto crc =
          crc32c::Value(raw_block_encoding.data(), raw_block_encoding.size());
      crc = crc32c::Extend(crc, trailer, 1);  // Extend to cover block type
      EncodeFixed32(trailer_without_type, crc32c::Mask(crc));
      break;
    }
    case kxxHash: {
      void* xxh = XXH32_init(0);
      XXH32_update(xxh, raw_block_encoding.data(),
                   static_cast<uint32_t>(raw_block_encoding.size()));
      XXH32_update(xxh, trailer, 1);  // Extend  to cover block type
      EncodeFixed32(trailer_without_type, XXH32_digest(xxh));
      break;
    }
  }
  std::string block_with_trailer = raw_block_encoding.ToString();
  block_with_trailer.append(trailer, 5);

  BlockStats block_stats;
  block_stats.first_key_ =
      InternalKey("AAAAAAAAA", 0, kTypeValue).Encode().ToString();
  block_stats.data_size_ = 9180;
  block_stats.key_size_ = 153;
  block_stats.value_size_ = 9027;
  block_stats.rows_ = 9;
  block_stats.actual_disk_size_ = 9261;
  block_stats.entry_put_ = 9;
  block_stats.entry_deletes_ = 0;
  block_stats.entry_single_deletes_ = 0;
  block_stats.entry_merges_ = 0;
  block_stats.entry_others_ = 0;
  std::string block_stats_str = block_stats.encode();

  Slice block_stats_encoding = Slice{block_stats_str};
  Slice block_encoding{block_with_trailer};
  table->AddBlock(block_encoding, block_stats_encoding,
                  InternalKey("JJJJJ", 0, kTypeValue).Encode());

  table->Add(InternalKey("KKKKKKKKK", 0, kTypeValue).Encode(),
             mock_value + "KKK");
  table->Add(InternalKey("LLLLLLLLL", 0, kTypeValue).Encode(),
             mock_value + "LLL");
  table->Add(InternalKey("MMMMMMMMM", 0, kTypeValue).Encode(),
             mock_value + "MMM");

  table->Finish();

  c.set_sst_file_name();
}

static std::string GetFromFile(TableReader* table_reader,
                               const std::string& key, ReadOptions& ro,
                               const Comparator* comparator) {
  PinnableSlice value;
  GetContext get_context(comparator, nullptr, nullptr, nullptr,
                         GetContext::kNotFound, Slice(key), &value, nullptr,
                         nullptr, nullptr, nullptr);
  LookupKey lk{key.c_str(), kMaxSequenceNumber};
  table_reader->Get(ro, lk.internal_key(), &get_context);
  return std::string(value.data(), value.size());
  // return value;
}

class GeneralTableTest : public TableTest {};
class BlockBasedTableTest : public TableTest {};
class ExtentBasedTableTest : public TableTest {};
class PlainTableTest : public TableTest {};
class TablePropertyTest : public testing::Test {};

// This test serves as the living tutorial for the prefix scan of user collected
// properties.
TEST_F(TablePropertyTest, PrefixScanTest) {
  UserCollectedProperties props{
      {"num.111.1", "1"}, {"num.111.2", "2"}, {"num.111.3", "3"},
      {"num.333.1", "1"}, {"num.333.2", "2"}, {"num.333.3", "3"},
      {"num.555.1", "1"}, {"num.555.2", "2"}, {"num.555.3", "3"},
  };

  // prefixes that exist
  for (const std::string& prefix : {"num.111", "num.333", "num.555"}) {
    int num = 0;
    for (auto pos = props.lower_bound(prefix);
         pos != props.end() &&
         pos->first.compare(0, prefix.size(), prefix) == 0;
         ++pos) {
      ++num;
      auto key = prefix + "." + ToString(num);
      ASSERT_EQ(key, pos->first);
      ASSERT_EQ(ToString(num), pos->second);
    }
    ASSERT_EQ(3, num);
  }

  // prefixes that don't exist
  for (const std::string& prefix :
       {"num.000", "num.222", "num.444", "num.666"}) {
    auto pos = props.lower_bound(prefix);
    ASSERT_TRUE(pos == props.end() ||
                pos->first.compare(0, prefix.size(), prefix) != 0);
  }
}

TEST_F(ExtentBasedTableTest, BlockStatsTest) {
  BlockStats block_stats;
  block_stats.first_key_ = "first_key";
  block_stats.data_size_ = 100;
  block_stats.key_size_ = 50;
  block_stats.value_size_ = 50;
  block_stats.rows_ = 10;
  block_stats.actual_disk_size_ = 100;
  block_stats.entry_put_ = 6;
  block_stats.entry_deletes_ = 1;
  block_stats.entry_single_deletes_ = 1;
  block_stats.entry_merges_ = 1;
  block_stats.entry_others_ = 1;
  std::string block_stats_str = block_stats.encode();
  BlockStats block_stats_result;
  Slice block_stats_encoding = Slice{block_stats_str};
  block_stats_result.decode(block_stats_encoding);
  ASSERT_TRUE(block_stats.equal(block_stats_result));
}

TEST_F(ExtentBasedTableTest, AddTest) {
  ExtentTableConstructor c(BytewiseComparator(),
                           true /* convert_to_internal_key*/, "optimized_add");

  std::string mock_value(1000, 'v');
  c.Add("a1", mock_value + "a1");
  c.Add("b2", mock_value);
  c.Add("c3", mock_value);
  c.Add("d4", mock_value);
  c.Add("e5", mock_value);
  c.Add("f6", mock_value);
  c.Add("g7", mock_value);
  c.Add("h8", mock_value);
  c.Add("j9", mock_value + "j9");

  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  options.compression = kNoCompression;
  ReadOptions ro;
  PinnableSlice value;

  BlockBasedTableOptions table_options;
  table_options.block_restart_interval = 1;
  options.table_factory.reset(NewExtentBasedTableFactory(table_options));
  const ImmutableCFOptions ioptions(options);
  InternalKeyComparator icmp(options.comparator);
  c.Finish(options, ioptions, table_options, icmp, &keys, &kvmap);
  // GetPlainInternalComparator(options.comparator),
  TableReader* reader = c.GetTableReader();
  GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                         GetContext::kNotFound, Slice("a1"), &value, nullptr,
                         nullptr, nullptr, nullptr);
  LookupKey lk{"a1", kMaxSequenceNumber};
  reader->Get(ro, lk.internal_key(), &get_context);
  fprintf(stderr, "get %s\n", value.data());
  PinnableSlice value2;
  GetContext get_context2(options.comparator, nullptr, nullptr, nullptr,
                          GetContext::kNotFound, Slice("j9"), &value2, nullptr,
                          nullptr, nullptr, nullptr);
  LookupKey lk2{"j9", kMaxSequenceNumber};
  reader->Get(ro, lk2.internal_key(), &get_context2);
  fprintf(stderr, "get %s\n", value2.data());
  c.ResetTableReader();
}

TEST_F(ExtentBasedTableTest, AddBlockTest) {
  ExtentTableConstructor c(BytewiseComparator(),
                           true /*convert_to_internal_key*/,
                           "optimized_add_block");

  Options options;
  options.compression = kNoCompression;
  BlockBasedTableOptions table_options;
  table_options.block_restart_interval = 16;
  options.table_factory.reset(NewExtentBasedTableFactory(table_options));
  const ImmutableCFOptions ioptions(options);
  InternalKeyComparator icmp(options.comparator);

  GenerateOptimizedTable(c, options, table_options, ioptions, icmp);
  TableReader* reader = c.GenReader(ioptions, icmp);
  ASSERT_TRUE(reader != nullptr);

  // ASSERT_EQ(FileVersion::BLOCK_BASED_COMPACTION, reader->GetFileVersion());
  ReadOptions ro;

  std::string value = GetFromFile(reader, "111111111", ro, options.comparator);
  fprintf(stderr, "get %s\n", value.c_str());
  value = GetFromFile(reader, "AAAAAAAAA", ro, options.comparator);
  fprintf(stderr, "get %s\n", value.c_str());
  value = GetFromFile(reader, "HHHHHHHHH", ro, options.comparator);
  fprintf(stderr, "get %s\n", value.c_str());
  value = GetFromFile(reader, "LLLLLLLLL", ro, options.comparator);
  fprintf(stderr, "get %s\n", value.c_str());

  c.ResetTableReader();
}

TEST_F(ExtentBasedTableTest, OptionsCheckTest) {
  {
    Options options;

    DBOptions db_opts(options);
    ColumnFamilyOptions cf_opts(options);
    BlockBasedTableOptions table_options;
    table_options.index_type = BlockBasedTableOptions::kHashSearch;

    options.table_factory.reset(NewExtentBasedTableFactory(table_options));
    Status s = options.table_factory->SanitizeOptions(db_opts, cf_opts);
    ASSERT_TRUE(s.IsInvalidArgument());
  }
  {
    Options options;
    options.compression_opts.max_dict_bytes = 1;

    DBOptions db_opts(options);
    BlockBasedTableOptions table_options;
    ColumnFamilyOptions cf_opts(options);
    options.table_factory.reset(NewExtentBasedTableFactory(table_options));
    Status s = options.table_factory->SanitizeOptions(db_opts, cf_opts);
    ASSERT_TRUE(s.IsInvalidArgument());
  }
  {
    Options options;
    DBOptions db_opts(options);
    BlockBasedTableOptions table_options;
    ColumnFamilyOptions cf_opts(options);
    options.table_factory.reset(NewExtentBasedTableFactory(table_options));
    Status s = options.table_factory->SanitizeOptions(db_opts, cf_opts);
    ASSERT_TRUE(s.ok());
  }
}

TEST_F(ExtentBasedTableTest, PropertiesTest) {
  ExtentTableConstructor c(BytewiseComparator(),
                           true /*convert_to_internal_key*/,
                           "optimized_add_block_properties");

  Options options;
  options.compression = kNoCompression;
  BlockBasedTableOptions table_options;
  table_options.block_restart_interval = 16;
  options.table_factory.reset(NewExtentBasedTableFactory(table_options));
  const ImmutableCFOptions ioptions(options);
  InternalKeyComparator icmp(options.comparator);

  GenerateOptimizedTable(c, options, table_options, ioptions, icmp);
  TableReader* reader = c.GenReader(ioptions, icmp);
  ASSERT_TRUE(reader != nullptr);
  // ASSERT_EQ(reader->GetFileVersion(), FileVersion::BLOCK_BASED_COMPACTION);
  auto& props = *reader->GetTableProperties();
  ASSERT_GT(props.index_size, 0);
  ASSERT_GT(props.data_size, props.raw_key_size + props.raw_value_size);

  ASSERT_EQ(props.num_entries, 21);
  // Each key has length 17
  ASSERT_EQ(props.raw_key_size, 21 * 17);
  // Each value has length 1003
  ASSERT_EQ(props.raw_value_size, 21 * 1003);
  // The first 9 key will genrate 3 blocks, then a directly added block and last
  // block.
  ASSERT_EQ(props.num_data_blocks, 5);
  // No filter
  ASSERT_EQ("", props.filter_policy_name);
  // Default comparator
  ASSERT_EQ("leveldb.BytewiseComparator", props.comparator_name);
  // No merge operator
  ASSERT_EQ("nullptr", props.merge_operator_name);
  // No property collectors
  ASSERT_EQ("[]", props.property_collectors_names);
  // Compression type == that set:
  ASSERT_EQ("NoCompression", props.compression_name);

  c.ResetTableReader();
}

TEST_F(ExtentBasedTableTest, AddBlockOnlyTest) {
  ExtentTableConstructor c(BytewiseComparator(),
                           true /*convert_to_internal_key*/,
                           "optimized_add_block_properties");

  Options options;
  options.compression = kNoCompression;
  BlockBasedTableOptions table_options;
  table_options.block_restart_interval = 16;
  options.table_factory.reset(NewExtentBasedTableFactory(table_options));
  const ImmutableCFOptions ioptions(options);
  InternalKeyComparator icmp(options.comparator);
  unique_ptr<TableBuilder> table =
      c.GenBuilder(options, ioptions, table_options, icmp);
  ASSERT_TRUE(table->SupportAddBlock());

  std::string mock_value(1000, 'v');
  BlockBuilder block_builder(16);
  block_builder.Add(InternalKey("AAAAAAAAA", 0, kTypeValue).Encode(),
                    mock_value + "AAA");
  block_builder.Add(InternalKey("BBBBBBBBB", 0, kTypeValue).Encode(),
                    mock_value + "BBB");
  block_builder.Add(InternalKey("CCCCCCCCC", 0, kTypeValue).Encode(),
                    mock_value + "CCC");
  block_builder.Add(InternalKey("DDDDDDDDD", 0, kTypeValue).Encode(),
                    mock_value + "DDD");
  block_builder.Add(InternalKey("EEEEEEEEE", 0, kTypeValue).Encode(),
                    mock_value + "EEE");
  block_builder.Add(InternalKey("FFFFFFFFF", 0, kTypeValue).Encode(),
                    mock_value + "FFF");
  block_builder.Add(InternalKey("GGGGGGGGG", 0, kTypeValue).Encode(),
                    mock_value + "GGG");
  block_builder.Add(InternalKey("HHHHHHHHH", 0, kTypeValue).Encode(),
                    mock_value + "HHH");
  block_builder.Add(InternalKey("IIIIIIIII", 0, kTypeValue).Encode(),
                    mock_value + "III");
  Slice raw_block_encoding = block_builder.Finish();
  char trailer[kBlockTrailerSize];
  trailer[0] = kNoCompression;
  char* trailer_without_type = trailer + 1;
  switch (table_options.checksum) {
    case kNoChecksum:
      // we don't support no checksum yet
      assert(false);
    // intentional fallthrough in release binary
    case kCRC32c: {
      auto crc =
          crc32c::Value(raw_block_encoding.data(), raw_block_encoding.size());
      crc = crc32c::Extend(crc, trailer, 1);  // Extend to cover block type
      EncodeFixed32(trailer_without_type, crc32c::Mask(crc));
      break;
    }
    case kxxHash: {
      void* xxh = XXH32_init(0);
      XXH32_update(xxh, raw_block_encoding.data(),
                   static_cast<uint32_t>(raw_block_encoding.size()));
      XXH32_update(xxh, trailer, 1);  // Extend  to cover block type
      EncodeFixed32(trailer_without_type, XXH32_digest(xxh));
      break;
    }
  }
  std::string block_with_trailer = raw_block_encoding.ToString();
  block_with_trailer.append(trailer, 5);

  BlockStats block_stats;
  block_stats.first_key_ =
      InternalKey("AAAAAAAAA", 0, kTypeValue).Encode().ToString();
  block_stats.data_size_ = 9180;
  block_stats.key_size_ = 153;
  block_stats.value_size_ = 9027;
  block_stats.rows_ = 9;
  block_stats.actual_disk_size_ = 9261;
  block_stats.entry_put_ = 9;
  block_stats.entry_deletes_ = 0;
  block_stats.entry_single_deletes_ = 0;
  block_stats.entry_merges_ = 0;
  block_stats.entry_others_ = 0;
  std::string block_stats_str = block_stats.encode();

  Slice block_stats_encoding = Slice{block_stats_str};
  Slice block_encoding{block_with_trailer};
  table->AddBlock(block_encoding, block_stats_encoding,
                  InternalKey("JJJJJ", 0, kTypeValue).Encode());
  table->Add(InternalKey("KKKKKKKKK", 0, kTypeValue).Encode(),
             mock_value + "KKK");
  table->Add(InternalKey("LLLLLLLLL", 0, kTypeValue).Encode(),
             mock_value + "LLL");
  table->Add(InternalKey("MMMMMMMMM", 0, kTypeValue).Encode(),
             mock_value + "MMM");
  table->Finish();
  c.set_sst_file_name();

  ReadOptions ro;
  TableReader* reader = c.GenReader(ioptions, icmp);
  ASSERT_TRUE(reader != nullptr);
  // ASSERT_EQ(reader->GetFileVersion(), FileVersion::BLOCK_BASED_COMPACTION);
  std::string value = GetFromFile(reader, "AAAAAAAAA", ro, options.comparator);
  fprintf(stderr, "get %s\n", value.c_str());
  value = GetFromFile(reader, "HHHHHHHHH", ro, options.comparator);
  fprintf(stderr, "get %s\n", value.c_str());
  value = GetFromFile(reader, "KKKKKKKKK", ro, options.comparator);
  fprintf(stderr, "get %s\n", value.c_str());
  value = GetFromFile(reader, "LLLLLLLLL", ro, options.comparator);
  fprintf(stderr, "get %s\n", value.c_str());
  value = GetFromFile(reader, "MMMMMMMMM", ro, options.comparator);
  fprintf(stderr, "get %s\n", value.c_str());
  c.ResetTableReader();
}

// This test include all the basic checks except those for index size and block
// size, which will be conducted in separated unit tests.
TEST_F(BlockBasedTableTest, BasicBlockBasedTableProperties) {
  ExtentTableConstructor c(BytewiseComparator(),
                           true /* convert_to_internal_key_ */, "bbb");

  c.Add("a1", "val1");
  c.Add("b2", "val2");
  c.Add("c3", "val3");
  c.Add("d4", "val4");
  c.Add("e5", "val5");
  c.Add("f6", "val6");
  c.Add("g7", "val7");
  c.Add("h8", "val8");
  c.Add("j9", "val9");
  uint64_t diff_internal_user_bytes = 9 * 8;  // 8 is seq size, 9 k-v totally

  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  options.compression = kNoCompression;
  BlockBasedTableOptions table_options;
  table_options.block_restart_interval = 1;
  options.table_factory.reset(NewExtentBasedTableFactory(table_options));

  const ImmutableCFOptions ioptions(options);
  // InternalKeyComparator icmp(options.comparator);
  c.Finish(options, ioptions, table_options,
           // icmp, &keys, &kvmap);
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);

  auto& props = *c.GetTableReader()->GetTableProperties();
  ASSERT_EQ(kvmap.size(), props.num_entries);

  auto raw_key_size = kvmap.size() * 2ul;
  auto raw_value_size = kvmap.size() * 4ul;

  ASSERT_EQ(raw_key_size + diff_internal_user_bytes, props.raw_key_size);
  ASSERT_EQ(raw_value_size, props.raw_value_size);
  ASSERT_EQ(1ul, props.num_data_blocks);
  ASSERT_EQ("", props.filter_policy_name);  // no filter policy is used

  // Verify data size.
  BlockBuilder block_builder(1);
  for (const auto& item : kvmap) {
    block_builder.Add(item.first, item.second);
  }
  Slice content = block_builder.Finish();
  ASSERT_EQ(content.size() + kBlockTrailerSize + diff_internal_user_bytes,
            props.data_size);
  c.ResetTableReader();
}

TEST_F(BlockBasedTableTest, BlockBasedTableProperties2) {
  ExtentTableConstructor c(&reverse_key_comparator, true);
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;

  {
    Options options;
    options.compression = CompressionType::kNoCompression;
    BlockBasedTableOptions table_options;
    options.table_factory.reset(NewExtentBasedTableFactory(table_options));

    const ImmutableCFOptions ioptions(options);
    c.Finish(options, ioptions, table_options,
             GetPlainInternalComparator(options.comparator), &keys, &kvmap);

    auto& props = *c.GetTableReader()->GetTableProperties();

    // Default comparator
    ASSERT_EQ("leveldb.BytewiseComparator", props.comparator_name);
    // No merge operator
    ASSERT_EQ("nullptr", props.merge_operator_name);
    // No prefix extractor
    ASSERT_EQ("nullptr", props.prefix_extractor_name);
    // No property collectors
    ASSERT_EQ("[]", props.property_collectors_names);
    // No filter policy is used
    ASSERT_EQ("", props.filter_policy_name);
    // Compression type == that set:
    ASSERT_EQ("NoCompression", props.compression_name);
    c.ResetTableReader();
  }

  {
    Options options;
    BlockBasedTableOptions table_options;
    options.table_factory.reset(NewExtentBasedTableFactory(table_options));
    options.comparator = &reverse_key_comparator;
    options.merge_operator = MergeOperators::CreateUInt64AddOperator();
    options.prefix_extractor.reset(NewNoopTransform());
    options.table_properties_collector_factories.emplace_back(
        new DummyPropertiesCollectorFactory1());
    options.table_properties_collector_factories.emplace_back(
        new DummyPropertiesCollectorFactory2());

    // c.Add("a1", "val1");

    const ImmutableCFOptions ioptions(options);
    c.Finish(options, ioptions, table_options,
             GetPlainInternalComparator(options.comparator), &keys, &kvmap);

    auto& props = *c.GetTableReader()->GetTableProperties();

    ASSERT_EQ("rocksdb.ReverseBytewiseComparator", props.comparator_name);
    ASSERT_EQ("UInt64AddOperator", props.merge_operator_name);
    ASSERT_EQ("rocksdb.Noop", props.prefix_extractor_name);
    ASSERT_EQ("[DummyPropertiesCollector1,DummyPropertiesCollector2]",
              props.property_collectors_names);
    ASSERT_EQ("", props.filter_policy_name);  // no filter policy is used
    c.ResetTableReader();
  }
}

//
// BlockBasedTableTest::PrefetchTest
//
void AssertKeysInCache(ExtentBasedTable* table_reader,
                       const std::vector<std::string>& keys_in_cache,
                       const std::vector<std::string>& keys_not_in_cache,
                       bool convert = false) {
  if (convert) {
    for (auto key : keys_in_cache) {
      InternalKey ikey(key, kMaxSequenceNumber, kTypeValue);
      ASSERT_TRUE(table_reader->TEST_KeyInCache(ReadOptions(), ikey.Encode()));
    }
    for (auto key : keys_not_in_cache) {
      InternalKey ikey(key, kMaxSequenceNumber, kTypeValue);
      ASSERT_TRUE(!table_reader->TEST_KeyInCache(ReadOptions(), ikey.Encode()));
    }
  } else {
    for (auto key : keys_in_cache) {
      ASSERT_TRUE(table_reader->TEST_KeyInCache(ReadOptions(), key));
    }
    for (auto key : keys_not_in_cache) {
      ASSERT_TRUE(!table_reader->TEST_KeyInCache(ReadOptions(), key));
    }
  }
}

void PrefetchRange(ExtentTableConstructor* c, Options* opt,
                   BlockBasedTableOptions* table_options, const char* key_begin,
                   const char* key_end,
                   const std::vector<std::string>& keys_in_cache,
                   const std::vector<std::string>& keys_not_in_cache,
                   const Status expected_status = Status::OK()) {
  // reset the cache and reopen the table
  table_options->block_cache = NewLRUCache(16 * 1024 * 1024, 4);
  opt->table_factory.reset(NewExtentBasedTableFactory(*table_options));
  const ImmutableCFOptions ioptions2(*opt);
  ASSERT_OK(c->Reopen(ioptions2));

  // prefetch
  auto* table_reader = dynamic_cast<ExtentBasedTable*>(c->GetTableReader());
  Status s;
  unique_ptr<Slice> begin, end;
  unique_ptr<InternalKey> i_begin, i_end;
  if (key_begin != nullptr) {
    if (c->ConvertToInternalKey()) {
      i_begin.reset(new InternalKey(key_begin, kMaxSequenceNumber, kTypeValue));
      begin.reset(new Slice(i_begin->Encode()));
    } else {
      begin.reset(new Slice(key_begin));
    }
  }
  if (key_end != nullptr) {
    if (c->ConvertToInternalKey()) {
      i_end.reset(new InternalKey(key_end, kMaxSequenceNumber, kTypeValue));
      end.reset(new Slice(i_end->Encode()));
    } else {
      end.reset(new Slice(key_end));
    }
  }
  s = table_reader->Prefetch(begin.get(), end.get());

  ASSERT_TRUE(s.code() == expected_status.code());

  // assert our expectation in cache warmup
  AssertKeysInCache(table_reader, keys_in_cache, keys_not_in_cache,
                    c->ConvertToInternalKey());
  c->ResetTableReader();
}

TEST_F(BlockBasedTableTest, PrefetchTest) {
  // The purpose of this test is to test the prefetching operation built into
  // ExtentBasedTable.
  Options opt;
  unique_ptr<InternalKeyComparator> ikc;
  ikc.reset(new test::PlainInternalKeyComparator(opt.comparator));
  opt.compression = kNoCompression;
  BlockBasedTableOptions table_options;
  table_options.block_size = 1024;
  // big enough so we don't ever lose cached values.
  table_options.block_cache = NewLRUCache(16 * 1024 * 1024, 4);
  opt.table_factory.reset(NewExtentBasedTableFactory(table_options));

  ExtentTableConstructor c(BytewiseComparator(),
                           true /* convert_to_internal_key_ */);
  c.Add("k01", "hello");
  c.Add("k02", "hello2");
  c.Add("k03", std::string(10000, 'x'));
  c.Add("k04", std::string(200000, 'x'));
  c.Add("k05", std::string(300000, 'x'));
  c.Add("k06", "hello3");
  c.Add("k07", std::string(100000, 'x'));
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  const ImmutableCFOptions ioptions(opt);
  c.Finish(opt, ioptions, table_options, *ikc, &keys, &kvmap);
  c.ResetTableReader();

  // We get the following data spread :
  //
  // Data block         Index
  // ========================
  // [ k01 k02 k03 ]    k03
  // [ k04         ]    k04
  // [ k05         ]    k05
  // [ k06 k07     ]    k07

  // Simple
  PrefetchRange(&c, &opt, &table_options,
                /*key_range=*/"k01", "k05",
                /*keys_in_cache=*/{"k01", "k02", "k03", "k04", "k05"},
                /*keys_not_in_cache=*/{"k06", "k07"});
  PrefetchRange(&c, &opt, &table_options, "k01", "k01", {"k01", "k02", "k03"},
                {"k04", "k05", "k06", "k07"});
  // odd
  PrefetchRange(&c, &opt, &table_options, "a", "z",
                {"k01", "k02", "k03", "k04", "k05", "k06", "k07"}, {});
  PrefetchRange(&c, &opt, &table_options, "k00", "k00", {"k01", "k02", "k03"},
                {"k04", "k05", "k06", "k07"});
  // Edge cases
  PrefetchRange(&c, &opt, &table_options, "k00", "k06",
                {"k01", "k02", "k03", "k04", "k05", "k06", "k07"}, {});
  PrefetchRange(&c, &opt, &table_options, "k00", "zzz",
                {"k01", "k02", "k03", "k04", "k05", "k06", "k07"}, {});
  // null keys
  PrefetchRange(&c, &opt, &table_options, nullptr, nullptr,
                {"k01", "k02", "k03", "k04", "k05", "k06", "k07"}, {});
  PrefetchRange(&c, &opt, &table_options, "k04", nullptr,
                {"k04", "k05", "k06", "k07"}, {"k01", "k02", "k03"});
  PrefetchRange(&c, &opt, &table_options, nullptr, "k05",
                {"k01", "k02", "k03", "k04", "k05"}, {"k06", "k07"});
  // invalid
  PrefetchRange(&c, &opt, &table_options, "k06", "k00", {}, {},
                Status::InvalidArgument(Slice("k06 "), Slice("k07")));
  c.ResetTableReader();
}

TEST_F(BlockBasedTableTest, NoopTransformSeek) {
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));

  Options options;
  options.comparator = BytewiseComparator();
  options.table_factory.reset(new ExtentBasedTableFactory(table_options));
  options.prefix_extractor.reset(NewNoopTransform());

  ExtentTableConstructor c(options.comparator);
  // To tickle the PrefixMayMatch bug it is important that the
  // user-key is a single byte so that the index key exactly matches
  // the user-key.
  InternalKey key("a", 1, kTypeValue);
  c.Add(key.Encode().ToString(), "b");
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  const ImmutableCFOptions ioptions(options);
  const InternalKeyComparator internal_comparator(options.comparator);
  c.Finish(options, ioptions, table_options, internal_comparator, &keys,
           &kvmap);

  auto* reader = c.GetTableReader();
  for (int i = 0; i < 2; ++i) {
    ReadOptions ro;
    ro.total_order_seek = (i == 0);
    std::unique_ptr<InternalIterator> iter(reader->NewIterator(ro));

    iter->Seek(key.Encode());
    ASSERT_OK(iter->status());
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ("a", ExtractUserKey(iter->key()).ToString());
  }
}

TEST_F(BlockBasedTableTest, SkipPrefixBloomFilter) {
  // if DB is opened with a prefix extractor of a different name,
  // prefix bloom is skipped when read the file
  BlockBasedTableOptions table_options;
  table_options.filter_policy.reset(NewBloomFilterPolicy(2));
  table_options.whole_key_filtering = false;

  Options options;
  options.comparator = BytewiseComparator();
  options.table_factory.reset(new ExtentBasedTableFactory(table_options));
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));

  ExtentTableConstructor c(options.comparator);
  InternalKey key("abcdefghijk", 1, kTypeValue);
  c.Add(key.Encode().ToString(), "test");
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  const ImmutableCFOptions ioptions(options);
  const InternalKeyComparator internal_comparator(options.comparator);
  c.Finish(options, ioptions, table_options, internal_comparator, &keys,
           &kvmap);
  options.prefix_extractor.reset(NewFixedPrefixTransform(9));
  const ImmutableCFOptions new_ioptions(options);
  c.Reopen(new_ioptions);
  auto reader = c.GetTableReader();
  std::unique_ptr<InternalIterator> db_iter(reader->NewIterator(ReadOptions()));

  // Test point lookup
  // only one kv
  for (auto& kv : kvmap) {
    db_iter->Seek(kv.first);
    ASSERT_TRUE(db_iter->Valid());
    ASSERT_OK(db_iter->status());
    ASSERT_EQ(db_iter->key(), kv.first);
    ASSERT_EQ(db_iter->value(), kv.second);
  }
}

static std::string RandomString(Random* rnd, int len) {
  std::string r;
  test::RandomString(rnd, len, &r);
  return r;
}

void AddInternalKey(ExtentTableConstructor* c, const std::string& prefix,
                    int suffix_len = 800) {
  static Random rnd(1023);
  InternalKey k(prefix + RandomString(&rnd, 800), 0, kTypeValue);
  c->Add(k.Encode().ToString(), "v");
}

void TableTest::IndexTest(BlockBasedTableOptions table_options) {
  ExtentTableConstructor c(BytewiseComparator());

  // keys with prefix length 3, make sure the key/value is big enough to fill
  // one block
  AddInternalKey(&c, "0015");
  AddInternalKey(&c, "0035");

  AddInternalKey(&c, "0054");
  AddInternalKey(&c, "0055");

  AddInternalKey(&c, "0056");
  AddInternalKey(&c, "0057");

  AddInternalKey(&c, "0058");
  AddInternalKey(&c, "0075");

  AddInternalKey(&c, "0076");
  AddInternalKey(&c, "0095");

  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(3));
  table_options.block_size = 1700;
  table_options.block_cache = NewLRUCache(1024, 4);
  options.table_factory.reset(NewExtentBasedTableFactory(table_options));

  std::unique_ptr<InternalKeyComparator> comparator(
      new InternalKeyComparator(BytewiseComparator()));
  const ImmutableCFOptions ioptions(options);
  c.Finish(options, ioptions, table_options, *comparator, &keys, &kvmap);
  auto reader = c.GetTableReader();

  auto props = reader->GetTableProperties();
  ASSERT_EQ(5u, props->num_data_blocks);

  std::unique_ptr<InternalIterator> index_iter(
      reader->NewIterator(ReadOptions()));

  // -- Find keys do not exist, but have common prefix.
  std::vector<std::string> prefixes = {"001", "003", "005", "007", "009"};
  std::vector<std::string> lower_bound = {
      keys[0], keys[1], keys[2], keys[7], keys[9],
  };

  // find the lower bound of the prefix
  for (size_t i = 0; i < prefixes.size(); ++i) {
    index_iter->Seek(InternalKey(prefixes[i], 0, kTypeValue).Encode());
    ASSERT_OK(index_iter->status());
    ASSERT_TRUE(index_iter->Valid());

    // seek the first element in the block
    ASSERT_EQ(lower_bound[i], index_iter->key().ToString());
    ASSERT_EQ("v", index_iter->value().ToString());
  }

  // find the upper bound of prefixes
  std::vector<std::string> upper_bound = {
      keys[1], keys[2], keys[7], keys[9],
  };

  // find existing keys
  for (const auto& item : kvmap) {
    auto ukey = ExtractUserKey(item.first).ToString();
    index_iter->Seek(ukey);

    // ASSERT_OK(regular_iter->status());
    ASSERT_OK(index_iter->status());

    // ASSERT_TRUE(regular_iter->Valid());
    ASSERT_TRUE(index_iter->Valid());

    ASSERT_EQ(item.first, index_iter->key().ToString());
    ASSERT_EQ(item.second, index_iter->value().ToString());
  }

  for (size_t i = 0; i < prefixes.size(); ++i) {
    // the key is greater than any existing keys.
    auto key = prefixes[i] + "9";
    index_iter->Seek(InternalKey(key, 0, kTypeValue).Encode());

    ASSERT_OK(index_iter->status());
    if (i == prefixes.size() - 1) {
      // last key
      ASSERT_TRUE(!index_iter->Valid());
    } else {
      ASSERT_TRUE(index_iter->Valid());
      // seek the first element in the block
      ASSERT_EQ(upper_bound[i], index_iter->key().ToString());
      ASSERT_EQ("v", index_iter->value().ToString());
    }
  }

  // find keys with prefix that don't match any of the existing prefixes.
  std::vector<std::string> non_exist_prefixes = {"002", "004", "006", "008"};
  for (const auto& prefix : non_exist_prefixes) {
    index_iter->Seek(InternalKey(prefix, 0, kTypeValue).Encode());
    // regular_iter->Seek(prefix);

    ASSERT_OK(index_iter->status());
    // Seek to non-existing prefixes should yield either invalid, or a
    // key with prefix greater than the target.
    if (index_iter->Valid()) {
      Slice ukey = ExtractUserKey(index_iter->key());
      Slice ukey_prefix = options.prefix_extractor->Transform(ukey);
      ASSERT_TRUE(BytewiseComparator()->Compare(prefix, ukey_prefix) < 0);
    }
  }
  c.ResetTableReader();
}

TEST_F(TableTest, BinaryIndexTest) {
  BlockBasedTableOptions table_options;
  table_options.index_type = BlockBasedTableOptions::kBinarySearch;
  IndexTest(table_options);
}

// It's very hard to figure out the index block size of a block accurately.
// To make sure we get the index size, we just make sure as key number
// grows, the filter block size also grows.
TEST_F(BlockBasedTableTest, IndexSizeStat) {
  uint64_t last_index_size = 0;

  // we need to use random keys since the pure human readable texts
  // may be well compressed, resulting insignifcant change of index
  // block size.
  Random rnd(test::RandomSeed());
  std::vector<std::string> keys;

  for (int i = 0; i < 10; ++i) {  // TODO: 100 is too large in 2MB
    keys.push_back(RandomString(&rnd, 10000));
  }

  // Each time we load one more key to the table. the table index block
  // size is expected to be larger than last time's.
  for (size_t i = 1; i < keys.size(); ++i) {
    ExtentTableConstructor c(BytewiseComparator(),
                             true /* convert_to_internal_key_ */);
    for (size_t j = 0; j < i; ++j) {
      c.Add(keys[j], "val");
    }

    std::vector<std::string> ks;
    stl_wrappers::KVMap kvmap;
    Options options;
    options.compression = kNoCompression;
    BlockBasedTableOptions table_options;
    table_options.block_restart_interval = 1;
    options.table_factory.reset(NewExtentBasedTableFactory(table_options));

    const ImmutableCFOptions ioptions(options);
    c.Finish(options, ioptions, table_options,
             GetPlainInternalComparator(options.comparator), &ks, &kvmap);
    auto index_size = c.GetTableReader()->GetTableProperties()->index_size;
    ASSERT_GT(index_size, last_index_size);
    last_index_size = index_size;
    c.ResetTableReader();
  }
}

TEST_F(BlockBasedTableTest, NumBlockStat) {
  Random rnd(test::RandomSeed());
  ExtentTableConstructor c(BytewiseComparator(),
                           true /* convert_to_internal_key_ */);
  Options options;
  options.compression = kNoCompression;
  BlockBasedTableOptions table_options;
  table_options.block_restart_interval = 1;
  table_options.block_size = 1000;
  options.table_factory.reset(NewExtentBasedTableFactory(table_options));

  for (int i = 0; i < 10; ++i) {
    // the key/val are slightly smaller than block size, so that each block
    // holds roughly one key/value pair.
    c.Add(RandomString(&rnd, 900), "val");
  }

  std::vector<std::string> ks;
  stl_wrappers::KVMap kvmap;
  const ImmutableCFOptions ioptions(options);
  c.Finish(options, ioptions, table_options,
           GetPlainInternalComparator(options.comparator), &ks, &kvmap);
  ASSERT_EQ(kvmap.size(),
            c.GetTableReader()->GetTableProperties()->num_data_blocks);
  c.ResetTableReader();
}

// A simple tool that takes the snapshot of block cache statistics.
class BlockCachePropertiesSnapshot {
 public:
  explicit BlockCachePropertiesSnapshot(Statistics* statistics) {
    block_cache_miss = statistics->getTickerCount(BLOCK_CACHE_MISS);
    block_cache_hit = statistics->getTickerCount(BLOCK_CACHE_HIT);
    index_block_cache_miss = statistics->getTickerCount(BLOCK_CACHE_INDEX_MISS);
    index_block_cache_hit = statistics->getTickerCount(BLOCK_CACHE_INDEX_HIT);
    data_block_cache_miss = statistics->getTickerCount(BLOCK_CACHE_DATA_MISS);
    data_block_cache_hit = statistics->getTickerCount(BLOCK_CACHE_DATA_HIT);
    filter_block_cache_miss =
        statistics->getTickerCount(BLOCK_CACHE_FILTER_MISS);
    filter_block_cache_hit = statistics->getTickerCount(BLOCK_CACHE_FILTER_HIT);
    block_cache_bytes_read = statistics->getTickerCount(BLOCK_CACHE_BYTES_READ);
    block_cache_bytes_write =
        statistics->getTickerCount(BLOCK_CACHE_BYTES_WRITE);
  }

  void AssertIndexBlockStat(int64_t expected_index_block_cache_miss,
                            int64_t expected_index_block_cache_hit) {
    ASSERT_EQ(expected_index_block_cache_miss, index_block_cache_miss);
    ASSERT_EQ(expected_index_block_cache_hit, index_block_cache_hit);
  }

  void AssertFilterBlockStat(int64_t expected_filter_block_cache_miss,
                             int64_t expected_filter_block_cache_hit) {
    ASSERT_EQ(expected_filter_block_cache_miss, filter_block_cache_miss);
    ASSERT_EQ(expected_filter_block_cache_hit, filter_block_cache_hit);
  }

  // Check if the fetched props matches the expected ones.
  // TODO(kailiu) Use this only when you disabled filter policy!
  void AssertEqual(int64_t expected_index_block_cache_miss,
                   int64_t expected_index_block_cache_hit,
                   int64_t expected_data_block_cache_miss,
                   int64_t expected_data_block_cache_hit) const {
    ASSERT_EQ(expected_index_block_cache_miss, index_block_cache_miss);
    ASSERT_EQ(expected_index_block_cache_hit, index_block_cache_hit);
    ASSERT_EQ(expected_data_block_cache_miss, data_block_cache_miss);
    ASSERT_EQ(expected_data_block_cache_hit, data_block_cache_hit);
    ASSERT_EQ(expected_index_block_cache_miss + expected_data_block_cache_miss,
              block_cache_miss);
    ASSERT_EQ(expected_index_block_cache_hit + expected_data_block_cache_hit,
              block_cache_hit);
  }

  int64_t GetCacheBytesRead() { return block_cache_bytes_read; }

  int64_t GetCacheBytesWrite() { return block_cache_bytes_write; }

 private:
  int64_t block_cache_miss = 0;
  int64_t block_cache_hit = 0;
  int64_t index_block_cache_miss = 0;
  int64_t index_block_cache_hit = 0;
  int64_t data_block_cache_miss = 0;
  int64_t data_block_cache_hit = 0;
  int64_t filter_block_cache_miss = 0;
  int64_t filter_block_cache_hit = 0;
  int64_t block_cache_bytes_read = 0;
  int64_t block_cache_bytes_write = 0;
};

// Make sure, by default, index/filter blocks were pre-loaded (meaning we won't
// use block cache to store them).
TEST_F(BlockBasedTableTest, BlockCacheDisabledTest) {
  Options options;
  options.create_if_missing = true;
  options.statistics = CreateDBStatistics();
  BlockBasedTableOptions table_options;
  table_options.block_cache = NewLRUCache(1024, 4);
  table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  options.table_factory.reset(new ExtentBasedTableFactory(table_options));
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;

  ExtentTableConstructor c(BytewiseComparator(),
                           true /* convert_to_internal_key_ */);
  c.Add("key", "value");
  const ImmutableCFOptions ioptions(options);
  c.Finish(options, ioptions, table_options,
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);

  // preloading filter/index blocks is enabled.
  auto reader = dynamic_cast<ExtentBasedTable*>(c.GetTableReader());
  // ASSERT_TRUE(reader->TEST_filter_block_preloaded());
  ASSERT_TRUE(reader->TEST_index_reader_preloaded());

  {
    // nothing happens in the beginning
    BlockCachePropertiesSnapshot props(options.statistics.get());
    props.AssertIndexBlockStat(0, 0);
    props.AssertFilterBlockStat(0, 0);
  }

  {
    GetContext get_context(options.comparator, nullptr, nullptr, nullptr,
                           GetContext::kNotFound, Slice(), nullptr, nullptr,
                           nullptr, nullptr, nullptr);
    // a hack that just to trigger ExtentBasedTable::GetFilter.
    reader->Get(ReadOptions(), "non-exist-key", &get_context);
    BlockCachePropertiesSnapshot props(options.statistics.get());
    props.AssertIndexBlockStat(0, 0);
    props.AssertFilterBlockStat(0, 0);
  }
}

void ValidateBlockSizeDeviation(int value, int expected) {
  BlockBasedTableOptions table_options;
  table_options.block_size_deviation = value;
  ExtentBasedTableFactory* factory = new ExtentBasedTableFactory(table_options);

  const BlockBasedTableOptions* normalized_table_options =
      (const BlockBasedTableOptions*)factory->GetOptions();
  ASSERT_EQ(normalized_table_options->block_size_deviation, expected);

  delete factory;
}

void ValidateBlockRestartInterval(int value, int expected) {
  BlockBasedTableOptions table_options;
  table_options.block_restart_interval = value;
  ExtentBasedTableFactory* factory = new ExtentBasedTableFactory(table_options);

  const BlockBasedTableOptions* normalized_table_options =
      (const BlockBasedTableOptions*)factory->GetOptions();
  ASSERT_EQ(normalized_table_options->block_restart_interval, expected);

  delete factory;
}

TEST_F(BlockBasedTableTest, InvalidOptions) {
  // invalid values for block_size_deviation (<0 or >100) are silently set to 0
  ValidateBlockSizeDeviation(-10, 0);
  ValidateBlockSizeDeviation(-1, 0);
  ValidateBlockSizeDeviation(0, 0);
  ValidateBlockSizeDeviation(1, 1);
  ValidateBlockSizeDeviation(99, 99);
  ValidateBlockSizeDeviation(100, 100);
  ValidateBlockSizeDeviation(101, 0);
  ValidateBlockSizeDeviation(1000, 0);

  // invalid values for block_restart_interval (<1) are silently set to 1
  ValidateBlockRestartInterval(-10, 1);
  ValidateBlockRestartInterval(-1, 1);
  ValidateBlockRestartInterval(0, 1);
  ValidateBlockRestartInterval(1, 1);
  ValidateBlockRestartInterval(2, 2);
  ValidateBlockRestartInterval(1000, 1000);
}

TEST_F(BlockBasedTableTest, BlockCacheLeak) {
  // Check that when we reopen a table we don't lose access to blocks already
  // in the cache. This test checks whether the Table actually makes use of the
  // unique ID from the file.

  Options opt;
  unique_ptr<InternalKeyComparator> ikc;
  ikc.reset(new test::PlainInternalKeyComparator(opt.comparator));
  opt.compression = kNoCompression;
  BlockBasedTableOptions table_options;
  table_options.block_size = 1024;
  // big enough so we don't ever lose cached values.
  table_options.block_cache = NewLRUCache(16 * 1024 * 1024, 4);
  opt.table_factory.reset(NewExtentBasedTableFactory(table_options));

  ExtentTableConstructor c(BytewiseComparator(),
                           true /* convert_to_internal_key_ */);
  c.Add("k01", "hello");
  c.Add("k02", "hello2");
  c.Add("k03", std::string(10000, 'x'));
  c.Add("k04", std::string(200000, 'x'));
  c.Add("k05", std::string(300000, 'x'));
  c.Add("k06", "hello3");
  c.Add("k07", std::string(100000, 'x'));
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  const ImmutableCFOptions ioptions(opt);
  c.Finish(opt, ioptions, table_options, *ikc, &keys, &kvmap);

  unique_ptr<InternalIterator> iter(c.NewIterator());
  iter->SeekToFirst();
  while (iter->Valid()) {
    iter->key();
    iter->value();
    iter->Next();
  }
  ASSERT_OK(iter->status());

  const ImmutableCFOptions ioptions1(opt);
  ASSERT_OK(c.Reopen(ioptions1));
  auto table_reader = dynamic_cast<ExtentBasedTable*>(c.GetTableReader());
  for (const std::string& key : keys) {
    // FIXME the extent now can't support
    // reopen and find  the blocks in the cache
    ASSERT_TRUE(!table_reader->TEST_KeyInCache(ReadOptions(), key));
  }

  c.ResetTableReader();

  // rerun with different block cache
  table_options.block_cache = NewLRUCache(16 * 1024 * 1024, 4);
  opt.table_factory.reset(NewExtentBasedTableFactory(table_options));
  const ImmutableCFOptions ioptions2(opt);
  ASSERT_OK(c.Reopen(ioptions2));
  table_reader = dynamic_cast<ExtentBasedTable*>(c.GetTableReader());
  for (const std::string& key : keys) {
    ASSERT_TRUE(!table_reader->TEST_KeyInCache(ReadOptions(), key));
  }
  c.ResetTableReader();
}

TEST_F(BlockBasedTableTest, NewIndexIteratorLeak) {
  // A regression test to avoid data race described in
  // https://github.com/facebook/rocksdb/issues/1267
  ExtentTableConstructor c(BytewiseComparator(),
                           true /* convert_to_internal_key_ */);
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  c.Add("a1", "val1");
  Options options;
  options.prefix_extractor.reset(NewFixedPrefixTransform(1));
  BlockBasedTableOptions table_options;
  table_options.index_type = BlockBasedTableOptions::kHashSearch;
  table_options.cache_index_and_filter_blocks = true;
  table_options.block_cache = NewLRUCache(0);
  options.table_factory.reset(NewExtentBasedTableFactory(table_options));
  const ImmutableCFOptions ioptions(options);
  c.Finish(options, ioptions, table_options,
           GetPlainInternalComparator(options.comparator), &keys, &kvmap);

  SyncPoint::GetInstance()->LoadDependencyAndMarkers(
      {
          {"ExtentBasedTable::NewIndexIterator::thread1:1",
           "ExtentBasedTable::NewIndexIterator::thread2:2"},
          {"ExtentBasedTable::NewIndexIterator::thread2:3",
           "ExtentBasedTable::NewIndexIterator::thread1:4"},
      },
      {
          {"BlockBasedTableTest::NewIndexIteratorLeak:Thread1Marker",
           "ExtentBasedTable::NewIndexIterator::thread1:1"},
          {"BlockBasedTableTest::NewIndexIteratorLeak:Thread1Marker",
           "ExtentBasedTable::NewIndexIterator::thread1:4"},
          {"BlockBasedTableTest::NewIndexIteratorLeak:Thread2Marker",
           "ExtentBasedTable::NewIndexIterator::thread2:2"},
          {"BlockBasedTableTest::NewIndexIteratorLeak:Thread2Marker",
           "ExtentBasedTable::NewIndexIterator::thread2:3"},
      });

  SyncPoint::GetInstance()->EnableProcessing();
  ReadOptions ro;
  auto* reader = c.GetTableReader();

  std::function<void()> func1 = [&]() {
    TEST_SYNC_POINT("BlockBasedTableTest::NewIndexIteratorLeak:Thread1Marker");
    std::unique_ptr<InternalIterator> iter(reader->NewIterator(ro));
    iter->Seek(InternalKey("a1", 0, kTypeValue).Encode());
  };

  std::function<void()> func2 = [&]() {
    TEST_SYNC_POINT("BlockBasedTableTest::NewIndexIteratorLeak:Thread2Marker");
    std::unique_ptr<InternalIterator> iter(reader->NewIterator(ro));
  };

  auto thread1 = port::Thread(func1);
  auto thread2 = port::Thread(func2);
  thread1.join();
  thread2.join();
  SyncPoint::GetInstance()->DisableProcessing();
  c.ResetTableReader();
}

TEST_F(GeneralTableTest, ApproximateOffsetOfPlain) {
  ExtentTableConstructor c(BytewiseComparator(),
                           true /* convert_to_internal_key_ */);
  c.Add("k01", "hello");
  c.Add("k02", "hello2");
  c.Add("k03", std::string(10000, 'x'));
  c.Add("k04", std::string(200000, 'x'));
  c.Add("k05", std::string(300000, 'x'));
  c.Add("k06", "hello3");
  c.Add("k07", std::string(100000, 'x'));
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  test::PlainInternalKeyComparator internal_comparator(options.comparator);
  options.compression = kNoCompression;
  BlockBasedTableOptions table_options;
  table_options.block_size = 1024;
  options.table_factory.reset(NewExtentBasedTableFactory(table_options));
  const ImmutableCFOptions ioptions(options);
  c.Finish(options, ioptions, table_options, internal_comparator, &keys,
           &kvmap);

  ASSERT_TRUE(Between(c.ApproximateOffsetOf("abc"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k01"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k01a"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k02"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k03"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k04"), 10000, 11000));
  // k04 and k05 will be in two consecutive blocks, the index is
  // an arbitrary slice between k04 and k05, either before or after k04a
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k04a"), 10000, 211000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k05"), 210000, 211000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k06"), 510000, 511000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k07"), 510000, 511000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("xyz"), 610000, 612000));
  c.ResetTableReader();
}

static void DoCompressionTest(CompressionType comp) {
  Random rnd(301);
  ExtentTableConstructor c(BytewiseComparator(),
                           true /* convert_to_internal_key_ */);
  std::string tmp;
  c.Add("k01", "hello");
  c.Add("k02", test::CompressibleString(&rnd, 0.25, 10000, &tmp));
  c.Add("k03", "hello3");
  c.Add("k04", test::CompressibleString(&rnd, 0.25, 10000, &tmp));
  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  test::PlainInternalKeyComparator ikc(options.comparator);
  options.compression = comp;
  BlockBasedTableOptions table_options;
  table_options.block_size = 1024;
  options.table_factory.reset(NewExtentBasedTableFactory(table_options));
  const ImmutableCFOptions ioptions(options);
  c.Finish(options, ioptions, table_options, ikc, &keys, &kvmap);

  ASSERT_TRUE(Between(c.ApproximateOffsetOf("abc"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k01"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k02"), 0, 0));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k03"), 2000, 3000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("k04"), 2000, 3000));
  ASSERT_TRUE(Between(c.ApproximateOffsetOf("xyz"), 4000, 6100));
  c.ResetTableReader();
}

TEST_F(GeneralTableTest, ApproximateOffsetOfCompressed) {
  std::vector<CompressionType> compression_state;
  if (!Snappy_Supported()) {
    fprintf(stderr, "skipping snappy compression tests\n");
  } else {
    compression_state.push_back(kSnappyCompression);
  }

  if (!Zlib_Supported()) {
    fprintf(stderr, "skipping zlib compression tests\n");
  } else {
    compression_state.push_back(kZlibCompression);
  }

  // TODO(kailiu) DoCompressionTest() doesn't work with BZip2.
  /*
  if (!BZip2_Supported()) {
    fprintf(stderr, "skipping bzip2 compression tests\n");
  } else {
    compression_state.push_back(kBZip2Compression);
  }
  */

  if (!LZ4_Supported()) {
    fprintf(stderr, "skipping lz4 and lz4hc compression tests\n");
  } else {
    compression_state.push_back(kLZ4Compression);
    compression_state.push_back(kLZ4HCCompression);
  }

  if (!XPRESS_Supported()) {
    fprintf(stderr, "skipping xpress and xpress compression tests\n");
  } else {
    compression_state.push_back(kXpressCompression);
  }

  for (auto state : compression_state) {
    DoCompressionTest(state);
  }
}

TEST_F(HarnessTest, Randomized) {
  std::vector<TestArgs> args = GenerateArgList();
  for (unsigned int i = 0; i < args.size(); i++) {
    Init(args[i]);
    Random rnd(test::RandomSeed() + 5);
    for (int num_entries = 0; num_entries < 2000;
         num_entries += (num_entries < 50 ? 1 : 200)) {
      if ((num_entries % 10) == 0) {
        fprintf(stderr, "case %d of %d: num_entries = %d\n", (i + 1),
                static_cast<int>(args.size()), num_entries);
      }
      for (int e = 0; e < num_entries; e++) {
        std::string v;
        Add(test::RandomKey(&rnd, rnd.Skewed(4)),
            test::RandomString(&rnd, rnd.Skewed(5), &v).ToString());
      }
      Test(&rnd);
    }
  }
}

class MemTableTest : public testing::Test {};

TEST_F(MemTableTest, Simple) {
  InternalKeyComparator cmp(BytewiseComparator());
  auto table_factory = std::make_shared<memtable::SkipListFactory>();
  Options options;
  options.memtable_factory = table_factory;
  ImmutableCFOptions ioptions(options);
  WriteBufferManager wb(options.db_write_buffer_size);
  MemTable* memtable = new MemTable(cmp, ioptions, MutableCFOptions(options),
                                    &wb, kMaxSequenceNumber);
  memtable->Ref();
  WriteBatch batch;
  WriteBatchInternal::SetSequence(&batch, 100);
  batch.Put(std::string("k1"), std::string("v1"));
  batch.Put(std::string("k2"), std::string("v2"));
  batch.Put(std::string("k3"), std::string("v3"));
  batch.Put(std::string("largekey"), std::string("vlarge"));
  batch.DeleteRange(std::string("chi"), std::string("xigua"));
  batch.DeleteRange(std::string("begin"), std::string("end"));
  ColumnFamilyMemTablesDefault cf_mems_default(memtable);
  ASSERT_TRUE(
      WriteBatchInternal::InsertInto(&batch, &cf_mems_default, nullptr).ok());

  for (int i = 0; i < 2; ++i) {
    Arena arena;
    ScopedArenaIterator arena_iter_guard;
    std::unique_ptr<InternalIterator, memory::ptr_delete<InternalIterator>> iter_guard;
    InternalIterator* iter;
    if (i == 0) {
      iter = memtable->NewIterator(ReadOptions(), &arena);
      arena_iter_guard.set(iter);
    } else {
      iter = memtable->NewRangeTombstoneIterator(ReadOptions());
      iter_guard.reset(iter);
    }
    if (iter == nullptr) {
      continue;
    }
    iter->SeekToFirst();
    while (iter->Valid()) {
      fprintf(stderr, "key: '%s' -> '%s'\n", iter->key().ToString().c_str(),
              iter->value().ToString().c_str());
      iter->Next();
    }
  }

  delete memtable->Unref();
}

// Test the empty key
TEST_F(HarnessTest, SimpleEmptyKey) {
  auto args = GenerateArgList();
  for (const auto& arg : args) {
    Init(arg);
    Random rnd(test::RandomSeed() + 1);
    Add("", "v");
    Test(&rnd);
  }
}

TEST_F(HarnessTest, SimpleSingle) {
  auto args = GenerateArgList();
  for (const auto& arg : args) {
    Init(arg);
    Random rnd(test::RandomSeed() + 2);
    Add("abc", "v");
    Test(&rnd);
  }
}

TEST_F(HarnessTest, SimpleMulti) {
  auto args = GenerateArgList();
  for (const auto& arg : args) {
    Init(arg);
    Random rnd(test::RandomSeed() + 3);
    Add("abc", "v");
    Add("abcd", "v");
    Add("ac", "v2");
    Test(&rnd);
  }
}

TEST_F(HarnessTest, SimpleSpecialKey) {
  auto args = GenerateArgList();
  for (const auto& arg : args) {
    Init(arg);
    Random rnd(test::RandomSeed() + 4);
    Add("\xff\xff", "v3");
    Test(&rnd);
  }
}

class IndexBlockRestartIntervalTest
    : public BlockBasedTableTest,
      public ::testing::WithParamInterface<int> {
 public:
  static std::vector<int> GetRestartValues() { return {-1, 0, 1, 8, 16, 32}; }
};

INSTANTIATE_TEST_CASE_P(
    IndexBlockRestartIntervalTest, IndexBlockRestartIntervalTest,
    ::testing::ValuesIn(IndexBlockRestartIntervalTest::GetRestartValues()));

TEST_P(IndexBlockRestartIntervalTest, IndexBlockRestartInterval) {
  // const int kKeysInTable = 10000;
  const int kKeysInTable = 1000;  // TODO
  const int kKeySize = 100;
  const int kValSize = 500;

  int index_block_restart_interval = GetParam();

  Options options;
  BlockBasedTableOptions table_options;
  table_options.block_size = 64;  // small block size to get big index block
  table_options.index_block_restart_interval = index_block_restart_interval;
  options.table_factory.reset(new ExtentBasedTableFactory(table_options));

  ExtentTableConstructor c(BytewiseComparator());
  static Random rnd(301);
  for (int i = 0; i < kKeysInTable; i++) {
    InternalKey k(RandomString(&rnd, kKeySize), 0, kTypeValue);
    c.Add(k.Encode().ToString(), RandomString(&rnd, kValSize));
  }

  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  std::unique_ptr<InternalKeyComparator> comparator(
      new InternalKeyComparator(BytewiseComparator()));
  const ImmutableCFOptions ioptions(options);
  c.Finish(options, ioptions, table_options, *comparator, &keys, &kvmap);
  auto reader = c.GetTableReader();

  std::unique_ptr<InternalIterator> db_iter(reader->NewIterator(ReadOptions()));

  // Test point lookup
  for (auto& kv : kvmap) {
    db_iter->Seek(kv.first);

    ASSERT_TRUE(db_iter->Valid());
    ASSERT_OK(db_iter->status());
    ASSERT_EQ(db_iter->key(), kv.first);
    ASSERT_EQ(db_iter->value(), kv.second);
  }

  // Test iterating
  auto kv_iter = kvmap.begin();
  for (db_iter->SeekToFirst(); db_iter->Valid(); db_iter->Next()) {
    ASSERT_EQ(db_iter->key(), kv_iter->first);
    ASSERT_EQ(db_iter->value(), kv_iter->second);
    kv_iter++;
  }
  ASSERT_EQ(kv_iter, kvmap.end());
  c.ResetTableReader();
}

class PrefixTest : public testing::Test {
 public:
  PrefixTest() : testing::Test() {}
  ~PrefixTest() {}
};

namespace {
// A simple PrefixExtractor that only works for test PrefixAndWholeKeyTest
class TestPrefixExtractor : public SliceTransform {
 public:
  ~TestPrefixExtractor() override{};
  const char* Name() const override { return "TestPrefixExtractor"; }

  Slice Transform(const Slice& src) const override {
    assert(IsValid(src));
    return Slice(src.data(), 3);
  }

  bool InDomain(const Slice& src) const override {
    assert(IsValid(src));
    return true;
  }

  bool InRange(const Slice& dst) const override { return true; }

  bool IsValid(const Slice& src) const {
    if (src.size() != 4) {
      return false;
    }
    if (src[0] != '[') {
      return false;
    }
    if (src[1] < '0' || src[1] > '9') {
      return false;
    }
    if (src[2] != ']') {
      return false;
    }
    if (src[3] < '0' || src[3] > '9') {
      return false;
    }
    return true;
  }
};
}  // namespace

#if 0
TEST_F(PrefixTest, PrefixAndWholeKeyTest) {
  rocksdb::Options options;
  options.compaction_style = rocksdb::kCompactionStyleUniversal;
  options.num_levels = 20;
  options.create_if_missing = true;
  options.optimize_filters_for_hits = false;
  options.target_file_size_base = 268435456;
  options.prefix_extractor = std::make_shared<TestPrefixExtractor>();
  rocksdb::BlockBasedTableOptions bbto;
  bbto.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10));
  bbto.block_size = 262144;
  bbto.whole_key_filtering = true;

  const std::string kDBPath = test::TmpDir() + "/table_prefix_test";
  options.table_factory.reset(NewExtentBasedTableFactory(bbto));
  DestroyDB(kDBPath, options);
  rocksdb::DB* db;
  ASSERT_OK(rocksdb::DB::Open(options, kDBPath, &db));

  // Create a bunch of keys with 10 filters.
  for (int i = 0; i < 10; i++) {
    std::string prefix = "[" + std::to_string(i) + "]";
    for (int j = 0; j < 10; j++) {
      std::string key = prefix + std::to_string(j);
      db->Put(rocksdb::WriteOptions(), key, "1");
    }
  }

  // Trigger compaction.
  db->CompactRange(CompactRangeOptions(), nullptr, nullptr);
  delete db;
  // In the second round, turn whole_key_filtering off and expect
  // rocksdb still works.
}
#endif

TEST_F(ExtentBasedTableTest, get_data_block) {
  ExtentTableConstructor c(BytewiseComparator(),
                           false /* convert_to_internal_key_ */);

  InternalKey key("aaa", 0x12345678876543, kTypeValue);
  c.Add(key.Encode().ToString(), "bbb");

  std::vector<std::string> keys;
  stl_wrappers::KVMap kvmap;
  Options options;
  options.compression = kNoCompression;
  BlockBasedTableOptions table_options;
  table_options.block_restart_interval = 1;
  options.table_factory.reset(NewExtentBasedTableFactory(table_options));
  const ImmutableCFOptions ioptions(options);
  InternalKeyComparator icmp(options.comparator);
  c.Finish(options, ioptions, table_options, icmp, &keys, &kvmap);

  auto* reader = dynamic_cast<ExtentBasedTable*>(c.GetTableReader());
  assert(reader != nullptr);

  BlockIter iiter_on_stack;
  ExtentBasedTable::IndexReader* index_reader = nullptr;
  memory::ArenaAllocator alloc;
  auto iiter = reader->create_index_iterator(ReadOptions(), &iiter_on_stack,
                                             index_reader, alloc);
  std::unique_ptr<InternalIterator> iiter_unique_ptr;
  if (iiter != &iiter_on_stack) {
    iiter_unique_ptr = std::unique_ptr<InternalIterator>(iiter);
  }
  ASSERT_TRUE(iiter->status().ok());
  iiter->Seek(key.Encode());
  ASSERT_TRUE(iiter->Valid());

  BlockHandle handle;
  Slice input = iiter->value();
  Status s = handle.DecodeFrom(&input);
  ASSERT_TRUE(s.ok());

  std::unique_ptr<char[]> buf(new char[handle.size() + kBlockTrailerSize]);
  Slice block(buf.get(), handle.size() + kBlockTrailerSize);
  int ret = reader->get_data_block(handle, block, true);
  ASSERT_TRUE(ret == Status::kOk);

  c.ResetTableReader();
}

}  // namespace table
}  // namespace xengine

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
	xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
