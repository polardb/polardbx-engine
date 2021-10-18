/*
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
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
#include "xengine/status.h"
#include "xengine/parallel_read.h"
#include "xengine/table.h"
#include "xengine/env.h"
#include "table/extent_table_factory.h"
#include "db/builder.h"
#include "options/options_helper.h"
#include "xengine/options.h"
#include "xengine/xengine_constants.h"
#include "table/table_reader.h"
#include "table/merging_iterator.h"
#include "xengine/types.h"
#include "xengine/write_buffer_manager.h"
#include "db/dbformat.h"
#include "db/db_impl.h"
#include "db/db_iter.h"
#include "monitoring/instrumented_mutex.h"
#include "db/db_test_util.h"
#include "xengine/utilities/transaction_db.h"
#include "utilities/transactions/transaction_db_impl.h"
#include "compact/compaction_job.h"
#include <atomic>
#include <cstring>
#include <chrono>


using namespace xengine;
using namespace storage;
using namespace common;
using namespace db;
using namespace cache;
using namespace table;
using namespace memtable;
using namespace util;
using namespace std;
using namespace monitor;
using namespace chrono;

struct Context {
  const Options *options_;
  DBOptions db_options_;
  EnvOptions env_options_;
  ImmutableDBOptions idb_options_;
  MutableCFOptions mutable_cf_options_;
  ImmutableCFOptions icf_options_;

  Context(const Options &opt)
      : options_(&opt),
        db_options_(opt),
        env_options_(db_options_),
        idb_options_(opt),
        mutable_cf_options_(opt),
        icf_options_(opt) {}
};

struct TestArgs {
  CompressionType compression;
  uint32_t format_version;

  TestArgs() : compression(kNoCompression), format_version(3) {}
};

void build_default_options(const TestArgs &args, common::Options &opt) {
  std::map<std::string, std::string>::const_iterator itr;

  BlockBasedTableOptions table_options;
  // table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  int block_size = 16 * 1024;
  //int block_size = 65535;
  table_options.block_size = block_size;
  // table_options.format_version = args.format_version;
  opt.table_factory.reset(NewExtentBasedTableFactory(table_options));
  opt.disable_auto_compactions = true;
  opt.compression = args.compression;
  opt.create_if_missing = true;
  opt.fail_if_options_file_error = true;
  opt.create_missing_column_families = true;
  opt.env = Env::Default();
  int db_write_buffer_size = 64 * 1024 * 1024;
  opt.db_write_buffer_size = db_write_buffer_size;
  int write_buffer_size = db_write_buffer_size;
  opt.write_buffer_size = write_buffer_size;

  int file_size = db_write_buffer_size * 1024;
  opt.target_file_size_base = file_size;

  opt.minor_window_size = 4;

  opt.compaction_type = 1; // should be 1 here

  std::string db_path_ = test::TmpDir() + "/parallel_read_test";
  if (opt.db_paths.size() == 0) {
    opt.db_paths.emplace_back(db_path_, std::numeric_limits<uint64_t>::max());
  }

  auto factory = std::make_shared<SkipListFactory>();
  opt.memtable_factory = factory;
  WriteBufferManager *wb = new WriteBufferManager(0);  // no limit space
  assert(wb != nullptr);
  // no free here ...
  opt.write_buffer_manager.reset(wb);
}

Context *get_default_context(const TestArgs &args) {
  common::Options *opt = new common::Options();
  build_default_options(args, *opt);
  Context *context = new Context(*opt);
  return context;
}

int parse_meta(const table::InternalIterator *iterator, ExtentMeta &extent_meta)
{
  int ret = Status::kOk;
  int64_t pos = 0;
  ExtentMeta *meta;
  if (nullptr == (meta = reinterpret_cast<ExtentMeta *>(
                      const_cast<char *>(iterator->key().data())))) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "unexpected error, extent meta must not nullptr", K(ret));
  }
  extent_meta = *meta;
  return ret;
}

class ParallelReadTest :  public testing::Test
{
public:
  ParallelReadTest()
      :internal_comparator_(BytewiseComparator()), next_file_number_(2),
       shutting_down_(false), bg_stopped_(false)
  {
  }
  virtual ~ParallelReadTest(){}

  virtual void SetUp();
  virtual void TearDown();

  //init and reset
  void init(const TestArgs args);
  void reset();

  //prepare data
  void open_for_write(const int64_t level, bool begin_trx = true);

  int meta_write(const int64_t level, const MiniTables &mini_tables) {
    return storage_manager_->apply(*(mini_tables.change_info_), false);
  }

  void close(const int64_t  level, bool finish = true);

  void write_data(const int64_t key_start, const int64_t key_end,
                  const int64_t sequence, const int64_t level,
                  const ValueType value_type = kTypeValue);

  void append(const int64_t key_start, const int64_t key_end,
              const int64_t sequence, const ValueType value_type = kTypeValue,
              const int64_t row_size = 128, bool finish = false);

  void append_memtable(const int64_t key_start, const int64_t key_end,
                       const int64_t sequence, const int64_t step = 1,
                       const ValueType value_type = kTypeValue);

  void build_memtable(MemTable*& mem);

  void write_batch_append(util::TransactionImpl *trx,
                          db::ColumnFamilyHandleImpl *column_family_impl,
                          const int64_t key_start, const int64_t key_end);

  //print and check
  void print_raw_meta() {
    storage_manager_->print_raw_meta();
  }
  void scan_all_data(
      std::function<bool(int64_t, const Slice &, const Slice &)> func);

  struct IntRange {
    int64_t start;
    int64_t end;
    int64_t step;
  };

  // check data
  void do_check(table::InternalIterator *iterator, const int64_t level,
                const IntRange *range, const int64_t size, int64_t &index);

  void check_result(const int64_t level, const IntRange *range,
                    const int64_t size);

  int create_extent_layer_iterator(util::Arena *arena,
                                   const Snapshot *extent_layer_version,
                                   const LayerPosition &layer_position,
                                   InternalIterator *&iterator);

  void build_range(int64_t start_key, int64_t end_key, storage::Range &range,
                  memory::ArenaAllocator &arena);
  void range_to_int(storage::Range &range, int &start_int, int &end_int);

  void parallel_run(storage::Range &range, int64_t max_threads, ReadOptions read_options, std::atomic<int64_t> &n_recs, util::TransactionImpl *trx = nullptr);

  void run_intra_l0_compact();
  void build_compact_context(CompactionContext *comp);

public:
  static const size_t DEFAULT_TABLE_CACHE_SIZE = 1 * 1024 * 1024; //1MB
  static const int DEFAULT_TABLE_CACHE_NUMSHARDBITS = 6;
public:

  //Env
  Env *env_;
  std::vector<std::string> names_;
  std::string dbname_;
  
  Context *context_ = nullptr;
  ColumnFamilyDesc cf_desc_;
  InternalKeyComparator internal_comparator_;
  std::string compression_dict_;
  std::vector<std::unique_ptr<db::IntTblPropCollectorFactory>> props_;

  std::shared_ptr<Cache> cache_;
  TableCache *table_cache_;

  SubTable *subtable_;
  VersionSet *version_set_;
  storage::StorageLogger *storage_logger_; 
  StorageManager *storage_manager_;

  ChangeInfo change_info_;
  ExtentSpaceManager *space_manager_;
  std::unique_ptr<db::log::Writer> descriptor_log_;
  Directory *db_dir_;

  MiniTables mini_tables_;
  std::unique_ptr<table::TableBuilder> extent_builder_;

  db::FileNumber next_file_number_;

  util::autovector<MemTable*> mems_;
  WriteBufferManager *wb_;
  memory::ArenaAllocator alloc_;

  std::atomic<bool> shutting_down_;
  std::atomic<bool> bg_stopped_;

  util::TransactionDBImpl *trx_db_impl_;
  DBImpl *db_impl_;
  monitor::InstrumentedMutex db_impl_mutex_;
  db::ColumnFamilyHandleImpl *column_family_impl_;
};

void ParallelReadTest::SetUp()
{
}

void ParallelReadTest::TearDown()
{ 
}

void ParallelReadTest::init(const TestArgs args) {
  reset();

  context_ = get_default_context(args);
  cache_ = NewLRUCache(50000, 16);
  env_ = context_->options_->env;
  dbname_ = context_->options_->db_paths[0].path;
  next_file_number_.store(2);

  xengine::util::test::remove_dir(dbname_.c_str());
  env_->CreateDir(dbname_);
  env_->NewDirectory(dbname_, db_dir_);     

  Status s;
  GlobalContext *global_ctx = ALLOC_OBJECT(GlobalContext, alloc_, dbname_, *(const_cast<Options*>(context_->options_)));

  WriteBufferManager *write_buffer_manager = ALLOC_OBJECT(WriteBufferManager, alloc_, 0);
  WriteController *write_controller = ALLOC_OBJECT(WriteController, alloc_);

  storage_logger_ = ALLOC_OBJECT(StorageLogger, alloc_);

  space_manager_ = ALLOC_OBJECT(ExtentSpaceManager, alloc_, env_, context_->env_options_, context_->db_options_);
 
  table_cache_ = ALLOC_OBJECT(TableCache, alloc_, context_->icf_options_, context_->env_options_, cache_.get(), space_manager_);

  version_set_ = ALLOC_OBJECT(VersionSet, alloc_, dbname_, &context_->idb_options_, context_->env_options_, reinterpret_cast<cache::Cache*>(table_cache_), write_buffer_manager, write_controller);

  global_ctx->env_ = env_;
  global_ctx->cache_ = cache_.get();
  global_ctx->storage_logger_ = storage_logger_;
  global_ctx->write_buf_mgr_ = write_buffer_manager;
  global_ctx->extent_space_mgr_ = space_manager_;
  global_ctx->env_options_ = context_->env_options_;
  global_ctx->options_ =  *(context_->options_);

  storage_logger_->TEST_reset();  
  storage_logger_->init(env_, dbname_, context_->env_options_, context_->idb_options_, version_set_, space_manager_, 1 * 1024 * 1024 * 1024);
  Options opt;
  version_set_->init(global_ctx);
  space_manager_->init(storage_logger_);
  space_manager_->create_table_space(0);

  uint64_t file_number = 1;
  std::string manifest_filename =
      util::DescriptorFileName(dbname_, file_number);
  WritableFile *descriptor_file = nullptr;
  EnvOptions opt_env_opts =
      env_->OptimizeForManifestWrite(context_->env_options_);
  s = NewWritableFile(env_, manifest_filename, descriptor_file, opt_env_opts);
  if (s.ok()) {
    descriptor_file->SetPreallocationBlockSize(
        context_->db_options_.manifest_preallocation_size);

    util::ConcurrentDirectFileWriter *file_writer =
        MOD_NEW_OBJECT(memory::ModId::kTestMod, util::ConcurrentDirectFileWriter,
                       descriptor_file, opt_env_opts);
    s = file_writer->init_multi_buffer();
    if (s.ok()) {
      db::log::Writer *log_writer = MOD_NEW_OBJECT(
          memory::ModId::kTestMod, db::log::Writer, file_writer, 0, false);
      storage_logger_->set_log_writer(log_writer);
    }
  }

  //create subtable
  CreateSubTableArgs subtable_args;
  subtable_args.index_id_ = 1;

  subtable_ = MOD_NEW_OBJECT(memory::ModId::kColumnFamilySet, ColumnFamilyData, global_ctx->options_);

  ColumnFamilySet *column_family_set = MOD_NEW_OBJECT(memory::ModId::kColumnFamilySet, ColumnFamilySet, global_ctx);

  //storage_manager_ = ALLOC_OBJECT(StorageManager, alloc_, context_->env_options_, context_->icf_options_, context_->mutable_cf_options_);

  subtable_->init(subtable_args, global_ctx, column_family_set);

  storage_manager_ = subtable_->get_storage_manager();

  wb_ = ALLOC_OBJECT(WriteBufferManager, alloc_,
                     context_->db_options_.db_write_buffer_size);

  // create column family
  db_impl_ = MOD_NEW_OBJECT(memory::ModId::kDBImpl, DBImpl, context_->db_options_, dbname_);

  util::TransactionDBOptions txn_db_options;
  trx_db_impl_ = MOD_NEW_OBJECT(memory::ModId::kDBImpl, util::TransactionDBImpl, dynamic_cast<DB*>(db_impl_), txn_db_options);

  column_family_impl_ = MOD_NEW_OBJECT(memory::ModId::kParallelRead, ColumnFamilyHandleImpl, subtable_, db_impl_, &db_impl_mutex_);

}

void ParallelReadTest::reset() {
  storage_manager_ = nullptr;
  space_manager_ = nullptr;

  db_dir_ = nullptr;
  props_.clear();

  mini_tables_.metas.clear();
  mini_tables_.props.clear();
  extent_builder_.reset();

  table_cache_ = nullptr;
  cache_.reset();

  if (context_ != nullptr) { 
    delete context_;
    context_ = nullptr;
  }

  mems_.clear();

 if (db_impl_ != nullptr) {
   //MOD_DELETE_OBJECT(DBImpl, db_impl_);
   db_impl_ = nullptr;
 }
}

void ParallelReadTest::open_for_write(const int64_t level, bool begin_trx)
{
  mini_tables_.change_info_ = &change_info_;
  int ret = Status::kOk;
  storage::LayerPosition output_layer_position =
      (0 == level)
          ? LayerPosition(level,
                          storage::LayerPosition::NEW_GENERATE_LAYER_INDEX)
          : LayerPosition(level, 0);
  if (begin_trx) {
    if (0 == level) {
      ret = storage_logger_->begin(FLUSH);
      //        mini_tables_.change_info_->task_type_ = TaskType::FLUSH_TASK;
      ASSERT_EQ(Status::kOk, ret);
    } else if (1 == level) {
      ret = storage_logger_->begin(MINOR_COMPACTION);
      //        mini_tables_.change_info_->task_type_ = TaskType::SPLIT_TASK;
      ASSERT_EQ(Status::kOk, ret);
    } else {
      ret = storage_logger_->begin(MAJOR_COMPACTION);
      //        mini_tables_.change_info_->task_type_ =
      //        TaskType::MAJOR_SELF_COMPACTION_TASK;
      ASSERT_EQ(Status::kOk, ret);
    }
  }
  mini_tables_.space_manager = space_manager_;
  mini_tables_.table_space_id_ = 0;
  extent_builder_.reset(NewTableBuilder(
      context_->icf_options_, internal_comparator_, &props_,
      cf_desc_.column_family_id_, cf_desc_.column_family_name_, &mini_tables_,
      GetCompressionType(context_->icf_options_, context_->mutable_cf_options_,
                         level /*level*/) /* compression type */,
      context_->icf_options_.compression_opts, output_layer_position,
      &compression_dict_, true));
}

void ParallelReadTest::close(const int64_t level, bool finish)
{
  int ret = Status::kOk;
  int64_t commit_seq = 0;
  if (finish) {
    extent_builder_->Finish();
    ASSERT_TRUE(extent_builder_->status().ok());
  }

  ret = storage_logger_->commit(commit_seq);
  ASSERT_EQ(Status::kOk, ret);

  meta_write(level, mini_tables_);
  mini_tables_.metas.clear();
  mini_tables_.props.clear();
  mini_tables_.change_info_->clear();
}

void ParallelReadTest::write_data(const int64_t key_start,
                                    const int64_t key_end,
                                    const int64_t sequence, const int64_t level,
                                    const ValueType value_type)
{
  open_for_write(level);

  if (level < 2) {
    append(key_start, key_end, sequence, value_type);
  } else {
    append(key_start, key_end, 0, value_type);
  }

  close(level);
}

void ParallelReadTest::build_memtable(MemTable*& mem) {
  InternalKeyComparator cmp(BytewiseComparator());
  mem = new MemTable(cmp, context_->icf_options_,
        context_->mutable_cf_options_,
        wb_, kMaxSequenceNumber);
}

void ParallelReadTest::append_memtable(const int64_t key_start,
                                       const int64_t key_end,
                                       const int64_t sequence,
                                       const int64_t step,
                                       const ValueType value_type)
{
  MemTable *mem = nullptr;
  build_memtable(mem);
  assert(nullptr != mem);
  const int64_t key_size = 20;
  int64_t row_size = 128;
  memory::ArenaAllocator alloc;
  char *buf = (char *)alloc.alloc(128);
  memset(buf, 0, row_size);
  int64_t start_seq = sequence;
  for (int64_t key = key_start; key < key_end; key += step) {
    snprintf(buf, key_size, "%010ld", key);
    mem->Add(start_seq, value_type, Slice(buf, strlen(buf)),
             Slice(buf, strlen(buf)));
  }

  subtable_->SetMemtable(mem);
  mems_.push_back(mem);
}

void ParallelReadTest::write_batch_append(
    util::TransactionImpl *trx, db::ColumnFamilyHandleImpl *column_family_impl,
    const int64_t key_start, const int64_t key_end)
{
  const int64_t key_size = 20;
  char buf[128];
  memset(buf, 0, 128);

  for (int64_t key = key_start; key < key_end; key++) {
    snprintf(buf, key_size, "%010ld", key);
    //trx->GetBatchForWrite()->Put(
    trx->Put(
        dynamic_cast<db::ColumnFamilyHandle *>(column_family_impl),
        Slice(buf, strlen(buf)) /* key */, Slice(buf, 128) /* value */);
  }
}

void ParallelReadTest::append(const int64_t key_start, const int64_t key_end,
                              const int64_t sequence,
                              const ValueType value_type,
                              const int64_t row_size, bool finish)
{
  ASSERT_TRUE(nullptr != extent_builder_.get());
  const int64_t key_size = 20;
  char buf[row_size];

  memset(buf, 0, row_size);

  for (int64_t key = key_start; key < key_end; key++) {
    snprintf(buf, key_size, "%010ld", key);
    InternalKey ikey(Slice(buf, strlen(buf)), sequence, value_type);
    extent_builder_->Add(ikey.Encode() /* key */,
                         Slice(buf, row_size) /* value */);
    ASSERT_TRUE(extent_builder_->status().ok());
  }

  if (finish) {
    extent_builder_->Finish();
    ASSERT_TRUE(extent_builder_->status().ok());
  }
}

void ParallelReadTest::scan_all_data(
    std::function<bool(int64_t, const Slice &, const Slice &)> func)
{
  Arena arena;
  MergeIteratorBuilder iter_builder(&internal_comparator_, &arena, false);
  RangeDelAggregator range_del_agg(InternalKeyComparator(BytewiseComparator()),
                                   kMaxSequenceNumber, true);
  ReadOptions read_options;
  storage_manager_->add_iterators(table_cache_, nullptr, read_options,
                                  &iter_builder, &range_del_agg,
                                  storage_manager_->get_current_version());

  db::Iterator *iterator = NewDBIterator(
      context_->icf_options_.env, read_options, context_->icf_options_,
      BytewiseComparator(), iter_builder.Finish(), kMaxSequenceNumber,
      kMaxSequenceNumber, kMaxSequenceNumber);
  ASSERT_TRUE(nullptr != iterator);
  iterator->SeekToFirst();
  int64_t row = 0;
  while (iterator->Valid()) {
    const Slice key = iterator->key();
    const Slice value = iterator->value();
    bool ret = func(row, key, value);
    if (!ret) {
      XHANDLER_LOG(ERROR, "check error", K(row), K(key), K(value));
    }
    ASSERT_TRUE(ret);
    ++row;
    iterator->Next();
  }
}

int ParallelReadTest::create_extent_layer_iterator(
    util::Arena *arena, const Snapshot *extent_layer_version,
    const LayerPosition &layer_position, InternalIterator *&iterator)
{
  int ret = Status::kOk;
  ExtentLayer *extent_layer = nullptr;
  ExtentLayerIterator *layer_iterator = nullptr;

  if (nullptr ==
      (extent_layer = extent_layer_version->get_extent_layer(layer_position))) {
    ret = Status::kErrorUnexpected;
  } else if (nullptr ==
             (layer_iterator = PLACEMENT_NEW(ExtentLayerIterator, *arena))) {
    ret = Status::kMemoryLimit;
  } else if (Status::kOk !=
             (ret = layer_iterator->init(&internal_comparator_, layer_position,
                                         extent_layer))) {
  } else {
    iterator = layer_iterator;
  }
  return ret;
}

void ParallelReadTest::do_check(table::InternalIterator *iterator,
                                const int64_t level, const ParallelReadTest::IntRange *range,
                                const int64_t size, int64_t &index)
{
  iterator->SeekToFirst();
  const int64_t row_size = 100;
  char buf[row_size];
  ExtentMeta extent_meta;
  while (iterator->Valid() && index < size) {
    ASSERT_EQ(0, parse_meta(iterator, extent_meta));
    snprintf(buf, row_size, "%010ld", range[index].end);
    ASSERT_EQ(0, memcmp(extent_meta.largest_key_.Encode().data(), buf,
                        extent_meta.largest_key_.Encode().size() - 8));
    snprintf(buf, row_size, "%010ld", range[index].start);
    ASSERT_EQ(0, memcmp(extent_meta.smallest_key_.Encode().data(), buf,
                        extent_meta.smallest_key_.Encode().size() - 8));
    iterator->Next();
    ++index;
  }
}

void ParallelReadTest::check_result(const int64_t level, const ParallelReadTest::IntRange *range,
                                    const int64_t size)
{
  Arena arena_;
  ReadOptions read_options;
  table::InternalIterator *iterator = nullptr;
  const Snapshot *snapshot = nullptr;
  int64_t index = 0;

  
  snapshot = storage_manager_->get_current_version();
  if (0 == level) {
    ExtentLayerVersion *extent_layer_version =
        snapshot->get_extent_layer_version(0);
    for (int32_t i = extent_layer_version->get_extent_layer_size() - 1; i >= 0;
         i--) {
      LayerPosition layer_position(0, i);
      create_extent_layer_iterator(&arena_, snapshot, layer_position, iterator);
      do_check(iterator, level, range, size, index);
    }
  } else {
    if (1 == level) {
      LayerPosition layer_position(1, 0);
      create_extent_layer_iterator(&arena_, snapshot, layer_position, iterator);
    } else {
      LayerPosition layer_position(2, 0);
      create_extent_layer_iterator(&arena_, snapshot, layer_position, iterator);
    }

    do_check(iterator, level, range, size, index);
  }

  ASSERT_EQ(index, size);
}

bool check_key(int64_t row, const Slice &key, const Slice &value,
                      const ParallelReadTest::IntRange &range)
{
  UNUSED(value);
  const int64_t row_size = 100;
  char buf[row_size];
  snprintf(buf, row_size, "%010ld", range.start + row * range.step);
  if (memcmp(buf, key.data(), key.size()) != 0) {
    return false;
  } else {
    return true;
  }
}

//Range is internal-key
void ParallelReadTest::build_range(int64_t start_key_int, int64_t end_key_int, storage::Range &range, memory::ArenaAllocator& arena) {
  char user_key_buf[18];

  //build start_key
  memset(user_key_buf, 0, 18);
  sprintf(user_key_buf, "%010ld", start_key_int);
  Slice user_key_start(user_key_buf, 10);
  db::InternalKey internal_key_start(user_key_start, kMaxSequenceNumber,
                                           kValueTypeForSeek);
  Slice start_key = common::Slice(internal_key_start.Encode());
 
  //build end_key
  memset(user_key_buf, 0, 18);
  sprintf(user_key_buf, "%010ld", end_key_int);
  Slice user_key_end(user_key_buf, 10);
  db::InternalKey internal_key_end(user_key_end, kMaxSequenceNumber,
                                         kValueTypeForSeek);
  Slice end_key = common::Slice(internal_key_end.Encode());

  range.start_key_ = start_key.deep_copy(arena);
  range.end_key_ = end_key.deep_copy(arena);
}

void ParallelReadTest::range_to_int(storage::Range &range,
                                  int &start_int,
                                  int &end_int)
{
  const char *start_user_key = range.start_key_.data();
  start_int = atoi(start_user_key);

  const char *end_user_key = range.end_key_.data();
  end_int = atoi(end_user_key);
}

void ParallelReadTest::parallel_run(storage::Range &range, int64_t max_threads, ReadOptions read_options, std::atomic<int64_t> &n_recs, util::TransactionImpl *trx) {
  ParallelReader preader(max_threads);
  ParallelReader::Config config(column_family_impl_, range, read_options);

  WriteOptions write_options;
  TransactionOptions txn_options;

  if (trx == nullptr) {
    trx = MOD_NEW_OBJECT(memory::ModId::kTestMod, util::TransactionImpl,
                         trx_db_impl_, write_options, txn_options);
  }

  preader.add_scan(
      dynamic_cast<util::Transaction *>(trx), config,
      [&](const ParallelReader::ExecuteCtx *ctx, db::Iterator* it) {
        int ret_inner = 0;
        sleep(0.0001);
        // fprintf(stderr, "key: %s\n", key.ToString().c_str());
        n_recs++;

        return ret_inner;
      });

  preader.run();  
}

void ParallelReadTest::build_compact_context(CompactionContext *comp) {
  shutting_down_.store(false);
  comp->shutting_down_ = &shutting_down_;
  comp->bg_stopped_ = &bg_stopped_;
  comp->cf_options_ = &context_->icf_options_;
  comp->mutable_cf_options_ = &context_->mutable_cf_options_;
  comp->env_options_ = &context_->env_options_;
  comp->data_comparator_ = BytewiseComparator();
  comp->internal_comparator_ = &internal_comparator_;
  comp->earliest_write_conflict_snapshot_ = 0;
  comp->storage_logger_ = storage_logger_;
  comp->table_space_id_ = 0;
  // Default is minor task
  comp->task_type_ = db::TaskType::MINOR_COMPACTION_TASK;
  comp->minor_compaction_type_ = 2; // new stream compaction
}

void ParallelReadTest::run_intra_l0_compact() {
  // util::Arena arena;
  memory::ArenaAllocator arena;
  storage::CompactionJob job(arena);
  CompactionContext ct;
  build_compact_context(&ct);
  ct.task_type_ = db::TaskType::INTRA_COMPACTION_TASK;
  ct.space_manager_ = space_manager_;
  int ret =
      job.init(ct, cf_desc_, storage_manager_,
               storage_manager_->get_current_version());
  storage_manager_->get_current_version()->get_extent_layer_version(0)->ref();
  ASSERT_EQ(ret, 0);
  ret = job.prepare();
  ASSERT_EQ(ret, 0);
  ret = storage_logger_->begin(MINOR_COMPACTION);
  ASSERT_EQ(ret, 0);
  ret = job.run();
  ASSERT_EQ(ret, 0);
  storage::Compaction *compaction = nullptr;
  while (nullptr != (compaction = job.get_next_task())) {
    job.append_change_info(compaction->get_change_info());
  }
  int64_t commit_seq;
  ret = storage_logger_->commit(commit_seq);
  ASSERT_EQ(ret, 0);
  ret = storage_manager_->apply(job.get_change_info(), false);
  ASSERT_EQ(ret, 0);
  //ret = job.install();
  //ASSERT_EQ(ret, 0);
}

TEST_F(ParallelReadTest, parallel_run_basic)
{
  TestArgs arg;
  init(arg);
  write_data(60, 2500, 0, 0);
  write_data(50, 2300, 10, 1);
  write_data(2400, 3000, 200, 1);
  write_data(50, 2300, 0, 2); 
 
  ParallelReadTest::IntRange r_level_0[1] = {{60, 2500-1, 1}};
  ParallelReadTest::IntRange r_level_1[2] = {{50, 2300-1, 1}, {2400, 3000-1,1}};
  ParallelReadTest::IntRange r_level_2[1] = {{50, 2300-1, 1}};

  //check meta-data
  check_result(0, r_level_0, 1);
  check_result(1, r_level_1, 2);
  check_result(2, r_level_2, 1);

  //check data
  ParallelReadTest::IntRange r_all[1] = {{50, 3000-1, 1}};
  auto check_func = [&r_all](int64_t row, const Slice &key, const Slice &value) -> bool {
    return check_key(row, key, value, r_all[0]);
  };

  scan_all_data(check_func);

  print_raw_meta();
}

/*there is no override range between memtable and level extents*/
TEST_F(ParallelReadTest, parallel_run_memtable_l0)
{
  TestArgs arg;
  init(arg);
  write_data(100, 1000, 0, 0);

  append_memtable(1100,2000,0,1);
  //update superversion
  {
    InstrumentedMutexLock l(&db_impl_mutex_);
    db::SuperVersion *new_sv =
        MOD_NEW_OBJECT(memory::ModId::kSuperVersion, SuperVersion);
    db::SuperVersion *old_sv = subtable_->InstallSuperVersion(
        new_sv, &db_impl_mutex_, *subtable_->GetLatestMutableCFOptions());
    MOD_DELETE_OBJECT(SuperVersion, old_sv);
  }

  //create range
  storage::Range range;
  ReadOptions read_options;
  memory::ArenaAllocator arena;
  build_range(50, 3000, range, arena);

  atomic<int64_t> n_recs(0); //out
  parallel_run(range, 10, read_options, n_recs);

  ASSERT_EQ(n_recs, 1800);

  subtable_->ResetThreadLocalSuperVersions();
}

TEST_F(ParallelReadTest, parallel_multi_layer_l0)
{
  TestArgs arg;
  init(arg);
  write_data(100, 1000, 0, 0);
  write_data(50, 200, 0, 0);
  write_data(200, 2000, 0, 0);
  write_data(800, 1000, 0, 0);
  write_data(200, 800, 0, 0);
  write_data(1000, 2000, 0, 0);

  //update superversion
  {
    InstrumentedMutexLock l(&db_impl_mutex_);
    db::SuperVersion *new_sv =
        MOD_NEW_OBJECT(memory::ModId::kSuperVersion, SuperVersion);
    db::SuperVersion *old_sv = subtable_->InstallSuperVersion(
        new_sv, &db_impl_mutex_, *subtable_->GetLatestMutableCFOptions());
    MOD_DELETE_OBJECT(SuperVersion, old_sv);
  }

  //create range
  storage::Range range;
  ReadOptions read_options;
  memory::ArenaAllocator arena;
  build_range(50, 2000, range, arena);

  atomic<int64_t> n_recs(0); //out
  parallel_run(range, 10, read_options, n_recs);

  ASSERT_EQ(n_recs, 1950);

  build_range(1000, 2000, range, arena);
  n_recs = 0; //out
  parallel_run(range, 10, read_options, n_recs);
  ASSERT_EQ(n_recs, 1000);

  subtable_->ResetThreadLocalSuperVersions();
}

TEST_F(ParallelReadTest, parallel_multi_layer_l0_l2)
{
  TestArgs arg;
  init(arg);
  write_data(50, 200, 0, 0);
  write_data(1000, 2000, 0, 0);
  write_data(1300, 1500, 0, 0);
  write_data(1500, 1800, 0, 0);
  write_data(1800, 2000, 0, 0);
  write_data(400, 600, 0, 0);
  write_data(600, 800, 0, 0);
  write_data(0, 1000, 0, 2);

  //update superversion
  {
    InstrumentedMutexLock l(&db_impl_mutex_);
    db::SuperVersion *new_sv =
        MOD_NEW_OBJECT(memory::ModId::kSuperVersion, SuperVersion);
    db::SuperVersion *old_sv = subtable_->InstallSuperVersion(
        new_sv, &db_impl_mutex_, *subtable_->GetLatestMutableCFOptions());
    MOD_DELETE_OBJECT(SuperVersion, old_sv);
  }

  //create range
  storage::Range range;
  ReadOptions read_options;
  memory::ArenaAllocator arena;
  build_range(1200, 2000, range, arena);

  atomic<int64_t> n_recs(0); //out
  parallel_run(range, 10, read_options, n_recs);

  ASSERT_EQ(n_recs, 800);

  subtable_->ResetThreadLocalSuperVersions();
}

TEST_F(ParallelReadTest, parallel_run_after_compaction)
{
  TestArgs arg;
  init(arg);
  write_data(50, 300, 0, 0);
  write_data(50, 300, 0, 0, kTypeSingleDeletion);
  //write_data(50, 300, 100, 0);

  print_raw_meta();

  //check meta
  //ParallelReadTest::IntRange r_level_0[2] = {{50, 299, 1}, {50, 99, 1}};
  //check_result(0, r_level_0, 2);

  //check data
  ParallelReadTest::IntRange r_all[1] = {{50, 399, 1}};
  auto check_func = [&r_all](int64_t row, const Slice &key, const Slice &value) -> bool {   return check_key(row, key, value, r_all[0]);
  };

  scan_all_data(check_func);

  //update superversion
  {
    InstrumentedMutexLock l(&db_impl_mutex_);
    db::SuperVersion *new_sv =
        MOD_NEW_OBJECT(memory::ModId::kSuperVersion, SuperVersion);
    db::SuperVersion *old_sv = subtable_->InstallSuperVersion(
        new_sv, &db_impl_mutex_, *subtable_->GetLatestMutableCFOptions());
    MOD_DELETE_OBJECT(SuperVersion, old_sv);
  }

  //run compaction
  run_intra_l0_compact();
  print_raw_meta();


  //update superversion
  {
    InstrumentedMutexLock l(&db_impl_mutex_);
    db::SuperVersion *new_sv =
        MOD_NEW_OBJECT(memory::ModId::kSuperVersion, SuperVersion);
    db::SuperVersion *old_sv = subtable_->InstallSuperVersion(
        new_sv, &db_impl_mutex_, *subtable_->GetLatestMutableCFOptions());
    MOD_DELETE_OBJECT(SuperVersion, old_sv);
  }


  //create range
  storage::Range range;
  ReadOptions read_options;
  memory::ArenaAllocator arena;
  build_range(50, 200, range, arena);

  atomic<int64_t> n_recs(0); //out
  parallel_run(range, 4, read_options, n_recs);

  ASSERT_EQ(n_recs, 0);

  subtable_->ResetThreadLocalSuperVersions();
}

/**
  suppose:
  level-2 is [10,20], [30,40]
  level-0 is [25, 28]
  we use level-2 to split, so range is (start_key, 21), [30, end_key).
  we need to add [21, 30) range, for data in the range maybe exists in other levels.
*/
TEST_F(ParallelReadTest, parallel_multi_layer_l0_l2_overlap)
{
  TestArgs arg;
  init(arg);
  write_data(10, 20, 0, 2);
  write_data(30, 40, 0, 2);
  write_data(25, 28, 0, 0);

  //update superversion
  {
    InstrumentedMutexLock l(&db_impl_mutex_);
    db::SuperVersion *new_sv =
        MOD_NEW_OBJECT(memory::ModId::kSuperVersion, SuperVersion);
    db::SuperVersion *old_sv = subtable_->InstallSuperVersion(
        new_sv, &db_impl_mutex_, *subtable_->GetLatestMutableCFOptions());
    MOD_DELETE_OBJECT(SuperVersion, old_sv);
  }

  //create range
  storage::Range range;
  ReadOptions read_options;
  memory::ArenaAllocator arena;
  build_range(1, 50, range, arena);

  atomic<int64_t> n_recs(0); //out
  parallel_run(range, 2 , read_options, n_recs);

  ASSERT_EQ(n_recs, 23);

  subtable_->ResetThreadLocalSuperVersions();
}

TEST_F(ParallelReadTest, parallel_run_only_memtable)
{
  TestArgs arg;
  init(arg);

  append_memtable(1,1000,0,1);
  //update superversion
  {
    InstrumentedMutexLock l(&db_impl_mutex_);
    db::SuperVersion *new_sv =
        MOD_NEW_OBJECT(memory::ModId::kSuperVersion, SuperVersion);
    db::SuperVersion *old_sv = subtable_->InstallSuperVersion(
        new_sv, &db_impl_mutex_, *subtable_->GetLatestMutableCFOptions());
    MOD_DELETE_OBJECT(SuperVersion, old_sv);
  }

  //create range
  storage::Range range;
  ReadOptions read_options;
  memory::ArenaAllocator arena;
  build_range(50, 70, range, arena);

  atomic<int64_t> n_recs(0);
  parallel_run(range, 10, read_options, n_recs);

  ASSERT_EQ(n_recs, 20);

  subtable_->ResetThreadLocalSuperVersions();
}


TEST_F(ParallelReadTest, parallel_run_level2)
{
  TestArgs arg;
  init(arg);
  write_data(60, 2500, 0, 0);
  write_data(50, 2300, 10, 1);
  write_data(2400, 3000, 200, 1);

  int64_t start = 50;
  for (int i = 0; i < 10; i++) {
    write_data(start, start+100, 0, 2);
    start += 100; 
  }

  //check meta-data
  ParallelReadTest::IntRange r_level_0[1] = {{60, 2500-1, 1}};
  ParallelReadTest::IntRange r_level_1[2] = {{50, 2300-1, 1}, {2400, 3000-1,1}};
  ParallelReadTest::IntRange r_level_2[1] = {{50, 2300-1, 1}};
  check_result(0, r_level_0, 1);
  check_result(1, r_level_1, 2);
  //check_result(2, r_level_2, 1);

  //check data
  ParallelReadTest::IntRange r_all[1] = {{50, 3000-1, 1}};
  auto check_func = [&r_all](int64_t row, const Slice &key, const Slice &value) -> bool {
    return check_key(row, key, value, r_all[0]);
  };

  scan_all_data(check_func);

  print_raw_meta();

  //update superversion
  {
    InstrumentedMutexLock l(&db_impl_mutex_);
    db::SuperVersion *new_sv =
        MOD_NEW_OBJECT(memory::ModId::kSuperVersion, SuperVersion);
    db::SuperVersion *old_sv = subtable_->InstallSuperVersion(
        new_sv, &db_impl_mutex_, *subtable_->GetLatestMutableCFOptions());
    MOD_DELETE_OBJECT(SuperVersion, old_sv);
  }

  //parallel_scan
  storage::Range range;
  ReadOptions read_options;
  memory::ArenaAllocator arena;
  build_range(50, 70, range, arena);

  atomic<int64_t> n_recs(0);
  parallel_run(range, 1, read_options, n_recs);
  
  ASSERT_EQ(n_recs, 20);

  subtable_->ResetThreadLocalSuperVersions();
}

TEST_F(ParallelReadTest, parallel_run_parallel)
{
  TestArgs arg;
  init(arg);
  //write_data(60, 2500, 0, 0);
  //write_data(50, 2300, 10, 1);
  //write_data(2400, 3000, 200, 1);

  int64_t start = 50;
  for (int i = 0; i < 10000; i++) {
    write_data(start, start+10, 0, 2);
    start += 10; 
  }

  //update superversion
  {
    InstrumentedMutexLock l(&db_impl_mutex_);
    db::SuperVersion *new_sv =
        MOD_NEW_OBJECT(memory::ModId::kSuperVersion, SuperVersion);
    db::SuperVersion *old_sv = subtable_->InstallSuperVersion(
        new_sv, &db_impl_mutex_, *subtable_->GetLatestMutableCFOptions());
    MOD_DELETE_OBJECT(SuperVersion, old_sv);
  }

  //parallel_scan
  storage::Range range;
  ReadOptions read_options;
  memory::ArenaAllocator arena;
  build_range(50, 100000, range, arena);

  auto begin = system_clock::now();
  atomic<int64_t> n_recs(0);
  parallel_run(range, 1, read_options, n_recs); 
  ASSERT_EQ(n_recs, 99950);

  auto end = system_clock::now();
  auto duration = duration_cast<microseconds>(end - begin);

  cout << "thread_num:1-->duration" << double(duration.count()) * microseconds::period::num / microseconds::period::den << endl;
  
  begin = system_clock::now();
  n_recs = 0;
  parallel_run(range, 2, read_options, n_recs); 
  ASSERT_EQ(n_recs, 99950);
  end = system_clock::now();
  duration = duration_cast<microseconds>(end - begin);

  cout << "thread_num:2-->duration:" << double(duration.count()) * microseconds::period::num / microseconds::period::den << endl;

  begin = system_clock::now();
  n_recs = 0;
  parallel_run(range, 4, read_options, n_recs); 
  ASSERT_EQ(n_recs, 99950);
  end = system_clock::now();
  duration = duration_cast<microseconds>(end - begin);

  cout << "thread_num:4-->duration:" << double(duration.count()) * microseconds::period::num / microseconds::period::den << endl;

  begin = system_clock::now();
  n_recs = 0;
  parallel_run(range, 8, read_options, n_recs); 
  ASSERT_EQ(n_recs, 99950);
  end = system_clock::now();
  duration = duration_cast<microseconds>(end - begin);

  cout << "thread_num:8-->duration:" << double(duration.count()) * microseconds::period::num / microseconds::period::den << endl;

  begin = system_clock::now();
  n_recs = 0;
  parallel_run(range, 16, read_options, n_recs); 
  ASSERT_EQ(n_recs, 99950);
  end = system_clock::now();
  duration = duration_cast<microseconds>(end - begin);

  cout << "thread_num:16-->duration:" << double(duration.count()) * microseconds::period::num / microseconds::period::den << endl;

  subtable_->ResetThreadLocalSuperVersions();
}

TEST_F(ParallelReadTest, parallel_run_balance)
{
  TestArgs arg;
  init(arg);

  //level 1, old data from [50,1050)
  int64_t start = 50;
  for (int i = 0; i < 1000; i++) {
    write_data(start, start+10, 0, 2);
    start += 10; 
  }

  //level 2, new data from [1000, 2000)
  write_data(0, 20000, 0, 1);

  //update superversion
  {
    InstrumentedMutexLock l(&db_impl_mutex_);
    db::SuperVersion *new_sv =
        MOD_NEW_OBJECT(memory::ModId::kSuperVersion, SuperVersion);
    db::SuperVersion *old_sv = subtable_->InstallSuperVersion(
        new_sv, &db_impl_mutex_, *subtable_->GetLatestMutableCFOptions());
    MOD_DELETE_OBJECT(SuperVersion, old_sv);
  }

  print_raw_meta();

  //parallel_scan
  storage::Range range;
  ReadOptions read_options;
  memory::ArenaAllocator arena;
  build_range(50, 20000, range, arena);

  auto begin = system_clock::now();
  atomic<int64_t> n_recs(0);
  parallel_run(range, 1, read_options, n_recs); 
  ASSERT_EQ(n_recs, 19950);

  auto end = system_clock::now();
  auto duration = duration_cast<microseconds>(end - begin);

  cout << "thread_num:1-->duration" << double(duration.count()) * microseconds::period::num / microseconds::period::den << endl;
  
  begin = system_clock::now();
  n_recs = 0;
  parallel_run(range, 2, read_options, n_recs); 
  ASSERT_EQ(n_recs, 19950);
  end = system_clock::now();
  duration = duration_cast<microseconds>(end - begin);

  cout << "thread_num:2-->duration:" << double(duration.count()) * microseconds::period::num / microseconds::period::den << endl;

  begin = system_clock::now();
  n_recs = 0;
  parallel_run(range, 16, read_options, n_recs); 
  ASSERT_EQ(n_recs, 19950);
  end = system_clock::now();
  duration = duration_cast<microseconds>(end - begin);

  cout << "thread_num:16-->duration:" << double(duration.count()) * microseconds::period::num / microseconds::period::den << endl;

  subtable_->ResetThreadLocalSuperVersions();
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  std::string log_path = test::TmpDir() + "/parallel_scan_test.log";
  xengine::util::test::init_logger(log_path.c_str(), logger::INFO_LEVEL);
  return RUN_ALL_TESTS();
}
