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
#include "compaction_job.h"
#include "task_type.h"
#include "mt_ext_compaction.h"
#include "logger/logger.h"
#include "db/builder.h"
#include "db/column_family.h"
#include "db/db_impl.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "memory/page_arena.h"
#include "options/options_helper.h"
#include "storage/extent_space_manager.h"
#include "storage/io_extent.h"
#include "storage/multi_version_extent_meta_layer.h"
#include "storage/storage_logger.h"
#include "storage/storage_manager.h"
#include "storage/storage_manager_mock.h"
#include "table/block.h"
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
using namespace memory;

DEFINE_int32(compaction_thread_num, 1, "Number of compaction threads");
DEFINE_int32(key_size, 20, "Inserted key size, sequence and type size excluded");
DEFINE_int32(value_size, 128, "Inserted value size, generated randomly");
DEFINE_int32(kv_num, 10000, "Number of KV to insert");
DEFINE_bool(compression, false, "Whether to compress datablock");
DEFINE_bool(use_fpga, false, "Whether to offload compaction to FPGA");
DEFINE_bool(snapshot_task, false, "Whether to snapshot each task");
DEFINE_int32(stats_interval, 5, "Interval between two reports");
DEFINE_int32(device_id, 0, "FPGA device ID");
DEFINE_int32(fpga_driver_threads_num, 12, "FPGA driver thread num");
DEFINE_int32(compaction_mode, 0, "0 cpu, 1 fpga, 2 check");

//size_t g_thread_num = 1;
//size_t g_key_size = 20;
//size_t g_value_size = 128;
//size_t g_kv_num = 10000;
//size_t g_compression = 0;
Random rnd(2017);


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

static std::vector<TestArgs> GenerateArgList() {
  std::vector<TestArgs> test_args;

  // Only add compression if it is supported
  std::vector<std::pair<CompressionType, bool>> compression_types;

  if (FLAGS_compression) {
    compression_types.emplace_back(kNoCompression, false);
  } else {
    compression_types.emplace_back(kNoCompression, false);
    if (Snappy_Supported()) {
      compression_types.emplace_back(kSnappyCompression, false);
    }
    if (Zlib_Supported()) {
      compression_types.emplace_back(kZlibCompression, false);
    }
    if (BZip2_Supported()) {
      compression_types.emplace_back(kBZip2Compression, false);
    }
    if (LZ4_Supported()) {
      compression_types.emplace_back(kLZ4Compression, false);
      compression_types.emplace_back(kLZ4HCCompression, false);
    }
    if (XPRESS_Supported()) {
      compression_types.emplace_back(kXpressCompression, false);
    }
    if (ZSTD_Supported()) {
      compression_types.emplace_back(kZSTD, false);
    }
  }

  for (auto compression_type : compression_types) {
    TestArgs one_arg;
    one_arg.compression = compression_type.first;
    one_arg.format_version = 3;
    test_args.push_back(one_arg);
  }

  return test_args;
}

void build_default_options(const TestArgs &args, common::Options &opt) {
  std::map<std::string, std::string>::const_iterator itr;

  BlockBasedTableOptions table_options;
  // table_options.filter_policy.reset(NewBloomFilterPolicy(10));
  int block_size = 16 * 1024;
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
  // Arena will assert kBlockSize in 4096 to (2u << 30)
  opt.arena_block_size = 4096 * 2;
  opt.memtable_huge_page_size = 4096 * 2;

  opt.compaction_type = 0; // should be 0 here

  int file_size = db_write_buffer_size * 1024;
  opt.target_file_size_base = file_size;

  std::string db_path_ = test::TmpDir() + "/compaction_test";
  if (opt.db_paths.size() == 0) {
    opt.db_paths.emplace_back(db_path_, std::numeric_limits<uint64_t>::max());
  }

  auto factory = std::make_shared<SkipListFactory>();
  opt.memtable_factory = factory;
  WriteBufferManager *wb = new WriteBufferManager(0);  // no limit space
  assert(wb != nullptr);
  // no free here ...
  opt.write_buffer_manager.reset(wb);

  //DBOptions db_options(opt);
  //CreateLoggerFromOptions(db_path_, db_options, &opt.info_log);
}

Context *get_default_context(const TestArgs &args) {
  common::Options *opt = new common::Options();
//  common::Options *opt = ALLOC_OBJECT(Options, alloc_);
  build_default_options(args, *opt);
  Context *context = new Context(*opt);
//  Context *context = ALLOC_OBJECT(Context, alloc_, *opt);
  return context;
}

class CompactionTest : public testing::Test {
 public:
  CompactionTest()
      : context_(nullptr),
        // cache_(NewLRUCache(50000, 16)),
        // write_buffer_manager_(context_->db_options_.db_write_buffer_size),
        // env_(context_->options_->env),
        // dbname_(context_->options_->db_paths[0].path),
        space_manager_(nullptr),
        internal_comparator_(BytewiseComparator()),
        next_file_number_(2),
        shutting_down_(false),
        bg_stopped_(false) {}

  void reset() {
    // Env *env_;
    names_.clear();
    keys_.clear();
    //space_manager_.reset();
    space_manager_ = nullptr;
    descriptor_log_.reset();
    db_dir = nullptr;
    props_.clear();
    // std::string compression_dict_;
    mini_tables_.metas.clear();
    mini_tables_.props.clear();
//    if (mini_tables_.schema == nullptr) {
//      mini_tables_.schema = new common::XengineSchema;
//    }
    extent_builder_.reset();
    // internal_comparator_;
    shutting_down_.store(false);
    table_cache_ = nullptr;
    cache_.reset();
    delete context_;
    mems_.clear();
  }

  // We should call init() first
  void init(const TestArgs args) {
    reset();
    context_ = get_default_context(args);
    cache_ = NewLRUCache(50000, 16);
    env_ = context_->options_->env;
    dbname_ = context_->options_->db_paths[0].path;
    next_file_number_.store(2);
    env_->DeleteDir(dbname_);
    test::remove_dir(dbname_.c_str());

    env_->CreateDir(dbname_);
    env_->NewDirectory(dbname_, db_dir);
    Status s;

    // GlobalContext  is_valid will check or use these members
    //    util::Env *env_;
    //    cache::Cache *cache_;
    //    WriteBufferManager *write_buf_mgr_;
    //    storage::StorageLogger *storage_logger_;
    //    storage::ExtentSpaceManager *extent_space_mgr_;

//    GlobalContext *global_ctx = new GlobalContext(dbname_, *(const_cast<Options*>(context_->options_)));
    GlobalContext *global_ctx = ALLOC_OBJECT(GlobalContext, alloc_, dbname_, *(const_cast<Options*>(context_->options_)));
//    WriteBufferManager *write_buffer_manager = new WriteBufferManager(0);
    WriteBufferManager *write_buffer_manager = ALLOC_OBJECT(WriteBufferManager, alloc_, 0);
//    WriteController *write_controller = new WriteController();
    WriteController *write_controller = ALLOC_OBJECT(WriteController, alloc_);
//    storage_logger_ = new StorageLogger();
    storage_logger_ = ALLOC_OBJECT(StorageLogger, alloc_);
//    space_manager_ = new ExtentSpaceManager(context_->db_options_, next_file_number_);
    space_manager_ = ALLOC_OBJECT(ExtentSpaceManager, alloc_, env_, context_->env_options_, context_->db_options_);
//    table_cache_ = new TableCache(context_->icf_options_, context_->env_options_, cache_.get(), space_manager_);
    table_cache_ = ALLOC_OBJECT(TableCache, alloc_, context_->icf_options_, context_->env_options_, cache_.get(), space_manager_);
//    version_set_ = new VersionSet(dbname_, &context_->idb_options_, context_->env_options_, reinterpret_cast<cache::Cache*>(table_cache_), write_buffer_manager, write_controller);
    version_set_ = ALLOC_OBJECT(VersionSet, alloc_, dbname_, &context_->idb_options_, context_->env_options_, reinterpret_cast<cache::Cache*>(table_cache_), write_buffer_manager, write_controller);

    global_ctx->env_ = env_;
    global_ctx->cache_ = cache_.get();
    global_ctx->storage_logger_ = storage_logger_;
    global_ctx->write_buf_mgr_ = write_buffer_manager;
    global_ctx->extent_space_mgr_ = space_manager_;

    // __thread int64_t StorageLogger::local_trans_id_ = 0;
    storage_logger_->TEST_reset();
    storage_logger_->init(env_, dbname_, context_->env_options_, context_->idb_options_, version_set_, space_manager_, 1 * 1024 * 1024 * 1024);
    Options opt;
    version_set_->init(global_ctx);
    space_manager_->init(storage_logger_);
    space_manager_->create_table_space(0);

    uint64_t file_number = 1;
    std::string manifest_filename =
        util::DescriptorFileName(dbname_, file_number);
//    std::unique_ptr<WritableFile> descriptor_file;
    WritableFile *descriptor_file = nullptr;
    EnvOptions opt_env_opts =
        env_->OptimizeForManifestWrite(context_->env_options_);
    s = NewWritableFile(env_, manifest_filename, descriptor_file,
                        opt_env_opts);
    if (s.ok()) {
      descriptor_file->SetPreallocationBlockSize(
          context_->db_options_.manifest_preallocation_size);

//      unique_ptr<util::ConcurrentDirectFileWriter> file_writer(
//          new util::ConcurrentDirectFileWriter(std::move(descriptor_file),
//                                               opt_env_opts));
      util::ConcurrentDirectFileWriter *file_writer = MOD_NEW_OBJECT(ModId::kTestMod, util::ConcurrentDirectFileWriter,
          descriptor_file, opt_env_opts);
      s = file_writer->init_multi_buffer();
      if (s.ok()) {
//        descriptor_log_.reset(
        db::log::Writer *log_writer = MOD_NEW_OBJECT(ModId::kTestMod, db::log::Writer, file_writer, 0, false);
        storage_logger_->set_log_writer(log_writer);
      }
    }
    // get subtable
    //SubTableMap &sub_table_map = version_set_->get_global_ctx()->all_sub_table_.sub_table_map_;
    ColumnFamilyData *sub_table = nullptr;
    // Mock parameters here
//    CreateSubTableArgs subtable_args;
//    subtable_args.create_paxos_index_ = 1;
//    subtable_args.index_id_ = 1;
//    subtable_args.index_no_ = 1;
//    subtable_args.wal_log_file_id_ = 1;


//    storage_manager_ = new StorageManager(context_->env_options_, context_->icf_options_, context_->mutable_cf_options_);
    storage_manager_ = ALLOC_OBJECT(StorageManager, alloc_, context_->env_options_, context_->icf_options_, context_->mutable_cf_options_);
    storage_manager_->init(env_, space_manager_, cache_.get());

    //s = storage_manager_->init(space_manager_.get(), 0);
    assert(s.ok());
    wb_ = ALLOC_OBJECT(WriteBufferManager, alloc_, context_->db_options_.db_write_buffer_size);
//    wb_ = new WriteBufferManager(context_->db_options_.db_write_buffer_size);
  }

  void shutdown() { Close(); }
  ~CompactionTest() {
    Close();
    storage_manager_->~StorageManager();
    version_set_->~VersionSet();
    table_cache_->~TableCache();
    space_manager_->~ExtentSpaceManager();
    storage_logger_->~StorageLogger();
 //   Destroy();
  };

  void Close() {
    names_.clear();
  }

  void Destroy() {
    ASSERT_OK(DestroyDB(dbname_, *context_->options_));
  }

  void build_compact_context(CompactionContext *comp) {
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

  void print_raw_meta(const db::MemTable *memtable) {
    storage_manager_->print_raw_meta();
  }

  void print_raw_meta() {
    storage_manager_->print_raw_meta();
  }

  int parse_meta(const table::InternalIterator *iterator, ExtentMeta &extent_meta)
  {
    int ret = Status::kOk;
    int64_t pos = 0;
    ExtentMeta* meta;
    if (nullptr == (meta = reinterpret_cast<ExtentMeta *>(const_cast<char *>(iterator->key().data())))) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "unexpected error, extent meta must not nullptr", K(ret));
    }
    extent_meta = *meta;
    return ret;
  }

  struct IntRange {
    int64_t start;
    int64_t end;
    int64_t step;
  };

  int create_extent_layer_iterator(util::Arena *arena,
                                   const Snapshot *extent_layer_version,
                                   const LayerPosition &layer_position,
                                   InternalIterator *&iterator)
  {
    int ret = Status::kOk;
    ExtentLayer *extent_layer = nullptr;
    ExtentLayerIterator *layer_iterator = nullptr;

    if (nullptr == (extent_layer = extent_layer_version->get_extent_layer(layer_position))) {
      ret = Status::kErrorUnexpected;
    } else if (nullptr == (layer_iterator = PLACEMENT_NEW(ExtentLayerIterator, *arena))) {
      ret = Status::kMemoryLimit;
    } else if (Status::kOk != (ret = layer_iterator->init(&internal_comparator_, layer_position, extent_layer))) {
    } else {
      iterator = layer_iterator;
    }
    return ret;
  }

  void do_check(table::InternalIterator *iterator, const int64_t level, const IntRange *range,
                const int64_t size, int64_t &index) {
    iterator->SeekToFirst();
    const int64_t row_size = 100;
    char buf[row_size];
    ExtentMeta extent_meta;
    while (iterator->Valid() && index < size) {
      ASSERT_EQ(0, parse_meta(iterator, extent_meta));
      snprintf(buf, row_size, "%010ld", range[index].end);
      ASSERT_EQ(0, memcmp(extent_meta.largest_key_.Encode().data(), buf, extent_meta.largest_key_.Encode().size() - 8));
      snprintf(buf, row_size, "%010ld", range[index].start);
      ASSERT_EQ(0, memcmp(extent_meta.smallest_key_.Encode().data(), buf, extent_meta.smallest_key_.Encode().size() - 8));
      iterator->Next();
      ++index;
    }
  }
  void check_result(const int64_t level, const IntRange *range,
                    const int64_t size) {
    Arena arena_;
    ReadOptions read_options;
    table::InternalIterator *iterator = nullptr;
    const Snapshot *snapshot = nullptr;
    int64_t index = 0;

    snapshot = storage_manager_->get_current_version();
    if (0 == level) {
      ExtentLayerVersion *extent_layer_version = snapshot->get_extent_layer_version(0);
      for (int32_t i = extent_layer_version->get_extent_layer_size() - 1; i >= 0; i--) {
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

      CompactionContext ct;
      build_compact_context(&ct);
      do_check(iterator, level, range, size, index);
    }

    ASSERT_EQ(index, size);
  }

//  static bool check_key_fpga(int64_t row, const Slice &key, const Slice &value,
//                        const IntRange &range) {
//    UNUSED(value);
//    const int64_t row_size = key.size();
//    char buf[row_size+1];
//    std::string format;
//    format.append("%0");
//    format.append(std::to_string(row_size));
//    format.append("ld");
//    snprintf(buf, row_size+1, format.c_str(), range.start + row * range.step);
//    return memcmp(buf, key.data(), key.size()) == 0;
//  }

  static bool check_key(int64_t row, const Slice &key, const Slice &value,
                        const IntRange &range) {
    UNUSED(value);
    const int64_t row_size = 100;
    char buf[row_size];
    snprintf(buf, row_size, "%010ld", range.start + row * range.step);
    return memcmp(buf, key.data(), key.size()) == 0;
  }

  static int check_key(const Slice& key, const int64_t num) {
    assert(key.size() >= 8);
    const int64_t row_size = 100;
    char buf[row_size];
    snprintf(buf, row_size, "%010ld", num);
    return memcmp(key.data(), buf, key.size() - 8);
  }

  static int check_metakey(const Slice& meta_key, const int64_t num) {
    const int64_t row_size = 100;
    char buf[row_size];
    snprintf(buf, row_size, "%010ld", num);
    return memcmp(meta_key.data(), buf, meta_key.size() - 8);
  }

  static bool check_key_segment(int64_t row, const Slice &key,
                                const Slice &value, const IntRange *range,
                                const int64_t size) {
    UNUSED(value);
    // find in range segments;
    int64_t offset = row;
    int64_t i = 0;
    for (i = 0; i < size; ++i) {
      if (range[i].start + offset * range[i].step > range[i].end) {
        offset = range[i].start + offset * range[i].step - range[i].end -
                 range[i].step;
      } else {
        break;
      }
    }
    const int64_t row_size = 100;
    char buf[row_size];
    snprintf(buf, row_size, "%010ld", range[i].start + offset * range[i].step);
    int ret = memcmp(buf, key.data(), key.size());
    if (ret)
      fprintf(stderr, "check_key_segment failed expect %s but provide %s\n",
              buf, key.data());

    return ret == 0;
  }

  void scan_all_data(
      std::function<bool(int64_t, const Slice &, const Slice &)> func) {
    Arena arena;
    MergeIteratorBuilder iter_builder(&internal_comparator_, &arena, false);
    RangeDelAggregator range_del_agg(InternalKeyComparator(BytewiseComparator()),
                                     kMaxSequenceNumber, true);
    ReadOptions read_options;
    storage_manager_->add_iterators(table_cache_, nullptr,
                                    read_options, &iter_builder, &range_del_agg,
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
        //fprintf(stderr, "check error, row(%ld), key(%s), value(%s)\n", row,
                //is::util::to_cstring(key), is::util::to_cstring(value));
        XENGINE_LOG(ERROR, "check error", K(row), K(key), K(value));
      }
      ASSERT_TRUE(ret);
      ++row;
      iterator->Next();
    }
  }

  int meta_write(const int64_t level, const MiniTables &mini_tables) {
//    ChangeInfo info;
    //info.add(cf_desc_.column_family_id_, level, mini_tables);
    int ret = storage_manager_->apply(*(mini_tables.change_info_), false);

    return ret;
  }

  // We will fake 2 schema versions here: 
  //  schema_version 2 has 1 int, schema_versio 1 has 2 int, while 0 means invalid.
  void open_for_write(int level = 1, bool begin_trax = true, int64_t schema_version = 0) {
    //mini_tables_.space_manager = space_manager_.get();

    if (schema_version == 2) {
//      XengineSchema* schema = const_cast<XengineSchema*>(mini_tables_.schema);
//      schema->reset();
//      schema->null_bytes_in_rec = 0;
//      schema->maybe_unpack_info = false;
//      XengineFieldInfo field;
//      schema->fields.push_back(field);

      //field 0:int,not null
//      schema->fields[0].field_unit.fu_struct.storage_type = xengine::common::STORE_ALL;
//      schema->fields[0].field_unit.fu_struct.field_type = xengine::common::XENGINE_TYPE_NORMAL;
//      schema->fields[0].column_id = 0;
//      schema->fields[0].field_unit.fu_struct.null_mask = (unsigned char) 0x0; //useless
//      schema->fields[0].field_unit.fu_struct.null_offset = 0; // useless
//      schema->fields[0].length_info = 4;
//      schema->index_map.insert(std::make_pair(schema->fields[0].column_id,0));
//      schema->fixed_notnull_cnt = 1;
//      schema->fixed_nullable_cnt = 0;
//      schema->var_len_cnt = 0;
//
//      schema->schema_version = 2;
    } else if (schema_version == 1) {
//      XengineSchema* schema = const_cast<XengineSchema*>(mini_tables_.schema);
//      schema->reset();
//      schema->null_bytes_in_rec = 0;
//      schema->maybe_unpack_info = false;
//      XengineFieldInfo field;
//      schema->fields.push_back(field);
//      schema->fields.push_back(field);
//
//      //field 0:int,not null
//      schema->fields[0].field_unit.fu_struct.storage_type = xengine::common::STORE_ALL;
//      schema->fields[0].field_unit.fu_struct.field_type = xengine::common::XENGINE_TYPE_NORMAL;
//      schema->fields[0].column_id = 0;
//      schema->fields[0].field_unit.fu_struct.null_mask = (unsigned char) 0x0; //useless
//      schema->fields[0].field_unit.fu_struct.null_offset = 0; // useless
//      schema->fields[0].length_info = 4;
//      schema->index_map.insert(std::make_pair(schema->fields[0].column_id,0));
//
//      //field 1:int,not null
//      schema->fields[1].field_unit.fu_struct.storage_type = xengine::common::STORE_ALL;
//      schema->fields[1].field_unit.fu_struct.field_type = xengine::common::XENGINE_TYPE_NORMAL;
//      schema->fields[1].column_id = 1;
//      schema->fields[1].field_unit.fu_struct.null_mask = (unsigned char) 0x0; //useless
//      schema->fields[1].field_unit.fu_struct.null_offset = 0; // useless
//      schema->fields[1].length_info = 4;
//      schema->index_map.insert(std::make_pair(schema->fields[1].column_id,1));
//      schema->fixed_notnull_cnt = 2;
//      schema->fixed_nullable_cnt = 0;
//      schema->var_len_cnt = 0;
//
//      schema->schema_version = 1;
    }

    mini_tables_.change_info_ = &change_info_;
    int ret = Status::kOk;
    storage::LayerPosition output_layer_position =
        (0 == level)
        ? LayerPosition(level, storage::LayerPosition::NEW_GENERATE_LAYER_INDEX)
        : LayerPosition(level, 0);
    if (begin_trax) {
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
//        mini_tables_.change_info_->task_type_ = TaskType::MAJOR_SELF_COMPACTION_TASK;
        ASSERT_EQ(Status::kOk, ret);
      }
    }
    mini_tables_.space_manager = space_manager_;
    mini_tables_.table_space_id_ = 0;
    extent_builder_.reset(NewTableBuilder(
        context_->icf_options_, internal_comparator_, &props_,
        cf_desc_.column_family_id_, cf_desc_.column_family_name_, &mini_tables_,
        GetCompressionType(context_->icf_options_,
                           context_->mutable_cf_options_,
                           level/*level*/ ) /* compression type */,
        context_->icf_options_.compression_opts, output_layer_position,
        &compression_dict_, true));
  }

  void build_memtable(MemTable*& mem) {
    InternalKeyComparator cmp(BytewiseComparator());
    mem = new MemTable(cmp, context_->icf_options_,
        context_->mutable_cf_options_,
        wb_, kMaxSequenceNumber);
  }

  void append_memtable(const int64_t key_start, const int64_t key_end,
                       const int64_t sequence,
                       const int64_t step = 1,
                       const ValueType value_type = kTypeValue) {
    MemTable *mem = nullptr;
    build_memtable(mem);
    assert(nullptr != mem);
    const int64_t key_size = 20;
    int64_t row_size = 128;
    ArenaAllocator alloc;
    char *buf = (char *)alloc.alloc(128);
    memset(buf, 0, row_size);
    int64_t start_seq = sequence;
    for (int64_t key = key_start; key < key_end; key += step) {
      snprintf(buf, key_size, "%010ld", key);
      mem->Add(start_seq++, value_type, Slice(buf, strlen(buf)), Slice(buf, strlen(buf)));
    }
    mems_.push_back(mem);
  }

  void append(const int64_t key_start, const int64_t key_end,
              const int64_t sequence, const int64_t step = 1,
              const ValueType value_type = kTypeValue,
              const int64_t row_size = 128,
              bool finish = false) {
    ASSERT_TRUE(nullptr != extent_builder_.get());
    int64_t commit_log_seq = 0;
    const int64_t key_size = 20;
    char buf[row_size];
    memset(buf, 0, row_size);
    for (int64_t key = key_start; key < key_end; key += step) {
      snprintf(buf, key_size, "%010ld", key);
      InternalKey ikey(Slice(buf, strlen(buf)), sequence, value_type);
      extent_builder_->Add(ikey.Encode(), Slice(buf, row_size));
      ASSERT_TRUE(extent_builder_->status().ok());
    }
    if (finish) {
      extent_builder_->Finish();
      ASSERT_TRUE(extent_builder_->status().ok());
    }
  }

  void append_with_schema(const int64_t key_start, const int64_t key_end,
              const int64_t sequence,
              const int64_t schema_version,
              const int64_t step = 1,
              const ValueType value_type = kTypeValue) {
              //const int64_t row_size = 128,
              //bool finish = false) {
    ASSERT_TRUE(nullptr != extent_builder_.get());
    int64_t commit_log_seq = 0;
    const int64_t key_size = 20;
    int row_size = 4;
    char buf[40];
    memset(buf, 0, sizeof(buf));
    if (schema_version == 1) {
      row_size = 8;
    } else if (schema_version == 2) {
      row_size = 4;
    }
    for (int64_t key = key_start; key < key_end; key += step) {
      snprintf(buf, key_size, "%010ld", key);
      InternalKey ikey(Slice(buf, strlen(buf)), sequence, value_type);
      extent_builder_->Add(ikey.Encode(), Slice(buf + 20, row_size));
      ASSERT_TRUE(extent_builder_->status().ok());
    }
  }
//  void append_special(const int64_t key_start, const int64_t key_end,
//                      const int64_t sequence, const int64_t step = 1,
//                      const ValueType value_type = kTypeValue,
//                      const int64_t row_size = 128,
//              bool finish = false) {
//    ASSERT_TRUE(nullptr != extent_builder_.get());
//    const int64_t key_size = 20;
//    char buf[row_size];
//    memset(buf, 0, row_size);
//    for (int64_t key = key_start; key < key_end; key += step) {
//      snprintf(buf, key_size, "%010ld", key);
//      InternalKey ikey(Slice(buf, strlen(buf)), sequence, value_type);
//      extent_builder_->Add(ikey.Encode(), Slice(buf, row_size));
//      ASSERT_TRUE(extent_builder_->status().ok());
//    }
//    if (finish) {
//      extent_builder_->Finish();
//      ASSERT_TRUE(extent_builder_->status().ok());
//    }
//  }
  void close(const int64_t level, bool finish = true) {
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

  void write_data_with_schema(const int64_t key_start, const int64_t key_end,
          const int64_t sequence, const int64_t level, const int64_t schema_version,
          const int64_t step = 1, const ValueType value_type = kTypeValue) {
    open_for_write(level, true, schema_version);
    if (level == 0)
      append_with_schema(key_start, key_end, sequence, schema_version, step, value_type);
    else if (1 == level)
      append_with_schema(key_start, key_end, sequence, schema_version, step, value_type);
    else
      append_with_schema(key_start, key_end, sequence, schema_version, step, value_type);
    close(level);
  }

 void open_write(const int64_t key_start, const int64_t key_end,
                  const int64_t sequence, const int64_t level,
                  const int64_t step = 1, bool begin_trax = true) {
    open_for_write(level, begin_trax);
    assert(level == 0);
    append(key_start, key_end, sequence, step, kTypeValue, 128, true);
  }

  void write_data(const int64_t key_start, const int64_t key_end,
                  const int64_t sequence, const int64_t level,
                  const int64_t step = 1, const ValueType value_type = kTypeValue) {
    open_for_write(level);
    if (level == 0)
      append(key_start, key_end, sequence, step, value_type);
    else if (1 == level)
      append(key_start, key_end, sequence, step, value_type);
    else
      append(key_start, key_end, 0, step, value_type);
    close(level);
  }

  void write_equal_data(const int64_t start_key, const int64_t end_key,
                        const int64_t level,
                        const int64_t repeat_start, const int64_t repeat_end,
                        const int64_t step = 1,
                        const ValueType vtype = kTypeValue,
                        const int64_t row_size = 128) {
    open_for_write(level);
    ASSERT_TRUE(nullptr != extent_builder_.get());
    const int64_t key_size = 20;
    char buf[row_size];
    memset(buf, 0, row_size);
    for (int64_t key = start_key; key <= end_key; key += step) {
      for (int64_t t = repeat_end; t >= repeat_start; --t) {
        snprintf(buf, key_size, "%010ld", key);
        InternalKey ikey(Slice(buf, strlen(buf)), t, vtype);
        extent_builder_->Add(ikey.Encode(), Slice(buf, row_size));
      }
      ASSERT_TRUE(extent_builder_->status().ok());
    }
    close(level);
  }

  //void write_data_fpga(const int64_t key_start, const int64_t key_end,
  //                     const int64_t sequence, const int64_t level,
  //                     const int64_t key_size, const int64_t row_size,
  //                     const int64_t step = 1) {
  //  open_for_write();
  //  if (level == 0)
  //    append_fpga(key_start, key_end, sequence, key_size, row_size, step);
  //  else
  //    append_fpga(key_start, key_end, 0, key_size, row_size, step);
  //  close(level);
  //}

  void run_mt_ext_task() {
    CompactionContext ct;
    build_compact_context(&ct);
    JobContext jct(0);
    jct.storage_logger_ = storage_logger_;
    jct.task_type_ = db::TaskType::FLUSH_LEVEL1_TASK;
    ct.space_manager_ = space_manager_;
    ct.task_type_ = db::TaskType::FLUSH_LEVEL1_TASK;
    ImmutableCFOptions &ioptions = context_->icf_options_;
    LogBuffer log_buffer(logger::InfoLogLevel::INFO_LEVEL);
    Options option;
    ColumnFamilyData sub_table(option);
    monitor::InstrumentedMutex mutex(nullptr, env_);
//    util::EventLogger event_logger;
    Directory* output_file_directory = nullptr;
    monitor::Statistics* stats = nullptr;
    ArenaAllocator arena;
    FlushJob flush_job(
        dbname_, &sub_table,
        context_->idb_options_,
        jct,
        output_file_directory,
        GetCompressionFlush(ioptions, *ct.mutable_cf_options_, 0),
        stats, ct, arena);
    flush_job.set_memtables(mems_);
    flush_job.set_meta_snapshot(storage_manager_->get_current_version());
    MiniTables minitables;
    int ret = flush_job.prepare_flush_task(minitables);
    ASSERT_EQ(ret, 0);

    MtExtCompaction *compaction = flush_job.get_compaction();
    ret = storage_logger_->begin(FLUSH);
    ASSERT_EQ(ret, 0);
    ret = compaction->run();
    ASSERT_EQ(ret, 0);
    const storage::ChangeInfo &change_info = compaction->get_change_info();
    ret = storage_manager_->apply(change_info, false);
    ASSERT_EQ(ret, 0);
    sub_table.Unref();
  }
  void run_compact() {
    // util::Arena arena;
    ArenaAllocator arena;
    storage::CompactionJob job(arena);
    CompactionContext ct;
    build_compact_context(&ct);
    ct.space_manager_ = space_manager_;
    ct.task_type_ = db::TaskType::STREAM_COMPACTION_TASK;
    // after removing the snapshot list check, minor no need the snapshot,
    // just set the max seq
    ct.earliest_write_conflict_snapshot_ = db::kMaxSequenceNumber;
    int ret =
        job.init(ct, cf_desc_, storage_manager_,
                 storage_manager_->get_current_version());
    if (nullptr != (storage_manager_->get_current_version()->get_extent_layer_version(0))) {
      storage_manager_->get_current_version()->get_extent_layer_version(0)->ref();
    }
    ASSERT_EQ(ret, 0);
    ret = job.prepare();
    ASSERT_EQ(ret, 0);
    ret = storage_logger_->begin(MINOR_COMPACTION);
    ASSERT_EQ(ret, 0);
    ret = job.run();
    ASSERT_EQ(ret, 0);
    // install result once
    storage::Compaction *compaction = nullptr;
    while (nullptr != (compaction = job.get_next_task())) {
      job.append_change_info(compaction->get_change_info());
    }
    int64_t commit_seq;
    ret = storage_logger_->commit(commit_seq);
    ASSERT_EQ(ret, 0);
    ret = storage_manager_->apply(job.get_change_info(), false);
    ASSERT_EQ(ret, 0);
    assert(ct.output_level_ < 2);
    //ret = job.install();
    //ASSERT_EQ(ret, 0);
  }

  void run_intra_l0_compact() {
    // util::Arena arena;
    ArenaAllocator arena;
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

  void run_major_compact(const int64_t extents_limit = 100) {
    // util::Arena arena;
    ArenaAllocator arena;
    storage::CompactionJob job(arena);
    CompactionContext ct;
    build_compact_context(&ct);
    ct.task_type_ = db::TaskType::MAJOR_COMPACTION_TASK;
    ct.space_manager_ = space_manager_;
    std::vector<common::SequenceNumber> existing_snapshots;
    //existing_snapshots.push_back(10);
    //existing_snapshots.push_back(20);
    //existing_snapshots.push_back(50);
    ct.existing_snapshots_ = existing_snapshots;
    ct.earliest_write_conflict_snapshot_ = db::kMaxSequenceNumber;
    SnapshotImpl *snapshot = new SnapshotImpl();
    snapshot->number_ = storage_manager_->get_current_version()->GetSequenceNumber();
    snapshot->extent_layer_versions_[0] = storage_manager_->get_current_version()->get_extent_layer_version(0);
    snapshot->extent_layer_versions_[1] = storage_manager_->get_current_version()->get_extent_layer_version(1);
    snapshot->extent_layer_versions_[2] = storage_manager_->get_current_version()->get_extent_layer_version(2);
    snapshot->extent_layer_versions_[0]->ref();
    snapshot->extent_layer_versions_[1]->ref();
    snapshot->extent_layer_versions_[2]->ref();
    int ret = job.init(ct, cf_desc_, storage_manager_,
                       snapshot);
    /*
    if (storage_manager_->get_current_version()->get_level0() != nullptr) {
      storage_manager_->get_current_version()->get_level0()->ref();
    }
    */
    ASSERT_EQ(ret, 0);
    ret = job.prepare_major_task(1, extents_limit);
    ASSERT_EQ(ret, 0);
    int64_t commit_seq;
    if (job.get_task_type() == db::TaskType::SPLIT_TASK) {
      storage::Compaction *task = job.get_next_task();
      ret = storage_logger_->begin(MAJOR_COMPACTION);
      ASSERT_EQ(ret, 0);
      ret = task->run();
      ASSERT_EQ(ret, 0);
      const storage::ChangeInfo &change_info = task->get_change_info();
      ret = storage_manager_->apply(change_info, false);
      ASSERT_EQ(ret, 0);
      // We need manual commit here
      ret = storage_logger_->commit(commit_seq);
      ASSERT_EQ(ret, 0);
      fprintf(stderr, "\nAfter Split compaction\n");
      print_raw_meta();
      SnapshotImpl *new_snapshot = new SnapshotImpl();
      new_snapshot->number_ = storage_manager_->get_current_version()->GetSequenceNumber();
      new_snapshot->extent_layer_versions_[0] = storage_manager_->get_current_version()->get_extent_layer_version(0);
      new_snapshot->extent_layer_versions_[1] = storage_manager_->get_current_version()->get_extent_layer_version(1);
      new_snapshot->extent_layer_versions_[2] = storage_manager_->get_current_version()->get_extent_layer_version(2);
      new_snapshot->extent_layer_versions_[0]->ref();
      new_snapshot->extent_layer_versions_[1]->ref();
      new_snapshot->extent_layer_versions_[2]->ref();
      job.update_snapshot(new_snapshot);
      job.prepare_major_task(1, extents_limit, true);
    }
    //ret = job.run_major_task();
    //ASSERT_EQ(ret, 0);
    storage::Compaction *compaction = nullptr;
    while (nullptr != (compaction = job.get_next_task())) {
      ret = storage_logger_->begin(MAJOR_COMPACTION);
      ASSERT_EQ(ret, 0);
      // Note reuse case need to set
      static_cast<storage::GeneralCompaction*>(compaction)->set_delete_percent(10);
      ret = compaction->run();
      ASSERT_EQ(ret, 0);
      const storage::ChangeInfo &change_info = compaction->get_change_info();
      ret = storage_manager_->apply(change_info, false);
      ASSERT_EQ(ret, 0);
      // We need manual commit here
      ret = storage_logger_->commit(commit_seq);
      ASSERT_EQ(ret, 0);
      fprintf(stderr, "after major compaction\n");
      print_raw_meta();
      ASSERT_EQ(ret, 0);
    }
  }

 public:
  struct LogReporter : public db::log::Reader::Reporter {
    Status *status;
    virtual void Corruption(size_t bytes, const Status &s) override {
      if (this->status->ok()) *this->status = s;
    }
  };

  Context *context_;
  std::shared_ptr<Cache> cache_;
  TableCache *table_cache_;
  Env *env_;
  std::vector<std::string> names_;
  std::set<std::string> keys_;
  std::string dbname_;
  // DB *db_ = nullptr;
  VersionSet *version_set_;
  storage::StorageLogger *storage_logger_;
  StorageManager *storage_manager_;
  //unique_ptr<ExtentSpaceManager> space_manager_;
  ChangeInfo change_info_;
  ExtentSpaceManager* space_manager_;
  std::unique_ptr<db::log::Writer> descriptor_log_;
  Directory *db_dir;
  std::vector<std::unique_ptr<db::IntTblPropCollectorFactory>> props_;
  ColumnFamilyDesc cf_desc_;
  std::string compression_dict_;
  MiniTables mini_tables_;
  std::unique_ptr<table::TableBuilder> extent_builder_;
  InternalKeyComparator internal_comparator_;
  db::FileNumber next_file_number_;
  std::atomic<bool> shutting_down_;
  std::atomic<bool> bg_stopped_;
  //CompactionTestRunningStatus status_;
  std::unique_ptr<CompactionScheduler> compaction_scheduler_;
  util::autovector<MemTable*> mems_;
  WriteBufferManager *wb_;
  ArenaAllocator alloc_;
};

TEST_F(CompactionTest, test_mt_ext_compaction) {
  TestArgs arg;
  init(arg);
  // start, end, seq, step/level
  append_memtable(1, 1000, 100, 1);
  write_data(100, 2000, 50, 1);
  print_raw_meta();
  run_mt_ext_task();
  print_raw_meta();
}

TEST_F(CompactionTest, test_mt_ext_no_level1) {
  TestArgs arg;
  init(arg);
  // start, end, seq, step/level
  append_memtable(1, 1000, 100, 1);
  print_raw_meta();
  run_mt_ext_task();
  print_raw_meta();
}

TEST_F(CompactionTest, test_mt_ext_multi) {
  TestArgs arg;
  init(arg);
  append_memtable(1, 1000, 100, 1);
  append_memtable(200, 1200, 1100, 1);
  append_memtable(500, 2000, 2500, 1);
  append_memtable(800, 1500,4100, 1);
  write_data(50, 2300, 10, 1);
  print_raw_meta();
  run_mt_ext_task();
  print_raw_meta();
  IntRange r1[1] = {{1, 2299, 1}};
  check_result(1, r1, 1);

  auto check_func = [&r1](int64_t row, const Slice &key, const Slice &value) -> bool {
    return CompactionTest::check_key(row, key, value, r1[0]);
  };
  scan_all_data(check_func);
}

TEST_F(CompactionTest, test_mt_ext_delete) {
  TestArgs arg;
  init(arg);
  append_memtable(100, 500, 100, 1, kTypeDeletion);
  append_memtable(700, 800, 500, 1, kTypeDeletion);
  append_memtable(1000, 2000, 600, 1);
  write_data(1, 1500, 10, 1);
  print_raw_meta();
  run_mt_ext_task();
  print_raw_meta();
  IntRange r1[3] = {{1, 100 - 1, 1}, {500, 700 - 1, 1}, {800, 2000 - 1, 1}};
 // check_result(1, r1, 3);

  auto check_func = [&r1](int64_t row, const Slice &key, const Slice &value) -> bool {
    return CompactionTest::check_key_segment(row, key, value, r1, 3);
  };
  scan_all_data(check_func);
}

TEST_F(CompactionTest, test_get_all_l0_range) {
  TestArgs arg;
  init(arg);

  // seq:100               [1000------ ---------2000] [2000, 3000]           [4000, 5000]
  // seq:80   [500,800] [900,1200] [1500,1800] [1900,2200]           [3100,3500]
  // seq:70 [100 --               ----------------------------------------------------   5500]
  write_data(100, 5500, 70, 0);
  open_write(500, 800, 80, 0, 1, true/*begin_trax*/);
  open_write(900, 1200, 80, 0, 1, false);
  open_write(1500, 1800, 80, 0, 1, false);
  open_write(1900, 2200, 80, 0, 1, false);
  open_write(3100, 3500, 80, 0, 1, false);
  close(0, false);
  open_write(1000, 2000, 100, 0, 1, true);
  open_write(2000, 3000, 100, 0, 1, false);
  open_write(4000, 5000, 100, 0, 1, false);
  close(0, false);
  print_raw_meta();
  ArenaAllocator job_arena;
  storage::CompactionJob job(job_arena);
  CompactionContext ct;
  build_compact_context(&ct);
  ct.space_manager_ = space_manager_;
  //ct.space_manager_ = space_manager_.get();
  //ct.table_cache_ = table_cache_.get();

  int ret = job.init(ct, cf_desc_, storage_manager_,
        storage_manager_->get_current_version());
  storage_manager_->get_current_version()->get_extent_layer_version(0)->ref();
  ASSERT_EQ(ret, 0);

  int64_t way_size = 64;
  storage::CompactionJob::CompactWay *compact_way = new storage::CompactionJob::CompactWay[way_size];
  storage::Range wide_range;
  Arena arena;

  // Case 1. Normal
  ret = job.TEST_get_all_l0_range(job_arena, 64, compact_way, way_size, wide_range, true);

  ASSERT_EQ(ret, 0);
  ASSERT_EQ(way_size, 3);
  ASSERT_EQ(0, check_key(wide_range.start_key_, 100));
  ASSERT_EQ(0, check_key(wide_range.end_key_, 5499));
  ASSERT_EQ(0, check_metakey(compact_way[2].range_.start_key_, 1000));
  ASSERT_EQ(0, check_metakey(compact_way[2].range_.end_key_, 4999));
  ASSERT_EQ(0, check_metakey(compact_way[1].range_.start_key_, 500));
  ASSERT_EQ(0, check_metakey(compact_way[1].range_.end_key_, 3499));
  ASSERT_EQ(0, check_metakey(compact_way[0].range_.start_key_, 100));
  ASSERT_EQ(0, check_metakey(compact_way[0].range_.end_key_, 5499));

  // Case 2. limit 2 way
  way_size = 2;
  ret = job.TEST_get_all_l0_range(job_arena, 2, compact_way, way_size, wide_range);

  ASSERT_EQ(ret, 0);
  ASSERT_EQ(way_size, 2);
  ASSERT_EQ(0, check_key(wide_range.start_key_, 100));
  ASSERT_EQ(0, check_key(wide_range.end_key_, 5499));

  ASSERT_EQ(0, check_metakey(compact_way[1].range_.start_key_, 500));
  ASSERT_EQ(0, check_metakey(compact_way[1].range_.end_key_, 3499));
  ASSERT_EQ(0, check_metakey(compact_way[0].range_.start_key_, 100));
  ASSERT_EQ(0, check_metakey(compact_way[0].range_.end_key_, 5499));
  delete[] compact_way;

  run_compact();
  print_raw_meta();
  IntRange r[1] = {{100, 5499, 1}};
  check_result(1, r, 1);
  auto check_func = [&r](int64_t row, const Slice &key,
      const Slice &value) -> bool {
    return CompactionTest::check_key(row, key, value, r[0]);
  };
  scan_all_data(check_func);
}

TEST_F(CompactionTest, test_intra_l0_normal_compact) {
  TestArgs one_arg;
  init(one_arg);
  write_data(1000, 8000, 10, 1);
  write_data(1500, 2000, 20, 0);
  write_data(1700, 2300, 30, 0);
  write_data(800, 2500, 40, 0);
  print_raw_meta();
  run_intra_l0_compact();
  print_raw_meta();
  IntRange r0[1] = {{800, 2499, 1}};
  check_result(0, r0, 1);
  IntRange r1[1] = {{1000, 7999, 1}};
  check_result(1, r1, 1);

  IntRange ra[1] = {{800, 7999, 1}};
  auto check_func = [&ra](int64_t row, const Slice &key,
      const Slice &value) -> bool {
    return CompactionTest::check_key(row, key, value, ra[0]);
  };
  scan_all_data(check_func);
}

TEST_F(CompactionTest, test_intra_l0_down_level0) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);

    write_data(1000, 8000, 10, 1);
    write_data(2500, 3000, 20, 0);
    write_data(1500, 2500, 30, 0);
    write_data(800, 1500, 40, 0);
    print_raw_meta();
    run_intra_l0_compact();
    print_raw_meta();
    IntRange r0[3] = {
      {800, 1499, 1},
      {1500, 2499, 1},
      {2500, 2999, 1}};
    check_result(0, r0, 3);
    IntRange r1[1] = {{1000, 7999, 1}};
    check_result(1, r1, 1);

    IntRange ra[1] = {{800, 7999, 1}};
    auto check_func = [&ra](int64_t row, const Slice &key,
        const Slice &value) -> bool {
      return CompactionTest::check_key(row, key, value, ra[0]);
    };
    scan_all_data(check_func);
  }
}

TEST_F(CompactionTest, test_intra_l0_mix_reuse_and_down) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);

    write_data(1000, 8000, 10, 1);
    open_write(500, 1000, 20, 0, 1, true);
    open_write(2500, 3000, 20, 0, 1, false);
    close(0, false);
    open_write(1500, 2500, 30, 0, 1, true);
    open_write(3000, 4000, 30, 0, 1, false);
    close(0, false);
    open_write(800, 1500, 40, 0, 1, true);
    open_write(3500, 4500, 40, 0, 1, false);
    close(0, false);

    print_raw_meta();
    run_intra_l0_compact();
    print_raw_meta();
    IntRange r0[4] = {
      {500, 1499, 1},
      {1500, 2499, 1},
      {2500, 2999, 1},
      {3000, 4499, 1}};
    check_result(0, r0, 4);
    IntRange r1[1] = {{1000, 7999, 1}};
    check_result(1, r1, 1);

    IntRange ra[1] = {{500, 7999, 1}};
    auto check_func = [&ra](int64_t row, const Slice &key,
        const Slice &value) -> bool {
      return CompactionTest::check_key(row, key, value, ra[0]);
    };
    scan_all_data(check_func);
  }
}

TEST_F(CompactionTest, test_intra_l0_overflow_layer) {
  TestArgs one_arg;
  init(one_arg);

  int64_t seq = 10;
  for (int64_t i = 1; i <= 20; ++i) {
    open_write(i * 100, i * 1000, seq, 0, 15, true);
    open_write(i * 1000, i * 2000, seq, 0, 15, false);
    open_write(i * 10000, i * 20000, seq, 0, 100, false);
    close(0, false);
    seq++;
  }

  write_data(1000, 20000, 0, 1, 100);
  write_data(25000, 80000, 0, 1, 100);
  print_raw_meta();
  ArenaAllocator job_arena;
  storage::CompactionJob job(job_arena);
  CompactionContext ct;
  build_compact_context(&ct);
  ct.task_type_ = db::TaskType::INTRA_COMPACTION_TASK;

  ct.space_manager_ = space_manager_;
  int ret =
    job.init(ct, cf_desc_, storage_manager_, storage_manager_->get_current_version());
  storage_manager_->get_current_version()->get_extent_layer_version(0)->ref();
  ASSERT_EQ(ret, 0);
  // IntraL0 only support 1 task
  //ret = job.prepare_minor_task(5);
  ret = job.prepare_minor_task(db::kMaxSequenceNumber);
  ASSERT_EQ(ret, 0);

  ret = storage_logger_->begin(MINOR_COMPACTION);
  ASSERT_EQ(ret, 0);
  while (job.get_task_size() > 0) {
    storage::Compaction *compaction = job.get_next_task();
    ret = compaction->run();
    ASSERT_EQ(ret, 0);
    job.append_change_info(compaction->get_change_info());
    job.destroy_compaction(compaction);
  }
  int64_t commit_seq = 0;
  ret = storage_logger_->commit(commit_seq);
  ASSERT_EQ(ret, 0);
  ret = storage_manager_->apply(job.get_change_info(), false);
  ASSERT_EQ(ret, 0);
  print_raw_meta();

  //IntRange r[2] = {{100, 19990, 15}, {20000, 199900, 100}};
  //check_result(1, r, 2);
}

TEST_F(CompactionTest, test_intra_l0_multi_way_compact) {
  TestArgs one_arg;
  init(one_arg);

  write_data(1000, 8000, 10, 1);
  open_write(2500, 3000, 20, 0, 1, true);
  open_write(3000, 4000, 20, 0, 1, false);
  open_write(4000, 5000, 20, 0, 1, false);
  close(0, false);
  open_write(1500, 2500, 30, 0, 1, true);
  open_write(3000, 4500, 30, 0, 1, false);
  close(0, false);
  open_write(800, 1500, 40, 0, 1, true);
  open_write(1500, 3500, 40, 0, 1, false);
  close(0, false);

  //    [  ] [    ]
  //     [    ] [   ]
  //        [    ]
  print_raw_meta();
  run_intra_l0_compact();
  print_raw_meta();
  IntRange r0[2] = {
    {800, 1499, 1},
    {1500, 4999, 1}};
  check_result(0, r0, 2);
  IntRange r1[1] = {{1000, 7999, 1}};
  check_result(1, r1, 1);

  IntRange ra[1] = {{800, 7999, 1}};
  auto check_func = [&ra](int64_t row, const Slice &key,
      const Slice &value) -> bool {
    return CompactionTest::check_key(row, key, value, ra[0]);
  };
  scan_all_data(check_func);
}

TEST_F(CompactionTest, test_intra_l0_muliple_task) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);

    // ten layers batch level0 meta change
    int64_t seq = 10;
    for (int64_t i = 1; i <= 10; ++i) {
      open_write(i * 100, i * 1000, seq, 0, 15, true);
      open_write(i * 1000, i * 2000, seq, 0, 15, false);
      open_write(i * 10000, i * 20000, seq, 0, 100, false);
      close(0, false);
      seq++;
    }

    write_data(1000, 20000, 0, 1, 100);
    write_data(25000, 80000, 0, 1, 100);
    write_data(85000, 180000, 0, 1, 100);

    print_raw_meta();
    ArenaAllocator arena;
    storage::CompactionJob job(arena);
    CompactionContext ct;
    build_compact_context(&ct);
    ct.task_type_ = db::TaskType::INTRA_COMPACTION_TASK;

    ct.space_manager_ = space_manager_;
    int ret =
      job.init(ct, cf_desc_, storage_manager_, storage_manager_->get_current_version());
    storage_manager_->get_current_version()->get_extent_layer_version(0)->ref();
    ASSERT_EQ(ret, 0);
    // Intra L0 only split 1 task, so we use a big int
    ret = job.prepare_minor_task(db::kMaxSequenceNumber);
    ASSERT_EQ(ret, 0);

    ret = storage_logger_->begin(MINOR_COMPACTION);
    ASSERT_EQ(ret, 0);
    while (job.get_task_size() > 0) {
      storage::Compaction *compaction = job.get_next_task();
      ret = compaction->run();
      ASSERT_EQ(ret, 0);
      Status status = job.append_change_info(compaction->get_change_info());
      ASSERT_EQ(status.ok(), true);
      job.destroy_compaction(compaction);
    }
    int64_t commit_seq = 0;
    ret = storage_logger_->commit(commit_seq);
    ASSERT_EQ(ret, 0);
    ret = storage_manager_->apply(job.get_change_info(), false);
    ASSERT_EQ(ret, 0);
    print_raw_meta();

    IntRange r0[1] = {
      {100, 199900, 100}};
    check_result(0, r0, 1);
    IntRange r1[3] = {
      {1000, 19900, 1},
      {25000, 79900, 1},
      {85000, 179900, 1}};
    check_result(1, r1, 3);
  }
}

TEST_F(CompactionTest, test_minor_reuse_compact) {
  TestArgs one_arg;
  init(one_arg);
  // reused level 1
  write_data(2000, 2500, 10, 1);
  write_data(2500, 3500, 10, 1);
  open_write(500, 1000, 20, 0, 1, true);
  open_write(1500, 2000, 20, 0, 1, false);
  close(0, false);
  open_write(700, 1700, 30, 0, 1, true);
  open_write(3000, 4000, 30, 0, 1, false);
  close(0, false);

  print_raw_meta();
  run_compact();
  print_raw_meta();
  IntRange r1[3] = {
    {500, 1999, 1},
    {2000, 2499, 1},
    {2500, 3999, 1}};
  check_result(1, r1, 3);

  IntRange ra[1] = {{500, 3999, 1}};
  auto check_func = [&ra](int64_t row, const Slice &key,
      const Slice &value) -> bool {
    return CompactionTest::check_key(row, key, value, ra[0]);
  };
  scan_all_data(check_func);
}

TEST_F(CompactionTest, test_minor_delete_data) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);

    write_data(1000, 2000, 10, 2);
    write_data(1000, 1500, 20, 0, 1, kTypeDeletion);

    print_raw_meta();
    run_compact();
    print_raw_meta();
  }
}

 TEST_F(CompactionTest, test_normal_compact) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    write_data(1000, 8000, 10, 1);
    write_data(1500, 2000, 20, 0);
    print_raw_meta();
    run_compact();
    print_raw_meta();
    IntRange r[1] = {{1000, 7999, 1}};
    check_result(1, r, 1);
    auto check_func = [&r](int64_t row, const Slice &key,
                           const Slice &value) -> bool {
      return CompactionTest::check_key(row, key, value, r[0]);
    };
    scan_all_data(check_func);
  }
}

 TEST_F(CompactionTest, test_down_level0) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    write_data(1000, 3000, 10, 1);
    write_data(1500, 2000, 20, 0);
    // print_raw_meta();
    run_compact();
    IntRange r1[1] = {{1000, 2999, 1}};
    check_result(1, r1, 1);

    auto check_func = [&r1](int64_t row, const Slice &key,
                            const Slice &value) -> bool {
      return CompactionTest::check_key(row, key, value, r1[0]);
    };


    //auto print_f = [](int64_t row, const Slice& key, const Slice& value) ->
    //bool {
    //fprintf(stderr, "row[%ld]=[%.*s]\n", row, (int)key.size(), key.data());
    //return true;
    //};
    scan_all_data(check_func);
    write_data(5000, 6000, 30, 0);
    // print_raw_meta();
    run_compact();
    // print_raw_meta();
    IntRange r2[2] = {{1000, 2999, 1}, {5000, 5999, 1}};
    check_result(1, r2, 2);
    auto check_func2 = [&r2](int64_t row, const Slice &key,
                             const Slice &value) -> bool {
      return CompactionTest::check_key_segment(row, key, value, r2, 2);
    };
    scan_all_data(check_func2);
  }
}

 TEST_F(CompactionTest, test_multi_way_compact) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    write_data(1000, 3000, 10, 1);
    write_data(1500, 2000, 20, 0);
    write_data(5000, 6000, 30, 0);
    write_data(2000, 4000, 40, 0);
    print_raw_meta();
    run_compact();
    print_raw_meta();
    IntRange r[2] = {{1000, 3999, 1}, {5000, 5999, 1}};
    auto check_func2 = [&r](int64_t row, const Slice &key,
                            const Slice &value) -> bool {
      return CompactionTest::check_key_segment(row, key, value, r, 2);
    };
    check_result(1, r, 2);
    scan_all_data(check_func2);
  }
}

 TEST_F(CompactionTest, test_multi_way_multi_range_compact) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    // level 1 has three range;
    write_data(10000, 20000, 10, 1, 100);
    write_data(21111, 28888, 10, 1, 100);
    write_data(30000, 38888, 10, 1, 100);

    open_write(11000, 15000, 20, 0, 100, true);
    open_write(18000, 25000, 20, 0, 100, false);
    close(0, false);

    open_write(16000, 25000, 30, 0, 100, true);
    open_write(30000, 50000, 30, 0, 100, false);
    open_write(51000, 59000, 30, 0, 100, false);
    close(0, false);

    open_write(19000, 21000, 40, 0, 100, true);
    open_write(80000, 90000, 40, 0, 100, false);
    close(0, false);

    print_raw_meta();
    run_compact();
    print_raw_meta();

    IntRange r[3] = {
        {10000, 49900, 100}, {51000, 58900, 100}, {80000, 89900, 100}};
    auto check_func3 = [&r](int64_t row, const Slice &key,
                            const Slice &value) -> bool {
      return CompactionTest::check_key_segment(row, key, value, r, 3);
    };
    UNUSED(check_func3);
    check_result(1, r, 3);
    //auto print_f = [](int64_t row, const Slice& key, const Slice& value) ->
    //bool {
    //fprintf(stderr, "row[%ld]=[%.*s]\n", row, (int)key.size(), key.data());
    //return true;
    //};
    //scan_all_data(print_f);
  }
}

 TEST_F(CompactionTest, test_intersect_key_compact) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    // level 1 has three range;
    write_data(2887, 5500, 10, 1);
    write_data(5799, 7900, 10, 1);

    open_write(1100, 1500, 20, 0, 1, true);
    open_write(1999, 2500, 20, 0, 1, false);
    close(0, false);

    open_write(1000, 2000, 40, 0, 1, true);
    open_write(2111, 2888, 40, 0, 1, false);
    close(0, false);

    print_raw_meta();
    run_compact();
    print_raw_meta();
    IntRange r[2] = {{1000, 5499}, {5799, 7899}};
    check_result(1, r, 2);
  }
}

TEST_F(CompactionTest, test_intersect_key_compact2) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    // level 1 has three range;
    write_data(2887, 5500, 50, 1);
    write_data(5511, 7900, 50, 1);

    open_write(1100, 1200, 20, 0, 1, true);
    // Note same way should have different sequence.
    open_write(1199, 2500, 10, 0, 1, false);
    close(0, false);

    open_write(1000, 1155, 40, 0, 1, true);
    open_write(1255, 2200, 40, 0, 1, false);
    close(0, false);

    print_raw_meta();
    run_compact();
    print_raw_meta();
    IntRange r[3] = {{1000, 2499, 1}, {2887, 5499, 1}, {5511, 7899, 1}};
    check_result(1, r, 3);
    auto check_func3 = [&r](int64_t row, const Slice &key,
        const Slice &value) -> bool {
      return CompactionTest::check_key_segment(row, key, value, r, 3);
    };
    scan_all_data(check_func3);
  }
}

TEST_F(CompactionTest, test_add_new_block) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    write_data(1000, 1923, 10, 1);
    write_data(1924, 2500, 10, 1);
    write_data(1500, 1988, 20, 0);
    write_data(1925, 3000, 40, 0);
    print_raw_meta();
    run_compact();
    print_raw_meta();
    IntRange r[1] = {{1000, 2999, 1}};
    check_result(1, r, 1);
    auto check_func = [&r](int64_t row, const Slice &key,
                           const Slice &value) -> bool {
      return CompactionTest::check_key(row, key, value, r[0]);
    };
    scan_all_data(check_func);
  }
}

TEST_F(CompactionTest, test_add_new_way) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    open_write(1000, 1921, 10, 0, 1, true);
    open_write(2100, 2200, 10, 0, 1, false);
    close(0, false);
    write_data(1200, 2121, 20, 0);

    write_data(1924, 1935, 0, 1);

    print_raw_meta();
    run_compact();
    print_raw_meta();
  }
}

TEST_F(CompactionTest, test_pending_data_block) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);

    open_for_write(0);
    append(1000, 1461, 10, 1);
    append(10000, 20000, 10, 100);
    close(0);

    open_write(1002, 2200, 20, 0, 1, true);
    open_write(2500, 3000, 20, 0, 1, false);
    open_write(3000, 4000, 20, 0, 1, false);
    close(0, false);

    write_data(1005, 18000, 0, 1, 100);

    print_raw_meta();
    run_compact();
    print_raw_meta();
  }
}

TEST_F(CompactionTest, test_memory_case1) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);

    open_for_write(0);
    append(1000, 4688, 10, 1);
    append(10000, 12000, 10, 1);
    close(0);

    open_for_write();
    append(1002, 4690, 11, 1);
    append(5000, 5200, 11, 1);
    close(1);

    write_data(5500, 6000, 20, 1, 1);
    write_data(6500, 7000, 20, 1, 1);
    write_data(7500, 8000, 20, 1, 1);
    write_data(9000, 11000, 20, 1, 1);

    print_raw_meta();
    run_compact();
    print_raw_meta();
  }
}

TEST_F(CompactionTest, test_memory_case2) {
  TestArgs arg;
  arg.compression = kNoCompression;
  arg.format_version = 3;

  init(arg);

  open_for_write(0);
  append(1200, 2122, 50, 2);
  append(2700, 8000, 50, 100);
  close(0);

  open_for_write(0);
  append(1300, 2222, 40, 2);
  close(0);

  write_data(1310, 9000, 30, 0, 100);

  write_data(1000, 5000, 0, 1, 1);

  print_raw_meta();
  run_compact();
  print_raw_meta();
}

TEST_F(CompactionTest, test_memory_case3) {
  TestArgs arg;
  arg.compression = kNoCompression;
  arg.format_version = 3;

  init(arg);

  open_write(1000, 1460, 30, 0, 1, true);
  open_write(2000, 2460, 30, 0, 1, false);
  close(0, false);

  write_data(1200, 2600, 20, 0, 5);

  write_data(1800, 5000, 0, 1, 10);

  print_raw_meta();
  run_compact();
  print_raw_meta();
}

 TEST_F(CompactionTest, test_wild_range_block) {
  TestArgs arg;
  arg.compression = kNoCompression;
  arg.format_version = 3;

  init(arg);

  open_for_write(0);
  append(1000, 1461, 50, 1);
  append(10000, 20000, 50, 100);
  close(0);

  write_data(2000, 5000, 30, 0, 1);
  write_data(2100, 5100, 20, 0, 1);

  print_raw_meta();
  run_compact();
  print_raw_meta();
}

TEST_F(CompactionTest, test_lost_last_blocks) {
  TestArgs arg;
  arg.compression = kNoCompression;
  arg.format_version = 3;

  init(arg);

  open_for_write(0);
  append(1000, 2383, 50, 1);
  append(2383, 2844, 50, 1);
  append(2843, 3304, 40, 1);
  append(3303, 3764, 30, 1);
  append(3763, 4224, 20, 1);
  close(0);

  write_data(500, 2000, 0, 1, 10);
  write_data(2000, 5000, 0, 1, 10);

  print_raw_meta();
  run_compact();
  // print_raw_meta();
}

TEST_F(CompactionTest, test_lost_end_stream) {
  TestArgs arg;
  arg.compression = kNoCompression;
  arg.format_version = 3;

  init(arg);

  open_for_write(0);
  append(1000, 2383, 50, 1);
  close(0);

  open_for_write(0);
  append(1800, 1956, 30, 1);
  close(0);

  write_data(500, 5000, 0, 1, 60);

  print_raw_meta();
  run_compact();
  print_raw_meta();
}

// Handle this case:
// When we fetch next data block for [2805, 3265], we lost [3266, 3499].
// L0:                          [3000,                      5500]
// L0:            [1000,                              5000]
// L1:[500,960] [961,1421]...[2805,3265] [3266,3499]
TEST_F(CompactionTest, test_lost_end_stream2) {
  TestArgs arg;
  init(arg);

  open_for_write(0);
  append(3000, 3001, 80, 1);
  append(5500, 5501, 80, 1);
  close(0);

  open_for_write(0);
  append(1000, 1001, 50, 1);
  append(5000, 5001, 60, 1);
  close(0);

  write_data(500, 3500, 30, 1, 1);

  print_raw_meta();
  run_compact();
  print_raw_meta();

  IntRange r2[2] = {{500, 3499, 1}, {5000, 5500, 500}};
  //check_result(1, r2, 2);
  auto check_func2 = [&r2](int64_t row, const Slice &key,
      const Slice &value) -> bool {
    return CompactionTest::check_key_segment(row, key, value, r2, 2);
  };
  scan_all_data(check_func2);
}

// Handle this case:
// When we fetch next data block for [2805, 3265], we lost [3266, 3499].
// L0:                          [3000,                      5500]
// L0:          [1000,                                5000]
// L1:             [2342,2804][2805,3265] [3266,3499]
TEST_F(CompactionTest, test_lost_end_stream3) {
  TestArgs arg;
  init(arg);

  open_for_write(0);
  append(3000, 3001, 80, 1);
  append(5500, 5501, 80, 1);
  close(0);

  open_for_write(0);
  append(1000, 1001, 50, 1);
  append(5000, 5001, 60, 1);
  close(0);

  write_data(2342, 3500, 30, 1, 1);

  print_raw_meta();
  run_compact();
  print_raw_meta();

  IntRange r2[3] = {
    {1000, 1000, 1},
    {2342, 3499, 1},
    {5000, 5500, 500}};
  //check_result(1, r2, 2);
  auto check_func2 = [&r2](int64_t row, const Slice &key,
      const Slice &value) -> bool {
    return CompactionTest::check_key_segment(row, key, value, r2, 3);
  };
  scan_all_data(check_func2);
}

 TEST_F(CompactionTest, test_intersect_key_bug_12659392) {
  TestArgs arg;
  arg.compression = kNoCompression;
  arg.format_version = 3;

  init(arg);

  open_for_write(0);
  append(1923, 2384, 50, 1);
  append(2383, 2844, 45, 1);
  append(2843, 3304, 40, 1);
  append(3303, 3764, 30, 1);
  append(3763, 4224, 20, 1);
  close(0);

  open_for_write(0);
  append(1911, 2371, 10, 1);
  append(3305, 3318, 10, 1, kTypeValue, 5020);
  append(5000, 5461, 10, 1);
  close(1);

  print_raw_meta();
  run_compact();
  // print_raw_meta();
}

TEST_F(CompactionTest, test_f) {
  TestArgs arg;
  arg.compression = kNoCompression;
  arg.format_version = 3;

  init(arg);

  open_for_write(0);
  append(1000, 2383, 50, 1);
  append(2843, 3304, 50, 1);
  close(0);

  open_for_write(0);
  append(1001, 2384, 10, 1);
  append(2383, 2844, 5, 1);
  append(3303, 3764, 5, 1);
  // append(3500, 3900, 5, 1);
  close(1);
  // write_data(1001, 2384, 0, 1, 1);
  // write_data(2383, 2666, 0, 1, 1);

  print_raw_meta();
  run_compact();
  print_raw_meta();
}

TEST_F(CompactionTest, test_many_way_compact) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    for (int64_t i = 10; i < 30; ++i) {
      write_data(10000, 20000, i * 10, 0, i * 3);
    }

    write_data(10000, 20000, 0, 1, 100);

    print_raw_meta();
    run_compact();
    print_raw_meta();

    IntRange l1_r[1] = {10000, 19999, 100};
    check_result(1, l1_r, 1);
    IntRange l0_r[5] = {{10000, 19918, 30},
                        {10000, 19996, 33},
                        {10000, 19963, 36},
                        {10000, 19984, 39},
                        {10000, 19975, 42}};
    check_result(0, l0_r, 5);
  }
}

TEST_F(CompactionTest, test_muliple_task) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);

    // ten layers batch level0 meta change
    int64_t seq = 10;
    for (int64_t i = 1; i <= 10; ++i) {
      open_write(i * 100, i * 1000, seq, 0, 15, true);
      open_write(i * 1000, i * 2000, seq, 0, 15, false);
      open_write(i * 10000, i * 20000, seq, 0, 100, false);
      close(0, false);
      seq++;
    }

    write_data(1000, 20000, 0, 1, 100);
    write_data(25000, 80000, 0, 1, 100);
    write_data(85000, 180000, 0, 1, 100);

    print_raw_meta();
    ArenaAllocator arena;
    storage::CompactionJob job(arena);
    CompactionContext ct;
    build_compact_context(&ct);
    //ct.space_manager_ = space_manager_.get();
    ct.space_manager_ = space_manager_;
    int ret =
        job.init(ct, cf_desc_, storage_manager_,
                 storage_manager_->get_current_version());
    storage_manager_->get_current_version()->get_extent_layer_version(0)->ref();
    ASSERT_EQ(ret, 0);
    ret = job.prepare_minor_task(5);
    ASSERT_EQ(ret, 0);
    ret = storage_logger_->begin(MINOR_COMPACTION);
    ASSERT_EQ(ret, 0);

    while (job.get_task_size() > 0) {
      storage::Compaction *compaction = job.get_next_task();
      ret = compaction->run();
      ASSERT_EQ(ret, 0);
      Status status = job.append_change_info(compaction->get_change_info());
      ASSERT_EQ(status.ok(), true);
      job.destroy_compaction(compaction);
    }
    int64_t commit_seq = 0;
    ret = storage_logger_->commit(commit_seq);
    ASSERT_EQ(ret, 0);

    ret = storage_manager_->apply(job.get_change_info(), false);
    ASSERT_EQ(ret, 0);
    print_raw_meta();

    IntRange r[2] = {{100, 19990, 15}, {20000, 199900, 100}};
    check_result(1, r, 2);
    //IntRange r[1] = {{100, 199900, 15}};
    //check_result(1, r, 1);
  }
}

TEST_F(CompactionTest, test_mp) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    write_data(10000, 20000, 10, 0, 100);
    write_data(1000, 20000, 0, 1, 100);
    write_data(25000, 80000, 0, 1, 100);
    write_data(85000, 180000, 0, 1, 100);

    print_raw_meta();
    ArenaAllocator arena;
    storage::CompactionJob job(arena);
    CompactionContext ct;
    build_compact_context(&ct);
    //ct.space_manager_ = space_manager_.get();
    ct.space_manager_ = space_manager_;
    int ret =
        job.init(ct, cf_desc_, storage_manager_,
                 storage_manager_->get_current_version());
    storage_manager_->get_current_version()->get_extent_layer_version(0)->ref();
    ASSERT_EQ(ret, 0);
    ret = job.prepare_minor_task(5);
    ASSERT_EQ(ret, 0);
    ret = storage_logger_->begin(MINOR_COMPACTION);
    ASSERT_EQ(ret, 0);

    while (job.get_task_size() > 0) {
      storage::Compaction *compaction = job.get_next_task();
      ret = compaction->run();
      ASSERT_EQ(ret, 0);
      Status status = job.append_change_info(compaction->get_change_info());
      ASSERT_EQ(status.ok(), true);
      job.destroy_compaction(compaction);
    }
    int64_t commit_seq = 0;
    ret = storage_logger_->commit(commit_seq);
    ASSERT_EQ(ret, 0);
    ret = storage_manager_->apply(job.get_change_info(), false);
    ASSERT_EQ(ret, 0);
    print_raw_meta();
  }
}

TEST_F(CompactionTest, test_no_intersect_l0) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    cf_desc_.column_family_id_ = 0;
    open_write(1 * 1000, (1 + 1) * 1000, 100, 0, 5, true);
    for (int i = 2; i < 8; ++i) {
      open_write(i * 1000, (i + 1) * 1000, 100, 0, 5, false);
    }
    close(0, false);

    write_data(1000, 20000, 50, 0, 20);

    cf_desc_.column_family_id_ = 1;

    write_data(1500, 2000, 10, 0, 5);

    print_raw_meta();

    cf_desc_.column_family_id_ = 0;
    ArenaAllocator arena;
    storage::CompactionJob job(arena);
    CompactionContext ct;
    build_compact_context(&ct);
    //ct.space_manager_ = space_manager_.get();
    ct.space_manager_ = space_manager_;
    int ret =
        job.init(ct, cf_desc_, storage_manager_,
                 storage_manager_->get_current_version());
    storage_manager_->get_current_version()->get_extent_layer_version(0)->ref();
    ASSERT_EQ(ret, 0);
    ret = job.prepare_minor_task(10);
    ASSERT_EQ(ret, 0);
    ret = storage_logger_->begin(MINOR_COMPACTION);
    ASSERT_EQ(ret, 0);

    while (job.get_task_size() > 0) {
      storage::Compaction *compaction = job.get_next_task();
      ret = compaction->run();
      ASSERT_EQ(ret, 0);
      Status status = job.append_change_info(compaction->get_change_info());
      ASSERT_EQ(status.ok(), true);
      job.destroy_compaction(compaction);
    }
    int64_t commit_seq = 0;
    ret = storage_logger_->commit(commit_seq);
    ASSERT_EQ(ret, 0);
    ret = storage_manager_->apply(job.get_change_info(), false);
    ASSERT_EQ(ret, 0);
    print_raw_meta();
  }
}

TEST_F(CompactionTest, test_minor_rate_limiter) {
  TestArgs test_arg;
  for (int cnt = 0; cnt < 2; cnt++) {
    init(test_arg);

    write_data(100, 400000, 30, 0);
    write_data(1000, 1600000, 10, 1, 50);

    //print_raw_meta();
    ArenaAllocator arena;
    storage::CompactionJob job(arena);
    CompactionContext ct;
    build_compact_context(&ct);
    ct.space_manager_ = space_manager_;
    //ct.table_cache_ = table_cache_.get();
    if (cnt == 0) {
      context_->icf_options_.rate_limiter = NewGenericRateLimiter(100 << 20, 100 * 1000, 10);
      ct.cf_options_ = &context_->icf_options_;
    } else {
      context_->icf_options_.rate_limiter = NewGenericRateLimiter(21 << 20, 100 * 1000, 10);
      ct.cf_options_ = &context_->icf_options_;
    }

    int ret =
        job.init(ct, cf_desc_, storage_manager_,
                 storage_manager_->get_current_version());
    storage_manager_->get_current_version()->get_extent_layer_version(0)->ref();
    ASSERT_EQ(ret, 0);
    ret = job.prepare();
    ASSERT_EQ(ret, 0);
    ret = storage_logger_->begin(MINOR_COMPACTION);
    ASSERT_EQ(ret, 0);

    while (job.get_task_size() > 0) {
      auto start = env_->NowMicros();
      storage::Compaction *compaction = job.get_next_task();
      ret = compaction->run();
      ASSERT_EQ(ret, 0);
      Status status = job.append_change_info(compaction->get_change_info());
      ASSERT_EQ(status.ok(), true);

      int64_t total_input_extents = compaction->get_stats().record_stats_.total_input_extents;
      auto elapsed = env_->NowMicros() - start;
      double rate = total_input_extents * 2 * 1024 * 1024 * 1000000.0 / elapsed;
      //double rate = limiter->GetTotalBytesThrough() * 1000000.0 / elapsed;
      fprintf(stderr,
              "input_extent: %ld, limit rate: %d KB/sec,"
              " actual rate: %lf KB/s, elapsed %.2lf seconds\n",
              total_input_extents,
              cnt == 0 ? (100 * 1024) : (21 * 1024),
              rate / 1024, elapsed / 1000000.0);

      job.destroy_compaction(compaction);
    }
    int64_t commit_seq = 0;
    ret = storage_logger_->commit(commit_seq);
    ASSERT_EQ(ret, 0);

    ret = storage_manager_->apply(job.get_change_info(), false);
    ASSERT_EQ(ret, 0);

    //print_raw_meta();

    if (context_->icf_options_.rate_limiter != nullptr) {
      delete context_->icf_options_.rate_limiter;
      context_->icf_options_.rate_limiter = nullptr;
    }
  }
}

 TEST_F(CompactionTest, test_massive_data) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    // level 1 has three range;

    for (int64_t i = 1; i < 1000; i *= 10) {
      write_data(i * 100, i * 800, 0, 1);
      write_data(i * 80001, i * 160000, 0, 1);
    }

    write_data(1000, 3200000, 10, 0, 10000);

    // write_data(11000, 15000, 20, 0);
    // write_data(18000, 25000, 20, 0);

    // write_data(10000, 20000, 40, 0);
    // write_data(21111, 28888, 40, 0);

    print_raw_meta();
    //run_compact();
    print_raw_meta();
  }
}

TEST_F(CompactionTest, test_delete_data) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);

    write_data(1, 3, 10, 0, 1, kTypeDeletion);
    write_data(1, 3, 0, 1, 1, kTypeDeletion);

    print_raw_meta();
    run_compact();
    print_raw_meta();
  }
}

TEST_F(CompactionTest, test_single_del) {
  TestArgs test_arg;

  init(test_arg);
  write_data(0, 10000, 0, 1, 2, kTypeValue);
  write_data(10000, 20287, 0, 1, 1, kTypeValue);
  write_data(0, 10000, 1, 0, 2, kTypeSingleDeletion);
  write_data(10000, 20287, 2, 0, 2, kTypeSingleDeletion);
  write_data(10001, 15000, 3, 0, 2, kTypeSingleDeletion);
  write_data(15001, 40000, 4, 0, 2, kTypeSingleDeletion);
  print_raw_meta();
  run_compact();
  print_raw_meta();
  write_data(20287, 50000, 0, 2, 2, kTypeValue);
  run_major_compact();
  print_raw_meta();

  IntRange r[2] = { {40001, 49635, 2 }, {49637, 49999, 2 } };
  check_result(2, r, 2);
  auto check_func = [&r](int64_t row, const Slice &key,
      const Slice &value) -> bool {
    return CompactionTest::check_key(row, key, value, r[0]);
  };
  scan_all_data(check_func);
}

TEST_F(CompactionTest, test_single_del1) {
  TestArgs test_arg;

  init(test_arg);
  write_data(0, 10000, 0, 1, 2, kTypeValue);
  write_data(10000, 20287, 0, 1, 1, kTypeValue);
  write_data(0, 10000, 1, 0, 2, kTypeSingleDeletion);
  write_data(10000, 20287, 2, 0, 2, kTypeSingleDeletion);
  write_data(10001, 15000, 3, 0, 2, kTypeSingleDeletion);
  write_data(15001, 20291, 4, 0, 2, kTypeSingleDeletion);
  print_raw_meta();
  run_compact();
  print_raw_meta();
  write_data(20287, 30000, 0, 2, 2, kTypeValue);
  run_major_compact();
  print_raw_meta();

  IntRange r[2] = { {20291, 29999, 2 }};
  check_result(2, r, 1);
  auto check_func = [&r](int64_t row, const Slice &key,
      const Slice &value) -> bool {
    return CompactionTest::check_key(row, key, value, r[0]);
  };
  scan_all_data(check_func);
}

TEST_F(CompactionTest, major_normal_range) {
  //std::vector<TestArgs> test_args = GenerateArgList();
  TestArgs test_arg;
  test_arg.compression = kNoCompression;
  test_arg.format_version = 3;

  init(test_arg);
  for (int64_t i = 100; i < 300; i += 50) {
    // start,end,seq,level
    write_data(i, i + 50, 10, 1);
    write_data(i + 25, i + 75, 0, 2);
  }
  print_raw_meta();
  run_major_compact();
  print_raw_meta();
  IntRange r[1] = { { 100, 324, 1 } };  // start,end,range_cnt
  check_result(2, r, 1);
  auto check_func = [&r](int64_t row, const Slice &key,
      const Slice &value) -> bool {
    return CompactionTest::check_key(row, key, value, r[0]);
  };
  scan_all_data(check_func);

}

TEST_F(CompactionTest, major_reuse_range) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    IntRange r[61];
    int range_cnt = 0;
    for (int64_t i = 100; i < 3000; i += 100) {
      // start,end,seq,level
      write_data(i, i + 50, 10, 1);
      r[range_cnt] = {i, i + 49, 1};  // start,end,step
      write_data(i + 50, i + 100, 0, 2);
      r[range_cnt + 1] = {i + 50, i + 99, 1};
      range_cnt += 2;
    }
    // print_raw_meta();
    run_major_compact();
    // print_raw_meta();
    check_result(2, r, range_cnt - 1);
    auto check_func = [&r](int64_t row, const Slice &key,
                           const Slice &value) -> bool {
      return CompactionTest::check_key(row, key, value, r[0]);
    };
    scan_all_data(check_func);
  }
}

TEST_F(CompactionTest, major_reuse1_range) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    IntRange r[61];
    int range_cnt = 0;
    write_data(1000, 2000, 0, 2);
    int cnt = 0;
    r[cnt] = {1000, 2000 - 1, 1};

    // -     -     -
    //  ----- ----- ----
    write_data(2000, 3000, 10, 1, 300);
    write_data(2001, 2300, 0, 2);
    write_data(2301, 2600, 0, 2);
    write_data(2601, 3000, 0, 2);
    r[++cnt] = {2000, 2000, 1};
    r[++cnt] = {2001, 2300 - 1, 1};
    r[++cnt] = {2300, 2300, 1};
    r[++cnt] = {2301, 2600 - 1, 1};

    // 3
    write_data(3001, 3400, 10, 1);
    write_data(3401, 3800, 10, 1);
    // 1
    write_data(3801, 4000, 10, 1);

    write_data(3000, 4000, 0, 2, 400);

    r[++cnt] = {2600, 3000, 1};

    r[++cnt] = {3001, 3400 - 1, 1};
    r[++cnt] = {3400, 3400, 1};
    r[++cnt] = {3401, 3800 - 1, 1};
    r[++cnt] = {3800, 3800, 1};
    r[++cnt] = {3801, 4000 - 1, 1};

    // 2
    write_data(4500, 5000, 10, 1, 250);
    write_data(4000, 4750, 10, 2);
    write_data(4751, 5000, 10, 2);
    r[++cnt] = {4000, 4750, 1};
    r[++cnt] = {4751, 5000 - 1, 1};

    write_data(5001, 6000, 10, 1, 3);
    write_data(5400, 6000, 0, 2, 3);
    r[++cnt] = {5001, 6000 - 3, 3};
    write_data(6000, 7000, 0, 2, 10);
    r[++cnt] = {6000, 6990, 10};

    print_raw_meta();
    run_major_compact();
    print_raw_meta();
    check_result(2, r, cnt + 1);
    //    auto check_func = [&r](int64_t row, const Slice &key,
    //                           const Slice &value) -> bool {
    //      return CompactionTest::check_key(row, key, value, r[0]);
    //    };
    //    scan_all_data(check_func);
  }
}

TEST_F(CompactionTest, major_reuse_block_with_schema_change) {
  TestArgs one_arg;
  init(one_arg);

  IntRange r[61];
  int cnt = 0;
  r[cnt++] = {1000, 5099, 1};

  write_data_with_schema(2000, 5000, 10, 1/*level*/, 1/*schema*/, 1/*step*/);
  write_data_with_schema(1000, 2005, 0, 2/*level*/, 2/*schema*/, 1/*step*/);
  write_data_with_schema(4900, 5100, 0, 2/*level*/, 2/*schema*/, 1/*step*/);

  print_raw_meta();
  run_major_compact();
  print_raw_meta();
  check_result(2, r, cnt);

  //    auto check_func = [&r](int64_t row, const Slice &key,
  //                           const Slice &value) -> bool {
  //      return CompactionTest::check_key(row, key, value, r[0]);
  //    };
  //    scan_all_data(check_func);
}

TEST_F(CompactionTest, major_massive_data) {
 std::vector<TestArgs> test_args = GenerateArgList();
 for (auto &test_arg : test_args) {
   init(test_arg);

   for (int64_t i = 1; i < 1000; i *= 10) {
     write_data(i * 100, i * 800, 0, 2);
     write_data(i * 10000, i * 80000, 0, 2);
   }

   write_data(1000, 1600000, 1, 1, 10000);

   print_raw_meta();
   run_major_compact(5);
   print_raw_meta();
 }
}

TEST_F(CompactionTest, major_split_task)
{
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    IntRange r[61];
    int range_cnt = 0;
    // start,end,seq,level
    write_data(1000, 2000, 10, 1);
    write_data(2100, 3000, 10, 1);
    write_data(3100, 4000, 10, 1);
    write_data(4100, 5000, 10, 1);
    write_data(5100, 6000, 10, 1);

    write_data(1500, 2500, 0, 2);
    write_data(2500, 3500, 0, 2);
    write_data(3500, 4500, 0, 2);
    write_data(4500, 5500, 0, 2);
    int cnt = 0;
    r[cnt] = {1000, 2500 - 1, 1};
    r[++cnt] = {2500, 3500 - 1, 1};
    r[++cnt] = {3500, 4500 - 1, 1};
    r[++cnt] = {4500, 6000 - 1, 1};
    print_raw_meta();
    run_major_compact(2);
    print_raw_meta();
    check_result(2, r, cnt + 1);
  }
}

TEST_F(CompactionTest, major_split_task1)
{
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    IntRange r[61];
    int range_cnt = 0;
    // start,end,seq,level
    write_data(100, 2000, 10, 1);
    write_data(2100, 3000, 10, 1);

    write_data(0, 250, 0, 2); // 1
    write_data(250, 550, 0, 2); // 2
    write_data(550, 750, 0, 2); // 3
    write_data(750, 1000, 0, 2); // 4
    write_data(1050, 1550, 0, 2); // 5
    write_data(1550, 1750, 0, 2); // 6
    write_data(1750, 2550, 0, 2); // 7
    write_data(2550, 2750, 0, 2); // 8
    write_data(2750, 3550, 0, 2); // 9

    int cnt = 0;
    r[cnt] = {0, 750 - 1, 1};
    r[++cnt] = {750, 1750 - 1, 1};
    r[++cnt] = {1750, 2750 - 1, 1};
    r[++cnt] = {2750, 3550 - 1, 1};
    print_raw_meta();
    run_major_compact(3);
    print_raw_meta();
    check_result(2, r, cnt + 1);
  }
}

TEST_F(CompactionTest, major_more_range) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    IntRange r[61];
    int range_cnt = 0;
    // start,end,seq,level
    write_data(2100, 3000, 0, 1);
    write_data(1000, 2000, 0, 1);
    write_data(5100, 6000, 0, 1);
    write_data(4100, 5000, 0, 1);
    write_data(3100, 4000, 0, 1);
    int cnt = 0;
    r[cnt] = {1000, 2000 - 1, 1};
    r[++cnt] = {2100, 3000 - 1, 1};
    r[++cnt] = {3100, 4000 - 1, 1};
    r[++cnt] = {4100, 5000 - 1, 1};
    r[++cnt] = {5100, 6000 - 1, 1};
    print_raw_meta();
    run_major_compact(3);
    print_raw_meta();
    check_result(2, r, cnt + 1);
  }
}

TEST_F(CompactionTest, major_repeat_test) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    IntRange r[61];
    int range_cnt = 0;
    // start,end,level, seq_start,seq_end
    write_equal_data(1, 5, 1, 1000, 3000);
    write_equal_data(0, 2, 2, 4000, 5000);
    write_equal_data(4, 5, 2, 1, 500);
    //    (1...2...3...)  l1
    //(0..1..)     (3..4..) l2
    int cnt = 0;
    r[cnt] = {0, 5, 1};
    print_raw_meta();
    run_major_compact();
    print_raw_meta();
    check_result(2, r, cnt + 1);
  }
}

TEST_F(CompactionTest, major_repeat1_test) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    IntRange r[61];
    int range_cnt = 0;
    // start,end,level, seq_start,seq_end
    write_equal_data(1, 10, 1, 1000, 1000);
    write_equal_data(10, 20, 1, 100, 100);
    write_equal_data(0, 8, 2, 800, 800);
    write_equal_data(30, 40, 2, 1, 1);
    //    (1...2...3...)  l1
    //(0..1..)     (3..4..) l2
    int cnt = 0;
    r[cnt] = {0, 20, 1};
    r[++cnt] = {30, 40, 1};
    print_raw_meta();
    run_major_compact();
    print_raw_meta();
    check_result(2, r, cnt + 1);
  }
}

//TEST_F(CompactionTest, major_single_test) {
//  std::vector<TestArgs> test_args = GenerateArgList();
//  for (auto &test_arg : test_args) {
//    init(test_arg);
//    IntRange r[61];
//    int range_cnt = 0;
//    // start,end,seq,level
//    write_data(1000, 2000, 0, 1);
//    write_data(2100, 3000, 0, 1);
//    write_data(3100, 4000, 0, 1);
//    write_data(4000, 5000 + 1, 0, 1);
//    write_data(5000, 6000 + 1, 0, 1, kTypeDeletion);
//    write_data(6000, 7000, 0, 1, 1, kTypeDeletion);
//
//    write_data(4500, 4800, 0, 2);
//    int cnt = 0;
//    r[cnt] = {1000, 2000 - 1, 1};
//    r[++cnt] = {2100, 3000 - 1, 1};
//    r[++cnt] = {3100, 4000 - 1, 1};
//    r[++cnt] = {4100, 5000 - 1, 1};
//    r[++cnt] = {5100, 6000 - 1, 1};
//    print_raw_meta();
//    run_major_compact(3);
//    print_raw_meta();
//    check_result(2, r, cnt + 1);
//  }
//}

TEST_F(CompactionTest, major_snapshot_test) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    IntRange r[61];
    int range_cnt = 0;
    write_data(102, 100 + 3, 1, 2);
    write_data(100, 100 + 1, 40, 1, 1, kTypeDeletion);
    write_data(101, 100 + 2, 0, 2);
    write_data(101, 100 + 2, 5, 1, 1);
    write_data(101, 100 + 2, 10, 1);
    write_data(101, 100 + 2, 15, 1, 1, kTypeDeletion);
    int cnt = 0;
    r[cnt] = {102, 102, 1};
    print_raw_meta();
    run_major_compact();
    print_raw_meta();
    check_result(2, r, cnt + 1);
  }
}

TEST_F(CompactionTest, major_snapshot_test1) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    IntRange r[61];
    int range_cnt = 0;
    write_equal_data(1, 2, 1, 1, 50);
    write_equal_data(1, 2, 2, 51, 100);
    int cnt = 0;
    r[cnt] = {1, 2, 1};
    print_raw_meta();
    run_major_compact();
    print_raw_meta();
    check_result(2, r, cnt + 1);
  }
}

TEST_F(CompactionTest, major_single_deletion_test1) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    IntRange r[61];
    int range_cnt = 0;
    write_equal_data(1, 10, 1, 20, 20, 1, kTypeSingleDeletion);
    write_equal_data(5, 15, 2, 15, 15);
    int cnt = 0;
    r[cnt] = {11, 15, 1};
    print_raw_meta();
    run_major_compact();
    print_raw_meta();
    check_result(2, r, cnt + 1);
  }
}

void init_logger() {
  std::string log_path = test::TmpDir() + "/compaction_test.log";

  xengine::logger::Logger::get_log().init(log_path.c_str(), xengine::logger::WARN_LEVEL);
}

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  init_logger();
  return RUN_ALL_TESTS();
}
