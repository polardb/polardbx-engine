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
#include "minor_compaction.h"
#include "compaction_job.h"
#include "db/builder.h"
#include "db/column_family.h"
#include "db/db_impl.h"
#include "db/db_iter.h"
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "logger/logger.h"
#include "options/options_helper.h"
#include "storage/extent_space_manager.h"
#include "storage/io_extent.h"
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
#include "util/random.h"
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

DEFINE_int32(compaction_mode, 0, "0 cpu, 1 fpga, 2 check");

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

  std::string db_path_ = test::TmpDir() + "/minor_compaction_test";
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

class MinorCompactionTest : public testing::Test {
 public:
  MinorCompactionTest()
      : context_(nullptr),
        // cache_(NewLRUCache(50000, 16)),
        // write_buffer_manager_(context_->db_options_.db_write_buffer_size),
        // env_(context_->options_->env),
        // dbname_(context_->options_->db_paths[0].path),
        space_manager_(nullptr),
        internal_comparator_(BytewiseComparator()),
        next_file_number_(2) {}

  void reset() {
    // Env *env_;
    names_.clear();
    keys_.clear();
    storage_manager_.reset();
    //space_manager_.reset();
    if (space_manager_ != nullptr) {
      delete space_manager_;
    }
    space_manager_ = nullptr;
    descriptor_log_.reset();
    db_dir.reset();
    props_.clear();
    // std::string compression_dict_;
    mini_tables_.metas.clear();
    mini_tables_.props.clear();
    extent_builder_.reset();
    // internal_comparator_;
    shutting_down_.store(false);
    table_cache_.reset();
    cache_.reset();
    delete context_;
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

    env_->CreateDir(dbname_);
    env_->NewDirectory(dbname_, &db_dir);
    Status s;

    //space_manager_.reset(
    //    new ExtentSpaceManager(context_->db_options_, next_file_number_));
    space_manager_ = new ExtentSpaceManager(context_->db_options_, next_file_number_);
    table_cache_.reset(new TableCache(context_->icf_options_,
                                      context_->env_options_, cache_.get(),
                                      //space_manager_.get()));
                                      space_manager_));

    uint64_t file_number = 1;
    std::string manifest_filename =
        util::DescriptorFileName(dbname_, file_number);
    std::unique_ptr<WritableFile> descriptor_file;
    EnvOptions opt_env_opts =
        env_->OptimizeForManifestWrite(context_->env_options_);
    s = NewWritableFile(env_, manifest_filename, &descriptor_file,
                        opt_env_opts);
    if (s.ok()) {
      descriptor_file->SetPreallocationBlockSize(
          context_->db_options_.manifest_preallocation_size);

      unique_ptr<util::ConcurrentDirectFileWriter> file_writer(
          new util::ConcurrentDirectFileWriter(std::move(descriptor_file),
                                               opt_env_opts));
      s = file_writer->init_multi_buffer();
      if (s.ok()) {
        descriptor_log_.reset(
            new db::log::Writer(std::move(file_writer), 0, false));
      }
    }

    //TODO:yuanfeng
    /*
    storage_manager_.reset(new StorageManager(
        *env_, dbname_, context_->db_options_,
        ColumnFamilyOptions(*context_->options_), context_->env_options_,
        db_dir.get(), cache_.get(), nullptr));
    */

    //s = storage_manager_->init(space_manager_.get(), 0);
    s = storage_manager_->init(space_manager_, cache_.get());
    assert(s.ok());
    //s = storage_manager_->set_descriptor_log(descriptor_log_.get());
    //assert(s.ok());
    //TODO get device id and cpu thread and fpga thread
    compaction_scheduler_.reset(
         new CompactionScheduler((CompactionMode)FLAGS_compaction_mode, 
                                 0, 
                                 4, 
                                 4, 
                                 context_->icf_options_.statistics));
    compaction_scheduler_->init();
  }

  void shutdown() { Close(); }
  ~MinorCompactionTest() {
    Close();
    Destroy();
  };

  void Close() { names_.clear(); }

  void Destroy() {
    // ASSERT_OK(DestroyDB(dbname_, *context_->options_));
  }

  void build_compact_context(CompactionContext *comp) {
    /*
    shutting_down_.store(false);
    comp->shutting_down_ = &shutting_down_;
    comp->cf_options_ = &context_->icf_options_;
    comp->mutable_cf_options_ = &context_->mutable_cf_options_;
    comp->env_options_ = &context_->env_options_;
    //comp->meta_comparator_ = MetaKeyComparator(BytewiseComparator());
    comp->data_comparator_ = BytewiseComparator();
    comp->internal_comparator_ = &internal_comparator_;
    comp->earliest_write_conflict_snapshot_ = 0;
    //TODO get from 
    comp->compaction_type_ = 1; // MinorCompaction
    comp->compaction_scheduler_ = this->compaction_scheduler_.get();
    */
  }

  void print_raw_meta(const db::MemTable *memtable) {
    /*
    ReadOptions read_options;
    Arena arena_;
    table::InternalIterator *memtable_iterator =
        memtable->NewIterator(read_options, &arena_);
    if (nullptr == memtable_iterator) {
      return;
    }
    int64_t index = 0;
    InternalIterator &iter = *memtable_iterator;
    memtable_iterator->SeekToFirst();
    while (memtable_iterator->Valid()) {
      MetaKey key;
      MetaValue value;
      int64_t pos = 0;
      key.deserialize(iter.key().data(), iter.key().size(), pos);
      pos = 0;
      value.deserialize(iter.value().data(), iter.value().size(), pos);

      ParsedInternalKey pik("", 0, kTypeValue);
      ParsedInternalKey mik("", 0, kTypeValue);
      ParseInternalKey(key.largest_key_, &mik);
      ParseInternalKey(mik.user_key, &pik);
      Slice uk = pik.user_key;
      fprintf(stderr,
              "key[%ld]:cf[%d], level[%d], seq[%ld], end "
              "key:[%.*s][%ld][%d]_[%ld][%d]\n",
              index, key.column_family_id_, key.level_, key.sequence_number_,
              (int32_t)uk.size(), uk.data(), pik.sequence, pik.type,
              mik.sequence, mik.type);

      if (mik.type == db::kTypeValue) {
        ParseInternalKey(value.smallest_key_, &pik);
        uk = pik.user_key;
        fprintf(stderr,
                "value[%ld]: start key:[%.*s][%ld][%d], extent[%d,%d]\n", index,
                (int32_t)uk.size(), uk.data(), pik.sequence, pik.type,
                value.extent_id_.offset, value.extent_id_.file_number);
      }
      ++index;
      memtable_iterator->Next();
    }
    fprintf(stderr, "-----------------------------\n");
    */
  }

  void print_level0() {
    /*
    int ret = 0;
    Arena arena;
    storage::Level0Version* level0 = storage_manager_->get_current_version()->get_level0();
    if (level0 == nullptr)  return;

    int64_t index = 0;
    fprintf(stderr, "----------Level 0-------------------\n");
    for (int32_t layer = 0; layer < level0->size(); layer++) {
      Level0LayerIterator *level0_iter =  (Level0LayerIterator *)
        level0->layers()[layer]->create_iterator(ReadOptions(), &arena,
            0cfd id );
      if (nullptr == level0_iter) return;

      for (level0_iter->SeekToFirst(); level0_iter->Valid(); level0_iter->Next()) {
        MetaKey key;
        MetaValue value;
        int64_t pos = 0;
        key.deserialize(level0_iter->key().data(),
            level0_iter->key().size(), pos);
        pos = 0;
        value.deserialize(level0_iter->value().data(),
            level0_iter->value().size(), pos);

        ParsedInternalKey pik("", 0, kTypeValue);
        ParsedInternalKey mik("", 0, kTypeValue);
        ParseInternalKey(key.largest_key_, &mik);
        ParseInternalKey(mik.user_key, &pik);
        Slice uk = pik.user_key;
        fprintf(stderr,
            "key[%ld]:cf[%d], level[%d], seq[%ld], end "
            "key:[%.*s][%ld][%d]\n",
            index, key.column_family_id_, key.level_, key.sequence_number_,
            //(int32_t)uk.size(), uk.data(), pik.sequence, pik.type,
            (int32_t)mik.user_key.size(), mik.user_key.data(), mik.sequence, mik.type);

        if (mik.type == db::kTypeValue) {
          ParseInternalKey(value.smallest_key_, &pik);
          uk = pik.user_key;
          fprintf(stderr,
              "value[%ld]: start key:[%.*s][%ld][%d], extent[%d,%d]\n", index,
              (int32_t)uk.size(), uk.data(), pik.sequence, pik.type,
              value.extent_id_.offset, value.extent_id_.file_number);
        }
        ++index;
      }
    }
    fprintf(stderr, "-----------------------------\n");
    */
  }

  void print_raw_meta() {
    // current MetaVersion
    /*
    print_level0();
    fprintf(stderr, "-----------Level 1------------------\n");
    print_raw_meta(storage_manager_->get_mem_table_level1());
    fprintf(stderr, "-----------Level 2------------------\n");
    print_raw_meta(storage_manager_->get_mem_table_level2());
    */
  }

  /*
  int parse_meta(const db::Iterator &iter, MetaKey &key, MetaValue &value) {
    int ret = 0;
    int64_t pos = 0;
    FAIL_RETURN(key.deserialize(iter.key().data(), iter.key().size(), pos));
    pos = 0;
    FAIL_RETURN(
        value.deserialize(iter.value().data(), iter.value().size(), pos));
    return Status::kOk;
  }
  */

  struct IntRange {
    int64_t start;
    int64_t end;
    int64_t step;
  };
  void do_check(db::Iterator *iterator, const int64_t level, const IntRange *range,
                const int64_t size, int64_t &index) {
    /*
    if (0 == level) {
      iterator->SeekToFirst();
    } else {
      MetaKey l0_start_key(cf_desc_.column_family_id_, 0, kMaxSequenceNumber,
                         Slice());
      const int64_t max_meta_key_size = 64;
      char sk_buf[max_meta_key_size];
      int64_t pos = 0;
      ASSERT_EQ(0, l0_start_key.serialize(sk_buf, max_meta_key_size, pos));
      iterator->Seek(Slice(sk_buf, pos));
    }

    MetaKey current_key;
    MetaValue current_value;
    const int64_t row_size = 100;
    char buf[row_size];

    while (iterator->Valid() && index < size) {
      //ASSERT_EQ(0, parse_meta(*iterator, current_key, current_value));
      snprintf(buf, row_size, "%010ld", range[index].end);
      ASSERT_EQ(current_key.column_family_id_, 0);
      ASSERT_EQ(current_key.level_, level);
      ASSERT_EQ(0, memcmp(current_key.largest_key_.data(), buf,
                          current_key.largest_key_.size() - 8));
      snprintf(buf, row_size, "%010ld", range[index].start);
      ASSERT_EQ(0, memcmp(current_value.smallest_key_.data(), buf,
                          current_value.smallest_key_.size() - 8));
      iterator->Next();
      ++index;
    }

    */
  }
  void check_result(const int64_t level, const IntRange *range,
                    const int64_t size) {
    /*
    Arena arena_;
    ReadOptions read_options;
    InternalIterator *memtable_iterator = nullptr;
    db::Iterator *iterator = nullptr;
    int64_t index = 0;
    if (0 == level) {
      int32_t layers = storage_manager_->get_current_version()->get_level0()->size();
      for (int32_t i = 0; i < layers; i++) {
        iterator = 
        storage_manager_->get_current_version()->get_level0()->layers()[layers - i - 1]->create_iterator(
                                                        read_options, &arena_, 0);
        do_check(iterator, level, range, size, index);
      } 
    } else {
      if (1 == level) {
        memtable_iterator = storage_manager_->get_mem_table_level1()->NewIterator(
            read_options, &arena_);
      } else {
        memtable_iterator =
          storage_manager_->get_mem_table_level2()->NewIterator(read_options, &arena_);
      }
      ASSERT_TRUE(nullptr != memtable_iterator);

      CompactionContext ct;
      build_compact_context(&ct);
      // TODO use arena allocate DbIterator's memory.
      iterator = NewDBIterator(
        ct.cf_options_->env, read_options, *ct.cf_options_, ct.meta_comparator_,
        memtable_iterator,
        storage_manager_->get_current_version()->GetSequenceNumber(), true,
        kMaxSequenceNumber, kMaxSequenceNumber);
      do_check(iterator, level, range, size, index);
    }
 
    ASSERT_EQ(index, size);
    */
  }

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
    size_t col_lev_seq_size = sizeof(int32_t) + sizeof(int32_t) + sizeof(int64_t);
    assert(meta_key.size() >= col_lev_seq_size + 8);
    const int64_t row_size = 100;
    char buf[row_size];
    snprintf(buf, row_size, "%010ld", num);
    return memcmp(meta_key.data() + col_lev_seq_size, buf,
        meta_key.size() - col_lev_seq_size - 8);
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
    /*
    Arena arena;
    MergeIteratorBuilder iter_builder(&internal_comparator_, &arena, false);
    RangeDelAggregator range_del_agg(InternalKeyComparator(BytewiseComparator()), 
                                     kMaxSequenceNumber, true);
    ReadOptions read_options;
    storage_manager_->add_iterators(table_cache_.get(), nullptr, 
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
        fprintf(stderr, "check error, row(%ld), key(%s), value(%s)\n", row,
                util::to_cstring(key), util::to_cstring(value));
      }
      ASSERT_TRUE(ret);
      ++row;
      iterator->Next();
    }
    */
  }

  int build_update_batch(int32_t cfd_id, int32_t level, int64_t sequence,
                         const Slice &largest_key, const Slice &smallest_key,
                         const ExtentId &extent_id,
                         WriteBatch &batch) {
    /*
    MetaKey key =
        MetaKey(cfd_id, level, level == 0 ? sequence : 0, largest_key);
    MetaValue value = MetaValue(smallest_key, extent_id);
    MetaEntry meta = MetaEntry(key, value);

    int64_t size = meta.get_serialize_size();
    int64_t pos = 0;
    std::unique_ptr<char> buf(new char[size]);
    int ret = meta.serialize(buf.get(), size, pos);
    if (ret) return ret;
    int64_t key_size = key.get_serialize_size();
    batch.Put(Slice(buf.get(), key_size),
              Slice(buf.get() + key_size, size - key_size));
    */
    return Status::kOk;
  }

  int build_delete_batch(int32_t cfd_id, int32_t level, int64_t sequence,
                         const Slice &largest_key, WriteBatch &batch) {
    /*
    MetaKey key = MetaKey(cfd_id, level, sequence, largest_key);
    int64_t size = key.get_serialize_size();
    int64_t pos = 0;
    std::unique_ptr<char> buf(new char[size]);
    int ret = key.serialize(buf.get(), size, pos);
    if (ret) return ret;
    batch.Delete(Slice(buf.get(), pos));
    */
    return Status::kOk;
  }

  int meta_insert(int32_t cfd_id, int32_t level, int64_t sequence,
                  const Slice &largest_key, const Slice &smallest_key,
                  const ExtentId &extent_id) {
    /*
    WriteBatch batch;
    int ret = build_update_batch(cfd_id, level, sequence, largest_key,
                                 smallest_key, extent_id, batch);

    ExtentMeta extent_meta;
    autovector<ExtentMeta> extent_metas;
    extent_meta.extent_id_ = extent_id;
    extent_meta.smallest_key_ = InternalKey(smallest_key, 0, db::kTypeValue);
    extent_meta.largest_key_ = InternalKey(largest_key, 0, db::kTypeValue);
    extent_metas.emplace_back(extent_meta);

    ChangeInfo info;
    info.batch_ = batch;
    info.extent_meta_ = extent_metas;
    ret = storage_manager_->apply(info, false);

    return ret;
    */
    return Status::kOk;
  }

  int meta_write(const int64_t level, const MiniTables &mini_tables) {
    /*
    ChangeInfo info;
    info.add(cf_desc_.column_family_id_, level, mini_tables);
    int ret = storage_manager_->apply(info, false);
    return ret;
    */
    return Status::kOk;
  }

  void open_for_write(int level = 1) {
    //mini_tables_.space_manager = space_manager_.get();
    mini_tables_.space_manager = space_manager_;
    extent_builder_.reset(NewTableBuilder(
        context_->icf_options_, internal_comparator_, &props_,
        cf_desc_.column_family_id_, cf_desc_.column_family_name_, &mini_tables_,
        GetCompressionType(context_->icf_options_,
                           context_->mutable_cf_options_,
                           level/*level*/ ) /* compression type */,
        context_->icf_options_.compression_opts, level /* level */,
        &compression_dict_, true));
  }

  void append(const int64_t key_start, const int64_t key_end,
              const int64_t sequence, const int64_t step = 1,
              const ValueType value_type = kTypeValue,
              const int64_t row_size = 128,
              bool finish = false) {
    ASSERT_TRUE(nullptr != extent_builder_.get());
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

  void close(const int64_t level, bool finish = true) {
    if (finish) {
      extent_builder_->Finish();
      ASSERT_TRUE(extent_builder_->status().ok());
    }
    meta_write(level, mini_tables_);
    mini_tables_.metas.clear();
    mini_tables_.props.clear();
  }

 void open_write(const int64_t key_start, const int64_t key_end,
                  const int64_t sequence, const int64_t level,
                  const int64_t step = 1) {
    open_for_write();
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

  void write_large_data(const int64_t key_start, const int64_t key_end,
                  const int64_t sequence, const int64_t level,
                  const int64_t step = 1, const ValueType value_type = kTypeValue) {
    open_for_write(level);
    ASSERT_TRUE(nullptr != extent_builder_.get());
    const int64_t key_size = 20;
    const int64_t row_size = 200 * 1024;
    char *buf = new char[row_size];
    memset(buf, 0, row_size);
    Random rand{(uint32_t)(env_->NowMicros())};
    for (int64_t key = key_start; key < key_end; key += step) {
      snprintf(buf, key_size, "%010ld", key);
      InternalKey ikey(Slice(buf, strlen(buf)), sequence, value_type);
      uint64_t value_size = rand.Uniform(row_size / 2) + row_size / 2;
      extent_builder_->Add(ikey.Encode(), Slice(buf, value_size));
      ASSERT_TRUE(extent_builder_->status().ok());
    }

    close(level);
  }

  void write_equal_data(const int64_t start_key, const int64_t end_key,
                        const int64_t level,
                        const int64_t repeat_start, const int64_t repeat_end,
                        const int64_t step = 1,
                        const ValueType vtype = kTypeValue,
                        const int64_t row_size = 128) {
    open_for_write();
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

  void run_compact() {
    // util::Arena arena;
    /*
    storage::CompactionJob job;
    CompactionContext ct;
    build_compact_context(&ct);
    //ct.space_manager_ = space_manager_.get();
    ct.space_manager_ = space_manager_;
    ct.table_cache_ = table_cache_.get();
    ct.output_level_ = 1;
    */
    //TODO:yuanfeng
    /*
    int ret =
        job.init(ct, cf_desc_, storage_manager_.get(),
                 storage_manager_->get_current_version());
    */
    int ret = Status::kOk;
    //storage_manager_->get_current_version()->get_level0()->ref();
    ASSERT_EQ(ret, 0);
    ret = job.prepare();
    ASSERT_EQ(ret, 0);
    ret = job.run();
    ASSERT_EQ(ret, 0);
    storage::MinorCompaction *compaction = nullptr;
    while (nullptr != (compaction = job.get_next_minor_task())) {
      job.append_change_info(compaction->get_change_info());
    }
    ret = storage_manager_->apply(job.get_change_info(), false);
    ASSERT_EQ(ret, 0);
    //ret = job.install();
    //ASSERT_EQ(ret, 0);
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
  unique_ptr<TableCache> table_cache_;
  Env *env_;
  std::vector<std::string> names_;
  std::set<std::string> keys_;
  std::string dbname_;
  // DB *db_ = nullptr;
  unique_ptr<StorageManager> storage_manager_;
  //unique_ptr<ExtentSpaceManager> space_manager_;
  ExtentSpaceManager* space_manager_;
  std::unique_ptr<db::log::Writer> descriptor_log_;
  std::unique_ptr<Directory> db_dir;
  std::vector<std::unique_ptr<db::IntTblPropCollectorFactory>> props_;
  ColumnFamilyDesc cf_desc_;
  std::string compression_dict_;
  MiniTables mini_tables_;
  std::unique_ptr<table::TableBuilder> extent_builder_;
  InternalKeyComparator internal_comparator_;
  db::FileNumber next_file_number_;
  std::atomic<bool> shutting_down_;
  std::unique_ptr<CompactionScheduler> compaction_scheduler_;
};

TEST_F(MinorCompactionTest, test_normal_compact) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    write_data(1000, 4000, 10, 1);
    //write_data(1000, 8000, 10, 1);
    write_data(1500, 2000, 20, 0);
    print_raw_meta();
    run_compact();
    print_raw_meta();
    IntRange r[1] = {{1000, 3999, 1}};
    check_result(1, r, 1);
    auto check_func = [&r](int64_t row, const Slice &key,
                           const Slice &value) -> bool {
      return MinorCompactionTest::check_key(row, key, value, r[0]);
    };
    scan_all_data(check_func);
  }
}

TEST_F(MinorCompactionTest, test_down_level0) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    write_data(1000, 3000, 10, 1);
    write_data(1500, 2000, 20, 0);
    fprintf(stderr, "Before compaction\n");
    print_raw_meta();
    run_compact();
    fprintf(stderr, "After compaction\n");
    print_raw_meta();
    IntRange r1[1] = {{1000, 2999, 1}};
    check_result(1, r1, 1);

    auto check_func = [&r1](int64_t row, const Slice &key,
                            const Slice &value) -> bool {
      return MinorCompactionTest::check_key(row, key, value, r1[0]);
    };

    scan_all_data(check_func);
    write_data(5000, 6000, 30, 0);
    // print_raw_meta();
    run_compact();
    // print_raw_meta();
    IntRange r2[2] = {{1000, 2999, 1}, {5000, 5999, 1}};
    check_result(1, r2, 2);
    auto check_func2 = [&r2](int64_t row, const Slice &key,
                             const Slice &value) -> bool {
      return MinorCompactionTest::check_key_segment(row, key, value, r2, 2);
    };
    scan_all_data(check_func2);
  }
}
#if 1

TEST_F(MinorCompactionTest, test_same_key_compact) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);

    open_for_write();
    append(1000, 1001, 50, 1);
    append(1000, 1001, 30, 1);
    append(1001, 1002, 80, 1);
    append(1001, 1002, 70, 1);
    append(1001, 1002, 60, 1);
    close(0, true);

    open_for_write();
    append(1000, 1001, 40, 1);
    append(1000, 1001, 20, 1);
    append(1001, 1002, 90, 1);
    append(1001, 1002, 50, 1);
    close(0, true);

    print_raw_meta();
    run_compact();
    print_raw_meta();
    IntRange r[1] = {{1000, 1001, 1}};
    auto check_func2 = [&r](int64_t row, const Slice &key,
                            const Slice &value) -> bool {
      return MinorCompactionTest::check_key_segment(row, key, value, r, 2);
    };
    check_result(1, r, 1);
    scan_all_data(check_func2);
  }
}

TEST_F(MinorCompactionTest, test_same_key2_compact) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);

    open_for_write();
    append(1000, 1001, 50, 1);
    append(1000, 1001, 40, 1);
    append(1000, 1001, 30, 1);
    close(0, true);

    open_for_write();
    append(1000, 1001, 100, 1);
    append(1000, 1001, 90, 1);
    append(1000, 1001, 80, 1);
    close(0, true);

    open_for_write();
    append(1000, 1001, 150, 1);
    append(1000, 1001, 140, 1);
    append(1000, 1001, 130, 1);
    append(1000, 1001, 120, 1);
    append(1001, 1002, 200, 1);
    append(1001, 1002, 190, 1);
    close(0, true);

    print_raw_meta();
    run_compact();
    print_raw_meta();
    IntRange r[1] = {{1000, 1001, 1}};
    auto check_func2 = [&r](int64_t row, const Slice &key,
                            const Slice &value) -> bool {
      return MinorCompactionTest::check_key_segment(row, key, value, r, 2);
    };
    check_result(1, r, 1);
    scan_all_data(check_func2);
  }
}

TEST_F(MinorCompactionTest, test_multi_way_compact) {
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
      return MinorCompactionTest::check_key_segment(row, key, value, r, 2);
    };
    check_result(1, r, 2);
    scan_all_data(check_func2);
  }
}

TEST_F(MinorCompactionTest, test_multi_way_multi_range_compact) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    // level 1 has three range;
    write_data(10000, 20000, 10, 1, 100);
    write_data(21111, 28888, 10, 1, 100);
    write_data(30000, 38888, 10, 1, 100);

    open_write(11000, 15000, 20, 0, 100);
    open_write(18000, 25000, 20, 0, 100);
    close(0, false);

    open_write(16000, 25000, 30, 0, 100);
    open_write(30000, 50000, 30, 0, 100);
    open_write(51000, 59000, 30, 0, 100);
    close(0, false);

    open_write(19000, 21000, 40, 0, 100);
    open_write(80000, 90000, 40, 0, 100);
    close(0, false);

    print_raw_meta();
    run_compact();
    print_raw_meta();

    IntRange r[3] = {
        {10000, 49900, 100}, {51000, 58900, 100}, {80000, 89900, 100}};
    auto check_func3 = [&r](int64_t row, const Slice &key,
                            const Slice &value) -> bool {
      return MinorCompactionTest::check_key_segment(row, key, value, r, 3);
    };
    UNUSED(check_func3);
    check_result(1, r, 3);
    /*
       auto print_f = [](int64_t row, const Slice& key, const Slice& value) ->
       bool {
       fprintf(stderr, "row[%ld]=[%.*s]\n", row, (int)key.size(), key.data());
       return true;
       };
       scan_all_data(print_f);*/
  }
}

TEST_F(MinorCompactionTest, test_intersect_key_compact) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    // level 1 has three range;
    write_data(2887, 5500, 10, 1);
    write_data(5799, 7900, 10, 1);

    open_write(1100, 1500, 20, 0);
    open_write(1999, 2500, 20, 0);
    close(0, false);

    open_write(1000, 2000, 40, 0);
    open_write(2111, 2888, 40, 0);
    close(0, false);

    print_raw_meta();
    run_compact();
    print_raw_meta();
    IntRange r[2] = {{1000, 5499}, {5799, 7899}};
    check_result(1, r, 2);
  }
}

TEST_F(MinorCompactionTest, test_intersect_key_compact2) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    // level 1 has three range;
    write_data(2887, 5500, 50, 1);
    write_data(5511, 7900, 50, 1);

    open_write(1100, 1200, 20, 0);
    open_write(1199, 2500, 10, 0);
    close(0, false);

    open_write(1000, 1155, 40, 0);
    open_write(1255, 2200, 40, 0);
    close(0, false);

    print_raw_meta();
    run_compact();
    print_raw_meta();
    IntRange r[3] = {{1000, 2499, 1}, {2887, 5499, 1}, {5511, 7899, 1}};
    check_result(1, r, 3);
    auto check_func3 = [&r](int64_t row, const Slice &key,
                            const Slice &value) -> bool {
      return MinorCompactionTest::check_key_segment(row, key, value, r, 3);
    };
    scan_all_data(check_func3);
  }
}

TEST_F(MinorCompactionTest, test_add_new_block) {
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
      return MinorCompactionTest::check_key(row, key, value, r[0]);
    };
    scan_all_data(check_func);
  }
}

TEST_F(MinorCompactionTest, test_add_new_way) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    open_write(1000, 1921, 10, 0);
    open_write(2100, 2200, 10, 0);
    close(0, false);
    write_data(1200, 2121, 20, 0);

    write_data(1924, 1935, 0, 1);

    print_raw_meta();
    run_compact();
    print_raw_meta();
  }
}

TEST_F(MinorCompactionTest, test_pending_data_block) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);

    open_for_write();
    append(1000, 1461, 10, 1);
    append(10000, 20000, 10, 100);
    close(0);

    open_write(1002, 2200, 20, 0);
    open_write(2500, 3000, 20, 0);
    open_write(3000, 4000, 20, 0);
    close(0, false);

    write_data(1005, 18000, 0, 1, 100);

    print_raw_meta();
    run_compact();
    print_raw_meta();
  }
}

TEST_F(MinorCompactionTest, test_successive_compact) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);

    write_data(1, 40000, 0, 1);
    //write_data(1000, 1600000, 10, 0, 10000);

    //print_raw_meta();
    //run_compact();
    //print_raw_meta();

    //print_raw_meta();
    //IntRange r[3] = {{1, 14607, 1},
    //                 {14608, 29212, 1},
    //                 {29213, 1591000, 1}};
    //check_result(1, r, 3);

    for (int i = 2; i < 5; i++) {
      write_data(i * 1000, 1600000, i + 10, 0, 10000);
      print_raw_meta();
      run_compact();
      print_raw_meta();
      //TODO(anan) since the different block size will change extent's key range
      // we comment here after port the compaction branch
      //IntRange r[3] = {{1, 14609, 1},
      //  {14610, 29214, 1},
      //  {29215, i * 1000 + 1590000, 1}};
      //check_result(1, r, 3);
    }
    break;
  }
}

TEST_F(MinorCompactionTest, test_sparse_range) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);

    write_data(1, 40000, 0, 1);
    write_data(1000, 1600000, 10, 0, 10000);

    print_raw_meta();
    run_compact();
    print_raw_meta();

    if (test_arg.compression == kNoCompression) {
      //TODO(anan) after merge compaction branch
      //IntRange r[3] = {{1, 14607, 1},
      //  {14608, 29212, 1},
      //  {29213, 1591000, 1}};
     // IntRange r[3] = {{1, 14603, 1},
     //   {14604, 29207, 1},
     //   {29208, 1591000, 1}};
     // check_result(1, r, 3);
    } else {
      IntRange r[1] = {{1, 1591000, 1}};
      check_result(1, r, 1);
    }
  }
}

TEST_F(MinorCompactionTest, test_memory_case1) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);

    open_for_write();
    append(1000, 4688, 10, 1);
    append(10000, 12000, 10, 1);
    close(0);

    open_for_write();
    append(1002, 4690, 15, 1);
    append(5000, 5200, 15, 1);
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

TEST_F(MinorCompactionTest, test_memory_case2) {
  TestArgs arg;
  arg.compression = kNoCompression;
  arg.format_version = 3;

  init(arg);

  open_for_write();
  append(1200, 2122, 50, 2);
  append(2700, 8000, 50, 100);
  close(0);

  open_for_write();
  append(1300, 2222, 40, 2);
  close(0);

  write_data(1310, 9000, 30, 0, 100);

  write_data(1000, 5000, 0, 1, 1);

  print_raw_meta();
  run_compact();
  print_raw_meta();
}

TEST_F(MinorCompactionTest, test_memory_case3) {
  TestArgs arg;
  arg.compression = kNoCompression;
  arg.format_version = 3;

  init(arg);

  open_write(1000, 1460, 30, 0, 1);
  open_write(2000, 2460, 30, 0, 1);
  close(0, false);

  write_data(1200, 2600, 20, 0, 5);

  write_data(1800, 5000, 0, 1, 10);

  print_raw_meta();
  run_compact();
  print_raw_meta();
}

TEST_F(MinorCompactionTest, test_wild_range_block) {
  TestArgs arg;
  arg.compression = kNoCompression;
  arg.format_version = 3;

  init(arg);

  open_for_write();
  append(1000, 1461, 50, 1);
  append(10000, 20000, 50, 100);
  close(0);

  write_data(2000, 5000, 30, 0, 1);
  write_data(2100, 5100, 20, 0, 1);

  print_raw_meta();
  run_compact();
  print_raw_meta();
}

TEST_F(MinorCompactionTest, test_lost_last_blocks) {
  TestArgs arg;
  arg.compression = kNoCompression;
  arg.format_version = 3;

  init(arg);

  open_for_write();
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

TEST_F(MinorCompactionTest, test_intersect_key_bug_12659392) {
  TestArgs arg;
  arg.compression = kNoCompression;
  arg.format_version = 3;

  init(arg);

  open_for_write();
  append(1923, 2384, 50, 1);
  append(2383, 2844, 45, 1);
  append(2843, 3304, 40, 1);
  append(3303, 3764, 30, 1);
  append(3763, 4224, 20, 1);
  close(0);

  open_for_write();
  append(1911, 2371, 10, 1);
  append(3305, 3318, 10, 1, kTypeValue, 5020);
  append(5000, 5461, 10, 1);
  close(1);

  print_raw_meta();
  run_compact();
  // print_raw_meta();
}

TEST_F(MinorCompactionTest, test_f) {
  TestArgs arg;
  arg.compression = kNoCompression;
  arg.format_version = 3;

  init(arg);

  open_for_write();
  append(1000, 2383, 50, 1);
  append(2843, 3304, 50, 1);

  close(0);

  open_for_write();
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

TEST_F(MinorCompactionTest, test_many_way_compact) {
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

TEST_F(MinorCompactionTest, test_large_block_many_way_compact) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    write_large_data(1000, 2000, 100, 0, 5);
    write_large_data(1001, 2000, 110, 0, 5);
    write_large_data(1002, 2000, 120, 0, 5);
    write_large_data(1003, 2000, 130, 0, 5);
    write_large_data(1004, 2000, 140, 0, 5);
    write_data(1000, 2000, 0, 1, 10);

    print_raw_meta();
    run_compact();
    print_raw_meta();

    //IntRange l1_r[1] = {10000, 19999, 100};
    //check_result(1, l1_r, 1);
    //IntRange l0_r[5] = {{10000, 19918, 30},
    //                    {10000, 19996, 33},
    //                    {10000, 19963, 36},
    //                    {10000, 19984, 39},
    //                    {10000, 19975, 42}};
    //check_result(0, l0_r, 5);
  }
}

TEST_F(MinorCompactionTest, test_muliple_task) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    int64_t seq = 10;
    for (int64_t i = 1; i <= 10; ++i) {
      open_write(i * 100, i * 1000, seq, 0, 15);
      open_write(i * 1000, i * 2000, seq, 0, 15);
      open_write(i * 10000, i * 20000, seq, 0, 100);
      close(0, false);
      seq++; 
    }

    write_data(1000, 20000, 0, 1, 100);
    write_data(25000, 80000, 0, 1, 100);
    write_data(85000, 180000, 0, 1, 100);

    print_raw_meta();

    // MinorCompaction will handle 4 ways at most, so we compact 4 times to
    // handle 10 L0 ways.
    for (int i = 0; i < 4; i++) {
      storage::CompactionJob job;
      CompactionContext ct;
      build_compact_context(&ct);
      //ct.space_manager_ = space_manager_.get();
      ct.space_manager_ = space_manager_;
      ct.table_cache_ = table_cache_.get();
      //TODO:yuanfeng
      /*
      int ret =
        job.init(ct, cf_desc_, storage_manager_.get(),
            storage_manager_->get_current_version());

      ASSERT_EQ(ret, 0);
      storage_manager_->get_current_version()->get_level0()->ref();
      */
      int ret = Status::kOk;
      ASSERT_EQ(ret, 0);
      ret = job.prepare_multiple_tasks(5);
      ASSERT_EQ(ret, 0);

      while (job.minor_task_size() > 0) {
        storage::MinorCompaction *compaction = job.get_next_minor_task();
        ret = compaction->run();
        ASSERT_EQ(ret, 0);
        job.append_change_info(compaction->get_change_info());
        job.destroy_minor_compaction(compaction);
      }
      ret = storage_manager_->apply(job.get_change_info(), false);
      ASSERT_EQ(ret, 0);
    }
    print_raw_meta();

    //IntRange r[2] = {{100, 19990, 15}, {20000, 199900, 100}};
    //check_result(1, r, 2);
    IntRange r[1] = {{100, 199900, 15}};
    check_result(1, r, 1);
  }
}

TEST_F(MinorCompactionTest, test_mp) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    write_data(10000, 20000, 10, 0, 100);
    write_data(1000, 20000, 0, 1, 100);
    write_data(25000, 80000, 0, 1, 100);
    write_data(85000, 180000, 0, 1, 100);

    print_raw_meta();

    storage::CompactionJob job;
    CompactionContext ct;
    build_compact_context(&ct);
    //ct.space_manager_ = space_manager_.get();
    ct.space_manager_ = space_manager_; 
    ct.table_cache_ = table_cache_.get();
    //TODO:yuanfeng
    /*
    int ret =
        job.init(ct, cf_desc_, storage_manager_.get(),
                 storage_manager_->get_current_version());
    */
    int ret = Status::kOk;
    ASSERT_EQ(ret, 0);
    //storage_manager_->get_current_version()->get_level0()->ref();
    ASSERT_EQ(ret, 0);
    ret = job.prepare_multiple_tasks(5);
    ASSERT_EQ(ret, 0);

    while (job.minor_task_size() > 0) {
      storage::MinorCompaction *compaction = job.get_next_minor_task();
      ret = compaction->run();
      ASSERT_EQ(ret, 0);
      job.append_change_info(compaction->get_change_info());
      job.destroy_minor_compaction(compaction);
    }
    ret = storage_manager_->apply(job.get_change_info(), false);
    ASSERT_EQ(ret, 0);
    print_raw_meta();
  }
}

TEST_F(MinorCompactionTest, test_no_intersect_l0) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    cf_desc_.column_family_id_ = 0;
    for (int i = 1; i < 8; ++i) {
      open_write(i * 1000, (i + 1) * 1000, 100, 0, 5);
    }
    close(0, false);

    write_data(1000, 20000, 50, 0, 20);

    cf_desc_.column_family_id_ = 1;

    write_data(1500, 2000, 10, 0, 5);

    print_raw_meta();

    cf_desc_.column_family_id_ = 0;
    storage::CompactionJob job;
    CompactionContext ct;
    build_compact_context(&ct);
    //ct.space_manager_ = space_manager_.get();
    ct.space_manager_ = space_manager_;
    ct.table_cache_ = table_cache_.get();
    //TODO:yuanfeng
    /*
    int ret =
        job.init(ct, cf_desc_, storage_manager_.get(),
                 storage_manager_->get_current_version());
    */
    int ret = Status::kOk;
    ASSERT_EQ(ret, 0);
    //storage_manager_->get_current_version()->get_level0()->ref();
    ASSERT_EQ(ret, 0);
    ret = job.prepare_multiple_tasks(10);
    ASSERT_EQ(ret, 0);

    while (job.minor_task_size() > 0) {
      storage::MinorCompaction *compaction = job.get_next_minor_task();
      ret = compaction->run();
      ASSERT_EQ(ret, 0);
      job.append_change_info(compaction->get_change_info());
      job.destroy_minor_compaction(compaction);
    }
    ret = storage_manager_->apply(job.get_change_info(), false);
    ASSERT_EQ(ret, 0);
    print_raw_meta();
  }
}

// should be ordered
//TEST_F(MinorCompactionTest, test_massive_data) {
//  std::vector<TestArgs> test_args = GenerateArgList();
//  for (auto &test_arg : test_args) {
//    init(test_arg);
//    // level 1 has three range;
//
//    for (int64_t i = 1; i < 1000; i *= 10) {
//      write_data(i * 100, i * 800, 0, 1);
//      write_data(i * 10000, i * 80000, 0, 1);
//    }
//    write_data(1000, 1600000, 10, 0, 10000);
//
//    print_raw_meta();
//    run_compact();
//    print_raw_meta();
//  }
//}


//TODO test
TEST_F(MinorCompactionTest, test_ordered_massive_data) {
  std::vector<TestArgs> test_args = GenerateArgList();
  for (auto &test_arg : test_args) {
    init(test_arg);
    // level 1 has three range;

    write_data(100, 800, 0, 1);
    write_data(10000, 80000, 0, 1);
    write_data(80000, 160000, 0, 1);
    write_data(1000, 1600000, 10, 0, 10000);

    // write_data(11000, 15000, 20, 0);
    // write_data(18000, 25000, 20, 0);

    // write_data(10000, 20000, 40, 0);
    // write_data(21111, 28888, 40, 0);

    print_raw_meta();
    run_compact();
    print_raw_meta();
  }
}
#endif

int main(int argc, char **argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  ::testing::InitGoogleTest(&argc, argv);
	xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}

