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
#pragma once

#include "db/db_impl.h"
#include "xengine/cache.h"
#include "db/version_set.h"
#include "db/dbformat.h"
#include "storage/extent_space_manager.h"
#include "storage/storage_logger.h"
#include "storage/storage_manager.h"
#include "compact/compaction.h"
#include "util/testharness.h"
#include "xengine/xengine_constants.h"
#define private public
#define protected public
#include "table/extent_table_builder.h"

namespace xengine
{
namespace table
{

class TESTFlushBlockPolicy : public FlushBlockPolicy
{
public:
  TESTFlushBlockPolicy() : need_flush_(false)
  {}
  virtual bool Update(const common::Slice& key, const common::Slice& value) override
  {
    bool need_flush = need_flush_;
    need_flush_ = false;
    return need_flush;
  }
  void set_need_flush()
  {
    need_flush_ = true;
  }
private:
  bool need_flush_;
};

class TESTFlushBlockPolicyFactory : public FlushBlockPolicyFactory
{
public:
  TESTFlushBlockPolicyFactory() {}
  const char* Name() const override {
    return "TESTFlushBlockPolicyFactory";
  }
  FlushBlockPolicy* NewFlushBlockPolicy(const BlockBasedTableOptions& table_options,
                                        const BlockBuilder& data_block_builder) const override {
    return new TESTFlushBlockPolicy;
  }
};

struct TestArgs {
  common::CompressionType compression_;
  uint32_t format_version_;
  std::string db_path_;
  common::Options *options_;
  BlockBasedTableOptions table_options_;
  size_t block_cache_size_;

  TestArgs() : compression_(common::CompressionType::kNoCompression),
               format_version_(3),
               db_path_(util::test::TmpDir() + "/interal_iterator_test_base"),
               options_(nullptr),
               block_cache_size_(0)
  {}
  void reset ()
  {
    delete options_;
  }
  void build_default_options()
  {
    reset();
    options_ = new common::Options();
    int block_size = 16 * 1024;
    table_options_.block_size = block_size;
    table_options_.flush_block_policy_factory.reset(new TESTFlushBlockPolicyFactory());
    table_options_.block_cache = cache::NewLRUCache(block_cache_size_ == 0 ? 50000 : block_cache_size_, 1);
    options_->table_factory.reset(NewExtentBasedTableFactory(table_options_));
    options_->disable_auto_compactions = true;
    options_->compression = compression_;
    options_->create_if_missing = true;
    options_->fail_if_options_file_error = true;
    options_->create_missing_column_families = true;
    options_->env = util::Env::Default();
    int db_write_buffer_size = 64 * 1024 * 1024;
    options_->db_write_buffer_size = db_write_buffer_size;
    int write_buffer_size = db_write_buffer_size;
    options_->write_buffer_size = write_buffer_size;
    // Arena will assert kBlockSize in 4096 to (2u << 30)
    options_->arena_block_size = 4096 * 2;
    options_->memtable_huge_page_size = 4096 * 2;

    options_->compaction_type = 0; // should be 0 here

    int file_size = db_write_buffer_size * 1024;
    options_->target_file_size_base = file_size;

    if (options_->db_paths.size() == 0) {
      options_->db_paths.emplace_back(db_path_, std::numeric_limits<uint64_t>::max());
    }

    auto factory = std::make_shared<memtable::SkipListFactory>();
    options_->memtable_factory = factory;
    db::WriteBufferManager *wb = new db::WriteBufferManager(0);  // no limit space
    assert(wb != nullptr);
    options_->write_buffer_manager.reset(wb);
  }
};

struct TestContext
{
  common::Options *options_;
  common::DBOptions db_options_;
  util::EnvOptions env_options_;
  common::ImmutableDBOptions idb_options_;
  common::MutableCFOptions mutable_cf_options_;
  common::ImmutableCFOptions icf_options_;

  TestContext(common::Options *options) : options_(options),
                                          db_options_(*options_),
                                          env_options_(db_options_),
                                          idb_options_(*options_),
                                          mutable_cf_options_(*options_),
                                          icf_options_(*options_)
  {
  }
};

class InternalIteratorTestBase : public testing::Test
{
public:
  InternalIteratorTestBase() : context_(nullptr),
                               env_(nullptr),
                               global_ctx_(nullptr),
                               write_buffer_manager_(nullptr),
                               next_file_number_(2),
                               space_manager_(nullptr),
                               version_set_(nullptr),
                               descriptor_log_(nullptr),
                               storage_logger_(nullptr),
                               internal_comparator_(util::BytewiseComparator()),
                               extent_builder_(nullptr),
                               //block_cache_(nullptr),
                               table_cache_(nullptr),
                               row_size_(0),
                               key_size_(0),
                               //mutex_(monitor::mutex_dbimpl_key),
                               level_(1)
  {}
  virtual ~InternalIteratorTestBase()
  {
    destroy();
    reset();
  }

  void init(const TestArgs &args);
  void reset();
  void destroy();
  void open_extent_builder();
  void append_block(const int64_t start_key,
                    const int64_t end_key,
                    const bool flush_block);
  void append_rows(const int64_t start_key,
                   const int64_t end_key,
                   bool flush_block);
  void close_extent_builder();
  void make_key(char *buf,
                const int64_t size,
                const int64_t key);
  void make_value(char *buf,
                  const int64_t size,
                  const int64_t key);

protected:
  static const int64_t INDEX_ID = 0;
protected:
  // env
  TestContext *context_;
  util::Env *env_;
  std::unique_ptr<util::Directory, memory::ptr_destruct_delete<util::Directory>> db_dir_;
  std::string dbname_;
  db::GlobalContext *global_ctx_;
  db::WriteBufferManager *write_buffer_manager_;
  // extent space manager
  db::FileNumber next_file_number_;
  storage::ExtentSpaceManager* space_manager_;
  // storage
  unique_ptr<storage::StorageManager> storage_manager_;
  // stroage logger
  db::VersionSet *version_set_;
  db::log::Writer *descriptor_log_;
  storage::StorageLogger *storage_logger_;
  // extent budiler
  storage::ChangeInfo change_info_;
  db::InternalKeyComparator internal_comparator_;
  std::vector<std::unique_ptr<db::IntTblPropCollectorFactory>> props_;
  std::string compression_dict_;
  storage::ColumnFamilyDesc cf_desc_;
  db::MiniTables mini_tables_;
  std::unique_ptr<table::TableBuilder> extent_builder_;
  // scan
  //std::shared_ptr<cache::Cache> block_cache_;
  std::unique_ptr<db::TableCache> table_cache_;
  std::shared_ptr<cache::Cache> clock_cache_;
  int64_t row_size_;
  int64_t key_size_;
  // used to flush extent
  //monitor::InstrumentedMutex mutex_;
  int64_t level_;
};

void InternalIteratorTestBase::destroy()
{
  ASSERT_OK(db::DestroyDB(dbname_, *context_->options_));
  delete context_->options_;
  context_->options_ = nullptr;
}

void InternalIteratorTestBase::reset()
{
  env_ = nullptr;
  db_dir_.reset();
  props_.clear();
  mini_tables_.metas.clear();
  mini_tables_.props.clear();
  //if (mini_tables_.schema == nullptr) {
    //mini_tables_.schema = new common::XengineSchema;
  //}
  extent_builder_.reset();

  if (nullptr != storage_logger_) {
    delete storage_logger_;
    storage_logger_ = nullptr;
  }
  descriptor_log_ = nullptr;
  storage_manager_.reset();
  if (nullptr != space_manager_) {
    delete space_manager_;
    space_manager_ = nullptr;
  }
  xengine::common::Options* options = nullptr;
  if (nullptr != context_) {
    options = context_->options_;
    delete context_;
    context_ = nullptr;
  }
  if (nullptr != version_set_) {
    delete version_set_;
    version_set_ = nullptr;
  }
  //block_cache_.reset();
  clock_cache_.reset();
  table_cache_.reset();
  //block_cache_size_ = 0;
  row_size_ = 0;
  key_size_ = 0;
  if (nullptr != global_ctx_) {
    delete global_ctx_;
  }
  if (nullptr != write_buffer_manager_) {
    delete write_buffer_manager_;
  }
  delete options;
}

void InternalIteratorTestBase::init(const TestArgs &args)
{
  reset();
  context_ = new TestContext(args.options_);
  env_ = context_->options_->env;
  dbname_ = context_->options_->db_paths[0].path;
  env_->DeleteDir(dbname_);
  env_->CreateDir(dbname_);
  util::Directory *ptr = nullptr;
  env_->NewDirectory(dbname_, ptr);
  db_dir_.reset(ptr);

  // new
  clock_cache_ = cache::NewLRUCache(50000, 1);
  global_ctx_ = new db::GlobalContext(dbname_, *context_->options_);
  space_manager_ = new storage::ExtentSpaceManager(env_, global_ctx_->env_options_, context_->db_options_);
  write_buffer_manager_ = new db::WriteBufferManager(0);
  db::WriteController *write_controller = nullptr; // new db::WriteController();
  storage_logger_ = new storage::StorageLogger();
  version_set_ = new db::VersionSet(dbname_,
                                    &context_->idb_options_,
                                    context_->env_options_,
                                    reinterpret_cast<cache::Cache*>(table_cache_.get()),
                                    write_buffer_manager_,
                                    write_controller);
  table_cache_.reset(new db::TableCache(context_->icf_options_,
                                        context_->env_options_,
                                        clock_cache_.get(),
                                        space_manager_));

  // init storage logger
  global_ctx_->env_ = env_;
  global_ctx_->cache_ = clock_cache_.get();
  global_ctx_->storage_logger_ = storage_logger_;
  global_ctx_->write_buf_mgr_ = write_buffer_manager_;
  global_ctx_->extent_space_mgr_ = space_manager_;
  //global_ctx_->db_mutex_ = &mutex_;
  version_set_->init(global_ctx_);

  storage_logger_->TEST_reset();
  storage_logger_->init(env_,
                        dbname_,
                        context_->env_options_,
                        context_->idb_options_,
                        version_set_,
                        space_manager_,
                        1 * 1024 * 1024 * 1024);
  uint64_t file_number = 1;
  common::Status s;
  std::string manifest_filename = util::DescriptorFileName(dbname_, file_number);
//  std::unique_ptr<util::WritableFile> descriptor_file;
  util::WritableFile *descriptor_file = nullptr;
  util::EnvOptions opt_env_opts = env_->OptimizeForManifestWrite(context_->env_options_);
  s = NewWritableFile(env_, manifest_filename, descriptor_file, opt_env_opts);
  assert(s.ok());
  descriptor_file->SetPreallocationBlockSize(context_->db_options_.manifest_preallocation_size);
//  unique_ptr<util::ConcurrentDirectFileWriter> file_writer(
//      new util::ConcurrentDirectFileWriter(descriptor_file, descriptor_file));
  util::ConcurrentDirectFileWriter *file_writer = MOD_NEW_OBJECT(memory::ModId::kDefaultMod,
      util::ConcurrentDirectFileWriter, descriptor_file, opt_env_opts);
  s = file_writer->init_multi_buffer();
  assert(s.ok());
  descriptor_log_ = MOD_NEW_OBJECT(memory::ModId::kStorageLogger,
                                   db::log::Writer,
                                   file_writer,
                                   0,
                                   false);
  assert(descriptor_log_ != nullptr);
  storage_logger_->set_log_writer(descriptor_log_);

  // storage manager
  db::CreateSubTableArgs subtable_args;
  //subtable_args.create_paxos_index_ = 1;
  subtable_args.index_id_ = 1;
  //subtable_args.index_no_ = 1;
  //subtable_args.wal_log_file_id_ = 1;
  //memory::ArenaAllocator *allocator = new memory::ArenaAllocator();
  //db::ColumnFamilyData *sub_table = new db::ColumnFamilyData(global_ctx_->options_);
  //PartitionKey p_key(1, 1, 0);
  //sub_table->init(subtable_args, p_key, allocator, global_ctx_);
  //sub_table->init(subtable_args, global_ctx_);
  //storage_manager_.reset(new storage::StorageManager(context_->db_options_, ColumnFamilyOptions(*context_->options_), sub_table));
  storage_manager_.reset(new storage::StorageManager(context_->env_options_, context_->icf_options_, context_->mutable_cf_options_));
  storage_manager_->init(env_, space_manager_, clock_cache_.get());

  // space manager
  space_manager_->init(storage_logger_);
  space_manager_->create_table_space(0);

  //
  row_size_ = 16;
  key_size_ = 32;
  level_ = 1;
}

void InternalIteratorTestBase::open_extent_builder()
{
  mini_tables_.change_info_ = &change_info_;
  ASSERT_EQ(storage_logger_->begin(storage::MINOR_COMPACTION), common::Status::kOk);
  //mini_tables_.change_info_->task_type_ = db::TaskType::SPLIT_TASK;
  mini_tables_.space_manager = space_manager_;
  mini_tables_.table_space_id_ = 0;
  cf_desc_.column_family_id_ = 1;
  storage::LayerPosition output_layer_position(level_, 0);
  extent_builder_.reset(NewTableBuilder(context_->icf_options_,
                                        internal_comparator_,
                                        &props_,
                                        cf_desc_.column_family_id_,
                                        cf_desc_.column_family_name_,
                                        &mini_tables_,
                                        storage::GetCompressionType(context_->icf_options_,
                                                                    context_->mutable_cf_options_,
                                                                    level_),
                                        context_->icf_options_.compression_opts,
                                        output_layer_position,
                                        &compression_dict_,
                                        true));
}

// NOTICE! the end_key will in the next block/extent
void InternalIteratorTestBase::append_block(const int64_t start_key,
                                            const int64_t end_key,
                                            const bool flush_block)
{
  append_rows(start_key, end_key, flush_block);
}

void InternalIteratorTestBase::make_key(char *buf,
                                        const int64_t size,
                                        const int64_t key)
{
  memset(buf, 0, size);
  snprintf(buf, size, "%04ld%010ld", INDEX_ID, key);
}

void InternalIteratorTestBase::make_value(char *buf,
                                          const int64_t size,
                                          const int64_t key)
{
  memset(buf, 0, size);
  snprintf(buf, size, "%010ld", key);
}

void InternalIteratorTestBase::append_rows(const int64_t start_key,
                                           const int64_t end_key,
                                           bool flush_block)
{
  ASSERT_TRUE(nullptr != extent_builder_.get());
  int64_t commit_log_seq = 0;
  char key_buf[key_size_];
  char row_buf[row_size_];
  memset(key_buf, 0, key_size_);
  memset(row_buf, 0, row_size_);

  for (int64_t key = start_key; key <= end_key; key++) {
    make_key(key_buf, key_size_, key);
    if (key == end_key) {
      if (flush_block) {
        reinterpret_cast<TESTFlushBlockPolicy *>(
            reinterpret_cast<ExtentBasedTableBuilder *>(
            extent_builder_.get())->rep_->flush_block_policy.get())->set_need_flush();
      }
    }
    db::InternalKey ikey(common::Slice(key_buf, strlen(key_buf)), 10 /*sequence*/, db::kTypeValue);
    make_value(row_buf, row_size_, key);
    extent_builder_->Add(ikey.Encode(), common::Slice(row_buf, row_size_));
    ASSERT_TRUE(extent_builder_->status().ok());
  }
}

void InternalIteratorTestBase::close_extent_builder()
{
  int64_t commit_seq = 0;
  extent_builder_->Finish();
  ASSERT_TRUE(extent_builder_->status().ok());
  ASSERT_EQ(storage_logger_->commit(commit_seq), common::Status::kOk);
  ASSERT_EQ(storage_manager_->apply(*(mini_tables_.change_info_), false), common::Status::kOk);
  mini_tables_.metas.clear();
  mini_tables_.props.clear();
  mini_tables_.change_info_->clear();
}

} // namespace table
} // namespace xengine
