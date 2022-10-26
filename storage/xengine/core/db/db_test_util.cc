// Portions Copyright (c) 2020, Alibaba Group Holding Limited
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/db_test_util.h"
#include "db/forward_iterator.h"

using namespace xengine;
using namespace util;
using namespace test;
using namespace table;
using namespace memtable;
using namespace cache;
using namespace common;

namespace xengine {
namespace db {

// Special Env used to delay background operations

SpecialEnv::SpecialEnv(Env* base)
    : EnvWrapper(base),
      rnd_(301),
      sleep_counter_(this),
      addon_time_(0),
      time_elapse_only_sleep_(false),
      no_slowdown_(false) {
  delay_sstable_sync_.store(false, std::memory_order_release);
  drop_writes_.store(false, std::memory_order_release);
  no_space_.store(false, std::memory_order_release);
  non_writable_.store(false, std::memory_order_release);
  count_random_reads_ = false;
  count_sequential_reads_ = false;
  manifest_sync_error_.store(false, std::memory_order_release);
  manifest_write_error_.store(false, std::memory_order_release);
  log_write_error_.store(false, std::memory_order_release);
  random_file_open_counter_.store(0, std::memory_order_relaxed);
  delete_count_.store(0, std::memory_order_relaxed);
  num_open_wal_file_.store(0);
  log_write_slowdown_ = 0;
  bytes_written_ = 0;
  sync_counter_ = 0;
  non_writeable_rate_ = 0;
  new_writable_count_ = 0;
  non_writable_count_ = 0;
  table_write_callback_ = nullptr;
}

DBTestBase::DBTestBase(const std::string path)
    : option_config_(kDefault),
      mem_env_(!getenv("MEM_ENV") ? nullptr : new MockEnv(Env::Default())),
      env_(new SpecialEnv(mem_env_ ? mem_env_ : Env::Default())) {
  env_->SetBackgroundThreads(1, Env::LOW);
  env_->SetBackgroundThreads(1, Env::HIGH);
  env_->SetBackgroundThreads(4, Env::FILTER);
  dbname_ = test::TmpDir(env_) + path;
  alternative_wal_dir_ = dbname_ + "/wal";
  alternative_db_log_dir_ = dbname_ + "/db_log_dir";
  auto options = CurrentOptions();
  options.env = env_;
  auto delete_options = options;
  delete_options.wal_dir = alternative_wal_dir_;
  DestroyDB(dbname_, delete_options);
  // Destroy it for not alternative WAL dir is used.
  DestroyDB(dbname_, options);
  db_ = nullptr;
  Reopen(options);
  Random::GetTLSInstance()->Reset(0xdeadbeef);
}

DBTestBase::~DBTestBase() {
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->LoadDependency({});
  SyncPoint::GetInstance()->ClearAllCallBacks();
  Close();
  Options options;
  options.db_paths.emplace_back(dbname_, 0);
  options.db_paths.emplace_back(dbname_ + "_2", 0);
  options.db_paths.emplace_back(dbname_ + "_3", 0);
  options.db_paths.emplace_back(dbname_ + "_4", 0);
  options.env = env_;

  if (getenv("KEEP_DB")) {
    printf("DB is still at %s\n", dbname_.c_str());
  } else {
    DestroyDB(dbname_, options);
  }
  delete env_;
}

bool DBTestBase::ShouldSkipOptions(int option_config, int skip_mask) {
#ifdef ROCKSDB_LITE
  // These options are not supported in ROCKSDB_LITE
  if (option_config == kHashSkipList ||
      option_config == kPlainTableFirstBytePrefix ||
      option_config == kPlainTableCappedPrefix ||
      option_config == kPlainTableCappedPrefixNonMmap ||
      option_config == kPlainTableAllBytesPrefix ||
      option_config == kVectorRep || option_config == kHashLinkList ||
      option_config == kHashCuckoo || option_config == kUniversalCompaction ||
      option_config == kUniversalCompactionMultiLevel ||
      option_config == kUniversalSubcompactions ||
      option_config == kFIFOCompaction ||
      option_config == kConcurrentSkipList) {
    return true;
  }
#endif

  if ((skip_mask & kSkipUniversalCompaction) &&
      (option_config == kUniversalCompaction ||
       option_config == kUniversalCompactionMultiLevel)) {
    return true;
  }
  if ((skip_mask & kSkipMergePut) && option_config == kMergePut) {
    return true;
  }
  if ((skip_mask & kSkipNoSeekToLast) &&
      (option_config == kHashLinkList || option_config == kHashSkipList)) {
    return true;
  }
  if ((skip_mask & kSkipPlainTable) &&
      (option_config == kPlainTableAllBytesPrefix ||
       option_config == kPlainTableFirstBytePrefix ||
       option_config == kPlainTableCappedPrefix ||
       option_config == kPlainTableCappedPrefixNonMmap)) {
    return true;
  }
  if ((skip_mask & kSkipHashIndex) &&
      (option_config == kBlockBasedTableWithPrefixHashIndex ||
       option_config == kBlockBasedTableWithWholeKeyHashIndex)) {
    return true;
  }
  if ((skip_mask & kSkipHashCuckoo) && (option_config == kHashCuckoo)) {
    return true;
  }
  if ((skip_mask & kSkipFIFOCompaction) && option_config == kFIFOCompaction) {
    return true;
  }
  if ((skip_mask & kSkipMmapReads) && option_config == kWalDirAndMmapReads) {
    return true;
  }
  return false;
}

// Switch to a fresh database with the next option configuration to
// test.  Return false if there are no more configurations to test.
bool DBTestBase::ChangeOptions(int skip_mask) {
  for (option_config_++; option_config_ < kEnd; option_config_++) {
    // only support concurrent insert
    if (ShouldSkipOptions(option_config_, skip_mask)) {
      continue;
    }
    break;
  }

  if (option_config_ >= kEnd) {
    Destroy(last_options_);
    return false;
  } else {
    auto options = CurrentOptions();
    options.create_if_missing = true;
    DestroyAndReopen(options);
    return true;
  }
}

// Switch between different compaction styles.
bool DBTestBase::ChangeCompactOptions() {
  if (option_config_ == kDefault) {
    option_config_ = kUniversalCompaction;
    Destroy(last_options_);
    auto options = CurrentOptions();
    options.create_if_missing = true;
    TryReopen(options);
    return true;
  } else if (option_config_ == kUniversalCompaction) {
    option_config_ = kUniversalCompactionMultiLevel;
    Destroy(last_options_);
    auto options = CurrentOptions();
    options.create_if_missing = true;
    TryReopen(options);
    return true;
  } else if (option_config_ == kUniversalCompactionMultiLevel) {
    option_config_ = kLevelSubcompactions;
    Destroy(last_options_);
    auto options = CurrentOptions();
    assert(options.max_subcompactions > 1);
    TryReopen(options);
    return true;
  } else if (option_config_ == kLevelSubcompactions) {
    option_config_ = kUniversalSubcompactions;
    Destroy(last_options_);
    auto options = CurrentOptions();
    assert(options.max_subcompactions > 1);
    TryReopen(options);
    return true;
  } else {
    return false;
  }
}

// Switch between different WAL settings
bool DBTestBase::ChangeWalOptions() {
  if (option_config_ == kDefault) {
    option_config_ = kDBLogDir;
    Destroy(last_options_);
    auto options = CurrentOptions();
    Destroy(options);
    options.create_if_missing = true;
    TryReopen(options);
    return true;
  } else if (option_config_ == kDBLogDir) {
    option_config_ = kWalDirAndMmapReads;
    Destroy(last_options_);
    auto options = CurrentOptions();
    Destroy(options);
    options.create_if_missing = true;
    TryReopen(options);
    return true;
  } else if (option_config_ == kWalDirAndMmapReads) {
    option_config_ = kRecycleLogFiles;
    Destroy(last_options_);
    auto options = CurrentOptions();
    Destroy(options);
    TryReopen(options);
    return true;
  } else {
    return false;
  }
}

// Switch between different filter policy
// Jump from kDefault to kFilter to kFullFilter
bool DBTestBase::ChangeFilterOptions() {
  if (option_config_ == kDefault) {
    option_config_ = kFilter;
  } else if (option_config_ == kFilter) {
    option_config_ = kFullFilterWithNewTableReaderForCompactions;
  } else {
    return false;
  }
  Destroy(last_options_);

  auto options = CurrentOptions();
  options.create_if_missing = true;
  TryReopen(options);
  return true;
}

// Return the current option configuration.
Options DBTestBase::CurrentOptions(
    const anon::OptionsOverride& options_override) {
  Options options;
  options.write_buffer_size = 4090 * 4096;
  options.target_file_size_base = 2 * 1024 * 1024;
  options.max_bytes_for_level_base = 10 * 1024 * 1024;
  options.max_open_files = 5000;
  options.base_background_compactions = -1;
  options.wal_recovery_mode = WALRecoveryMode::kTolerateCorruptedTailRecords;
  options.compaction_pri = CompactionPri::kByCompensatedSize;

  return CurrentOptions(options, options_override);
}

Options DBTestBase::CurrentOptions(
    const Options& defaultOptions,
    const anon::OptionsOverride& options_override) {
  // this redundant copy is to minimize code change w/o having lint error.
  Options options = defaultOptions;
  BlockBasedTableOptions table_options;
  bool set_block_based_table_factory = true;
  switch (option_config_) {
#ifndef ROCKSDB_LITE
    case kHashSkipList:
      options.prefix_extractor.reset(NewFixedPrefixTransform(1));
      options.memtable_factory.reset(NewHashSkipListRepFactory(16));
      options.allow_concurrent_memtable_write = false;
      break;
    case kPlainTableFirstBytePrefix:
      // options.table_factory.reset(new PlainTableFactory());
      options.prefix_extractor.reset(NewFixedPrefixTransform(1));
      options.allow_mmap_reads = true;
      options.max_sequential_skip_in_iterations = 999999;
      // set_block_based_table_factory = false;
      break;
    case kPlainTableCappedPrefix:
      // options.table_factory.reset(new PlainTableFactory());
      options.prefix_extractor.reset(NewCappedPrefixTransform(8));
      options.allow_mmap_reads = true;
      options.max_sequential_skip_in_iterations = 999999;
      // set_block_based_table_factory = false;
      break;
    case kPlainTableCappedPrefixNonMmap:
      // options.table_factory.reset(new PlainTableFactory());
      options.prefix_extractor.reset(NewCappedPrefixTransform(8));
      options.allow_mmap_reads = false;
      options.max_sequential_skip_in_iterations = 999999;
      // set_block_based_table_factory = false;
      break;
    case kPlainTableAllBytesPrefix:
      // options.table_factory.reset(new PlainTableFactory());
      options.prefix_extractor.reset(NewNoopTransform());
      options.allow_mmap_reads = true;
      options.max_sequential_skip_in_iterations = 999999;
      // set_block_based_table_factory = false;
      break;
    case kVectorRep:
      options.memtable_factory.reset(new VectorRepFactory(100));
      options.allow_concurrent_memtable_write = false;
      break;
    case kHashLinkList:
      options.prefix_extractor.reset(NewFixedPrefixTransform(1));
      options.memtable_factory.reset(
          NewHashLinkListRepFactory(4, 0, 3, true, 4));
      options.allow_concurrent_memtable_write = false;
      break;
    case kHashCuckoo:
      options.memtable_factory.reset(
          NewHashCuckooRepFactory(options.write_buffer_size));
      options.allow_concurrent_memtable_write = false;
      break;
#endif  // ROCKSDB_LITE
    case kMergePut:
      options.merge_operator = util::MergeOperators::CreatePutOperator();
      break;
    case kFilter:
      table_options.filter_policy.reset(NewBloomFilterPolicy(10, true));
      break;
    case kFullFilterWithNewTableReaderForCompactions:
      table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
      options.new_table_reader_for_compaction_inputs = true;
      options.compaction_readahead_size = 10 * 1024 * 1024;
      break;
    case kPartitionedFilterWithNewTableReaderForCompactions:
      table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
      table_options.partition_filters = true;
      table_options.index_type =
          BlockBasedTableOptions::IndexType::kTwoLevelIndexSearch;
      options.new_table_reader_for_compaction_inputs = true;
      options.compaction_readahead_size = 10 * 1024 * 1024;
      break;
    case kUncompressed:
      options.compression = kNoCompression;
      break;
    case kNumLevel_3:
      options.num_levels = 3;
      break;
    case kDBLogDir:
      options.db_log_dir = alternative_db_log_dir_;
      break;
    case kWalDirAndMmapReads:
      options.wal_dir = alternative_wal_dir_;
      // mmap reads should be orthogonal to WalDir setting, so we piggyback to
      // this option config to test mmap reads as well
      options.allow_mmap_reads = true;
      break;
    case kManifestFileSize:
      options.max_manifest_file_size = 50;  // 50 bytes
      break;
    case kPerfOptions:
      options.soft_rate_limit = 2.0;
      options.delayed_write_rate = 8 * 1024 * 1024;
      options.report_bg_io_stats = true;
      // TODO(3.13) -- test more options
      break;
    case kUniversalCompaction:
      options.compaction_style = kCompactionStyleUniversal;
      options.num_levels = 1;
      break;
    case kUniversalCompactionMultiLevel:
      options.compaction_style = kCompactionStyleUniversal;
      options.num_levels = 8;
      break;
    case kCompressedBlockCache:
      options.allow_mmap_writes = true;
      table_options.block_cache_compressed = NewLRUCache(8 * 1024 * 1024);
      break;
    case kInfiniteMaxOpenFiles:
      options.max_open_files = -1;
      break;
    case kxxHashChecksum: {
      // table_options.checksum = kxxHash;
      break;
    }
    case kFIFOCompaction: {
      options.compaction_style = kCompactionStyleFIFO;
      break;
    }
    case kBlockBasedTableWithPrefixHashIndex: {
      // table_options.index_type = BlockBasedTableOptions::kHashSearch;
      // options.prefix_extractor.reset(NewFixedPrefixTransform(1));
      break;
    }
    case kBlockBasedTableWithWholeKeyHashIndex: {
      // table_options.index_type = BlockBasedTableOptions::kHashSearch;
      // options.prefix_extractor.reset(NewNoopTransform());
      break;
    }
    case kBlockBasedTableWithPartitionedIndex: {
      table_options.index_type = BlockBasedTableOptions::kTwoLevelIndexSearch;
      options.prefix_extractor.reset(NewNoopTransform());
      break;
    }
    case kBlockBasedTableWithIndexRestartInterval: {
      table_options.index_block_restart_interval = 8;
      break;
    }
    case kOptimizeFiltersForHits: {
      options.optimize_filters_for_hits = true;
      set_block_based_table_factory = true;
      break;
    }
    case kRowCache: {
      options.row_cache = NewRowCache(32 * 1024 * 1024);
      break;
    }
    case kRecycleLogFiles: {
      options.recycle_log_file_num = 2;
      break;
    }
    case kLevelSubcompactions: {
      options.max_subcompactions = 4;
      break;
    }
    case kUniversalSubcompactions: {
      options.compaction_style = kCompactionStyleUniversal;
      options.num_levels = 8;
      options.max_subcompactions = 4;
      break;
    }
    case kConcurrentSkipList: {
      options.allow_concurrent_memtable_write = true;
      options.enable_write_thread_adaptive_yield = true;
      break;
    }

    default:
      break;
  }

  if (options_override.filter_policy) {
    table_options.filter_policy = options_override.filter_policy;
    table_options.partition_filters = options_override.partition_filters;
    table_options.metadata_block_size = options_override.metadata_block_size;
  }
  if (set_block_based_table_factory) {
    options.table_factory.reset(
        table::NewExtentBasedTableFactory(table_options));
  }
  options.env = env_;
  options.create_if_missing = true;
  options.fail_if_options_file_error = true;
  return options;
}

void DBTestBase::CreateColumnFamilies(const std::vector<std::string>& cfs,
                                      const Options& options) {
  ColumnFamilyOptions cf_opts(options);
  for (auto cf : cfs) {
    uint32_t cfi = get_next_column_family_id();
    ColumnFamilyHandle *handle = nullptr;
    ColumnFamilyDescriptor cfd(cf, cf_opts);
    ASSERT_TRUE(0 == dbfull()->TEST_create_subtable(cfd, cfi, handle));
    ASSERT_TRUE(nullptr != handle);
    ASSERT_TRUE(cfi == handle->GetID());
    cfh_map_.emplace(cfi, handle);
  }
}

void DBTestBase::CreateAndReopenWithCF(const std::vector<std::string>& cfs,
                                       const Options& options) {
  CreateColumnFamilies(cfs, options);
  std::vector<std::string> cfs_plus_default = cfs;
  cfs_plus_default.insert(cfs_plus_default.begin(), kDefaultColumnFamilyName);
  ReopenWithColumnFamilies(cfs_plus_default, options);
}

void DBTestBase::DropColumnFamily(const int cf)
{
  auto cfh = get_column_family_handle(cf);
  dbfull()->DropColumnFamily(cfh);
  cfh_map_.erase(cfh->GetID());
}

void DBTestBase::ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                          const std::vector<Options>& options) {
  ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
}

void DBTestBase::ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                          const Options& options) {
  ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
}

Status DBTestBase::TryReopenWithColumnFamilies(
    const std::vector<std::string>& cfs, const std::vector<Options>& options) {
  Close();
  EXPECT_EQ(cfs.size(), options.size());
  std::vector<ColumnFamilyDescriptor> column_families;
  for (size_t i = 0; i < cfs.size(); ++i) {
    column_families.push_back(ColumnFamilyDescriptor(cfs[i], options[i]));
  }
  DBOptions db_opts = DBOptions(options[0]);
  std::vector<ColumnFamilyHandle*> handles;
  Status s = DB::Open(db_opts, dbname_, column_families, &handles, &db_);
  if (s.ok()) {
    for (auto h : handles) {
        cfh_map_.erase(h->GetID());
        cfh_map_.emplace(h->GetID(), h);
    }
  }

  return s;
}

Status DBTestBase::TryReopenWithColumnFamilies(
    const std::vector<std::string>& cfs, const Options& options) {
  Close();
  std::vector<Options> v_opts(cfs.size(), options);
  return TryReopenWithColumnFamilies(cfs, v_opts);
}

void DBTestBase::Reopen(const Options& options, const std::string *db_name) {
  ASSERT_OK(TryReopen(options, db_name));
}

void DBTestBase::Close() {
  for (auto &cfh : cfh_map_) {
    if (cfh.second != dbfull()->DefaultColumnFamily()) {
      db_->DestroyColumnFamilyHandle(cfh.second);
    }
  }
  cfh_map_.clear();
//  delete db_;
  MOD_DELETE_OBJECT(DB, db_);
  db_ = nullptr;
}

void DBTestBase::DestroyAndReopen(const Options& options) {
  // Destroy using last options
  Destroy(last_options_);
  ASSERT_OK(TryReopen(options));
}

void DBTestBase::Destroy(const Options& options) {
  Close();
  ASSERT_OK(DestroyDB(dbname_, options));
}

Status DBTestBase::ReadOnlyReopen(const Options& options) {
  return open_create_default_subtable(options);
}

Status DBTestBase::TryReopen(const Options& options, const std::string *db_name) {
  Close();
  last_options_.table_factory.reset();
  // Note: operator= is an unsafe approach here since it destructs shared_ptr in
  // the same order of their creation, in contrast to destructors which
  // destructs them in the opposite order of creation. One particular problme is
  // that the cache destructor might invoke callback functions that use Option
  // members such as statistics. To work around this problem, we manually call
  // destructor of table_facotry which eventually clears the block cache.
  last_options_ = options;
  last_options_.allow_concurrent_memtable_write = false;
  return open_create_default_subtable(options, db_name);
}

common::Status DBTestBase::open_create_default_subtable(const common::Options& options,
                                                        const std::string *db_name) {
  uint32_t kDefaultColumnFamilyId = 0;
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  // DBImpl::Open will filter out kDefaultColumnFamilyName from column_families
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  Status s = DB::Open(db_options, db_name == nullptr ? dbname_ : *db_name, column_families, &handles, &db_);
  if (s.ok()) {
    for (auto h : handles) {
      cfh_map_.erase(h->GetID());
      cfh_map_.emplace(h->GetID(), h);
    }
    bool to_create = false;
    // always create the default column family handle
    if (s.ok()) {
      auto cfh0 = get_column_family_handle(kDefaultColumnFamilyId);
      uint32_t default_id = 0;
      auto default_cfh = dbfull()->DefaultColumnFamily();
      if ((nullptr != cfh0) && (nullptr != default_cfh)) {
        if (kDefaultColumnFamilyId != (default_id = default_cfh->GetID())) {
          XENGINE_LOG(WARN, "Id for default sub table isn't",
                      K(kDefaultColumnFamilyId), K(default_id));
          //dbfull()->SetDefaultColumnFamily(cfh0);
        } else {
          if (cfh0 != default_cfh) {
            // They both must be attached with the default column family
            //assert(reinterpret_cast<ColumnFamilyHandleImpl*>(default_cfh)->cfd()
            //       == reinterpret_cast<ColumnFamilyHandleImpl*>(cfh0)->cfd());
//            delete cfh0;
            MOD_DELETE_OBJECT(ColumnFamilyHandle, cfh0);
            cfh_map_[kDefaultColumnFamilyId] = default_cfh;
          }
        }
      } else if (nullptr != cfh0) {
        //dbfull()->SetDefaultColumnFamily(cfh0);
      } else if (nullptr != default_cfh) {
        if (kDefaultColumnFamilyId != (default_id = default_cfh->GetID())) {
          XENGINE_LOG(WARN, "Id for default sub table isn't",
                      K(kDefaultColumnFamilyId), K(default_id));
          to_create = true;
        } else {
          cfh_map_.emplace(kDefaultColumnFamilyId, default_cfh);
        }
      } else { // cfh0 is null and default_cfh is null
        to_create = true;
      }
    }
    if (to_create) {
      ColumnFamilyHandle *handle = nullptr;
      int ret = 0;
      // table:0 for default column family, 1+ for user's column family
      ColumnFamilyDescriptor cf(kDefaultColumnFamilyName, cf_options);
      if (FAILED(dbfull()->TEST_create_subtable(cf, kDefaultColumnFamilyId, handle))) {
        XENGINE_LOG(ERROR, "Failed to create sub table!", K(ret));
        db_->DestroyColumnFamilyHandle(handle);
      } else {
        cfh_map_.emplace(kDefaultColumnFamilyId, handle);
        //dbfull()->SetDefaultColumnFamily(handle);
      }
      s = Status(ret);
    }
    //assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    //delete handles[0];
  }
  return s;
}
bool DBTestBase::IsDirectIOSupported() {
  util::EnvOptions env_options;
  env_options.use_mmap_writes = false;
  env_options.use_direct_writes = true;
  std::string tmp = TempFileName(dbname_, 999);
  Status s;
  {
//    unique_ptr<WritableFile> file;
    WritableFile *file = nullptr;
    s = env_->NewWritableFile(tmp, file, env_options);
  }
  if (s.ok()) {
    s = env_->DeleteFile(tmp);
  }
  return s.ok();
}

Status DBTestBase::Flush(int cf, bool wait) {
  FlushOptions flush_options;
  flush_options.wait = wait;
  if (cf == 0) {
    return db_->Flush(flush_options);
  } else {
    return db_->Flush(flush_options, get_column_family_handle(cf));
  }
}

Status DBTestBase::CompactRange(int cf, uint32_t compact_type) {
  if (0 == cf) {
    return db_->CompactRange(CompactRangeOptions(), nullptr, nullptr,
                             compact_type);
  } else {
    return db_->CompactRange(CompactRangeOptions(),
                             get_column_family_handle(cf), nullptr, nullptr,
                             compact_type);
  }
}

int DBTestBase::write_checkpoint()
{
  int32_t dummy_manifest_file_number = 0;
  return db_->do_manual_checkpoint(dummy_manifest_file_number);
}

void DBTestBase::schedule_shrink()
{
  (reinterpret_cast<DBImpl *>(db_))->TEST_schedule_shrink();
}

int DBTestBase::test_get_data_file_stats(const int64_t table_space_id,
    std::vector<storage::DataFileStatistics> &data_file_stats)
{
  return (reinterpret_cast<DBImpl *>(db_))->TEST_get_data_file_stats(table_space_id, data_file_stats);
}

Status DBTestBase::Put(const Slice& k, const Slice& v, WriteOptions wo) {
  /*
  if (kMergePut == option_config_) {
    return db_->Merge(wo, k, v);
  } else {
    return db_->Put(wo, k, v);
  }
  */
  return Put(0, k, v, wo);
}

Status DBTestBase::Put(int cf, const Slice& k, const Slice& v,
                       WriteOptions wo) {
  /*
  if (kMergePut == option_config_) {
    return db_->Merge(wo, handles_[cf], k, v);
  } else {
    return db_->Put(wo, handles_[cf], k, v);
  }
  */
  return db_->Put(wo, get_column_family_handle(cf), k, v);
}

Status DBTestBase::Merge(const Slice& k, const Slice& v, WriteOptions wo) {
  return db_->Merge(wo, k, v);
}

Status DBTestBase::Merge(int cf, const Slice& k, const Slice& v,
                         WriteOptions wo) {
  return db_->Merge(wo, get_column_family_handle(cf), k, v);
}

Status DBTestBase::Delete(const std::string& k) {
  return db_->Delete(WriteOptions(), k);
}

Status DBTestBase::Delete(int cf, const std::string& k) {
  return db_->Delete(WriteOptions(), get_column_family_handle(cf), k);
}

Status DBTestBase::SingleDelete(const std::string& k) {
  return db_->SingleDelete(WriteOptions(), k);
}

Status DBTestBase::SingleDelete(int cf, const std::string& k) {
  return db_->SingleDelete(WriteOptions(), get_column_family_handle(cf), k);
}

std::string DBTestBase::Get(const std::string& k, const Snapshot* snapshot) {
  ReadOptions options;
  options.verify_checksums = true;
  options.snapshot = snapshot;
  std::string result;
  Status s = db_->Get(options, k, &result);
  if (s.IsNotFound()) {
    result = "NOT_FOUND";
  } else if (!s.ok()) {
    result = s.ToString();
  }
  return result;
}

std::string DBTestBase::Get(int cf, const std::string& k,
                            const Snapshot* snapshot) {
  ReadOptions options;
  options.verify_checksums = true;
  options.snapshot = snapshot;
  std::string result;
  Status s = db_->Get(options, get_column_family_handle(cf), k, &result);
  if (s.IsNotFound()) {
    result = "NOT_FOUND";
  } else if (!s.ok()) {
    result = s.ToString();
  }
  return result;
}

uint64_t DBTestBase::GetNumSnapshots() {
  uint64_t int_num;
  EXPECT_TRUE(dbfull()->GetIntProperty("xengine.num-snapshots", &int_num));
  return int_num;
}

uint64_t DBTestBase::GetTimeOldestSnapshots() {
  uint64_t int_num;
  EXPECT_TRUE(
      dbfull()->GetIntProperty("xengine.oldest-snapshot-time", &int_num));
  return int_num;
}

// Return a string that contains all key,value pairs in order,
// formatted like "(k1->v1)(k2->v2)".
std::string DBTestBase::Contents(int cf) {
  std::vector<std::string> forward;
  std::string result;
  Iterator* iter = (cf == 0) ? db_->NewIterator(ReadOptions())
                             : db_->NewIterator(ReadOptions(), get_column_family_handle(cf));
  for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
    std::string s = IterStatus(iter);
    result.push_back('(');
    result.append(s);
    result.push_back(')');
    forward.push_back(s);
  }

  // Check reverse iteration results are the reverse of forward results
  unsigned int matched = 0;
  for (iter->SeekToLast(); iter->Valid(); iter->Prev()) {
    EXPECT_LT(matched, forward.size());
    EXPECT_EQ(IterStatus(iter), forward[forward.size() - matched - 1]);
    matched++;
  }
  EXPECT_EQ(matched, forward.size());

  delete iter;
  return result;
}

std::string DBTestBase::AllEntriesFor(const Slice& user_key, int cf) {
  util::Arena arena;
  auto options = CurrentOptions();
  InternalKeyComparator icmp(options.comparator);
  RangeDelAggregator range_del_agg(icmp, {} /* snapshots */);
  ScopedArenaIterator iter;
  if (cf == 0) {
    iter.set(dbfull()->NewInternalIterator(&arena, &range_del_agg));
  } else {
    iter.set(
        dbfull()->NewInternalIterator(&arena, &range_del_agg, get_column_family_handle(cf)));
  }
  InternalKey target(user_key, kMaxSequenceNumber, kTypeValue);
  iter->Seek(target.Encode());
  std::string result;
  if (!iter->status().ok()) {
    result = iter->status().ToString();
  } else {
    result = "[ ";
    bool first = true;
    while (iter->Valid()) {
      ParsedInternalKey ikey(Slice(), 0, kTypeValue);
      if (!ParseInternalKey(iter->key(), &ikey)) {
        result += "CORRUPTED";
      } else {
        if (!last_options_.comparator->Equal(ikey.user_key, user_key)) {
          break;
        }
        if (!first) {
          result += ", ";
        }
        first = false;
        switch (ikey.type) {
          case kTypeValue:
            result += iter->value().ToString();
            break;
          case kTypeMerge:
            // keep it the same as kTypeValue for testing kMergePut
            result += iter->value().ToString();
            break;
          case kTypeDeletion:
            result += "DEL";
            break;
          case kTypeSingleDeletion:
            result += "SDEL";
            break;
          default:
            assert(false);
            break;
        }
      }
      iter->Next();
    }
    if (!first) {
      result += " ";
    }
    result += "]";
  }
  return result;
}

#ifndef ROCKSDB_LITE
int DBTestBase::NumSortedRuns(int cf) {
  ColumnFamilyMetaData cf_meta;
  if (cf == 0) {
    db_->GetColumnFamilyMetaData(&cf_meta);
  } else {
    db_->GetColumnFamilyMetaData(get_column_family_handle(cf), &cf_meta);
  }
  int num_sr = static_cast<int>(cf_meta.levels[0].files.size());
  for (size_t i = 1U; i < cf_meta.levels.size(); i++) {
    if (cf_meta.levels[i].files.size() > 0) {
      num_sr++;
    }
  }
  return num_sr;
}

uint64_t DBTestBase::TotalSize(int cf) {
  ColumnFamilyMetaData cf_meta;
  if (cf == 0) {
    db_->GetColumnFamilyMetaData(&cf_meta);
  } else {
    db_->GetColumnFamilyMetaData(get_column_family_handle(cf), &cf_meta);
  }
  return cf_meta.size;
}

uint64_t DBTestBase::SizeAtLevel(int level) {
  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);
  uint64_t sum = 0;
  for (const auto& m : metadata) {
    if (m.level == level) {
      sum += m.size;
    }
  }
  return sum;
}

size_t DBTestBase::TotalLiveFiles(int cf) {
  ColumnFamilyMetaData cf_meta;
  if (cf == 0) {
    db_->GetColumnFamilyMetaData(&cf_meta);
  } else {
    db_->GetColumnFamilyMetaData(get_column_family_handle(cf), &cf_meta);
  }
  size_t num_files = 0;
  for (auto& level : cf_meta.levels) {
    num_files += level.files.size();
  }
  return num_files;
}

size_t DBTestBase::CountLiveFiles() {
  std::vector<LiveFileMetaData> metadata;
  db_->GetLiveFilesMetaData(&metadata);
  return metadata.size();
}
#endif  // ROCKSDB_LITE

int DBTestBase::NumTableFilesAtLevel(int level, int cf) {
  std::string property;
  if (cf == 0) {
    // default cfd
    EXPECT_TRUE(db_->GetProperty(
        "xengine.num-files-at-level" + NumberToString(level), &property));
  } else {
    EXPECT_TRUE(db_->GetProperty(
        get_column_family_handle(cf), "xengine.num-files-at-level" + NumberToString(level),
        &property));
  }
  return atoi(property.c_str());
}

double DBTestBase::CompressionRatioAtLevel(int level, int cf) {
  std::string property;
  if (cf == 0) {
    // default cfd
    EXPECT_TRUE(db_->GetProperty(
        "xengine.compression-ratio-at-level" + NumberToString(level),
        &property));
  } else {
    EXPECT_TRUE(db_->GetProperty(
        get_column_family_handle(cf),
        "xengine.compression-ratio-at-level" + NumberToString(level),
        &property));
  }
  return std::stod(property);
}

int DBTestBase::TotalTableFiles(int cf, int levels) {
  if (levels == -1) {
    levels = (cf == 0) ? db_->NumberLevels() : db_->NumberLevels(get_column_family_handle(1));
  }
  int result = 0;
  for (int level = 0; level < levels; level++) {
    result += NumTableFilesAtLevel(level, cf);
  }
  return result;
}

// Return spread of files per level
std::string DBTestBase::FilesPerLevel(int cf) {
  int num_levels =
      (cf == 0) ? db_->NumberLevels() : db_->NumberLevels(get_column_family_handle(1));
  std::string result;
  size_t last_non_zero_offset = 0;
  for (int level = 0; level < num_levels; level++) {
    int f = NumTableFilesAtLevel(level, cf);
    char buf[100];
    snprintf(buf, sizeof(buf), "%s%d", (level ? "," : ""), f);
    result += buf;
    if (f > 0) {
      last_non_zero_offset = result.size();
    }
  }
  result.resize(last_non_zero_offset);
  return result;
}

size_t DBTestBase::CountFiles() {
  std::vector<std::string> files;
  env_->GetChildren(dbname_, &files);

  std::vector<std::string> logfiles;
  if (dbname_ != last_options_.wal_dir) {
    env_->GetChildren(last_options_.wal_dir, &logfiles);
  }

  return files.size() + logfiles.size();
}

uint64_t DBTestBase::Size(const Slice& start, const Slice& limit, int cf) {
  Range r(start, limit);
  uint64_t size;
  if (cf == 0) {
    db_->GetApproximateSizes(&r, 1, &size);
  } else {
    db_->GetApproximateSizes(get_column_family_handle(1), &r, 1, &size);
  }
  return size;
}

void DBTestBase::Compact(int cf, const Slice& start, const Slice& limit,
                         uint32_t target_path_id) {
  CompactRangeOptions compact_options;
  compact_options.target_path_id = target_path_id;
  ASSERT_OK(db_->CompactRange(compact_options, get_column_family_handle(cf), &start, &limit));
}

void DBTestBase::Compact(int cf, const Slice& start, const Slice& limit) {
  ASSERT_OK(
      db_->CompactRange(CompactRangeOptions(), get_column_family_handle(cf), &start, &limit));
}

void DBTestBase::Compact(const Slice& start, const Slice& limit) {
  ASSERT_OK(db_->CompactRange(CompactRangeOptions(), &start, &limit));
}

// Do n memtable compactions, each of which produces an sstable
// covering the range [small,large].
void DBTestBase::MakeTables(int n, const std::string& small,
                            const std::string& large, int cf) {
  for (int i = 0; i < n; i++) {
    ASSERT_OK(Put(cf, small, "begin"));
    ASSERT_OK(Put(cf, large, "end"));
    ASSERT_OK(Flush(cf));
    MoveFilesToLevel(n - i - 1, cf);
  }
}

// Prevent pushing of new sstables into deeper levels by adding
// tables that cover a specified range to all levels.
void DBTestBase::FillLevels(const std::string& smallest,
                            const std::string& largest, int cf) {
  MakeTables(db_->NumberLevels(get_column_family_handle(cf)), smallest, largest, cf);
}

void DBTestBase::MoveFilesToLevel(int level, int cf) {
  for (int l = 0; l < level; ++l) {
    if (cf > 0) {
      dbfull()->TEST_CompactRange(l, nullptr, nullptr, get_column_family_handle(cf));
    } else {
      dbfull()->TEST_CompactRange(l, nullptr, nullptr);
    }
  }
}

void DBTestBase::DumpFileCounts(const char* label) {
  fprintf(stderr, "---\n%s:\n", label);
  fprintf(stderr, "maxoverlap: %" PRIu64 "\n",
          dbfull()->TEST_MaxNextLevelOverlappingBytes());
  for (int level = 0; level < db_->NumberLevels(); level++) {
    int num = NumTableFilesAtLevel(level);
    if (num > 0) {
      fprintf(stderr, "  level %3d : %d files\n", level, num);
    }
  }
}

std::string DBTestBase::DumpSSTableList() {
  std::string property;
  db_->GetProperty("xengine.sstables", &property);
  return property;
}

void DBTestBase::GetSstFiles(std::string path,
                             std::vector<std::string>* files) {
  env_->GetChildren(path, files);

  files->erase(std::remove_if(files->begin(), files->end(),
                              [](std::string name) {
                                uint64_t number;
                                FileType type;
                                return !(ParseFileName(name, &number, &type) &&
                                         type == kTableFile);
                              }),
               files->end());
}

int DBTestBase::GetSstFileCount(std::string path) {
  std::vector<std::string> files;
  GetSstFiles(path, &files);
  return static_cast<int>(files.size());
}

// this will generate non-overlapping files since it keeps increasing key_idx
void DBTestBase::GenerateNewFile(int cf, Random* rnd, int* key_idx,
                                 bool nowait) {
  for (int i = 0; i < KNumKeysByGenerateNewFile; i++) {
    ASSERT_OK(Put(cf, Key(*key_idx), RandomString(rnd, (i == 99) ? 1 : 990)));
    (*key_idx)++;
  }
  if (!nowait) {
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }
}

// this will generate non-overlapping files since it keeps increasing key_idx
void DBTestBase::GenerateNewFile(Random* rnd, int* key_idx, bool nowait) {
  for (int i = 0; i < KNumKeysByGenerateNewFile; i++) {
    ASSERT_OK(Put(Key(*key_idx), RandomString(rnd, (i == 99) ? 1 : 990)));
    (*key_idx)++;
  }
  if (!nowait) {
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }
}

const int DBTestBase::kNumKeysByGenerateNewRandomFile = 51;

void DBTestBase::GenerateNewRandomFile(Random* rnd, bool nowait) {
  for (int i = 0; i < kNumKeysByGenerateNewRandomFile; i++) {
    ASSERT_OK(Put("key" + RandomString(rnd, 7), RandomString(rnd, 2000)));
  }
  ASSERT_OK(Put("key" + RandomString(rnd, 7), RandomString(rnd, 200)));
  if (!nowait) {
    dbfull()->TEST_WaitForFlushMemTable();
    dbfull()->TEST_WaitForCompact();
  }
}

std::string DBTestBase::IterStatus(Iterator* iter) {
  std::string result;
  if (iter->Valid()) {
    result = iter->key().ToString() + "->" + iter->value().ToString();
  } else {
    result = "(invalid)";
  }
  return result;
}

Options DBTestBase::OptionsForLogIterTest() {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.WAL_ttl_seconds = 1000;
  return options;
}

std::string DBTestBase::DummyString(size_t len, char c) {
  return std::string(len, c);
}

void DBTestBase::VerifyIterLast(std::string expected_key, int cf) {
  Iterator* iter;
  ReadOptions ro;
  if (cf == 0) {
    iter = db_->NewIterator(ro);
  } else {
    iter = db_->NewIterator(ro, get_column_family_handle(cf));
  }
  iter->SeekToLast();
  ASSERT_EQ(IterStatus(iter), expected_key);
  delete iter;
}

// Used to test InplaceUpdate

// If previous value is nullptr or delta is > than previous value,
//   sets newValue with delta
// If previous value is not empty,
//   updates previous value with 'b' string of previous value size - 1.
UpdateStatus DBTestBase::updateInPlaceSmallerSize(char* prevValue,
                                                  uint32_t* prevSize,
                                                  Slice delta,
                                                  std::string* newValue) {
  if (prevValue == nullptr) {
    *newValue = std::string(delta.size(), 'c');
    return UpdateStatus::UPDATED;
  } else {
    *prevSize = *prevSize - 1;
    std::string str_b = std::string(*prevSize, 'b');
    memcpy(prevValue, str_b.c_str(), str_b.size());
    return UpdateStatus::UPDATED_INPLACE;
  }
}

UpdateStatus DBTestBase::updateInPlaceSmallerVarintSize(char* prevValue,
                                                        uint32_t* prevSize,
                                                        Slice delta,
                                                        std::string* newValue) {
  if (prevValue == nullptr) {
    *newValue = std::string(delta.size(), 'c');
    return UpdateStatus::UPDATED;
  } else {
    *prevSize = 1;
    std::string str_b = std::string(*prevSize, 'b');
    memcpy(prevValue, str_b.c_str(), str_b.size());
    return UpdateStatus::UPDATED_INPLACE;
  }
}

UpdateStatus DBTestBase::updateInPlaceLargerSize(char* prevValue,
                                                 uint32_t* prevSize,
                                                 Slice delta,
                                                 std::string* newValue) {
  *newValue = std::string(delta.size(), 'c');
  return UpdateStatus::UPDATED;
}

UpdateStatus DBTestBase::updateInPlaceNoAction(char* prevValue,
                                               uint32_t* prevSize, Slice delta,
                                               std::string* newValue) {
  return UpdateStatus::UPDATE_FAILED;
}

// Utility method to test InplaceUpdate
void DBTestBase::validateNumberOfEntries(int numValues, int cf) {
  util::Arena arena;
  ScopedArenaIterator iter;
  auto options = CurrentOptions();
  InternalKeyComparator icmp(options.comparator);
  RangeDelAggregator range_del_agg(icmp, {} /* snapshots */);
  if (cf != 0) {
    iter.set(
        dbfull()->NewInternalIterator(&arena, &range_del_agg, get_column_family_handle(cf)));
  } else {
    iter.set(dbfull()->NewInternalIterator(&arena, &range_del_agg));
  }
  iter->SeekToFirst();
  ASSERT_EQ(iter->status().ok(), true);
  int seq = numValues;
  while (iter->Valid()) {
    ParsedInternalKey ikey;
    ikey.sequence = -1;
    ASSERT_EQ(ParseInternalKey(iter->key(), &ikey), true);

    // checks sequence number for updates
    ASSERT_EQ(ikey.sequence, (unsigned)seq--);
    iter->Next();
  }
  ASSERT_EQ(0, seq);
}

void DBTestBase::CopyFile(const std::string& source,
                          const std::string& destination, uint64_t size) {
  const util::EnvOptions soptions;
//  unique_ptr<util::SequentialFile> srcfile;
  SequentialFile *srcfile = nullptr;
  ASSERT_OK(env_->NewSequentialFile(source, srcfile, soptions));
//  unique_ptr<WritableFile> destfile;
  WritableFile *destfile = nullptr;
  ASSERT_OK(env_->NewWritableFile(destination, destfile, soptions));

  if (size == 0) {
    // default argument means copy everything
    ASSERT_OK(env_->GetFileSize(source, &size));
  }

  char buffer[4096];
  Slice slice;
  while (size > 0) {
    uint64_t one = std::min(uint64_t(sizeof(buffer)), size);
    ASSERT_OK(srcfile->Read(one, &slice, buffer));
    ASSERT_OK(destfile->Append(slice));
    size -= slice.size();
  }
  ASSERT_OK(destfile->Close());
}

std::unordered_map<std::string, uint64_t> DBTestBase::GetAllSSTFiles(
    uint64_t* total_size) {
  std::unordered_map<std::string, uint64_t> res;

  if (total_size) {
    *total_size = 0;
  }
  std::vector<std::string> files;
  env_->GetChildren(dbname_, &files);
  for (auto& file_name : files) {
    uint64_t number;
    FileType type;
    std::string file_path = dbname_ + "/" + file_name;
    if (ParseFileName(file_name, &number, &type) && type == kTableFile) {
      uint64_t file_size = 0;
      env_->GetFileSize(file_path, &file_size);
      res[file_path] = file_size;
      if (total_size) {
        *total_size += file_size;
      }
    }
  }
  return res;
}

std::vector<std::uint64_t> DBTestBase::ListTableFiles(Env* env,
                                                      const std::string& path) {
  std::vector<std::string> files;
  std::vector<uint64_t> file_numbers;
  env->GetChildren(path, &files);
  uint64_t number;
  FileType type;
  for (size_t i = 0; i < files.size(); ++i) {
    if (ParseFileName(files[i], &number, &type)) {
      if (type == kTableFile) {
        file_numbers.push_back(number);
      }
    }
  }
  return file_numbers;
}

void DBTestBase::VerifyDBFromMap(std::map<std::string, std::string> true_data,
                                 size_t* total_reads_res, bool tailing_iter,
                                 std::map<std::string, Status> status) {
  size_t total_reads = 0;

  for (auto& kv : true_data) {
    Status s = status[kv.first];
    if (s.ok()) {
      ASSERT_EQ(Get(kv.first), kv.second);
    } else {
      std::string value;
      ASSERT_EQ(s, db_->Get(ReadOptions(), kv.first, &value));
    }
    total_reads++;
  }

  // Normal Iterator
  {
    int iter_cnt = 0;
    ReadOptions ro;
    ro.total_order_seek = true;
    Iterator* iter = db_->NewIterator(ro);
    // Verify Iterator::Next()
    iter_cnt = 0;
    auto data_iter = true_data.begin();
    Status s;
    for (iter->SeekToFirst(); iter->Valid(); iter->Next(), data_iter++) {
      ASSERT_EQ(iter->key().ToString(), data_iter->first);
      Status current_status = status[data_iter->first];
      if (!current_status.ok()) {
        s = current_status;
      }
      ASSERT_EQ(iter->status(), s);
      if (current_status.ok()) {
        ASSERT_EQ(iter->value().ToString(), data_iter->second);
      }
      iter_cnt++;
      total_reads++;
    }
    ASSERT_EQ(data_iter, true_data.end()) << iter_cnt << " / "
                                          << true_data.size();
    delete iter;

    // Verify Iterator::Prev()
    // Use a new iterator to make sure its status is clean.
    iter = db_->NewIterator(ro);
    iter_cnt = 0;
    s = Status::OK();
    auto data_rev = true_data.rbegin();
    for (iter->SeekToLast(); iter->Valid(); iter->Prev(), data_rev++) {
      ASSERT_EQ(iter->key().ToString(), data_rev->first);
      Status current_status = status[data_rev->first];
      if (!current_status.ok()) {
        s = current_status;
      }
      ASSERT_EQ(iter->status(), s);
      if (current_status.ok()) {
        ASSERT_EQ(iter->value().ToString(), data_rev->second);
      }
      iter_cnt++;
      total_reads++;
    }
    ASSERT_EQ(data_rev, true_data.rend()) << iter_cnt << " / "
                                          << true_data.size();

    // Verify Iterator::Seek()
    for (auto kv : true_data) {
      iter->Seek(kv.first);
      ASSERT_EQ(kv.first, iter->key().ToString());
      ASSERT_EQ(kv.second, iter->value().ToString());
      total_reads++;
    }
    delete iter;
  }

  if (tailing_iter) {
#ifndef ROCKSDB_LITE
    // Tailing iterator
    int iter_cnt = 0;
    ReadOptions ro;
    ro.tailing = true;
    ro.total_order_seek = true;
    Iterator* iter = db_->NewIterator(ro);

    // Verify ForwardIterator::Next()
    iter_cnt = 0;
    auto data_iter = true_data.begin();
    for (iter->SeekToFirst(); iter->Valid(); iter->Next(), data_iter++) {
      ASSERT_EQ(iter->key().ToString(), data_iter->first);
      ASSERT_EQ(iter->value().ToString(), data_iter->second);
      iter_cnt++;
      total_reads++;
    }
    ASSERT_EQ(data_iter, true_data.end()) << iter_cnt << " / "
                                          << true_data.size();

    // Verify ForwardIterator::Seek()
    for (auto kv : true_data) {
      iter->Seek(kv.first);
      ASSERT_EQ(kv.first, iter->key().ToString());
      ASSERT_EQ(kv.second, iter->value().ToString());
      total_reads++;
    }

    delete iter;
#endif  // ROCKSDB_LITE
  }

  if (total_reads_res) {
    *total_reads_res = total_reads;
  }
}

void DBTestBase::VerifyDBInternal(
    std::vector<std::pair<std::string, std::string>> true_data) {
  util::Arena arena;
  InternalKeyComparator icmp(last_options_.comparator);
  RangeDelAggregator range_del_agg(icmp, {});
  auto iter = dbfull()->NewInternalIterator(&arena, &range_del_agg);
  iter->SeekToFirst();
  for (auto p : true_data) {
    ASSERT_TRUE(iter->Valid());
    ParsedInternalKey ikey;
    ASSERT_TRUE(ParseInternalKey(iter->key(), &ikey));
    ASSERT_EQ(p.first, ikey.user_key);
    ASSERT_EQ(p.second, iter->value());
    iter->Next();
  };
  ASSERT_FALSE(iter->Valid());
  iter->~InternalIterator();
}

#ifndef ROCKSDB_LITE

uint64_t DBTestBase::GetNumberOfSstFilesForColumnFamily(
    DB* db, std::string column_family_name) {
  std::vector<LiveFileMetaData> metadata;
  db->GetLiveFilesMetaData(&metadata);
  uint64_t result = 0;
  for (auto& fileMetadata : metadata) {
    result += (fileMetadata.column_family_name == column_family_name);
  }
  return result;
}
#endif  // ROCKSDB_LITE

uint32_t DBTestBase::get_next_column_family_id() const {
  uint32_t next_id = 0;
  for (auto const &iter : cfh_map_) {
    if (iter.first > next_id) {
      next_id = iter.first;
    }
  }
  return ++next_id;
}

void DBTestBase::get_column_family_handles(
    std::vector<ColumnFamilyHandle*> &handles) const {
  handles.clear();
  for (auto const &iter : cfh_map_) {
    handles.emplace_back(iter.second);
  }
}

ColumnFamilyHandle* DBTestBase::get_column_family_handle(int64_t cf) const {
  ColumnFamilyHandle* h = nullptr;
  if (cf >= 0) {
    auto iter = cfh_map_.find((uint32_t)cf);
    if (iter != cfh_map_.end()) {
      h = iter->second;
    }
  }
  return h;
}
}
}  // namespace xengine
