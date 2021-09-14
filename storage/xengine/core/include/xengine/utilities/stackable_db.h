/*
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <map>
#include <string>
#include "xengine/db.h"

#ifdef _WIN32
// Windows API macro interference
#undef DeleteFile
#endif

namespace xengine {
namespace db {
class ColumnFamilyHandle;
struct MiniTables;
struct CreateSubTableArgs;
}

namespace util {

using xengine::db::ColumnFamilyHandle;
using xengine::db::MiniTables;
using xengine::db::Snapshot;
using xengine::db::Range;
using xengine::db::Iterator;

// This class contains APIs to stack DB wrappers.Eg. Stack TTL over base db
class StackableDB : public db::DB {
 public:
  // StackableDB is the owner of db now!
  explicit StackableDB(DB* db) : db_(db) {}

  ~StackableDB() { 
//   delete db_;
    MOD_DELETE_OBJECT(DB, db_);
  }

  virtual DB* GetBaseDB() { return db_; }

  virtual DB* GetRootDB() override { return db_->GetRootDB(); }

  virtual common::Status CreateColumnFamily(db::CreateSubTableArgs &args, ColumnFamilyHandle** handle) override {
    return db_->CreateColumnFamily(args, handle);
  }

  virtual common::Status DropColumnFamily(
      ColumnFamilyHandle* column_family) override {
    return db_->DropColumnFamily(column_family);
  }

  virtual common::Status DestroyColumnFamilyHandle(
      ColumnFamilyHandle* column_family) override {
    return db_->DestroyColumnFamilyHandle(column_family);
  }

  using DB::Put;
  virtual common::Status Put(const common::WriteOptions& options,
                             ColumnFamilyHandle* column_family,
                             const common::Slice& key,
                             const common::Slice& val) override {
    return db_->Put(options, column_family, key, val);
  }

  using DB::Get;
  virtual common::Status Get(const common::ReadOptions& options,
                             ColumnFamilyHandle* column_family,
                             const common::Slice& key,
                             common::PinnableSlice* value) override {
    return db_->Get(options, column_family, key, value);
  }

  using DB::MultiGet;
  virtual std::vector<common::Status> MultiGet(
      const common::ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_family,
      const std::vector<common::Slice>& keys,
      std::vector<std::string>* values) override {
    return db_->MultiGet(options, column_family, keys, values);
  }

  using DB::InstallSstExternal;
  virtual common::Status InstallSstExternal(ColumnFamilyHandle* column_family,
                                            MiniTables* mtables) override {
    return db_->InstallSstExternal(column_family, mtables);
  }
  using DB::GetStorageLogger;
  virtual storage::StorageLogger * GetStorageLogger() {
    return db_->GetStorageLogger();
  }


  using DB::KeyMayExist;
  virtual bool KeyMayExist(const common::ReadOptions& options,
                           ColumnFamilyHandle* column_family,
                           const common::Slice& key, std::string* value,
                           bool* value_found = nullptr) override {
    return db_->KeyMayExist(options, column_family, key, value, value_found);
  }

  using DB::Delete;
  virtual common::Status Delete(const common::WriteOptions& wopts,
                                ColumnFamilyHandle* column_family,
                                const common::Slice& key) override {
    return db_->Delete(wopts, column_family, key);
  }

  using DB::SingleDelete;
  virtual common::Status SingleDelete(const common::WriteOptions& wopts,
                                      ColumnFamilyHandle* column_family,
                                      const common::Slice& key) override {
    return db_->SingleDelete(wopts, column_family, key);
  }

  using DB::Merge;
  virtual common::Status Merge(const common::WriteOptions& options,
                               ColumnFamilyHandle* column_family,
                               const common::Slice& key,
                               const common::Slice& value) override {
    return db_->Merge(options, column_family, key, value);
  }

  virtual common::Status Write(const common::WriteOptions& opts,
                               db::WriteBatch* updates) override {
    return db_->Write(opts, updates);
  }

  using DB::NewIterator;
  virtual Iterator* NewIterator(const common::ReadOptions& opts,
                                ColumnFamilyHandle* column_family) override {
    return db_->NewIterator(opts, column_family);
  }

  virtual common::Status NewIterators(
      const common::ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_families,
      std::vector<Iterator*>* iterators) override {
    return db_->NewIterators(options, column_families, iterators);
  }

  virtual const Snapshot* GetSnapshot() override { return db_->GetSnapshot(); }

  virtual void ReleaseSnapshot(const Snapshot* snapshot) override {
    return db_->ReleaseSnapshot(snapshot);
  }

  using DB::GetMapProperty;
  using DB::GetProperty;
  virtual bool GetProperty(ColumnFamilyHandle* column_family,
                           const common::Slice& property,
                           std::string* value) override {
    return db_->GetProperty(column_family, property, value);
  }
  virtual bool GetMapProperty(ColumnFamilyHandle* column_family,
                              const common::Slice& property,
                              std::map<std::string, double>* value) override {
    return db_->GetMapProperty(column_family, property, value);
  }

  using DB::GetIntProperty;
  virtual bool GetIntProperty(ColumnFamilyHandle* column_family,
                              const common::Slice& property,
                              uint64_t* value) override {
    return db_->GetIntProperty(column_family, property, value);
  }

  using DB::GetAggregatedIntProperty;
  virtual bool GetAggregatedIntProperty(const common::Slice& property,
                                        uint64_t* value) override {
    return db_->GetAggregatedIntProperty(property, value);
  }

  using DB::GetApproximateSizes;
  virtual void GetApproximateSizes(
      ColumnFamilyHandle* column_family, const Range* r, int n, uint64_t* sizes,
      uint8_t include_flags = INCLUDE_FILES) override {
    return db_->GetApproximateSizes(column_family, r, n, sizes, include_flags);
  }

  using DB::GetApproximateMemTableStats;
  virtual void GetApproximateMemTableStats(ColumnFamilyHandle* column_family,
                                           const Range& range,
                                           uint64_t* const count,
                                           uint64_t* const size) override {
    return db_->GetApproximateMemTableStats(column_family, range, count, size);
  }

  using DB::CompactRange;
  virtual common::Status CompactRange(
      const common::CompactRangeOptions& options,
      ColumnFamilyHandle* column_family,
      const common::Slice* begin,
      const common::Slice* end,
      const uint32_t manual_compact_type = 8/* Stream*/) override {
    return db_->CompactRange(options, column_family, begin, end, manual_compact_type);
  }

  virtual common::Status CompactRange(
      const common::CompactRangeOptions& options,
      const common::Slice* begin,
      const common::Slice* end,
      const uint32_t compact_type = 8) override {
    return db_->CompactRange(options, begin, end, compact_type);
  }

  using DB::CompactFiles;
  virtual common::Status CompactFiles(
      const common::CompactionOptions& compact_options,
      ColumnFamilyHandle* column_family,
      const std::vector<std::string>& input_file_names, const int output_level,
      const int output_path_id = -1) override {
    return db_->CompactFiles(compact_options, column_family, input_file_names,
                             output_level, output_path_id);
  }

  virtual common::Status PauseBackgroundWork() override {
    return db_->PauseBackgroundWork();
  }
  virtual common::Status ContinueBackgroundWork() override {
    return db_->ContinueBackgroundWork();
  }

  virtual common::Status EnableAutoCompaction(
      const std::vector<ColumnFamilyHandle*>& column_family_handles) override {
    return db_->EnableAutoCompaction(column_family_handles);
  }

  using DB::NumberLevels;
  virtual int NumberLevels(ColumnFamilyHandle* column_family) override {
    return db_->NumberLevels(column_family);
  }

  using DB::MaxMemCompactionLevel;
  virtual int MaxMemCompactionLevel(
      ColumnFamilyHandle* column_family) override {
    return db_->MaxMemCompactionLevel(column_family);
  }

  using DB::Level0StopWriteTrigger;
  virtual int Level0StopWriteTrigger(
      ColumnFamilyHandle* column_family) override {
    return db_->Level0StopWriteTrigger(column_family);
  }

  virtual const std::string& GetName() const override { return db_->GetName(); }

  virtual util::Env* GetEnv() const override { return db_->GetEnv(); }

  using DB::GetOptions;
  virtual common::Options GetOptions(
      ColumnFamilyHandle* column_family) const override {
    return db_->GetOptions(column_family);
  }

  using DB::GetDBOptions;
  virtual common::DBOptions GetDBOptions() const override {
    return db_->GetDBOptions();
  }

  using DB::Flush;
  virtual common::Status Flush(const common::FlushOptions& fopts,
                               ColumnFamilyHandle* column_family) override {
    return db_->Flush(fopts, column_family);
  }

  virtual common::Status SyncWAL() override { return db_->SyncWAL(); }

#ifndef ROCKSDB_LITE

  virtual common::Status DisableFileDeletions() override {
    return db_->DisableFileDeletions();
  }

  virtual common::Status EnableFileDeletions(bool force) override {
    return db_->EnableFileDeletions(force);
  }

  virtual void GetLiveFilesMetaData(
      std::vector<common::LiveFileMetaData>* metadata) override {
    db_->GetLiveFilesMetaData(metadata);
  }

  virtual void GetColumnFamilyMetaData(
      ColumnFamilyHandle* column_family,
      common::ColumnFamilyMetaData* cf_meta) override {
    db_->GetColumnFamilyMetaData(column_family, cf_meta);
  }

#endif  // ROCKSDB_LITE

  virtual common::Status GetLiveFiles(std::vector<std::string>& vec,
                                      uint64_t* mfs,
                                      bool flush_memtable = true) override {
    return db_->GetLiveFiles(vec, mfs, flush_memtable);
  }

  virtual common::SequenceNumber GetLatestSequenceNumber() const override {
    return db_->GetLatestSequenceNumber();
  }

  virtual common::Status GetSortedWalFiles(db::VectorLogPtr& files) override {
    return db_->GetSortedWalFiles(files);
  }

  virtual common::Status DeleteFile(std::string name) override {
    return db_->DeleteFile(name);
  }

  virtual common::Status GetDbIdentity(std::string& identity) const override {
    return db_->GetDbIdentity(identity);
  }

  virtual int reset_pending_shrink(const uint64_t subtable_id) override {
    return db_->reset_pending_shrink(subtable_id);
  }

  using DB::SetOptions;
  virtual common::Status SetOptions(
      const std::unordered_map<std::string, std::string>& new_options) {
    return db_->SetOptions(new_options);
  }

  virtual common::Status SetOptions(
      ColumnFamilyHandle* column_family_handle,
      const std::unordered_map<std::string, std::string>& new_options)
      override {
    return db_->SetOptions(column_family_handle, new_options);
  }

  virtual common::Status SetDBOptions(
      const std::unordered_map<std::string, std::string>& new_options)
      override {
    return db_->SetDBOptions(new_options);
  }

  using DB::GetPropertiesOfAllTables;
  virtual common::Status GetPropertiesOfAllTables(
      ColumnFamilyHandle* column_family,
      db::TablePropertiesCollection* props) override {
    return db_->GetPropertiesOfAllTables(column_family, props);
  }

  using DB::GetPropertiesOfTablesInRange;
  virtual common::Status GetPropertiesOfTablesInRange(
      ColumnFamilyHandle* column_family, const Range* range, std::size_t n,
      db::TablePropertiesCollection* props) override {
    return db_->GetPropertiesOfTablesInRange(column_family, range, n, props);
  }

  virtual common::Status GetUpdatesSince(
      common::SequenceNumber seq_number,
      unique_ptr<db::TransactionLogIterator>* iter,
      const db::TransactionLogIterator::ReadOptions& read_options) override {
    return db_->GetUpdatesSince(seq_number, iter, read_options);
  }

  virtual ColumnFamilyHandle* DefaultColumnFamily() const override {
    return db_->DefaultColumnFamily();
  }

  virtual int shrink_table_space(int32_t table_space_id) override {
    return db_->shrink_table_space(table_space_id);
  }

  virtual int get_all_subtable(
    std::vector<xengine::db::ColumnFamilyHandle*> &subtables) const override {
    return db_->get_all_subtable(subtables);
  }

  virtual int return_all_subtable(std::vector<xengine::db::ColumnFamilyHandle*> &subtables) override {
    return db_->return_all_subtable(subtables);
  }

  virtual db::SuperVersion *GetAndRefSuperVersion(db::ColumnFamilyData *cfd) override {
    return db_->GetAndRefSuperVersion(cfd);
  }

  virtual void ReturnAndCleanupSuperVersion(db::ColumnFamilyData *cfd, db::SuperVersion *sv) override {
    db_->ReturnAndCleanupSuperVersion(cfd, sv);
  }

  virtual int get_data_file_stats(std::vector<storage::DataFileStatistics> &data_file_stats) override
  {
    return db_->get_data_file_stats(data_file_stats);
  }

  virtual std::list<storage::CompactionJobStatsInfo*> &get_compaction_history(std::mutex **mu,
          storage::CompactionJobStatsInfo **sum) override {
    return db_->get_compaction_history(mu, sum);
  }

  bool get_columnfamily_stats(ColumnFamilyHandle* column_family, int64_t &data_size,
                               int64_t &num_entries, int64_t &num_deletes, int64_t &disk_size) {
    return db_->get_columnfamily_stats(column_family, data_size, num_entries, num_deletes, disk_size);
  }

 protected:
  DB* db_;
};

}  // namespace db
}  // namespace xengine
