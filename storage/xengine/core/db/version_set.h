// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// The representation of a DBImpl consists of a set of Versions.  The
// newest version is called "current".  Older versions may be kept
// around to provide a consistent view to live iterators.
//
// Each Version keeps track of a set of Table files per level.  The
// entire set of versions is maintained in a VersionSet.
//
// Version,VersionSet are thread-compatible, but require external
// synchronization on all accesses.

#pragma once
#include <atomic>
#include <deque>
#include <limits>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "db/column_family.h"
#include "db/dbformat.h"
#include "db/file_indexer.h"
#include "db/log_reader.h"
#include "db/range_del_aggregator.h"
#include "db/table_cache.h"
#include "db/version_builder.h"
#include "db/version_edit.h"
#include "db/write_controller.h"
#include "monitoring/instrumented_mutex.h"
#include "options/db_options.h"
#include "port/port.h"
#include "storage/storage_manager.h"
#include "xengine/env.h"
#include "file_number.h"

namespace xengine {

namespace util {
class LogBuffer;
}

namespace table {
class MergeIteratorBuilder;
class InternalIterator;
}

namespace storage {
class StorageManagerTest;
struct CheckpointBlockHeader;
}

namespace db {

namespace log {
class Writer;
}

class WriteBufferManager;
class Compaction;
class LookupKey;
class MemTable;
class VersionSet;
class MergeContext;
class ColumnFamilySet;
class TableCache;

// Return the smallest index i such that file_level.files[i]->largest >= key.
// Return file_level.num_files if there is no such file.
// REQUIRES: "file_level.files" contains a sorted list of
// non-overlapping files.
extern int FindFile(const InternalKeyComparator& icmp,
                    const LevelFilesBrief& file_level,
                    const common::Slice& key);

// Returns true iff some file in "files" overlaps the user key range
// [*smallest,*largest].
// smallest==nullptr represents a key smaller than all keys in the DB.
// largest==nullptr represents a key largest than all keys in the DB.
// REQUIRES: If disjoint_sorted_files, file_level.files[]
// contains disjoint ranges in sorted order.
extern bool SomeFileOverlapsRange(const InternalKeyComparator& icmp,
                                  bool disjoint_sorted_files,
                                  const LevelFilesBrief& file_level,
                                  const common::Slice* smallest_user_key,
                                  const common::Slice* largest_user_key);

// Generate LevelFilesBrief from vector<FdWithKeyRange*>
// Would copy smallest_key and largest_key data to sequential memory
// arena: util::Arena used to allocate the memory
extern void DoGenerateLevelFilesBrief(LevelFilesBrief* file_level,
                                      const std::vector<FileMetaData*>& files,
                                      util::Arena* arena);

struct AllSubTable {
public:
  static int DUMMY;
  static void *const kAllSubtableInUse;
  static void *const kAllSubtableObsolete;

public:
  int64_t version_number_;
  std::atomic<int64_t> refs_;
  std::mutex *all_sub_table_mutex_;
  SubTableMap sub_table_map_;


  AllSubTable();
  AllSubTable(SubTableMap &sub_table_map, std::mutex *mutex);
  ~AllSubTable();
  void reset();
  void ref() {refs_.fetch_add(1, std::memory_order_relaxed); }
  bool unref() { return 1 == refs_.fetch_sub(1); }
  int add_sub_table(int64_t index_id, SubTable *sub_table);
  int remove_sub_table(int64_t index_id);
  int get_sub_table(int64_t, SubTable *&sub_table);
};

class AllSubTableGuard
{
public:
  AllSubTableGuard() = delete;
  AllSubTableGuard(const AllSubTableGuard &guard) = delete;
  AllSubTableGuard(GlobalContext *global_ctx);
  ~AllSubTableGuard();
  const SubTableMap &get_subtable_map()
  {
    return all_sub_table_->sub_table_map_;
  }
private:
  GlobalContext *global_ctx_;
  AllSubTable *all_sub_table_;
};

class VersionSet {
 public:
  // after this version increase we need recyle
  // meta background
  static const int META_VERSION_INTERVAL = 128;
  static const int EXTENT_ID_BLOCK = 16 * 1024;
  // start from 1
  static const int INVALID_META_LOG_NUMBER = 0;
  static const int64_t DEFAULT_BUFFER_SIZE = 2 * 1024 * 1024; //2MB
  VersionSet(const std::string& dbname,
             const common::ImmutableDBOptions* db_options,
             const util::EnvOptions& env_options, cache::Cache* table_cache,
             WriteBufferManager* write_buffer_manager,
             WriteController* write_controller);
  ~VersionSet();

  int init(GlobalContext *global_ctx);
  int recover_extent_space_manager();

  // Reads a manifest file and returns a list of column families in
  // column_families.
  static common::Status ListColumnFamilies(
      std::vector<std::string>* column_families, const std::string& dbname,
      util::Env* env);

  // Return the current manifest file number
  uint64_t manifest_file_number() const { return manifest_file_number_; }

  uint64_t checkpoint_file_number() const { return checkpoint_file_number_; }

  //uint64_t options_file_number() const { return options_file_number_; }

  //uint64_t pending_manifest_file_number() const {
  //  return pending_manifest_file_number_;
  //}

  //uint64_t current_next_file_number() const { return next_file_number_.load(); }

  uint64_t purge_checkpoint_file_number() const { return purge_checkpoint_file_number_; }

  uint64_t purge_manifest_file_number() const { return purge_manifest_file_number_; }

  // Allocate and return a new file number
  uint64_t NewFileNumber() { return next_file_number_.increase(1); }

  FileNumber& get_file_number_generator() { return next_file_number_; }

  // Return the last sequence number.
  uint64_t LastSequence() const {
    return last_sequence_.load(std::memory_order_acquire);
  }

  // Set the last sequence number to s.
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_.store(s, std::memory_order_release);
  }

  uint64_t LastAllocatedSequence() const {
    return last_allocated_sequence_.load(std::memory_order_acquire);
  }

  void SetLastAllocatedSequence(uint64_t s) {
    assert(s >= last_allocated_sequence_);
    assert(s >= last_sequence_);
    last_allocated_sequence_.store(s, std::memory_order_release);
  }

  uint64_t AllocateSequence(uint64_t sequence_size) {
    return last_allocated_sequence_.fetch_add(sequence_size);
  }
  // Mark the specified file number as used.
  // REQUIRED: this is only called during single-threaded recovery
  void MarkFileNumberUsedDuringRecovery(uint64_t number);

  // Return the log file number for the log file that is currently
  // being compacted, or zero if there is no such log file.
  uint64_t prev_log_number() const { return prev_log_number_; }

  // Returns the minimum log number such that all
  // log numbers less than or equal to it can be deleted
  uint64_t MinLogNumber() const {
    uint64_t min_log_num = std::numeric_limits<uint64_t>::max();
    for (auto cfd : *column_family_set_) {
      if (0 == cfd->GetID()) {
        //default cf is only used in unittest,
        //so the normal recyle wal point need not include the default_cf's recovery point
      } else if (min_log_num > static_cast<uint64_t>(cfd->get_recovery_point().log_file_number_)
          && !cfd->IsDropped()) {
        // It's safe to ignore dropped column families here:
        // cfd->IsDropped() becomes true after the drop is persisted in MANIFEST.
        min_log_num = static_cast<uint64_t>(cfd->get_recovery_point().log_file_number_);
      }
    }

    if (std::numeric_limits<uint64_t>::max() == min_log_num) {
      //reach here, indicate that only default_cf exist
      //only occur when bootstrap, because system_cf will been created after bootstrap
      //here, reset the min_log_number to zero, prevent recycle all wal files
      min_log_num = 0;
    }
    return min_log_num;
  }

  // Return the approximate size of data to be scanned for range [start, end)
  // in levels [start_level, end_level). If end_level == 0 it will search
  // through all non-empty levels
  uint64_t ApproximateSize(ColumnFamilyData* cfd, 
                           const db::Snapshot *sn,
                           const common::Slice& start,
                           const common::Slice& end,
                           int start_level,
                           int end_level,
                           int64_t estimate_cost_depth);

  // Return the size of the current manifest file
  uint64_t manifest_file_size() const { return manifest_file_size_; }

  log::Writer *get_descriptor_log() { return descriptor_log_.get(); }

  // This function doesn't support leveldb SST filenames
  void GetLiveFilesMetaData(std::vector<common::LiveFileMetaData>* metadata,
                            monitor::InstrumentedMutex *mu = nullptr);

  void GetObsoleteFiles(std::vector<FileMetaData*>* files,
                        std::vector<std::string>* manifest_filenames,
                        uint64_t min_pending_output);

  ColumnFamilySet* GetColumnFamilySet() { return column_family_set_.get(); }
  GlobalContext *get_global_ctx() const { return global_ctx_; }

  const util::EnvOptions& env_options() { return env_options_; }

  //static uint64_t GetNumLiveVersions(Version* dummy_versions);

  int recycle_storage_manager_meta(
    std::unordered_map<int32_t, common::SequenceNumber>* all,
    monitor::InstrumentedMutex &mu) const;

  // write all the storage manager checkpoint
  //common::Status write_storage_manager_checkpoint(
  //                        monitor::InstrumentedMutex *mu,
  //                        uint64_t file_number = 0);

  // recover the all storage manager checkpoint
  //common::Status recover_from_storage_manager_checkpoint(
  //               const common::ColumnFamilyOptions& cf_options,
  //               std::unordered_set<uint32_t> &delete_cfs);  

  // do nanual checkpoint for hot backup
  int do_manual_checkpoint(monitor::InstrumentedMutex *mu, bool update_current_file = true);
  int stream_log_extents(std::function<int(const char*, int, int64_t, int)> *stream_extent,
                         int64_t start, int64_t end, int dest_fd);

  /*
  int log_and_apply(ColumnFamilyData *cfd,
                    monitor::InstrumentedMutex *mu,
                    const storage::ChangeInfo &info,
                    bool update_current_file = true);
  */

  /*
  int replay_storage_manager_log(
    const uint64_t log_number, std::unordered_set<int32_t> &file_numbers,
                                std::unordered_set<uint32_t> &delete_cfs,
                                uint64_t manifest_numbe); 
  */

  std::atomic<uint64_t> &get_meta_log_number() { return meta_log_number_; }

  // hot backup
  int create_backup_snapshot(MetaSnapshotMap &meta_snapshot);
  int64_t last_manifest_file_size() const { return last_manifest_file_size_; }
  int64_t last_wal_file_size() const { return last_wal_file_size_; }

  struct LogReporter : public log::Reader::Reporter {
    common::Status* status;
    virtual void Corruption(size_t bytes, const common::Status& s) override {
      if (this->status->ok()) *this->status = s;
    }
  };

  const common::ImmutableDBOptions* get_db_options() const { return db_options_; }

  common::Status set_current_file(util::Directory* db_directory) {
    return SetCurrentFile(env_, dbname_, manifest_file_number_,
                         db_directory, file_number_, log_number_);
  }

  int add_sub_table(CreateSubTableArgs &args, bool write_log, bool is_replay, ColumnFamilyData *&sub_table);
  int remove_sub_table(ColumnFamilyData *sub_table, bool write_log, bool is_replay);
  int do_checkpoint(util::WritableFile *checkpoint_writer, storage::CheckpointHeader *header);
  int load_checkpoint(util::RandomAccessFile *checkpoint_reader, storage::CheckpointHeader *header);
  int replay(int64_t log_type, char *log_data, int64_t log_len);
  int recover_M02L0();
#ifndef NDEBUG
  void TEST_inject_write_checkpoint_failed();
  bool TEST_is_write_checkpoint_failed();
#endif
 private:
  struct ManifestWriter;

  friend class Version;
  friend class DBImpl;
  friend class storage::StorageManagerTest;

  // ApproximateSize helper
  /*
  uint64_t ApproximateSizeLevel0(Version* v, const LevelFilesBrief& files_brief,
                                 const common::Slice& start,
                                 const common::Slice& end);

  uint64_t ApproximateSize(Version* v, const FdWithKeyRange& f,
                           const common::Slice& key);
  */

  // Save current contents to *log
  common::Status WriteSnapshot(log::Writer* log);

  ColumnFamilyData* CreateColumnFamily(
      const common::ColumnFamilyOptions& cf_options, VersionEdit* edit);
  // create it at start up most of the time no need file_name
  common::Status create_descriptor_log_writer();
  int write_big_subtable(util::WritableFile *checkpoint_writer, ColumnFamilyData *sub_table);
  int read_big_subtable(util::RandomAccessFile *checkpoint_reader,int64_t block_size, int64_t &file_offset);
  int reserve_checkpoint_block_header(char *buf,
                                      int64_t buf_size,
                                      int64_t &offset,
                                      storage::CheckpointBlockHeader *&block_header);
  int replay_add_subtable_log(const char *log, int64_t log_len);
  int replay_remove_subtable_log(const char *log, int64_t log_len);
  int replay_modify_subtable_log(const char *log_data, int64_t log_len);
  bool is_inited_;
  GlobalContext *global_ctx_;
  std::unique_ptr<ColumnFamilySet, memory::ptr_destruct_delete<ColumnFamilySet>> column_family_set_;

  util::Env* const env_;
  const std::string dbname_;
  const common::ImmutableDBOptions* const db_options_;
  FileNumber next_file_number_;
  uint64_t manifest_file_number_;
  uint64_t options_file_number_;
  uint64_t pending_manifest_file_number_;
  std::atomic<uint64_t> last_sequence_;
  std::atomic<uint64_t> last_allocated_sequence_;  // when shutdown
                                                   // allocated_sequence ==
                                                   // last_sequence_;
  uint64_t prev_log_number_;  // 0 or backing store for memtable being compacted

  // Opened lazily
  unique_ptr<log::Writer, memory::ptr_destruct_delete<log::Writer>> descriptor_log_;

  // generates a increasing version number for every new version
  uint64_t current_version_number_;

  // Queue of writers to the manifest file
  std::deque<ManifestWriter*> manifest_writers_;

  // Current size of manifest file
  uint64_t manifest_file_size_;

  std::vector<FileMetaData*> obsolete_files_;
  std::vector<std::string> obsolete_manifests_;

  // env options for all reads and writes except compactions
  const util::EnvOptions& env_options_;

  // env options used for compactions. This is a copy of
  // env_options_ but with readaheads set to readahead_compactions_.
  const util::EnvOptions env_options_compactions_;

  // record the recovery manifest log file name and
  // newest checkpoint log number
  std::string recovered_manifest_file_name_;
  uint64_t checkpoint_log_number_;
  // menifest storage manager meta log number
  std::atomic<uint64_t> meta_log_number_;
  storage::StorageLogger *storage_logger_;

  //AllSubTable all_sub_table_;

  // No copying allowed
  VersionSet(const VersionSet&);
  void operator=(const VersionSet&);

  // hot backup
  // the two file size with binlog position are the
  // consistency backup position
  std::unordered_map<int32_t, const Snapshot*> meta_snapshots_;
  int64_t last_manifest_file_size_;
  int64_t last_wal_file_size_;
  // current checkpoint file number
  uint64_t checkpoint_file_number_; 
  // the number small than the numbers can be recycled
  uint64_t purge_checkpoint_file_number_;
  uint64_t purge_manifest_file_number_;
  // update current file after recover finished
  uint64_t file_number_;
  uint64_t log_number_;
#ifndef NDEBUG
  bool write_checkpoint_failed_;
#endif
};
}
}  // namespace xengine
