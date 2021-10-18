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

#pragma once
#ifndef ROCKSDB_LITE

#include "xengine/status.h"
#include "xengine/db.h"
#include "util/filename.h"
#include "xengine/xengine_constants.h"
#include "logger/logger.h"
#include "xengine/utilities/hotbackup.h"

namespace xengine
{
namespace db
{
  class Snapshot;
}
namespace util
{

class BackupSnapshotImpl : public BackupSnapshot
{
public:
  // Check backup job and do init
  virtual int init(db::DB *db, const char *backup_tmp_dir_path = nullptr) override;
  // Do a manual checkpoint and flush memtable
  virtual int do_checkpoint(db::DB *db) override;
  // Acquire snapshots and hard-link/copy MANIFEST files
  virtual int acquire_snapshots(db::DB *db) override;
  // Parse incremental MANIFEST files and record the modified extent ids
  virtual int record_incremental_extent_ids(db::DB *db) override;
  // Release the snapshots
  virtual int release_snapshots(db::DB *db) override;

public:
  BackupSnapshotImpl() : process_tid_(free_tid_),
                         first_manifest_file_num_(0),
                         last_manifest_file_num_(0),
                         last_manifest_file_size_(0),
                         last_wal_file_num_(0)
  {}
  virtual ~BackupSnapshotImpl() {}

private:
  int create_tmp_dir(db::DB *db);
  int link_sst_files(db::DB *db);
  template<typename DataFileChecker, typename WalFileChecker>
  int link_files(db::DB *db, DataFileChecker *data_file_checker, WalFileChecker *wal_file_checker);
  template<typename FileChecker>
  int link_dir_files(db::DB *db, const std::string &dir_path, const std::vector<std::string> &files, FileChecker *file_checker);
  int check_status();
  // Cleanup the tmp dir
  int do_cleanup(db::DB *db);
  void reset();

private:
  static const int free_tid_ = -1;
  // A backup job is in process
  std::atomic<int> process_tid_;
  // The written MANIFEST file after do checkpoint
  int32_t first_manifest_file_num_;
  // The written MANIFEST file used when acquiring snapshots
  int32_t last_manifest_file_num_;
  // The size of last_manifest_file
  uint64_t last_manifest_file_size_;
  // The written WAL log file after switch memtable in acquiring_snapshots
  uint64_t last_wal_file_num_;
  // Snapshots of all subtables
  db::MetaSnapshotMap meta_snapshots_;
  std::string backup_tmp_dir_path_;
};

struct SSTFileChecker
{
  inline bool operator()(const util::FileType type, const uint64_t file_num)
  {
    return type == util::kTableFile;
  }
};

struct DataDirFileChecker
{
  DataDirFileChecker(const uint64_t first_manifest_file_num,
      const uint64_t last_manifest_file_num)
      : first_manifest_file_num_(first_manifest_file_num),
        last_manifest_file_num_(last_manifest_file_num)
  {}
  inline bool operator()(const util::FileType &type, const uint64_t &file_num)
  {
    return (type == util::kTableFile)
        || (type == util::kDescriptorFile && file_num < last_manifest_file_num_ && file_num >= first_manifest_file_num_)
        || (type == util::kCheckpointFile && file_num <= last_manifest_file_num_ && file_num >= first_manifest_file_num_ - 1)
        || (type == kCurrentFile)
        || (type == kCurrentCheckpointFile);

  }
  uint64_t first_manifest_file_num_;
  uint64_t last_manifest_file_num_;
};

struct WalDirFileChecker
{
  WalDirFileChecker(const uint64_t last_wal_file_num) : last_wal_file_num_(last_wal_file_num)
  {}
  inline bool operator()(const util::FileType &type, const uint64_t &file_num)
  {
    return (type == util::kLogFile && file_num < last_wal_file_num_);
  }
  uint64_t last_wal_file_num_;
};

template<typename DataFileChecker, typename WalFileChecker>
int BackupSnapshotImpl::link_files(db::DB *db, DataFileChecker *data_file_checker, WalFileChecker *wal_file_checker)
{
  int ret = common::Status::kOk;
  if (ISNULL(db)) {
    ret = common::Status::kInvalidArgument;
    XENGINE_LOG(WARN, "db is nullptr", K(ret));
  } else {
    db->DisableFileDeletions();
    std::vector<std::string> data_files;
    std::vector<std::string> wal_files;
    if (FAILED(db->GetEnv()->GetChildren(db->GetName(), &data_files).code())) {
      XENGINE_LOG(WARN, "Failed to get all files in data dir", K(ret));
    } else if (FAILED(link_dir_files(db, db->GetName(), data_files, data_file_checker))) {
      XENGINE_LOG(WARN, "Failed to link files in data dir", K(ret), "Dir", db->GetName());
    } else if (nullptr == wal_file_checker) {
      // skip link wal files
    } else if (FAILED(db->GetEnv()->GetChildren(db->GetDBOptions().wal_dir, &wal_files).code())) {
      XENGINE_LOG(WARN, "Failed to get all files in wal dir", K(ret));
    } else if (FAILED(link_dir_files(db, db->GetDBOptions().wal_dir, wal_files, wal_file_checker))) {
      XENGINE_LOG(WARN, "Failed to link files in wal dir", K(ret), "Dir", db->GetDBOptions().wal_dir);
    }
    db->EnableFileDeletions(false);
  }
  return ret;
}

template<typename FileChecker>
int BackupSnapshotImpl::link_dir_files(db::DB *db, const std::string &dir_path,
    const std::vector<std::string> &files, FileChecker *file_checker)
{
  int ret = common::Status::kOk;
  if (ISNULL(db) || ISNULL(file_checker)) {
    ret = common::Status::kInvalidArgument;
    XENGINE_LOG(WARN, "db or file_checker is nullptr", K(ret), KP(db), KP(file_checker));
  } else {
    uint64_t file_num = 0;
    util::FileType type;
    for (size_t i = 0; SUCC(ret) && i < files.size(); i++) {
      if (ParseFileName(files[i], &file_num, &type) && file_checker->operator()(type, file_num)) {
        std::string file_path = dir_path + "/" + files[i];
        std::string link_file_path = backup_tmp_dir_path_ + "/" + files[i];
        if (common::Status::kOk == (ret = db->GetEnv()->FileExists(link_file_path).code())) {
          // skip existing file
          XENGINE_LOG(DEBUG, "File already exist", K(ret), K(file_path), K(link_file_path));
        } else if (ret != common::Status::kNotFound) {
          XENGINE_LOG(WARN, "IO error when checking file", K(ret), K(link_file_path));
        } else if (FAILED(db->GetEnv()->FileExists(file_path).code())) {
          if (ret == common::Status::kNotFound) {
            // this file was deleted, skip, overwrite ret
            ret = common::Status::kOk;
          } else {
            XENGINE_LOG(WARN, "IO error when checking file", K(ret), K(file_path));
          }
        } else if (FAILED(db->GetEnv()->LinkFile(file_path, link_file_path).code())) {
          XENGINE_LOG(WARN, "Failed to link file", K(ret), K(file_path), K(link_file_path));
        } else {
          XENGINE_LOG(INFO, "Success to link file", K(ret), K(file_path), K(link_file_path));
        }
      }
    }
  }
  return ret;
}

} // namespace util
} // namespace xengien

#endif
