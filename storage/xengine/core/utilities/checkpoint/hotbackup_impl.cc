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

#ifndef ROCKSDB_LITE

#include "utilities/checkpoint/hotbackup_impl.h"
#include "util/file_util.h"
#include "memory/thread_local_store.h"

namespace xengine
{
using namespace common;
using namespace db;
using namespace storage;

namespace util
{

int BackupSnapshot::create(BackupSnapshot *&backup_instance)
{
  int ret = Status::kOk;
  backup_instance = new BackupSnapshotImpl();
  if (ISNULL(backup_instance)) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(ERROR, "Failed to allocate backup_instance", K(ret));
  }
  return ret;
}

int BackupSnapshot::init(DB *db, const char *backup_tmp_dir_path)
{
  return Status::kNotSupported;
}

int BackupSnapshot::do_checkpoint(DB *db)
{
  return Status::kNotSupported;
}

int BackupSnapshot::acquire_snapshots(DB *db)
{
  return Status::kNotSupported;
}

int BackupSnapshot::record_incremental_extent_ids(DB *db)
{
  return Status::kNotSupported;
}

int BackupSnapshot::release_snapshots(DB *db)
{
  return Status::kNotSupported;
}

int BackupSnapshotImpl::init(DB *db, const char *backup_tmp_dir_path)
{
  int ret = Status::kOk;
  int tid = memory::get_tc_tid();
  int free_tid = free_tid_;
  if (!process_tid_.compare_exchange_strong(free_tid, tid)) {
    ret = Status::kInitTwice;
    XENGINE_LOG(WARN, "There is another backup job still running!", K(ret));
  } else if (ISNULL(db)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "db is nullptr", K(ret));
  } else {
    first_manifest_file_num_ = 0;
    last_manifest_file_num_ = 0;
    last_manifest_file_size_ = 0;
    last_wal_file_num_ = 0;
    backup_tmp_dir_path_ = (nullptr == backup_tmp_dir_path) ?
        db->GetName() + BACKUP_TMP_DIR : backup_tmp_dir_path;
    do_cleanup(db);
  }
  return ret;
}

void BackupSnapshotImpl::reset()
{
  first_manifest_file_num_ = 0;
  last_manifest_file_num_ = 0;
  last_manifest_file_size_ = 0;
  last_wal_file_num_ = 0;
  backup_tmp_dir_path_.clear();
  process_tid_.store(free_tid_);
}

int BackupSnapshotImpl::check_status()
{
  int ret = Status::kOk;
  int tid = memory::get_tc_tid();
  int process_tid = process_tid_.load();
  if (free_tid_ == process_tid) {
    ret = Status::kNotInit;
    XENGINE_LOG(INFO, "There is no backup job running", K(ret));
  } else if (tid != process_tid) {
    ret = Status::kInitTwice;
    XENGINE_LOG(WARN, "There is another backup job still running!", K(ret), K(process_tid));
  }
  return ret;
}

int BackupSnapshotImpl::do_checkpoint(DB *db)
{
  int ret = Status::kOk;
  SSTFileChecker sst_file_checker;
  WalDirFileChecker *wal_file_checker = nullptr;
  if (FAILED(check_status())) {
    XENGINE_LOG(WARN, "Failed to check status", K(ret));
  } else if (ISNULL(db)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "db is nullptr", K(ret));
  } else if (FAILED(db->do_manual_checkpoint(first_manifest_file_num_))) {
    XENGINE_LOG(WARN, "Failed to do manual checkpoint", K(ret));
  } else if (FAILED(create_tmp_dir(db))) {
    XENGINE_LOG(WARN, "Failed to create tmp dir", K(ret));
  } else if (FAILED(link_files(db, &sst_file_checker, wal_file_checker))) {
    XENGINE_LOG(WARN, "Failed to link sst files", K(ret));
  } else {
    XENGINE_LOG(INFO, "Success to do checkpoint", K(ret), K(first_manifest_file_num_));
  }
  if (FAILED(ret) && Status::kInitTwice != ret) {
    do_cleanup(db);
    reset();
  }
  return ret;
}

int BackupSnapshotImpl::acquire_snapshots(DB *db)
{
  int ret = Status::kOk;
  if (FAILED(check_status())) {
    XENGINE_LOG(WARN, "Failed to check status", K(ret));
  } else if (ISNULL(db)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "db is nullptr", K(ret));
  } else if (FAILED(db->create_backup_snapshot(meta_snapshots_,
                                               last_manifest_file_num_,
                                               last_manifest_file_size_,
                                               last_wal_file_num_))) {
    XENGINE_LOG(WARN, "Failed to acquire backup snapshot", K(ret));
  } else {
    std::string last_manifest_file_dest = DescriptorFileName(backup_tmp_dir_path_, last_manifest_file_num_);
    std::string last_manifest_file_src = DescriptorFileName(db->GetName(), last_manifest_file_num_);
    DataDirFileChecker data_file_checker(first_manifest_file_num_,
                                       last_manifest_file_num_);
    WalDirFileChecker wal_file_checker(last_wal_file_num_);
    if (FAILED(link_files(db, &data_file_checker, &wal_file_checker))) {
      XENGINE_LOG(WARN, "Failed to link backup files", K(ret));
    } else if (FAILED(CopyFile(db->GetEnv(),
                               last_manifest_file_src,
                               last_manifest_file_dest,
                               last_manifest_file_size_,
                               db->GetDBOptions().use_fsync).code())) { // copy last MANIFEST file
      XENGINE_LOG(WARN, "Failed to copy last manifest file", K(ret));
    } else {
      XENGINE_LOG(INFO, "Success to copy last MANIFEST file and acquire snapshots",
          K(last_manifest_file_dest), K(last_manifest_file_num_),
          K(last_manifest_file_size_), K(last_wal_file_num_));
    }
  }
  if (FAILED(ret) && Status::kInitTwice != ret) {
    do_cleanup(db);
    reset();
  }
  return ret;
}

int BackupSnapshotImpl::record_incremental_extent_ids(DB *db)
{
  int ret = Status::kOk;
  if (FAILED(check_status())) {
    XENGINE_LOG(WARN, "Failed to check status", K(ret));
  } else if (ISNULL(db)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "db is nullptr", K(ret));
  } else if (FAILED(db->record_incremental_extent_ids(first_manifest_file_num_,
                                                      last_manifest_file_num_,
                                                      last_manifest_file_size_))) {
    XENGINE_LOG(WARN, "Failed to record incremental extents", K(ret));
  } else {
    XENGINE_LOG(INFO, "Success to record incremental extent-ids", K(ret));
  }
  if (FAILED(ret) && Status::kInitTwice != ret) {
    do_cleanup(db);
    reset();
  }
  return ret;
}

int BackupSnapshotImpl::release_snapshots(DB *db)
{
  int ret = Status::kOk;
  if (FAILED(check_status())) {
    if (Status::kInitTwice == ret) {
      XENGINE_LOG(WARN, "There is another backup job still running!", K(ret));
    }
  } else if (ISNULL(db)) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "db is nullptr", K(ret));
  } else if (FAILED(db->release_backup_snapshot(meta_snapshots_))) {
    XENGINE_LOG(WARN, "Failed to acquire backup snapshot", K(ret));
  }
  if (Status::kInitTwice != ret) {
    do_cleanup(db);
    reset();
  }
  return ret;
}

int BackupSnapshotImpl::create_tmp_dir(DB *db)
{
  int ret = Status::kOk;
  if (ISNULL(db)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "db is nullptr", K(ret));
  } else if (Status::kOk == (ret = db->GetEnv()->FileExists(backup_tmp_dir_path_).code())) {
    ret = Status::kErrorUnexpected;
    XENGINE_LOG(WARN, "Backup tmp dir exists, unexpected", K(ret), K(backup_tmp_dir_path_));
  } else if (ret != Status::kNotFound) {
    XENGINE_LOG(WARN, "IO error when checking backup tmp dir", K(ret), K(backup_tmp_dir_path_));
  } else if (FAILED(db->GetEnv()->CreateDir(backup_tmp_dir_path_).code())) { // overwrite ret
    XENGINE_LOG(WARN, "Failed to create tmp dir for backup", K(ret), K(backup_tmp_dir_path_));
  } else {
    XENGINE_LOG(INFO, "Success to create tmp dir for backup", K(ret), K(backup_tmp_dir_path_));
  }
  return ret;
}

// clean tmp files
int BackupSnapshotImpl::do_cleanup(DB *db)
{
  int ret = Status::kOk;
  if (ISNULL(db)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "db is nullptr", K(ret));
  } else {
    // release snapshots
    if (meta_snapshots_.size() > 0) {
      db->release_backup_snapshot(meta_snapshots_);
      meta_snapshots_.clear();
    }
    // delete tmp dir
    std::vector<std::string> all_files;
    if (FAILED(db->GetEnv()->GetChildren(backup_tmp_dir_path_, &all_files).code())) {
      if (Status::kNotFound != ret) {
        XENGINE_LOG(WARN, "Failed to get all files in tmp dir", K(ret), K(backup_tmp_dir_path_));
      } else {
        // tmp dir not exist, overwrite ret
        ret = Status::kOk;
        XENGINE_LOG(INFO, "Backup tmp dir is empty", K(backup_tmp_dir_path_));
      }
    } else {
      for (size_t i = 0; i < all_files.size(); i++) {
        if (all_files[i][0] != '.') {
          std::string file_path = backup_tmp_dir_path_ + "/" + all_files[i];
          if (FAILED(db->GetEnv()->DeleteFile(file_path).code())) { // overwrite ret
            XENGINE_LOG(WARN, "Failed to delete file in backup tmp dir", K(ret), K(file_path));
          } else {
            XENGINE_LOG(INFO, "Success to delete file in backup tmp dir", K(file_path));
          }
        }
      }
      if (FAILED(db->GetEnv()->DeleteDir(backup_tmp_dir_path_).code())) { // overwrite ret
        XENGINE_LOG(WARN, "Failed to delete backup tmp dir", K(ret), K(backup_tmp_dir_path_));
      } else {
        XENGINE_LOG(INFO, "Success to delete backup tmp dir", K(backup_tmp_dir_path_));
      }
    }
  }
  return ret;
}


} // namespace util
} // namespace xengine

#endif // ROCKSDB_LITE
