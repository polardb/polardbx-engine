//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2012 Facebook.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file.

#ifndef ROCKSDB_LITE

#include "xengine/utilities/checkpoint.h"

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include <inttypes.h>
#include <algorithm>
#include <string>
#include "db/wal_manager.h"
#include "port/port.h"
#include "util/file_util.h"
#include "util/filename.h"
#include "util/sync_point.h"
#include "xengine/db.h"
#include "xengine/env.h"
#include "xengine/transaction_log.h"
#include "storage/storage_manager.h"

using namespace xengine::common;
using namespace xengine::port;
using namespace xengine::db;
using namespace xengine::storage;

namespace xengine {
namespace util {

class CheckpointImpl : public Checkpoint {
 public:
  // direct io aligned size
  const size_t PAGE_SIZE = sysconf(_SC_PAGESIZE);
  // Creates a Checkpoint object to be used for creating openable snapshots
  explicit CheckpointImpl(DB* db) : db_(db) {}

  // Builds an openable snapshot on the same disk, which
  // accepts an output directory on the same disk, and under the directory
  // (1) hard-linked SST files pointing to existing live SST files
  // SST files will be copied if output directory is on a different filesystem
  // (2) a copied manifest files and other files
  // The directory should not already exist and will be created by this API.
  // The directory will be an absolute path
  using Checkpoint::CreateCheckpoint;
  virtual Status CreateCheckpoint(const std::string& checkpoint_dir,
                                  uint64_t log_size_for_flush) override;

  virtual int manual_checkpoint(const std::string &checkpoint_dir,
                                int32_t phase = 1,
                                int dest_fd = -1);

 private:
  DB* db_;
  int checkpoint_phase_one(const std::string &checkpoint_dir);
  int checkpoint_phase_two(const std::string &checkpoint_dir,
                        std::function<int(const char*, int, int64_t, int)> *stream_extent,
                        int dest_fd);
  int checkpoint_phase_three(const std::string &checkpoint_dir);
  int checkpoint_phase_four(const std::string &checkpoint_dir,
                        std::function<int(const char*, int, int64_t, int)> *stream_extent,
                        int dest_fd);
  int create_temp_directory(const std::string &checkpoint_dir);
  int rename_temp_directory(const std::string &checkpoint_dir);  
  int remove_temp_directory(const std::string &checkpoint_dir);
};

Status Checkpoint::Create(DB* db, Checkpoint** checkpoint_ptr) {
  *checkpoint_ptr = new CheckpointImpl(db);
  return Status::OK();
}

Status Checkpoint::CreateCheckpoint(const std::string& checkpoint_dir,
                                    uint64_t log_size_for_flush) {
  return Status::NotSupported("");
}

int Checkpoint::manual_checkpoint(const std::string& checkpoint_dir,
                                  int32_t phase,
                                  int dest_fd) {
  return Status::kNotSupported;
}

int CheckpointImpl::create_temp_directory(const std::string &checkpoint_dir) {
  int ret = Status::kOk;
  Status s = db_->GetEnv()->FileExists(checkpoint_dir);
  if (s.ok()) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(ERROR, "Directory exists.", K(ret));
    return ret; 
  } else if(!s.IsNotFound()) {
    ret = s.code();
    XENGINE_LOG(ERROR, "IO error of checkpoint path.", K(ret));
    return ret; 
  }
  std::string tmp_path = checkpoint_dir + ".tmp";
  // create snapshot directory
  s = db_->GetEnv()->CreateDir(tmp_path);
  if (!s.ok()) {
    ret = s.code();
    XENGINE_LOG(ERROR, "Create the tmperary directory failed.", K(ret));
  }

  return ret;
}

int CheckpointImpl::remove_temp_directory(const std::string &tmp_path) {
  // we have to delete the dir and all its children
  Status s;
  std::vector<std::string> subchildren;
  db_->GetEnv()->GetChildren(tmp_path, &subchildren);       
  for (auto& subchild : subchildren) {
    std::string subchild_path = tmp_path + "/" + subchild;
    s = db_->GetEnv()->DeleteFile(subchild_path);
  }
  // finally delete the private dir
  s = db_->GetEnv()->DeleteDir(tmp_path);
  return s.code();
}

int CheckpointImpl::rename_temp_directory(const std::string &checkpoint_dir) {
  std::string tmp_path = checkpoint_dir + ".tmp";
  Status s = db_->GetEnv()->RenameFile(tmp_path, checkpoint_dir);
  if (s.ok()) {
//    unique_ptr<Directory> checkpoint_directory;
    Directory *checkpoint_directory = nullptr;
    db_->GetEnv()->NewDirectory(checkpoint_dir, checkpoint_directory);
    if (checkpoint_directory != nullptr) {
      s = checkpoint_directory->Fsync();
    }
  }
   
  if (!s.ok()) {
    return remove_temp_directory(tmp_path);  
  }
  return Status::kOk;
}

// do checkpoint phase one hard link the meta checkpoint file
// CURRENT_CHECKPOINT file and sst files to checkpoint dir 
int CheckpointImpl::checkpoint_phase_one(const std::string &checkpoint_dir) {
  int ret = Status::kOk;
  // step one: flush memtable
  std::vector<std::string> live_files;
  int64_t manifest_file_size = 0;
  Status s = db_->GetLiveFiles(live_files, 
                        reinterpret_cast<uint64_t*>(&manifest_file_size), true);
  if (!s.ok()) {
    ret = s.code();
    XENGINE_LOG(ERROR, "Flush memtable of checkpoint failed", K(ret));
    return ret;
  }

  //FAIL_RETURN_MSG_NEW(db_->do_manual_checkpoint(),
                      //"Do manual checkpoint failed.", ret);
  
  // step two: create the sst and checkpoint file hard links
  FAIL_RETURN_MSG_NEW(create_temp_directory(checkpoint_dir),
                      "Create the tmperary directory failed %d.", ret);
 
  std::string tmp_path = checkpoint_dir + ".tmp";
  // link live files
  std::vector<std::string> subchildren;
  db_->GetEnv()->GetChildren(db_->GetName(), &subchildren);
  for (auto &file_name : subchildren) {
    uint64_t number;
    FileType type;
    bool ok = ParseFileName(file_name, &number, &type);
    if (!ok) {
      // ignore those files
      __XENGINE_LOG(WARN, "Parse file name %s failed.", file_name.c_str());
      continue;
    }

    if (type == kTableFile ||
        type == kCurrentCheckpointFile ||
        type == kCurrentFile ||
        type == kCheckpointFile) {
      __XENGINE_LOG(INFO, "Hard linking %s", file_name.c_str());
      s = db_->GetEnv()->LinkFile(db_->GetName() + "/" + file_name, 
                                  tmp_path + "/" + file_name);
      if (!s.ok()) {
        ret = s.code();
        XENGINE_LOG(ERROR, "Hard link file failed.", K(ret));
        break;
      }
    }
  }
  if (ret != Status::kOk) {
    return remove_temp_directory(tmp_path); 
  } else {
    return rename_temp_directory(checkpoint_dir);
  } 
}

// copy the manifest file and the extents it recorded
int CheckpointImpl::checkpoint_phase_two(const std::string &checkpoint_dir,
                 std::function<int(const char*, int, int64_t, int)> *stream_extent,
                 int dest_fd) {
  int ret = Status::kOk; 

  // flush memtable  
  std::vector<std::string> live_files;
  int64_t manifest_file_size = 0;
  Status s = db_->GetLiveFiles(live_files, 
                        reinterpret_cast<uint64_t*>(&manifest_file_size), true);
  if (!s.ok()) {
    ret = s.code();
    XENGINE_LOG(ERROR, "Flush memtable of checkpoint failed", K(ret));
    return ret;
  }

  // copy manifest file
  FAIL_RETURN_MSG_NEW(create_temp_directory(checkpoint_dir),
                      "Create the tmperary directory failed %d.", ret);
  std::string tmp_path = checkpoint_dir + ".tmp";
  // store the extents data temporarily
  std::string extents_path = tmp_path + "/extents.inc"; 
  dest_fd = open(extents_path.c_str(), O_CREAT | O_WRONLY | O_NOFOLLOW | O_EXCL,
                     0644);
  if (dest_fd < 0) {
      ret = errno;
      __XENGINE_LOG(ERROR, "XEngine: create extent incremental file failed errno %d", ret);
      return ret;
  }
  // stream the extents of the manifest log record
  ret = db_->stream_log_extents(stream_extent, 0, manifest_file_size, dest_fd);
  close(dest_fd);
  if (Status::kOk != ret) {
    XENGINE_LOG(ERROR, "Stream the logged extents failed", K(ret));
    return ret;
  }
  
  std::string manifest_file_name = live_files[1];
  s = CopyFile(db_->GetEnv(), db_->GetName() + manifest_file_name,
                   tmp_path + manifest_file_name,
                   manifest_file_size,
                   db_->GetDBOptions().use_fsync);
  if (!s.ok()) {
    return remove_temp_directory(tmp_path); 
  } else {
    return rename_temp_directory(checkpoint_dir);
  }
}

//  this phase is under global read lock
//  link the wal files and rename the last one
//  record the manifest file size and last wal file size
int CheckpointImpl::checkpoint_phase_three(const std::string &checkpoint_dir) {
  int ret = Status::kOk; 
  uint64_t sequence_number = db_->GetLatestSequenceNumber();
 
  FAIL_RETURN_MSG_NEW(create_temp_directory(checkpoint_dir),
                      "Create the tmperary directory failed %d.", ret);
  std::string tmp_path = checkpoint_dir + ".tmp";
  // hard link all wal files except the last one 
  // hard link the last wal file and record the size, 
  // avoid delete after release lock 
  VectorLogPtr live_wal_files; 
  Status s = db_->GetSortedWalFiles(live_wal_files);
  if (!s.ok()) {
    ret = s.code();
    XENGINE_LOG(ERROR, "Get all wal files failed.", K(ret));
    return ret;
  }
  size_t wal_size = live_wal_files.size();
  int64_t last_wal_file_size = 0;
  for (size_t i = 0; s.ok() && i < wal_size; ++i) {
    if (i + 1 == wal_size) {
      std::string old_name = live_wal_files[i]->PathName();
      __XENGINE_LOG(INFO, "Hard Linking lasting wal file %s\n", old_name.c_str());
      // rename to a tmp file avoid copy it in this phase
      std::string tmp_name = old_name.substr(0, old_name.find_last_of('.')) + ".tmp"; 
      s = db_->GetEnv()->LinkFile(
        db_->GetDBOptions().wal_dir + old_name, tmp_path + tmp_name);
      // copy the last block if wal is direct io
      last_wal_file_size = live_wal_files[i]->SizeFileBytes();
      if (0 == (last_wal_file_size % PAGE_SIZE)) {
        last_wal_file_size -= PAGE_SIZE;
      }
      if (live_wal_files[i]->SizeFileBytes() > 
          static_cast<uint64_t>(last_wal_file_size)) {
        std::string last_page_name = old_name.substr(0, old_name.find_last_of('.')) + ".last";
        s = CopyFile(db_->GetEnv(), db_->GetDBOptions().wal_dir + old_name,
                   tmp_path + last_page_name,
                   live_wal_files[i]->SizeFileBytes() - last_wal_file_size,
                   db_->GetDBOptions().use_fsync, last_wal_file_size);
        if (!s.ok()) {
          __XENGINE_LOG(ERROR, "Copy the last block of wal file failed.");
        }
      }
      break;
    }
    __XENGINE_LOG(INFO, "Hard Linking %s\n",
                   live_wal_files[i]->PathName().c_str());
    s = db_->GetEnv()->LinkFile(
        db_->GetDBOptions().wal_dir + live_wal_files[i]->PathName(),
        tmp_path + live_wal_files[i]->PathName());
  }
  //FAIL_RETURN_MSG_NEW(db_->create_backup_snapshot(last_wal_file_size),
                          //"Create backup snapshot failed.", ret);
  
  if (!s.ok()) {
    ret = remove_temp_directory(tmp_path); 
  } else {
    ret = rename_temp_directory(checkpoint_dir);
    __XENGINE_LOG(INFO, "Snapshot sequence number: %" PRIu64, sequence_number);
  }
  return ret;
}

//  this phase is out of global read lock and hold snapshots
//  copy the incremental manifest file and the extents
int CheckpointImpl::checkpoint_phase_four(const std::string &checkpoint_dir,
                std::function<int(const char*, int, int64_t, int)> *stream_extent,
                int dest_fd) {
  int ret = Status::kOk; 
 
  FAIL_RETURN_MSG_NEW(create_temp_directory(checkpoint_dir),
                      "Create the tmperary directory failed %d.", ret);
  std::string tmp_path = checkpoint_dir + ".tmp";
   
  // get the manifest file size of phase two
  size_t i = checkpoint_dir.rfind('/', checkpoint_dir.length());
  std::string old_manifest_dir = checkpoint_dir.substr(0, i) + "/2";
  std::string last_manifest_dir = checkpoint_dir.substr(0, i) + "/3";
  std::vector<std::string> subchildren;
  
  // the backup consistency manifest file size
  Status s;
  int64_t manifest_file_size = db_->backup_manifest_file_size();
  int64_t old_manifest_file_size = 0;
  db_->GetEnv()->GetChildren(old_manifest_dir, &subchildren);
  for (auto &file_name : subchildren) {
    if (0 == file_name.compare(0, 8, "MANIFEST")) { 
      s = db_->GetEnv()->GetFileSize(old_manifest_dir + "/" + file_name,
                  reinterpret_cast<uint64_t*>(&old_manifest_file_size));
      if (!s.ok()) {
        ret = s.code();
        XENGINE_LOG(ERROR, "Get old manifest file size failed", K(ret));
        remove_temp_directory(tmp_path);
        remove_temp_directory(old_manifest_dir);
        remove_temp_directory(last_manifest_dir);
      } else {
        // copy the incremental manifest file
        assert(old_manifest_file_size <= manifest_file_size);
        if (manifest_file_size > old_manifest_file_size) {
          s = CopyFile(db_->GetEnv(), db_->GetName() + "/" +  file_name,
                   tmp_path + "/" +  file_name,
                   manifest_file_size - old_manifest_file_size,
                   db_->GetDBOptions().use_fsync, old_manifest_file_size);
          if (!s.ok()) {
            ret = s.code();
            XENGINE_LOG(ERROR, "copy the incremental manifest file failed", K(ret));
            remove_temp_directory(tmp_path);
            remove_temp_directory(old_manifest_dir);
            remove_temp_directory(last_manifest_dir);
          }
        }
      }
     break;
    }
  }
  // copy the incremental extents between phase two and three 
  if (s.ok() && manifest_file_size > old_manifest_file_size) {
    // store the extents data temporarily
    std::string extents_path = tmp_path + "/extents.inc"; 
    dest_fd = open(extents_path.c_str(), O_CREAT | O_WRONLY | O_NOFOLLOW | O_EXCL,
                      0644);
    if (dest_fd < 0) {
      ret = errno;
      __XENGINE_LOG(ERROR, "XEngine: create extent incremental file failed errno %d", ret);
      return ret;
    }
    // stream the incremental extents of the log
    ret = db_->stream_log_extents(stream_extent, old_manifest_file_size, 
                                  manifest_file_size, dest_fd);
    close(dest_fd);
    if (Status::kOk != ret) {
      XENGINE_LOG(ERROR, "Stream the logged extents failed", K(ret));
    } 
  }
  // release the snapshot here and copy the last wal file size to
  // recorded backup snapshot consistency file size
  int64_t last_wal_file_size = db_->get_last_wal_file_size();
  if (Status::kOk == ret) {
    db_->GetEnv()->GetChildren(last_manifest_dir, &subchildren);
    std::size_t found = std::string::npos;
    std::string tmp_name;
    for (auto &file_name : subchildren) {
      found = file_name.find_last_of('.');
      if (found != std::string::npos && 
          0 == file_name.compare(found, file_name.length(), ".tmp")) { 
        tmp_name = file_name;
        s = CopyFile(db_->GetEnv(), last_manifest_dir + "/" + file_name,
                   tmp_path + "/" + file_name, last_wal_file_size,
                   db_->GetDBOptions().use_fsync);
        if (!s.ok()) {
          ret = s.code();
          XENGINE_LOG(ERROR, "Copy the the last wal file failed", K(ret));
          remove_temp_directory(tmp_path);
        }
        break;
      }
    }
    if (s.ok()) {
      bool has_last = false;
      for (auto &file_name : subchildren) {
        // rename to log and copy to dest dir outsize
        found = file_name.find_last_of('.');
        if (found != std::string::npos && 
            0 == file_name.compare(found, file_name.length(), ".last")) { 
          std::string old_name = file_name.substr(0, 
                               file_name.find_last_of('.')) + ".tmp";
          ret = append_file(db_->GetEnv(), last_manifest_dir + "/" + file_name,
                   tmp_path + "/" + old_name, PAGE_SIZE, db_->GetDBOptions().use_fsync);
          if (ret != Status::kOk) {
            XENGINE_LOG(ERROR, "Copy the the last wal file failed", K(ret));
            remove_temp_directory(tmp_path);
          }
          has_last = true;
          break;
        }
      }
      if (has_last || tmp_name.size() > 0) { // need rename the .tmp file
        std::string new_name = tmp_name.substr(0, 
                               tmp_name.find_last_of('.')) + ".log";
        const EnvOptions soptions;
//        unique_ptr<WritableFile> destfile;
        WritableFile *destfile = nullptr;
        s = db_->GetEnv()->ReuseWritableFile(tmp_path + "/" + new_name, 
                                             tmp_path + "/" + tmp_name, destfile, soptions);
        if (!s.ok()) {
          ret = s.code();
          XENGINE_LOG(ERROR, "Rename the the last wal file failed", K(ret));
        }
      }
    }
  }
  if (Status::kOk == ret) {
    ret = rename_temp_directory(checkpoint_dir);    
  } else {
    remove_temp_directory(checkpoint_dir);
  }
  remove_temp_directory(old_manifest_dir);
  remove_temp_directory(last_manifest_dir);
  return ret;
}

// do manual checkpoint
int CheckpointImpl::manual_checkpoint(const std::string &checkpoint_dir,
                                      int32_t phase, 
                                      int dest_fd) {
  int ret = Status::kOk;
  switch (phase) {
    case 1:
      return checkpoint_phase_one(checkpoint_dir);
    case 2:
      return checkpoint_phase_two(checkpoint_dir, nullptr, dest_fd);
    case 3:
      return checkpoint_phase_three(checkpoint_dir);
    case 4:
      return checkpoint_phase_four(checkpoint_dir, nullptr, dest_fd);
    case 5:
      //return db_->release_backup_snapshot();
    default:
      ret = Status::kInvalidArgument;
      XENGINE_LOG(ERROR, "Input a wrong phase number of checkpoint", K(ret));
      return ret; 
  } 
}

// Builds an openable snapshot
Status CheckpointImpl::CreateCheckpoint(const std::string& checkpoint_dir,
                                        uint64_t log_size_for_flush) {
  Status s;
  std::vector<std::string> live_files;
  uint64_t manifest_file_size = 0;
  DBOptions db_options = db_->GetDBOptions();
  uint64_t min_log_num = port::kMaxUint64;
  uint64_t sequence_number = db_->GetLatestSequenceNumber();
  bool same_fs = true;
  VectorLogPtr live_wal_files;

  s = db_->GetEnv()->FileExists(checkpoint_dir);
  if (s.ok()) {
    return Status::InvalidArgument("Directory exists");
  } else if (!s.IsNotFound()) {
    assert(s.IsIOError());
    return s;
  }

  s = db_->DisableFileDeletions();
  bool flush_memtable = true;
  if (s.ok()) {
    if (!db_options.allow_2pc) {
      // If out standing log files are small, we skip the flush.
      s = db_->GetSortedWalFiles(live_wal_files);

      if (!s.ok()) {
        db_->EnableFileDeletions(false);
        return s;
      }

      // Don't flush column families if total log size is smaller than
      // log_size_for_flush. We copy the log files instead.
      // We may be able to cover 2PC case too.
      uint64_t total_wal_size = 0;
      for (auto& wal : live_wal_files) {
        total_wal_size += wal->SizeFileBytes();
      }
      if (total_wal_size < log_size_for_flush) {
        flush_memtable = false;
      }
      live_wal_files.clear();
    }

    // this will return live_files prefixed with "/"
    s = db_->GetLiveFiles(live_files, &manifest_file_size, flush_memtable);

    if (s.ok() && db_options.allow_2pc) {
      // If 2PC is enabled, we need to get minimum log number after the flush.
      // Need to refetch the live files to recapture the snapshot.
      if (!db_->GetIntProperty(DB::Properties::kMinLogNumberToKeep,
                               &min_log_num)) {
        db_->EnableFileDeletions(false);
        return Status::InvalidArgument(
            "2PC enabled but cannot fine the min log number to keep.");
      }
      // We need to refetch live files with flush to handle this case:
      // A previous 000001.log contains the prepare record of transaction tnx1.
      // The current log file is 000002.log, and sequence_number points to this
      // file.
      // After calling GetLiveFiles(), 000003.log is created.
      // Then tnx1 is committed. The commit record is written to 000003.log.
      // Now we fetch min_log_num, which will be 3.
      // Then only 000002.log and 000003.log will be copied, and 000001.log will
      // be skipped. 000003.log contains commit message of tnx1, but we don't
      // have respective prepare record for it.
      // In order to avoid this situation, we need to force flush to make sure
      // all transactions commited before getting min_log_num will be flushed
      // to SST files.
      // We cannot get min_log_num before calling the GetLiveFiles() for the
      // first time, because if we do that, all the logs files will be included,
      // far more than needed.
      s = db_->GetLiveFiles(live_files, &manifest_file_size, /* flush */ true);
    }

    TEST_SYNC_POINT("CheckpointImpl::CreateCheckpoint:SavedLiveFiles1");
    TEST_SYNC_POINT("CheckpointImpl::CreateCheckpoint:SavedLiveFiles2");
  }
  // if we have more than one column family, we need to also get WAL files
  if (s.ok()) {
    s = db_->GetSortedWalFiles(live_wal_files);
  }
  if (!s.ok()) {
    db_->EnableFileDeletions(false);
    return s;
  }

  size_t wal_size = live_wal_files.size();
  __XENGINE_LOG(INFO, "Started the snapshot process -- creating snapshot in directory %s", checkpoint_dir.c_str());

  std::string full_private_path = checkpoint_dir + ".tmp";

  // create snapshot directory
  s = db_->GetEnv()->CreateDir(full_private_path);

  // copy/hard link live_files
  std::string manifest_fname, current_fname;
  for (size_t i = 0; s.ok() && i < live_files.size(); ++i) {
    uint64_t number;
    FileType type;
    bool ok = ParseFileName(live_files[i], &number, &type);
    if (!ok) {
      s = Status::Corruption("Can't parse file name. This is very bad");
      break;
    }
    // we should only get sst, options, manifest and current files here
    assert(type == kTableFile || type == kDescriptorFile ||
           type == kCurrentFile || type == kOptionsFile ||
           type == kCheckpointFile);
    assert(live_files[i].size() > 0 && live_files[i][0] == '/');
    if (type == kCurrentFile) {
      // We will craft the current file manually to ensure it's consistent with
      // the manifest number. This is necessary because current's file contents
      // can change during checkpoint creation.
      current_fname = live_files[i];
      continue;
    } else if (type == kDescriptorFile) {
      manifest_fname = live_files[i];
    }
    std::string src_fname = live_files[i];

    // rules:
    // * if it's kTableFile, then it's shared
    // * if it's kDescriptorFile, limit the size to manifest_file_size
    // * always copy if cross-device link
    if ((type == kTableFile) && same_fs) {
      __XENGINE_LOG(INFO, "Hard Linking %s", src_fname.c_str());
      s = db_->GetEnv()->LinkFile(db_->GetName() + src_fname,
                                  full_private_path + src_fname);
      if (s.IsNotSupported()) {
        same_fs = false;
        s = Status::OK();
      }
    }
    if ((type != kTableFile) || (!same_fs)) {
      __XENGINE_LOG(INFO, "Copying %s", src_fname.c_str());
      s = CopyFile(db_->GetEnv(), db_->GetName() + src_fname,
                   full_private_path + src_fname,
                   (type == kDescriptorFile) ? manifest_file_size : 0,
                   db_options.use_fsync);
    }
  }
  if (s.ok() && !current_fname.empty() && !manifest_fname.empty()) {
    s = CreateFile(db_->GetEnv(), full_private_path + current_fname,
                   manifest_fname.substr(1) + "\n");
  }
  __XENGINE_LOG(INFO, "Number of log files %" ROCKSDB_PRIszt, live_wal_files.size());

  // Link WAL files. Copy exact size of last one because it is the only one
  // that has changes after the last flush.
  for (size_t i = 0; s.ok() && i < wal_size; ++i) {
    if ((live_wal_files[i]->Type() == kAliveLogFile) &&
        (!flush_memtable ||
         live_wal_files[i]->StartSequence() >= sequence_number ||
         live_wal_files[i]->LogNumber() >= min_log_num)) {
      if (i + 1 == wal_size) {
        __XENGINE_LOG(INFO, "Copying %s", live_wal_files[i]->PathName().c_str());
        s = CopyFile(db_->GetEnv(),
                     db_options.wal_dir + live_wal_files[i]->PathName(),
                     full_private_path + live_wal_files[i]->PathName(),
                     live_wal_files[i]->SizeFileBytes(), db_options.use_fsync);
        break;
      }
      if (same_fs) {
        // we only care about live log files
        __XENGINE_LOG(INFO, "Hard Linking %s", live_wal_files[i]->PathName().c_str());
        s = db_->GetEnv()->LinkFile(
            db_options.wal_dir + live_wal_files[i]->PathName(),
            full_private_path + live_wal_files[i]->PathName());
        if (s.IsNotSupported()) {
          same_fs = false;
          s = Status::OK();
        }
      }
      if (!same_fs) {
        __XENGINE_LOG(INFO, "Copying %s", live_wal_files[i]->PathName().c_str());
        s = CopyFile(db_->GetEnv(),
                     db_options.wal_dir + live_wal_files[i]->PathName(),
                     full_private_path + live_wal_files[i]->PathName(), 0,
                     db_options.use_fsync);
      }
    }
  }

  // we copied all the files, enable file deletions
  db_->EnableFileDeletions(false);

  if (s.ok()) {
    // move tmp private backup to real snapshot directory
    s = db_->GetEnv()->RenameFile(full_private_path, checkpoint_dir);
  }
  if (s.ok()) {
//    unique_ptr<Directory> checkpoint_directory;
    Directory *checkpoint_directory = nullptr;
    db_->GetEnv()->NewDirectory(checkpoint_dir, checkpoint_directory);
    if (checkpoint_directory != nullptr) {
      s = checkpoint_directory->Fsync();
    }
  }

  if (!s.ok()) {
    // clean all the files we might have created
    __XENGINE_LOG(INFO, "Snapshot failed -- %s", s.ToString().c_str());
    // we have to delete the dir and all its children
    std::vector<std::string> subchildren;
    db_->GetEnv()->GetChildren(full_private_path, &subchildren);
    for (auto& subchild : subchildren) {
      std::string subchild_path = full_private_path + "/" + subchild;
      Status s1 = db_->GetEnv()->DeleteFile(subchild_path);
      __XENGINE_LOG(INFO, "Delete file %s -- %s",
                     subchild_path.c_str(), s1.ToString().c_str());
    }
    // finally delete the private dir
    Status s1 = db_->GetEnv()->DeleteDir(full_private_path);
    __XENGINE_LOG(INFO, "Delete dir %s -- %s",
                   full_private_path.c_str(), s1.ToString().c_str());
    return s;
  }

  // here we know that we succeeded and installed the new snapshot
  __XENGINE_LOG(INFO, "Snapshot DONE. All is good");
  __XENGINE_LOG(INFO, "Snapshot sequence number: %" PRIu64,
                 sequence_number);

  return s;
}
}  //  namespace util
}  //  namespace xengine

#endif  // ROCKSDB_LITE
