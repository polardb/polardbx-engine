// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright 2014 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// This test uses a custom Env to keep track of the state of a filesystem as of
// the last "sync". It then checks for data loss errors by purposely dropping
// file data (or entire files) not protected by a "sync".

#ifndef UTIL_FAULT_INJECTION_TEST_ENV_H_
#define UTIL_FAULT_INJECTION_TEST_ENV_H_

#include <map>
#include <set>
#include <string>

#include "db/version_set.h"
#include "env/mock_env.h"
#include "util/filename.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "xengine/db.h"
#include "xengine/env.h"
#include "xengine/status.h"

namespace xengine {
namespace util {

class TestWritableFile;
class FaultInjectionTestEnv;

struct FileState {
  std::string filename_;
  ssize_t pos_;
  ssize_t pos_at_last_sync_;
  ssize_t pos_at_last_flush_;

  explicit FileState(const std::string& filename)
      : filename_(filename),
        pos_(-1),
        pos_at_last_sync_(-1),
        pos_at_last_flush_(-1) {}

  FileState() : pos_(-1), pos_at_last_sync_(-1), pos_at_last_flush_(-1) {}

  bool IsFullySynced() const { return pos_ <= 0 || pos_ == pos_at_last_sync_; }

  common::Status DropUnsyncedData(util::Env* env) const;

  common::Status DropRandomUnsyncedData(util::Env* env,
                                        util::Random* rand) const;
};

// A wrapper around WritableFileWriter* file
// is written to or sync'ed.
class TestWritableFile : public WritableFile {
 public:
  explicit TestWritableFile(const std::string& fname,
                            unique_ptr<WritableFile>&& f,
                            FaultInjectionTestEnv* env);
  virtual ~TestWritableFile();
  virtual common::Status Append(const common::Slice& data) override;
  virtual common::Status Truncate(uint64_t size) override {
    return target_->Truncate(size);
  }
  virtual common::Status PositionedAppend(const common::Slice& data,
                                          uint64_t offset) {
    return target_->PositionedAppend(data, offset);
  }
  virtual uint64_t GetFileSize() { return target_->GetFileSize(); }
  virtual common::Status Close() override;
  virtual common::Status Flush() override;
  virtual common::Status Sync() override;
  virtual bool IsSyncThreadSafe() const override { return true; }

 private:
  FileState state_;
  unique_ptr<WritableFile> target_;
  bool writable_file_opened_;
  FaultInjectionTestEnv* env_;
};

class TestDirectory : public Directory {
 public:
  explicit TestDirectory(FaultInjectionTestEnv* env, std::string dirname,
                         Directory* dir)
      : env_(env), dirname_(dirname), dir_(dir) {}
  ~TestDirectory() {}

  virtual common::Status Fsync() override;

 private:
  FaultInjectionTestEnv* env_;
  std::string dirname_;
  unique_ptr<Directory, memory::ptr_destruct_delete<Directory>> dir_;
};

class FaultInjectionTestEnv : public EnvWrapper {
 public:
  explicit FaultInjectionTestEnv(util::Env* base)
      : EnvWrapper(base), filesystem_active_(true) {}
  virtual ~FaultInjectionTestEnv() {}

  common::Status NewDirectory(const std::string& name,
                              Directory*& result) override;

  common::Status NewWritableFile(const std::string& fname,
                                 WritableFile*& result,
                                 const EnvOptions& soptions) override;

  virtual common::Status DeleteFile(const std::string& f) override;

  virtual common::Status RenameFile(const std::string& s,
                                    const std::string& t) override;

  void WritableFileClosed(const FileState& state);

  // For every file that is not fully synced, make a call to `func` with
  // FileState of the file as the parameter.
  common::Status DropFileData(
      std::function<common::Status(util::Env*, FileState)> func);

  common::Status DropUnsyncedFileData();

  common::Status DropRandomUnsyncedFileData(Random* rnd);

  common::Status DeleteFilesCreatedAfterLastDirSync();

  void ResetState();

  void UntrackFile(const std::string& f);

  void SyncDir(const std::string& dirname) {
    MutexLock l(&mutex_);
    dir_to_new_files_since_last_sync_.erase(dirname);
  }

  // Setting the filesystem to inactive is the test equivalent to simulating a
  // system reset. Setting to inactive will freeze our saved filesystem state so
  // that it will stop being recorded. It can then be reset back to the state at
  // the time of the reset.
  bool IsFilesystemActive() {
    MutexLock l(&mutex_);
    return filesystem_active_;
  }
  void SetFilesystemActiveNoLock(bool active) { filesystem_active_ = active; }
  void SetFilesystemActive(bool active) {
    MutexLock l(&mutex_);
    SetFilesystemActiveNoLock(active);
  }
  void AssertNoOpenFile() { assert(open_files_.empty()); }

 private:
  port::Mutex mutex_;
  std::map<std::string, FileState> db_file_state_;
  std::set<std::string> open_files_;
  std::unordered_map<std::string, std::set<std::string>>
      dir_to_new_files_since_last_sync_;
  bool filesystem_active_;  // Record flushes, syncs, writes
};

}  // namespace util
}  // namespace xengine

#endif  // UTIL_FAULT_INJECTION_TEST_ENV_H_
