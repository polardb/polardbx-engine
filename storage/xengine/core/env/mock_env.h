// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <atomic>
#include <map>
#include <string>
#include <vector>
#include "port/port.h"
#include "util/mutexlock.h"
#include "xengine/env.h"
#include "xengine/status.h"

namespace xengine {
namespace util {

class MemFile;
class MockEnv : public EnvWrapper {
 public:
  explicit MockEnv(Env* base_env);

  virtual ~MockEnv();

  // Partial implementation of the Env interface.
  virtual common::Status NewSequentialFile(const std::string& fname,
                                           SequentialFile *&result,
                                           const EnvOptions& soptions) override;

  virtual common::Status NewRandomAccessFile(
      const std::string& fname, RandomAccessFile *&result,
      const EnvOptions& soptions) override;

  virtual common::Status NewRandomRWFile(const std::string& fname,
                                         RandomRWFile *&result,
                                         const EnvOptions& options) override;

  virtual common::Status ReuseWritableFile(const std::string& fname,
                                           const std::string& old_fname,
                                           WritableFile *&result,
                                           const EnvOptions& options) override;

  virtual common::Status NewWritableFile(
      const std::string& fname, WritableFile *&result,
      const EnvOptions& env_options) override;

  virtual common::Status NewDirectory(const std::string& name,
                                      Directory *&result) override;

  virtual common::Status FileExists(const std::string& fname) override;

  virtual common::Status GetChildren(const std::string& dir,
                                     std::vector<std::string>* result) override;

  void DeleteFileInternal(const std::string& fname);

  virtual common::Status DeleteFile(const std::string& fname) override;

  virtual common::Status CreateDir(const std::string& dirname) override;

  virtual common::Status CreateDirIfMissing(
      const std::string& dirname) override;

  virtual common::Status DeleteDir(const std::string& dirname) override;

  virtual common::Status GetFileSize(const std::string& fname,
                                     uint64_t* file_size) override;

  virtual common::Status GetFileModificationTime(const std::string& fname,
                                                 uint64_t* time) override;

  virtual common::Status RenameFile(const std::string& src,
                                    const std::string& target) override;

  virtual common::Status LinkFile(const std::string& src,
                                  const std::string& target) override;

  virtual common::Status NewLogger(const std::string& fname,
                                   shared_ptr<Logger>* result) override;

  virtual common::Status LockFile(const std::string& fname,
                                  FileLock** flock) override;

  virtual common::Status UnlockFile(FileLock* flock) override;

  virtual common::Status GetTestDirectory(std::string* path) override;

  // Results of these can be affected by FakeSleepForMicroseconds()
  virtual common::Status GetCurrentTime(int64_t* unix_time) override;
  virtual uint64_t NowMicros() override;
  virtual uint64_t NowNanos() override;

  // Non-virtual functions, specific to MockEnv
  common::Status Truncate(const std::string& fname, size_t size);

  common::Status CorruptBuffer(const std::string& fname);

  // Doesn't really sleep, just affects output of GetCurrentTime(), NowMicros()
  // and NowNanos()
  void FakeSleepForMicroseconds(int64_t micros);

 private:
  std::string NormalizePath(const std::string path);

  // Map from filenames to MemFile objects, representing a simple file system.
  typedef std::map<std::string, MemFile*> FileSystem;
  port::Mutex mutex_;
  FileSystem file_map_;  // Protected by mutex_.

  std::atomic<int64_t> fake_sleep_micros_;
};

}  // namespace util
}  // namespace xengine
