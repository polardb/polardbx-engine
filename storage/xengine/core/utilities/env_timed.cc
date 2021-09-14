// Portions Copyright (c) 2020, Alibaba Group Holding Limited
// Copyright (c) 2017-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "monitoring/query_perf_context.h"
#include "xengine/env.h"
#include "xengine/status.h"

using namespace xengine::common;
using namespace xengine::monitor;

namespace xengine {
namespace util {

#ifndef ROCKSDB_LITE

// An environment that measures function call times for filesystem
// operations, reporting results to variables in PerfContext.
class TimedEnv : public EnvWrapper {
 public:
  explicit TimedEnv(Env* base_env) : EnvWrapper(base_env) {}

  virtual Status NewSequentialFile(const std::string& fname,
                                   SequentialFile *&result,
                                   const EnvOptions& options) override {
    QUERY_TRACE_SCOPE(TracePoint::ENV_NEW_SEQ_FILE);
    return EnvWrapper::NewSequentialFile(fname, result, options);
  }

  virtual Status NewRandomAccessFile(const std::string& fname,
                                     RandomAccessFile *&result,
                                     const EnvOptions& options) override {
    QUERY_TRACE_SCOPE(TracePoint::ENV_NEW_RND_FILE);
    return EnvWrapper::NewRandomAccessFile(fname, result, options);
  }

  virtual Status NewWritableFile(const std::string& fname,
                                 WritableFile *&result,
                                 const EnvOptions& options) override {
    QUERY_TRACE_SCOPE(TracePoint::ENV_NEW_WRITE_FILE);
    return EnvWrapper::NewWritableFile(fname, result, options);
  }

  virtual Status ReuseWritableFile(const std::string& fname,
                                   const std::string& old_fname,
                                   WritableFile *&result,
                                   const EnvOptions& options) override {
    QUERY_TRACE_SCOPE(TracePoint::ENV_REUSE_WRITE_FILE);
    return EnvWrapper::ReuseWritableFile(fname, old_fname, result, options);
  }

  virtual Status NewRandomRWFile(const std::string& fname,
                                 RandomRWFile *&result,
                                 const EnvOptions& options) override {
    QUERY_TRACE_SCOPE(TracePoint::ENV_NEW_RND_RW_FILE);
    return EnvWrapper::NewRandomRWFile(fname, result, options);
  }

  virtual Status NewDirectory(const std::string& name,
                              Directory *&result) override {
    QUERY_TRACE_SCOPE(TracePoint::ENV_NEW_DIR);
    return EnvWrapper::NewDirectory(name, result);
  }

  virtual Status FileExists(const std::string& fname) override {
    QUERY_TRACE_SCOPE(TracePoint::ENV_FILE_EXISTS);
    return EnvWrapper::FileExists(fname);
  }

  virtual Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result) override {
    QUERY_TRACE_SCOPE(TracePoint::ENV_GET_CHILDREN);
    return EnvWrapper::GetChildren(dir, result);
  }

  virtual Status GetChildrenFileAttributes(
      const std::string& dir, std::vector<FileAttributes>* result) override {
    QUERY_TRACE_SCOPE(TracePoint::ENV_GET_CHILDREN_ATTR);
    return EnvWrapper::GetChildrenFileAttributes(dir, result);
  }

  virtual Status DeleteFile(const std::string& fname) override {
    QUERY_TRACE_SCOPE(TracePoint::ENV_DELETE_FILE);
    return EnvWrapper::DeleteFile(fname);
  }

  virtual Status CreateDir(const std::string& dirname) override {
    QUERY_TRACE_SCOPE(TracePoint::ENV_CREATE_DIR);
    return EnvWrapper::CreateDir(dirname);
  }

  virtual Status CreateDirIfMissing(const std::string& dirname) override {
    QUERY_TRACE_SCOPE(TracePoint::ENV_CREATE_DIR);
    return EnvWrapper::CreateDirIfMissing(dirname);
  }

  virtual Status DeleteDir(const std::string& dirname) override {
    QUERY_TRACE_SCOPE(TracePoint::ENV_DELETE_DIR);
    return EnvWrapper::DeleteDir(dirname);
  }

  virtual Status GetFileSize(const std::string& fname,
                             uint64_t* file_size) override {
    QUERY_TRACE_SCOPE(TracePoint::ENV_GET_FILE_SIZE);
    return EnvWrapper::GetFileSize(fname, file_size);
  }

  virtual Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* file_mtime) override {
    QUERY_TRACE_SCOPE(TracePoint::ENV_GET_MODIFIED);
    return EnvWrapper::GetFileModificationTime(fname, file_mtime);
  }

  virtual Status RenameFile(const std::string& src,
                            const std::string& dst) override {
    QUERY_TRACE_SCOPE(TracePoint::ENV_RENAME);
    return EnvWrapper::RenameFile(src, dst);
  }

  virtual Status LinkFile(const std::string& src,
                          const std::string& dst) override {
    QUERY_TRACE_SCOPE(TracePoint::ENV_LINK_FILE);
    return EnvWrapper::LinkFile(src, dst);
  }

  virtual Status LockFile(const std::string& fname, FileLock** lock) override {
    QUERY_TRACE_SCOPE(TracePoint::ENV_LOCK_FILE);
    return EnvWrapper::LockFile(fname, lock);
  }

  virtual Status UnlockFile(FileLock* lock) override {
    QUERY_TRACE_SCOPE(TracePoint::ENV_UNLOCK_FILE);
    return EnvWrapper::UnlockFile(lock);
  }

  virtual Status NewLogger(const std::string& fname,
                           shared_ptr<Logger>* result) override {
    QUERY_TRACE_SCOPE(TracePoint::NEW_LOGGER);
    return EnvWrapper::NewLogger(fname, result);
  }
};

Env* NewTimedEnv(Env* base_env) { return new TimedEnv(base_env); }

#else  // ROCKSDB_LITE

Env* NewTimedEnv(Env* base_env) { return nullptr; }

#endif  // !ROCKSDB_LITE

}  //  namespace util
}  //  namespace xengine
