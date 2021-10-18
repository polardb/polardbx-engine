/*
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#ifndef ROCKSDB_UTILITIES_ENV_LIBRADOS_H
#define ROCKSDB_UTILITIES_ENV_LIBRADOS_H

#include <memory>
#include <string>

#include "xengine/status.h"
#include "xengine/utilities/env_mirror.h"

#include <rados/librados.hpp>

namespace xengine {
namespace util {
class LibradosWritableFile;

class EnvLibrados : public EnvWrapper {
 public:
  // Create a brand new sequentially-readable file with the specified name.
  // On success, stores a pointer to the new file in *result and returns OK.
  // On failure stores nullptr in *result and returns non-OK.  If the file does
  // not exist, returns a non-OK status.
  //
  // The returned file will only be accessed by one thread at a time.
  common::Status NewSequentialFile(const std::string& fname,
                                   std::unique_ptr<SequentialFile>* result,
                                   const EnvOptions& options) override;

  // Create a brand new random access read-only file with the
  // specified name.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.  If the file does not exist, returns a non-OK
  // status.
  //
  // The returned file may be concurrently accessed by multiple threads.
  common::Status NewRandomAccessFile(const std::string& fname,
                                     std::unique_ptr<RandomAccessFile>* result,
                                     const EnvOptions& options) override;

  // Create an object that writes to a new file with the specified
  // name.  Deletes any existing file with the same name and creates a
  // new file.  On success, stores a pointer to the new file in
  // *result and returns OK.  On failure stores nullptr in *result and
  // returns non-OK.
  //
  // The returned file will only be accessed by one thread at a time.
  common::Status NewWritableFile(const std::string& fname,
                                 std::unique_ptr<WritableFile>* result,
                                 const EnvOptions& options) override;

  // Reuse an existing file by renaming it and opening it as writable.
  common::Status ReuseWritableFile(const std::string& fname,
                                   const std::string& old_fname,
                                   std::unique_ptr<WritableFile>* result,
                                   const EnvOptions& options) override;

  // Create an object that represents a directory. Will fail if directory
  // doesn't exist. If the directory exists, it will open the directory
  // and create a new Directory object.
  //
  // On success, stores a pointer to the new Directory in
  // *result and returns OK. On failure stores nullptr in *result and
  // returns non-OK.
  common::Status NewDirectory(const std::string& name,
                              std::unique_ptr<Directory>* result) override;

  // Returns OK if the named file exists.
  //         NotFound if the named file does not exist,
  //                  the calling process does not have permission to determine
  //                  whether this file exists, or if the path is invalid.
  //         IOError if an IO Error was encountered
  common::Status FileExists(const std::string& fname) override;

  // Store in *result the names of the children of the specified directory.
  // The names are relative to "dir".
  // Original contents of *results are dropped.
  common::Status GetChildren(const std::string& dir,
                             std::vector<std::string>* result);

  // Delete the named file.
  common::Status DeleteFile(const std::string& fname) override;

  // Create the specified directory. Returns error if directory exists.
  common::Status CreateDir(const std::string& dirname) override;

  // Creates directory if missing. Return Ok if it exists, or successful in
  // Creating.
  common::Status CreateDirIfMissing(const std::string& dirname) override;

  // Delete the specified directory.
  common::Status DeleteDir(const std::string& dirname) override;

  // Store the size of fname in *file_size.
  common::Status GetFileSize(const std::string& fname,
                             uint64_t* file_size) override;

  // Store the last modification time of fname in *file_mtime.
  common::Status GetFileModificationTime(const std::string& fname,
                                         uint64_t* file_mtime) override;
  // Rename file src to target.
  common::Status RenameFile(const std::string& src,
                            const std::string& target) override;
  // Hard Link file src to target.
  common::Status LinkFile(const std::string& src,
                          const std::string& target) override;

  // Lock the specified file.  Used to prevent concurrent access to
  // the same db by multiple processes.  On failure, stores nullptr in
  // *lock and returns non-OK.
  //
  // On success, stores a pointer to the object that represents the
  // acquired lock in *lock and returns OK.  The caller should call
  // UnlockFile(*lock) to release the lock.  If the process exits,
  // the lock will be automatically released.
  //
  // If somebody else already holds the lock, finishes immediately
  // with a failure.  I.e., this call does not wait for existing locks
  // to go away.
  //
  // May create the named file if it does not already exist.
  common::Status LockFile(const std::string& fname, FileLock** lock);

  // Release the lock acquired by a previous successful call to LockFile.
  // REQUIRES: lock was returned by a successful LockFile() call
  // REQUIRES: lock has not already been unlocked.
  common::Status UnlockFile(FileLock* lock);

  // Get full directory name for this db.
  common::Status GetAbsolutePath(const std::string& db_path,
                                 std::string* output_path);

  // Generate unique id
  std::string GenerateUniqueId();

  // Get default EnvLibrados
  static EnvLibrados* Default();

  explicit EnvLibrados(const std::string& db_name,
                       const std::string& config_path,
                       const std::string& db_pool);

  explicit EnvLibrados(
      const std::string& client_name,  // first 3 parameters are
                                       // for RADOS client init
      const std::string& cluster_name, const uint64_t flags,
      const std::string& db_name, const std::string& config_path,
      const std::string& db_pool, const std::string& wal_dir,
      const std::string& wal_pool, const uint64_t write_buffer_size);
  ~EnvLibrados() { _rados.shutdown(); }

 private:
  std::string _client_name;
  std::string _cluster_name;
  uint64_t _flags;
  std::string _db_name;  // get from user, readable string; Also used as db_id
                         // for db metadata
  std::string _config_path;
  librados::Rados _rados;  // RADOS client
  std::string _db_pool_name;
  librados::IoCtx _db_pool_ioctx;  // IoCtx for connecting db_pool
  std::string _wal_dir;            // WAL dir path
  std::string _wal_pool_name;
  librados::IoCtx _wal_pool_ioctx;  // IoCtx for connecting wal_pool
  uint64_t _write_buffer_size;      // WritableFile buffer max size

  /* private function to communicate with rados */
  std::string _CreateFid();
  common::Status _GetFid(const std::string& fname, std::string& fid);
  common::Status _GetFid(const std::string& fname, std::string& fid,
                         int fid_len);
  common::Status _RenameFid(const std::string& old_fname,
                            const std::string& new_fname);
  common::Status _AddFid(const std::string& fname, const std::string& fid);
  common::Status _DelFid(const std::string& fname);
  common::Status _GetSubFnames(const std::string& dirname,
                               std::vector<std::string>* result);
  librados::IoCtx* _GetIoctx(const std::string& prefix);
  friend class LibradosWritableFile;
};
}  // namespace util
}  // namespace xengine
#endif
