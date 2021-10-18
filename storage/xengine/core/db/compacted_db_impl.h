// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#ifndef ROCKSDB_LITE
#include <string>
#include <vector>
#include "db/db_impl.h"
#include "storage/storage_manager.h"

namespace xengine {

namespace db {

class CompactedDBImpl : public DBImpl {
 public:
  CompactedDBImpl(const common::DBOptions& options, const std::string& dbname);
  virtual ~CompactedDBImpl();

  static common::Status Open(const common::Options& options,
                             const std::string& dbname, DB** dbptr);

  // Implementations of the DB interface
  using DB::Get;
  virtual common::Status Get(const common::ReadOptions& options,
                             ColumnFamilyHandle* column_family,
                             const common::Slice& key,
                             common::PinnableSlice* value) override;
  using DB::MultiGet;
  virtual std::vector<common::Status> MultiGet(
      const common::ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>&,
      const std::vector<common::Slice>& keys,
      std::vector<std::string>* values) override;

  using DBImpl::Put;
  virtual common::Status Put(const common::WriteOptions& options,
                             ColumnFamilyHandle* column_family,
                             const common::Slice& key,
                             const common::Slice& value) override {
    return common::Status::NotSupported("Not supported in compacted db mode.");
  }
  using DBImpl::Merge;
  virtual common::Status Merge(const common::WriteOptions& options,
                               ColumnFamilyHandle* column_family,
                               const common::Slice& key,
                               const common::Slice& value) override {
    return common::Status::NotSupported("Not supported in compacted db mode.");
  }
  using DBImpl::Delete;
  virtual common::Status Delete(const common::WriteOptions& options,
                                ColumnFamilyHandle* column_family,
                                const common::Slice& key) override {
    return common::Status::NotSupported("Not supported in compacted db mode.");
  }
  virtual common::Status Write(const common::WriteOptions& options,
                               WriteBatch* updates) override {
    return common::Status::NotSupported("Not supported in compacted db mode.");
  }
  using DBImpl::CompactRange;
  virtual common::Status CompactRange(
      const common::CompactRangeOptions& options,
      ColumnFamilyHandle* column_family, const common::Slice* begin,
      const common::Slice* end,
      const uint32_t manual_compact_type = 1/* Minor*/) override {
    return common::Status::NotSupported("Not supported in compacted db mode.");
  }

  virtual common::Status DisableFileDeletions() override {
    return common::Status::NotSupported("Not supported in compacted db mode.");
  }
  virtual common::Status EnableFileDeletions(bool force) override {
    return common::Status::NotSupported("Not supported in compacted db mode.");
  }
  virtual common::Status GetLiveFiles(std::vector<std::string>&,
                                      uint64_t* manifest_file_size,
                                      bool flush_memtable = true) override {
    return common::Status::NotSupported("Not supported in compacted db mode.");
  }
  using DBImpl::Flush;
  virtual common::Status Flush(const common::FlushOptions& options,
                               ColumnFamilyHandle* column_family) override {
    return common::Status::NotSupported("Not supported in compacted db mode.");
  }

 private:
  friend class DB;
  inline size_t FindFile(const common::Slice& key);
  common::Status Init(const common::Options& options);

  ColumnFamilyData* cfd_;
  Version* version_;
  const util::Comparator* user_comparator_;
  LevelFilesBrief files_;

  // No copying allowed
  CompactedDBImpl(const CompactedDBImpl&);
  void operator=(const CompactedDBImpl&);
};
}  // db
}  // xengine
#endif  // ROCKSDB_LITE
