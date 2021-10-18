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

namespace xengine {
namespace db {

class DBImplReadOnly : public DBImpl {
 public:
  DBImplReadOnly(const common::DBOptions& options, const std::string& dbname);
  virtual ~DBImplReadOnly();

  // Implementations of the DB interface
  using DB::Get;
  virtual common::Status Get(const common::ReadOptions& options,
                             ColumnFamilyHandle* column_family,
                             const common::Slice& key,
                             common::PinnableSlice* value) override;

  // TODO: Implement ReadOnly MultiGet?

  using DBImpl::NewIterator;
  virtual Iterator* NewIterator(const common::ReadOptions&,
                                ColumnFamilyHandle* column_family) override;

  virtual common::Status NewIterators(
      const common::ReadOptions& options,
      const std::vector<ColumnFamilyHandle*>& column_families,
      std::vector<Iterator*>* iterators) override;

  using DBImpl::Put;
  virtual common::Status Put(const common::WriteOptions& options,
                             ColumnFamilyHandle* column_family,
                             const common::Slice& key,
                             const common::Slice& value) override {
    return common::Status::NotSupported(
        "Not supported operation in read only mode.");
  }
  using DBImpl::Merge;
  virtual common::Status Merge(const common::WriteOptions& options,
                               ColumnFamilyHandle* column_family,
                               const common::Slice& key,
                               const common::Slice& value) override {
    return common::Status::NotSupported(
        "Not supported operation in read only mode.");
  }
  using DBImpl::Delete;
  virtual common::Status Delete(const common::WriteOptions& options,
                                ColumnFamilyHandle* column_family,
                                const common::Slice& key) override {
    return common::Status::NotSupported(
        "Not supported operation in read only mode.");
  }
  using DBImpl::SingleDelete;
  virtual common::Status SingleDelete(const common::WriteOptions& options,
                                      ColumnFamilyHandle* column_family,
                                      const common::Slice& key) override {
    return common::Status::NotSupported(
        "Not supported operation in read only mode.");
  }
  virtual common::Status Write(const common::WriteOptions& options,
                               WriteBatch* updates) override {
    return common::Status::NotSupported(
        "Not supported operation in read only mode.");
  }
  using DBImpl::CompactRange;
  virtual common::Status CompactRange(
      const common::CompactRangeOptions& options,
      ColumnFamilyHandle* column_family, const common::Slice* begin,
      const common::Slice* end,
      const uint32_t manual_compact_type = 1/* Minor*/) override {
    return common::Status::NotSupported(
        "Not supported operation in read only mode.");
  }

  using DBImpl::CompactFiles;
  virtual common::Status CompactFiles(
      const common::CompactionOptions& compact_options,
      ColumnFamilyHandle* column_family,
      const std::vector<std::string>& input_file_names, const int output_level,
      const int output_path_id = -1) override {
    return common::Status::NotSupported(
        "Not supported operation in read only mode.");
  }

  virtual common::Status DisableFileDeletions() override {
    return common::Status::NotSupported(
        "Not supported operation in read only mode.");
  }

  virtual common::Status EnableFileDeletions(bool force) override {
    return common::Status::NotSupported(
        "Not supported operation in read only mode.");
  }
  virtual common::Status GetLiveFiles(std::vector<std::string>&,
                                      uint64_t* manifest_file_size,
                                      bool flush_memtable = true) override {
    return common::Status::NotSupported(
        "Not supported operation in read only mode.");
  }

  using DBImpl::Flush;
  virtual common::Status Flush(const common::FlushOptions& options,
                               ColumnFamilyHandle* column_family) override {
    return common::Status::NotSupported(
        "Not supported operation in read only mode.");
  }

  using DBImpl::SyncWAL;
  virtual common::Status SyncWAL() override {
    return common::Status::NotSupported(
        "Not supported operation in read only mode.");
  }

 private:
  friend class DB;

  // No copying allowed
  DBImplReadOnly(const DBImplReadOnly&);
  void operator=(const DBImplReadOnly&);
};
}
}
#endif  // !ROCKSDB_LITE
