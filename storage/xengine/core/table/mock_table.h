// Portions Copyright (c) 2020, Alibaba Group Holding Limited
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <algorithm>
#include <atomic>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "port/port.h"
#include "table/internal_iterator.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
#include "util/kv_map.h"
#include "util/mutexlock.h"
#include "util/testharness.h"
#include "util/testutil.h"
#include "xengine/comparator.h"
#include "xengine/table.h"

namespace xengine {

namespace db {
struct MiniTables;
}

namespace table {
namespace mock {

util::stl_wrappers::KVMap MakeMockFile(
    std::initializer_list<std::pair<const std::string, std::string>> l = {});

struct MockTableFileSystem {
  port::Mutex mutex;
  std::map<uint32_t, util::stl_wrappers::KVMap> files;
};

class MockTableReader : public TableReader {
 public:
  explicit MockTableReader(const util::stl_wrappers::KVMap& table)
      : table_(table) {}

  InternalIterator* NewIterator(const common::ReadOptions&,
                                memory::SimpleAllocator* arena = nullptr,
                                bool skip_filters = false,
                                const uint64_t scan_add_blocks_limit = 0) override;

  common::Status Get(const common::ReadOptions&, const common::Slice& key,
                     GetContext* get_context,
                     bool skip_filters = false) override;

  uint64_t ApproximateOffsetOf(const common::Slice& key) override { return 0; }

  virtual size_t ApproximateMemoryUsage() const override { return 0; }

  void SetupForCompaction() override {}

  std::shared_ptr<const TableProperties> GetTableProperties() const override;

  ~MockTableReader() {}

 private:
  const util::stl_wrappers::KVMap& table_;
};

class MockTableIterator : public InternalIterator {
 public:
  explicit MockTableIterator(const util::stl_wrappers::KVMap& table)
      : table_(table) {
    itr_ = table_.end();
  }

  bool Valid() const override { return itr_ != table_.end(); }

  void SeekToFirst() override { itr_ = table_.begin(); }

  void SeekToLast() override {
    itr_ = table_.end();
    --itr_;
  }

  void Seek(const common::Slice& target) override {
    std::string str_target(target.data(), target.size());
    itr_ = table_.lower_bound(str_target);
  }

  void SeekForPrev(const common::Slice& target) override {
    std::string str_target(target.data(), target.size());
    itr_ = table_.upper_bound(str_target);
    Prev();
  }

  void Next() override { ++itr_; }

  void Prev() override {
    if (itr_ == table_.begin()) {
      itr_ = table_.end();
    } else {
      --itr_;
    }
  }

  common::Slice key() const override { return common::Slice(itr_->first); }

  common::Slice value() const override { return common::Slice(itr_->second); }

  common::Status status() const override { return common::Status::OK(); }

 private:
  const util::stl_wrappers::KVMap& table_;
  util::stl_wrappers::KVMap::const_iterator itr_;
};

class MockTableBuilder : public TableBuilder {
 public:
  MockTableBuilder(uint32_t id, MockTableFileSystem* file_system)
      : id_(id), file_system_(file_system) {
    table_ = MakeMockFile({});
  }

  // REQUIRES: Either Finish() or Abandon() has been called.
  ~MockTableBuilder() {}

  // Add key,value to the table being constructed.
  // REQUIRES: key is after any previously added key according to comparator.
  // REQUIRES: Finish(), Abandon() have not been called
  int Add(const common::Slice& key, const common::Slice& value) override {
    table_.insert({key.ToString(), value.ToString()});
    return common::Status::kOk;
  }

  // Return non-ok iff some error has been detected.
  common::Status status() const override { return common::Status::OK(); }

  int Finish() override {
    util::MutexLock lock_guard(&file_system_->mutex);
    file_system_->files.insert({id_, table_});
    return common::Status::kOk;
  }

  int Abandon() override { return common::Status::kOk; }

  uint64_t NumEntries() const override { return table_.size(); }

  uint64_t FileSize() const override { return table_.size(); }

  TableProperties GetTableProperties() const override {
    return TableProperties();
  }

 private:
  uint32_t id_;
  MockTableFileSystem* file_system_;
  util::stl_wrappers::KVMap table_;
};

class MockTableFactory : public TableFactory {
 public:
  MockTableFactory();
  const char* Name() const override { return "MockTable"; }
  common::Status NewTableReader(
      const TableReaderOptions& table_reader_options,
      util::RandomAccessFileReader *file, uint64_t file_size,
      TableReader *&table_reader,
      bool prefetch_index_and_filter_in_cache = true,
      memory::SimpleAllocator *arena = nullptr) const override;
  TableBuilder* NewTableBuilder(
      const TableBuilderOptions& table_builder_options,
      uint32_t column_familly_id,
      util::WritableFileWriter* file) const override;

  TableBuilder* NewTableBuilderExt(
      const TableBuilderOptions& table_builder_options,
      uint32_t column_family_id, db::MiniTables* mtables) const override;

  // This function will directly create mock table instead of going through
  // MockTableBuilder. file_contents has to have a format of <internal_key,
  // value>. Those key-value pairs will then be inserted into the mock table.
  common::Status CreateMockTable(util::Env* env, const std::string& fname,
                                 util::stl_wrappers::KVMap file_contents);

  virtual common::Status SanitizeOptions(
      const common::DBOptions& db_opts,
      const common::ColumnFamilyOptions& cf_opts) const override {
    return common::Status::OK();
  }

  virtual std::string GetPrintableTableOptions() const override {
    return std::string();
  }

  // This function will assert that only a single file exists and that the
  // contents are equal to file_contents
  void AssertSingleFile(const util::stl_wrappers::KVMap& file_contents);
  void AssertLatestFile(const util::stl_wrappers::KVMap& file_contents);

 private:
  uint32_t GetAndWriteNextID(util::WritableFileWriter* file) const;
  uint32_t GetIDFromFile(util::RandomAccessFileReader* file) const;

  mutable MockTableFileSystem file_system_;
  mutable std::atomic<uint32_t> next_id_;
};

}  // namespace mock
}  // namespace table
}  // namespace xengine
