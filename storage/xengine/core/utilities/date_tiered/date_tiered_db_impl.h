//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#ifndef ROCKSDB_LITE

#include <map>
#include <string>
#include <vector>

#include "monitoring/instrumented_mutex.h"
#include "options/cf_options.h"
#include "xengine/db.h"
#include "xengine/utilities/date_tiered_db.h"

namespace xengine {
namespace util {

// Implementation of DateTieredDB.
class DateTieredDBImpl : public DateTieredDB {
 public:
  DateTieredDBImpl(db::DB* db, common::Options options,
                   const std::vector<db::ColumnFamilyDescriptor>& descriptors,
                   const std::vector<db::ColumnFamilyHandle*>& handles,
                   int64_t ttl, int64_t column_family_interval);

  virtual ~DateTieredDBImpl();

  common::Status Put(const common::WriteOptions& options,
                     const common::Slice& key,
                     const common::Slice& val) override;

  common::Status Get(const common::ReadOptions& options,
                     const common::Slice& key, std::string* value) override;

  common::Status Delete(const common::WriteOptions& options,
                        const common::Slice& key) override;

  bool KeyMayExist(const common::ReadOptions& options, const common::Slice& key,
                   std::string* value, bool* value_found = nullptr) override;

  common::Status Merge(const common::WriteOptions& options,
                       const common::Slice& key,
                       const common::Slice& value) override;

  db::Iterator* NewIterator(const common::ReadOptions& opts) override;

  common::Status DropObsoleteColumnFamilies() override;

  // Extract timestamp from key.
  static common::Status GetTimestamp(const common::Slice& key, int64_t* result);

 private:
  // Base database object
  db::DB* db_;

  const common::ColumnFamilyOptions cf_options_;

  const common::ImmutableCFOptions ioptions_;

  // Storing all column family handles for time series data.
  std::vector<db::ColumnFamilyHandle*> handles_;

  // Manages a mapping from a column family's maximum timestamp to its handle.
  std::map<int64_t, db::ColumnFamilyHandle*> handle_map_;

  // A time-to-live value to indicate when the data should be removed.
  int64_t ttl_;

  // An variable to indicate the time range of a column family.
  int64_t column_family_interval_;

  // Indicate largest maximum timestamp of a column family.
  int64_t latest_timebound_;

  // Mutex to protect handle_map_ operations.
  monitor::InstrumentedMutex mutex_;

  // Internal method to execute Put and Merge in batch.
  common::Status Write(const common::WriteOptions& opts,
                       db::WriteBatch* updates);

  common::Status CreateColumnFamily(db::ColumnFamilyHandle** column_family);

  common::Status FindColumnFamily(int64_t keytime,
                                  db::ColumnFamilyHandle** column_family,
                                  bool create_if_missing);

  static bool IsStale(int64_t keytime, int64_t ttl, Env* env);
};

}  //  namespace util
}  //  namespace xengine
#endif  // ROCKSDB_LITE
