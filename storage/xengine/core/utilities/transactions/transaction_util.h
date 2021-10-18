// Portions Copyright (c) 2020, Alibaba Group Holding Limited
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#ifndef ROCKSDB_LITE

#include <string>
#include <unordered_map>

#include "xengine/db.h"
#include "xengine/slice.h"
#include "xengine/status.h"
#include "xengine/types.h"

namespace xengine {

namespace common
{
class ReadOptions;
}

namespace db {
class DBImpl;
struct SuperVersion;
}

namespace util {

struct TransactionKeyMapInfo {
  // Earliest sequence number that is relevant to this transaction for this key
  common::SequenceNumber seq;

  uint32_t num_writes;
  uint32_t num_reads;

  bool exclusive;

  explicit TransactionKeyMapInfo(common::SequenceNumber seq_no)
      : seq(seq_no), num_writes(0), num_reads(0), exclusive(false) {}
};

using TransactionKeyMap =
    std::unordered_map<uint32_t,
                       std::unordered_map<std::string, TransactionKeyMapInfo>>;

class WriteBatchWithIndex;

class TransactionUtil {
 public:
  // Verifies there have been no writes to this key in the db since this
  // sequence number.
  //
  // If cache_only is true, then this function will not attempt to read any
  // SST files.  This will make it more likely this function will
  // return an error if it is unable to determine if there are any conflicts.
  //
  // Returns OK on success, BUSY if there is a conflicting write, or other error
  // status for any unexpected errors.
  static common::Status CheckKeyForConflicts(
      db::DBImpl* db_impl, db::ColumnFamilyHandle* column_family,
      const std::string& key, common::SequenceNumber key_seq, bool cache_only,
      const bool lock_uk, const common::ReadOptions *read_opts);

  // For each key,common::SequenceNumber pair in the TransactionKeyMap,
  // this function
  // will verify there have been no writes to the key in the db since that
  // sequence number.
  //
  // Returns OK on success, BUSY if there is a conflicting write, or other error
  // status for any unexpected errors.
  //
  // REQUIRED: this function should only be called on the write thread or if the
  // mutex is held.
  static common::Status CheckKeysForConflicts(db::DBImpl* db_impl,
                                              const TransactionKeyMap& keys,
                                              bool cache_only);

  static common::Status check_unique_key(db::DBImpl* db_impl,
                                         db::ColumnFamilyHandle *column_family,
                                         const std::string &key,
                                         const common::SequenceNumber &key_seq,
                                         const common::ReadOptions *read_opts);

 private:
  static common::Status CheckKey(db::DBImpl* db_impl, db::SuperVersion* sv,
                                 common::SequenceNumber earliest_seq,
                                 common::SequenceNumber key_seq,
                                 const std::string& key, bool cache_only);
};

}  //  namespace util
}  //  namespace xengine
#endif  // ROCKSDB_LITE
