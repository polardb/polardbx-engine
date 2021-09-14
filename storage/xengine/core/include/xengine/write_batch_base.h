/*
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <cstddef>

namespace xengine {
namespace common {
class Slice;
class Status;
struct SliceParts;
}

namespace db {
class ColumnFamilyHandle;
class WriteBatch;
// Abstract base class that defines the basic interface for a write batch.
// See WriteBatch for a basic implementation and WrithBatchWithIndex for an
// indexed implemenation.
class WriteBatchBase {
 public:
  virtual ~WriteBatchBase() {}

  // Store the mapping "key->value" in the database.
  virtual common::Status Put(ColumnFamilyHandle* column_family,
                             const common::Slice& key,
                             const common::Slice& value) = 0;
  virtual common::Status Put(const common::Slice& key,
                             const common::Slice& value) = 0;

  // Variant of Put() that gathers output like writev(2).  The key and value
  // that will be written to the database are concatentations of arrays of
  // slices.
  virtual common::Status Put(ColumnFamilyHandle* column_family,
                             const common::SliceParts& key,
                             const common::SliceParts& value);
  virtual common::Status Put(const common::SliceParts& key,
                             const common::SliceParts& value);

  // Merge "value" with the existing value of "key" in the database.
  // "key->merge(existing, value)"
  virtual common::Status Merge(ColumnFamilyHandle* column_family,
                               const common::Slice& key,
                               const common::Slice& value) = 0;
  virtual common::Status Merge(const common::Slice& key,
                               const common::Slice& value) = 0;

  // variant that takes common::SliceParts
  virtual common::Status Merge(ColumnFamilyHandle* column_family,
                               const common::SliceParts& key,
                               const common::SliceParts& value);
  virtual common::Status Merge(const common::SliceParts& key,
                               const common::SliceParts& value);

  // If the database contains a mapping for "key", erase it.  Else do nothing.
  virtual common::Status Delete(ColumnFamilyHandle* column_family,
                                const common::Slice& key) = 0;
  virtual common::Status Delete(const common::Slice& key) = 0;

  // variant that takes common::SliceParts
  virtual common::Status Delete(ColumnFamilyHandle* column_family,
                                const common::SliceParts& key);
  virtual common::Status Delete(const common::SliceParts& key);

  // If the database contains a mapping for "key", erase it. Expects that the
  // key was not overwritten. Else do nothing.
  virtual common::Status SingleDelete(ColumnFamilyHandle* column_family,
                                      const common::Slice& key) = 0;
  virtual common::Status SingleDelete(const common::Slice& key) = 0;

  // variant that takes common::SliceParts
  virtual common::Status SingleDelete(ColumnFamilyHandle* column_family,
                                      const common::SliceParts& key);
  virtual common::Status SingleDelete(const common::SliceParts& key);

  // If the database contains mappings in the range ["begin_key", "end_key"],
  // erase them. Else do nothing.
  virtual common::Status DeleteRange(ColumnFamilyHandle* column_family,
                                     const common::Slice& begin_key,
                                     const common::Slice& end_key) = 0;
  virtual common::Status DeleteRange(const common::Slice& begin_key,
                                     const common::Slice& end_key) = 0;

  // variant that takes common::SliceParts
  virtual common::Status DeleteRange(ColumnFamilyHandle* column_family,
                                     const common::SliceParts& begin_key,
                                     const common::SliceParts& end_key);
  virtual common::Status DeleteRange(const common::SliceParts& begin_key,
                                     const common::SliceParts& end_key);

  // Append a blob of arbitrary size to the records in this batch. The blob will
  // be stored in the transaction log but not in any other file. In particular,
  // it will not be persisted to the SST files. When iterating over this
  // WriteBatch, WriteBatch::Handler::LogData will be called with the contents
  // of the blob as it is encountered. Blobs, puts, deletes, and merges will be
  // encountered in the same order in thich they were inserted. The blob will
  // NOT consume sequence number(s) and will NOT increase the count of the batch
  //
  // Example application: add timestamps to the transaction log for use in
  // replication.
  virtual common::Status PutLogData(const common::Slice& blob) = 0;

  // Clear all updates buffered in this batch.
  virtual void Clear() = 0;

  // Covert this batch into a WriteBatch.  This is an abstracted way of
  // converting any WriteBatchBase(eg WriteBatchWithIndex) into a basic
  // WriteBatch.
  virtual WriteBatch* GetWriteBatch() = 0;

  // Records the state of the batch for future calls to RollbackToSavePoint().
  // May be called multiple times to set multiple save points.
  virtual void SetSavePoint() = 0;

  // Remove all entries in this batch (Put, Merge, Delete, PutLogData) since the
  // most recent call to SetSavePoint() and removes the most recent save point.
  // If there is no previous call to SetSavePoint(), behaves the same as
  // Clear().
  virtual common::Status RollbackToSavePoint() = 0;

  // Sets the maximum size of the write batch in bytes. 0 means no limit.
  virtual void SetMaxBytes(size_t max_bytes) = 0;
};

}  // namespace db
}  // namespace xengine
