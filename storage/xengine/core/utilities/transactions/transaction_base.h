// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#ifndef ROCKSDB_LITE

#include <stack>
#include <string>
#include <vector>

#include "utilities/transactions/transaction_util.h"
#include "xengine/db.h"
#include "xengine/slice.h"
#include "xengine/snapshot.h"
#include "xengine/status.h"
#include "xengine/types.h"
#include "xengine/utilities/transaction.h"
#include "xengine/utilities/transaction_db.h"
#include "xengine/utilities/write_batch_with_index.h"

namespace xengine {
namespace util {

class TransactionBaseImpl : public util::Transaction {
 public:
  TransactionBaseImpl(db::DB* db, const common::WriteOptions& write_options);

  virtual ~TransactionBaseImpl();

  // Remove pending operations queued in this transaction.
  virtual void Clear();

  void Reinitialize(db::DB* db, const common::WriteOptions& write_options);

  // Called before executing Put, Merge, Delete, and GetForUpdate.  If TryLock
  // returns non-OK, the Put/Merge/Delete/GetForUpdate will be failed.
  // untracked will be true if called from PutUntracked, DeleteUntracked, or
  // MergeUntracked.
  virtual common::Status TryLock(db::ColumnFamilyHandle* column_family,
                                 const common::Slice& key, bool read_only,
                                 bool exclusive, bool untracked = false,
                                 bool lock_uk = false, const common::ReadOptions *options = nullptr) = 0;

  void SetSavePoint() override;

  common::Status RollbackToSavePoint() override;

  common::Status Get(const common::ReadOptions& options,
                     db::ColumnFamilyHandle* column_family,
                     const common::Slice& key, std::string* value) override;

  common::Status Get(const common::ReadOptions& options,
                     const common::Slice& key, std::string* value) override {
    return Get(options, db_->DefaultColumnFamily(), key, value);
  }

  common::Status GetForUpdate(const common::ReadOptions& options,
                              db::ColumnFamilyHandle* column_family,
                              const common::Slice& key, std::string* value,
                              bool exclusive) override;

  common::Status GetForUpdate(const common::ReadOptions& options,
                              const common::Slice& key, std::string* value,
                              bool exclusive) override {
    return GetForUpdate(options, db_->DefaultColumnFamily(), key, value,
                        exclusive);
  }

  std::vector<common::Status> MultiGet(
      const common::ReadOptions& options,
      const std::vector<db::ColumnFamilyHandle*>& column_family,
      const std::vector<common::Slice>& keys,
      std::vector<std::string>* values) override;

  std::vector<common::Status> MultiGet(
      const common::ReadOptions& options,
      const std::vector<common::Slice>& keys,
      std::vector<std::string>* values) override {
    return MultiGet(options, std::vector<db::ColumnFamilyHandle*>(
                                 keys.size(), db_->DefaultColumnFamily()),
                    keys, values);
  }

  std::vector<common::Status> MultiGetForUpdate(
      const common::ReadOptions& options,
      const std::vector<db::ColumnFamilyHandle*>& column_family,
      const std::vector<common::Slice>& keys,
      std::vector<std::string>* values) override;

  std::vector<common::Status> MultiGetForUpdate(
      const common::ReadOptions& options,
      const std::vector<common::Slice>& keys,
      std::vector<std::string>* values) override {
    return MultiGetForUpdate(options,
                             std::vector<db::ColumnFamilyHandle*>(
                                 keys.size(), db_->DefaultColumnFamily()),
                             keys, values);
  }

  Iterator* GetIterator(const common::ReadOptions& read_options) override;
  Iterator* GetIterator(const common::ReadOptions& read_options,
                        db::ColumnFamilyHandle* column_family) override;

  common::Status Put(db::ColumnFamilyHandle* column_family,
                     const common::Slice& key,
                     const common::Slice& value) override;
  common::Status Put(const common::Slice& key,
                     const common::Slice& value) override {
    return Put(nullptr, key, value);
  }

  common::Status Put(db::ColumnFamilyHandle* column_family,
                     const common::SliceParts& key,
                     const common::SliceParts& value) override;
  common::Status Put(const common::SliceParts& key,
                     const common::SliceParts& value) override {
    return Put(nullptr, key, value);
  }

  common::Status Merge(db::ColumnFamilyHandle* column_family,
                       const common::Slice& key,
                       const common::Slice& value) override;
  common::Status Merge(const common::Slice& key,
                       const common::Slice& value) override {
    return Merge(nullptr, key, value);
  }

  common::Status Delete(db::ColumnFamilyHandle* column_family,
                        const common::Slice& key) override;
  common::Status Delete(const common::Slice& key) override {
    return Delete(nullptr, key);
  }
  common::Status Delete(db::ColumnFamilyHandle* column_family,
                        const common::SliceParts& key) override;
  common::Status Delete(const common::SliceParts& key) override {
    return Delete(nullptr, key);
  }

  common::Status SingleDelete(db::ColumnFamilyHandle* column_family,
                              const common::Slice& key) override;
  common::Status SingleDelete(const common::Slice& key) override {
    return SingleDelete(nullptr, key);
  }
  common::Status SingleDelete(db::ColumnFamilyHandle* column_family,
                              const common::SliceParts& key) override;
  common::Status SingleDelete(const common::SliceParts& key) override {
    return SingleDelete(nullptr, key);
  }

  common::Status PutUntracked(db::ColumnFamilyHandle* column_family,
                              const common::Slice& key,
                              const common::Slice& value) override;
  common::Status PutUntracked(const common::Slice& key,
                              const common::Slice& value) override {
    return PutUntracked(nullptr, key, value);
  }

  common::Status PutUntracked(db::ColumnFamilyHandle* column_family,
                              const common::SliceParts& key,
                              const common::SliceParts& value) override;
  common::Status PutUntracked(const common::SliceParts& key,
                              const common::SliceParts& value) override {
    return PutUntracked(nullptr, key, value);
  }

  common::Status MergeUntracked(db::ColumnFamilyHandle* column_family,
                                const common::Slice& key,
                                const common::Slice& value) override;
  common::Status MergeUntracked(const common::Slice& key,
                                const common::Slice& value) override {
    return MergeUntracked(nullptr, key, value);
  }

  common::Status DeleteUntracked(db::ColumnFamilyHandle* column_family,
                                 const common::Slice& key) override;
  common::Status DeleteUntracked(const common::Slice& key) override {
    return DeleteUntracked(nullptr, key);
  }
  common::Status DeleteUntracked(db::ColumnFamilyHandle* column_family,
                                 const common::SliceParts& key) override;
  common::Status DeleteUntracked(const common::SliceParts& key) override {
    return DeleteUntracked(nullptr, key);
  }

  void PutLogData(const common::Slice& blob) override;

  WriteBatchWithIndex* GetWriteBatch() override;

  virtual void SetLockTimeout(int64_t timeout) override { /* Do nothing */
  }

  const db::Snapshot* GetSnapshot() const override {
    return snapshot_ ? snapshot_.get() : nullptr;
  }

  void SetSnapshot() override;
  void SetSnapshotOnNextOperation(
      std::shared_ptr<TransactionNotifier> notifier = nullptr) override;

  void ClearSnapshot() override {
    snapshot_.reset();
    snapshot_needed_ = false;
    snapshot_notifier_ = nullptr;
  }

  void DisableIndexing() override { indexing_enabled_ = false; }

  void EnableIndexing() override { indexing_enabled_ = true; }

  uint64_t GetElapsedTime() const override;

  uint64_t GetNumPuts() const override;

  uint64_t GetNumDeletes() const override;

  uint64_t GetNumMerges() const override;

  uint64_t GetNumKeys() const override;

  void UndoGetForUpdate(db::ColumnFamilyHandle* column_family,
                        const common::Slice& key) override;
  void UndoGetForUpdate(const common::Slice& key) override {
    return UndoGetForUpdate(nullptr, key);
  };

  // Get list of keys in this transaction that must not have any conflicts
  // with writes in other transactions.
  const TransactionKeyMap& GetTrackedKeys() const { return tracked_keys_; }

  TransactionKeyMap& GetTrackedKeysRef() { return tracked_keys_; }

  common::WriteOptions* GetWriteOptions() override { return &write_options_; }

  void SetWriteOptions(const common::WriteOptions& write_options) override {
    write_options_ = write_options;
  }

  // Used for memory management for snapshot_
  void ReleaseSnapshot(const db::Snapshot* snapshot, db::DB* db);

  // iterates over the given batch and makes the appropriate inserts.
  // used for rebuilding prepared transactions after recovery.
  common::Status RebuildFromWriteBatch(db::WriteBatch* src_batch) override;

  db::WriteBatch* GetCommitTimeWriteBatch() override;

 protected:
  // Add a key to the list of tracked keys.
  //
  // seqno is the earliest seqno this key was involved with this transaction.
  // readonly should be set to true if no data was written for this key
  void TrackKey(uint32_t cfh_id, const std::string& key,
                common::SequenceNumber seqno, bool readonly, bool exclusive);

  // Helper function to add a key to the given TransactionKeyMap
  static void TrackKey(TransactionKeyMap* key_map, uint32_t cfh_id,
                       const std::string& key, common::SequenceNumber seqno,
                       bool readonly, bool exclusive);

  // Called when UndoGetForUpdate determines that this key can be unlocked.
  virtual void UnlockGetForUpdate(db::ColumnFamilyHandle* column_family,
                                  const common::Slice& key) = 0;

  std::unique_ptr<TransactionKeyMap> GetTrackedKeysSinceSavePoint();

  // Sets a snapshot if SetSnapshotOnNextOperation() has been called.
  void SetSnapshotIfNeeded();

  bool IsSnapshotNeeded();

  db::DB* db_;
  db::DBImpl* dbimpl_;

  common::WriteOptions write_options_;

  const Comparator* cmp_;

  // Stores that time the txn was constructed, in microseconds.
  uint64_t start_time_;

  // Stores the current snapshot that was set by SetSnapshot or null if
  // no snapshot is currently set.
  std::shared_ptr<const db::Snapshot> snapshot_;

  // Count of various operations pending in this transaction
  uint64_t num_puts_ = 0;
  uint64_t num_deletes_ = 0;
  uint64_t num_merges_ = 0;

  struct SavePoint {
    std::shared_ptr<const db::Snapshot> snapshot_;
    bool snapshot_needed_;
    std::shared_ptr<TransactionNotifier> snapshot_notifier_;
    uint64_t num_puts_;
    uint64_t num_deletes_;
    uint64_t num_merges_;

    // Record all keys tracked since the last savepoint
    TransactionKeyMap new_keys_;

    SavePoint(std::shared_ptr<const db::Snapshot> snapshot,
              bool snapshot_needed,
              std::shared_ptr<TransactionNotifier> snapshot_notifier,
              uint64_t num_puts, uint64_t num_deletes, uint64_t num_merges)
        : snapshot_(snapshot),
          snapshot_needed_(snapshot_needed),
          snapshot_notifier_(snapshot_notifier),
          num_puts_(num_puts),
          num_deletes_(num_deletes),
          num_merges_(num_merges) {}
  };

  // Records writes pending in this transaction
  WriteBatchWithIndex write_batch_;

 private:
  // batch to be written at commit time
  db::WriteBatch commit_time_batch_;

  // Stack of the db::Snapshot saved at each save point.  Saved
  // snapshots may be
  // nullptr if there was no snapshot at the time SetSavePoint() was called.
  std::unique_ptr<std::stack<TransactionBaseImpl::SavePoint>> save_points_;

  // Map from column_family_id to map of keys that are involved in this
  // transaction.
  // Pessimistic Transactions will do conflict checking before adding a key
  // by calling TrackKey().
  // Optimistic Transactions will wait till commit time to do conflict checking.
  TransactionKeyMap tracked_keys_;

  // If true, future Put/Merge/Deletes will be indexed in the
  // WriteBatchWithIndex.
  // If false, future Put/Merge/Deletes will be inserted directly into the
  // underlying WriteBatch and not indexed in the WriteBatchWithIndex.
  bool indexing_enabled_;

  // SetSnapshotOnNextOperation() has been called and the snapshot has not yet
  // been reset.
  bool snapshot_needed_ = false;

  // SetSnapshotOnNextOperation() has been called and the caller would like
  // a notification through the TransactionNotifier interface
  std::shared_ptr<TransactionNotifier> snapshot_notifier_ = nullptr;

  common::Status TryLock(db::ColumnFamilyHandle* column_family,
                         const common::SliceParts& key, bool read_only,
                         bool exclusive, bool untracked = false,
                         bool lock_uk = false, const common::ReadOptions *options = nullptr);

  db::WriteBatchBase* GetBatchForWrite();

  void SetSnapshotInternal(const db::Snapshot* snapshot);
};

}  // namespace util
}  // namespace xengine
#endif  // ROCKSDB_LITE
