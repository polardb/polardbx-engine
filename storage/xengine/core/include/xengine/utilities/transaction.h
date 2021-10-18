/*
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#ifndef ROCKSDB_LITE

#include <string>
#include <vector>

#include "xengine/comparator.h"
#include "xengine/db.h"
#include "xengine/status.h"

namespace xengine {

namespace db {
class Iterator;
class WriteBatch;
}

namespace util {
class TransactionDB;
class WriteBatchWithIndex;

using TransactionName = std::string;

using TransactionID = uint64_t;

// Provides notification to the caller of SetSnapshotOnNextOperation when
// the actual snapshot gets created
class TransactionNotifier {
 public:
  virtual ~TransactionNotifier() {}

  // Implement this method to receive notification when a snapshot is
  // requested via SetSnapshotOnNextOperation.
  virtual void SnapshotCreated(const db::Snapshot* newSnapshot) = 0;
};

// Provides BEGIN/COMMIT/ROLLBACK transactions.
//
// To use transactions, you must first create either an OptimisticTransactionDB
// or a TransactionDB.  See examples/[optimistic_]transaction_example.cc for
// more information.
//
// To create a transaction, use [Optimistic]TransactionDB::BeginTransaction().
//
// It is up to the caller to synchronize access to this object.
//
// See examples/transaction_example.cc for some simple examples.
//
// TODO(agiardullo): Not yet implemented
//  -PerfContext statistics
//  -Support for using Transactions with DBWithTTL
class Transaction {
 public:
  virtual ~Transaction() {}

  // If a transaction has a snapshot set, the transaction will ensure that
  // any keys successfully written(or fetched via GetForUpdate()) have not
  // been modified outside of this transaction since the time the snapshot was
  // set.
  // If a snapshot has not been set, the transaction guarantees that keys have
  // not been modified since the time each key was first written (or fetched via
  // GetForUpdate()).
  //
  // Using SetSnapshot() will provide stricter isolation guarantees at the
  // expense of potentially more transaction failures due to conflicts with
  // other writes.
  //
  // Calling SetSnapshot() has no effect on keys written before this function
  // has been called.
  //
  // SetSnapshot() may be called multiple times if you would like to change
  // the snapshot used for different operations in this transaction.
  //
  // Calling SetSnapshot will not affect the version of Data returned by Get()
  // methods.  See Transaction::Get() for more details.
  virtual void SetSnapshot() = 0;

  // Similar to SetSnapshot(), but will not change the current snapshot
  // until Put/Merge/Delete/GetForUpdate/MultigetForUpdate is called.
  // By calling this function, the transaction will essentially call
  // SetSnapshot() for you right before performing the next write/GetForUpdate.
  //
  // Calling SetSnapshotOnNextOperation() will not affect what snapshot is
  // returned by GetSnapshot() until the next write/GetForUpdate is executed.
  //
  // When the snapshot is created the notifier's SnapshotCreated method will
  // be called so that the caller can get access to the snapshot.
  //
  // This is an optimization to reduce the likelihood of conflicts that
  // could occur in between the time SetSnapshot() is called and the first
  // write/GetForUpdate operation.  Eg, this prevents the following
  // race-condition:
  //
  //   txn1->SetSnapshot();
  //                             txn2->Put("A", ...);
  //                             txn2->Commit();
  //   txn1->GetForUpdate(opts, "A", ...);  // FAIL!
  virtual void SetSnapshotOnNextOperation(
      std::shared_ptr<TransactionNotifier> notifier = nullptr) = 0;

  // Returns the db::Snapshot created by the last call to SetSnapshot().
  //
  // REQUIRED: The returned db::Snapshot is only valid up until the next time
  // SetSnapshot()/SetSnapshotOnNextSavePoint() is called, ClearSnapshot()
  // is called, or the Transaction is deleted.
  virtual const db::Snapshot* GetSnapshot() const = 0;

  // Clears the current snapshot (i.e. no snapshot will be 'set')
  //
  // This removes any snapshot that currently exists or is set to be created
  // on the next update operation (SetSnapshotOnNextOperation).
  //
  // Calling ClearSnapshot() has no effect on keys written before this function
  // has been called.
  //
  // If a reference to a snapshot was retrieved via GetSnapshot(), it will no
  // longer be valid and should be discarded after a call to ClearSnapshot().
  virtual void ClearSnapshot() = 0;

  // Prepare the current transation for 2PC
  virtual common::Status Prepare() = 0;

  // Write all batched keys to the db atomically.
  //
  // Returns OK on success.
  //
  // May return any error status that could be returned by DB:Write().
  //
  // If this transaction was created by an OptimisticTransactionDB(),
  // common::Status::Busy() may be returned if the transaction could not
  // guarantee
  // that there are no write conflicts.  common::Status::TryAgain() may be
  // returned
  // if the memtable history size is not large enough
  //  (See max_write_buffer_number_to_maintain).
  //
  // If this transaction was created by a TransactionDB(),
  // common::Status::Expired()
  // may be returned if this transaction has lived for longer than
  // TransactionOptions.expiration.
  virtual common::Status Commit() = 0;

  // Write all batched keys to db in AsyncMode
  //
  // Return OK  when post Commit request success.
  //
  // Attention! When this founction return, this transaction would not commit
  // You should check the call_back status
  virtual common::Status CommitAsync(common::AsyncCallback* call_back) = 0;

  // Discard all batched writes in this transaction.
  virtual common::Status Rollback() = 0;

  // Records the state of the transaction for future calls to
  // RollbackToSavePoint().  May be called multiple times to set multiple save
  // points.
  virtual void SetSavePoint() = 0;

  // Undo all operations in this transaction (Put, Merge, Delete, PutLogData)
  // since the most recent call to SetSavePoint() and removes the most recent
  // SetSavePoint().
  // If there is no previous call to SetSavePoint(), returns
  // common::Status::NotFound()
  virtual common::Status RollbackToSavePoint() = 0;

  // This function is similar to DB::Get() except it will also read pending
  // changes in this transaction.  Currently, this function will return
  // common::Status::MergeInProgress if the most recent write to the queried key
  // in
  // this batch is a Merge.
  //
  // If read_options.snapshot is not set, the current version of the key will
  // be read.  Calling SetSnapshot() does not affect the version of the data
  // returned.
  //
  // Note that setting read_options.snapshot will affect what is read from the
  // DB but will NOT change which keys are read from this transaction (the keys
  // in this transaction do not yet belong to any snapshot and will be fetched
  // regardless).
  virtual common::Status Get(const common::ReadOptions& options,
                             db::ColumnFamilyHandle* column_family,
                             const common::Slice& key, std::string* value) = 0;

  virtual common::Status Get(const common::ReadOptions& options,
                             const common::Slice& key, std::string* value) = 0;

  virtual std::vector<common::Status> MultiGet(
      const common::ReadOptions& options,
      const std::vector<db::ColumnFamilyHandle*>& column_family,
      const std::vector<common::Slice>& keys,
      std::vector<std::string>* values) = 0;

  virtual std::vector<common::Status> MultiGet(
      const common::ReadOptions& options,
      const std::vector<common::Slice>& keys,
      std::vector<std::string>* values) = 0;

  // Read this key and ensure that this transaction will only
  // be able to be committed if this key is not written outside this
  // transaction after it has first been read (or after the snapshot if a
  // snapshot is set in this transaction).  The transaction behavior is the
  // same regardless of whether the key exists or not.
  //
  // Note: Currently, this function will return common::Status::MergeInProgress
  // if the most recent write to the queried key in this batch is a Merge.
  //
  // The values returned by this function are similar to Transaction::Get().
  // If value==nullptr, then this function will not read any data, but will
  // still ensure that this key cannot be written to by outside of this
  // transaction.
  //
  // If this transaction was created by an OptimisticTransaction, GetForUpdate()
  // could cause commit() to fail.  Otherwise, it could return any error
  // that could be returned by DB::Get().
  //
  // If this transaction was created by a TransactionDB, it can return
  // common::Status::OK() on success,
  // common::Status::Busy() if there is a write conflict,
  // common::Status::TimedOut() if a lock could not be acquired,
  // common::Status::TryAgain() if the memtable history size is not large enough
  //  (See max_write_buffer_number_to_maintain)
  // common::Status::MergeInProgress() if merge operations cannot be resolved.
  // or other errors if this key could not be read.
  virtual common::Status GetForUpdate(const common::ReadOptions& options,
                                      db::ColumnFamilyHandle* column_family,
                                      const common::Slice& key,
                                      std::string* value,
                                      bool exclusive = true) = 0;

  virtual common::Status GetForUpdate(const common::ReadOptions& options,
                                      const common::Slice& key,
                                      std::string* value,
                                      bool exclusive = true) = 0;

  virtual std::vector<common::Status> MultiGetForUpdate(
      const common::ReadOptions& options,
      const std::vector<db::ColumnFamilyHandle*>& column_family,
      const std::vector<common::Slice>& keys,
      std::vector<std::string>* values) = 0;

  virtual std::vector<common::Status> MultiGetForUpdate(
      const common::ReadOptions& options,
      const std::vector<common::Slice>& keys,
      std::vector<std::string>* values) = 0;

  virtual common::Status TryLock(db::ColumnFamilyHandle* column_family,
                                 const common::Slice& key, bool read_only,
                                 bool exclusive, bool untracked = false,
                                 bool lock_uk = false, const common::ReadOptions *options = nullptr) = 0;
  // Returns an iterator that will iterate on all keys in the default
  // column family including both keys in the DB and uncommitted keys in this
  // transaction.
  //
  // Setting read_options.snapshot will affect what is read from the
  // DB but will NOT change which keys are read from this transaction (the keys
  // in this transaction do not yet belong to any snapshot and will be fetched
  // regardless).
  //
  // Caller is responsible for deleting the returned db::Iterator.
  //
  // The returned iterator is only valid until Commit(), Rollback(), or
  // RollbackToSavePoint() is called.
  virtual db::Iterator* GetIterator(
      const common::ReadOptions& read_options) = 0;

  virtual db::Iterator* GetIterator(const common::ReadOptions& read_options,
                                    db::ColumnFamilyHandle* column_family) = 0;

  // Put, Merge, Delete, and SingleDelete behave similarly to the corresponding
  // functions in WriteBatch, but will also do conflict checking on the
  // keys being written.
  //
  // If this Transaction was created on an OptimisticTransactionDB, these
  // functions should always return common::Status::OK().
  //
  // If this Transaction was created on a TransactionDB, the status returned
  // can be:
  // common::Status::OK() on success,
  // common::Status::Busy() if there is a write conflict,
  // common::Status::TimedOut() if a lock could not be acquired,
  // common::Status::TryAgain() if the memtable history size is not large enough
  //  (See max_write_buffer_number_to_maintain)
  // or other errors on unexpected failures.
  virtual common::Status Put(db::ColumnFamilyHandle* column_family,
                             const common::Slice& key,
                             const common::Slice& value) = 0;
  virtual common::Status Put(const common::Slice& key,
                             const common::Slice& value) = 0;
  virtual common::Status Put(db::ColumnFamilyHandle* column_family,
                             const common::SliceParts& key,
                             const common::SliceParts& value) = 0;
  virtual common::Status Put(const common::SliceParts& key,
                             const common::SliceParts& value) = 0;

  virtual common::Status Merge(db::ColumnFamilyHandle* column_family,
                               const common::Slice& key,
                               const common::Slice& value) = 0;
  virtual common::Status Merge(const common::Slice& key,
                               const common::Slice& value) = 0;

  virtual common::Status Delete(db::ColumnFamilyHandle* column_family,
                                const common::Slice& key) = 0;
  virtual common::Status Delete(const common::Slice& key) = 0;
  virtual common::Status Delete(db::ColumnFamilyHandle* column_family,
                                const common::SliceParts& key) = 0;
  virtual common::Status Delete(const common::SliceParts& key) = 0;

  virtual common::Status SingleDelete(db::ColumnFamilyHandle* column_family,
                                      const common::Slice& key) = 0;
  virtual common::Status SingleDelete(const common::Slice& key) = 0;
  virtual common::Status SingleDelete(db::ColumnFamilyHandle* column_family,
                                      const common::SliceParts& key) = 0;
  virtual common::Status SingleDelete(const common::SliceParts& key) = 0;

  // PutUntracked() will write a Put to the batch of operations to be committed
  // in this transaction.  This write will only happen if this transaction
  // gets committed successfully.  But unlike Transaction::Put(),
  // no conflict checking will be done for this key.
  //
  // If this Transaction was created on a TransactionDB, this function will
  // still acquire locks necessary to make sure this write doesn't cause
  // conflicts in other transactions and may return common::Status::Busy().
  virtual common::Status PutUntracked(db::ColumnFamilyHandle* column_family,
                                      const common::Slice& key,
                                      const common::Slice& value) = 0;
  virtual common::Status PutUntracked(const common::Slice& key,
                                      const common::Slice& value) = 0;
  virtual common::Status PutUntracked(db::ColumnFamilyHandle* column_family,
                                      const common::SliceParts& key,
                                      const common::SliceParts& value) = 0;
  virtual common::Status PutUntracked(const common::SliceParts& key,
                                      const common::SliceParts& value) = 0;

  virtual common::Status MergeUntracked(db::ColumnFamilyHandle* column_family,
                                        const common::Slice& key,
                                        const common::Slice& value) = 0;
  virtual common::Status MergeUntracked(const common::Slice& key,
                                        const common::Slice& value) = 0;

  virtual common::Status DeleteUntracked(db::ColumnFamilyHandle* column_family,
                                         const common::Slice& key) = 0;

  virtual common::Status DeleteUntracked(const common::Slice& key) = 0;
  virtual common::Status DeleteUntracked(db::ColumnFamilyHandle* column_family,
                                         const common::SliceParts& key) = 0;
  virtual common::Status DeleteUntracked(const common::SliceParts& key) = 0;

  // Similar to WriteBatch::PutLogData
  virtual void PutLogData(const common::Slice& blob) = 0;

  // By default, all Put/Merge/Delete operations will be indexed in the
  // transaction so that Get/GetForUpdate/GetIterator can search for these
  // keys.
  //
  // If the caller does not want to fetch the keys about to be written,
  // they may want to avoid indexing as a performance optimization.
  // Calling DisableIndexing() will turn off indexing for all future
  // Put/Merge/Delete operations until EnableIndexing() is called.
  //
  // If a key is Put/Merge/Deleted after DisableIndexing is called and then
  // is fetched via Get/GetForUpdate/GetIterator, the result of the fetch is
  // undefined.
  virtual void DisableIndexing() = 0;
  virtual void EnableIndexing() = 0;

  // Returns the number of distinct Keys being tracked by this transaction.
  // If this transaction was created by a TransactinDB, this is the number of
  // keys that are currently locked by this transaction.
  // If this transaction was created by an OptimisticTransactionDB, this is the
  // number of keys that need to be checked for conflicts at commit time.
  virtual uint64_t GetNumKeys() const = 0;

  // Returns the number of Puts/Deletes/Merges that have been applied to this
  // transaction so far.
  virtual uint64_t GetNumPuts() const = 0;
  virtual uint64_t GetNumDeletes() const = 0;
  virtual uint64_t GetNumMerges() const = 0;

  // Returns the elapsed time in milliseconds since this Transaction began.
  virtual uint64_t GetElapsedTime() const = 0;

  // Fetch the underlying write batch that contains all pending changes to be
  // committed.
  //
  // Note:  You should not write or delete anything from the batch directly and
  // should only use the functions in the Transaction class to
  // write to this transaction.
  virtual WriteBatchWithIndex* GetWriteBatch() = 0;

  // Change the value of TransactionOptions.lock_timeout (in milliseconds) for
  // this transaction.
  // Has no effect on OptimisticTransactions.
  virtual void SetLockTimeout(int64_t timeout) = 0;

  // Return the common::WriteOptions that will be used during Commit()
  virtual common::WriteOptions* GetWriteOptions() = 0;

  // Reset the common::WriteOptions that will be used during Commit().
  virtual void SetWriteOptions(const common::WriteOptions& write_options) = 0;

  // If this key was previously fetched in this transaction using
  // GetForUpdate/MultigetForUpdate(), calling UndoGetForUpdate will tell
  // the transaction that it no longer needs to do any conflict checking
  // for this key.
  //
  // If a key has been fetched N times via GetForUpdate/MultigetForUpdate(),
  // then UndoGetForUpdate will only have an effect if it is also called N
  // times.  If this key has been written to in this transaction,
  // UndoGetForUpdate() will have no effect.
  //
  // If SetSavePoint() has been called after the GetForUpdate(),
  // UndoGetForUpdate() will not have any effect.
  //
  // If this Transaction was created by an OptimisticTransactionDB,
  // calling UndoGetForUpdate can affect whether this key is conflict checked
  // at commit time.
  // If this Transaction was created by a TransactionDB,
  // calling UndoGetForUpdate may release any held locks for this key.
  virtual void UndoGetForUpdate(db::ColumnFamilyHandle* column_family,
                                const common::Slice& key) = 0;
  virtual void UndoGetForUpdate(const common::Slice& key) = 0;

  virtual common::Status RebuildFromWriteBatch(db::WriteBatch* src_batch) = 0;

  virtual db::WriteBatch* GetCommitTimeWriteBatch() = 0;

  virtual void SetLogNumber(uint64_t log) { log_number_ = log; }

  virtual uint64_t GetLogNumber() const { return log_number_; }

  virtual common::Status SetName(const TransactionName& name) = 0;

  virtual TransactionName GetName() const { return name_; }

  virtual TransactionID GetID() const { return 0; }

  virtual bool IsDeadlockDetect() const { return false; }

  virtual std::vector<TransactionID> GetWaitingTxns(uint32_t* column_family_id,
                                                    std::string* key) const {
    assert(false);
    return std::vector<TransactionID>();
  }

  enum TransactionState {
    STARTED = 0,
    AWAITING_PREPARE = 1,
    PREPARED = 2,
    AWAITING_COMMIT = 3,
    COMMITED = 4,
    AWAITING_ROLLBACK = 5,
    ROLLEDBACK = 6,
    LOCKS_STOLEN = 7,
  };

  TransactionState GetState() const { return txn_state_; }
  void SetState(TransactionState state) { txn_state_ = state; }

 protected:
  explicit Transaction(const TransactionDB* db) {}
  Transaction() {}

  // the log in which the prepared section for this txn resides
  // (for two phase commit)
  uint64_t log_number_;
  TransactionName name_;

  // Execution status of the transaction.
  std::atomic<TransactionState> txn_state_;

 private:
  // No copying allowed
  Transaction(const Transaction&);
  void operator=(const Transaction&);
};

}  // namespace util
}  // namespace xengine

#endif  // ROCKSDB_LITE
