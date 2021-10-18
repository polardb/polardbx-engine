// Portions Copyright (c) 2020, Alibaba Group Holding Limited
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#ifndef ROCKSDB_LITE

#include <algorithm>
#include <atomic>
#include <mutex>
#include <stack>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/write_callback.h"
#include "util/autovector.h"
#include "utilities/transactions/transaction_base.h"
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

class TransactionDBImpl;

class TransactionCallbackOnCommit : public common::AsyncCallback {
 public:
  TransactionCallbackOnCommit() : common::AsyncCallback(false) {}

  void reset(void* trx_impl, common::AsyncCallback* next_call_back) {
    this->trx_impl_ = trx_impl;
    this->next_call_back_ = next_call_back;
  }

 public:
  common::Status call_back() override;

 private:
  void* trx_impl_;
};

class TransactionImpl : public TransactionBaseImpl {
 public:
  TransactionImpl(util::TransactionDB* db,
                  const common::WriteOptions& write_options,
                  const util::TransactionOptions& txn_options);

  virtual ~TransactionImpl();

  void Reinitialize(util::TransactionDB* txn_db,
                    const common::WriteOptions& write_options,
                    const util::TransactionOptions& txn_options);

  common::Status Prepare() override;

  common::Status Commit() override;

  common::Status CommitBatch(db::WriteBatch* batch);

  common::Status CommitAsync(common::AsyncCallback* call_back) override;

  common::Status CommitBatchAsync(db::WriteBatch* batch,
                                  common::AsyncCallback* call_back);

  common::Status Rollback() override;

  common::Status RollbackToSavePoint() override;

  common::Status SetName(const TransactionName& name) override;

  // Generate a new unique transaction identifier
  static util::TransactionID GenTxnID();

  util::TransactionID GetID() const override { return txn_id_; }

  std::vector<util::TransactionID> GetWaitingTxns(
      uint32_t* column_family_id, std::string* key) const override {
    std::lock_guard<std::mutex> lock(wait_mutex_);
    std::vector<util::TransactionID> ids(waiting_txn_ids_.size());
    if (key) *key = waiting_key_ ? *waiting_key_ : "";
    if (column_family_id) *column_family_id = waiting_cf_id_;
    std::copy(waiting_txn_ids_.begin(), waiting_txn_ids_.end(), ids.begin());
    return ids;
  }

  void SetWaitingTxn(autovector<util::TransactionID> ids,
                     uint32_t column_family_id, const std::string* key) {
    std::lock_guard<std::mutex> lock(wait_mutex_);
    waiting_txn_ids_ = ids;
    waiting_cf_id_ = column_family_id;
    waiting_key_ = key;
  }

  void ClearWaitingTxn() {
    std::lock_guard<std::mutex> lock(wait_mutex_);
    waiting_txn_ids_.clear();
    waiting_cf_id_ = 0;
    waiting_key_ = nullptr;
  }

  // Returns the time (in microseconds according to Env->GetMicros())
  // that this transaction will be expired.  Returns 0 if this transaction does
  // not expire.
  uint64_t GetExpirationTime() const { return expiration_time_; }

  // returns true if this transaction has an expiration_time and has expired.
  bool IsExpired() const;

  // Returns the number of microseconds a transaction can wait on acquiring a
  // lock or -1 if there is no timeout.
  int64_t GetLockTimeout() const { return lock_timeout_; }
  void SetLockTimeout(int64_t timeout) override {
    lock_timeout_ = timeout * 1000;
  }

  // Returns true if locks were stolen successfully, false otherwise.
  bool TryStealingLocks();

  bool IsDeadlockDetect() const override { return deadlock_detect_; }

  int64_t GetDeadlockDetectDepth() const { return deadlock_detect_depth_; }

 protected:
  common::Status TryLock(db::ColumnFamilyHandle* column_family,
                         const common::Slice& key, bool read_only,
                         bool exclusive, bool untracked = false,
                         bool lock_uk = false, const common::ReadOptions *options = nullptr) override;

 private:
  TransactionDBImpl* txn_db_impl_;
  db::DBImpl* db_impl_;

  // Used to create unique ids for transactions.
  static std::atomic<util::TransactionID> txn_id_counter_;

  // Unique ID for this transaction
  util::TransactionID txn_id_;

  // IDs for the transactions that are blocking the current transaction.
  //
  // empty if current transaction is not waiting.
  autovector<util::TransactionID> waiting_txn_ids_;

  // The following two represents the (cf, key) that a transaction is waiting
  // on.
  //
  // If waiting_key_ is not null, then the pointer should always point to
  // a valid string object. The reason is that it is only non-null when the
  // transaction is blocked in the TransactionLockMgr::AcquireWithTimeout
  // function. At that point, the key string object is one of the function
  // parameters.
  uint32_t waiting_cf_id_;
  const std::string* waiting_key_;

  // Mutex protecting waiting_txn_ids_, waiting_cf_id_ and waiting_key_.
  mutable std::mutex wait_mutex_;

  // If non-zero, this transaction should not be committed after this time (in
  // microseconds according to Env->NowMicros())
  uint64_t expiration_time_;

  // Timeout in microseconds when locking a key or -1 if there is no timeout.
  int64_t lock_timeout_;

  // Whether to perform deadlock detection or not.
  bool deadlock_detect_;

  // Whether to perform deadlock detection or not.
  int64_t deadlock_detect_depth_;

  void Clear() override;

  void Initialize(const util::TransactionOptions& txn_options);

  common::Status ValidateSnapshot(db::ColumnFamilyHandle* column_family,
                                  const common::Slice& key,
                                  common::SequenceNumber prev_seqno,
                                  common::SequenceNumber* new_seqno,
                                  const bool lock_uk,
                                  const common::ReadOptions *read_opts);

  common::Status LockBatch(db::WriteBatch* batch,
                           TransactionKeyMap* keys_to_unlock);

  common::Status DoCommit(db::WriteBatch* batch);

  void RollbackLastN(size_t num);

  void UnlockGetForUpdate(db::ColumnFamilyHandle* column_family,
                          const common::Slice& key) override;

  // No copying allowed
  TransactionImpl(const TransactionImpl&);
  void operator=(const TransactionImpl&);
  friend class TransactionCallbackOnCommit;
  friend class TransactionCallbackOnCommitBatch;
  TransactionCallbackOnCommit call_back_on_commit_;
};

// Used at commit time to check whether transaction is committing before its
// expiration time.
class TransactionCallback : public db::WriteCallback {
 public:
  explicit TransactionCallback(TransactionImpl* txn) : txn_(txn) {}

  common::Status Callback(db::DB* db) override {
    if (txn_->IsExpired()) {
      return common::Status::Expired();
    } else {
      return common::Status::OK();
    }
  }

  bool AllowWriteBatching() override { return true; }

 private:
  TransactionImpl* txn_;
};

class TransactionCallbackOnCommitBatch : public common::AsyncCallback {
 public:
  TransactionCallbackOnCommitBatch(TransactionImpl* trx_impl,
                                   common::AsyncCallback* next_call_back)
      : common::AsyncCallback(next_call_back, true), trx_impl_(trx_impl) {
    // assert(next_call_back != nullptr);
    assert(trx_impl != nullptr);
  }

 public:
  common::Status call_back() override;

 private:
  TransactionImpl* trx_impl_;
};

}  //  namespace util
}  //  namespace xengine

#endif  // ROCKSDB_LITE
