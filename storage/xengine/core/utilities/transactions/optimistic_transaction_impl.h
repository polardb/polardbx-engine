// Portions Copyright (c) 2020, Alibaba Group Holding Limited
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#ifndef ROCKSDB_LITE

#include <stack>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/write_callback.h"
#include "utilities/transactions/transaction_base.h"
#include "utilities/transactions/transaction_util.h"
#include "xengine/db.h"
#include "xengine/slice.h"
#include "xengine/snapshot.h"
#include "xengine/status.h"
#include "xengine/types.h"
#include "xengine/utilities/optimistic_transaction_db.h"
#include "xengine/utilities/transaction.h"
#include "xengine/utilities/write_batch_with_index.h"

namespace xengine {
namespace util {

class OptimisticTransactionImpl : public TransactionBaseImpl {
 public:
  OptimisticTransactionImpl(
      util::OptimisticTransactionDB* db,
      const common::WriteOptions& write_options,
      const util::OptimisticTransactionOptions& txn_options);

  virtual ~OptimisticTransactionImpl();

  void Reinitialize(util::OptimisticTransactionDB* txn_db,
                    const common::WriteOptions& write_options,
                    const util::OptimisticTransactionOptions& txn_options);

  common::Status Prepare() override;

  common::Status Commit() override;

  common::Status CommitAsync(common::AsyncCallback* call_back) override;

  common::Status Rollback() override;

  common::Status SetName(const TransactionName& name) override;

 protected:
  common::Status TryLock(db::ColumnFamilyHandle* column_family,
                         const common::Slice& key, bool read_only,
                         bool exclusive, bool untracked = false, 
                         bool lock_uk = false, const common::ReadOptions *options = nullptr) override;

 private:
  util::OptimisticTransactionDB* const txn_db_;

  friend class OptimisticTransactionCallback;

  void Initialize(const util::OptimisticTransactionOptions& txn_options);

  // Returns OK if it is safe to commit this transaction.  Returns
  // common::Status::Busy
  // if there are read or write conflicts that would prevent us from committing
  // OR if we can not determine whether there would be any such conflicts.
  //
  // Should only be called on writer thread.
  common::Status CheckTransactionForConflicts(db::DB* db);

  void Clear() override;

  void UnlockGetForUpdate(db::ColumnFamilyHandle* column_family,
                          const common::Slice& key) override {
    // Nothing to unlock.
  }

  // No copying allowed
  OptimisticTransactionImpl(const OptimisticTransactionImpl&);
  void operator=(const OptimisticTransactionImpl&);
};

// Used at commit time to trigger transaction validation
class OptimisticTransactionCallback : public db::WriteCallback {
 public:
  explicit OptimisticTransactionCallback(OptimisticTransactionImpl* txn)
      : txn_(txn) {}

  common::Status Callback(db::DB* db) override {
    return txn_->CheckTransactionForConflicts(db);
  }

  bool AllowWriteBatching() override { return false; }

 private:
  OptimisticTransactionImpl* txn_;
};

}  //  namespace util
}  //  namespace xengine

#endif  // ROCKSDB_LITE
