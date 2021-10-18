/*
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#ifndef ROCKSDB_LITE

#include <string>
#include <vector>

#include "xengine/comparator.h"
#include "xengine/db.h"

namespace xengine {
namespace util {

class Transaction;

// Database with Transaction support.
//
// See optimistic_transaction.h and examples/transaction_example.cc

// common::Options to use when starting an Optimistic Transaction
struct OptimisticTransactionOptions {
  // Setting set_snapshot=true is the same as calling SetSnapshot().
  bool set_snapshot = false;

  // Should be set if the DB has a non-default comparator.
  // See comment in WriteBatchWithIndex constructor.
  const Comparator* cmp = BytewiseComparator();
};

class OptimisticTransactionDB {
 public:
  // Open an OptimisticTransactionDB similar to DB::Open().
  static common::Status Open(const common::Options& options,
                             const std::string& dbname,
                             OptimisticTransactionDB** dbptr);

  static common::Status Open(
      const common::DBOptions& db_options, const std::string& dbname,
      const std::vector<db::ColumnFamilyDescriptor>& column_families,
      std::vector<db::ColumnFamilyHandle*>* handles,
      OptimisticTransactionDB** dbptr);

  virtual ~OptimisticTransactionDB() {}

  // Starts a new Transaction.
  //
  // Caller is responsible for deleting the returned transaction when no
  // longer needed.
  //
  // If old_txn is not null, BeginTransaction will reuse this Transaction
  // handle instead of allocating a new one.  This is an optimization to avoid
  // extra allocations when repeatedly creating transactions.
  virtual Transaction* BeginTransaction(
      const common::WriteOptions& write_options,
      const OptimisticTransactionOptions& txn_options =
          OptimisticTransactionOptions(),
      Transaction* old_txn = nullptr) = 0;

  // Return the underlying Database that was opened
  virtual db::DB* GetBaseDB() = 0;

 protected:
  // To Create an OptimisticTransactionDB, call Open()
  explicit OptimisticTransactionDB(db::DB* db) {}
  OptimisticTransactionDB() {}

 private:
  // No copying allowed
  OptimisticTransactionDB(const OptimisticTransactionDB&);
  void operator=(const OptimisticTransactionDB&);
};

}  // namespace util
}  // namespace xengine

#endif  // ROCKSDB_LITE
