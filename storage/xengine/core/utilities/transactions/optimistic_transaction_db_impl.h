//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#ifndef ROCKSDB_LITE

#include "xengine/db.h"
#include "xengine/options.h"
#include "xengine/utilities/optimistic_transaction_db.h"

namespace xengine {
namespace util {

class OptimisticTransactionDBImpl : public OptimisticTransactionDB {
 public:
  explicit OptimisticTransactionDBImpl(db::DB* db)
      : OptimisticTransactionDB(db), db_(db) {}

  ~OptimisticTransactionDBImpl() {}

  Transaction* BeginTransaction(const common::WriteOptions& write_options,
                                const OptimisticTransactionOptions& txn_options,
                                Transaction* old_txn) override;

  db::DB* GetBaseDB() override { return db_.get(); }

 private:
  std::unique_ptr<db::DB> db_;

  void ReinitializeTransaction(util::Transaction* txn,
                               const common::WriteOptions& write_options,
                               const OptimisticTransactionOptions& txn_options =
                                   OptimisticTransactionOptions());
};

}  //  namespace util
}  //  namespace xengine
#endif  // ROCKSDB_LITE
