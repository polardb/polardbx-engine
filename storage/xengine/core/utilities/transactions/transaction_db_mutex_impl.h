//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#ifndef ROCKSDB_LITE

#include "xengine/utilities/transaction_db_mutex.h"

namespace xengine {
namespace util {

class TransactionDBMutex;
class TransactionDBCondVar;

// Default implementation of TransactionDBMutexFactory.  May be overridden
// by TransactionDBOptions.custom_mutex_factory.
class TransactionDBMutexFactoryImpl : public TransactionDBMutexFactory {
 public:
  std::shared_ptr<util::TransactionDBMutex> AllocateMutex() override;
  std::shared_ptr<util::TransactionDBCondVar> AllocateCondVar() override;
};

}  //  namespace util
}  //  namespace xengine

#endif  // ROCKSDB_LITE
