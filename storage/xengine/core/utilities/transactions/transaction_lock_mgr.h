//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#ifndef ROCKSDB_LITE

#include <chrono>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "monitoring/instrumented_mutex.h"
#include "util/autovector.h"
#include "util/hash_map.h"
#include "util/murmurhash.h"
#include "util/thread_local.h"
#include "utilities/transactions/transaction_impl.h"
#include "xengine/utilities/transaction.h"
#include "xengine/utilities/transaction_db_mutex.h"
#include "memory/base_malloc.h"

namespace xengine {
namespace db {
class ColumnFamilyHandle;
}

namespace common {
class Slice;
}

namespace util {

struct LockInfo;
struct LockMap;
struct LockMapStripe;

class TransactionDBImpl;

struct LockKey
{
  uint32_t index_id_;
  std::string key_;

  LockKey(uint32_t index_id, std::string key)
      : index_id_(index_id),
        key_(key)
  {
  }
  ~LockKey() {}
  inline size_t hash() const
  {
    size_t hash_val = 0;
    hash_val = MurmurHash(&index_id_, sizeof(index_id_), hash_val);
    hash_val = MurmurHash(key_.data(), key_.size(), hash_val);
    return hash_val;
  }

  DECLARE_AND_DEFINE_TO_STRING(KV_(index_id), KV_(key));
};

struct LockInfo
{
  uint32_t index_id_;
  bool exclusive_;
  uint64_t expiration_time_;
  autovector<TransactionID> trans_ids_;

  LockInfo(const uint32_t index_id,
           const bool exclusive,
           const uint64_t expiration_time,
           const TransactionID trans_id)
      : index_id_(index_id),
        exclusive_(exclusive),
        expiration_time_(expiration_time),
        trans_ids_()
  {
    trans_ids_.push_back(trans_id);
  }
  LockInfo(const LockInfo &lock_info)
      : index_id_(lock_info.index_id_),
        exclusive_(lock_info.exclusive_),
        expiration_time_(lock_info.expiration_time_),
        trans_ids_(lock_info.trans_ids_)
  {
  }
  ~LockInfo() {}

  DECLARE_AND_DEFINE_TO_STRING(KV_(index_id), KV_(exclusive), KV_(expiration_time));
};

struct LockMapStripe
{
  std::shared_ptr<TransactionDBMutex> stripe_mutex_; // Mutex must be held before modifying keys map
  std::shared_ptr<TransactionDBCondVar> stripe_cv_; // Condition Variable per stripe for waiting on a lock
  std::unordered_map<uint64_t, LockInfo> keys_; // Use hash(string_key) as lock key for fast comparison

  explicit LockMapStripe(std::shared_ptr<TransactionDBMutexFactory> factory)
      : stripe_mutex_(),
        stripe_cv_(),
        keys_()
  {
    stripe_mutex_ = factory->AllocateMutex();
    stripe_cv_ = factory->AllocateCondVar();
    assert(stripe_mutex_);
    assert(stripe_cv_);
  }
  ~LockMapStripe() {}
};

struct LockMap
{
  const size_t stripes_num_;
  std::atomic<int64_t> lock_cnt_;
  std::vector<LockMapStripe *> lock_map_stripes_;

  explicit LockMap(const size_t stripes_num, std::shared_ptr<TransactionDBMutexFactory> factory)
      : stripes_num_(stripes_num),
        lock_cnt_(0),
        lock_map_stripes_()
  {
    LockMapStripe *stripe = nullptr;
    for (size_t i = 0; i < stripes_num_; ++i) {
      stripe = MOD_NEW_OBJECT(memory::ModId::kTransactionLockMgr, LockMapStripe, factory);
      assert(stripe);
      lock_map_stripes_.push_back(stripe);

    }
  }
  ~LockMap()
  {
    destroy();
  }

  void destroy()
  {
    for (auto stripe : lock_map_stripes_) {
      MOD_DELETE_OBJECT(LockMapStripe, stripe);
    }
  }

  inline LockMapStripe *get_lock_map_stripe(const LockKey &key) const
  {
    return lock_map_stripes_.at(key.hash() % stripes_num_);
  }
  inline LockMapStripe *get_lock_map_stripe(const size_t stripe_index) const
  {
    return lock_map_stripes_.at(stripe_index);
  }
  inline size_t get_lock_map_stripe_index(const LockKey &key) const
  {
    return key.hash() % stripes_num_;
  }
};

class TransactionLockMgr {
 public:
  TransactionLockMgr(util::TransactionDB* txn_db, size_t default_num_stripes,
                     int64_t max_num_locks,
                     std::shared_ptr<TransactionDBMutexFactory> factory);

  ~TransactionLockMgr();

  // Attempt to lock key.  If OK status is returned, the caller is responsible
  // for calling UnLock() on this key.
  common::Status TryLock(TransactionImpl* txn, uint32_t index_id,
                         const std::string& key, util::Env* env,
                         bool exclusive);

  // Unlock a key locked by TryLock().  txn must be the same Transaction that
  // locked this key.
  void UnLock(const TransactionImpl* txn, const TransactionKeyMap* keys,
              util::Env* env);

  void UnLock(TransactionImpl* txn, uint32_t index_id,
              const std::string& key, util::Env* env);

  using LockStatusData = std::unordered_multimap<uint32_t, KeyLockInfo>;
  LockStatusData GetLockStatusData();

 private:
  static const int64_t DEFAULT_LOCK_MAP_STRIPES_NUM = 102400;

 private:
  TransactionDBImpl* txn_db_impl_;

  // Default number of lock map stripes per column family
  const size_t default_num_stripes_;

  // Limit on number of keys locked per column family
  const int64_t max_num_locks_;

  //Used to allocate mutexs/condvars to use when locking keys
  std::shared_ptr<TransactionDBMutexFactory> mutex_factory_;

  //Gloabl lock map, used for all transaction
  LockMap lock_map_;

  // Must be held when modifying wait_txn_map_ and rev_wait_txn_map_.
  std::mutex wait_txn_map_mutex_;

  // Maps from waitee -> number of waiters.
  HashMap<TransactionID, int> rev_wait_txn_map_;
  // Maps from waiter -> waitee.
  HashMap<TransactionID, autovector<TransactionID>> wait_txn_map_;

  std::shared_ptr<LockMap> local_lock_map_ptr;

  bool IsLockExpired(const TransactionID trans_id, const LockInfo& lock_info,
                     util::Env* env, uint64_t* wait_time);

  std::shared_ptr<LockMap> GetLockMap(uint32_t column_family_id);

  common::Status AcquireWithTimeout(TransactionImpl* trans, LockMap* lock_map,
                                    LockMapStripe* stripe,
                                    uint32_t index_id,
                                    const std::string& key, size_t key_hash,
                                    util::Env* env, int64_t timeout,
                                    const LockInfo& lock_info);

  common::Status AcquireLocked(LockMap* lock_map, LockMapStripe* stripe,
                               size_t key, util::Env* env,
                               const LockInfo& lock_info, uint64_t* wait_time,
                               autovector<TransactionID>* txn_ids);

  void UnLockKey(const TransactionImpl* trans, size_t key, LockMapStripe* stripe,
                 LockMap* lock_map, util::Env* env);

  bool IncrementWaiters(const TransactionImpl* txn,
                        const autovector<TransactionID>& wait_ids);
  void DecrementWaiters(const TransactionImpl* txn,
                        const autovector<TransactionID>& wait_ids);
  void DecrementWaitersImpl(const TransactionImpl* txn,
                            const autovector<TransactionID>& wait_ids);

  // No copying allowed
  TransactionLockMgr(const TransactionLockMgr&);
  void operator=(const TransactionLockMgr&);
};

}  //  namespace util
}  //  namespace xengine
#endif  // ROCKSDB_LITE
