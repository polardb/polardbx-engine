//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#ifndef ROCKSDB_LITE

#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include "utilities/transactions/transaction_impl.h"
#include "utilities/transactions/transaction_lock_mgr.h"
#include "xengine/db.h"
#include "xengine/options.h"
#include "xengine/utilities/transaction_db.h"

namespace xengine {
namespace util {

class TransactionDBImpl : public util::TransactionDB {
 public:
  explicit TransactionDBImpl(db::DB* db,
                             const util::TransactionDBOptions& txn_db_options);

  explicit TransactionDBImpl(util::StackableDB* db,
                             const util::TransactionDBOptions& txn_db_options);

  ~TransactionDBImpl();

  common::Status Initialize(
      const std::vector<size_t>& compaction_enabled_cf_indices,
      const std::vector<db::ColumnFamilyHandle*>& handles);

  util::Transaction* BeginTransactionWrap(const common::WriteOptions& opts);

  util::Transaction* BeginTransaction(const common::WriteOptions& write_options,
                                      const TransactionOptions& txn_options,
                                      util::Transaction* old_txn) override;

  using xengine::util::StackableDB::Put;
  virtual common::Status Put(const common::WriteOptions& opts,
                             db::ColumnFamilyHandle* column_family,
                             const common::Slice& key,
                             const common::Slice& val) override;

  using xengine::util::StackableDB::Delete;
  virtual common::Status Delete(const common::WriteOptions& opts,
                                db::ColumnFamilyHandle* column_family,
                                const common::Slice& key) override;

  using xengine::util::StackableDB::Merge;
  virtual common::Status Merge(const common::WriteOptions& opts,
                               db::ColumnFamilyHandle* column_family,
                               const common::Slice& key,
                               const common::Slice& value) override;

  using xengine::util::StackableDB::Write;
  virtual common::Status Write(const common::WriteOptions& opts,
                               db::WriteBatch* updates) override;

  common::Status WriteAsync(const common::WriteOptions& opts,
                            db::WriteBatch* updates,
                            common::AsyncCallback* call_back);

  using xengine::util::StackableDB::CreateColumnFamily;
  virtual common::Status CreateColumnFamily(db::CreateSubTableArgs &args, db::ColumnFamilyHandle** handle) override;

  using xengine::util::StackableDB::DropColumnFamily;
  virtual common::Status DropColumnFamily(
      db::ColumnFamilyHandle* column_family) override;

  common::Status TryLock(TransactionImpl* txn, uint32_t cfh_id,
                         const std::string& key, bool exclusive);

  void UnLock(TransactionImpl* txn, const TransactionKeyMap* keys);
  void UnLock(TransactionImpl* txn, uint32_t cfh_id, const std::string& key);

  static util::TransactionDBOptions ValidateTxnDBOptions(
      const util::TransactionDBOptions& txn_db_options);

  const util::TransactionDBOptions& GetTxnDBOptions() const {
    return txn_db_options_;
  }

  void InsertExpirableTransaction(TransactionID tx_id, TransactionImpl* tx);
  void RemoveExpirableTransaction(TransactionID tx_id);

  // If transaction is no longer available, locks can be stolen
  // If transaction is available, try stealing locks directly from transaction
  // It is the caller's responsibility to ensure that the referred transaction
  // is expirable (GetExpirationTime() > 0) and that it is expired.
  bool TryStealingExpiredTransactionLocks(TransactionID tx_id);

  util::Transaction* GetTransactionByName(const TransactionName& name) override;

  void RegisterTransaction(util::Transaction* txn);
  void UnregisterTransaction(util::Transaction* txn);

  // not thread safe. current use case is during recovery (single thread)
  void GetAllPreparedTransactions(
      std::vector<util::Transaction*>* trans) override;

  TransactionLockMgr::LockStatusData GetLockStatusData() override;

  db::DBImpl* GetDBImpl() {
    return db_impl_;
  }

  virtual int do_manual_checkpoint(int32_t &manifest_file_num) override;

  virtual int stream_log_extents(
              std::function<int(const char*, int, int64_t, int)> *stream_extent,
              int64_t start, int64_t end, int dest_fd) override;

  virtual int create_backup_snapshot(db::MetaSnapshotMap &meta_snapshot,
                                     int32_t &last_manifest_file_num,
                                     uint64_t &last_manifest_file_size,
                                     uint64_t &last_wal_file_num) override;

  virtual int64_t get_last_wal_file_size() const override;

  virtual int release_backup_snapshot(db::MetaSnapshotMap &meta_snapshot) override;

  virtual int record_incremental_extent_ids(const int32_t first_manifest_file_num,
                                            const int32_t last_manifest_file_num,
                                            const uint64_t last_manifest_file_size) override;

  virtual int64_t backup_manifest_file_size() const override;
 private:
  void ReinitializeTransaction(
      util::Transaction* txn, const common::WriteOptions& write_options,
      const TransactionOptions& txn_options = TransactionOptions());

  db::DBImpl* db_impl_;
  const util::TransactionDBOptions txn_db_options_;
  TransactionLockMgr lock_mgr_;

  // Must be held when adding/dropping column families.
  monitor::InstrumentedMutex column_family_mutex_;
  util::Transaction* BeginInternalTransaction(
      const common::WriteOptions& options);
  common::Status WriteHelper(db::WriteBatch* updates,
                             TransactionImpl* txn_impl);

  // Used to ensure that no locks are stolen from an expirable transaction
  // that has started a commit. Only transactions with an expiration time
  // should be in this map.
  std::mutex map_mutex_;
  std::unordered_map<TransactionID, TransactionImpl*>
      expirable_transactions_map_;

  // map from name to two phase transaction instance
  std::mutex name_map_mutex_;
  std::unordered_map<TransactionName, util::Transaction*> transactions_;

  static thread_local util::Transaction* cached_txn_;
};

}  //  namespace util
}  //  namespace xengine
#endif  // ROCKSDB_LITE
