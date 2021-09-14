//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE

#include "utilities/transactions/transaction_db_impl.h"

#include <string>
#include <unordered_set>
#include <vector>

#include "db/db_impl.h"
#include "utilities/transactions/transaction_db_mutex_impl.h"
#include "utilities/transactions/transaction_impl.h"
#include "xengine/db.h"
#include "xengine/options.h"
#include "xengine/utilities/transaction_db.h"

using namespace xengine;
using namespace db;
using namespace common;
using namespace util;
using namespace monitor;

namespace xengine {
namespace util {

thread_local Transaction* TransactionDBImpl::cached_txn_ = nullptr;

TransactionDBImpl::TransactionDBImpl(DB* db,
                                     const TransactionDBOptions& txn_db_options)
    : TransactionDB(db),
      db_impl_(dynamic_cast<DBImpl*>(db)),
      txn_db_options_(txn_db_options),
      lock_mgr_(this, txn_db_options_.num_stripes, txn_db_options.max_num_locks,
                txn_db_options_.custom_mutex_factory
                    ? txn_db_options_.custom_mutex_factory
                    : std::shared_ptr<TransactionDBMutexFactory>(
                          new TransactionDBMutexFactoryImpl())) {
  assert(db_impl_ != nullptr);
}

// Support initiliazing TransactionDBImpl from a stackable db
//
//    TransactionDBImpl
//     ^        ^
//     |        |
//     |        +
//     |   StackableDB
//     |   ^
//     |   |
//     +   +
//     DBImpl
//       ^
//       |(inherit)
//       +
//       DB
//
TransactionDBImpl::TransactionDBImpl(StackableDB* db,
                                     const TransactionDBOptions& txn_db_options)
    : TransactionDB(db),
      db_impl_(dynamic_cast<DBImpl*>(db->GetRootDB())),
      txn_db_options_(txn_db_options),
      lock_mgr_(this, txn_db_options_.num_stripes, txn_db_options.max_num_locks,
                txn_db_options_.custom_mutex_factory
                    ? txn_db_options_.custom_mutex_factory
                    : std::shared_ptr<TransactionDBMutexFactory>(
                          new TransactionDBMutexFactoryImpl())) {
  assert(db_impl_ != nullptr);
}

TransactionDBImpl::~TransactionDBImpl() {
  while (!transactions_.empty()) {
    delete transactions_.begin()->second;
  }
  delete cached_txn_;
  cached_txn_ = nullptr;
}

Status TransactionDBImpl::Initialize(
    const std::vector<size_t>& compaction_enabled_cf_indices,
    const std::vector<ColumnFamilyHandle*>& handles) {
  // Re-enable compaction for the column families that initially had
  // compaction enabled.
  std::vector<ColumnFamilyHandle*> compaction_enabled_cf_handles;
  compaction_enabled_cf_handles.reserve(compaction_enabled_cf_indices.size());
  for (auto index : compaction_enabled_cf_indices) {
    compaction_enabled_cf_handles.push_back(handles[index]);
  }

  Status s = EnableAutoCompaction(compaction_enabled_cf_handles);

  // create 'real' transactions from recovered shell transactions
  auto dbimpl = reinterpret_cast<DBImpl*>(GetRootDB());
  assert(dbimpl != nullptr);
  auto rtrxs = dbimpl->recovered_transactions();

  for (auto it = rtrxs->begin(); it != rtrxs->end(); ++it) {
    auto recovered_trx = it->second;
    assert(recovered_trx);
    assert(recovered_trx->name_.length());
    if (recovered_trx->state_ == Transaction::PREPARED) {
      assert(recovered_trx->prepare_log_num_);
      assert(recovered_trx->batch_);
      WriteOptions w_options;
      w_options.sync = true;
      TransactionOptions t_options;

      Transaction* real_trx = BeginTransaction(w_options, t_options, nullptr);
      assert(real_trx);
      real_trx->SetLogNumber(recovered_trx->prepare_log_num_);

      s = real_trx->SetName(recovered_trx->name_);
      if (!s.ok()) {
        break;
      }

      s = real_trx->RebuildFromWriteBatch(recovered_trx->batch_);
      real_trx->SetState(Transaction::PREPARED);
      if (!s.ok()) {
        break;
      }
    }
  }
  if (s.ok()) {
    dbimpl->delete_all_recovered_transactions();
  }
  return s;
}

Transaction* TransactionDBImpl::BeginTransaction(
    const WriteOptions& write_options, const TransactionOptions& txn_options,
    Transaction* old_txn) {
  if (old_txn != nullptr) {
    ReinitializeTransaction(old_txn, write_options, txn_options);
    return old_txn;
  } else {
    return new TransactionImpl(this, write_options, txn_options);
  }
}

TransactionDBOptions TransactionDBImpl::ValidateTxnDBOptions(
    const TransactionDBOptions& txn_db_options) {
  TransactionDBOptions validated = txn_db_options;

  if (txn_db_options.num_stripes == 0) {
    validated.num_stripes = 1;
  }

  return validated;
}

Status TransactionDB::Open(const Options& options,
                           const TransactionDBOptions& txn_db_options,
                           const std::string& dbname, TransactionDB** dbptr) {
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;
  Status s = TransactionDB::Open(db_options, txn_db_options, dbname,
                                 column_families, &handles, dbptr);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a reference to
    // default column family
    MOD_DELETE_OBJECT(ColumnFamilyHandle, handles[0]);
  }

  return s;
}

Status TransactionDB::Open(
    const DBOptions& db_options, const TransactionDBOptions& txn_db_options,
    const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, TransactionDB** dbptr) {
  Status s;
  DB* db;

  std::vector<ColumnFamilyDescriptor> column_families_copy = column_families;
  std::vector<size_t> compaction_enabled_cf_indices;
  DBOptions db_options_2pc = db_options;
  PrepareWrap(&db_options_2pc, &column_families_copy,
              &compaction_enabled_cf_indices);
  s = DB::Open(db_options_2pc, dbname, column_families_copy, handles, &db);
  if (s.ok()) {
    s = WrapDB(db, txn_db_options, compaction_enabled_cf_indices, *handles,
               dbptr);
  }
  return s;
}

void TransactionDB::PrepareWrap(
    DBOptions* db_options, std::vector<ColumnFamilyDescriptor>* column_families,
    std::vector<size_t>* compaction_enabled_cf_indices) {
  compaction_enabled_cf_indices->clear();

  // Enable MemTable History if not already enabled
  for (size_t i = 0; i < column_families->size(); i++) {
    ColumnFamilyOptions* cf_options = &(*column_families)[i].options;
/*
    if (cf_options->max_write_buffer_number_to_maintain == 0) {
      // Setting to -1 will set the History size to max_write_buffer_number.
      cf_options->max_write_buffer_number_to_maintain = -1;
    }
*/
    if (!cf_options->disable_auto_compactions) {
      // Disable compactions momentarily to prevent race with DB::Open
      cf_options->disable_auto_compactions = true;
      compaction_enabled_cf_indices->push_back(i);
    }
  }
  db_options->allow_2pc = true;
}

Status TransactionDB::WrapDB(
    // make sure this db is already opened with memtable history enabled,
    // auto compaction distabled and 2 phase commit enabled
    DB* db, const TransactionDBOptions& txn_db_options,
    const std::vector<size_t>& compaction_enabled_cf_indices,
    const std::vector<ColumnFamilyHandle*>& handles, TransactionDB** dbptr) {
  TransactionDBImpl* txn_db = new TransactionDBImpl(
      db, TransactionDBImpl::ValidateTxnDBOptions(txn_db_options));
  *dbptr = txn_db;
  Status s = txn_db->Initialize(compaction_enabled_cf_indices, handles);
  return s;
}

Status TransactionDB::WrapStackableDB(
    // make sure this stackable_db is already opened with memtable history
    // enabled,
    // auto compaction distabled and 2 phase commit enabled
    StackableDB* db, const TransactionDBOptions& txn_db_options,
    const std::vector<size_t>& compaction_enabled_cf_indices,
    const std::vector<ColumnFamilyHandle*>& handles, TransactionDB** dbptr) {
  TransactionDBImpl* txn_db = new TransactionDBImpl(
      db, TransactionDBImpl::ValidateTxnDBOptions(txn_db_options));
  *dbptr = txn_db;
  Status s = txn_db->Initialize(compaction_enabled_cf_indices, handles);
  return s;
}

Status TransactionDBImpl::CreateColumnFamily(
    db::CreateSubTableArgs &args,
    ColumnFamilyHandle** handle) {
  InstrumentedMutexLock l(&column_family_mutex_);
  Status s = db_->CreateColumnFamily(args, handle);
  return s;
}

// Let TransactionLockMgr know that it can deallocate the LockMap for this
// column family.
Status TransactionDBImpl::DropColumnFamily(ColumnFamilyHandle* column_family) {
  InstrumentedMutexLock l(&column_family_mutex_);
  Status s = db_->DropColumnFamily(column_family);
  return s;
}

Status TransactionDBImpl::TryLock(TransactionImpl* txn, uint32_t cfh_id,
                                  const std::string& key, bool exclusive) {
  return lock_mgr_.TryLock(txn, cfh_id, key, GetEnv(), exclusive);
}

void TransactionDBImpl::UnLock(TransactionImpl* txn,
                               const TransactionKeyMap* keys) {
  lock_mgr_.UnLock(txn, keys, GetEnv());
}

void TransactionDBImpl::UnLock(TransactionImpl* txn, uint32_t cfh_id,
                               const std::string& key) {
  lock_mgr_.UnLock(txn, cfh_id, key, GetEnv());
}

// Used when wrapping DB write operations in a transaction
Transaction* TransactionDBImpl::BeginInternalTransaction(
    const WriteOptions& options) {
  TransactionOptions txn_options;
  Transaction* txn = BeginTransaction(options, txn_options, nullptr);

  // Use default timeout for non-transactional writes
  txn->SetLockTimeout(txn_db_options_.default_lock_timeout);
  return txn;
}

Transaction* TransactionDBImpl::BeginTransactionWrap(const WriteOptions& opts) {
  Transaction* txn = nullptr;

  if (UNLIKELY(cached_txn_ == nullptr)) {
    txn = BeginInternalTransaction(opts);
    cached_txn_ = txn;
  } else {
    TransactionOptions txn_options;
    txn = BeginTransaction(opts, txn_options, cached_txn_);
    // Use default timeout for non-transactional writes
    txn->SetLockTimeout(txn_db_options_.default_lock_timeout);
  }

  return txn;
}

// All user Put, Merge, Delete, and Write requests must be intercepted to make
// sure that they lock all keys that they are writing to avoid causing conflicts
// with any concurent transactions. The easiest way to do this is to wrap all
// write operations in a transaction.
//
// Put(), Merge(), and Delete() only lock a single key per call.  Write() will
// sort its keys before locking them.  This guarantees that TransactionDB write
// methods cannot deadlock with eachother (but still could deadlock with a
// Transaction).
Status TransactionDBImpl::Put(const WriteOptions& opts,
                              ColumnFamilyHandle* column_family,
                              const Slice& key, const Slice& val) {
  Status s;

  Transaction* txn = BeginTransactionWrap(opts);
  txn->DisableIndexing();

  // Since the client didn't create a transaction, they don't care about
  // conflict checking for this write.  So we just need to do PutUntracked().
  s = txn->PutUntracked(column_family, key, val);

  if (s.ok()) {
    s = txn->Commit();
  }

  return s;
}

Status TransactionDBImpl::Delete(const WriteOptions& opts,
                                 ColumnFamilyHandle* column_family,
                                 const Slice& key) {
  Status s;

  Transaction* txn = BeginTransactionWrap(opts);
  txn->DisableIndexing();

  // Since the client didn't create a transaction, they don't care about
  // conflict checking for this write.  So we just need to do
  // DeleteUntracked().
  s = txn->DeleteUntracked(column_family, key);

  if (s.ok()) {
    s = txn->Commit();
  }

  return s;
}

Status TransactionDBImpl::Merge(const WriteOptions& opts,
                                ColumnFamilyHandle* column_family,
                                const Slice& key, const Slice& value) {
  Status s;

  Transaction* txn = BeginTransactionWrap(opts);
  txn->DisableIndexing();

  // Since the client didn't create a transaction, they don't care about
  // conflict checking for this write.  So we just need to do
  // MergeUntracked().
  s = txn->MergeUntracked(column_family, key, value);

  if (s.ok()) {
    s = txn->Commit();
  }

  return s;
}

Status TransactionDBImpl::Write(const WriteOptions& opts, WriteBatch* updates) {
  Transaction* txn = BeginTransactionWrap(opts);
  txn->DisableIndexing();

  assert(dynamic_cast<TransactionImpl*>(txn) != nullptr);
  auto txn_impl = reinterpret_cast<TransactionImpl*>(txn);

  // Since commitBatch sorts the keys before locking, concurrent Write()
  // operations will not cause a deadlock.
  // In order to avoid a deadlock with a concurrent Transaction, Transactions
  // should use a lock timeout.
  Status s = txn_impl->CommitBatch(updates);

  return s;
}

Status TransactionDBImpl::WriteAsync(const WriteOptions& opts,
                                     WriteBatch* updates,
                                     AsyncCallback* call_back) {
  TransactionOptions txn_options;

  Transaction* txn = BeginInternalTransaction(opts);

  txn->DisableIndexing();

  assert(dynamic_cast<TransactionImpl*>(txn) != nullptr);
  auto txn_impl = reinterpret_cast<TransactionImpl*>(txn);

  // Since commitBatch sorts the keys before locking, concurrent Write()
  // operations will not cause a deadlock.
  // In order to avoid a deadlock with a concurrent Transaction, Transactions
  // should use a lock timeout.
  Status s = txn_impl->CommitBatchAsync(updates, call_back);

  return s;
}

void TransactionDBImpl::InsertExpirableTransaction(TransactionID tx_id,
                                                   TransactionImpl* tx) {
  assert(tx->GetExpirationTime() > 0);
  std::lock_guard<std::mutex> lock(map_mutex_);
  expirable_transactions_map_.insert({tx_id, tx});
}

void TransactionDBImpl::RemoveExpirableTransaction(TransactionID tx_id) {
  std::lock_guard<std::mutex> lock(map_mutex_);
  expirable_transactions_map_.erase(tx_id);
}

bool TransactionDBImpl::TryStealingExpiredTransactionLocks(
    TransactionID tx_id) {
  std::lock_guard<std::mutex> lock(map_mutex_);

  auto tx_it = expirable_transactions_map_.find(tx_id);
  if (tx_it == expirable_transactions_map_.end()) {
    return true;
  }
  TransactionImpl& tx = *(tx_it->second);
  return tx.TryStealingLocks();
}

void TransactionDBImpl::ReinitializeTransaction(
    Transaction* txn, const WriteOptions& write_options,
    const TransactionOptions& txn_options) {
  assert(dynamic_cast<TransactionImpl*>(txn) != nullptr);
  auto txn_impl = reinterpret_cast<TransactionImpl*>(txn);

  txn_impl->Reinitialize(this, write_options, txn_options);
}

Transaction* TransactionDBImpl::GetTransactionByName(
    const TransactionName& name) {
  std::lock_guard<std::mutex> lock(name_map_mutex_);
  auto it = transactions_.find(name);
  if (it == transactions_.end()) {
    return nullptr;
  } else {
    return it->second;
  }
}

void TransactionDBImpl::GetAllPreparedTransactions(
    std::vector<Transaction*>* transv) {
  assert(transv);
  transv->clear();
  std::lock_guard<std::mutex> lock(name_map_mutex_);
  for (auto it = transactions_.begin(); it != transactions_.end(); it++) {
    if (it->second->GetState() == Transaction::PREPARED) {
      transv->push_back(it->second);
    }
  }
}

// for hotbackup
int TransactionDBImpl::do_manual_checkpoint(int32_t &manifest_file_num) {
  return db_impl_->do_manual_checkpoint(manifest_file_num);
}

int TransactionDBImpl::stream_log_extents(
                       std::function<int(const char*, int, int64_t, int)> *stream_extent,
                       int64_t start, int64_t end, int dest_fd) {
  return db_impl_->stream_log_extents(stream_extent, start, end, dest_fd);
}

int TransactionDBImpl::create_backup_snapshot(MetaSnapshotMap &meta_snapshot,
                                              int32_t &last_manifest_file_num,
                                              uint64_t &last_manifest_file_size,
                                              uint64_t &last_wal_file_num)
{
  return db_impl_->create_backup_snapshot(meta_snapshot,
                                          last_manifest_file_num,
                                          last_manifest_file_size,
                                          last_wal_file_num);
}

int TransactionDBImpl::release_backup_snapshot(MetaSnapshotMap &meta_snapshot) {
  return db_impl_->release_backup_snapshot(meta_snapshot);
}

int TransactionDBImpl::record_incremental_extent_ids(const int32_t first_manifest_file_num,
                                                     const int32_t last_manifest_file_num,
                                                     const uint64_t last_manifest_file_size)
{
  return db_impl_->record_incremental_extent_ids(first_manifest_file_num,
                                                 last_manifest_file_num,
                                                 last_manifest_file_size);
}

int64_t TransactionDBImpl::get_last_wal_file_size() const {
  return db_impl_->get_last_wal_file_size();
}

int64_t TransactionDBImpl::backup_manifest_file_size() const {
  return db_impl_->backup_manifest_file_size();
}

TransactionLockMgr::LockStatusData TransactionDBImpl::GetLockStatusData() {
  return lock_mgr_.GetLockStatusData();
}

void TransactionDBImpl::RegisterTransaction(Transaction* txn) {
  assert(txn);
  assert(txn->GetName().length() > 0);
  assert(GetTransactionByName(txn->GetName()) == nullptr);
  assert(txn->GetState() == Transaction::STARTED);
  std::lock_guard<std::mutex> lock(name_map_mutex_);
  transactions_[txn->GetName()] = txn;
}

void TransactionDBImpl::UnregisterTransaction(Transaction* txn) {
  assert(txn);
  std::lock_guard<std::mutex> lock(name_map_mutex_);
  auto it = transactions_.find(txn->GetName());
  assert(it != transactions_.end());
  transactions_.erase(it);
}

}  //  namespace util
}  //  namespace xengine
#endif  // ROCKSDB_LITE
