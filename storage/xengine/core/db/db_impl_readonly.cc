// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "db/db_impl_readonly.h"

#include "db/compacted_db_impl.h"
#include "db/db_impl.h"
#include "db/db_iter.h"
#include "db/merge_context.h"
#include "db/range_del_aggregator.h"

using namespace xengine;
using namespace common;
using namespace monitor;

namespace xengine {
namespace db {

#ifndef ROCKSDB_LITE

DBImplReadOnly::DBImplReadOnly(const DBOptions& db_options,
                               const std::string& dbname)
    : DBImpl(db_options, dbname) {
  __XENGINE_LOG(INFO, "Opening the db in read only mode");
  // LogFlush(immutable_db_options_.info_log);
}

DBImplReadOnly::~DBImplReadOnly() {}

// Implementations of the DB interface
Status DBImplReadOnly::Get(const ReadOptions& read_options,
                           ColumnFamilyHandle* column_family, const Slice& key,
                           PinnableSlice* pinnable_val) {
  int ret = Status::kOk;
  assert(pinnable_val != nullptr);
  Status s;
  SequenceNumber snapshot = versions_->LastSequence();
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();
  SuperVersion* super_version = cfd->GetSuperVersion();
  MergeContext merge_context;
  RangeDelAggregator range_del_agg(cfd->internal_comparator(), snapshot);
  LookupKey lkey(key, snapshot);
  bool value_found;
  if (super_version->mem->Get(lkey, pinnable_val->GetSelf(), &s, &merge_context,
                              &range_del_agg, read_options)) {
    pinnable_val->PinSelf();
  } else if (FAILED(cfd->get_from_storage_manager(read_options,
                                                  *super_version->current_meta_,
                                                  lkey,
                                                  *pinnable_val,
                                                  value_found))) {
    XENGINE_LOG(WARN, "fail to get from storage manager", K(ret));
  }
  return ret;
}

Iterator* DBImplReadOnly::NewIterator(const ReadOptions& read_options,
                                      ColumnFamilyHandle* column_family) {
  auto cfh = reinterpret_cast<ColumnFamilyHandleImpl*>(column_family);
  auto cfd = cfh->cfd();
  SuperVersion* super_version = cfd->GetSuperVersion()->Ref();
  SequenceNumber latest_snapshot = versions_->LastSequence();
  auto db_iter = NewArenaWrappedDbIterator(
      env_, read_options, *cfd->ioptions(), cfd->user_comparator(),
      (read_options.snapshot != nullptr
           ? reinterpret_cast<const SnapshotImpl*>(read_options.snapshot)
                 ->number_
           : latest_snapshot),
      super_version->mutable_cf_options.max_sequential_skip_in_iterations,
      super_version->version_number);
  auto internal_iter =
      NewInternalIterator(read_options, cfd, super_version, db_iter->GetArena(),
                          db_iter->GetRangeDelAggregator());
  db_iter->SetIterUnderDBIter(internal_iter);
  return db_iter;
}

Status DBImplReadOnly::NewIterators(
    const ReadOptions& read_options,
    const std::vector<ColumnFamilyHandle*>& column_families,
    std::vector<Iterator*>* iterators) {
  if (iterators == nullptr) {
    return Status::InvalidArgument("iterators not allowed to be nullptr");
  }
  iterators->clear();
  iterators->reserve(column_families.size());
  SequenceNumber latest_snapshot = versions_->LastSequence();

  for (auto cfh : column_families) {
    auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(cfh)->cfd();
    auto* sv = cfd->GetSuperVersion()->Ref();
    auto* db_iter = NewArenaWrappedDbIterator(
        env_, read_options, *cfd->ioptions(), cfd->user_comparator(),
        (read_options.snapshot != nullptr
             ? reinterpret_cast<const SnapshotImpl*>(read_options.snapshot)
                   ->number_
             : latest_snapshot),
        sv->mutable_cf_options.max_sequential_skip_in_iterations,
        sv->version_number);
    auto* internal_iter =
        NewInternalIterator(read_options, cfd, sv, db_iter->GetArena(),
                            db_iter->GetRangeDelAggregator());
    db_iter->SetIterUnderDBIter(internal_iter);
    iterators->push_back(db_iter);
  }

  return Status::OK();
}

Status DB::OpenForReadOnly(const Options& options, const std::string& dbname,
                           DB** dbptr, bool error_if_log_file_exist) {
  *dbptr = nullptr;

  // Try to first open DB as fully compacted DB
  Status s;
  s = CompactedDBImpl::Open(options, dbname, dbptr);
  if (s.ok()) {
    return s;
  }

  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::vector<ColumnFamilyDescriptor> column_families;
  column_families.push_back(
      ColumnFamilyDescriptor(kDefaultColumnFamilyName, cf_options));
  std::vector<ColumnFamilyHandle*> handles;

  s = DB::OpenForReadOnly(db_options, dbname, column_families, &handles, dbptr);
  if (s.ok()) {
    assert(handles.size() == 1);
    // i can delete the handle since DBImpl is always holding a
    // reference to default column family
    delete handles[0];
  }
  return s;
}

Status DB::OpenForReadOnly(
    const DBOptions& db_options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, DB** dbptr,
    bool error_if_log_file_exist) {
  *dbptr = nullptr;
  handles->clear();

  DBImplReadOnly* impl = MOD_NEW_OBJECT(memory::ModId::kDBImpl, DBImplReadOnly, db_options, dbname);
  // inherent the options
  Status s = impl->prepare_create_storage_manager(impl->initial_db_options_,
                                                  column_families[0].options);
  if (!s.ok()) {
    MOD_DELETE_OBJECT(DBImplReadOnly, impl);
    return s;
  }
  impl->mutex_.Lock();
  s = impl->Recover(column_families, column_families[0].options, true /* read only */,
                    error_if_log_file_exist);

  if (s.ok()) {
    SubTable* sub_table = nullptr;
    // todo delete
    for (auto iter :
         impl->versions_->get_global_ctx()->all_sub_table_->sub_table_map_) {
      handles->push_back(MOD_NEW_OBJECT(memory::ModId::kDBImpl, ColumnFamilyHandleImpl, iter.second, impl, &impl->mutex_));
//          new ColumnFamilyHandleImpl(iter.second, impl, &impl->mutex_));
      // no compaction schedule in recovery
      sub_table = iter.second;
      SuperVersion *old_sv = sub_table->InstallSuperVersion(
          MOD_NEW_OBJECT(memory::ModId::kSuperVersion, SuperVersion), &impl->mutex_);
      MOD_DELETE_OBJECT(SuperVersion, old_sv);

//      delete sub_table->InstallSuperVersion(
//          new SuperVersion(), &impl->mutex_);
      }
  }

  impl->mutex_.Unlock();
  if (s.ok()) {
    *dbptr = impl;
    for (auto* h : *handles) {
      impl->NewThreadStatusCfInfo(
          reinterpret_cast<ColumnFamilyHandleImpl*>(h)->cfd());
    }
  } else {
    for (auto h : *handles) {
      MOD_DELETE_OBJECT(ColumnFamilyHandle, h);
    }
    handles->clear();
    MOD_DELETE_OBJECT(DBImplReadOnly, impl);
  }
  return s;
}

#else   // !ROCKSDB_LITE

Status DB::OpenForReadOnly(const Options& options, const std::string& dbname,
                           DB** dbptr, bool error_if_log_file_exist) {
  return Status::NotSupported("Not supported in ROCKSDB_LITE.");
}

Status DB::OpenForReadOnly(
    const DBOptions& db_options, const std::string& dbname,
    const std::vector<ColumnFamilyDescriptor>& column_families,
    std::vector<ColumnFamilyHandle*>* handles, DB** dbptr,
    bool error_if_log_file_exist) {
  return Status::NotSupported("Not supported in ROCKSDB_LITE.");
}
#endif  // !ROCKSDB_LITE
}
}  // namespace xengine
