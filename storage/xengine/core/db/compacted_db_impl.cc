// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE
#include "db/compacted_db_impl.h"
#include "db/db_impl.h"
#include "db/version_set.h"
#include "table/get_context.h"
#include "storage/multi_version_extent_meta_layer.h"

using namespace xengine;
using namespace common;
using namespace util;

namespace xengine {

namespace db {

extern void MarkKeyMayExist(void* arg);
extern bool SaveValue(void* arg, const ParsedInternalKey& parsed_key,
                      const Slice& v, bool hit_and_return);

CompactedDBImpl::CompactedDBImpl(const DBOptions& options,
                                 const std::string& dbname)
    : DBImpl(options, dbname) {}

CompactedDBImpl::~CompactedDBImpl() {}

size_t CompactedDBImpl::FindFile(const Slice& key) {
  size_t left = 0;
  size_t right = files_.num_files - 1;
  while (left < right) {
    size_t mid = (left + right) >> 1;
    const FdWithKeyRange& f = files_.files[mid];
    if (user_comparator_->Compare(ExtractUserKey(f.largest_key), key) < 0) {
      // Key at "mid.largest" is < "target".  Therefore all
      // files at or before "mid" are uninteresting.
      left = mid + 1;
    } else {
      // Key at "mid.largest" is >= "target".  Therefore all files
      // after "mid" are uninteresting.
      right = mid;
    }
  }
  return right;
}

Status CompactedDBImpl::Get(const ReadOptions& options, ColumnFamilyHandle*,
                            const Slice& key, PinnableSlice* value) {
  //version_ = cfd_->GetSuperVersion()->current;
  LookupKey lkey(key, kMaxSequenceNumber);
  Status s;
  //TODO:yuanfeng, CompactedDBImpl not used?
  /*
  version_->get_from_storage_manager(options, lkey, value, &s, nullptr, nullptr,
                                     nullptr, cfd_->get_storage_manager(),
                                     cfd_->get_storage_manager()->get_current_version());
  */
  return s;
}

std::vector<Status> CompactedDBImpl::MultiGet(
    const ReadOptions& options, const std::vector<ColumnFamilyHandle*>&,
    const std::vector<Slice>& keys, std::vector<std::string>* values) {
  std::vector<Status> statuses(keys.size(), Status::NotFound());
  values->resize(keys.size());
  int idx = 0;
  for (const auto& key : keys) {
    PinnableSlice pinnable_val;
    std::string& value = (*values)[idx];
    LookupKey lkey(keys[idx], kMaxSequenceNumber);
    Status s;
    //TODO:yuanfeng, CompacteDBImpl not used?
    /*
    version_->get_from_storage_manager(
        options, lkey, &pinnable_val, &s, nullptr, nullptr, nullptr,
        cfd_->get_storage_manager(), 
        cfd_->get_storage_manager()->get_current_version());
    */
    value.assign(pinnable_val.data(), pinnable_val.size());
    // if (get_context.State() == GetContext::kFound) {
    statuses[idx] = s;
    //}
    ++idx;
  }
  return statuses;
}

Status CompactedDBImpl::Init(const Options& options) {
  mutex_.Lock();
  common::ColumnFamilyOptions cf_options(options);
  ColumnFamilyDescriptor cf(kDefaultColumnFamilyName,
                            cf_options);
  Status s = Recover({cf}, cf_options, true /* read only */, false, true);
  if (s.ok()) {
    cfd_ =
        reinterpret_cast<ColumnFamilyHandleImpl*>(DefaultColumnFamily())->cfd();
    SuperVersion *old_sv = cfd_->InstallSuperVersion(
        MOD_NEW_OBJECT(memory::ModId::kSuperVersion, SuperVersion),  &mutex_);
    MOD_DELETE_OBJECT(SuperVersion, old_sv);
//    delete cfd_->InstallSuperVersion(new SuperVersion(), &mutex_);
  }
  mutex_.Unlock();
  if (!s.ok()) {
    return s;
  }
  NewThreadStatusCfInfo(cfd_);
  // std::vector<MetaEntry> meta_entries;
  Arena arena;
  ReadOptions read_options;

  const Snapshot *sn = cfd_->get_meta_snapshot();
  if (nullptr == sn) {
    return Status::Aborted("Can't get meta snapshot for read");
  }
  int32_t l0 = sn->get_extent_layer_version(0)->get_total_normal_extent_count();
  int32_t l1 = sn->get_extent_layer_version(1)->get_total_normal_extent_count();
 
  cfd_->release_meta_snapshot(sn, &mutex_);

  if (l0 > 1) {
    return Status::NotSupported("L0 contain more than 1 file");
  }
  if (l0 == 1) {
    if (l1 > 0) {
      return Status::NotSupported("Both L0 and other level contain files");
    }
    return Status::OK();
  }
  if (l1 > 0) {
    return Status::OK();
  }
  return Status::NotSupported("no file exists");
}

Status CompactedDBImpl::Open(const Options& options, const std::string& dbname,
                             DB** dbptr) {
  *dbptr = nullptr;

  if (options.max_open_files != -1) {
    return Status::InvalidArgument("require max_open_files = -1");
  }
  if (options.merge_operator.get() != nullptr) {
    return Status::InvalidArgument("merge operator is not supported");
  }
  DBOptions db_options(options);
  ColumnFamilyOptions cf_options(options);
  std::unique_ptr<CompactedDBImpl> db(new CompactedDBImpl(db_options, dbname));
  Status s = db->prepare_create_storage_manager(db->initial_db_options_, cf_options);
  if (!s.ok()) {
    return s;
  }
  s = db->Init(options);
  if (s.ok()) {
    __XENGINE_LOG(INFO, "Opened the db as fully compacted mode");
    // LogFlush(db->immutable_db_options_.info_log);
    *dbptr = db.release();
  }
  return s;
}

}  // namespace db
}  // namespace xengine
#endif  // ROCKSDB_LITE
