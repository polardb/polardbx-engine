// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#ifndef ROCKSDB_LITE
#include <deque>
#include <string>
#include <vector>

#include "db/db_impl.h"
#include "xengine/compaction_filter.h"
#include "xengine/db.h"
#include "xengine/env.h"
#include "xengine/merge_operator.h"
#include "xengine/utilities/db_ttl.h"
#include "xengine/utilities/utility_db.h"

#ifdef _WIN32
// Windows API macro interference
#undef GetCurrentTime
#endif

namespace xengine {
namespace util {

class DBWithTTLImpl : public DBWithTTL {
 public:
  static void SanitizeOptions(int32_t ttl, common::ColumnFamilyOptions* options,
                              util::Env* env);

  explicit DBWithTTLImpl(DB* db);

  virtual ~DBWithTTLImpl();

  common::Status CreateColumnFamilyWithTtl(db::CreateSubTableArgs &args, db::ColumnFamilyHandle** handle,
      int ttl) override;

  common::Status CreateColumnFamily(db::CreateSubTableArgs &args, db::ColumnFamilyHandle** handle) override;

  using xengine::util::StackableDB::Put;
  virtual common::Status Put(const common::WriteOptions& options,
                             db::ColumnFamilyHandle* column_family,
                             const common::Slice& key,
                             const common::Slice& val) override;

  using xengine::util::StackableDB::Get;
  virtual common::Status Get(const common::ReadOptions& options,
                             db::ColumnFamilyHandle* column_family,
                             const common::Slice& key,
                             common::PinnableSlice* value) override;

  using xengine::util::StackableDB::MultiGet;
  virtual std::vector<common::Status> MultiGet(
      const common::ReadOptions& options,
      const std::vector<db::ColumnFamilyHandle*>& column_family,
      const std::vector<common::Slice>& keys,
      std::vector<std::string>* values) override;

  using xengine::util::StackableDB::KeyMayExist;
  virtual bool KeyMayExist(const common::ReadOptions& options,
                           db::ColumnFamilyHandle* column_family,
                           const common::Slice& key, std::string* value,
                           bool* value_found = nullptr) override;

  using xengine::util::StackableDB::Merge;
  virtual common::Status Merge(const common::WriteOptions& options,
                               db::ColumnFamilyHandle* column_family,
                               const common::Slice& key,
                               const common::Slice& value) override;

  virtual common::Status Write(const common::WriteOptions& opts,
                               db::WriteBatch* updates) override;

  using xengine::util::StackableDB::NewIterator;
  virtual Iterator* NewIterator(const common::ReadOptions& opts,
                                db::ColumnFamilyHandle* column_family) override;

  virtual DB* GetBaseDB() override { return db_; }

  static bool IsStale(const common::Slice& value, int32_t ttl, util::Env* env);

  static common::Status AppendTS(const common::Slice& val,
                                 std::string* val_with_ts, util::Env* env);

  static common::Status SanityCheckTimestamp(const common::Slice& str);

  static common::Status StripTS(std::string* str);

  static common::Status StripTS(common::PinnableSlice* str);

  static const uint32_t kTSLength = sizeof(int32_t);  // size of timestamp

  static const int32_t kMinTimestamp = 1368146402;  // 05/09/2013:5:40PM GMT-8

  static const int32_t kMaxTimestamp = 2147483647;  // 01/18/2038:7:14PM GMT-8
};

class TtlIterator : public Iterator {
 public:
  explicit TtlIterator(Iterator* iter) : iter_(iter) { assert(iter_); }

  ~TtlIterator() {
//    delete iter_;
    MOD_DELETE_OBJECT(Iterator, iter_);
  }

  bool Valid() const override { return iter_->Valid(); }

  void SeekToFirst() override { iter_->SeekToFirst(); }

  void SeekToLast() override { iter_->SeekToLast(); }

  void Seek(const common::Slice& target) override { iter_->Seek(target); }

  void SeekForPrev(const common::Slice& target) override {
    iter_->SeekForPrev(target);
  }

  void Next() override { iter_->Next(); }

  void Prev() override { iter_->Prev(); }

  common::Slice key() const override { return iter_->key(); }

  int32_t timestamp() const {
    return DecodeFixed32(iter_->value().data() + iter_->value().size() -
                         DBWithTTLImpl::kTSLength);
  }

  common::Slice value() const override {
    // TODO: handle timestamp corruption like in general iterator semantics
    assert(DBWithTTLImpl::SanityCheckTimestamp(iter_->value()).ok());
    common::Slice trimmed_value = iter_->value();
    trimmed_value.size_ -= DBWithTTLImpl::kTSLength;
    return trimmed_value;
  }

  common::Status status() const override { return iter_->status(); }

 private:
  Iterator* iter_;
};

class TtlCompactionFilter : public storage::CompactionFilter {
 public:
  TtlCompactionFilter(int32_t ttl, util::Env* env,
                      const storage::CompactionFilter* user_comp_filter,
                      std::unique_ptr<const storage::CompactionFilter>
                          user_comp_filter_from_factory = nullptr)
      : ttl_(ttl),
        env_(env),
        user_comp_filter_(user_comp_filter),
        user_comp_filter_from_factory_(
            std::move(user_comp_filter_from_factory)) {
    // Unlike the merge operator, compaction filter is necessary for TTL, hence
    // this would be called even if user doesn't specify any compaction-filter
    if (!user_comp_filter_) {
      user_comp_filter_ = user_comp_filter_from_factory_.get();
    }
  }

  virtual bool Filter(int level, const common::Slice& key,
                      const common::Slice& old_val, std::string* new_val,
                      bool* value_changed) const override {
    if (DBWithTTLImpl::IsStale(old_val, ttl_, env_)) {
      return true;
    }
    if (user_comp_filter_ == nullptr) {
      return false;
    }
    assert(old_val.size() >= DBWithTTLImpl::kTSLength);
    common::Slice old_val_without_ts(old_val.data(),
                                     old_val.size() - DBWithTTLImpl::kTSLength);
    if (user_comp_filter_->Filter(level, key, old_val_without_ts, new_val,
                                  value_changed)) {
      return true;
    }
    if (*value_changed) {
      new_val->append(
          old_val.data() + old_val.size() - DBWithTTLImpl::kTSLength,
          DBWithTTLImpl::kTSLength);
    }
    return false;
  }

  virtual const char* Name() const override { return "Delete By TTL"; }

 private:
  int32_t ttl_;
  util::Env* env_;
  const storage::CompactionFilter* user_comp_filter_;
  std::unique_ptr<const storage::CompactionFilter>
      user_comp_filter_from_factory_;
};

class TtlCompactionFilterFactory : public storage::CompactionFilterFactory {
 public:
  TtlCompactionFilterFactory(
      int32_t ttl, util::Env* env,
      std::shared_ptr<storage::CompactionFilterFactory> comp_filter_factory)
      : ttl_(ttl), env_(env), user_comp_filter_factory_(comp_filter_factory) {}

  virtual std::unique_ptr<storage::CompactionFilter> CreateCompactionFilter(
      const storage::CompactionFilter::Context& context) override {
    std::unique_ptr<const storage::CompactionFilter>
        user_comp_filter_from_factory = nullptr;
    if (user_comp_filter_factory_) {
      user_comp_filter_from_factory =
          user_comp_filter_factory_->CreateCompactionFilter(context);
    }

    return std::unique_ptr<TtlCompactionFilter>(new TtlCompactionFilter(
        ttl_, env_, nullptr, std::move(user_comp_filter_from_factory)));
  }

  virtual const char* Name() const override {
    return "TtlCompactionFilterFactory";
  }

 private:
  int32_t ttl_;
  util::Env* env_;
  std::shared_ptr<storage::CompactionFilterFactory> user_comp_filter_factory_;
};

class TtlMergeOperator : public db::MergeOperator {
 public:
  explicit TtlMergeOperator(const std::shared_ptr<db::MergeOperator>& merge_op,
                            util::Env* env)
      : user_merge_op_(merge_op), env_(env) {
    assert(merge_op);
    assert(env);
  }

  virtual bool FullMergeV2(const MergeOperationInput& merge_in,
                           MergeOperationOutput* merge_out) const override {
    const uint32_t ts_len = DBWithTTLImpl::kTSLength;
    if (merge_in.existing_value && merge_in.existing_value->size() < ts_len) {
      __XENGINE_LOG(ERROR, "Error: Could not remove timestamp from existing value.");
      return false;
    }

    // Extract time-stamp from each operand to be passed to user_merge_op_
    std::vector<common::Slice> operands_without_ts;
    for (const auto& operand : merge_in.operand_list) {
      if (operand.size() < ts_len) {
        __XENGINE_LOG(ERROR, "Error: Could not remove timestamp from operand value.");
        return false;
      }
      operands_without_ts.push_back(operand);
      operands_without_ts.back().remove_suffix(ts_len);
    }

    // Apply the user merge operator (store result in *new_value)
    bool good = true;
    MergeOperationOutput user_merge_out(merge_out->new_value,
                                        merge_out->existing_operand);
    if (merge_in.existing_value) {
      common::Slice existing_value_without_ts(
          merge_in.existing_value->data(),
          merge_in.existing_value->size() - ts_len);
      good = user_merge_op_->FullMergeV2(
          MergeOperationInput(merge_in.key, &existing_value_without_ts,
                              operands_without_ts),
          &user_merge_out);
    } else {
      good = user_merge_op_->FullMergeV2(
          MergeOperationInput(merge_in.key, nullptr, operands_without_ts),
          &user_merge_out);
    }

    // Return false if the user merge operator returned false
    if (!good) {
      return false;
    }

    if (merge_out->existing_operand.data()) {
      merge_out->new_value.assign(merge_out->existing_operand.data(),
                                  merge_out->existing_operand.size());
      merge_out->existing_operand = common::Slice(nullptr, 0);
    }

    // Augment the *new_value with the ttl time-stamp
    int64_t curtime;
    if (!env_->GetCurrentTime(&curtime).ok()) {
      __XENGINE_LOG(ERROR, "Error: Could not get current time to be attached internally to the new value.");
      return false;
    } else {
      char ts_string[ts_len];
      EncodeFixed32(ts_string, (int32_t)curtime);
      merge_out->new_value.append(ts_string, ts_len);
      return true;
    }
  }

  virtual bool PartialMergeMulti(const common::Slice& key,
                                 const std::deque<common::Slice>& operand_list,
                                 std::string* new_value) const override {
    const uint32_t ts_len = DBWithTTLImpl::kTSLength;
    std::deque<common::Slice> operands_without_ts;

    for (const auto& operand : operand_list) {
      if (operand.size() < ts_len) {
        __XENGINE_LOG(ERROR, "Error: Could not remove timestamp from value.");
        return false;
      }

      operands_without_ts.push_back(
          common::Slice(operand.data(), operand.size() - ts_len));
    }

    // Apply the user partial-merge operator (store result in *new_value)
    assert(new_value);
    if (!user_merge_op_->PartialMergeMulti(key, operands_without_ts, new_value)) {
      return false;
    }

    // Augment the *new_value with the ttl time-stamp
    int64_t curtime;
    if (!env_->GetCurrentTime(&curtime).ok()) {
      __XENGINE_LOG(ERROR, "Error: Could not get current time to be attached internally to the new value.");
      return false;
    } else {
      char ts_string[ts_len];
      EncodeFixed32(ts_string, (int32_t)curtime);
      new_value->append(ts_string, ts_len);
      return true;
    }
  }

  virtual const char* Name() const override { return "Merge By TTL"; }

 private:
  std::shared_ptr<db::MergeOperator> user_merge_op_;
  util::Env* env_;
};
}  //  namespace util
}  //  namespace xengine
#endif  // ROCKSDB_LITE
