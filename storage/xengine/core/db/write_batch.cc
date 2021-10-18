// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring
//    kTypeDeletion varstring
//    kTypeSingleDeletion varstring
//    kTypeMerge varstring varstring
//    kTypeColumnFamilyValue varint32 varstring varstring
//    kTypeColumnFamilyDeletion varint32 varstring varstring
//    kTypeColumnFamilySingleDeletion varint32 varstring varstring
//    kTypeColumnFamilyMerge varint32 varstring varstring
//    kTypeBeginPrepareXID varstring
//    kTypeEndPrepareXID
//    kTypeCommitXID varstring
//    kTypeRollbackXID varstring
//    kTypeNoop
// varstring :=
//    len: varint32
//    data: uint8[len]

#include "xengine/write_batch.h"

#include <map>
#include <stack>
#include <stdexcept>
#include <type_traits>
#include <vector>

#include "db/column_family.h"
#include "db/db_impl.h"
#include "db/dbformat.h"
#include "db/flush_scheduler.h"
#include "db/memtable.h"
#include "db/merge_context.h"
#include "db/merge_helper.h"
#include "db/snapshot_impl.h"
#include "db/write_batch_internal.h"
#include "monitoring/statistics.h"
#include "util/coding.h"
#include "util/string_util.h"
#include "xengine/merge_operator.h"
#include "xengine/utilities/transaction.h"

using namespace xengine;
using namespace util;
using namespace common;
using namespace monitor;

namespace xengine {
namespace db {

// anon namespace for file-local types
namespace {

enum ContentFlags : uint32_t {
  DEFERRED = 1 << 0,
  HAS_PUT = 1 << 1,
  HAS_DELETE = 1 << 2,
  HAS_SINGLE_DELETE = 1 << 3,
  HAS_MERGE = 1 << 4,
  HAS_BEGIN_PREPARE = 1 << 5,
  HAS_END_PREPARE = 1 << 6,
  HAS_COMMIT = 1 << 7,
  HAS_ROLLBACK = 1 << 8,
  HAS_DELETE_RANGE = 1 << 9,
};

struct BatchContentClassifier : public WriteBatch::Handler {
  uint32_t content_flags = 0;

  common::Status PutCF(uint32_t, const common::Slice&,
                       const common::Slice&) override {
    content_flags |= ContentFlags::HAS_PUT;
    return common::Status::OK();
  }

  common::Status DeleteCF(uint32_t, const common::Slice&) override {
    content_flags |= ContentFlags::HAS_DELETE;
    return common::Status::OK();
  }

  common::Status SingleDeleteCF(uint32_t, const common::Slice&) override {
    content_flags |= ContentFlags::HAS_SINGLE_DELETE;
    return common::Status::OK();
  }

  common::Status DeleteRangeCF(uint32_t, const common::Slice&,
                               const common::Slice&) override {
    content_flags |= ContentFlags::HAS_DELETE_RANGE;
    return common::Status::OK();
  }

  common::Status MergeCF(uint32_t, const common::Slice&,
                         const common::Slice&) override {
    content_flags |= ContentFlags::HAS_MERGE;
    return common::Status::OK();
  }

  common::Status MarkBeginPrepare() override {
    content_flags |= ContentFlags::HAS_BEGIN_PREPARE;
    return common::Status::OK();
  }

  common::Status MarkEndPrepare(const common::Slice&, common::SequenceNumber) override {
    content_flags |= ContentFlags::HAS_END_PREPARE;
    return common::Status::OK();
  }

  common::Status MarkCommit(const common::Slice&, common::SequenceNumber) override {
    content_flags |= ContentFlags::HAS_COMMIT;
    return common::Status::OK();
  }

  common::Status MarkRollback(const common::Slice&, common::SequenceNumber) override {
    content_flags |= ContentFlags::HAS_ROLLBACK;
    return common::Status::OK();
  }
};

}  // anon namespace

struct SavePoints {
  std::stack<SavePoint> stack;
};

WriteBatch::WriteBatch(size_t reserved_bytes, size_t max_bytes)
    : save_points_(nullptr),
      content_flags_(0),
      max_bytes_(max_bytes),
      rep_() {
  rep_.reserve((reserved_bytes > WriteBatchInternal::kHeader)
                   ? reserved_bytes
                   : WriteBatchInternal::kHeader);
  rep_.resize(WriteBatchInternal::kHeader);
}

WriteBatch::WriteBatch(const memory::xstring& rep)
    : save_points_(nullptr),
      content_flags_(ContentFlags::DEFERRED),
      max_bytes_(0),
      rep_(rep) {}

WriteBatch::WriteBatch(const WriteBatch& src)
    : save_points_(src.save_points_),
      wal_term_point_(src.wal_term_point_),
      content_flags_(src.content_flags_.load(std::memory_order_relaxed)),
      max_bytes_(src.max_bytes_),
      rep_(src.rep_) {}

WriteBatch::WriteBatch(WriteBatch&& src)
    : save_points_(std::move(src.save_points_)),
      wal_term_point_(std::move(src.wal_term_point_)),
      content_flags_(src.content_flags_.load(std::memory_order_relaxed)),
      max_bytes_(src.max_bytes_),
      rep_(std::move(src.rep_)) {}

WriteBatch& WriteBatch::operator=(const WriteBatch& src) {
  if (&src != this) {
    this->~WriteBatch();
    new (this) WriteBatch(src);
  }
  return *this;
}

WriteBatch& WriteBatch::operator=(WriteBatch&& src) {
  if (&src != this) {
    this->~WriteBatch();
    new (this) WriteBatch(std::move(src));
  }
  return *this;
}

WriteBatch::~WriteBatch() {
//  delete save_points_;
  MOD_DELETE_OBJECT(SavePoints, save_points_);
}

WriteBatch::Handler::~Handler() {}

void WriteBatch::Handler::LogData(const common::Slice& blob) {
  // If the user has not specified something to do with blobs, then we ignore
  // them.
}

bool WriteBatch::Handler::Continue() { return true; }

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(WriteBatchInternal::kHeader);

  content_flags_.store(0, std::memory_order_relaxed);

  if (save_points_ != nullptr) {
    while (!save_points_->stack.empty()) {
      save_points_->stack.pop();
    }
  }

  wal_term_point_.clear();
}

int WriteBatch::Count() const { return WriteBatchInternal::Count(this); }

uint32_t WriteBatch::ComputeContentFlags() const {
  auto rv = content_flags_.load(std::memory_order_relaxed);
  if ((rv & ContentFlags::DEFERRED) != 0) {
    BatchContentClassifier classifier;
    Iterate(&classifier);
    rv = classifier.content_flags;

    // this method is conceptually const, because it is performing a lazy
    // computation that doesn't affect the abstract state of the batch.
    // content_flags_ is marked mutable so that we can perform the
    // following assignment
    content_flags_.store(rv, std::memory_order_relaxed);
  }
  return rv;
}

void WriteBatch::MarkWalTerminationPoint() {
  wal_term_point_.size = GetDataSize();
  wal_term_point_.count = Count();
  wal_term_point_.content_flags = content_flags_;
}

bool WriteBatch::HasPut() const {
  return (ComputeContentFlags() & ContentFlags::HAS_PUT) != 0;
}

bool WriteBatch::HasDelete() const {
  return (ComputeContentFlags() & ContentFlags::HAS_DELETE) != 0;
}

bool WriteBatch::HasSingleDelete() const {
  return (ComputeContentFlags() & ContentFlags::HAS_SINGLE_DELETE) != 0;
}

bool WriteBatch::HasDeleteRange() const {
  return (ComputeContentFlags() & ContentFlags::HAS_DELETE_RANGE) != 0;
}

bool WriteBatch::HasMerge() const {
  return (ComputeContentFlags() & ContentFlags::HAS_MERGE) != 0;
}

bool ReadKeyFromWriteBatchEntry(common::Slice* input, common::Slice* key,
                                bool cf_record) {
  assert(input != nullptr && key != nullptr);
  // Skip tag byte
  input->remove_prefix(1);

  if (cf_record) {
    // Skip column_family bytes
    uint32_t cf;
    if (!GetVarint32(input, &cf)) {
      return false;
    }
  }

  // Extract key
  return GetLengthPrefixedSlice(input, key);
}

bool WriteBatch::HasBeginPrepare() const {
  return (ComputeContentFlags() & ContentFlags::HAS_BEGIN_PREPARE) != 0;
}

bool WriteBatch::HasEndPrepare() const {
  return (ComputeContentFlags() & ContentFlags::HAS_END_PREPARE) != 0;
}

bool WriteBatch::HasCommit() const {
  return (ComputeContentFlags() & ContentFlags::HAS_COMMIT) != 0;
}

bool WriteBatch::HasRollback() const {
  return (ComputeContentFlags() & ContentFlags::HAS_ROLLBACK) != 0;
}

common::Status ReadRecordFromWriteBatch(common::Slice* input, char* tag,
                                        uint32_t* column_family,
                                        common::Slice* key,
                                        common::Slice* value,
                                        common::Slice* blob,
                                        common::Slice* xid,
                                        common::SequenceNumber* prepare_seq) {
  assert(key != nullptr && value != nullptr);
  *tag = (*input)[0];
  input->remove_prefix(1);
  *column_family = 0;  // default
  switch (*tag) {
    case kTypeColumnFamilyValue:
      if (!GetVarint32(input, column_family)) {
        return common::Status::Corruption("bad WriteBatch Put");
      }
    // intentional fallthrough
    case kTypeValue:
      if (!GetLengthPrefixedSlice(input, key) ||
          !GetLengthPrefixedSlice(input, value)) {
        return common::Status::Corruption("bad WriteBatch Put");
      }
      break;
    case kTypeColumnFamilyDeletion:
    case kTypeColumnFamilySingleDeletion:
      if (!GetVarint32(input, column_family)) {
        return common::Status::Corruption("bad WriteBatch Delete");
      }
    // intentional fallthrough
    case kTypeDeletion:
    case kTypeSingleDeletion:
      if (!GetLengthPrefixedSlice(input, key)) {
        return common::Status::Corruption("bad WriteBatch Delete");
      }
      break;
    case kTypeColumnFamilyRangeDeletion:
      if (!GetVarint32(input, column_family)) {
        return common::Status::Corruption("bad WriteBatch DeleteRange");
      }
    // intentional fallthrough
    case kTypeRangeDeletion:
      // for range delete, "key" is begin_key, "value" is end_key
      if (!GetLengthPrefixedSlice(input, key) ||
          !GetLengthPrefixedSlice(input, value)) {
        return common::Status::Corruption("bad WriteBatch DeleteRange");
      }
      break;
    case kTypeColumnFamilyMerge:
      if (!GetVarint32(input, column_family)) {
        return common::Status::Corruption("bad WriteBatch Merge");
      }
    // intentional fallthrough
    case kTypeMerge:
      if (!GetLengthPrefixedSlice(input, key) ||
          !GetLengthPrefixedSlice(input, value)) {
        return common::Status::Corruption("bad WriteBatch Merge");
      }
      break;
    case kTypeLogData:
      assert(blob != nullptr);
      if (!GetLengthPrefixedSlice(input, blob)) {
        return common::Status::Corruption("bad WriteBatch Blob");
      }
      break;
    case kTypeNoop:
    case kTypeBeginPrepareXID:
      break;
    case kTypeEndPrepareXID:
      if (!GetLengthPrefixedSlice(input, xid)) {
        return common::Status::Corruption("bad EndPrepare XID");
      }
      if (prepare_seq != nullptr) {
        GetVarint64(input, prepare_seq);
      }
      break;
    case kTypeCommitXID:
      if (!GetLengthPrefixedSlice(input, xid)) {
        return common::Status::Corruption("bad Commit XID");
      }
      if (prepare_seq != nullptr) {
        GetVarint64(input, prepare_seq);
      }
      break;
    case kTypeRollbackXID:
      if (!GetLengthPrefixedSlice(input, xid)) {
        return common::Status::Corruption("bad Rollback XID");
      }
      if (prepare_seq != nullptr) {
        GetVarint64(input, prepare_seq);
      }
      break;
    default:
      return common::Status::Corruption("unknown WriteBatch tag");
  }
  return common::Status::OK();
}

common::Status WriteBatch::Iterate(Handler* handler, bool is_parallel_recovering) const {
  common::Slice input(rep_);
  auto count = WriteBatchInternal::Count(this);
  uint64_t original_input_size = input.size();
  if (original_input_size < WriteBatchInternal::kHeader) {
    return common::Status::Corruption("malformed WriteBatch (too small)");
  }

  input.remove_prefix(WriteBatchInternal::kHeader);
  common::Slice key, value, blob, xid;
  int found = 0;
  common::Status s;
  uint64_t replayed_length = 0;
  uint64_t uncommit_offset = 0;
  uint64_t replayed_count = 0;
  while (s.ok() && !input.empty() && handler->Continue()) {
    if (handler->is_during_recovery() && uncommit_offset == 0) {
      replayed_length = original_input_size - input.size();
    }
    char tag = 0;
    uint32_t column_family = 0;  // default
    SequenceNumber prepare_seq = 0;
    if (!is_parallel_recovering) {
      s = ReadRecordFromWriteBatch(&input, &tag, &column_family, &key, &value,
                                   &blob, &xid, nullptr);
    } else {
      s = ReadRecordFromWriteBatch(&input, &tag, &column_family, &key, &value,
                                   &blob, &xid, &prepare_seq);
    }
    if (!s.ok()) {
      return s;
    }

    switch (tag) {
      case kTypeColumnFamilyValue:
      case kTypeValue:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_PUT));
        s = handler->PutCF(column_family, key, value);
        found++;
        break;
      case kTypeColumnFamilyDeletion:
      case kTypeDeletion:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_DELETE));
        s = handler->DeleteCF(column_family, key);
        found++;
        break;
      case kTypeColumnFamilySingleDeletion:
      case kTypeSingleDeletion:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_SINGLE_DELETE));
        s = handler->SingleDeleteCF(column_family, key);
        found++;
        break;
      case kTypeColumnFamilyRangeDeletion:
      case kTypeRangeDeletion:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_DELETE_RANGE));
        s = handler->DeleteRangeCF(column_family, key, value);
        found++;
        break;
      case kTypeColumnFamilyMerge:
      case kTypeMerge:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_MERGE));
        s = handler->MergeCF(column_family, key, value);
        found++;
        break;
      case kTypeLogData:
        handler->LogData(blob);
        break;
      case kTypeBeginPrepareXID:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_BEGIN_PREPARE));
        s = handler->MarkBeginPrepare();
        break;
      case kTypeEndPrepareXID:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_END_PREPARE));
        s = handler->MarkEndPrepare(xid, prepare_seq);
        break;
      case kTypeCommitXID:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_COMMIT));
        s = handler->MarkCommit(xid, prepare_seq);
        // bugfix: #31730974
        // Status::RecoverCommitLater means current transaction cannot be committed
        // immediately during recovering
        if (handler->is_during_recovery() && s.is_recover_commit_later()) {
          if (uncommit_offset == 0) {
            uncommit_offset = replayed_length;
            replayed_count = found;
          }
          s = Status::OK();
        }
        break;
      case kTypeRollbackXID:
        assert(content_flags_.load(std::memory_order_relaxed) &
               (ContentFlags::DEFERRED | ContentFlags::HAS_ROLLBACK));
        s = handler->MarkRollback(xid, prepare_seq);
        break;
      case kTypeNoop:
        break;
      default:
        return common::Status::Corruption("unknown WriteBatch tag");
    }
  }
  if (!s.ok()) {
    return s;
  }
  if (found != count) {
    return common::Status::Corruption("WriteBatch has wrong count");
  } else {
    if (uncommit_offset > 0) {
      handler->set_uncommit_info(uncommit_offset, count - replayed_count);
    }
    return common::Status::OK();
  }
}

int WriteBatchInternal::Count(const WriteBatch* b) {
  return DecodeFixed32(b->rep_.data() + 8);
}

void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[8], n);
}

SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}

size_t WriteBatchInternal::GetFirstOffset(WriteBatch* b) {
  return WriteBatchInternal::kHeader;
}

common::Status WriteBatchInternal::Put(WriteBatch* b, uint32_t column_family_id,
                                       const common::Slice& key,
                                       const common::Slice& value) {
  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeValue));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyValue));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSlice(&b->rep_, key);
  PutLengthPrefixedSlice(&b->rep_, value);
  b->content_flags_.store(
      b->content_flags_.load(std::memory_order_relaxed) | ContentFlags::HAS_PUT,
      std::memory_order_relaxed);
  return save.commit();
}

common::Status WriteBatch::Put(ColumnFamilyHandle* column_family,
                               const common::Slice& key,
                               const common::Slice& value) {
  return WriteBatchInternal::Put(this, GetColumnFamilyID(column_family), key,
                                 value);
}

common::Status WriteBatchInternal::Put(WriteBatch* b, uint32_t column_family_id,
                                       const common::SliceParts& key,
                                       const common::SliceParts& value) {
  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeValue));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyValue));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSliceParts(&b->rep_, key);
  PutLengthPrefixedSliceParts(&b->rep_, value);
  b->content_flags_.store(
      b->content_flags_.load(std::memory_order_relaxed) | ContentFlags::HAS_PUT,
      std::memory_order_relaxed);
  return save.commit();
}

common::Status WriteBatch::Put(ColumnFamilyHandle* column_family,
                               const common::SliceParts& key,
                               const common::SliceParts& value) {
  return WriteBatchInternal::Put(this, GetColumnFamilyID(column_family), key,
                                 value);
}

common::Status WriteBatchInternal::InsertNoop(WriteBatch* b) {
  b->rep_.push_back(static_cast<char>(kTypeNoop));
  return common::Status::OK();
}

common::Status WriteBatchInternal::MarkBeginPrepare(WriteBatch* b) {
  b->rep_.push_back(static_cast<char>(kTypeBeginPrepareXID));
  return common::Status::OK();
}

common::Status WriteBatchInternal::MarkEndPrepare(WriteBatch* b,
                                                  const common::Slice& xid) {
  // a manually constructed batch can only contain one prepare section
  assert(b->rep_[12] == static_cast<char>(kTypeNoop));

  // all savepoints up to this point are cleared
  if (b->save_points_ != nullptr) {
    while (!b->save_points_->stack.empty()) {
      b->save_points_->stack.pop();
    }
  }

  // rewrite noop as begin marker
  b->rep_[12] = static_cast<char>(kTypeBeginPrepareXID);
  b->rep_.push_back(static_cast<char>(kTypeEndPrepareXID));
  PutLengthPrefixedSlice(&b->rep_, xid);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_END_PREPARE |
                              ContentFlags::HAS_BEGIN_PREPARE,
                          std::memory_order_relaxed);
  return common::Status::OK();
}

common::Status WriteBatchInternal::MarkEndPrepare(WriteBatch* b,
                                                  const common::Slice& xid,
                                                  common::SequenceNumber prepare_seq) {
  b->rep_.push_back(static_cast<char>(kTypeEndPrepareXID));
  PutLengthPrefixedSlice(&b->rep_, xid);
  PutVarint64(&b->rep_, prepare_seq);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_END_PREPARE |
                              ContentFlags::HAS_BEGIN_PREPARE,
                          std::memory_order_relaxed);
  return common::Status::OK();
}

common::Status WriteBatchInternal::MarkCommit(WriteBatch* b,
                                              const common::Slice& xid) {
  b->rep_.push_back(static_cast<char>(kTypeCommitXID));
  PutLengthPrefixedSlice(&b->rep_, xid);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_COMMIT,
                          std::memory_order_relaxed);
  return common::Status::OK();
}

common::Status WriteBatchInternal::MarkCommit(WriteBatch* b,
                                              const common::Slice& xid,
                                              SequenceNumber prepare_seq) {
  b->rep_.push_back(static_cast<char>(kTypeCommitXID));
  PutLengthPrefixedSlice(&b->rep_, xid);
  PutVarint64(&b->rep_, prepare_seq);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_COMMIT,
                          std::memory_order_relaxed);
  return common::Status::OK();
}

common::Status WriteBatchInternal::MarkRollback(WriteBatch* b,
                                                const common::Slice& xid) {
  b->rep_.push_back(static_cast<char>(kTypeRollbackXID));
  PutLengthPrefixedSlice(&b->rep_, xid);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_ROLLBACK,
                          std::memory_order_relaxed);
  return common::Status::OK();
}

common::Status WriteBatchInternal::MarkRollback(WriteBatch* b,
                                                const common::Slice& xid,
                                                SequenceNumber prepare_seq) {
  b->rep_.push_back(static_cast<char>(kTypeRollbackXID));
  PutLengthPrefixedSlice(&b->rep_, xid);
  PutVarint64(&b->rep_, prepare_seq);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_ROLLBACK,
                          std::memory_order_relaxed);
  return common::Status::OK();
}

common::Status WriteBatchInternal::Delete(WriteBatch* b,
                                          uint32_t column_family_id,
                                          const common::Slice& key) {
  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeDeletion));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyDeletion));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSlice(&b->rep_, key);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_DELETE,
                          std::memory_order_relaxed);
  return save.commit();
}

common::Status WriteBatch::Delete(ColumnFamilyHandle* column_family,
                                  const common::Slice& key) {
  return WriteBatchInternal::Delete(this, GetColumnFamilyID(column_family),
                                    key);
}

common::Status WriteBatchInternal::Delete(WriteBatch* b,
                                          uint32_t column_family_id,
                                          const common::SliceParts& key) {
  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeDeletion));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyDeletion));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSliceParts(&b->rep_, key);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_DELETE,
                          std::memory_order_relaxed);
  return save.commit();
}

common::Status WriteBatch::Delete(ColumnFamilyHandle* column_family,
                                  const common::SliceParts& key) {
  return WriteBatchInternal::Delete(this, GetColumnFamilyID(column_family),
                                    key);
}

common::Status WriteBatchInternal::SingleDelete(WriteBatch* b,
                                                uint32_t column_family_id,
                                                const common::Slice& key) {
  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeSingleDeletion));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilySingleDeletion));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSlice(&b->rep_, key);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_SINGLE_DELETE,
                          std::memory_order_relaxed);
  return save.commit();
}

common::Status WriteBatch::SingleDelete(ColumnFamilyHandle* column_family,
                                        const common::Slice& key) {
  return WriteBatchInternal::SingleDelete(
      this, GetColumnFamilyID(column_family), key);
}

common::Status WriteBatchInternal::SingleDelete(WriteBatch* b,
                                                uint32_t column_family_id,
                                                const common::SliceParts& key) {
  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeSingleDeletion));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilySingleDeletion));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSliceParts(&b->rep_, key);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_SINGLE_DELETE,
                          std::memory_order_relaxed);
  return save.commit();
}

common::Status WriteBatch::SingleDelete(ColumnFamilyHandle* column_family,
                                        const common::SliceParts& key) {
  return WriteBatchInternal::SingleDelete(
      this, GetColumnFamilyID(column_family), key);
}

common::Status WriteBatchInternal::DeleteRange(WriteBatch* b,
                                               uint32_t column_family_id,
                                               const common::Slice& begin_key,
                                               const common::Slice& end_key) {
  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeRangeDeletion));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyRangeDeletion));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSlice(&b->rep_, begin_key);
  PutLengthPrefixedSlice(&b->rep_, end_key);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_DELETE_RANGE,
                          std::memory_order_relaxed);
  return save.commit();
}

common::Status WriteBatch::DeleteRange(ColumnFamilyHandle* column_family,
                                       const common::Slice& begin_key,
                                       const common::Slice& end_key) {
  return WriteBatchInternal::DeleteRange(this, GetColumnFamilyID(column_family),
                                         begin_key, end_key);
}

common::Status WriteBatchInternal::DeleteRange(
    WriteBatch* b, uint32_t column_family_id,
    const common::SliceParts& begin_key, const common::SliceParts& end_key) {
  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeRangeDeletion));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyRangeDeletion));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSliceParts(&b->rep_, begin_key);
  PutLengthPrefixedSliceParts(&b->rep_, end_key);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_DELETE_RANGE,
                          std::memory_order_relaxed);
  return save.commit();
}

common::Status WriteBatch::DeleteRange(ColumnFamilyHandle* column_family,
                                       const common::SliceParts& begin_key,
                                       const common::SliceParts& end_key) {
  return WriteBatchInternal::DeleteRange(this, GetColumnFamilyID(column_family),
                                         begin_key, end_key);
}

common::Status WriteBatchInternal::Merge(WriteBatch* b,
                                         uint32_t column_family_id,
                                         const common::Slice& key,
                                         const common::Slice& value) {
  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeMerge));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyMerge));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSlice(&b->rep_, key);
  PutLengthPrefixedSlice(&b->rep_, value);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_MERGE,
                          std::memory_order_relaxed);
  return save.commit();
}

common::Status WriteBatch::Merge(ColumnFamilyHandle* column_family,
                                 const common::Slice& key,
                                 const common::Slice& value) {
  return WriteBatchInternal::Merge(this, GetColumnFamilyID(column_family), key,
                                   value);
}

common::Status WriteBatchInternal::Merge(WriteBatch* b,
                                         uint32_t column_family_id,
                                         const common::SliceParts& key,
                                         const common::SliceParts& value) {
  LocalSavePoint save(b);
  WriteBatchInternal::SetCount(b, WriteBatchInternal::Count(b) + 1);
  if (column_family_id == 0) {
    b->rep_.push_back(static_cast<char>(kTypeMerge));
  } else {
    b->rep_.push_back(static_cast<char>(kTypeColumnFamilyMerge));
    PutVarint32(&b->rep_, column_family_id);
  }
  PutLengthPrefixedSliceParts(&b->rep_, key);
  PutLengthPrefixedSliceParts(&b->rep_, value);
  b->content_flags_.store(b->content_flags_.load(std::memory_order_relaxed) |
                              ContentFlags::HAS_MERGE,
                          std::memory_order_relaxed);
  return save.commit();
}

common::Status WriteBatch::Merge(ColumnFamilyHandle* column_family,
                                 const common::SliceParts& key,
                                 const common::SliceParts& value) {
  return WriteBatchInternal::Merge(this, GetColumnFamilyID(column_family), key,
                                   value);
}

common::Status WriteBatch::PutLogData(const common::Slice& blob) {
  LocalSavePoint save(this);
  rep_.push_back(static_cast<char>(kTypeLogData));
  PutLengthPrefixedSlice(&rep_, blob);
  return save.commit();
}

void WriteBatch::SetSavePoint() {
  if (save_points_ == nullptr) {
//    save_points_ = new SavePoints();
    save_points_ = MOD_NEW_OBJECT(memory::ModId::kWriteBatch, SavePoints);
  }
  // Record length and count of current batch of writes.
  save_points_->stack.push(SavePoint(
      GetDataSize(), Count(), content_flags_.load(std::memory_order_relaxed)));
}

common::Status WriteBatch::RollbackToSavePoint() {
  if (save_points_ == nullptr || save_points_->stack.size() == 0) {
    return common::Status::NotFound();
  }

  // Pop the most recent savepoint off the stack
  SavePoint savepoint = save_points_->stack.top();
  save_points_->stack.pop();

  assert(savepoint.size <= rep_.size());
  assert(savepoint.count <= Count());

  if (savepoint.size == rep_.size()) {
    // No changes to rollback
  } else if (savepoint.size == 0) {
    // Rollback everything
    Clear();
  } else {
    rep_.resize(savepoint.size);
    WriteBatchInternal::SetCount(this, savepoint.count);
    content_flags_.store(savepoint.content_flags, std::memory_order_relaxed);
  }

  return common::Status::OK();
}

class MemTableInserter : public WriteBatch::Handler {
  SequenceNumber sequence_;
  ColumnFamilyMemTables* const cf_mems_;
  FlushScheduler* const flush_scheduler_;
  const bool ignore_missing_column_families_;
  uint64_t recovering_log_number_;
  // log number that all Memtables inserted into should reference
  uint64_t log_number_ref_;
  DBImpl* db_;
  const bool concurrent_memtable_writes_;
  bool post_info_created_;

  bool* has_valid_writes_;
  std::list<std::string> uncommit_transaction_list_;
  // On some (!) platforms just default creating
  // a map is too expensive in the Write() path as they
  // cause memory allocations though unused.
  // Make creation optional but do not incur
  // unique_ptr additional allocation
  using MemPostInfoMap = std::map<MemTable*, MemTablePostProcessInfo>;
  using PostMapType = std::aligned_storage<sizeof(MemPostInfoMap)>::type;
  PostMapType mem_post_info_map_;
  // current recovered transaction we are rebuilding (recovery)
  WriteBatch* rebuilding_trx_;
  std::unordered_map<int64_t, int64_t> *missing_subtable_during_recovery_;
  uint64_t uncommit_offset_;
  uint64_t uncommit_count_;

  MemPostInfoMap& GetPostMap() {
    assert(concurrent_memtable_writes_);
    if (!post_info_created_) {
      new (&mem_post_info_map_) MemPostInfoMap();
      post_info_created_ = true;
    }
    return *reinterpret_cast<MemPostInfoMap*>(&mem_post_info_map_);
  }

 public:
  // cf_mems should not be shared with concurrent inserters
  MemTableInserter(SequenceNumber sequence, ColumnFamilyMemTables* cf_mems,
                   FlushScheduler* flush_scheduler,
                   bool ignore_missing_column_families,
                   uint64_t recovering_log_number, DB* db,
                   bool concurrent_memtable_writes,
                   bool* has_valid_writes = nullptr,
                   std::unordered_map<int64_t, int64_t> *missing_subtable_during_recovery = nullptr)
      : sequence_(sequence),
        cf_mems_(cf_mems),
        flush_scheduler_(flush_scheduler),
        ignore_missing_column_families_(ignore_missing_column_families),
        recovering_log_number_(recovering_log_number),
        log_number_ref_(0),
        db_(reinterpret_cast<DBImpl*>(db)),
        concurrent_memtable_writes_(concurrent_memtable_writes),
        post_info_created_(false),
        has_valid_writes_(has_valid_writes),
        rebuilding_trx_(nullptr),
        missing_subtable_during_recovery_(missing_subtable_during_recovery),
        uncommit_offset_(0),
        uncommit_count_(0) {
    assert(cf_mems_);
  }

  ~MemTableInserter() {
    if (post_info_created_) {
      reinterpret_cast<MemPostInfoMap*>(&mem_post_info_map_)->~MemPostInfoMap();
    }
  }

  MemTableInserter(const MemTableInserter&) = delete;
  MemTableInserter& operator=(const MemTableInserter&) = delete;

  void set_log_number_ref(uint64_t log) { log_number_ref_ = log; }

  SequenceNumber get_final_sequence() const { return sequence_; }

  void PostProcess() {
    assert(concurrent_memtable_writes_);
    // If post info was not created there is nothing
    // to process and no need to create on demand
    if (post_info_created_) {
      for (auto& pair : GetPostMap()) {
        pair.first->BatchPostProcess(pair.second);
      }
    }
  }

  bool SeekToColumnFamily(uint32_t column_family_id, common::Status* s) {
    // If we are in a concurrent mode, it is the caller's responsibility
    // to clone the original ColumnFamilyMemTables so that each thread
    // has its own instance.  Otherwise, it must be guaranteed that there
    // is no concurrent access
    bool found = cf_mems_->Seek(column_family_id);
    if (!found) {
      if (cf_mems_->is_subtable_dropped(column_family_id)) {
        *s = common::Status::OK();
      } else if (ignore_missing_column_families_) {
        XENGINE_LOG(DEBUG, "can't find the subtable in write batch, which maybe dropped", "index_id", column_family_id);
        *s = common::Status::OK();
      } else if (db_->get_version_set()->GetColumnFamilySet()->get_during_replay_wal()) {
        if (nullptr != missing_subtable_during_recovery_) { // TODO
          if (missing_subtable_during_recovery_->end() == missing_subtable_during_recovery_->find(column_family_id)) {
            missing_subtable_during_recovery_->emplace(column_family_id, column_family_id);
            XENGINE_LOG(INFO, "can't find the subtable in write batch, which maybe dropped", "index_id", column_family_id);
          }
          *s = common::Status::OK();
        } else {
          *s = common::Status::InvalidArgument("can't find the subtable in write batch");
        }
      } else {
        *s = common::Status::InvalidArgument(
            "Invalid sub table specified in write batch");
      }
      if (!s->ok()) {
        XENGINE_LOG(ERROR, "Invalid subtable specified in write batch", "index_id", column_family_id);
      }
      return false;
    }
    if (recovering_log_number_ != 0 &&
        (recovering_log_number_ < cf_mems_->GetLogNumber()
         || sequence_ <= cf_mems_->GetSequence())) {
      // This is true only in recovery environment (recovering_log_number_ is
      // always 0 in
      // non-recovery, regular write code-path)
      // * If recovering_log_number_ < cf_mems_->GetLogNumber(), this means that
      // column
      // family already contains updates from this log. We can't apply updates
      // twice because of update-in-place or merge workloads -- ignore the
      // update
      // * check the recovery sequence, if sequence <= rp.seq, no need to replay
      XENGINE_LOG(DEBUG, "won't replay", K(recovering_log_number_), K(sequence_), K(column_family_id));
      *s = common::Status::OK();
      return false;
    }

    if (has_valid_writes_ != nullptr) {
      *has_valid_writes_ = true;
    }

    if (log_number_ref_ > 0) {
      cf_mems_->GetMemTable()->RefLogContainingPrepSection(log_number_ref_);
    }

    return true;
  }

  Status recovery_final_commit(const WriteBatch *source_write_batch) {
    Status s;
    if (recovering_log_number_ != 0 && !uncommit_transaction_list_.empty()) {
      // bugfix: #31730974
      // there is uncommitted transactions during recovering source_write_batch
      // check if we can commit them now or put them into global commit trasaction
      // groups and wait for committing later
      auto source_contents = WriteBatchInternal::Contents(source_write_batch);
      assert(uncommit_offset_ >= WriteBatchInternal::kHeader && uncommit_offset_ <= source_contents.size());
      auto contents = Slice(source_contents.data() + uncommit_offset_ - WriteBatchInternal::kHeader,
          source_contents.size() - uncommit_offset_ + WriteBatchInternal::kHeader);
      bool commit = true;
      db_->lock_recovered_transaction_mutex();
      for (auto transaction_name : uncommit_transaction_list_) {
        auto transaction = db_->get_recovered_transaction(transaction_name);
        if (transaction == nullptr || transaction->batch_ == nullptr) {
          commit = false;
          break;
        }
      }
      if (commit) {
        db_->unlock_recovered_transaction_mutex();
        WriteBatch write_batch;
        WriteBatchInternal::SetContents(&write_batch, contents);
        WriteBatchInternal::SetSequence(&write_batch, sequence_);
        WriteBatchInternal::SetCount(&write_batch, uncommit_count_);
        uncommit_transaction_list_.clear();
        s = write_batch.Iterate(this, recovering_log_number_ != 0 && concurrent_memtable_writes_);
      } else {
        WriteBatch *write_batch = MOD_NEW_OBJECT(memory::ModId::kWriteBatch, WriteBatch);
        WriteBatchInternal::SetContents(write_batch, contents);
        WriteBatchInternal::SetSequence(write_batch, sequence_);
        WriteBatchInternal::SetCount(write_batch, uncommit_count_);
        db_->insert_commit_transaction_groups(sequence_, uncommit_transaction_list_, write_batch);
        db_->unlock_recovered_transaction_mutex();
      }
    }
    return s;
  }

  void set_uncommit_info(uint64_t uncommit_offset, uint64_t uncommit_count) override {
    uncommit_offset_ = uncommit_offset;
    uncommit_count_ = uncommit_count;
  }

  bool is_during_recovery() override {
    return recovering_log_number_ != 0;
  }

  virtual common::Status PutCF(uint32_t column_family_id,
                               const common::Slice& key,
                               const common::Slice& value) override {
    if (recovering_log_number_ != 0 && !uncommit_transaction_list_.empty()) {
      return common::Status::OK();
    }
    if (rebuilding_trx_ != nullptr) {
      WriteBatchInternal::Put(rebuilding_trx_, column_family_id, key, value);
      return common::Status::OK();
    }

    common::Status seek_status;
    if (!SeekToColumnFamily(column_family_id, &seek_status)) {
      ++sequence_;
      return seek_status;
    }

    MemTable* mem = cf_mems_->GetMemTable();
    auto* moptions = mem->GetMemTableOptions();
    if (!moptions->inplace_update_support) {
      mem->Add(sequence_, kTypeValue, key, value, concurrent_memtable_writes_,
               get_post_process_info(mem));
    } else if (moptions->inplace_callback == nullptr) {
      assert(!concurrent_memtable_writes_);
      mem->Update(sequence_, key, value);
      QUERY_COUNT(CountPoint::NUMBER_KEYS_UPDATE);
    } else {
      assert(!concurrent_memtable_writes_);
      if (mem->UpdateCallback(sequence_, key, value)) {
      } else {
        // key not found in memtable. Do sst get, update, add
        SnapshotImpl read_from_snapshot;
        read_from_snapshot.number_ = sequence_;
        ReadOptions ropts;
        ropts.snapshot = &read_from_snapshot;

        std::string prev_value;
        std::string merged_value;

        auto cf_handle = cf_mems_->GetColumnFamilyHandle();
        common::Status s = common::Status::NotSupported();
        if (db_ != nullptr && recovering_log_number_ == 0) {
          if (cf_handle == nullptr) {
            cf_handle = db_->DefaultColumnFamily();
          }
          s = db_->Get(ropts, cf_handle, key, &prev_value);
        }

        char* prev_buffer = const_cast<char*>(prev_value.c_str());
        uint32_t prev_size = static_cast<uint32_t>(prev_value.size());
        auto status = moptions->inplace_callback(s.ok() ? prev_buffer : nullptr,
                                                 s.ok() ? &prev_size : nullptr,
                                                 value, &merged_value);
        if (status == UpdateStatus::UPDATED_INPLACE) {
          // prev_value is updated in-place with final value.
          mem->Add(sequence_, kTypeValue, key,
                   common::Slice(prev_buffer, prev_size));
          QUERY_COUNT(CountPoint::NUMBER_KEYS_WRITTEN);
        } else if (status == UpdateStatus::UPDATED) {
          // merged_value contains the final value.
          mem->Add(sequence_, kTypeValue, key, common::Slice(merged_value));
          QUERY_COUNT(CountPoint::NUMBER_KEYS_WRITTEN);
        }
      }
    }
    // Since all Puts are logged in trasaction logs (if enabled), always bump
    // sequence number. Even if the update eventually fails and does not result
    // in memtable add/update.
    sequence_++;
    CheckMemtableFull();
    return common::Status::OK();
  }

  common::Status DeleteImpl(uint32_t column_family_id, const common::Slice& key,
                            const common::Slice& value, ValueType delete_type) {
    MemTable* mem = cf_mems_->GetMemTable();
    mem->Add(sequence_, delete_type, key, value, concurrent_memtable_writes_,
             get_post_process_info(mem));
    sequence_++;
    CheckMemtableFull();
    return common::Status::OK();
  }

  virtual common::Status DeleteCF(uint32_t column_family_id,
                                  const common::Slice& key) override {
    if (recovering_log_number_ != 0 && !uncommit_transaction_list_.empty()) {
      return common::Status::OK();
    }
    if (rebuilding_trx_ != nullptr) {
      WriteBatchInternal::Delete(rebuilding_trx_, column_family_id, key);
      return common::Status::OK();
    }

    common::Status seek_status;
    if (!SeekToColumnFamily(column_family_id, &seek_status)) {
      ++sequence_;
      return seek_status;
    }
    return DeleteImpl(column_family_id, key, common::Slice(), kTypeDeletion);
  }

  virtual common::Status SingleDeleteCF(uint32_t column_family_id,
                                        const common::Slice& key) override {
    if (recovering_log_number_ != 0 && !uncommit_transaction_list_.empty()) {
      return common::Status::OK();
    }
    if (rebuilding_trx_ != nullptr) {
      WriteBatchInternal::SingleDelete(rebuilding_trx_, column_family_id, key);
      return common::Status::OK();
    }

    common::Status seek_status;
    if (!SeekToColumnFamily(column_family_id, &seek_status)) {
      ++sequence_;
      return seek_status;
    }
    return DeleteImpl(column_family_id, key, common::Slice(),
                      kTypeSingleDeletion);
  }

  virtual common::Status DeleteRangeCF(uint32_t column_family_id,
                                       const common::Slice& begin_key,
                                       const common::Slice& end_key) override {
    if (recovering_log_number_ != 0 && !uncommit_transaction_list_.empty()) {
      return common::Status::OK();
    }
    if (rebuilding_trx_ != nullptr) {
      WriteBatchInternal::DeleteRange(rebuilding_trx_, column_family_id,
                                      begin_key, end_key);
      return common::Status::OK();
    }

    common::Status seek_status;
    if (!SeekToColumnFamily(column_family_id, &seek_status)) {
      ++sequence_;
      return seek_status;
    }
    if (db_ != nullptr) {
      auto cf_handle = cf_mems_->GetColumnFamilyHandle();
      if (cf_handle == nullptr) {
        cf_handle = db_->DefaultColumnFamily();
      }
      auto* cfd = reinterpret_cast<ColumnFamilyHandleImpl*>(cf_handle)->cfd();
      if (!cfd->is_delete_range_supported()) {
        return common::Status::NotSupported(
            std::string("DeleteRange not supported for table type ") +
            cfd->ioptions()->table_factory->Name() + " in CF " +
            cfd->GetName());
      }
    }

    return DeleteImpl(column_family_id, begin_key, end_key, kTypeRangeDeletion);
  }

  virtual common::Status MergeCF(uint32_t column_family_id,
                                 const common::Slice& key,
                                 const common::Slice& value) override {
    assert(!concurrent_memtable_writes_);
    if (recovering_log_number_ != 0 && !uncommit_transaction_list_.empty()) {
      return common::Status::OK();
    }
    if (rebuilding_trx_ != nullptr) {
      WriteBatchInternal::Merge(rebuilding_trx_, column_family_id, key, value);
      return common::Status::OK();
    }

    common::Status seek_status;
    if (!SeekToColumnFamily(column_family_id, &seek_status)) {
      ++sequence_;
      return seek_status;
    }

    MemTable* mem = cf_mems_->GetMemTable();
    auto* moptions = mem->GetMemTableOptions();
    bool perform_merge = false;

    // If we pass DB through and options.max_successive_merges is hit
    // during recovery, Get() will be issued which will try to acquire
    // DB mutex and cause deadlock, as DB mutex is already held.
    // So we disable merge in recovery
    if (moptions->max_successive_merges > 0 && db_ != nullptr &&
        recovering_log_number_ == 0) {
      LookupKey lkey(key, sequence_);

      // Count the number of successive merges at the head
      // of the key in the memtable
      size_t num_merges = mem->CountSuccessiveMergeEntries(lkey);

      if (num_merges >= moptions->max_successive_merges) {
        perform_merge = true;
      }
    }

    if (perform_merge) {
      // 1) Get the existing value
      std::string get_value;

      // Pass in the sequence number so that we also include previous merge
      // operations in the same batch.
      SnapshotImpl read_from_snapshot;
      read_from_snapshot.number_ = sequence_;
      ReadOptions read_options;
      read_options.snapshot = &read_from_snapshot;

      auto cf_handle = cf_mems_->GetColumnFamilyHandle();
      if (cf_handle == nullptr) {
        cf_handle = db_->DefaultColumnFamily();
      }
      db_->Get(read_options, cf_handle, key, &get_value);
//      common::Slice get_value_slice = common::Slice(get_value);

      // 2) Apply this merge
      auto merge_operator = moptions->merge_operator;
      assert(merge_operator);

      std::string new_value;
      common::Status merge_status = common::Status::OK();
          /*MergeHelper::TimedFullMerge(
          merge_operator, key, &get_value_slice, {value}, &new_value,
          moptions->info_log, moptions->statistics, Env::Default());*/

      if (!merge_status.ok()) {
        // Failed to merge!
        // Store the delta in memtable
        perform_merge = false;
      } else {
        // 3) Add value to memtable
        mem->Add(sequence_, kTypeValue, key, new_value);
      }
    }

    if (!perform_merge) {
      // Add merge operator to memtable
      mem->Add(sequence_, kTypeMerge, key, value);
    }

    sequence_++;
    CheckMemtableFull();
    return common::Status::OK();
  }

  void CheckMemtableFull() {
    if (flush_scheduler_ != nullptr) {
      auto* cfd = cf_mems_->current();
      assert(cfd != nullptr);
      if (cfd->mem()->ShouldScheduleFlush() &&
          cfd->mem()->MarkFlushScheduled()) {
        // MarkFlushScheduled only returns true if we are the one that
        // should take action, so no need to dedup further
        flush_scheduler_->ScheduleFlush(cfd);
      }
    }
  }

  common::Status MarkBeginPrepare() override {
    assert(rebuilding_trx_ == nullptr);
    assert(db_);
    if (recovering_log_number_ != 0) {
      // during recovery we rebuild a hollow transaction
      // from all encountered prepare sections of the wal
      if (db_->allow_2pc() == false) {
        return common::Status::NotSupported(
            "WAL contains prepared transactions. Open with "
            "TransactionDB::Open().");
      }

      if (!uncommit_transaction_list_.empty()) {
        // fix bug #32047980 can't replay prepare log if there are uncommited
        // transactions
        return common::Status::OK();
      }

      // we are now iterating through a prepared section
      rebuilding_trx_ = MOD_NEW_OBJECT(memory::ModId::kWriteBatch, WriteBatch);
      if (has_valid_writes_ != nullptr) {
        *has_valid_writes_ = true;
      }
    } else {
      // in non-recovery we ignore prepare markers
      // and insert the values directly. making sure we have a
      // log for each insertion to reference.
      assert(log_number_ref_ > 0);
    }

    return common::Status::OK();
  }

  common::Status MarkEndPrepare(const common::Slice& name, common::SequenceNumber prepare_seq) override {
    assert(db_);
    common::Status s;
    if (recovering_log_number_ != 0 && !uncommit_transaction_list_.empty()) {
      // fix bug #32047980 can't replay prepare log if there are uncommited
      // transactions
      return s;
    }
    assert((rebuilding_trx_ != nullptr) == (recovering_log_number_ != 0));
    if (recovering_log_number_ != 0) {
      assert(db_->allow_2pc());
      auto transaction_write_batch = rebuilding_trx_;
      rebuilding_trx_ = nullptr;
      std::string recovered_transaction_name = name.ToString();
      if (prepare_seq != 0) {
        recovered_transaction_name.append(std::to_string(prepare_seq));
      }
      db::DBImpl::RecoveredTransaction *transaction = MOD_NEW_OBJECT(memory::ModId::kRecoveredTransaction,
                db::DBImpl::RecoveredTransaction, name.ToString(), util::Transaction::PREPARED,
                recovering_log_number_, prepare_seq);
      SequenceNumber commit_sequence = 0;
      db::DBImpl::CommitWriteBatch *commit_write_batch = nullptr;
      bool help_commit = false;
      db_->lock_recovered_transaction_mutex();
      if (!db_->insert_recovered_transaction(recovered_transaction_name, transaction, true)) {
        MOD_DELETE_OBJECT(RecoveredTransaction, transaction);
        transaction = db_->get_recovered_transaction(recovered_transaction_name);
        assert(transaction);
        transaction->prepare_log_num_ = recovering_log_number_;
        transaction->batch_ = transaction_write_batch;
        // insert failed, the commit/rollback log of this transaction has been
        // replayed, so we just help commit/rollback this transaction
        if (transaction->state_ == util::Transaction::COMMITED) {
          db_->MarkLogAsContainingPrepSection(recovering_log_number_);
          // bugfix: #31730974
          // check if we can help commit current transaction now
          commit_sequence = transaction->commit_seq_;
          commit_write_batch = db_->get_commit_write_batch(commit_sequence);
          if (commit_write_batch == nullptr) {
            // can't get global commit_transaction_list by commit_sequence,
            // transaction will by committed by other replay threads during
            // preparing or final committing, do nothing here
          } else {
            help_commit = true;
            for (auto transaction_name : commit_write_batch->uncommit_transaction_list_) {
              auto recovered_transaction = db_->get_recovered_transaction(transaction_name);
              if (recovered_transaction == nullptr || recovered_transaction->batch_ == nullptr) {
                // there are unprepared transactions in the same commit_write_batch,
                // cannot help commit
                help_commit = false;
                break;
              }
            }
            if (help_commit) {
              // remove commit_write_batch from global commit_transaction_groups
              // to avoid duplicate commit
              db_->remove_commit_transaction_groups(commit_sequence);
            }
          }
        } else if (transaction->state_ == util::Transaction::ROLLEDBACK) {
          // current transaction is rollback, just remove it and do nothing
          db_->delete_recovered_transaction(recovered_transaction_name, false);
        } else {
          db_->delete_recovered_transaction(recovered_transaction_name, false);
          s = Status::Corruption("wrong state of recovered transaction for prepare, name:" + name.ToString(true));
        }
      } else {
        // current transaction is prepared without commit or rollback logs replayed
        // just update prepare info, transaction will be committed or rollbacked later
        transaction = db_->get_recovered_transaction(recovered_transaction_name);
        transaction->prepare_log_num_ = recovering_log_number_;
        transaction->batch_ = transaction_write_batch;
      }
      db_->unlock_recovered_transaction_mutex();
      if (help_commit) {
        // all transactions in the same commit_write_batch are prepared now
        // help commit them
        assert(commit_write_batch != nullptr && commit_write_batch->batch_ != nullptr);
        uint64_t next_allocate_sequence;
        s = WriteBatchInternal::InsertInto(commit_write_batch->batch_,
            cf_mems_, flush_scheduler_, ignore_missing_column_families_,
            transaction->commit_log_num_, db_, concurrent_memtable_writes_,
            &next_allocate_sequence, has_valid_writes_, missing_subtable_during_recovery_);
        if (s.ok()) {
          int update_commit_sequence_ret =
            db_->update_max_sequence_and_log_number(next_allocate_sequence,
                                                    transaction->commit_log_num_);
          if (UNLIKELY(update_commit_sequence_ret != common::Status::kOk)) {
            XENGINE_LOG(WARN, "failed to update commit_sequence", K(sequence_),
                        K(prepare_seq), K(commit_sequence),
                        K(next_allocate_sequence), K(transaction->commit_log_num_),
                        K(name.ToString(true)), K(update_commit_sequence_ret));
            s = Status::Corruption("failed to update commit sequence");
          }
        }
        MOD_DELETE_OBJECT(WriteBatch, commit_write_batch->batch_);
        MOD_DELETE_OBJECT(CommitWriteBatch, commit_write_batch);
      }
    } else {
      assert(rebuilding_trx_ == nullptr);
      assert(log_number_ref_ > 0);
    }

    return s;
  }

  common::Status MarkCommit(const common::Slice& name, common::SequenceNumber prepare_seq) override {
    assert(db_);
    common::Status s;

    if (recovering_log_number_ != 0) {
      // in recovery when we encounter a commit marker
      // we lookup this transaction in our set of rebuilt transactions
      // and commit.
      std::string recovered_transaction_name = name.ToString();
      if (prepare_seq != 0) {
        recovered_transaction_name.append(std::to_string(prepare_seq));
      }

      db::DBImpl::RecoveredTransaction *transaction = MOD_NEW_OBJECT(memory::ModId::kRecoveredTransaction,
                db::DBImpl::RecoveredTransaction, name.ToString(), util::Transaction::COMMITED, 0, prepare_seq,
                recovering_log_number_, sequence_);
      db_->lock_recovered_transaction_mutex();
      if (db_->insert_recovered_transaction(recovered_transaction_name, transaction, false)) {
        db_->unlock_recovered_transaction_mutex();
        // insert success, the prepare log of this transaction hasn't been replayed
        // just return and wait for prepare
        // all uncommitted transactions in the same write_batch should be
        // inserted into uncommit_transaction_list_ and committed in order
        // in the future
        if (prepare_seq != 0) {
          uncommit_transaction_list_.emplace_back(recovered_transaction_name);
          s = Status::RecoverCommitLater();
        }
        return s;
      }
      // current transaction has been prepared before, now we can check if it's able to commit it
      auto trx = db_->get_recovered_transaction(recovered_transaction_name);
      assert(trx != nullptr);
      MOD_DELETE_OBJECT(RecoveredTransaction, transaction);
      if (trx->state_ != util::Transaction::PREPARED && trx->state_ != util::Transaction::COMMITED) {
        s = Status::Corruption("wrong state of recovered transaction for commit, name:" + name.ToString(true));
      } else if (trx->state_ == util::Transaction::PREPARED) {
        // update commit info and trx state
        trx->commit_log_num_ = recovering_log_number_;
        trx->commit_seq_ = sequence_;
        trx->state_ = util::Transaction::COMMITED;
      }
      db_->unlock_recovered_transaction_mutex();
      MOD_DELETE_OBJECT(RecoveredTransaction, transaction);
      if (!s.ok()) {
        return s;
      }
      if (uncommit_transaction_list_.empty()) {
        // bugfix: #31730974
        // there is no uncommitted transactions in the same write_batch
        // commit current trasaction now
        assert(log_number_ref_ == 0);
        // at this point individual CF lognumbers will prevent
        // duplicate re-insertion of values.
        // all insertes must reference this trx log number
        log_number_ref_ = trx->prepare_log_num_;
        s = trx->batch_->Iterate(this, recovering_log_number_ != 0 && concurrent_memtable_writes_);
        log_number_ref_ = 0;
        if (has_valid_writes_ != nullptr) {
          *has_valid_writes_ = true;
        }
        db_->lock_recovered_transaction_mutex();
        db_->delete_recovered_transaction(recovered_transaction_name, true);
        db_->unlock_recovered_transaction_mutex();
      } else {
		    // bugfix: #31730974
        // there is uncommitted transactions in the same write_batch
        // all transactions committed in the same write_batch should be replayed
        // by order, so just insert current transaction into uncommit_transaction_list_
        // and wait for later committing
        uncommit_transaction_list_.emplace_back(recovered_transaction_name);
        s = Status::RecoverCommitLater();
      }
    }
    // in non recovery we simply ignore this tag
    return s;
  }

  common::Status MarkRollback(const common::Slice& name, common::SequenceNumber prepare_seq) override {
    assert(db_);
    Status s;
    if (recovering_log_number_ != 0) {
      if (!uncommit_transaction_list_.empty()) {
        // fix bug #32047980 can't replay prepare log if there are uncommited
        // transactions
        return s;
      }
      std::string recovered_transaction_name = name.ToString();
      if (prepare_seq != 0) {
        recovered_transaction_name.append(std::to_string(prepare_seq));
      }
      db::DBImpl::RecoveredTransaction *transaction = MOD_NEW_OBJECT(memory::ModId::kRecoveredTransaction,
                db::DBImpl::RecoveredTransaction, name.ToString(), util::Transaction::ROLLEDBACK, 0, prepare_seq,
                recovering_log_number_, 0);
      db_->lock_recovered_transaction_mutex();
      if (db_->insert_recovered_transaction(recovered_transaction_name, transaction, false)) {
        db_->unlock_recovered_transaction_mutex();
        return s;
      } else {
        auto trx = db_->get_recovered_transaction(recovered_transaction_name);
        assert(trx != nullptr);
        // the log containing the transactions prep section
        // may have been released in the previous incarnation
        // because we knew it had been rolled back
        if (trx->state_ != util::Transaction::PREPARED) {
          s = Status::Corruption("wrong state of recovered transaction for rollback, name: " + name.ToString(true));
        } else {
          db_->delete_recovered_transaction(recovered_transaction_name, true);
        }
        db_->unlock_recovered_transaction_mutex();
        MOD_DELETE_OBJECT(RecoveredTransaction, transaction);
      }
    }
    // in non recovery we simply ignore this tag
    return s;
  }

private:
  MemTablePostProcessInfo* get_post_process_info(MemTable* mem) {
    if (!concurrent_memtable_writes_) {
      // No need to batch counters locally if we don't use concurrent mode.
      return nullptr;
    }
    return &GetPostMap()[mem];
  }
};

// This function can only be called in these conditions:
// 1) During Recovery()
// 2) During Write(), in a single-threaded write thread
// 3) During Write(), in a concurrent context where memtables has been cloned
// The reason is that it calls memtables->Seek(), which has a stateful cache
common::Status WriteBatchInternal::InsertInto(
    const autovector<WriteThread::Writer*>& writers, SequenceNumber sequence,
    ColumnFamilyMemTables* memtables, FlushScheduler* flush_scheduler,
    bool ignore_missing_column_families, uint64_t recovery_log_number, DB* db,
    bool concurrent_memtable_writes) {
  MemTableInserter inserter(sequence, memtables, flush_scheduler,
                            ignore_missing_column_families, recovery_log_number,
                            db, concurrent_memtable_writes);
  for (size_t i = 0; i < writers.size(); i++) {
    auto w = writers[i];
    if (!w->ShouldWriteToMemtable()) {
      continue;
    }
    inserter.set_log_number_ref(w->log_ref);
    w->status = w->batch->Iterate(&inserter);
    if (!w->status.ok()) {
      return w->status;
    }
  }
  return common::Status::OK();
}

common::Status WriteBatchInternal::InsertInto(
    WriteThread::Writer* writer, ColumnFamilyMemTables* memtables,
    FlushScheduler* flush_scheduler, bool ignore_missing_column_families,
    uint64_t log_number, DB* db, bool concurrent_memtable_writes) {
  MemTableInserter inserter(WriteBatchInternal::Sequence(writer->batch),
                            memtables, flush_scheduler,
                            ignore_missing_column_families, log_number, db,
                            concurrent_memtable_writes);
  assert(writer->ShouldWriteToMemtable());
  inserter.set_log_number_ref(writer->log_ref);
  common::Status s = writer->batch->Iterate(&inserter);
  if (concurrent_memtable_writes) {
    inserter.PostProcess();
  }
  return s;
}

common::Status WriteBatchInternal::InsertInto(
    db::WriteRequest* writer, SequenceNumber sequence,
    ColumnFamilyMemTables* memtables, FlushScheduler* flush_scheduler,
    bool ignore_missing_column_families, uint64_t recovery_log_number, DB* db,
    bool concurrent_memtable_writes) {
  MemTableInserter inserter(sequence, memtables, flush_scheduler,
                            ignore_missing_column_families, recovery_log_number,
                            db, concurrent_memtable_writes);
  assert(writer->should_write_to_memtable());
  inserter.set_log_number_ref(writer->wal_log_ref_);
  common::Status s = writer->batch_->Iterate(&inserter,
                        recovery_log_number != 0 && concurrent_memtable_writes);
  return s;
}

common::Status WriteBatchInternal::InsertInto(
    db::WriteRequest* writer, ColumnFamilyMemTables* memtables,
    FlushScheduler* flush_scheduler, bool ignore_missing_column_families,
    uint64_t log_number, DB* db, bool concurrent_memtable_writes) {
  MemTableInserter inserter(WriteBatchInternal::Sequence(writer->batch_),
                            memtables, flush_scheduler,
                            ignore_missing_column_families, log_number, db,
                            concurrent_memtable_writes);
  assert(writer->should_write_to_memtable());
  inserter.set_log_number_ref(writer->wal_log_ref_);
  common::Status s = writer->batch_->Iterate(&inserter);
  if (concurrent_memtable_writes) {
    inserter.PostProcess();
  }
  return s;
}

common::Status WriteBatchInternal::InsertInto(
    const WriteBatch* batch, ColumnFamilyMemTables* memtables,
    FlushScheduler* flush_scheduler, bool ignore_missing_column_families,
    uint64_t log_number, DB* db, bool concurrent_memtable_writes,
    SequenceNumber* last_seq_used, bool* has_valid_writes,
    std::unordered_map<int64_t, int64_t> *missing_subtable_during_recovery) {
  MemTableInserter inserter(WriteBatchInternal::Sequence(batch), memtables,
                            flush_scheduler, ignore_missing_column_families,
                            log_number, db, concurrent_memtable_writes,
                            has_valid_writes, missing_subtable_during_recovery);
  common::Status s = batch->Iterate(&inserter, log_number != 0 && concurrent_memtable_writes);
  if (!s.ok()) {
    XENGINE_LOG(WARN, "insert into memtable failed", "sequence", WriteBatchInternal::Sequence(batch),
                K(log_number), "error msg", s.ToString());
  } else {
    s = inserter.recovery_final_commit(batch);
  }
  if (concurrent_memtable_writes) {
    inserter.PostProcess();
  }
  if (last_seq_used != nullptr) {
    *last_seq_used = inserter.get_final_sequence();
  }
  return s;
}

common::Status WriteBatchInternal::SetContents(WriteBatch* b,
                                               const common::Slice& contents) {
  assert(contents.size() >= WriteBatchInternal::kHeader);
  b->rep_.assign(contents.data(), contents.size());
  b->content_flags_.store(ContentFlags::DEFERRED, std::memory_order_relaxed);
  return common::Status::OK();
}

common::Status WriteBatchInternal::Append(WriteBatch* dst,
                                          const WriteBatch* src,
                                          const bool wal_only) {
  size_t src_len;
  int src_count;
  uint32_t src_flags;

  const SavePoint& batch_end = src->GetWalTerminationPoint();

  if (wal_only && !batch_end.is_cleared()) {
    src_len = batch_end.size - WriteBatchInternal::kHeader;
    src_count = batch_end.count;
    src_flags = batch_end.content_flags;
  } else {
    src_len = src->rep_.size() - WriteBatchInternal::kHeader;
    src_count = Count(src);
    src_flags = src->content_flags_.load(std::memory_order_relaxed);
  }

  SetCount(dst, Count(dst) + src_count);
  assert(src->rep_.size() >= WriteBatchInternal::kHeader);
  dst->rep_.append(src->rep_.data() + WriteBatchInternal::kHeader, src_len);
  dst->content_flags_.store(
      dst->content_flags_.load(std::memory_order_relaxed) | src_flags,
      std::memory_order_relaxed);
  return common::Status::OK();
}

size_t WriteBatchInternal::AppendedByteSize(size_t leftByteSize,
                                            size_t rightByteSize) {
  if (leftByteSize == 0 || rightByteSize == 0) {
    return leftByteSize + rightByteSize;
  } else {
    return leftByteSize + rightByteSize - WriteBatchInternal::kHeader;
  }
}
}  // namespace db
}  // namespace xengine
