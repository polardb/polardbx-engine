/*
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// A WriteBatchWithIndex with a binary searchable index built for all the keys
// inserted.
#pragma once

#ifndef ROCKSDB_LITE

#include <memory>
#include <string>

#include "xengine/comparator.h"
#include "xengine/iterator.h"
#include "xengine/slice.h"
#include "xengine/status.h"
#include "xengine/write_batch.h"
#include "xengine/write_batch_base.h"

namespace xengine {

namespace db {
class DB;
class ColumnFamilyHandle;
class Iterator;

}
namespace common {
struct ReadOptions;
struct DBOptions;
}

namespace util {
class Comparator;
enum WriteType {
  kPutRecord,
  kMergeRecord,
  kDeleteRecord,
  kSingleDeleteRecord,
  kDeleteRangeRecord,
  kLogDataRecord,
  kXIDRecord,
};

// an entry for Put, Merge, Delete, or SingleDelete entry for write batches.
// Used in WBWIIterator.
struct WriteEntry {
  WriteType type;
  common::Slice key;
  common::Slice value;
};

// Iterator of one column family out of a WriteBatchWithIndex.
class WBWIIterator {
 public:
  virtual ~WBWIIterator() {}

  virtual bool Valid() const = 0;

  virtual void SeekToFirst() = 0;

  virtual void SeekToLast() = 0;

  virtual void Seek(const common::Slice& key) = 0;

  virtual void SeekForPrev(const common::Slice& key) = 0;

  virtual void Next() = 0;

  virtual void Prev() = 0;

  // the return WriteEntry is only valid until the next mutation of
  // WriteBatchWithIndex
  virtual WriteEntry Entry() const = 0;

  virtual common::Status status() const = 0;

  virtual void set_end_key(const common::Slice& end_key, const Comparator* comparator)
  {
    end_key_ = end_key;
    comparator_ = comparator;
  }

protected:
  common::Slice end_key_;
  const Comparator* comparator_ = nullptr;
};

// A WriteBatchWithIndex with a binary searchable index built for all the keys
// inserted.
// In Put(), Merge() Delete(), or SingleDelete(), the same function of the
// wrapped will be called. At the same time, indexes will be built.
// By calling GetWriteBatch(), a user will get the WriteBatch for the data
// they inserted, which can be used for DB::Write().
// A user can call NewIterator() to create an iterator.
class WriteBatchWithIndex : public db::WriteBatchBase {
 public:
  // backup_index_comparator: the backup comparator used to compare keys
  // within the same column family, if column family is not given in the
  // interface, or we can't find a column family from the column family handle
  // passed in, backup_index_comparator will be used for the column family.
  // reserved_bytes: reserved bytes in underlying WriteBatch
  // max_bytes: maximum size of underlying WriteBatch in bytes
  // overwrite_key: if true, overwrite the key in the index when inserting
  //                the same key as previously, so iterator will never
  //                show two entries with the same key.
  explicit WriteBatchWithIndex(
      const Comparator* backup_index_comparator = BytewiseComparator(),
      size_t reserved_bytes = 0, bool overwrite_key = false,
      size_t max_bytes = 0);

  virtual ~WriteBatchWithIndex();

  using xengine::db::WriteBatchBase::Put;
  common::Status Put(db::ColumnFamilyHandle* column_family,
                     const common::Slice& key,
                     const common::Slice& value) override;

  common::Status Put(const common::Slice& key,
                     const common::Slice& value) override;

  using xengine::db::WriteBatchBase::Merge;
  common::Status Merge(db::ColumnFamilyHandle* column_family,
                       const common::Slice& key,
                       const common::Slice& value) override;

  common::Status Merge(const common::Slice& key,
                       const common::Slice& value) override;

  using xengine::db::WriteBatchBase::Delete;
  common::Status Delete(db::ColumnFamilyHandle* column_family,
                        const common::Slice& key) override;
  common::Status Delete(const common::Slice& key) override;

  using xengine::db::WriteBatchBase::SingleDelete;
  common::Status SingleDelete(db::ColumnFamilyHandle* column_family,
                              const common::Slice& key) override;
  common::Status SingleDelete(const common::Slice& key) override;

  using xengine::db::WriteBatchBase::DeleteRange;
  common::Status DeleteRange(db::ColumnFamilyHandle* column_family,
                             const common::Slice& begin_key,
                             const common::Slice& end_key) override;
  common::Status DeleteRange(const common::Slice& begin_key,
                             const common::Slice& end_key) override;

  using xengine::db::WriteBatchBase::PutLogData;
  common::Status PutLogData(const common::Slice& blob) override;

  using xengine::db::WriteBatchBase::Clear;
  void Clear() override;

  using xengine::db::WriteBatchBase::GetWriteBatch;
  db::WriteBatch* GetWriteBatch() override;

  // Create an iterator of a column family. User can call iterator.Seek() to
  // search to the next entry of or after a key. Keys will be iterated in the
  // order given by index_comparator. For multiple updates on the same key,
  // each update will be returned as a separate entry, in the order of update
  // time.
  //
  // The returned iterator should be deleted by the caller.
  WBWIIterator* NewIterator(db::ColumnFamilyHandle* column_family);
  // Create an iterator of the default column family.
  WBWIIterator* NewIterator();

  // Will create a new Iterator that will use WBWIIterator as a delta and
  // base_iterator as base.
  //
  // This function is only supported if the WriteBatchWithIndex was
  // constructed with overwrite_key=true.
  //
  // The returned iterator should be deleted by the caller.
  // The base_iterator is now 'owned' by the returned iterator. Deleting the
  // returned iterator will also delete the base_iterator.
  db::Iterator* NewIteratorWithBase(db::ColumnFamilyHandle* column_family,
                                    db::Iterator* base_iterator);
  // default column family
  db::Iterator* NewIteratorWithBase(db::Iterator* base_iterator);

  // Similar to DB::Get() but will only read the key from this batch.
  // If the batch does not have enough data to resolve Merge operations,
  // MergeInProgress status may be returned.
  common::Status GetFromBatch(db::ColumnFamilyHandle* column_family,
                              const common::DBOptions& options,
                              const common::Slice& key, std::string* value);

  // Similar to previous function but does not require a column_family.
  // Note:  An InvalidArgument status will be returned if there are any Merge
  // operators for this key.  Use previous method instead.
  common::Status GetFromBatch(const common::DBOptions& options,
                              const common::Slice& key, std::string* value) {
    return GetFromBatch(nullptr, options, key, value);
  }

  // Similar to DB::Get() but will also read writes from this batch.
  //
  // This function will query both this batch and the DB and then merge
  // the results using the DB's merge operator (if the batch contains any
  // merge requests).
  //
  // Setting read_options.snapshot will affect what is read from the DB
  // but will NOT change which keys are read from the batch (the keys in
  // this batch do not yet belong to any snapshot and will be fetched
  // regardless).
  common::Status GetFromBatchAndDB(db::DB* db,
                                   const common::ReadOptions& read_options,
                                   const common::Slice& key,
                                   std::string* value);
  common::Status GetFromBatchAndDB(db::DB* db,
                                   const common::ReadOptions& read_options,
                                   db::ColumnFamilyHandle* column_family,
                                   const common::Slice& key,
                                   std::string* value);

  // Records the state of the batch for future calls to RollbackToSavePoint().
  // May be called multiple times to set multiple save points.
  void SetSavePoint() override;

  // Remove all entries in this batch (Put, Merge, Delete, SingleDelete,
  // PutLogData) since the most recent call to SetSavePoint() and removes the
  // most recent save point.
  // If there is no previous call to SetSavePoint(), behaves the same as
  // Clear().
  //
  // Calling RollbackToSavePoint invalidates any open iterators on this batch.
  //
  // Returns common::Status::OK() on success,
  //         common::Status::NotFound() if no previous call to SetSavePoint(),
  //         or other common::Status on corruption.
  common::Status RollbackToSavePoint() override;

  void SetMaxBytes(size_t max_bytes) override;

 private:
  struct Rep;
  std::unique_ptr<Rep> rep;
};

// when direction == forward
// * current_at_base_ <=> base_iterator > delta_iterator
// when direction == backwards
// * current_at_base_ <=> base_iterator < delta_iterator
// always:
// * equal_keys_ <=> base_iterator == delta_iterator
class BaseDeltaIterator : public db::Iterator {
 public:
  BaseDeltaIterator(db::Iterator* base_iterator, WBWIIterator* delta_iterator,
                    const Comparator* comparator)
      : forward_(true),
        current_at_base_(true),
        equal_keys_(false),
        status_(common::Status::OK()),
        base_iterator_(base_iterator),
        delta_iterator_(delta_iterator),
        comparator_(comparator),
        delta_valid_(true) {}

  virtual ~BaseDeltaIterator() {}

  bool Valid() const override {
    return current_at_base_ ? BaseValid() : DeltaValid();
  }

  //for commit_in_the_middle
  void InvalidDelta() {
    delta_valid_ = false;
  }

  void SeekToFirst() override {
    forward_ = true;
    base_iterator_->SeekToFirst();
    delta_iterator_->SeekToFirst();
    UpdateCurrent();
  }

  void SeekToLast() override {
    forward_ = false;
    base_iterator_->SeekToLast();
    delta_iterator_->SeekToLast();
    UpdateCurrent();
  }

  void Seek(const common::Slice& k) override {
    forward_ = true;
    base_iterator_->Seek(k);
    delta_iterator_->Seek(k);
    UpdateCurrent();
  }

  void SeekForPrev(const common::Slice& k) override {
    forward_ = false;
    base_iterator_->SeekForPrev(k);
    delta_iterator_->SeekForPrev(k);
    UpdateCurrent();
  }

  void Next() override {
    if (!forward_) {
      // Need to change direction
      // if our direction was backward and we're not equal, we have two states:
      // * both iterators are valid: we're already in a good state (current
      // shows to smaller)
      // * only one iterator is valid: we need to advance that iterator
      forward_ = true;
      equal_keys_ = false;
      if (!BaseValid()) {
        //for commit in the middle
        //assert(DeltaValid());
        base_iterator_->SeekToFirst();
      } else if (!DeltaValid()) {
        delta_iterator_->SeekToFirst();
      } else if (current_at_base_) {
        // Change delta from larger than base to smaller
        AdvanceDelta();
      } else {
        // Change base from larger than delta to smaller
        AdvanceBase();
      }
      if (DeltaValid() && BaseValid()) {
        if (comparator_->Equal(delta_iterator_->Entry().key,
                               base_iterator_->key())) {
          equal_keys_ = true;
        }
      }
    }
    Advance();
  }

  void Prev() override {
    if (!Valid()) {
      status_ = common::Status::NotSupported("Prev() on invalid iterator");
    }

    if (forward_) {
      // Need to change direction
      // if our direction was backward and we're not equal, we have two states:
      // * both iterators are valid: we're already in a good state (current
      // shows to smaller)
      // * only one iterator is valid: we need to advance that iterator
      forward_ = false;
      equal_keys_ = false;
      if (!BaseValid()) {
        //for commit in the middle
        //assert(DeltaValid());
        base_iterator_->SeekToLast();
      } else if (!DeltaValid()) {
        delta_iterator_->SeekToLast();
      } else if (current_at_base_) {
        // Change delta from less advanced than base to more advanced
        AdvanceDelta();
      } else {
        // Change base from less advanced than delta to more advanced
        AdvanceBase();
      }
      if (DeltaValid() && BaseValid()) {
        if (comparator_->Equal(delta_iterator_->Entry().key,
                               base_iterator_->key())) {
          equal_keys_ = true;
        }
      }
    }

    Advance();
  }

  common::Slice key() const override {
    return current_at_base_ ? base_iterator_->key()
                            : delta_iterator_->Entry().key;
  }

  common::Slice value() const override {
    return current_at_base_ ? base_iterator_->value()
                            : delta_iterator_->Entry().value;
  }

  common::Status status() const override {
    if (!status_.ok()) {
      return status_;
    }
    if (!base_iterator_->status().ok()) {
      return base_iterator_->status();
    }
    return delta_iterator_->status();
  }

  virtual int set_end_key(const common::Slice& end_key_slice) override;

 protected:
  void AssertInvariants() {
#ifndef NDEBUG
    if (!Valid()) {
      return;
    }
    if (!BaseValid()) {
      assert(!current_at_base_ && delta_iterator_->Valid());
      return;
    }
    if (!DeltaValid()) {
      assert(current_at_base_ && base_iterator_->Valid());
      return;
    }
    // we don't support those yet
    assert(delta_iterator_->Entry().type != kMergeRecord &&
           delta_iterator_->Entry().type != kLogDataRecord);
    int compare = comparator_->Compare(delta_iterator_->Entry().key,
                                       base_iterator_->key());
    if (forward_) {
      // current_at_base -> compare < 0
      assert(!current_at_base_ || compare < 0);
      // !current_at_base -> compare <= 0
      assert(current_at_base_ && compare >= 0);
    } else {
      // current_at_base -> compare > 0
      assert(!current_at_base_ || compare > 0);
      // !current_at_base -> compare <= 0
      assert(current_at_base_ && compare <= 0);
    }
    // equal_keys_ <=> compare == 0
    assert((equal_keys_ || compare != 0) && (!equal_keys_ || compare == 0));
#endif
  }

  void Advance() {
    if (equal_keys_) {
      //assert(BaseValid() && DeltaValid());
      //for commit_in_the_middle
      assert(BaseValid());
      AdvanceBase();
      AdvanceDelta();
    } else {
      if (current_at_base_) {
        assert(BaseValid());
        AdvanceBase();
      } else {
        //assert(BaseValid() && DeltaValid());
        //for commit_in_the_middle
        //assert(DeltaValid());
        AdvanceDelta();
      }
    }
    UpdateCurrent();
  }

  void AdvanceDelta() {
    //if delta_valid_ return directly.
    if (!delta_valid_) {
      return;
    }

    if (forward_) {
      delta_iterator_->Next();
    } else {
      delta_iterator_->Prev();
    }
  }
  void AdvanceBase() {
    if (forward_) {
      base_iterator_->Next();
    } else {
      base_iterator_->Prev();
    }
  }
  bool BaseValid() const { return base_iterator_->Valid(); }
  bool DeltaValid() const { return delta_valid_ && delta_iterator_->Valid(); }
  void UpdateCurrent() {
// Suppress false positive clang analyzer warnings.
#ifndef __clang_analyzer__
    while (true) {
      WriteEntry delta_entry;
      if (DeltaValid()) {
        delta_entry = delta_iterator_->Entry();
      }
      equal_keys_ = false;
      if (!BaseValid()) {
        // Base has finished.
        if (!DeltaValid()) {
          // Finished
          return;
        }
        if (delta_entry.type == kDeleteRecord ||
            delta_entry.type == kSingleDeleteRecord) {
          AdvanceDelta();
        } else {
          current_at_base_ = false;
          return;
        }
      } else if (!DeltaValid()) {
        // Delta has finished.
        current_at_base_ = true;
        return;
      } else {
        int compare =
            (forward_ ? 1 : -1) *
            comparator_->Compare(delta_entry.key, base_iterator_->key());
        if (compare <= 0) {  // delta bigger or equal
          if (compare == 0) {
            equal_keys_ = true;
          }
          if (delta_entry.type != kDeleteRecord &&
              delta_entry.type != kSingleDeleteRecord) {
            current_at_base_ = false;
            return;
          }
          // Delta is less advanced and is delete.
          AdvanceDelta();
          if (equal_keys_) {
            AdvanceBase();
          }
        } else {
          current_at_base_ = true;
          return;
        }
      }
    }

    AssertInvariants();
#endif  // __clang_analyzer__
  }

  bool forward_;
  bool current_at_base_;
  bool equal_keys_;
  common::Status status_;
  std::unique_ptr<db::Iterator, memory::ptr_destruct_delete<db::Iterator>> base_iterator_;
  std::unique_ptr<WBWIIterator> delta_iterator_;
  const Comparator* comparator_;  // not owned
  bool delta_valid_; //indicate whether delta_iterator(write_batch) is Valid
};

class UniqueCheckBaseDeltaIterator: public BaseDeltaIterator {
public:
  UniqueCheckBaseDeltaIterator(db::Iterator* base_iterator,
      WBWIIterator* delta_iterator, const Comparator* comparator)
      : BaseDeltaIterator(base_iterator, delta_iterator, comparator)
  {}

  void Next() override {
    if (!current_at_base_) {
      assert(delta_iterator_->Valid());
      delta_iterator_->Next();
      current_at_base_ = !delta_iterator_->Valid();
    } else {
      assert(base_iterator_->Valid());
      base_iterator_->Next();
    }
  }

  void Seek(const common::Slice& key) override {
    forward_ = true;
    delta_iterator_->Seek(key);
    base_iterator_->Seek(key);

    current_at_base_ = !delta_iterator_->Valid();
  }

  common::SequenceNumber key_seq() const override
  {
    return current_at_base_ ? base_iterator_->key_seq() : 0;
  }

  db::Iterator::RecordStatus key_status() const override
  {
    if (!current_at_base_ && delta_iterator_->Valid()) {
      WriteEntry delta_entry;
      delta_entry = delta_iterator_->Entry();
      if (delta_entry.type == kDeleteRecord ||
          delta_entry.type == kSingleDeleteRecord) {
        return db::Iterator::kDeleted;
      } else {
        return db::Iterator::kExist;
      }
    } else if (current_at_base_ && base_iterator_->Valid()) {
      return base_iterator_->key_status();
    } else {
      return db::Iterator::kNonExist;
    }
  }

  bool is_at_base() { return current_at_base_; }
  void change_to_base() { current_at_base_ = true; }
};

}  // namespace util
}  // namespace xengine

#endif  // !ROCKSDB_LITE
