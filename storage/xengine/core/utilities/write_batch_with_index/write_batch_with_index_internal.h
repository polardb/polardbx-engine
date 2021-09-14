// Portions Copyright (c) 2020, Alibaba Group Holding Limited
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#ifndef ROCKSDB_LITE

#include <limits>
#include <string>
#include <vector>

#include "options/db_options.h"
#include "port/port.h"
#include "xengine/comparator.h"
#include "xengine/iterator.h"
#include "xengine/slice.h"
#include "xengine/status.h"
#include "xengine/utilities/write_batch_with_index.h"

namespace xengine {
namespace db {
class MergeContext;
}

namespace util {

struct Options;

// Key used by skip list, as the binary searchable index of WriteBatchWithIndex.
struct WriteBatchIndexEntry {
  WriteBatchIndexEntry(size_t o, uint32_t c, size_t ko, size_t ksz)
      : offset(o),
        column_family(c),
        key_offset(ko),
        key_size(ksz),
        search_key(nullptr) {}
  WriteBatchIndexEntry(const common::Slice* sk, uint32_t c)
      : offset(0),
        column_family(c),
        key_offset(0),
        key_size(0),
        search_key(sk) {}

  // If this flag appears in the offset, it indicates a key that is smaller
  // than any other entry for the same column family
  static const size_t kFlagMin = port::kMaxSizet;

  size_t offset;           // offset of an entry in write batch's string buffer.
  uint32_t column_family;  // column family of the entry.
  size_t key_offset;       // offset of the key in write batch's string buffer.
  size_t key_size;         // size of the key.

  const common::Slice* search_key;  // if not null, instead of reading keys from
  // write batch, use it to compare. This is used
  // for lookup key.
};

class ReadableWriteBatch : public db::WriteBatch {
 public:
  explicit ReadableWriteBatch(size_t reserved_bytes = 0, size_t max_bytes = 0)
      : db::WriteBatch(reserved_bytes, max_bytes) {}
  // Retrieve some information from a write entry in the write batch, given
  // the start offset of the write entry.
  common::Status GetEntryFromDataOffset(size_t data_offset, WriteType* type,
                                        common::Slice* Key,
                                        common::Slice* value,
                                        common::Slice* blob,
                                        common::Slice* xid) const;
};

class WriteBatchEntryComparator {
 public:
  WriteBatchEntryComparator(const util::Comparator* _default_comparator,
                            const ReadableWriteBatch* write_batch)
      : default_comparator_(_default_comparator), write_batch_(write_batch) {}
  // Compare a and b. Return a negative value if a is less than b, 0 if they
  // are equal, and a positive value if a is greater than b
  int operator()(const WriteBatchIndexEntry* entry1,
                 const WriteBatchIndexEntry* entry2) const;

  int CompareKey(uint32_t column_family, const common::Slice& key1,
                 const common::Slice& key2) const;

  void SetComparatorForCF(uint32_t column_family_id,
                          const util::Comparator* comparator) {
    cf_comparators_map_[column_family_id] = comparator;
  }

  const util::Comparator* default_comparator() { return default_comparator_; }

 private:
  const util::Comparator* default_comparator_;
  std::unordered_map<uint32_t, const util::Comparator*> cf_comparators_map_;
  const ReadableWriteBatch* write_batch_;
};

class WriteBatchWithIndexInternal {
 public:
  enum Result { kFound, kDeleted, kNotFound, kMergeInProgress, kError };

  // If batch contains a value for key, store it in *value and return kFound.
  // If batch contains a deletion for key, return Deleted.
  // If batch contains Merge operations as the most recent entry for a key,
  //   and the merge process does not stop (not reaching a value or delete),
  //   prepend the current merge operands to *operands,
  //   and return kMergeInProgress
  // If batch does not contain this key, return kNotFound
  // Else, return kError on error with error Status stored in *s.
  static WriteBatchWithIndexInternal::Result GetFromBatch(
      const common::ImmutableDBOptions& ioptions, WriteBatchWithIndex* batch,
      db::ColumnFamilyHandle* column_family, const common::Slice& key,
      db::MergeContext* merge_context, WriteBatchEntryComparator* cmp,
      std::string* value, bool overwrite_key, common::Status* s);
};

}  //  namespace util
}  //  namespace xengine
#endif  // !ROCKSDB_LITE
