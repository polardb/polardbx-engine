// Portions Copyright (c) 2020, Alibaba Group Holding Limited
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#ifndef ROCKSDB_LITE
#include <stdint.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "db/dbformat.h"
#include "table/plain_table_factory.h"
#include "table/plain_table_index.h"
#include "table/table_reader.h"
#include "util/arena.h"
#include "util/dynamic_bloom.h"
#include "util/file_reader_writer.h"
#include "xengine/env.h"
#include "xengine/iterator.h"
#include "xengine/slice_transform.h"
#include "xengine/table.h"
#include "xengine/table_properties.h"

namespace xengine {

namespace db {
class InternalKeyComparator;
}

namespace common {
struct ReadOptions;
}

namespace table {

class TableCache;
class TableReader;
class PlainTableKeyDecoder;
class GetContext;
class InternalIterator;

using std::unordered_map;
using std::vector;
extern const uint32_t kPlainTableVariableLength;

struct PlainTableReaderFileInfo {
  bool is_mmap_mode;
  common::Slice file_data;
  uint32_t data_end_offset;
  std::unique_ptr<util::RandomAccessFileReader> file;

  PlainTableReaderFileInfo(
      std::unique_ptr<util::RandomAccessFileReader>&& _file,
      const util::EnvOptions& storage_options, uint32_t _data_size_offset)
      : is_mmap_mode(storage_options.use_mmap_reads),
        data_end_offset(_data_size_offset),
        file(std::move(_file)) {}
};

// Based on following output file format shown in plain_table_factory.h
// When opening the output file, IndexedTableReader creates a hash table
// from key prefixes to offset of the output file. IndexedTable will decide
// whether it points to the data offset of the first key with the key prefix
// or the offset of it. If there are too many keys share this prefix, it will
// create a binary search-able index from the suffix to offset on disk.
//
// The implementation of IndexedTableReader requires output file is mmaped
class PlainTableReader : public TableReader {
 public:
  static common::Status Open(
      const common::ImmutableCFOptions& ioptions,
      const util::EnvOptions& env_options,
      const db::InternalKeyComparator& internal_comparator,
      std::unique_ptr<util::RandomAccessFileReader>&& file, uint64_t file_size,
      std::unique_ptr<TableReader>* table, const int bloom_bits_per_key,
      double hash_table_ratio, size_t index_sparseness,
      size_t huge_page_tlb_size, bool full_scan_mode);

  InternalIterator* NewIterator(const common::ReadOptions&,
                                memory::SimpleAllocator* arena = nullptr,
                                bool skip_filters = false,
                                const uint64_t scan_add_blocks_limit = 0) override;

  void Prepare(const common::Slice& target) override;

  common::Status Get(const common::ReadOptions&, const common::Slice& key,
                     GetContext* get_context,
                     bool skip_filters = false) override;

  uint64_t ApproximateOffsetOf(const common::Slice& key) override;

  uint32_t GetIndexSize() const { return index_.GetIndexSize(); }
  void SetupForCompaction() override;

  std::shared_ptr<const TableProperties> GetTableProperties() const override {
    return table_properties_;
  }

  virtual size_t ApproximateMemoryUsage() const override {
    return arena_.MemoryAllocatedBytes();
  }

  PlainTableReader(const common::ImmutableCFOptions& ioptions,
                   std::unique_ptr<util::RandomAccessFileReader>&& file,
                   const util::EnvOptions& env_options,
                   const db::InternalKeyComparator& internal_comparator,
                   EncodingType encoding_type, uint64_t file_size,
                   const TableProperties* table_properties);
  virtual ~PlainTableReader();

 protected:
  // Check bloom filter to see whether it might contain this prefix.
  // The hash of the prefix is given, since it can be reused for index lookup
  // too.
  virtual bool MatchBloom(uint32_t hash) const;

  // PopulateIndex() builds index of keys. It must be called before any query
  // to the table.
  //
  // props: the table properties object that need to be stored. Ownership of
  //        the object will be passed.
  //

  common::Status PopulateIndex(TableProperties* props, int bloom_bits_per_key,
                               double hash_table_ratio, size_t index_sparseness,
                               size_t huge_page_tlb_size);

  common::Status MmapDataIfNeeded();

 private:
  const db::InternalKeyComparator internal_comparator_;
  EncodingType encoding_type_;
  // represents plain table's current status.
  common::Status status_;

  PlainTableIndex index_;
  bool full_scan_mode_;

  // data_start_offset_ and data_end_offset_ defines the range of the
  // sst file that stores data.
  const uint32_t data_start_offset_ = 0;
  const uint32_t user_key_len_;
  const common::SliceTransform* prefix_extractor_;

  static const size_t kNumInternalBytes = 8;

  // Bloom filter is used to rule out non-existent key
  bool enable_bloom_;
  util::DynamicBloom bloom_;
  PlainTableReaderFileInfo file_info_;
  util::Arena arena_;
  std::unique_ptr<char[], memory::ptr_delete<char>> index_block_alloc_;
  std::unique_ptr<char[], memory::ptr_delete<char>> bloom_block_alloc_;

  const common::ImmutableCFOptions& ioptions_;
  uint64_t file_size_;
  std::shared_ptr<const TableProperties> table_properties_;

  bool IsFixedLength() const {
    return user_key_len_ != kPlainTableVariableLength;
  }

  size_t GetFixedInternalKeyLength() const {
    return user_key_len_ + kNumInternalBytes;
  }

  common::Slice GetPrefix(const common::Slice& target) const {
    assert(target.size() >= 8);  // target is internal key
    return GetPrefixFromUserKey(GetUserKey(target));
  }

  common::Slice GetPrefix(const db::ParsedInternalKey& target) const {
    return GetPrefixFromUserKey(target.user_key);
  }

  common::Slice GetUserKey(const common::Slice& key) const {
    return common::Slice(key.data(), key.size() - 8);
  }

  common::Slice GetPrefixFromUserKey(const common::Slice& user_key) const {
    if (!IsTotalOrderMode()) {
      return prefix_extractor_->Transform(user_key);
    } else {
      // Use empty slice as prefix if prefix_extractor is not set.
      // In that case,
      // it falls back to pure binary search and
      // total iterator seek is supported.
      return common::Slice();
    }
  }

  friend class TableCache;
  friend class PlainTableIterator;

  // Internal helper function to generate an IndexRecordList object from all
  // the rows, which contains index records as a list.
  // If bloom_ is not null, all the keys' full-key hash will be added to the
  // bloom filter.
  common::Status PopulateIndexRecordList(PlainTableIndexBuilder* index_builder,
                                         vector<uint32_t>* prefix_hashes);

  // Internal helper function to allocate memory for bloom filter and fill it
  void AllocateAndFillBloom(int bloom_bits_per_key, int num_prefixes,
                            size_t huge_page_tlb_size,
                            vector<uint32_t>* prefix_hashes);

  void FillBloom(vector<uint32_t>* prefix_hashes);

  // Read the key and value at `offset` to parameters for keys, the and
  // `seekable`.
  // On success, `offset` will be updated as the offset for the next key.
  // `parsed_key` will be key in parsed format.
  // if `internal_key` is not empty, it will be filled with key with slice
  // format.
  // if `seekable` is not null, it will return whether we can directly read
  // data using this offset.
  common::Status Next(PlainTableKeyDecoder* decoder, uint32_t* offset,
                      db::ParsedInternalKey* parsed_key,
                      common::Slice* internal_key, common::Slice* value,
                      bool* seekable = nullptr) const;
  // Get file offset for key target.
  // return value prefix_matched is set to true if the offset is confirmed
  // for a key with the same prefix as target.
  common::Status GetOffset(PlainTableKeyDecoder* decoder,
                           const common::Slice& target,
                           const common::Slice& prefix, uint32_t prefix_hash,
                           bool& prefix_matched, uint32_t* offset) const;

  bool IsTotalOrderMode() const { return (prefix_extractor_ == nullptr); }

  // No copying allowed
  explicit PlainTableReader(const TableReader&) = delete;
  void operator=(const TableReader&) = delete;
};
}  // namespace table
}  // namespace xengine
#endif  // ROCKSDB_LITE
