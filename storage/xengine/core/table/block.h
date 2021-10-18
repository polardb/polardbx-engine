// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once
#include <stddef.h>
#include <stdint.h>
#include <string>
#include <vector>
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
#ifdef OS_FREEBSD
#include <malloc_np.h>
#else
#include <malloc.h>
#endif
#endif

#include "db/dbformat.h"
#include "db/pinned_iterators_manager.h"
#include "table/block_prefix_index.h"
#include "table/internal_iterator.h"
#include "memory/base_malloc.h"
#include "xengine/iterator.h"
#include "xengine/options.h"
#include "monitoring/query_perf_context.h"

#include "format.h"

namespace xengine {

namespace util {
class Comparator;
}

namespace table {

struct BlockContents;
class BlockIter;
class BlockPrefixIndex;

// BlockReadAmpBitmap is a bitmap that map the xengine::table::Block data bytes to
// a bitmap with ratio bytes_per_bit. Whenever we access a range of bytes in
// the Block we update the bitmap and increment READ_AMP_ESTIMATE_USEFUL_BYTES.
class BlockReadAmpBitmap {
 public:
  explicit BlockReadAmpBitmap(size_t block_size, size_t bytes_per_bit)
      : bitmap_(nullptr), bytes_per_bit_pow_(0) {
    assert(block_size > 0 && bytes_per_bit > 0);

    // convert bytes_per_bit to be a power of 2
    while (bytes_per_bit >>= 1) {
      bytes_per_bit_pow_++;
    }

    // num_bits_needed = ceil(block_size / bytes_per_bit)
    size_t num_bits_needed =
        (block_size >> static_cast<size_t>(bytes_per_bit_pow_)) +
        (block_size % (static_cast<size_t>(1)
                       << static_cast<size_t>(bytes_per_bit_pow_)) !=
         0);

    // bitmap_size = ceil(num_bits_needed / kBitsPerEntry)
    size_t bitmap_size = (num_bits_needed / kBitsPerEntry) +
                         (num_bits_needed % kBitsPerEntry != 0);

    // Create bitmap and set all the bits to 0
    bitmap_ = new std::atomic<uint32_t>[ bitmap_size ];
    memset(bitmap_, 0, bitmap_size * kBytesPersEntry);
    QUERY_COUNT_ADD(xengine::monitor::CountPoint::READ_AMP_TOTAL_READ_BYTES,
                    num_bits_needed << bytes_per_bit_pow_);
  }

  ~BlockReadAmpBitmap() {
    delete[] bitmap_;
  }

  void Mark(uint32_t start_offset, uint32_t end_offset) {
    assert(end_offset >= start_offset);

    // Every new bit we set will bump this counter
    uint32_t new_useful_bytes = 0;
    // Index of first bit in mask (start_offset / bytes_per_bit)
    uint32_t start_bit = start_offset >> bytes_per_bit_pow_;
    // Index of last bit in mask (end_offset / bytes_per_bit)
    uint32_t end_bit = end_offset >> bytes_per_bit_pow_;
    // Index of middle bit (unique to this range)
    uint32_t mid_bit = start_bit + 1;

    // It's guaranteed that ranges sent to Mark() wont overlap, this mean that
    // we dont need to set the middle bits, we can simply set only one bit of
    // the middle bits, and check this bit if we want to know if the whole
    // range is set or not.
    if (mid_bit < end_bit) {
      if (GetAndSet(mid_bit) == 0) {
        new_useful_bytes += (end_bit - mid_bit) << bytes_per_bit_pow_;
      } else {
        // If the middle bit is set, it's guaranteed that start and end bits
        // are also set
        return;
      }
    } else {
      // This range dont have a middle bit, the whole range fall in 1 or 2 bits
    }

    if (GetAndSet(start_bit) == 0) {
      new_useful_bytes += (1 << bytes_per_bit_pow_);
    }

    if (GetAndSet(end_bit) == 0) {
      new_useful_bytes += (1 << bytes_per_bit_pow_);
    }

    if (new_useful_bytes > 0) {
      QUERY_COUNT_ADD(
          xengine::monitor::CountPoint::READ_AMP_ESTIMATE_USEFUL_BYTES,
          new_useful_bytes);
    }
  }

  uint32_t GetBytesPerBit() { return 1 << bytes_per_bit_pow_; }

 private:
  // Get the current value of bit at `bit_idx` and set it to 1
  inline bool GetAndSet(uint32_t bit_idx) {
    const uint32_t byte_idx = bit_idx / kBitsPerEntry;
    const uint32_t bit_mask = 1 << (bit_idx % kBitsPerEntry);

    return bitmap_[byte_idx].fetch_or(bit_mask, std::memory_order_relaxed) &
           bit_mask;
  }

  const uint32_t kBytesPersEntry = sizeof(uint32_t);   // 4 bytes
  const uint32_t kBitsPerEntry = kBytesPersEntry * 8;  // 32 bits

  // Bitmap used to record the bytes that we read, use atomic to protect
  // against multiple threads updating the same bit
  std::atomic<uint32_t>* bitmap_;
  // (1 << bytes_per_bit_pow_) is bytes_per_bit. Use power of 2 to optimize
  // muliplication and division
  uint8_t bytes_per_bit_pow_;
};

class Block {
 public:
  // Initialize the block with the specified contents.
  explicit Block(BlockContents&& contents, common::SequenceNumber _global_seqno,
                 size_t read_amp_bytes_per_bit = 0);

  ~Block() = default;

  size_t size() const { return size_; }
  const char* data() const { return data_; }
  bool cachable() const { return contents_.cachable; }
  size_t usable_size() const {
#ifdef ROCKSDB_MALLOC_USABLE_SIZE
    if (contents_.allocation.get() != nullptr) {
      return memory::base_malloc_usable_size(contents_.allocation.get());
    }
#endif  // ROCKSDB_MALLOC_USABLE_SIZE
    return size_;
  }
  uint32_t NumRestarts() const;
  common::CompressionType compression_type() const {
    return contents_.compression_type;
  }

  // If hash index lookup is enabled and `use_hash_index` is true. This block
  // will do hash lookup for the key prefix.
  //
  // NOTE: for the hash based lookup, if a key prefix doesn't match any key,
  // the iterator will simply be set as "invalid", rather than returning
  // the key that is just pass the target key.
  //
  // If iter is null, return new Iterator
  // If iter is not null, update this one and return it as Iterator*
  //
  // If total_order_seek is true, hash_index_ and prefix_index_ are ignored.
  // This option only applies for index block. For data block, hash_index_
  // and prefix_index_ are null, so this option does not matter.
  InternalIterator* NewIterator(const util::Comparator* comparator,
                                BlockIter* iter = nullptr,
                                bool total_order_seek = true,
                                monitor::Statistics* stats = nullptr,
                                const bool is_index_block = false,
                                memory::SimpleAllocator *alloc = nullptr);
  void SetBlockPrefixIndex(BlockPrefixIndex* prefix_index);

  // Report an approximation of how much memory has been used.
  size_t ApproximateMemoryUsage() const;

  common::SequenceNumber global_seqno() const { return global_seqno_; }

 private:
  BlockContents contents_;
  const char* data_;         // contents_.data.data()
  size_t size_;              // contents_.data.size()
  uint32_t restart_offset_;  // Offset in data_ of restart array
  std::unique_ptr<BlockPrefixIndex, memory::ptr_destruct_delete<BlockPrefixIndex>> prefix_index_;
  std::unique_ptr<BlockReadAmpBitmap, memory::ptr_destruct_delete<BlockReadAmpBitmap>> read_amp_bitmap_;
  // All keys in the block will have seqno = global_seqno_, regardless of
  // the encoded value (kDisableGlobalSequenceNumber means disabled)
  const common::SequenceNumber global_seqno_;

  // No copying allowed
  Block(const Block&);
  void operator=(const Block&);
};

class BlockIter : public InternalIterator {
 public:
  BlockIter()
      : comparator_(nullptr),
        data_(nullptr),
        restarts_(0),
        num_restarts_(0),
        current_(0),
        restart_index_(0),
        status_(common::Status::OK()),
        prefix_index_(nullptr),
        key_pinned_(false),
        global_seqno_(db::kDisableGlobalSequenceNumber),
        read_amp_bitmap_(nullptr),
        last_bitmap_offset_(0),
        last_(0),
        is_index_block_(false),
        prev_entries_idx_(-1) {}

  BlockIter(const util::Comparator* comparator, const char* data,
            uint32_t restarts, uint32_t num_restarts,
            BlockPrefixIndex* prefix_index, common::SequenceNumber global_seqno,
            BlockReadAmpBitmap* read_amp_bitmap, const bool is_index_block)
      : BlockIter() {
    Initialize(comparator, data, restarts, num_restarts, prefix_index,
               global_seqno, read_amp_bitmap, is_index_block);
  }

  void reset() {
    comparator_ = nullptr;
    data_ = nullptr;
    restarts_ = 0;
    num_restarts_ = 0;
    current_ = 0;
    restart_index_ = 0;
    status_ = 0;
    prefix_index_ = nullptr;
    global_seqno_ = db::kDisableGlobalSequenceNumber;
    read_amp_bitmap_ = nullptr;
    last_bitmap_offset_ = 0;
    last_ = 0;
    is_index_block_ = false;
    prev_entries_idx_ = -1;
    InternalIterator::reset();
  }

  void Initialize(const util::Comparator* comparator, const char* data,
                  uint32_t restarts, uint32_t num_restarts,
                  BlockPrefixIndex* prefix_index,
                  common::SequenceNumber global_seqno,
                  BlockReadAmpBitmap* read_amp_bitmap,
                  const bool is_index_block) {
    assert(data_ == nullptr);  // Ensure it is called only once
    assert(num_restarts > 0);  // Ensure the param is valid

    comparator_ = comparator;
    data_ = data;
    restarts_ = restarts;
    num_restarts_ = num_restarts;
    current_ = restarts_;
    restart_index_ = num_restarts_;
    prefix_index_ = prefix_index;
    global_seqno_ = global_seqno;
    read_amp_bitmap_ = read_amp_bitmap;
    last_bitmap_offset_ = current_ + 1;
    last_ = restarts_;
    is_index_block_ = is_index_block;
    prev_entries_idx_ = -1;
  }

  void SetStatus(common::Status s) { status_ = s; }

  virtual bool Valid() const override 
  { 
    return is_index_block_ ? (current_ <= last_ && current_ < restarts_) : current_ < last_;
  }
  virtual common::Status status() const override { return status_; }
  virtual common::Slice key() const override {
    assert(Valid());
    return key_.GetInternalKey();
  }
  virtual common::Slice value() const override {
    assert(Valid());
    if (read_amp_bitmap_ && current_ < restarts_ &&
        current_ != last_bitmap_offset_) {
      read_amp_bitmap_->Mark(current_ /* current entry offset */,
                             NextEntryOffset() - 1);
      last_bitmap_offset_ = current_;
    }
    return value_;
  }

  virtual void Next() override;

  virtual void Prev() override;

  virtual void Seek(const common::Slice& target) override;

  virtual void SeekForPrev(const common::Slice& target) override;

  virtual void SeekToFirst() override;

  virtual void SeekToLast() override;

#ifndef NDEBUG
  ~BlockIter() {
    // Assert that the BlockIter is never deleted while Pinning is Enabled.
    assert(!pinned_iters_mgr_ ||
           (pinned_iters_mgr_ && !pinned_iters_mgr_->PinningEnabled()));
  }
  virtual void SetPinnedItersMgr(
      db::PinnedIteratorsManager* pinned_iters_mgr) override {
    pinned_iters_mgr_ = pinned_iters_mgr;
  }
  db::PinnedIteratorsManager* pinned_iters_mgr_ = nullptr;
#endif

  virtual bool IsKeyPinned() const override { return key_pinned_; }

  virtual bool IsValuePinned() const override { return true; }

  size_t TEST_CurrentEntrySize() { return NextEntryOffset() - current_; }

  uint32_t ValueOffset() const {
    return static_cast<uint32_t>(value_.data() - data_);
  }

private:
  void seek_end_key();

 private:
  const util::Comparator* comparator_;
  const char* data_;       // underlying block contents
  uint32_t restarts_;      // Offset of restart array (list of fixed32)
  uint32_t num_restarts_;  // Number of uint32_t entries in restart array

  // current_ is offset in data_ of current entry.  >= restarts_ if !Valid
  uint32_t current_;
  uint32_t restart_index_;  // Index of restart block in which current_ falls
  db::IterKey key_;
  common::Slice value_;
  common::Status status_;
  BlockPrefixIndex* prefix_index_;
  bool key_pinned_;
  common::SequenceNumber global_seqno_;

  // read-amp bitmap
  BlockReadAmpBitmap* read_amp_bitmap_;
  // last `current_` value we report to read-amp bitmp
  mutable uint32_t last_bitmap_offset_;
    uint32_t last_;
  bool is_index_block_;

  struct CachedPrevEntry {
    explicit CachedPrevEntry(uint32_t _offset, const char* _key_ptr,
                             size_t _key_offset, size_t _key_size,
                             common::Slice _value)
        : offset(_offset),
          key_ptr(_key_ptr),
          key_offset(_key_offset),
          key_size(_key_size),
          value(_value) {}

    // offset of entry in block
    uint32_t offset;
    // Pointer to key data in block (nullptr if key is delta-encoded)
    const char* key_ptr;
    // offset of key in prev_entries_keys_buff_ (0 if key_ptr is not nullptr)
    size_t key_offset;
    // size of key
    size_t key_size;
    // value slice pointing to data in block
    common::Slice value;
  };
  std::string prev_entries_keys_buff_;
  std::vector<CachedPrevEntry> prev_entries_;
  int32_t prev_entries_idx_ = -1;

  inline int Compare(const common::Slice& a, const common::Slice& b) const {
    return comparator_->Compare(a, b);
  }

  // Return the offset in data_ just past the end of the current entry.
  inline uint32_t NextEntryOffset() const {
    // NOTE: We don't support blocks bigger than 2GB
    return static_cast<uint32_t>((value_.data() + value_.size()) - data_);
  }

  inline uint32_t next_entry_offset(const common::Slice& value) const
  {
    return static_cast<uint32_t>((value.data() + value.size()) - data_);
  }

  uint32_t GetRestartPoint(uint32_t index) {
    assert(index < num_restarts_);
    return util::DecodeFixed32(data_ + restarts_ + index * sizeof(uint32_t));
  }

  void SeekToRestartPoint(uint32_t index) {
    key_.Clear();
    restart_index_ = index;
    // current_ will be fixed by ParseNextKey();

    // ParseNextKey() starts at the end of value_, so set value_ accordingly
    uint32_t offset = GetRestartPoint(index);
    value_ = common::Slice(data_ + offset, 0);
  }

  void CorruptionError();

  bool ParseNextKey();

  bool parse_next_key(uint32_t& current,
                      uint32_t& restart_index,
                      db::IterKey& key,
                      common::Slice& value,
                      bool& key_pinned);

  bool BinarySeek(const common::Slice& target, uint32_t left, uint32_t right,
                  uint32_t* index);

  int CompareBlockKey(uint32_t block_index, const common::Slice& target);

  bool BinaryBlockIndexSeek(const common::Slice& target, uint32_t* block_ids,
                            uint32_t left, uint32_t right, uint32_t* index);

  bool PrefixSeek(const common::Slice& target, uint32_t* index);
};

}  // namespace table
}  // namespace xengine
