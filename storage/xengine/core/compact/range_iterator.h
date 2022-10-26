/*
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef XENGINE_RANGE_ITERATOR_ADAPTOR_H_
#define XENGINE_RANGE_ITERATOR_ADAPTOR_H_

#include <stdint.h>
#include <mutex>
#include <utility>
#include "memory/stl_adapt_allocator.h"
#include "memory/mod_info.h"
#include "db/dbformat.h"
#include "table/internal_iterator.h"
#include "util/to_string.h"
#include "xengine/slice.h"
#include "xengine/status.h"
#include "storage/io_extent.h"
#include "storage/storage_common.h"
#include "util/heap.h"
#include "compaction_stats.h"
#include "table/block.h"

namespace xengine {
namespace storage {

// can represents Extent or DataBlock
// if represents Extent, first and second are continuous extent_id
// if represents DataBlock, first is offset and second is size of data block;
typedef std::pair<int64_t, int64_t> BlockPosition;

class CompactionContext;
class ExtentMeta;
class GeneralCompaction;

struct Range {
  common::Slice start_key_;
  common::Slice end_key_;
  Range deep_copy(memory::Allocator &allocator) const;
  Range deep_copy(memory::SimpleAllocator &allocator) const;
  DECLARE_TO_STRING();
};

struct MetaType {
  enum StoreType {
    SSTable = 0,
    MemTable = 1,
  };
  enum DataType {
    Extent = 0,
    DataBlock = 1,
  };
  enum KeyType {
    InternalKey = 0,
    UserKey = 1,
  };

  int8_t store_type_;  // memtable or sstable
  int8_t data_type_;   // extent or data block
  int8_t key_type_;    // UserKey or InternalKey
  int8_t level_;       // sstable level
  int16_t way_;
  int64_t sequence_;

  MetaType();
  MetaType(int8_t st, int8_t dt, int8_t kt, int8_t lv, int16_t w, int64_t seq);
  DECLARE_TO_STRING();
};

struct MetaDescriptor {
  storage::MetaType type_;
  storage::Range range_;
  storage::BlockPosition block_position_;
  storage::LayerPosition layer_position_;
  ExtentId extent_id_;
  common::Slice key_;    // copy iterator's key
  common::Slice value_;  // copy iterator's value
//  const common::XengineSchema *schema_;  // extent's/block's schema
  int64_t delete_percent_;
  MetaDescriptor();
  MetaDescriptor deep_copy(memory::Allocator &allocator) const;
  MetaDescriptor deep_copy(memory::SimpleAllocator &allocator) const;
  DECLARE_TO_STRING();

  bool is_user_key() const { return type_.key_type_ == MetaType::UserKey; }
  const ExtentId get_extent_id() const { return extent_id_; }
  common::Slice get_user_key(const common::Slice &key) const {
    if (is_user_key()) {
      return key;
    } else {
      return db::ExtractUserKey(key);
    }
  }
  common::Slice get_start_user_key() const {
    return get_user_key(range_.start_key_);
  }
  common::Slice get_end_user_key() const {
    return get_user_key(range_.end_key_);
  }

//  void set_schema(const common::XengineSchema *schema) { schema_ = schema; }
//  const common::XengineSchema *get_schema() const { return schema_; }
};

using MetaDescriptorList = std::vector<MetaDescriptor,
  memory::stl_adapt_allocator<MetaDescriptor, memory::ModId::kCompaction>>;
using BlockPositionList = std::vector<BlockPosition,
  memory::stl_adapt_allocator<BlockPosition, memory::ModId::kCompaction>>;

class RangeIterator {
 public:
  virtual ~RangeIterator() {}

  virtual bool middle() const = 0;
  virtual bool valid() const = 0;
  virtual common::Status status() const = 0;

  virtual common::Slice user_key() const = 0;
  virtual common::Slice key() const = 0;
  virtual common::Slice value() const = 0;

  virtual void seek(const common::Slice &lookup_key) = 0;
  virtual void seek_to_first() = 0;
  virtual void next() = 0;

  virtual const MetaDescriptor &get_meta_descriptor() const = 0;
};

/**
 * RangeAdaptorIterator takes a range(start_key,end_key) iterator
 * and iterate start_key, end_key sequentially.
 * just forward, Do not iterate backward direction
 */
template <typename IterType>
class RangeAdaptorIterator : public RangeIterator {
 public:
  RangeAdaptorIterator()
      : valid_(false), start_(false), status_(0), iter_(nullptr), extent_meta_(nullptr) {}
  explicit RangeAdaptorIterator(const MetaType &meta_type, IterType *iter)
      : valid_(false), start_(false), status_(0), iter_(iter), extent_meta_(nullptr) {
    meta_descriptor_.type_ = meta_type;
    update();
  }

  virtual ~RangeAdaptorIterator() {}

  IterType *get_inner_iterator() { return iter_; }

  const storage::MetaDescriptor &get_meta_descriptor() const override {
    assert(valid_);
    return meta_descriptor_;
  }

  const storage::ExtentMeta* get_extent_meta() const {
    assert(valid_);
    return extent_meta_;
  }

  // we are in the middle of the range;
  virtual bool middle() const override { return start_; }
  // still has more data if is valid.
  virtual bool valid() const override { return valid_; }

  virtual common::Slice user_key() const override {
    assert(valid_);
    return db::ExtractUserKey(key());
  }

  virtual common::Slice key() const override {
    assert(valid_);
    return start_ ? meta_descriptor_.range_.start_key_
                  : meta_descriptor_.range_.end_key_;
  }

  virtual common::Slice value() const override {
    assert(valid_);
    assert(iter_);
    return iter_->value();
  }

  virtual common::Status status() const override {
    assert(iter_);
    if (common::Status::kOk != status_) return common::Status(status_);
    return iter_->status();
  }

  virtual void next() override {
    assert(iter_);
    if (start_) {
      start_ = false;
    } else {
      iter_->Next();
      update();
    }
  }

  virtual void seek(const common::Slice &lookup_key) override {
    assert(iter_);
    iter_->Seek(lookup_key);
    update();
  }

  virtual void seek_to_first() override {
    assert(iter_);
    iter_->SeekToFirst();
    update();
  }

 protected:
  // update shoudle setup all data we need.
  virtual void update() {
    if (nullptr != iter_) {
      valid_ = iter_->Valid();
      if (valid_) {
        start_ = true;
        status_ = extract_range(iter_->key(), iter_->value());
      }
    }
  }

  virtual int extract_range(const common::Slice &k, const common::Slice &v) {
    UNUSED(k);
    UNUSED(v);
    return 0;
  }

 protected:
  bool valid_;
  bool start_;  // start_ == true if we'are in the middle of range;
  int status_;
  IterType *iter_;
  storage::MetaDescriptor meta_descriptor_;
  storage::ExtentMeta* extent_meta_;
};

class SequentialRangeIterator : public RangeIterator {
 public:
  SequentialRangeIterator() : index_(0) {}
  virtual ~SequentialRangeIterator() {}

  int add_iterator(RangeIterator *iter) {
    list_iters_.push_back(iter);
    return common::Status::kOk;
  }
  void clear() {
    list_iters_.clear();
    index_ = 0;
  }
  int64_t size() const { return (int64_t)list_iters_.size(); }
  int64_t current() const { return index_; }

  virtual bool middle() const override {
    assert(index_ < (int64_t)list_iters_.size());
    return list_iters_[index_]->middle();
  }

  virtual bool valid() const override {
    return (index_ < (int64_t)list_iters_.size() &&
            list_iters_[index_]->valid());
  }

  virtual common::Status status() const override {
    if (index_ < (int64_t)list_iters_.size()) {
      return list_iters_[index_]->status();
    }
    return common::Status(common::Status::kCorruption);
  }

  virtual common::Slice user_key() const override {
    assert(index_ < (int64_t)list_iters_.size());
    return list_iters_[index_]->user_key();
  }

  virtual common::Slice key() const override {
    assert(index_ < (int64_t)list_iters_.size());
    return list_iters_[index_]->key();
  }

  virtual common::Slice value() const override {
    assert(index_ < (int64_t)list_iters_.size());
    return list_iters_[index_]->value();
  }

  virtual void seek(const common::Slice &lookup_key) override {
    while (index_ < (int64_t)list_iters_.size()) {
      list_iters_[index_]->seek(lookup_key);
      if (list_iters_[index_]->valid()) {
        break;
      }
      ++index_;
    }
  }

  virtual void seek_to_first() override {
    index_ = 0;
    if (index_ < (int64_t)list_iters_.size()) {
      list_iters_[index_]->seek_to_first();
    }
  }

  virtual void next() override {
    if (index_ < (int64_t)list_iters_.size()) {
      list_iters_[index_]->next();
      if (!list_iters_[index_]->valid()) {
        ++index_;
        if (index_ < (int64_t)list_iters_.size()) {
          list_iters_[index_]->seek_to_first();
        }
      }
    }
  }

  virtual const MetaDescriptor &get_meta_descriptor() const override {
    assert(index_ < (int64_t)list_iters_.size());
    return list_iters_[index_]->get_meta_descriptor();
  }

 private:
  util::autovector<RangeIterator *> list_iters_;
  int64_t index_;
};

class SequentialDataBlockIterator : public table::InternalIterator {
 public:
  struct Handle {
    storage::BlockPosition data_block_;
    int64_t extent_sequence_;
    Handle() : data_block_(0, 0), extent_sequence_(0) {}
    Handle(const storage::BlockPosition &bp, int64_t seq)
        : data_block_(bp), extent_sequence_(seq) {}
  };
  SequentialDataBlockIterator() : index_(0) {}
  int add_data_block(const Handle &block) {
    data_blocks_.push_back(block);
    return common::Status::kOk;
  }
  void clear() {
    data_blocks_.clear();
    index_ = 0;
  }
  int64_t size() const { return (int64_t)data_blocks_.size(); }
  int64_t current() const { return index_; }

  virtual bool Valid() const override {
    return index_ < (int64_t)data_blocks_.size();
  }
  virtual void Seek(const common::Slice &target) override {
    // not implements;
  }
  virtual void SeekForPrev(const common::Slice &target) override {
    // not implements;
  }

  virtual void SeekToFirst() override { index_ = 0; }
  virtual void SeekToLast() override {
    index_ = (data_blocks_.size() == 0)
                 ? 0
                 : static_cast<int64_t>(data_blocks_.size()) - 1;
  }
  virtual void Next() override {
    assert(Valid());
    index_++;
  }
  virtual void Prev() override {
    assert(Valid());
    if (index_ == 0) {
      index_ = static_cast<int64_t>(data_blocks_.size());  // Marks as invalid
    } else {
      index_--;
    }
  }
  common::Slice key() const override {
    assert(Valid());
    return value();
  }
  common::Slice value() const override {
    assert(Valid());
    return common::Slice(reinterpret_cast<const char *>(&data_blocks_[index_]),
                         sizeof(Handle));
  }
  virtual common::Status status() const override {
    return common::Status::OK();
  }

 private:
  util::autovector<Handle> data_blocks_;
  int64_t index_;
};


class DataBlockIterator {
 public:
  explicit DataBlockIterator(const MetaType type, table::BlockIter *iter)
      : block_iter_(iter) {
    meta_descriptor_.type_ = type;
  }
  virtual ~DataBlockIterator() {
    block_iter_->~BlockIter();
  }
  void seek_to_first() {
    assert(block_iter_);
    block_iter_->SeekToFirst();
    if (block_iter_->Valid()) {
      update(block_iter_->key(), block_iter_->value());
    }
  }
  void next() {
    assert(block_iter_);
    block_iter_->Next();
    if (block_iter_->Valid()) {
      update(block_iter_->key(), block_iter_->value());
    }
  }
  int update(const common::Slice &start, const common::Slice &end) {
    int ret = 0;
    meta_descriptor_.range_.end_key_ = start;
    common::Slice block_index_content = end;
    table::BlockHandle handle;
    handle.DecodeFrom(const_cast<common::Slice *>(&block_index_content));
    meta_descriptor_.block_position_.first = handle.offset();
    meta_descriptor_.block_position_.second = handle.size();
    meta_descriptor_.value_ = block_index_content;
    int64_t delete_percent = 0;
    FAIL_RETURN(db::BlockStats::decode(block_index_content,
                                       meta_descriptor_.range_.start_key_,
                                       delete_percent));
    meta_descriptor_.delete_percent_ = delete_percent;
    return ret;
  }
  bool valid() { return nullptr != block_iter_ && block_iter_->Valid(); }
  const MetaDescriptor &get_meta_descriptor() const { return meta_descriptor_; }

 private:
  MetaDescriptor meta_descriptor_;
  table::BlockIter *block_iter_;
};
class SEIterator {
 public:
  enum IterLevel { kExtentLevel, kBlockLevel, kKVLevel, kDataEnd };
  SEIterator();
  virtual ~SEIterator();
  virtual void seek_to_first() = 0;
  // through compare other key to check reuse
  // if kv level, get (key,value)
  // if not kv level, do reuse and return output_level=block/extent
  virtual int get_current_row(const common::Slice &other_key,
                              const common::Slice &last_key,
                              const bool has_last_key,
                              IterLevel &output_level) = 0;
  // deal with equal condition
  // if block/extent level, just open util it turn to kv level
  // if kv level, get (key, value)
  virtual int get_special_curent_row(IterLevel &out_level) = 0;

  // if block/extent level, do reuse while it turn to kv level
  // if kv level, get (key, value)
  virtual int get_single_row(const common::Slice &last_key,
                             const bool has_last_key,
                             IterLevel &out_level) = 0;
  virtual int next() = 0;
  virtual void reset() = 0;
  virtual bool valid() const = 0;
  virtual common::Slice get_key() const = 0;
  virtual common::Slice get_value() const = 0;
  virtual bool has_data() const = 0;
  // internal start key
  inline const common::Slice &get_startkey() const {
    assert(kDataEnd != iter_level_);
    return startkey_;
  }
  // internal end key
  inline const common::Slice &get_endkey() const {
    assert(kDataEnd != iter_level_);
    return endkey_;
  }
  // user key
  common::Slice get_start_ukey() const;
  common::Slice get_end_ukey() const;
  void set_compaction(GeneralCompaction *compaction) { compaction_ = compaction; }
//  const common::XengineSchema *get_schema() const { return schema_; }
//  void set_schema(const common::XengineSchema *schema) {
//    schema_ = schema;
//  }
public:
  GeneralCompaction *compaction_;
//  const common::XengineSchema *schema_;
  IterLevel iter_level_;
  common::Slice startkey_;
  common::Slice endkey_;
};

class MemSEIterator : public SEIterator {
 public:
  MemSEIterator();
  virtual ~MemSEIterator();
  virtual void seek_to_first();
  // through compare other key to check reuse
  // if kv level, get (key,value)
  // if not kv level, do reuse and return output_level=block/extent
  virtual int get_current_row(const common::Slice &other_key,
                              const common::Slice &last_key,
                              const bool has_last_key,
                              IterLevel &output_level);
  // deal with equal condition
  // if block/extent level, just open util it turn to kv level
  // if kv level, get (key, value)
  virtual int get_special_curent_row(IterLevel &out_level);

  // if block/extent level, do reuse while it turn to kv level
  // if kv level, get (key, value)
  virtual int get_single_row(const common::Slice &last_key,
                             const bool has_last_key,
                             IterLevel &out_level);
  virtual int next();
  virtual void reset();
  virtual bool valid() const {
    assert(mem_iter_);
    return mem_iter_->Valid();
  }
  virtual common::Slice get_key() const {
    assert(mem_iter_);
    return mem_iter_->key();
  }
  virtual common::Slice get_value() const {
    assert(mem_iter_);
    return mem_iter_->value();
  }
  virtual bool has_data() const {
    return nullptr != mem_iter_;
  }
  void set_mem_iter(table::InternalIterator *mem_iter) {
    mem_iter_ = mem_iter;
  }
 private:
  int next_kv(IterLevel &output_level);
  table::InternalIterator* mem_iter_;
};

class ExtSEIterator : public SEIterator{
 public:
  static const int64_t RESERVE_READER_NUM = 64;
  struct ReaderRep {
    ReaderRep()
        : extent_id_(0),
          table_reader_(nullptr),
          index_iterator_(nullptr),
          block_iter_(nullptr)
    {}
    int64_t extent_id_;
    table::TableReader *table_reader_;
    table::BlockIter *index_iterator_;
    DataBlockIterator *block_iter_;
  };

  ExtSEIterator(const util::Comparator *cmp,
             const util::Comparator *interal_cmp);
  virtual ~ExtSEIterator();
  virtual void seek_to_first();
  // through compare other key to check reuse
  // if kv level, get (key,value)
  // if not kv level, do reuse and return output_level=block/extent
  virtual int get_current_row(const common::Slice &other_key,
                              const common::Slice &last_key,
                              const bool has_last_key,
                              IterLevel &output_level);
  // deal with equal condition
  // if block/extent level, just open util it turn to kv level
  // if kv level, get (key, value)
  virtual int get_special_curent_row(IterLevel &out_level);

  // if block/extent level, do reuse while it turn to kv level
  // if kv level, get (key, value)
  virtual int get_single_row(const common::Slice &last_key,
                             const bool has_last_key,
                             IterLevel &out_level);
  virtual void reset();
  virtual bool valid() const {
    return (kDataEnd != iter_level_);
  }
  virtual common::Slice get_key() const {
    assert(current_block_iter_);
    return current_block_iter_->key();
  }
  virtual common::Slice get_value() const {
    assert(current_block_iter_);
    return current_block_iter_->value();
  }
  virtual bool has_data() const {
    return extent_list_.size() > 0;
  }
  virtual int next();
  // add extents' meta need to merge
  void add_extent(const storage::MetaDescriptor &extent) {
    extent_list_.push_back(extent);
  }
  void set_delete_percent(const int64_t delete_percent) {
    delete_percent_ = delete_percent;
  }
  // get extent/block meta
  const MetaDescriptor &get_meta_descriptor() const {
    assert(valid());
    if (kBlockLevel == iter_level_) {
      assert(current_iterator_);
      return current_iterator_->get_meta_descriptor();
    } else {
      assert(extent_index_ < extent_list_.size());
      return extent_list_[extent_index_];
    }
  }
  // return the reuse block/extent meta
  const MetaDescriptor &get_reuse_meta(/*const XengineSchema *&schema*/) const {
//    schema = schema_;
    return reuse_meta_;
  }
  IterLevel get_iter_level() const { return iter_level_; }
  size_t get_extent_index() const { return extent_index_; }
  int64_t get_extent_level() const {
    assert(extent_index_ < extent_list_.size());
    return extent_list_[extent_index_].type_.level_;
  }
 private:
  int next_extent();
  int next_block();
  int next_kv();
  int create_current_iterator();
  int check_reuse_meta(const common::Slice &last_key,
                       const bool has_last_key,
                       IterLevel &output_level);
  int create_block_iter(const MetaDescriptor &meta);
  void prefetch_next_extent();
  int get_mem_row(IterLevel &output_level);

 private:
  DataBlockIterator *current_iterator_;
  table::BlockIter *current_block_iter_;
  const util::Comparator *cmp_;
  const util::Comparator *internal_cmp_;
  size_t iterator_index_;
  size_t extent_index_;
  bool reuse_;
  bool at_next_; // at next block or extent
  MetaDescriptorList extent_list_;
  ReaderRep cur_rep_;
  int64_t delete_percent_;
  MetaDescriptor reuse_meta_;
  memory::ArenaAllocator meta_descriptor_arena_;
};


class SEIteratorComparator {
 public:
  SEIteratorComparator(const util::Comparator* comparator)
      : comparator_(comparator) {}

  bool operator()(SEIterator * a, SEIterator* b) const {
    return comparator_->Compare(a->get_startkey(), b->get_startkey()) > 0;
  }

 private:
  const util::Comparator* comparator_;
};

class MultipleSEIterator {
public:
  MultipleSEIterator(const util::Comparator* user_cmp,
                     const util::Comparator* internal_cmp);
  ~MultipleSEIterator() {
  }
  typedef util::BinaryHeap<SEIterator *,
                           SEIteratorComparator>
      MergerSEIterHeap;
  void reset();
  int seek_to_first();
  int next();
  int get_single_row(const bool has_last_kv);
  int add_se_iterator(SEIterator *se_iterator);
  bool valid() const {
    return SEIterator::kDataEnd != output_level_;
  }
  common::Slice get_key() {
    assert(current_iterator_);
    return current_iterator_->get_key();
  }
  common::Slice get_value() {
    assert(current_iterator_);
    return current_iterator_->get_value();
  }

  // return the reuse block/extent meta
  const MetaDescriptor &get_reuse_meta(/*const XengineSchema *&schema*/) const {
    assert(current_iterator_);
    return static_cast<ExtSEIterator *>(current_iterator_)->get_reuse_meta(/*schema*/);
  }

//  const XengineSchema *get_schema() const {
//    assert(current_iterator_);
//    return current_iterator_->get_schema();
//  }
  void set_last_user_key(const common::Slice &last_user_key) {
    last_user_key_ = last_user_key;
  }

  SEIterator::IterLevel get_output_level() const {
    return output_level_;
  }
private:
  const util::Comparator *user_cmp_;
  const util::Comparator *internal_cmp_;
  MergerSEIterHeap se_heap_;
  SEIterator *current_iterator_;
  common::Slice last_user_key_;
  SEIterator::IterLevel output_level_;
  bool has_one_way_;
};
} /* meta */
} /* xengine*/

#endif  // XENGINE_RANGE_ITERATOR_ADAPTOR_H_
