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

#ifndef XENGINE_STORAGE_MAJOR_COMPACTION_H_
#define XENGINE_STORAGE_MAJOR_COMPACTION_H_

#include "compaction.h"
#include "compaction_stats.h"
#include "reuse_block_merge_iterator.h"
#include "storage/storage_manager.h"
#include "table/block.h"
#include "table/two_level_iterator.h"
#include "util/aligned_buffer.h"
#include "memory/page_arena.h"
#include "xengine/cache.h"
#include "xengine/options.h"

namespace xengine {
namespace db {
class MajorCompactionIterator;
class IntTblPropCollectorFactory;
class ColumnFamilyData;
class TableCache;
};

namespace util {
class EnvOptions;
}

namespace common {
class ImmutableCFOptions;
}

namespace table {
class BlockIter;
class TableBuilder;
}

namespace storage {

class ExtentSpaceManager;
class AsyncRandomAccessExtent;

class MajorCompaction;

class DataBlockIterator {
 public:
  explicit DataBlockIterator(const MetaType type, table::BlockIter *iter)
      : block_iter_(iter) {
    meta_descriptor_.type_ = type;
  }
  virtual ~DataBlockIterator() {}
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
  static const int64_t RESERVE_READER_NUM = 64;
  enum IterLevel { kExtentLevel, kBlockLevel, kKVLevel, kDataEnd };

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

  SEIterator(const util::Comparator *cmp,
             const util::Comparator *interal_cmp,
             bool l1_level = false);
  ~SEIterator();
  void reset();
  void seek_to_first();
  int next();
  int next_extent();
  int next_block();
  int next_kv();

  void set_compaction(MajorCompaction *compaction) { compaction_ = compaction; }
  void set_l1_level(const bool l1_level) { is_l1level_ = l1_level; }
  // add extents' meta need to merge
  void add_extent(const storage::MetaDescriptor &extent) {
    extent_list_.push_back(extent);
  }

  // through compare other key to check reuse
  // if kv level, get (key,value)
  // if not kv level, do reuse and return output_level=block/extent
  int get_current_row(const common::Slice &other_key,
                      const common::Slice &last_key,
                      const bool has_last_key,
                      IterLevel &output_level);

  // deal with equal condition
  // if block/extent level, just open util it turn to kv level
  // if kv level, get (key, value)
  int get_special_curent_row(IterLevel &out_level);

  // if block/extent level, do reuse while it turn to kv level
  // if kv level, get (key, value)
  int get_single_row(const common::Slice &last_key,
                     const bool has_last_key,
                     IterLevel &out_level);

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

  common::Slice get_key() const {
    assert(current_block_iter_);
    return current_block_iter_->key();
  }
  common::Slice get_value() const {
    assert(current_block_iter_);
    return current_block_iter_->value();
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
  bool valid() const { return (kDataEnd != iter_level_); }
  IterLevel get_iter_level() const { return iter_level_; }
  size_t get_extent_index() const { return extent_index_; }
  int64_t get_extent_level() const {
    assert(extent_index_ < extent_list_.size());
    return extent_list_[extent_index_].type_.level_;
  }
  bool is_l1_level() const { return is_l1level_; }

 private:
  int create_current_iterator();
  int check_reuse_meta(const common::Slice &last_key,
                       const bool has_last_key,
                       IterLevel &output_level);
  int create_block_iter(const MetaDescriptor &meta);
  void prefetch_next_extent();

 private:
  MajorCompaction *compaction_;
  DataBlockIterator *current_iterator_;
  table::BlockIter *current_block_iter_;
  const util::Comparator *cmp_;
  const util::Comparator *internal_cmp_;
  IterLevel iter_level_;
  size_t iterator_index_;
  size_t extent_index_;
  common::Slice startkey_;
  common::Slice endkey_;
  bool is_l1level_;
  bool reuse_;
  bool at_next_; // at next block or extent
  util::autovector<storage::MetaDescriptor> extent_list_;
  ReaderRep cur_rep_;
};

class MajorCompaction {
 public:
  // MajorCompaction output
  struct Statstics {
    CompactRecordStats record_stats_;
    CompactPerfStats perf_stats_;
  };
  const static int16_t L1_LEVEL = 1;
  const static int16_t L2_LEVEL = 2;

 public:
  MajorCompaction(const CompactionContext &context, const ColumnFamilyDesc &cf);
  ~MajorCompaction();

  // add extents batch to task
  int add_merge_batch(
      const ReuseBlockMergeIterator::MetaDescriptorList &extents,
      const size_t start, const size_t end);
  void add_split_key(const common::Slice &split_key);
  void set_delete_percent(const int64_t delete_percent) {
    delete_percent_ = delete_percent;
  }
  // run a task
  int run();
  int run_split_task();
  int cleanup();

  const Statstics &get_stats() const { return stats_; }
  storage::ChangeInfo &get_change_info() { return change_info_; }
  bool in_merge_stream() const { return in_merge_; }
  bool not_do_reuse(const storage::MetaDescriptor &meta) const;
  common::CompressionType GetCompressionType(
      const common::ImmutableCFOptions &ioptions,
      const common::MutableCFOptions &mutable_cf_options, int level,
      const bool enable_compression = true);
  size_t get_extent_size() const { return merge_extents_.size(); }
  memory::ArenaAllocator &get_allocator() { return arena_; }
 private:
  friend class SEIterator;
  int open_extent();
  int close_extent();
  int close_split_extent(const int64_t level);

  int split_extents(SEIterator &iterator);
  int merge_extents(const int64_t batch_index,
                    const storage::BlockPosition &batch);
  int merge_data_stream(const int64_t batch_index,
                        const storage::BlockPosition &batch);

  int build_compactor();
  int down_level_extent(const MetaDescriptor &extent);
  int copy_data_block(const MetaDescriptor &data_block);

  int add_extents_to_iterator(const int64_t batch_index,
                              const storage::BlockPosition &batch,
                              db::MajorCompactionIterator *&compactor);

  int get_table_reader(const MetaDescriptor &extent,
                       table::TableReader *&reader);
  int get_extent_index_iterator(const MetaDescriptor &extent,
                                table::TableReader *&reader,
                                table::BlockIter *&index_iterator);
  int get_extent_index_iterator(table::TableReader *reader,
                                table::BlockIter *&index_iterator);
  int create_extent_index_iterator(const MetaDescriptor &extent,
                                   size_t &iterator_index,
                                   DataBlockIterator *&iterator,
                                   SEIterator::ReaderRep &rep);
  int destroy_extent_index_iterator(const size_t iterator_index);
  int create_data_block_iterator(const storage::BlockPosition &data_block,
                                 table::TableReader *reader,
                                 table::BlockIter *&block_iterator);
  int delete_extent_meta(const MetaDescriptor &extent);
  int destroy_data_block_iterator(table::BlockIter *&block_iterator);
  int prefetch_extent(int64_t extent_id);
  AsyncRandomAccessExtent *get_async_extent_reader(int64_t extent_id) const;
  void destroy_async_extent_reader(int64_t extent_id);

  void clear_current_readers();
  void clear_current_writers();

  void record_compaction_iterator_stats(const db::MajorCompactionIterator &iter,
                                        CompactRecordStats &stats);

 private:
  struct MergeStats {
    common::Slice min_key_;
    common::Slice max_key_;
    DECLARE_TO_STRING();
  };
  // TODO default options for create table builder, remove future
  std::vector<std::unique_ptr<db::IntTblPropCollectorFactory>> props_;
  std::string compression_dict_;

  memory::ArenaAllocator arena_;
  // options for create builder and reader;
  CompactionContext context_;
  ColumnFamilyDesc cf_desc_;

  // all extents need to merge in one compaciton task.
  util::autovector<storage::MetaDescriptor> merge_extents_;
  // [start, end) sub task in %merge_extents_;
  util::autovector<storage::BlockPosition> merge_batch_indexes_;
  util::autovector<SEIterator::ReaderRep, SEIterator::RESERVE_READER_NUM>
      reader_reps_;
  // concatenate sequential data block indexes in same way;
  std::unordered_map<int64_t, AsyncRandomAccessExtent *> prefetch_extents_;
  // MajorCompaction writer,
  bool write_extent_opened_;
  std::unique_ptr<table::TableBuilder> extent_builder_;
  db::MiniTables mini_tables_;
  storage::ChangeInfo change_info_;
  util::WritableBuffer block_buffer_;
  db::MajorCompactionIterator *compactor_;
  // MajorCompaction result, written meta, statistics;
  Statstics stats_;
  bool in_merge_;
  util::autovector<common::Slice> split_keys_;
  int64_t delete_percent_;
};
}  // namespace storage
}  // namespace xengine

#endif
