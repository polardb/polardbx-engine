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
#pragma once

#include "monitoring/query_perf_context.h"
#include "storage/multi_version_extent_meta_layer.h"
#include "table/sstable_scan_struct.h"

namespace xengine
{
namespace db
{
class PinnedIteratorsManager;
}
namespace storage
{
class ExtentId;
}
namespace table
{

static const int64_t DISABLE_PREFETCH = -1;

struct IOReqMergeHandle
{
  IOReqMergeHandle() : start_handle_pos_(INVALID_POS),
                       end_handle_pos_(INVALID_POS)
  {}
  bool is_empty() const
  {
    return INVALID_POS == start_handle_pos_ && INVALID_POS == end_handle_pos_;
  }
  void set_start_pos(const int64_t start_pos)
  {
    start_handle_pos_ = start_pos;
  }
  void set_end_pos(const int64_t end_pos)
  {
    end_handle_pos_ = end_pos;
  }
  int64_t get_start_pos() const
  {
    return start_handle_pos_;
  }
  int64_t get_end_pos() const
  {
    return end_handle_pos_;
  }
  void reset()
  {
    start_handle_pos_ = INVALID_POS;
    end_handle_pos_ = INVALID_POS;
  }
  static const int64_t INVALID_POS = -1;
  int64_t start_handle_pos_;
  int64_t end_handle_pos_;
};

// BlockPrefetchHelper will do table reader and index block prefetch and init next/prev index block iterator
class TablePrefetchHelper
{
public:
  TablePrefetchHelper() : scan_param_(nullptr),
                          table_reader_prefetch_cnt_(DISABLE_PREFETCH),
                          table_reader_cur_pos_(0),
                          table_reader_prefetch_pos_(0),
                          index_block_cur_pos_(0),
                          enable_prefetch_(false),
                          valid_(false),
                          is_inited_(false)
  {}
  ~TablePrefetchHelper()
  {
    for (int64_t i = 0; i < TABLE_READER_HANDLE_CNT; i++) {
      table_reader_handles_[i].reset();
      index_block_handles_[i].reset();
    }
  }

  int init(const ScanParam &param);
  int seek(const common::Slice &target);
  int seek_to_first();
  int seek_to_last();
  int next();
  int prev();
  // prefetch table reader and index block forward
  int prefetch_next();
  // prefetch table reader and index block backward
  int prefetch_prev();
  int init_index_block_iter(BlockIter &index_block_iter);
  bool valid()
  {
    return valid_;
  }
  bool is_empty()
  {
    return 1 == (table_reader_prefetch_pos_ - index_block_cur_pos_);
  }
  void set_end_key(const common::Slice& end_key_slice, const bool need_seek_end_key)
  {
    // enable prefetch
    if (end_key_slice.size() > 0) {
      enable_prefetch_ = true;
      table_reader_prefetch_cnt_ = 2;
    } else {
      enable_prefetch_ = false;
    }
    extent_layer_iter_.set_end_key(end_key_slice, need_seek_end_key);
  }
  // The table reader will be released only when all the data blocks of this table
  // have been scanned. This func is used to get the table reader to new data
  // block iterator, and will return the cur table reader or the next table reader
  // (if need switch table reader) and release the previous table reader.
  int get_table_reader(const storage::ExtentId &extent_id, ExtentBasedTable *&table_reader)
  {
    int ret = common::Status::kOk;
    table_reader = nullptr;
    if (!extent_id.is_equal(get_table_reader_handle(table_reader_cur_pos_).extent_id_)) {
      // release table handle
      get_table_reader_handle(table_reader_cur_pos_).reset();
      table_reader_cur_pos_++;
      if (UNLIKELY(!extent_id.is_equal(get_table_reader_handle(table_reader_cur_pos_).extent_id_))) {
        ret = common::Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "unexpected table reader", K(ret), K(extent_id), K(table_reader_cur_pos_),
            K(get_table_reader_handle(table_reader_cur_pos_).extent_id_));
      }
    }
    if (SUCCED(ret)) {
      table_reader = get_table_reader_handle(table_reader_cur_pos_).reader();
    }
    return ret;
  }
  // get the table reader to prefetch data blocks
  ExtentBasedTable *index_table_reader()
  {
    return get_table_reader_handle(index_block_cur_pos_).reader();
  }
  void reset()
  {
    valid_ = false;
    table_reader_prefetch_cnt_ = enable_prefetch_ ? 2 : DISABLE_PREFETCH;
    table_reader_cur_pos_ = 0;
    table_reader_prefetch_pos_ = 0;
    index_block_cur_pos_ = 0;
  }

private:
  int load_table_reader(const common::Slice &meta_handle, TableReaderHandle &table_reader_handle);
  // prefetch a table reader and the corresponding index block
  int do_prefetch_index_block();
  void release_handle(BlockDataHandle<ExtentBasedTable::IndexReader> &index_handle)
  {
    index_handle.reset();
  }
  int64_t prefetched_cnt()
  {
    return table_reader_prefetch_pos_ - table_reader_cur_pos_;
  }
  bool need_prefetch()
  {
    return prefetched_cnt() <= (TABLE_READER_HANDLE_CNT / 2);
  }
  int64_t calc_prefetch_cnt()
  {
    return DISABLE_PREFETCH == table_reader_prefetch_cnt_
        ? 1
        : std::min(table_reader_prefetch_cnt_, TABLE_READER_HANDLE_CNT - prefetched_cnt());
  }
  TableReaderHandle &get_table_reader_handle(const int64_t pos)
  {
    return table_reader_handles_[pos % TABLE_READER_HANDLE_CNT];
  }
  BlockDataHandle<ExtentBasedTable::IndexReader> &get_index_handle(const int64_t pos)
  {
    return index_block_handles_[pos % TABLE_READER_HANDLE_CNT];
  }

private:
  static const int64_t TABLE_READER_HANDLE_CNT = 4;
  const ScanParam *scan_param_;
  storage::ExtentLayerIterator extent_layer_iter_;
  // used as a fixed queue, table_reader_prefetch_pos_ is the header
  // pointed to the prefetched position, and table_reader_prefetch_pos_
  // is the tail pointed to the reading data block
  TableReaderHandle table_reader_handles_[TABLE_READER_HANDLE_CNT];
  int64_t table_reader_prefetch_cnt_;
  int64_t table_reader_cur_pos_;
  int64_t table_reader_prefetch_pos_;
  // used as a fixed queue, index_block_cur_pos_ points the index block,
  // which is using to prefetch the data block
  BlockDataHandle<ExtentBasedTable::IndexReader> index_block_handles_[TABLE_READER_HANDLE_CNT];
  int64_t index_block_cur_pos_;
  bool enable_prefetch_;
  bool valid_;
  bool is_inited_;
};

// BlockPrefetchHelper will do data block prefetch and init next/prev data block iterator
class BlockPrefetchHelper
{
public:
  BlockPrefetchHelper() : data_block_prefetch_cnt_(DISABLE_PREFETCH),
                          data_block_prefetch_pos_(0),
                          data_block_cur_pos_(0),
                          pinned_iters_mgr_(nullptr),
                          enable_prefetch_(false),
                          valid_(false),
                          is_inited_(false)
  {}
  ~BlockPrefetchHelper()
  {
    for (int64_t i = 0; i < DATA_BLOCK_HANDLE_CNT; i++) {
      block_handles_[i].reset();
    }
  }

  int init(const ScanParam &param);
  int seek(const common::Slice& target);
  int seek_to_first();
  int seek_to_last();
  int next();
  int prev();
  // prefetch data blocks forward
  int prefetch_next();
  // prefetch data blocks backward
  int prefetch_prev();
  int init_data_block_iter(BlockIter &data_block_iter);
  bool valid()
  {
    return valid_;
  }
  void set_end_key(const common::Slice &end_key_slice, const bool need_seek_end_key)
  {
    // enable prefetch
    if (end_key_slice.size() > 0) {
      enable_prefetch_ = true;
      data_block_prefetch_cnt_ = 2;
    } else {
      enable_prefetch_ = false;
    }
    table_prefetch_helper_.set_end_key(end_key_slice, need_seek_end_key);
  }
  // get the table reader to new data block iterator
  int get_table_reader(const storage::ExtentId &extent_id, ExtentBasedTable *&table_reader)
  {
    return table_prefetch_helper_.get_table_reader(extent_id, table_reader);
  }
  // get the table reader to prefetch data blocks
  ExtentBasedTable *index_table_reader()
  {
    return table_prefetch_helper_.index_table_reader();
  }
  void reset()
  {
    valid_ = false;
    data_block_prefetch_cnt_ = enable_prefetch_ ? 2 : DISABLE_PREFETCH;
    data_block_prefetch_pos_ = 0;
    data_block_cur_pos_ = 0;
    add_blocks_ = 0;
    table_prefetch_helper_.reset();
  }
  void set_pinned_iters_mgr(db::PinnedIteratorsManager* pinned_iters_mgr)
  {
    pinned_iters_mgr_ = pinned_iters_mgr;
  }

private:
  int do_prefetch_data_block(const bool force_send_req);
  int merge_io_request(const BlockDataHandle<Block> &handle, bool &need_send_req);
  int send_merged_io_request();
  void release_handle(BlockDataHandle<Block> &handle)
  {
    if (pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled()) {
      handle.reset(pinned_iters_mgr_);
    } else {
      handle.reset();
    }
  }
  int64_t prefetched_cnt()
  {
    return data_block_prefetch_pos_ - data_block_cur_pos_;
  }
  bool need_prefetch()
  {
    return DISABLE_PREFETCH == data_block_prefetch_cnt_
        ? true
        : prefetched_cnt() <= (DATA_BLOCK_HANDLE_CNT / 2) && prefetched_cnt() <= (data_block_prefetch_cnt_ / 2);
  }
  int64_t calc_prefetch_cnt()
  {
    int64_t prefetch_cnt = 1;
    if (DISABLE_PREFETCH != data_block_prefetch_cnt_) {
      // prefetch cnt is the min one between the number of reset slots in
      // block_handles_ or data_block_prefetch_cnt_
      prefetch_cnt = std::min(DATA_BLOCK_HANDLE_CNT - prefetched_cnt(), data_block_prefetch_cnt_);
      // re-caculate data_block_prefetch_cnt_
      data_block_prefetch_cnt_ = std::min(DATA_BLOCK_HANDLE_CNT / 2, data_block_prefetch_cnt_ * 2);
    }
    return prefetch_cnt;
  }
  BlockDataHandle<Block> &get_block_handle(const int64_t pos)
  {
    return block_handles_[pos % DATA_BLOCK_HANDLE_CNT];
  }

private:
  static const int64_t DATA_BLOCK_HANDLE_CNT = 16;
  const ScanParam *scan_param_;
  TablePrefetchHelper table_prefetch_helper_;
  BlockIter index_block_iter_;
  // used as a fixed queue, data_block_prefetch_pos_ is the header
  // and data_block_prefetch_pos_ is the tail
  BlockDataHandle<Block> block_handles_[DATA_BLOCK_HANDLE_CNT];
  int64_t data_block_prefetch_cnt_;
  int64_t data_block_prefetch_pos_;
  int64_t data_block_cur_pos_;
  // the count of blocks added into block cache
  uint64_t add_blocks_;
  // merge io request
  IOReqMergeHandle io_merge_handle_;
  db::PinnedIteratorsManager* pinned_iters_mgr_;
  bool enable_prefetch_;
  bool valid_;
  bool is_inited_;
};

class SSTableScanIterator : public InternalIterator
{
public:
  SSTableScanIterator() : pinned_iters_mgr_(nullptr),
                          first_time_prefetch_(false),
                          valid_(false),
                          is_inited_(false)
  {}
  virtual ~SSTableScanIterator()
  {}

  int init(const ScanParam &param);
  virtual void Seek(const common::Slice& target) override;
  virtual void SeekForPrev(const common::Slice& target) override;
  virtual void SeekToFirst() override;
  virtual void SeekToLast() override;
  virtual void Next() override;
  virtual void Prev() override;

  virtual bool Valid() const override
  {
    return valid_;
  }
  virtual common::Slice key() const override
  {
    assert(Valid());
    return data_block_iter_.key();
  }
  virtual common::Slice value() const override
  {
    assert(Valid());
    return data_block_iter_.value();
  }
  virtual common::Status status() const override
  {
    return status_;
  }
  virtual void SetPinnedItersMgr(db::PinnedIteratorsManager* pinned_iters_mgr) override
  {
    pinned_iters_mgr_ = pinned_iters_mgr;
    block_prefetch_helper_.set_pinned_iters_mgr(pinned_iters_mgr);
  }
  virtual bool IsKeyPinned() const override
  {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() && data_block_iter_.IsKeyPinned();
  }
  virtual bool IsValuePinned() const override
  {
    return pinned_iters_mgr_ && pinned_iters_mgr_->PinningEnabled() && data_block_iter_.IsValuePinned();
  }
  virtual RowSource get_source() const override
  {
    return data_block_iter_.get_source();
  }
  virtual void set_end_key(const common::Slice& end_key_slice, const bool need_seek_end_key) override
  {
    end_ikey_ = end_key_slice;
    first_time_prefetch_ = end_ikey_.size() > 0 ? true : false;
    block_prefetch_helper_.set_end_key(end_key_slice, need_seek_end_key);
  }
  // for reentrancy
  void reset()
  {
    valid_ = false;
    first_time_prefetch_ = end_ikey_.size() > 0 ? true : false;
    block_prefetch_helper_.reset();
  }

private:
  void skip_empty_data_blocks_forward();
  void skip_empty_data_blocks_backward();

private:
  ScanParam scan_param_;
  common::ReadOptions read_options_;
  BlockPrefetchHelper block_prefetch_helper_;
  BlockIter data_block_iter_;
  db::PinnedIteratorsManager* pinned_iters_mgr_;
  common::Status status_;
  bool first_time_prefetch_;
  bool valid_;
  bool is_inited_;
};

} // namespace table
} // namespace xengine
