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

#ifndef XENGINE_STORAGE_MINOR_COMPACTION_H_
#define XENGINE_STORAGE_MINOR_COMPACTION_H_

#include "xengine/options.h"
#include "xengine/status.h"
#include "xengine/xengine_constants.h"
#include "xengine/cache.h"
#include "xengine/table.h"

#include "compact/compaction.h"
#include "compact/reuse_block_merge_iterator.h"
#include "compact/compaction_stats.h"
#include "compact/compaction_iterator.h"

#include "monitoring/instrumented_mutex.h"
#include "storage/storage_manager.h"
#include "table/block.h"
#include "table/two_level_iterator.h"
#include "util/aligned_buffer.h"
#include "util/threadpool_imp.h"

namespace xengine {
namespace db {
class CompactionIterator;
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
class Block;
class BlockIter;
class TableBuilder;
}

namespace storage {

class ExtentSpaceManager;
class AsyncRandomAccessExtent;

struct BlockInfo;
struct MultiWayBlocks;

class MinorTask;
class MinorTaskPool;

class MinorCompaction;
#ifndef COMP_IO_CLASS
#define COMP_IO_CLASS
#endif

class MinorSequentialExtentIndexIterator : public RangeIterator {
 public:
  MinorSequentialExtentIndexIterator()
      : compaction_(nullptr),
        current_iterator_(nullptr),
        extent_index_(0) {}
  virtual ~MinorSequentialExtentIndexIterator();

  void set_compaction(MinorCompaction *compaction) { compaction_ = compaction; }
  void add_extent(const storage::MetaDescriptor &extent) {
    extent_list_.push_back(extent);
    iterator_index_list_.push_back(-1);
  }
  void reset();
  int64_t size() const { return (int64_t)extent_list_.size(); }
  int64_t current() const { return extent_index_; }
  void maybe_clear_iterators(
      const db::InternalKeyComparator *internal_comparator,
      const common::Slice& last_key);

  void print_debug_info();

  virtual bool middle() const override {
    assert(valid());
    return current_iterator_->middle();
  }

  virtual bool valid() const override {
    return (extent_index_ < (int64_t)extent_list_.size() &&
            nullptr != current_iterator_ && current_iterator_->valid());
  }

  virtual common::Status status() const override {
    if (nullptr != current_iterator_) {
      return current_iterator_->status();
    }
    return common::Status(common::Status::kOk);
  }

  virtual common::Slice user_key() const override {
    assert(valid());
    return current_iterator_->user_key();
  }

  virtual common::Slice key() const override {
    assert(valid());
    return current_iterator_->key();
  }

  virtual common::Slice value() const override {
    assert(valid());
    return current_iterator_->value();
  }

  virtual void seek(const common::Slice &lookup_key) override {
    UNUSED(lookup_key);
    current_iterator_ = nullptr;
  }

  virtual void seek_to_first() override;
  virtual void next() override;

  virtual const MetaDescriptor &get_meta_descriptor() const override {
    assert(valid());
    return current_iterator_->get_meta_descriptor();
  }

 private:
  void create_current_iterator();
  void prefetch_next_extent();

 private:
  MinorCompaction *compaction_;
  RangeIterator *current_iterator_;
  int64_t extent_index_;
  util::autovector<int64_t> iterator_index_list_;
  util::autovector<storage::MetaDescriptor> extent_list_;
};

class MinorTaskPool {
 public:
  explicit MinorTaskPool();
  ~MinorTaskPool() {}
  
  // Note: update and reap should be called by different threads.
  void update(MinorTask* minor); 
  int reap(MinorTask* minor);

 private:
  monitor::InstrumentedMutex mutex_;
  monitor::InstrumentedCondVar fill_cv_;
  std::unordered_set<MinorTask*> done_minors_;
};


// We keep a fix sized slide window of handled MinorTasks.
// For one single MinorCompaction, it could be organized as below.
//  [Trival Minor 0] -> [Minor 1] -> [Minor 2] -> [Minor 3] -> [Minor 4]
// |                                          |
// |--------- Slide window -------------------|
//                     |- After M0 finished  -|
//                     |---------------- Supply M3 --------|
static const int64_t MINOR_WAY_NUM = 4;
//static const int64_t SPLIT_SINGLE_WAY_NUM = MAX_EXTENT_SIZE / DATA_BLOCK_SIZE / 4;
//static const int64_t SPLIT_SINGLE_WAY_SIZE = (1 * 1024 * 1024); 
static const int64_t SPLIT_SINGLE_WAY_NUM = MAX_EXTENT_SIZE / DATA_BLOCK_SIZE;
static const int64_t SPLIT_SINGLE_WAY_SIZE = (4 * 1024 * 1024); 
static const int64_t MAX_MINOR_TASK_NUM = 1025; 
// Not used
static const int64_t SPLIT_ALL_WAY_NUM = SPLIT_SINGLE_WAY_NUM * MINOR_WAY_NUM;
static const int64_t FPGA_MAX_BLOCK_SIZE = 18 * 1024;

enum class MinorTaskReason {
  kUnknown = 0,
  kSplitOverSize,
  kSplitTrivalOverSize,
  kSplitTrival,
  kSplitClearCut,
  kSplitRemain,
};

class MinorCompaction : public GeneralCompaction {
 public:
  MinorCompaction(const CompactionContext &context, const ColumnFamilyDesc &cf);
  ~MinorCompaction();

  //int add_merge_batch(
  //    const ReuseBlockMergeIterator::MetaDescriptorList &extents);
  virtual int run() override;
  virtual int cleanup() override;

  MinorTask* get_one_minor();
  MinorTask* create_one_minor();

  // Update group information by the newly created minor
  //int maybe_update_minor_group(MinorTask* minor);
  int maybe_add_minor_list(MinorTask* minor);

  void update_task_status(MinorTask *task);

  CompactionScheduler* get_scheduler() {
    assert(context_.compaction_scheduler_ != nullptr);
    return context_.compaction_scheduler_;
  }

#ifndef NDEBUG
  int TEST_clip_block(BlockInfo* in_block, storage::Range& valid_range,
      BlockInfo* out_block, util::WritableBuffer* buf) {
    return clip_block(in_block, valid_range, out_block, buf);
  }
#endif


 private:
  friend class MinorSequentialExtentIndexIterator;

  static const int64_t RESERVE_READER_NUM = 64;
  
  typedef util::BinaryHeap<RangeIterator *,
          MinRawIteratorComparator<RangeIterator>> MergerMinIterHeap;

  struct ReaderRep {
    int64_t extent_id_;
    table::TableReader *table_reader_;
    table::BlockIter *index_iterator_;  // first level, index
    storage::DataBlockIndexRangeIterator *block_range_iterator_;
//    const common::XengineSchema *schema_;
  };

  struct MergeStats {
    common::Slice min_key_;
    common::Slice max_key_;
    DECLARE_TO_STRING();
  };

  struct StreamStatus {
    bool middle_;
    std::deque<MetaDescriptor> pending_data_blocks_;
    StreamStatus() { reset(); }
    void reset() {
      middle_ = false;
      pending_data_blocks_.clear();
    }
    void step() {
      middle_ = false;
    }
    void step_and_add_block(MetaDescriptor& md) {
      // Note: this is shallow copy
      pending_data_blocks_.push_back(md);
      middle_ = true;
    }
  };

  //int open_extent();
  //int close_extent();
  int stream_oversize(const size_t num_limit, const size_t size_limit);
  int merge_extents(const int64_t batch_index,
                    const storage::BlockPosition &batch);

  int down_level_extent(const MetaDescriptor &extent);
  //int copy_data_block(const MetaDescriptor &data_block);

  //int init_data_blocks_merger(const int64_t batch_index,
  //                             const storage::BlockPosition &batch);

  //int get_table_reader(const MetaDescriptor &extent,
  //                     table::TableReader *&reader);
  int get_extent_index_iterator(const MetaDescriptor &extent,
                                table::TableReader *&reader,
                                table::BlockIter *&index_iterator);
  int get_extent_index_iterator(table::TableReader *reader,
                                table::BlockIter *&index_iterator);
  int create_extent_index_iterator(const MetaDescriptor &extent,
                                   int64_t &iterator_index,
                                   RangeIterator *&iterator);
  int destroy_extent_index_iterator(const int64_t iterator_index,
                                    RangeIterator *iterator);
  int create_data_block_iterator(const storage::BlockPosition &data_block,
                                 table::TableReader *reader,
                                 table::BlockIter *&block_iterator);
  int destroy_data_block_iterator(table::BlockIter *block_iterator);
  //int fetch_next_data_block(int64_t data_stream_way, int64_t &extent_index,
  //                          int64_t &block_index, BlockPosition &data_block);
  //int prefetch_extent(int64_t extent_id);
  //AsyncRandomAccessExtent *get_async_extent_reader(int64_t extent_id) const;
  //void destroy_async_extent_reader(int64_t extent_id);

  int init_extent_iterators(const int64_t batch_index,
      const storage::BlockPosition &batch);

  int init_extent_index_heap();
  int reset_extent_index_heap();
  int forward_extent_index_heap(RangeIterator *iterator);
  //int forward_extent_index_heap(RangeIterator *iterator, int32_t way);
  
  void get_middle_way_num(int32_t &middle_way_num) const;

  void clear_current_readers();
  //void clear_current_writers();

  int get_block_info(const storage::MetaDescriptor &meta, BlockInfo* &block_info);
//  int change_block_schema(const MetaDescriptor& meta, table::TableReader *table_reader,
//      const XengineSchema* src_schema, const XengineSchema* dst_schema,
//      BlockInfo* out_block, util::WritableBuffer* buf);
  //int get_raw_data_block(const storage::MetaDescriptor &md,
  //    common::Slice* data_block);
  int create_minor_task(MultiWayBlocks* input_multiway_blocks,
      const MinorTaskReason reason, MinorTask* &minor);
  int add_minor_task(const MinorTaskReason& reason, bool keep_block_info = false);
  int maybe_schedule_minor_task();
  int collect_minor_task(bool force_wait_minor = false);
  int clip_block(BlockInfo* in_block, storage::Range& valid_range,
      BlockInfo* out_block, util::WritableBuffer* buf);

  int add_minor_list(MinorTask *minor);
  void update_merge_stats(const storage::Range& range);
  bool window_full();
  //int check_and_maybe_resplit();

  //int reset_group();
  bool intersect(const db::InternalKeyComparator* ik_comparator,
      const storage::Range& lrange, const storage::Range& rrange);

  int wait_for_minor(MinorTask *minor);
  int flush_minor_task(MinorTask* minor);
  int flush_data(const BlockInfo* block_info);

  int flush_and_clip_minor_task(MinorTask* minor);
  //void record_compaction_iterator_stats(const db::CompactionIterator &iter,
  //                                      CompactRecordStats &stats);

  // print current input and output data for debug;
  void print_debug_info();

  // TODO default options for create table builder, remove future
  //std::vector<std::unique_ptr<db::IntTblPropCollectorFactory>> props_;
  //std::string compression_dict_;
  common::CompressionType compression_type_;

  memory::ArenaAllocator arena_;

  // options for create builder and reader;
  //CompactionContext context_;
  //ColumnFamilyDesc cf_desc_;

  // all extents need to merge in one compaciton task.
  //util::autovector<storage::MetaDescriptor> merge_extents_;
  // [start, end) sub task in %merge_extents_;
  //util::autovector<storage::BlockPosition> merge_batch_indexes_;

  // Record the current handled MinorTask or stream number,
  // used to check wether window full.
  int64_t minor_task_cnt_;
  std::atomic<int> active_stream_num_;
  monitor::InstrumentedMutex group_mutex_;
  //storage::Range merge_range_;

  // We use thread unsafe list
  std::list<MinorTask*> all_minor_list_;
  std::list<MinorTask*> pending_minor_list_;

  //TODO
  MinorTaskPool* pool_;

  int32_t pending_block_num_;
  int32_t middle_way_num_;
  int8_t last_way_index_;
  // concatenate sequential data block indexes in same way;
  MinorSequentialExtentIndexIterator extent_index_iterators_[MINOR_WAY_NUM];
  StreamStatus stream_status_[MINOR_WAY_NUM];

  MergerMinIterHeap extent_index_heap_;
  MultiWayBlocks* reused_multiway_blocks_;    // store the reused blocks
  MultiWayBlocks* overlap_multiway_blocks_;   // store the overlap blocks
 
  MergeStats merge_stats_;
  storage::Range current_range_;

  //std::unordered_map<int64_t, AsyncRandomAccessExtent *> prefetch_extents_;

  //bool write_extent_opened_;
  //std::unique_ptr<table::TableBuilder> extent_builder_;
  //db::MiniTables mini_tables_;
  //storage::ChangeInfo change_info_;
  //util::WritableBuffer block_buffer_;

  util::autovector<ReaderRep, RESERVE_READER_NUM> reader_reps_;

  // Compaction result, written meta, statistics;
  //Compaction::Statstics stats_;

  // No copying allowed
  MinorCompaction(const MinorCompaction&) = delete;
  void operator=(const MinorCompaction&) = delete;
};

// A wrapper struct used for FPGA input and ouput.
struct BlockInfo {
  common::Slice raw_block_;
  // Note: BlockContents memory
  table::BlockContents block_contents_;
  common::Slice block_stats_content_;
  common::Slice first_key_;
  common::Slice last_key_;
  size_t data_size_;          // used to estimate the output buffer size
  bool has_large_value_;
  int64_t extent_id_;
  common::CompressionType compression_type_;
  // ref count only for MultiWayBlocks
  std::atomic<int> refs_;

  BlockInfo()
    : raw_block_(),
      block_stats_content_(),
      first_key_(),
      last_key_(),
      data_size_(0),
      has_large_value_(false),
      extent_id_(0),
      compression_type_(common::CompressionType::kNoCompression),
      refs_(0) {}

  ~BlockInfo();

  void ref() { refs_.fetch_add(1, std::memory_order_relaxed); }

  bool unref() {
    int old_refs = refs_.fetch_sub(1, std::memory_order_relaxed);
    assert(old_refs > 0);
    return old_refs == 1;
  }
  void clear();

  DECLARE_TO_STRING();
};

// A multi-way struct consists of BlockInfo
struct MultiWayBlocks {
  const db::InternalKeyComparator *internal_comparator_;
  int block_num_;
  int way_num_;
  storage::Range range_;
  size_t data_size_[MINOR_WAY_NUM];
  std::vector<BlockInfo*> block_infos_[MINOR_WAY_NUM];

  explicit MultiWayBlocks(const db::InternalKeyComparator* comparator);
  ~MultiWayBlocks();

  // FPGA has single way limit. 
  bool over_size(const size_t num_limit, const size_t size_limit);
  void clear();
  int update_range(const BlockInfo* block_info);
  int append_block_info(const int way, BlockInfo* block_info);

  // Try to append block_info to the same way as much as possible
  // while the result way number don't exceed the way_limit.
  bool tight_append_block_info(const int way_limit,
      BlockInfo* block_info, int8_t* way_index);

  void print_debug_info();

#ifndef NDEBUG
  bool check_data();
  void Dump(const std::string filename);
#endif
};

// MinorTask is a task unit.
class MinorTask {
 public:
  explicit MinorTask();
  MinorTask(const CompactionContext &context, const ColumnFamilyDesc &cf_desc,
      const MinorTaskReason& reason, MultiWayBlocks* input_multiway_blocks,
      MultiWayBlocks* result_blocks,
      int64_t local_task_id,
      common::CompressionType compression_type);
  ~MinorTask();

  //if ready ,post to queue
  CompactionMode mode() {
    return comp_mode_;
  }
  int prepare();
  int run_cpu_task_if_any();
  int run_fpga_task_if_any();
  int parse_output_blocks();
  int build_task_info(bool rebuild = false);
  bool is_trival() {
    return trival_minor_ == 1 ? true : false;
  }
  int task_done() {
    if (trival_minor_ || task_done_status_.load())
      return 1;
    return 0;
  }
  void set_task_done() { 
    this->task_done_status_.store(1);
  }
  void set_mode(CompactionMode p_mode);
  CompactionMode get_mode() { return this->comp_mode_; }

  static void run_cpu_task(CompIO* param);
  
  int run();

  // Note: call cleanup before dstor to free the input_multiway_blocks
  void cleanup();
  //void cleanup(memory::ArenaAllocator& arena);

  static std::string print_compaction_stats(AsyncCompactionTaskInfo *task);

  // shallow copy, MinorTask is resposilbe for the free
  void set_valid_range(storage::Range& in_range) {
    valid_range_ = in_range;
  }
  storage::Range& valid_range() { return valid_range_; }

  void print_debug_info();

  storage::Range& range() {
    return range_;
  }
#ifndef NDEBUG
  void set_range(storage::Range& rhs) {
    range_ = rhs;
  }
  void set_compaction_context(CompactionContext ct) {
    context_ = ct;
  }
#endif

  MultiWayBlocks* result_blocks() {
    return result_blocks_;
  }

  int build_compaction_task_info(memory::ArenaAllocator* arena,
                                 AsyncCompactionTaskInfo *task,
                                 size_t level_type,
                                 uint64_t min_ref_seq_no, 
                                 char **input_blocks,
                                 size_t **input_blocks_size,
                                 size_t *num_input_blocks,
                                 size_t num_ways,
                                 CBFunc func);

  static int check_cpu_and_fpga(MinorTask* task);

  static void cpu_task_done_callback(size_t level_type,
                                 uint64_t min_ref_seq_no, 
                                 char **input_blocks,
                                 size_t **input_blocks_size,
                                 size_t *num_input_blocks,
                                 size_t num_ways,
                                 char *output,
                                 size_t *out_blocks_size,
                                 size_t *num_output_blocks,
                                 void *group,
                                 CompStats *stats,
                                 int rc);
  static void fpga_task_done_callback(size_t level_type,
                                 uint64_t min_ref_seq_no, 
                                 char **input_blocks,
                                 size_t **input_blocks_size,
                                 size_t *num_input_blocks,
                                 size_t num_ways,
                                 char *output,
                                 size_t *out_blocks_size,
                                 size_t *num_output_blocks,
                                 void *group,
                                 CompStats *stats,
                                 int rc);
  uint64_t get_input_way_num()  { return this->total_way_num_;}
  uint64_t get_input_blocks_num() {return this->total_blocks_num_;}
  uint64_t get_input_blocks_bytes() {return this->total_blocks_bytes_;}

  uint64_t                                  task_id_;
  uint64_t                                  cf_id_;
  std::string                               cf_name_;
  AsyncCompactionTaskInfo*                  cpu_task_info_;
  AsyncCompactionTaskInfo*                  fpga_task_info_;
  uint64_t                                  total_way_num_;
  uint64_t                                  total_blocks_num_;
  uint64_t                                  total_blocks_bytes_;
  

 private:
  friend class CompactionScheduler;
  friend class MinorCompaction;

  int to_aligned_buffer(memory::ArenaAllocator& arena,
      MultiWayBlocks* multiway_blocks,
      char** &blocks_buffer,
      size_t** &blocks_size, 
      size_t* &blocks_num,
      size_t &way_num);

  int parse_fpga_blocks();
  int parse_block_stats(const int block_index, BlockInfo* &info);

  memory::ArenaAllocator* arena_;
  CompactionContext context_;
  ColumnFamilyDesc cf_desc_;
  storage::Range range_;
  storage::Range valid_range_;
  //common::Slice valid_end_key_;

  bool trival_minor_;      // trival minor has only 1 way, don't need to merge
  MultiWayBlocks* input_multiway_blocks_;
  MultiWayBlocks* result_blocks_;
  
  common::CompressionType compression_type_;

  uint64_t                                 level_type_;
  uint64_t                                 min_ref_seq_;
  std::mutex                               task_done_mutex_;
  std::condition_variable                  task_done_cv_;
  std::atomic<bool>                        task_done_status_;
  std::atomic<int>                         task_done_ref_;
  CompactionMode                           comp_mode_;
  void*                                    cb_data_;

  char**                                    input_blocks_buffer_;
  uint64_t**                                input_blocks_size_;
  uint64_t*                                 input_blocks_num_;
  uint64_t                                  input_way_num_;  

  size_t                                    output_buf_size_;
  char*                                     output_blocks_;
  uint64_t*                                 output_blocks_size_;
  uint64_t                                  output_blocks_num_;

  MinorTaskReason reason_;
  CompStats stats_;
  uint64_t local_task_id_;

  // No copying allowed
  MinorTask(const MinorTask&) = delete;
  void operator=(const MinorTask&) = delete;
};

bool TEST_check_order(const std::list<MinorTask*> list, const db::InternalKeyComparator *cmp);

}  // namespace storage
}  // namespace xengine

#endif
