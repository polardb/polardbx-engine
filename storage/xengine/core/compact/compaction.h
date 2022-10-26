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

#ifndef XENGINE_STORAGE_COMPACTION_H_
#define XENGINE_STORAGE_COMPACTION_H_

#include "compaction_stats.h"
#include "task_type.h"
#include "memory/page_arena.h"
#include "reuse_block_merge_iterator.h"
#include "storage/storage_manager.h"

#include "table/block.h"
#include "table/two_level_iterator.h"
#include "util/aligned_buffer.h"
#include "xengine/cache.h"
#include "xengine/env.h"
#include "xengine/options.h"
#include "util/threadpool_imp.h"

#ifdef FPGA_COMPACTION
#include "db-compaction-fpga/comp_aclr.h"
#include "db-compaction-fpga/comp_stats.h"
#else
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#include <inttypes.h>
#endif
#include "fpga/comp_stats.h"
#include "fpga/comp_io.h"
#endif

namespace xengine {

namespace db {
class InternalKeyComparator;
//enum CompactionTaskType : int;
};

namespace util {
class EnvOptions;
class Env;
class Comparator;
}

namespace common {
class ImmutableCFOptions;
class MutableCFOptions;
}

namespace table {
class BlockIter;
}

namespace storage {

class ExtentSpaceManager;
class StorageLogger;
class CompactionIterator;
class NewCompactionIterator;

// This function, which is merely used in compaction, is moved from lagacy
// db/compaction.h.
common::CompressionType GetCompressionType(
    const common::ImmutableCFOptions &ioptions,
    const common::MutableCFOptions &mutable_cf_options, int level,
    const bool enable_compression = true);


enum CompactionMode {
  kTypeCPU = 0,
  kTypeFPGA = 1,
  kTypeCheck = 2,
  kTypeMax = 3
};

struct AsyncCompactionTaskInfo {
public:
  AsyncCompactionTaskInfo() {
    comp_mode_ = kTypeMax;
    output_blocks_ = nullptr;
    output_blocks_size_ = nullptr;
    output_blocks_num_ = 0;
  }
  ~AsyncCompactionTaskInfo() {}
public:
  CompactionMode   comp_mode_;
  char             *output_blocks_;
  size_t           *output_blocks_size_;
  size_t           output_blocks_num_; 
  CompIO           comp_param_;
  CompStats        comp_stats_;
};
class MinorCompaction;
class MinorTask;

//Compaction Task control block
class CompactionTCB {
public:
  uint64_t            task_id_;
  uint64_t            cpu_failed_;
  uint64_t            fpga_failed_;
  MinorCompaction     *minor_compaction_;
  MinorTask           *minor_task_;
//following for statistic
  uint64_t            queue_ts_;  //
  uint64_t            start_process_ts_;  //post to queue timestame 
  uint64_t            process_done_ts_;
  uint64_t            start_done_ts_;
  uint64_t            task_done_ts_;
};
//
// compaction scheduler, we will schedule both cpu and fpga
// we can run in three mode 
//  (1)kTypeCPU only use cpu 
//  (2)kTypeFPGA use FPGA if availale, use cpu only if this task can not h
//               be handle by fpga
//  (3)kTypeCheck run both in CPU and FPGA to check the correctness
//
class CompactionScheduler {
public:
  CompactionScheduler(const CompactionMode mode_p,
                      const int fpga_device_id,
                      const uint32_t fpga_driver_thread_num,
                      const uint32_t cpu_compaction_thread_num,
                      monitor::Statistics* stats_p) : stats_(stats_p) {
    this->config_mode_ = mode_p;
    this->comp_task_id_.store(0);
    this->done_task_id_.store(0);
    this->trival_task_num_.store(0);
    this->succeed_fpga_task_num_.store(0);
    this->succeed_cpu_task_num_.store(0);
    this->failed_cpu_task_num_.store(0);
    this->failed_fpga_task_num_.store(0);
    this->fpga_device_id_ = fpga_device_id;
    this->fpga_driver_thread_num_ = fpga_driver_thread_num;
    this->cpu_compaction_thread_num_ = cpu_compaction_thread_num;
    this->inited_ = false;
    this->fpga_driver_ = nullptr;
    this->stop_.store(false);
    this->thread_num_.store(0);
    this->thread_pool_impl_ = nullptr;
  }
  ~CompactionScheduler() {
    if (!inited_) return;
    //TODO WAIT all work done before deconstruct fpga_driver_
#ifdef FPGA_COMPACTION
    if (fpga_driver_ != nullptr) {
      //google::ShutdownGoogleLogging();
      delete fpga_driver_;
    }
#endif
    this->stop();
    assert(get_cpu_task_num() == 0);
    assert(get_fpga_task_num() == 0);
    assert(get_done_task_num() == 0);
    //this->thread_pool_impl_->JoinAllThreads();
    if (nullptr != thread_pool_impl_)
      delete thread_pool_impl_;
  }

  int init();
  void switch_mode_if_needed();
  bool stopped() {return this->stop_.load();}
  void stop() { 
    if (stopped()) {
      return;
    }
    this->stop_.store(true);
    //wait all thread exit
    this->thread_pool_impl_->JoinAllThreads();
  }
 
  uint64_t get_thread_num() { 
    return this->thread_num_.load(std::memory_order_relaxed);
  } 
  void maybe_add_worker();
  void increase_worker_thread() {
    if (this->thread_num_.load() >= cpu_compaction_thread_num_)
      return;
    this->thread_num_.fetch_add(1);
    this->thread_pool_impl_->Schedule(&bg_work_minor_compaction, 
                                      this, nullptr, nullptr);
  }
  void decrease_worker_thread() {
    this->thread_num_.fetch_sub(1);
  }
  void check_and_maybe_dump_status();
  void record_tcb_stats(CompactionTCB* tcb);
  uint64_t get_trival_task_num() {
    return this->trival_task_num_.load(std::memory_order_relaxed);
  }
  uint64_t get_succeed_fpga_task_num() {
    return this->succeed_fpga_task_num_.load(std::memory_order_relaxed);
  }
  uint64_t get_succeed_cpu_task_num() {
    return this->succeed_cpu_task_num_.load(std::memory_order_relaxed);
  }
  uint64_t get_failed_fpga_task_num() {
    return this->failed_fpga_task_num_.load(std::memory_order_relaxed);
  }
  uint64_t get_failed_cpu_task_num() {
    return this->failed_cpu_task_num_.load(std::memory_order_relaxed);
  }
  int post_compaction_task(MinorCompaction* minor_comapction,
                           std::vector<MinorTask*> &task_list);
  int do_compaction_job(uint64_t &job_done);
  int do_post_done_job(CompactionTCB* tcb);
  static void bg_work_minor_compaction(void *arg);

  uint64_t get_current_ts() { return util::Env::Default()->NowMicros();}
  uint64_t next_task_id() { return this->comp_task_id_.fetch_add(1); }
  uint64_t get_current_task_id() {
    return  this->comp_task_id_.load(std::memory_order_relaxed);
  }  

  bool put_task_to_device(AsyncCompactionTaskInfo *task) {
    assert(nullptr != task);
#ifdef FPGA_COMPACTION
    return this->fpga_driver_->MergeBlocks(&(task->comp_param_));
#else
    return false;
#endif
  }
  uint64_t get_cpu_task_num() {
    std::unique_lock<std::mutex> lck(cpu_queue_mutex_);
    return this->cpu_queue_.size();
  }
  void add_cpu_task(CompactionTCB * tcb) {
    assert(tcb != nullptr);
    std::unique_lock<std::mutex> lck(cpu_queue_mutex_);
    this->cpu_queue_.push_back(tcb);
    tcb->queue_ts_ = this->get_current_ts();
  }
  CompactionTCB* get_cpu_task() {
    CompactionTCB* tcb = nullptr;
    std::unique_lock<std::mutex> lck(cpu_queue_mutex_);
    if (this->cpu_queue_.size() == 0)
      return tcb;
    tcb = this->cpu_queue_.front();
    this->cpu_queue_.pop_front();
    tcb->start_process_ts_ = this->get_current_ts();
    return tcb;
  }
  uint64_t get_fpga_task_num() {
    std::unique_lock<std::mutex> lck(cpu_queue_mutex_);
    return this->fpga_queue_.size();
  }
  void add_fpga_task_hight_priority(CompactionTCB * tcb) {
    assert(tcb != nullptr);
    std::unique_lock<std::mutex> lck(fpga_queue_mutex_);
    this->fpga_queue_.push_front(tcb);
    tcb->queue_ts_ = this->get_current_ts();
  }
  void add_fpga_task(CompactionTCB * tcb) {
    assert(tcb != nullptr);
    std::unique_lock<std::mutex> lck(fpga_queue_mutex_);
    this->fpga_queue_.push_back(tcb);
    tcb->queue_ts_ = this->get_current_ts();
  }
  CompactionTCB* get_fpga_task() {
    CompactionTCB* tcb = nullptr;
    std::unique_lock<std::mutex> lck(fpga_queue_mutex_);
    if (this->fpga_queue_.size() == 0)
      return tcb;
    tcb = this->fpga_queue_.front();
    this->fpga_queue_.pop_front();
    tcb->start_process_ts_ = this->get_current_ts();
    return tcb;
  }
  uint64_t get_done_task_num() {
    std::unique_lock<std::mutex> lck(cpu_queue_mutex_);
    return this->done_queue_.size();
  }
  void add_done_task(CompactionTCB * tcb);
  CompactionTCB* get_done_task() {
    CompactionTCB* tcb = nullptr;
    std::unique_lock<std::mutex> lck(done_queue_mutex_);
    if (this->done_queue_.size() == 0)
      return tcb;
    tcb = this->done_queue_.front();
    this->done_queue_.pop_front();
    tcb->start_done_ts_ = this->get_current_ts();
    return tcb;
  }
  uint64_t get_ongoing_task_num() {
    uint64_t done_id = this->done_task_id_.load(std::memory_order_relaxed); 
    uint64_t current_id = this->comp_task_id_.load(std::memory_order_relaxed);
    return current_id > (done_id + 1) ? (current_id - done_id - 1) : 0;
  }
  void set_mode(int run_mode) {
    assert(run_mode >= 0);
    assert(run_mode < kTypeMax);
    current_mode_.store(run_mode);
  }
  CompactionMode get_mode() {
    return  static_cast<CompactionMode>(current_mode_.load());
  }
  std::string get_mode_str() {
    std::string ret = "unkown";
    if (get_mode() == kTypeCPU)
      ret = "cpu";
    else if (get_mode() == kTypeFPGA) 
      ret = "fpga";
    else {
      assert(get_mode() == kTypeCheck);
      ret = "check";
    }
    return ret;
  }

public:
  bool                                 inited_;
  std::atomic<int>                     current_mode_;
  CompactionMode                       config_mode_;
  std::atomic<uint64_t>                comp_task_id_;
  std::atomic<uint64_t>                done_task_id_;

  //statistic
  std::atomic<uint64_t>                trival_task_num_;
  std::atomic<uint64_t>                succeed_fpga_task_num_;
  std::atomic<uint64_t>                succeed_cpu_task_num_;
  std::atomic<uint64_t>                failed_fpga_task_num_;
  std::atomic<uint64_t>                failed_cpu_task_num_;

  std::atomic<bool>                    stop_;
  monitor::Statistics*                 stats_;

  std::atomic<uint64_t>                thread_num_;  
  util::ThreadPoolImpl*                thread_pool_impl_;
  //FPGA
  int                                  fpga_device_id_;
  uint32_t                             fpga_driver_thread_num_;
  std::atomic<int>                     fpga_status_; //0 ok, otherwsie error
#ifdef FPGA_COMPACTION
  CompAclr                             *fpga_driver_; 
#else
  void*                                fpga_driver_;
#endif
  //CPU
  uint32_t                             cpu_compaction_thread_num_;

  std::mutex                           cpu_queue_mutex_;
  std::deque<CompactionTCB*>           cpu_queue_;

  std::mutex                           fpga_queue_mutex_;
  std::deque<CompactionTCB*>           fpga_queue_;

  std::mutex                           done_queue_mutex_;
  std::deque<CompactionTCB*>           done_queue_;
};

void CompactionMainLoop(void *arg);

struct CompactionContext {
  const std::atomic<bool> *shutting_down_;
  const std::atomic<bool> *bg_stopped_;
  const std::atomic<int64_t> *cancel_type_;
  const common::ImmutableCFOptions *cf_options_;
  const common::MutableCFOptions *mutable_cf_options_;
  const util::EnvOptions *env_options_;
  // user Comparator in data block level;
  const util::Comparator *data_comparator_;
  const db::InternalKeyComparator *internal_comparator_;
  ExtentSpaceManager *space_manager_;
  int64_t table_space_id_;
  common::SequenceNumber earliest_write_conflict_snapshot_;
  std::vector<common::SequenceNumber> existing_snapshots_;
  // maybe 0 (StreamCompaction), or 1 (MinorCompaction for FPGA)
  // or 2 (NewStreamCompaction)
  int32_t minor_compaction_type_;
  CompactionScheduler *compaction_scheduler_;
  // maybe 0, 1, or 2, default is 1
  int32_t output_level_;
  db::TaskType task_type_;
  // Intra Level-0 need to force all the reused and newly created extents
  // to have the same layer sequence.
  // Selected layer sequence should be different from the old layer sequences.
  // We use smallest (Extent.type_.sequence_-1) as the selected layer sequence.
  // Default: -1, means have not been set.
  int64_t force_layer_sequence_;
  storage::StorageLogger *storage_logger_;
  bool enable_thread_tracking_;
  bool need_check_snapshot_;

  CompactionContext()
      : shutting_down_(nullptr),
        bg_stopped_(nullptr),
        cancel_type_(nullptr),
        cf_options_(nullptr),
        mutable_cf_options_(nullptr),
        env_options_(nullptr),
        data_comparator_(nullptr),
        internal_comparator_(nullptr),
        space_manager_(nullptr),
        table_space_id_(-1),
        earliest_write_conflict_snapshot_(0),
        minor_compaction_type_(0),
        compaction_scheduler_(nullptr),
        output_level_(1),
        task_type_(db::TaskType::INVALID_TYPE_TASK),
        force_layer_sequence_(-1),
        storage_logger_(nullptr),
        enable_thread_tracking_(false),
        need_check_snapshot_(true)
  {
    existing_snapshots_.clear();
  }

  bool valid() const {
    return nullptr != shutting_down_ && nullptr != bg_stopped_ &&
           nullptr != cf_options_ &&
           nullptr != mutable_cf_options_ && nullptr != env_options_ &&
           nullptr != data_comparator_ && nullptr != internal_comparator_ &&
           nullptr != space_manager_ && table_space_id_ >= 0 &&
           nullptr != storage_logger_;
  }
};

struct ColumnFamilyDesc {
  int32_t column_family_id_;
  std::string column_family_name_;
  ColumnFamilyDesc() : column_family_id_(0), column_family_name_("default") {}
  ColumnFamilyDesc(int32_t id, const std::string &name)
      : column_family_id_(id), column_family_name_(name) {}
};

class Compaction {
 public:
  // Compaction output
  struct Statstics {
    CompactRecordStats record_stats_;
    CompactPerfStats perf_stats_;
    MinorCompactStats minor_stats_;
  };

  enum Level {
    L0 = 0,
    L1 = 1,
    L2 = 2,
  };

 public:
  virtual ~Compaction(){};
  // input extent meta data iterator;
  virtual int add_merge_batch(
      const MetaDescriptorList &extents,
      const size_t start, const size_t end) = 0;
  virtual int run() = 0;
  virtual int cleanup() = 0;

  virtual const Statstics &get_stats() const = 0;
  virtual storage::ChangeInfo &get_change_info() = 0;
  virtual const int64_t *get_input_extent() const = 0;
};

// compaction history information schema
struct CompactionJobStatsInfo {
  uint32_t subtable_id_;
  int64_t sequence_;
  int type_;
  Compaction::Statstics stats_;
};

class GeneralCompaction : public Compaction {
  static const int64_t DEFAULT_ROW_LENGTH = 8 * 1024;  // 8kb
  static const int64_t RESERVE_READER_NUM = 64;
  static const int64_t RESERVE_MERGE_WAY_SIZE = 16;
 public:
  GeneralCompaction(const CompactionContext &context,
                    const ColumnFamilyDesc &cf,
                    memory::ArenaAllocator &arena);
  virtual ~GeneralCompaction();

  virtual int run();
  virtual int cleanup();

  // input extent meta data iterator;
  virtual int add_merge_batch(
      const MetaDescriptorList &extents,
      const size_t start, const size_t end);

  virtual const Statstics &get_stats() const { return stats_; }
  virtual storage::ChangeInfo &get_change_info() {
    return change_info_;
  }

  virtual const int64_t *get_input_extent() const {
    return input_extents_;
  }

  size_t get_extent_size() const { return merge_extents_.size(); }
  // set level2's largest key
  void set_level2_largest_key(const common::Slice *l2_largest_key) {
    l2_largest_key_ = l2_largest_key;
  }
  // set delete percent
  void set_delete_percent(const int64_t delete_percent) {
    delete_percent_ = delete_percent;
  }
  int down_level_extent(const MetaDescriptor &extent);
  int copy_data_block(const MetaDescriptor &data_block
                      /*const common::XengineSchema *schema*/);
  int create_extent_index_iterator(const MetaDescriptor &extent,
                                   size_t &iterator_index,
                                   DataBlockIterator *&iterator,
                                   ExtSEIterator::ReaderRep &rep);
  int destroy_extent_index_iterator(const int64_t iterator_index);
  int delete_extent_meta(const MetaDescriptor &extent);
  bool check_do_reuse(const MetaDescriptor &meta) const;
 protected:
  friend class ExtSEIterator;

  int open_extent();
  int close_extent(db::MiniTables *flush_tables = nullptr);

  void start_record_compaction_stats();
  void stop_record_compaction_stats();

  int build_multiple_seiterators(const int64_t batch_index,
      const storage::BlockPosition &batch,
      MultipleSEIterator *&merge_iterator);
  int build_compactor(NewCompactionIterator *&compactor,
      MultipleSEIterator *merge_iterator);

  int merge_extents(MultipleSEIterator *&merge_iterator,
      db::MiniTables *flush_tables = nullptr);

  int prefetch_extent(int64_t extent_id);

  int get_table_reader(const MetaDescriptor &extent,
                       table::TableReader *&reader);
  int get_extent_index_iterator(const MetaDescriptor &extent,
                                table::TableReader *&reader,
                                table::BlockIter *&index_iterator);
  int get_extent_index_iterator(table::TableReader *reader,
                                table::BlockIter *&index_iterator);

  int create_data_block_iterator(const storage::BlockPosition &data_block,
                                 table::TableReader *reader,
                                 table::BlockIter *&block_iterator);
  int destroy_data_block_iterator(table::BlockIter *block_iterator);

  AsyncRandomAccessExtent *get_async_extent_reader(int64_t extent_id) const;
  void destroy_async_extent_reader(int64_t extent_id, bool is_reuse = false);

  int switch_schema_for_block(const MetaDescriptor &data_block,
                              /*const common::XengineSchema *src_schema,*/
                              table::TableReader *table_reader);
  virtual void clear_current_readers();
  virtual void clear_current_writers();
  void record_compaction_iterator_stats(
      const NewCompactionIterator &compactor,
      CompactRecordStats &stats);

 protected:
  using PrefetchExtentMap = std::unordered_map<int64_t, AsyncRandomAccessExtent *,
  std::hash<int64_t>, std::equal_to<int64_t>,
  memory::stl_adapt_allocator<std::pair<int64_t,
  AsyncRandomAccessExtent *>, memory::ModId::kCompaction>>;

  // TODO default options for create table builder, remove future
  std::vector<std::unique_ptr<db::IntTblPropCollectorFactory>> props_;
  std::string compression_dict_;
  // options for create builder and reader;
  CompactionContext context_;
  ColumnFamilyDesc cf_desc_;

  // all extents need to merge in one compaciton task.
  MetaDescriptorList merge_extents_;
  // [start, end) sub task in %merge_extents_;
  BlockPositionList merge_batch_indexes_;
  PrefetchExtentMap prefetch_extents_;

  // compaction writer,
  bool write_extent_opened_;
  std::unique_ptr<table::TableBuilder> extent_builder_;
  util::WritableBuffer block_buffer_;
  db::MiniTables mini_tables_;
  storage::ChangeInfo change_info_;

  // Compaction result, written meta, statistics;
  Statstics stats_;
//  memory::ArenaAllocator row_arena_; // for schema switch

  // information schema compaction input extents
  int64_t input_extents_[3]; // todo to check max_level
  // for minor
  const common::Slice *l2_largest_key_;
  // for major
  int64_t delete_percent_;
  ExtSEIterator *se_iterators_;
  memory::ArenaAllocator arena_;
  memory::WrapAllocator wrap_alloc_;
  memory::stl_adapt_allocator<ExtSEIterator::ReaderRep> stl_alloc_;
  using ReadRepList = std::vector<ExtSEIterator::ReaderRep, memory::stl_adapt_allocator<ExtSEIterator::ReaderRep>>;
  ReadRepList reader_reps_;
};

}  // namespace storage
}  // namespace xengine

#endif
