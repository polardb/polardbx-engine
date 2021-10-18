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
#include "compact/mt_ext_compaction.h"
#include "compact/new_compaction_iterator.h"
#include "compact/task_type.h"
#include "table/extent_table_builder.h"

namespace xengine {
namespace storage {
using namespace common;
using namespace table;
using namespace util;
using namespace memory;

MtExtCompaction::MtExtCompaction(const CompactionContext &context,
                                 const ColumnFamilyDesc &cf,
                                 ArenaAllocator &arena)
    : GeneralCompaction(context, cf, arena),
      mem_se_iterators_(nullptr) {
}

MtExtCompaction::~MtExtCompaction() {
  close_extent();
  cleanup();
}

int MtExtCompaction::cleanup() {
  int ret = GeneralCompaction::cleanup();
  mem_iterators_.clear();
  if (nullptr != mem_se_iterators_) {
    for (int64_t i = 0; i < (int64_t)mem_iterators_.size(); ++i) {
      mem_se_iterators_[i].~MemSEIterator();
    }
  }
  mem_se_iterators_ = nullptr;
  return ret;
}

int MtExtCompaction::add_mem_iterators(autovector<InternalIterator *> &iters) {
  int ret = 0;
  for (size_t i = 0; i < iters.size(); ++i) {
    if (nullptr != iters.at(i)) {
      iters.at(i)->SeekToFirst();
      if (iters.at(i)->Valid()) {
        // only add not null mem
        mem_iterators_.push_back(iters.at(i));
      }
    }
  }
  return ret;
}

int MtExtCompaction::update_row_cache() {
  int ret = Status::kOk;
  uint64_t start_micros = context_.cf_options_->env->NowMicros();
  if (nullptr != context_.cf_options_ && nullptr != context_.cf_options_->row_cache) {
    for (int64_t i = 0; i < (int64_t)mem_iterators_.size() && SUCC(ret); ++i) {
      InternalIterator *mem_iter = mem_iterators_.at(i);
      if (nullptr != mem_iter) {
        mem_iter->SeekToFirst();
        while(mem_iter->Valid() && SUCC(ret)) {
          if (FAILED(ExtentBasedTableBuilder::update_row_cache(
              cf_desc_.column_family_id_, mem_iter->key(), mem_iter->value(), *context_.cf_options_))) {
            XENGINE_LOG(WARN, "failed to update row cache", K(ret), K(mem_iter->key()), K(mem_iter->value()));
          } else {
            mem_iter->Next();
          }
        }
      }
    }
  }
  uint64_t use_micros = context_.cf_options_->env->NowMicros() - start_micros;
  XENGINE_LOG(INFO, "update row cache ok", K(use_micros));
  return ret;
}

int MtExtCompaction::run() {
  int ret = 0;
  stats_.record_stats_.start_micros = context_.cf_options_->env->NowMicros();
  COMPACTION_LOG(INFO, "begin to run flush to level1 task",
      K(cf_desc_.column_family_id_),K(mem_iterators_.size()));
  if (mem_iterators_.size() <= 0) {
    XENGINE_LOG(INFO, "memtable is empty, no use to do mt_ext_compaction");
    return ret;
  }
  if (FAILED(build_mem_se_iterators())) {
    COMPACTION_LOG(WARN, "failed to build mem_se_iterators", K(ret));
  } else {
    // build multiple_se_iterators
    MultipleSEIterator *merge_iterator = nullptr;
    if (merge_batch_indexes_.size() > 0) {
      const BlockPosition &batch = merge_batch_indexes_.at(0);
      if (FAILED(build_multiple_seiterators(0, batch, merge_iterator))) {
        COMPACTION_LOG(WARN, "failed to build multiple seiterators",
            K(ret), K(batch.first), K(batch.second));
      }
      stats_.record_stats_.total_input_extents += (batch.second - batch.first);
    }
    if (SUCC(ret)) {
      NewCompactionIterator *compactor = nullptr;
      if (FAILED(build_mem_merge_iterator(merge_iterator))) {
        COMPACTION_LOG(WARN, "failed to build mem merge iterator", K(ret));
      } else if (FAILED(merge_extents(merge_iterator, &flush_minitables_))) {
        COMPACTION_LOG(WARN, "merge extents failed", K(ret));
      } else if (FAILED(update_row_cache())) {
        COMPACTION_LOG(WARN, "failed to update row cache", K(ret));
      }
    }
    FREE_OBJECT(MultipleSEIterator, arena_, merge_iterator);
    clear_current_readers();
  }
  // merge extents end
  if (SUCC(ret) && FAILED(close_extent(&flush_minitables_))) {
    COMPACTION_LOG(WARN, "close extent failed.", K(ret));
  }
  if (nullptr != mini_tables_.change_info_) {
    mini_tables_.change_info_->task_type_ = db::TaskType::FLUSH_LEVEL1_TASK;
  }
  stats_.record_stats_.end_micros = context_.cf_options_->env->NowMicros();
  stats_.record_stats_.micros = stats_.record_stats_.end_micros - stats_.record_stats_.start_micros;
  if (0 == stats_.record_stats_.micros) {
    stats_.record_stats_.micros = 1;
  }

  COMPACTION_LOG(INFO, "compact ok.",
      K(context_.output_level_),
      K(cf_desc_.column_family_id_),
      "total time", stats_.record_stats_.micros / 1000000);
  // todo calc merge_rate, merge_ratio
//  MeasureTime(context_.cf_options_->statistics, COMPACTION_TIME,
//              stats_.record_stats_.micros);
  return ret;
}

int MtExtCompaction::build_mem_merge_iterator(MultipleSEIterator *&merge_iterator) {
  int ret = 0;
  if (nullptr == merge_iterator) {
    merge_iterator = ALLOC_OBJECT(MultipleSEIterator, arena_,
      context_.data_comparator_, context_.internal_comparator_);
  }
  if (IS_NULL(merge_iterator) || IS_NULL(mem_se_iterators_)) {
    ret = Status::kMemoryLimit;
    COMPACTION_LOG(WARN, "failed to alloc memory for multiple_se_iterators",
        K(ret), KP(merge_iterator));
  } else {
    for (int64_t i = 0; i < (int64_t)mem_iterators_.size(); ++i) {
      if (mem_se_iterators_[i].has_data()) {
        mem_se_iterators_[i].set_compaction(this);
        merge_iterator->add_se_iterator(&mem_se_iterators_[i]);
      }
    }
  }
  return ret;
}

int MtExtCompaction::build_mem_se_iterators() {
  int ret = 0;
  int64_t mem_num = (int64_t)mem_iterators_.size();
  mem_se_iterators_ = static_cast<MemSEIterator *>(arena_.alloc(sizeof(MemSEIterator) * mem_num));
  if (IS_NULL(mem_se_iterators_)) {
    ret = Status::kMemoryLimit;
    COMPACTION_LOG(WARN, "alloc memory for se_iterators failed", K(ret), K(mem_num));
  }
  for (int64_t  i = 0; i < mem_num && SUCC(ret); ++i) {
    new(mem_se_iterators_ + i) MemSEIterator();
    mem_se_iterators_[i].set_mem_iter(mem_iterators_.at(i));
    // todo support schema
//    mem_se_iterators_[i].set_schema(schema_);
  }
  return ret;
}

} // storage
} // xengine
