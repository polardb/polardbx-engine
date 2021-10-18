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
#include "logger/logger.h"
#include "split_compaction.h"
#include "db/builder.h"
#include "db/merge_helper.h"
#include "db/table_cache.h"
#include "db/table_properties_collector.h"
#include "db/version_edit.h"
#include "options/db_options.h"
#include "storage/extent_space_manager.h"
#include "storage/io_extent.h"
#include "table/extent_table_factory.h"
#include "table/extent_table_reader.h"
#include "table/merging_iterator.h"
#include "table/table_builder.h"
#include "util/arena.h"
#include "util/file_reader_writer.h"
#include "memory/mod_info.h"
#include "util/stop_watch.h"
#include "xengine/env.h"
#include "xengine/options.h"
#include "xengine/xengine_constants.h"

using namespace xengine;
using namespace table;
using namespace util;
using namespace common;
using namespace db;
using namespace monitor;

namespace xengine {
namespace storage {



SplitCompaction::SplitCompaction(const CompactionContext &context,
                                 const ColumnFamilyDesc &cf,
                                 memory::ArenaAllocator &arena)
    : GeneralCompaction(context, cf, arena), split_keys_(stl_alloc_) {}

SplitCompaction::~SplitCompaction() {
  close_extent();
  cleanup();
}

void SplitCompaction::add_split_key(const Slice &split_key)
{
  const Slice key = split_key.deep_copy(arena_);
  split_keys_.push_back(key);
}

int SplitCompaction::cleanup() {
  GeneralCompaction::cleanup();
  split_keys_.clear();
  return 0;
}

int SplitCompaction::split_extents(ExtSEIterator &iterator)
{
  int ret = 0;
  iterator.seek_to_first();
  size_t key_idx = 0;
  size_t last_idx = iterator.get_extent_index();
  int64_t extent_level = iterator.get_extent_level();
  SEIterator::IterLevel output_level = SEIterator::kDataEnd;
  Slice split_key = split_keys_.at(key_idx);
  const Comparator *cmp = context_.data_comparator_;
  if (nullptr == cmp) {
    ret = Status::kInvalidArgument;
    COMPACTION_LOG(WARN, "cmp or extent builder is null", K(ret));
  }
  while(iterator.valid() && Status::kOk == ret) {
    output_level = SEIterator::kDataEnd;
    if (FAILED(iterator.get_special_curent_row(output_level))) {
      COMPACTION_LOG(WARN, "get special current row failed", K(ret));
    } else if (SEIterator::kKVLevel == output_level) {
      if (key_idx < split_keys_.size()
          && cmp->Compare(iterator.get_start_ukey(), split_key) >= 0) {
        // next split point
        while (++key_idx < split_keys_.size()) {
          // skip the split_key has the same compare result with last_split_key
          split_key = split_keys_.at(key_idx);
          if (cmp->Compare(iterator.get_start_ukey(), split_key) < 0) {
            break;
          }
        }
        if (FAILED(close_split_extent(extent_level))) {
          COMPACTION_LOG(WARN, "failed to close extent", K(ret));
        }
      } else if (iterator.get_extent_index() != last_idx) {
        // next extent
        last_idx = iterator.get_extent_index();
        if (FAILED(close_split_extent(extent_level))) {
          COMPACTION_LOG(WARN, "failed to close extent", K(ret));
        } else {
          extent_level = iterator.get_extent_level();
        }
      }
      if (FAILED(ret)) {
      } else if (FAILED(open_extent())) {
        COMPACTION_LOG(WARN, "open extent failed", K(ret));
      } else if (nullptr == extent_builder_) {
        ret = Status::kCorruption;
        COMPACTION_LOG(WARN, "extent builder is null", K(ret));
      } else if (FAILED(extent_builder_->Add(iterator.get_key(), iterator.get_value()))) {
        COMPACTION_LOG(WARN, "add kv failed", K(ret), K(iterator.get_key()));
      } else if (FAILED(iterator.next())) {
        COMPACTION_LOG(WARN, "failed to call next", K(ret));
      }
    } else if (SEIterator::kDataEnd != output_level) {
      ret = Status::kCorruption;
      COMPACTION_LOG(WARN, "invalid output level", K(ret), K((int64_t)output_level));
    }
  }
  if (Status::kOk == ret) {
    if (FAILED(close_split_extent(extent_level))) {
      COMPACTION_LOG(WARN, "failed to close extent", K(ret));
    } else if (key_idx != split_keys_.size()) {
      ret = Status::kCorruption;
      COMPACTION_LOG(WARN, "invalid split idx",
          K(ret), K(key_idx), K(split_keys_.size()));
    }
  }
  return ret;
}

int SplitCompaction::run()
{
  int ret = 0;
  COMPACTION_LOG(INFO, "begin to run a split task.",
                 K(cf_desc_.column_family_id_),
                 K(cf_desc_.column_family_name_.c_str()),
                 K(merge_extents_.size()));
  ExtSEIterator *iterator = ALLOC_OBJECT(ExtSEIterator, arena_,
      context_.data_comparator_, context_.internal_comparator_);
  //change_info_.task_type_ = db::TaskType::SPLIT_TASK;
  if (nullptr == iterator) {
    ret = Status::kMemoryLimit;
    COMPACTION_LOG(WARN, "alloc memory for iterator failed", K(ret));
  } else {
    stats_.record_stats_.total_input_extents += merge_extents_.size();
    iterator->set_compaction(this);
    for (size_t idx = 0; idx < merge_extents_.size(); ++idx) {
      MetaDescriptor &meta = merge_extents_.at(idx);
      iterator->add_extent(meta);
    }
    if (FAILED(split_extents(*iterator))) {
      COMPACTION_LOG(WARN, "split extents failed", K(ret));
    }
  }
  clear_current_readers();
  // fixed #19943705 memory leak
  FREE_OBJECT(ExtSEIterator, arena_, iterator);
  return ret;
}

int SplitCompaction::close_split_extent(const int64_t level)
{
  int ret = 0;
  if (!write_extent_opened_) {
    return 0;
  }
  // just split one extent to two extents, not change other info
  if (FAILED(extent_builder_->Finish())) {
    COMPACTION_LOG(WARN, "write extent failed",
                   K(extent_builder_->status().getState()));
  } else {
    stats_.record_stats_.merge_output_extents += mini_tables_.metas.size();
    for (FileMetaData &meta : mini_tables_.metas) {
      stats_.record_stats_.merge_output_records += meta.num_entries;
      stats_.record_stats_.total_output_bytes += meta.fd.file_size;
      COMPACTION_LOG(INFO, "[SPLIT]GENERATE new extent", K(meta));
    }
    assert(1 == level);
    //TODO: @yuanfeng ugly, level_ should been set to 1 in split task
//    for (uint32_t i = 0; i < change_info_.add_extents_.size(); ++i) {
//      change_info_.add_extents_.at(i).level_ = level;
//    }
  }
  write_extent_opened_ = false;
  clear_current_writers();
  return ret;
}


}  // namespace storage
}  // namespace xengine
