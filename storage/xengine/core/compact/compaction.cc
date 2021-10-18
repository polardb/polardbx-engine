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

#include "compaction.h"
#include "compact/new_compaction_iterator.h"
#include "db/builder.h"
#include "db/merge_helper.h"
#include "db/table_cache.h"
#include "db/table_properties_collector.h"
#include "db/version_edit.h"
#include "logger/logger.h"
#include "memory/mod_info.h"
#include "options/db_options.h"
#include "storage/extent_space_manager.h"
#include "storage/io_extent.h"
#include "table/extent_table_factory.h"
#include "table/extent_table_reader.h"
#include "table/merging_iterator.h"
#include "table/table_builder.h"
#include "util/arena.h"
#include "util/file_reader_writer.h"
//#include "utilities/field_extractor/field_extractor.h"
#include "util/string_util.h"
#include "util/to_string.h"
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
using namespace memory;
using namespace logger;

#define START_PERF_STATS(item) \
  util::StopWatchNano item(context_.cf_options_->env, true);
#define RESTART_PERF_STATS(item) item.Start()
#define RECORD_PERF_STATS(item) \
  stats_.perf_stats_.item += item.ElapsedNanos(false);

namespace xengine {
namespace storage {

common::CompressionType GetCompressionType(
    const common::ImmutableCFOptions &ioptions,
    const common::MutableCFOptions &mutable_cf_options, int level,
    const bool enable_compression) {
  if (!enable_compression) {
    // disable compression
    return kNoCompression;
  }

  // TODO bottommost_compression deprecated

  // If the user has specified a different compression level for each level,
  // then pick the compression for that level.
  if (!ioptions.compression_per_level.empty()) {
    assert(level >= 0 && level <= 2);
    const int n = static_cast<int>(ioptions.compression_per_level.size()) - 1;
    return ioptions.compression_per_level[std::max(0, std::min(level, n))];
  } else {
    return mutable_cf_options.compression;
  }
}

GeneralCompaction::GeneralCompaction(const CompactionContext &context,
                                     const ColumnFamilyDesc &cf,
                                     ArenaAllocator &arena)
    : context_(context),
      cf_desc_(cf),
      write_extent_opened_(false),
//      row_arena_(DEFAULT_ROW_LENGTH, ModId::kCompaction),
      input_extents_{0,0,0},
      l2_largest_key_(nullptr),
      delete_percent_(0),
//      arena_(arena),
      arena_(CharArena::DEFAULT_PAGE_SIZE, ModId::kCompaction),
      stl_alloc_(WrapAllocator(arena_)),
      reader_reps_(stl_alloc_) {
  //change_info_.task_type_ = context_.task_type_;
  se_iterators_ = static_cast<ExtSEIterator *>(arena_.alloc(sizeof(ExtSEIterator) * RESERVE_MERGE_WAY_SIZE));
  for (int  i = 0; i < RESERVE_MERGE_WAY_SIZE; ++i) {
    new(se_iterators_ + i) ExtSEIterator(context.data_comparator_, context.internal_comparator_);
  }
}

GeneralCompaction::~GeneralCompaction() {
  close_extent();
  cleanup();
}

int GeneralCompaction::add_merge_batch(
    const MetaDescriptorList &extents,
    const size_t start, const size_t end) {
  int ret = Status::kOk;
  ExtentSpaceManager *space_manager = context_.space_manager_;
  if (end > extents.size() || start > extents.size() || start >= end) {
    ret = Status::kInvalidArgument;
    COMPACTION_LOG(WARN, "invalid end or start.", K(start), K(end), K(ret));
  } else if (nullptr == space_manager) {
    ret = Status::kNotInit;
    COMPACTION_LOG(WARN, "space manager is null", K(ret));
  } else {
    BlockPosition pos;
    pos.first = merge_extents_.size();
    ExtentMeta *extent_meta = nullptr;
    for (size_t i = start; i < end && SUCC(ret); i++) {
      // find the newest schema in current compaction task's extents
      MetaDescriptor meta_dt = extents.at(i);
      if (FAILED(space_manager->get_meta(meta_dt.get_extent_id(), extent_meta))) {
        COMPACTION_LOG(WARN, "failed to get meta", K(ret), K(meta_dt), K(i));
      } else if (nullptr == extent_meta /*|| nullptr == extent_meta->get_schema()*/) {
        ret = Status::kCorruption;
        COMPACTION_LOG(WARN, "extent_meta is null", K(ret), K(meta_dt), KP(extent_meta));
      } else {
        // todo support schema
//        meta_dt.set_schema(extent_meta->get_schema());
//        if (nullptr == mini_tables_.schema ||
//            mini_tables_.schema->get_schema_version() <
//                extent_meta->get_schema()->get_schema_version()) {
//          mini_tables_.schema = extent_meta->get_schema();
//        }
        // suppose extent hold the keys memory, no need do deep copy again.
        merge_extents_.emplace_back(meta_dt);
        assert(meta_dt.layer_position_.level_ >= 0);
        assert(meta_dt.layer_position_.level_ < 3 /*StorageManager::MAX_LEVEL*/);
        input_extents_[meta_dt.layer_position_.level_] += 1;
      }
    }
    if (SUCC(ret)) {
      pos.second = merge_extents_.size();
      merge_batch_indexes_.emplace_back(pos);
      MeasureTime(context_.cf_options_->statistics,
                  NUM_FILES_IN_SINGLE_COMPACTION, merge_extents_.size());
    }
  }
  return ret;
}

int GeneralCompaction::open_extent() {
  if (write_extent_opened_) {
    // already open
    return 0;
  }
  START_PERF_STATS(create_extent);
  mini_tables_.space_manager = context_.space_manager_;
  mini_tables_.change_info_ = &change_info_;
  mini_tables_.table_space_id_ = context_.table_space_id_;
  bool is_flush= TaskType::FLUSH_LEVEL1_TASK == context_.task_type_;
  storage::LayerPosition output_layer_position(context_.output_level_);
  if (0 == context_.output_level_) {
    output_layer_position.layer_index_ = storage::LayerPosition::NEW_GENERATE_LAYER_INDEX;
  } else {
    output_layer_position.layer_index_ = 0;
  }

  extent_builder_.reset(NewTableBuilder(
      *context_.cf_options_,
      *context_.internal_comparator_,
      &props_,
      cf_desc_.column_family_id_,
      cf_desc_.column_family_name_,
      &mini_tables_,
      GetCompressionType(*context_.cf_options_, *context_.mutable_cf_options_,
                         context_.output_level_) /* compression type */,
      context_.cf_options_->compression_opts,
      output_layer_position,
      &compression_dict_,
      true,
      is_flush));
  if (nullptr == extent_builder_.get()) {
    COMPACTION_LOG(WARN,
                   "create new table builder error",
                   K(cf_desc_.column_family_id_),
                   K(cf_desc_.column_family_name_.c_str()));
    return Status::kMemoryLimit;
  }
  write_extent_opened_ = true;
  RECORD_PERF_STATS(create_extent);
  return 0;
}

int GeneralCompaction::close_extent(MiniTables *flush_tables) {
  if (!write_extent_opened_) {
    return 0;
  }
  START_PERF_STATS(finish_extent);
  int ret = extent_builder_->Finish();
  RECORD_PERF_STATS(finish_extent);
  if (Status::kOk != ret) {
    COMPACTION_LOG(ERROR, "write extent failed", K(ret));
  } else {
    int64_t compaction_delete_percent = context_.mutable_cf_options_->compaction_delete_percent;
    stats_.record_stats_.merge_output_extents += mini_tables_.metas.size();

    COMPACTION_LOG(INFO, "compaction generate new extent stats", "column_family_id", cf_desc_.column_family_id_, "extent_count", mini_tables_.metas.size());
    for (FileMetaData &meta : mini_tables_.metas) {
      stats_.record_stats_.total_output_bytes += meta.fd.file_size;
      if (nullptr != flush_tables) {
        // for mt_ext task, need preheat table_cache
        flush_tables->metas.push_back(meta);
      }
    }
    for (table::TableProperties &prop : mini_tables_.props) {
      stats_.record_stats_.merge_datablocks += prop.num_data_blocks;
    }
  }
  write_extent_opened_ = false;
  clear_current_writers();
  return ret;
}

void GeneralCompaction::start_record_compaction_stats() {
  assert(nullptr != context_.cf_options_ && nullptr != context_.cf_options_->env);
  if (2 == context_.output_level_) {
    COMPACTION_LOG(INFO, "begin to run major compaction.",
        K(cf_desc_.column_family_id_),K(cf_desc_.column_family_name_.c_str()));
  } else if (1 == context_.output_level_) {
    COMPACTION_LOG(INFO, "begin to run minor compaction.",
        K(cf_desc_.column_family_id_),K(cf_desc_.column_family_name_.c_str()));
  } else {
    COMPACTION_LOG(INFO, "begin to run intra_l0 compaction.",
        K(cf_desc_.column_family_id_),K(cf_desc_.column_family_name_.c_str()));
  }
  stats_.record_stats_.start_micros = context_.cf_options_->env->NowMicros();
}

void GeneralCompaction::stop_record_compaction_stats() {
  assert(nullptr != context_.cf_options_ && nullptr != context_.cf_options_->env);
  stats_.record_stats_.end_micros = context_.cf_options_->env->NowMicros();
  stats_.record_stats_.micros = stats_.record_stats_.end_micros - stats_.record_stats_.start_micros;
  if (stats_.record_stats_.micros == 0) {
    stats_.record_stats_.micros = 1;
  }
  MeasureTime(context_.cf_options_->statistics, COMPACTION_TIME,
              stats_.record_stats_.micros);

  // Write amplification = (not_reused_merge_input_extent) / (not_reused_input_level_extents)
  int64_t input_level_not_reused_extents = 0;
  if (0 != stats_.record_stats_.total_input_extents_at_l0) {
    input_level_not_reused_extents =
        stats_.record_stats_.total_input_extents_at_l0 - stats_.record_stats_.reuse_extents_at_l0;
  } else {
    input_level_not_reused_extents =
        stats_.record_stats_.total_input_extents_at_l1 - stats_.record_stats_.reuse_extents_at_l1;
  }
  stats_.record_stats_.write_amp = input_level_not_reused_extents == 0 ? 0 :
    ((stats_.record_stats_.total_input_extents - stats_.record_stats_.reuse_extents)
     / input_level_not_reused_extents);

  // Calulate rate if rate_limiter is set.
  int64_t old_total_bytes_through = 0;
  if (context_.cf_options_->rate_limiter != nullptr) {
    old_total_bytes_through = context_.cf_options_->rate_limiter->GetTotalBytesThrough();
  }
  char buf[64] = "NONE";
  if (context_.cf_options_->rate_limiter != nullptr) {
    snprintf (buf, 64, "%.2fMB/s",
        (context_.cf_options_->rate_limiter->GetTotalBytesThrough() -
         old_total_bytes_through) * 1000000.0 / stats_.record_stats_.micros
        / 1024 / 1024);
  }
  double merge_rate = (1000000.0 * (stats_.record_stats_.total_input_extents -
      stats_.record_stats_.reuse_extents) * MAX_EXTENT_SIZE /
      1024 / 1024 / stats_.record_stats_.micros);
  Slice merge_ratio(buf, 64);
  COMPACTION_LOG(INFO, "compact ok.",
      K(context_.output_level_),
      K(cf_desc_.column_family_id_),
      "total time", stats_.record_stats_.micros / 1000000,
      "merge rate", merge_rate,
      "merge ratio", merge_ratio.ToString(false).c_str());
}

int GeneralCompaction::create_extent_index_iterator(const MetaDescriptor &extent,
                                                    size_t &iterator_index,
                                                    DataBlockIterator *&iterator,
                                                    ExtSEIterator::ReaderRep &rep) {
  int ret = 0;
  if (IS_NULL(get_async_extent_reader(extent.block_position_.first))) {
    // if prefetch failed, just ignore it
    prefetch_extent(extent.block_position_.first);
  }
  iterator_index = reader_reps_.size();
  rep.extent_id_ = extent.block_position_.first;
  if (FAILED(get_extent_index_iterator(extent, rep.table_reader_, rep.index_iterator_))) {
    COMPACTION_LOG(WARN, "get extent index iterator failed.", K(ret));
  } else {
    MetaType type(MetaType::SSTable, MetaType::DataBlock, MetaType::InternalKey,
                  extent.type_.level_, extent.type_.way_, iterator_index);
    rep.block_iter_ = ALLOC_OBJECT(DataBlockIterator, arena_, type, rep.index_iterator_);
    if (nullptr == rep.block_iter_) {
      ret = Status::kMemoryLimit;
      COMPACTION_LOG(WARN, "create new block iterator error", K(ret));
    } else {
      stats_.record_stats_.total_input_bytes += rep.table_reader_->GetTableProperties()->data_size;
      iterator = rep.block_iter_;
      reader_reps_.emplace_back(rep);
    }
  }
  return ret;
}

int GeneralCompaction::down_level_extent(const MetaDescriptor &extent) {
  // move level 1 's extent to level 2;
  int ret = 0;
  COMPACTION_LOG(INFO, "reuse extent", K(extent),
      K(context_.output_level_), K(cf_desc_.column_family_id_));
  ExtentMeta *extent_meta = nullptr;
  LayerPosition layer_position = (0 == context_.output_level_)
                                 ? (LayerPosition(0, storage::LayerPosition::NEW_GENERATE_LAYER_INDEX))
                                 : (LayerPosition(context_.output_level_, 0));

  if (0 != extent.layer_position_.level_
      && (extent.layer_position_.level_ == context_.output_level_)) {
    // no need down
  } else if (FAILED(delete_extent_meta(extent))) {
    COMPACTION_LOG(WARN, "delete extent meta failed.", K(ret), K(extent));
  } else if (FAILED(context_.space_manager_->get_meta(extent.extent_id_, extent_meta))) {
    COMPACTION_LOG(WARN, "failed to get meta", K(ret), K(extent.extent_id_));
  } else if (FAILED(change_info_.add_extent(layer_position, extent.extent_id_))) {
    COMPACTION_LOG(WARN, "fail to reuse extent.", K(ret), K(extent));
  } else {
    destroy_async_extent_reader(extent.block_position_.first, true);
  }

  // stats
  stats_.record_stats_.reuse_extents += 1;
  if (1 == extent.type_.level_) {
    stats_.record_stats_.reuse_extents_at_l1 += 1;
  } else if (0 == extent.type_.level_) {
    stats_.record_stats_.reuse_extents_at_l0 += 1;
  }
  return ret;
}

bool GeneralCompaction::check_do_reuse(const MetaDescriptor &meta) const {
  bool bret = true;
  if (2 == context_.output_level_ && meta.delete_percent_ >= delete_percent_) {
    bret = false;
    COMPACTION_LOG(DEBUG, "REUSE CHECK: not do reuse", K(meta), K(delete_percent_));
  } else {
    COMPACTION_LOG(DEBUG, "REUSE CHECK: do reuse", K(meta), K(delete_percent_));
  }
  return bret;
}

int GeneralCompaction::copy_data_block(const MetaDescriptor &data_block
                                       /*const XengineSchema *schema*/) {
  int ret = 0;
  if (!write_extent_opened_) {
    FAIL_RETURN(open_extent());
  }

  TableReader *table_reader = reader_reps_[data_block.type_.sequence_].table_reader_;
  // covert row according to target_schema, then add it to extent_builder one
  // by one
//  if (IS_NULL(schema) || IS_NULL(mini_tables_.schema)) {
//    ret = Status::kNotInit;
//    COMPACTION_LOG(WARN, "block's or mini_tables' schema is null", K(ret), KP(schema));
//  } else if (mini_tables_.schema->get_schema_version() > schema->get_schema_version()) {
//    if (FAILED(switch_schema_for_block(data_block, schema, table_reader))) {
//      COMPACTION_LOG(WARN, "switch schema for block failed", K(ret), K(data_block));
//    }
//  } else {  // no need convert, row's schema is equal to the newest schema in
//            // the task
  COMPACTION_LOG(DEBUG, "NOT changed: copy block to dest.", K(data_block));

  ExtentBasedTable *reader = dynamic_cast<ExtentBasedTable *>(table_reader);
  BlockHandle handle(data_block.block_position_.first,
                     data_block.block_position_.second);
  int64_t block_size = handle.size() + kBlockTrailerSize;
  if (FAILED(block_buffer_.reserve(block_size))) {
    COMPACTION_LOG(WARN, "reserve buffer for read block failed", K(ret),
                   K(handle.offset()), K(handle.size()), K(data_block));
  } else {
    common::Slice block_data(block_buffer_.all_data(), block_size);
    if (FAILED(reader->get_data_block(handle, block_data))) {
      COMPACTION_LOG(WARN, "read data block from extent failed", K(ret), K(handle.offset()), K(handle.size()));
    } else if (FAILED(extent_builder_->AddBlock(block_data, data_block.value_, data_block.range_.end_key_))) {
      COMPACTION_LOG(ERROR, "add block to extent failed", K(data_block.range_.end_key_), K(data_block.range_.start_key_));
    }
  }
  if (SUCC(ret)) {
    stats_.record_stats_.total_input_bytes += block_size;
//    stats_.record_stats_.merge_input_records +=
    stats_.record_stats_.reuse_datablocks += 1;
    if (1 == data_block.type_.level_) {
      stats_.record_stats_.reuse_datablocks_at_l1 += 1;
    } else if (0 == data_block.type_.level_) {
      stats_.record_stats_.reuse_datablocks_at_l0 += 1;
    }
  }
  return ret;
}

int GeneralCompaction::destroy_extent_index_iterator(
    const int64_t iterator_index) {
  int ret = 0;
  if (iterator_index >= (int64_t)reader_reps_.size()) {
  } else {
    ExtSEIterator::ReaderRep &rep = reader_reps_[iterator_index];
    if (0 == rep.extent_id_) return ret;
    destroy_async_extent_reader(rep.extent_id_);
    if (nullptr != rep.table_reader_) {
//      delete rep.table_reader_;
      FREE_OBJECT(TableReader, arena_, rep.table_reader_);
//      rep.table_reader_ = nullptr;
    }
    if (nullptr != rep.block_iter_) {
      rep.block_iter_->~DataBlockIterator();
    }
    // reset reader_rep, avoid re-destroy
    rep.extent_id_ = 0;
  }
  stats_.record_stats_.merge_extents += 1;
  return ret;
}

int GeneralCompaction::delete_extent_meta(const MetaDescriptor &extent) {
  int ret = Status::kOk;
  COMPACTION_LOG(INFO, "delete extent", K(cf_desc_.column_family_id_), K(extent), K(context_.output_level_));
  ret = change_info_.delete_extent(extent.layer_position_, extent.extent_id_);
  return ret;
}

int GeneralCompaction::get_table_reader(const MetaDescriptor &extent,
                                        table::TableReader *&reader) {
  Status s;
  int ret = Status::kOk;
//  std::unique_ptr<RandomAccessFile> file;
  RandomAccessFile *file = nullptr;
  AsyncRandomAccessExtent *async_file = get_async_extent_reader(extent.block_position_.first);
  if (nullptr != async_file) {
//    file.reset(async_file);
    file = async_file;
  } else {
    //RandomAccessExtent *async_file = ALLOC_OBJECT(RandomAccessExtent, arena_);
//    file.reset(new RandomAccessExtent());
    file = ALLOC_OBJECT(RandomAccessExtent, arena_);
    RandomAccessExtent *extent_file = dynamic_cast<RandomAccessExtent *>(file);
    // used for cache key construct if cache_key changed
//    extent_file->set_subtable_id(cf_desc_.column_family_id_);
    if (IS_NULL(file)) {
      ret = Status::kMemoryLimit;
      COMPACTION_LOG(WARN, "alloc memory for file failed", K(ret), K(extent));
    } else {
      s = context_.space_manager_->get_random_access_extent(
          extent.block_position_.first, *extent_file);
      if (FAILED(s.code())) {
        COMPACTION_LOG(ERROR, "open extent for read failed.", K(ret), K(extent));
      }
    }
    if (FAILED(ret)) {
      FREE_OBJECT(RandomAccessExtent, arena_, extent_file);
    }
  }
  if (SUCC(ret)) {
//    std::unique_ptr<table::TableReader> table_reader;
//    std::unique_ptr<RandomAccessFileReader> file_reader(
//        new RandomAccessFileReader(file, context_.cf_options_->env,
//                                   context_.cf_options_->statistics, 0, nullptr,
//                                   context_.cf_options_, *context_.env_options_));

    table::TableReader *table_reader = nullptr;
    RandomAccessFileReader *file_reader = ALLOC_OBJECT(RandomAccessFileReader, arena_,
        file, context_.cf_options_->env, context_.cf_options_->statistics, 0, nullptr,
        context_.cf_options_, *context_.env_options_, true);
    s = context_.cf_options_->table_factory->NewTableReader(
        TableReaderOptions(*context_.cf_options_, *context_.env_options_,
                           *context_.internal_comparator_, nullptr, nullptr,
                           false, extent.type_.level_),
        file_reader, 2 * 1024 * 1024, table_reader, false, &arena_);
    if (!s.ok()) {
      COMPACTION_LOG(WARN, "create new table reader error", K(s.getState()),
          K(cf_desc_.column_family_id_), K(cf_desc_.column_family_name_.c_str()));
    } else {
//      reader = table_reader.release();
      reader = table_reader;
    }
  }
  return s.code();
}

int GeneralCompaction::get_extent_index_iterator(
    const MetaDescriptor &extent, table::TableReader *&table_reader,
    table::BlockIter *&index_iterator) {
  int ret = 0;

  START_PERF_STATS(read_extent);
  FAIL_RETURN(get_table_reader(extent, table_reader));
  RECORD_PERF_STATS(read_extent);

  FAIL_RETURN(get_extent_index_iterator(table_reader, index_iterator));
  return ret;
}

int GeneralCompaction::get_extent_index_iterator(
    table::TableReader *table_reader, table::BlockIter *&index_iterator) {
  int ret = 0;
  table::ExtentBasedTable *reader = dynamic_cast<ExtentBasedTable *>(table_reader);
  table::BlockIter *iterator = ALLOC_OBJECT(BlockIter, arena_);
  if (nullptr == iterator) {
    ret = Status::kMemoryLimit;
    COMPACTION_LOG(WARN, "create new iterator object error", K(ret));
  } else {
    START_PERF_STATS(read_index);
    ExtentBasedTable::IndexReader *index_reader = nullptr;
    reader->create_index_iterator(ReadOptions(), iterator, index_reader, arena_);

    index_iterator = iterator;
    RECORD_PERF_STATS(read_index);

    if (!iterator->status().ok()) {
      COMPACTION_LOG(WARN, "create new index iterator error(%s)", K(iterator->status().ToString().c_str()));
      ret = iterator->status().code();
    }
  }
  return ret;
}

int GeneralCompaction::create_data_block_iterator(
    const storage::BlockPosition &data_block,
    table::TableReader *table_reader,
    table::BlockIter *&iterator) {
  int ret = 0;
  assert(nullptr != table_reader);
  table::BlockHandle handle(data_block.first, data_block.second);
  START_PERF_STATS(read_block);
  ExtentBasedTable *reader = dynamic_cast<ExtentBasedTable *>(table_reader);
  table::BlockIter *input_iterator = MOD_NEW_OBJECT(ModId::kCompaction, BlockIter);
//  BlockIter *input_iterator = ALLOC_OBJECT(BlockIter, arena_);
  reader->create_data_block_iterator(ReadOptions(), handle, input_iterator);
  iterator = input_iterator;
  RECORD_PERF_STATS(read_block);
  bool in_cache = false;
  if (nullptr == iterator) {
    ret = Status::kMemoryLimit;
    COMPACTION_LOG(WARN, "create data block iterator error", K(ret), K(handle.offset()), K(handle.size()));
  } else if (iterator->status().code()) {
    COMPACTION_LOG(WARN, "create data block iterator error(%s)", K(iterator->status().ToString().c_str()));
    ret = iterator->status().code();
  } else if (1 == context_.output_level_
             && FAILED(reader->check_block_if_in_cache(handle, in_cache))) {
    COMPACTION_LOG(WARN, "check block if is in cache failed", K(handle.offset()), K(handle.size()));
  } else if (in_cache) {
//    if (FAILED(reader->erase_block_from_cache(handle))) {
//      COMPACTION_LOG(WARN, "erase block from cache failed", K(handle.offset()), K(handle.size()));
    if (nullptr != extent_builder_) {
      extent_builder_->set_in_cache_flag();
    }
  } else {}
  return ret;
}

int GeneralCompaction::destroy_data_block_iterator(
    table::BlockIter *block_iterator) {
  int ret = 0;
  if (nullptr != block_iterator) {
//    FREE_OBJECT(BlockIter, arena_, block_iterator);
    MOD_DELETE_OBJECT(BlockIter, block_iterator);
  }
  return ret;
}

int GeneralCompaction::build_multiple_seiterators(
    const int64_t batch_index,
    const storage::BlockPosition &batch,
    MultipleSEIterator *&merge_iterator) {
  int ret = 0;
  MultipleSEIterator *multiple_se_iterators =
      ALLOC_OBJECT(MultipleSEIterator, arena_,
      context_.data_comparator_, context_.internal_comparator_);
  if (nullptr == multiple_se_iterators) {
    ret = Status::kMemoryLimit;
    COMPACTION_LOG(WARN, "failed to alloc memory for multiple_se_iterators", K(ret));
  } else {
    for (int64_t index = batch.first; index < batch.second; ++index) {
      const storage::MetaDescriptor &extent = merge_extents_.at(index);
      se_iterators_[extent.type_.way_].add_extent(extent);
      if (1 == extent.layer_position_.level_) {
        stats_.record_stats_.total_input_extents_at_l1 += 1;
      } else if (0 == extent.layer_position_.level_) {
        stats_.record_stats_.total_input_extents_at_l0 += 1;
      }
    }
    for (int i = 0; i < RESERVE_MERGE_WAY_SIZE; ++i) {
      if (se_iterators_[i].has_data()) {
        se_iterators_[i].set_compaction(this);
        multiple_se_iterators->add_se_iterator(&se_iterators_[i]);
      }
    }
    merge_iterator = multiple_se_iterators;
  }
  return ret;
}

int GeneralCompaction::build_compactor(NewCompactionIterator *&compactor,
      MultipleSEIterator *merge_iterator) {
  int ret = 0;
  compactor = ALLOC_OBJECT(NewCompactionIterator, arena_,
      context_.data_comparator_, context_.internal_comparator_,
      kMaxSequenceNumber, &context_.existing_snapshots_,
      context_.earliest_write_conflict_snapshot_,
      true /*do not allow corrupt key*/,
      merge_iterator,
     /* mini_tables_.schema,*/
      arena_, change_info_, context_.output_level_/*output level*/,
      context_.shutting_down_, context_.bg_stopped_, context_.cancel_type_, true,
      context_.mutable_cf_options_->background_disable_merge);
      COMPACTION_LOG(INFO, "backgroud_merge is disabled, subtable_id: ", K(cf_desc_.column_family_id_));
  return ret;
}

int GeneralCompaction::merge_extents(MultipleSEIterator *&merge_iterator,
                                     MiniTables *flush_tables) {
  assert(reader_reps_.size() == 0);
  int ret = 0;
  // 1. build compaction_iterator
  NewCompactionIterator *compactor = nullptr;
  if (FAILED(build_compactor(compactor, merge_iterator))) {
    COMPACTION_LOG(WARN, "failed to build compactor", K(ret));
  } else if (nullptr == compactor) {
    ret = Status::kMemoryLimit;
    COMPACTION_LOG(WARN, "create compact iterator error.", K(ret));
  } else if (FAILED(compactor->seek_to_first())) {
    COMPACTION_LOG(WARN, "compactor seek to first failed.", K(ret));
  } else {
  }

  // 2. output the row or reuse extent/block
  while (SUCC(ret) && compactor->valid() && !context_.bg_stopped_->load()) {
    SEIterator::IterLevel output_level = compactor->get_output_level();
    if (SEIterator::kKVLevel == output_level) {
      if (!write_extent_opened_) {
        if (FAILED(open_extent())) {
          COMPACTION_LOG(WARN, "open extent failed", K(ret));
          break;
        }
      }
      if (FAILED(extent_builder_->Add(compactor->key(), compactor->value()))) {
        COMPACTION_LOG(WARN, "add kv failed", K(ret), K(compactor->key()));
      } else {
        stats_.record_stats_.merge_output_records += 1;
      }
      // reuse memory for row need switch schema
//      row_arena_.get_arena().fast_reuse();
    } else if (SEIterator::kExtentLevel == output_level) {
      if (FAILED(close_extent(flush_tables))) {
        COMPACTION_LOG(WARN, "close extent failed", K(ret));
      } else { // down l1 to l2 / l0 to l1 / l0 to l0
        // todo schema
//        const XengineSchema *schema = nullptr;
        ret = down_level_extent(compactor->get_reuse_meta());
      }
    } else if (SEIterator::kBlockLevel == output_level) {
//      const XengineSchema *schema = nullptr;
      MetaDescriptor meta = compactor->get_reuse_meta(/*schema*/);
      ret = copy_data_block(meta/*, schema*/);
    } else if (SEIterator::kDataEnd == output_level) {
      ret = Status::kCorruption;
      COMPACTION_LOG(WARN, "output level is invalid.", K(ret));
    } else {
      // has been reused, extent/block
    }
    // get next item
    if (SUCC(ret) && FAILED(compactor->next())) {
      COMPACTION_LOG(WARN, "compaction iterator get next failed.", K(ret));
    }
  }
  // deal with shutting down
  if (context_.shutting_down_->load(std::memory_order_acquire) ||
     context_.bg_stopped_->load(std::memory_order_acquire)) {
    // Since the next() method will check shutting down, invalid iterator
    // and set status_, we distinguish shutting down and normal ending
    ret = Status::kShutdownInProgress;  // cover ret, by design
    COMPACTION_LOG(WARN, "process shutting down or bg_stop, break compaction.", K(ret));
  }

  if (SUCC(ret)) {
    record_compaction_iterator_stats(*compactor, stats_.record_stats_);
  }

  FREE_OBJECT(NewCompactionIterator, arena_, compactor);
  FREE_OBJECT(MultipleSEIterator, arena_, merge_iterator);
  clear_current_readers();
  return ret;
}

int GeneralCompaction::run() {
  int ret = Status::kOk;
  start_record_compaction_stats();
  int64_t batch_size = (int64_t)merge_batch_indexes_.size();
  // merge extents start
  for (int64_t index = 0; SUCC(ret) && index < batch_size; ++index) {
    if (context_.shutting_down_->load(std::memory_order_acquire)) {
      ret = Status::kShutdownInProgress;
      COMPACTION_LOG(WARN, "process shutting down, break compaction.", K(ret));
      break;
    }
    const BlockPosition &batch = merge_batch_indexes_.at(index);
    int64_t extents_size = batch.second - batch.first;
    bool no_reuse = true;
    if (extents_size <= 0) {
      ret = Status::kCorruption;
      COMPACTION_LOG(ERROR, "batch is empty", K(batch.second), K(ret));
    } else if (1 == extents_size) {
      if (batch.first < 0 || batch.first >= (int64_t)merge_extents_.size()) {
        ret = Status::kCorruption;
        COMPACTION_LOG(WARN, "batch index is not valid > extents, BUG here!",
            K(batch.first), K(batch.second), K(merge_extents_.size()));
      } else if (!check_do_reuse(merge_extents_.at(batch.first))) {
        // (1->2)has many delete records or arrange extents' space, not to do reuse
      } else if (FAILED(close_extent())) {
        COMPACTION_LOG(WARN, "failed to close extent,write_opened, mini tables", K(ret),
            K(write_extent_opened_), K(mini_tables_.metas.size()), K(mini_tables_.props.size()));
      } else {
        no_reuse = false;
        const MetaDescriptor &extent = merge_extents_.at(batch.first);
        if (FAILED(down_level_extent(extent))) {
          COMPACTION_LOG(WARN, "down level extent failed", K(ret), K(extent));
        }
      }
    }
    if (SUCC(ret) && no_reuse) {
      START_PERF_STATS(create_extent);
      RECORD_PERF_STATS(create_extent);
      MultipleSEIterator *merge_iterator = nullptr;
      if (FAILED(build_multiple_seiterators(index, batch, merge_iterator))) {
        COMPACTION_LOG(WARN, "failed to build multiple seiterators",
            K(ret), K(index), K(batch.first), K(batch.second));
      } else if (FAILED(merge_extents(merge_iterator))) {
        COMPACTION_LOG(WARN, "merge extents failed", K(index), K(ret));
      }
    }
    stats_.record_stats_.total_input_extents += extents_size;
  }
  // merge extents end
  if (SUCC(ret) && FAILED(close_extent())) {
    COMPACTION_LOG(WARN, "close extent failed.", K(ret));
  }
  stop_record_compaction_stats();
  return ret;
}

int GeneralCompaction::prefetch_extent(int64_t extent_id) {
  int ret = 0;
  AsyncRandomAccessExtent *reader = ALLOC_OBJECT(AsyncRandomAccessExtent, arena_);
//  AsyncRandomAccessExtent *reader = new AsyncRandomAccessExtent();

  Status s;
  if (IS_NULL(reader)) {
    ret = Status::kMemoryLimit;
    COMPACTION_LOG(WARN, "failed to alloc memory for reader", K(ret));
  } else if (IS_NULL(context_.space_manager_)) {
    ret = Status::kAborted;
    COMPACTION_LOG(WARN, "space_manager_ is nullptr", K(ret));
  } else if (FAILED(context_.space_manager_->get_random_access_extent(extent_id, *reader).code())) {
    COMPACTION_LOG(ERROR, "open extent for read failed.", K(extent_id), K(ret));
  } else {
    // todo if cache key changed
//    reader->set_subtable_id(cf_desc_.column_family_id_);
    if (nullptr != context_.cf_options_->rate_limiter) {
      reader->set_rate_limiter(context_.cf_options_->rate_limiter);
    }
    if (FAILED(reader->prefetch())) {
      COMPACTION_LOG(WARN, "failed to prefetch", K(ret));
    } else {
      prefetch_extents_.insert(std::make_pair(extent_id, reader));
    }
  }
  return ret;
}

AsyncRandomAccessExtent *GeneralCompaction::get_async_extent_reader(
    int64_t extent_id) const {
  AsyncRandomAccessExtent *reader = nullptr;
  std::unordered_map<int64_t, AsyncRandomAccessExtent *>::const_iterator iter =
      prefetch_extents_.find(extent_id);
  if (iter != prefetch_extents_.end()) {
    reader = iter->second;
  }
  return reader;
}

void GeneralCompaction::destroy_async_extent_reader(int64_t extent_id, bool is_reuse) {
  std::unordered_map<int64_t, AsyncRandomAccessExtent *>::iterator iter =
      prefetch_extents_.find(extent_id);
  if (iter != prefetch_extents_.end() && nullptr != iter->second) {
    iter->second->reset();
    if (is_reuse) {
//      delete iter->second;
      FREE_OBJECT(AsyncRandomAccessExtent, arena_, iter->second);
    } else {
      // no need delete it->second(AsyncRandomAccessExtent), delete with table_reader
    }
    iter->second = nullptr;
  }
  if (iter != prefetch_extents_.end()) {
    prefetch_extents_.erase(iter);
  }
}

int GeneralCompaction::cleanup() {
  l2_largest_key_ = nullptr;
  if (nullptr != se_iterators_) {
    for (int64_t i = 0; i < RESERVE_MERGE_WAY_SIZE; ++i) {
      se_iterators_[i].~ExtSEIterator();
    }
  }
  se_iterators_ = nullptr;
  clear_current_readers();
  clear_current_writers();
  if (write_extent_opened_) {
    extent_builder_->Abandon();
    write_extent_opened_ = false;
  }
  // cleanup change_info_
  change_info_.clear();
  return 0;
}

/*int GeneralCompaction::switch_schema_for_block(
    const MetaDescriptor &data_block,
    const XengineSchema *src_schema,
    TableReader *table_reader) {
  int ret = 0;
  COMPACTION_LOG(DEBUG,
      "NOT changed: copy block to dest.need switch schema for each row.",
      K(data_block));
  BlockIter *block_iter = nullptr;
  if (FAILED(create_data_block_iterator(data_block.block_position_, table_reader, block_iter))) {
    COMPACTION_LOG(WARN, "create data block iterator failed", K(ret), K(data_block));
  } else if (IS_NULL(block_iter)) {
    ret = Status::kCorruption;
    COMPACTION_LOG(WARN, "block iter is null", K(ret), K(data_block));
  } else {
    block_iter->SeekToFirst();
    Slice tmp_value;
    while (block_iter->Valid() && SUCC(ret)) {
      Slice key = block_iter->key();
      Slice tmp_value = block_iter->value();
      uint64_t num = util::DecodeFixed64(key.data() + key.size() - 8);
      unsigned char c = num & 0xff;
      ValueType type = static_cast<ValueType>(c);
      if (kTypeValue == type && block_iter->value().size() > 0
          && FAILED(FieldExtractor::get_instance()->convert_schema(src_schema,
              mini_tables_.schema, block_iter->value(), tmp_value, row_arena_))) {
        COMPACTION_LOG(WARN, "switch value failed.", K(ret), K(block_iter->value()), K(tmp_value));
      } else if (FAILED(extent_builder_->Add(key, tmp_value))) {
        COMPACTION_LOG(WARN, "add kv failed", K(ret), K(key));
      } else {
        row_arena_.get_arena().fast_reuse();
        stats_.record_stats_.switch_output_records += 1;
        block_iter->Next();
      }
    }
  }
  return ret;
}
*/

void GeneralCompaction::clear_current_readers() {
  // clear se_iterators
  if (nullptr != se_iterators_) {
    for (int64_t i = 0; i < RESERVE_MERGE_WAY_SIZE; ++i) {
      se_iterators_[i].reset();
    }
  }

 //  incase some readers not used while error happens break compaction.
  for (int64_t i = 0; i < (int64_t)reader_reps_.size(); ++i) {
    destroy_extent_index_iterator(i);
  }
  reader_reps_.clear();
  for (auto &item : prefetch_extents_) {
    if (nullptr != item.second) {
      item.second->reset();
      // AsyncExtentExtent needs delete if not used.
//      delete item.second;
      FREE_OBJECT(AsyncRandomAccessExtent, arena_, item.second);
    }
  }
  prefetch_extents_.clear();
}

void GeneralCompaction::clear_current_writers() {
  mini_tables_.metas.clear();
  mini_tables_.props.clear();
}

void GeneralCompaction::record_compaction_iterator_stats(
    const NewCompactionIterator &iter, CompactRecordStats &stats) {
  const auto &c_iter_stats = iter.iter_stats();
  stats.merge_input_records += c_iter_stats.num_input_records;
  stats.merge_delete_records = c_iter_stats.num_input_deletion_records;
  stats.merge_corrupt_keys = c_iter_stats.num_input_corrupt_records;
  stats.single_del_fallthru = c_iter_stats.num_single_del_fallthru;
  stats.single_del_mismatch = c_iter_stats.num_single_del_mismatch;
  stats.merge_input_raw_key_bytes += c_iter_stats.total_input_raw_key_bytes;
  stats.merge_input_raw_value_bytes += c_iter_stats.total_input_raw_value_bytes;
  stats.merge_replace_records += c_iter_stats.num_record_drop_hidden;
  stats.merge_expired_records += c_iter_stats.num_record_drop_obsolete;
}

}  // namespace storage
}  // namespace xengine
