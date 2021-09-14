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
#include <assert.h>
#include "db/builder.h"
#include "table/block_based_table_builder.h"
#include "fpga_compaction_task_group.h"
#include "fpga/fpga_compaction_job.h"
#include "table/table_builder.h"
#include "table/extent_table_reader.h"
#include "table/extent_table_factory.h"
#include "db/dbformat.h"
#include "util/crc32c.h"
#include "util/coding.h"

using namespace xengine;
using namespace fpga;
using namespace table;
using namespace util;
using namespace db;

namespace xengine {
namespace fpga {

  FPGACompactionTaskGroup::FPGACompactionTaskGroup(CompactionContext context, ColumnFamilyDesc cf_desc) {
    context_ = context;
    cf_desc_ = cf_desc;
    num_tasks_finished_ = 0;
    ready_to_build_extent_ = false;
  }

  FPGACompactionTaskGroup::~FPGACompactionTaskGroup() {
    for (CompIO *task : tasks_) {
      delete task;
    }
  }

  void FPGACompactionTaskGroup::add_task(CompIO *task) {
    tasks_.emplace_back(task);
  }

  bool FPGACompactionTaskGroup::increase_finished_job_num() {
    ++ num_tasks_finished_;
    return num_tasks_finished_ == tasks_.size();
    //if (num_tasks_finished_ == tasks_.size()) {
       // all the tasks within the groupbeen finished
       // ready to build extent
       // open_extent();
       // build_extent();
       // close_extent();

       // delete this;
    //}
  }

  void FPGACompactionTaskGroup::flush() {
    open_extent();
    build_extent();
    close_extent();
  }

  size_t FPGACompactionTaskGroup::get_group_size() {
    return tasks_.size();
  }

  CompIO* FPGACompactionTaskGroup::get_task(size_t pos) {
    assert(pos < tasks_.size());
    return tasks_.at(pos);
  }

  int FPGACompactionTaskGroup::open_extent() {
    mini_tables_.space_manager = context_.space_manager_;
    extent_builder_.reset(NewTableBuilder(
                *context_.cf_options_, *context_.internal_comparator_, &props_,
                cf_desc_.column_family_id_, cf_desc_.column_family_name_, &mini_tables_,
                GetCompressionType(*context_.cf_options_, *context_.mutable_cf_options_, 1),
                context_.cf_options_->compression_opts, 1, &compression_dict_, true));
    if (nullptr == extent_builder_.get()) {
      ROCKS_LOG_WARN(context_.cf_options_->info_log,
                     "create new table builder error, cfd(%d, %s)",
                     cf_desc_.column_family_id_,
                     cf_desc_.column_family_name_.c_str());
      return Status::kMemoryLimit;
    }

    return 0;
  }

  void FPGACompactionTaskGroup::build_extent() {
    // build extent by the order of tasks
    for (CompIO *task : tasks_) {
      char *output = task->output_blocks;
      size_t *out_blocks_size = task->out_blocks_size;
      size_t num_output_blocks = *task->num_output_blocks;
      CompStats multi_way_block_merge_stats = *task->stats;

      char *block_ptr = output;
      for(size_t i = 0;i < num_output_blocks;++i){
        // raw block
        size_t block_size = out_blocks_size[i];
        Slice raw_block_encoding(block_ptr, block_size);
        block_ptr += block_size;

        // compress data block
        // mock FPGA GZIP
        // so we will get a compressed compacted datablock stream
        // from FPGA Compaction IP

        CompressionType compression_type = GetCompressionType(*context_.cf_options_,
                *context_.mutable_cf_options_,
                1);
        std::string buffer;
        BlockBasedTableOptions table_options;
        if (compression_type != kNoCompression){
            // need compress data block before addBlock to extent
            raw_block_encoding = CompressBlock(raw_block_encoding,
                                               context_.cf_options_->compression_opts,
                                               &compression_type,
                                               table_options.format_version,
                                               compression_dict_,
                                               &buffer);
        }

        // add crc32(5 bits)
        char trailer[table::kBlockTrailerSize];
        trailer[0] = compression_type;
        char *trailer_without_type = trailer + 1;
        auto crc = crc32c::Value(raw_block_encoding.data(), raw_block_encoding.size());
        crc = crc32c::Extend(crc, trailer, 1);
        EncodeFixed32(trailer_without_type, crc32c::Mask(crc));
        std::string block_with_trailer = raw_block_encoding.ToString();
        block_with_trailer.append(trailer, 5);

        BlockStats block_stats;
        block_stats.first_key_ = multi_way_block_merge_stats.first_keys_.at(i);
        block_stats.data_size_ = multi_way_block_merge_stats.data_size_blocks_.at(i);
        block_stats.key_size_ = multi_way_block_merge_stats.key_size_blocks_.at(i);
        block_stats.value_size_ = multi_way_block_merge_stats.value_size_blocks_.at(i);
        block_stats.rows_ = multi_way_block_merge_stats.rows_blocks_.at(i);
        block_stats.entry_put_ = multi_way_block_merge_stats.entry_put_blocks_.at(i);
        block_stats.entry_deletes_ = multi_way_block_merge_stats.entry_delete_blocks_.at(i);
        block_stats.entry_merges_ = multi_way_block_merge_stats.entry_merge_blocks_.at(i);

        std::string block_stats_str = block_stats.encode();
        Slice block_stats_encoding = Slice{block_stats_str};
        Slice block_encoding {block_with_trailer};

        Slice last_key = multi_way_block_merge_stats.last_keys_.at(i);
        extent_builder_->AddBlock(block_encoding, block_stats_encoding, last_key);
      }
    }
  }

  int FPGACompactionTaskGroup::close_extent() {
    extent_builder_->Finish();
    int ret = extent_builder_->status().code();
    change_info_.add(cf_desc_.column_family_id_, 1 /* level 1 */, mini_tables_);
    if (ret) {
      ROCKS_LOG_ERROR(context_.cf_options_->info_log, "write extent failed:%s",
                      extent_builder_->status().getState());
    }

    return ret;
  }

  void FPGACompactionTaskGroup::set_change_info(ChangeInfo change_info) {
    change_info_ = change_info;
  }


}
}
