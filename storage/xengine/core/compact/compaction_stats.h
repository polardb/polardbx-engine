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

#ifndef XENGINE_STORAGE_COMPACTION_STAT_H_
#define XENGINE_STORAGE_COMPACTION_STAT_H_

#include <stdint.h>
#include "util/to_string.h"
#include "util/serialization.h"

namespace xengine {
namespace storage {

struct CompactRecordStats {
  CompactRecordStats();

  void reset();
  CompactRecordStats &add(const CompactRecordStats &stats);

  DECLARE_SERIALIZATION();
  DECLARE_TO_STRING();

  uint64_t start_micros;
  uint64_t end_micros;

  // valid run time, sum of each task;
  uint64_t micros;
  // total input extents needs compact
  int64_t total_input_extents;
  // total input extents at the output level.
  int64_t total_input_extents_at_l0;

  // total input extents at the output level.
  int64_t total_input_extents_at_l1;

  // the size of the compaction input in bytes.
  int64_t total_input_bytes;
  // the size of the compaction output in bytes.
  int64_t total_output_bytes;

  // total input records involved merge.
  int64_t merge_input_records;
  // output record by compaction iterator
  int64_t merge_output_records;
  // record need switch schema
  int64_t switch_output_records;
  // new generate extents by extent builder
  int64_t merge_output_extents;

  // the number of extents has not changed, can be reuse
  int64_t reuse_extents;
  int64_t merge_extents;
  int64_t reuse_extents_at_l0;
  int64_t reuse_extents_at_l1;
  int64_t reuse_datablocks;
  int64_t merge_datablocks;
  int64_t reuse_datablocks_at_l1;
  int64_t reuse_datablocks_at_l0;

  // the sum of the uncompressed input keys in bytes.
  int64_t merge_input_raw_key_bytes;
  // the sum of the uncompressed input values in bytes.
  int64_t merge_input_raw_value_bytes;

  // number of records being replaced by newer record associated with same key.
  // this could be a new value or a deletion entry for that key so this field
  // sums up all updated and deleted keys
  int64_t merge_replace_records;

  // the number of deletion entries before compaction. Deletion entries
  // can disappear after compaction because they expired
  int64_t merge_delete_records;
  // number of deletion records that were found obsolete and discarded
  // because it is not possible to delete any more keys with this entry
  // (i.e. all possible deletions resulting from it have been completed)
  int64_t merge_expired_records;

  // number of corrupt keys (ParseInternalKey returned false when applied to
  // the key) encountered and written out.
  int64_t merge_corrupt_keys;

  // number of single-deletes which do not meet a put
  int64_t single_del_fallthru;

  // number of single-deletes which meet something other than a put
  int64_t single_del_mismatch;

  // write amplification
  double write_amp;
};

struct MinorCompactStats {
  MinorCompactStats();

  void reset();
  MinorCompactStats &add(const MinorCompactStats &stats);

  DECLARE_SERIALIZATION();
  DECLARE_TO_STRING();
  // MinorCompaction(FPGA) related
  int64_t split_minor_tasks;
  int64_t trival_minor_tasks;
  int64_t total_minor_ways;
  int64_t total_minor_blocks;
  int64_t shared_blocks;
  int64_t cliped_blocks;
};

struct CompactPerfStats {
  CompactPerfStats();

  void reset();
  CompactPerfStats &add(const CompactPerfStats &stats);

  DECLARE_SERIALIZATION();
  DECLARE_TO_STRING();

  // Following counters are only populated if
  // options.report_bg_io_stats = true;

  int64_t check_intersect_extent;
  int64_t check_intersect_datablock;

  int64_t write_row;
  int64_t write_block;

  int64_t read_row;
  int64_t read_extent;
  int64_t read_index;
  int64_t read_block;

  int64_t create_extent;
  int64_t finish_extent;

  // MinorCompaction related
  int64_t split;
  int64_t resplit;
  int64_t reset_extent_heap;
  int64_t reset_data_heap;
  int64_t renew_task;
  int64_t schedule_task;
  int64_t wait_task;
  int64_t wait_task_pend;
};
}
}

#endif  // XENGINE_STORAGE_COMPACTION_STAT_H_
