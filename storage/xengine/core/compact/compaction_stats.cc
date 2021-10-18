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

#include "compaction_stats.h"

namespace xengine {
namespace storage {

CompactRecordStats::CompactRecordStats() { reset(); }

void CompactRecordStats::reset() {
  memset(this, 0, sizeof(CompactRecordStats));
  start_micros = std::numeric_limits<uint64_t>::max();
}

CompactRecordStats &CompactRecordStats::add(const CompactRecordStats &stats) {
  start_micros = std::min(start_micros, stats.start_micros);
  end_micros = std::max(end_micros, stats.end_micros);
  micros += stats.micros;
  total_input_extents += stats.total_input_extents;
  total_input_extents_at_l1 += stats.total_input_extents_at_l1;
  total_input_bytes += stats.total_input_bytes;
  total_output_bytes += stats.total_output_bytes;

  merge_input_records += stats.merge_input_records;
  merge_output_records += stats.merge_output_records;
  merge_output_extents += stats.merge_output_extents;

  reuse_extents += stats.reuse_extents;
  merge_extents += stats.merge_extents;
  reuse_extents_at_l1 += stats.reuse_extents_at_l1;
  reuse_datablocks += stats.reuse_datablocks;
  merge_datablocks += stats.merge_datablocks;
  reuse_datablocks_at_l1 += stats.reuse_datablocks_at_l1;

  merge_input_raw_key_bytes += stats.merge_input_raw_key_bytes;
  merge_input_raw_value_bytes += stats.merge_input_raw_value_bytes;
  merge_replace_records += stats.merge_replace_records;
  merge_delete_records += stats.merge_delete_records;
  merge_expired_records += stats.merge_expired_records;
  merge_corrupt_keys += stats.merge_corrupt_keys;
  single_del_fallthru += stats.single_del_fallthru;
  single_del_mismatch += stats.single_del_mismatch;

  write_amp += stats.write_amp;
  return *this;
}

DEFINE_SERIALIZATION(CompactRecordStats, total_input_extents,
                     total_input_extents_at_l1, total_input_bytes,
                     total_output_bytes, merge_input_records,
                     merge_output_records, merge_output_extents, reuse_extents,
                     merge_extents, reuse_extents_at_l1, reuse_datablocks,
                     merge_datablocks, reuse_datablocks_at_l1,
                     merge_input_raw_key_bytes, merge_replace_records,
                     merge_input_raw_value_bytes, merge_delete_records,
                     merge_expired_records, merge_corrupt_keys,
                     single_del_fallthru, single_del_mismatch);

DEFINE_TO_STRING(CompactRecordStats,
                 KV(total_input_extents),
                 KV(total_input_extents_at_l0),
                 KV(total_input_extents_at_l1),
                 KV(total_input_bytes),
                 KV(total_output_bytes),
                 KV(merge_input_records),
                 KV(merge_output_records),
                 KV(merge_output_extents),
                 KV(reuse_extents),
                 KV(merge_extents),
                 KV(reuse_extents_at_l0),
                 KV(reuse_extents_at_l1),
                 KV(reuse_datablocks),
                 KV(merge_datablocks),
                 KV(reuse_datablocks_at_l1),
                 KV(reuse_datablocks_at_l0),
                 KV(merge_input_raw_key_bytes),
                 KV(merge_replace_records),
                 KV(merge_input_raw_value_bytes),
                 KV(merge_delete_records),
                 KV(merge_expired_records),
                 KV(merge_corrupt_keys),
                 KV(single_del_fallthru),
                 KV(single_del_mismatch),
                 KV(micros),
                 KV(write_amp));

MinorCompactStats::MinorCompactStats() { reset(); }

void MinorCompactStats::reset() {
  memset(this, 0, sizeof(MinorCompactStats));
}

MinorCompactStats &MinorCompactStats::add(const MinorCompactStats &stats) {
  split_minor_tasks += stats.split_minor_tasks;
  trival_minor_tasks += stats.trival_minor_tasks;
  total_minor_ways += stats.total_minor_ways;
  total_minor_blocks += stats.total_minor_blocks;
  shared_blocks += stats.shared_blocks;
  cliped_blocks += stats.cliped_blocks;
  return *this;
}

DEFINE_SERIALIZATION(MinorCompactStats,
                     split_minor_tasks, trival_minor_tasks,
                     total_minor_ways, total_minor_blocks, shared_blocks,
                     cliped_blocks);

DEFINE_TO_STRING(MinorCompactStats,
                 KV(split_minor_tasks),
                 KV(trival_minor_tasks),
                 KV(total_minor_ways),
                 KV(total_minor_blocks),
                 KV(shared_blocks),
                 KV(cliped_blocks));

CompactPerfStats::CompactPerfStats() { reset(); }

void CompactPerfStats::reset() { memset(this, 0, sizeof(CompactPerfStats)); }

CompactPerfStats &CompactPerfStats::add(const CompactPerfStats &stats) {
  check_intersect_extent += stats.check_intersect_extent;
  check_intersect_datablock += stats.check_intersect_datablock;
  write_row += stats.write_row;
  write_block += stats.write_block;

  split += stats.split;
  resplit += stats.resplit;
  reset_extent_heap += stats.reset_extent_heap;
  reset_data_heap += stats.reset_data_heap;
  renew_task += stats.renew_task;
  schedule_task += stats.schedule_task;
  wait_task += stats.wait_task;
  wait_task_pend += stats.wait_task_pend;
  
  return *this;
}

DEFINE_SERIALIZATION(CompactPerfStats, check_intersect_extent,
                     check_intersect_datablock, write_row, write_block,
                     read_row, read_extent, read_index, read_block,
                     create_extent, finish_extent,
                     split,
                     resplit,
                     reset_extent_heap,
                     reset_data_heap,
                     renew_task,
                     schedule_task,
                     wait_task,
                     wait_task_pend);

DEFINE_TO_STRING(CompactPerfStats, KV(check_intersect_extent),
                 KV(check_intersect_datablock), KV(write_row), KV(write_block),
                 KV(read_row), KV(read_extent), KV(read_index), KV(read_block),
                 KV(create_extent), KV(finish_extent),
                 KV(split),
                 KV(resplit),
                 KV(reset_extent_heap),
                 KV(reset_data_heap),
                 KV(renew_task),
                 KV(schedule_task),
                 KV(wait_task),
                 KV(wait_task_pend));
}
}
