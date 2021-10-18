// Portions Copyright (c) 2020, Alibaba Group Holding Limited.
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/builder.h"

#include <algorithm>
#include <deque>
#include <vector>

#include "compact/compaction_iterator.h"
#include "db/dbformat.h"
#include "db/event_helpers.h"
#include "db/internal_stats.h"
#include "db/merge_helper.h"
#include "db/table_cache.h"
#include "db/version_edit.h"
#include "monitoring/iostats_context_imp.h"
#include "monitoring/thread_status_util.h"
#include "table/internal_iterator.h"
#include "util/file_reader_writer.h"
#include "util/filename.h"
#include "util/stop_watch.h"
#include "util/sync_point.h"
#include "xengine/db.h"
#include "xengine/env.h"
#include "xengine/iterator.h"
#include "xengine/options.h"
#include "xengine/table.h"

using namespace xengine;
using namespace storage;
using namespace monitor;
using namespace util;
using namespace table;
using namespace common;

namespace xengine {

namespace db {

class TableFactory;
// TODO: Overload the following functions so it's not necessary to change all
//   the occurrences of BuildTable right now. We can remove the original ones
//   after they are all switched.
table::TableBuilder* NewTableBuilder(
    const ImmutableCFOptions& ioptions,
    const InternalKeyComparator& internal_comparator,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    uint32_t column_family_id,
    const std::string& column_family_name,
    MiniTables* mtables,
    const CompressionType compression_type,
    const CompressionOptions& compression_opts,
    const storage::LayerPosition &output_position,
    const std::string* compression_dict,
    const bool skip_filters,
    const bool is_flush) {
  assert((column_family_id !=
          TablePropertiesCollectorFactory::Context::kUnknownColumnFamily) /*==
         column_family_name.empty()*/);
  return ioptions.table_factory->NewTableBuilderExt(
      TableBuilderOptions(ioptions, internal_comparator,
                          int_tbl_prop_collector_factories, compression_type,
                          compression_opts, compression_dict, skip_filters,
                          column_family_name, output_position, is_flush),
      column_family_id, mtables);
}

int BuildTable(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options,
    InternalIterator* iter,
    MiniTables* mtables,
    const InternalKeyComparator& internal_comparator,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    uint32_t column_family_id,
    const std::string& column_family_name,
    std::vector<SequenceNumber> snapshots,
    SequenceNumber earliest_write_conflict_snapshot,
    const CompressionType compression,
    const CompressionOptions& compression_opts,
    bool paranoid_file_checks,
    InternalStats* internal_stats,
    const LayerPosition &output_layer_position,
    const Env::IOPriority io_priority,
    const std::atomic<int64_t> *cancel_type,
    const bool is_flush) {
  int ret = Status::kOk;
  // check column_family_name.empty()
  assert((column_family_id !=
          TablePropertiesCollectorFactory::Context::kUnknownColumnFamily));
  // Reports the IOStats for flush for every following bytes.
  const size_t kReportFlushIOStatsEvery = 1048576;
  iter->SeekToFirst();
  // overwritten by the real one
  if (iter->Valid()) {
    mtables->io_priority = io_priority;

    TableBuilder* builder = NewTableBuilder(
        ioptions, internal_comparator, int_tbl_prop_collector_factories,
        column_family_id, column_family_name, mtables, compression,
        compression_opts, output_layer_position, nullptr, false, is_flush);
    storage::ChangeInfo change_info;
    memory::ArenaAllocator arena;
    CompactionIterator comp_iter(
        iter, internal_comparator.user_comparator(), nullptr, kMaxSequenceNumber,
        &snapshots, earliest_write_conflict_snapshot, nullptr,
        true /* internal key corruption is not ok */,
        change_info, arena, nullptr, nullptr, nullptr, nullptr,
        nullptr, cancel_type, nullptr,
        mutable_cf_options.background_disable_merge);
    comp_iter.SeekToFirst();
    while (SUCC(ret) && comp_iter.Valid()) {
      const Slice& key = comp_iter.key();
      const Slice& value = comp_iter.value();
      if (FAILED(builder->Add(key, value))) {
        XENGINE_LOG(WARN, "failed to add record", K(ret), K(key), K(value));
      } else if (io_priority == Env::IO_HIGH &&
          IOSTATS(bytes_written) >= kReportFlushIOStatsEvery) {
        // TODO(noetzli): Update stats after flush, too.
        ThreadStatusUtil::SetThreadOperationProperty(
            ThreadStatus::FLUSH_BYTES_WRITTEN, IOSTATS(bytes_written));
      }
      if (SUCC(ret)) {
        comp_iter.Next();
        ret = comp_iter.status().code();
      }
    }

    // Finish and check for builder errors
    bool empty = builder->NumEntries() == 0;
    if (FAILED(ret) || empty) {
      builder->Abandon();
    } else if (FAILED(builder->Finish())) {
      XENGINE_LOG(WARN, "failed to finish builder", K(ret));
    }
    delete builder;
  }
  return ret;
}

}  // namespace db
}  // namespace xengine
