// Portions Copyright (c) 2020, Alibaba Group Holding Limited.
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once
#include <string>
#include <utility>
#include <vector>
#include "db/table_properties_collector.h"
#include "options/cf_options.h"
#include "storage/storage_common.h"
#include "table/scoped_arena_iterator.h"
#include "util/event_logger.h"
#include "xengine/comparator.h"
#include "xengine/env.h"
#include "xengine/listener.h"
#include "xengine/options.h"
#include "xengine/status.h"
#include "xengine/table_properties.h"
#include "xengine/types.h"

namespace xengine {

namespace common {
struct Options;
}

namespace util {
class Env;
struct EnvOptions;
class WritableFileWriter;
}

namespace table {
class InternalIterator;
class TableBuilder;
}

namespace db {
class Iterator;
struct MiniTables;
struct FileMetaData;
class TableCache;
class VersionEdit;
class InternalStats;

table::TableBuilder* NewTableBuilder(
    const common::ImmutableCFOptions& options,
    const InternalKeyComparator& internal_comparator,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    uint32_t column_family_id, const std::string& column_family_name,
    MiniTables* mtables, const common::CompressionType compression_type,
    const common::CompressionOptions& compression_opts, const storage::LayerPosition &output_layer_position,
    const std::string* compression_dict = nullptr,
    const bool skip_filters = false,
    const bool is_flush = false);

int BuildTable(
    const common::ImmutableCFOptions& ioptions,
    const common::MutableCFOptions& mutable_cf_options,
    table::InternalIterator* iter,
    MiniTables* mtables,
    const InternalKeyComparator& internal_comparator,
    const std::vector<std::unique_ptr<IntTblPropCollectorFactory>>*
        int_tbl_prop_collector_factories,
    uint32_t column_family_id,
    const std::string& column_family_name,
    std::vector<common::SequenceNumber> snapshots,
    common::SequenceNumber earliest_write_conflict_snapshot,
    const common::CompressionType compression,
    const common::CompressionOptions& compression_opts,
    bool paranoid_file_checks,
    InternalStats* internal_stats,
    const storage::LayerPosition &output_layer_position,
    const util::Env::IOPriority io_priority = util::Env::IO_HIGH,
    const std::atomic<int64_t> *cancel_type = nullptr,
    const bool is_flush = false);
}  // namespace db
}  // namespace xengine
