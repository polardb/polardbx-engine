/*
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#pragma once
#ifndef ROCKSDB_LITE

#include <string>

#include "xengine/db.h"

namespace xengine {
namespace tools {

struct DumpOptions {
  // Database that will be dumped
  std::string db_path;
  // File location that will contain dump output
  std::string dump_location;
  // Dont include db information header in the dump
  bool anonymous = false;
};

class DbDumpTool {
 public:
  bool Run(const DumpOptions& dump_options,
           common::Options options = common::Options());
};

struct UndumpOptions {
  // Database that we will load the dumped file into
  std::string db_path;
  // File location of the dumped file that will be loaded
  std::string dump_location;
  // Compact the db after loading the dumped file
  bool compact_db = false;
};

class DbUndumpTool {
 public:
  bool Run(const UndumpOptions& undump_options,
           common::Options options = common::Options());
};
}  // namespace tools
}  // namespace xengine
#endif  // ROCKSDB_LITE
