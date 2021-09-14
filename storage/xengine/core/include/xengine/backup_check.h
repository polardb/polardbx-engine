/*
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
#pragma once
#include <string>
using namespace std;

namespace xengine {
namespace storage {
struct ExtentId;
}

namespace common {
class Slice;
}
}

namespace xengine {
namespace tools {

class BackupCheckTool {
 public:
  int run(int argc, char** argv);
 private:
  friend class BackupCheckToolTest;
  std::string data_dir;
  int check_one_extent(storage::ExtentId &extent, const common::Slice &begin, 
                       const common::Slice &end, bool &check_result);
};

}  // namespace tools
}  // namespace xengine
