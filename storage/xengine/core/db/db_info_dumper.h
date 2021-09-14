// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#include <string>

#include "options/db_options.h"

namespace xengine {
namespace db {
void DumpDBFileSummary(const common::ImmutableDBOptions& options,
                       const std::string& dbname);
}
}  // namespace xengine
