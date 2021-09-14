//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#pragma once
#include <string>

#include "options/db_options.h"
#include "xengine/env.h"
#include "xengine/status.h"
#include "xengine/types.h"

namespace xengine {
namespace util {
// use_fsync maps to options.use_fsync, which determines the way that
// the file is synced after copying.
extern common::Status CopyFile(Env* env, const std::string& source,
                               const std::string& destination, uint64_t size,
                               bool use_fsync, uint64_t skip = 0 /* copy from here */);
// append the source file to destination of size
extern int append_file(Env* env, const std::string& source,
                       const std::string& destination,
                       uint64_t size, uint64_t offset);

extern common::Status CreateFile(Env* env, const std::string& destination,
                                 const std::string& contents);

extern common::Status DeleteSSTFile(
    const common::ImmutableDBOptions* db_options, const std::string& fname,
    uint32_t path_id);

}  // namespace util
}  // namespace xengine
