// Portions Copyright (c) 2020, Alibaba Group Holding Limited
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include "xengine/status.h"

namespace xengine {
namespace db {

class DB;

class WriteCallback {
 public:
  virtual ~WriteCallback() {}

  // Will be called while on the write thread before the write executes.  If
  // this function returns a non-OK status, the write will be aborted and this
  // status will be returned to the caller of DB::Write().
  virtual common::Status Callback(DB* db) = 0;

  // return true if writes with this callback can be batched with other writes
  virtual bool AllowWriteBatching() = 0;
};
}
}  // namespace xengine
