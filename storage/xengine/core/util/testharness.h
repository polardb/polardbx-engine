// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#pragma once

#include <gtest/gtest.h>

#include <string>
#include "xengine/env.h"
#include "logger/logger.h"

#define MAX_CMD_LENGTH 1024

namespace xengine {

namespace util {
namespace test {

// Return the directory to use for temporary storage.
std::string TmpDir(Env* env = Env::Default());

void remove_dir(const char *file_path);

// Return a randomization seed for this run.  Typically returns the
// same number on repeated invocations of this binary, but automated
// runs may be able to vary the seed.
int RandomSeed();

::testing::AssertionResult AssertStatus(const char* s_expr,
                                        const xengine::common::Status& s);

#define ASSERT_OK(s) ASSERT_PRED_FORMAT1(xengine::util::test::AssertStatus, s)
#define ASSERT_NOK(s) ASSERT_FALSE((s).ok())
#define EXPECT_OK(s) EXPECT_PRED_FORMAT1(xengine::util::test::AssertStatus, s)
#define EXPECT_NOK(s) EXPECT_FALSE((s).ok())

void init_logger(const std::string &name, 
                 const logger::InfoLogLevel log_level = logger::DEBUG_LEVEL);

}  // namespace test
}  // namespace util
}  // namespace xengine
