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

#include "util/testharness.h"
#include <string>

using namespace xengine::common;

namespace xengine {
namespace util {
namespace test {

::testing::AssertionResult AssertStatus(const char* s_expr, const Status& s) {
  if (s.ok()) {
    return ::testing::AssertionSuccess();
  } else {
    return ::testing::AssertionFailure() << s_expr << std::endl << s.ToString();
  }
}

std::string TmpDir(Env* env) {
  std::string dir;
  Status s = env->GetTestDirectory(&dir);
  EXPECT_TRUE(s.ok()) << s.ToString();
  return dir;
}

void remove_dir(const char *file_path)
{
  char cmd[MAX_CMD_LENGTH];
  snprintf(cmd, MAX_CMD_LENGTH, "rm -rf %s", file_path);
  system(cmd);
}

int RandomSeed() {
  const char* env = getenv("TEST_RANDOM_SEED");
  int result = (env != nullptr ? atoi(env) : 301);
  if (result <= 0) {
    result = 301;
  }
  return result;
}

std::string split_file_name(const std::string &name)
{
  size_t s_pos = 0;
  size_t len = std::string::npos;
  s_pos = name.find_last_of("/");
  s_pos = (s_pos == std::string::npos) ? 0 : (s_pos + 1);
  len = name.substr(s_pos).find_first_of(".");
  len = (len == std::string::npos) ? std::string::npos : len;
  return name.substr(s_pos, len);
}

void init_logger(const std::string &name, const logger::InfoLogLevel log_level)
{
  std::string file_name = split_file_name(name);
  std::string log_path = TmpDir() + "/" + file_name + ".log";
  logger::Logger::get_log().init(log_path.c_str(), log_level);
}

}  // namespace test
}  // namespace util
}  // namespace xengine
