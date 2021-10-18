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

#include "util/time_interval.h"
#include "util/testharness.h"
#include "xengine/env.h"
#include "xengine/xengine_constants.h"

using namespace xengine;
using namespace common;
using namespace util;

namespace xengine
{
namespace util
{

static std::atomic<int64_t> t_k(0);
static std::atomic<int64_t> tl_k(0);

static void time_interval_test(void *ptr)
{
  for (int64_t i = 0; i < 1000; i++) {
    if (reach_time_interval(100 * 1000ULL)) { // 100ms
      t_k++;
    }
    usleep(1000); // 1ms
  }
}

static void tl_time_interval_test(void *ptr)
{
  for (int64_t i = 0; i < 1000; i++) {
    if (reach_tl_time_interval(100 * 1000ULL)) { // 100ms
      tl_k++;
    }
    usleep(1000); // 1ms
  }
}

TEST(TimeInterval, time_interval)
{
  auto* env = Env::Default();
  for (int64_t th = 0; th < 10; th++) {
    env->StartThread(time_interval_test, nullptr);
  }
  env->WaitForJoin();
  ASSERT_TRUE(t_k == 11) << t_k;
}

TEST(TimeInterval, tl_time_interval)
{
  auto* env = Env::Default();
  for (int64_t th = 0; th < 10; th++) {
    env->StartThread(tl_time_interval_test, nullptr);
  }
  env->WaitForJoin();
  ASSERT_TRUE(tl_k == 110) << tl_k;
}

} // namespace util
} // namespace xengine

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
	xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
