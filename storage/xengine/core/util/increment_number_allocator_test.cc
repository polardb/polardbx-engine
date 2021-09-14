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

#include "testharness.h"
#include "increment_number_allocator.h"

namespace xengine
{
namespace util
{
TEST(IncreamentNumberAllocator, alloc)
{
  int64_t number = 0;
  for (int64_t i = 0; i < 100; ++i) {
    ASSERT_EQ(i, TestAllocator::get_instance().alloc());
  }
  
  number = 10;
  TestAllocator::get_instance().set(number);
  for (int64_t i = 0; i < 100; ++i) {
    ASSERT_EQ(number + i, TestAllocator::get_instance().alloc());
  }
}
  
} //namespace util
} //namespace xengine

int main(int argc, char **argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
