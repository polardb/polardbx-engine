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

#include "util/ring_buffer.h"
#include "util/testharness.h"
#include "storage/io_extent.h"

using namespace xengine;
using namespace storage;

namespace xengine {

namespace storage {
struct ExtentId;
}

namespace util {

class RingBufferTest : public testing::Test {};

TEST_F(RingBufferTest, dump) {
  RingBuffer<int> r(4, 1);
  for (int i = 0; i < 3; i++) {
    ASSERT_EQ(r.put(i), i);
  }
  int *tmp = r.dump();
  ASSERT_EQ(tmp[2], 2);
  ASSERT_TRUE(r.remove(2));
  for (int i = 3; i < 5; i ++) {
    ASSERT_EQ(r.put(i), i);
  }
  delete[] tmp;
  tmp = r.dump();
  ASSERT_EQ(tmp[0], 2);
  delete[] tmp;
}

TEST_F(RingBufferTest, put) {
  // basic
  RingBuffer<int> r(1,1);
  ASSERT_EQ(r.put(1), 0);
  ASSERT_EQ(r.size(), 1);
  ASSERT_TRUE(r.remove());
  ASSERT_EQ(r.size(), 0);
  for (int i = 0; i < 100; i++) {
    ASSERT_EQ(r.put(10000), i + 1);
  }
  ASSERT_EQ(r.size(), 100);
  for (int i = 0; i < 50; i++) {
    ASSERT_TRUE(r.remove());
  }
  ASSERT_EQ(r.size(), 50);
  // realloc of tail_ <= head_   
  RingBuffer<int> r1(10,2);
  for (int i = 0; i < 9; i++) {
    ASSERT_EQ(r1.put(i), i);
  }
  ASSERT_EQ(r1.size(), 9);
  ASSERT_EQ(r1.capacity(), 10);
  ASSERT_EQ(r1.put(9), 9);
  ASSERT_EQ(r1.size(), 10);
  ASSERT_EQ(r1.capacity(), 12);
  // realloc of head_ < tail_
  // head_ <= increment
  RingBuffer<int> r2(10,2);
  for (int i = 0; i < 9; i++) {
    ASSERT_EQ(r2.put(i), i);
  }
  ASSERT_TRUE(r2.remove());
  ASSERT_EQ(r2.put(9), 9);
  ASSERT_EQ(r2.size(), 9);
  ASSERT_EQ(r2.head(), 0);
  ASSERT_EQ(r2.put(10), 10);
  ASSERT_EQ(r2.capacity(), 12);
  ASSERT_EQ(r2.head(), 11);
  RingBuffer<int> r3(10,2);
  for (int i = 0; i < 9; i++) {
    ASSERT_EQ(r3.put(i), i);
  }
  for (int i = 0; i < 3; i++) {
    ASSERT_TRUE(r3.remove());
  }
  for (int i = 9; i < 12; i++) {
    ASSERT_EQ(r3.put(i), i);
  } 
  ASSERT_EQ(r3.capacity(), 10);
  ASSERT_EQ(r3.head(), 2);
  ASSERT_EQ(r3.put(12), 12);
  ASSERT_EQ(r3.head(), 1);   
  RingBuffer<int> r4(10,2);
  for (int i = 0; i < 9; i++) {
    ASSERT_EQ(r4.put(i), i);
  }
  for (int i = 0; i < 4; i++) {
    ASSERT_TRUE(r4.remove());
  }
  for (int i = 9; i < 13; i++) {
    ASSERT_EQ(r4.put(i), i);
  }
  ASSERT_EQ(r4.capacity(), 10);
  ASSERT_EQ(r4.put(13), 13);
  ASSERT_EQ(r4.head(), 2);
  ASSERT_EQ(r4.capacity(), 12);
}

TEST_F(RingBufferTest, remove) {
  // basic
  RingBuffer<ExtentId> r(1,1);
  ASSERT_EQ(r.put(ExtentId(1,1)), 0);
  ASSERT_EQ(r.size(), 1);
  ASSERT_TRUE(r.remove());
  ASSERT_EQ(r.size(), 0);
  ASSERT_EQ(r.put(ExtentId(2,2)), 1);
  ASSERT_EQ(r.get(1).file_number, 2);
}

TEST_F(RingBufferTest, get) {
  // basic
  RingBuffer<int> r(10, 1);
  for (int i = 0; i < 8; i++) {
    ASSERT_EQ(r.put(i), i);
  }
  ASSERT_EQ(r.size(), 8);
  for (int i = 0; i < 4; i++) {
    ASSERT_TRUE(r.remove());
  } 
  ASSERT_EQ(r.size(), 4);
  ASSERT_EQ(r.get(4), 4);
  ASSERT_EQ(r.get(10), 0);
  for (int i = 8; i < 13; i++) {
    ASSERT_EQ(r.put(i), i);
  }
  ASSERT_EQ(r.size(), 9);
  ASSERT_EQ(r.get(10), 10);
  ASSERT_EQ(r.get(12), 12);
}

} // util
} // xengine

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
	xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
