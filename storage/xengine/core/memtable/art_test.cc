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
#include "memtable/art.h"
#include "memtable/art_node.h"
#include "util/testharness.h"
#include "xengine/env.h"
#include "xengine/slice.h"
#include "util/coding.h"
#include "db/dbformat.h"
#include "xengine/comparator.h"
#include "xengine/write_buffer_manager.h"
#include "util/concurrent_arena.h"
#include "db/memtable.h"
#include "logger/log_module.h"
#include "xengine/memtablerep.h"
#include "util/random.h"

#include <unistd.h>
#include <endian.h>
#include <string>
#include <sys/time.h>
#include <thread>
#include <cstdio>
#include <atomic>
#include <set>

using namespace xengine;
using namespace xengine::common;
using namespace xengine::util;
using namespace xengine::db;
using namespace xengine::logger;

namespace xengine {
namespace memtable {

std::atomic<uint64_t> seq{0};

uint64_t decode_int_from_key(const char *key) {
  const uint8_t *buf = reinterpret_cast<const uint8_t *>(key);
  int32_t bytes_to_fill = 8;
  uint64_t ret = 0;
  for (int i = 0; i < bytes_to_fill; i++) {
    ret += (static_cast<uint64_t>(buf[i]) << (bytes_to_fill - i - 1) * 8);
  }
  return ret;
}

void generate_key_from_int(char *buf, uint64_t v) {
  int32_t bytes_to_fill = 8;
  char *pos = buf;
  for (int i = 0; i < bytes_to_fill; ++i) {
    pos[i] = (v >> ((bytes_to_fill - i - 1) << 3)) & 0xFF;
  }
}

class ARTTest : public testing::Test {
private:
  WriteBufferManager wb;
  InternalKeyComparator ikc;
  MemTable::KeyComparator cmp;
  ConcurrentArena arena;
  MemTableAllocator alloc;
  char buf[4096];

public:
  ARTTest() 
  : wb(10000000000),
    ikc(BytewiseComparator()),
    cmp(ikc),
    arena(),
    alloc(&arena, &wb),
    build_target(buf, 0),
    insert_artvalue(nullptr) {}
  ART *init() {
    ART *art = new ART(cmp, &alloc);
    art->init();
    return art;
  }
  void gen_key_from_int(uint64_t v, uint32_t len) {
    uint32_t internal_key_size = len + 8;
    char *p = buf;
    generate_key_from_int(p, v);
    p += len;
    uint64_t packed = PackSequenceAndType(seq.fetch_add(1), ValueType::kTypeValue);
    EncodeFixed64(p, packed);
    build_target.assign(buf, internal_key_size);
  }
  void gen_key_from_str(const char *v, uint32_t len) {
    uint32_t internal_key_size = len + 8;
    char *p = buf;
    memcpy(p, v, len);
    p += len;
    uint64_t packed = PackSequenceAndType(seq.fetch_add(1), ValueType::kTypeValue);
    EncodeFixed64(p, packed);
    build_target.assign(buf, internal_key_size);
  }
  void gen_artvalue_from_int(ART *idx, uint64_t v, uint64_t len) {
    uint32_t internal_key_size = len + 8;
    const uint32_t encoded_len = VarintLength(internal_key_size) + internal_key_size;
    insert_artvalue = idx->allocate_art_value(encoded_len);
    char *p = const_cast<char *>(insert_artvalue->entry());
    p = EncodeVarint32(p, internal_key_size);
    generate_key_from_int(p, v);
    p += len;
    uint64_t packed = PackSequenceAndType(seq.fetch_add(1), ValueType::kTypeValue);
    EncodeFixed64(p, packed);
  }

  void gen_artvalue_from_str(ART *idx, const char *v, uint64_t len) {
    uint32_t internal_key_size = len + 8;
    const uint32_t encoded_len = VarintLength(internal_key_size) + internal_key_size;
    insert_artvalue = idx->allocate_art_value(encoded_len);
    char *p = const_cast<char *>(insert_artvalue->entry());
    p = EncodeVarint32(p, internal_key_size);
    memcpy(p, v, len);
    p += len;
    uint64_t packed = PackSequenceAndType(seq.fetch_add(1), ValueType::kTypeValue);
    EncodeFixed64(p, packed);
  }
  void insert_from_int(ART *idx, uint64_t v, uint32_t len) {
    gen_artvalue_from_int(idx, v, len);
    idx->insert(insert_artvalue);
  }
  void insert_from_str(ART *idx, const char *v, uint64_t len) {
    gen_artvalue_from_str(idx, v, len);
    idx->insert(insert_artvalue);
  }

  static const uint32_t key_len = 8;
  Slice build_target;
  ARTValue *insert_artvalue;
};

TEST_F(ARTTest, Empty) {
  ART *art = init();
  ART::Iterator iter(art);
  ASSERT_TRUE(!iter.valid());
  iter.seek_to_first();
  ASSERT_TRUE(!iter.valid());
  gen_key_from_int(100, key_len);
  iter.seek(build_target);
  ASSERT_TRUE(!iter.valid());
  iter.seek_for_prev(build_target);
  ASSERT_TRUE(!iter.valid());
  iter.seek_to_last();
  ASSERT_TRUE(!iter.valid());
}

TEST_F(ARTTest, InsertSeqAndLookup) {
  ART *art = init();
  const uint64_t range = 10000;

  for (uint64_t i = 0; i < range; i++) {
    insert_from_int(art, i, key_len);
  }

  ART::Iterator iter(art);

  // Point get test
  for (uint64_t i = 0; i < range; i++) {
    gen_key_from_int(i, key_len);
    iter.seek(build_target);
    ASSERT_TRUE(iter.valid());
    ASSERT_EQ(i, decode_int_from_key(iter.key().data()));
  }

  // Forward iteration test
  iter.seek_to_first();
  for (uint64_t i = 0; i < range; i++) {
    ASSERT_TRUE(iter.valid());
    ASSERT_EQ(i, decode_int_from_key(iter.key().data()));
    iter.next();
  }

  // Backward iteration test
  iter.seek_to_last();
  for (int64_t i = range - 1; i >= 0; i--) {
    ASSERT_TRUE(iter.valid());
    ASSERT_EQ(i, decode_int_from_key(iter.key().data()));
    iter.prev();
  }
}

TEST_F(ARTTest, InsertAndLookup) {
  ART *art = init();
  const int64_t range = 10000;
  const int64_t duration = 5000;
  std::set<uint64_t> keys;
  Random64 rnd(2020);
  
  for (int i = 0; i < duration; i++) {
    uint64_t key = rnd.Next() % range;
    if (keys.insert(key).second) {
      insert_from_int(art, key, key_len);
    }
  }

  ART::Iterator iter(art);
  int count = 0;
  for (uint64_t i = 0; i < range; i++) {
    gen_key_from_int(i, key_len);
    iter.seek(build_target);
    if (iter.valid()) {
      uint64_t data = decode_int_from_key(iter.key().data());
      ASSERT_GE(data, i);
      if (data == i) {
        ASSERT_EQ(keys.count(i), 1);
        count++;
      } else {
        ASSERT_EQ(keys.count(i), 0);
      }
    }
  }
  ASSERT_EQ(count, keys.size());

  {
    gen_key_from_int(0, key_len);
    iter.seek(build_target);
    ASSERT_TRUE(iter.valid());
    ASSERT_EQ(*(keys.begin()), decode_int_from_key(iter.key().data()));

    gen_key_from_int(range, key_len);
    iter.seek_for_prev(build_target);
    ASSERT_TRUE(iter.valid());
    ASSERT_EQ(*(keys.rbegin()), decode_int_from_key(iter.key().data()));

    iter.seek_to_first();
    ASSERT_TRUE(iter.valid());
    ASSERT_EQ(*(keys.begin()), decode_int_from_key(iter.key().data()));

    iter.seek_to_last();
    ASSERT_TRUE(iter.valid());
    ASSERT_EQ(*(keys.rbegin()), decode_int_from_key(iter.key().data()));
  }

  // Forward iteration test
  for (uint64_t i = 0; i < range; i++) {
    gen_key_from_int(i, key_len);
    iter.seek(build_target);

    // Compare against model iterator
    std::set<uint64_t>::iterator model_iter = keys.lower_bound(i);
    for (int j = 0; j < 3; j++) {
      if (model_iter == keys.end()) {
        ASSERT_TRUE(!iter.valid());
        break;
      } else {
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(*model_iter, decode_int_from_key(iter.key().data()));
        ++model_iter;
        iter.next();
      }
    }
  }

  // Backward iteration test
  for (uint64_t i = 0; i < range; i++) {
    gen_key_from_int(i, key_len);
    iter.seek_for_prev(build_target);

    // Compare against model iterator
    std::set<uint64_t>::iterator model_iter = keys.lower_bound(i);
    for (int j = 0; j < 3; j++) {
      if (model_iter == keys.begin()) {
        ASSERT_TRUE(!iter.valid());
        break;
      } else {
        ASSERT_TRUE(iter.valid());
        ASSERT_EQ(*--model_iter, decode_int_from_key(iter.key().data()));
        iter.prev();
      }
    }
  }
}

TEST_F(ARTTest, InsertAndLookup2) {
  ART *art = init();

  insert_from_str(art, "10001", 5);
  insert_from_str(art, "10002", 5);

  ART::Iterator iter(art);
  gen_key_from_str("0", 1);
  iter.seek(build_target);
  ASSERT_TRUE(iter.valid());
  ASSERT_EQ(0, memcmp(iter.key().data(), "10001", 5));
}

TEST_F(ARTTest, EstimateCount) {
  ART *art = init();
  const uint64_t range = 10000;
  const uint64_t step = 499;

  for (uint64_t i = 0; i < range; i += 2) {
    insert_from_int(art, i, key_len);
  }

  int64_t count = 0;
  for (uint64_t i = 0; i < range + step * 2; i += step) {
    gen_key_from_int(i, key_len);
    art->estimate_lower_bound_count(build_target, count);
    fprintf(stderr, "estimate count: %ld\n", count);
  }
}

TEST_F(ARTTest, DumpInfo) {
  ART *art = init();
  const uint64_t range = 1000;

  for (uint64_t i = 0; i < range; i++) {
    insert_from_int(art, i, key_len);
  }

  art->dump_art_struct();
}

TEST_F(ARTTest, SplitPrefixWithNoDifferentiableSuffix1) {
  ART *art = init();
  
  insert_from_str(art, "10001", 5);
  insert_from_str(art, "10002", 5);
  insert_from_str(art, "100", 3);

  ART::Iterator iter(art);
  gen_key_from_str("10001", 5);
  iter.seek(build_target);
  ASSERT_TRUE(iter.valid());
  ASSERT_EQ(0, memcmp(iter.key().data(), "10001", 5));
  gen_key_from_str("10002", 5);
  iter.seek(build_target);
  ASSERT_TRUE(iter.valid());
  ASSERT_EQ(0, memcmp(iter.key().data(), "10002", 5));
  gen_key_from_str("100", 3);
  ASSERT_TRUE(iter.valid());
  ASSERT_EQ(0, memcmp(iter.key().data(), "100", 3));
}

TEST_F(ARTTest, SplitPrefixWithNoDifferentiableSuffix2) {
  ART *art = init();
  
  insert_from_str(art, "10001", 5);
  insert_from_str(art, "10002", 5);
  insert_from_str(art, "1000", 4);

  ART::Iterator iter(art);
  gen_key_from_str("10001", 5);
  iter.seek(build_target);
  ASSERT_TRUE(iter.valid());
  ASSERT_EQ(0, memcmp(iter.key().data(), "10001", 5));
  gen_key_from_str("10002", 5);
  iter.seek(build_target);
  ASSERT_TRUE(iter.valid());
  ASSERT_EQ(0, memcmp(iter.key().data(), "10002", 5));
  gen_key_from_str("1000", 4);
  ASSERT_TRUE(iter.valid());
  ASSERT_EQ(0, memcmp(iter.key().data(), "1000", 4));
}

TEST_F(ARTTest, SplitPrefixWithNoDifferentiableSuffix3) {
  ART *art = init();
  
  insert_from_str(art, "10001", 5);
  insert_from_str(art, "10002", 5);
  insert_from_str(art, "1", 1);

  ART::Iterator iter(art);
  gen_key_from_str("10001", 5);
  iter.seek(build_target);
  ASSERT_TRUE(iter.valid());
  ASSERT_EQ(0, memcmp(iter.key().data(), "10001", 5));
  gen_key_from_str("10002", 5);
  iter.seek(build_target);
  ASSERT_TRUE(iter.valid());
  ASSERT_EQ(0, memcmp(iter.key().data(), "10002", 5));
  gen_key_from_str("1", 1);
  ASSERT_TRUE(iter.valid());
  ASSERT_EQ(0, memcmp(iter.key().data(), "1", 1));
}

TEST_F(ARTTest, SplitPrefixWithNoDifferentiableSuffix4) {
  ART *art = init();
  
  insert_from_str(art, "11", 2);
  insert_from_str(art, "12", 2);
  insert_from_str(art, "1", 1);

  ART::Iterator iter(art);
  gen_key_from_str("11", 2);
  iter.seek(build_target);
  ASSERT_TRUE(iter.valid());
  ASSERT_EQ(0, memcmp(iter.key().data(), "11", 2));
  gen_key_from_str("12", 2);
  iter.seek(build_target);
  ASSERT_TRUE(iter.valid());
  ASSERT_EQ(0, memcmp(iter.key().data(), "12", 2));
  gen_key_from_str("1", 1);
  ASSERT_TRUE(iter.valid());
  ASSERT_EQ(0, memcmp(iter.key().data(), "1", 1));
}

TEST_F(ARTTest, SplitARTValueWithNoDifferentiableSuffix1) {
  ART *art = init();

  insert_from_str(art, "10000", 5);
  insert_from_str(art, "100", 3);

  ART::Iterator iter(art);
  gen_key_from_str("10000", 5);
  iter.seek(build_target);
  ASSERT_TRUE(iter.valid());
  ASSERT_EQ(0, memcmp(iter.key().data(), "10000", 5));
  gen_key_from_str("100", 3);
  iter.seek(build_target);
  ASSERT_TRUE(iter.valid());
  ASSERT_EQ(0, memcmp(iter.key().data(), "100", 3));
}

TEST_F(ARTTest, SplitARTValueWithNoDifferentiableSuffix2) {
  ART *art = init();

  insert_from_str(art, "10000", 5);
  insert_from_str(art, "1", 1);

  ART::Iterator iter(art);
  gen_key_from_str("10000", 5);
  iter.seek(build_target);
  ASSERT_TRUE(iter.valid());
  ASSERT_EQ(0, memcmp(iter.key().data(), "10000", 5));
  gen_key_from_str("1", 1);
  iter.seek(build_target);
  ASSERT_TRUE(iter.valid());
  ASSERT_EQ(0, memcmp(iter.key().data(), "1", 1));
}

} // namespace memtable
} // namespace xengine

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
	xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
