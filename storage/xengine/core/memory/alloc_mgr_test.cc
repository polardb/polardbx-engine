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
#include "memory/page_arena.h"
#include "memory/stl_adapt_allocator.h"
#include <gtest/gtest.h>
#include <memory>
#include <vector>
#include <cstddef>

namespace xengine {
namespace memory {
int64_t align_calc_test(int64_t val) {
  int64_t align_val = alignof(std::max_align_t);
  return ((val + align_val - 1) / align_val) * align_val;
}

void simple_test(int64_t max, int64_t step_len) {
  AllocMgr talloc;
  void *buf = nullptr;
  int64_t alloc_size = 0;
  for (int i = 1; i < max; i += step_len) {
    buf = nullptr;
    buf = talloc.balloc(i, ModId::kTestMod);
    assert(nullptr != buf);
    AllocMgr::Block *bk =
        reinterpret_cast<AllocMgr::Block *>((char *)buf - AllocMgr::kBlockMeta);
    assert(nullptr != bk);
    assert(AllocMgr::kMagicNumber == bk->magic_);
    assert(bk->get_size() == static_cast<int64_t>(AllocMgr::kBlockMeta + i));
    int64_t msize = static_cast<int64_t>(talloc.bmalloc_usable_size(buf));
    assert(msize >= bk->get_size());
    assert(msize < max * 2);
    alloc_size += bk->get_size();
    assert(alloc_size == talloc.get_allocated_size(ModId::kTestMod));
  }
}

void realloc_test() {
  AllocMgr *alloc_mgr = AllocMgr::get_instance();
  assert(alloc_mgr);
  assert(0 == alloc_mgr->get_hold_size(ModId::kTestMod));

  int64_t buf_len = 10;
  char *buf = static_cast<char *>(base_realloc(nullptr, buf_len, ModId::kTestMod));
  // test size = 0
  base_realloc(buf, 0, ModId::kTestMod);
  assert(0 == alloc_mgr->get_hold_size(ModId::kTestMod));

  buf = static_cast<char *>(base_realloc(nullptr, buf_len, ModId::kTestMod));
  assert(buf);
  memcpy(buf, "1234567890", buf_len);
  char *copy_buf = static_cast<char *>(base_malloc(buf_len, ModId::kTestMod));
  assert(copy_buf);
  memcpy(copy_buf, buf, buf_len);
  buf = (char *)base_realloc(buf, buf_len + 10, ModId::kTestMod);
  for (int i = 0; i < buf_len; i++) {
    assert(buf[i] == copy_buf[i]);
  }
  base_realloc(buf, 0, ModId::kTestMod);
  base_realloc(copy_buf, 0, ModId::kTestMod);
  assert(0 == alloc_mgr->get_hold_size(ModId::kTestMod));
}

void realloc_test_small() {
  std::string stats;
  AllocMgr *alloc_mgr = AllocMgr::get_instance();
  assert(alloc_mgr);
  assert(0 == alloc_mgr->get_hold_size(ModId::kTestMod));

  int64_t buf_len = 10;
  char *buf = static_cast<char *>(base_realloc(nullptr, buf_len, ModId::kTestMod));
  assert(buf);
  EXPECT_EQ(buf_len, alloc_mgr->get_allocated_size(ModId::kTestMod) - 24);
  stats.clear();
  AllocMgr::get_instance()->print_memory_usage(stats);
  std::cerr << stats << std::endl;
  memcpy(buf, "1234567890", buf_len);

  buf = (char *)base_realloc(buf, buf_len + 10, ModId::kTestMod);
  EXPECT_EQ(buf_len + 10U, alloc_mgr->get_allocated_size(ModId::kTestMod) - 24);
  stats.clear();
  AllocMgr::get_instance()->print_memory_usage(stats);
  std::cerr << stats << std::endl;

  buf = (char *)base_realloc(buf, buf_len + 20, ModId::kTestMod);
  EXPECT_EQ(buf_len + 20U, alloc_mgr->get_allocated_size(ModId::kTestMod) - 24);
  stats.clear();
  AllocMgr::get_instance()->print_memory_usage(stats);
  std::cerr << stats << std::endl;
  buf = (char *)base_realloc(buf, 0, ModId::kTestMod);
  ASSERT_EQ(0U, alloc_mgr->get_allocated_size(ModId::kTestMod));
}

void align_test() {
  AllocMgr *alloc_mgr = AllocMgr::get_instance();
  assert(alloc_mgr);
  assert(0 == alloc_mgr->get_hold_size(ModId::kTestMod));
  size_t align[15] = {2,   4,    8,    16,   32,   64,    128,  256,
                      512, 1024, 2048, 4096, 8192, 16384, 32768};
  char *buf[16][61001];

  int64_t cnt = 0;
  int64_t size = 0;
  for (int i = 0; i < 15; ++i) {
    for (int j = 1; j < 1024 * 1024; j += 188) {
      buf[i][cnt] =
          static_cast<char *>(base_memalign(j, align[i], ModId::kTestMod));
      ++cnt;
      size += j;
    }
  }

  cnt = 0;
  for (int i = 0; i < 15; i++)
    for (int j = 1; j < 1024 * 1024; j += 188) {
      base_memalign_free(buf[i][cnt]);
      ++cnt;
    }
  assert(0 == alloc_mgr->get_hold_size(ModId::kTestMod));
}

TEST(TestAllocMgr, test_base_alloc) {
  simple_test(8 * 1024, 12);
  simple_test(1024 * 1024, 210);
}

TEST(TestAllocMgr, test_aligned_alloc) { align_test(); }

TEST(TestAllocMgr, test_realloc) { realloc_test(); }
TEST(TestAllocMgr, test_realloc_small) { realloc_test_small(); }

struct POD {
  int64_t x_;
  int32_t y_;
  POD() : x_(0), y_(0) {}
  POD(int64_t x, int32_t y) : x_(x), y_(y) {}
  ~POD() {}
};

TEST(TestAllocMgr, test_macro) {
  AllocMgr *alloc_mgr = AllocMgr::get_instance();
  ASSERT_EQ(0, alloc_mgr->get_allocated_size(ModId::kTestMod));
  POD *pod = MOD_NEW_OBJECT(ModId::kTestMod, POD);
  ASSERT_EQ(pod->x_, 0);
  ASSERT_EQ(pod->y_, 0);
  ASSERT_EQ(static_cast<int64_t>(sizeof(POD) + AllocMgr::kBlockMeta) + alignof(std::max_align_t),
            alloc_mgr->get_allocated_size(ModId::kTestMod));
  MOD_DELETE_OBJECT(POD, pod);
  ASSERT_TRUE(nullptr == pod);
  ASSERT_EQ(0, alloc_mgr->get_allocated_size(ModId::kTestMod));

  pod = MOD_NEW_OBJECT(ModId::kTestMod, POD, 1, 2);
  ASSERT_TRUE(nullptr != pod);
  ASSERT_EQ(pod->x_, 1);
  ASSERT_EQ(pod->y_, 2);
  ASSERT_EQ(static_cast<int64_t>(sizeof(POD) + AllocMgr::kBlockMeta) + alignof(std::max_align_t),
            alloc_mgr->get_allocated_size(ModId::kTestMod));
  MOD_DELETE_OBJECT(POD, pod);
  ASSERT_TRUE(nullptr == pod);
  ASSERT_EQ(0, alloc_mgr->get_hold_size(ModId::kTestMod));

  MOD_DEFINE_UNIQUE_PTR(up, ModId::kTestMod, POD, 3, 4);
  ASSERT_EQ(up->x_, 3);
  ASSERT_EQ(up->y_, 4);
  ASSERT_EQ(static_cast<int64_t>(sizeof(POD) + AllocMgr::kBlockMeta),
            alloc_mgr->get_allocated_size(ModId::kTestMod));
  POD *raw = up.release();
  ptr_delete<POD>()(raw);
  ASSERT_EQ(0, alloc_mgr->get_hold_size(ModId::kTestMod));
}

TEST(TestAllocMgr, test_stl_adaptor) {
  AllocMgr *alloc_mgr = AllocMgr::get_instance();
  std::vector<int64_t, stl_adapt_allocator<int64_t, ModId::kTestMod>> vs;
  vs.reserve(2);
  ASSERT_EQ(static_cast<int64_t>(sizeof(int64_t) * 2 + AllocMgr::kBlockMeta),
      alloc_mgr->get_allocated_size(ModId::kTestMod));
}

TEST(TestAllocMgr, test_alloc_chunk) {
  AllocMgr *alloc_mgr = AllocMgr::get_instance();
  void *ptr = alloc_mgr->balloc_chunk(16 * 1024);
  assert(ptr);
}

} // namespace memory
} // namespace xengine

int main(int argc, char **argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
   
