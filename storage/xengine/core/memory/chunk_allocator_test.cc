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
#include "memory/chunk_allocator.h"
#include "db/db_test_util.h"

using namespace xengine;
using namespace common;
using namespace util;
using namespace db;

namespace xengine {
namespace memory {
struct TestEntry {
  ChunkAllocator *alloc_;
  int64_t size_;
};
static void delete_entry_test(void *value, void* handler) {
  TestEntry *entry = reinterpret_cast<TestEntry *>(value);
  entry->alloc_->free(value, entry->size_);
}

class ChunkAllocatorTest : public DBTestBase {
  public:
  ChunkAllocatorTest()
    : DBTestBase("/chunk_allocator_test") {
    allocator_ = new ChunkAllocator(&delete_entry_test, nullptr);
    allocator_->init(1);
    chunk_manager_ = new ChunkManager();
  }

  void init(const int64_t chunks, int64_t shards, bool small) {
    chunk_manager_->init(chunks, &delete_entry_test, nullptr);
  }
  void reset() {
  }
  void *alloc(int64_t size) {
    return allocator_->alloc(size);
  }
  void free(void *ptr, int64_t chunk_id) {
    allocator_->free(ptr, chunk_id);
  }

  void *cache_alloc(int64_t size, int64_t &chunk_id, ChunkAllocator *&alloc) {
    return chunk_manager_->alloc(size, alloc);
  }
  void *evict_and_alloc_again(int64_t size, int64_t &chunk_id, ChunkAllocator *&alloc) {
    chunk_manager_->async_evict_chunks();
    return cache_alloc(size, chunk_id, alloc);
  }
  void *evict_one_and_alloc_again(int64_t size, int64_t &chunk_id, ChunkAllocator *&alloc) {
    chunk_manager_->evict_one_chunk();
    return cache_alloc(size, chunk_id, alloc);
  }
  void print_stat() {
    std::string stat;
    allocator_->print_chunk_stat(stat);
    fprintf(stderr, "----------------CHUNK INFO-------------\n%s\n", stat.c_str());
  }
  void print_chunks_stat() {
    std::string stat;
    chunk_manager_->print_chunks_stat(stat);
    fprintf(stderr, "----------------CHUNKS INFO-------------\n%s\n", stat.c_str());
  }
  int64_t get_usage() {
    return chunk_manager_->get_usage();
  }
  int64_t get_allocated() {
    return chunk_manager_->get_allocated_size();
  }
private:
  ChunkAllocator *allocator_;
  ChunkManager *chunk_manager_;
};

TEST_F(ChunkAllocatorTest, normal_test) {
  std::vector<char *> ptrs;
  for (int i = 0 ;i < 10; ++i) {
    ptrs.push_back((char *)alloc(129));
  }
  for (int i = 0; i < 10 ; i += 2) {
    free(ptrs.at(i), 1);
  }
  for (int i = 1; i < 10 ; i += 2) {
    free(ptrs.at(i), 1);
  }
  print_stat();
}

TEST_F(ChunkAllocatorTest, single_test) {
  init(1, true, 1);
  std::vector<char *> ptrs;
  std::vector<int64_t> chunks;
  std::vector<ChunkAllocator *>allocators;
  for (int i = 0 ;i < 10; ++i) {
    int64_t id = 0;
    ChunkAllocator *alloc = nullptr;
    ptrs.push_back((char *)cache_alloc(129, id, alloc));
    chunks.push_back(id);
    allocators.push_back(alloc);
    if (nullptr != alloc) {
      alloc->set_commit();
    }
  }
  print_chunks_stat();
  for (int i = 0; i < 10 ; i += 2) {
    allocators.at(i)->free(ptrs.at(i), 129);
  }
  for (int i = 1; i < 9 ; i += 2) {
    allocators.at(i)->free(ptrs.at(i), 129);
  }
  assert(129 == get_usage());
  assert(160 == get_allocated());
  allocators.at(9)->free(ptrs.at(9), 129);
  print_chunks_stat();
}

TEST_F(ChunkAllocatorTest, multi_test) {
  init(10, 3, true);
  std::vector<char *> ptrs;
  std::vector<int64_t> chunks;
  std::vector<ChunkAllocator *>allocators;
  std::vector<int64_t> sizes;
  for (int i = 0 ;i < 20000; ++i) {
    int64_t id = 0;
    int64_t size = std::rand() % (2 * 1024) + 1;
    ChunkAllocator *alloc = nullptr;
    ptrs.push_back((char *)cache_alloc(size, id, alloc));
    chunks.push_back(id);
    allocators.push_back(alloc);
    sizes.push_back(size);
    if (nullptr != alloc) {
      alloc->set_commit();
    }
  }
  print_chunks_stat();
  for (int i = 0 ;i < 20000; i ++) {
    ChunkAllocator *alloc = allocators.at(i);
    if (nullptr == alloc) {
      assert(nullptr == ptrs.at(i));
    } else {
      alloc->free(ptrs.at(i), sizes.at(i));
    }
  }
  assert(0 == get_usage());
  assert(0 == get_allocated());
  print_chunks_stat();
}

TEST_F(ChunkAllocatorTest, multi_test_rev) {
  init(10, 3, true);
  std::vector<char *> ptrs;
  std::vector<int64_t> chunks;
  std::vector<ChunkAllocator *>allocators;
  std::vector<int64_t> sizes;
  for (int i = 0 ;i < 20000; ++i) {
    int64_t id = 0;
    int64_t size = std::rand() % (2 * 1024) + 1;
    ChunkAllocator *alloc = nullptr;
    ptrs.push_back((char *)cache_alloc(size, id, alloc));
    chunks.push_back(id);
    allocators.push_back(alloc);
    sizes.push_back(size);
    if (nullptr != alloc) {
      alloc->set_commit();
    }
  }
  print_chunks_stat();
  for (int i = 19999 ;i >= 0; i --) {
    ChunkAllocator *alloc = allocators.at(i);
    if (nullptr == alloc) {
      assert(nullptr == ptrs.at(i));
    } else {
      alloc->free(ptrs.at(i), sizes.at(i));
    }
  }
  assert(0 == get_usage());
  assert(0 == get_allocated());
  print_chunks_stat();
}

TEST_F(ChunkAllocatorTest, evict_test) {
  init(10, 3, true);
  std::vector<char *> ptrs;
  std::vector<int64_t> chunks;
  std::vector<ChunkAllocator *>allocators;
  std::vector<int64_t> sizes;
  for (int i = 0 ;i < 20000; ++i) {
    int64_t id = 0;
    int64_t size = std::rand() % (8* 1024) + 16;
    ChunkAllocator *alloc = nullptr;
    char *ptr = (char *)cache_alloc(size, id, alloc);
    if (nullptr == ptr) {
      ptr = (char *)evict_one_and_alloc_again(size, id, alloc);
    }
    TestEntry *entry = new (ptr) TestEntry();
    entry->alloc_ = alloc;
    entry->size_ = size;
    ptrs.push_back(ptr);
    chunks.push_back(id);
    allocators.push_back(alloc);
    sizes.push_back(size);
    if (nullptr != alloc) {
      alloc->set_commit();
    }
  }
  print_chunks_stat();
  for (int i = 0 ;i < 20000; ++i) {
    int64_t id = 0;
    int64_t size = std::rand() % (7 * 1024) + 16;
    ChunkAllocator *alloc = nullptr;
    char *ptr = (char *)cache_alloc(size, id, alloc);
    if (nullptr == ptr) {
      ptr = (char *)evict_and_alloc_again(size, id, alloc);
      if (nullptr == ptr) {
        ptr = (char *)evict_one_and_alloc_again(size, id, alloc);
      }
    }
    TestEntry *entry = new (ptr) TestEntry();
    entry->alloc_ = alloc;
    entry->size_ = size;
    ptrs.push_back(ptr);
    chunks.push_back(id);
    allocators.push_back(alloc);
    sizes.push_back(size);
    if (nullptr != alloc) {
      alloc->set_commit();
    }
  }
  print_chunks_stat();
}
}
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  std::string log_path = test::TmpDir() + "/chunk_allocator_test.log";
  xengine::logger::Logger::get_log().init(log_path.c_str(), xengine::logger::INFO_LEVEL);
  return RUN_ALL_TESTS();
}
