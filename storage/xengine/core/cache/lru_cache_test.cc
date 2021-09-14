//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#include "cache/lru_cache.h"

#include <string>
#include <vector>
#include "util/testharness.h"

using namespace xengine;
using namespace common;
using namespace util;

namespace xengine {
namespace cache {

class LRUCacheTest : public testing::Test {
 public:
  LRUCacheTest() {}
  ~LRUCacheTest() {}

  void NewCache(size_t capacity,
      double high_pri_pool_ratio = 0.0,
      double old_pool_ratio = 0.0) {
    cache_.reset(new LRUCacheShard());
    cache_->SetCapacity(capacity);
    cache_->SetStrictCapacityLimit(false);
    cache_->SetHighPriorityPoolRatio(high_pri_pool_ratio);
    cache_->set_old_pool_ratio(old_pool_ratio);
    cache_->set_old_list_min_capacity(3);
    cache_->set_lru_old_adjust_capacity(0);
  }

  void Insert(const std::string& key,
              Cache::Priority priority = Cache::Priority::LOW) {
    cache_->Insert(key, 0 /*hash*/, nullptr /*value*/, 1 /*charge*/,
                   nullptr /*deleter*/, nullptr /*handle*/, priority);
  }

  void Insert(char key, Cache::Priority priority = Cache::Priority::LOW) {
    Insert(std::string(1, key), priority);
  }

  void insert_with_handle(const std::string& key,
                          Cache::Priority priority = Cache::Priority::LOW) {
    LRUHandle* e = new LRUHandle();
    cache::Cache::Handle *ee = reinterpret_cast<Cache::Handle*>(e);
    cache_->Insert(key, 0, nullptr, 1, nullptr, &ee, priority, true);
  }

  void insert_with_handle(char key, Cache::Priority priority = Cache::Priority::LOW) {
    insert_with_handle(std::string(1, key), priority);
  }

  bool Lookup(const std::string& key, uint64_t wait_time = 0) {
    auto handle = cache_->Lookup(key, 0 /*hash*/, 0, 0);
    // for test threshold time
    sleep(wait_time);
    if (handle) {
      cache_->Release(handle);
      return true;
    }
    return false;
  }

  bool Lookup(char key) { return Lookup(std::string(1, key)); }

  void Erase(const std::string& key) { cache_->Erase(key, 0 /*hash*/); }
  void print_lru(const LRUHandle* lru, const LRUHandle* start) {
    const LRUHandle *node = start;
    while (true) {
      fprintf(stderr,"%.*s", (int32_t)node->key_length, node->key_data);
      node = node->prev;
      if (node == lru) {
        break;
      }
      fprintf(stderr,"->");
    }
    fprintf(stderr,"\n");
  }

  void print_usage() {
    fprintf(stderr,"lru_usage: %lu\nlru_old_usage: %lu\n"
        "high_pool_usage: %lu\n", cache_->get_lru_usage(),
        cache_->get_lru_old_usage(), cache_->get_high_pool_usage());
  }

  void check_usage(const size_t lru_usage,
                   const size_t lru_old_usage,
                   const size_t high_pool_usage) {
    assert(lru_usage == cache_->get_lru_usage());
    assert(lru_old_usage == cache_->get_lru_old_usage());
    assert(high_pool_usage == cache_->get_high_pool_usage());
  }

  void ValidateLRUList(std::vector<std::string> keys,
                       size_t num_high_pri_pool_keys = 0) {
    LRUHandle* lru;
    LRUHandle* lru_low_pri;
    LRUHandle* lru_old_start;
    cache_->TEST_GetLRUList(&lru, &lru_low_pri, &lru_old_start);
    LRUHandle* iter = lru;
    bool in_high_pri_pool = false;
    bool in_old_pool = false;
    size_t high_pri_pool_keys = 0;
    if (iter == lru_low_pri) {
      in_high_pri_pool = true;
    }
    // print start
    fprintf(stderr,"***************************************************\n");
    fprintf(stderr,"LRU LIST: \n");
    print_lru(iter, iter->prev);
    if (nullptr != lru_old_start) {
      fprintf(stderr,"OLD LRU LIST: \n");
      print_lru(iter, lru_old_start);
    }
    if (nullptr != lru_low_pri) {
      fprintf(stderr,"LOW LRU LIST: \n");
      print_lru(iter, lru_low_pri);
    }
    // print finish
    for (const auto& key : keys) {
      iter = iter->next;
      ASSERT_NE(lru, iter);
      ASSERT_EQ(key, iter->key().ToString());
      ASSERT_EQ(in_high_pri_pool, iter->InHighPriPool());
      if (in_high_pri_pool) {
        high_pri_pool_keys++;
      }
      if (iter == lru_low_pri) {
        ASSERT_FALSE(in_high_pri_pool);
        in_high_pri_pool = true;
      }
    }
    ASSERT_EQ(lru, iter->next);
    ASSERT_TRUE(in_high_pri_pool);
    ASSERT_EQ(num_high_pri_pool_keys, high_pri_pool_keys);
  }

 private:
  std::unique_ptr<LRUCacheShard> cache_;
};

TEST_F(LRUCacheTest, BasicLRU) {
  NewCache(5);
  for (char ch = 'a'; ch <= 'e'; ch++) {
    Insert(ch);
  }
  ValidateLRUList({"a", "b", "c", "d", "e"});
  for (char ch = 'x'; ch <= 'z'; ch++) {
    Insert(ch);
  }
  ValidateLRUList({"d", "e", "x", "y", "z"});
  ASSERT_FALSE(Lookup("b"));
  ValidateLRUList({"d", "e", "x", "y", "z"});
  ASSERT_TRUE(Lookup("e"));
  ValidateLRUList({"d", "x", "y", "z", "e"});
  ASSERT_TRUE(Lookup("z"));
  ValidateLRUList({"d", "x", "y", "e", "z"});
  Erase("x");
  ValidateLRUList({"d", "y", "e", "z"});
  ASSERT_TRUE(Lookup("d"));
  ValidateLRUList({"y", "e", "z", "d"});
  Insert("u");
  ValidateLRUList({"y", "e", "z", "d", "u"});
  Insert("v");
  ValidateLRUList({"e", "z", "d", "u", "v"});
}

TEST_F(LRUCacheTest, MidPointInsertion) {
  // Allocate 2 cache entries to high-pri pool.
  NewCache(5, 0.45);

  Insert("a", Cache::Priority::LOW);
  Insert("b", Cache::Priority::LOW);
  Insert("c", Cache::Priority::LOW);
  ValidateLRUList({"a", "b", "c"}, 0);

  // Low-pri entries can take high-pri pool capacity if available
  Insert("u", Cache::Priority::LOW);
  Insert("v", Cache::Priority::LOW);
  ValidateLRUList({"a", "b", "c", "u", "v"}, 0);

  Insert("X", Cache::Priority::HIGH);
  Insert("Y", Cache::Priority::HIGH);
  ValidateLRUList({"c", "u", "v", "X", "Y"}, 2);

  // High-pri entries can overflow to low-pri pool.
  Insert("Z", Cache::Priority::HIGH);
  ValidateLRUList({"u", "v", "X", "Y", "Z"}, 2);

  // Low-pri entries will be inserted to head of low-pri pool.
  Insert("a", Cache::Priority::LOW);
  ValidateLRUList({"v", "X", "a", "Y", "Z"}, 2);

  // Low-pri entries will be inserted to head of low-pri pool after lookup.
  ASSERT_TRUE(Lookup("v"));
  ValidateLRUList({"X", "a", "v", "Y", "Z"}, 2);

  // High-pri entries will be inserted to the head of the list after lookup.
  ASSERT_TRUE(Lookup("X"));
  ValidateLRUList({"a", "v", "Y", "Z", "X"}, 2);
  ASSERT_TRUE(Lookup("Z"));
  ValidateLRUList({"a", "v", "Y", "X", "Z"}, 2);

  Erase("Y");
  ValidateLRUList({"a", "v", "X", "Z"}, 2);
  Erase("X");
  ValidateLRUList({"a", "v", "Z"}, 1);
  Insert("d", Cache::Priority::LOW);
  Insert("e", Cache::Priority::LOW);
  ValidateLRUList({"a", "v", "d", "e", "Z"}, 1);
  Insert("f", Cache::Priority::LOW);
  Insert("g", Cache::Priority::LOW);
  ValidateLRUList({"d", "e", "f", "g", "Z"}, 1);
  ASSERT_TRUE(Lookup("d"));
  ValidateLRUList({"e", "f", "g", "d", "Z"}, 1);
}

TEST_F(LRUCacheTest, test_lru_complex) {
  NewCache(10, 0.5, 0.3);
  for (char ch = 'a'; ch <= 'j'; ch++) {
    Insert(ch);
  }
  print_usage();
  ValidateLRUList({"a", "b", "c", "j", "i", "h","g", "f", "e", "d"});
  for (char ch = 'x'; ch <= 'y'; ch++) {
    Insert(ch);
  }
  print_usage();
  ValidateLRUList({"c", "x", "y", "j", "i", "h", "g", "f", "e", "d"});
  for (char ch = 'a'; ch <= 'b'; ch++) {
    Insert(ch, Cache::Priority::HIGH);
  }
  // low: y, high: a, b
  print_usage();
  ValidateLRUList({"y", "j", "i", "h", "g", "f", "e", "d", "a", "b"}, 2);
  ASSERT_TRUE(Lookup("y"));
  print_usage();
  // Low-pri entries will be inserted to head of low-pri pool after lookup from old_list/young_list.
  ValidateLRUList({"j", "i", "h", "g", "f", "e", "d", "y", "a", "b"}, 2);

  ASSERT_TRUE(Lookup("a"));
  print_usage();
  ValidateLRUList({"j", "i", "h", "g", "f", "e", "d", "y", "b", "a"}, 2);

  ASSERT_TRUE(Lookup("d", 1));
  print_usage();
  ValidateLRUList({"j", "i", "h", "g", "f", "e", "y", "d", "b", "a"}, 2);

  ASSERT_TRUE(Lookup("j"));
  print_usage();
  ValidateLRUList({"i", "h", "j", "g", "f", "e", "y", "d", "b", "a"}, 2);
}

TEST_F(LRUCacheTest, test_lru_scan) {
  NewCache(10, 0.5, 0.3);
  for (char ch = 'a'; ch <= 'j'; ch++) {
    Insert(ch);
  }
  print_usage();
  ValidateLRUList({"a", "b", "c", "j", "i", "h","g", "f", "e", "d"});

  for (char ch = 'x'; ch <= 'y'; ch++) {
    Insert(ch, Cache::Priority::HIGH);
  }

  print_usage();
  ValidateLRUList({"c", "j", "i", "h","g", "f", "e", "d", "x", "y"}, 2);
  for (char ch = 'k'; ch <= 't'; ch++) {
    Insert(ch);
  }

  print_usage();
  ValidateLRUList({"r", "s", "t", "h","g", "f", "e", "d", "x", "y"}, 2);
}

TEST_F(LRUCacheTest, test_lru_ref) {
  NewCache(5, 0, 0);
  for (char ch = 'a'; ch <= 'j'; ch++) {
    insert_with_handle(ch);
  }
  print_usage();
  ValidateLRUList({});
}

}  // namespace cache
}  // namespace xengine

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
	xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
