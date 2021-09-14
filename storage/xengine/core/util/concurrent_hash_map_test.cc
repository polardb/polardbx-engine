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

#include <assert.h>
#include <iostream>

#include "db/db_test_util.h"
#include "util/concurrent_hash_map.h"
#include "logger/logger.h"
#include "util/testutil.h"
#include <unistd.h>
#include <stdlib.h>
#include <time.h>
#include <thread>

namespace xengine {
namespace util {

class ConcurrentHashMapTest : public testing::Test {
public:
  ConcurrentHashMapTest() {
    srand(time(nullptr));
  }
  ~ConcurrentHashMapTest() = default;

  void map_insert(std::string key, int value, ConcurrentHashMap<std::string, int> *map) {
    int sleep_time = rand() % 10 + 1;
    usleep(sleep_time * 1000);
    bool result = map->insert(key, value);
    XENGINE_LOG(INFO, "insert", K(key), K(value), K(result));
  }
  void map_put(std::string key, int value, ConcurrentHashMap<std::string, int> *map) {
    int sleep_time = rand() % 10 + 1;
    usleep(sleep_time * 1000);
    bool result = map->put(key, value);
    XENGINE_LOG(INFO, "put", K(key), K(value), K(result));
  }
  void map_erase(std::string key, ConcurrentHashMap<std::string, int> *map) {
    int sleep_time = rand() % 10 + 1;
    usleep(sleep_time * 1000);
    bool result = map->erase(key);
    XENGINE_LOG(INFO, "erase", K(key), K(result));
  }

};

TEST_F(ConcurrentHashMapTest, test_single_thread) {
  ConcurrentHashMap<std::string, std::string> map;
  ASSERT_TRUE(map.insert("key0", "value0"));
  ASSERT_FALSE(map.insert("key0", "value1"));
  ASSERT_TRUE(map.insert("key1", "value1"));
  ASSERT_FALSE(map.put("key1", "new_value1"));
  ASSERT_TRUE(map.insert("key2", "value2"));
  ASSERT_TRUE(map.insert("key3", "value3"));
  ASSERT_EQ(4, map.size());
  ASSERT_EQ(1, map.count("key0"));
  ASSERT_EQ("value0", map.at("key0"));
  ASSERT_EQ("new_value1", map.at("key1"));
  ASSERT_EQ("value2", map.at("key2"));
  ASSERT_EQ("value3", map.at("key3"));
  ASSERT_TRUE(map.erase("key3"));
  ASSERT_FALSE(map.erase("test"));
  ASSERT_EQ(3, map.size());
  ASSERT_EQ(map.end(), map.find("key3"));
  int i = 0;
  for (auto& entry : map) {
    XENGINE_LOG(INFO, "map", K(entry.first), K(entry.second), K(i));
    i++;
  }
  map.clear();
  ASSERT_EQ(0, map.count("key0"));
  ASSERT_EQ(0, map.size());
  map["hello"] = "world";
  ASSERT_EQ("world", map["hello"]);
  auto it = map.find("hello");
  ASSERT_NE(map.end(), it);
  for (it = map.begin(); it != map.end(); ++it) {
    XENGINE_LOG(INFO, "map", K(it->first), K(it->second));
  }
}

TEST_F(ConcurrentHashMapTest, test_multi_thread) {
  ConcurrentHashMap<std::string, int> map;
  std::thread threads[60];
  for (int i = 0; i < 20; ++i) {
    threads[i] = std::thread(std::bind(&ConcurrentHashMapTest::map_insert, this, std::to_string(i), i, &map));
  }
  for (int i = 0; i < 20; ++i) {
    threads[i + 20] = std::thread(std::bind(&ConcurrentHashMapTest::map_put, this, std::to_string(i), i + 1, &map));
  }
  for (int i = 0; i < 40; i++) {
    threads[i].join();
  }
  ASSERT_EQ(20, map.size());
  for (auto& entry : map) {
    XENGINE_LOG(INFO, "map", K(entry.first), K(entry.second));
    ASSERT_EQ(std::stoi(entry.first), entry.second - 1);
  }
  for (int i = 0; i < 20; ++i) {
    threads[i + 40] = std::thread(std::bind(&ConcurrentHashMapTest::map_erase, this, std::to_string(i), &map));
  }
  for (int i = 40; i < 60; i++) {
    threads[i].join();
  }
  ASSERT_EQ(0, map.size());
}

} // namespace util
} // namespace xengine

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
	xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
