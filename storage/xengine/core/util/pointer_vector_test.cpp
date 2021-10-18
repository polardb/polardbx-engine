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

#include "gtest/gtest.h"
#include "pointer_vector.h"
using namespace xengine;
using namespace util;

struct Nice
{
  int fi;
  Nice(int i) : fi(i) {}
  static void dump(const Nice* n)
  {
    printf("nice value:%d\n", n->fi);
  }
  static void free(Nice* n)
  {
    if (NULL != n) delete n;
  }
};

bool compare(const Nice * a, const Nice * b)
{
  return a->fi < b->fi;
}

bool unique_a(const Nice * a, const Nice * b)
{
  return a->fi == b->fi;
}


int test_add_pointer_vector(PointerSortedVector<Nice*> & v, int i)
{
  PointerSortedVector<Nice*>::iterator iter;
  const Nice *n1  = new Nice(i);
  int ret = v.insert(n1, iter, compare);
  //printf("insert ele:%d,%p, ret pos:%p, ret:%d\n", i, n1, iter, ret);
  return ret;
}

int test_add_pointer_vector_unique(PointerSortedVector<Nice*> & v, int i)
{
  PointerSortedVector<Nice*>::iterator iter;
  const Nice *n1  = new Nice(i);
  int ret = v.insert_unique(n1, iter, compare, unique_a);
  //printf("insert ele:%d,%p, ret pos:%p,%p, ret:%d\n", i, n1, iter, (iter == NULL) ? 0: *iter, ret);
  return ret;
}


TEST(TestPointerVector, insert)
{
  PointerVector<Nice*> v; 
  EXPECT_EQ(0, v.size());
  EXPECT_EQ(0, v.capacity());
  v.push_back(new Nice(1));
  EXPECT_EQ(2, v.capacity());
  v.push_back(new Nice(2));
  EXPECT_EQ(2, v.capacity());
  v.push_back(new Nice(5));
  EXPECT_EQ(6, v.capacity());
  EXPECT_EQ(1, v.at(0)->fi);
  EXPECT_EQ(2, v.at(1)->fi);
  EXPECT_EQ(5, v.at(2)->fi);
  std::for_each(v.begin(), v.end(), Nice::free);
}

TEST(TestPointerVector, insert2)
{
  PointerVector<Nice*> v; 
  v.reserve(20000);
  for (int i = 0; i < 10000; ++i)
  {
    v.push_back(new Nice(i));
  }
  for (int i = 0; i < 10000; ++i)
  {
    EXPECT_EQ(i, v.at(i)->fi);
  }
  std::for_each(v.begin(), v.end(), Nice::free);
}

TEST(TestPointerVector, insert_batch)
{
  PointerVector<int64_t> v; 
  EXPECT_EQ(0, v.size());
  EXPECT_EQ(0, v.capacity());
  v.push_back(1);
  EXPECT_EQ(2, v.capacity());
  v.push_back(2);
  EXPECT_EQ(2, v.capacity());
  v.push_back(5);
  EXPECT_EQ(6, v.capacity());

  int64_t add_1[] = { 10, 20, 30 };
  v.insert(v.begin(), add_1, add_1+3);
  EXPECT_EQ(6, v.capacity());
  EXPECT_EQ(0, v.remain());
  
  int64_t add_2[] = { 100, 200, 300, 400 };
  v.insert(v.begin() + 2, add_2, add_2+3);
  EXPECT_EQ(18, v.capacity());
  EXPECT_EQ(9, v.remain());

  int64_t r[] = { 10, 20, 100, 200, 300, 30, 1, 2, 5 };
  EXPECT_TRUE(std::equal(v.begin(), v.end(), r));
  //char output[1024];
  //v.to_string(output, 1024);
  //fprintf(stderr, "insert r:%s\n", output);
}

TEST(TestPointerSortedVector, insert)
{
  PointerSortedVector<Nice*> v; 
  EXPECT_EQ(0, test_add_pointer_vector(v, 1));
  EXPECT_EQ(0, test_add_pointer_vector(v, 20));
  EXPECT_EQ(0, test_add_pointer_vector(v, 10));
  EXPECT_EQ(0, test_add_pointer_vector(v, 5));
  EXPECT_EQ(1, v.at(0)->fi);
  EXPECT_EQ(5, v.at(1)->fi);
  EXPECT_EQ(10, v.at(2)->fi);
  EXPECT_EQ(20, v.at(3)->fi);
  std::for_each(v.begin(), v.end(), Nice::free);
}

TEST(TestPointerSortedVector, insert2)
{
  PointerSortedVector<Nice*> v; 
  v.reserve(20000);
  
  for (int i = 10000; i > 0; --i)
  {
    test_add_pointer_vector(v, i);
  }
  std::for_each(v.begin(), v.end(), Nice::free);
}

TEST(TestPointerSortedVector, insert3)
{
  PointerSortedVector<Nice*> v; 
  v.reserve(20000);
  
  for (int i = 0; i < 10000; ++i)
  {
    test_add_pointer_vector(v, i);
  }
  std::for_each(v.begin(), v.end(), Nice::free);
}

TEST(TestPointerSortedVector, insert_unique)
{
  PointerSortedVector<Nice*> v; 
  EXPECT_EQ(0, test_add_pointer_vector_unique(v, 1));
  EXPECT_EQ(0, test_add_pointer_vector_unique(v, 20));
  EXPECT_NE(0, test_add_pointer_vector_unique(v, 20));
  EXPECT_EQ(0, test_add_pointer_vector_unique(v, 5));
  EXPECT_EQ(1, v.at(0)->fi);
  EXPECT_EQ(5, v.at(1)->fi);
  EXPECT_EQ(20, v.at(2)->fi);
  std::for_each(v.begin(), v.end(), Nice::free);
}

TEST(TestPointerSortedVector, test_find)
{
  PointerSortedVector<Nice*> v; 
  v.reserve(20000);
  
  for (int i = 0; i < 10000; ++i)
  {
    EXPECT_EQ(0, test_add_pointer_vector_unique(v, i));
  }

  for (int i = 0; i < 10000; ++i)
  {
    PointerSortedVector<Nice*>::iterator fiter;
    const Nice* pn = new Nice(i);
    EXPECT_EQ(0, v.find(pn, fiter, compare));
    EXPECT_EQ(i, (*fiter)->fi);
  }

  std::for_each(v.begin(), v.end(), Nice::free);
}

TEST(TestPointerSortedVector, insert_batch)
{
  PointerSortedVector<int64_t> v; 
  EXPECT_EQ(0, v.size());
  EXPECT_EQ(0, v.capacity());
  v.push_back(1);
  EXPECT_EQ(2, v.capacity());
  v.push_back(20);
  EXPECT_EQ(2, v.capacity());
  v.push_back(500);
  EXPECT_EQ(6, v.capacity());

  int64_t add_1[] = { 40, 50, 60 };

  PointerSortedVector<int64_t>::iterator pos;

  int r = v.insert(add_1, add_1+3, pos, std::less<int64_t>());
  EXPECT_EQ(6, v.capacity());
  EXPECT_EQ(0, v.remain());
  EXPECT_EQ(0, r);

  int64_t add_2[] = { 501, 502, 503};
  r = v.insert(add_2, add_2+3, pos, std::less<int64_t>());
  EXPECT_EQ(0, r);
  int64_t add_3[] = { 45, 48, 52};
  r = v.insert(add_3, add_3+3, pos, std::less<int64_t>());
  EXPECT_EQ(E_DATA_CORRUPT, r);
  
  char output[1024];
  v.to_string(output, 1024);
  fprintf(stderr, "insert r:%s\n", output);
}

/*
int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
*/


