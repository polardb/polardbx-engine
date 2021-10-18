// Copyright (c) 2016-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#ifndef ROCKSDB_LITE

#include "xengine/utilities/object_registry.h"
#include "util/testharness.h"

using namespace xengine::common;

namespace xengine {
namespace util {

class EnvRegistryTest : public testing::Test {
 public:
  static int num_a, num_b;
};

int EnvRegistryTest::num_a = 0;
int EnvRegistryTest::num_b = 0;

static Registrar<Env> test_reg_a("a://.*", [](const std::string& uri,
                                              std::unique_ptr<Env>* env_guard) {
  ++EnvRegistryTest::num_a;
  return Env::Default();
});

static Registrar<Env> test_reg_b("b://.*", [](const std::string& uri,
                                              std::unique_ptr<Env>* env_guard) {
  ++EnvRegistryTest::num_b;
  // Env::Default() is a singleton so we can't grant ownership directly to the
  // caller - we must wrap it first.
  env_guard->reset(new EnvWrapper(Env::Default()));
  return env_guard->get();
});

TEST_F(EnvRegistryTest, Basics) {
  std::unique_ptr<Env> env_guard;
  auto res = NewCustomObject<Env>("a://test", &env_guard);
  ASSERT_NE(res, nullptr);
  ASSERT_EQ(env_guard, nullptr);
  ASSERT_EQ(1, num_a);
  ASSERT_EQ(0, num_b);

  res = NewCustomObject<Env>("b://test", &env_guard);
  ASSERT_NE(res, nullptr);
  ASSERT_NE(env_guard, nullptr);
  ASSERT_EQ(1, num_a);
  ASSERT_EQ(1, num_b);

  res = NewCustomObject<Env>("c://test", &env_guard);
  ASSERT_EQ(res, nullptr);
  ASSERT_EQ(env_guard, nullptr);
  ASSERT_EQ(1, num_a);
  ASSERT_EQ(1, num_b);
}

}  //  namespace util
}  //  namespace xengine

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
	xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}

#else  // ROCKSDB_LITE
#include <stdio.h>

int main(int argc, char** argv) {
  fprintf(stderr, "SKIPPED as EnvRegistry is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // ROCKSDB_LITE
