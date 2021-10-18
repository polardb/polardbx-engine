//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#include "xengine/env.h"
#include "xengine/status.h"

#include <vector>
#include "util/coding.h"
#include "util/testharness.h"

using namespace xengine::common;

namespace xengine {
namespace util {

class LockTest : public testing::Test {
 public:
  static LockTest* current_;
  std::string file_;
  Env* env_;

  LockTest()
      : file_(test::TmpDir() + "/db_testlock_file"), env_(Env::Default()) {
    current_ = this;
  }

  ~LockTest() {}

  Status LockFile(FileLock** db_lock) { return env_->LockFile(file_, db_lock); }

  Status UnlockFile(FileLock* db_lock) { return env_->UnlockFile(db_lock); }
};
LockTest* LockTest::current_;

TEST_F(LockTest, LockBySameThread) {
  FileLock* lock1;
  FileLock* lock2;

  // acquire a lock on a file
  ASSERT_OK(LockFile(&lock1));

  // re-acquire the lock on the same file. This should fail.
  ASSERT_TRUE(LockFile(&lock2).IsIOError());

  // release the lock
  ASSERT_OK(UnlockFile(lock1));
}

}  // namespace util
}  // namespace xengine

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
	xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
