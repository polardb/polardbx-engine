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

#include "util/concurrent_direct_file_writer.h"
#include <algorithm>
#include <vector>
#include "util/random.h"
#include "util/testharness.h"
#include "util/testutil.h"

using namespace xengine::common;

namespace xengine {
namespace util {

using xengine::util::ConcurrentDirectFileWriter;

class ConcurrentDirectFileWriterTest : public testing::Test {};

const uint32_t kMb = 1 << 20;

TEST_F(ConcurrentDirectFileWriterTest, PositionAppend) {
  class FakeWF : public WritableFile {
   public:
    explicit FakeWF()
        : size_(0), last_synced_(0), use_direct_io_(true), io_error_(false) {}
    ~FakeWF() {}

    virtual bool use_direct_io() const override { return use_direct_io_; }

    Status Append(const Slice& data) override {
      size_ += data.size();
      return Status::OK();
    }
    virtual Status Truncate(uint64_t size) override { return Status::OK(); }
    Status Close() override { return Status::OK(); }
    Status Flush() override { return Status::OK(); }
    Status Sync() override { return Status::OK(); }
    Status Fsync() override { return Status::OK(); }
    Status PositionedAppend(const Slice& data, uint64_t offset) override {
      EXPECT_EQ(use_direct_io(), true);
      EXPECT_EQ(offset % 4096, 0u);
      EXPECT_EQ(data.size() % 4096, 0u);
      return Status::OK();
    }
    void SetIOPriority(Env::IOPriority pri) override {}
    uint64_t GetFileSize() override { return size_; }
    void GetPreallocationStatus(size_t* block_size,
                                size_t* last_allocated_block) override {}
    size_t GetUniqueId(char* id, size_t max_size) const override { return 0; }
    Status InvalidateCache(size_t offset, size_t length) override {
      return Status::OK();
    }

   protected:
    Status Allocate(uint64_t offset, uint64_t len) override {
      return Status::OK();
    }

    uint64_t size_;
    uint64_t last_synced_;
    bool use_direct_io_;
    bool io_error_;
  };

  EnvOptions env_options;
  env_options.bytes_per_sync = kMb;
//  unique_ptr<FakeWF> wf(new FakeWF);
  FakeWF *wf = MOD_NEW_OBJECT(memory::ModId::kDefaultMod, FakeWF);
  unique_ptr<ConcurrentDirectFileWriter> writer(
      new ConcurrentDirectFileWriter(wf, env_options));

  EXPECT_EQ(Status::OK(), writer->init_multi_buffer());
  Random r(301);

  std::unique_ptr<char[]> large_buf(new char[10 * kMb]);
  size_t appended_bytes = 0;
  for (int i = 0; i < 1000; i++) {
    int skew_limit = (i < 700) ? 10 : 15;
    uint32_t num = r.Skewed(skew_limit) * 100 + r.Uniform(100);
    ASSERT_OK(writer->append(Slice(large_buf.get(), num)));
    appended_bytes += num;

    // Flush in a chance of 1/10.
    if (r.Uniform(10) == 0) {
      ASSERT_OK(writer->flush());
      EXPECT_EQ(writer->get_file_size(), appended_bytes);
      EXPECT_EQ(writer->get_file_size(), writer->get_imm_buffer_pos());
      EXPECT_EQ(writer->get_file_size(), writer->get_flush_pos());
    }
  }
  ASSERT_OK(writer->flush());
  // EXPECT_EQ(writer->get_file_size(), wf->GetFileSize());
  EXPECT_EQ(writer->get_file_size(), appended_bytes);
  EXPECT_EQ(writer->get_file_size(), writer->get_imm_buffer_pos());
  EXPECT_EQ(writer->get_file_size(), writer->get_flush_pos());

  ASSERT_OK(writer->close());
}

TEST_F(ConcurrentDirectFileWriterTest, SwitchLogBuffer) {
  class FakeWF : public WritableFile {
   public:
    explicit FakeWF() : use_direct_io_(true), io_error_(false) {}

    virtual bool use_direct_io() const override { return use_direct_io_; }
    Status Append(const Slice& data) override {
      if (io_error_) {
        return Status::IOError("Fake IO error");
      }
      return Status::OK();
    }
    Status PositionedAppend(const Slice& data, uint64_t offset) override {
      if (io_error_) {
        return Status::IOError("Fake IO error");
      }
      EXPECT_EQ(use_direct_io(), true);
      EXPECT_EQ(offset % 4096, 0u);
      EXPECT_EQ(data.size() % 4096, 0u);
      return Status::OK();
    }
    Status Close() override { return Status::OK(); }
    Status Flush() override { return Status::OK(); }
    Status Sync() override { return Status::OK(); }
    void Setuse_direct_io(bool val) { use_direct_io_ = val; }
    void SetIOError(bool val) { io_error_ = val; }

   protected:
    bool use_direct_io_;
    bool io_error_;
  };
  EnvOptions env_options = EnvOptions();
  env_options.concurrent_writable_file_buffer_num = 8;
  env_options.concurrent_writable_file_single_buffer_size = 1024 * 1024;
  env_options.concurrent_writable_file_buffer_switch_limit = 512 * 1024;
  env_options.use_direct_writes = 1;
//  unique_ptr<FakeWF> wf(new FakeWF());
  FakeWF *wf = MOD_NEW_OBJECT(memory::ModId::kDefaultMod, FakeWF);
  wf->Setuse_direct_io(true);
  unique_ptr<ConcurrentDirectFileWriter> writer(
      new ConcurrentDirectFileWriter(wf, env_options));

  EXPECT_EQ(Status::OK(), writer->init_multi_buffer());

  ASSERT_OK(writer->append(std::string(2 * kMb, 'a')));

  EXPECT_EQ(writer->get_file_size(), 2 * kMb);
  // test auto switch, we will switch every 512 * 1024 bytes, so with 2kMb
  // ,switch 3 times
  EXPECT_EQ(4 * env_options.concurrent_writable_file_buffer_switch_limit,
            writer->get_imm_buffer_pos());

  writer->switch_buffer();

  EXPECT_EQ(2 * kMb, writer->get_imm_buffer_pos());

  ASSERT_OK(writer->append(std::string(1024, 'a')));

  writer->switch_buffer();

  // test switch affter  switch
  EXPECT_EQ(2 * kMb + 1024, writer->get_imm_buffer_pos());

  ASSERT_OK(writer->flush());

  // test append after flush
  ASSERT_OK(writer->append(std::string(1024, 'a')));

  EXPECT_EQ(0, writer->get_imm_buffer_num());

  EXPECT_EQ(2 * kMb + 1024, writer->get_flush_pos());

  EXPECT_EQ(2 * kMb + 1024, writer->get_imm_buffer_pos());

  EXPECT_EQ(2 * kMb + 1024 + 1024, writer->get_file_size());

  // Next call to WritableFile::Append() should fail
  dynamic_cast<FakeWF*>(writer->writable_file())->SetIOError(true);

  // will still switch succeed , we have one imm buffer
  int switch_code = writer->switch_buffer();
  uint64_t file_size = writer->get_file_size();
  uint64_t imm_pos = writer->get_imm_buffer_pos();

  EXPECT_EQ(0, switch_code);
  EXPECT_EQ(1, writer->get_imm_buffer_num());
  EXPECT_EQ(file_size, imm_pos);

  // append 4kMb  ,trigger auto switch,and flush ,but failed
  // for big string single buffer reach size of
  // concurrent_wirtable_file_single_buffer_size;
  ASSERT_NOK(writer->append(std::string(8 * kMb, 'b')));
}
}  // namespace util
}  // namespace xengine

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
	xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
