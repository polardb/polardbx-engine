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
#pragma once

#include "util/aio_wrapper.h"
#include "memory/base_malloc.h"
#include "util/coding.h"
#include "util/serialization.h"
#include "xengine/env.h"
#include "xengine/xengine_constants.h"
#include "xengine/rate_limiter.h"
#include "storage_common.h"

namespace xengine {

namespace table {
class Footer;
}

namespace util
{
class AIOInfo;
}

namespace storage {

class ExtentSpaceManager;

// change system error number to string
extern const size_t PAGE_SIZE;
extern size_t upper(const size_t size, const size_t fac);
extern std::unique_ptr<void, void (&)(void *)> NewAligned(const size_t size);
common::Status io_error(const std::string &context, int err_number);
extern common::Status unintr_pwrite(const int32_t fd, const char *src,
                                    const int64_t len, const int64_t pos,
                                    bool dio = true);
extern common::Status unintr_pread(const int32_t fd, char *dest,
                                   const int64_t len, const int64_t pos);
extern std::unique_ptr<void, void (&)(void *)> new_aligned(const size_t size);

// class WritableExtent
// The fixed size of sst it's space can be reused by upper layer.
// The main purpose is speedup the compaction process. When
// the compaction range has some untouched extents and just
// reference the old ones into the output range.
class WritableExtent : public util::WritableFile {
 public:
  WritableExtent();
  virtual ~WritableExtent();
  void operator=(const WritableExtent &r);

  int init(const ExtentIOInfo &io_info);
  void reset();
  // Means Close() will properly take care of truncate
  // and it does not need any additional information
  virtual common::Status Truncate(uint64_t size) override {
    return common::Status::OK();
  }
  virtual common::Status Close() override { return common::Status::OK(); }
  // write the _data to extent
  virtual common::Status Append(const common::Slice &data) override;
  // DO NOT USE, only used in unittest
  virtual common::Status AsyncAppend(util::AIO &aio, util::AIOReq *req, const common::Slice &data);
  virtual common::Status Flush() override { return common::Status::OK(); }
  virtual common::Status Sync() override { return common::Status::OK(); }
  virtual common::Status Fsync() override { return common::Status::OK(); }
  virtual bool IsSyncThreadSafe() const override { return true; }
  virtual uint64_t GetFileSize() override { return current_write_size_; }
  // use direct io
  bool UseOSBuffer() const { return false; }
  size_t GetRequiredBufferAlignment() const override { return 4 * 1024; }
  bool UseDirectIO() const { return true; }
  size_t GetUniqueId(char *id, size_t max_size) const override;
  common::Status InvalidateCache(size_t offset, size_t length) override {
    return common::Status::OK();
  }

  const ExtentId &get_extent_id() const { return io_info_.extent_id_; }
  int32_t get_extent_size() const { return current_write_size_; }

 private:
  ExtentIOInfo io_info_;
  int32_t current_write_size_;
};

// random access extent (read data from extent)
// direct IO implementation
class RandomAccessExtent : public util::RandomAccessFile {
public:
  RandomAccessExtent();
  virtual ~RandomAccessExtent();
  void destroy();
  virtual int init(const ExtentIOInfo &io_info, ExtentSpaceManager *space_manager);
  // read n byte from the offset of the extent
  common::Status Read(uint64_t offset, size_t n, common::Slice *result,
                      char *scratch) const override;
  size_t GetUniqueId(char *id, size_t max_size) const override;
  ExtentSpaceManager *space_manager() { return space_manager_; }

  // convert the offset in the extent to the offset in the file
  virtual int fill_aio_info(const int64_t offset, const int64_t size, util::AIOInfo &aio_info) const override;

protected:
  ExtentIOInfo io_info_;
  ExtentSpaceManager *space_manager_;
};

class AsyncRandomAccessExtent : public RandomAccessExtent {
 public:
  AsyncRandomAccessExtent();
  ~AsyncRandomAccessExtent();

  virtual int init(const ExtentIOInfo &io_info, ExtentSpaceManager *space_manager);
  void set_rate_limiter(util::RateLimiter *rate_limiter) {
    rate_limiter_ = rate_limiter;
  }
  // call prefetch & read in pair
  int prefetch();
  common::Status Read(uint64_t offset, size_t n, common::Slice *result,
                      char *scratch) const override;

  void reset()
  {
    aio_ = nullptr;
    aio_req_.reset();
    rate_limiter_ = nullptr;
  }

 private:
  int reap();

  util::AIO *aio_;
  util::AIOReq aio_req_;

  util::RateLimiter* rate_limiter_;
};

}  // storage
}  // xengine
