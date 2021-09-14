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

#include "storage/io_extent.h"
#include <unistd.h>

#include "storage/extent_space_manager.h"
#include "table/format.h"

using namespace xengine;
using namespace util;
using namespace common;
using namespace table;

namespace xengine {

namespace table {
extern const uint64_t kExtentBasedTableMagicNumber;
}

namespace storage {

// change system error number to string
Status io_error(const std::string &context, int err_number) {
  switch (err_number) {
    case ENOSPC:
      return Status::NoSpace(context, strerror(err_number));
    case ESTALE:
      return Status(Status::kStaleFile);
    default:
      return Status::IOError(context, strerror(err_number));
  }
}

/*
 * DirectIOHelper
 * Aligned buffer for read and write
 */
const size_t SECTOR_SIZE = 4096;
const size_t PAGE_SIZE = sysconf(_SC_PAGESIZE);

std::unique_ptr<void, void (&)(void *)> new_aligned(const size_t size) {
  void *ptr = nullptr;
  if (posix_memalign(&ptr, 4 * 1024, size) != 0) {
    return std::unique_ptr<char, void(&)(void *)>(nullptr, free);
  }
  std::unique_ptr<void, void(&)(void *)> uptr(ptr, free);
  return uptr;
}

size_t upper(const size_t size, const size_t fac) {
  if (size % fac == 0) {
    return size;
  }
  return size + (fac - size % fac);
}

size_t lower(const size_t size, const size_t fac) {
  if (size % fac == 0) {
    return size;
  }
  return size - (size % fac);
}

bool is_sector_aligned(const size_t off) { return off % SECTOR_SIZE == 0; }

static bool is_page_aligned(const void *ptr) {
  return uintptr_t(ptr) % (PAGE_SIZE) == 0;
}

Status unintr_pread(int32_t fd, char *dest, const int64_t len,
                    const int64_t pos) {
  assert(is_sector_aligned(pos));
  assert(is_sector_aligned(len));
  assert(is_page_aligned(dest));

  int64_t bytes_read = 0;
  ssize_t status = -1;
  while (bytes_read < len) {
    status = pread(fd, dest + bytes_read, len - bytes_read, pos + bytes_read);
    if (status <= 0) {
      if (errno == EINTR) {
        continue;
      }
      break;
    }
    bytes_read += status;
  }

  return status < 0 ? Status::IOError(strerror(errno)) : Status::OK();
}

Status read_aligned(int fd, Slice *data, const uint64_t offset,
                    const size_t size, char *scratch) {
  Status s = unintr_pread(fd, scratch, size, offset);
  if (s.ok()) {
    *data = Slice(scratch, size);
  }

  return s;
}

Status read_unaligned(int fd, Slice *data, const uint64_t offset,
                      const size_t size, char *scratch) {
  assert(scratch);
  assert(!is_sector_aligned(offset) || !is_sector_aligned(size) ||
         !is_page_aligned(scratch));

  const uint64_t aligned_off = lower(offset, SECTOR_SIZE);
  const size_t aligned_size = upper(size + (offset - aligned_off), SECTOR_SIZE);
  auto aligned_scratch = new_aligned(aligned_size);
  assert(aligned_scratch);
  if (!aligned_scratch) {
    return Status::IOError("Unable to allocate");
  }

  assert(is_sector_aligned(aligned_off));
  assert(is_sector_aligned(aligned_size));
  assert(aligned_scratch);
  assert(is_page_aligned(aligned_scratch.get()));
  assert(offset + size <= aligned_off + aligned_size);

  Slice scratch_slice;
  Status s = read_aligned(fd, &scratch_slice, aligned_off, aligned_size,
                          reinterpret_cast<char *>(aligned_scratch.get()));

  // copy data upto min(size, what was read)
  memcpy(scratch, reinterpret_cast<char *>(aligned_scratch.get()) +
                      (offset % SECTOR_SIZE),
         std::min(size, scratch_slice.size()));
  *data = Slice(scratch, std::min(size, scratch_slice.size()));
  return s;
}

// read data in aligned or unaligned buffer
Status direct_io_read(int fd, Slice *result, size_t off, size_t n,
                      char *scratch) {
  if (is_sector_aligned(off) && is_sector_aligned(n) &&
      is_page_aligned(scratch)) {
    return read_aligned(fd, result, off, n, scratch);
  }
  return read_unaligned(fd, result, off, n, scratch);
}

Status unintr_pwrite(const int32_t fd, const char *src, const int64_t len,
                     const int64_t pos, bool dio) {
  if (dio && (!is_sector_aligned(len) || !is_page_aligned(src))) {
    return Status::IOError("Unaligned buffer for direct IO.");
  }

  int64_t left = len;
  int64_t start = pos;
  while (left != 0) {
    ssize_t done = pwrite(fd, src, left, start);
    if (done < 0) {
      if (errno == EINTR) {
        continue;
      }
      return io_error("unintr_pwrite", errno);
    }
    left -= done;
    src += done;
    start += done;
  }
  return Status::OK();
}

WritableExtent::WritableExtent()
    : io_info_(),
      current_write_size_(0)
{
}

WritableExtent::~WritableExtent()
{
}

void WritableExtent::operator=(const WritableExtent &r)
{
  io_info_ = r.io_info_;
  current_write_size_ = r.current_write_size_;
}

int WritableExtent::init(const ExtentIOInfo &io_info)
{
  int ret = Status::kOk;

  if (!io_info.is_valid()) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(io_info));
  } else {
    io_info_ = io_info;
  }

  return ret;
}

void WritableExtent::reset()
{
  io_info_.reset();
  current_write_size_ = 0;
}

// use direct io to write data
Status WritableExtent::Append(const Slice &data)
{
  int ret = Status::kOk;

  if ((current_write_size_ + data.size()) > static_cast<uint64_t>(io_info_.extent_size_)) {
    ret = Status::kIOError;
    XENGINE_LOG(WARN, "io error, extent overflow", K(ret), K_(current_write_size), K_(io_info), "data_size", data.size());
  } else if (FAILED(unintr_pwrite(io_info_.fd_, data.data(), data.size(), io_info_.get_offset() + current_write_size_).code())) {
      XENGINE_LOG(WARN, "fail to write extent", K(ret), K_(current_write_size), K_(io_info), "data_size", data.size());
  } else {
    current_write_size_ += data.size();
    XENGINE_LOG(DEBUG, "success to write extent", K_(current_write_size), K_(io_info), "data_size", data.size());
  }

  return Status(ret);
}

// DO NOT USE, only used in unittest
Status WritableExtent::AsyncAppend(util::AIO &aio, util::AIOReq *req, const Slice &data) {
  int ret = Status::kOk;
  AIOInfo aio_info(io_info_.fd_, io_info_.get_offset(), data.size());
  if (ISNULL(req)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "req is null", K(ret));
  } else if (FAILED(req->prepare_write(aio_info, data.data()))) {
    XENGINE_LOG(WARN, "failed to prepare_write", K(ret), K(*req));
  } else if (FAILED(aio.submit(req, 1))) {
    XENGINE_LOG(WARN, "failed to submit aio request", K(ret), K(*req));
  }
  return Status(ret);
}

size_t WritableExtent::GetUniqueId(char *id, size_t max_size) const {
  size_t len = std::min(sizeof(io_info_.unique_id_), max_size);
  memcpy(id, (char *)(&io_info_.unique_id_), len);
  return len;
}

RandomAccessExtent::RandomAccessExtent()
    : io_info_(),
      space_manager_(nullptr)
{
}

RandomAccessExtent::~RandomAccessExtent()
{
  destroy();
}

int RandomAccessExtent::init(const ExtentIOInfo &io_info, ExtentSpaceManager *space_manager)
{
  int ret = Status::kOk;

  if (UNLIKELY(!io_info.is_valid())) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(INFO, "invalid argument", K(ret), K(io_info));
  } else {
    io_info_ = io_info;
    space_manager_ = space_manager;
  }

  return ret;
}

void RandomAccessExtent::destroy()
{
  io_info_.reset();  
  space_manager_ = nullptr;
}

Status RandomAccessExtent::Read(uint64_t offset, size_t n, Slice *result,
                                char *scratch) const
{
  int ret = Status::kOk;

  if ((offset + n) > static_cast<uint64_t>(io_info_.extent_size_)) {
    ret = Status::kIOError;
    XENGINE_LOG(WARN, "io error, extent overflow", K(ret), K(offset), K(n), K_(io_info));
  } else if (FAILED(direct_io_read(io_info_.fd_, result, io_info_.get_offset() + offset, n, scratch).code())) {
    XENGINE_LOG(WARN, "fail to read extent", K(ret), K(offset), K(n), K_(io_info));
  } else {
    //XENGINE_LOG(DEBUG, "success to read extent", K(offset), K(n), K_(io_info));
  }

  return Status(ret);
}

int RandomAccessExtent::fill_aio_info(const int64_t offset, const int64_t size, AIOInfo &aio_info) const
{
  int ret = Status::kOk;
  aio_info.reset();
  if (offset + size > io_info_.extent_size_) {
    ret = Status::kNotSupported;
    XENGINE_LOG(INFO, "larger than one extent, will try sync IO!");
  } else {
    aio_info.fd_ = io_info_.fd_;
    // convert the offset in the extent to the offset in the file
    aio_info.offset_ = io_info_.get_offset() + offset;
    aio_info.size_ = size;
  }
  return ret;
}

size_t RandomAccessExtent::GetUniqueId(char *id, size_t max_size) const {
  size_t len = std::min(sizeof(io_info_.unique_id_), max_size);
  memcpy(id, (char *)(&(io_info_.unique_id_)), len);
  return len;
}

AsyncRandomAccessExtent::AsyncRandomAccessExtent()
  : aio_(nullptr),
    aio_req_(),
    rate_limiter_(nullptr)
{
}

AsyncRandomAccessExtent::~AsyncRandomAccessExtent() {}

int AsyncRandomAccessExtent::init(const ExtentIOInfo &io_info, ExtentSpaceManager *space_manager)
{
  int ret = Status::kOk;

  if (UNLIKELY(!io_info.is_valid()) || IS_NULL(space_manager)) {
    ret = Status::kInvalidArgument;
    XENGINE_LOG(WARN, "invalid argument", K(ret), K(io_info), KP(space_manager));
  } else if (FAILED(RandomAccessExtent::init(io_info, space_manager))) {
    XENGINE_LOG(WARN, "fail to init random access extent", K(ret), K(io_info));
  } else {
    aio_req_.status_ = Status::kInvalidArgument;  // not prepare
    if (ISNULL(aio_ = AIO::get_instance())) {
      ret = Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "aio instance is nullptr", K(ret));
    }
  }

  return ret;
}

int AsyncRandomAccessExtent::prefetch() {
  int ret = Status::kOk;
  AIOInfo aio_info(io_info_.fd_, io_info_.get_offset(), MAX_EXTENT_SIZE);
  if (FAILED(aio_req_.prepare_read(aio_info))) {
    XENGINE_LOG(WARN, "failed to prepare iocb", K(ret));
  } else {
    if (nullptr != rate_limiter_) {
      rate_limiter_->Request(MAX_EXTENT_SIZE, Env::IOPriority::IO_LOW, nullptr /*stats_*/);
    }
    if (FAILED(aio_->submit(&aio_req_, 1))) {
      XENGINE_LOG(WARN, "failed to submit aio request", K(ret));
    }
  }
  return ret;
}

int AsyncRandomAccessExtent::reap() {
  if (Status::kBusy == aio_req_.status_) {
    // have submitted but not reap
    if ((Status::kOk != aio_->reap(&aio_req_)) ||
        (Status::kOk != aio_req_.status_)) {
      aio_req_.status_ = Status::kIOError;
      return Status::kIOError;
    }
  }
  return aio_req_.status_;
}

Status AsyncRandomAccessExtent::Read(uint64_t offset, size_t n, Slice *result,
                                     char *scratch) const {
  int ret = 0;
  if (Status::kOk !=
      (ret = const_cast<AsyncRandomAccessExtent *>(this)->reap())) {
    return Status(ret);
  } else if ((int64_t)(offset + n) > aio_req_.aio_buf_.size()) {
    return Status::NoSpace("Read out of bound.");
  } else {
    *result = Slice(aio_req_.aio_buf_.data() + offset, n);
    XENGINE_LOG(DEBUG, "success async read", K_(io_info), K(offset), K(n));
  }
  return Status::kOk;
}


}  // storage
}  // xengine
