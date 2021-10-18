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

#include <stdio.h>
#include <libaio.h>
#include <unistd.h>
#include <unordered_set>
//#include "env/env_myfs.h"
#include "xengine/status.h"
#include "logger/logger.h"
#include "xengine/xengine_constants.h"
#include "monitoring/query_perf_context.h"
#include "thread_local.h"

namespace xengine {
namespace util {

const int64_t DIO_ALIGN_SIZE = 4096;

class AIOBuffer
{
public:
  AIOBuffer() : io_buf_(nullptr),
                aligned_offset_(0),
                aligned_size_(0),
                data_buf_(nullptr),
                data_size_(0) {}

  ~AIOBuffer() { reset(); }

  bool is_valid()
  {
    return io_buf_ != nullptr
        && aligned_offset_ >= 0
        && aligned_size_ > 0
        && is_aligned(aligned_offset_)
        && is_aligned(aligned_size_)
        && is_aligned(uintptr_t(io_buf_));
  }

  void reset()
  {
    if (nullptr != io_buf_) {
      memory::base_memalign_free(io_buf_);
      io_buf_ = nullptr;
    }
    aligned_offset_ = 0;
    aligned_size_ = 0;
    data_buf_ = nullptr;
    data_size_ = 0;
  }

  char *data() const { return data_buf_; }
  int64_t size() const { return data_size_; }
  char *aio_buf() { return io_buf_; }
  int64_t aio_size() { return aligned_size_; }
  int64_t aio_offset() { return aligned_offset_; }
  void set_size(int64_t actual_size) {
    aligned_size_ = actual_size;
  }

  // for AIO write, offset and size should be aligned
  int prepare_write_io_buf(const int64_t offset, const int64_t size, const char* data)
  {
    int ret = common::Status::kOk;
    if (UNLIKELY(offset < 0) || UNLIKELY(size <= 0)) {
      ret = common::Status::kInvalidArgument;
      XENGINE_LOG(WARN, "invalid argument", K(ret), K(offset), K(size));
    } else if (UNLIKELY(nullptr != io_buf_)) {
      ret = common::Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "io buf is not null", K(ret), KP(io_buf_));
    } else if (FAILED(alloc_io_buf(offset, size))) {
      XENGINE_LOG(WARN, "failed to alloc io buf", K(ret), K(offset), K(size));
    } else {
      memcpy(io_buf_, data, size);
      data_size_ = size;
    }
    return ret;
  }

  int prepare_read_io_buf(const int64_t offset, const int64_t size)
  {
    int ret = common::Status::kOk;
    if (UNLIKELY(offset < 0) || UNLIKELY(size <= 0)) {
      ret = common::Status::kInvalidArgument;
      XENGINE_LOG(WARN, "invalid argument", K(ret), K(offset), K(size));
    } else if (UNLIKELY(nullptr != io_buf_)) {
      ret = common::Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "io buf is not null", K(ret), KP(io_buf_));
    } else {
      int64_t aligned_offset = align_offset(offset);
      int64_t aligned_size = align_size(size + offset - aligned_offset);
      if (FAILED(alloc_io_buf(aligned_offset, aligned_size))) {
        XENGINE_LOG(WARN, "failed to alloc io buf", K(ret), K(offset), K(size), K(aligned_offset), K(aligned_size));
      } else {
        data_size_ = size;
        data_buf_ = io_buf_ + (offset - aligned_offset_);
      }
    }
    return ret;
  }

  DECLARE_AND_DEFINE_TO_STRING(KVP(io_buf_),
                               KV(aligned_offset_),
                               KV(aligned_size_),
                               KVP(data_buf_),
                               KV(data_size_));

private:
  // input offset and size should be aligned
  int alloc_io_buf(const int64_t offset, const int64_t size)
  {
    int ret = common::Status::kOk;
    if (UNLIKELY(!is_aligned(offset)) || UNLIKELY(!is_aligned(size))) {
      ret = common::Status::kInvalidArgument;
      XENGINE_LOG(WARN, "not aligned", K(ret), K(size), K(offset));
    } else if (ISNULL(io_buf_ = static_cast<char *>(memory::base_memalign(DIO_ALIGN_SIZE, size, memory::ModId::kAIOBuffer)))) {
      ret = common::Status::kNoSpace;
      XENGINE_LOG(WARN, "failed to alloc io buf", K(ret), K(aligned_size_));
    } else if (UNLIKELY(!is_aligned(uintptr_t(io_buf_)))) {
      ret = common::Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "io buf not aligned", K(ret), K(size), K(offset), KP(io_buf_));
    } else {
      aligned_offset_ = offset;
      aligned_size_ = size;
    }
    // clean up
    if (FAILED(ret)) {
      reset();
    }
    return ret;
  }

  bool is_aligned(const int64_t v)
  {
    return (v % DIO_ALIGN_SIZE) == 0;
  }

  int64_t align_offset(const int64_t offset)
  {
    return offset - (offset & (DIO_ALIGN_SIZE - 1));
  }

  int64_t align_size(const int64_t size)
  {
    return (size + DIO_ALIGN_SIZE - 1) & ~(DIO_ALIGN_SIZE - 1);
  }

private:
  // aligned buffer for io_submit
  char *io_buf_;
  // aligned offset for io_submit
  int64_t aligned_offset_;
  // aligned size for io_submit
  int64_t aligned_size_;
  // actual data buffer for read
  char *data_buf_;
  int64_t data_size_;
};

struct AIOInfo
{
  AIOInfo() : fd_(0), offset_(0), size_(0) {}

  AIOInfo(const int fd, const int64_t offset, const int64_t size)
      : fd_(fd), offset_(offset), size_(size)
  {}

  void reset()
  {
    fd_ = 0;
    offset_ = 0;
    size_ = 0;
  }

  bool is_valid() const
  {
    return fd_ > 0 && offset_ >= 0 && size_ > 0;
  }

  int fd_;
  int64_t offset_;
  int64_t size_;

  DECLARE_AND_DEFINE_TO_STRING(KV(fd_), KV(offset_), KV(size_));
};

struct AIOReq
{
  // input: I/O info
  AIOInfo aio_info_;
  // aio buffer and size
  AIOBuffer aio_buf_;
  // aio struct
  struct iocb iocb_;
  // output: I/O finish status
  int status_;
  // extra user attached info, checked on completion, like iocb.data
  void *aio_data_;

  AIOReq() : aio_info_(),
             aio_buf_(),
             status_(common::Status::kInvalidArgument),
             aio_data_(nullptr)
  {
    memset(&iocb_, 0, sizeof(iocb));
  }

  ~AIOReq() { reset(); }

  void reset();

  bool is_valid()
  {
    return aio_info_.is_valid() && aio_buf_.is_valid();
  }

  int prepare_write(const AIOInfo &aio_info, const char *data)
  {
    int ret = common::Status::kOk;
    if (UNLIKELY(!aio_info.is_valid()) || ISNULL(data)) {
      ret = common::Status::kInvalidArgument;
      XENGINE_LOG(WARN, "invalid argument", K(ret), K(aio_info), KP(data));
    } else {
      aio_info_ = aio_info;
      if (FAILED(aio_buf_.prepare_write_io_buf(aio_info_.offset_, aio_info_.size_, data))) {
        XENGINE_LOG(WARN, "failed to prepare read io buf", K(ret), K(aio_info_));
      } else {
        io_prep_pwrite(&iocb_, aio_info_.fd_, aio_buf_.aio_buf(), aio_buf_.aio_size(), aio_buf_.aio_offset());
      }
      iocb_.data = this;
      status_ = common::Status::kBusy;
    }
    return ret;
  }

  int prepare_read(const AIOInfo &aio_info)
  {
    int ret = common::Status::kOk;
    if (UNLIKELY(!aio_info.is_valid())) {
      ret = common::Status::kInvalidArgument;
      XENGINE_LOG(WARN, "invalid argument", K(ret), K(aio_info));
    } else {
      aio_info_ = aio_info;
      if (FAILED(aio_buf_.prepare_read_io_buf(aio_info_.offset_, aio_info_.size_))) {
        XENGINE_LOG(WARN, "failed to prepare read io buf", K(ret), K(aio_info_));
      } else {
        io_prep_pread(&iocb_, aio_info_.fd_, aio_buf_.aio_buf(), aio_buf_.aio_size(), aio_buf_.aio_offset());
      }
      iocb_.data = this;
      status_ = common::Status::kBusy;
    }
    return ret;
  }

  DECLARE_AND_DEFINE_TO_STRING(KV(aio_info_),
                               KV(aio_buf_),
                               KV(status_));
};

class AIO
{
private:
  AIO() : ctx_(0),
          max_events_(0),
          events_(nullptr),
          inited_(false)
  {}

  static void cleanup(void *ptr) {
    auto inst = static_cast<AIO*>(ptr);
    if (nullptr != inst) {
      delete inst;
    }
  }
public:
  ~AIO()
  {
    reset();
  }

  inline static AIO *get_instance()
  {
    static __thread AIO* instance = nullptr;
    if (nullptr == instance) {
      instance = new AIO();
      if (UNLIKELY(common::Status::kOk != instance->init(MAX_EVENT_CNT))) {
        XENGINE_LOG(ERROR, "init thread local aio instance failed!");
        delete instance;
        instance = nullptr;
      } else {
        // ToDo error handling
        (void)ThreadLocalHelper::instance().register_deleter(cleanup, instance);
      }
    }
    return instance;
  }

  int init(unsigned nr_events) {
    int ret = common::Status::kOk;
    if (UNLIKELY(inited_)) {
      ret = common::Status::kInitTwice;
      XENGINE_LOG(WARN, "init twice", K(ret));
    } else {
      max_events_ = nr_events;
      if (0 != io_setup(max_events_, &ctx_)) {
        char line[100];
        memset(line, 0, sizeof(line));
        char *p = NULL;
        FILE* fp = fopen("/proc/sys/fs/aio-nr", "r");
        if (fp != NULL) {
          p = fgets(line, 100, fp);
          fclose(fp);
        }
        ret = common::Status::kIOError;
        __XENGINE_LOG(ERROR, "aio init failed, ret=%d, max_events=%lu, aio-nr=%s", ret, max_events_, p != NULL ? p : "");
      } else if (ISNULL(events_ = new io_event[max_events_])) {
        ret = common::Status::kMemoryLimit;
        XENGINE_LOG(WARN, "alloc events failed", K(ret), K(max_events_));
      } else {
        inited_ = true;
      }
    }
    return ret;
  }

  void reset()
  {
    if (ctx_ != 0) {
      io_destroy(ctx_);
      ctx_ = 0;
    }
    max_events_ = 0;
    if (nullptr != events_) {
      delete[] events_;
      events_ = nullptr;
    }
    inited_ = false;
  }

  // one at a time right now for simplicity
  int submit(struct AIOReq *reqs, const int64_t count) {
    int ret = common::Status::kOk;
    if (UNLIKELY(!inited_)) {
      ret = common::Status::kNotInit;
      XENGINE_LOG(WARN, "aio not inited", K(ret));
    } else if (ISNULL(reqs) || UNLIKELY(count > max_events_)) {
      ret = common::Status::kInvalidArgument;
      XENGINE_LOG(WARN, "invalid argument", K(ret), KP(reqs), K(count));
      // these reqs won't be submitted,
      // thus set error to these reqs to avoid reaping them later
      for (int64_t i = 0; i < count; i++) {
        reqs[i].status_ = common::Status::kIOError;
      }
    } else {
      int64_t remain = count;
      int io_ret = 0;
      struct iocb *iocbp = nullptr;
      for (int64_t i = 0; SUCCED(ret) && i < count; i++) {
        iocbp = &(reqs[i].iocb_);
        if (UNLIKELY(!reqs[i].is_valid())) {
          reqs[i].status_ = common::Status::kIOError;
          ret = common::Status::kInvalidArgument;
          XENGINE_LOG(WARN, "invalid request", K(ret), K(reqs[i]));
        } else if (1 != (io_ret = io_submit(ctx_, 1, &iocbp))) {
          reqs[i].status_ = common::Status::kIOError;
          ret = common::Status::kIOError;
          XENGINE_LOG(WARN, "failed to submit AIO request", K(ret), K(io_ret));
        } else {
          QUERY_COUNT(monitor::CountPoint::AIO_REQUEST_SUBMIT);
        }
      }
    }
    return ret;
  }

  // reap the specific req, it doesn't return until req is done
  int reap(struct AIOReq *req) {
    int ret = common::Status::kOk;
    if (UNLIKELY(!inited_)) {
      ret = common::Status::kNotInit;
      XENGINE_LOG(WARN, "aio not inited", K(ret));
    } else if (ISNULL(req)) {
      ret = common::Status::kInvalidArgument;
      XENGINE_LOG(WARN, "invalid argument", K(ret), KP(req));
    } else if (common::Status::kOk == req->status_) {
      // already reaped
    } else if (UNLIKELY(common::Status::kBusy != req->status_)) {
      ret = common::Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "something wrong for this request", K(*req));
    } else {
      // get more events
      bool found = false;
      while (SUCCED(ret) && !found) {
        int io_ret = io_getevents(ctx_, 1, max_events_, events_, nullptr);
        if (io_ret > 0) {
          for (int i = 0; i < io_ret; i++) {
            AIOReq *done_req = reinterpret_cast<AIOReq *>(events_[i].data);
            int64_t res = events_[i].res;   // bytes written
            int64_t res2 = events_[i].res2;  // r/w status
            if ((res2 != 0) || (done_req->aio_buf_.aio_size() != res)) {
              done_req->status_ = common::Status::kIOError;
              XENGINE_LOG(ERROR, "reap io request failed", KP(done_req), K(res), K(res2), K(*done_req));
            } else {
              done_req->status_ = common::Status::kOk;
            }
            if (done_req == req) {
              found = true;
            }
          }
        } else if (io_ret == -EINTR || io_ret == 0) {
          continue;
        } else if (io_ret < 0) {
          ret = common::Status::kIOError;
          XENGINE_LOG(WARN, "aio failed", K(ret), K(io_ret));
        }
      }
    }
    return ret;
  }

  // reap the specific req, it doesn't return until req is done
  // if read size if less than request size, this method will return the actual size and won't fail
  int reap_by_actual_size(struct AIOReq *req) {
    int ret = common::Status::kOk;
    if (UNLIKELY(!inited_)) {
      ret = common::Status::kNotInit;
      XENGINE_LOG(WARN, "aio not inited", K(ret));
    } else if (ISNULL(req)) {
      ret = common::Status::kInvalidArgument;
      XENGINE_LOG(WARN, "invalid argument", K(ret), KP(req));
    } else if (common::Status::kOk == req->status_) {
      // already reaped
    } else if (UNLIKELY(common::Status::kBusy != req->status_)) {
      ret = common::Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "something wrong for this request", K(*req));
    } else {
      // get more events
      bool found = false;
      while (SUCCED(ret) && !found) {
        int io_ret = io_getevents(ctx_, 1, max_events_, events_, nullptr);
        if (io_ret > 0) {
          for (int i = 0; i < io_ret; i++) {
            AIOReq *done_req = reinterpret_cast<AIOReq *>(events_[i].data);
            int64_t res = events_[i].res;   // bytes written
            int64_t res2 = events_[i].res2;  // r/w status
            if (res2 != 0) {
              done_req->status_ = common::Status::kIOError;
              XENGINE_LOG(WARN, "reap io request failed", KP(done_req), K(res), K(res2), K(*done_req));
            } else {
              done_req->status_ = common::Status::kOk;
              if (done_req->aio_buf_.aio_size() != res) {
                done_req->aio_buf_.set_size(res);
              }
            }
            if (done_req == req) {
              found = true;
            }
          }
        } else if (io_ret == -EINTR || io_ret == 0) {
          continue;
        } else if (io_ret < 0) {
          ret = common::Status::kIOError;
          XENGINE_LOG(WARN, "aio failed", K(ret), K(io_ret), K(req->aio_info_));
        }
      }
    }
    return ret;
  }

  // reap [one, num] unspecific requests, NOT recommended
  int reap_some(int &num, struct AIOReq *reqs[]) {
    int ret = common::Status::kOk;
    if (UNLIKELY(!inited_)) {
      ret = common::Status::kNotInit;
      XENGINE_LOG(WARN, "aio not inited", K(ret));
    } else if (UNLIKELY(num <= 0)) {
      ret = common::Status::kInvalidArgument;
      XENGINE_LOG(WARN, "invalid argument", K(ret), K(num));
    } else if (!done_reqs_.empty()) {
        if (static_cast<size_t>(num) > done_reqs_.size()) {
          num = done_reqs_.size();
        }
        std::unordered_set<struct AIOReq *>::iterator it = done_reqs_.begin();
        for (int i = 0; i < num; i++, it++) {
          reqs[i] = *it;
        }
        done_reqs_.erase(done_reqs_.begin(), it);
    } else {
      while (SUCCED(ret)) {
        int io_ret = io_getevents(ctx_, 1, max_events_, events_, nullptr);
        if (io_ret > 0) {
          for (int i = 0; i < io_ret; i++) {
            AIOReq *done_req = reinterpret_cast<AIOReq *>(events_[i].data);
            int64_t res = events_[i].res;   // bytes written
            int64_t res2 = events_[i].res2;  // r/w status
            if ((res2 != 0) || (done_req->aio_buf_.aio_size() != res)) {
              done_req->status_ = common::Status::kIOError;
            } else {
              done_req->status_ = common::Status::kOk;
            }
            if (i < num) {
              reqs[i] = done_req;
            } else {
              done_reqs_.insert(done_req);
            }
          }
          assert(done_reqs_.size() <= max_events_);
          if (io_ret < num) {
            num = io_ret;
          }
          break;
        } else if (io_ret == -EINTR || io_ret == 0) {
          continue;
        } else if (io_ret < 0) {
          ret = common::Status::kIOError;
        }
      }
    }
    return ret;
  }

private:
  static const int64_t MAX_EVENT_CNT = 64;
  io_context_t ctx_;
  unsigned max_events_;
  struct io_event *events_;
  std::unordered_set<struct AIOReq *> done_reqs_;
  bool inited_;
};

struct AIOHandle
{
  AIOHandle() {}
  ~AIOHandle() { reset(); }

  void reset() {
    aio_req_.reset();
  }

  int prefetch(const AIOInfo &aio_info) {
    int ret = common::Status::kOk;
    AIO *aio_instance = AIO::get_instance();
    if (ISNULL(aio_req_.get()) || ISNULL(aio_instance)) {
      ret = common::Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "aio req or aio instance is nullptr", K(ret), KP(aio_req_.get()), KP(aio_instance));
    } else if (FAILED(aio_req_->prepare_read(aio_info))) {
      XENGINE_LOG(WARN, "failed to prepare iocb", K(ret));
    } else if (FAILED(aio_instance->submit(aio_req_.get(), 1))) {
      XENGINE_LOG(WARN, "failed to submit aio request", K(ret));
    }
    return ret;
  }

  int read(int64_t offset, int64_t size, common::Slice *result, char *scratch) {
    int ret = common::Status::kOk;
    AIO *aio_instance = AIO::get_instance();
    if (ISNULL(aio_req_.get()) || ISNULL(aio_instance)) {
      ret = common::Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "aio req or aio instance is nullptr", K(ret), KP(aio_req_.get()), KP(aio_instance));
    } else if (common::Status::kOk == aio_req_->status_) {
      // already reaped
    } else if (FAILED(aio_instance->reap(aio_req_.get()))) {
      XENGINE_LOG(WARN, "aio instance reap failed", K(ret));
    } else if (FAILED(aio_req_->status_)) {
      ret = aio_req_->status_;
      XENGINE_LOG(WARN, "aio req reap failed", K(ret));
    }
    if (SUCCED(ret)) {
      const int64_t aio_offset = aio_req_->aio_buf_.aio_offset();
      const int64_t aio_size = aio_req_->aio_buf_.aio_size();
      if (UNLIKELY(offset < aio_offset)
          || UNLIKELY((offset + size) > (aio_offset + aio_size))) {
        // overflow
        ret = common::Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "read overflow", K(ret), K(offset), K(size), K(aio_offset), K(aio_size));
      } else {
        const int64_t delta = offset - aio_offset;
        memcpy(scratch, aio_req_->aio_buf_.aio_buf() + delta, size);
        *result = common::Slice(scratch, size);
      }
    }
    return ret;
  }

  /*
   * Read the file as much as possible
   * Caller should deal with partial read outside
   * */
  int read_by_actual_size(int64_t offset, int64_t size, common::Slice *result, char *scratch) {
    int ret = common::Status::kOk;
    AIO *aio_instance = AIO::get_instance();
    if (ISNULL(aio_req_.get()) || ISNULL(aio_instance)) {
      ret = common::Status::kErrorUnexpected;
      XENGINE_LOG(WARN, "aio req or aio instance is nullptr", K(ret), KP(aio_req_.get()), KP(aio_instance));
    } else if (common::Status::kOk == aio_req_->status_) {
      // already reaped
    } else if (FAILED(aio_instance->reap_by_actual_size(aio_req_.get()))) {
      XENGINE_LOG(WARN, "aio instance reap failed", K(ret));
    } else if (FAILED(aio_req_->status_)) {
      ret = aio_req_->status_;
      XENGINE_LOG(WARN, "aio req reap failed", K(ret));
    }
    if (SUCCED(ret)) {
      const int64_t aio_offset = aio_req_->aio_buf_.aio_offset();
      const int64_t aio_size = aio_req_->aio_buf_.aio_size();
      if (UNLIKELY(offset < aio_offset)
          || UNLIKELY(aio_size < offset - aio_offset)) {
        // overflow
        ret = common::Status::kErrorUnexpected;
        XENGINE_LOG(WARN, "read overflow", K(ret), K(offset), K(size), K(aio_offset), K(aio_size));
      } else {
        const int64_t delta = offset - aio_offset;
        const int64_t actual_read_size = aio_size - delta;
        if (actual_read_size != size) { // TODO deal with out of range
          XENGINE_LOG(DEBUG, "partial read", K(size), K(actual_read_size), K(aio_req_->aio_info_));
        }
        memcpy(scratch, aio_req_->aio_buf_.aio_buf() + delta, actual_read_size);
        *result = common::Slice(scratch, actual_read_size);
      }
    }
    return ret;
  }

  std::shared_ptr<AIOReq> aio_req_;
};

}  // namespace util
}  // namespace xengine
