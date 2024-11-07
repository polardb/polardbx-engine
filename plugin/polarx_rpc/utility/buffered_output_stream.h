/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/


//
// Created by zzy on 2022/7/27.
//

#pragma once

#include <utility>
#include <vector>

#include "../common_define.h"
#include "../server/server_variables.h"

#include "../coders/protocol_fwd.h"
#include "buffer.h"

namespace polarx_rpc {

static constexpr size_t DEFAULT_BUFFER_SIZE = 0x1000;  /// 4KB

class CbufferedOutputStream final
    : public google::protobuf::io::ZeroCopyOutputStream {
  NO_COPY(CbufferedOutputStream);

 private:
  std::vector<Cbuffer> buffers_;
  int buffer_index_;
  size_t bytes_total_;

  inline void init() {
    if (UNLIKELY(buffer_index_ < 0)) {
      assert(-1 == buffer_index_);
      buffer_index_ = 0;
      if (UNLIKELY(buffers_.empty()))
        buffers_.emplace_back(DEFAULT_BUFFER_SIZE);
    }
  }

 public:
  CbufferedOutputStream() : buffer_index_(-1), bytes_total_(0) {}
  CbufferedOutputStream(CbufferedOutputStream &&another) noexcept
      : buffers_(std::move(another.buffers_)),
        buffer_index_(another.buffer_index_),
        bytes_total_(another.bytes_total_) {
    another.buffers_.clear();
    another.buffer_index_ = -1;
    another.bytes_total_ = 0;
  }

  CbufferedOutputStream &operator=(CbufferedOutputStream &&another) noexcept {
    buffers_ = std::move(another.buffers_);
    buffer_index_ = another.buffer_index_;
    bytes_total_ = another.bytes_total_;
    another.buffers_.clear();
    another.buffer_index_ = -1;
    another.bytes_total_ = 0;
  }

  inline void clear() {
    auto cache_pages = max_cached_output_buffer_pages;
    if (UNLIKELY(buffers_.size() > cache_pages)) buffers_.resize(cache_pages);
    for (auto &buf : buffers_) buf.pos() = 0;
    buffer_index_ = -1;
    bytes_total_ = 0;
  }

  inline void *reserve(size_t sz) {
    if (sz > DEFAULT_BUFFER_SIZE) return nullptr;

    init();
    auto buf = &buffers_[buffer_index_];
    if (UNLIKELY(buf->cap() - buf->pos() < sz)) {
      assert(buf->pos() <= buf->cap());
      /// move to next buf
      bytes_total_ += buf->pos();
      ++buffer_index_;
      if (UNLIKELY(buffer_index_ >= static_cast<int>(buffers_.size())))
        buffers_.emplace_back(DEFAULT_BUFFER_SIZE);
      buf = &buffers_[buffer_index_];
    }

    auto ptr = buf->ptr() + buf->pos();
    buf->pos() += sz;
    assert(buf->pos() <= buf->cap());
    return ptr;
  }

  bool Next(void **data, int *size) final {
    init();
    auto buf = &buffers_[buffer_index_];
    if (UNLIKELY(buf->pos() >= buf->cap())) {
      assert(buf->pos() == buf->cap());
      /// move to next buf
      bytes_total_ += buf->pos();
      ++buffer_index_;
      if (UNLIKELY(buffer_index_ >= static_cast<int>(buffers_.size())))
        buffers_.emplace_back(DEFAULT_BUFFER_SIZE);
      buf = &buffers_[buffer_index_];
    }

    *data = buf->ptr() + buf->pos();
    *size = static_cast<int>(buf->cap() - buf->pos());
    buf->pos() = buf->cap();
    return true;
  }

  void BackUp(int count) final {
    assert(buffer_index_ < static_cast<int>(buffers_.size()));
    auto &buf = buffers_[buffer_index_];
    assert(count < static_cast<int>(buf.pos()));
    buf.pos() -= count;
  }

  int64_t ByteCount() const final {
    if (UNLIKELY(-1 == buffer_index_))
      return static_cast<int64_t>(bytes_total_);
    return static_cast<int64_t>(bytes_total_ + buffers_[buffer_index_].pos());
  }

  /**
   * Backup data for bad data restore.
   */

  struct backup_t final {
    size_t buf_pos;
    int index;
    size_t bytes_total;
  };

  inline void backup(backup_t &b) const {
    if (UNLIKELY(buffer_index_ < 0)) {
      assert(-1 == buffer_index_);
      assert(0 == bytes_total_);
      b.buf_pos = 0;
      b.index = -1;
      b.bytes_total = 0;
    } else {
      assert(buffer_index_ < static_cast<int>(buffers_.size()));
      b.buf_pos = buffers_[buffer_index_].pos();
      b.index = buffer_index_;
      b.bytes_total = bytes_total_;
    }
  }

  inline void restore(const backup_t &b) {
    if (UNLIKELY(-1 == b.index))
      clear();
    else {
      assert(buffer_index_ >= b.index);
      for (auto i = buffer_index_; i > b.index; --i) buffers_[i].pos() = 0;
      buffer_index_ = b.index;
      buffers_[buffer_index_].pos() = b.buf_pos;
      bytes_total_ = b.bytes_total;
    }
  }
};

}  // namespace polarx_rpc
