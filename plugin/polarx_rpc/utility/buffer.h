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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>

#include "../common_define.h"

namespace polarx_rpc {

class Cbuffer final {
  NO_COPY(Cbuffer);

 private:
  std::unique_ptr<uint8_t[]> buf_;
  size_t pos_;
  size_t cap_;

 public:
  Cbuffer() : pos_(0), cap_(0) {}
  explicit Cbuffer(size_t sz) : buf_(new uint8_t[sz]), pos_(0), cap_(sz) {}
  Cbuffer(Cbuffer &&another) noexcept
      : buf_(std::move(another.buf_)), pos_(another.pos_), cap_(another.cap_) {
    another.buf_.reset();
    another.cap_ = 0;
    another.pos_ = 0;
  }

  Cbuffer &operator=(Cbuffer &&another) noexcept {
    buf_ = std::move(another.buf_);
    pos_ = another.pos_;
    cap_ = another.cap_;
    another.buf_.reset();
    another.pos_ = 0;
    another.cap_ = 0;
    return *this;
  }

  inline uint8_t *ptr() { return buf_.get(); }

  inline const uint8_t *ptr() const { return buf_.get(); }

  inline size_t &pos() { return pos_; }

  inline const size_t &pos() const { return pos_; }

  inline const size_t &cap() const { return cap_; }
};

}  // namespace polarx_rpc
