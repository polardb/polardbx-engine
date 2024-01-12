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
