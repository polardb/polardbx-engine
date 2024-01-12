//
// Created by zzy on 2021/11/16.
//

#pragma once

#include <string>
#include <utility>

namespace polarx_rpc {

class RawBinary {
 private:
  std::string value_;

 public:
  explicit RawBinary(std::string val) : value_(std::move(val)) {}

  const std::string &get_value() const { return value_; }

  std::string to_hex_string() const {
    std::string buf;
    buf.resize(3 + value_.length() * 2);  // x'{hex string}'
    buf[0] = 'x';
    buf[1] = '\'';
    auto idx = 2;
    for (const auto &b : value_) {
      auto h = (((uint8_t)b) >> 4) & 0xF;
      buf[idx++] = h >= 10 ? (char)(h - 10 + 'a') : (char)(h + '0');
      auto l = ((uint8_t)b) & 0xF;
      buf[idx++] = l >= 10 ? (char)(l - 10 + 'a') : (char)(l + '0');
    }
    buf[idx] = '\'';
    return buf;
  }
};

}  // namespace polarx_rpc
