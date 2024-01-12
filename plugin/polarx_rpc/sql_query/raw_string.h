//
// Created by zzy on 2021/10/11.
//

#pragma once

#include <string>
#include <utility>

namespace polarx_rpc {

class RawString {
 private:
  std::string value_;

 public:
  explicit RawString(std::string val) : value_(std::move(val)) {}

  const std::string &get_value() const { return value_; }
};

}  // namespace polarx_rpc
