/************************************************************************
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.

 * $Id:  try.h,v 1.0 01/09/2018 11:32:26 AM
 *
 ************************************************************************/

/**
 * @file try.h
 * @date 01/09/2018 11:32:26 AM
 * @version 1.0
 * @brief
 *
 **/

#pragma once
#include <memory>
#include <util/error.h>

namespace xengine {
namespace util {
template <typename _T>
class Try {
  static_assert(!std::is_reference<_T>::value,
                "Try can not be used with reference types");
  using ErrorType = Error;
  enum class TryType {
    Value = 0,
    Error,
    Nothing,
  };

 public:

  explicit Try() : type_(TryType::Nothing) {}
  explicit Try(_T&& v) : type_(TryType::Value), value_(std::move(v)) {}
  explicit Try(const ErrorType& e) : type_(TryType::Error), error_(e) {}

  Try(Try<_T>&& t) noexcept;
  Try& operator=(Try<_T>&& t) noexcept;
  Try(const Try& t);
  Try& operator=(const Try& t);
  ~Try();

  _T& value() &;
  _T&& value() &&;
  const _T& value() const &;
  const _T&& value() const &&;

  ErrorType error() const {
    return error_;
  }

  bool has_value() const { return type_ == TryType::Value; }
  bool has_error() const { return type_ == TryType::Error; }

 protected:
  void throw_if_no_value_();
 protected:
  TryType type_{TryType::Nothing};
  union {
    _T value_;
    ErrorType error_;
  };
};
}  // namespace is
}

#include "util/try.ipp"
