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

 * $Id:  try.ipp,v 1.0 01/09/2018 11:54:17 AM
 *
 ************************************************************************/

/**
 * @file try.ipp
 * @date 01/09/2018 11:54:17 AM
 * @version 1.0
 * @brief
 *
 **/

#include <memory>
#include "try.h"

namespace xengine {
namespace util {    
template <typename _T>
Try<_T>::Try(Try<_T>&& t) noexcept : type_(t.type_) {
  if (type_ == TryType::Value) {
    new (&value_) _T(std::move(t.value_));
  } else if (type_ == TryType::Error) {
    error_ = t.error_;
  }
}
template <typename _T>
Try<_T>& Try<_T>::operator=(Try<_T>&& t) noexcept {
  if (this == &t) {
    return *this;
  }
  this->~Try();
  type_ = t.type_;
  if (type_ == TryType::Value) {
    new (&value_) _T(std::move(t.value_));
  } else if (type_ == TryType::Error) {
    error_ = t.error_;
  }
  return *this;
}
template <typename _T>
Try<_T>::Try(const Try& t) {
  static_assert(std::is_copy_constructible<_T>::value,
                "T must be copyable for Try<T> to be copyable");
  type_ = t.type_;
  if (type_ == TryType::Value) {
    new (&value_) _T(t.value_);
  } else if (type_ == TryType::Error) {
    error_ = t.error_;
  }
}
template <typename _T>
Try<_T>& Try<_T>::operator=(const Try& t) {
  static_assert(std::is_copy_constructible<_T>::value,
                "T must be copyable for Try<T> to be copyable");
  this->~Try();
  type_ = t.type_;
  if (type_ == TryType::Value) {
    new (&value_) _T(t.value_);
  } else if (type_ == TryType::Error) {
    error_ = t.error_;
  }
}
template <typename _T>
Try<_T>::~Try() {
  if (type_ == TryType::Value) {
    value_.~_T();
  }
}

template <typename _T>
_T& Try<_T>::value() & {
  throw_if_no_value_();
  return value_;
}
template <typename _T>
_T&& Try<_T>::value() && {
  throw_if_no_value_();
  return std::move(value_);
}
template <typename _T>
const _T& Try<_T>::value() const & {
  throw_if_no_value_();
  return value_;
}
template <typename _T>
const _T&& Try<_T>::value() const && {
  throw_if_no_value_();
  return std::move(value_);
}
/*
template <typename _T>
FutureErrorType Try<_T>::error() const {
  return error_;
}
*/

template <typename _T>
void Try<_T>::throw_if_no_value_() {
  switch (type_) {
    case TryType::Value:
      return;

    case TryType::Error:
      std::__throw_logic_error("get error then get try value");

    default:
      std::__throw_logic_error("uninitialize value then get try value");
  }  // endswitch
}
}  // namespace is
}
