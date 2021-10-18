/************************************************************************
 *
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

 * $Id:  callback_util.h,v 1.0 12/26/2017 05:25:40 PM
 *
 ************************************************************************/

/**
 * @file callback_util.h
 * @date 12/26/2017 05:25:40 PM
 * @version 1.0
 * @brief
 *
 **/

#pragma once

#include <memory>

namespace xengine {
namespace util {

struct CallbackBase;
typedef std::unique_ptr<CallbackBase> CallbackType;

struct CallbackBase {
  virtual ~CallbackBase() = default;
  virtual void run() = 0;
  void operator ()() {
    this->run();
  }
};

template <typename _Func>
struct Callback : public CallbackBase {
  _Func cb;
  Callback(_Func &&f) : cb(std::forward<_Func>(f)) {}
  virtual void run() { cb(); }
};

template <typename _Func>
std::unique_ptr<Callback<_Func>> make_callback(_Func &&f) {
  return std::unique_ptr<Callback<_Func>>(
      new Callback<_Func>(std::forward<_Func>(f)));
}

template <typename _Arg>
struct CallbackArgBase;
template <typename _Arg>
using CallbackArgType = std::unique_ptr<CallbackArgBase<_Arg>>;
template <typename _Arg>
using CallbackArgTypeShared = std::shared_ptr<CallbackArgBase<_Arg>>;

template <typename _Arg>
struct CallbackArgBase {
  virtual ~CallbackArgBase() = default;
  virtual void run(_Arg&&) = 0;
  void operator ()(_Arg&& arg) {
    this->run(std::forward<_Arg>(arg));
  }
};

template <typename _Arg, typename _Func>
struct CallbackArg : public CallbackArgBase<_Arg> {
  _Func cb;
  CallbackArg(_Func &&f) : cb(std::forward<_Func>(f)) {}
  virtual void run(_Arg&& arg) { cb(std::move(arg)); }
};

template <typename _Arg, typename _Func>
std::unique_ptr<CallbackArg<_Arg, _Func>> make_callback_arg(_Func &&f) {
  return std::unique_ptr<CallbackArg<_Arg, _Func>>(
      new CallbackArg<_Arg, _Func>(std::forward<_Func>(f)));
}
template <typename _Arg, typename _Func>
std::shared_ptr<CallbackArg<_Arg, _Func>> make_callback_arg_shared(_Func &&f) {
  return std::shared_ptr<CallbackArg<_Arg, _Func>>(
      new CallbackArg<_Arg, _Func>(std::forward<_Func>(f)));
}
}  // namespace util
}
