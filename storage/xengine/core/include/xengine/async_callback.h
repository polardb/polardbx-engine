/*
 * Portions Copyright (c) 2020, Alibaba Group Holding Limited
 */
// Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#pragma once

#include "xengine/status.h"

namespace xengine {
namespace common {

class AsyncCallback {
 public:
  AsyncCallback(bool delete_on_completed = false)
      : next_call_back_(nullptr), delete_on_completed_(delete_on_completed) {}

  AsyncCallback(AsyncCallback* next_call_back, bool delete_on_completed = false)
      : next_call_back_(next_call_back),
        delete_on_completed_(delete_on_completed) {}

  virtual ~AsyncCallback() {
    if (next_call_back_ != nullptr && next_call_back_->destroy_on_complete()) {
      delete next_call_back_;
      next_call_back_ = nullptr;
    }
  }

  virtual Status call_back() = 0;

  virtual Status call_back_on_fail() { return Status::NotSupported(); }

  virtual bool destroy_on_complete() { return this->delete_on_completed_; }

  Status run_call_back(bool commit_succeed = true) {
    Status cb_status = Status::OK();
    Status next_cb_status = Status::OK();
    if (commit_succeed)
      cb_status = this->call_back();
    else
      cb_status = this->call_back_on_fail();

    if (nullptr == this->next_call_back_) return cb_status;
    // run next callback
    AsyncCallback* next_cb = this->next_call_back_;
    bool destory_on_complete = next_cb->destroy_on_complete();
    if (cb_status.ok())
      next_cb_status = next_cb->run_call_back(true);
    else
      next_cb_status = next_cb->run_call_back(false);

    if (destory_on_complete) {
      delete next_cb;
      this->next_call_back_ = nullptr;
    }

    if (!cb_status.ok()) {
      return cb_status;
    }
    return next_cb_status;
  }

 public:
  AsyncCallback* next_call_back_;
  bool delete_on_completed_;
};

}  // namespace common
}  // namespace xengine
