// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "xengine/status.h"
#include <stdio.h>
#include <cstring>
#include "port/port.h"

namespace xengine {
namespace common {

const char* Status::CopyState(const char* state) {
  char* const result =
      new char[std::strlen(state) + 1];  // +1 for the null terminator
  std::strcpy(result, state);
  return result;
}

Status::Status(Code _code, SubCode _subcode, const Slice& msg,
               const Slice& msg2)
    : code_(_code), subcode_(_subcode) {
  assert(code_ != kOk);
  assert(subcode_ != kMaxSubCode);
  const size_t len1 = msg.size();
  const size_t len2 = msg2.size();
  const size_t size = len1 + (len2 ? (2 + len2) : 0);
  char* const result = new char[size + 1];  // +1 for null terminator
  memcpy(result, msg.data(), len1);
  if (len2) {
    result[len1] = ':';
    result[len1 + 1] = ' ';
    memcpy(result + len1 + 2, msg2.data(), len2);
  }
  result[size] = '\0';  // null terminator for C style string
  state_ = result;
}

std::string Status::ToString() const {
  char tmp[30];
  const char* type;
  switch (code_) {
    case kOk:
      return "OK";
    case kNotFound:
      type = "NotFound: ";
      break;
    case kCorruption:
      type = "Corruption: ";
      break;
    case kNotSupported:
      type = "Not implemented: ";
      break;
    case kInvalidArgument:
      type = "Invalid argument: ";
      break;
    case kIOError:
      type = "IO error: ";
      break;
    case kMergeInProgress:
      type = "Merge in progress: ";
      break;
    case kIncomplete:
      type = "Result incomplete: ";
      break;
    case kShutdownInProgress:
      type = "Shutdown in progress: ";
      break;
    case kTimedOut:
      type = "Operation timed out: ";
      break;
    case kAborted:
      type = "Operation aborted: ";
      break;
    case kBusy:
      type = "Resource busy: ";
      break;
    case kExpired:
      type = "Operation expired: ";
      break;
    case kTryAgain:
      type = "Operation failed. Try again.: ";
      break;
    case kMutexTimeout:
      type = "Mutex time out";
      break;
    case kLockTimeout:
      type = "Lock time out";
      break;
    case kLockLimit:
      type = "Lock limit";
      break;
    case kNoSpace:
      type = "No space";
      break;
    case kDeadlock:
      type = "Dead lock";
      break;
    case kStaleFile:
      type = "Stale file";
      break;
    case kMemoryLimit:
      type = "Memory limit";
      break;
    default:
      snprintf(tmp, sizeof(tmp), "Unknown code(%d): ",
               static_cast<int>(code()));
      type = tmp;
      break;
  }
  std::string result(type);
  if (state_ != nullptr) {
    result.append(state_);
  }
  return result;
}

}  // namespace common
}  // namespace xengine
