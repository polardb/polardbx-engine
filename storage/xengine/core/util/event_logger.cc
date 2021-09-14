//  Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif

#include "util/event_logger.h"

#include <inttypes.h>
#include <cassert>
#include <sstream>
#include <string>

#include "logger/logger.h"
#include "util/string_util.h"

using namespace xengine::common;

namespace xengine {
namespace util {

EventLoggerStream::EventLoggerStream(LogBuffer* log_buffer/* = nullptr*/)
    : log_buffer_(log_buffer), json_writer_(nullptr) {}

EventLoggerStream::~EventLoggerStream() {
  if (json_writer_) {
    json_writer_->EndObject();
#ifdef ROCKSDB_PRINT_EVENTS_TO_STDOUT
    printf("%s\n", json_writer_->Get().c_str());
#else
    if (log_buffer_) {
      EventLogger::LogToBuffer(log_buffer_, *json_writer_);
    } else {
      EventLogger::Log(*json_writer_);
    }
#endif
    delete json_writer_;
  }
}

void EventLogger::Log(const JSONWriter& jwriter) {
#ifdef ROCKSDB_PRINT_EVENTS_TO_STDOUT
  printf("%s\n", jwriter.Get().c_str());
#else
  __XENGINE_LOG(INFO, "%s %s", Prefix(), jwriter.Get().c_str());
#endif
}

void EventLogger::LogToBuffer(LogBuffer* log_buffer,
                              const JSONWriter& jwriter) {
#ifdef ROCKSDB_PRINT_EVENTS_TO_STDOUT
  printf("%s\n", jwriter.Get().c_str());
#else
  assert(log_buffer);
  util::LogToBuffer(log_buffer, "%s %s", Prefix(), jwriter.Get().c_str());
#endif
}

}  // namespace util
}  // namespace xengine
