/*
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
 */

#pragma once
#include "util/to_string.h"
#include "util/serialization.h"
#include "xengine/types.h"

namespace xengine
{
namespace db
{
struct RecoveryPoint
{
  static const int64_t RECOVERY_POINT_VERSION = 1;
  int64_t log_file_number_;
  uint64_t seq_;

  RecoveryPoint() : log_file_number_(0), seq_(0)
  {
  }
  RecoveryPoint(int64_t log_file_number, uint64_t seq) : log_file_number_(log_file_number), seq_(seq)
  {
  }
  ~RecoveryPoint()
  {
  }

  void reset()
  {
    log_file_number_ = 0;
    seq_ = 0;
  }
  bool is_valid() const
  {
    return log_file_number_ >= 0 && seq_ > 0;
  }

  bool operator > (const RecoveryPoint &recovery_point) const
  {
    if (log_file_number_ == recovery_point.log_file_number_) {
      return seq_ > recovery_point.seq_;
    }
    return log_file_number_ > recovery_point.log_file_number_;
  }
  DECLARE_TO_STRING();
  DECLARE_COMPACTIPLE_SERIALIZATION(RECOVERY_POINT_VERSION);

};
} //namespace db
} //namespace xengine
