// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#pragma once

#include <stdint.h>

#include <memory>

#include "db/log_format.h"
#include "util/concurrent_direct_file_writer.h"
#include "util/crc32c.h"
#include "xengine/slice.h"
#include "xengine/status.h"
#include "logger/logger.h"

namespace xengine {

namespace util {
class WritableFileWriter;
}

namespace db {

using std::unique_ptr;

namespace log {

/**
 * Writer is a general purpose log stream writer. It provides an append-only
 * abstraction for writing data. The details of the how the data is written is
 * handled by the WriteableFile sub-class implementation.
 *
 * File format:
 *
 * File is broken down into variable sized records. The format of each record
 * is described below.
 *       +-----+-------------+--+----+----------+------+-- ... ----+
 * File  | r0  |        r1   |P | r2 |    r3    |  r4  |           |
 *       +-----+-------------+--+----+----------+------+-- ... ----+
 *       <--- kBlockSize ------>|<-- kBlockSize ------>|
 *  rn = variable size records
 *  P = Padding
 *
 * Data is written out in kBlockSize chunks. If next record does not fit
 * into the space left, the leftover space will be padded with \0.
 *
 * Legacy record format:
 *
 * +---------+-----------+-----------+--- ... ---+
 * |CRC (4B) | Size (2B) | Type (1B) | Payload   |
 * +---------+-----------+-----------+--- ... ---+
 *
 * CRC = 32bit hash computed over the payload using CRC
 * Size = Length of the payload data
 * Type = Type of record
 *        (kZeroType, kFullType, kFirstType, kLastType, kMiddleType )
 *        The type is used to group a bunch of records together to represent
 *        blocks that are larger than kBlockSize
 * Payload = Byte stream as long as specified by the payload size
 *
 * Recyclable record format:
 *
 * +---------+-----------+-----------+----------------+--- ... ---+
 * |CRC (4B) | Size (2B) | Type (1B) | Log number (4B)| Payload   |
 * +---------+-----------+-----------+----------------+--- ... ---+
 *
 * Same as above, with the addition of
 * Log number = 32bit log file number, so that we can distinguish between
 * records written by the most recent log writer vs a previous one.
 */
class Writer {
 public:
  // Create a writer that will append data to "*dest".
  // "*dest" must be initially empty.
  // "*dest" must remain live while this Writer is in use.
  explicit Writer(util::ConcurrentDirectFileWriter *dest,
                  uint64_t log_number, bool recycle_log_files,
                  bool use_allocator = false);

  explicit Writer(unique_ptr<util::WritableFileWriter>&& dest,
                  uint64_t log_number, bool recycle_log_files);
#ifndef NDEBUG
  virtual ~Writer();

  virtual common::Status AddRecord(const common::Slice& slice);
#else
  ~Writer();

  common::Status AddRecord(const common::Slice& slice);
#endif

  bool use_allocator() {
    return use_allocator_;
  }
  util::ConcurrentDirectFileWriter *release_file_writer() {
    auto dest = dest_;
    dest_ = nullptr;
    return dest;
  }
  void delete_file_writer(memory::SimpleAllocator *arena = nullptr);
  common::Status AddRecord(const common::Slice& slice, uint32_t crc);

  util::ConcurrentDirectFileWriter* file() { return dest_; }

  const util::ConcurrentDirectFileWriter* file() const { return dest_; }

  uint64_t get_log_number() const { return log_number_; }

  static uint32_t calculate_crc(const common::Slice& slice) {
    uint32_t crc = 0;
    // skip sequence number which accupy 8 bytes
    if (slice.size() <= 8) {
      crc = util::crc32c::Extend(crc, slice.data(), slice.size());
    } else {
      crc = util::crc32c::Extend(crc, slice.data() + 6, slice.size() - 6);
    }
    crc = util::crc32c::Mask(crc);
    return crc;
  }

  int sync()
  {
    int ret = common::Status::kOk;
    if (common::Status::kOk != (ret = dest_->sync(true).code())) {
      XENGINE_LOG(ERROR, "fail to sync file", K(ret));
      abort();
    }
    return ret;
  }
 private:
  util::ConcurrentDirectFileWriter *dest_;
  size_t block_offset_;  // Current offset in block
  uint64_t log_number_;
  bool recycle_log_files_;
  bool use_allocator_;

  // util::crc32c values for all supported record types.  These are
  // pre-computed to reduce the overhead of computing the crc of the
  // record type stored in the header.
  uint32_t type_crc_[kMaxRecordType + 1];

  common::Status add_record_with_crc(const common::Slice& slice,
                                     const uint32_t crc32);

  common::Status EmitPhysicalRecord(RecordType type, const char* ptr,
                                    size_t length, uint32_t crc);

  // No copying allowed
  Writer(const Writer&);
  void operator=(const Writer&);
};

}  // namespace log
}
}  // namespace xengine
