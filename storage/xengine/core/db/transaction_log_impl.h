// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
#pragma once

#ifndef ROCKSDB_LITE
#include <vector>

#include "db/log_reader.h"
#include "db/version_set.h"
#include "options/db_options.h"
#include "port/port.h"
#include "util/filename.h"
#include "xengine/env.h"
#include "xengine/options.h"
#include "xengine/transaction_log.h"
#include "xengine/types.h"

namespace xengine {
namespace db {

class LogFileImpl : public LogFile {
 public:
  LogFileImpl(uint64_t logNum, WalFileType logType,
              common::SequenceNumber startSeq, uint64_t sizeBytes)
      : logNumber_(logNum),
        type_(logType),
        startSequence_(startSeq),
        sizeFileBytes_(sizeBytes) {}

  std::string PathName() const override {
    if (type_ == kArchivedLogFile) {
      return util::ArchivedLogFileName("", logNumber_);
    }
    return util::LogFileName("", logNumber_);
  }

  uint64_t LogNumber() const override { return logNumber_; }

  WalFileType Type() const override { return type_; }

  common::SequenceNumber StartSequence() const override {
    return startSequence_;
  }

  uint64_t SizeFileBytes() const override { return sizeFileBytes_; }

  bool operator<(const LogFile& that) const {
    return LogNumber() < that.LogNumber();
  }

 private:
  uint64_t logNumber_;
  WalFileType type_;
  common::SequenceNumber startSequence_;
  uint64_t sizeFileBytes_;
};

class TransactionLogIteratorImpl : public TransactionLogIterator {
 public:
  TransactionLogIteratorImpl(
      const std::string& dir, const common::ImmutableDBOptions* options,
      const db::TransactionLogIterator::ReadOptions& read_options,
      const util::EnvOptions& soptions, const common::SequenceNumber seqNum,
      std::unique_ptr<VectorLogPtr, memory::ptr_destruct_delete<VectorLogPtr>> files,
      VersionSet const* const versions);

  virtual bool Valid() override;

  virtual void Next() override;

  virtual common::Status status() override;

  virtual BatchResult GetBatch() override;

 private:
  const std::string& dir_;
  const common::ImmutableDBOptions* options_;
  const TransactionLogIterator::ReadOptions read_options_;
  const util::EnvOptions& soptions_;
  common::SequenceNumber startingSequenceNumber_;
  std::unique_ptr<VectorLogPtr, memory::ptr_destruct_delete<VectorLogPtr>> files_;
  bool started_;
  bool isValid_;  // not valid when it starts of.
  common::Status currentStatus_;
  size_t currentFileIndex_;
  std::unique_ptr<WriteBatch, memory::ptr_destruct_delete<WriteBatch>> currentBatch_;
  unique_ptr<log::Reader, memory::ptr_destruct_delete<log::Reader>> currentLogReader_;
  common::Status OpenLogFile(const LogFile* logFile,
                             util::SequentialFileReader *&file);

  struct LogReporter : public log::Reader::Reporter {
    util::Env* env;
    virtual void Corruption(size_t bytes, const common::Status& s) override {
      __XENGINE_LOG(INFO, "dropping %" ROCKSDB_PRIszt " bytes; %s", bytes,
                      s.ToString().c_str());
    }
    virtual void Info(const char* s) { __XENGINE_LOG(INFO, "%s", s); }
  } reporter_;

  common::SequenceNumber currentBatchSeq_;  // sequence number at start of current batch
  common::SequenceNumber currentLastSeq_;  // last sequence in the current batch
  // Used only to get latest seq. num
  // TODO(icanadi) can this be just a callback?
  VersionSet const* const versions_;

  // Reads from transaction log only if the writebatch record has been written
  bool RestrictedRead(common::Slice* record, std::string* scratch);
  // Seeks to startingSequenceNumber reading from startFileIndex in files_.
  // If strict is set,then must get a batch starting with startingSequenceNumber
  void SeekToStartSequence(uint64_t startFileIndex = 0, bool strict = false);
  // Implementation of Next. SeekToStartSequence calls it internally with
  // internal=true to let it find next entry even if it has to jump gaps because
  // the iterator may start off from the first available entry but promises to
  // be continuous after that
  void NextImpl(bool internal = false);
  // Check if batch is expected, else return false
  bool IsBatchExpected(const WriteBatch* batch,
                       common::SequenceNumber expectedSeq);
  // Update current batch if a continuous batch is found, else return false
  void UpdateCurrentWriteBatch(const common::Slice& record);
  common::Status OpenLogReader(const LogFile* file);
};
}
}  // namespace xengine
#endif  // ROCKSDB_LITE
