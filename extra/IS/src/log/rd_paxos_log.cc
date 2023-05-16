/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  rd_paxos_log.cc,v 1.0 08/15/2016 02:15:50 PM
 *hangfeng.fj(hangfeng.fj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file rd_paxos_log.cc
 * @author hangfeng.fj(hangfeng.fj@alibaba-inc.com)
 * @date 08/15/2016 02:15:50 PM
 * @version 1.0
 * @brief
 *
 **/

#include "rd_paxos_log.h"
#include "files.h"
#include "rocksdb/cache.h"
#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/options.h"
#include "rocksdb/slice.h"
#include "rocksdb/table.h"

const std::string logDBName = "@RDPaxosLog";
const std::string lastIndexTag = "@PAXOS_LOG_LASTINDEX@";
const std::string firstIndexTag =
    "@PAXOS_LOG_FIRSTINDEX@"; /* for truncate forward */

namespace alisql {

RDPaxosLog::RDPaxosLog(const std::string &dataDir, bool compress,
                       size_t writeBufferSize, bool sync)
    : db_(NULL),
      lastLogIndex_(UINT64_MAX),
      lastLogTerm_(0),
      firstLogIndex_(0),
      async_(false),
      sync_(sync),
      purgeLogFilter_(nullptr) {
  bool ok = mkdirs(dataDir.c_str());

  if (!ok) {
    easy_warn_log("failed to create dir :%s\n", dataDir.c_str());
    abort();
  }

  std::string fullName = dataDir + "/" + logDBName;
  rocksdb::Options options;
  options.create_if_missing = true;
  if (compress) {
    options.compression = rocksdb::kSnappyCompression;
    easy_warn_log("enable snappy compress for data storage\n");
  } else {
    options.compression = rocksdb::kNoCompression;
    easy_warn_log("enable snappy compress for data storage\n");
  }

  options.write_buffer_size = writeBufferSize;
  easy_warn_log("[rocksdb paxos log]: writer_buffer_size: %ld\n",
                options.write_buffer_size);

  // The maximum number of write buffers that are built up in memory.
  options.max_write_buffer_number = 8;

  // The minimum number of write buffers that will be merged together
  // before writing to storage.
  options.min_write_buffer_number_to_merge = 1;

  // Maximum number of concurrent background compaction jobs, submitted to
  // the default LOW priority thread pool.
  options.max_background_compactions = 16;

  options.max_background_flushes = 16;

  // Control maximum total data size for base level (level 1).
  options.max_bytes_for_level_base = 64 * 1024 * 1024;

  // Target file size for compaction.
  options.target_file_size_base = 64 * 1024 * 1024;

  rocksdb::BlockBasedTableOptions table_options;
  // table_options.block_cache= rocksdb::NewLRUCache(512 * 1024 * 1024);
  table_options.block_cache = rocksdb::NewLRUCache(writeBufferSize);
  table_options.block_size = 64 * 1024;
  table_options.filter_policy.reset(rocksdb::NewBloomFilterPolicy(10, false));
  options.table_factory.reset(
      rocksdb::NewBlockBasedTableFactory(table_options));

  rocksdb::Status status = rocksdb::DB::Open(options, fullName, &db_);
  if (!status.ok()) {
    easy_warn_log("failed to open db: %s err: %s\n", fullName.c_str(),
                  status.ToString().c_str());
    assert(status.ok());
  }

  initLastLogTerm();

  if (lastLogIndex_ == UINT64_MAX) {
    appendEmptyEntry();
  }
}

RDPaxosLog::~RDPaxosLog() { delete db_; }

int RDPaxosLog::getEmptyEntry(LogEntry &entry) {
  entry.set_term(0);
  entry.set_index(0);
  entry.set_optype(kNormal);
  entry.set_ikey("");
  entry.set_value("");

  return 0;
}

void RDPaxosLog::appendEmptyEntry() {
  LogEntry logEntry;
  getEmptyEntry(logEntry);
  appendEntry(logEntry);
}

void RDPaxosLog::initLastLogTerm() {
  std::string value;
  rocksdb::Status status =
      db_->Get(rocksdb::ReadOptions(), lastIndexTag, &value);
  if (status.ok() && !value.empty()) {
    lastLogIndex_ = stringToInt(value);
    if (lastLogIndex_ > 0) {
      LogEntry logEntry;
      bool ok = readEntry(lastLogIndex_, &logEntry);
      assert(ok);
      lastLogTerm_ = logEntry.term();
    }
  }
  status = db_->Get(rocksdb::ReadOptions(), firstIndexTag, &value);
  if (status.ok() && !value.empty()) {
    firstLogIndex_.store(stringToInt(value));
  } else {
    /* set to 0 */
    status = db_->Put(rocksdb::WriteOptions(), firstIndexTag,
                      intToString(firstLogIndex_.load()));
    assert(status.ok());
  }
}

int RDPaxosLog::getEntry(uint64_t logIndex, LogEntry &entry, bool fastfail) {
  bool ret = readEntry(logIndex, &entry);

  if (ret) {
    return 0;
  } else {
    /* log not exists or have been truncated */
    entry.set_optype(kMock);
    return -1;
  }
}

uint64_t RDPaxosLog::append(const LogEntry &entry) {
  uint64_t index = appendEntry(entry);
  return (async_ && entry.optype() != kNormal &&
          entry.optype() != kConfigureChange)
             ? 0
             : index;
}

uint64_t RDPaxosLog::append(
    const ::google::protobuf::RepeatedPtrField<LogEntry> &entries) {
  std::string buf;
  std::string curIndex;
  uint64_t index, startIndex, llt = 0, len;

  len = entries.size();
  if (len == 0) return 0;
  rocksdb::WriteOptions write_options;
  write_options.sync = sync_;
  rocksdb::WriteBatch batch;
  startIndex = index = entries.begin()->index();
  assert(index == (lastLogIndex_ + 1));
  for (auto it = entries.begin(); it != entries.end(); ++it) {
    assert(index == it->index());
    intToString(index, curIndex);
    encodeLogEntry(*it, &buf);
    batch.Put(curIndex, buf);
    if (index == (startIndex + len - 1)) llt = it->term();
    ++index;
  }
  assert((index - startIndex) == len);
  batch.Put(lastIndexTag, intToString(index - 1));
  rocksdb::Status status = db_->Write(write_options, &batch);
  assert(status.ok());

  std::lock_guard<std::mutex> lg(lock_);
  lastLogTerm_ = llt;
  lastLogIndex_ = index - 1;
  return lastLogIndex_;
}

void RDPaxosLog::encodeLogEntry(const LogEntry &logEntry, std::string *buf) {
  assert(buf);
  logEntry.SerializeToString(buf);
}

void RDPaxosLog::decodeLogEntry(const std::string &buf, LogEntry *logEntry) {
  assert(logEntry);
  logEntry->ParseFromString(buf);
}

uint64_t RDPaxosLog::appendEntry(const LogEntry &logEntry) {
  std::string buf;
  LogEntry le;
  le.CopyFrom(logEntry);
  std::string curIndex;
  uint64_t ret;
  {
    std::lock_guard<std::mutex> lg(lock_);
    if (lastLogIndex_ == UINT64_MAX) {
      lastLogIndex_ = 0;
    } else {
      lastLogIndex_++;
    }
    le.set_index(lastLogIndex_);
    encodeLogEntry(le, &buf);
    ret = lastLogIndex_;
    curIndex = intToString(lastLogIndex_);
    rocksdb::WriteOptions write_options;
    write_options.sync = sync_;
    rocksdb::WriteBatch batch;
    batch.Put(curIndex, buf);
    batch.Put(lastIndexTag, curIndex);
    rocksdb::Status status = db_->Write(write_options, &batch);
    assert(status.ok());
    lastLogTerm_ = le.term();
  }
  return ret;
}

uint64_t RDPaxosLog::getLastLogIndex() {
  std::lock_guard<std::mutex> lg(lock_);
  return lastLogIndex_;
}

uint64_t RDPaxosLog::getLastLogTerm() {
  std::lock_guard<std::mutex> lg(lock_);
  return lastLogTerm_;
}

uint64_t RDPaxosLog::getFirstLogIndex() { return firstLogIndex_.load(); }

bool RDPaxosLog::readEntry(uint64_t index, LogEntry *logEntry) {
  std::string value;
  std::string key = intToString(index);
  rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, &value);
  if (status.ok()) {
    decodeLogEntry(value, logEntry);
    return true;
  } else if (status.IsNotFound()) {
    return false;
  } else {
    abort();
  }
}

uint64_t RDPaxosLog::getLength() {
  std::lock_guard<std::mutex> lg(lock_);
  return lastLogIndex_ - firstLogIndex_ + 1;
}

void RDPaxosLog::truncateBackward(uint64_t firstIndex) {
  if (firstIndex < 1) {
    firstIndex = 1;
  }

  {
    std::lock_guard<std::mutex> lg(lock_);
    lastLogIndex_ = firstIndex - 1;
    rocksdb::WriteOptions write_options;
    write_options.sync = sync_;
    rocksdb::Status status =
        db_->Put(write_options, lastIndexTag, intToString(lastLogIndex_));
    assert(status.ok());
    if (lastLogIndex_ > 0) {
      LogEntry logEntry;
      bool slot_ok = readEntry(lastLogIndex_, &logEntry);
      assert(slot_ok);
      lastLogTerm_ = logEntry.term();
    }
  }
}

void RDPaxosLog::truncateForward(uint64_t lastIndex) {
  if (lastIndex > lastLogIndex_) {
    /* not allowed to truncate all log， keep the last one */
    lastIndex = lastLogIndex_;
  }
  std::lock_guard<std::mutex> lg(lock_);
  rocksdb::Status status;
  rocksdb::WriteOptions write_options;
  //    write_options.sync= sync_;
  write_options.sync = false;
  LogEntry entry;
  bool ret;
  /* remove [firstLogIndex_, lastIndex) */
  uint64_t currentIndex = firstLogIndex_;
  while (currentIndex < lastIndex) {
    if (unlikely(purgeLogFilter_ != nullptr)) {
      ret = readEntry(currentIndex, &entry);
      if (ret && (purgeLogFilter_ == nullptr || purgeLogFilter_(entry))) {
        status = db_->Delete(write_options, intToString(currentIndex));
      }
    } else {
      status = db_->Delete(write_options, intToString(currentIndex));
    }
    currentIndex++;
    status = db_->Put(write_options, firstIndexTag, intToString(currentIndex));
  }
  firstLogIndex_.store(currentIndex);
}

int RDPaxosLog::getMetaData(const std::string &key, std::string &value) {
  rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, &value);

  if (status.ok())
    return 0;
  else
    return -1;
}

int RDPaxosLog::setMetaData(const std::string &key, const std::string &value) {
  rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), key, value);
  return (status.ok()) ? 0 : -1;
}

int RDPaxosLog::getMetaData(const std::string &key, uint64_t *value) {
  std::string strValue;
  rocksdb::Status status = db_->Get(rocksdb::ReadOptions(), key, &strValue);

  if (status.ok()) {
    *value = stringToInt(strValue);
    return 0;
  } else {
    return -1;
  }
}

int RDPaxosLog::setMetaData(const std::string &key, const uint64_t value) {
  std::string strVal = intToString(value);
  rocksdb::Status status = db_->Put(rocksdb::WriteOptions(), key, strVal);
  return (status.ok()) ? 0 : -1;
}

std::string RDPaxosLog::intToString(uint64_t num) {
  std::string key;
  key.resize(sizeof(uint64_t));
  memcpy(&key[0], &num, sizeof(uint64_t));
  return key;
}

void RDPaxosLog::intToString(uint64_t num, std::string &key) {
  key.resize(sizeof(uint64_t));
  memcpy(&key[0], &num, sizeof(uint64_t));
}

uint64_t RDPaxosLog::stringToInt(const std::string &s) {
  assert(s.size() == sizeof(uint64_t));
  uint64_t num = 0;
  memcpy(&num, &s[0], sizeof(uint64_t));
  return num;
}

}  // namespace alisql
