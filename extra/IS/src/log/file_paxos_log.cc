/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  file_paxos_log.cc,v 1.0 08/15/2016 02:15:50 PM
 *hangfeng.fj(hangfeng.fj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file file_paxos_log.cc
 * @author hangfeng.fj(hangfeng.fj@alibaba-inc.com)
 * @date 08/15/2016 02:15:50 PM
 * @version 1.0
 * @brief
 *
 **/

#include "file_paxos_log.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

const std::string logDBName = "@FilePaxosLog";
const std::string lastIndexTag = "@PAXOS_LOG_LEN@";

namespace alisql {

FilePaxosLog::FilePaxosLog(const std::string &dataDir, LogTypeT type)
    : length_(0), lastLogTerm_(0), async_(false), type_(type), fd_(-1) {
  fd_ = open(dataDir.c_str(), O_CREAT | O_RDWR | O_APPEND, 0644);
  log_.reserve(1000);
  initLastLogTerm();

  if (length_ == 0) {
    appendEmptyEntry();
  }
}

FilePaxosLog::~FilePaxosLog() {
  if (fd_ != -1) close(fd_);

  for (auto le : log_) delete le;
}

int FilePaxosLog::getEmptyEntry(LogEntry &entry) {
  entry.set_term(0);
  entry.set_index(0);
  entry.set_optype(kNormal);
  entry.set_ikey("");
  entry.set_value("");

  return 0;
}

void FilePaxosLog::appendEmptyEntry() {
  LogEntry logEntry;
  getEmptyEntry(logEntry);
  appendEntry(logEntry);
}

void FilePaxosLog::initLastLogTerm() {
  /*TODO read from file*/
  lastLogTerm_ = 0;
}

int FilePaxosLog::getEntry(uint64_t logIndex, LogEntry &entry, bool fastfail) {
  lock_.lock();
  bool ret = readEntry(logIndex, &entry);
  lock_.unlock();

  entry.set_index(logIndex);
  if (ret) {
    return 0;
  } else {
    return -1;
  }
}

const LogEntry *FilePaxosLog::getEntry(uint64_t logIndex, bool fastfail) {
  if (logIndex > length_ - 1) return NULL;

  lock_.lock();
  LogEntry *le = log_[logIndex];
  lock_.unlock();
  le->set_index(logIndex);

  return le;
}

uint64_t FilePaxosLog::append(const LogEntry &entry) {
  uint64_t index = appendEntry(entry);
  return async_ ? 0 : index;
}

uint64_t FilePaxosLog::appendWithCheck(const LogEntry &logEntry) {
  uint64_t ret;
  std::string buf;
  auto le = new LogEntry(logEntry);
  le->set_index(0);
  encodeLogEntry(*le, &buf);

  lock_.lock();
  if (currentTerm_ != le->term()) {
    lock_.unlock();
    return 0;
  }
  if (length_ + 1 >= log_.capacity()) log_.reserve(2 * log_.capacity());
  log_.push_back(le);
  ret = length_++;
  lastLogTerm_ = le->term();
  /* we set index when read entry. */
  // le->set_index(ret);
  // encodeLogEntry(*le, &buf);
  if (type_ >= LTFile) write(fd_, buf.c_str(), buf.size());
  if (type_ >= LTSync) fdatasync(fd_);
  // fs_.sync();
  lock_.unlock();

  return ret;
}

uint64_t FilePaxosLog::append(
    const ::google::protobuf::RepeatedPtrField<LogEntry> &entries) {
  uint64_t index, startIndex, len;
  std::string buf;

  startIndex = index = entries.begin()->index();
  len = entries.size();
  assert(index == length_);

  lock_.lock();
  if (length_ + len >= log_.capacity()) log_.reserve(2 * log_.capacity());
  for (auto it = entries.begin(); it != entries.end(); ++it) {
    assert(index == it->index());
    auto le = new LogEntry(*it);
    log_.push_back(le);
    // log_[index]= le;

    le->set_index(index);
    encodeLogEntry(*le, &buf);
    if (type_ >= LTFile) write(fd_, buf.c_str(), buf.size());
    if (index == (startIndex + len - 1)) lastLogTerm_ = it->term();
    ++index;
  }
  if (type_ >= LTSync) fdatasync(fd_);
  // fs_.sync();
  length_ = index;
  lock_.unlock();
  assert((index - startIndex) == len);

  return index - 1;
}

void FilePaxosLog::encodeLogEntry(const LogEntry &logEntry, std::string *buf) {
  assert(buf);
  logEntry.SerializeToString(buf);
}

void FilePaxosLog::decodeLogEntry(const std::string &buf, LogEntry *logEntry) {
  assert(logEntry);
  logEntry->ParseFromString(buf);
}

uint64_t FilePaxosLog::appendEntry(const LogEntry &logEntry) {
  uint64_t ret;
  std::string buf;
  auto le = new LogEntry(logEntry);
  le->set_index(0);
  encodeLogEntry(*le, &buf);

  lock_.lock();
  if (length_ + 1 >= log_.capacity()) log_.reserve(2 * log_.capacity());
  log_.push_back(le);
  ret = length_++;
  lastLogTerm_ = le->term();
  /* we set index when read entry. */
  // le->set_index(ret);
  // encodeLogEntry(*le, &buf);
  if (type_ >= LTFile) write(fd_, buf.c_str(), buf.size());
  if (type_ >= LTSync) fdatasync(fd_);
  // fs_.sync();
  lock_.unlock();

  return ret;
}

uint64_t FilePaxosLog::getLastLogIndex() {
  std::lock_guard<std::mutex> lg(lock_);
  return length_ - 1;
}

uint64_t FilePaxosLog::getLastLogTerm() {
  std::lock_guard<std::mutex> lg(lock_);
  return lastLogTerm_;
}

bool FilePaxosLog::readEntry(uint64_t index, LogEntry *logEntry) {
  if (index > length_ - 1) return false;

  logEntry->CopyFrom(*(log_[index]));
  return true;
}

uint64_t FilePaxosLog::getLength() {
  std::lock_guard<std::mutex> lg(lock_);
  return length_;
}

void FilePaxosLog::truncateBackward(uint64_t firstIndex) {
  if (firstIndex < 0) {
    firstIndex = 0;
  }

  {
    std::lock_guard<std::mutex> lg(lock_);
    for (auto it = log_.begin() + firstIndex; it != log_.end(); ++it)
      delete *it;
    log_.erase(log_.begin() + firstIndex, log_.end());
    length_ = firstIndex;
    lastLogTerm_ = log_[length_ - 1]->term();
  }
}

void FilePaxosLog::truncateForward(uint64_t lastIndex) {}

int FilePaxosLog::getMetaData(const std::string &key, uint64_t *value) {
  if (key == Paxos::keyCurrentTerm)
    *value = 1;
  else if (key == Paxos::keyVoteFor)
    *value = 0;
  else
    return -1;
  return 0;
}

int FilePaxosLog::setMetaData(const std::string &key, const uint64_t value) {
  return 0;
}

std::string FilePaxosLog::intToString(uint64_t num) {
  std::string key;
  key.resize(sizeof(uint64_t));
  memcpy(&key[0], &num, sizeof(uint64_t));
  return key;
}

void FilePaxosLog::intToString(uint64_t num, std::string &key) {
  key.resize(sizeof(uint64_t));
  memcpy(&key[0], &num, sizeof(uint64_t));
}

uint64_t FilePaxosLog::stringToInt(const std::string &s) {
  assert(s.size() == sizeof(uint64_t));
  uint64_t num = 0;
  memcpy(&num, &s[0], sizeof(uint64_t));
  return num;
}

bool FilePaxosLog::debugDisableWriteFile = false;

}  // namespace alisql
