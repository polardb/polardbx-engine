/************************************************************************
 *
 * Copyright (c) 2016 Alibaba.com, Inc. All Rights Reserved
 * $Id:  mem_paxos_log.cc,v 1.0 12/22/2016 3:05:36 PM
 *jarry.zj(jarry.zj@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file mem_paxos_log.cc
 * @author jarry.zj(jarry.zj@alibaba-inc.com)
 * @date 12/22/2016 3:05:36 PM
 * @version 1.0
 * @brief impl of memory based paxos log
 *
 **/

#include "mem_paxos_log.h"
#include <easy_log.h>
#include <chrono>

namespace alisql {
MemPaxosLog::MemPaxosLog(uint64_t lastLogIndex, uint64_t readTimeout,
                         uint64_t cacheSize)
    : lastLogIndex_(lastLogIndex),
      cacheSize_(cacheSize),
      readTimeout_(readTimeout),
      appendTimeout_(500) {}

MemPaxosLog::~MemPaxosLog() { shutdown(); }

int MemPaxosLog::getEntry(uint64_t logIndex, LogEntry &entry, bool fastfail) {
  std::lock_guard<std::mutex> lg(lock_);
  if (log_.size() == 0 || logIndex > lastLogIndex_ ||
      logIndex < log_.front()->index()) {
    if (fastfail)
      return -1;
    else {
    }
  }
  for (LogEntry *le : log_) {
    if (le->index() == logIndex) {
      entry.CopyFrom(*le);
      return 0;
    } else if (le->index() > logIndex) {
      return -1;
    }
  }
  return -1;
}

int MemPaxosLog::getEntry(LogEntry &entry) {
  std::unique_lock<std::mutex> ul(lock_);
  if (readTimeout_ == 0) {
    isEmptyCond_.wait(ul, [this]() { return log_.size() > 0; });
  } else {
    bool ret =
        isEmptyCond_.wait_for(ul, std::chrono::milliseconds(readTimeout_),
                              [this]() { return log_.size() > 0; });
    if (ret == false) {
      easy_warn_log("MemPaxosLog: log is emtry, getEntry timeout.\n");
      return -1;
    }
  }
  if (log_.size() == 0) {
    return -1;
  }
  LogEntry *fle = log_.front();
  entry.CopyFrom(*fle);
  delete fle;
  log_.pop_front();
  isFullCond_.notify_all();
  return 0;
}

int MemPaxosLog::getEmptyEntry(LogEntry &entry) {
  entry.set_term(0);
  entry.set_index(0);
  entry.set_optype(kNormal);
  entry.set_ikey("");
  entry.set_value("");

  return 0;
}

void MemPaxosLog::appendEmptyEntry() {
  LogEntry logEntry;
  getEmptyEntry(logEntry);
  append(logEntry);
}

uint64_t MemPaxosLog::getLastLogIndex() { return lastLogIndex_; }

uint64_t MemPaxosLog::getLength() {
  std::lock_guard<std::mutex> lg(lock_);
  return log_.size();
}

uint64_t MemPaxosLog::append(const LogEntry &entry) {
  std::unique_lock<std::mutex> ul(lock_);
  if (appendTimeout_ == 0) {
    isFullCond_.wait(ul, [this]() { return log_.size() < cacheSize_; });
  } else {
    bool ret =
        isFullCond_.wait_for(ul, std::chrono::milliseconds(appendTimeout_),
                             [this]() { return log_.size() < cacheSize_; });
    if (ret == false) {
      easy_warn_log("MemPaxosLog: log is full, append timeout.\n");
      return lastLogIndex_;
    }
  }

  LogEntry *le = new LogEntry(entry);
  if (le->index() == 0) {
    le->set_index(lastLogIndex_ + 1);
  }
  assert(le->index() == (lastLogIndex_ + 1));
  log_.push_back(le);
  lastLogIndex_ = le->index();
  isEmptyCond_.notify_all();
  return lastLogIndex_;
}

uint64_t MemPaxosLog::append(
    const ::google::protobuf::RepeatedPtrField<LogEntry> &entries) {
  std::unique_lock<std::mutex> ul(lock_);
  if (appendTimeout_ == 0) {
    isFullCond_.wait(ul, [this]() { return log_.size() < cacheSize_; });
  } else {
    bool ret =
        isFullCond_.wait_for(ul, std::chrono::milliseconds(appendTimeout_),
                             [this]() { return log_.size() < cacheSize_; });
    if (ret == false) {
      easy_warn_log("MemPaxosLog: log is full, append timeout.\n");
      return lastLogIndex_;
    }
  }

  for (auto it = entries.begin(); it != entries.end(); ++it) {
    LogEntry *le = new LogEntry(*it);
    assert(le->index() == (lastLogIndex_ + 1));
    log_.push_back(le);
    lastLogIndex_ = le->index();
  }
  isEmptyCond_.notify_all();
  return lastLogIndex_;
}

void MemPaxosLog::shutdown() {
  isFullCond_.notify_all();
  isEmptyCond_.notify_all();
  std::unique_lock<std::mutex> ul(lock_);
  for (auto le : log_) {
    delete le;
  }
  log_.clear();
}

void MemPaxosLog::truncateBackward(uint64_t firstIndex) {
  // not impl
}

void MemPaxosLog::truncateForward(uint64_t lastIndex) {
  /* clear log before lastIndex (exclude!) */
  if (lastIndex > lastLogIndex_) {
    lastIndex = lastLogIndex_;
  }
  std::unique_lock<std::mutex> ul(lock_);
  uint64_t l = 0, r = 0;
  while (!log_.empty()) {
    LogEntry *fle = log_.front();
    if (fle->index() < lastIndex) {
      if (l == 0) l = fle->index();
      r = fle->index();
      delete fle;
      log_.pop_front();
    } else {
      break;
    }
  }
  easy_warn_log("truncate log index [%ld-%ld].\n", l, r);
  isFullCond_.notify_all();
}

int MemPaxosLog::getMetaData(const std::string &key, uint64_t *value) {
  // not impl
  return -1;
}

int MemPaxosLog::setMetaData(const std::string &key, const uint64_t value) {
  // not impl
  return -1;
}

void MemPaxosLog::resetLastLogIndex(uint lli) {
  /*
   * we already hold witness lock outside, so no concurrent append, nobody wait
   * append we clear the log here, so if someone wait getEntry, keep waiting
   */
  std::unique_lock<std::mutex> ul(lock_);
  log_.clear();
  lastLogIndex_ = lli;
}

}  // namespace alisql
