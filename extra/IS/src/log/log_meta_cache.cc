/************************************************************************
 *
 * Copyright (c) 2019 Alibaba.com, Inc. All Rights Reserved
 * $Id:  log_meta_cache.cc,v 1.0 07/13/2019 17:27:00 PM
 *aili.xp(aili.xp@alibaba-inc.com) $
 *
 ************************************************************************/

/**
 * @file log_meta_cache.cc
 * @author aili.xp(aili.xp@alibaba-inc.com)
 * @date 07/13/2019 17:27:00 PM
 * @version 1.0
 * @brief paxos log meta cache
 *
 **/

#include "log_meta_cache.h"

#include <cassert>

namespace alisql {

LogMetaCache::LogMetaCache() { reset(); }

void LogMetaCache::init() { array_.resize(MaxEntrySize); }

void LogMetaCache::reset() {
  count_ = 0;
  left_ = 0;
  right_ = 0;
  maxIndex_ = 0;
}

bool LogMetaCache::putLogMeta(uint64_t index, uint64_t term, uint64_t optype,
                              uint64_t info) {
  assert(index);
  if (count_ != 0 && index < maxIndex_ + 1) return false;
  if (count_ != 0 && index > maxIndex_ + 1) reset();

  if (count_ >= MaxEntrySize) {
    left_ = (left_ + 1) % MaxEntrySize;
    count_ = count_ - 1;
  }

  array_[right_] = LogMetaEntry{index, term, optype, info};
  right_ = (right_ + 1) % MaxEntrySize;
  ++count_;
  maxIndex_ = index;
  return true;
}

bool LogMetaCache::getLogMeta(uint64_t index, uint64_t *term, uint64_t *optype,
                              uint64_t *info) {
  if (count_ == 0 || index < array_[left_].index || index > maxIndex_)
    return false;

  uint64_t pos = (left_ + index - array_[left_].index) % MaxEntrySize;
  assert(index == array_[pos].index);
  *term = array_[pos].term;
  *optype = array_[pos].optype;
  *info = array_[pos].info;
  return true;
}

}  // namespace alisql
